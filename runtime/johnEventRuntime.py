from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

import discord

from db.sqlite import execute, fetchOne
from runtime import eventIngest

log = logging.getLogger(__name__)

FetchOne = Callable[[str, tuple], Awaitable[dict | None]]
Execute = Callable[[str, tuple], Awaitable[Any]]


def _isProtectedSheetsWriteError(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        lowered = str(current).lower()
        if "protected cell" in lowered or "protected cell or object" in lowered:
            return True
        current = current.__cause__ if isinstance(current.__cause__, BaseException) else None
    return False


class JohnEventCoordinator:
    """Ingest John event logs and maintain the startup backfill lifecycle."""

    successEmoji = "\N{WHITE HEAVY CHECK MARK}"

    def __init__(
        self,
        *,
        botClient,
        configModule,
        taskBudgeter,
        orbatSheets,
        robloxUsersModule,
        orbatAuditRuntime,
        privateExtensionsEnabled: bool,
        fetchOneFn: FetchOne = fetchOne,
        executeFn: Execute = execute,
        dispatcher: eventIngest.EventIngestDispatcher | None = None,
    ) -> None:
        self.botClient = botClient
        self.config = configModule
        self.taskBudgeter = taskBudgeter
        self.orbatSheets = orbatSheets
        self.robloxUsers = robloxUsersModule
        self.orbatAuditRuntime = orbatAuditRuntime
        self.privateExtensionsEnabled = bool(privateExtensionsEnabled)
        self.fetchOne = fetchOneFn
        self.execute = executeFn
        self.dispatcher = dispatcher or eventIngest.EventIngestDispatcher(
            [eventIngest.JohnEventLogAdapter(configModule=configModule)]
        )
        self._startupBackfillTask: asyncio.Task | None = None
        self._startupBackfillStarted = False
        self._sheetWritesSuspended = False

    def writesEnabled(self) -> bool:
        return bool(getattr(self.config, "johnEventOrbatWritesEnabled", False)) and (
            self.privateExtensionsEnabled
        )

    async def parse(self, message: discord.Message) -> list[eventIngest.IngestEvent]:
        return await self.dispatcher.parse(message)

    async def _alreadyProcessed(self, messageId: int) -> bool:
        row = await self.fetchOne(
            "SELECT messageId FROM john_event_log_messages WHERE messageId = ?",
            (messageId,),
        )
        return row is not None

    async def _markProcessed(
        self,
        message: discord.Message,
        hostId: int | None,
        category: str,
    ) -> None:
        await self.execute(
            """
            INSERT OR IGNORE INTO john_event_log_messages
            (messageId, channelId, hostId, eventCategory)
            VALUES (?, ?, ?, ?)
            """,
            (message.id, message.channel.id, hostId, category),
        )

    async def _reactToProcessed(self, message: discord.Message) -> None:
        try:
            await self.taskBudgeter.runDiscord(
                lambda: message.add_reaction(self.successEmoji)
            )
        except (discord.Forbidden, discord.NotFound):
            log.warning(
                "Could not add success reaction to John log message %s.",
                message.id,
                exc_info=True,
            )
        except discord.HTTPException:
            log.exception(
                "Discord rejected success reaction for John log message %s.",
                message.id,
            )
        except Exception:
            log.exception("Failed to add success reaction for John log message %s.", message.id)

    def _messageHasSuccessReaction(self, message: discord.Message) -> bool:
        for reaction in getattr(message, "reactions", []) or []:
            if str(getattr(reaction, "emoji", "")) != self.successEmoji:
                continue
            if bool(getattr(reaction, "me", False)):
                return True
        return False

    def _isExpectedAuthor(self, message: discord.Message) -> bool:
        try:
            johnBotId = int(getattr(self.config, "johnClankerBotId", 0) or 0)
            authorId = int(getattr(getattr(message, "author", None), "id", 0) or 0)
        except (TypeError, ValueError):
            return False
        return johnBotId > 0 and authorId == johnBotId

    async def _incrementCounter(
        self,
        message: discord.Message,
        hostId: int,
        columnKey: str,
    ) -> int:
        if self._sheetWritesSuspended:
            return 0

        try:
            row = await self.taskBudgeter.runSheetsThread(
                self.orbatSheets.incrementEventCount,
                hostId,
                columnKey,
                1,
            )
            if row != 0:
                return int(row)

            guildId = int(getattr(getattr(message, "guild", None), "id", 0) or 0)
            lookup = await self.robloxUsers.fetchRobloxUser(
                hostId,
                guildId if guildId > 0 else None,
            )
            if lookup.robloxUsername:
                row = await self.taskBudgeter.runSheetsThread(
                    self.orbatSheets.incrementEventCount,
                    hostId,
                    columnKey,
                    1,
                    robloxUser=lookup.robloxUsername,
                )
            return int(row or 0)
        except Exception as exc:
            if _isProtectedSheetsWriteError(exc):
                self._sheetWritesSuspended = True
                log.warning(
                    "John event PMD counter writes suspended after protected-sheet error "
                    "on message %s.",
                    message.id,
                    exc_info=True,
                )
                return 0
            log.exception("John event PMD counter write failed for message %s.", message.id)
            return 0

    async def handleIngestedEvent(
        self,
        message: discord.Message,
        event: eventIngest.IngestEvent,
    ) -> None:
        if event.eventType != "john.orbatIncrement":
            return
        if await self._alreadyProcessed(event.messageId):
            return
        if not self.writesEnabled():
            return

        hostId = int(event.hostId or 0)
        if hostId <= 0:
            log.warning("John log message %s missing host mention.", message.id)
            return

        category = str(event.payload.get("eventCategory") or "other").strip().lower()
        columnKey = "shifts" if category == "shift" else "otherEvents"
        row = await self._incrementCounter(message, hostId, columnKey)
        if row == 0:
            log.warning("ORBAT row not found for host %s in John log %s.", hostId, message.id)
            return

        try:
            await self.orbatAuditRuntime.sendOrbatChangeLog(
                self.botClient,
                title="Spreadsheet Change",
                change="Incremented ORBAT event counter from John event log.",
                requestedBy=f"<@{hostId}>",
                authorizedBy="John event log",
                requestMessageUrl=message.jump_url,
                details=(
                    f"Category: {category if category in {'shift', 'other'} else 'other'} | "
                    f"Column: {columnKey} | Row: {int(row)} | "
                    f"Event: {str(event.payload.get('eventTypeRaw') or '').strip() or 'unknown'}"
                ),
                sheetKey="generalStaff",
            )
        except Exception:
            log.exception(
                "Failed to post ORBAT increment audit log for John message %s.",
                message.id,
            )

        persistedCategory = category if category in {"shift", "other"} else "other"
        await self._markProcessed(message, hostId, persistedCategory)
        await self._reactToProcessed(message)

    async def handleHistoryMessage(self, message: discord.Message) -> str:
        if not self._isExpectedAuthor(message):
            return "ignored-author"
        if self._messageHasSuccessReaction(message):
            return "already-reacted"

        parsedEvents = await self.parse(message)
        johnEvents = [
            event for event in parsedEvents if event.eventType == "john.orbatIncrement"
        ]
        if not johnEvents:
            return "ignored"

        if await self._alreadyProcessed(message.id):
            await self._reactToProcessed(message)
            return "already-processed-marked"

        try:
            await self.handleIngestedEvent(message, johnEvents[0])
        except Exception:
            log.exception("John event startup backfill failed for message %s.", message.id)
            return "failed"

        if await self._alreadyProcessed(message.id):
            return "processed"
        return "skipped"

    @staticmethod
    async def _fetchStartupHistory(channel, since: datetime) -> list[discord.Message]:
        return [
            message
            async for message in channel.history(
                limit=None,
                after=since,
                oldest_first=True,
            )
        ]

    async def _runStartupBackfill(self) -> None:
        await self.botClient.wait_until_ready()
        if not self.writesEnabled():
            log.info("John event startup backfill skipped; ORBAT event writes are disabled.")
            return

        try:
            channelId = int(getattr(self.config, "johnEventLogChannelId", 0) or 0)
        except (TypeError, ValueError):
            channelId = 0
        if channelId <= 0:
            log.info("John event startup backfill skipped; no channel is configured.")
            return

        since = datetime.now(timezone.utc) - timedelta(hours=24)
        try:
            channel = self.botClient.get_channel(channelId)
            if channel is None:
                channel = await self.taskBudgeter.runLowPriorityDiscord(
                    lambda: self.botClient.fetch_channel(channelId)
                )
        except Exception:
            log.exception("John event startup backfill could not fetch channel %s.", channelId)
            return

        if not hasattr(channel, "history"):
            log.warning(
                "John event startup backfill skipped; channel %s does not support message history.",
                channelId,
            )
            return

        try:
            messages = await self.taskBudgeter.runLowPriorityDiscord(
                lambda: self._fetchStartupHistory(channel, since)
            )
        except Exception:
            log.exception(
                "John event startup backfill could not read history for channel %s.",
                channelId,
            )
            return

        counts: dict[str, int] = {}
        for message in messages:
            try:
                result = await self.handleHistoryMessage(message)
            except Exception:
                log.exception(
                    "John event startup backfill crashed while processing message %s.",
                    getattr(message, "id", "unknown"),
                )
                result = "failed"
            counts[result] = counts.get(result, 0) + 1

        log.info(
            "John event startup backfill complete: channel=%s since=%s total=%s results=%s",
            channelId,
            since.isoformat(),
            len(messages),
            counts,
        )

    def start(self) -> None:
        if self._startupBackfillStarted:
            return
        if self._startupBackfillTask is not None and not self._startupBackfillTask.done():
            return
        self._startupBackfillStarted = True
        task = asyncio.create_task(
            self._runStartupBackfill(),
            name="john-event-startup-backfill",
        )
        self._startupBackfillTask = task

        def _doneCallback(doneTask: asyncio.Task) -> None:
            try:
                doneTask.result()
            except asyncio.CancelledError:
                log.info("John event startup backfill task was cancelled.")
            except Exception:
                log.exception("John event startup backfill task crashed.")

        task.add_done_callback(_doneCallback)

    async def stop(self) -> None:
        task = self._startupBackfillTask
        if task is None:
            return
        if not task.done():
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        self._startupBackfillTask = None
