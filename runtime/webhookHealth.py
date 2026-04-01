from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import discord

from db.sqlite import fetchAll

log = logging.getLogger(__name__)


def _nowUtc() -> datetime:
    return datetime.now(timezone.utc)


class WebhookHealthWatcher:
    def __init__(
        self,
        *,
        botClient: Any,
        taskBudgeter: Any,
        auditStream: Any,
        checkIntervalSec: int = 600,
    ) -> None:
        self.botClient = botClient
        self.taskBudgeter = taskBudgeter
        self.auditStream = auditStream
        self.checkIntervalSec = max(120, int(checkIntervalSec))
        self.workerTask: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._lastRunAt: datetime | None = None
        self._lastSummary: dict[str, int] = {
            "checked": 0,
            "missing": 0,
            "errors": 0,
        }
        self._missingDedupUntil: dict[str, datetime] = {}
        self._dedupTtl = timedelta(hours=2)

    async def _resolveChannel(self, channelId: int) -> Any | None:
        channel = self.botClient.get_channel(int(channelId))
        if channel is not None:
            return channel
        try:
            channel = await self.taskBudgeter.runDiscord(lambda: self.botClient.fetch_channel(int(channelId)))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException, discord.InvalidData):
            return None
        return channel

    async def _messageExists(self, *, channelId: int, messageId: int) -> bool:
        channel = await self._resolveChannel(channelId)
        if channel is None:
            return False
        if not hasattr(channel, "fetch_message"):
            return False
        try:
            await self.taskBudgeter.runDiscord(lambda: channel.fetch_message(int(messageId)))
            return True
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return False

    def _dedupKey(self, source: str, itemId: int) -> str:
        return f"{source}:{int(itemId)}"

    async def _logMissing(
        self,
        *,
        source: str,
        guildId: int,
        itemId: int,
        message: str,
        details: dict[str, Any],
    ) -> None:
        key = self._dedupKey(source, itemId)
        now = _nowUtc()
        cutoff = self._missingDedupUntil.get(key)
        if cutoff is not None and now < cutoff:
            return
        self._missingDedupUntil[key] = now + self._dedupTtl
        await self.auditStream.logEvent(
            source="webhook-health",
            action=message,
            guildId=int(guildId or 0),
            targetType=source,
            targetId=str(itemId),
            severity="WARN",
            details=details,
            authorizedBy="automatic watcher",
            postToDiscord=True,
        )

    async def runCheck(self) -> dict[str, int]:
        summary = {"checked": 0, "missing": 0, "errors": 0}

        # Applications hubs
        hubRows = await fetchAll(
            """
            SELECT messageId, guildId, channelId, divisionKey
            FROM division_hub_messages
            """
        )
        for row in hubRows:
            summary["checked"] += 1
            messageId = int(row.get("messageId") or 0)
            channelId = int(row.get("channelId") or 0)
            guildId = int(row.get("guildId") or 0)
            if messageId <= 0 or channelId <= 0:
                continue
            try:
                if await self._messageExists(channelId=channelId, messageId=messageId):
                    continue
                summary["missing"] += 1
                await self._logMissing(
                    source="division_hub_message",
                    guildId=guildId,
                    itemId=messageId,
                    message="Application hub message missing",
                    details={
                        "channelId": channelId,
                        "divisionKey": str(row.get("divisionKey") or ""),
                    },
                )
            except Exception:
                summary["errors"] += 1
                log.exception("Webhook health check failed for application hub message %s.", messageId)

        # Scheduled events posts
        eventRows = await fetchAll(
            """
            SELECT eventId, guildId, channelId, messageId, title
            FROM scheduled_events
            WHERE status = 'ACTIVE' AND messageId > 0
            """
        )
        for row in eventRows:
            summary["checked"] += 1
            eventId = int(row.get("eventId") or 0)
            channelId = int(row.get("channelId") or 0)
            messageId = int(row.get("messageId") or 0)
            guildId = int(row.get("guildId") or 0)
            if messageId <= 0 or channelId <= 0:
                continue
            try:
                if await self._messageExists(channelId=channelId, messageId=messageId):
                    continue
                summary["missing"] += 1
                await self._logMissing(
                    source="scheduled_event",
                    guildId=guildId,
                    itemId=eventId,
                    message="Scheduled event message missing",
                    details={
                        "channelId": channelId,
                        "messageId": messageId,
                        "title": str(row.get("title") or ""),
                    },
                )
            except Exception:
                summary["errors"] += 1
                log.exception("Webhook health check failed for scheduled event %s.", eventId)

        self._lastRunAt = _nowUtc()
        self._lastSummary = dict(summary)
        return summary

    def getStats(self) -> dict[str, Any]:
        return {
            "lastRunAt": self._lastRunAt.isoformat() if self._lastRunAt else "",
            "summary": dict(self._lastSummary),
            "workerRunning": bool(self.workerTask and not self.workerTask.done()),
        }

    async def _runLoop(self) -> None:
        while True:
            try:
                async with self._lock:
                    await self.runCheck()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Webhook health watcher loop error.")
            await asyncio.sleep(self.checkIntervalSec)

    def start(self) -> None:
        if self.workerTask is not None and not self.workerTask.done():
            return
        self.workerTask = asyncio.create_task(self._runLoop())

    async def stop(self) -> None:
        if self.workerTask is None:
            return
        task = self.workerTask
        self.workerTask = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
