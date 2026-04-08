from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import discord

from db.sqlite import execute, fetchAll, fetchOne

log = logging.getLogger(__name__)

_hostMentionRegex = re.compile(r"<@!?(\d+)>")
_certificationTitleRegex = re.compile(
    r"^(Grid|Emergency|Turbine|Solo|Supervisor) Certification(?: (Training|Examination))? Session Completed$",
    re.IGNORECASE,
)
_defaultStatsOrder = [
    "ORIENTATION",
    "GRID_TRAINING",
    "GRID_EXAM",
    "EMERGENCY_TRAINING",
    "EMERGENCY_EXAM",
    "TURBINE",
    "SOLO",
    "SUPERVISOR",
]
_weeklySummaryTypeOrder = [
    ("GRID", "Grid"),
    ("EMERGENCY", "Emergency"),
    ("TURBINE", "Turbine"),
    ("SOLO", "Solo"),
    ("SUPERVISOR", "Supervisor"),
]


@dataclass(slots=True)
class ParsedTrainingResult:
    eventKind: str
    certType: str
    certVariant: str
    title: str
    hostId: int
    hostText: str
    passCount: int
    failCount: int


def _normalizeWhitespace(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def _normalizeNameLookup(value: object) -> str:
    text = _normalizeWhitespace(value)
    if text.startswith("@"):
        text = text[1:].strip()
    text = re.sub(r"\[[^\]]+\]", "", text).strip()
    return _normalizeWhitespace(text).casefold()


def _formatPercent(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "n/a"
    return f"{(float(numerator) / float(denominator)) * 100.0:.1f}%"


def _parseIsoOrNow(rawValue: object) -> datetime:
    rawText = str(rawValue or "").strip()
    if rawText:
        try:
            parsed = datetime.fromisoformat(rawText)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


class TrainingLogCoordinator:
    def __init__(
        self,
        *,
        botClient: discord.Client,
        configModule: Any,
        taskBudgeter: Any,
        recruitmentService: Any,
        webhookModule: Any,
    ) -> None:
        self.botClient = botClient
        self.config = configModule
        self.taskBudgeter = taskBudgeter
        self.recruitmentService = recruitmentService
        self.webhooks = webhookModule
        self._syncLock = asyncio.Lock()
        self._summaryLock = asyncio.Lock()
        self._messageLocks: dict[int, asyncio.Lock] = {}
        self._lastReadySyncAt: datetime | None = None
        self._readySyncCooldownSec = 120
        self._summarySettingKey = "trainingLogSummaryMessageId"
        self._summaryChannelSettingKey = "trainingLogSummaryChannelId"

    def _sourceChannelId(self) -> int:
        try:
            channelId = int(getattr(self.config, "trainingResultsChannelId", 0) or 0)
        except (TypeError, ValueError):
            channelId = 0
        return channelId if channelId > 0 else 0

    def _archiveChannelId(self) -> int:
        try:
            channelId = int(
                getattr(self.config, "trainingArchiveChannelId", 0)
                or getattr(self.config, "johnTrainingLogChannelId", 0)
                or 0
            )
        except (TypeError, ValueError):
            channelId = 0
        return channelId if channelId > 0 else 0

    def _backfillDays(self) -> int:
        try:
            days = int(getattr(self.config, "trainingLogBackfillDays", 365) or 365)
        except (TypeError, ValueError):
            days = 365
        return max(7, min(days, 365))

    def _summaryWebhookName(self) -> str:
        configured = str(getattr(self.config, "trainingSummaryWebhookName", "") or "").strip()
        return configured or "Jane Training Summary"

    async def _getChannel(self, channelId: int) -> discord.TextChannel | discord.Thread | None:
        if channelId <= 0:
            return None
        channel = self.botClient.get_channel(int(channelId))
        if channel is None:
            try:
                channel = await self.taskBudgeter.runDiscord(lambda: self.botClient.fetch_channel(int(channelId)))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                channel = None
        if isinstance(channel, (discord.TextChannel, discord.Thread)):
            return channel
        return None

    def _extractHost(self, hostLine: str, guild: Optional[discord.Guild]) -> tuple[int, str]:
        mentionMatch = _hostMentionRegex.search(hostLine)
        if mentionMatch:
            return int(mentionMatch.group(1)), _normalizeWhitespace(hostLine.split(":", 1)[-1])

        hostText = _normalizeWhitespace(hostLine.split(":", 1)[-1])
        lookupTarget = _normalizeNameLookup(hostText)
        if guild is not None and lookupTarget:
            matches = [
                member
                for member in guild.members
                if _normalizeNameLookup(member.display_name) == lookupTarget or _normalizeNameLookup(member.name) == lookupTarget
            ]
            if len(matches) == 1:
                return int(matches[0].id), hostText
        return 0, hostText

    def _countSectionEntries(self, lines: list[str], headerPrefix: str) -> int:
        inSection = False
        count = 0
        normalizedHeader = headerPrefix.strip().lower()
        for rawLine in lines:
            line = str(rawLine or "").strip()
            if not inSection:
                if line.lower().startswith(normalizedHeader):
                    inSection = True
                continue
            if not line:
                continue

            lowerLine = line.lower()
            if lowerLine.startswith("**") and lowerLine.endswith(":**"):
                break
            if lowerLine.startswith("host:") or lowerLine.startswith("co-host") or lowerLine.startswith("other cohosts"):
                break
            if lowerLine.startswith("each recipient"):
                break
            if lowerLine.startswith("common mistakes") or lowerLine.startswith("please do not") or lowerLine.startswith("don't be discouraged"):
                break
            if lowerLine.startswith("totally emergency exam") or lowerLine.startswith("supervisor cert examination"):
                break
            if lowerLine.startswith("passed") or lowerLine.startswith("failed"):
                break
            if lowerLine == "none" or lowerLine.startswith("none!"):
                break

            count += 1
        return count

    def parseSourceMessage(self, message: discord.Message) -> ParsedTrainingResult | None:
        content = str(message.content or "").strip()
        if not content:
            return None
        lines = [str(line or "").rstrip() for line in content.splitlines()]
        if not lines:
            return None
        firstLine = str(next((line.strip() for line in lines if line.strip()), "")).strip()
        if not firstLine:
            return None

        hostLine = next((line for line in lines if str(line).strip().lower().startswith("host:")), "")
        hostId, hostText = self._extractHost(hostLine, message.guild if isinstance(message.guild, discord.Guild) else None)
        passCount = self._countSectionEntries(lines, "**Certified Recipients (Pass):**")
        if passCount <= 0:
            passCount = self._countSectionEntries(lines, "Certified Recipients (Pass):")
        failCount = self._countSectionEntries(lines, "**Failed Attendees:**")
        if failCount <= 0:
            failCount = self._countSectionEntries(lines, "Failed Attendees:")

        if firstLine == "### Orientation Results":
            return ParsedTrainingResult(
                eventKind="ORIENTATION",
                certType="ORIENTATION",
                certVariant="GENERAL",
                title="Orientation Results",
                hostId=hostId,
                hostText=hostText,
                passCount=passCount,
                failCount=failCount,
            )

        titleMatch = _certificationTitleRegex.match(firstLine)
        if titleMatch is None:
            return None

        certType = str(titleMatch.group(1) or "").strip().upper()
        variantRaw = str(titleMatch.group(2) or "").strip().upper()
        if variantRaw == "TRAINING":
            certVariant = "TRAINING"
        elif variantRaw == "EXAMINATION":
            certVariant = "EXAM"
        else:
            certVariant = "GENERAL"

        return ParsedTrainingResult(
            eventKind="CERTIFICATION",
            certType=certType,
            certVariant=certVariant,
            title=firstLine,
            hostId=hostId,
            hostText=hostText,
            passCount=passCount,
            failCount=failCount,
        )

    async def _getStoredLog(self, messageId: int) -> dict[str, Any] | None:
        return await fetchOne("SELECT * FROM training_result_logs WHERE messageId = ?", (int(messageId),))

    def _storedRowDiffers(self, storedRow: dict[str, Any] | None, message: discord.Message, parsed: ParsedTrainingResult) -> bool:
        if not isinstance(storedRow, dict):
            return True
        comparisons: list[tuple[object, object]] = [
            (storedRow.get("sourceGuildId") or 0, int(getattr(getattr(message, "guild", None), "id", 0) or 0)),
            (storedRow.get("sourceChannelId") or 0, int(getattr(getattr(message, "channel", None), "id", 0) or 0)),
            (storedRow.get("sourceAuthorId") or 0, int(getattr(message.author, "id", 0) or 0)),
            (str(storedRow.get("sourceCreatedAt") or "").strip(), message.created_at.astimezone(timezone.utc).isoformat()),
            (str(storedRow.get("eventKind") or "").strip(), str(parsed.eventKind or "").strip()),
            (str(storedRow.get("certType") or "").strip(), str(parsed.certType or "").strip()),
            (str(storedRow.get("certVariant") or "").strip(), str(parsed.certVariant or "").strip()),
            (str(storedRow.get("title") or "").strip(), str(parsed.title or "").strip()),
            (int(storedRow.get("hostId") or 0), int(parsed.hostId or 0)),
            (str(storedRow.get("hostText") or "").strip(), str(parsed.hostText or "").strip()),
            (int(storedRow.get("passCount") or 0), int(parsed.passCount or 0)),
            (int(storedRow.get("failCount") or 0), int(parsed.failCount or 0)),
            (str(storedRow.get("rawContent") or ""), str(message.content or "")),
        ]
        return any(left != right for left, right in comparisons)

    async def _upsertParsedLog(self, message: discord.Message, parsed: ParsedTrainingResult) -> None:
        sourceGuildId = int(getattr(getattr(message, "guild", None), "id", 0) or 0)
        sourceChannelId = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
        await execute(
            """
            INSERT INTO training_result_logs (
                messageId,
                sourceGuildId,
                sourceChannelId,
                sourceAuthorId,
                sourceCreatedAt,
                eventKind,
                certType,
                certVariant,
                title,
                hostId,
                hostText,
                passCount,
                failCount,
                rawContent
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(messageId) DO UPDATE SET
                sourceGuildId = excluded.sourceGuildId,
                sourceChannelId = excluded.sourceChannelId,
                sourceAuthorId = excluded.sourceAuthorId,
                sourceCreatedAt = excluded.sourceCreatedAt,
                eventKind = excluded.eventKind,
                certType = excluded.certType,
                certVariant = excluded.certVariant,
                title = excluded.title,
                hostId = excluded.hostId,
                hostText = excluded.hostText,
                passCount = excluded.passCount,
                failCount = excluded.failCount,
                rawContent = excluded.rawContent
            """,
            (
                int(message.id),
                sourceGuildId,
                sourceChannelId,
                int(message.author.id),
                message.created_at.astimezone(timezone.utc).isoformat(),
                str(parsed.eventKind or "").strip(),
                str(parsed.certType or "").strip(),
                str(parsed.certVariant or "").strip(),
                str(parsed.title or "").strip(),
                int(parsed.hostId or 0),
                str(parsed.hostText or "").strip(),
                int(parsed.passCount or 0),
                int(parsed.failCount or 0),
                str(message.content or ""),
            ),
        )

    async def _setMirrorMessage(self, messageId: int, channelId: int, mirrorMessageId: int) -> None:
        await execute(
            "UPDATE training_result_logs SET mirrorChannelId = ?, mirrorMessageId = ? WHERE messageId = ?",
            (int(channelId), int(mirrorMessageId), int(messageId)),
        )

    def _messageLock(self, messageId: int) -> asyncio.Lock:
        normalizedMessageId = int(messageId or 0)
        lock = self._messageLocks.get(normalizedMessageId)
        if lock is None:
            lock = asyncio.Lock()
            self._messageLocks[normalizedMessageId] = lock
        return lock

    def _buildMirrorContent(self, message: discord.Message) -> str:
        sourceSuffix = f"\n\nSource: {message.jump_url}"
        content = str(message.content or "").strip()
        if len(content) + len(sourceSuffix) <= 1950:
            return f"{content}{sourceSuffix}"
        truncated = content[: max(0, 1950 - len(sourceSuffix) - 3)].rstrip()
        return f"{truncated}...\n\nSource: {message.jump_url}"

    async def _fetchMirrorMessage(self, storedRow: dict[str, Any]) -> tuple[discord.TextChannel | discord.Thread | None, discord.Message | None]:
        mirrorChannelId = int((storedRow or {}).get("mirrorChannelId") or 0)
        mirrorMessageId = int((storedRow or {}).get("mirrorMessageId") or 0)
        if mirrorChannelId <= 0 or mirrorMessageId <= 0:
            return None, None
        mirrorChannel = await self._getChannel(mirrorChannelId)
        if mirrorChannel is None:
            return None, None
        try:
            mirrorMessage = await self.taskBudgeter.runDiscord(lambda: mirrorChannel.fetch_message(mirrorMessageId))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return mirrorChannel, None
        return mirrorChannel, mirrorMessage

    async def _ensureMirrorMessage(self, message: discord.Message, storedRow: dict[str, Any]) -> bool:
        archiveChannel = await self._getChannel(self._archiveChannelId())
        if archiveChannel is None:
            return False
        desiredContent = self._buildMirrorContent(message)
        _, existingMirror = await self._fetchMirrorMessage(storedRow)
        if existingMirror is not None:
            if str(existingMirror.content or "") == desiredContent:
                return False
            try:
                await self.taskBudgeter.runDiscord(
                    lambda: existingMirror.edit(
                        content=desiredContent,
                        allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False),
                    )
                )
                return True
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        try:
            sentMessage = await self.taskBudgeter.runDiscord(
                lambda: archiveChannel.send(
                    desiredContent,
                    allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False),
                )
            )
        except (discord.Forbidden, discord.HTTPException):
            return False

        await self._setMirrorMessage(int(message.id), int(archiveChannel.id), int(sentMessage.id))
        return True

    def _isRelevantSourceMessage(self, message: discord.Message) -> bool:
        if int(getattr(message.channel, "id", 0) or 0) != self._sourceChannelId():
            return False
        allowedAuthorIds: set[int] = set()
        try:
            johnBotId = int(getattr(self.config, "johnClankerBotId", 0) or 0)
        except (TypeError, ValueError):
            johnBotId = 0
        if johnBotId > 0:
            allowedAuthorIds.add(johnBotId)
        botUser = getattr(self.botClient, "user", None)
        if botUser is not None:
            allowedAuthorIds.add(int(botUser.id))
        return int(message.author.id) in allowedAuthorIds

    async def _captureRelevantMessage(self, message: discord.Message, *, refreshSummary: bool) -> bool:
        if not self._isRelevantSourceMessage(message):
            return False
        messageLock = self._messageLock(int(message.id))
        async with messageLock:
            parsed = self.parseSourceMessage(message)
            if parsed is None:
                return False

            previousRow = await self._getStoredLog(int(message.id))
            rowChanged = self._storedRowDiffers(previousRow, message, parsed)
            await self._upsertParsedLog(message, parsed)
            storedRow = await self._getStoredLog(int(message.id))
            if storedRow is None:
                return False

            mirrored = await self._ensureMirrorMessage(message, storedRow)
            if refreshSummary and (rowChanged or mirrored):
                await self.refreshSummaryPanel()
            return True

    async def handleSourceMessage(self, message: discord.Message) -> bool:
        return await self._captureRelevantMessage(message, refreshSummary=True)

    async def _fetchStoredRows(self, *, hostId: int | None = None) -> list[dict[str, Any]]:
        if hostId is not None and int(hostId or 0) > 0:
            return await fetchAll(
                "SELECT * FROM training_result_logs WHERE hostId = ? ORDER BY datetime(sourceCreatedAt) DESC",
                (int(hostId),),
            )
        return await fetchAll(
            "SELECT * FROM training_result_logs ORDER BY datetime(sourceCreatedAt) DESC",
        )

    def _statsKeyForRow(self, row: dict[str, Any]) -> str:
        certType = str(row.get("certType") or "").strip().upper()
        certVariant = str(row.get("certVariant") or "").strip().upper()
        if certType == "ORIENTATION":
            return "ORIENTATION"
        if certType == "GRID" and certVariant == "TRAINING":
            return "GRID_TRAINING"
        if certType == "GRID":
            return "GRID_EXAM"
        if certType == "EMERGENCY" and certVariant == "TRAINING":
            return "EMERGENCY_TRAINING"
        if certType == "EMERGENCY":
            return "EMERGENCY_EXAM"
        return certType

    def _labelForStatsKey(self, statsKey: str) -> str:
        mapping = {
            "ORIENTATION": "Orientation",
            "GRID_TRAINING": "Grid Training",
            "GRID_EXAM": "Grid Exam",
            "EMERGENCY_TRAINING": "Emergency Training",
            "EMERGENCY_EXAM": "Emergency Exam",
            "TURBINE": "Turbine",
            "SOLO": "Solo",
            "SUPERVISOR": "Supervisor",
        }
        return mapping.get(statsKey, statsKey.replace("_", " ").title())

    def _weeklyCountEligible(self, row: dict[str, Any], certType: str) -> bool:
        if str(row.get("eventKind") or "").strip().upper() != "CERTIFICATION":
            return False
        rowType = str(row.get("certType") or "").strip().upper()
        if rowType != certType:
            return False
        rowVariant = str(row.get("certVariant") or "").strip().upper()
        if rowType in {"GRID", "EMERGENCY"}:
            return rowVariant == "TRAINING"
        return True

    def _passRateEligible(self, row: dict[str, Any], certType: str) -> bool:
        if str(row.get("eventKind") or "").strip().upper() != "CERTIFICATION":
            return False
        rowType = str(row.get("certType") or "").strip().upper()
        if rowType != certType:
            return False
        rowVariant = str(row.get("certVariant") or "").strip().upper()
        if rowType in {"GRID", "EMERGENCY"}:
            return rowVariant == "EXAM"
        return rowVariant in {"GENERAL", "EXAM"}

    async def refreshSummaryPanel(self) -> None:
        archiveChannel = await self._getChannel(self._archiveChannelId())
        if archiveChannel is None:
            return

        async with self._summaryLock:
            rows = await self._fetchStoredRows()
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(days=7)

            embed = discord.Embed(
                title="Training Log Summary",
                description=(
                    "Hosted counts use the last 7 days. "
                    "Grid/Emergency counts use trainings, and Grid/Emergency pass rates use exams."
                ),
                color=discord.Color.blurple(),
                timestamp=now,
            )
            for certType, label in _weeklySummaryTypeOrder:
                weeklyRows = [
                    row
                    for row in rows
                    if self._weeklyCountEligible(row, certType)
                    and _parseIsoOrNow(row.get("sourceCreatedAt")) >= cutoff
                ]
                passRateRows = [row for row in rows if self._passRateEligible(row, certType)]
                passed = sum(int(row.get("passCount") or 0) for row in passRateRows)
                failed = sum(int(row.get("failCount") or 0) for row in passRateRows)
                embed.add_field(
                    name=label,
                    value=(
                        f"Hosted last 7d: `{len(weeklyRows)}`\n"
                        f"Tracked avg pass rate: `{_formatPercent(passed, passed + failed)}`"
                    ),
                    inline=True,
                )

            orientationRows = [row for row in rows if str(row.get("certType") or "").strip().upper() == "ORIENTATION"]
            orientationWeekly = [
                row for row in orientationRows if _parseIsoOrNow(row.get("sourceCreatedAt")) >= cutoff
            ]
            if orientationRows:
                embed.add_field(
                    name="Orientations",
                    value=(
                        f"Hosted last 7d: `{len(orientationWeekly)}`\n"
                        f"Tracked total: `{len(orientationRows)}`"
                    ),
                    inline=True,
                )
            embed.set_footer(text=f"Tracked logs: {len(rows)}")

            oldMessageId = 0
            try:
                oldMessageId = int((await self.recruitmentService.getSetting(self._summarySettingKey)) or 0)
            except Exception:
                oldMessageId = 0
            oldChannelId = 0
            try:
                oldChannelId = int((await self.recruitmentService.getSetting(self._summaryChannelSettingKey)) or 0)
            except Exception:
                oldChannelId = 0

            if oldMessageId > 0 and oldChannelId > 0:
                oldChannel = await self._getChannel(oldChannelId)
                if oldChannel is None:
                    oldMessage = None
                else:
                    try:
                        oldMessage = await self.taskBudgeter.runDiscord(lambda: oldChannel.fetch_message(oldMessageId))
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        oldMessage = None
            else:
                oldMessage = None

            sentMessage = await self.webhooks.sendOwnedWebhookMessageDetailed(
                botClient=self.botClient,
                channel=archiveChannel,
                webhookName=self._summaryWebhookName(),
                embed=embed,
                reason="Training summary refresh",
            )
            if sentMessage is None:
                return
            if oldMessage is not None and int(oldMessage.id or 0) != int(sentMessage.id or 0):
                try:
                    await self.taskBudgeter.runDiscord(lambda: oldMessage.delete())
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass
            try:
                await self.recruitmentService.setSetting(self._summarySettingKey, str(int(sentMessage.id)))
                await self.recruitmentService.setSetting(self._summaryChannelSettingKey, str(int(archiveChannel.id)))
            except Exception:
                log.exception("Failed to persist training summary panel state.")

    async def syncRecentMessages(self) -> None:
        now = datetime.now(timezone.utc)
        if self._lastReadySyncAt is not None:
            if (now - self._lastReadySyncAt).total_seconds() < float(self._readySyncCooldownSec):
                return
        async with self._syncLock:
            now = datetime.now(timezone.utc)
            if self._lastReadySyncAt is not None:
                if (now - self._lastReadySyncAt).total_seconds() < float(self._readySyncCooldownSec):
                    return
            sourceChannel = await self._getChannel(self._sourceChannelId())
            if sourceChannel is None:
                return
            cutoff = now - timedelta(days=self._backfillDays())
            try:
                async for message in sourceChannel.history(limit=None, after=cutoff, oldest_first=True):
                    await self._captureRelevantMessage(message, refreshSummary=False)
            except Exception:
                log.exception("Failed to backfill training-results messages.")
            await self.refreshSummaryPanel()
            self._lastReadySyncAt = datetime.now(timezone.utc)

    async def handleTrainingStats(self, message: discord.Message) -> bool:
        if message.author.bot or not message.content:
            return False
        stripped = str(message.content or "").strip()
        token = str(stripped.split(maxsplit=1)[0] if stripped else "").lower()
        if token not in {"?trainingstats", "?hoststats"}:
            return False
        if not message.guild or not isinstance(message.author, discord.Member):
            return True

        targetUserId = int(message.author.id)
        mentionMatch = _hostMentionRegex.search(str(message.content or ""))
        if mentionMatch:
            targetUserId = int(mentionMatch.group(1))
        elif len(stripped.split(maxsplit=1)) > 1:
            rawTarget = stripped.split(maxsplit=1)[1].strip()
            if rawTarget.isdigit():
                targetUserId = int(rawTarget)

        targetLabels: set[str] = set()
        targetMember = message.guild.get_member(targetUserId)
        if targetMember is None:
            try:
                targetMember = await self.taskBudgeter.runDiscord(lambda: message.guild.fetch_member(targetUserId))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                targetMember = None
        if targetMember is not None:
            for value in [targetMember.display_name, targetMember.name, getattr(targetMember, "global_name", None)]:
                normalized = _normalizeNameLookup(value)
                if normalized:
                    targetLabels.add(normalized)
        if not targetLabels:
            cachedUser = self.botClient.get_user(targetUserId)
            if cachedUser is None:
                try:
                    cachedUser = await self.taskBudgeter.runDiscord(lambda: self.botClient.fetch_user(targetUserId))
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    cachedUser = None
            if cachedUser is not None:
                for value in [cachedUser.name, getattr(cachedUser, "global_name", None)]:
                    normalized = _normalizeNameLookup(value)
                    if normalized:
                        targetLabels.add(normalized)

        allRows = await self._fetchStoredRows()
        rows = [
            row
            for row in allRows
            if int(row.get("hostId") or 0) == int(targetUserId)
            or (
                int(row.get("hostId") or 0) <= 0
                and _normalizeNameLookup(row.get("hostText")) in targetLabels
            )
        ]
        if not rows:
            await message.channel.send(
                f"No tracked training or orientation logs were found for <@{int(targetUserId)}>.",
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
            return True

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=7)
        totalCounts = {key: 0 for key in _defaultStatsOrder}
        weeklyCounts = {key: 0 for key in _defaultStatsOrder}
        latestAt: datetime | None = None
        hostText = ""
        for row in rows:
            statsKey = self._statsKeyForRow(row)
            totalCounts[statsKey] = totalCounts.get(statsKey, 0) + 1
            createdAt = _parseIsoOrNow(row.get("sourceCreatedAt"))
            if createdAt >= cutoff:
                weeklyCounts[statsKey] = weeklyCounts.get(statsKey, 0) + 1
            if latestAt is None or createdAt > latestAt:
                latestAt = createdAt
            if not hostText:
                hostText = str(row.get("hostText") or "").strip()

        embed = discord.Embed(
            title="Training Stats",
            description=f"<@{int(targetUserId)}>" + (f"\nTracked host label: `{hostText}`" if hostText else ""),
            color=discord.Color.blurple(),
            timestamp=now,
        )
        totalLines = [
            f"{self._labelForStatsKey(key)}: `{int(totalCounts.get(key, 0) or 0)}`"
            for key in _defaultStatsOrder
            if int(totalCounts.get(key, 0) or 0) > 0
        ]
        weeklyLines = [
            f"{self._labelForStatsKey(key)}: `{int(weeklyCounts.get(key, 0) or 0)}`"
            for key in _defaultStatsOrder
            if int(weeklyCounts.get(key, 0) or 0) > 0
        ]
        embed.add_field(
            name="Tracked Totals",
            value="\n".join(totalLines) if totalLines else "`0`",
            inline=False,
        )
        embed.add_field(
            name="Last 7 Days",
            value="\n".join(weeklyLines) if weeklyLines else "`0`",
            inline=False,
        )
        if latestAt is not None:
            embed.add_field(name="Most Recent Logged Event", value=discord.utils.format_dt(latestAt, "f"), inline=False)
        await message.channel.send(
            embed=embed,
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        return True
