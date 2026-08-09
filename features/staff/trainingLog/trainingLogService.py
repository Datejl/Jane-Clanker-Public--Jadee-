from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import discord

from db.sqlite import execute, fetchAll, fetchOne
from features.staff.sessions.Roblox import robloxUsers
from features.staff.trainingLog import parsing as trainingLogParsing
from runtime import orgFeatureGate
from runtime import orgProfiles

log = logging.getLogger(__name__)

ParsedTrainingResult = trainingLogParsing.ParsedTrainingResult
_hostMentionRegex = trainingLogParsing.hostMentionRegex
_mirrorSourceFooterRegex = trainingLogParsing.mirrorSourceFooterRegex
_defaultStatsOrder = trainingLogParsing.defaultStatsOrder
_weeklySummaryTypeOrder = trainingLogParsing.weeklySummaryTypeOrder
_trainingMirrorColor = trainingLogParsing.trainingMirrorColor
_summaryEmbedTitle = trainingLogParsing.summaryEmbedTitle
_normalizeWhitespace = trainingLogParsing.normalizeWhitespace
_normalizeNameLookup = trainingLogParsing.normalizeNameLookup
_formatPercent = trainingLogParsing.formatPercent
_parseIsoOrNow = trainingLogParsing.parseIsoOrNow


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
        self._summarySettingBaseKey = "trainingLogSummaryMessageId"
        self._summaryChannelSettingBaseKey = "trainingLogSummaryChannelId"

    def _sourceChannelId(self) -> int:
        return self._sourceChannelIdForOrg(None)

    def _sourceChannelIdForOrg(self, orgKey: str | None) -> int:
        try:
            channelId = int(
                orgProfiles.getOrganizationValue(
                    self.config,
                    "trainingResultsChannelId",
                    orgKey=orgKey,
                    default=0,
                )
                or 0
            )
        except (TypeError, ValueError):
            channelId = 0
        return channelId if channelId > 0 else 0

    def _archiveChannelId(self) -> int:
        return self._archiveChannelIdForOrg(None)

    def _archiveChannelIdForOrg(self, orgKey: str | None) -> int:
        try:
            channelId = int(
                orgProfiles.getOrganizationValue(
                    self.config,
                    "trainingArchiveChannelId",
                    orgKey=orgKey,
                    default=0,
                )
                or 0
            )
        except (TypeError, ValueError):
            channelId = 0
        return channelId if channelId > 0 else 0

    def _backfillDays(self) -> int:
        return self._backfillDaysForOrg(None)

    def _backfillDaysForOrg(self, orgKey: str | None) -> int:
        try:
            days = int(
                orgProfiles.getOrganizationValue(
                    self.config,
                    "trainingLogBackfillDays",
                    orgKey=orgKey,
                    default=365,
                )
                or 365
            )
        except (TypeError, ValueError):
            days = 365
        return max(1, min(days, 365))

    def _historyLimitForOrg(
        self,
        key: str,
        orgKey: str | None,
        *,
        default: int,
        maximum: int,
    ) -> int | None:
        try:
            configured = int(
                orgProfiles.getOrganizationValue(
                    self.config,
                    key,
                    orgKey=orgKey,
                    default=default,
                )
                or default
            )
        except (TypeError, ValueError):
            configured = default
        if configured <= 0:
            return None
        return max(1, min(configured, maximum))

    def _startupBackfillMessageLimitForOrg(self, orgKey: str | None) -> int | None:
        return self._historyLimitForOrg(
            "trainingLogStartupBackfillMessageLimit",
            orgKey,
            default=250,
            maximum=5000,
        )

    def _startupMirrorNewRowsForOrg(self, orgKey: str | None) -> bool:
        configured = orgProfiles.getOrganizationValue(
            self.config,
            "trainingLogStartupMirrorNewRows",
            orgKey=orgKey,
            default=True,
        )
        if isinstance(configured, bool):
            return configured
        text = str(configured or "").strip().lower()
        if text in {"0", "false", "no", "off", "disabled"}:
            return False
        if text in {"1", "true", "yes", "on", "enabled"}:
            return True
        return True

    def _archiveIndexLimitForOrg(self, orgKey: str | None) -> int | None:
        return self._historyLimitForOrg(
            "trainingLogArchiveIndexLimit",
            orgKey,
            default=500,
            maximum=10000,
        )

    def _summaryWebhookName(self) -> str:
        return self._summaryWebhookNameForOrg(None)

    def _summaryWebhookNameForOrg(self, orgKey: str | None) -> str:
        configured = str(
            orgProfiles.getOrganizationValue(
                self.config,
                "trainingSummaryWebhookName",
                orgKey=orgKey,
                default="",
            )
            or ""
        ).strip()
        return configured or "Jane Training Summary"

    def _mirrorWebhookName(self) -> str:
        return self._mirrorWebhookNameForOrg(None)

    def _mirrorWebhookNameForOrg(self, orgKey: str | None) -> str:
        configured = str(
            orgProfiles.getOrganizationValue(
                self.config,
                "trainingMirrorWebhookName",
                orgKey=orgKey,
                default="",
            )
            or ""
        ).strip()
        return configured or "Jane Training Log"

    def _trainingProfiles(self) -> list[orgProfiles.OrganizationProfile]:
        profiles = [
            profile
            for profile in orgProfiles.getOrganizationProfiles(self.config).values()
            if self._sourceChannelIdForOrg(profile.key) > 0
            and orgFeatureGate.isFeatureEnabledForGuild(
                self.config,
                int(profile.primaryGuildId or 0),
                "anro-training-logs",
            )
        ]
        if profiles:
            return profiles
        fallbackProfile = orgProfiles.getOrganizationProfile(self.config)
        if (
            fallbackProfile is not None
            and self._sourceChannelIdForOrg(fallbackProfile.key) > 0
            and orgFeatureGate.isFeatureEnabledForGuild(
                self.config,
                int(fallbackProfile.primaryGuildId or 0),
                "anro-training-logs",
            )
        ):
            return [fallbackProfile]
        return []

    def _trainingProfileForSourceChannel(self, channelId: int) -> orgProfiles.OrganizationProfile | None:
        normalizedChannelId = int(channelId or 0)
        if normalizedChannelId <= 0:
            return None
        for profile in self._trainingProfiles():
            if self._sourceChannelIdForOrg(profile.key) == normalizedChannelId:
                return profile
        return None

    def _summarySettingKey(self, orgKey: str | None) -> str:
        suffix = str(orgKey or "").strip().upper()
        if suffix:
            return f"{self._summarySettingBaseKey}:{suffix}"
        return self._summarySettingBaseKey

    def _summaryChannelSettingKey(self, orgKey: str | None) -> str:
        suffix = str(orgKey or "").strip().upper()
        if suffix:
            return f"{self._summaryChannelSettingBaseKey}:{suffix}"
        return self._summaryChannelSettingBaseKey

    def _rowBelongsToOrg(self, row: dict[str, Any], orgKey: str) -> bool:
        profile = orgProfiles.getOrganizationProfile(self.config, orgKey=orgKey)
        if profile is None:
            return False
        rowSourceChannelId = int(row.get("sourceChannelId") or 0)
        if rowSourceChannelId > 0 and rowSourceChannelId == self._sourceChannelIdForOrg(orgKey):
            return True
        rowSourceGuildId = int(row.get("sourceGuildId") or 0)
        return rowSourceGuildId > 0 and rowSourceGuildId in set(profile.guildIds)

    def _orgSqlFilters(self, orgKey: str | None, *, tableAlias: str = "") -> tuple[list[str], list[Any]]:
        requestedOrg = str(orgKey or "").strip()
        if not requestedOrg:
            return [], []

        profile = orgProfiles.getOrganizationProfile(self.config, orgKey=requestedOrg)
        if profile is None:
            return ["1 = 0"], []

        prefix = f"{tableAlias}." if tableAlias else ""
        filters: list[str] = []
        params: list[Any] = []

        sourceChannelId = self._sourceChannelIdForOrg(profile.key)
        if sourceChannelId > 0:
            filters.append(f"{prefix}sourceChannelId = ?")
            params.append(int(sourceChannelId))

        guildIds: list[int] = []
        for value in list(getattr(profile, "guildIds", []) or []):
            try:
                guildId = int(value or 0)
            except (TypeError, ValueError):
                continue
            if guildId > 0 and guildId not in guildIds:
                guildIds.append(guildId)
        if guildIds:
            placeholders = ",".join("?" for _ in guildIds)
            filters.append(f"{prefix}sourceGuildId IN ({placeholders})")
            params.extend(guildIds)

        if not filters:
            return ["1 = 0"], []
        return [f"({' OR '.join(filters)})"], params

    @staticmethod
    def _whereClause(filters: list[str]) -> str:
        if not filters:
            return ""
        return " WHERE " + " AND ".join(f"({entry})" for entry in filters)

    def _authorLooksLikeJohn(self, message: discord.Message) -> bool:
        author = getattr(message, "author", None)
        for value in [
            getattr(author, "display_name", None),
            getattr(author, "global_name", None),
            getattr(author, "name", None),
        ]:
            normalized = _normalizeNameLookup(value)
            if normalized == "john clanker":
                return True
        return False

    def _authorLooksLikeJane(self, message: discord.Message) -> bool:
        author = getattr(message, "author", None)
        for value in [
            getattr(author, "display_name", None),
            getattr(author, "global_name", None),
            getattr(author, "name", None),
        ]:
            normalized = _normalizeNameLookup(value)
            if normalized == "jane clanker":
                return True
        return False

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
            (str(storedRow.get("rawContent") or ""), trainingLogParsing.extractMessageText(message)),
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
                trainingLogParsing.extractMessageText(message),
            ),
        )

    async def _setMirrorMessage(self, messageId: int, channelId: int, mirrorMessageId: int) -> None:
        await execute(
            "UPDATE training_result_logs SET mirrorChannelId = ?, mirrorMessageId = ? WHERE messageId = ?",
            (int(channelId), int(mirrorMessageId), int(messageId)),
        )

    async def _getExportState(self, messageId: int) -> dict[str, Any] | None:
        return await fetchOne(
            "SELECT * FROM training_result_export_state WHERE messageId = ?",
            (int(messageId),),
        )

    def _exportStateNeedsRefresh(self, exportState: dict[str, Any] | None) -> bool:
        if not isinstance(exportState, dict):
            return True
        status = str(exportState.get("hostResolveStatus") or "").strip().upper()
        username = str(exportState.get("hostRobloxUsername") or "").strip()
        return status != "RESOLVED" or not username

    def _sourceMessageUrl(self, row: dict[str, Any]) -> str:
        guildId = int(row.get("sourceGuildId") or 0)
        channelId = int(row.get("sourceChannelId") or 0)
        messageId = int(row.get("messageId") or 0)
        if guildId <= 0 or channelId <= 0 or messageId <= 0:
            return ""
        return f"https://discord.com/channels/{guildId}/{channelId}/{messageId}"

    def _displayEventTypeParts(
        self,
        *,
        eventKind: str,
        certType: str,
        certVariant: str,
        title: str = "",
    ) -> str:
        normalizedEventKind = str(eventKind or "").strip().upper()
        normalizedCertType = str(certType or "").strip().upper()
        normalizedVariant = str(certVariant or "").strip().upper()

        if normalizedEventKind == "ORIENTATION" or normalizedCertType == "ORIENTATION":
            return "Orientation"
        if normalizedCertType == "GRID" and normalizedVariant == "TRAINING":
            return "Grid Training"
        if normalizedCertType == "GRID":
            return "Grid Exam"
        if normalizedCertType == "EMERGENCY" and normalizedVariant == "TRAINING":
            return "Emergency Training"
        if normalizedCertType == "EMERGENCY":
            return "Emergency Exam"
        if normalizedCertType == "TURBINE":
            return "Turbine"
        if normalizedCertType == "SOLO":
            return "Solo"
        if normalizedCertType == "SUPERVISOR":
            return "Supervisor"
        return str(title or "Unknown")

    async def _resolveHostRobloxIdentity(
        self,
        message: discord.Message,
        parsed: ParsedTrainingResult,
    ) -> tuple[int | None, str, str, str]:
        hostId = int(getattr(parsed, "hostId", 0) or 0)
        if hostId <= 0:
            return None, "", "NO_HOST", "No host Discord mention was parsed."

        guildId = int(getattr(getattr(message, "guild", None), "id", 0) or 0)
        try:
            lookup = await robloxUsers.fetchRobloxUser(hostId, guildId if guildId > 0 else None)
        except Exception as exc:
            return None, "", "ERROR", f"{exc.__class__.__name__}: {exc}"

        username = str(getattr(lookup, "robloxUsername", "") or "").strip()
        try:
            robloxUserId = int(getattr(lookup, "robloxId", 0) or 0) or None
        except (TypeError, ValueError):
            robloxUserId = None
        if username:
            return robloxUserId, username, "RESOLVED", ""
        error = str(getattr(lookup, "error", "") or "No Roblox username resolved.").strip()
        return robloxUserId, "", "UNRESOLVED", error

    async def _prepareExportState(
        self,
        message: discord.Message,
        parsed: ParsedTrainingResult,
    ) -> None:
        robloxUserId, robloxUsername, resolveStatus, resolveError = await self._resolveHostRobloxIdentity(message, parsed)
        exportStatus = "READY" if str(robloxUsername or "").strip() else "HOST_UNRESOLVED"
        await execute(
            """
            INSERT INTO training_result_export_state (
                messageId,
                hostRobloxUserId,
                hostRobloxUsername,
                hostResolveStatus,
                hostResolveError,
                exportStatus,
                preparedAt,
                updatedAt
            )
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(messageId) DO UPDATE SET
                hostRobloxUserId = excluded.hostRobloxUserId,
                hostRobloxUsername = excluded.hostRobloxUsername,
                hostResolveStatus = excluded.hostResolveStatus,
                hostResolveError = excluded.hostResolveError,
                exportStatus = excluded.exportStatus,
                exportError = '',
                preparedAt = excluded.preparedAt,
                updatedAt = datetime('now')
            """,
            (
                int(message.id),
                robloxUserId,
                str(robloxUsername or "").strip(),
                str(resolveStatus or "").strip(),
                str(resolveError or "").strip()[:500],
                exportStatus,
            ),
        )

    async def _persistSummaryMessage(
        self,
        *,
        orgKey: str,
        channelId: int,
        messageId: int,
    ) -> None:
        await self.recruitmentService.setSetting(self._summarySettingKey(orgKey), str(int(messageId)))
        await self.recruitmentService.setSetting(self._summaryChannelSettingKey(orgKey), str(int(channelId)))

    async def _getStoredSummaryMessage(
        self,
        *,
        orgKey: str,
    ) -> tuple[discord.TextChannel | discord.Thread | None, discord.Message | None]:
        try:
            oldMessageId = int((await self.recruitmentService.getSetting(self._summarySettingKey(orgKey))) or 0)
        except Exception:
            oldMessageId = 0
        try:
            oldChannelId = int((await self.recruitmentService.getSetting(self._summaryChannelSettingKey(orgKey))) or 0)
        except Exception:
            oldChannelId = 0
        if oldMessageId <= 0 or oldChannelId <= 0:
            return None, None

        oldChannel = await self._getChannel(oldChannelId)
        if oldChannel is None:
            return None, None
        try:
            oldMessage = await self.taskBudgeter.runDiscord(lambda: oldChannel.fetch_message(oldMessageId))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            oldMessage = None
        return oldChannel, oldMessage

    def _messageLooksLikeSummaryPanel(self, message: discord.Message, *, orgKey: str) -> bool:
        hasSummaryEmbed = any(
            _normalizeWhitespace(getattr(embed, "title", "")).casefold() == _summaryEmbedTitle.casefold()
            for embed in list(getattr(message, "embeds", []) or [])
        )
        if not hasSummaryEmbed:
            return False

        webhookName = _normalizeWhitespace(self._summaryWebhookNameForOrg(orgKey)).casefold()
        author = getattr(message, "author", None)
        authorNames = [
            getattr(author, "display_name", None),
            getattr(author, "global_name", None),
            getattr(author, "name", None),
        ]
        if webhookName and any(_normalizeWhitespace(value).casefold() == webhookName for value in authorNames):
            return True

        botUserId = int(getattr(getattr(self.botClient, "user", None), "id", 0) or 0)
        authorId = int(getattr(author, "id", 0) or 0)
        if botUserId > 0 and authorId == botUserId:
            return True

        # The embed title is Jane's stable marker; the name checks above are just extra guardrails.
        return True

    async def _fetchLatestArchiveMessage(
        self,
        archiveChannel: discord.TextChannel | discord.Thread,
    ) -> discord.Message | None:
        try:
            async for message in archiveChannel.history(limit=1):
                return message
        except (discord.Forbidden, discord.HTTPException):
            return None
        return None

    async def _findLatestSummaryPanelMessage(
        self,
        archiveChannel: discord.TextChannel | discord.Thread,
        *,
        orgKey: str,
    ) -> discord.Message | None:
        try:
            async for message in archiveChannel.history(limit=250):
                if self._messageLooksLikeSummaryPanel(message, orgKey=orgKey):
                    return message
        except (discord.Forbidden, discord.HTTPException):
            return None
        return None

    async def _deleteSummaryMessage(
        self,
        *,
        message: discord.Message,
        orgKey: str,
    ) -> None:
        deleted = False
        try:
            deleted = await self.webhooks.deleteOwnedWebhookMessage(
                botClient=self.botClient,
                message=message,
                webhookName=self._summaryWebhookNameForOrg(orgKey),
                reason="Training summary replacement",
            )
        except AttributeError:
            deleted = False
        if deleted:
            return
        try:
            await self.taskBudgeter.runDiscord(lambda: message.delete())
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    def _messageLock(self, messageId: int) -> asyncio.Lock:
        normalizedMessageId = int(messageId or 0)
        lock = self._messageLocks.get(normalizedMessageId)
        if lock is None:
            lock = asyncio.Lock()
            self._messageLocks[normalizedMessageId] = lock
        return lock

    def _buildMirrorEmbeds(self, parsed: ParsedTrainingResult, message: discord.Message) -> list[discord.Embed]:
        totalAttendees = int(parsed.passCount or 0) + int(parsed.failCount or 0)
        passRateText = _formatPercent(int(parsed.passCount or 0), totalAttendees)
        embed = discord.Embed(
            title="Training Result",
            color=_trainingMirrorColor,
            timestamp=getattr(message, "created_at", datetime.now(timezone.utc)),
        )
        embed.add_field(name="Event Type", value=self._displayEventType(parsed), inline=False)
        embed.add_field(name="Host", value=self._displayHost(parsed), inline=False)
        embed.add_field(
            name="This Session Pass Rate",
            value=f"`{passRateText}` ({int(parsed.passCount or 0)}/{max(0, totalAttendees)})",
            inline=False,
        )
        passChunks = self.webhooks.buildEmbedFieldChunks(
            [str(value or "") for value in list(parsed.passAttendees or [])],
            emptyText="None",
            overflowNoun="pass attendee(s)",
            maxChunks=3,
        )
        failChunks = self.webhooks.buildEmbedFieldChunks(
            [str(value or "") for value in list(parsed.failAttendees or [])],
            emptyText="None",
            overflowNoun="failed attendee(s)",
            maxChunks=3,
        )
        embed.add_field(
            name=f"Passed ({int(parsed.passCount or 0)})",
            value=passChunks[0],
            inline=False,
        )
        embed.add_field(
            name=f"Failed ({int(parsed.failCount or 0)})",
            value=failChunks[0],
            inline=False,
        )
        embed.set_footer(text=f"Source message ID: {int(getattr(message, 'id', 0) or 0)}")

        extraEmbeds: list[discord.Embed] = []
        remainingChunks = passChunks[1:] + failChunks[1:]
        if remainingChunks:
            overflowEmbed = discord.Embed(
                title="Training Result Attendee Overflow",
                color=_trainingMirrorColor,
                timestamp=getattr(message, "created_at", datetime.now(timezone.utc)),
            )
            passOverflowChunks = passChunks[1:]
            failOverflowChunks = failChunks[1:]
            for idx, chunk in enumerate(passOverflowChunks, start=2):
                overflowEmbed.add_field(name=f"Passed ({idx})", value=chunk, inline=False)
            for idx, chunk in enumerate(failOverflowChunks, start=2):
                overflowEmbed.add_field(name=f"Failed ({idx})", value=chunk, inline=False)
            extraEmbeds.append(overflowEmbed)
        return [embed, *extraEmbeds][:10]

    def _mirrorWebhookUsername(self, message: discord.Message) -> str:
        author = getattr(message, "author", None)
        for value in [
            getattr(author, "display_name", None),
            getattr(author, "global_name", None),
            getattr(author, "name", None),
        ]:
            normalized = _normalizeWhitespace(value)
            if normalized:
                return normalized[:80]
        return "Jane Training Log"

    def _mirrorWebhookAvatarUrl(self, message: discord.Message) -> str | None:
        author = getattr(message, "author", None)
        avatar = getattr(author, "display_avatar", None)
        url = str(getattr(avatar, "url", "") or "").strip()
        return url or None

    def _displayHost(self, parsed: ParsedTrainingResult | None) -> str:
        if parsed is None:
            return "Unknown"
        hostId = int(getattr(parsed, "hostId", 0) or 0)
        hostText = str(getattr(parsed, "hostText", "") or "").strip()
        if hostId > 0:
            if hostText:
                return f"<@{hostId}> ({hostText})"
            return f"<@{hostId}>"
        return hostText or "Unknown"

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

    async def _recoverMirrorMessageBySourceId(
        self,
        archiveChannel: discord.TextChannel | discord.Thread,
        sourceMessageId: int,
    ) -> discord.Message | None:
        targetFooter = f"Source message ID: {int(sourceMessageId or 0)}"
        try:
            async for candidate in archiveChannel.history(limit=250):
                for embed in list(getattr(candidate, "embeds", []) or []):
                    footer = getattr(embed, "footer", None)
                    footerText = str(getattr(footer, "text", "") or "").strip()
                    if footerText == targetFooter:
                        return candidate
        except (discord.Forbidden, discord.HTTPException):
            return None
        return None

    def _sourceMessageIdFromMirrorMessage(self, message: discord.Message) -> int:
        for embed in list(getattr(message, "embeds", []) or []):
            footer = getattr(embed, "footer", None)
            footerText = str(getattr(footer, "text", "") or "").strip()
            match = _mirrorSourceFooterRegex.match(footerText)
            if match:
                try:
                    return int(match.group(1))
                except (TypeError, ValueError):
                    return 0
        return 0

    async def _buildArchiveMirrorIndex(
        self,
        archiveChannel: discord.TextChannel | discord.Thread,
        *,
        orgKey: str,
        limit: int | None = None,
    ) -> dict[int, int] | None:
        mirroredSourceIds: dict[int, int] = {}
        scannedCount = 0
        try:
            async for message in archiveChannel.history(limit=limit):
                scannedCount += 1
                sourceMessageId = self._sourceMessageIdFromMirrorMessage(message)
                if sourceMessageId > 0 and sourceMessageId not in mirroredSourceIds:
                    mirroredSourceIds[int(sourceMessageId)] = int(message.id)
                if scannedCount % 500 == 0:
                    await asyncio.sleep(0)
        except (discord.Forbidden, discord.HTTPException):
            log.warning(
                "Training mirror archive index failed: org=%s channelId=%s scanned=%s found=%s.",
                str(orgKey or "").strip().upper() or "UNKNOWN",
                int(getattr(archiveChannel, "id", 0) or 0),
                scannedCount,
                len(mirroredSourceIds),
                exc_info=True,
            )
            return None

        log.info(
            "Training mirror archive index built: org=%s channelId=%s scanned=%s found=%s limit=%s.",
            str(orgKey or "").strip().upper() or "UNKNOWN",
            int(getattr(archiveChannel, "id", 0) or 0),
            scannedCount,
            len(mirroredSourceIds),
            str(limit or "none"),
        )
        return mirroredSourceIds

    async def _ensureMirrorMessage(
        self,
        message: discord.Message,
        storedRow: dict[str, Any],
        parsed: ParsedTrainingResult,
        *,
        orgKey: str,
    ) -> bool:
        archiveChannelId = self._archiveChannelIdForOrg(orgKey)
        archiveChannel = await self._getChannel(archiveChannelId)
        if archiveChannel is None:
            log.warning(
                "Training mirror skipped: archive channel %s is unavailable for source message %s.",
                int(archiveChannelId or 0),
                int(getattr(message, "id", 0) or 0),
            )
            return False
        desiredEmbeds = self._buildMirrorEmbeds(parsed, message)
        webhookName = self._mirrorWebhookNameForOrg(orgKey)
        storedMirrorChannelId = int((storedRow or {}).get("mirrorChannelId") or 0)
        storedMirrorMessageId = int((storedRow or {}).get("mirrorMessageId") or 0)
        hadStoredMirror = storedMirrorChannelId > 0 and storedMirrorMessageId > 0
        existingMirror: discord.Message | None = None
        if storedMirrorChannelId > 0 and storedMirrorChannelId == int(archiveChannel.id):
            _, existingMirror = await self._fetchMirrorMessage(storedRow)
        if existingMirror is None:
            recoveredMirror = await self._recoverMirrorMessageBySourceId(archiveChannel, int(getattr(message, "id", 0) or 0))
            if recoveredMirror is not None:
                existingMirror = recoveredMirror
                await self._setMirrorMessage(int(message.id), int(archiveChannel.id), int(recoveredMirror.id))
        if existingMirror is None and hadStoredMirror:
            log.warning(
                "Training mirror replacement skipped for source message %s because a stored mirror already exists but could not be fetched.",
                int(getattr(message, "id", 0) or 0),
            )
            return False
        if existingMirror is not None:
            try:
                edited = await self.webhooks.editOwnedWebhookMessage(
                    botClient=self.botClient,
                    message=existingMirror,
                    webhookName=webhookName,
                    content="",
                    embeds=desiredEmbeds,
                    reason="Training result mirror refresh",
                )
                if edited:
                    return True
            except Exception:
                pass

        sentMessage = await self.webhooks.sendOwnedWebhookMessageDetailed(
            botClient=self.botClient,
            channel=archiveChannel,
            webhookName=webhookName,
            content="",
            embeds=desiredEmbeds,
            reason="Training result mirror",
        )
        if sentMessage is None:
            try:
                sentMessage = await self.taskBudgeter.runDiscord(
                    lambda: archiveChannel.send(
                        embeds=desiredEmbeds,
                        allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False),
                    )
                )
            except (discord.Forbidden, discord.HTTPException):
                log.warning(
                    "Training mirror send failed for source message %s into archive channel %s.",
                    int(getattr(message, "id", 0) or 0),
                    int(getattr(archiveChannel, "id", 0) or 0),
                )
                return False

        await self._setMirrorMessage(int(message.id), int(archiveChannel.id), int(sentMessage.id))
        if existingMirror is not None and int(getattr(existingMirror, "id", 0) or 0) != int(getattr(sentMessage, "id", 0) or 0):
            try:
                await self.taskBudgeter.runDiscord(lambda: existingMirror.delete())
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        return True

    def _displayEventType(self, parsed: ParsedTrainingResult | None) -> str:
        if parsed is None:
            return "Unknown"
        return self._displayEventTypeParts(
            eventKind=str(parsed.eventKind or ""),
            certType=str(parsed.certType or ""),
            certVariant=str(parsed.certVariant or ""),
            title=str(parsed.title or ""),
        )

    def _isRelevantSourceMessage(
        self,
        message: discord.Message,
        parsed: ParsedTrainingResult | None = None,
        *,
        orgKey: str,
    ) -> bool:
        if int(getattr(message.channel, "id", 0) or 0) != self._sourceChannelIdForOrg(orgKey):
            return False
        if parsed is None:
            return False
        authorId = int(getattr(getattr(message, "author", None), "id", 0) or 0)
        botUserId = int(getattr(getattr(self.botClient, "user", None), "id", 0) or 0)
        if str(parsed.eventKind or "").strip().upper() == "ORIENTATION":
            return authorId == botUserId or self._authorLooksLikeJane(message)
        if str(parsed.eventKind or "").strip().upper() == "CERTIFICATION":
            return True
        return False

    def shouldInspectSourceMessage(self, message: discord.Message, *, orgKey: str | None = None) -> bool:
        sourceChannelId = int(getattr(message.channel, "id", 0) or 0)
        profile = (
            orgProfiles.getOrganizationProfile(self.config, orgKey=orgKey)
            if str(orgKey or "").strip()
            else self._trainingProfileForSourceChannel(sourceChannelId)
        )
        if profile is None:
            return False
        resolvedOrgKey = profile.key
        if sourceChannelId != self._sourceChannelIdForOrg(resolvedOrgKey):
            return False
        return orgFeatureGate.isFeatureEnabledForGuild(
            self.config,
            int(profile.primaryGuildId or 0),
            "anro-training-logs",
        )

    async def _captureRelevantMessage(
        self,
        message: discord.Message,
        *,
        refreshSummary: bool,
        orgKey: str | None = None,
        archiveMirrorIndex: dict[int, int] | None = None,
        mirrorNewRows: bool = True,
        mirrorExistingRows: bool = True,
        mirrorWhenArchiveIndexUnavailable: bool = True,
    ) -> bool:
        sourceChannelId = int(getattr(message.channel, "id", 0) or 0)
        profile = (
            orgProfiles.getOrganizationProfile(self.config, orgKey=orgKey)
            if str(orgKey or "").strip()
            else self._trainingProfileForSourceChannel(sourceChannelId)
        )
        if profile is None:
            return False
        resolvedOrgKey = profile.key
        if sourceChannelId != self._sourceChannelIdForOrg(resolvedOrgKey):
            return False
        if not orgFeatureGate.isFeatureEnabledForGuild(
            self.config,
            int(profile.primaryGuildId or 0),
            "anro-training-logs",
        ):
            return False
        messageLock = self._messageLock(int(message.id))
        async with messageLock:
            parsed = trainingLogParsing.parseSourceMessage(message)
            if parsed is None:
                return False
            if not self._isRelevantSourceMessage(message, parsed, orgKey=resolvedOrgKey):
                return False

            sourceMessageId = int(message.id)
            previousRow = await self._getStoredLog(sourceMessageId)
            rowChanged = self._storedRowDiffers(previousRow, message, parsed)
            await self._upsertParsedLog(message, parsed)
            storedRow = await self._getStoredLog(sourceMessageId)
            if storedRow is None:
                return False
            try:
                exportState = await self._getExportState(sourceMessageId)
                if rowChanged or self._exportStateNeedsRefresh(exportState):
                    await self._prepareExportState(message, parsed)
            except Exception:
                log.exception(
                    "Training export preparation failed: org=%s messageId=%s.",
                    resolvedOrgKey,
                    sourceMessageId,
                )

            archivedMirrorMessageId = 0
            if archiveMirrorIndex is not None:
                archivedMirrorMessageId = int(archiveMirrorIndex.get(sourceMessageId) or 0)
            if archivedMirrorMessageId > 0:
                await self._setMirrorMessage(
                    sourceMessageId,
                    self._archiveChannelIdForOrg(resolvedOrgKey),
                    archivedMirrorMessageId,
                )
                if refreshSummary and rowChanged:
                    await self.refreshSummaryPanel(orgKey=resolvedOrgKey)
                return True

            if previousRow is not None and not mirrorExistingRows:
                log.debug(
                    "Training mirror skipped for existing stored source during backfill: org=%s messageId=%s.",
                    resolvedOrgKey,
                    sourceMessageId,
                )
                return True

            if previousRow is None and not mirrorNewRows:
                log.debug(
                    "Training mirror skipped for newly stored source during startup backfill: org=%s messageId=%s.",
                    resolvedOrgKey,
                    sourceMessageId,
                )
                return True

            if archiveMirrorIndex is None and not mirrorWhenArchiveIndexUnavailable:
                log.warning(
                    "Training mirror skipped because archive index was unavailable: org=%s messageId=%s.",
                    resolvedOrgKey,
                    sourceMessageId,
                )
                return True

            mirrored = await self._ensureMirrorMessage(message, storedRow, parsed, orgKey=resolvedOrgKey)
            if refreshSummary and (rowChanged or mirrored):
                await self.refreshSummaryPanel(orgKey=resolvedOrgKey)
            return True

    async def handleSourceMessage(self, message: discord.Message) -> bool:
        return await self._captureRelevantMessage(message, refreshSummary=True)

    async def _fetchStoredRows(self, *, hostId: int | None = None, orgKey: str | None = None) -> list[dict[str, Any]]:
        filters, params = self._orgSqlFilters(orgKey)
        if hostId is not None and int(hostId or 0) > 0:
            filters.append("hostId = ?")
            params.append(int(hostId))
        return await fetchAll(
            "SELECT * FROM training_result_logs"
            f"{self._whereClause(filters)} "
            "ORDER BY datetime(sourceCreatedAt) DESC",
            tuple(params),
        )

    async def listTrainingExportRows(
        self,
        *,
        orgKey: str | None = None,
        onlyReady: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        safeLimit: int | None = None
        if limit is not None:
            try:
                safeLimit = max(0, int(limit or 0))
            except (TypeError, ValueError):
                safeLimit = None
            if safeLimit is not None and safeLimit <= 0:
                return []
        filters, params = self._orgSqlFilters(orgKey, tableAlias="logs")
        if onlyReady:
            filters.append("UPPER(COALESCE(exports.exportStatus, 'PENDING')) = 'READY'")
        limitClause = ""
        if safeLimit is not None:
            limitClause = " LIMIT ?"
            params.append(int(safeLimit))

        rows = await fetchAll(
            f"""
            SELECT
                logs.messageId,
                logs.sourceGuildId,
                logs.sourceChannelId,
                logs.sourceAuthorId,
                logs.sourceCreatedAt,
                logs.eventKind,
                logs.certType,
                logs.certVariant,
                logs.title,
                logs.hostId,
                logs.hostText,
                logs.passCount,
                logs.failCount,
                logs.mirrorChannelId,
                logs.mirrorMessageId,
                logs.rawContent,
                exports.hostRobloxUserId,
                exports.hostRobloxUsername,
                exports.hostResolveStatus,
                exports.hostResolveError,
                exports.exportStatus,
                exports.preparedAt,
                exports.exportedAt,
                exports.exportError,
                exports.sheetRowNumber
            FROM training_result_logs AS logs
            LEFT JOIN training_result_export_state AS exports
                ON exports.messageId = logs.messageId
            {self._whereClause(filters)}
            ORDER BY datetime(logs.sourceCreatedAt) DESC, logs.messageId DESC
            {limitClause}
            """,
            tuple(params),
        )
        out: list[dict[str, Any]] = []
        for row in rows:
            exportStatus = str(row.get("exportStatus") or "PENDING").strip().upper()
            passCount = int(row.get("passCount") or 0)
            failCount = int(row.get("failCount") or 0)
            attendeeCount = max(0, passCount + failCount)
            item = dict(row)
            item.update(
                {
                    "eventTypeLabel": self._displayEventTypeParts(
                        eventKind=str(row.get("eventKind") or ""),
                        certType=str(row.get("certType") or ""),
                        certVariant=str(row.get("certVariant") or ""),
                        title=str(row.get("title") or ""),
                    ),
                    "attendeeCount": attendeeCount,
                    "passerCount": passCount,
                    "passerAttendeeText": f"{passCount}/{attendeeCount}",
                    "sourceMessageUrl": self._sourceMessageUrl(row),
                    "exportStatus": exportStatus,
                    "hostResolveStatus": str(row.get("hostResolveStatus") or "PENDING").strip().upper(),
                    "hostRobloxUsername": str(row.get("hostRobloxUsername") or "").strip(),
                }
            )
            out.append(item)
        return out

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

    async def ensureSummaryPanelAtBottom(self, *, orgKey: str | None = None) -> None:
        if not str(orgKey or "").strip():
            for profile in self._trainingProfiles():
                await self.ensureSummaryPanelAtBottom(orgKey=profile.key)
            return

        archiveChannelId = self._archiveChannelIdForOrg(orgKey)
        archiveChannel = await self._getChannel(archiveChannelId)
        if archiveChannel is None:
            log.warning(
                "Training summary bottom check skipped: org=%s archive channel %s is unavailable.",
                str(orgKey or "").strip().upper() or "UNKNOWN",
                int(archiveChannelId or 0),
            )
            return

        latestMessage = await self._fetchLatestArchiveMessage(archiveChannel)
        if latestMessage is not None and self._messageLooksLikeSummaryPanel(latestMessage, orgKey=str(orgKey)):
            try:
                await self._persistSummaryMessage(
                    orgKey=str(orgKey),
                    channelId=int(archiveChannel.id),
                    messageId=int(latestMessage.id),
                )
            except Exception:
                log.exception("Failed to persist latest training summary panel state for org %s.", orgKey)
            log.info(
                "Training summary bottom check passed: org=%s messageId=%s.",
                str(orgKey or "").strip().upper() or "UNKNOWN",
                int(getattr(latestMessage, "id", 0) or 0),
            )
            return

        await self.refreshSummaryPanel(orgKey=orgKey)

    async def refreshSummaryPanel(self, *, orgKey: str | None = None) -> None:
        if not str(orgKey or "").strip():
            for profile in self._trainingProfiles():
                await self.refreshSummaryPanel(orgKey=profile.key)
            return

        archiveChannelId = self._archiveChannelIdForOrg(orgKey)
        archiveChannel = await self._getChannel(archiveChannelId)
        if archiveChannel is None:
            log.warning(
                "Training summary refresh skipped: org=%s archive channel %s is unavailable.",
                str(orgKey or "").strip().upper() or "UNKNOWN",
                int(archiveChannelId or 0),
            )
            return

        async with self._summaryLock:
            rows = await self._fetchStoredRows(orgKey=str(orgKey))
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(days=7)

            embed = discord.Embed(
                title=_summaryEmbedTitle,
                description=(
                    "Hosted counts use the last 7 days. "
                    "Grid/Emergency counts use trainings, and Grid/Emergency pass rates use exams."
                ),
                color=_trainingMirrorColor,
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

            _, oldMessage = await self._getStoredSummaryMessage(orgKey=str(orgKey))
            if oldMessage is None:
                oldMessage = await self._findLatestSummaryPanelMessage(archiveChannel, orgKey=str(orgKey))

            sentMessage = await self.webhooks.sendOwnedWebhookMessageDetailed(
                botClient=self.botClient,
                channel=archiveChannel,
                webhookName=self._summaryWebhookNameForOrg(orgKey),
                embed=embed,
                reason="Training summary refresh",
            )
            if sentMessage is None:
                try:
                    sentMessage = await self.taskBudgeter.runDiscord(
                        lambda: archiveChannel.send(
                            embed=embed,
                            allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False),
                        )
                    )
                except (discord.Forbidden, discord.HTTPException):
                    log.warning(
                        "Training summary send failed: org=%s archive channel %s.",
                        str(orgKey or "").strip().upper() or "UNKNOWN",
                        int(getattr(archiveChannel, "id", 0) or 0),
                    )
                    return
            if oldMessage is not None and int(oldMessage.id or 0) != int(sentMessage.id or 0):
                await self._deleteSummaryMessage(message=oldMessage, orgKey=str(orgKey))
            try:
                await self._persistSummaryMessage(
                    orgKey=str(orgKey),
                    channelId=int(archiveChannel.id),
                    messageId=int(sentMessage.id),
                )
            except Exception:
                log.exception("Failed to persist training summary panel state for org %s.", orgKey)

    async def syncRecentMessages(self, *, force: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = {
            "forced": bool(force),
            "sourceChannelId": 0,
            "archiveChannelId": 0,
            "scannedCount": 0,
            "capturedCount": 0,
            "failedCount": 0,
            "skipped": False,
            "reason": "",
            "orgResults": [],
        }
        now = datetime.now(timezone.utc)
        if not force and self._lastReadySyncAt is not None:
            if (now - self._lastReadySyncAt).total_seconds() < float(self._readySyncCooldownSec):
                log.info(
                    "Training log backfill skipped: last sync was %.1fs ago.",
                    (now - self._lastReadySyncAt).total_seconds(),
                )
                result["skipped"] = True
                result["reason"] = "cooldown"
                return result
        async with self._syncLock:
            now = datetime.now(timezone.utc)
            if not force and self._lastReadySyncAt is not None:
                if (now - self._lastReadySyncAt).total_seconds() < float(self._readySyncCooldownSec):
                    log.info(
                        "Training log backfill skipped inside lock: last sync was %.1fs ago.",
                        (now - self._lastReadySyncAt).total_seconds(),
                    )
                    result["skipped"] = True
                    result["reason"] = "cooldown"
                    return result
            orgResults: list[dict[str, Any]] = []
            aggregateScanned = 0
            aggregateCaptured = 0
            aggregateFailed = 0
            for profile in self._trainingProfiles():
                orgKey = profile.key
                sourceChannelId = int(self._sourceChannelIdForOrg(orgKey) or 0)
                archiveChannelId = int(self._archiveChannelIdForOrg(orgKey) or 0)
                orgResult = {
                    "orgKey": orgKey,
                    "sourceChannelId": sourceChannelId,
                    "archiveChannelId": archiveChannelId,
                    "scannedCount": 0,
                    "capturedCount": 0,
                    "failedCount": 0,
                    "reason": "",
                }
                log.info(
                    "Training log backfill starting: org=%s sourceChannelId=%s archiveChannelId=%s days=%s",
                    orgKey,
                    sourceChannelId,
                    archiveChannelId,
                    int(self._backfillDaysForOrg(orgKey) or 0),
                )
                sourceChannel = await self._getChannel(sourceChannelId)
                if sourceChannel is None:
                    log.warning(
                        "Training log backfill aborted for org %s: source channel %s is unavailable.",
                        orgKey,
                        sourceChannelId,
                    )
                    orgResult["reason"] = "source-channel-unavailable"
                    orgResults.append(orgResult)
                    await self.ensureSummaryPanelAtBottom(orgKey=orgKey)
                    continue

                archiveMirrorIndex: dict[int, int] | None = None
                archiveChannel = await self._getChannel(archiveChannelId)
                if archiveChannel is None:
                    log.warning(
                        "Training log backfill will not create mirrors for org %s: archive channel %s is unavailable.",
                        orgKey,
                        archiveChannelId,
                    )
                else:
                    archiveMirrorIndex = await self._buildArchiveMirrorIndex(
                        archiveChannel,
                        orgKey=orgKey,
                        limit=None if force else self._archiveIndexLimitForOrg(orgKey),
                    )

                cutoff = now - timedelta(days=self._backfillDaysForOrg(orgKey))
                messageLimit = None if force else self._startupBackfillMessageLimitForOrg(orgKey)
                mirrorNewRows = bool(force) or (
                    archiveMirrorIndex is not None and self._startupMirrorNewRowsForOrg(orgKey)
                )
                scannedCount = 0
                capturedCount = 0
                failedCount = 0
                try:
                    async for message in sourceChannel.history(
                        limit=messageLimit,
                        after=cutoff,
                        oldest_first=True,
                    ):
                        scannedCount += 1
                        try:
                            captured = await self._captureRelevantMessage(
                                message,
                                refreshSummary=False,
                                orgKey=orgKey,
                                archiveMirrorIndex=archiveMirrorIndex,
                                mirrorNewRows=mirrorNewRows,
                                mirrorExistingRows=False,
                                mirrorWhenArchiveIndexUnavailable=False,
                            )
                        except Exception:
                            failedCount += 1
                            log.exception(
                                "Training log backfill failed for org %s message %s in channel %s.",
                                orgKey,
                                int(getattr(message, "id", 0) or 0),
                                int(getattr(getattr(message, "channel", None), "id", 0) or 0),
                            )
                            continue
                        if captured:
                            capturedCount += 1
                        if scannedCount % 50 == 0:
                            log.info(
                                "Training log backfill progress: org=%s sourceChannelId=%s scanned=%s captured=%s failed=%s",
                                orgKey,
                                sourceChannelId,
                                scannedCount,
                                capturedCount,
                                failedCount,
                            )
                            await asyncio.sleep(0)
                except Exception:
                    log.exception("Failed to backfill training-results messages for org %s.", orgKey)
                    orgResult["reason"] = "history-read-failed"
                if messageLimit is not None and scannedCount >= messageLimit and not orgResult.get("reason"):
                    orgResult["reason"] = "message-limit-reached"
                    log.warning(
                        "Training log startup backfill hit message limit: org=%s sourceChannelId=%s limit=%s.",
                        orgKey,
                        sourceChannelId,
                        messageLimit,
                    )

                orgResult["scannedCount"] = int(scannedCount)
                orgResult["capturedCount"] = int(capturedCount)
                orgResult["failedCount"] = int(failedCount)
                orgResults.append(orgResult)
                aggregateScanned += int(scannedCount)
                aggregateCaptured += int(capturedCount)
                aggregateFailed += int(failedCount)
                log.info(
                    "Training log backfill finished: org=%s sourceChannelId=%s scanned=%s captured=%s failed=%s cutoff=%s",
                    orgKey,
                    sourceChannelId,
                    scannedCount,
                    capturedCount,
                    failedCount,
                    cutoff.isoformat(),
                )
                await self.ensureSummaryPanelAtBottom(orgKey=orgKey)

            result["orgResults"] = orgResults
            if orgResults:
                result["sourceChannelId"] = int(orgResults[0].get("sourceChannelId") or 0)
                result["archiveChannelId"] = int(orgResults[0].get("archiveChannelId") or 0)
            result["scannedCount"] = int(aggregateScanned)
            result["capturedCount"] = int(aggregateCaptured)
            result["failedCount"] = int(aggregateFailed)
            self._lastReadySyncAt = datetime.now(timezone.utc)
            result["completed"] = True
            return result

    async def runManualMirrorBackfillOnce(self, *, userId: int) -> tuple[bool, str]:
        result = await self.syncRecentMessages(force=True)
        if not bool(result.get("completed")):
            reason = str(result.get("reason") or "unknown").strip()
            return False, (
                "Training history mirror did not complete.\n"
                f"reason: `{reason or 'unknown'}`\n"
                f"sourceChannelId: `{int(result.get('sourceChannelId') or 0)}`\n"
                f"archiveChannelId: `{int(result.get('archiveChannelId') or 0)}`"
            )

        return True, (
            "Training history mirror finished.\n"
            f"scanned: `{int(result.get('scannedCount') or 0)}`\n"
            f"captured: `{int(result.get('capturedCount') or 0)}`\n"
            f"failed: `{int(result.get('failedCount') or 0)}`\n"
            f"archiveChannelId: `{int(result.get('archiveChannelId') or 0)}`"
        )

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
        statsOrgKey = orgProfiles.getOrganizationKeyForGuild(self.config, int(message.guild.id))
        rows = [
            row
            for row in allRows
            if self._rowBelongsToOrg(row, statsOrgKey)
            and (
                int(row.get("hostId") or 0) == int(targetUserId)
                or (
                    int(row.get("hostId") or 0) <= 0
                    and _normalizeNameLookup(row.get("hostText")) in targetLabels
                )
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
