from __future__ import annotations

import logging
from datetime import date
from typing import Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands

import config
from cogs.staff.honorGuardViews import (
    HonorGuardEventClockinView,
    HonorGuardEventManageModal,
    HonorGuardPointAwardReviewView,
    HonorGuardSentryReviewView,
)
from features.staff.clockins import ClockinEngine, resolveAttendeeUserIdFromToken
from features.staff.clockins.honorGuardEventAdapter import HonorGuardEventClockinAdapter
from features.staff.honorGuard import buildScaffoldStatus
from features.staff.honorGuard import rendering as honorGuardRendering
from features.staff.honorGuard import service as honorGuardService
from runtime import cogGuards as runtimeCogGuards
from runtime import interaction as interactionRuntime
from runtime import normalization
from runtime import orbatAudit as orbatAuditRuntime
from runtime import permissions as runtimePermissions

log = logging.getLogger(__name__)

PLUGIN_MANIFEST = {
    "displayName": "Honor-Guard",
    "category": "staff",
    "description": "Honor-Guard ORBAT integration status and review-flow backend.",
}


def _displayChannel(channelId: int) -> str:
    return f"<#{channelId}>" if int(channelId or 0) > 0 else "`not set`"


def _displayText(value: str) -> str:
    text = str(value or "").strip()
    return f"`{text}`" if text else "`not set`"


def _isImageAttachment(attachment: discord.Attachment) -> bool:
    contentType = (attachment.content_type or "").lower()
    if contentType.startswith("image/"):
        return True
    filename = (attachment.filename or "").lower()
    return filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"))


def _evidenceLinks(attachments: Sequence[discord.Attachment]) -> list[str]:
    return [attachment.url for attachment in attachments if _isImageAttachment(attachment)]


def _reviewerMention() -> str:
    roleId = int(
        getattr(
            config,
            "honorGuardReviewerPingRoleId",
            getattr(config, "honorGuardReviewerRoleId", 0),
        )
        or 0
    )
    if roleId > 0:
        return f"<@&{roleId}>"
    return ""


def _hasRole(member: discord.Member, roleId: Optional[int]) -> bool:
    return runtimePermissions.hasAnyRole(member, [roleId])


def _normalizeRoleIdList(rawValues: object) -> set[int]:
    return normalization.normalizeIntSet(rawValues)


class HonorGuardCog(runtimeCogGuards.InteractionGuardMixin, commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._eventClockinAdapter = HonorGuardEventClockinAdapter()
        self._eventClockinEngine = ClockinEngine(bot, self._eventClockinAdapter)

    async def cog_load(self) -> None:
        await self._eventClockinEngine.restoreOpenViews(
            lambda sessionId: HonorGuardEventClockinView(self, sessionId),
        )

    def _canAwardPoints(self, member: discord.Member) -> bool:
        honorGuardReviewerRoleId = int(getattr(config, "honorGuardReviewerRoleId", 0) or 0)
        if honorGuardReviewerRoleId <= 0:
            return True
        return _hasRole(member, honorGuardReviewerRoleId)

    def _canHostEventClockin(self, member: discord.Member) -> bool:
        configuredRoleIds = _normalizeRoleIdList(getattr(config, "honorGuardEventHostRoleIds", []))
        if configuredRoleIds:
            return any(role.id in configuredRoleIds for role in member.roles)
        if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
            return True
        return self._canAwardPoints(member)

    def _honorGuardCommandGuildIds(self) -> set[int]:
        configuredGuildIds = _normalizeRoleIdList(getattr(config, "honorGuardCommandGuildIds", []))
        if configuredGuildIds:
            return configuredGuildIds
        fallbackGuildIds = [
            getattr(config, "serverId", 0),
            getattr(config, "serverIdTesting", 0),
            *(getattr(config, "testGuildIds", []) or []),
        ]
        return _normalizeRoleIdList(fallbackGuildIds)

    async def _ensureHonorGuardCommandGuild(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            await self._safeReply(
                interaction,
                "This command can only be used in a server channel.",
            )
            return False
        allowedGuildIds = self._honorGuardCommandGuildIds()
        if not allowedGuildIds or int(interaction.guild.id) in allowedGuildIds:
            return True
        await self._safeReply(
            interaction,
            "Honor-Guard commands can only be used in the CE server or configured test servers.",
        )
        return False

    @staticmethod
    def _memberDisplayName(member: discord.Member) -> str:
        return str(
            getattr(member, "display_name", None)
            or getattr(member, "global_name", None)
            or getattr(member, "name", None)
            or member.id
        ).strip()

    @staticmethod
    def _parseUserIdList(raw: str) -> list[int]:
        values: list[int] = []
        for token in str(raw or "").replace(",", " ").split():
            userId = normalization.parseDiscordUserId(token)
            if userId <= 0 or userId in values:
                continue
            values.append(userId)
        return values

    async def _resolveReviewChannel(
        self,
        guild: discord.Guild,
        fallback: Optional[discord.abc.Messageable],
        *,
        channelId: Optional[int] = None,
    ) -> Optional[discord.abc.Messageable]:
        targetChannelId = int(channelId or 0)
        if targetChannelId > 0:
            channel = self.bot.get_channel(targetChannelId)
            if channel is None:
                channel = guild.get_channel(targetChannelId)
            if channel is None:
                channel = await interactionRuntime.safeFetchChannel(self.bot, targetChannelId)
            if channel is not None:
                return channel
        return fallback

    async def _updateEventClockinMessage(
        self,
        sessionId: int,
        *,
        message: Optional[discord.Message] = None,
    ) -> None:
        await self._eventClockinEngine.updateClockinMessage(
            int(sessionId),
            viewFactory=lambda resolvedSessionId: HonorGuardEventClockinView(self, resolvedSessionId),
            message=message,
        )

    async def _deleteEventClockinMessage(
        self,
        session: dict,
        *,
        message: Optional[discord.Message] = None,
    ) -> None:
        await self._eventClockinEngine.deleteClockinMessage(
            session,
            message=message,
        )

    async def _refreshEventClockinMessageFromInteraction(
        self,
        sessionId: int,
        interaction: discord.Interaction,
    ) -> None:
        if isinstance(interaction.message, discord.Message):
            await self._updateEventClockinMessage(sessionId, message=interaction.message)
            return
        await self._updateEventClockinMessage(sessionId)

    async def _isEventHost(self, interaction: discord.Interaction, session: dict) -> bool:
        return interaction.user.id == int(session.get("hostId") or 0)

    async def openHonorGuardEventManage(self, interaction: discord.Interaction, sessionId: int) -> None:
        session = await self._eventClockinEngine.getSession(int(sessionId))
        if not session:
            await self._safeReply(interaction, "This Honor-Guard event clock-in no longer exists.")
            return
        if not await self._isEventHost(interaction, session):
            await self._safeReply(interaction, "Only the event host can manage attendees.")
            return
        if str(session.get("status") or "").upper() != "OPEN":
            await self._safeReply(interaction, "This Honor-Guard event is no longer open.")
            return
        attendees = await self._eventClockinEngine.listAttendees(int(sessionId))
        if not attendees:
            await self._safeReply(interaction, "No attendees are currently clocked in.")
            return
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            HonorGuardEventManageModal(self, int(sessionId)),
        )

    async def handleHonorGuardEventManage(
        self,
        interaction: discord.Interaction,
        sessionId: int,
        token: str,
    ) -> None:
        session = await self._eventClockinEngine.getSession(int(sessionId))
        if not session:
            await self._safeReply(interaction, "This Honor-Guard event clock-in no longer exists.")
            return
        if not await self._isEventHost(interaction, session):
            await self._safeReply(interaction, "Only the event host can manage attendees.")
            return
        attendees = await self._eventClockinEngine.listAttendees(int(sessionId))
        if not attendees:
            await self._safeReply(interaction, "No attendees are currently clocked in.")
            return
        targetUserId = resolveAttendeeUserIdFromToken(token, attendees)
        if not targetUserId:
            await self._safeReply(interaction, "Could not match that attendee in this event.")
            return
        await self._eventClockinEngine.removeAttendee(int(sessionId), int(targetUserId))
        await self._safeReply(interaction, f"Removed <@{int(targetUserId)}> from this event.")
        await self._refreshEventClockinMessageFromInteraction(int(sessionId), interaction)

    async def handleHonorGuardEventJoin(self, interaction: discord.Interaction, sessionId: int) -> None:
        if interaction.user.bot:
            await self._safeReply(interaction, "Bots cannot clock in to Honor-Guard events.")
            return
        session = await self._eventClockinEngine.getSession(int(sessionId))
        if not session:
            await self._safeReply(interaction, "This Honor-Guard event clock-in no longer exists.")
            return
        if str(session.get("status") or "").upper() != "OPEN":
            await self._safeReply(interaction, "This Honor-Guard event is no longer open.")
            return
        if interaction.user.id == int(session.get("hostId") or 0):
            await self._safeReply(interaction, "You are already listed as the host for this event.")
            return
        if int(interaction.user.id) in set(session.get("coHostUserIds") or []):
            await self._safeReply(interaction, "You are already listed as a co-host for this event.")
            return
        if int(interaction.user.id) in set(session.get("supervisorUserIds") or []):
            await self._safeReply(interaction, "You are already listed as a supervisor for this event.")
            return
        attendees = await self._eventClockinEngine.listAttendees(int(sessionId))
        attendeeUserIds = {int(row.get("userId") or 0) for row in attendees}
        if int(interaction.user.id) in attendeeUserIds:
            await self._safeReply(interaction, "You are already clocked in to this event.")
            return
        maxAttendeeLimit = max(1, int(session.get("maxAttendeeLimit") or 30))
        if len(attendeeUserIds) >= maxAttendeeLimit:
            await self._safeReply(interaction, "This Honor-Guard event has reached its attendee limit.")
            return
        await self._eventClockinEngine.addAttendee(int(sessionId), int(interaction.user.id))
        await self._safeReply(interaction, "You have been added to this Honor-Guard event.")
        await self._refreshEventClockinMessageFromInteraction(int(sessionId), interaction)

    async def handleHonorGuardEventDelete(self, interaction: discord.Interaction, sessionId: int) -> None:
        session = await self._eventClockinEngine.getSession(int(sessionId))
        if not session:
            await self._safeReply(interaction, "This Honor-Guard event clock-in no longer exists.")
            return
        if not await self._isEventHost(interaction, session):
            await self._safeReply(interaction, "Only the event host can delete this event clock-in.")
            return
        await self._eventClockinEngine.updateSessionStatus(int(sessionId), "CANCELED")
        await self._safeReply(interaction, "Honor-Guard event clock-in canceled.")
        if isinstance(interaction.message, discord.Message):
            await self._deleteEventClockinMessage(session, message=interaction.message)
            return
        await self._deleteEventClockinMessage(session)

    async def handleHonorGuardEventFinish(self, interaction: discord.Interaction, sessionId: int) -> None:
        session = await self._eventClockinEngine.getSession(int(sessionId))
        if not session:
            await self._safeReply(interaction, "This Honor-Guard event clock-in no longer exists.")
            return
        if not await self._isEventHost(interaction, session):
            await self._safeReply(interaction, "Only the event host can finish this event clock-in.")
            return
        if str(session.get("status") or "").upper() != "OPEN":
            await self._safeReply(interaction, "This Honor-Guard event is no longer open.")
            return

        await interactionRuntime.safeInteractionDefer(
            interaction,
            ephemeral=True,
            thinking=True,
        )
        try:
            result = await honorGuardService.finalizeEventClockinSession(
                int(sessionId),
                finalizedBy=int(interaction.user.id),
            )
        except ValueError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return
        except Exception as exc:
            await interaction.followup.send(
                f"Honor-Guard event finalization failed: {exc}",
                ephemeral=True,
            )
            return

        if isinstance(interaction.message, discord.Message):
            await self._deleteEventClockinMessage(session, message=interaction.message)
        else:
            await self._deleteEventClockinMessage(session)

        extraNotes: list[str] = []
        unresolvedUsers = list(result.get("unresolvedUsers") or [])
        if unresolvedUsers:
            extraNotes.append(
                "Unresolved sheet users: " + ", ".join(f"<@{int(userId)}>" for userId in unresolvedUsers[:8])
            )
        archiveError = str(result.get("archiveError") or "").strip()
        if archiveError:
            extraNotes.append(f"Archive/schedule sync warning: {archiveError}")
        summary = (
            f"Honor-Guard event finalized.\n"
            f"Type: `{result.get('eventType') or 'event'}`\n"
            f"Attendees: `{int(result.get('attendeeCount') or 0)}`\n"
            f"Attendance records: `{int(result.get('createdAttendanceRecords') or 0)}`\n"
            f"Member sheet updates: `{int(result.get('updatedUsers') or 0)}`"
        )
        if extraNotes:
            summary += "\n" + "\n".join(extraNotes)

        try:
            archiveResult = result.get("archiveResult") if isinstance(result.get("archiveResult"), dict) else {}
            sheetRefs: list[dict[str, str]] = []
            if int(result.get("updatedUsers") or 0) > 0:
                sheetRefs.append({"sheetKey": "honorGuard_members"})
            if bool(archiveResult.get("archiveSynced")):
                sheetRefs.append({"sheetKey": "honorGuard_archive"})
            if archiveResult.get("scheduleRemoval") is not None:
                sheetRefs.append({"sheetKey": "honorGuard_schedule"})
            if archiveResult.get("eventHostUpdate") is not None:
                sheetRefs.append({"sheetKey": "honorGuard_eventHosts"})
            if sheetRefs:
                detailParts = [
                    f"Event: {str(result.get('eventTitle') or result.get('eventType') or 'Honor Guard event')}",
                    f"Date: {str(result.get('eventDate') or 'unknown')}",
                    f"Attendees: {int(result.get('attendeeCount') or 0)}",
                    f"Attendance records: {int(result.get('createdAttendanceRecords') or 0)}",
                    f"Member sheet updates: {int(result.get('updatedUsers') or 0)}",
                ]
                if archiveResult.get("scheduleRemoval") is not None:
                    detailParts.append("Schedule: removed archived event row")
                if archiveResult.get("eventHostUpdate") is not None:
                    detailParts.append("Event host stats: updated")
                if unresolvedUsers:
                    detailParts.append(f"Unresolved users: {len(unresolvedUsers)}")
                await orbatAuditRuntime.sendOrbatChangeLog(
                    self.bot,
                    title="Spreadsheet Change",
                    change="Updated Honor-Guard spreadsheets from finalized event.",
                    requestedBy=interaction.user.mention,
                    authorizedBy=interaction.user.mention,
                    requestMessageUrl=str(getattr(interaction.message, "jump_url", "") or ""),
                    details=" | ".join(detailParts),
                    sheetRefs=sheetRefs,
                )
        except Exception:
            log.exception(
                "Failed to post Honor-Guard spreadsheet audit log for finalized event session %s.",
                sessionId,
            )

        await interaction.followup.send(summary, ephemeral=True)

    @app_commands.command(
        name="honor-guard-status",
        description="Show the current Honor-Guard ORBAT integration wiring.",
    )
    async def honorGuardStatus(self, interaction: discord.Interaction) -> None:
        member = await self._requireAdminOrManageGuild(interaction)
        if member is None:
            return
        status = buildScaffoldStatus(configModule=config)
        summary = [
            f"Enabled: `{status.config.enabled}`",
            f"Review channel: {_displayChannel(status.config.reviewChannelId)}",
            f"Log channel: {_displayChannel(status.config.logChannelId)}",
            f"Archive channel: {_displayChannel(status.config.archiveChannelId)}",
            f"Spreadsheet: {_displayText(status.config.spreadsheetId)}",
            f"Member sheet: {_displayText(status.config.memberSheetName)}",
            f"Schedule sheet: {_displayText(status.config.scheduleSheetName)}",
            f"Archive sheet: {_displayText(status.config.archiveSheetName)}",
            f"Event hosts sheet: {_displayText(status.config.eventHostsSheetName)}",
        ]
        embed = discord.Embed(
            title="Honor-Guard Integration",
            description="Backend tables, point rules, and sheet adapter are wired. Review commands are still separate.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Current Wiring", value="\n".join(summary), inline=False)
        embed.add_field(
            name="DB Tables",
            value="\n".join(f"`{name}`" for name in status.plannedDbTables),
            inline=False,
        )
        if status.sheetProblems:
            embed.add_field(
                name="Sheet Adapter Warnings",
                value="\n".join(f"- {problem}" for problem in status.sheetProblems),
                inline=False,
            )
        embed.add_field(
            name="Next Milestones",
            value="\n".join(status.nextMilestones),
            inline=False,
        )
        await self._safeReply(interaction, embed=embed)

    @app_commands.command(
        name="honorguard-award-points",
        description="Award points to a member of the Honor-Guard.",
    )
    @app_commands.describe(
        awarded_user="User you want to award",
        quota_points="Quota Points you want to award",
        event_points="Event Points you want to award",
        reason="The reason for the award",
    )
    @app_commands.rename(awarded_user="awarded-user")
    @app_commands.rename(quota_points="quota-points")
    @app_commands.rename(event_points="event-points")
    async def honorGuardAwardPoints(
        self,
        interaction: discord.Interaction,
        awarded_user: discord.Member,
        reason: str,
        event_points: float,
        quota_points: float = 0.0,
    ) -> None:
        if not await self._ensureHonorGuardCommandGuild(interaction):
            return
        if not interaction.channel or not isinstance(interaction.user, discord.Member):
            await self._safeReply(
                interaction,
                "This command can only be used in a server channel.",
            )
            return
        if not self._canAwardPoints(interaction.user):
            await self._safeReply(
                interaction,
                "You do not have permission to award Honor-Guard points.",
            )
            return
        if float(event_points or 0) < 0 or float(quota_points or 0) < 0:
            await self._safeReply(
                interaction,
                "Honor-Guard point awards cannot be negative.",
            )
            return
        if float(event_points or 0) <= 0 and float(quota_points or 0) <= 0:
            await self._safeReply(
                interaction,
                "Set at least one positive point value before submitting the award.",
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        submissionId = await honorGuardService.createPointAwardSubmission(
            guildId=int(interaction.guild.id),
            channelId=int(interaction.channel.id),
            submitterId=int(interaction.user.id),
            awardedUserId=int(awarded_user.id),
            quotaPoints=float(quota_points or 0),
            eventPoints=float(event_points or 0),
            reason=str(reason or "").strip(),
            awardedUserDisplayName=self._memberDisplayName(awarded_user),
        )
        submission = await honorGuardService.getPointAwardSubmission(submissionId)
        if not submission:
            await interaction.followup.send(
                "Failed to create point award submission.",
                ephemeral=True,
            )
            return

        embed = honorGuardRendering.buildPointAwardEmbed(submission)
        view = HonorGuardPointAwardReviewView(self, submissionId)
        reviewMessage = await self._postHonorGuardForReview(
            guild=interaction.guild,
            fallbackChannel=interaction.channel,
            embed=embed,
            view=view,
            reviewChannelId=int(getattr(config, "honorGuardReviewChannelId", 0) or 0),
        )
        if not reviewMessage:
            await interaction.followup.send(
                "Submission saved, but I could not post it for review.",
                ephemeral=True,
            )
            return

        await honorGuardService.setPointAwardMessageId(submissionId, reviewMessage.id)
        await interaction.followup.send(
            "Submitted point award log.",
            ephemeral=True,
        )

    @app_commands.command(
        name="honorguard-solo-sentry",
        description="Submit a solo sentry log for Honor-Guard review.",
    )
    @app_commands.describe(
        duty_date="Duty date in YYYY-MM-DD format.",
        roblox_username="Your Roblox username as it appears on the HG ORBAT.",
        image="Primary sentry screenshot.",
        extra_image="Second sentry screenshot.",
    )
    @app_commands.rename(duty_date="duty-date")
    @app_commands.rename(roblox_username="roblox-username")
    @app_commands.rename(extra_image="extra-image")
    async def honorGuardSoloSentry(
        self,
        interaction: discord.Interaction,
        duty_date: str,
        roblox_username: str,
        image: discord.Attachment,
        extra_image: discord.Attachment,
    ) -> None:
        if not await self._ensureHonorGuardCommandGuild(interaction):
            return
        if not interaction.channel or not isinstance(interaction.user, discord.Member):
            await self._safeReply(
                interaction,
                "This command can only be used in a server channel.",
            )
            return

        try:
            normalizedDutyDate = date.fromisoformat(str(duty_date or "").strip()).isoformat()
        except ValueError:
            await self._safeReply(
                interaction,
                "Duty date must use the `YYYY-MM-DD` format.",
            )
            return

        attachments = [image, extra_image]
        imageUrls = _evidenceLinks(attachments)
        if len(imageUrls) < 2:
            await self._safeReply(
                interaction,
                "Two valid image attachments are required for solo sentry logs.",
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        imageFiles: list[discord.File] = []
        for attachment in attachments:
            if not _isImageAttachment(attachment):
                continue
            try:
                imageFiles.append(await attachment.to_file())
            except (discord.HTTPException, OSError):
                continue
        if len(imageFiles) < 2:
            await interaction.followup.send(
                "I could not copy both sentry screenshots for review. Please try again.",
                ephemeral=True,
            )
            return

        try:
            submissionId = await honorGuardService.createSentrySubmission(
                guildId=int(interaction.guild.id),
                channelId=int(interaction.channel.id),
                submitterId=int(interaction.user.id),
                targetUserId=int(interaction.user.id),
                targetRobloxUsername=str(roblox_username or "").strip(),
                targetDisplayName=self._memberDisplayName(interaction.user),
                dutyDate=normalizedDutyDate,
                imageUrls=imageUrls,
            )
        except ValueError as exc:
            await interaction.followup.send(
                str(exc),
                ephemeral=True,
            )
            return

        submission = await honorGuardService.getSentrySubmission(submissionId)
        if not submission:
            await interaction.followup.send(
                "Failed to create solo sentry submission.",
                ephemeral=True,
            )
            return

        embed = honorGuardRendering.buildSentrySubmissionEmbed(submission)
        view = HonorGuardSentryReviewView(self, submissionId)
        reviewMessage = await self._postHonorGuardForReview(
            guild=interaction.guild,
            fallbackChannel=interaction.channel,
            embed=embed,
            view=view,
            files=imageFiles,
            reviewChannelId=int(getattr(config, "honorGuardReviewChannelId", 0) or 0),
        )
        if not reviewMessage:
            await interaction.followup.send(
                "Submission saved, but I could not post it for review.",
                ephemeral=True,
            )
            return

        await honorGuardService.setSubmissionMessageId(submissionId, reviewMessage.id)
        await interaction.followup.send(
            "Submitted solo sentry log.",
            ephemeral=True,
        )

    @app_commands.command(
        name="honorguard-event-log",
        description="Create a clock-in for an Honor-Guard event.",
    )
    @app_commands.describe(
        event_type="Event type like gamenight, orientation, training, lecture, jge, nco_exam, tryout, or inspection.",
        event_title="Displayed event title or detail.",
        event_time_utc="Optional event date/time text for archive and schedule matching.",
        cohosts="Optional comma-separated co-host mentions or Discord IDs.",
        supervisors="Optional comma-separated supervisor mentions or Discord IDs.",
        schedule_event_id="Optional schedule event ID for cleaner archive removal.",
        notes="Optional event notes.",
        max_attendees="Maximum number of attendees allowed to clock in.",
    )
    @app_commands.rename(event_type="event-type")
    @app_commands.rename(event_title="event-title")
    @app_commands.rename(event_time_utc="event-time-utc")
    @app_commands.rename(schedule_event_id="schedule-event-id")
    @app_commands.rename(max_attendees="max-attendees")
    async def honorGuardEventLog(
        self,
        interaction: discord.Interaction,
        event_type: str,
        event_title: str,
        event_time_utc: str = "",
        cohosts: str = "",
        supervisors: str = "",
        schedule_event_id: str = "",
        notes: str = "",
        max_attendees: app_commands.Range[int, 1, 100] = 30,
    ) -> None:
        if not await self._ensureHonorGuardCommandGuild(interaction):
            return
        if not interaction.channel or not isinstance(interaction.user, discord.Member):
            await self._safeReply(
                interaction,
                "This command can only be used in a server channel.",
            )
            return
        if not self._canHostEventClockin(interaction.user):
            await self._safeReply(
                interaction,
                "You do not have permission to start Honor-Guard event clock-ins.",
            )
            return

        normalizedCohosts = [
            userId
            for userId in self._parseUserIdList(cohosts)
            if userId != int(interaction.user.id)
        ]
        normalizedSupervisors = [
            userId
            for userId in self._parseUserIdList(supervisors)
            if userId not in {int(interaction.user.id), *normalizedCohosts}
        ]
        normalizedEventTime = str(event_time_utc or "").strip() or date.today().isoformat()

        await interaction.response.defer(ephemeral=True, thinking=True)
        sessionId = await self._eventClockinEngine.createSession(
            guildId=int(interaction.guild.id),
            channelId=int(interaction.channel.id),
            hostId=int(interaction.user.id),
            maxAttendeeLimit=int(max_attendees or 30),
            eventType=str(event_type or "").strip(),
            eventTitle=str(event_title or "").strip(),
            eventDate=normalizedEventTime,
            coHostUserIds=normalizedCohosts,
            supervisorUserIds=normalizedSupervisors,
            scheduleEventId=str(schedule_event_id or "").strip(),
            notes=str(notes or "").strip(),
            createdBy=int(interaction.user.id),
        )
        session = await self._eventClockinEngine.getSession(int(sessionId))
        if not session:
            await interaction.followup.send(
                "Could not create Honor-Guard event clock-in.",
                ephemeral=True,
            )
            return

        embed = self._eventClockinAdapter.buildEmbed(session, [])
        view = HonorGuardEventClockinView(self, int(sessionId))
        message = await interactionRuntime.safeChannelSend(
            interaction.channel,
            embed=embed,
            view=view,
        )
        if message is None:
            await interaction.followup.send(
                "Could not create the Honor-Guard clock-in message in this channel.",
                ephemeral=True,
            )
            return
        await self._eventClockinEngine.setSessionMessageId(int(sessionId), int(message.id))
        await interaction.followup.send(
            "Honor-Guard event clock-in created.",
            ephemeral=True,
        )

    @app_commands.command(
        name="honorguard-schedule-event",
        description="Schedule an event for Honor-Guard.",
    )
    @app_commands.describe(
        member="Member associated with the scheduled Honor-Guard event.",
        event_description="Short description of the event to schedule.",
    )
    @app_commands.rename(event_description="event-description")
    async def honorGuardScheduleEvent(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        event_description: str,
    ) -> None:
        _ = member
        _ = event_description
        await self._safeReply(
            interaction,
            "Honor-Guard event scheduling is not wired yet.",
        )

    @app_commands.command(
        name="honorguard-quota-cycle",
        description="Cycle the quota for Honor-Guard.",
    )
    async def honorGuardQuotaCycle(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
    ) -> None:
        _ = member
        await self._safeReply(
            interaction,
            "Honor-Guard quota cycling is not wired yet.",
        )

    async def _postHonorGuardForReview(
        self,
        *,
        guild: discord.Guild,
        fallbackChannel: Optional[discord.abc.Messageable],
        embed: discord.Embed,
        view: discord.ui.View,
        files: Optional[list[discord.File]] = None,
        reviewChannelId: Optional[int] = None,
    ) -> Optional[discord.Message]:
        channel = await self._resolveReviewChannel(
            guild,
            fallbackChannel,
            channelId=reviewChannelId,
        )
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return None
        mention = _reviewerMention()
        content = mention or None
        allowedMentions = discord.AllowedMentions(roles=True, users=True)
        return await interactionRuntime.safeChannelSend(
            channel,
            content=content,
            embed=embed,
            view=view,
            files=files or [],
            allowed_mentions=allowedMentions,
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(HonorGuardCog(bot))
