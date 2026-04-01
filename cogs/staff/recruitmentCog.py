
import asyncio
import logging
from typing import Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands

from cogs.staff.recruitmentViews import (
    GroupPatrolFinishModal,
    GroupPatrolManageModal,
    GroupPatrolView,
    RecruitmentReviewView,
    SoloPatrolDetailsModal,
)
from features.staff.clockins import ClockinEngine, resolveAttendeeUserIdFromToken
from features.staff.clockins.recruitmentPatrolAdapter import RecruitmentPatrolAdapter
import config
from features.staff.recruitment import rendering as recruitmentRendering
from features.staff.recruitment import service as recruitmentService
from runtime import interaction as interactionRuntime


log = logging.getLogger(__name__)
patrolTypeChoices = [
    app_commands.Choice(name="Solo", value="solo"),
    app_commands.Choice(name="Group", value="group"),
]


def _isImageAttachment(attachment: discord.Attachment) -> bool:
    contentType = (attachment.content_type or "").lower()
    if contentType.startswith("image/"):
        return True
    filename = (attachment.filename or "").lower()
    return filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"))


def _patrolPoints(durationMinutes: int) -> int:
    pointsPer15 = int(getattr(config, "recruitmentPointsPer15Minutes", 1) or 1)
    if durationMinutes <= 0 or pointsPer15 <= 0:
        return 0
    return max(0, durationMinutes // 15) * pointsPer15


def _hasRole(member: discord.Member, roleId: Optional[int]) -> bool:
    if not roleId:
        return False
    return any(role.id == int(roleId) for role in member.roles)


def _normalizeRoleIdList(rawValues) -> set[int]:
    out: set[int] = set()
    for value in rawValues or []:
        try:
            out.add(int(value))
        except (TypeError, ValueError):
            continue
    return out

def _reviewerMention() -> str:
    roleId = int(getattr(config, "recruitmentReviewerRoleId", 0) or 0)
    if roleId > 0:
        return f"<@&{roleId}>"
    return ""


def _evidenceLinks(attachments: Sequence[discord.Attachment]) -> list[str]:
    return [attachment.url for attachment in attachments if _isImageAttachment(attachment)]


class RecruitmentCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._patrolLocks: dict[int, asyncio.Lock] = {}
        self._groupPatrolAdapter = RecruitmentPatrolAdapter()
        self._groupPatrolEngine = ClockinEngine(bot, self._groupPatrolAdapter)

    async def cog_load(self) -> None:
        await self._restoreReviewViews()
        await self._restoreOpenPatrolViews()

    async def _restoreReviewViews(self) -> None:
        recruitRows = await recruitmentService.listRecruitmentPendingStatuses()
        for row in recruitRows:
            messageId = int(row.get("messageId") or 0)
            if messageId <= 0:
                continue
            self.bot.add_view(
                RecruitmentReviewView(self, "recruitment", int(row["submissionId"])),
                message_id=messageId,
            )

        timeRows = await recruitmentService.listRecruitmentTimePendingStatuses()
        for row in timeRows:
            messageId = int(row.get("messageId") or 0)
            if messageId <= 0:
                continue
            self.bot.add_view(
                RecruitmentReviewView(self, "time", int(row["submissionId"])),
                message_id=messageId,
            )

    async def _restoreOpenPatrolViews(self) -> None:
        await self._groupPatrolEngine.restoreOpenViews(
            lambda patrolId: GroupPatrolView(self, patrolId),
        )

    def _canSubmitRecruitment(self, member: discord.Member) -> bool:
        recruiterRoleId = int(getattr(config, "recruiterRoleId", 0) or 0)
        if recruiterRoleId <= 0:
            return True
        return _hasRole(member, recruiterRoleId)

    def _canHostGroupPatrol(self, member: discord.Member) -> bool:
        configuredRoleIds = _normalizeRoleIdList(
            getattr(config, "recruitmentPatrolGroupHostRoleIds", []),
        )
        if configuredRoleIds:
            return any(role.id in configuredRoleIds for role in member.roles)
        return self._canSubmitRecruitment(member)

    async def _resolveReviewChannel(
        self,
        guild: discord.Guild,
        fallback: Optional[discord.abc.Messageable],
    ) -> Optional[discord.abc.Messageable]:
        channelId = int(getattr(config, "recruitmentChannelId", 0) or 0)
        if channelId > 0:
            # Use client-level channel resolution so review channels can live
            # outside the invoking guild (cross-server review setup).
            channel = self.bot.get_channel(channelId)
            if channel is None:
                channel = guild.get_channel(channelId)
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(channelId)
                except (discord.Forbidden, discord.NotFound, discord.HTTPException, discord.InvalidData):
                    channel = None
            if channel is not None:
                return channel
        return fallback

    async def _postRecruitmentForReview(
        self,
        *,
        guild: discord.Guild,
        fallbackChannel: Optional[discord.abc.Messageable],
        embed: discord.Embed,
        view: discord.ui.View,
        extraContent: Optional[str] = None,
        files: Optional[list[discord.File]] = None,
    ) -> Optional[discord.Message]:
        channel = await self._resolveReviewChannel(guild, fallbackChannel)
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return None
        mention = _reviewerMention()
        contentParts: list[str] = []
        if mention:
            contentParts.append(mention)
        if extraContent:
            contentParts.append(str(extraContent).strip())
        content = "\n".join(part for part in contentParts if part)
        if not content:
            content = None
        allowedMentions = discord.AllowedMentions(roles=True, users=True)
        try:
            return await channel.send(
                content=content,
                embed=embed,
                view=view,
                files=files or [],
                allowed_mentions=allowedMentions,
            )
        except (discord.Forbidden, discord.HTTPException):
            return None

    async def _collectTwoImageEvidenceMessage(
        self,
        *,
        channel: discord.abc.Messageable,
        userId: int,
        timeoutSec: float = 180.0,
    ) -> Optional[discord.Message]:
        channelId = getattr(channel, "id", None)
        if channelId is None:
            return None

        def check(message: discord.Message) -> bool:
            # We only accept the submitter's next message in this channel with
            # at least two image attachments.
            if message.author.id != userId:
                return False
            if message.channel.id != channelId:
                return False
            images = [att for att in message.attachments if _isImageAttachment(att)]
            return len(images) >= 2

        try:
            message = await self.bot.wait_for("message", check=check, timeout=timeoutSec)
        except asyncio.TimeoutError:
            return None
        return message

    async def _updatePatrolMessage(
        self,
        patrolId: int,
        *,
        message: Optional[discord.Message] = None,
    ) -> None:
        await self._groupPatrolEngine.updateClockinMessage(
            int(patrolId),
            viewFactory=lambda sessionId: GroupPatrolView(self, sessionId),
            message=message,
        )

    async def _deletePatrolClockinMessage(
        self,
        patrol: dict,
        *,
        message: Optional[discord.Message] = None,
    ) -> None:
        await self._groupPatrolEngine.deleteClockinMessage(
            patrol,
            message=message,
        )

    async def _refreshPatrolMessageFromInteraction(
        self,
        patrolId: int,
        interaction: discord.Interaction,
    ) -> None:
        if isinstance(interaction.message, discord.Message):
            await self._updatePatrolMessage(patrolId, message=interaction.message)
            return
        await self._updatePatrolMessage(patrolId)

    async def _isHost(self, interaction: discord.Interaction, patrol: dict) -> bool:
        return interaction.user.id == int(patrol.get("hostId") or 0)

    async def openGroupPatrolManage(self, interaction: discord.Interaction, patrolId: int) -> None:
        patrol = await self._groupPatrolEngine.getSession(int(patrolId))
        if not patrol:
            await interaction.response.send_message("This patrol session no longer exists.", ephemeral=True)
            return
        if not await self._isHost(interaction, patrol):
            await interaction.response.send_message(
                "Only the patrol host can manage attendees.",
                ephemeral=True,
            )
            return
        if str(patrol.get("status") or "").upper() != "OPEN":
            await interaction.response.send_message("This patrol is no longer open.", ephemeral=True)
            return
        attendees = await self._groupPatrolEngine.listAttendees(int(patrolId))
        if not attendees:
            await interaction.response.send_message("No attendees to remove.", ephemeral=True)
            return
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            GroupPatrolManageModal(self, patrolId),
        )

    async def handleGroupPatrolManage(
        self,
        interaction: discord.Interaction,
        patrolId: int,
        token: str,
    ) -> None:
        patrol = await self._groupPatrolEngine.getSession(int(patrolId))
        if not patrol:
            await interaction.response.send_message("This patrol session no longer exists.", ephemeral=True)
            return
        if not await self._isHost(interaction, patrol):
            await interaction.response.send_message(
                "Only the patrol host can manage attendees.",
                ephemeral=True,
            )
            return
        attendees = await self._groupPatrolEngine.listAttendees(int(patrolId))
        if not attendees:
            await interaction.response.send_message("No attendees to remove.", ephemeral=True)
            return

        targetUserId = resolveAttendeeUserIdFromToken(token, attendees)
        if not targetUserId:
            await interaction.response.send_message(
                "Could not match that attendee in this patrol.",
                ephemeral=True,
            )
            return

        await self._groupPatrolEngine.removeAttendee(int(patrolId), int(targetUserId))
        await interaction.response.send_message(
            f"Removed <@{targetUserId}> from this patrol.",
            ephemeral=True,
        )
        await self._refreshPatrolMessageFromInteraction(patrolId, interaction)

    async def handleGroupPatrolJoin(self, interaction: discord.Interaction, patrolId: int) -> None:
        if interaction.user.bot:
            await interaction.response.send_message("Bots cannot join patrols.", ephemeral=True)
            return
        patrol = await self._groupPatrolEngine.getSession(int(patrolId))
        if not patrol:
            await interaction.response.send_message("This patrol session no longer exists.", ephemeral=True)
            return
        if str(patrol.get("status") or "").upper() != "OPEN":
            await interaction.response.send_message("This patrol is no longer open.", ephemeral=True)
            return
        if interaction.user.id == int(patrol.get("hostId") or 0):
            await interaction.response.send_message(
                "You are the host of this patrol and cannot clock in as an attendee.",
                ephemeral=True,
            )
            return
        await self._groupPatrolEngine.addAttendee(int(patrolId), int(interaction.user.id))
        await interaction.response.send_message("You have been added to this patrol.", ephemeral=True)
        await self._refreshPatrolMessageFromInteraction(patrolId, interaction)

    async def handleGroupPatrolDelete(self, interaction: discord.Interaction, patrolId: int) -> None:
        patrol = await self._groupPatrolEngine.getSession(int(patrolId))
        if not patrol:
            await interaction.response.send_message("This patrol session no longer exists.", ephemeral=True)
            return
        if not await self._isHost(interaction, patrol):
            await interaction.response.send_message("Only the patrol host can delete this patrol.", ephemeral=True)
            return
        await self._groupPatrolEngine.updateSessionStatus(int(patrolId), "CANCELED")
        await interaction.response.send_message("Patrol deleted.", ephemeral=True)
        await self._refreshPatrolMessageFromInteraction(patrolId, interaction)

    async def openGroupPatrolFinish(self, interaction: discord.Interaction, patrolId: int) -> None:
        patrol = await self._groupPatrolEngine.getSession(int(patrolId))
        if not patrol:
            await interaction.response.send_message("This patrol session no longer exists.", ephemeral=True)
            return
        if not await self._isHost(interaction, patrol):
            await interaction.response.send_message("Only the patrol host can finish this patrol.", ephemeral=True)
            return
        if str(patrol.get("status") or "").upper() != "OPEN":
            await interaction.response.send_message("This patrol is no longer open.", ephemeral=True)
            return
        attendees = await self._groupPatrolEngine.listAttendees(int(patrolId))
        if not attendees:
            await interaction.response.send_message(
                "This patrol has no attendees yet.",
                ephemeral=True,
            )
            return
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            GroupPatrolFinishModal(self, patrolId),
        )

    async def handleGroupPatrolFinish(
        self,
        interaction: discord.Interaction,
        patrolId: int,
        durationMinutes: int,
    ) -> None:
        lock = self._patrolLocks.setdefault(patrolId, asyncio.Lock())
        async with lock:
            # Guard against double-finalize clicks while one reviewer is still
            # uploading evidence / posting the review message.
            patrol = await self._groupPatrolEngine.getSession(int(patrolId))
            if not patrol:
                await interaction.response.send_message("This patrol session no longer exists.", ephemeral=True)
                return
            if not await self._isHost(interaction, patrol):
                await interaction.response.send_message(
                    "Only the patrol host can finish this patrol.",
                    ephemeral=True,
                )
                return
            if str(patrol.get("status") or "").upper() != "OPEN":
                await interaction.response.send_message("This patrol is no longer open.", ephemeral=True)
                return

            attendees = await self._groupPatrolEngine.listAttendees(int(patrolId))
            participantIds = [int(row["userId"]) for row in attendees]
            if not participantIds:
                await interaction.response.send_message(
                    "Cannot finish this patrol with no attendees.",
                    ephemeral=True,
                )
                return

            channel = interaction.channel
            if channel is None:
                await interaction.response.send_message(
                    "Could not resolve the channel for screenshot upload.",
                    ephemeral=True,
                )
                return

            await interaction.response.send_message(
                "Upload two patrol screenshots in your next message in this channel within 3 minutes.",
                ephemeral=True,
            )
            # We reuse the evidence collector so solo/group flows behave the same.
            evidenceMessage = await self._collectTwoImageEvidenceMessage(
                channel=channel,
                userId=interaction.user.id,
            )
            if evidenceMessage is None:
                await interaction.followup.send(
                    "Timed out waiting for two image screenshots. Patrol is still open.",
                    ephemeral=True,
                )
                return

            imageUrls = _evidenceLinks(evidenceMessage.attachments)
            points = _patrolPoints(durationMinutes)
            submissionId = await recruitmentService.createRecruitmentTimeSubmission(
                guildId=int(patrol["guildId"]),
                channelId=int(patrol["channelId"]),
                submitterId=int(patrol["hostId"]),
                durationMinutes=int(durationMinutes),
                imageUrls=imageUrls,
                points=points,
                patrolType="group",
                participantUserIds=participantIds,
                evidenceMessageUrl=evidenceMessage.jump_url,
            )
            submission = await recruitmentService.getRecruitmentTimeSubmission(submissionId)
            if not submission:
                await interaction.followup.send(
                    "Failed to create patrol submission.",
                    ephemeral=True,
                )
                return

            embed = recruitmentRendering.buildRecruitmentTimeEmbed(submission)
            embed.add_field(
                name="Evidence Message",
                value=f"[Open message]({evidenceMessage.jump_url})",
                inline=False,
            )
            reviewView = RecruitmentReviewView(self, "time", submissionId)
            reviewMessage = await self._postRecruitmentForReview(
                guild=interaction.guild,
                fallbackChannel=interaction.channel,
                embed=embed,
                view=reviewView,
            )
            if not reviewMessage:
                await interaction.followup.send(
                    "Could not post this submission for review. Patrol remains open.",
                    ephemeral=True,
                )
                return

            await recruitmentService.setRecruitmentTimeMessageId(submissionId, reviewMessage.id)
            await self._groupPatrolEngine.updateSessionStatus(int(patrolId), "FINISHED")
            if isinstance(interaction.message, discord.Message):
                await self._deletePatrolClockinMessage(patrol, message=interaction.message)
            else:
                await self._deletePatrolClockinMessage(patrol)

            await interaction.followup.send(
                "Patrol finished and submitted for review.",
                ephemeral=True,
            )

    @app_commands.command(name="recruitment", description="Submit a recruitment log.")
    @app_commands.describe(
        recruit="Member you recruited.",
        image="Primary screenshot proof.",
        extra_image="Second screenshot proof.",
    )
    @app_commands.rename(extra_image="extra-image")
    async def recruitment(
        self,
        interaction: discord.Interaction,
        recruit: discord.Member,
        image: discord.Attachment,
        extra_image: discord.Attachment,
    ) -> None:
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message(
                "This command can only be used in a server channel.",
                ephemeral=True,
            )
            return
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "This command can only be used in a server channel.",
                ephemeral=True,
            )
            return
        if not self._canSubmitRecruitment(interaction.user):
            await interaction.response.send_message(
                "You do not have permission to submit recruitment logs.",
                ephemeral=True,
            )
            return

        attachments = [image, extra_image]
        imageUrls = _evidenceLinks(attachments)
        # Two screenshots are required to reduce ambiguity during review.
        if len(imageUrls) < 2:
            await interaction.response.send_message(
                "Two valid image attachments are required.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        basePoints = int(getattr(config, "recruitmentPointsBase", 2) or 2)
        submissionId = await recruitmentService.createRecruitmentSubmission(
            guildId=interaction.guild.id,
            channelId=interaction.channel.id,
            submitterId=interaction.user.id,
            recruitUserId=recruit.id,
            passedOrientation=False,
            imageUrls=imageUrls,
            points=basePoints,
        )
        submission = await recruitmentService.getRecruitmentSubmission(submissionId)
        if not submission:
            await interaction.followup.send(
                "Failed to create recruitment submission.",
                ephemeral=True,
            )
            return

        embed = recruitmentRendering.buildRecruitmentEmbed(submission)
        imageFiles: list[discord.File] = []
        for attachment in attachments:
            if not _isImageAttachment(attachment):
                continue
            try:
                imageFiles.append(await attachment.to_file())
            except (discord.HTTPException, OSError):
                continue

        view = RecruitmentReviewView(self, "recruitment", submissionId)
        reviewMessage = await self._postRecruitmentForReview(
            guild=interaction.guild,
            fallbackChannel=interaction.channel,
            embed=embed,
            view=view,
            extraContent=None if imageFiles else "\n".join(imageUrls),
            files=imageFiles,
        )
        if not reviewMessage:
            await interaction.followup.send(
                "Submission saved, but I could not post it for review.",
                ephemeral=True,
            )
            return

        await recruitmentService.setRecruitmentMessageId(submissionId, reviewMessage.id)
        await interaction.followup.send(
            "Submitted recruitment log.",
            ephemeral=True,
        )

    async def _submitSoloPatrolForReview(
        self,
        interaction: discord.Interaction,
        *,
        durationMinutes: int,
        imageUrls: list[str],
        evidenceMessageUrl: Optional[str] = None,
    ) -> None:
        points = _patrolPoints(int(durationMinutes))
        submissionId = await recruitmentService.createRecruitmentTimeSubmission(
            guildId=interaction.guild.id,
            channelId=interaction.channel.id,
            submitterId=interaction.user.id,
            durationMinutes=int(durationMinutes),
            imageUrls=imageUrls,
            points=points,
            patrolType="solo",
            evidenceMessageUrl=evidenceMessageUrl,
        )
        submission = await recruitmentService.getRecruitmentTimeSubmission(submissionId)
        if not submission:
            await interaction.followup.send(
                "Failed to create patrol submission.",
                ephemeral=True,
            )
            return

        embed = recruitmentRendering.buildRecruitmentTimeEmbed(submission)
        if evidenceMessageUrl:
            # Prefer the source message link so reviewers can inspect original
            # attachments/context directly.
            embed.add_field(
                name="Evidence Message",
                value=f"[Open message]({evidenceMessageUrl})",
                inline=False,
            )
        else:
            embed.add_field(
                name="Evidence",
                value="\n".join(f"[Image {index + 1}]({url})" for index, url in enumerate(imageUrls)),
                inline=False,
            )
        view = RecruitmentReviewView(self, "time", submissionId)
        reviewMessage = await self._postRecruitmentForReview(
            guild=interaction.guild,
            fallbackChannel=interaction.channel,
            embed=embed,
            view=view,
        )
        if not reviewMessage:
            await interaction.followup.send(
                "Submission saved, but I could not post it for review.",
                ephemeral=True,
            )
            return
        await recruitmentService.setRecruitmentTimeMessageId(submissionId, reviewMessage.id)
        await interaction.followup.send(
            "Submitted solo patrol log.",
            ephemeral=True,
        )

    async def _startGroupPatrolClockin(self, interaction: discord.Interaction) -> None:
        patrolId = await self._groupPatrolEngine.createSession(
            guildId=interaction.guild.id,
            channelId=interaction.channel.id,
            hostId=interaction.user.id,
        )
        patrol = await self._groupPatrolEngine.getSession(int(patrolId))
        if not patrol:
            await interaction.followup.send(
                "Could not create patrol clock-in.",
                ephemeral=True,
            )
            return
        embed = self._groupPatrolAdapter.buildEmbed(patrol, [])
        view = GroupPatrolView(self, patrolId)
        try:
            message = await interaction.channel.send(embed=embed, view=view)
        except (discord.Forbidden, discord.HTTPException):
            await interaction.followup.send(
                "Could not create the patrol clock-in message in this channel.",
                ephemeral=True,
            )
            return
        await self._groupPatrolEngine.setSessionMessageId(int(patrolId), int(message.id))
        await interaction.followup.send(
            "Group patrol clock-in created.",
            ephemeral=True,
        )

    async def handleSoloPatrolDetails(
        self,
        interaction: discord.Interaction,
        durationMinutes: int,
    ) -> None:
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message(
                "This command can only be used in a server channel.",
                ephemeral=True,
            )
            return
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "This command can only be used in a server channel.",
                ephemeral=True,
            )
            return
        if not self._canSubmitRecruitment(interaction.user):
            await interaction.response.send_message(
                "You do not have permission to submit patrol logs.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            "Upload two patrol screenshots in your next message in this channel within 3 minutes.",
            ephemeral=True,
        )
        evidenceMessage = await self._collectTwoImageEvidenceMessage(
            channel=interaction.channel,
            userId=interaction.user.id,
        )
        if evidenceMessage is None:
            await interaction.followup.send(
                "Timed out waiting for two image screenshots. Submit the command again when ready.",
                ephemeral=True,
            )
            return

        imageUrls = _evidenceLinks(evidenceMessage.attachments)
        if len(imageUrls) < 2:
            await interaction.followup.send(
                "Two image attachments are required for patrol logs.",
                ephemeral=True,
            )
            return
        await self._submitSoloPatrolForReview(
            interaction,
            durationMinutes=durationMinutes,
            imageUrls=imageUrls,
            evidenceMessageUrl=evidenceMessage.jump_url,
        )

    @app_commands.command(name="recruitment-patrol", description="Create a solo or group recruitment patrol.")
    @app_commands.describe(patrol_type="Select patrol type.")
    @app_commands.choices(patrol_type=patrolTypeChoices)
    @app_commands.rename(patrol_type="type")
    async def recruitmentPatrol(
        self,
        interaction: discord.Interaction,
        patrol_type: str,
    ) -> None:
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message(
                "This command can only be used in a server channel.",
                ephemeral=True,
            )
            return
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "This command can only be used in a server channel.",
                ephemeral=True,
            )
            return

        patrolType = str(patrol_type or "").strip().lower()
        if patrolType == "group":
            if not self._canHostGroupPatrol(interaction.user):
                await interaction.response.send_message(
                    "You do not have permission to host group patrol clock-ins.",
                    ephemeral=True,
                )
                return
            await interaction.response.defer(ephemeral=True, thinking=True)
            await self._startGroupPatrolClockin(interaction)
            return

        if patrolType != "solo":
            await interaction.response.send_message(
                "Invalid patrol type. Please select solo or group.",
                ephemeral=True,
            )
            return
        if not self._canSubmitRecruitment(interaction.user):
            await interaction.response.send_message(
                "You do not have permission to submit patrol logs.",
                ephemeral=True,
            )
            return

        await interactionRuntime.safeInteractionSendModal(
            interaction,
            SoloPatrolDetailsModal(self),
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(RecruitmentCog(bot))

