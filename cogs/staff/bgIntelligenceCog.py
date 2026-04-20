from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

import discord
from discord import app_commands
from discord.ext import commands

import config
from features.staff.bgIntelligence import rendering, scoring, service
from runtime import interaction as interactionRuntime
from runtime import permissions as runtimePermissions

log = logging.getLogger(__name__)

ProgressUpdater = Callable[[str], Awaitable[bool]]


BG_INTEL_SECTIONS: tuple[tuple[str, str], ...] = (
    ("overview", "Overview"),
    ("scan", "Detection Summary"),
    ("profile", "Profile Information"),
    ("connections", "Connections"),
    ("groups", "Groups"),
    ("inventory", "Inventory"),
    ("gamepasses", "Gamepasses"),
    ("games", "Favorites"),
    ("outfits", "Outfits"),
    ("badges", "Badges"),
    ("external", "Clanning Record"),
)


class BgIntelSectionSelect(discord.ui.Select):
    def __init__(self, selectedSection: str = "overview") -> None:
        super().__init__(
            placeholder="Expand a BG intelligence section",
            min_values=1,
            max_values=1,
            row=0,
            options=[
                discord.SelectOption(label=label, value=section, default=section == selectedSection)
                for section, label in BG_INTEL_SECTIONS
            ],
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, BgIntelDetailsView):
            return
        section = str(self.values[0] if self.values else "overview")
        await view.showSection(interaction, section)


class BgIntelDetailsView(discord.ui.View):
    def __init__(
        self,
        *,
        ownerId: int,
        report,
        riskScore: scoring.RiskScore,
        reportId: int,
    ) -> None:
        super().__init__(timeout=900)
        self.ownerId = int(ownerId)
        self.report = report
        self.riskScore = riskScore
        self.reportId = int(reportId or 0)
        self.currentSection = "overview"
        self.add_item(BgIntelSectionSelect("overview"))
        self._syncSelectedSection("overview")

    def _badgeGraphFilename(self) -> str:
        reportId = self.reportId if self.reportId > 0 else 0
        robloxUserId = int(getattr(self.report, "robloxUserId", 0) or 0)
        suffix = reportId or robloxUserId or int(self.ownerId)
        return f"bg-intel-badges-{suffix}.png"

    def _applyBadgeGraph(self, embed: discord.Embed, section: str) -> discord.File | None:
        normalizedSection = str(section or "overview").strip().lower()
        if normalizedSection not in {"overview", "badges"}:
            return None
        return rendering.applyBadgeTimelineGraph(
            embed,
            self.report,
            filename=self._badgeGraphFilename(),
        )

    def _buildPublicPayload(self, section: str) -> tuple[discord.Embed, list[discord.File]]:
        normalizedSection = str(section or "overview").strip().lower()
        embed = rendering.buildPublicSectionEmbed(
            self.report,
            score=self.riskScore,
            section=normalizedSection,
            reportId=self.reportId if self.reportId > 0 else None,
        )
        graphFile = self._applyBadgeGraph(embed, normalizedSection)
        return embed, [graphFile] if graphFile is not None else []

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) == int(self.ownerId):
            return True
        await interactionRuntime.safeInteractionReply(
            interaction,
            content="This BG intelligence panel belongs to the reviewer who ran the scan.",
            ephemeral=True,
        )
        return False

    def _syncSelectedSection(self, section: str) -> None:
        normalizedSection = str(section or "overview").strip().lower()
        validSections = {item[0] for item in BG_INTEL_SECTIONS}
        if normalizedSection not in validSections:
            normalizedSection = "overview"
        self.currentSection = normalizedSection
        for child in self.children:
            if isinstance(child, BgIntelSectionSelect):
                for option in child.options:
                    option.default = option.value == normalizedSection

    async def showSection(self, interaction: discord.Interaction, section: str) -> None:
        normalizedSection = str(section or "overview").strip().lower()
        self._syncSelectedSection(normalizedSection)
        embed, attachments = self._buildPublicPayload(normalizedSection)
        try:
            await interaction.response.edit_message(embed=embed, view=self, attachments=attachments)
            return
        except (discord.NotFound, discord.HTTPException):
            message = getattr(interaction, "message", None)
            if message is not None:
                fallbackEmbed, fallbackAttachments = self._buildPublicPayload(normalizedSection)
                edited = await interactionRuntime.safeMessageEdit(
                    message,
                    embed=fallbackEmbed,
                    view=self,
                    attachments=fallbackAttachments,
                )
                if edited:
                    await interactionRuntime.safeInteractionDefer(interaction, ephemeral=True)
                    return
        await interactionRuntime.safeInteractionReply(
            interaction,
            content="I couldn't expand that section on the webhook message.",
            ephemeral=True,
            allowedMentions=discord.AllowedMentions.none(),
        )


class BgIntelligenceCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @staticmethod
    def _bgIntelProgressContent(status: str, *, targetLabel: str) -> str:
        cleanStatus = str(status or "Starting scan...").strip() or "Starting scan..."
        cleanTarget = str(targetLabel or "selected user").strip() or "selected user"
        cleanTarget = cleanTarget.replace("`", "'")[:80]
        return (
            "Jane is running the background intel scan.\n"
            f"Target: `{cleanTarget}`\n"
            f"Status: {cleanStatus}\n"
            "Large badge or inventory histories can take a moment."
        )

    async def _editBgIntelStatus(
        self,
        interaction: discord.Interaction,
        status: str,
        *,
        targetLabel: str,
    ) -> bool:
        try:
            await interaction.edit_original_response(
                content=self._bgIntelProgressContent(status, targetLabel=targetLabel),
            )
            return True
        except (discord.NotFound, discord.HTTPException, AttributeError, TypeError):
            return False

    async def _finishBgIntelStatus(
        self,
        interaction: discord.Interaction,
        message: str,
    ) -> bool:
        try:
            await interaction.edit_original_response(content=message)
            return True
        except (discord.NotFound, discord.HTTPException, AttributeError, TypeError):
            return await interactionRuntime.safeInteractionReply(
                interaction,
                content=message,
                ephemeral=True,
                allowedMentions=discord.AllowedMentions.none(),
            )

    async def _sendBgIntelMessage(
        self,
        interaction: discord.Interaction,
        *,
        embed: discord.Embed,
        view: BgIntelDetailsView,
        files: list[discord.File] | None = None,
    ) -> None:
        channel = getattr(interaction, "channel", None)
        sentMessage = None
        if channel is not None:
            payload = {
                "embed": embed,
                "view": view,
                "allowed_mentions": discord.AllowedMentions.none(),
            }
            if files:
                payload["files"] = files
            sentMessage = await interactionRuntime.safeChannelSend(channel, **payload)
        if sentMessage is not None:
            await self._finishBgIntelStatus(
                interaction,
                "Background-check overview posted.",
            )
            return

        await self._finishBgIntelStatus(
            interaction,
            "I couldn't post the background-check overview in this channel.",
        )

    async def _safeEphemeral(self, interaction: discord.Interaction, message: str) -> None:
        await interactionRuntime.safeInteractionReply(
            interaction,
            content=message,
            ephemeral=True,
        )

    def _canUse(self, member: discord.Member) -> bool:
        extraReviewerRoleIds: set[int] = set()
        for rawRoleId in list(getattr(config, "bgCheckMinorReviewRoleIds", []) or []):
            try:
                parsedRoleId = int(rawRoleId)
            except (TypeError, ValueError):
                continue
            if parsedRoleId > 0:
                extraReviewerRoleIds.add(parsedRoleId)
        try:
            primaryMinorRoleId = int(getattr(config, "bgCheckMinorReviewRoleId", 0) or 0)
        except (TypeError, ValueError):
            primaryMinorRoleId = 0
        if primaryMinorRoleId > 0:
            extraReviewerRoleIds.add(primaryMinorRoleId)
        return (
            runtimePermissions.hasBgCheckCertifiedRole(member)
            or runtimePermissions.hasAdminOrManageGuild(member)
            or any(int(role.id) in extraReviewerRoleIds for role in list(member.roles or []))
        )

    @staticmethod
    def _parseDiscordId(rawValue: str | None) -> Optional[int]:
        clean = str(rawValue or "").strip()
        if not clean:
            return None
        if clean.startswith("<@") and clean.endswith(">"):
            clean = clean[2:-1].lstrip("!")
        if not clean.isdigit():
            return None
        parsed = int(clean)
        return parsed if parsed > 0 else None

    async def _fetchGuildMemberById(self, guild: discord.Guild, discordUserId: int) -> discord.Member | None:
        member = guild.get_member(int(discordUserId))
        if member is not None:
            return member
        try:
            return await guild.fetch_member(int(discordUserId))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    def _mainGuildId(self) -> int:
        try:
            mainGuildId = int(getattr(config, "serverId", 0) or 0)
        except (TypeError, ValueError):
            return 0
        return mainGuildId if mainGuildId > 0 else 0

    def _mainGuild(self) -> discord.Guild | None:
        mainGuildId = self._mainGuildId()
        if mainGuildId <= 0:
            return None
        return self.bot.get_guild(mainGuildId)

    async def _fetchMainGuildMemberById(
        self,
        discordUserId: int,
        *,
        currentGuild: discord.Guild,
    ) -> discord.Member | None:
        mainGuildId = self._mainGuildId()
        if mainGuildId <= 0 or int(mainGuildId) == int(currentGuild.id):
            return None
        mainGuild = self._mainGuild()
        if mainGuild is None:
            try:
                mainGuild = await self.bot.fetch_guild(mainGuildId)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                return None
        return await self._fetchGuildMemberById(mainGuild, int(discordUserId))

    @app_commands.command(
        name="bg-intel",
        description="Run Jane's standalone Roblox background intelligence report.",
    )
    @app_commands.guild_only()
    @app_commands.describe(
        member="The Discord member to analyze. Optional if a Discord ID or Roblox username is supplied.",
        discord_id="Optional Discord user ID. Paste as plain ID or mention.",
        roblox_username="Optional Roblox username. Can be used without a Discord member.",
        notify_private_inventory="DM the user if their inventory is private or hidden. Defaults to off.",
    )
    async def bgIntel(
        self,
        interaction: discord.Interaction,
        member: discord.Member | None = None,
        discord_id: str | None = None,
        roblox_username: str | None = None,
        notify_private_inventory: bool = False,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await self._safeEphemeral(interaction, "This command can only be used inside a server.")
        if not self._canUse(interaction.user):
            return await self._safeEphemeral(interaction, "You do not have permission to run BG intelligence scans.")
        cleanRobloxUsername = str(roblox_username or "").strip()
        cleanDiscordId = str(discord_id or "").strip()
        parsedDiscordId = self._parseDiscordId(cleanDiscordId)
        if cleanDiscordId and parsedDiscordId is None:
            return await self._safeEphemeral(interaction, "Please provide a valid Discord user ID or mention.")
        if member is not None and parsedDiscordId is not None:
            return await self._safeEphemeral(interaction, "Please provide either a Discord member or Discord ID, not both.")
        if member is None and parsedDiscordId is None and not cleanRobloxUsername:
            return await self._safeEphemeral(
                interaction,
                "Please provide a Discord member, a Discord ID, or a Roblox username.",
            )

        await interactionRuntime.safeInteractionDefer(
            interaction,
            ephemeral=True,
            thinking=True,
        )

        targetLabel = str(
            getattr(member, "display_name", None)
            or cleanRobloxUsername
            or parsedDiscordId
            or "selected user"
        )
        progressUpdater: ProgressUpdater = lambda status: self._editBgIntelStatus(
            interaction,
            status,
            targetLabel=targetLabel,
        )
        await progressUpdater("Checking Discord membership and main-server lookup...")

        targetMember = member
        if targetMember is None and parsedDiscordId is not None:
            targetMember = await self._fetchGuildMemberById(interaction.guild, parsedDiscordId)
        targetDiscordId = int(getattr(targetMember, "id", 0) or parsedDiscordId or 0)
        mainGuildMember = (
            await self._fetchMainGuildMemberById(targetDiscordId, currentGuild=interaction.guild)
            if targetDiscordId > 0
            else None
        )
        scanMember = targetMember or mainGuildMember
        roverMember = mainGuildMember or targetMember
        roverGuildId = int(getattr(getattr(roverMember, "guild", None), "id", 0) or 0) if roverMember is not None else None
        if member is not None and member.bot:
            return await self._finishBgIntelStatus(
                interaction,
                "That is a bot account. Jane is not emotionally prepared to background-check the appliances.",
            )
        if targetMember is not None and targetMember.bot:
            return await self._finishBgIntelStatus(
                interaction,
                "That is a bot account. Jane is not emotionally prepared to background-check the appliances.",
            )
        if mainGuildMember is not None and mainGuildMember.bot:
            return await self._finishBgIntelStatus(
                interaction,
                "That is a bot account. Jane is not emotionally prepared to background-check the appliances.",
            )

        try:
            if scanMember is not None:
                report = await service.buildReport(
                    scanMember,
                    guild=interaction.guild,
                    reviewBucketOverride="adult",
                    roverGuildId=roverGuildId,
                    robloxUsernameOverride=cleanRobloxUsername or None,
                    notifyPrivateInventory=bool(notify_private_inventory),
                    reviewer=interaction.user,
                    configModule=config,
                    progressCallback=progressUpdater,
                )
            elif parsedDiscordId is not None:
                report = await service.buildReportForDiscordId(
                    guild=interaction.guild,
                    discordUserId=parsedDiscordId,
                    displayMember=mainGuildMember,
                    roverGuildId=roverGuildId,
                    robloxUsernameOverride=cleanRobloxUsername or None,
                    reviewBucketOverride="adult",
                    configModule=config,
                    progressCallback=progressUpdater,
                )
            else:
                report = await service.buildReportForRobloxIdentity(
                    guild=interaction.guild,
                    robloxUsername=cleanRobloxUsername or None,
                    reviewBucketOverride="adult",
                    configModule=config,
                    progressCallback=progressUpdater,
                )
            await progressUpdater("Scoring the completed scan...")
            riskScore = scoring.scoreReport(report, configModule=config)
            try:
                await progressUpdater("Saving the audit record...")
                channelId = int(getattr(getattr(interaction, "channel", None), "id", 0) or 0)
                reportId = await service.recordReport(
                    guildId=int(interaction.guild.id),
                    channelId=channelId,
                    reviewerId=int(interaction.user.id),
                    report=report,
                    riskScore=riskScore,
                )
            except Exception:
                reportId = 0
                log.exception(
                    "BG intelligence audit insert failed for guild=%s target=%s.",
                    int(interaction.guild.id),
                    int(report.discordUserId or 0),
                )
        except Exception:
            log.exception(
                "BG intelligence scan failed for guild=%s target=%s.",
                int(interaction.guild.id),
                int(scanMember.id) if scanMember is not None else int(parsedDiscordId or 0),
            )
            return await self._finishBgIntelStatus(
                interaction,
                "BG intelligence scan failed internally. Check Jane's logs before trusting the result.",
            )

        await progressUpdater("Rendering the overview...")
        view = BgIntelDetailsView(
            ownerId=int(interaction.user.id),
            report=report,
            riskScore=riskScore,
            reportId=reportId,
        )
        embed, files = view._buildPublicPayload("overview")
        await progressUpdater("Posting the overview...")
        await self._sendBgIntelMessage(interaction, embed=embed, view=view, files=files)


async def setup(bot: commands.Bot):
    await bot.add_cog(BgIntelligenceCog(bot))
