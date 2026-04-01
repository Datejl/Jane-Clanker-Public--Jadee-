from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands

import config
from features.operations.curfew import service as curfewService
from runtime import cogGuards as runtimeCogGuards
from runtime import interaction as interactionRuntime
from runtime import timezones as timezoneRuntime
from runtime import viewBases as runtimeViewBases

log = logging.getLogger(__name__)
_userMentionRegex = re.compile(r"^<@!?(\d+)>$")


def _normalizeTimezoneName(value: str) -> str:
    return str(value or "").strip()


def _timeoutWindowEndUtc(nowUtc: datetime, timezoneName: str) -> datetime | None:
    try:
        tz, _ = timezoneRuntime.resolveTimezoneToken(timezoneName, allowIana=True)
    except ValueError:
        return None

    localNow = nowUtc.astimezone(tz)
    if localNow.hour >= 8:
        return None
    localEnd = localNow.replace(hour=8, minute=0, second=0, microsecond=0)
    return localEnd.astimezone(timezone.utc)


class CurfewAddModal(discord.ui.Modal):
    userInput = discord.ui.TextInput(
        label="User",
        placeholder="Mention, user ID, username, or display name",
        required=True,
        max_length=100,
    )
    timezoneInput = discord.ui.TextInput(
        label="Timezone",
        placeholder="Example: CST, EST, UTC+2, America/Chicago",
        required=True,
        max_length=100,
    )

    def __init__(self, *, cog: "CurfewCog", openerId: int, panelMessageId: int) -> None:
        super().__init__(title="Add / Update Curfew", timeout=300)
        self.cog = cog
        self.openerId = int(openerId)
        self.panelMessageId = int(panelMessageId)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await self.cog._requireAdmin(interaction):
            return
        target = await self.cog._resolveMemberFromInput(interaction.guild, str(self.userInput.value or ""))
        if target is None:
            await self.cog._safeReply(interaction, "I couldn't find that server member.")
            return

        timezoneName = _normalizeTimezoneName(str(self.timezoneInput.value or ""))
        try:
            _, timezoneLabel = timezoneRuntime.resolveTimezoneToken(timezoneName, allowIana=False)
        except ValueError as exc:
            await self.cog._safeReply(interaction, str(exc))
            return

        await curfewService.upsertCurfewTarget(
            guildId=int(interaction.guild_id or 0),
            userId=int(target.id),
            timezoneName=timezoneLabel,
            addedBy=int(interaction.user.id),
        )
        appliedNow = await self.cog._enforceSingleTarget(
            int(interaction.guild_id or 0),
            int(target.id),
            timezoneLabel,
        )
        nowText = " Curfew timeout was applied immediately." if appliedNow else ""
        await self.cog._safeReply(
            interaction,
            f"Curfew enabled for {target.mention} in `{timezoneLabel}`.{nowText}",
        )
        await self.cog.refreshPanelMessageById(interaction, messageId=self.panelMessageId)


class CurfewRemoveModal(discord.ui.Modal):
    userInput = discord.ui.TextInput(
        label="User",
        placeholder="Mention, user ID, username, or display name",
        required=True,
        max_length=100,
    )

    def __init__(self, *, cog: "CurfewCog", openerId: int, panelMessageId: int) -> None:
        super().__init__(title="Remove Curfew", timeout=300)
        self.cog = cog
        self.openerId = int(openerId)
        self.panelMessageId = int(panelMessageId)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await self.cog._requireAdmin(interaction):
            return
        target = await self.cog._resolveMemberFromInput(interaction.guild, str(self.userInput.value or ""))
        if target is None:
            await self.cog._safeReply(interaction, "I couldn't find that server member.")
            return
        await curfewService.disableCurfewTarget(
            guildId=int(interaction.guild_id or 0),
            userId=int(target.id),
        )
        await self.cog._safeReply(
            interaction,
            f"Curfew disabled for {target.mention}.",
        )
        await self.cog.refreshPanelMessageById(interaction, messageId=self.panelMessageId)


class CurfewPanelView(runtimeViewBases.OwnerLockedView):
    def __init__(self, *, cog: "CurfewCog", openerId: int) -> None:
        super().__init__(
            openerId=openerId,
            timeout=900,
            ownerMessage="This curfew panel belongs to someone else.",
        )
        self.cog = cog
        self.noticeText = ""

    async def refresh(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            return
        embed = await self.cog.buildCurfewPanelEmbed(guild, noticeText=self.noticeText)
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=embed,
            view=self,
        )

    @discord.ui.button(label="Add / Update", style=discord.ButtonStyle.success, row=0)
    async def addBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            CurfewAddModal(
                cog=self.cog,
                openerId=self.openerId,
                panelMessageId=int(getattr(interaction.message, "id", 0) or 0),
            ),
        )

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger, row=0)
    async def removeBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            CurfewRemoveModal(
                cog=self.cog,
                openerId=self.openerId,
                panelMessageId=int(getattr(interaction.message, "id", 0) or 0),
            ),
        )

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=0)
    async def refreshBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.noticeText = "Panel refreshed."
        await self.refresh(interaction)


class CurfewCog(runtimeCogGuards.InteractionGuardMixin, commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._curfewTask: asyncio.Task | None = None

    async def cog_load(self) -> None:
        if self._curfewTask is None or self._curfewTask.done():
            self._curfewTask = asyncio.create_task(self._runCurfewLoop())

    def cog_unload(self) -> None:
        if self._curfewTask and not self._curfewTask.done():
            self._curfewTask.cancel()
        self._curfewTask = None

    async def _getMember(self, guild: discord.Guild, userId: int) -> discord.Member | None:
        member = guild.get_member(int(userId))
        if member is not None:
            return member
        try:
            return await guild.fetch_member(int(userId))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    async def _resolveMemberFromInput(self, guild: discord.Guild | None, rawValue: str) -> discord.Member | None:
        if guild is None:
            return None
        value = str(rawValue or "").strip()
        if not value:
            return None
        mentionMatch = _userMentionRegex.match(value)
        if mentionMatch:
            return await self._getMember(guild, int(mentionMatch.group(1)))
        if value.isdigit():
            return await self._getMember(guild, int(value))

        lowered = value.casefold()
        exactDisplay = discord.utils.find(lambda member: str(member.display_name).casefold() == lowered, guild.members)
        if exactDisplay is not None:
            return exactDisplay
        exactName = discord.utils.find(lambda member: str(member.name).casefold() == lowered, guild.members)
        if exactName is not None:
            return exactName
        partial = discord.utils.find(
            lambda member: lowered in str(member.display_name).casefold() or lowered in str(member.name).casefold(),
            guild.members,
        )
        return partial

    async def _enforceSingleTarget(
        self,
        guildId: int,
        userId: int,
        timezoneName: str,
    ) -> bool:
        guild = self.bot.get_guild(int(guildId))
        if guild is None:
            return False
        member = await self._getMember(guild, int(userId))
        if member is None or member.bot:
            return False

        me = guild.me
        if me is None or not guild.me.guild_permissions.moderate_members:
            return False

        nowUtc = datetime.now(timezone.utc)
        windowEndUtc = _timeoutWindowEndUtc(nowUtc, timezoneName)
        if windowEndUtc is None:
            return False

        currentUntil = member.timed_out_until
        minimumRemaining = nowUtc + timedelta(minutes=1)
        if currentUntil is not None and currentUntil >= minimumRemaining and currentUntil >= windowEndUtc - timedelta(minutes=1):
            return False

        try:
            await member.edit(
                timed_out_until=windowEndUtc,
                reason=f"Daily curfew auto-timeout ({timezoneName})",
            )
        except (discord.Forbidden, discord.HTTPException):
            return False

        await curfewService.setCurfewAppliedAt(
            guildId=int(guildId),
            userId=int(userId),
            appliedAtIso=nowUtc.isoformat(),
        )
        return True

    async def _enforceCurfewOnce(self) -> tuple[int, int]:
        rows = await curfewService.listActiveCurfewTargets()
        checked = 0
        applied = 0
        for row in rows:
            guildId = int(row.get("guildId") or 0)
            userId = int(row.get("userId") or 0)
            timezoneName = str(row.get("timezone") or "").strip()
            if guildId <= 0 or userId <= 0 or not timezoneName:
                continue
            checked += 1
            try:
                if await self._enforceSingleTarget(guildId, userId, timezoneName):
                    applied += 1
            except Exception:
                log.exception(
                    "Curfew enforcement failed for guild=%s user=%s timezone=%s",
                    guildId,
                    userId,
                    timezoneName,
                )
        return checked, applied

    async def _runCurfewLoop(self) -> None:
        await self.bot.wait_until_ready()
        intervalSec = max(30, int(getattr(config, "curfewCheckIntervalSec", 60) or 60))
        while not self.bot.is_closed():
            try:
                checked, applied = await self._enforceCurfewOnce()
                if checked > 0 and applied > 0:
                    log.info("Curfew enforcement: checked=%d, applied=%d", checked, applied)
            except Exception:
                log.exception("Curfew enforcement loop error.")
            await asyncio.sleep(intervalSec)

    async def _requireAdmin(self, interaction: discord.Interaction) -> bool:
        return await self._requireAdministrator(interaction) is not None

    async def buildCurfewPanelEmbed(self, guild: discord.Guild, *, noticeText: str = "") -> discord.Embed:
        rows = await curfewService.listGuildCurfewTargets(
            guildId=int(guild.id),
            includeDisabled=False,
        )
        embed = discord.Embed(
            title="Curfew Panel",
            color=discord.Color.orange(),
            description="Manage daily curfew auto-timeouts from one panel.",
        )
        embed.add_field(name="Status", value="Enabled" if rows else "No active targets", inline=True)
        embed.add_field(name="Active Targets", value=str(len(rows)), inline=True)
        intervalSec = max(30, int(getattr(config, "curfewCheckIntervalSec", 60) or 60))
        embed.add_field(name="Check Interval", value=f"{intervalSec}s", inline=True)

        if rows:
            lines: list[str] = []
            for row in rows[:20]:
                userId = int(row.get("userId") or 0)
                timezoneName = str(row.get("timezone") or "unknown")
                lastAppliedAt = str(row.get("lastAppliedAt") or "").strip()
                appliedText = f" | last: `{lastAppliedAt}`" if lastAppliedAt else ""
                lines.append(f"- <@{userId}> -> `{timezoneName}`{appliedText}")
            if len(rows) > 20:
                lines.append(f"... and {len(rows) - 20} more.")
            embed.add_field(name="Tracked Users", value="\n".join(lines), inline=False)
        else:
            embed.add_field(
                name="Tracked Users",
                value="No active curfew targets in this server.",
                inline=False,
            )

        embed.add_field(
            name="Usage",
            value="Use `Add / Update` to assign a timezone, or `Remove` to clear a user from curfew.",
            inline=False,
        )
        if noticeText:
            embed.add_field(name="Last Action", value=noticeText, inline=False)
        return embed

    async def refreshPanelMessageById(self, interaction: discord.Interaction, *, messageId: int) -> None:
        guild = interaction.guild
        channel = interaction.channel
        if guild is None or not isinstance(channel, (discord.TextChannel, discord.Thread)) or int(messageId or 0) <= 0:
            return
        try:
            message = await channel.fetch_message(int(messageId))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return
        view = CurfewPanelView(cog=self, openerId=int(getattr(interaction.user, "id", 0) or 0))
        embed = await self.buildCurfewPanelEmbed(guild)
        await interactionRuntime.safeMessageEdit(message, embed=embed, view=view)

    @app_commands.command(name="curfew", description="Open the curfew control panel.")
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    async def curfewPanel(self, interaction: discord.Interaction) -> None:
        if not await self._requireAdmin(interaction):
            return
        view = CurfewPanelView(cog=self, openerId=int(interaction.user.id))
        embed = await self.buildCurfewPanelEmbed(interaction.guild)
        await interactionRuntime.safeInteractionReply(
            interaction,
            embed=embed,
            view=view,
            ephemeral=True,
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CurfewCog(bot))
