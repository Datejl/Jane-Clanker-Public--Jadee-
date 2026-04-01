from __future__ import annotations

import asyncio
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands

from features.staff.applications import service as applicationsService
from features.staff.applications.panel import ApplicationsPanelView
from features.staff.applications.cogMixins.configMixin import ApplicationsConfigMixin
from features.staff.applications.cogMixins.flowMixin import ApplicationsFlowMixin
from features.staff.applications.cogMixins.opsMixin import ApplicationsOpsMixin
from runtime import interaction as interactionRuntime


class ApplicationsCog(ApplicationsConfigMixin, ApplicationsFlowMixin, ApplicationsOpsMixin, commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.divisions: dict[str, dict[str, Any]] = {}
        self.divisionOrder: list[str] = []
        self.appLocks: dict[int, asyncio.Lock] = {}
        self.divisionsConfigPath: Optional[str] = None
        self.loadDivisionConfig()

    @app_commands.command(name="applications", description="Open the applications manager panel.")
    async def applicationsPanel(self, interaction: discord.Interaction) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await self.safeReply(interaction, "This command can only be used in a server.")
        if not self.canUseApplicationsPanel(interaction.user):
            return await self.safeReply(interaction, "You are not authorized to use the applications manager panel.")

        embed = discord.Embed(
            title="Applications Manager",
            description=(
                "Use the panel buttons below to manage applications.\n"
                "This combines hub posting and staff queue tools."
            ),
            color=discord.Color.blurple(),
        )
        await interactionRuntime.safeInteractionReply(
            interaction,
            embed=embed,
            view=ApplicationsPanelView(
                self,
                canBulkClose=self.isServerAdministrator(interaction.user),
            ),
            ephemeral=True,
        )

    @commands.command(name="applications", hidden=True)
    async def applicationsTextCommand(
        self,
        ctx: commands.Context,
        divisionKey: Optional[str] = None,
        action: Optional[str] = None,
    ) -> None:
        if not ctx.guild or not isinstance(ctx.author, discord.Member):
            return
        if not self.canControlDivisionState(ctx.author):
            await ctx.reply("You are not authorized to open/close applications.", mention_author=False)
            return

        divisionKeyNormalized = str(divisionKey or "").strip().lower()
        actionNormalized = str(action or "").strip().lower()
        if not divisionKeyNormalized or not actionNormalized:
            await ctx.reply("Usage: `!applications <divisionKey> <open|close|status>`", mention_author=False)
            return

        division = self.getDivision(divisionKeyNormalized)
        if not division:
            known = ", ".join(self.divisionOrder) if self.divisionOrder else "none configured"
            await ctx.reply(f"Unknown division key. Known keys: {known}", mention_author=False)
            return

        if actionNormalized in {"status", "state"}:
            isOpen = await applicationsService.isDivisionOpen(ctx.guild.id, division["key"])
            statusText = "OPEN" if isOpen else "CLOSED"
            await ctx.reply(f"Applications for `{division['key']}` are currently **{statusText}**.", mention_author=False)
            return

        if actionNormalized in {"close", "closed", "disable", "disabled", "off"}:
            isOpen = False
        elif actionNormalized in {"open", "opened", "enable", "enabled", "reopen", "on"}:
            isOpen = True
        else:
            await ctx.reply("Action must be one of: `open`, `close`, `status`.", mention_author=False)
            return

        await applicationsService.setDivisionOpen(ctx.guild.id, division["key"], isOpen)
        updated = await self.refreshHubViewsForDivision(ctx.guild, division["key"])
        stateText = "OPEN" if isOpen else "CLOSED"
        await ctx.reply(
            f"Applications for `{division['key']}` are now **{stateText}**. Updated {updated} hub message(s).",
            mention_author=False,
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ApplicationsCog(bot))

