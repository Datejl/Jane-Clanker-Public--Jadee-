from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands

import config
from features.community.publicUtility import RoleMenuView, configuredRoleMenus, menuConfig
from runtime import cogGuards as runtimeCogGuards


class PublicUtilityCog(runtimeCogGuards.InteractionGuardMixin, commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for menuKey in configuredRoleMenus(config).keys():
            self.bot.add_view(RoleMenuView(configModule=config, menuKey=menuKey))

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        channelId = int(getattr(config, "welcomeChannelId", 0) or 0)
        if channelId <= 0:
            return
        channel = member.guild.get_channel(channelId)
        if not isinstance(channel, discord.TextChannel):
            return
        template = str(
            getattr(
                config,
                "welcomeMessageTemplate",
                "Welcome to **{guild}**, {mention}.",
            )
            or "Welcome to **{guild}**, {mention}."
        )
        try:
            await channel.send(
                template.format(
                    mention=member.mention,
                    user=member.display_name,
                    guild=member.guild.name,
                )
            )
        except (discord.Forbidden, discord.HTTPException, KeyError):
            return

    @app_commands.command(name="post-role-menu", description="Post a public self-role menu.")
    @app_commands.rename(menu_key="menu-key")
    async def postRoleMenu(self, interaction: discord.Interaction, menu_key: str) -> None:
        if await self._requireAdminOrManageGuild(interaction) is None:
            return
        row = menuConfig(config, menu_key)
        if row is None:
            return await self._safeReply(interaction, "That role menu key is not configured.")
        title = str(row.get("title") or "Choose Your Roles").strip()[:256]
        description = str(row.get("description") or "Select the roles you want from the menu below.").strip()
        embed = discord.Embed(
            title=title,
            description=description,
            color=discord.Color.blurple(),
        )
        await self._safeReply(
            interaction,
            embed=embed,
            view=RoleMenuView(configModule=config, menuKey=menu_key),
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(PublicUtilityCog(bot))
