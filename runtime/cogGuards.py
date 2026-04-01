from __future__ import annotations

import discord

from runtime import commandScopes as runtimeCommandScopes
from runtime import interaction as interactionRuntime
from runtime import permissions as runtimePermissions


class InteractionGuardMixin:
    async def _safeReply(
        self,
        interaction: discord.Interaction,
        content: str | None = None,
        *,
        embed: discord.Embed | None = None,
        embeds: list[discord.Embed] | None = None,
        view: discord.ui.View | None = None,
        ephemeral: bool = True,
    ) -> None:
        await interactionRuntime.safeInteractionReply(
            interaction,
            content=content,
            embed=embed,
            embeds=embeds,
            view=view,
            ephemeral=ephemeral,
        )

    async def _requireGuildMember(
        self,
        interaction: discord.Interaction,
        *,
        failureMessage: str = "Use this command in a server.",
    ) -> discord.Member | None:
        if interaction.guild and isinstance(interaction.user, discord.Member):
            return interaction.user
        await self._safeReply(interaction, failureMessage)
        return None

    async def _requireAdminOrManageGuild(
        self,
        interaction: discord.Interaction,
        *,
        failureMessage: str = "Administrator or manage-server required.",
    ) -> discord.Member | None:
        member = await self._requireGuildMember(interaction)
        if member is None:
            return None
        if runtimePermissions.hasAdminOrManageGuild(member):
            return member
        await self._safeReply(interaction, failureMessage)
        return None

    async def _requireAdministrator(
        self,
        interaction: discord.Interaction,
        *,
        failureMessage: str = "Administrator only.",
    ) -> discord.Member | None:
        member = await self._requireGuildMember(interaction)
        if member is None:
            return None
        if runtimePermissions.hasAdministrator(member):
            return member
        await self._safeReply(interaction, failureMessage)
        return None

    async def _requireTestGuild(
        self,
        interaction: discord.Interaction,
        *,
        failureMessage: str = runtimeCommandScopes.TEST_GUILD_ONLY_MESSAGE,
    ) -> bool:
        if runtimeCommandScopes.isInteractionInTestGuild(interaction):
            return True
        await self._safeReply(interaction, failureMessage)
        return False
