from __future__ import annotations

from typing import Any

import discord

from runtime import viewBases as runtimeViewBases


class PauseMenuView(runtimeViewBases.OwnerLockedView):
    def __init__(self, *, cog: Any, openerId: int) -> None:
        super().__init__(
            openerId=openerId,
            timeout=900,
            ownerMessage="This pause panel belongs to someone else.",
        )
        self.cog = cog
        self.noticeText = ""

    async def refresh(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            return
        self.cog.configurePauseButtons(self)
        embed = self.cog.buildPauseEmbed(guild, noticeText=self.noticeText)
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=embed,
            view=self,
        )

    @discord.ui.button(label="Pause Jane", style=discord.ButtonStyle.danger, row=0)
    async def toggleBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.noticeText = await self.cog.togglePauseFromPanel(interaction)
        await self.refresh(interaction)
