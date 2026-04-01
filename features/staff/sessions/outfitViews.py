from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional

import discord
from discord import ui


class OutfitGalleryView(ui.View):
    def __init__(
        self,
        sessionId: int,
        targetUserId: int,
        outfits: list[dict[str, Any]],
        thumbnails: dict[int, Optional[str]],
        viewerId: int,
        safeReply: Callable[..., Awaitable[None]],
        buildOutfitEmbed: Callable[[int, Optional[str], list[dict[str, Any]], dict[int, Optional[str]], int], discord.Embed],
        robloxUsername: Optional[str] = None,
    ):
        super().__init__(timeout=900)
        self.sessionId = int(sessionId)
        self.targetUserId = int(targetUserId)
        self.outfits = outfits
        self.thumbnails = thumbnails
        self.viewerId = int(viewerId)
        self.robloxUsername = robloxUsername
        self.safeReply = safeReply
        self.buildOutfitEmbed = buildOutfitEmbed
        self.index = 0
        self._syncButtons()

    def _syncButtons(self) -> None:
        total = len(self.outfits)
        self.prevBtn.disabled = self.index <= 0
        self.nextBtn.disabled = self.index >= total - 1

    def _currentEmbed(self) -> discord.Embed:
        return self.buildOutfitEmbed(
            self.targetUserId,
            self.robloxUsername,
            self.outfits,
            self.thumbnails,
            self.index,
        )

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.viewerId:
            await self.safeReply(
                interaction,
                "This panel is only for the moderator who opened it.",
                ephemeral=True,
            )
            return False
        return True

    @ui.button(label="Prev", style=discord.ButtonStyle.secondary, emoji="\u25C0")
    async def prevBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        self.index = max(0, self.index - 1)
        self._syncButtons()
        await interaction.response.edit_message(embed=self._currentEmbed(), view=self)

    @ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="\u25B6")
    async def nextBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        self.index = min(len(self.outfits) - 1, self.index + 1)
        self._syncButtons()
        await interaction.response.edit_message(embed=self._currentEmbed(), view=self)

    @ui.button(label="Close", style=discord.ButtonStyle.danger)
    async def closeBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)


class OutfitPageView(ui.View):
    def __init__(
        self,
        sessionId: int,
        targetUserId: int,
        outfits: list[dict[str, Any]],
        thumbnails: dict[int, Optional[str]],
        viewerId: int,
        safeReply: Callable[..., Awaitable[None]],
        buildOutfitPageEmbeds: Callable[
            [int, Optional[str], list[dict[str, Any]], dict[int, Optional[str]], int, int],
            list[discord.Embed],
        ],
        robloxUsername: Optional[str] = None,
        pageSize: int = 10,
    ):
        super().__init__(timeout=900)
        self.sessionId = int(sessionId)
        self.targetUserId = int(targetUserId)
        self.outfits = outfits
        self.thumbnails = thumbnails
        self.viewerId = int(viewerId)
        self.robloxUsername = robloxUsername
        self.pageSize = max(1, min(10, int(pageSize)))
        self.pageIndex = 0
        self.pageCount = max(1, (len(self.outfits) + self.pageSize - 1) // self.pageSize)
        self.safeReply = safeReply
        self.buildOutfitPageEmbeds = buildOutfitPageEmbeds
        self._syncButtons()

    def _syncButtons(self) -> None:
        self.prevBtn.disabled = self.pageIndex <= 0
        self.nextBtn.disabled = self.pageIndex >= self.pageCount - 1

    def _currentEmbeds(self) -> list[discord.Embed]:
        return self.buildOutfitPageEmbeds(
            self.targetUserId,
            self.robloxUsername,
            self.outfits,
            self.thumbnails,
            self.pageIndex,
            self.pageSize,
        )

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.viewerId:
            await self.safeReply(
                interaction,
                "This panel is only for the moderator who opened it.",
                ephemeral=True,
            )
            return False
        return True

    @ui.button(label="Prev 10", style=discord.ButtonStyle.secondary, emoji="\u25C0")
    async def prevBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        self.pageIndex = max(0, self.pageIndex - 1)
        self._syncButtons()
        await interaction.response.edit_message(embeds=self._currentEmbeds(), view=self)

    @ui.button(label="Next 10", style=discord.ButtonStyle.secondary, emoji="\u25B6")
    async def nextBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        self.pageIndex = min(self.pageCount - 1, self.pageIndex + 1)
        self._syncButtons()
        await interaction.response.edit_message(embeds=self._currentEmbeds(), view=self)

    @ui.button(label="Close", style=discord.ButtonStyle.danger)
    async def closeBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
