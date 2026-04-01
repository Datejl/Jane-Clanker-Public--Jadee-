from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional

import discord
from discord import ui


def _favoriteGameLine(index: int, game: dict[str, Any]) -> str:
    gameName = str(game.get("name") or "Untitled")
    placeId = game.get("placeId")
    universeId = game.get("universeId")
    if placeId:
        gameUrl = f"https://www.roblox.com/games/{placeId}"
        return f"{index}. [{gameName}]({gameUrl})"
    if universeId:
        gameUrl = f"https://www.roblox.com/games/{universeId}"
        return f"{index}. [{gameName}]({gameUrl})"
    return f"{index}. {gameName}"


def _joinFavoriteGameLines(lines: list[str]) -> str:
    if not lines:
        return "No favorited games available."
    safeLines: list[str] = []
    totalLen = 0
    limit = 1020
    for line in lines:
        compactLine = line if len(line) <= 200 else f"{line[:197]}..."
        projected = totalLen + len(compactLine) + (1 if safeLines else 0)
        if projected > limit:
            break
        safeLines.append(compactLine)
        totalLen = projected
    if len(safeLines) < len(lines):
        safeLines.append("...")
    return "\n".join(safeLines)


class FavoriteGamesPageView(ui.View):
    def __init__(
        self,
        targetUserId: int,
        viewerId: int,
        games: list[dict[str, Any]],
        safeReply: Callable[..., Awaitable[None]],
        robloxUsername: Optional[str] = None,
        pageSize: int = 12,
    ):
        super().__init__(timeout=600)
        self.targetUserId = int(targetUserId)
        self.viewerId = int(viewerId)
        self.games = list(games or [])
        self.robloxUsername = robloxUsername
        self.pageSize = max(1, min(20, int(pageSize or 12)))
        self.pageIndex = 0
        self.pageCount = max(1, (len(self.games) + self.pageSize - 1) // self.pageSize)
        self.safeReply = safeReply
        self._syncButtons()

    def _syncButtons(self) -> None:
        self.prevBtn.disabled = self.pageIndex <= 0
        self.nextBtn.disabled = self.pageIndex >= self.pageCount - 1

    def buildEmbed(self) -> discord.Embed:
        totalGames = len(self.games)
        start = self.pageIndex * self.pageSize
        end = min(start + self.pageSize, totalGames)
        pageGames = self.games[start:end]

        pageLines = [
            _favoriteGameLine(start + offset + 1, game)
            for offset, game in enumerate(pageGames)
            if isinstance(game, dict)
        ]

        embed = discord.Embed(title="Favorited Games")
        embed.add_field(
            name="Attendee",
            value=f"<@{self.targetUserId}>",
            inline=False,
        )
        embed.add_field(
            name=f"Games ({start + 1 if totalGames else 0}-{end} of {totalGames})",
            value=_joinFavoriteGameLines(pageLines),
            inline=False,
        )
        footerParts: list[str] = []
        if self.robloxUsername:
            footerParts.append(f"Roblox: {self.robloxUsername}")
        footerParts.append(f"Page {self.pageIndex + 1}/{self.pageCount}")
        embed.set_footer(text=" | ".join(footerParts))
        return embed

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.viewerId:
            await self.safeReply(
                interaction,
                content="This panel is only for the moderator who opened it.",
                ephemeral=True,
            )
            return False
        return True

    @ui.button(label="Prev", style=discord.ButtonStyle.secondary, emoji="\u25C0")
    async def prevBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        self.pageIndex = max(0, self.pageIndex - 1)
        self._syncButtons()
        await interaction.response.edit_message(embed=self.buildEmbed(), view=self)

    @ui.button(label="Next", style=discord.ButtonStyle.secondary, emoji="\u25B6")
    async def nextBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        self.pageIndex = min(self.pageCount - 1, self.pageIndex + 1)
        self._syncButtons()
        await interaction.response.edit_message(embed=self.buildEmbed(), view=self)

    @ui.button(label="Close", style=discord.ButtonStyle.danger)
    async def closeBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
