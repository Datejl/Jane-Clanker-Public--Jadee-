from __future__ import annotations

from typing import Any

import discord

from .rendering import parsePollOptions
from runtime import interaction as interactionRuntime


class PollVoteSelect(discord.ui.Select):
    def __init__(self, view: "PollView", *, disabled: bool) -> None:
        self.pollView = view
        pollOptions = parsePollOptions(view.pollRow)
        multiSelect = bool(int(view.pollRow.get("multiSelect") or 0))
        options = [
            discord.SelectOption(
                label=option[:100],
                value=str(index),
                description=f"Vote for option {index + 1}",
            )
            for index, option in enumerate(pollOptions[:25])
        ]
        super().__init__(
            placeholder="Choose an option",
            min_values=1,
            max_values=max(1, len(options)) if multiSelect else 1,
            options=options or [discord.SelectOption(label="No options", value="0")],
            disabled=disabled or not bool(options),
            row=0,
            custom_id=f"poll:vote:{int(view.pollId)}",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        await self.pollView.cog.handlePollVoteSelection(
            interaction,
            pollId=self.pollView.pollId,
            optionIndexes=[int(value) for value in self.values],
        )


class PollView(discord.ui.View):
    def __init__(self, *, cog: Any, pollRow: dict, closed: bool = False) -> None:
        super().__init__(timeout=None)
        self.cog = cog
        self.pollRow = dict(pollRow)
        self.pollId = int(self.pollRow.get("pollId") or 0)
        self.closed = bool(closed)
        self.add_item(PollVoteSelect(self, disabled=self.closed))
        self.refreshBtn.custom_id = f"poll:refresh:{self.pollId}"
        self.closeBtn.custom_id = f"poll:close:{self.pollId}"
        self.closeBtn.disabled = self.closed

    @discord.ui.button(label="Refresh Results", style=discord.ButtonStyle.secondary, row=1)
    async def refreshBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.refreshPollFromInteraction(interaction, pollId=self.pollId)

    @discord.ui.button(label="Close Poll", style=discord.ButtonStyle.danger, row=1)
    async def closeBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.closePollFromInteraction(interaction, pollId=self.pollId)
