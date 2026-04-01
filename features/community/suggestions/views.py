from __future__ import annotations

from typing import Any

import discord

from runtime import interaction as interactionRuntime


class SuggestionReviewModal(discord.ui.Modal):
    reviewNote = discord.ui.TextInput(
        label="Reviewer Note (optional)",
        required=False,
        style=discord.TextStyle.paragraph,
        max_length=500,
        placeholder="Optional context for this review decision.",
    )

    def __init__(self, *, cog: Any, suggestionId: int, newStatus: str) -> None:
        super().__init__(title=f"{str(newStatus or '').title()} Suggestion")
        self.cog = cog
        self.suggestionId = int(suggestionId)
        self.newStatus = str(newStatus or "").strip().upper()

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.applySuggestionStatusChange(
            interaction,
            suggestionId=self.suggestionId,
            newStatus=self.newStatus,
            reviewNote=str(self.reviewNote.value or "").strip(),
        )


class SuggestionReviewView(discord.ui.View):
    def __init__(self, *, cog: Any, suggestionId: int, resolved: bool = False) -> None:
        super().__init__(timeout=None)
        self.cog = cog
        self.suggestionId = int(suggestionId)
        self.resolved = bool(resolved)
        self.approveBtn.custom_id = f"suggestion:approve:{self.suggestionId}"
        self.rejectBtn.custom_id = f"suggestion:reject:{self.suggestionId}"
        self.implementBtn.custom_id = f"suggestion:implement:{self.suggestionId}"
        if self.resolved:
            for child in self.children:
                if isinstance(child, discord.ui.Button):
                    child.disabled = True

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, row=0)
    async def approveBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.openSuggestionReviewModal(interaction, suggestionId=self.suggestionId, newStatus="APPROVED")

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger, row=0)
    async def rejectBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.openSuggestionReviewModal(interaction, suggestionId=self.suggestionId, newStatus="REJECTED")

    @discord.ui.button(label="Implement", style=discord.ButtonStyle.primary, row=0)
    async def implementBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.openSuggestionReviewModal(
            interaction,
            suggestionId=self.suggestionId,
            newStatus="IMPLEMENTED",
        )
