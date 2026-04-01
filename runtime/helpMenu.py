from __future__ import annotations

from typing import Any

import discord

from runtime import viewBases as runtimeViewBases


class HelpSectionSelect(discord.ui.Select):
    def __init__(self, view: "HelpMenuView") -> None:
        self.helpView = view
        options: list[discord.SelectOption] = []
        for section in self.helpView.sections[:25]:
            key = str(section.get("key") or "")
            title = str(section.get("title") or key.title()).strip() or "Section"
            description = str(section.get("description") or "").strip() or "Browse this command section."
            itemCount = len(list(section.get("items") or []))
            countText = f"{itemCount} command{'s' if itemCount != 1 else ''}"
            options.append(
                discord.SelectOption(
                    label=title[:100],
                    description=f"{description} [{countText}]"[:100],
                    value=key,
                    default=key == self.helpView.currentSectionKey,
                )
            )

        super().__init__(
            placeholder="Choose a help section",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        self.helpView.currentSectionKey = str(self.values[0] or self.helpView.currentSectionKey)
        await self.helpView.refresh(interaction)


class HelpMenuView(runtimeViewBases.OwnerLockedView):
    def __init__(
        self,
        *,
        openerId: int,
        helpCommandsModule: Any,
        sections: list[dict[str, object]],
        currentSectionKey: str,
    ) -> None:
        super().__init__(
            openerId=openerId,
            timeout=900,
            ownerMessage="This help menu belongs to someone else.",
        )
        self.helpCommands = helpCommandsModule
        self.sections = list(sections)
        self.currentSectionKey = str(currentSectionKey or "overview")
        self.add_item(HelpSectionSelect(self))
        self.prevBtn.disabled = self.currentSectionIndex <= 0
        self.nextBtn.disabled = self.currentSectionIndex >= len(self.sections) - 1

    @property
    def currentSectionIndex(self) -> int:
        for index, section in enumerate(self.sections):
            if str(section.get("key") or "") == self.currentSectionKey:
                return index
        return 0

    @property
    def currentSection(self) -> dict[str, object]:
        if not self.sections:
            return {
                "key": "overview",
                "title": "Overview",
                "description": "No command help sections are available right now.",
                "items": [],
            }
        return self.sections[self.currentSectionIndex]

    def buildEmbed(self) -> discord.Embed:
        return self.helpCommands.buildHelpSectionEmbed(
            self.currentSection,
            currentIndex=self.currentSectionIndex + 1,
            totalSections=max(len(self.sections), 1),
        )

    def replacementView(self) -> "HelpMenuView":
        return HelpMenuView(
            openerId=self.openerId,
            helpCommandsModule=self.helpCommands,
            sections=self.sections,
            currentSectionKey=self.currentSectionKey,
        )

    async def refresh(self, interaction: discord.Interaction) -> None:
        replacement = self.replacementView()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=replacement.buildEmbed(),
            view=replacement,
        )

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, row=1)
    async def prevBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.currentSectionIndex > 0:
            self.currentSectionKey = str(self.sections[self.currentSectionIndex - 1].get("key") or "overview")
        await self.refresh(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, row=1)
    async def nextBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.currentSectionIndex < len(self.sections) - 1:
            self.currentSectionKey = str(self.sections[self.currentSectionIndex + 1].get("key") or "overview")
        await self.refresh(interaction)
