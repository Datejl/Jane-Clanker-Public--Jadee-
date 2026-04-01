from __future__ import annotations

from typing import TYPE_CHECKING

import discord

from runtime import interaction as interactionRuntime

if TYPE_CHECKING:
    from cogs.applicationsCog import ApplicationsCog


class ApplicationsHubPostModal(discord.ui.Modal, title="Post Hub(s)"):
    divisionInput = discord.ui.TextInput(
        label="Division Key (optional)",
        placeholder="Example: tqual (leave empty or use ALL for every division)",
        required=False,
        max_length=64,
    )
    channelInput = discord.ui.TextInput(
        label="Channel ID (optional)",
        placeholder="Defaults to current channel",
        required=False,
        max_length=64,
    )

    def __init__(self, cog: "ApplicationsCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        divisionKey = str(self.divisionInput.value or "").strip()
        if not divisionKey or divisionKey.lower() == "all":
            await self.cog.runApplicationsHubPostAll(
                interaction,
                channelInput=str(self.channelInput.value or "").strip(),
            )
            return
        await self.cog.runApplicationsHubPost(
            interaction,
            divisionKey=divisionKey,
            channelInput=str(self.channelInput.value or "").strip(),
        )


class ApplicationsHubPostAllModal(discord.ui.Modal, title="Post All Division Hubs"):
    channelInput = discord.ui.TextInput(
        label="Channel ID (optional)",
        placeholder="Defaults to current channel",
        required=False,
        max_length=64,
    )

    def __init__(self, cog: "ApplicationsCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.runApplicationsHubPostAll(
            interaction,
            channelInput=str(self.channelInput.value or "").strip(),
        )


class ApplicationsPendingModal(discord.ui.Modal, title="Pending Applications"):
    divisionInput = discord.ui.TextInput(
        label="Division Key (optional)",
        placeholder="Leave empty for all divisions",
        required=False,
        max_length=64,
    )

    def __init__(self, cog: "ApplicationsCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.runAppsPending(
            interaction,
            division=str(self.divisionInput.value or "").strip() or None,
        )


class ApplicationsStatsModal(discord.ui.Modal, title="Application Stats"):
    divisionInput = discord.ui.TextInput(
        label="Division Key (optional)",
        placeholder="Leave empty for all divisions",
        required=False,
        max_length=64,
    )
    windowInput = discord.ui.TextInput(
        label="Window",
        placeholder="Example: 7d or 30d",
        required=False,
        max_length=16,
    )

    def __init__(self, cog: "ApplicationsCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.runAppsStats(
            interaction,
            division=str(self.divisionInput.value or "").strip() or None,
            last=str(self.windowInput.value or "").strip() or "7d",
        )


class ApplicationsReopenModal(discord.ui.Modal, title="Reopen Application"):
    applicationInput = discord.ui.TextInput(
        label="Application ID",
        placeholder="Example: APP-3021",
        required=True,
        max_length=32,
    )

    def __init__(self, cog: "ApplicationsCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.runAppsReopen(
            interaction,
            applicationId=str(self.applicationInput.value or "").strip(),
        )


class ApplicationsForceApproveModal(discord.ui.Modal, title="Force Approve Application"):
    applicationInput = discord.ui.TextInput(
        label="Application ID",
        placeholder="Example: APP-3021",
        required=True,
        max_length=32,
    )

    def __init__(self, cog: "ApplicationsCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.runAppsForceApprove(
            interaction,
            applicationId=str(self.applicationInput.value or "").strip(),
        )


class ApplicationsCloseAllModal(discord.ui.Modal, title="Close All Applications"):
    confirmInput = discord.ui.TextInput(
        label="Type CLOSE ALL to confirm",
        placeholder="CLOSE ALL",
        required=True,
        max_length=32,
    )

    def __init__(self, cog: "ApplicationsCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.runApplicationsCloseAllActive(
            interaction,
            confirmation=str(self.confirmInput.value or "").strip(),
        )


class ApplicationsDivisionSelectModal(discord.ui.Modal, title="Select Division"):
    divisionInput = discord.ui.TextInput(
        label="Division Key",
        placeholder="Example: tqual",
        required=True,
        max_length=64,
    )

    def __init__(self, cog: "ApplicationsCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.openDivisionEditor(
            interaction,
            divisionKey=str(self.divisionInput.value or "").strip(),
        )


class ApplicationsPanelView(discord.ui.View):
    def __init__(self, cog: "ApplicationsCog", *, canBulkClose: bool = False) -> None:
        super().__init__(timeout=600)
        self.cog = cog
        if not canBulkClose:
            self.closeAllBtn.disabled = True

    @discord.ui.button(label="Post Hub(s)", style=discord.ButtonStyle.secondary, row=0)
    async def postHubBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(interaction, ApplicationsHubPostModal(self.cog))

    @discord.ui.button(label="Edit Divisions", style=discord.ButtonStyle.primary, row=0)
    async def editDivisionBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(interaction, ApplicationsDivisionSelectModal(self.cog))

    @discord.ui.button(label="Keys", style=discord.ButtonStyle.secondary, row=0)
    async def keysBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        lines: list[str] = []
        for key in self.cog.divisionOrder:
            division = self.cog.divisions.get(key) or {}
            displayName = str(division.get("displayName") or key)
            lines.append(f"`{key}` -> {displayName}")
        if not lines:
            lines.append("(No divisions loaded)")
        embed = discord.Embed(
            title="Division Keys",
            description="\n".join(lines[:40]),
            color=discord.Color.blurple(),
        )
        await interactionRuntime.safeInteractionReply(
            interaction,
            embed=embed,
            ephemeral=True,
        )

    @discord.ui.button(label="Close All", style=discord.ButtonStyle.danger, row=1)
    async def closeAllBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(interaction, ApplicationsCloseAllModal(self.cog))

    @discord.ui.button(label="Stats", style=discord.ButtonStyle.secondary, row=1)
    async def statsBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(interaction, ApplicationsStatsModal(self.cog))


__all__ = [
    "ApplicationsCloseAllModal",
    "ApplicationsDivisionSelectModal",
    "ApplicationsForceApproveModal",
    "ApplicationsHubPostAllModal",
    "ApplicationsHubPostModal",
    "ApplicationsPanelView",
    "ApplicationsPendingModal",
    "ApplicationsReopenModal",
    "ApplicationsStatsModal",
]
