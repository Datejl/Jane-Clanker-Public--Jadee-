from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import discord

from silly import gamblingService
from silly.gambling import access, work
from silly.gambling.tableView import GamblingTableView
from runtime import viewBases as runtimeViewBases


def _buildWorkTaskEmbed(task: dict) -> discord.Embed:
    title = work.titleText(task)
    prompt = work.promptText(task)
    embed = discord.Embed(
        title=f"Work Shift: {title}",
        description=prompt,
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Payout", value=f"`{access.anrobucks(10)}` on correct submission", inline=True)
    embed.set_footer(text="Complete all steps, then press Submit Answer.")
    return embed


class GamblingGameSelect(discord.ui.Select):
    def __init__(self, viewRef: "GamblingHubView"):
        self.viewRef = viewRef
        enabledGames = access.enabledGameKeys()
        options = [
            discord.SelectOption(label=access.gameLabels.get(gameKey, gameKey.title()), value=gameKey)
            for gameKey in enabledGames
        ]
        super().__init__(
            placeholder="Select a game table to open",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        selected = str((self.values or [access.defaultGameKey()])[0] or access.defaultGameKey()).strip().lower()
        if not access.isGameEnabled(selected):
            selected = access.defaultGameKey()
        await self.viewRef.openGameTable(interaction, selected)


class GamblingWorkModal(discord.ui.Modal):
    answerInput = discord.ui.TextInput(
        label="Task answer",
        placeholder="Complete all steps and submit the final formatted answer",
        required=True,
        max_length=600,
    )

    def __init__(self, viewRef: "GamblingHubView", task: dict):
        self.viewRef = viewRef
        self.task = dict(task or {})
        taskTitle = work.titleText(self.task)
        super().__init__(title=taskTitle[:45] or "Shift Task", timeout=300)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await access.enforceGamblingRoleAccess(interaction):
            return
        safeUserId = int(getattr(interaction.user, "id", 0) or 0)
        if safeUserId != self.viewRef.userId:
            await interaction.response.send_message("This shift task is not yours.", ephemeral=True)
            return

        givenAnswer = str(self.answerInput.value or "").strip()
        if not work.validateAnswer(self.task, givenAnswer):
            expected = work.expectedAnswer(self.task)
            self.viewRef.lastStatus = "Work shift failed. No payout awarded."
            await interaction.response.send_message(
                f"Incorrect. Expected: `{expected}`. No payout awarded.",
                ephemeral=True,
            )
            await self.viewRef.refreshSnapshot()
            return

        updatedWallet = await gamblingService.applyWorkPayout(self.viewRef.userId, 10)
        if not updatedWallet:
            await interaction.response.send_message("Unable to process your work payout right now.", ephemeral=True)
            return

        balance = int(updatedWallet.get("balance") or 0)
        self.viewRef.lastStatus = (
            f"Shift complete: earned `{access.anrobucks(10)}`. Balance is now `{access.anrobucks(balance)}`."
        )
        await interaction.response.send_message(
            f"Shift complete. You earned **{access.anrobucks(10)}**. Current balance: `{access.anrobucks(balance)}`.",
            ephemeral=True,
        )
        await self.viewRef.refreshSnapshot()


class GamblingWorkTaskView(runtimeViewBases.OwnerLockedView):
    def __init__(self, hubView: "GamblingHubView", task: dict):
        super().__init__(
            openerId=hubView.userId,
            timeout=600,
            ownerMessage="This shift task is not yours.",
        )
        self.hubView = hubView
        self.ownerUserId = int(hubView.userId)
        self.task = dict(task or {})

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not await access.enforceGamblingRoleAccess(interaction):
            return False
        if not await super().interaction_check(interaction):
            return False
        if not await access.enforceGamblingButtonCooldown(interaction):
            return False
        return True

    @discord.ui.button(label="Submit Answer", style=discord.ButtonStyle.success, row=0)
    async def submitBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(GamblingWorkModal(self.hubView, self.task))

    @discord.ui.button(label="New Task", style=discord.ButtonStyle.secondary, row=0)
    async def newTaskBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.task = work.createTask()
        embed = _buildWorkTaskEmbed(self.task)
        await access.safeEditInteractionMessage(interaction, embed=embed, view=self)


class GamblingHubView(runtimeViewBases.OwnerLockedView):
    def __init__(self, cog: Any, *, userId: int):
        super().__init__(
            openerId=userId,
            timeout=900,
            ownerMessage="This casino hub is not yours.",
        )
        self.cog = cog
        self.userId = int(userId)
        self.lastStatus = "Select a game below to post a public table."
        self.hubMessage: discord.Message | None = None
        self.add_item(GamblingGameSelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not await access.enforceGamblingRoleAccess(interaction):
            return False
        if not await super().interaction_check(interaction):
            return False
        if not await access.enforceGamblingButtonCooldown(interaction):
            return False
        return True

    async def openGameTable(self, interaction: discord.Interaction, selectedGame: str) -> None:
        if not access.isGameEnabled(selectedGame):
            await interaction.response.send_message(access.disabledGameMessage, ephemeral=True)
            return
        channel = interaction.channel
        if channel is None or not hasattr(channel, "send"):
            await interaction.response.send_message("Unable to open a table here.", ephemeral=True)
            return

        wallet = await gamblingService.getWallet(self.userId)
        tableView = GamblingTableView(self.cog, userId=self.userId, selectedGame=selectedGame)
        tableEmbed = self.cog.buildTableEmbed(
            userId=self.userId,
            wallet=wallet,
            selectedGame=selectedGame,
            lastResult=tableView.tablePrompt(),
            settingsText=tableView.tableSettingsText(),
        )

        try:
            await channel.send(
                content=f"{interaction.user.mention} opened a **{access.gameLabels[selectedGame]}** table.",
                embed=tableEmbed,
                view=tableView,
            )
        except discord.HTTPException:
            await interaction.response.send_message("I could not post the table in this channel.", ephemeral=True)
            return

        self.lastStatus = f"Opened **{access.gameLabels[selectedGame]}** table in {getattr(channel, 'mention', '#channel')}."
        await self.refresh(interaction)

    async def refresh(self, interaction: discord.Interaction) -> None:
        sourceMessage = getattr(interaction, "message", None)
        if isinstance(sourceMessage, discord.Message):
            self.hubMessage = sourceMessage
        wallet = await gamblingService.getWallet(self.userId)
        embed = self.cog.buildHubEmbed(userId=self.userId, wallet=wallet, lastStatus=self.lastStatus)
        await access.safeEditInteractionMessage(interaction, embed=embed, view=self)

    async def refreshSnapshot(self) -> None:
        if self.hubMessage is None:
            return
        wallet = await gamblingService.getWallet(self.userId)
        embed = self.cog.buildHubEmbed(userId=self.userId, wallet=wallet, lastStatus=self.lastStatus)
        try:
            await self.hubMessage.edit(embed=embed, view=self)
        except discord.HTTPException:
            return

    @discord.ui.button(label="Work (+10 anrobucks)", style=discord.ButtonStyle.success, row=1)
    async def workBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        task = work.createTask()
        promptView = GamblingWorkTaskView(self, task)
        embed = _buildWorkTaskEmbed(task)
        await interaction.response.send_message(embed=embed, view=promptView, ephemeral=True)
