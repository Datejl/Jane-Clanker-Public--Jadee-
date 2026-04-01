from __future__ import annotations

import re
from typing import TYPE_CHECKING

import discord

from features.staff.applications import service as applicationsService
from runtime import interaction as interactionRuntime
from runtime import viewBases as runtimeViewBases

if TYPE_CHECKING:
    from cogs.applicationsCog import ApplicationsCog


def _toPositiveInt(value: str, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _parseIdListFromText(rawValue: str) -> list[int]:
    ids: list[int] = []
    for token in re.findall(r"\d+", str(rawValue or "")):
        try:
            parsed = int(token)
        except ValueError:
            continue
        if parsed > 0 and parsed not in ids:
            ids.append(parsed)
    return ids


def _parseBoolText(rawValue: str, *, default: bool = False) -> bool:
    text = str(rawValue or "").strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parseSnowflake(rawValue: str) -> int:
    digits = "".join(ch for ch in str(rawValue or "").strip() if ch.isdigit())
    if not digits:
        return 0
    try:
        parsed = int(digits)
    except ValueError:
        return 0
    return parsed if parsed > 0 else 0


class ApplicationsDivisionEditorView(discord.ui.View):
    def __init__(self, cog: "ApplicationsCog", divisionKey: str, *, isOpen: bool = True):
        super().__init__(timeout=600)
        self.cog = cog
        self.divisionKey = divisionKey
        self.isOpen = bool(isOpen)
        self._syncToggleButton()

    def _syncToggleButton(self) -> None:
        if self.isOpen:
            self.toggleApplicationsBtn.label = "Close Applications"
        else:
            self.toggleApplicationsBtn.label = "Open Applications"
        self.toggleApplicationsBtn.style = discord.ButtonStyle.secondary

    @discord.ui.button(label="Basic Info", style=discord.ButtonStyle.secondary, row=0)
    async def basicBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            ApplicationsDivisionBasicModal(self.cog, self.divisionKey),
        )

    @discord.ui.button(label="Logistics", style=discord.ButtonStyle.secondary, row=0)
    async def logisticsBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            ApplicationsDivisionLogisticsModal(self.cog, self.divisionKey),
        )

    @discord.ui.button(label="Eligibility/Ops", style=discord.ButtonStyle.secondary, row=0)
    async def eligibilityOpsBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            ApplicationsDivisionEligibilityModal(self.cog, self.divisionKey),
        )

    @discord.ui.button(label="Questions", style=discord.ButtonStyle.secondary, row=1)
    async def questionsBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.openDivisionQuestionsPanel(
            interaction,
            divisionKey=self.divisionKey,
        )

    @discord.ui.button(label="Proof", style=discord.ButtonStyle.secondary, row=1)
    async def proofBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            ApplicationsDivisionProofModal(self.cog, self.divisionKey),
        )

    @discord.ui.button(label="Help", style=discord.ButtonStyle.primary, row=2)
    async def helpBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        embed = discord.Embed(
            title="Division Editor Help",
            description="Use each section below to edit a different part of the division profile.",
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Basic Info",
            value="Display name, description, requirements, what-you-do text, and banner URL.",
            inline=False,
        )
        embed.add_field(
            name="Logistics",
            value="Hub webhook name/avatar, grant roles, reviewer roles, and auto-accept group ID.",
            inline=False,
        )
        embed.add_field(
            name="Eligibility/Ops",
            value="ORBAT seeding toggle plus eligibility role requirements and deny message.",
            inline=False,
        )
        embed.add_field(
            name="Questions",
            value="Manage application questions with add/edit/remove/type controls.",
            inline=False,
        )
        embed.add_field(
            name="Proof",
            value="Configure whether proof is required and what proof prompt applicants see.",
            inline=False,
        )
        await interactionRuntime.safeInteractionReply(
            interaction,
            embed=embed,
            ephemeral=True,
        )

    @discord.ui.button(label="Close Applications", style=discord.ButtonStyle.secondary, row=2)
    async def toggleApplicationsBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interactionRuntime.safeInteractionReply(
                interaction,
                content="This action can only be used in a server.",
                ephemeral=True,
            )
            return
        division = self.cog.getDivision(self.divisionKey)
        if not division:
            await interactionRuntime.safeInteractionReply(
                interaction,
                content="Division not found.",
                ephemeral=True,
            )
            return
        if not self.cog.canEditDivisionConfig(interaction.user, division):
            await interactionRuntime.safeInteractionReply(
                interaction,
                content="You are not allowed to edit this division.",
                ephemeral=True,
            )
            return

        newIsOpen = not self.isOpen
        await applicationsService.setDivisionOpen(interaction.guild.id, self.divisionKey, newIsOpen)
        updatedCards = await self.cog.refreshHubViewsForDivision(interaction.guild, self.divisionKey)
        self.isOpen = newIsOpen
        self._syncToggleButton()

        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            view=self,
        )

        await interactionRuntime.safeInteractionReply(
            interaction,
            content=(
                f"Applications {'opened' if newIsOpen else 'closed'} for `{self.divisionKey}`. "
                f"Updated {updatedCards} hub card(s)."
            ),
            ephemeral=True,
        )


class ApplicationsDivisionBasicModal(discord.ui.Modal, title="Edit Division - Basic Info"):
    displayNameInput = discord.ui.TextInput(
        label="Display Name",
        required=True,
        max_length=80,
    )
    descriptionInput = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=1000,
    )
    requirementsInput = discord.ui.TextInput(
        label="Requirements",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=1000,
    )
    whatYouDoInput = discord.ui.TextInput(
        label="What You Do",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=1000,
    )
    bannerUrlInput = discord.ui.TextInput(
        label="Banner URL",
        required=False,
        max_length=400,
    )

    def __init__(self, cog: "ApplicationsCog", divisionKey: str):
        super().__init__()
        self.cog = cog
        self.divisionKey = divisionKey
        division = cog.getDivision(divisionKey) or {}
        self.displayNameInput.default = str(division.get("displayName") or "")
        self.descriptionInput.default = str(division.get("description") or "")
        self.requirementsInput.default = str(division.get("requirements") or "")
        self.whatYouDoInput.default = str(division.get("whatYouDo") or "")
        self.bannerUrlInput.default = str(division.get("bannerUrl") or "")

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.updateDivisionConfig(
            interaction,
            divisionKey=self.divisionKey,
            updates={
                "displayName": str(self.displayNameInput.value or "").strip(),
                "description": str(self.descriptionInput.value or "").strip(),
                "requirements": str(self.requirementsInput.value or "").strip(),
                "whatYouDo": str(self.whatYouDoInput.value or "").strip(),
                "bannerUrl": str(self.bannerUrlInput.value or "").strip(),
            },
        )


class ApplicationsDivisionLogisticsModal(discord.ui.Modal, title="Edit Division - Logistics"):
    hubNameInput = discord.ui.TextInput(
        label="Hub Message Name (optional)",
        required=False,
        max_length=80,
    )
    hubAvatarInput = discord.ui.TextInput(
        label="Hub Avatar URL (optional)",
        required=False,
        max_length=400,
    )
    grantRolesInput = discord.ui.TextInput(
        label="Grant Role IDs",
        placeholder="Comma-separated role IDs",
        required=False,
        max_length=400,
    )
    reviewerRolesInput = discord.ui.TextInput(
        label="Reviewer Role IDs",
        placeholder="Comma-separated role IDs",
        required=False,
        max_length=400,
    )
    autoAcceptGroupInput = discord.ui.TextInput(
        label="Auto-accept Group ID (optional)",
        required=False,
        max_length=32,
    )

    def __init__(self, cog: "ApplicationsCog", divisionKey: str):
        super().__init__()
        self.cog = cog
        self.divisionKey = divisionKey
        division = cog.getDivision(divisionKey) or {}
        self.hubNameInput.default = str(division.get("hubMessageName") or "")
        self.hubAvatarInput.default = str(division.get("hubMessageAvatarUrl") or "")
        self.grantRolesInput.default = ", ".join(str(value) for value in division.get("grantRoleIds") or [])
        self.reviewerRolesInput.default = ", ".join(str(value) for value in division.get("reviewerRoleIds") or [])
        self.autoAcceptGroupInput.default = str(division.get("autoAcceptGroupId") or "")

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.updateDivisionConfig(
            interaction,
            divisionKey=self.divisionKey,
            updates={
                "hubMessageName": str(self.hubNameInput.value or "").strip(),
                "hubMessageAvatarUrl": str(self.hubAvatarInput.value or "").strip(),
                "grantRoleIds": _parseIdListFromText(str(self.grantRolesInput.value or "")),
                "reviewerRoleIds": _parseIdListFromText(str(self.reviewerRolesInput.value or "")),
                "autoAcceptGroupId": _toPositiveInt(str(self.autoAcceptGroupInput.value or ""), 0),
            },
        )


class ApplicationsDivisionEligibilityModal(discord.ui.Modal, title="Edit Division - Eligibility/Ops"):
    seedOrbatInput = discord.ui.TextInput(
        label="Seed ORBAT On Approve (true/false)",
        required=False,
        max_length=16,
    )
    eligibilityRolesInput = discord.ui.TextInput(
        label="Eligibility Role IDs (optional)",
        placeholder="Comma-separated role IDs",
        required=False,
        max_length=400,
    )
    eligibilityDenyInput = discord.ui.TextInput(
        label="Eligibility Deny Message (optional)",
        required=False,
        max_length=200,
    )

    def __init__(self, cog: "ApplicationsCog", divisionKey: str):
        super().__init__()
        self.cog = cog
        self.divisionKey = divisionKey
        division = cog.getDivision(divisionKey) or {}
        self.seedOrbatInput.default = "true" if division.get("seedOrbatOnApprove") else "false"
        eligibility = division.get("eligibility") or {}
        self.eligibilityRolesInput.default = ", ".join(
            str(value) for value in eligibility.get("requiredRoleIds") or []
        )
        self.eligibilityDenyInput.default = str(eligibility.get("denyMessage") or "")

    async def on_submit(self, interaction: discord.Interaction) -> None:
        eligibilityRequiredRoles = _parseIdListFromText(str(self.eligibilityRolesInput.value or ""))
        eligibilityDenyMessage = str(self.eligibilityDenyInput.value or "").strip()
        await self.cog.updateDivisionConfig(
            interaction,
            divisionKey=self.divisionKey,
            updates={
                "seedOrbatOnApprove": _parseBoolText(str(self.seedOrbatInput.value or ""), default=False),
                "eligibility": {
                    "requiredRoleIds": eligibilityRequiredRoles,
                    "denyMessage": eligibilityDenyMessage,
                },
            },
        )


class ApplicationsDivisionProofModal(discord.ui.Modal, title="Edit Division - Proof"):
    requiresProofInput = discord.ui.TextInput(
        label="Requires Proof (true/false)",
        required=False,
        max_length=16,
    )
    proofPromptInput = discord.ui.TextInput(
        label="Proof Prompt",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=600,
    )

    def __init__(self, cog: "ApplicationsCog", divisionKey: str):
        super().__init__()
        self.cog = cog
        self.divisionKey = divisionKey
        division = cog.getDivision(divisionKey) or {}
        self.requiresProofInput.default = "true" if division.get("requiresProof") else "false"
        self.proofPromptInput.default = str(division.get("proofPrompt") or "")

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.updateDivisionConfig(
            interaction,
            divisionKey=self.divisionKey,
            updates={
                "requiresProof": _parseBoolText(str(self.requiresProofInput.value or ""), default=False),
                "proofPrompt": str(self.proofPromptInput.value or "").strip(),
            },
        )


__all__ = [
    "ApplicationsDivisionBasicModal",
    "ApplicationsDivisionEditorView",
    "ApplicationsDivisionEligibilityModal",
    "ApplicationsDivisionLogisticsModal",
    "ApplicationsDivisionProofModal",
    "_parseBoolText",
    "_parseIdListFromText",
    "_parseSnowflake",
]

