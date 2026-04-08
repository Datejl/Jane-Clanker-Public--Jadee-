from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import discord

import config
from runtime import interaction as interactionRuntime
from runtime import textFormatting as textFormattingRuntime
from runtime import viewBases as runtimeViewBases
from features.staff.ribbons import workflow as ribbonWorkflow

CATEGORY_ORDER = ("sacks", "gorget", "spbadge", "commendations", "corpus", "ribbons")
CATEGORY_LABELS = {
    "sacks": "Awards / Medals",
    "gorget": "Gorget",
    "spbadge": "Special Badge",
    "commendations": "Commendations",
    "corpus": "Corpus",
    "ribbons": "Ribbons",
}
FINAL_REQUEST_STATUSES = {"APPROVED", "REJECTED", "CANCELED"}
PENDING_REQUEST_STATUSES = {"PENDING", "NEEDS_INFO"}
NAMEPLATE_MAX_LENGTH = 7


def _toIntSet(values: Any) -> set[int]:
    out: set[int] = set()
    for value in values or []:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            out.add(parsed)
    return out


def _hasAnyRole(member: discord.Member, roleIds: set[int]) -> bool:
    return bool(roleIds) and any(role.id in roleIds for role in member.roles)


def _managerRoleIds() -> set[int]:
    return _toIntSet(getattr(config, "ribbonManagerRoleIds", []))


def _canManageRibbons(member: discord.Member) -> bool:
    if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
        return True
    managerRoleIds = _managerRoleIds()
    if managerRoleIds and _hasAnyRole(member, managerRoleIds):
        return True
    moderatorRoleId = int(getattr(config, "moderatorRoleId", 0) or 0)
    return moderatorRoleId > 0 and any(role.id == moderatorRoleId for role in member.roles)


def _parseDbDateTime(rawValue: Optional[str]) -> Optional[datetime]:
    if not rawValue:
        return None
    text = str(rawValue).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _discordTimestamp(rawValue: Optional[str], style: str = "f") -> str:
    parsed = _parseDbDateTime(rawValue)
    if not parsed:
        return "Unknown"
    return f"<t:{int(parsed.timestamp())}:{style}>"


def _jsonList(rawValue: Any) -> list[str]:
    return ribbonWorkflow.parseJsonList(rawValue)


def _splitSelection(raw: str) -> list[str]:
    values = [part.strip() for part in (raw or "").split(",")]
    return [value for value in values if value]


def _formatAssetNames(names: list[str], limit: int = 12) -> str:
    if not names:
        return "(none)"
    if len(names) <= limit:
        return ", ".join(names)
    return f"{', '.join(names[:limit])}, +{len(names) - limit} more"


def _clipEmbedValue(value: str, limit: int = 1024) -> str:
    return textFormattingRuntime.clipText(value, limit)


def _pageSlice(values: list[Any], pageIndex: int, pageSize: int = 25) -> tuple[list[Any], int]:
    if not values:
        return [], 1
    totalPages = (len(values) + pageSize - 1) // pageSize
    safePage = max(0, min(pageIndex, totalPages - 1))
    start = safePage * pageSize
    end = start + pageSize
    return values[start:end], totalPages


def _statusText(status: str) -> str:
    value = (status or "").upper()
    if value == "APPROVED":
        return ":white_check_mark: Approved"
    if value == "REJECTED":
        return ":x: Rejected"
    if value == "NEEDS_INFO":
        return ":warning: Needs Info"
    if value == "CANCELED":
        return ":stop_sign: Canceled"
    return ":hourglass_flowing_sand: Pending"


class RibbonNameplateModal(discord.ui.Modal, title="Set Nameplate"):
    nameplateInput = discord.ui.TextInput(
        label="Nameplate text",
        style=discord.TextStyle.short,
        required=False,
        max_length=NAMEPLATE_MAX_LENGTH,
        placeholder="Leave blank to keep current value",
    )

    def __init__(self, parentView: "RibbonRequestBuilderView"):
        super().__init__()
        self.parentView = parentView
        self.nameplateInput.default = parentView.nameplateText

    async def on_submit(self, interaction: discord.Interaction) -> None:
        self.parentView.nameplateText = str(self.nameplateInput.value or "").strip()[:NAMEPLATE_MAX_LENGTH]
        await interaction.response.send_message("Nameplate updated.", ephemeral=True)
        await self.parentView.refreshBoundMessage()


class RibbonNeedsInfoModal(discord.ui.Modal, title="Needs Clarification"):
    noteInput = discord.ui.TextInput(
        label="What proof/info is missing?",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=1000,
    )

    def __init__(self, cog: "RibbonCog", requestId: int):
        super().__init__()
        self.cog = cog
        self.requestId = int(requestId)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        note = str(self.noteInput.value or "").strip()
        await self.cog.handleRibbonReviewDecision(
            interaction,
            self.requestId,
            "NEEDS_INFO",
            note or "Please provide additional proof/information.",
        )


class RibbonRemovePickerView(runtimeViewBases.OwnerLockedView):
    def __init__(self, parentView: "RibbonRequestBuilderView"):
        super().__init__(
            openerId=parentView.requester.id,
            timeout=600,
            ownerMessage="Only the original requester can use this remove panel.",
        )
        self.parentView = parentView
        self.pageIndex = 0
        self.boundMessage: Optional[discord.Message] = None
        self._refreshComponents()

    def _formatRemoveOptions(self) -> tuple[list[discord.SelectOption], int]:
        rows = self.parentView._currentLoadoutRows()
        pageRows, totalPages = _pageSlice(rows, self.pageIndex)
        options: list[discord.SelectOption] = []
        for asset in pageRows:
            assetId = str(asset.get("assetId") or "")
            label = str(asset.get("displayName") or assetId)[:100]
            options.append(
                discord.SelectOption(
                    label=label,
                    value=assetId,
                    default=(assetId in self.parentView.selectedRemoveIds),
                )
            )
        if not options:
            options.append(
                discord.SelectOption(
                    label="No current ribbons to remove",
                    value="__none__",
                    default=True,
                )
            )
        return options, totalPages

    def _refreshComponents(self) -> None:
        options, totalPages = self._formatRemoveOptions()
        self.pageIndex = max(0, min(self.pageIndex, max(0, totalPages - 1)))
        self.removeSelect.options = options
        hasRealOptions = not (len(options) == 1 and options[0].value == "__none__")
        self.removeSelect.disabled = not hasRealOptions
        self.removeSelect.min_values = 0
        self.removeSelect.max_values = max(1, min(25, len(options))) if hasRealOptions else 1

        if totalPages > 1:
            self.removeSelect.placeholder = f"Select current ribbons to remove (Page {self.pageIndex + 1}/{totalPages})"
            self.prevButton.label = f"Prev ({self.pageIndex + 1}/{totalPages})"
            self.nextButton.label = f"Next ({self.pageIndex + 1}/{totalPages})"
        else:
            self.removeSelect.placeholder = "Select current ribbons to remove"
            self.prevButton.label = "Prev"
            self.nextButton.label = "Next"
        self.prevButton.disabled = not hasRealOptions or self.pageIndex <= 0
        self.nextButton.disabled = not hasRealOptions or self.pageIndex >= (totalPages - 1)
        self.clearButton.disabled = len(self.parentView.selectedRemoveIds) == 0

    async def refreshBoundMessage(self) -> None:
        self._refreshComponents()
        if not self.boundMessage:
            return
        try:
            await self.boundMessage.edit(view=self)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    @discord.ui.select(
        row=0,
        placeholder="Select current ribbons to remove",
        min_values=0,
        max_values=1,
        options=[discord.SelectOption(label="Loading...", value="__loading__")],
    )
    async def removeSelect(self, interaction: discord.Interaction, select: discord.ui.Select) -> None:
        pageIds = {option.value for option in self.removeSelect.options if option.value != "__none__"}
        self.parentView.selectedRemoveIds.difference_update(pageIds)
        for value in select.values:
            if value != "__none__":
                self.parentView.selectedRemoveIds.add(value)
                self.parentView.selectedAddIds.discard(value)
        await self.parentView.refreshBoundMessage()
        self._refreshComponents()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            content=f"Selected remove items: {len(self.parentView.selectedRemoveIds)}",
            view=self,
        )

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, row=1)
    async def prevButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.pageIndex = max(0, self.pageIndex - 1)
        self._refreshComponents()
        await runtimeViewBases.safeRefreshInteractionMessage(interaction, view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, row=1)
    async def nextButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.pageIndex += 1
        self._refreshComponents()
        await runtimeViewBases.safeRefreshInteractionMessage(interaction, view=self)

    @discord.ui.button(label="Clear Remove", style=discord.ButtonStyle.secondary, row=1)
    async def clearButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.parentView.selectedRemoveIds.clear()
        await self.parentView.refreshBoundMessage()
        self._refreshComponents()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            content="Remove list cleared.",
            view=self,
        )

    @discord.ui.button(label="Done", style=discord.ButtonStyle.success, row=1)
    async def doneButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        for child in self.children:
            if isinstance(child, (discord.ui.Button, discord.ui.Select)):
                child.disabled = True
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            content=f"Remove selection saved ({len(self.parentView.selectedRemoveIds)} selected).",
            view=self,
        )
        self.stop()


class RibbonRequestBuilderView(runtimeViewBases.OwnerLockedView):
    def __init__(
        self,
        cog: "RibbonCog",
        *,
        requester: discord.Member,
        currentRibbonIds: list[str],
        nameplateText: str,
    ):
        super().__init__(
            openerId=requester.id,
            timeout=900,
            ownerMessage="Only the original requester can use this panel.",
        )
        self.cog = cog
        self.requester = requester
        self.boundMessage: Optional[discord.Message] = None
        self.currentRibbonIds = sorted(set(currentRibbonIds))
        self.nameplateText = (nameplateText or "").strip()[:NAMEPLATE_MAX_LENGTH]
        self.initialNameplateText = self.nameplateText
        self.selectedAddIds: set[str] = set()
        self.selectedRemoveIds: set[str] = set()
        self.addCategory = self.cog.defaultCategoryForRequest()
        self.addPageIndex = 0
        self.submitting = False
        self.submitted = False
        self._refreshComponents()

    def hasNameplateChange(self) -> bool:
        currentValue = str(self.nameplateText or "").strip()
        initialValue = str(self.initialNameplateText or "").strip()
        return currentValue != initialValue

    def _getCategoryAssets(self, category: str) -> list[dict[str, Any]]:
        currentIds = set(self.currentRibbonIds)
        assets: list[dict[str, Any]] = []
        for asset in self.cog.assetsByCategory.get(category, []):
            assetId = str(asset.get("assetId") or "").strip()
            if not assetId:
                continue
            if assetId in currentIds:
                # Don't offer already-approved/current loadout assets in the add picker.
                continue
            assets.append(asset)
        return assets

    def _formatCategoryOptions(self) -> list[discord.SelectOption]:
        options: list[discord.SelectOption] = []
        for category in self.cog.categoryOrder():
            count = len(self._getCategoryAssets(category))
            if count <= 0:
                continue
            options.append(
                discord.SelectOption(
                    label=CATEGORY_LABELS.get(category, category.title()),
                    value=category,
                    description=f"{count} assets",
                    default=(category == self.addCategory),
                )
            )
        if not options:
            options.append(
                discord.SelectOption(
                    label="No categories available",
                    value="__none__",
                    default=True,
                )
            )
        return options[:25]

    def _formatAddOptions(self) -> tuple[list[discord.SelectOption], int]:
        assets = self._getCategoryAssets(self.addCategory)
        pageAssets, totalPages = _pageSlice(assets, self.addPageIndex)
        options: list[discord.SelectOption] = []
        for asset in pageAssets:
            assetId = str(asset.get("assetId") or "")
            label = str(asset.get("displayName") or assetId)[:100]
            options.append(
                discord.SelectOption(
                    label=label,
                    value=assetId,
                    default=(assetId in self.selectedAddIds),
                )
            )
        if not options:
            options.append(
                discord.SelectOption(
                    label="No assets in this category",
                    value="__none__",
                    default=True,
                )
            )
        return options, totalPages

    def _currentLoadoutRows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for assetId in self.currentRibbonIds:
            asset = self.cog.assetsById.get(assetId)
            if asset:
                rows.append(asset)
        rows.sort(key=lambda row: str(row.get("displayName") or "").lower())
        return rows

    def _refreshComponents(self) -> None:
        categoryOptions = self._formatCategoryOptions()
        self.categorySelect.options = categoryOptions
        self.categorySelect.disabled = bool(categoryOptions and categoryOptions[0].value == "__none__")
        self.categorySelect.max_values = 1
        self.categorySelect.min_values = 1
        self.categorySelect.placeholder = "Step 1) Choose category"

        addOptions, addPages = self._formatAddOptions()
        self.addPageIndex = max(0, min(self.addPageIndex, max(0, addPages - 1)))
        activeCategoryLabel = CATEGORY_LABELS.get(self.addCategory, self.addCategory.title())
        if addPages > 1:
            self.addSelect.placeholder = (
                f"Step 2) Add items from {activeCategoryLabel} (Page {self.addPageIndex + 1}/{addPages})"
            )
        else:
            self.addSelect.placeholder = f"Step 2) Add items from {activeCategoryLabel}"
        self.addSelect.options = addOptions
        addHasRealOptions = not (len(addOptions) == 1 and addOptions[0].value == "__none__")
        self.addSelect.disabled = not addHasRealOptions
        self.addSelect.min_values = 0
        self.addSelect.max_values = max(1, min(25, len(addOptions))) if addHasRealOptions else 1
        self.prevButton.disabled = not addHasRealOptions or self.addPageIndex <= 0
        self.nextButton.disabled = not addHasRealOptions or self.addPageIndex >= (addPages - 1)
        if addPages > 1:
            self.prevButton.label = f"Prev ({self.addPageIndex + 1}/{addPages})"
            self.nextButton.label = f"Next ({self.addPageIndex + 1}/{addPages})"
        else:
            self.prevButton.label = "Prev"
            self.nextButton.label = "Next"

        currentCount = len(self._currentLoadoutRows())
        self.removeButton.disabled = currentCount <= 0
        self.removeButton.label = (
            f"Manage Removals ({len(self.selectedRemoveIds)})" if currentCount > 0 else "Manage Removals"
        )

        changeCount = len(self.selectedAddIds) + len(self.selectedRemoveIds)
        if self.hasNameplateChange():
            changeCount += 1
        self.submitButton.label = f"Submit Request ({changeCount})"

        self.submitButton.disabled = self.submitting or self.submitted
        self.cancelButton.disabled = self.submitting or self.submitted

    async def refreshBoundMessage(self) -> None:
        self._refreshComponents()
        if not self.boundMessage:
            return
        try:
            await self.boundMessage.edit(embed=self.buildEmbed(), view=self)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    def buildEmbed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Ribbon Request Builder",
            description=(
                "Step 1: Choose a category.\n"
                "Step 2: Select items to add.\n"
                "Step 3: Use Manage Removals if needed, then submit."
            ),
            color=discord.Color.blurple(),
        )
        activeCategoryLabel = CATEGORY_LABELS.get(self.addCategory, self.addCategory.title())
        embed.add_field(name="Active Category", value=activeCategoryLabel, inline=True)
        embed.add_field(
            name="Nameplate",
            value=self.nameplateText or "(blank)",
            inline=True,
        )
        currentSummary = self.cog.summarizeCategoryCounts(self.currentRibbonIds)
        embed.add_field(
            name=f"Current Loadout ({len(self.currentRibbonIds)})",
            value=currentSummary or "(none)",
            inline=False,
        )

        addNames = self.cog.assetNamesForIds(sorted(self.selectedAddIds))
        removeNames = self.cog.assetNamesForIds(sorted(self.selectedRemoveIds))
        embed.add_field(
            name=f"Pending Add ({len(addNames)})",
            value=_formatAssetNames(addNames),
            inline=False,
        )
        embed.add_field(
            name=f"Pending Remove ({len(removeNames)})",
            value=_formatAssetNames(removeNames),
            inline=False,
        )
        embed.set_footer(text="Tip: Choose category first, then pick items.")
        return embed

    @discord.ui.select(
        row=0,
        placeholder="Step 1) Choose category",
        min_values=1,
        max_values=1,
        options=[discord.SelectOption(label="Loading...", value="__loading__")],
    )
    async def categorySelect(self, interaction: discord.Interaction, select: discord.ui.Select) -> None:
        category = str(select.values[0] if select.values else "").strip()
        if not category or category == "__none__":
            return await interaction.response.defer()
        self.addCategory = category
        self.addPageIndex = 0
        self._refreshComponents()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=self.buildEmbed(),
            view=self,
        )

    @discord.ui.select(
        row=1,
        placeholder="Step 2) Select items to add",
        min_values=0,
        max_values=1,
        options=[discord.SelectOption(label="Loading...", value="__loading__")],
    )
    async def addSelect(self, interaction: discord.Interaction, select: discord.ui.Select) -> None:
        beforeCount = len(self.selectedAddIds)
        pageIds = {option.value for option in self.addSelect.options if option.value != "__none__"}
        self.selectedAddIds.difference_update(pageIds)
        for value in select.values:
            if value != "__none__":
                self.selectedAddIds.add(value)
                self.selectedRemoveIds.discard(value)
        self._refreshComponents()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=self.buildEmbed(),
            view=self,
        )
        delta = len(self.selectedAddIds) - beforeCount
        if delta != 0:
            action = "Added" if delta > 0 else "Updated"
            await interaction.followup.send(
                f"{action} selection. Pending add count: {len(self.selectedAddIds)}.",
                ephemeral=True,
            )

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, row=2)
    async def prevButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.addPageIndex = max(0, self.addPageIndex - 1)
        self._refreshComponents()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=self.buildEmbed(),
            view=self,
        )

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, row=2)
    async def nextButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.addPageIndex += 1
        self._refreshComponents()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=self.buildEmbed(),
            view=self,
        )

    @discord.ui.button(label="Manage Removals", style=discord.ButtonStyle.secondary, row=3)
    async def removeButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        removeView = RibbonRemovePickerView(self)
        await interaction.response.send_message(
            content="Select current ribbons to remove:",
            view=removeView,
            ephemeral=True,
        )
        try:
            removeView.boundMessage = await interaction.original_response()
        except (discord.NotFound, discord.HTTPException):
            removeView.boundMessage = None

    @discord.ui.button(label="Set Nameplate", style=discord.ButtonStyle.primary, row=4)
    async def setNameplateButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(interaction, RibbonNameplateModal(self))

    @discord.ui.button(label="Submit Request", style=discord.ButtonStyle.success, row=4)
    async def submitButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.submitting or self.submitted:
            return await interaction.response.send_message(
                "This request is already being submitted.",
                ephemeral=True,
            )
        await self.cog.submitRibbonRequest(interaction, self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, row=4)
    async def cancelButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.submitting = False
        self.submitted = True
        self.disableAll()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            content="Ribbon request canceled.",
            embed=None,
            view=self,
        )

    def disableAll(self) -> None:
        for child in self.children:
            if isinstance(child, (discord.ui.Button, discord.ui.Select)):
                child.disabled = True
        self.stop()


class RibbonReviewView(discord.ui.View):
    def __init__(self, cog: "RibbonCog", requestId: int, status: str = "PENDING"):
        super().__init__(timeout=None)
        self.cog = cog
        self.requestId = int(requestId)
        self.status = str(status or "PENDING").upper()
        self.approveButton.custom_id = f"ribbons:approve:{self.requestId}"
        self.rejectButton.custom_id = f"ribbons:reject:{self.requestId}"
        self.needsInfoButton.custom_id = f"ribbons:needsinfo:{self.requestId}"
        self.fullListButton.custom_id = f"ribbons:fulllist:{self.requestId}"
        if self.status in FINAL_REQUEST_STATUSES:
            self.disableAll()

    def disableAll(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

    @discord.ui.button(label="Approve Request", style=discord.ButtonStyle.success, row=0)
    async def approveButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.handleRibbonReviewDecision(interaction, self.requestId, "APPROVED", "")

    @discord.ui.button(label="Reject Request", style=discord.ButtonStyle.danger, row=0)
    async def rejectButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.handleRibbonReviewDecision(interaction, self.requestId, "REJECTED", "")

    @discord.ui.button(label="Needs Info", style=discord.ButtonStyle.secondary, row=0)
    async def needsInfoButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            RibbonNeedsInfoModal(self.cog, self.requestId),
        )

    @discord.ui.button(label="View Full List", style=discord.ButtonStyle.secondary, row=1)
    async def fullListButton(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.sendFullList(interaction, self.requestId)



