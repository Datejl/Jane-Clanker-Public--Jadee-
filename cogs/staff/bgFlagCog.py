from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

import config
from features.staff.bgflags import service as flagService
from runtime import interaction as interactionRuntime
from runtime import viewBases as runtimeViewBases
from features.staff.sessions import roblox

_modsOnlyMessage = "Mods only."

_flagTypeChoices = [
    app_commands.Choice(name="Group", value="group"),
    app_commands.Choice(name="Username", value="username"),
    app_commands.Choice(name="Keyword", value="keyword"),
    app_commands.Choice(name="Group Keyword", value="group_keyword"),
    app_commands.Choice(name="Item Keyword", value="item_keyword"),
    app_commands.Choice(name="Item", value="item"),
    app_commands.Choice(name="Creator", value="creator"),
    app_commands.Choice(name="Badge", value="badge"),
]
_flagTypeValues = {choice.value for choice in _flagTypeChoices}
_numericRuleTypes = {"group", "item", "creator", "badge"}
_rulesCategoryChoices = [
    discord.SelectOption(label="Groups", value="groups"),
    discord.SelectOption(label="Items / Accessories", value="items"),
    discord.SelectOption(label="Keywords", value="keywords"),
    discord.SelectOption(label="Badges", value="badges"),
]
_rulesCategoryTypeMap = {
    "groups": {"group"},
    "items": {"item", "creator"},
    "keywords": {"keyword", "username", "group_keyword", "item_keyword"},
    "badges": {"badge"},
}


def _hasModPerm(member: discord.Member) -> bool:
    roleId = getattr(config, "moderatorRoleId", None)
    if roleId is None:
        return True
    return any(int(role.id) == int(roleId) for role in member.roles)


async def _requireModPermission(interaction: discord.Interaction) -> bool:
    member = interaction.user
    if isinstance(member, discord.Member) and _hasModPerm(member):
        return True
    await interactionRuntime.safeInteractionReply(
        interaction,
        content=_modsOnlyMessage,
        ephemeral=True,
    )
    return False


def _normalizeRuleType(value: str) -> Optional[str]:
    normalized = str(value or "").strip().lower()
    if normalized in _flagTypeValues:
        return normalized
    return None


def _rulesListText(rules: list[dict]) -> str:
    lines: list[str] = []
    for rule in rules[:40]:
        note = f" - {rule['note']}" if rule.get("note") else ""
        lines.append(f"#{rule['ruleId']} [{rule['ruleType']}] {rule['ruleValue']}{note}")
    if len(rules) > 40:
        lines.append(f"... and {len(rules) - 40} more")
    return "\n".join(lines)


def _ruleField(rule: dict) -> tuple[str, str]:
    ruleId = int(rule.get("ruleId") or 0)
    ruleType = str(rule.get("ruleType") or "").strip().lower()
    ruleValue = str(rule.get("ruleValue") or "").strip()
    note = str(rule.get("note") or "").strip()
    fieldName = f"#{ruleId} [{ruleType}]"
    fieldValue = f"Value: `{ruleValue}`\nNote: {note if note else '(none)'}"
    return fieldName, fieldValue


class BgRulesPanelView(discord.ui.View):
    def __init__(self, rules: list[dict], *, pageSize: int = 5) -> None:
        super().__init__(timeout=600)
        self.rules = list(rules or [])
        self.pageSize = max(1, min(10, int(pageSize)))
        self.selectedCategory = "groups"
        self.pageIndex = 0
        self._syncControls()

    def _filteredRules(self) -> list[dict]:
        allowedTypes = _rulesCategoryTypeMap.get(self.selectedCategory, set())
        if not allowedTypes:
            return []
        return [
            rule for rule in self.rules
            if str(rule.get("ruleType") or "").strip().lower() in allowedTypes
        ]

    def _pageCount(self, filtered: list[dict]) -> int:
        if not filtered:
            return 1
        return max(1, (len(filtered) + self.pageSize - 1) // self.pageSize)

    def _buildEmbed(self) -> discord.Embed:
        filtered = self._filteredRules()
        pageCount = self._pageCount(filtered)
        self.pageIndex = min(max(0, self.pageIndex), pageCount - 1)
        start = self.pageIndex * self.pageSize
        end = min(len(filtered), start + self.pageSize)
        pageRows = filtered[start:end]

        categoryLabel = next(
            (choice.label for choice in _rulesCategoryChoices if choice.value == self.selectedCategory),
            self.selectedCategory.title(),
        )
        embed = discord.Embed(
            title="BG Flag Rules",
            description=(
                f"Category: **{categoryLabel}**\n"
                f"Page **{self.pageIndex + 1}/{pageCount}** | "
                f"Showing **{start + 1 if filtered else 0}-{end}** of **{len(filtered)}**"
            ),
            color=discord.Color.blurple(),
        )
        if not pageRows:
            embed.add_field(name="Rules", value="No rules in this category.", inline=False)
        else:
            for rule in pageRows:
                fieldName, fieldValue = _ruleField(rule)
                embed.add_field(name=fieldName, value=fieldValue, inline=False)
        embed.set_footer(text="Use the dropdown to switch category and buttons to change pages.")
        return embed

    def _syncControls(self) -> None:
        filtered = self._filteredRules()
        pageCount = self._pageCount(filtered)
        self.pageIndex = min(max(0, self.pageIndex), pageCount - 1)
        self.prevBtn.disabled = self.pageIndex <= 0
        self.nextBtn.disabled = self.pageIndex >= pageCount - 1
        self.typeSelect.placeholder = f"Category: {self.selectedCategory}"

    async def _refresh(self, interaction: discord.Interaction) -> None:
        self._syncControls()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=self._buildEmbed(),
            view=self,
        )

    @discord.ui.select(
        row=0,
        options=_rulesCategoryChoices,
        placeholder="Select rule category",
        min_values=1,
        max_values=1,
    )
    async def typeSelect(self, interaction: discord.Interaction, select: discord.ui.Select) -> None:
        value = str(select.values[0] if select.values else "").strip().lower()
        if value not in _rulesCategoryTypeMap:
            await interactionRuntime.safeInteractionReply(
                interaction,
                content="Invalid rule category selection.",
                ephemeral=True,
            )
            return
        self.selectedCategory = value
        self.pageIndex = 0
        await self._refresh(interaction)

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, row=1)
    async def prevBtn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        self.pageIndex = max(0, self.pageIndex - 1)
        await self._refresh(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, row=1)
    async def nextBtn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        self.pageIndex += 1
        await self._refresh(interaction)


class BgFlagAddRuleModal(discord.ui.Modal, title="Add BG Flag Rule"):
    ruleType = discord.ui.TextInput(
        label="Rule Type",
        placeholder="group / username / keyword / group_keyword / item_keyword / item / creator / badge",
        required=True,
        max_length=32,
    )
    value = discord.ui.TextInput(
        label="Rule Value",
        placeholder="ID (numeric) or lowercase value/keyword",
        required=True,
        max_length=200,
    )
    note = discord.ui.TextInput(
        label="Note (optional)",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=300,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await _requireModPermission(interaction):
            return

        normalizedRuleType = _normalizeRuleType(str(self.ruleType))
        if not normalizedRuleType:
            valid = ", ".join(sorted(_flagTypeValues))
            await interactionRuntime.safeInteractionReply(
                interaction,
                content=f"Invalid rule type. Valid options: {valid}",
                ephemeral=True,
            )
            return

        rawValue = str(self.value).strip()
        if normalizedRuleType in _numericRuleTypes:
            try:
                parsed = int(rawValue)
            except ValueError:
                await interactionRuntime.safeInteractionReply(
                    interaction,
                    content="IDs must be numeric for group/item/creator/badge rules.",
                    ephemeral=True,
                )
                return
            normalizedValue = str(parsed)
        else:
            normalizedValue = rawValue.lower()

        noteText = str(self.note).strip() or None
        ruleId = await flagService.addRule(
            normalizedRuleType,
            normalizedValue,
            noteText,
            interaction.user.id,
        )
        await interactionRuntime.safeInteractionReply(
            interaction,
            content=f"Added {normalizedRuleType} rule #{ruleId}.",
            ephemeral=True,
        )


class BgFlagRemoveRuleModal(discord.ui.Modal, title="Remove BG Flag Rule"):
    ruleId = discord.ui.TextInput(
        label="Rule ID",
        placeholder="Numeric rule ID",
        required=True,
        max_length=20,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await _requireModPermission(interaction):
            return

        try:
            parsedRuleId = int(str(self.ruleId).strip())
        except ValueError:
            await interactionRuntime.safeInteractionReply(
                interaction,
                content="Rule ID must be numeric.",
                ephemeral=True,
            )
            return

        await flagService.removeRule(parsedRuleId)
        await interactionRuntime.safeInteractionReply(
            interaction,
            content=f"Removed rule #{parsedRuleId}.",
            ephemeral=True,
        )


class BgFlagImportBadgesModal(discord.ui.Modal, title="Import Badge Rules"):
    universeId = discord.ui.TextInput(
        label="Universe ID",
        placeholder="Numeric Roblox universe ID",
        required=True,
        max_length=30,
    )
    maxBadges = discord.ui.TextInput(
        label="Max Badges (optional)",
        placeholder="Default from config if blank",
        required=False,
        max_length=6,
    )
    note = discord.ui.TextInput(
        label="Note (optional)",
        required=False,
        max_length=200,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await _requireModPermission(interaction):
            return

        try:
            parsedUniverseId = int(str(self.universeId).strip())
        except ValueError:
            await interactionRuntime.safeInteractionReply(
                interaction,
                content="Universe ID must be numeric.",
                ephemeral=True,
            )
            return

        rawMax = str(self.maxBadges).strip()
        if rawMax:
            try:
                limit = int(rawMax)
            except ValueError:
                await interactionRuntime.safeInteractionReply(
                    interaction,
                    content="Max badges must be numeric.",
                    ephemeral=True,
                )
                return
        else:
            limit = int(getattr(config, "robloxBadgeImportMax", 200) or 200)

        if limit <= 0:
            await interactionRuntime.safeInteractionReply(
                interaction,
                content="Max badges must be greater than 0.",
                ephemeral=True,
            )
            return

        await interactionRuntime.safeInteractionDefer(interaction, ephemeral=True)

        existing = await flagService.listRules("badge")
        existingIds: set[int] = set()
        for rule in existing:
            try:
                existingIds.add(int(rule.get("ruleValue")))
            except (TypeError, ValueError):
                continue

        added = 0
        skipped = 0
        cursor: Optional[str] = None
        sortOrder = "Asc"
        batchSize = 100
        noteSuffix = str(self.note).strip() or None

        while added < limit:
            pageLimit = min(batchSize, limit - added)
            result = await roblox.fetchRobloxUniverseBadges(
                parsedUniverseId,
                limit=pageLimit,
                cursor=cursor,
                sortOrder=sortOrder,
            )
            if result.error:
                await interactionRuntime.safeInteractionReply(
                    interaction,
                    content=f"Import stopped: {result.error}",
                    ephemeral=True,
                )
                return
            if not result.badges:
                break

            for badge in result.badges:
                badgeId = badge.get("id")
                if badgeId is None:
                    continue
                if badgeId in existingIds:
                    skipped += 1
                    continue
                badgeName = badge.get("name")
                noteParts = []
                if badgeName:
                    noteParts.append(str(badgeName))
                noteParts.append(f"universe {parsedUniverseId}")
                if noteSuffix:
                    noteParts.append(noteSuffix)
                noteText = " | ".join(noteParts) if noteParts else None
                await flagService.addRule("badge", str(badgeId), noteText, interaction.user.id)
                existingIds.add(badgeId)
                added += 1
                if added >= limit:
                    break

            cursor = result.nextCursor
            if not cursor:
                break

        await interactionRuntime.safeInteractionReply(
            interaction,
            content=f"Imported {added} badge rules (skipped {skipped}).",
            ephemeral=True,
        )


class BgFlagPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=600)

    @discord.ui.button(label="Add Rule", style=discord.ButtonStyle.success, row=0)
    async def addRuleBtn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await _requireModPermission(interaction):
            return
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            BgFlagAddRuleModal(),
        )

    @discord.ui.button(label="Remove Rule", style=discord.ButtonStyle.danger, row=0)
    async def removeRuleBtn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await _requireModPermission(interaction):
            return
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            BgFlagRemoveRuleModal(),
        )

    @discord.ui.button(label="List Rules", style=discord.ButtonStyle.secondary, row=0)
    async def listRulesBtn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await _requireModPermission(interaction):
            return
        rules = await flagService.listRules(None)
        if not rules:
            await interactionRuntime.safeInteractionReply(
                interaction,
                content="No rules found.",
                ephemeral=True,
            )
            return
        view = BgRulesPanelView(rules)
        await interactionRuntime.safeInteractionReply(
            interaction,
            embed=view._buildEmbed(),
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(label="Import Badges", style=discord.ButtonStyle.primary, row=0)
    async def importBadgesBtn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await _requireModPermission(interaction):
            return
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            BgFlagImportBadgesModal(),
        )


class BgFlagCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="bg-flag", description="Open the background-check flag manager panel.")
    async def bgFlagPanel(self, interaction: discord.Interaction) -> None:
        if not await _requireModPermission(interaction):
            return

        embed = discord.Embed(
            title="BG Flag Manager",
            description=(
                "Use the panel buttons below to manage background-check flags.\n"
                "Supported rule types:\n"
                "`group`, `username`, `keyword`, `group_keyword`, `item_keyword`, `item`, `creator`, `badge`"
            ),
            color=discord.Color.blurple(),
        )
        await interactionRuntime.safeInteractionReply(
            interaction,
            embed=embed,
            view=BgFlagPanelView(),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(BgFlagCog(bot))

