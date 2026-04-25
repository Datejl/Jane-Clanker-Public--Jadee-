from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands

import config
from features.staff.ribbons import service as ribbonService
from features.staff.ribbons import workflow as ribbonWorkflow
from runtime import taskBudgeter

log = logging.getLogger(__name__)
_leadingPrefixRegex = re.compile(r"^\[[^\]]+\]\s*")
from features.staff.ribbons.ribbonUi import (
    CATEGORY_ORDER,
    CATEGORY_LABELS,
    FINAL_REQUEST_STATUSES,
    NAMEPLATE_MAX_LENGTH,
    RibbonRequestBuilderView,
    RibbonReviewView,
    _canManageRibbons,
    _clipEmbedValue,
    _discordTimestamp,
    _formatAssetNames,
    _jsonList,
    _managerRoleIds,
    _parseDbDateTime,
    _splitSelection,
    _statusText,
    _toIntSet,
)
from features.staff.ribbons.cogMixin import RibbonCogMixin


class RibbonCog(RibbonCogMixin, commands.Cog):
    ribbonGroup = app_commands.Group(
        name="ribbon",
        description="Ribbon request tools.",
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.assetsById: dict[str, dict[str, Any]] = {}
        self.assetsByCategory: dict[str, list[dict[str, Any]]] = {}
        self.rulesConfig: dict[str, Any] = {}
        self.rulesConfigUpdatedAtUtc: Optional[datetime] = None
        self.requestLocks: dict[int, asyncio.Lock] = {}
        self._reloadRulesConfig()

    async def ribbonStatus(self, interaction: discord.Interaction) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message(
                "This command can only be used in a server.",
                ephemeral=True,
            )
        if not _canManageRibbons(interaction.user):
            return await interaction.response.send_message(
                "You are not authorized to use ribbon manager commands.",
                ephemeral=True,
            )

        await self._ensureCatalogLoaded()
        status = ribbonService.getEngineStatus()
        openRequests = await ribbonWorkflow.getOpenRibbonRequestForUser(interaction.user.id)
        embed = discord.Embed(
            title="Ribbon System Status",
            color=discord.Color.green() if status.available else discord.Color.orange(),
        )
        embed.add_field(name="Engine Available", value="Yes" if status.available else "No", inline=True)
        embed.add_field(name="Active Assets", value=str(len(self.assetsById)), inline=True)
        embed.add_field(name="Rules Loaded", value=str(len(self.rulesConfig.get("rules", []))), inline=True)
        for category in self.categoryOrder():
            embed.add_field(
                name=CATEGORY_LABELS.get(category, category.title()),
                value=str(len(self.assetsByCategory.get(category, []))),
                inline=True,
            )
        if status.reason:
            embed.add_field(name="Details", value=status.reason[:1024], inline=False)
        if openRequests:
            embed.add_field(
                name="Your Open Request",
                value="You currently have an open ribbon request.",
                inline=False,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def ribbonAssets(
        self,
        interaction: discord.Interaction,
        category: Optional[str] = None,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message(
                "This command can only be used in a server.",
                ephemeral=True,
            )
        if not _canManageRibbons(interaction.user):
            return await interaction.response.send_message(
                "You are not authorized to use ribbon manager commands.",
                ephemeral=True,
            )

        await self._ensureCatalogLoaded()
        categoryKey = str(category or "").strip().lower()
        if categoryKey:
            rows = self.assetsByCategory.get(categoryKey)
            if rows is None:
                valid = ", ".join(self.categoryOrder()) or "none"
                return await interaction.response.send_message(
                    f"Unknown category. Use one of: {valid}",
                    ephemeral=True,
                )
            names = [str(row.get("displayName") or "") for row in rows if str(row.get("displayName") or "").strip()]
            lines = names[:80]
            if len(names) > 80:
                lines.append(f"... and {len(names) - 80} more")
            return await interaction.response.send_message(
                f"**{categoryKey}**\n" + ("\n".join(lines) if lines else "(none)"),
                ephemeral=True,
            )

        lines = []
        for key in self.categoryOrder():
            lines.append(f"{key}: {len(self.assetsByCategory.get(key, []))}")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    async def ribbonSyncCatalog(self, interaction: discord.Interaction) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message(
                "This command can only be used in a server.",
                ephemeral=True,
            )
        if not _canManageRibbons(interaction.user):
            return await interaction.response.send_message(
                "You are not authorized to use ribbon manager commands.",
                ephemeral=True,
            )
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            summary = await self.refreshCatalogFromDisk()
        except Exception as exc:
            return await interaction.followup.send(f"Catalog sync failed: {exc}", ephemeral=True)

        await interaction.followup.send(
            (
                "Catalog synced.\n"
                f"Discovered: {summary.get('discovered', 0)}\n"
                f"Inserted: {summary.get('inserted', 0)}\n"
                f"Updated: {summary.get('updated', 0)}\n"
                f"Retired: {summary.get('retired', 0)}\n"
                f"Revived: {summary.get('revived', 0)}"
            ),
            ephemeral=True,
        )

    async def ribbonRender(
        self,
        interaction: discord.Interaction,
        selection: str,
        nameplate: Optional[str] = "",
        strict: bool = False,
        allow_blank_name: bool = False,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message(
                "This command can only be used in a server.",
                ephemeral=True,
            )
        if not _canManageRibbons(interaction.user):
            return await interaction.response.send_message(
                "You are not authorized to use ribbon manager commands.",
                ephemeral=True,
            )

        selectedNames = _splitSelection(selection)
        if not selectedNames:
            return await interaction.response.send_message(
                "Selection cannot be blank. Provide comma-separated names.",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True, thinking=True)
        path = ""
        try:
            result = await taskBudgeter.runThreaded(
                "backgroundJobs",
                ribbonService.renderSelectionToTemp,
                selectedNames=selectedNames,
                nameplate=(nameplate or "")[:NAMEPLATE_MAX_LENGTH],
                strict=bool(strict),
                allowBlankName=bool(allow_blank_name),
                embedMetadata=True,
            )
        except Exception as exc:
            return await interaction.followup.send(f"Ribbon render failed: {exc}", ephemeral=True)

        path = str(result.get("path") or "")
        if not path:
            return await interaction.followup.send("Ribbon render failed: no output file produced.", ephemeral=True)

        unknown = result.get("unknown") or []
        summary = f"Rendered {int(result.get('usedCount', 0))} selected assets."
        if unknown:
            summary += f" Ignored unknown: {', '.join(unknown[:10])}"
        file = discord.File(path, filename="ribbon.png")
        try:
            await interaction.followup.send(content=summary, file=file, ephemeral=True)
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    @ribbonGroup.command(name="request", description="Create a ribbon delta request (add/remove).")
    async def ribbonsRequest(self, interaction: discord.Interaction) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message(
                "This command can only be used in a server.",
                ephemeral=True,
            )
        await self._ensureCatalogLoaded()
        self._reloadRulesConfig()

        openRequest = await ribbonWorkflow.getOpenRibbonRequestForUser(interaction.user.id)
        if openRequest:
            return await interaction.response.send_message(
                "You already have an open ribbon request.",
                ephemeral=True,
            )

        if not _canManageRibbons(interaction.user):
            latestRequest = await ribbonWorkflow.getLatestRibbonRequestForUser(interaction.user.id)
            if latestRequest:
                createdAt = _parseDbDateTime(latestRequest.get("createdAt"))
                cooldownDuration = self._requestCooldownDuration()
                if createdAt and cooldownDuration.total_seconds() > 0:
                    if not self._shouldBypassCooldownForCreatedAt(createdAt):
                        cooldownUntil = createdAt + cooldownDuration
                        if datetime.now(timezone.utc) < cooldownUntil:
                            return await interaction.response.send_message(
                                f"You are on cooldown. You can submit again {_discordTimestamp(cooldownUntil.isoformat(), 'R')}.",
                                ephemeral=True,
                            )

        profile = await ribbonWorkflow.getRibbonProfile(interaction.user.id)
        currentIds = _jsonList(profile.get("currentRibbonIdsJson")) if profile else []
        defaultNameplate = str((profile or {}).get("nameplateText") or "").strip()[:NAMEPLATE_MAX_LENGTH]
        if not defaultNameplate:
            cleanedDisplayName = self._cleanDisplayNameForNameplate(interaction.user.display_name)
            defaultNameplate = cleanedDisplayName[:NAMEPLATE_MAX_LENGTH]

        view = RibbonRequestBuilderView(
            self,
            requester=interaction.user,
            currentRibbonIds=currentIds,
            nameplateText=defaultNameplate,
        )
        await interaction.response.send_message(
            embed=view.buildEmbed(),
            view=view,
            ephemeral=True,
        )
        try:
            view.boundMessage = await interaction.original_response()
        except (discord.NotFound, discord.HTTPException):
            view.boundMessage = None

    async def ribbonsProfile(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message(
                "This command can only be used in a server.",
                ephemeral=True,
            )
        await self._ensureCatalogLoaded()

        targetUser = user or interaction.user
        if targetUser.id != interaction.user.id and not _canManageRibbons(interaction.user):
            return await interaction.response.send_message(
                "You are not authorized to view other users' ribbon profiles.",
                ephemeral=True,
            )

        profileRow = await ribbonWorkflow.getRibbonProfile(targetUser.id)
        if not profileRow:
            return await interaction.response.send_message(
                f"No stored ribbon loadout found for {targetUser.mention}.",
                ephemeral=True,
            )

        currentIds = _jsonList(profileRow.get("currentRibbonIdsJson"))
        medalIds = _jsonList(profileRow.get("medalSelectionJson"))
        currentNames = self.assetNamesForIds(currentIds)
        medalNames = self.assetNamesForIds(medalIds)

        embed = discord.Embed(
            title=f"Ribbon Profile - {targetUser.display_name}",
            color=discord.Color.blurple(),
        )
        embed.add_field(name="User", value=f"{targetUser.mention} (`{targetUser.id}`)", inline=False)
        embed.add_field(name="Nameplate", value=str(profileRow.get("nameplateText") or "(blank)"), inline=True)
        embed.add_field(name="Total Ribbons", value=str(len(currentIds)), inline=True)
        embed.add_field(name="Category Counts", value=self.summarizeCategoryCounts(currentIds), inline=False)
        embed.add_field(name="Medals", value=_formatAssetNames(medalNames, limit=8), inline=False)
        embed.add_field(name="Current Loadout", value=_formatAssetNames(currentNames, limit=20), inline=False)
        lastImagePath = str(profileRow.get("lastGeneratedImagePath") or "").strip()
        if lastImagePath:
            embed.add_field(name="Last Generated Image", value=f"[Open]({lastImagePath})", inline=False)
        embed.set_footer(text=f"Last updated: {_discordTimestamp(profileRow.get('updatedAt'), 'f')}")
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(RibbonCog(bot))

