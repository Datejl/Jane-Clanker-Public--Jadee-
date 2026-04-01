from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import discord
from discord.ext import commands

import config
from features.staff.ribbons import service as ribbonService
from features.staff.ribbons import workflow as ribbonWorkflow
from features.staff.ribbons import workflowBridge as ribbonWorkflowBridge
from features.staff.workflows import rendering as workflowRendering
from runtime import taskBudgeter
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

log = logging.getLogger(__name__)
_leadingPrefixRegex = re.compile(r"^\[[^\]]+\]\s*")


class RibbonCogMixin:
    async def cog_load(self) -> None:
        try:
            summary = await self.refreshCatalogFromDisk()
            log.info(
                "Ribbon catalog sync: discovered=%d inserted=%d updated=%d retired=%d revived=%d",
                summary.get("discovered", 0),
                summary.get("inserted", 0),
                summary.get("updated", 0),
                summary.get("retired", 0),
                summary.get("revived", 0),
            )
        except Exception:
            log.exception("Ribbon catalog sync failed during startup.")

        try:
            workflowRows = await ribbonWorkflow.listRibbonRequestsForWorkflowReconciliation()
            reconciled, changed = await ribbonWorkflowBridge.reconcileRibbonWorkflowRows(workflowRows)
            log.info(
                "Ribbon workflow reconciliation: checked=%d changed=%d",
                reconciled,
                changed,
            )
        except Exception:
            log.exception("Ribbon workflow reconciliation failed during startup.")

        await self._restoreRibbonReviewViews()

    async def refreshCatalogFromDisk(self) -> dict[str, int]:
        discoveredAssets = await taskBudgeter.runThreaded("backgroundJobs", ribbonWorkflow.scanAssetCatalog)
        summary = await ribbonWorkflow.syncCatalogToDb(discoveredAssets)
        activeRows = await ribbonWorkflow.getActiveAssets()
        self.assetsById = {str(row["assetId"]): row for row in activeRows}
        self.assetsByCategory = {}
        for row in activeRows:
            category = str(row.get("category") or "").strip().lower()
            if category not in self.assetsByCategory:
                self.assetsByCategory[category] = []
            self.assetsByCategory[category].append(row)
        for category in self.assetsByCategory:
            self.assetsByCategory[category].sort(key=lambda row: str(row.get("displayName") or "").lower())
        self._reloadRulesConfig()
        return summary

    def _resolveRulesConfigPath(self) -> str:
        rawPath = str(
            getattr(config, "ribbonRulesPath", "configData/ribbons.json")
            or "configData/ribbons.json"
        ).strip()
        if not rawPath:
            rawPath = "configData/ribbons.json"
        path = Path(rawPath)
        if path.is_absolute():
            return str(path)
        configDir = Path(getattr(config, "__file__", __file__)).resolve().parent
        return str((configDir / path).resolve())

    def _rulesConfigUpdatedAt(self) -> Optional[datetime]:
        try:
            modifiedTs = os.path.getmtime(self._resolveRulesConfigPath())
        except OSError:
            return None
        return datetime.fromtimestamp(modifiedTs, tz=timezone.utc)

    def _reloadRulesConfig(self) -> None:
        self.rulesConfig = ribbonWorkflow.loadEligibilityRules()
        self.rulesConfigUpdatedAtUtc = self._rulesConfigUpdatedAt()

    def _requestCooldownDuration(self) -> timedelta:
        rawMinutes = self.rulesConfig.get("requestCooldownMinutes")
        if rawMinutes is not None:
            try:
                minutesValue = float(rawMinutes)
            except (TypeError, ValueError):
                minutesValue = 0.0
            return timedelta(minutes=max(0.0, minutesValue))

        rawHours = self.rulesConfig.get("requestCooldownHours", 24)
        try:
            hoursValue = float(rawHours)
        except (TypeError, ValueError):
            hoursValue = 24.0
        return timedelta(hours=max(0.0, hoursValue))

    def _shouldBypassCooldownForCreatedAt(self, createdAt: datetime) -> bool:
        cutoff = self.rulesConfigUpdatedAtUtc
        if cutoff is None:
            return False
        return createdAt < cutoff

    async def _ensureCatalogLoaded(self) -> None:
        if self.assetsById:
            return
        await self.refreshCatalogFromDisk()

    def _reviewViewExpiryDuration(self) -> timedelta:
        rawHours = self.rulesConfig.get("reviewViewExpiryHours", 48)
        try:
            hoursValue = float(rawHours)
        except (TypeError, ValueError):
            hoursValue = 48.0
        return timedelta(hours=max(1.0, hoursValue))

    def _isReviewRequestExpired(
        self,
        requestRow: dict[str, Any],
        *,
        nowUtc: Optional[datetime] = None,
    ) -> bool:
        createdAtUtc = _parseDbDateTime(str(requestRow.get("createdAt") or ""))
        if createdAtUtc is None:
            return False
        now = nowUtc or datetime.now(timezone.utc)
        return (now - createdAtUtc) >= self._reviewViewExpiryDuration()

    async def _expireReviewRequest(
        self,
        requestRow: dict[str, Any],
        *,
        reason: str,
        clearReviewMessage: bool,
    ) -> None:
        requestId = int(requestRow.get("requestId") or 0)
        if requestId <= 0:
            return
        currentStatus = str(requestRow.get("status") or "").upper()
        if currentStatus in FINAL_REQUEST_STATUSES:
            if clearReviewMessage:
                await ribbonWorkflow.setRibbonRequestReviewMessage(requestId, 0, 0)
            return
        await ribbonWorkflow.setRibbonRequestStatus(
            requestId,
            status="CANCELED",
            reviewerId=None,
            reviewNote=reason,
        )
        await ribbonWorkflow.addRibbonRequestEvent(
            requestId,
            None,
            "AUTO_CANCELED",
            reason,
        )
        refreshedRow = await ribbonWorkflow.getRibbonRequestById(requestId)
        if refreshedRow:
            await ribbonWorkflowBridge.syncRibbonWorkflow(
                refreshedRow,
                stateKey="canceled",
                actorId=None,
                note=reason,
                eventType="AUTO_CANCELED",
            )
        if clearReviewMessage:
            await ribbonWorkflow.setRibbonRequestReviewMessage(requestId, 0, 0)

    async def _resolveReviewMessage(
        self,
        reviewChannelId: int,
        reviewMessageId: int,
    ) -> tuple[Optional[discord.Message], str]:
        if reviewChannelId <= 0 or reviewMessageId <= 0:
            return None, "missing_pointer"
        channel = self.bot.get_channel(reviewChannelId)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(reviewChannelId)
            except discord.NotFound:
                return None, "missing_channel"
            except discord.Forbidden:
                return None, "forbidden_channel"
            except discord.HTTPException:
                return None, "fetch_channel_error"
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return None, "invalid_channel_type"
        try:
            message = await channel.fetch_message(reviewMessageId)
            return message, "ok"
        except discord.NotFound:
            return None, "missing_message"
        except discord.Forbidden:
            return None, "forbidden_message"
        except discord.HTTPException:
            return None, "fetch_message_error"

    async def _restoreRibbonReviewViews(self) -> None:
        try:
            rows = await ribbonWorkflow.listRibbonRequestsForReviewViews()
        except Exception:
            log.exception("Failed to restore ribbon review views.")
            return

        restored = 0
        expired = 0
        orphaned = 0
        skipped = 0
        nowUtc = datetime.now(timezone.utc)
        expiryHours = int(self._reviewViewExpiryDuration().total_seconds() // 3600)

        for row in rows:
            requestId = int(row.get("requestId") or 0)
            messageId = int(row.get("reviewMessageId") or 0)
            channelId = int(row.get("reviewChannelId") or 0)
            if requestId <= 0:
                continue

            if self._isReviewRequestExpired(row, nowUtc=nowUtc):
                await self._expireReviewRequest(
                    row,
                    reason=f"Auto-canceled: review card expired after {expiryHours} hour(s).",
                    clearReviewMessage=True,
                )
                expired += 1
                continue

            message, resolution = await self._resolveReviewMessage(channelId, messageId)
            if message is None:
                if resolution in {"missing_pointer", "missing_channel", "missing_message"}:
                    await self._expireReviewRequest(
                        row,
                        reason="Auto-canceled: review message is no longer available.",
                        clearReviewMessage=True,
                    )
                    orphaned += 1
                else:
                    skipped += 1
                continue

            self.bot.add_view(
                RibbonReviewView(
                    self,
                    requestId,
                    str(row.get("status") or "PENDING"),
                ),
                message_id=messageId,
            )
            restored += 1

        log.info(
            "Ribbon review views restored: active=%d expired=%d orphaned=%d skipped=%d",
            restored,
            expired,
            orphaned,
            skipped,
        )

    def categoryOrder(self) -> list[str]:
        ordered = [category for category in CATEGORY_ORDER if category in self.assetsByCategory]
        remaining = [category for category in self.assetsByCategory.keys() if category not in ordered]
        ordered.extend(sorted(remaining))
        return ordered

    def defaultCategoryForRequest(self) -> str:
        if self.assetsByCategory.get("ribbons"):
            return "ribbons"
        for category in CATEGORY_ORDER:
            if self.assetsByCategory.get(category):
                return category
        for category in self.assetsByCategory.keys():
            return category
        return "ribbons"

    def assetNamesForIds(self, assetIds: list[str]) -> list[str]:
        names: list[str] = []
        for assetId in assetIds:
            asset = self.assetsById.get(assetId)
            if not asset:
                continue
            names.append(str(asset.get("displayName") or assetId))
        return names

    def _cleanDisplayNameForNameplate(self, displayName: str) -> str:
        text = str(displayName or "").strip()
        if not text:
            return ""
        if text.startswith("["):
            cleaned = _leadingPrefixRegex.sub("", text).strip()
            if cleaned:
                return cleaned
        return text

    def summarizeCategoryCounts(self, assetIds: list[str]) -> str:
        counts: dict[str, int] = {}
        for assetId in assetIds:
            asset = self.assetsById.get(assetId)
            if not asset:
                continue
            category = str(asset.get("category") or "").strip().lower()
            counts[category] = counts.get(category, 0) + 1
        if not counts:
            return "(none)"
        parts: list[str] = []
        for category in self.categoryOrder():
            count = counts.get(category, 0)
            if count <= 0:
                continue
            parts.append(f"{CATEGORY_LABELS.get(category, category.title())}: {count}")
        return ", ".join(parts) if parts else "(none)"

    def reviewRoleIds(self) -> set[int]:
        return _managerRoleIds()

    def canReview(self, member: discord.Member) -> bool:
        return _canManageRibbons(member)

    def reviewMentions(self) -> str:
        pingRoleIds = _toIntSet(getattr(config, "ribbonRequestPingRoleIds", []))
        roleIds = sorted(pingRoleIds or self.reviewRoleIds())
        return " ".join(f"<@&{roleId}>" for roleId in roleIds)

    async def resolveReviewChannel(
        self,
        guild: discord.Guild,
        fallbackChannel: discord.abc.Messageable,
    ) -> Optional[discord.abc.Messageable]:
        reviewChannelId = int(getattr(config, "ribbonReviewChannelId", 0) or 0)
        if reviewChannelId > 0:
            channel = guild.get_channel(reviewChannelId)
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(reviewChannelId)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    channel = None
            if isinstance(channel, (discord.TextChannel, discord.Thread)):
                return channel
        if isinstance(fallbackChannel, (discord.TextChannel, discord.Thread)):
            return fallbackChannel
        return None

    async def resolveApprovedOutputChannel(
        self,
        guild: discord.Guild,
        fallbackChannel: discord.abc.Messageable,
    ) -> Optional[discord.abc.Messageable]:
        outputChannelId = int(getattr(config, "ribbonApprovedOutputChannelId", 0) or 0)
        if outputChannelId > 0:
            channel = guild.get_channel(outputChannelId)
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(outputChannelId)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    channel = None
            if isinstance(channel, (discord.TextChannel, discord.Thread)):
                return channel
        if isinstance(fallbackChannel, (discord.TextChannel, discord.Thread)):
            return fallbackChannel
        return None

    async def _notifyRequester(self, requesterId: int, message: str) -> None:
        try:
            user = self.bot.get_user(int(requesterId)) or await self.bot.fetch_user(int(requesterId))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return
        try:
            await user.send(message)
        except (discord.Forbidden, discord.HTTPException):
            return

    async def _sendRibbonImageToRequesterDm(
        self,
        *,
        requesterId: int,
        approverId: int,
        renderPath: str,
    ) -> tuple[str, bool]:
        try:
            user = self.bot.get_user(int(requesterId)) or await self.bot.fetch_user(int(requesterId))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException) as exc:
            raise RuntimeError("Could not resolve requester user for DM delivery.") from exc

        file = discord.File(renderPath, filename="ribbons.png")
        try:
            dmMessage = await user.send(
                content=f"Approved by <@{int(approverId)}>.",
                file=file,
                allowed_mentions=discord.AllowedMentions(users=True),
            )
        except (discord.Forbidden, discord.HTTPException) as exc:
            raise RuntimeError(
                "Could not DM the completed ribbon image to the requester. They may have DMs disabled."
            ) from exc

        imageUrl = str(dmMessage.jump_url)
        attachmentSent = False
        if dmMessage.attachments:
            imageUrl = str(dmMessage.attachments[0].url)
            attachmentSent = True
        return imageUrl, attachmentSent

    def _classifyAdditions(
        self,
        member: discord.Member,
        addIds: list[str],
    ) -> tuple[list[str], list[str], list[str]]:
        autoApproved: list[str] = []
        needsProof: list[str] = []
        staffOnly: list[str] = []

        memberRoleIds = {role.id for role in member.roles}
        for assetId in addIds:
            asset = self.assetsById.get(assetId)
            if not asset:
                continue
            rule = ribbonWorkflow.resolveRuleForAsset(asset, self.rulesConfig)
            eligibilityType = str(rule.get("eligibilityType") or "PROOF_REQUIRED").upper()

            if eligibilityType == "ROLE_AUTO":
                requiredRoleIds = _toIntSet(rule.get("requiredRoleIds", []))
                if not requiredRoleIds or any(roleId in memberRoleIds for roleId in requiredRoleIds):
                    autoApproved.append(assetId)
                else:
                    staffOnly.append(assetId)
            elif eligibilityType == "STAFF_GRANT_ONLY":
                staffOnly.append(assetId)
            else:
                needsProof.append(assetId)

        return (
            sorted(set(autoApproved)),
            sorted(set(needsProof)),
            sorted(set(staffOnly)),
        )

    def _canRequesterPostInChannel(self, channel: discord.abc.Messageable, requester: discord.Member) -> bool:
        try:
            perms = channel.permissions_for(requester)  # type: ignore[attr-defined]
        except Exception:
            return False
        if not getattr(perms, "view_channel", False):
            return False
        if isinstance(channel, discord.Thread):
            if hasattr(perms, "send_messages_in_threads") and not getattr(perms, "send_messages_in_threads"):
                return False
            if hasattr(perms, "send_messages") and not getattr(perms, "send_messages"):
                return False
            return True
        return bool(getattr(perms, "send_messages", False))

    async def _createProofCollectionChannel(
        self,
        *,
        reviewMessage: discord.Message,
        fallbackChannel: discord.abc.Messageable,
        requester: discord.Member,
        requestId: int,
    ) -> tuple[Optional[discord.abc.Messageable], str]:
        threadName = f"Ribbon Proof #{requestId}"[:100]

        reviewChannel = reviewMessage.channel
        if isinstance(reviewChannel, discord.TextChannel):
            if self._canRequesterPostInChannel(reviewChannel, requester):
                try:
                    proofThread = await reviewMessage.create_thread(
                        name=threadName,
                        auto_archive_duration=1440,
                    )
                    return proofThread, "review-thread"
                except (discord.Forbidden, discord.HTTPException):
                    pass

        if isinstance(fallbackChannel, discord.Thread):
            if self._canRequesterPostInChannel(fallbackChannel, requester):
                return fallbackChannel, "fallback-thread"

        if isinstance(fallbackChannel, discord.TextChannel):
            if self._canRequesterPostInChannel(fallbackChannel, requester):
                try:
                    markerMessage = await fallbackChannel.send(
                        content=f"{requester.mention} proof collection thread initialized for ribbon request.",
                        allowed_mentions=discord.AllowedMentions(users=True),
                    )
                    proofThread = await markerMessage.create_thread(
                        name=threadName,
                        auto_archive_duration=1440,
                    )
                    return proofThread, "fallback-thread"
                except (discord.Forbidden, discord.HTTPException):
                    pass

        if isinstance(fallbackChannel, (discord.TextChannel, discord.Thread)):
            return fallbackChannel, "fallback-channel"

        return None, "unavailable"

    async def _buildReviewEmbed(self, requestRow: dict[str, Any]) -> discord.Embed:
        requestId = int(requestRow["requestId"])
        status = str(requestRow.get("status") or "PENDING")
        statusUpper = status.upper()
        if int(requestRow.get("reviewMessageId") or 0) > 0 or statusUpper != "PENDING":
            await ribbonWorkflowBridge.ensureRibbonWorkflowCurrent(requestRow)
        statusColor = {
            "PENDING": discord.Color.blurple(),
            "NEEDS_INFO": discord.Color.orange(),
            "APPROVED": discord.Color.green(),
            "REJECTED": discord.Color.red(),
            "CANCELED": discord.Color.dark_grey(),
        }.get(statusUpper, discord.Color.blurple())

        addIds = _jsonList(requestRow.get("addRibbonIdsJson"))
        removeIds = _jsonList(requestRow.get("removeRibbonIdsJson"))
        autoIds = _jsonList(requestRow.get("autoApprovedRibbonIdsJson"))
        proofIds = _jsonList(requestRow.get("needsProofRibbonIdsJson"))
        staffIds = _jsonList(requestRow.get("staffOnlyRibbonIdsJson"))

        snapshotRaw = str(requestRow.get("currentSnapshotJson") or "{}")
        try:
            snapshot = json.loads(snapshotRaw)
        except json.JSONDecodeError:
            snapshot = {}
        currentIds = []
        if isinstance(snapshot, dict):
            currentIds = [str(value) for value in snapshot.get("currentRibbonIds", []) if str(value).strip()]
        currentSummary = self.summarizeCategoryCounts(currentIds)

        addNames = self.assetNamesForIds(addIds)
        removeNames = self.assetNamesForIds(removeIds)
        autoNames = self.assetNamesForIds(autoIds)
        proofNames = self.assetNamesForIds(proofIds)
        staffNames = self.assetNamesForIds(staffIds)

        def _preview(names: list[str], limit: int = 6) -> str:
            if not names:
                return "(none)"
            if len(names) <= limit:
                return ", ".join(names)
            return f"{', '.join(names[:limit])}, +{len(names) - limit} more"

        proofs = await ribbonWorkflow.listRibbonProofsByRequest(requestId)
        proofMessageUrl = ""
        for row in proofs:
            if not proofMessageUrl:
                proofMessageUrl = str(row.get("messageUrl") or "").strip()
        if proofs:
            proofLines: list[str] = [f"{len(proofs)} proof item(s) recorded."]
            if proofMessageUrl:
                proofLines.append(f"[Open proof message]({proofMessageUrl})")
            proofValue = "\n".join(proofLines)
        elif proofIds:
            proofValue = "Manual proof requested in thread. Reviewer verification is manual (not tracked by bot)."
        else:
            proofValue = "(none)"

        embed = discord.Embed(
            title="Ribbon Request",
            color=statusColor,
        )
        embed.add_field(
            name="Applicant",
            value=f"<@{requestRow['requesterId']}> (`{requestRow['requesterId']}`)",
            inline=True,
        )
        embed.add_field(
            name="Submitted",
            value=f"{_discordTimestamp(requestRow.get('createdAt'), 'f')} ({_discordTimestamp(requestRow.get('createdAt'), 'R')})",
            inline=True,
        )
        embed.add_field(name="Nameplate", value=str(requestRow.get("nameplateText") or "(blank)"), inline=True)
        workflowSummary = await ribbonWorkflowBridge.getRibbonWorkflowSummary(requestRow)
        workflowHistorySummary = await ribbonWorkflowBridge.getRibbonWorkflowHistorySummary(requestRow)
        workflowRendering.addReviewWorkflowFields(
            embed,
            statusText=_statusText(status),
            workflowSummary=workflowSummary,
            workflowHistorySummary=workflowHistorySummary,
            reviewerNote=str(requestRow.get("reviewNote") or "").strip(),
        )
        embed.add_field(
            name="Current Loadout",
            value=_clipEmbedValue(f"{len(currentIds)} item(s)\n{currentSummary}"),
            inline=False,
        )

        changesLines = [
            f"**Add ({len(addIds)}):** {_preview(addNames)}",
            f"**Remove ({len(removeIds)}):** {_preview(removeNames)}",
        ]
        embed.add_field(
            name="Requested Changes",
            value=_clipEmbedValue("\n".join(changesLines)),
            inline=False,
        )

        eligibilityLines: list[str] = []
        if autoNames:
            eligibilityLines.append(f"**Auto-approved ({len(autoNames)}):** {_preview(autoNames)}")
        if proofNames:
            eligibilityLines.append(f"**Needs proof ({len(proofNames)}):** {_preview(proofNames)}")
        if staffNames:
            eligibilityLines.append(f"**Staff-only ({len(staffNames)}):** {_preview(staffNames)}")
        if eligibilityLines:
            embed.add_field(
                name="Review Buckets",
                value=_clipEmbedValue("\n".join(eligibilityLines)),
                inline=False,
            )

        embed.add_field(
            name="Proof",
            value=_clipEmbedValue(proofValue),
            inline=False,
        )

        return embed

    async def _refreshReviewCard(self, requestId: int) -> None:
        requestRow = await ribbonWorkflow.getRibbonRequestById(int(requestId))
        if not requestRow:
            return
        reviewMessageId = int(requestRow.get("reviewMessageId") or 0)
        reviewChannelId = int(requestRow.get("reviewChannelId") or 0)
        if reviewMessageId <= 0 or reviewChannelId <= 0:
            return

        channel = self.bot.get_channel(reviewChannelId)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(reviewChannelId)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                return
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return

        try:
            message = await channel.fetch_message(reviewMessageId)
        except discord.NotFound:
            await ribbonWorkflow.setRibbonRequestReviewMessage(int(requestId), 0, 0)
            return
        except (discord.Forbidden, discord.HTTPException):
            return

        embed = await self._buildReviewEmbed(requestRow)
        view = RibbonReviewView(self, int(requestRow["requestId"]), str(requestRow.get("status") or "PENDING"))
        try:
            await message.edit(embed=embed, view=view)
        except discord.NotFound:
            await ribbonWorkflow.setRibbonRequestReviewMessage(int(requestId), 0, 0)
            return
        except (discord.Forbidden, discord.HTTPException):
            return

    async def _deleteReviewCard(
        self,
        requestRow: dict[str, Any],
        interaction: Optional[discord.Interaction] = None,
    ) -> bool:
        reviewMessageId = int(requestRow.get("reviewMessageId") or 0)
        reviewChannelId = int(requestRow.get("reviewChannelId") or 0)
        if reviewMessageId <= 0 or reviewChannelId <= 0:
            return False

        if interaction and interaction.message and int(interaction.message.id) == reviewMessageId:
            try:
                await interaction.message.delete()
                return True
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        channel = self.bot.get_channel(reviewChannelId)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(reviewChannelId)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                return False
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return False

        try:
            message = await channel.fetch_message(reviewMessageId)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return False

        try:
            await message.delete()
            return True
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return False

    async def sendFullList(self, interaction: discord.Interaction, requestId: int) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message(
                "This action can only be used in a server.",
                ephemeral=True,
            )
        if not self.canReview(interaction.user):
            return await interaction.response.send_message(
                "You are not authorized to review ribbon requests.",
                ephemeral=True,
            )

        requestRow = await ribbonWorkflow.getRibbonRequestById(int(requestId))
        if not requestRow:
            return await interaction.response.send_message("Request not found.", ephemeral=True)

        snapshotRaw = str(requestRow.get("currentSnapshotJson") or "{}")
        try:
            snapshot = json.loads(snapshotRaw)
        except json.JSONDecodeError:
            snapshot = {}
        currentIds = []
        if isinstance(snapshot, dict):
            currentIds = [str(value) for value in snapshot.get("currentRibbonIds", []) if str(value).strip()]
        names = self.assetNamesForIds(currentIds)
        if not names:
            return await interaction.response.send_message("Current loadout is empty.", ephemeral=True)

        lines = [f"{index + 1}. {name}" for index, name in enumerate(names[:150])]
        if len(names) > 150:
            lines.append(f"...and {len(names) - 150} more")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    async def _renderAndApplyProfile(
        self,
        requestRow: dict[str, Any],
    ) -> tuple[list[str], str, str]:
        requesterId = int(requestRow["requesterId"])
        profileRow = await ribbonWorkflow.getRibbonProfile(requesterId)
        currentIds = _jsonList(profileRow.get("currentRibbonIdsJson")) if profileRow else []

        addIds = _jsonList(requestRow.get("addRibbonIdsJson"))
        removeIds = _jsonList(requestRow.get("removeRibbonIdsJson"))
        updatedSet = set(currentIds)
        updatedSet.update(addIds)
        updatedSet.difference_update(removeIds)
        updatedIds = sorted(updatedSet)

        rawNameplate = requestRow.get("nameplateText")
        oldNameplate = str((profileRow or {}).get("nameplateText") or "").strip()
        if rawNameplate is None:
            nameplateText = oldNameplate
        else:
            nameplateText = str(rawNameplate).strip()
        nameplateText = nameplateText[:NAMEPLATE_MAX_LENGTH]

        selectedNames: list[str] = []
        for assetId in updatedIds:
            asset = self.assetsById.get(assetId)
            if not asset:
                continue
            displayName = str(asset.get("displayName") or "").strip()
            if displayName and displayName not in selectedNames:
                selectedNames.append(displayName)

        renderResult = await taskBudgeter.runThreaded(
            "backgroundJobs",
            ribbonService.renderSelectionToTemp,
            selectedNames=selectedNames,
            nameplate=nameplateText,
            strict=False,
            allowBlankName=True,
            embedMetadata=True,
        )
        renderPath = str(renderResult.get("path") or "")
        if not renderPath:
            raise RuntimeError("Ribbon render did not return an output path.")

        return updatedIds, nameplateText, renderPath

    async def submitRibbonRequest(
        self,
        interaction: discord.Interaction,
        builderView: RibbonRequestBuilderView,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message(
                "This action can only be used in a server.",
                ephemeral=True,
            )
        await self._ensureCatalogLoaded()

        builderView.submitting = True
        await builderView.refreshBoundMessage()
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            self._reloadRulesConfig()
            openRequest = await ribbonWorkflow.getOpenRibbonRequestForUser(interaction.user.id)
            if openRequest:
                builderView.submitting = False
                await builderView.refreshBoundMessage()
                return await interaction.followup.send(
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
                                builderView.submitting = False
                                await builderView.refreshBoundMessage()
                                return await interaction.followup.send(
                                    f"You are on cooldown. You can submit again {_discordTimestamp(cooldownUntil.isoformat(), 'R')}.",
                                    ephemeral=True,
                                )

            addIds = sorted(builderView.selectedAddIds)
            removeIds = sorted(builderView.selectedRemoveIds)
            if not addIds and not removeIds and not builderView.hasNameplateChange():
                builderView.submitting = False
                await builderView.refreshBoundMessage()
                return await interaction.followup.send(
                    "No changes selected. Add/remove ribbons or change the nameplate.",
                    ephemeral=True,
                )

            autoApproved, needsProof, staffOnly = self._classifyAdditions(interaction.user, addIds)
            proofAlreadyOnFileById = await ribbonWorkflow.getApprovedProofRibbonIdsForUser(
                interaction.user.id,
                needsProof,
            )
            proofAlreadyOnFileByName = {
                str(name or "").strip().lower()
                for name in await ribbonWorkflow.getApprovedProofDisplayNamesForUser(interaction.user.id)
                if str(name or "").strip()
            }
            proofAlreadyOnFile: set[str] = set(proofAlreadyOnFileById)
            for assetId in needsProof:
                asset = self.assetsById.get(assetId)
                if not asset:
                    continue
                displayName = str(asset.get("displayName") or "").strip().lower()
                if displayName and displayName in proofAlreadyOnFileByName:
                    proofAlreadyOnFile.add(assetId)
            needsProof = [assetId for assetId in needsProof if assetId not in proofAlreadyOnFile]
            snapshot = {
                "currentRibbonIds": builderView.currentRibbonIds,
                "summary": self.summarizeCategoryCounts(builderView.currentRibbonIds),
            }

            requestRow = await ribbonWorkflow.createRibbonRequest(
                guildId=interaction.guild.id,
                channelId=interaction.channel_id,
                requesterId=interaction.user.id,
                nameplateText=(builderView.nameplateText or "")[:NAMEPLATE_MAX_LENGTH],
                medalSelection=[],
                addRibbonIds=addIds,
                removeRibbonIds=removeIds,
                autoApprovedRibbonIds=autoApproved,
                needsProofRibbonIds=needsProof,
                staffOnlyRibbonIds=staffOnly,
                currentSnapshot=snapshot,
                status="PENDING",
            )
            requestId = int(requestRow["requestId"])
            await ribbonWorkflowBridge.syncRibbonWorkflow(
                requestRow,
                stateKey="submitted",
                actorId=int(interaction.user.id),
                note="Ribbon request submitted.",
                eventType="SUBMITTED",
            )
            if proofAlreadyOnFile:
                await ribbonWorkflow.addRibbonRequestEvent(
                    requestId,
                    interaction.user.id,
                    "PROOF_REUSED",
                    ",".join(sorted(proofAlreadyOnFile)),
                )

            reviewChannel = await self.resolveReviewChannel(interaction.guild, interaction.channel)
            if not reviewChannel:
                await ribbonWorkflow.setRibbonRequestStatus(
                    requestId,
                    status="CANCELED",
                    reviewerId=None,
                    reviewNote="Could not resolve review channel.",
                )
                await ribbonWorkflow.addRibbonRequestEvent(
                    requestId,
                    interaction.user.id,
                    "CANCELED_REVIEW_CHANNEL_MISSING",
                    "",
                )
                canceledRow = await ribbonWorkflow.getRibbonRequestById(requestId)
                if canceledRow:
                    await ribbonWorkflowBridge.syncRibbonWorkflow(
                        canceledRow,
                        stateKey="canceled",
                        actorId=None,
                        note="Could not resolve review channel.",
                        eventType="AUTO_CANCELED",
                    )
                builderView.submitting = False
                await builderView.refreshBoundMessage()
                return await interaction.followup.send(
                    "Could not route this request to a review channel.",
                    ephemeral=True,
                )

            embed = await self._buildReviewEmbed(requestRow)
            reviewView = RibbonReviewView(self, requestId, "PENDING")
            mentionText = self.reviewMentions() or None
            try:
                reviewMessage = await reviewChannel.send(
                    content=mentionText,
                    embed=embed,
                    view=reviewView,
                    allowed_mentions=discord.AllowedMentions(roles=True),
                )
            except (discord.Forbidden, discord.HTTPException) as exc:
                await ribbonWorkflow.setRibbonRequestStatus(
                    requestId,
                    status="CANCELED",
                    reviewerId=None,
                    reviewNote=f"Failed to post review card: {exc}",
                )
                await ribbonWorkflow.addRibbonRequestEvent(
                    requestId,
                    interaction.user.id,
                    "CANCELED_REVIEW_POST_FAILED",
                    str(exc),
                )
                canceledRow = await ribbonWorkflow.getRibbonRequestById(requestId)
                if canceledRow:
                    await ribbonWorkflowBridge.syncRibbonWorkflow(
                        canceledRow,
                        stateKey="canceled",
                        actorId=None,
                        note=f"Failed to post review card: {exc}",
                        eventType="AUTO_CANCELED",
                    )
                builderView.submitting = False
                await builderView.refreshBoundMessage()
                return await interaction.followup.send(
                    f"Failed to post the review card. Request canceled. Error: {exc}",
                    ephemeral=True,
                )

            await ribbonWorkflow.setRibbonRequestReviewMessage(
                requestId,
                int(reviewMessage.channel.id),
                int(reviewMessage.id),
            )
            await ribbonWorkflow.addRibbonRequestEvent(
                requestId,
                interaction.user.id,
                "ROUTED_FOR_REVIEW",
                f"message={reviewMessage.id}",
            )
            routedRow = await ribbonWorkflow.getRibbonRequestById(requestId)
            if routedRow:
                await ribbonWorkflowBridge.syncRibbonWorkflow(
                    routedRow,
                    stateKey="pending-review",
                    actorId=None,
                    note="Routed for review.",
                    eventType="ROUTED_FOR_REVIEW",
                )
            self.bot.add_view(reviewView, message_id=reviewMessage.id)
            await self._refreshReviewCard(requestId)

            proofLocation = ""
            if needsProof:
                proofChannel, proofChannelMode = await self._createProofCollectionChannel(
                    reviewMessage=reviewMessage,
                    fallbackChannel=interaction.channel,
                    requester=interaction.user,
                    requestId=requestId,
                )
                if isinstance(proofChannel, (discord.TextChannel, discord.Thread)):
                    proofPrompt = (
                        f"<@{interaction.user.id}> Please upload proof for this ribbon request here.\n"
                        f"Needs proof for: {_formatAssetNames(self.assetNamesForIds(needsProof), limit=10)}\n"
                        "Proof review is manual by staff; the bot is not tracking proof uploads."
                    )
                    try:
                        await proofChannel.send(
                            proofPrompt,
                            allowed_mentions=discord.AllowedMentions(users=True),
                        )
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                    if isinstance(proofChannel, discord.Thread):
                        proofLocation = proofChannel.mention
                    elif isinstance(proofChannel, discord.TextChannel):
                        proofLocation = f"#{proofChannel.name}"
                    await ribbonWorkflow.addRibbonRequestEvent(
                        requestId,
                        interaction.user.id,
                        "PROOF_THREAD_OPENED",
                        f"source={proofChannelMode} channel={proofChannel.id}",
                    )
                else:
                    await ribbonWorkflow.addRibbonRequestEvent(
                        requestId,
                        interaction.user.id,
                        "PROOF_THREAD_UNAVAILABLE",
                        "",
                    )

            builderView.submitting = False
            builderView.submitted = True
            builderView.disableAll()
            await builderView.refreshBoundMessage()

            confirmationLines = ["Submitted ribbon request."]
            autoApprovedNames = self.assetNamesForIds(autoApproved)
            needsProofNames = self.assetNamesForIds(needsProof)
            staffOnlyNames = self.assetNamesForIds(staffOnly)
            if autoApprovedNames:
                confirmationLines.append(f"Auto-approved additions: {_formatAssetNames(autoApprovedNames, limit=10)}")
            if needsProofNames:
                if proofLocation:
                    confirmationLines.append(
                        f"Needs proof: {_formatAssetNames(needsProofNames, limit=10)}"
                    )
                    confirmationLines.append(
                        f"Upload proof in {proofLocation}. Reviewer verification is manual."
                    )
                else:
                    confirmationLines.append(
                        f"Needs proof: {_formatAssetNames(needsProofNames, limit=10)} (manual reviewer verification)"
                    )
            if staffOnlyNames:
                confirmationLines.append(f"Staff-only: {_formatAssetNames(staffOnlyNames, limit=10)}")
            proofReusedNames = self.assetNamesForIds(sorted(proofAlreadyOnFile))
            if proofReusedNames:
                confirmationLines.append(
                    f"Proof already on file: {_formatAssetNames(proofReusedNames, limit=10)}"
                )
            await interaction.followup.send("\n".join(confirmationLines), ephemeral=True)
        except Exception:
            log.exception("Failed to submit ribbon request.")
            builderView.submitting = False
            await builderView.refreshBoundMessage()
            if interaction.response.is_done():
                await interaction.followup.send("Failed to submit ribbon request.", ephemeral=True)
            else:
                await interaction.response.send_message("Failed to submit ribbon request.", ephemeral=True)

    async def handleRibbonReviewDecision(
        self,
        interaction: discord.Interaction,
        requestId: int,
        status: str,
        note: str,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            if interaction.response.is_done():
                await interaction.followup.send("This action can only be used in a server.", ephemeral=True)
            else:
                await interaction.response.send_message(
                    "This action can only be used in a server.",
                    ephemeral=True,
                )
            return
        if not self.canReview(interaction.user):
            if interaction.response.is_done():
                await interaction.followup.send("You are not authorized to review requests.", ephemeral=True)
            else:
                await interaction.response.send_message(
                    "You are not authorized to review requests.",
                    ephemeral=True,
                )
            return

        lock = self.requestLocks.setdefault(int(requestId), asyncio.Lock())
        async with lock:
            requestRow = await ribbonWorkflow.getRibbonRequestById(int(requestId))
            if not requestRow:
                if interaction.response.is_done():
                    await interaction.followup.send("Request not found.", ephemeral=True)
                else:
                    await interaction.response.send_message("Request not found.", ephemeral=True)
                return
            currentStatus = str(requestRow.get("status") or "").upper()
            if currentStatus in FINAL_REQUEST_STATUSES:
                if interaction.response.is_done():
                    await interaction.followup.send("This request is already finalized.", ephemeral=True)
                else:
                    await interaction.response.send_message(
                        "This request is already finalized.",
                        ephemeral=True,
                    )
                return

            if self._isReviewRequestExpired(requestRow):
                expiryHours = int(self._reviewViewExpiryDuration().total_seconds() // 3600)
                await self._expireReviewRequest(
                    requestRow,
                    reason=f"Auto-canceled: review card expired after {expiryHours} hour(s).",
                    clearReviewMessage=False,
                )
                await self._refreshReviewCard(int(requestId))
                if interaction.response.is_done():
                    await interaction.followup.send(
                        "This request has expired and is now closed.",
                        ephemeral=True,
                    )
                else:
                    await interaction.response.send_message(
                        "This request has expired and is now closed.",
                        ephemeral=True,
                    )
                return

            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True, thinking=True)

            normalizedStatus = str(status or "").upper()
            if normalizedStatus == "APPROVED":
                renderPath = ""
                try:
                    updatedIds, nameplateText, renderPath = await self._renderAndApplyProfile(requestRow)
                    summaryText = (
                        f"Added: {_formatAssetNames(self.assetNamesForIds(_jsonList(requestRow.get('addRibbonIdsJson'))), 10)}\n"
                        f"Removed: {_formatAssetNames(self.assetNamesForIds(_jsonList(requestRow.get('removeRibbonIdsJson'))), 10)}\n"
                        f"Total: {len(updatedIds)}"
                    )
                    lastImagePath, attachmentSent = await self._sendRibbonImageToRequesterDm(
                        requesterId=int(requestRow["requesterId"]),
                        approverId=int(interaction.user.id),
                        renderPath=renderPath,
                    )

                    medalSelection = []
                    for assetId in updatedIds:
                        asset = self.assetsById.get(assetId)
                        if not asset:
                            continue
                        if str(asset.get("category") or "").lower() == "sacks":
                            medalSelection.append(assetId)
                    await ribbonWorkflow.upsertRibbonProfile(
                        int(requestRow["requesterId"]),
                        nameplateText=nameplateText,
                        medalSelection=medalSelection,
                        currentRibbonIds=updatedIds,
                        lastGeneratedImagePath=lastImagePath,
                    )
                    await ribbonWorkflow.setRibbonRequestStatus(
                        int(requestId),
                        status="APPROVED",
                        reviewerId=interaction.user.id,
                        reviewNote=note or "",
                    )
                    await ribbonWorkflow.addRibbonRequestEvent(
                        int(requestId),
                        interaction.user.id,
                        "APPROVED",
                        summaryText,
                    )
                    refreshedRow = await ribbonWorkflow.getRibbonRequestById(int(requestId))
                    if refreshedRow:
                        await ribbonWorkflowBridge.syncRibbonWorkflow(
                            refreshedRow,
                            stateKey="approved",
                            actorId=int(interaction.user.id),
                            note=note or "Ribbon request approved.",
                            eventType="APPROVED",
                        )
                    deletedReviewCard = await self._deleteReviewCard(requestRow, interaction)
                    if deletedReviewCard:
                        await ribbonWorkflow.setRibbonRequestReviewMessage(
                            int(requestId),
                            0,
                            0,
                        )
                    deliveryNote = "DM image sent."
                    if not attachmentSent:
                        deliveryNote = "DM sent (image URL fallback recorded)."
                    await interaction.followup.send(
                        f"Ribbon request approved. {deliveryNote}",
                        ephemeral=True,
                    )
                except Exception as exc:
                    log.exception("Ribbon request approval failed.")
                    await interaction.followup.send(
                        f"Could not approve request: {exc}",
                        ephemeral=True,
                    )
                finally:
                    if renderPath:
                        try:
                            os.remove(renderPath)
                        except OSError:
                            pass
                return

            if normalizedStatus == "REJECTED":
                await ribbonWorkflow.setRibbonRequestStatus(
                    int(requestId),
                    status="REJECTED",
                    reviewerId=interaction.user.id,
                    reviewNote=note or "",
                )
                await ribbonWorkflow.addRibbonRequestEvent(
                    int(requestId),
                    interaction.user.id,
                    "REJECTED",
                    note or "",
                )
                refreshedRow = await ribbonWorkflow.getRibbonRequestById(int(requestId))
                if refreshedRow:
                    await ribbonWorkflowBridge.syncRibbonWorkflow(
                        refreshedRow,
                        stateKey="rejected",
                        actorId=int(interaction.user.id),
                        note=note or "Ribbon request rejected.",
                        eventType="REJECTED",
                    )
                await self._notifyRequester(
                    int(requestRow["requesterId"]),
                    "Your ribbon request was rejected.",
                )
                await self._refreshReviewCard(int(requestId))
                await interaction.followup.send("Ribbon request rejected.", ephemeral=True)
                return

            if normalizedStatus == "NEEDS_INFO":
                await ribbonWorkflow.setRibbonRequestStatus(
                    int(requestId),
                    status="NEEDS_INFO",
                    reviewerId=interaction.user.id,
                    reviewNote=note or "Needs clarification.",
                )
                await ribbonWorkflow.addRibbonRequestEvent(
                    int(requestId),
                    interaction.user.id,
                    "NEEDS_INFO",
                    note or "",
                )
                refreshedRow = await ribbonWorkflow.getRibbonRequestById(int(requestId))
                if refreshedRow:
                    await ribbonWorkflowBridge.syncRibbonWorkflow(
                        refreshedRow,
                        stateKey="needs-info",
                        actorId=int(interaction.user.id),
                        note=note or "Reviewer requested clarification.",
                        eventType="NEEDS_INFO",
                    )
                await self._notifyRequester(
                    int(requestRow["requesterId"]),
                    (
                        "Your ribbon request needs more information.\n"
                        f"Reviewer note: {note or 'Please provide additional proof.'}"
                    ),
                )
                await self._refreshReviewCard(int(requestId))
                await interaction.followup.send("Marked as needs info.", ephemeral=True)
                return

            await interaction.followup.send("Unsupported status update.", ephemeral=True)

