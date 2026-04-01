from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import discord
from discord import ui

log = logging.getLogger(__name__)

_deps: dict[str, Any] = {}


def configure(**deps: Any) -> None:
    _deps.update(deps)


def _dep(name: str) -> Any:
    value = _deps.get(name)
    if value is None:
        raise RuntimeError(f"bgQueueViews dependency not configured: {name}")
    return value


def buildBgAttendeeReviewEmbed(
    attendee: dict[str, Any],
    *,
    claimOwnerId: Optional[int] = None,
    includeClaimField: bool = True,
) -> discord.Embed:
    targetUserId = int(attendee["userId"])
    embed = discord.Embed(
        title="BG Check",
        description=f"<@{targetUserId}>",
    )
    embed.add_field(
        name="Inventory",
        value=_dep("inventoryReviewIcon")(attendee.get("robloxInventoryScanStatus")),
        inline=True,
    )
    embed.add_field(
        name="BG",
        value=_dep("bgReviewIcon")(attendee["bgStatus"]),
        inline=True,
    )
    embed.add_field(
        name="Flagged",
        value=_dep("flaggedReviewIcon")(attendee.get("robloxFlagged")),
        inline=True,
    )
    if includeClaimField:
        claimValue = "Unclaimed"
        if claimOwnerId:
            claimValue = f"<@{int(claimOwnerId)}>"
        embed.add_field(name="Claimed By", value=claimValue, inline=False)
    return embed


async def openBgAttendeePanel(
    interaction: discord.Interaction,
    sessionId: int,
    targetUserId: int,
) -> None:
    attendees = _dep("bgCandidates")(await _dep("service").getAttendees(sessionId))
    attendee = next((row for row in attendees if int(row["userId"]) == int(targetUserId)), None)
    if attendee is None:
        await _dep("safeInteractionReply")(
            interaction,
            content="This attendee is no longer present in the background-check queue.",
            ephemeral=True,
        )
        return

    embed = buildBgAttendeeReviewEmbed(
        attendee,
        claimOwnerId=_dep("getBgClaimOwnerId")(sessionId, targetUserId),
    )
    view = BgAttendeeReviewView(
        sessionId=sessionId,
        targetUserId=targetUserId,
        viewerId=int(interaction.user.id),
    )
    await _dep("safeInteractionReply")(interaction, embed=embed, view=view, ephemeral=True)


class BgAttendeeReviewView(ui.View):
    def __init__(self, sessionId: int, targetUserId: int, viewerId: int):
        super().__init__(timeout=900)
        self.sessionId = int(sessionId)
        self.targetUserId = int(targetUserId)
        self.viewerId = int(viewerId)
        self._syncClaimButton()

    def _syncClaimButton(self) -> None:
        ownerId = _dep("getBgClaimOwnerId")(self.sessionId, self.targetUserId)
        if ownerId == self.viewerId:
            self.claimBtn.label = "Unclaim"
            self.claimBtn.style = discord.ButtonStyle.success
        elif ownerId is None:
            self.claimBtn.label = "Claim"
            self.claimBtn.style = discord.ButtonStyle.primary
        else:
            self.claimBtn.label = "Claimed"
            self.claimBtn.style = discord.ButtonStyle.secondary

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.viewerId:
            await _dep("safeInteractionReply")(
                interaction,
                content="This review panel is only for the moderator who opened it.",
                ephemeral=True,
            )
            return False
        if not await _dep("requireModPermission")(interaction):
            return False
        return True

    async def _refreshMessage(self, interaction: discord.Interaction) -> None:
        if interaction.message is None:
            return
        attendee = await _dep("service").getAttendee(self.sessionId, self.targetUserId)
        if attendee is None:
            self.stop()
            for child in self.children:
                child.disabled = True
            try:
                await interaction.message.edit(
                    content="This attendee is no longer present in the background-check queue.",
                    view=self,
                )
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                await _dep("safeInteractionReply")(
                    interaction,
                    content="This attendee panel is no longer available.",
                    ephemeral=True,
                )
            return
        self._syncClaimButton()
        embed = buildBgAttendeeReviewEmbed(
            attendee,
            claimOwnerId=_dep("getBgClaimOwnerId")(self.sessionId, self.targetUserId),
        )
        try:
            await interaction.message.edit(embed=embed, view=self)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            await _dep("safeInteractionReply")(
                interaction,
                content="This attendee panel is no longer available.",
                ephemeral=True,
            )

    async def _applyDecision(self, interaction: discord.Interaction, newStatus: str) -> None:
        if not await self._guard(interaction):
            return
        claimOwnerId = _dep("getBgClaimOwnerId")(self.sessionId, self.targetUserId)
        if claimOwnerId and claimOwnerId != self.viewerId:
            await _dep("safeInteractionReply")(
                interaction,
                content=f"This attendee is currently claimed by <@{claimOwnerId}>.",
                ephemeral=True,
            )
            return

        await _dep("safeInteractionDefer")(interaction, ephemeral=True)
        statusChanged = await _dep("service").setBgStatusWithReviewer(
            self.sessionId,
            self.targetUserId,
            newStatus,
            int(interaction.user.id),
        )
        _dep("clearBgClaim")(self.sessionId, self.targetUserId)
        session = await _dep("service").getSession(self.sessionId)
        sessionType = (session or {}).get("sessionType")
        sessionGuild = _dep("sessionGuild")(interaction.client, session, interaction.guild)
        if newStatus == "REJECTED" and statusChanged:
            asyncio.create_task(
                _dep("postBgFailureForumEntry")(
                    interaction.client,
                    sessionGuild,
                    self.targetUserId,
                    int(interaction.user.id),
                )
            )
        if sessionType in {"orientation", "bg-check"}:
            await _dep("setPendingBgRole")(sessionGuild, self.targetUserId, False)
        if newStatus == "APPROVED":
            if sessionType == "orientation":
                await _dep("service").awardHostPointIfEligible(self.sessionId, self.targetUserId)
            asyncio.create_task(
                _dep("maybeAutoAcceptRoblox")(
                    interaction.client,
                    sessionGuild,
                    self.sessionId,
                    self.targetUserId,
                )
            )
            asyncio.create_task(
                _dep("sendRobloxJoinRequestDm")(interaction.client, self.sessionId, self.targetUserId)
            )
            if sessionType == "orientation":
                asyncio.create_task(
                    _dep("applyRecruitmentOrientationBonus")(
                        interaction.client,
                        self.targetUserId,
                    )
                )

        await _dep("updateSessionMessage")(interaction.client, self.sessionId)
        await _dep("requestBgQueueMessageUpdate")(interaction.client, self.sessionId)
        await _dep("maybeNotifyBgComplete")(interaction, self.sessionId)
        await self._refreshMessage(interaction)
        statusText = "Approved" if newStatus == "APPROVED" else "Rejected"
        await _dep("safeInteractionReply")(
            interaction,
            f"{statusText} <@{self.targetUserId}>.",
            ephemeral=True,
        )

    @ui.button(label="Approve", style=discord.ButtonStyle.success, row=0)
    async def approveBtn(self, interaction: discord.Interaction, _: ui.Button):
        await self._applyDecision(interaction, "APPROVED")

    @ui.button(label="Reject", style=discord.ButtonStyle.danger, row=0)
    async def rejectBtn(self, interaction: discord.Interaction, _: ui.Button):
        await self._applyDecision(interaction, "REJECTED")

    @ui.button(label="Claim", style=discord.ButtonStyle.primary, row=0)
    async def claimBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        ownerId = _dep("getBgClaimOwnerId")(self.sessionId, self.targetUserId)
        if ownerId is None:
            _dep("setBgClaimOwnerId")(self.sessionId, self.targetUserId, self.viewerId)
            await _dep("requestBgQueueMessageUpdate")(interaction.client, self.sessionId)
            await _dep("safeInteractionReply")(interaction, "Attendee claimed.", ephemeral=True)
        elif ownerId == self.viewerId:
            _dep("clearBgClaim")(self.sessionId, self.targetUserId)
            await _dep("requestBgQueueMessageUpdate")(interaction.client, self.sessionId)
            await _dep("safeInteractionReply")(interaction, "Attendee unclaimed.", ephemeral=True)
        else:
            await _dep("safeInteractionReply")(
                interaction,
                f"This attendee is currently claimed by <@{ownerId}>.",
                ephemeral=True,
            )
        await self._refreshMessage(interaction)

    @ui.button(label="Get Info", style=discord.ButtonStyle.secondary, row=1)
    async def infoBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        await _dep("sendBgInfoForTarget")(interaction, self.sessionId, self.targetUserId)

    @ui.button(label="Outfits", style=discord.ButtonStyle.secondary, row=1)
    async def outfitsBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await self._guard(interaction):
            return
        await _dep("sendBgOutfitsForTarget")(interaction, self.sessionId, self.targetUserId)

    @ui.button(label="Close", style=discord.ButtonStyle.secondary, row=1)
    async def closeBtn(self, interaction: discord.Interaction, _: ui.Button):
        if interaction.user.id != self.viewerId:
            await _dep("safeInteractionReply")(
                interaction,
                content="This review panel is only for the moderator who opened it.",
                ephemeral=True,
            )
            return
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)


class BgOpenAttendeeModal(ui.Modal, title="Open Attendee Review"):
    number = ui.TextInput(
        label="Attendee Number",
        placeholder="Number from the BG queue list",
        required=True,
    )

    def __init__(self, sessionId: int):
        super().__init__()
        self.sessionId = int(sessionId)

    async def on_submit(self, interaction: discord.Interaction):
        if not await _dep("requireModPermission")(interaction):
            return

        try:
            index = int(str(self.number.value).strip())
        except ValueError:
            await _dep("safeInteractionReply")(
                interaction,
                "Please enter a valid attendee number.",
                ephemeral=True,
            )
            return

        attendees = _dep("bgCandidates")(await _dep("service").getAttendees(self.sessionId))
        if index < 1 or index > len(attendees):
            await _dep("safeInteractionReply")(
                interaction,
                "The attendee number you entered is outside the current queue range.",
                ephemeral=True,
            )
            return

        attendee = attendees[index - 1]
        targetUserId = int(attendee["userId"])
        # "Open Attendee" intentionally overrides existing claims so staff can recover
        # from abandoned claims (e.g., reviewer AFK/asleep).
        _dep("setBgClaimOwnerId")(self.sessionId, targetUserId, int(interaction.user.id))
        await _dep("requestBgQueueMessageUpdate")(interaction.client, self.sessionId)
        await openBgAttendeePanel(interaction, self.sessionId, targetUserId)


class BgQueueView(ui.View):
    def __init__(self, sessionId: int):
        super().__init__(timeout=None)
        self.sessionId = int(sessionId)
        self.finishBtn.custom_id = f"bgqueue:finish:{sessionId}"
        self.openAttendeeBtn.custom_id = f"bgqueue:open:{sessionId}"
        self.nextPendingBtn.custom_id = f"bgqueue:next:{sessionId}"
        self.refreshBtn.custom_id = f"bgqueue:refresh:{sessionId}"

    @ui.button(label="Finish", style=discord.ButtonStyle.success, row=0)
    async def finishBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await _dep("requireModPermission")(interaction):
            return

        attendees = await _dep("service").getAttendees(self.sessionId)
        if not attendees:
            await _dep("safeInteractionReply")(
                interaction,
                "No attendees were found for this background-check queue.",
                ephemeral=True,
            )
            return

        candidates = _dep("bgCandidates")(attendees)
        if not candidates:
            for child in self.children:
                child.disabled = True
            if interaction.message:
                await interaction.message.edit(view=self)
            await _dep("safeInteractionReply")(
                interaction,
                "No background checks are required for this session.",
                ephemeral=True,
            )
            return
        if not _dep("isBgQueueComplete")(candidates):
            pending = [row for row in candidates if row["bgStatus"] == "PENDING"]
            await _dep("safeInteractionReply")(
                interaction,
                (
                    f"There are still {len(pending)} users pending a background check. "
                    "Are you sure you want to close this session?"
                ),
                view=BgQueueForceCloseConfirmView(
                    sessionId=self.sessionId,
                    requesterId=int(interaction.user.id),
                    pendingCount=len(pending),
                ),
                ephemeral=True,
            )
            return

        await _dep("closeBgQueueControls")(
            interaction.client,
            self.sessionId,
            clearMessageReference=False,
        )
        await _dep("safeInteractionReply")(interaction, "All attendees processed.", ephemeral=False)

    @ui.button(label="Open Attendee", style=discord.ButtonStyle.primary)
    async def openAttendeeBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await _dep("requireModPermission")(interaction):
            return
        await _dep("safeInteractionSendModal")(interaction, BgOpenAttendeeModal(self.sessionId))

    @ui.button(label="Next Pending", style=discord.ButtonStyle.secondary)
    async def nextPendingBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await _dep("requireModPermission")(interaction):
            return
        attendees = _dep("bgCandidates")(await _dep("service").getAttendees(self.sessionId))
        pendingRows = [row for row in attendees if row.get("bgStatus") == "PENDING"]
        if not pendingRows:
            await _dep("safeInteractionReply")(interaction, "No pending attendees remain.", ephemeral=True)
            return

        # Skip attendees claimed by someone else. The claimer can still iterate their own claims.
        visiblePending = [
            row
            for row in pendingRows
            if (
                _dep("getBgClaimOwnerId")(self.sessionId, int(row["userId"]))
                in {None, int(interaction.user.id)}
            )
        ]
        pending = visiblePending[0] if visiblePending else None
        if not pending:
            claimedByOthersCount = len(pendingRows)
            await _dep("safeInteractionReply")(
                interaction,
                (
                    "All pending attendees are currently claimed by other reviewers. "
                    f"({claimedByOthersCount} claimed)"
                ),
                ephemeral=True,
            )
            return
        await openBgAttendeePanel(interaction, self.sessionId, int(pending["userId"]))

    @ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def refreshBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await _dep("requireModPermission")(interaction):
            return
        await _dep("requestBgQueueMessageUpdate")(interaction.client, self.sessionId, delaySec=0)
        await _dep("safeInteractionReply")(interaction, "Background-check queue refreshed.", ephemeral=True)


class BgQueueForceCloseConfirmView(ui.View):
    def __init__(self, sessionId: int, requesterId: int, pendingCount: int):
        super().__init__(timeout=180)
        self.sessionId = int(sessionId)
        self.requesterId = int(requesterId)
        self.pendingCount = max(0, int(pendingCount))

    @ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirmBtn(self, interaction: discord.Interaction, _: ui.Button):
        if interaction.user.id != self.requesterId:
            await _dep("safeInteractionReply")(
                interaction,
                content="Only the moderator who opened this confirmation can use it.",
                ephemeral=True,
            )
            return
        if not await _dep("requireModPermission")(interaction):
            return

        await _dep("safeInteractionDefer")(interaction, ephemeral=True)
        await _dep("closeBgQueueControls")(
            interaction.client,
            self.sessionId,
            clearMessageReference=True,
        )
        for child in self.children:
            child.disabled = True
        try:
            if interaction.message:
                await interaction.message.edit(view=self)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
        await _dep("safeInteractionReply")(
            interaction,
            (
                "Session closed. "
                f"{self.pendingCount} attendee(s) were still pending a background check."
            ),
            ephemeral=True,
        )
