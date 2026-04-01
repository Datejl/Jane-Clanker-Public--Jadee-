from __future__ import annotations

import asyncio
import logging
from typing import Any

import discord
from discord import ui

log = logging.getLogger(__name__)

_deps: dict[str, Any] = {}


def configure(**deps: Any) -> None:
    _deps.update(deps)


def _dep(name: str) -> Any:
    value = _deps.get(name)
    if value is None:
        raise RuntimeError(f"bgCheckViews dependency not configured: {name}")
    return value


async def _getBgCandidateByIndex(sessionId: int, index: int) -> dict[str, Any] | None:
    attendees = _dep("bgCandidates")(await _dep("service").getAttendees(sessionId))
    if index < 1 or index > len(attendees):
        return None
    return attendees[index - 1]


class BgCheckView(ui.View):
    def __init__(self, sessionId: int, targetUserId: int):
        super().__init__(timeout=None)
        self.sessionId = int(sessionId)
        self.targetUserId = int(targetUserId)

        self.approveBtn.custom_id = f"bg:approve:{sessionId}:{targetUserId}"
        self.rejectBtn.custom_id = f"bg:reject:{sessionId}:{targetUserId}"
        self.infoBtn.custom_id = f"bg:info:{sessionId}:{targetUserId}"
        self.outfitsBtn.custom_id = f"bg:outfits:{sessionId}:{targetUserId}"

    @ui.button(label="Approve", style=discord.ButtonStyle.success)
    async def approveBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await _dep("requireModPermission")(interaction):
            return

        await _dep("service").setBgStatusWithReviewer(
            self.sessionId,
            self.targetUserId,
            "APPROVED",
            int(interaction.user.id),
        )
        _dep("clearBgClaim")(self.sessionId, self.targetUserId)
        session = await _dep("service").getSession(self.sessionId)
        sessionType = (session or {}).get("sessionType")
        sessionGuild = _dep("sessionGuild")(interaction.client, session, interaction.guild)
        if sessionType in {"orientation", "bg-check"}:
            await _dep("setPendingBgRole")(sessionGuild, self.targetUserId, False)
        if sessionType == "orientation":
            await _dep("service").awardHostPointIfEligible(self.sessionId, self.targetUserId)
        await _dep("safeInteractionReply")(interaction, f"Approved <@{self.targetUserId}>.", ephemeral=True)
        await _dep("updateSessionMessage")(interaction.client, self.sessionId)
        await _dep("updateBgCheckMessage")(interaction, self.sessionId, self.targetUserId)
        await _dep("requestBgQueueMessageUpdate")(interaction.client, self.sessionId)
        await _dep("maybeNotifyBgComplete")(interaction, self.sessionId)
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

    @ui.button(label="Reject", style=discord.ButtonStyle.danger)
    async def rejectBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await _dep("requireModPermission")(interaction):
            return

        statusChanged = await _dep("service").setBgStatusWithReviewer(
            self.sessionId,
            self.targetUserId,
            "REJECTED",
            int(interaction.user.id),
        )
        _dep("clearBgClaim")(self.sessionId, self.targetUserId)
        session = await _dep("service").getSession(self.sessionId)
        sessionType = (session or {}).get("sessionType")
        sessionGuild = _dep("sessionGuild")(interaction.client, session, interaction.guild)
        if statusChanged:
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
        await _dep("safeInteractionReply")(interaction, f"Rejected <@{self.targetUserId}>.", ephemeral=True)
        await _dep("updateSessionMessage")(interaction.client, self.sessionId)
        await _dep("updateBgCheckMessage")(interaction, self.sessionId, self.targetUserId)
        await _dep("requestBgQueueMessageUpdate")(interaction.client, self.sessionId)
        await _dep("maybeNotifyBgComplete")(interaction, self.sessionId)

    @ui.button(label="Get Info", style=discord.ButtonStyle.secondary, row=1)
    async def infoBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await _dep("requireModPermission")(interaction):
            return
        try:
            await _dep("sendBgInfoForTarget")(interaction, self.sessionId, self.targetUserId)
        except Exception:
            log.exception(
                "Get Info failed for session %s attendee %s.",
                self.sessionId,
                self.targetUserId,
            )
            await _dep("safeInteractionReply")(
                interaction,
                content="Get Info failed due to an internal error. Please try again.",
                ephemeral=True,
            )

    @ui.button(label="Outfits", style=discord.ButtonStyle.secondary, row=1)
    async def outfitsBtn(self, interaction: discord.Interaction, _: ui.Button):
        if not await _dep("requireModPermission")(interaction):
            return
        await _dep("sendBgOutfitsForTarget")(interaction, self.sessionId, self.targetUserId)


class BgInfoModal(ui.Modal, title="Get Attendee Info"):
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

        attendee = await _getBgCandidateByIndex(self.sessionId, index)
        if attendee is None:
            await _dep("safeInteractionReply")(
                interaction,
                "The attendee number you entered is outside the current queue range.",
                ephemeral=True,
            )
            return
        try:
            await _dep("sendBgInfoForTarget")(interaction, self.sessionId, attendee["userId"])
        except Exception:
            log.exception(
                "Get Info modal failed for session %s attendee %s.",
                self.sessionId,
                attendee["userId"],
            )
            await _dep("safeInteractionReply")(
                interaction,
                content="Get Info failed due to an internal error. Please try again.",
                ephemeral=True,
            )
