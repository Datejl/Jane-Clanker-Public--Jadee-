from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Any, Optional

import discord

_deps: dict[str, Any] = {}


def configure(**deps: Any) -> None:
    _deps.update(deps)


def _dep(name: str) -> Any:
    value = _deps.get(name)
    if value is None:
        raise RuntimeError(f"bgQueueMessaging dependency not configured: {name}")
    return value


def _buildQueueEmbedAndView(
    sessionId: int,
    session: dict[str, Any],
    attendees: list[dict[str, Any]],
) -> tuple[discord.Embed, discord.ui.View]:
    embed = _dep("buildBgQueueEmbed")(
        session,
        attendees,
        claimsByUserId=_dep("getBgClaimsForSession")(sessionId),
    )
    view = _dep("buildBgQueueMainView")(sessionId, attendees)
    return embed, view


def _syncQueueRepostState(bot: discord.Client, sessionId: int, attendees: list[dict[str, Any]]) -> None:
    if _dep("isBgQueueComplete")(attendees):
        _dep("stopBgQueueRepostTask")(sessionId)
        return
    _dep("ensureBgQueueRepostTask")(bot, sessionId)


async def _sendQueueStartupAlert(
    channel: discord.TextChannel | discord.Thread,
    *,
    reviewRoleId: int,
    attendeeCount: int,
) -> None:
    if int(reviewRoleId) <= 0:
        return
    try:
        await channel.send(
            content=(
                f"<@&{int(reviewRoleId)}>\n"
                f"Background-check queue startup: `{int(attendeeCount)}` attendee(s) waiting."
            ),
            allowed_mentions=discord.AllowedMentions(roles=True),
        )
    except (discord.Forbidden, discord.HTTPException):
        return


async def _recoverMissingQueueMessage(
    bot: discord.Client,
    sessionId: int,
    *,
    previousMessageId: int,
    attendees: list[dict[str, Any]],
) -> None:
    latestSession = await _dep("service").getSession(sessionId)
    latestMessageId = int((latestSession or {}).get("bgQueueMessageId") or 0)
    if latestMessageId > 0 and latestMessageId != int(previousMessageId):
        await _dep("requestBgQueueMessageUpdate")(bot, sessionId, delaySec=0)
        return
    if attendees and not _dep("isBgQueueComplete")(attendees):
        await repostBgQueueMessage(bot, sessionId)


def bgQueueChannelCandidateIds(session: dict[str, Any]) -> list[int]:
    candidateIds: list[int] = []
    sessionType = str(session.get("sessionType") or "").strip().lower()

    try:
        sessionChannelId = int(session.get("channelId") or 0)
    except (TypeError, ValueError):
        sessionChannelId = 0

    try:
        configuredChannelId = int(getattr(_dep("configModule"), "bgCheckChannelId", 0) or 0)
    except (TypeError, ValueError):
        configuredChannelId = 0

    # Orientation queues should prefer the configured BG review channel.
    # Manual ?bgCheck queues should prefer the channel where the command was run.
    preferredIds = [configuredChannelId, sessionChannelId] if sessionType == "orientation" else [sessionChannelId, configuredChannelId]
    for channelId in preferredIds:
        if channelId > 0 and channelId not in candidateIds:
            candidateIds.append(channelId)
    return candidateIds


async def resolveBgQueueChannelForSession(
    bot: discord.Client,
    session: dict[str, Any],
) -> Optional[discord.abc.Messageable]:
    for channelId in bgQueueChannelCandidateIds(session):
        channel = await _dep("getCachedChannel")(bot, channelId)
        if isinstance(channel, (discord.TextChannel, discord.Thread)):
            return channel
    return None


async def fetchBgQueueMessageForSession(
    bot: discord.Client,
    session: dict[str, Any],
    messageId: int,
) -> tuple[Optional[discord.abc.Messageable], Optional[discord.Message]]:
    targetMessageId = int(messageId or 0)
    if targetMessageId <= 0:
        return None, None

    for channelId in bgQueueChannelCandidateIds(session):
        channel = await _dep("getCachedChannel")(bot, channelId)
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            continue
        try:
            message = await channel.fetch_message(targetMessageId)
            return channel, message
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            continue
    return None, None


async def postBgFinalSummary(bot: discord.Client, sessionId: int) -> None:
    if int(sessionId) in _dep("bgFinalSummaryPosted"):
        return

    session = await _dep("service").getSession(sessionId)
    if not session:
        return

    attendees = _dep("bgCandidates")(await _dep("service").getAttendees(sessionId))
    if not attendees:
        return

    approved = [int(row["userId"]) for row in attendees if str(row.get("bgStatus") or "").upper() == "APPROVED"]
    rejected = [int(row["userId"]) for row in attendees if str(row.get("bgStatus") or "").upper() == "REJECTED"]
    pending = [int(row["userId"]) for row in attendees if str(row.get("bgStatus") or "").upper() == "PENDING"]
    sessionReviewStats = await _dep("service").getBgReviewSessionStats(int(sessionId))

    moderatorStatsLines: list[str] = []
    for row in sessionReviewStats:
        try:
            reviewerId = int(row.get("reviewerId") or 0)
        except (TypeError, ValueError):
            reviewerId = 0
        if reviewerId <= 0:
            continue
        user = await _dep("getCachedUser")(bot, reviewerId)
        username = str(user.name) if user is not None else f"user-{reviewerId}"
        approvals = int(row.get("approvals") or 0)
        rejections = int(row.get("rejections") or 0)
        total = int(row.get("total") or 0)
        moderatorStatsLines.append(f"{username}: {approvals} approved, {rejections} denied, {total} total.")

    if not approved and not rejected and not pending:
        return

    targetChannel = await resolveBgQueueChannelForSession(bot, session)
    if not isinstance(targetChannel, (discord.TextChannel, discord.Thread)):
        return

    summary = _dep("buildBgFinalSummaryText")(
        sessionId=int(sessionId),
        approvedUserIds=approved,
        rejectedUserIds=rejected,
        pendingUserIds=pending,
        moderatorStatsLines=moderatorStatsLines,
    )

    try:
        if len(summary) <= 1900:
            await targetChannel.send(
                summary,
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
        else:
            tempPath = ""
            try:
                with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".txt", delete=False) as handle:
                    handle.write(summary)
                    tempPath = handle.name
                await targetChannel.send(
                    content=(
                        "### BG Check Final Results\n"
                        f"Session `{int(sessionId)}` summary is attached."
                    ),
                    file=discord.File(tempPath, filename=f"bg-final-results-session-{int(sessionId)}.txt"),
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )
            finally:
                if tempPath:
                    try:
                        os.remove(tempPath)
                    except OSError:
                        pass
    except (discord.Forbidden, discord.HTTPException):
        return

    _dep("bgFinalSummaryPosted").add(int(sessionId))


async def closeBgQueueControls(
    bot: discord.Client,
    sessionId: int,
    *,
    clearMessageReference: bool,
) -> None:
    session = await _dep("service").getSession(sessionId)
    if not session:
        return

    messageId = int(session.get("bgQueueMessageId") or 0)
    if messageId > 0:
        _, queueMessage = await fetchBgQueueMessageForSession(bot, session, messageId)
        if queueMessage is not None:
            view = _dep("bgQueueViewClass")(sessionId)
            for child in view.children:
                child.disabled = True
            try:
                await queueMessage.edit(view=view)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

    _dep("stopBgQueueRepostTask")(sessionId)
    await postBgFinalSummary(bot, sessionId)
    _dep("clearBgClaimsForSession")(sessionId)
    if clearMessageReference:
        await _dep("service").setBgQueueMessage(sessionId, 0)
    asyncio.create_task(_dep("reconcileRecruitmentOrientationBonusesForSessionSafe")(bot, sessionId))


async def repostBgQueueMessage(bot: discord.Client, sessionId: int) -> bool:
    session = await _dep("service").getSession(sessionId)
    if not session:
        return False
    oldMessageId = session.get("bgQueueMessageId")
    if not oldMessageId:
        return False

    attendees = _dep("bgCandidates")(await _dep("service").getAttendees(sessionId))
    if not attendees or _dep("isBgQueueComplete")(attendees):
        _dep("stopBgQueueRepostTask")(sessionId)
        return False

    channel = await resolveBgQueueChannelForSession(bot, session)
    if channel is None or not isinstance(channel, (discord.TextChannel, discord.Thread)):
        return True

    embed, view = _buildQueueEmbedAndView(sessionId, session, attendees)

    try:
        newMessage = await channel.send(embed=embed, view=view)
    except (discord.Forbidden, discord.HTTPException):
        return True

    await _dep("service").setBgQueueMessage(sessionId, int(newMessage.id))
    _, oldMessage = await fetchBgQueueMessageForSession(bot, session, int(oldMessageId))
    if oldMessage is not None:
        try:
            await oldMessage.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
    return True


async def updateBgQueueMessage(bot: discord.Client, sessionId: int) -> None:
    session = await _dep("service").getSession(sessionId)
    if not session:
        return
    messageId = int(session.get("bgQueueMessageId") or 0)
    if messageId <= 0:
        return

    _, msg = await fetchBgQueueMessageForSession(bot, session, messageId)
    if msg is None:
        return

    attendees = _dep("bgCandidates")(await _dep("service").getAttendees(sessionId))
    updated = await _dep("scanRobloxGroupsForAttendees")(sessionId, attendees, bot=bot)
    if updated:
        attendees = _dep("bgCandidates")(await _dep("service").getAttendees(sessionId))

    embed, view = _buildQueueEmbedAndView(sessionId, session, attendees)
    _syncQueueRepostState(bot, sessionId, attendees)
    try:
        await msg.edit(embed=embed, view=view)
    except discord.NotFound:
        await _recoverMissingQueueMessage(
            bot,
            sessionId,
            previousMessageId=messageId,
            attendees=attendees,
        )
    except (discord.Forbidden, discord.HTTPException):
        return


async def postBgQueue(bot: discord.Client, sessionId: int, guild: discord.Guild) -> None:
    _ = guild
    session = await _dep("service").getSession(sessionId)
    if not session:
        return
    attendees = _dep("bgCandidates")(await _dep("service").getAttendees(sessionId))

    channel = await resolveBgQueueChannelForSession(bot, session)
    if channel is None:
        return

    _dep("clearBgClaimsForSession")(sessionId)
    embed, view = _buildQueueEmbedAndView(sessionId, session, attendees)
    reviewRoleIdInt = _dep("resolveBgQueuePingRoleId")(channel)
    try:
        await _sendQueueStartupAlert(
            channel,
            reviewRoleId=reviewRoleIdInt,
            attendeeCount=len(attendees),
        )
        msg = await channel.send(
            embed=embed,
            view=view,
        )
    except (discord.Forbidden, discord.HTTPException):
        return

    await _dep("service").setBgQueueMessage(sessionId, msg.id)
    if attendees and not _dep("isBgQueueComplete")(attendees):
        _syncQueueRepostState(bot, sessionId, attendees)
        await _dep("requestBgQueueMessageUpdate")(bot, sessionId, delaySec=0)
