from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Sequence

import discord

import config
from features.staff.honorGuard import sheets as honorGuardSheets
from runtime import normalization
from runtime import orbatAudit as orbatAuditRuntime


log = logging.getLogger(__name__)


async def postHonorGuardLogEmbed(
    botClient: discord.Client,
    *,
    embed: Optional[discord.Embed] = None,
    content: Optional[str] = None,
) -> None:
    try:
        channelId = int(getattr(config, "honorGuardLogChannelId", 0) or 0)
        if channelId <= 0:
            return
        channel = botClient.get_channel(channelId)
        if channel is None:
            try:
                channel = await botClient.fetch_channel(channelId)
            except (discord.Forbidden, discord.NotFound, discord.HTTPException, discord.InvalidData):
                return
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return
        await channel.send(
            content=content,
            embed=embed,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except Exception:
        log.exception("Failed to post to Honor-Guard log channel.")


@dataclass(frozen=True)
class ResolvedSheetLogEntry:
    discordUserId: int
    quotaDelta: float = 0
    pointsDelta: float = 0


SheetUpdateBuilder = Callable[[Sequence[ResolvedSheetLogEntry]], list[dict[str, Any]]]
SheetWriter = Callable[[list[dict[str, Any]], bool], dict[str, Any]]


def buildHonorGuardSheetUpdates(
    entries: Sequence[ResolvedSheetLogEntry],
) -> list[dict[str, Any]]:
    return [
        {
            "discordUserId": int(entry.discordUserId),
            "quotaDelta": float(entry.quotaDelta),
            "pointsDelta": float(entry.pointsDelta),
        }
        for entry in entries
    ]


async def syncApprovedLogsToSheet(
    discordUserIds: Sequence[int],
    quotaDelta: int,
    pointsDelta: int,
    *,
    organizeAfter: bool = True,
    updateBuilder: SheetUpdateBuilder = buildHonorGuardSheetUpdates,
    sheetWriter: SheetWriter | None = None,
    sheetConfigured: bool | None = None,
) -> dict[str, Any]:
    entries = [
        ResolvedSheetLogEntry(
            discordUserId=userId,
            quotaDelta=float(quotaDelta),
            pointsDelta=float(pointsDelta),
        )
        for userId in normalization.normalizeIntList(discordUserIds)
    ]
    updates = updateBuilder(entries)
    if sheetWriter is not None:
        return sheetWriter(updates, organizeAfter)
    if sheetConfigured is False:
        return {"updatedUsers": 0, "updatedRows": 0, "organized": 0, "error": "sheet-disabled"}

    updatedRows = 0
    failures: list[int] = []
    for entry in entries:
        try:
            honorGuardSheets.applyMemberPointDeltas(
                discordId=int(entry.discordUserId),
                quotaDelta=float(entry.quotaDelta),
                promotionEventDelta=float(entry.pointsDelta),
            )
            updatedRows += 1
        except Exception:
            failures.append(int(entry.discordUserId))
            log.exception("Honor-Guard sheet sync failed for discord user %s", int(entry.discordUserId))
    result: dict[str, Any] = {
        "updatedUsers": updatedRows,
        "updatedRows": updatedRows,
        "organized": 0,
    }
    if failures:
        result["failedUserIds"] = failures
    return result


async def sendHonorGuardSheetChangeLog(
    botClient: discord.Client,
    *,
    reviewerId: int,
    requestedBy: str = "",
    requestMessageUrl: str = "",
    change: str,
    details: str,
    sheetKey: str = "honorGuard_members",
) -> None:
    reviewerText = f"<@{int(reviewerId)}>" if int(reviewerId or 0) > 0 else "system"
    try:
        await orbatAuditRuntime.sendOrbatChangeLog(
            botClient,
            change=change,
            requestedBy=str(requestedBy or "").strip() or reviewerText,
            authorizedBy=reviewerText,
            requestMessageUrl=str(requestMessageUrl or "").strip(),
            details=details,
            sheetKey=sheetKey,
        )
    except Exception:
        log.exception("Failed to post Honor-Guard ORBAT audit log.")

    receiptEmbed = discord.Embed(
        title="Honor-Guard Approval",
        color=discord.Color.blurple(),
        timestamp=datetime.now(tz=timezone.utc),
    )
    receiptEmbed.add_field(name="Change", value=str(change or "Unknown"), inline=False)
    receiptEmbed.add_field(
        name="Requested By",
        value=str(requestedBy or "").strip() or reviewerText,
        inline=True,
    )
    receiptEmbed.add_field(name="Authorized By", value=reviewerText, inline=True)
    if details:
        receiptEmbed.add_field(name="Details", value=str(details), inline=False)
    requestUrl = str(requestMessageUrl or "").strip()
    if requestUrl:
        receiptEmbed.add_field(
            name="Request Message",
            value=f"[Open message]({requestUrl})",
            inline=False,
        )
    await postHonorGuardLogEmbed(botClient, embed=receiptEmbed)


def _clipFieldValue(text: str, *, limit: int = 1024) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


async def sendHonorGuardSheetAudit(
    botClient: discord.Client,
    *,
    reviewerId: int,
    requestedBy: str = "",
    requestMessageUrl: str = "",
    change: str,
    details: str = "",
    auditLogs: Optional[Sequence[str]] = None,
) -> None:
    reviewerText = f"<@{int(reviewerId)}>" if int(reviewerId or 0) > 0 else "system"
    embed = discord.Embed(
        title="Honor-Guard Sheet Audit",
        color=discord.Color.dark_teal(),
        timestamp=datetime.now(tz=timezone.utc),
    )
    embed.add_field(name="Change", value=str(change or "Unknown"), inline=False)
    embed.add_field(
        name="Requested By",
        value=str(requestedBy or "").strip() or reviewerText,
        inline=True,
    )
    embed.add_field(name="Authorized By", value=reviewerText, inline=True)
    requestUrl = str(requestMessageUrl or "").strip()
    if requestUrl:
        embed.add_field(
            name="Request Message",
            value=f"[Open message]({requestUrl})",
            inline=False,
        )
    if details:
        embed.add_field(name="Details", value=_clipFieldValue(str(details)), inline=False)
    logs = [str(line) for line in (auditLogs or []) if str(line or "").strip()]
    if logs:
        embed.add_field(name="Changes", value=_clipFieldValue("\n\n".join(logs)), inline=False)
    await postHonorGuardLogEmbed(botClient, embed=embed)


async def sendHonorGuardEventChangeLog(
    botClient: discord.Client,
    *,
    reviewerId: int,
    requestedBy: str = "",
    requestMessageUrl: str = "",
    change: str,
    hostMention: str = "",
    hostRobloxUsername: str = "",
    eventType: str = "",
    participantCount: int = 0,
    eventDateText: str = "",
    archived: bool = False,
    hostUpdated: bool = False,
    sheetStatus: str = "",
    auditLogs: Optional[Sequence[str]] = None,
    eventHostUpdate: Any = None,
    sheetKey: str = "honorGuard_members",
) -> None:
    reviewerText = f"<@{int(reviewerId)}>" if int(reviewerId or 0) > 0 else "system"
    try:
        await orbatAuditRuntime.sendOrbatChangeLog(
            botClient,
            change=change,
            requestedBy=str(requestedBy or "").strip() or reviewerText,
            authorizedBy=reviewerText,
            requestMessageUrl=str(requestMessageUrl or "").strip(),
            details="",
            sheetKey=sheetKey,
        )
    except Exception:
        log.exception("Failed to post Honor-Guard ORBAT audit log.")

    detailParts: list[str] = []
    if hostMention:
        detailParts.append(f"Host: {hostMention}")
    if eventType:
        detailParts.append(f"Type: {eventType}")
    detailParts.append(f"Participants: {int(participantCount)}")
    if eventDateText:
        detailParts.append(f"Date: {eventDateText}")
    detailParts.append(f"Archived: {bool(archived)}")
    detailParts.append(f"Host Update: {bool(hostUpdated)}")
    if sheetStatus:
        detailParts.append(f"Sheet: {sheetStatus}")

    embed = discord.Embed(
        title="ORBAT Change",
        color=discord.Color.blurple(),
        timestamp=datetime.now(tz=timezone.utc),
    )
    embed.add_field(name="Change", value=str(change or "Unknown"), inline=False)
    embed.add_field(
        name="Requested By",
        value=str(requestedBy or "").strip() or reviewerText,
        inline=False,
    )
    embed.add_field(name="Authorized By", value=reviewerText, inline=False)
    requestUrl = str(requestMessageUrl or "").strip()
    embed.add_field(
        name="Request Message",
        value=f"[Open message]({requestUrl})" if requestUrl else "N/A",
        inline=False,
    )
    embed.add_field(name="Details", value=_clipFieldValue(" | ".join(detailParts)), inline=False)

    logs = [str(line) for line in (auditLogs or []) if str(line or "").strip()]
    if logs:
        embed.add_field(name="Changes", value=_clipFieldValue("\n\n".join(logs)), inline=False)

    if eventHostUpdate is not None:
        hostLabel = hostMention or hostRobloxUsername or "Host"
        hostRoblox = f" ({hostRobloxUsername})" if hostRobloxUsername else ""
        embed.add_field(
            name="​",
            value=(
                f"Host: {hostLabel}{hostRoblox} "
                f"{int(getattr(eventHostUpdate, 'previousValue', 0))} -> "
                f"{int(getattr(eventHostUpdate, 'value', 0))} hosted Events"
            ),
            inline=False,
        )

    await postHonorGuardLogEmbed(botClient, embed=embed)
