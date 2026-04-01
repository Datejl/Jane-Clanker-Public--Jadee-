from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import discord

from db.sqlite import fetchOne


def _safeInt(value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _reportChannelId(configModule: Any) -> int:
    configured = _safeInt(getattr(configModule, "automationReportChannelId", 0))
    if configured > 0:
        return configured
    return _safeInt(getattr(configModule, "auditLogChannelId", 0))


async def _resolveReportChannel(botClient: Any, taskBudgeter: Any, channelId: int) -> discord.TextChannel | discord.Thread | None:
    if channelId <= 0:
        return None
    channel = botClient.get_channel(channelId)
    if channel is None:
        try:
            channel = await taskBudgeter.runDiscord(lambda: botClient.fetch_channel(channelId))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException, discord.InvalidData):
            channel = None
    return channel if isinstance(channel, (discord.TextChannel, discord.Thread)) else None


async def _dailyDigestStats(nowUtc: datetime) -> dict[str, int]:
    tomorrow = (nowUtc + timedelta(days=1)).date().isoformat()
    today = nowUtc.date().isoformat()
    row = await fetchOne(
        """
        SELECT
            (SELECT COUNT(*) FROM reminders WHERE status = 'PENDING') AS pendingReminders,
            (SELECT COUNT(*) FROM sessions WHERE status = 'OPEN') AS openSessions,
            (
                SELECT COUNT(*)
                FROM loa_requests
                WHERE status = 'APPROVED'
                  AND date(endDate) >= date(?)
                  AND date(endDate) <= date(?)
            ) AS expiringLoas,
            (
                SELECT COUNT(*) FROM division_applications WHERE status IN ('PENDING', 'NEEDS_INFO')
            ) AS pendingApplications,
            (
                SELECT COUNT(*) FROM ribbon_requests WHERE status IN ('PENDING', 'NEEDS_INFO')
            ) AS pendingRibbons
        """,
        (today, tomorrow),
    ) or {}
    return {key: _safeInt(value) for key, value in row.items()}


async def _windowSummaryStats(days: int) -> dict[str, int]:
    row = await fetchOne(
        """
        SELECT
            (SELECT COUNT(*) FROM division_applications WHERE datetime(createdAt) >= datetime('now', ?)) AS applicationsCreated,
            (SELECT COUNT(*) FROM ribbon_requests WHERE datetime(createdAt) >= datetime('now', ?)) AS ribbonRequestsCreated,
            (SELECT COUNT(*) FROM suggestions WHERE datetime(createdAt) >= datetime('now', ?)) AS suggestionsCreated,
            (SELECT COUNT(*) FROM department_projects WHERE datetime(createdAt) >= datetime('now', ?)) AS projectsCreated,
            (SELECT COUNT(*) FROM scheduled_events WHERE datetime(createdAt) >= datetime('now', ?)) AS eventsCreated
        """,
        tuple([f"-{int(days)} days"] * 5),
    ) or {}
    return {key: _safeInt(value) for key, value in row.items()}


async def _sendEmbed(
    *,
    botClient: Any,
    configModule: Any,
    taskBudgeter: Any,
    title: str,
    color: discord.Color,
    fields: list[tuple[str, str, bool]],
) -> bool:
    channel = await _resolveReportChannel(botClient, taskBudgeter, _reportChannelId(configModule))
    if channel is None:
        return False
    embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
    for name, value, inline in fields:
        embed.add_field(name=name, value=value, inline=inline)
    try:
        await taskBudgeter.runDiscord(lambda: channel.send(embed=embed))
    except (discord.Forbidden, discord.HTTPException):
        return False
    return True


async def runDailyDigest(botClient: Any, configModule: Any, taskBudgeter: Any) -> bool:
    stats = await _dailyDigestStats(datetime.now(timezone.utc))
    return await _sendEmbed(
        botClient=botClient,
        configModule=configModule,
        taskBudgeter=taskBudgeter,
        title="Daily Automation Digest",
        color=discord.Color.blurple(),
        fields=[
            ("Live Queue", f"open sessions: `{stats['openSessions']}`\npending reminders: `{stats['pendingReminders']}`", True),
            ("Expiring Soon", f"LOAs ending in 24h: `{stats['expiringLoas']}`", True),
            (
                "Pending Review",
                f"applications: `{stats['pendingApplications']}`\nribbons: `{stats['pendingRibbons']}`",
                True,
            ),
        ],
    )


async def runWeeklySummary(botClient: Any, configModule: Any, taskBudgeter: Any) -> bool:
    stats = await _windowSummaryStats(7)
    return await _sendEmbed(
        botClient=botClient,
        configModule=configModule,
        taskBudgeter=taskBudgeter,
        title="Weekly Summary",
        color=discord.Color.green(),
        fields=[
            ("Created This Week", f"apps: `{stats['applicationsCreated']}`\nribbons: `{stats['ribbonRequestsCreated']}`", True),
            ("Community", f"suggestions: `{stats['suggestionsCreated']}`\nevents: `{stats['eventsCreated']}`", True),
            ("Projects", f"projects opened: `{stats['projectsCreated']}`", True),
        ],
    )


async def runMonthlyReport(botClient: Any, configModule: Any, taskBudgeter: Any) -> bool:
    stats = await _windowSummaryStats(30)
    return await _sendEmbed(
        botClient=botClient,
        configModule=configModule,
        taskBudgeter=taskBudgeter,
        title="Monthly Report",
        color=discord.Color.gold(),
        fields=[
            ("Created This Month", f"apps: `{stats['applicationsCreated']}`\nribbons: `{stats['ribbonRequestsCreated']}`", True),
            ("Community", f"suggestions: `{stats['suggestionsCreated']}`\nevents: `{stats['eventsCreated']}`", True),
            ("Projects", f"projects opened: `{stats['projectsCreated']}`", True),
        ],
    )
