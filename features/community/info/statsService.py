from __future__ import annotations

from datetime import datetime, timedelta, timezone

import discord

from db.sqlite import execute, executeReturnId, fetchAll, fetchOne


def _todayUtcDate() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _normalizeDate(value: datetime | str) -> str:
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.date().isoformat()
    return str(value or "").strip()


async def captureGuildSnapshot(guild: discord.Guild) -> int:
    totalMembers = guild.member_count or len(guild.members)
    botCount = sum(1 for member in guild.members if member.bot)
    humanCount = max(0, totalMembers - botCount)
    return await executeReturnId(
        """
        INSERT INTO guild_stats_snapshots
            (guildId, memberCount, humanCount, botCount, textChannelCount, voiceChannelCount, forumChannelCount, stageChannelCount, roleCount, boostCount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(guild.id),
            int(totalMembers),
            int(humanCount),
            int(botCount),
            len(guild.text_channels),
            len(guild.voice_channels),
            len(guild.forums),
            len(guild.stage_channels),
            len(guild.roles),
            int(guild.premium_subscription_count or 0),
        ),
    )


async def getLatestSnapshot(guildId: int) -> dict | None:
    return await fetchOne(
        """
        SELECT *
        FROM guild_stats_snapshots
        WHERE guildId = ?
        ORDER BY datetime(capturedAt) DESC, snapshotId DESC
        LIMIT 1
        """,
        (int(guildId),),
    )


async def getLatestSnapshotBefore(guildId: int, beforeUtcIso: str) -> dict | None:
    return await fetchOne(
        """
        SELECT *
        FROM guild_stats_snapshots
        WHERE guildId = ? AND datetime(capturedAt) <= datetime(?)
        ORDER BY datetime(capturedAt) DESC, snapshotId DESC
        LIMIT 1
        """,
        (int(guildId), str(beforeUtcIso or "").strip()),
    )


async def listRecentSnapshots(guildId: int, *, limit: int = 14) -> list[dict]:
    return await fetchAll(
        """
        SELECT *
        FROM guild_stats_snapshots
        WHERE guildId = ?
        ORDER BY datetime(capturedAt) DESC, snapshotId DESC
        LIMIT ?
        """,
        (int(guildId), max(1, int(limit or 14))),
    )


async def pruneOldSnapshots(*, keepDays: int = 60) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(7, int(keepDays or 60)))).isoformat()
    await execute(
        "DELETE FROM guild_stats_snapshots WHERE datetime(capturedAt) < datetime(?)",
        (cutoff,),
    )


async def incrementJoinCount(guildId: int) -> None:
    await execute(
        """
        INSERT INTO guild_member_activity_daily (guildId, activityDate, joinCount, leaveCount)
        VALUES (?, ?, 1, 0)
        ON CONFLICT(guildId, activityDate)
        DO UPDATE SET joinCount = joinCount + 1
        """,
        (int(guildId), _todayUtcDate()),
    )


async def incrementLeaveCount(guildId: int) -> None:
    await execute(
        """
        INSERT INTO guild_member_activity_daily (guildId, activityDate, joinCount, leaveCount)
        VALUES (?, ?, 0, 1)
        ON CONFLICT(guildId, activityDate)
        DO UPDATE SET leaveCount = leaveCount + 1
        """,
        (int(guildId), _todayUtcDate()),
    )


async def incrementChannelMessageCount(guildId: int, channelId: int) -> None:
    await execute(
        """
        INSERT INTO guild_channel_activity_daily (guildId, channelId, activityDate, messageCount)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(guildId, channelId, activityDate)
        DO UPDATE SET messageCount = messageCount + 1
        """,
        (int(guildId), int(channelId), _todayUtcDate()),
    )


async def listMemberActivitySince(guildId: int, sinceDate: datetime | str) -> list[dict]:
    return await fetchAll(
        """
        SELECT *
        FROM guild_member_activity_daily
        WHERE guildId = ? AND activityDate >= ?
        ORDER BY activityDate ASC
        """,
        (int(guildId), _normalizeDate(sinceDate)),
    )


async def listChannelActivitySince(guildId: int, sinceDate: datetime | str, *, limit: int = 10) -> list[dict]:
    return await fetchAll(
        """
        SELECT
            guildId,
            channelId,
            SUM(messageCount) AS messageCount
        FROM guild_channel_activity_daily
        WHERE guildId = ? AND activityDate >= ?
        GROUP BY guildId, channelId
        ORDER BY messageCount DESC, channelId ASC
        LIMIT ?
        """,
        (int(guildId), _normalizeDate(sinceDate), max(1, int(limit or 10))),
    )


async def pruneOldActivity(*, keepDays: int = 90) -> None:
    cutoffDate = (datetime.now(timezone.utc) - timedelta(days=max(14, int(keepDays or 90)))).date().isoformat()
    await execute("DELETE FROM guild_member_activity_daily WHERE activityDate < ?", (cutoffDate,))
    await execute("DELETE FROM guild_channel_activity_daily WHERE activityDate < ?", (cutoffDate,))
