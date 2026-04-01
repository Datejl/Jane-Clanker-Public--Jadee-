from __future__ import annotations

from typing import Optional

from db.sqlite import execute, fetchOne


def _normalizeHallType(hallType: str) -> str:
    value = str(hallType or "").strip().upper()
    if value not in {"FAME", "SHAME"}:
        raise ValueError(f"Unsupported hall type: {hallType}")
    return value


async def getHallPost(messageId: int, hallType: str) -> Optional[dict]:
    normalizedHallType = _normalizeHallType(hallType)
    return await fetchOne(
        """
        SELECT *
        FROM hall_reaction_posts
        WHERE messageId = ? AND hallType = ?
        LIMIT 1
        """,
        (
            int(messageId),
            normalizedHallType,
        ),
    )


async def createHallPost(
    *,
    messageId: int,
    hallType: str,
    guildId: int,
    sourceChannelId: int,
    targetChannelId: int,
    sourceAuthorId: int,
    reactionEmoji: str,
    reactionCount: int,
    postedMessageId: int,
) -> None:
    normalizedHallType = _normalizeHallType(hallType)
    await execute(
        """
        INSERT OR REPLACE INTO hall_reaction_posts
            (messageId, hallType, guildId, sourceChannelId, targetChannelId, sourceAuthorId, reactionEmoji, reactionCount, postedMessageId, createdAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            int(messageId),
            normalizedHallType,
            int(guildId),
            int(sourceChannelId),
            int(targetChannelId),
            int(sourceAuthorId),
            str(reactionEmoji or ""),
            int(reactionCount),
            int(postedMessageId),
        ),
    )


async def updateHallPostCount(
    *,
    messageId: int,
    hallType: str,
    reactionCount: int,
) -> None:
    normalizedHallType = _normalizeHallType(hallType)
    await execute(
        """
        UPDATE hall_reaction_posts
        SET reactionCount = ?
        WHERE messageId = ? AND hallType = ?
        """,
        (
            int(reactionCount),
            int(messageId),
            normalizedHallType,
        ),
    )


async def deleteHallPost(messageId: int, hallType: str) -> None:
    normalizedHallType = _normalizeHallType(hallType)
    await execute(
        """
        DELETE FROM hall_reaction_posts
        WHERE messageId = ? AND hallType = ?
        """,
        (
            int(messageId),
            normalizedHallType,
        ),
    )
