from __future__ import annotations

import json
from typing import Mapping, Optional

from db.sqlite import execute, fetchOne


def _normalizeHallType(hallType: str) -> str:
    value = str(hallType or "").strip().upper()
    if value not in {"FAME", "SHAME"}:
        raise ValueError(f"Unsupported hall type: {hallType}")
    return value


def _normalizeReactionBreakdown(reactionBreakdown: Mapping[str, object] | None) -> dict[str, int]:
    if not isinstance(reactionBreakdown, Mapping):
        return {}

    normalized: dict[str, int] = {}
    for rawEmoji, rawCount in reactionBreakdown.items():
        emoji = str(rawEmoji or "").strip()
        if not emoji:
            continue
        try:
            count = int(rawCount or 0)
        except (TypeError, ValueError):
            continue
        normalized[emoji] = max(0, count)
    return normalized


def _serializeReactionBreakdown(reactionBreakdown: Mapping[str, object] | None) -> str:
    return json.dumps(
        _normalizeReactionBreakdown(reactionBreakdown),
        ensure_ascii=False,
        sort_keys=True,
    )


def loadReactionBreakdown(hallPost: Optional[dict]) -> dict[str, int]:
    if not isinstance(hallPost, dict):
        return {}

    rawJson = hallPost.get("reactionBreakdownJson")
    if isinstance(rawJson, str) and rawJson.strip():
        try:
            parsed = json.loads(rawJson)
        except json.JSONDecodeError:
            parsed = None
        normalized = _normalizeReactionBreakdown(parsed if isinstance(parsed, Mapping) else None)
        if normalized:
            return normalized

    legacyEmoji = str(hallPost.get("reactionEmoji") or "").strip()
    if not legacyEmoji:
        return {}
    try:
        legacyCount = int(hallPost.get("reactionCount") or 0)
    except (TypeError, ValueError):
        legacyCount = 0
    return {legacyEmoji: max(0, legacyCount)}


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
    reactionBreakdown: Mapping[str, object] | None = None,
    postedMessageId: int,
) -> None:
    normalizedHallType = _normalizeHallType(hallType)
    await execute(
        """
        INSERT OR REPLACE INTO hall_reaction_posts
            (messageId, hallType, guildId, sourceChannelId, targetChannelId, sourceAuthorId, reactionEmoji, reactionCount, reactionBreakdownJson, postedMessageId, createdAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
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
            _serializeReactionBreakdown(reactionBreakdown),
            int(postedMessageId),
        ),
    )


async def updateHallPostReactionState(
    *,
    messageId: int,
    hallType: str,
    reactionEmoji: str,
    reactionCount: int,
    reactionBreakdown: Mapping[str, object] | None = None,
) -> None:
    normalizedHallType = _normalizeHallType(hallType)
    await execute(
        """
        UPDATE hall_reaction_posts
        SET reactionEmoji = ?, reactionCount = ?, reactionBreakdownJson = ?
        WHERE messageId = ? AND hallType = ?
        """,
        (
            str(reactionEmoji or ""),
            int(reactionCount),
            _serializeReactionBreakdown(reactionBreakdown),
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
