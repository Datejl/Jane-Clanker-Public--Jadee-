from __future__ import annotations

import json
from typing import Any

from db.sqlite import execute, executeReturnId, fetchAll, fetchOne


def _normalizeText(value: object) -> str:
    return str(value or "").strip()


def normalizePollOptions(options: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in options:
        text = _normalizeText(value)
        if not text or text in normalized:
            continue
        normalized.append(text)
    return normalized


async def createPoll(
    *,
    guildId: int,
    channelId: int,
    creatorId: int,
    question: str,
    options: list[str],
    anonymous: bool = False,
    multiSelect: bool = False,
    roleGateIds: list[int] | None = None,
    hideResultsUntilClosed: bool = False,
    messageResultsToCreator: bool = False,
    closesAtIso: str | None = None,
) -> int:
    return await executeReturnId(
        """
        INSERT INTO community_polls
            (guildId, channelId, creatorId, question, optionsJson, anonymous, multiSelect, roleGateIdsJson, hideResultsUntilClosed, messageResultsToCreator, closesAt, status, updatedAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', datetime('now'))
        """,
        (
            int(guildId),
            int(channelId),
            int(creatorId),
            _normalizeText(question),
            json.dumps(normalizePollOptions(options), ensure_ascii=True),
            1 if anonymous else 0,
            1 if multiSelect else 0,
            json.dumps(_normalizePositiveIntList(roleGateIds), ensure_ascii=True),
            1 if hideResultsUntilClosed else 0,
            1 if messageResultsToCreator else 0,
            _normalizeText(closesAtIso) or None,
        ),
    )


async def setPollMessageId(pollId: int, messageId: int) -> None:
    await execute(
        """
        UPDATE community_polls
        SET messageId = ?, updatedAt = datetime('now')
        WHERE pollId = ?
        """,
        (int(messageId), int(pollId)),
    )


async def getPoll(pollId: int) -> dict[str, Any] | None:
    return await fetchOne("SELECT * FROM community_polls WHERE pollId = ?", (int(pollId),))


async def getPollByMessageId(messageId: int) -> dict[str, Any] | None:
    return await fetchOne("SELECT * FROM community_polls WHERE messageId = ?", (int(messageId),))


async def listOpenPolls() -> list[dict[str, Any]]:
    return await fetchAll(
        """
        SELECT *
        FROM community_polls
        WHERE status = 'OPEN'
        ORDER BY pollId ASC
        """
    )


async def listGuildPolls(guildId: int, *, status: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
    normalizedStatus = _normalizeText(status).upper()
    if normalizedStatus:
        return await fetchAll(
            """
            SELECT *
            FROM community_polls
            WHERE guildId = ? AND status = ?
            ORDER BY pollId DESC
            LIMIT ?
            """,
            (int(guildId), normalizedStatus, max(1, int(limit or 10))),
        )
    return await fetchAll(
        """
        SELECT *
        FROM community_polls
        WHERE guildId = ?
        ORDER BY pollId DESC
        LIMIT ?
        """,
        (int(guildId), max(1, int(limit or 10))),
    )


async def closePoll(pollId: int) -> None:
    await execute(
        """
        UPDATE community_polls
        SET status = 'CLOSED',
            closedAt = datetime('now'),
            updatedAt = datetime('now')
        WHERE pollId = ?
        """,
        (int(pollId),),
    )


async def listPollVotes(pollId: int) -> list[dict[str, Any]]:
    return await fetchAll(
        """
        SELECT *
        FROM community_poll_votes
        WHERE pollId = ?
        ORDER BY updatedAt ASC, userId ASC
        """,
        (int(pollId),),
    )


def _normalizePositiveIntList(values: list[int] | None) -> list[int]:
    out: list[int] = []
    for value in values or []:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in out:
            out.append(parsed)
    return out


def parseRoleGateIds(pollRow: dict[str, Any]) -> list[int]:
    raw = str(pollRow.get("roleGateIdsJson") or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return _normalizePositiveIntList(data)


async def getUserPollVote(pollId: int, userId: int) -> dict[str, Any] | None:
    return await fetchOne(
        """
        SELECT *
        FROM community_poll_votes
        WHERE pollId = ? AND userId = ?
        """,
        (int(pollId), int(userId)),
    )


async def setPollVote(pollId: int, userId: int, optionIndex: int) -> None:
    await setPollVotes(pollId, userId, [int(optionIndex)])


async def setPollVotes(pollId: int, userId: int, optionIndexes: list[int]) -> None:
    normalized = sorted({int(value) for value in optionIndexes if int(value) >= 0})
    if not normalized:
        return
    await execute(
        """
        INSERT INTO community_poll_votes (pollId, userId, optionIndex, optionIndexesJson, updatedAt)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(pollId, userId)
        DO UPDATE SET
            optionIndex = excluded.optionIndex,
            optionIndexesJson = excluded.optionIndexesJson,
            updatedAt = datetime('now')
        """,
        (
            int(pollId),
            int(userId),
            int(normalized[0]),
            json.dumps(normalized, ensure_ascii=True),
        ),
    )
