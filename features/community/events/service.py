from __future__ import annotations

import json
from typing import Optional

from db.sqlite import execute, executeReturnId, fetchAll, fetchOne


def _normalizeText(value: object) -> str:
    return str(value or "").strip()


def _normalizeOptionalText(value: object | None) -> str | None:
    normalized = _normalizeText(value)
    return normalized if normalized else None


def _normalizePositiveIntList(values: Optional[list[int]]) -> list[int]:
    out: list[int] = []
    for value in values or []:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in out:
            out.append(parsed)
    return out


async def createScheduledEvent(
    *,
    guildId: int,
    channelId: int,
    creatorId: int,
    title: str,
    subtitle: Optional[str],
    eventAtUtcIso: str,
    timezoneName: str,
    maxAttendees: int = 0,
    lockRsvpAtStart: bool = False,
    pingRoleIds: Optional[list[int]] = None,
) -> int:
    pingRoleIdsJson = json.dumps(_normalizePositiveIntList(pingRoleIds))
    return await executeReturnId(
        """
        INSERT INTO scheduled_events
            (guildId, channelId, creatorId, title, subtitle, eventAtUtc, timezone, maxAttendees, lockRsvpAtStart, pingRoleIdsJson, status, updatedAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', datetime('now'))
        """,
        (
            int(guildId),
            int(channelId),
            int(creatorId),
            _normalizeText(title),
            _normalizeOptionalText(subtitle),
            _normalizeText(eventAtUtcIso),
            _normalizeText(timezoneName),
            max(0, int(maxAttendees or 0)),
            1 if bool(lockRsvpAtStart) else 0,
            pingRoleIdsJson,
        ),
    )


async def setScheduledEventMessageId(eventId: int, messageId: int) -> None:
    await execute(
        """
        UPDATE scheduled_events
        SET messageId = ?, updatedAt = datetime('now')
        WHERE eventId = ?
        """,
        (int(messageId), int(eventId)),
    )


async def getScheduledEvent(eventId: int) -> Optional[dict]:
    return await fetchOne(
        "SELECT * FROM scheduled_events WHERE eventId = ?",
        (int(eventId),),
    )


async def listActiveScheduledEvents() -> list[dict]:
    return await fetchAll(
        """
        SELECT *
        FROM scheduled_events
        WHERE status = 'ACTIVE'
        ORDER BY datetime(eventAtUtc) ASC, eventId ASC
        """,
    )


async def listActiveScheduledEventsWithRsvpCounts() -> list[dict]:
    return await fetchAll(
        """
        SELECT
            se.*,
            COALESCE(SUM(CASE WHEN sr.response = 'ATTENDING' THEN 1 ELSE 0 END), 0) AS attendingCount,
            COALESCE(SUM(CASE WHEN sr.response = 'TENTATIVE' THEN 1 ELSE 0 END), 0) AS tentativeCount
        FROM scheduled_events AS se
        LEFT JOIN scheduled_event_rsvps AS sr
            ON sr.eventId = se.eventId
        WHERE se.status = 'ACTIVE'
        GROUP BY se.eventId
        ORDER BY datetime(se.eventAtUtc) ASC, se.eventId ASC
        """,
    )


async def findDuplicateActiveEvent(
    *,
    guildId: int,
    creatorId: int,
    title: str,
    eventAtUtcIso: str,
    excludeEventId: Optional[int] = None,
) -> Optional[dict]:
    normalizedTitle = _normalizeText(title).casefold()
    if not normalizedTitle:
        return None
    query = """
        SELECT *
        FROM scheduled_events
        WHERE guildId = ?
          AND creatorId = ?
          AND status = 'ACTIVE'
          AND lower(trim(title)) = ?
          AND eventAtUtc = ?
    """
    params: list[object] = [
        int(guildId),
        int(creatorId),
        normalizedTitle,
        _normalizeText(eventAtUtcIso),
    ]
    if excludeEventId is not None:
        query += " AND eventId != ?"
        params.append(int(excludeEventId))
    query += " ORDER BY eventId DESC LIMIT 1"
    return await fetchOne(query, tuple(params))


async def updateScheduledEventDetails(
    eventId: int,
    *,
    title: str,
    subtitle: Optional[str],
    eventAtUtcIso: str,
    timezoneName: str,
    maxAttendees: int = 0,
    lockRsvpAtStart: bool = False,
    pingRoleIds: Optional[list[int]] = None,
) -> None:
    pingRoleIdsJson = json.dumps(_normalizePositiveIntList(pingRoleIds))
    await execute(
        """
        UPDATE scheduled_events
        SET title = ?,
            subtitle = ?,
            eventAtUtc = ?,
            timezone = ?,
            maxAttendees = ?,
            lockRsvpAtStart = ?,
            pingRoleIdsJson = ?,
            reminderSentAt = NULL,
            reminderThreadId = NULL,
            updatedAt = datetime('now')
        WHERE eventId = ?
        """,
        (
            _normalizeText(title),
            _normalizeOptionalText(subtitle),
            _normalizeText(eventAtUtcIso),
            _normalizeText(timezoneName),
            max(0, int(maxAttendees or 0)),
            1 if bool(lockRsvpAtStart) else 0,
            pingRoleIdsJson,
            int(eventId),
        ),
    )


async def markScheduledEventDeleted(eventId: int) -> None:
    await execute(
        """
        UPDATE scheduled_events
        SET status = 'DELETED',
            deletedAt = datetime('now'),
            updatedAt = datetime('now')
        WHERE eventId = ?
        """,
        (int(eventId),),
    )


async def markScheduledEventReminderSent(
    eventId: int,
    *,
    reminderThreadId: Optional[int] = None,
) -> None:
    await execute(
        """
        UPDATE scheduled_events
        SET reminderSentAt = datetime('now'),
            reminderThreadId = ?,
            updatedAt = datetime('now')
        WHERE eventId = ?
        """,
        (
            int(reminderThreadId) if reminderThreadId is not None else None,
            int(eventId),
        ),
    )


async def clearScheduledEventReminder(eventId: int) -> None:
    await execute(
        """
        UPDATE scheduled_events
        SET reminderSentAt = NULL,
            reminderThreadId = NULL,
            updatedAt = datetime('now')
        WHERE eventId = ?
        """,
        (int(eventId),),
    )


async def setScheduledEventRsvp(eventId: int, userId: int, response: str) -> None:
    normalized = str(response or "").strip().upper()
    if normalized not in {"ATTENDING", "TENTATIVE"}:
        return
    await execute(
        """
        INSERT INTO scheduled_event_rsvps (eventId, userId, response, updatedAt)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(eventId, userId)
        DO UPDATE SET response = excluded.response, updatedAt = datetime('now')
        """,
        (int(eventId), int(userId), normalized),
    )


async def clearScheduledEventRsvp(eventId: int, userId: int) -> None:
    await execute(
        "DELETE FROM scheduled_event_rsvps WHERE eventId = ? AND userId = ?",
        (int(eventId), int(userId)),
    )


async def listScheduledEventRsvps(eventId: int) -> list[dict]:
    return await fetchAll(
        """
        SELECT *
        FROM scheduled_event_rsvps
        WHERE eventId = ?
        ORDER BY updatedAt ASC, userId ASC
        """,
        (int(eventId),),
    )
