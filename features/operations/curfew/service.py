from __future__ import annotations

from typing import Optional

from db.sqlite import execute, fetchAll, fetchOne


async def upsertCurfewTarget(
    *,
    guildId: int,
    userId: int,
    timezoneName: str,
    addedBy: int,
) -> None:
    await execute(
        """
        INSERT INTO curfew_targets (guildId, userId, timezone, enabled, addedBy, createdAt, updatedAt)
        VALUES (?, ?, ?, 1, ?, datetime('now'), datetime('now'))
        ON CONFLICT(guildId, userId)
        DO UPDATE SET
            timezone = excluded.timezone,
            enabled = 1,
            addedBy = excluded.addedBy,
            updatedAt = datetime('now')
        """,
        (
            int(guildId),
            int(userId),
            str(timezoneName or "").strip(),
            int(addedBy),
        ),
    )


async def disableCurfewTarget(*, guildId: int, userId: int) -> None:
    await execute(
        """
        UPDATE curfew_targets
        SET enabled = 0, updatedAt = datetime('now')
        WHERE guildId = ? AND userId = ?
        """,
        (int(guildId), int(userId)),
    )


async def getCurfewTarget(*, guildId: int, userId: int) -> Optional[dict]:
    return await fetchOne(
        """
        SELECT *
        FROM curfew_targets
        WHERE guildId = ? AND userId = ?
        """,
        (int(guildId), int(userId)),
    )


async def listGuildCurfewTargets(*, guildId: int, includeDisabled: bool = False) -> list[dict]:
    query = """
        SELECT *
        FROM curfew_targets
        WHERE guildId = ?
    """
    params: tuple[object, ...] = (int(guildId),)
    if not includeDisabled:
        query += " AND enabled = 1"
    query += " ORDER BY enabled DESC, userId ASC"
    return await fetchAll(query, params)


async def listActiveCurfewTargets() -> list[dict]:
    return await fetchAll(
        """
        SELECT *
        FROM curfew_targets
        WHERE enabled = 1
        ORDER BY guildId ASC, userId ASC
        """,
    )


async def setCurfewAppliedAt(*, guildId: int, userId: int, appliedAtIso: str) -> None:
    await execute(
        """
        UPDATE curfew_targets
        SET lastAppliedAt = ?, updatedAt = datetime('now')
        WHERE guildId = ? AND userId = ?
        """,
        (
            str(appliedAtIso or "").strip(),
            int(guildId),
            int(userId),
        ),
    )
