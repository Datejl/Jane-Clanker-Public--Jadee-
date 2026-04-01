from __future__ import annotations

from db.sqlite import execute, executeReturnId, fetchAll


def _normalizeSubjectType(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"USER", "DIVISION", "PROCESS"} else "PROCESS"


def _normalizeSubjectKey(value: object) -> str:
    return str(value or "").strip()[:120]


async def addNote(
    *,
    guildId: int,
    subjectType: str,
    subjectKey: str,
    content: str,
    createdBy: int,
) -> int:
    return await executeReturnId(
        """
        INSERT INTO assistant_notes (
            guildId, subjectType, subjectKey, content, createdBy, createdAt, updatedAt
        )
        VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        """,
        (
            int(guildId),
            _normalizeSubjectType(subjectType),
            _normalizeSubjectKey(subjectKey),
            str(content or "").strip(),
            int(createdBy or 0),
        ),
    )


async def listNotes(
    *,
    guildId: int,
    subjectType: str,
    subjectKey: str,
    limit: int = 10,
) -> list[dict]:
    return await fetchAll(
        """
        SELECT noteId, guildId, subjectType, subjectKey, content, createdBy, createdAt, updatedAt
        FROM assistant_notes
        WHERE guildId = ? AND subjectType = ? AND subjectKey = ?
        ORDER BY datetime(updatedAt) DESC, noteId DESC
        LIMIT ?
        """,
        (
            int(guildId),
            _normalizeSubjectType(subjectType),
            _normalizeSubjectKey(subjectKey),
            max(1, min(25, int(limit or 10))),
        ),
    )


async def deleteNote(*, noteId: int, guildId: int) -> None:
    await execute(
        "DELETE FROM assistant_notes WHERE noteId = ? AND guildId = ?",
        (int(noteId), int(guildId)),
    )


async def listRecentNotes(*, guildId: int, limit: int = 15) -> list[dict]:
    return await fetchAll(
        """
        SELECT noteId, subjectType, subjectKey, content, createdBy, createdAt, updatedAt
        FROM assistant_notes
        WHERE guildId = ?
        ORDER BY datetime(updatedAt) DESC, noteId DESC
        LIMIT ?
        """,
        (int(guildId), max(1, min(50, int(limit or 15)))),
    )
