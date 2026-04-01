from __future__ import annotations

from typing import Optional

from db.sqlite import execute, executeReturnId, fetchAll, fetchOne


PROJECT_STATUSES = {
    "PENDING_APPROVAL",
    "APPROVED",
    "DENIED",
    "SUBMITTED",
    "FINALIZED",
}


def _normalizeText(value: object) -> str:
    return str(value or "").strip()


def _normalizeOptionalText(value: object | None) -> str | None:
    text = _normalizeText(value)
    return text if text else None


def _normalizeStatus(value: object, *, default: str = "PENDING_APPROVAL") -> str:
    status = str(value or "").strip().upper()
    if status in PROJECT_STATUSES:
        return status
    return default


async def createProject(
    *,
    guildId: int,
    channelId: int,
    creatorId: int,
    title: str,
    idea: str,
    requestedPoints: int,
) -> int:
    return await executeReturnId(
        """
        INSERT INTO department_projects
            (guildId, channelId, creatorId, title, idea, requestedPoints, status, updatedAt)
        VALUES (?, ?, ?, ?, ?, ?, 'PENDING_APPROVAL', datetime('now'))
        """,
        (
            int(guildId),
            int(channelId),
            int(creatorId),
            _normalizeText(title),
            _normalizeText(idea),
            max(0, int(requestedPoints or 0)),
        ),
    )


async def getProject(projectId: int) -> Optional[dict]:
    return await fetchOne(
        """
        SELECT *
        FROM department_projects
        WHERE projectId = ?
        """,
        (int(projectId),),
    )


async def setProjectReviewMessage(
    *,
    projectId: int,
    reviewChannelId: int,
    reviewMessageId: int,
) -> None:
    await execute(
        """
        UPDATE department_projects
        SET reviewChannelId = ?,
            reviewMessageId = ?,
            updatedAt = datetime('now')
        WHERE projectId = ?
        """,
        (
            int(reviewChannelId),
            int(reviewMessageId),
            int(projectId),
        ),
    )


async def setProjectThreadId(projectId: int, threadId: int) -> None:
    await execute(
        """
        UPDATE department_projects
        SET threadId = ?,
            updatedAt = datetime('now')
        WHERE projectId = ?
        """,
        (int(threadId), int(projectId)),
    )


async def markProjectApproved(
    *,
    projectId: int,
    reviewerId: int,
    note: str | None = None,
) -> None:
    await execute(
        """
        UPDATE department_projects
        SET status = 'APPROVED',
            hodReviewerId = ?,
            hodReviewNote = ?,
            hodReviewedAt = datetime('now'),
            closedAt = NULL,
            updatedAt = datetime('now')
        WHERE projectId = ?
        """,
        (
            int(reviewerId),
            _normalizeOptionalText(note),
            int(projectId),
        ),
    )


async def markProjectDenied(
    *,
    projectId: int,
    reviewerId: int,
    note: str | None = None,
) -> None:
    await execute(
        """
        UPDATE department_projects
        SET status = 'DENIED',
            hodReviewerId = ?,
            hodReviewNote = ?,
            hodReviewedAt = datetime('now'),
            closedAt = datetime('now'),
            updatedAt = datetime('now')
        WHERE projectId = ?
        """,
        (
            int(reviewerId),
            _normalizeOptionalText(note),
            int(projectId),
        ),
    )


async def markProjectSubmitted(
    *,
    projectId: int,
    summary: str,
    proof: str | None = None,
) -> None:
    await execute(
        """
        UPDATE department_projects
        SET status = 'SUBMITTED',
            submitSummary = ?,
            submitProof = ?,
            submittedAt = datetime('now'),
            closedAt = NULL,
            updatedAt = datetime('now')
        WHERE projectId = ?
        """,
        (
            _normalizeText(summary),
            _normalizeOptionalText(proof),
            int(projectId),
        ),
    )


async def markProjectFinalized(
    *,
    projectId: int,
    reviewerId: int,
    awardedPoints: int,
    note: str | None = None,
) -> None:
    await execute(
        """
        UPDATE department_projects
        SET status = 'FINALIZED',
            finalReviewerId = ?,
            finalReviewNote = ?,
            awardedPoints = ?,
            finalizedAt = datetime('now'),
            closedAt = datetime('now'),
            updatedAt = datetime('now')
        WHERE projectId = ?
        """,
        (
            int(reviewerId),
            _normalizeOptionalText(note),
            max(0, int(awardedPoints or 0)),
            int(projectId),
        ),
    )


async def listProjects(
    *,
    guildId: int,
    statuses: Optional[list[str]] = None,
    creatorId: int | None = None,
    limit: int = 25,
) -> list[dict]:
    safeStatuses = [
        _normalizeStatus(status)
        for status in (statuses or [])
        if _normalizeStatus(status) in PROJECT_STATUSES
    ]
    params: list[object] = [int(guildId)]
    whereParts: list[str] = ["guildId = ?"]
    if safeStatuses:
        placeholders = ", ".join("?" for _ in safeStatuses)
        whereParts.append(f"status IN ({placeholders})")
        params.extend(safeStatuses)
    if creatorId is not None and int(creatorId) > 0:
        whereParts.append("creatorId = ?")
        params.append(int(creatorId))
    safeLimit = max(1, min(100, int(limit or 25)))
    params.append(safeLimit)
    whereSql = " AND ".join(whereParts)
    return await fetchAll(
        f"""
        SELECT *
        FROM department_projects
        WHERE {whereSql}
        ORDER BY datetime(updatedAt) DESC, projectId DESC
        LIMIT ?
        """,
        tuple(params),
    )


async def listProjectsForWorkflowReconciliation() -> list[dict]:
    return await fetchAll(
        """
        SELECT *
        FROM department_projects
        ORDER BY datetime(updatedAt) DESC, projectId DESC
        """
    )


async def appendProjectHistory(
    *,
    projectId: int,
    guildId: int,
    actorId: int | None,
    action: str,
    fromStatus: str | None = None,
    toStatus: str | None = None,
    note: str | None = None,
) -> int:
    return await executeReturnId(
        """
        INSERT INTO department_project_history
            (projectId, guildId, actorId, action, fromStatus, toStatus, note)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(projectId),
            int(guildId),
            int(actorId) if actorId is not None else None,
            _normalizeText(action).upper() or "UPDATE",
            _normalizeOptionalText(fromStatus),
            _normalizeOptionalText(toStatus),
            _normalizeOptionalText(note),
        ),
    )


async def listProjectHistory(projectId: int, *, limit: int = 30) -> list[dict]:
    safeLimit = max(1, min(200, int(limit or 30)))
    return await fetchAll(
        """
        SELECT *
        FROM department_project_history
        WHERE projectId = ?
        ORDER BY historyId DESC
        LIMIT ?
        """,
        (int(projectId), safeLimit),
    )
