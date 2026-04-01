from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

import aiosqlite

from db.sqlite import fetchAll, fetchOne, runWriteTransaction

from .definitions import WorkflowDefinition, getWorkflowDefinition


def _jsonText(value: object) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        text = value.strip()
        return text or "{}"
    try:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
    except Exception:
        return "{}"


async def getRunById(runId: int) -> Optional[dict[str, Any]]:
    return await fetchOne(
        "SELECT * FROM workflow_runs WHERE runId = ?",
        (int(runId),),
    )


async def getRunBySubject(
    *,
    workflowKey: str,
    subjectType: str,
    subjectId: int,
) -> Optional[dict[str, Any]]:
    return await fetchOne(
        """
        SELECT *
        FROM workflow_runs
        WHERE workflowKey = ? AND subjectType = ? AND subjectId = ?
        LIMIT 1
        """,
        (
            str(workflowKey or "").strip().lower(),
            str(subjectType or "").strip().lower(),
            int(subjectId or 0),
        ),
    )


async def listPendingRuns(
    *,
    guildId: int,
    workflowKey: Optional[str] = None,
    pendingWith: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    safeLimit = max(1, min(500, int(limit or 100)))
    whereParts = ["guildId = ?", "isTerminal = 0"]
    params: list[Any] = [int(guildId or 0)]

    normalizedWorkflowKey = str(workflowKey or "").strip().lower()
    if normalizedWorkflowKey:
        whereParts.append("workflowKey = ?")
        params.append(normalizedWorkflowKey)

    normalizedPendingWith = str(pendingWith or "").strip().lower()
    if normalizedPendingWith:
        whereParts.append("pendingWith = ?")
        params.append(normalizedPendingWith)

    whereSql = " AND ".join(whereParts)
    rows = await fetchAll(
        f"""
        SELECT *
        FROM workflow_runs
        WHERE {whereSql}
        ORDER BY updatedAt DESC, runId DESC
        LIMIT ?
        """,
        tuple(params + [safeLimit]),
    )
    return rows


async def countPendingRuns(
    *,
    guildId: int,
    workflowKey: Optional[str] = None,
    pendingWith: Optional[str] = None,
) -> int:
    whereParts = ["guildId = ?", "isTerminal = 0"]
    params: list[Any] = [int(guildId or 0)]

    normalizedWorkflowKey = str(workflowKey or "").strip().lower()
    if normalizedWorkflowKey:
        whereParts.append("workflowKey = ?")
        params.append(normalizedWorkflowKey)

    normalizedPendingWith = str(pendingWith or "").strip().lower()
    if normalizedPendingWith:
        whereParts.append("pendingWith = ?")
        params.append(normalizedPendingWith)

    whereSql = " AND ".join(whereParts)
    row = await fetchOne(
        f"""
        SELECT COUNT(*) AS countValue
        FROM workflow_runs
        WHERE {whereSql}
        """,
        tuple(params),
    )
    if not row:
        return 0
    try:
        return int(row.get("countValue") or 0)
    except (TypeError, ValueError):
        return 0


async def listRunEvents(runId: int, *, limit: int = 20) -> list[dict[str, Any]]:
    safeLimit = max(1, min(100, int(limit or 20)))
    return await fetchAll(
        """
        SELECT *
        FROM workflow_events
        WHERE runId = ?
        ORDER BY eventId DESC
        LIMIT ?
        """,
        (int(runId), safeLimit),
    )


async def getLatestRunEvent(runId: int) -> Optional[dict[str, Any]]:
    return await fetchOne(
        """
        SELECT *
        FROM workflow_events
        WHERE runId = ?
        ORDER BY eventId DESC
        LIMIT 1
        """,
        (int(runId),),
    )


async def transitionSubjectRun(
    *,
    workflowKey: str,
    subjectType: str,
    subjectId: int,
    guildId: int,
    stateKey: str,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    displayName: str = "",
    metadata: Optional[dict[str, Any]] = None,
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    definition = getWorkflowDefinition(workflowKey)
    return await _transitionRun(
        definition=definition,
        subjectType=subjectType,
        subjectId=int(subjectId or 0),
        guildId=int(guildId or 0),
        stateKey=stateKey,
        actorId=actorId,
        note=note,
        eventType=eventType,
        displayName=displayName,
        metadata=metadata,
        allowNoopEvent=allowNoopEvent,
    )


async def ensureRun(
    *,
    workflowKey: str,
    subjectType: str,
    subjectId: int,
    guildId: int,
    initialStateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    displayName: str = "",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    definition = getWorkflowDefinition(workflowKey)
    return await _transitionRun(
        definition=definition,
        subjectType=subjectType,
        subjectId=int(subjectId or 0),
        guildId=int(guildId or 0),
        stateKey=str(initialStateKey or definition.defaultStateKey),
        actorId=actorId,
        note=note,
        eventType="RUN_CREATED",
        displayName=displayName,
        metadata=metadata,
        allowNoopEvent=False,
    )


async def _transitionRun(
    *,
    definition: WorkflowDefinition,
    subjectType: str,
    subjectId: int,
    guildId: int,
    stateKey: str,
    actorId: Optional[int],
    note: str,
    eventType: str,
    displayName: str,
    metadata: Optional[dict[str, Any]],
    allowNoopEvent: bool,
) -> dict[str, Any]:
    normalizedSubjectType = str(subjectType or definition.subjectType).strip().lower()
    safeSubjectId = int(subjectId or 0)
    if safeSubjectId <= 0:
        raise ValueError("subjectId must be a positive integer.")

    state = definition.getState(stateKey)
    safeDisplayName = str(displayName or "").strip()
    metadataJson = _jsonText(metadata)
    safeEventType = str(eventType or "STATE_CHANGE").strip().upper()
    safeNote = str(note or "").strip()
    safeGuildId = int(guildId or 0)
    safeActorId = int(actorId or 0) if actorId else None

    async def _callback(db: aiosqlite.Connection) -> dict[str, Any]:
        existing = await _txFetchOne(
            db,
            """
            SELECT *
            FROM workflow_runs
            WHERE workflowKey = ? AND subjectType = ? AND subjectId = ?
            LIMIT 1
            """,
            (
                definition.key,
                normalizedSubjectType,
                safeSubjectId,
            ),
        )

        if existing is None:
            if not definition.isTransitionAllowed(fromStateKey=None, toStateKey=state.key):
                raise ValueError(
                    f"Transition into '{state.key}' is not allowed as the initial state for workflow '{definition.key}'."
                )
            runId = await _insertRun(
                db,
                definition=definition,
                subjectType=normalizedSubjectType,
                subjectId=safeSubjectId,
                guildId=safeGuildId,
                state=state,
                actorId=safeActorId,
                displayName=safeDisplayName,
                metadataJson=metadataJson,
            )
            await _insertEvent(
                db,
                runId=runId,
                definition=definition,
                subjectType=normalizedSubjectType,
                subjectId=safeSubjectId,
                actorId=safeActorId,
                fromStateKey=None,
                toStateKey=state.key,
                toStateLabel=state.label,
                eventType=safeEventType,
                note=safeNote,
                detailsJson=metadataJson,
            )
            row = await _txFetchOne(
                db,
                "SELECT * FROM workflow_runs WHERE runId = ?",
                (runId,),
            )
            if row is None:
                raise RuntimeError("Workflow run was created but could not be reloaded.")
            return row

        currentStateKey = str(existing.get("currentStateKey") or "").strip().lower()
        existingDisplayName = str(existing.get("displayName") or "").strip()
        existingMetadataJson = str(existing.get("metadataJson") or "{}").strip() or "{}"
        metadataChanged = existingMetadataJson != metadataJson
        displayNameChanged = bool(safeDisplayName) and existingDisplayName != safeDisplayName
        sameState = currentStateKey == state.key

        if not sameState and not definition.isTransitionAllowed(fromStateKey=currentStateKey, toStateKey=state.key):
            raise ValueError(
                f"Transition '{currentStateKey or '(none)'}' -> '{state.key}' is not allowed for workflow '{definition.key}'."
            )

        if sameState and not allowNoopEvent:
            if metadataChanged or displayNameChanged:
                await db.execute(
                    """
                    UPDATE workflow_runs
                    SET displayName = ?,
                        metadataJson = ?,
                        updatedAt = datetime('now')
                    WHERE runId = ?
                    """,
                    (
                        safeDisplayName or existingDisplayName,
                        metadataJson,
                        int(existing["runId"]),
                    ),
                )
            row = await _txFetchOne(
                db,
                "SELECT * FROM workflow_runs WHERE runId = ?",
                (int(existing["runId"]),),
            )
            if row is None:
                raise RuntimeError("Workflow run disappeared after refresh.")
            return row

        await db.execute(
            """
            UPDATE workflow_runs
            SET displayName = ?,
                currentStateKey = ?,
                currentStateLabel = ?,
                pendingWith = ?,
                isTerminal = ?,
                metadataJson = ?,
                updatedAt = datetime('now'),
                closedAt = CASE WHEN ? = 1 THEN datetime('now') ELSE NULL END
            WHERE runId = ?
            """,
            (
                safeDisplayName or existingDisplayName,
                state.key,
                state.label,
                state.pendingWith,
                1 if state.isTerminal else 0,
                metadataJson if metadataChanged else existingMetadataJson,
                1 if state.isTerminal else 0,
                int(existing["runId"]),
            ),
        )
        await _insertEvent(
            db,
            runId=int(existing["runId"]),
            definition=definition,
            subjectType=normalizedSubjectType,
            subjectId=safeSubjectId,
            actorId=safeActorId,
            fromStateKey=currentStateKey or None,
            toStateKey=state.key,
            toStateLabel=state.label,
            eventType=safeEventType,
            note=safeNote,
            detailsJson=metadataJson if metadataChanged else existingMetadataJson,
        )
        row = await _txFetchOne(
            db,
            "SELECT * FROM workflow_runs WHERE runId = ?",
            (int(existing["runId"]),),
        )
        if row is None:
            raise RuntimeError("Workflow run disappeared after update.")
        return row

    return await runWriteTransaction(_callback)


async def _insertRun(
    db: aiosqlite.Connection,
    *,
    definition: WorkflowDefinition,
    subjectType: str,
    subjectId: int,
    guildId: int,
    state: Any,
    actorId: Optional[int],
    displayName: str,
    metadataJson: str,
) -> int:
    try:
        cursor = await db.execute(
            """
            INSERT INTO workflow_runs (
                workflowKey,
                subjectType,
                subjectId,
                guildId,
                displayName,
                currentStateKey,
                currentStateLabel,
                pendingWith,
                isTerminal,
                createdBy,
                metadataJson,
                createdAt,
                updatedAt,
                closedAt
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), ?)
            """,
            (
                definition.key,
                subjectType,
                subjectId,
                guildId,
                displayName,
                state.key,
                state.label,
                state.pendingWith,
                1 if state.isTerminal else 0,
                int(actorId or 0),
                metadataJson,
                None,
            ),
        )
    except sqlite3.IntegrityError:
        existing = await _txFetchOne(
            db,
            """
            SELECT runId
            FROM workflow_runs
            WHERE workflowKey = ? AND subjectType = ? AND subjectId = ?
            LIMIT 1
            """,
            (definition.key, subjectType, subjectId),
        )
        if existing is None:
            raise
        return int(existing["runId"])

    runId = int(cursor.lastrowid or 0)
    if runId <= 0:
        existing = await _txFetchOne(
            db,
            """
            SELECT runId
            FROM workflow_runs
            WHERE workflowKey = ? AND subjectType = ? AND subjectId = ?
            LIMIT 1
            """,
            (definition.key, subjectType, subjectId),
        )
        if existing is None:
            raise RuntimeError("Workflow run insert returned no row id.")
        runId = int(existing["runId"])

    if state.isTerminal:
        await db.execute(
            """
            UPDATE workflow_runs
            SET closedAt = datetime('now')
            WHERE runId = ?
            """,
            (runId,),
        )
    return runId


async def _insertEvent(
    db: aiosqlite.Connection,
    *,
    runId: int,
    definition: WorkflowDefinition,
    subjectType: str,
    subjectId: int,
    actorId: Optional[int],
    fromStateKey: Optional[str],
    toStateKey: str,
    toStateLabel: str,
    eventType: str,
    note: str,
    detailsJson: str,
) -> None:
    await db.execute(
        """
        INSERT INTO workflow_events (
            runId,
            workflowKey,
            subjectType,
            subjectId,
            actorId,
            fromStateKey,
            toStateKey,
            toStateLabel,
            eventType,
            note,
            detailsJson
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(runId),
            definition.key,
            subjectType,
            int(subjectId),
            int(actorId or 0) if actorId else None,
            str(fromStateKey or "").strip().lower() or None,
            str(toStateKey or "").strip().lower(),
            str(toStateLabel or "").strip(),
            str(eventType or "STATE_CHANGE").strip().upper(),
            str(note or "").strip(),
            detailsJson,
        ),
    )


async def _txFetchOne(
    db: aiosqlite.Connection,
    query: str,
    params: tuple[Any, ...] = (),
) -> Optional[dict[str, Any]]:
    async with db.execute(query, params) as cur:
        row = await cur.fetchone()
        return dict(row) if row else None


__all__ = [
    "countPendingRuns",
    "ensureRun",
    "getLatestRunEvent",
    "getRunById",
    "getRunBySubject",
    "listPendingRuns",
    "listRunEvents",
    "transitionSubjectRun",
]
