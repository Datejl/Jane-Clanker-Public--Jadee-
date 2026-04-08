from __future__ import annotations

from typing import Any, Optional

from features.staff.workflows import rendering as workflowRendering
from features.staff.workflows import service as workflowService

_ORBAT_WORKFLOW_KEY = "orbat-requests"
_ORBAT_SUBJECT_TYPE = "orbat_request"
_LOA_WORKFLOW_KEY = "loa-requests"
_LOA_SUBJECT_TYPE = "loa_request"


def _stateForOrbatStatus(status: object) -> str:
    normalized = str(status or "").strip().upper()
    if normalized == "APPROVED":
        return "approved"
    if normalized == "REJECTED":
        return "rejected"
    if normalized == "NEEDS_INFO":
        return "needs-info"
    if normalized == "PENDING":
        return "pending-review"
    return "submitted"


def _stateForLoaStatus(status: object) -> str:
    normalized = str(status or "").strip().upper()
    if normalized == "APPROVED":
        return "approved"
    if normalized == "REJECTED":
        return "rejected"
    if normalized == "NEEDS_INFO":
        return "needs-info"
    if normalized == "PENDING":
        return "pending-review"
    return "submitted"


def _orbatDisplayName(requestRow: dict[str, Any]) -> str:
    requestId = int(requestRow.get("requestId") or 0)
    submitterId = int(requestRow.get("submitterId") or 0)
    robloxUser = str(requestRow.get("robloxUser") or "").strip()
    if requestId > 0 and robloxUser:
        return f"ORBAT #{requestId} ({robloxUser})"
    if requestId > 0 and submitterId > 0:
        return f"ORBAT #{requestId} ({submitterId})"
    if requestId > 0:
        return f"ORBAT #{requestId}"
    return "ORBAT Request"


def _loaDisplayName(requestRow: dict[str, Any]) -> str:
    requestId = int(requestRow.get("requestId") or 0)
    submitterId = int(requestRow.get("submitterId") or 0)
    if requestId > 0 and submitterId > 0:
        return f"LOA #{requestId} ({submitterId})"
    if requestId > 0:
        return f"LOA #{requestId}"
    return "LOA Request"


def _orbatMetadata(requestRow: dict[str, Any]) -> dict[str, Any]:
    return {
        "requestId": int(requestRow.get("requestId") or 0),
        "submitterId": int(requestRow.get("submitterId") or 0),
        "robloxUser": str(requestRow.get("robloxUser") or "").strip(),
        "messageId": int(requestRow.get("messageId") or 0),
        "status": str(requestRow.get("status") or "").strip().upper(),
        "threadId": int(requestRow.get("threadId") or 0),
        "sheetRow": int(requestRow.get("sheetRow") or 0),
    }


def _loaMetadata(requestRow: dict[str, Any]) -> dict[str, Any]:
    return {
        "requestId": int(requestRow.get("requestId") or 0),
        "submitterId": int(requestRow.get("submitterId") or 0),
        "messageId": int(requestRow.get("messageId") or 0),
        "status": str(requestRow.get("status") or "").strip().upper(),
        "threadId": int(requestRow.get("threadId") or 0),
        "startDate": str(requestRow.get("startDate") or "").strip(),
        "endDate": str(requestRow.get("endDate") or "").strip(),
    }


async def syncOrbatWorkflow(
    requestRow: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    requestId = int(requestRow.get("requestId") or 0)
    guildId = int(requestRow.get("guildId") or 0)
    if requestId <= 0 or guildId <= 0:
        raise ValueError("ORBAT request row is missing workflow identifiers.")
    return await workflowService.transitionSubjectRun(
        workflowKey=_ORBAT_WORKFLOW_KEY,
        subjectType=_ORBAT_SUBJECT_TYPE,
        subjectId=requestId,
        guildId=guildId,
        stateKey=stateKey or _stateForOrbatStatus(requestRow.get("status")),
        actorId=actorId,
        note=note,
        eventType=eventType,
        displayName=_orbatDisplayName(requestRow),
        metadata=_orbatMetadata(requestRow),
        allowNoopEvent=allowNoopEvent,
    )


async def ensureOrbatWorkflowCurrent(requestRow: dict[str, Any]) -> dict[str, Any]:
    return await syncOrbatWorkflow(
        requestRow,
        actorId=None,
        note="Workflow synchronized from ORBAT request status.",
        eventType="SYNC",
        allowNoopEvent=False,
    )


async def getOrbatWorkflowSummary(requestRow: dict[str, Any]) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_ORBAT_WORKFLOW_KEY,
        subjectType=_ORBAT_SUBJECT_TYPE,
        subjectId=int(requestRow.get("requestId") or 0),
    )
    if not run:
        return ""
    latestEvent = await workflowService.getLatestRunEvent(int(run["runId"]))
    return workflowRendering.buildCompactSummary(run, latestEvent)


async def getOrbatWorkflowHistorySummary(requestRow: dict[str, Any], *, limit: int = 3) -> str:
    return await _getHistorySummary(
        workflowKey=_ORBAT_WORKFLOW_KEY,
        subjectType=_ORBAT_SUBJECT_TYPE,
        subjectId=int(requestRow.get("requestId") or 0),
        limit=limit,
    )


async def syncLoaWorkflow(
    requestRow: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    requestId = int(requestRow.get("requestId") or 0)
    guildId = int(requestRow.get("guildId") or 0)
    if requestId <= 0 or guildId <= 0:
        raise ValueError("LOA request row is missing workflow identifiers.")
    return await workflowService.transitionSubjectRun(
        workflowKey=_LOA_WORKFLOW_KEY,
        subjectType=_LOA_SUBJECT_TYPE,
        subjectId=requestId,
        guildId=guildId,
        stateKey=stateKey or _stateForLoaStatus(requestRow.get("status")),
        actorId=actorId,
        note=note,
        eventType=eventType,
        displayName=_loaDisplayName(requestRow),
        metadata=_loaMetadata(requestRow),
        allowNoopEvent=allowNoopEvent,
    )


async def ensureLoaWorkflowCurrent(requestRow: dict[str, Any]) -> dict[str, Any]:
    return await syncLoaWorkflow(
        requestRow,
        actorId=None,
        note="Workflow synchronized from LOA request status.",
        eventType="SYNC",
        allowNoopEvent=False,
    )


async def getLoaWorkflowSummary(requestRow: dict[str, Any]) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_LOA_WORKFLOW_KEY,
        subjectType=_LOA_SUBJECT_TYPE,
        subjectId=int(requestRow.get("requestId") or 0),
    )
    if not run:
        return ""
    latestEvent = await workflowService.getLatestRunEvent(int(run["runId"]))
    return workflowRendering.buildCompactSummary(run, latestEvent)


async def getLoaWorkflowHistorySummary(requestRow: dict[str, Any], *, limit: int = 3) -> str:
    return await _getHistorySummary(
        workflowKey=_LOA_WORKFLOW_KEY,
        subjectType=_LOA_SUBJECT_TYPE,
        subjectId=int(requestRow.get("requestId") or 0),
        limit=limit,
    )


async def reconcileOrbatWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    return await _reconcileRows(
        rows,
        workflowKey=_ORBAT_WORKFLOW_KEY,
        subjectType=_ORBAT_SUBJECT_TYPE,
        subjectIdField="requestId",
        ensureFn=ensureOrbatWorkflowCurrent,
    )


async def reconcileLoaWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    return await _reconcileRows(
        rows,
        workflowKey=_LOA_WORKFLOW_KEY,
        subjectType=_LOA_SUBJECT_TYPE,
        subjectIdField="requestId",
        ensureFn=ensureLoaWorkflowCurrent,
    )


async def _reconcileRows(
    rows: list[dict[str, Any]],
    *,
    workflowKey: str,
    subjectType: str,
    subjectIdField: str,
    ensureFn,
) -> tuple[int, int]:
    checked = 0
    changed = 0
    for row in rows:
        subjectId = int(row.get(subjectIdField) or 0)
        guildId = int(row.get("guildId") or 0)
        if subjectId <= 0 or guildId <= 0:
            continue
        checked += 1
        existingRun = await workflowService.getRunBySubject(
            workflowKey=workflowKey,
            subjectType=subjectType,
            subjectId=subjectId,
        )
        beforeUpdatedAt = str(existingRun.get("updatedAt") or "").strip() if existingRun else ""
        await ensureFn(row)
        afterRun = await workflowService.getRunBySubject(
            workflowKey=workflowKey,
            subjectType=subjectType,
            subjectId=subjectId,
        )
        afterUpdatedAt = str(afterRun.get("updatedAt") or "").strip() if afterRun else ""
        if existingRun is None or afterUpdatedAt != beforeUpdatedAt:
            changed += 1
    return checked, changed


async def _getHistorySummary(
    *,
    workflowKey: str,
    subjectType: str,
    subjectId: int,
    limit: int,
) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=workflowKey,
        subjectType=subjectType,
        subjectId=subjectId,
    )
    if not run:
        return ""
    rows = await workflowService.listRunEvents(int(run["runId"]), limit=max(1, min(int(limit or 3), 5)))
    if not rows:
        return ""
    return workflowRendering.buildWorkflowEventSummary(rows)


__all__ = [
    "ensureLoaWorkflowCurrent",
    "ensureOrbatWorkflowCurrent",
    "getLoaWorkflowHistorySummary",
    "getLoaWorkflowSummary",
    "getOrbatWorkflowHistorySummary",
    "getOrbatWorkflowSummary",
    "reconcileLoaWorkflowRows",
    "reconcileOrbatWorkflowRows",
    "syncLoaWorkflow",
    "syncOrbatWorkflow",
]

