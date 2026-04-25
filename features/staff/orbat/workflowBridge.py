from __future__ import annotations

from typing import Any, Optional

from features.staff.workflows.bridge import (
    WorkflowSubjectBridge,
    normalizedStatus,
    stateKeyForStatus,
)

_ORBAT_WORKFLOW_KEY = "orbat-requests"
_ORBAT_SUBJECT_TYPE = "orbat_request"
_LOA_WORKFLOW_KEY = "loa-requests"
_LOA_SUBJECT_TYPE = "loa_request"
_REQUEST_STATUS_STATES = {
    "APPROVED": "approved",
    "REJECTED": "rejected",
    "NEEDS_INFO": "needs-info",
    "PENDING": "pending-review",
}


def _stateForOrbatStatus(status: object) -> str:
    return stateKeyForStatus(status, _REQUEST_STATUS_STATES, default="submitted")


def _stateForLoaStatus(status: object) -> str:
    return stateKeyForStatus(status, _REQUEST_STATUS_STATES, default="submitted")


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
        "status": normalizedStatus(requestRow.get("status")),
        "threadId": int(requestRow.get("threadId") or 0),
        "sheetRow": int(requestRow.get("sheetRow") or 0),
    }


def _loaMetadata(requestRow: dict[str, Any]) -> dict[str, Any]:
    return {
        "requestId": int(requestRow.get("requestId") or 0),
        "submitterId": int(requestRow.get("submitterId") or 0),
        "messageId": int(requestRow.get("messageId") or 0),
        "status": normalizedStatus(requestRow.get("status")),
        "threadId": int(requestRow.get("threadId") or 0),
        "startDate": str(requestRow.get("startDate") or "").strip(),
        "endDate": str(requestRow.get("endDate") or "").strip(),
    }


_orbatBridge = WorkflowSubjectBridge(
    workflowKey=_ORBAT_WORKFLOW_KEY,
    subjectType=_ORBAT_SUBJECT_TYPE,
    subjectIdField="requestId",
    displayName=_orbatDisplayName,
    metadata=_orbatMetadata,
    stateForStatus=_stateForOrbatStatus,
    missingIdentifiersMessage="ORBAT request row is missing workflow identifiers.",
)
_loaBridge = WorkflowSubjectBridge(
    workflowKey=_LOA_WORKFLOW_KEY,
    subjectType=_LOA_SUBJECT_TYPE,
    subjectIdField="requestId",
    displayName=_loaDisplayName,
    metadata=_loaMetadata,
    stateForStatus=_stateForLoaStatus,
    missingIdentifiersMessage="LOA request row is missing workflow identifiers.",
)


async def syncOrbatWorkflow(
    requestRow: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    return await _orbatBridge.sync(
        requestRow,
        stateKey=stateKey,
        actorId=actorId,
        note=note,
        eventType=eventType,
        allowNoopEvent=allowNoopEvent,
    )


async def ensureOrbatWorkflowCurrent(requestRow: dict[str, Any]) -> dict[str, Any]:
    return await _orbatBridge.ensureCurrent(
        requestRow,
        note="Workflow synchronized from ORBAT request status.",
    )


async def getOrbatWorkflowSummary(requestRow: dict[str, Any]) -> str:
    return await _orbatBridge.summary(requestRow)


async def getOrbatWorkflowHistorySummary(requestRow: dict[str, Any], *, limit: int = 3) -> str:
    return await _orbatBridge.historySummary(requestRow, limit=limit)


async def syncLoaWorkflow(
    requestRow: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    return await _loaBridge.sync(
        requestRow,
        stateKey=stateKey,
        actorId=actorId,
        note=note,
        eventType=eventType,
        allowNoopEvent=allowNoopEvent,
    )


async def ensureLoaWorkflowCurrent(requestRow: dict[str, Any]) -> dict[str, Any]:
    return await _loaBridge.ensureCurrent(
        requestRow,
        note="Workflow synchronized from LOA request status.",
    )


async def getLoaWorkflowSummary(requestRow: dict[str, Any]) -> str:
    return await _loaBridge.summary(requestRow)


async def getLoaWorkflowHistorySummary(requestRow: dict[str, Any], *, limit: int = 3) -> str:
    return await _loaBridge.historySummary(requestRow, limit=limit)


async def reconcileOrbatWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    return await _orbatBridge.reconcileRows(rows, ensureFn=ensureOrbatWorkflowCurrent)


async def reconcileLoaWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    return await _loaBridge.reconcileRows(rows, ensureFn=ensureLoaWorkflowCurrent)


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

