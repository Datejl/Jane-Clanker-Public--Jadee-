from __future__ import annotations

from typing import Any, Optional

from features.staff.workflows.bridge import (
    WorkflowSubjectBridge,
    normalizedStatus,
    stateKeyForStatus,
)

_RIBBON_WORKFLOW_KEY = "ribbons"
_RIBBON_SUBJECT_TYPE = "ribbon_request"
_RIBBON_STATUS_STATES = {
    "APPROVED": "approved",
    "REJECTED": "rejected",
    "NEEDS_INFO": "needs-info",
    "CANCELED": "canceled",
    "PENDING": "pending-review",
}


def _stateForRibbonStatus(status: object) -> str:
    return stateKeyForStatus(status, _RIBBON_STATUS_STATES, default="submitted")


def _ribbonDisplayName(requestRow: dict[str, Any]) -> str:
    requestCode = str(requestRow.get("requestCode") or "").strip()
    requesterId = int(requestRow.get("requesterId") or 0)
    if requestCode and requesterId > 0:
        return f"{requestCode} ({requesterId})"
    if requestCode:
        return requestCode
    requestId = int(requestRow.get("requestId") or 0)
    if requestId > 0:
        return f"Ribbon Request {requestId}"
    return "Ribbon Request"


def _ribbonMetadata(requestRow: dict[str, Any]) -> dict[str, Any]:
    return {
        "requestId": int(requestRow.get("requestId") or 0),
        "requestCode": str(requestRow.get("requestCode") or "").strip(),
        "requesterId": int(requestRow.get("requesterId") or 0),
        "reviewMessageId": int(requestRow.get("reviewMessageId") or 0),
        "reviewChannelId": int(requestRow.get("reviewChannelId") or 0),
        "status": normalizedStatus(requestRow.get("status")),
    }


_ribbonBridge = WorkflowSubjectBridge(
    workflowKey=_RIBBON_WORKFLOW_KEY,
    subjectType=_RIBBON_SUBJECT_TYPE,
    subjectIdField="requestId",
    displayName=_ribbonDisplayName,
    metadata=_ribbonMetadata,
    stateForStatus=_stateForRibbonStatus,
    missingIdentifiersMessage="Ribbon request row is missing workflow identifiers.",
)


async def syncRibbonWorkflow(
    requestRow: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    return await _ribbonBridge.sync(
        requestRow,
        stateKey=stateKey,
        actorId=actorId,
        note=note,
        eventType=eventType,
        allowNoopEvent=allowNoopEvent,
    )


async def ensureRibbonWorkflowCurrent(requestRow: dict[str, Any]) -> dict[str, Any]:
    return await _ribbonBridge.ensureCurrent(
        requestRow,
        note="Workflow synchronized from ribbon request status.",
    )


async def getRibbonWorkflowSummary(requestRow: dict[str, Any]) -> str:
    return await _ribbonBridge.summary(requestRow)


async def getRibbonWorkflowHistorySummary(
    requestRow: dict[str, Any],
    *,
    limit: int = 3,
) -> str:
    return await _ribbonBridge.historySummary(requestRow, limit=limit)


async def reconcileRibbonWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    return await _ribbonBridge.reconcileRows(rows, ensureFn=ensureRibbonWorkflowCurrent)

__all__ = [
    "ensureRibbonWorkflowCurrent",
    "getRibbonWorkflowHistorySummary",
    "getRibbonWorkflowSummary",
    "reconcileRibbonWorkflowRows",
    "syncRibbonWorkflow",
]

