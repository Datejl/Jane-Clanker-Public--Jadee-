from __future__ import annotations

from typing import Any, Optional

from features.staff.workflows.bridge import (
    WorkflowSubjectBridge,
    normalizedStatus,
    stateKeyForStatus,
)

_APPLICATION_WORKFLOW_KEY = "applications"
_APPLICATION_SUBJECT_TYPE = "division_application"
_APPLICATION_STATUS_STATES = {
    "APPROVED": "approved",
    "DENIED": "denied",
    "NEEDS_INFO": "needs-info",
    "PENDING": "pending-review",
}


def _stateForApplicationStatus(status: object) -> str:
    return stateKeyForStatus(status, _APPLICATION_STATUS_STATES, default="submitted")


def _applicationDisplayName(application: dict[str, Any]) -> str:
    appCode = str(application.get("appCode") or "").strip()
    divisionKey = str(application.get("divisionKey") or "").strip()
    if appCode and divisionKey:
        return f"{appCode} ({divisionKey})"
    if appCode:
        return appCode
    applicationId = int(application.get("applicationId") or 0)
    if applicationId > 0:
        return f"Application {applicationId}"
    return "Application"


def _applicationMetadata(application: dict[str, Any]) -> dict[str, Any]:
    return {
        "applicationId": int(application.get("applicationId") or 0),
        "appCode": str(application.get("appCode") or "").strip(),
        "divisionKey": str(application.get("divisionKey") or "").strip(),
        "applicantId": int(application.get("applicantId") or 0),
        "reviewMessageId": int(application.get("reviewMessageId") or 0),
        "reviewChannelId": int(application.get("reviewChannelId") or 0),
        "status": normalizedStatus(application.get("status")),
    }


_applicationBridge = WorkflowSubjectBridge(
    workflowKey=_APPLICATION_WORKFLOW_KEY,
    subjectType=_APPLICATION_SUBJECT_TYPE,
    subjectIdField="applicationId",
    displayName=_applicationDisplayName,
    metadata=_applicationMetadata,
    stateForStatus=_stateForApplicationStatus,
    missingIdentifiersMessage="Application row is missing workflow identifiers.",
)


async def syncApplicationWorkflow(
    application: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    return await _applicationBridge.sync(
        application,
        stateKey=stateKey,
        actorId=actorId,
        note=note,
        eventType=eventType,
        allowNoopEvent=allowNoopEvent,
    )


async def ensureApplicationWorkflowCurrent(application: dict[str, Any]) -> dict[str, Any]:
    return await _applicationBridge.ensureCurrent(
        application,
        note="Workflow synchronized from application status.",
    )


async def getApplicationWorkflowSummary(application: dict[str, Any]) -> str:
    return await _applicationBridge.summary(application)


async def getApplicationWorkflowHistorySummary(
    application: dict[str, Any],
    *,
    limit: int = 3,
) -> str:
    return await _applicationBridge.historySummary(application, limit=limit)


async def reconcileApplicationWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    return await _applicationBridge.reconcileRows(rows, ensureFn=ensureApplicationWorkflowCurrent)

__all__ = [
    "ensureApplicationWorkflowCurrent",
    "getApplicationWorkflowHistorySummary",
    "getApplicationWorkflowSummary",
    "reconcileApplicationWorkflowRows",
    "syncApplicationWorkflow",
]

