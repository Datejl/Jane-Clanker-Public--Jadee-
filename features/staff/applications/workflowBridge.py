from __future__ import annotations

from typing import Any, Optional

from features.staff.workflows import rendering as workflowRendering
from features.staff.workflows import service as workflowService

_APPLICATION_WORKFLOW_KEY = "applications"
_APPLICATION_SUBJECT_TYPE = "division_application"


def _stateForApplicationStatus(status: object) -> str:
    normalized = str(status or "").strip().upper()
    if normalized == "APPROVED":
        return "approved"
    if normalized == "DENIED":
        return "denied"
    if normalized == "NEEDS_INFO":
        return "needs-info"
    if normalized == "PENDING":
        return "pending-review"
    return "submitted"


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
        "status": str(application.get("status") or "").strip().upper(),
    }


async def syncApplicationWorkflow(
    application: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    applicationId = int(application.get("applicationId") or 0)
    guildId = int(application.get("guildId") or 0)
    if applicationId <= 0 or guildId <= 0:
        raise ValueError("Application row is missing workflow identifiers.")

    return await workflowService.transitionSubjectRun(
        workflowKey=_APPLICATION_WORKFLOW_KEY,
        subjectType=_APPLICATION_SUBJECT_TYPE,
        subjectId=applicationId,
        guildId=guildId,
        stateKey=stateKey or _stateForApplicationStatus(application.get("status")),
        actorId=actorId,
        note=note,
        eventType=eventType,
        displayName=_applicationDisplayName(application),
        metadata=_applicationMetadata(application),
        allowNoopEvent=allowNoopEvent,
    )


async def ensureApplicationWorkflowCurrent(application: dict[str, Any]) -> dict[str, Any]:
    return await syncApplicationWorkflow(
        application,
        actorId=None,
        note="Workflow synchronized from application status.",
        eventType="SYNC",
        allowNoopEvent=False,
    )


async def getApplicationWorkflowSummary(application: dict[str, Any]) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_APPLICATION_WORKFLOW_KEY,
        subjectType=_APPLICATION_SUBJECT_TYPE,
        subjectId=int(application.get("applicationId") or 0),
    )
    if not run:
        return ""
    latestEvent = await workflowService.getLatestRunEvent(int(run["runId"]))
    return workflowRendering.buildCompactSummary(run, latestEvent)


async def getApplicationWorkflowHistorySummary(
    application: dict[str, Any],
    *,
    limit: int = 3,
) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_APPLICATION_WORKFLOW_KEY,
        subjectType=_APPLICATION_SUBJECT_TYPE,
        subjectId=int(application.get("applicationId") or 0),
    )
    if not run:
        return ""
    rows = await workflowService.listRunEvents(int(run["runId"]), limit=max(1, min(int(limit or 3), 5)))
    if not rows:
        return ""
    return workflowRendering.buildWorkflowEventSummary(rows)


async def reconcileApplicationWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    checked = 0
    changed = 0
    for row in rows:
        applicationId = int(row.get("applicationId") or 0)
        guildId = int(row.get("guildId") or 0)
        if applicationId <= 0 or guildId <= 0:
            continue
        checked += 1
        existingRun = await workflowService.getRunBySubject(
            workflowKey=_APPLICATION_WORKFLOW_KEY,
            subjectType=_APPLICATION_SUBJECT_TYPE,
            subjectId=applicationId,
        )
        beforeUpdatedAt = str(existingRun.get("updatedAt") or "").strip() if existingRun else ""
        await ensureApplicationWorkflowCurrent(row)
        afterRun = await workflowService.getRunBySubject(
            workflowKey=_APPLICATION_WORKFLOW_KEY,
            subjectType=_APPLICATION_SUBJECT_TYPE,
            subjectId=applicationId,
        )
        afterUpdatedAt = str(afterRun.get("updatedAt") or "").strip() if afterRun else ""
        if existingRun is None or afterUpdatedAt != beforeUpdatedAt:
            changed += 1
    return checked, changed

__all__ = [
    "ensureApplicationWorkflowCurrent",
    "getApplicationWorkflowHistorySummary",
    "getApplicationWorkflowSummary",
    "reconcileApplicationWorkflowRows",
    "syncApplicationWorkflow",
]

