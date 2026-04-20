from __future__ import annotations

from typing import Any, Optional

from features.staff.workflows.bridge import (
    WorkflowSubjectBridge,
    normalizedStatus,
    stateKeyForStatus,
)

_PROJECT_WORKFLOW_KEY = "projects"
_PROJECT_SUBJECT_TYPE = "department_project"
_PROJECT_STATUS_STATES = {
    "APPROVED": "approved",
    "DENIED": "denied",
    "SUBMITTED": "submitted",
    "FINALIZED": "finalized",
}


def _stateForProjectStatus(status: object) -> str:
    return stateKeyForStatus(status, _PROJECT_STATUS_STATES, default="pending-approval")


def _projectDisplayName(project: dict[str, Any]) -> str:
    projectId = int(project.get("projectId") or 0)
    title = str(project.get("title") or "").strip()
    if projectId > 0 and title:
        return f"Project #{projectId} ({title})"
    if projectId > 0:
        return f"Project #{projectId}"
    return title or "Project"


def _projectMetadata(project: dict[str, Any]) -> dict[str, Any]:
    return {
        "projectId": int(project.get("projectId") or 0),
        "creatorId": int(project.get("creatorId") or 0),
        "title": str(project.get("title") or "").strip(),
        "status": normalizedStatus(project.get("status")),
        "requestedPoints": int(project.get("requestedPoints") or 0),
        "awardedPoints": int(project.get("awardedPoints") or 0),
        "reviewMessageId": int(project.get("reviewMessageId") or 0),
        "reviewChannelId": int(project.get("reviewChannelId") or 0),
        "threadId": int(project.get("threadId") or 0),
    }


_projectBridge = WorkflowSubjectBridge(
    workflowKey=_PROJECT_WORKFLOW_KEY,
    subjectType=_PROJECT_SUBJECT_TYPE,
    subjectIdField="projectId",
    displayName=_projectDisplayName,
    metadata=_projectMetadata,
    stateForStatus=_stateForProjectStatus,
    missingIdentifiersMessage="Project row is missing workflow identifiers.",
)


async def syncProjectWorkflow(
    project: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    return await _projectBridge.sync(
        project,
        stateKey=stateKey,
        actorId=actorId,
        note=note,
        eventType=eventType,
        allowNoopEvent=allowNoopEvent,
    )


async def ensureProjectWorkflowCurrent(project: dict[str, Any]) -> dict[str, Any]:
    return await _projectBridge.ensureCurrent(
        project,
        note="Workflow synchronized from project status.",
    )


async def getProjectWorkflowSummary(project: dict[str, Any]) -> str:
    return await _projectBridge.summary(project)


async def getProjectWorkflowHistorySummary(
    project: dict[str, Any],
    *,
    limit: int = 3,
) -> str:
    return await _projectBridge.historySummary(project, limit=limit)


async def reconcileProjectWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    return await _projectBridge.reconcileRows(rows, ensureFn=ensureProjectWorkflowCurrent)

__all__ = [
    "ensureProjectWorkflowCurrent",
    "getProjectWorkflowHistorySummary",
    "getProjectWorkflowSummary",
    "reconcileProjectWorkflowRows",
    "syncProjectWorkflow",
]

