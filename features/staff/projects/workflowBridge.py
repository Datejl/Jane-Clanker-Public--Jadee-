from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from features.staff.workflows import rendering as workflowRendering
from features.staff.workflows import service as workflowService

_PROJECT_WORKFLOW_KEY = "projects"
_PROJECT_SUBJECT_TYPE = "department_project"


def _stateForProjectStatus(status: object) -> str:
    normalized = str(status or "").strip().upper()
    if normalized == "APPROVED":
        return "approved"
    if normalized == "DENIED":
        return "denied"
    if normalized == "SUBMITTED":
        return "submitted"
    if normalized == "FINALIZED":
        return "finalized"
    return "pending-approval"


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
        "status": str(project.get("status") or "").strip().upper(),
        "requestedPoints": int(project.get("requestedPoints") or 0),
        "awardedPoints": int(project.get("awardedPoints") or 0),
        "reviewMessageId": int(project.get("reviewMessageId") or 0),
        "reviewChannelId": int(project.get("reviewChannelId") or 0),
        "threadId": int(project.get("threadId") or 0),
    }


async def syncProjectWorkflow(
    project: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    projectId = int(project.get("projectId") or 0)
    guildId = int(project.get("guildId") or 0)
    if projectId <= 0 or guildId <= 0:
        raise ValueError("Project row is missing workflow identifiers.")

    return await workflowService.transitionSubjectRun(
        workflowKey=_PROJECT_WORKFLOW_KEY,
        subjectType=_PROJECT_SUBJECT_TYPE,
        subjectId=projectId,
        guildId=guildId,
        stateKey=stateKey or _stateForProjectStatus(project.get("status")),
        actorId=actorId,
        note=note,
        eventType=eventType,
        displayName=_projectDisplayName(project),
        metadata=_projectMetadata(project),
        allowNoopEvent=allowNoopEvent,
    )


async def ensureProjectWorkflowCurrent(project: dict[str, Any]) -> dict[str, Any]:
    return await syncProjectWorkflow(
        project,
        actorId=None,
        note="Workflow synchronized from project status.",
        eventType="SYNC",
        allowNoopEvent=False,
    )


async def getProjectWorkflowSummary(project: dict[str, Any]) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_PROJECT_WORKFLOW_KEY,
        subjectType=_PROJECT_SUBJECT_TYPE,
        subjectId=int(project.get("projectId") or 0),
    )
    if not run:
        return ""
    latestEvent = await workflowService.getLatestRunEvent(int(run["runId"]))
    return workflowRendering.buildCompactSummary(run, latestEvent)


async def getProjectWorkflowHistorySummary(
    project: dict[str, Any],
    *,
    limit: int = 3,
) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_PROJECT_WORKFLOW_KEY,
        subjectType=_PROJECT_SUBJECT_TYPE,
        subjectId=int(project.get("projectId") or 0),
    )
    if not run:
        return ""
    rows = await workflowService.listRunEvents(int(run["runId"]), limit=max(1, min(int(limit or 3), 5)))
    if not rows:
        return ""
    lines: list[str] = []
    for row in reversed(rows):
        toLabel = str(row.get("toStateLabel") or row.get("toStateKey") or "Unknown").strip()
        actorId = int(row.get("actorId") or 0)
        actorText = f"<@{actorId}>" if actorId > 0 else "system"
        note = str(row.get("note") or "").strip()
        transitionText = f"{toLabel} - {note}" if note else toLabel
        lines.append(f"{_discordTimestamp(row.get('createdAt'))}: {transitionText} ({actorText})")
    return "\n".join(lines)[:1024]


async def reconcileProjectWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    checked = 0
    changed = 0
    for row in rows:
        projectId = int(row.get("projectId") or 0)
        guildId = int(row.get("guildId") or 0)
        if projectId <= 0 or guildId <= 0:
            continue
        checked += 1
        existingRun = await workflowService.getRunBySubject(
            workflowKey=_PROJECT_WORKFLOW_KEY,
            subjectType=_PROJECT_SUBJECT_TYPE,
            subjectId=projectId,
        )
        beforeUpdatedAt = str(existingRun.get("updatedAt") or "").strip() if existingRun else ""
        await ensureProjectWorkflowCurrent(row)
        afterRun = await workflowService.getRunBySubject(
            workflowKey=_PROJECT_WORKFLOW_KEY,
            subjectType=_PROJECT_SUBJECT_TYPE,
            subjectId=projectId,
        )
        afterUpdatedAt = str(afterRun.get("updatedAt") or "").strip() if afterRun else ""
        if existingRun is None or afterUpdatedAt != beforeUpdatedAt:
            changed += 1
    return checked, changed


def _discordTimestamp(rawValue: Any) -> str:
    text = str(rawValue or "").strip()
    if not text:
        return "unknown"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return "unknown"
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    return f"<t:{int(parsed.timestamp())}:R>"


__all__ = [
    "ensureProjectWorkflowCurrent",
    "getProjectWorkflowHistorySummary",
    "getProjectWorkflowSummary",
    "reconcileProjectWorkflowRows",
    "syncProjectWorkflow",
]

