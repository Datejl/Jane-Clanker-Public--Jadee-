from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from features.staff.workflows import rendering as workflowRendering
from features.staff.workflows import service as workflowService

_RIBBON_WORKFLOW_KEY = "ribbons"
_RIBBON_SUBJECT_TYPE = "ribbon_request"


def _stateForRibbonStatus(status: object) -> str:
    normalized = str(status or "").strip().upper()
    if normalized == "APPROVED":
        return "approved"
    if normalized == "REJECTED":
        return "rejected"
    if normalized == "NEEDS_INFO":
        return "needs-info"
    if normalized == "CANCELED":
        return "canceled"
    if normalized == "PENDING":
        return "pending-review"
    return "submitted"


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
        "status": str(requestRow.get("status") or "").strip().upper(),
    }


async def syncRibbonWorkflow(
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
        raise ValueError("Ribbon request row is missing workflow identifiers.")

    return await workflowService.transitionSubjectRun(
        workflowKey=_RIBBON_WORKFLOW_KEY,
        subjectType=_RIBBON_SUBJECT_TYPE,
        subjectId=requestId,
        guildId=guildId,
        stateKey=stateKey or _stateForRibbonStatus(requestRow.get("status")),
        actorId=actorId,
        note=note,
        eventType=eventType,
        displayName=_ribbonDisplayName(requestRow),
        metadata=_ribbonMetadata(requestRow),
        allowNoopEvent=allowNoopEvent,
    )


async def ensureRibbonWorkflowCurrent(requestRow: dict[str, Any]) -> dict[str, Any]:
    return await syncRibbonWorkflow(
        requestRow,
        actorId=None,
        note="Workflow synchronized from ribbon request status.",
        eventType="SYNC",
        allowNoopEvent=False,
    )


async def getRibbonWorkflowSummary(requestRow: dict[str, Any]) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_RIBBON_WORKFLOW_KEY,
        subjectType=_RIBBON_SUBJECT_TYPE,
        subjectId=int(requestRow.get("requestId") or 0),
    )
    if not run:
        return ""
    latestEvent = await workflowService.getLatestRunEvent(int(run["runId"]))
    return workflowRendering.buildCompactSummary(run, latestEvent)


async def getRibbonWorkflowHistorySummary(
    requestRow: dict[str, Any],
    *,
    limit: int = 3,
) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_RIBBON_WORKFLOW_KEY,
        subjectType=_RIBBON_SUBJECT_TYPE,
        subjectId=int(requestRow.get("requestId") or 0),
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


async def reconcileRibbonWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    checked = 0
    changed = 0
    for row in rows:
        requestId = int(row.get("requestId") or 0)
        guildId = int(row.get("guildId") or 0)
        if requestId <= 0 or guildId <= 0:
            continue
        checked += 1
        existingRun = await workflowService.getRunBySubject(
            workflowKey=_RIBBON_WORKFLOW_KEY,
            subjectType=_RIBBON_SUBJECT_TYPE,
            subjectId=requestId,
        )
        beforeUpdatedAt = str(existingRun.get("updatedAt") or "").strip() if existingRun else ""
        await ensureRibbonWorkflowCurrent(row)
        afterRun = await workflowService.getRunBySubject(
            workflowKey=_RIBBON_WORKFLOW_KEY,
            subjectType=_RIBBON_SUBJECT_TYPE,
            subjectId=requestId,
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
    "ensureRibbonWorkflowCurrent",
    "getRibbonWorkflowHistorySummary",
    "getRibbonWorkflowSummary",
    "reconcileRibbonWorkflowRows",
    "syncRibbonWorkflow",
]

