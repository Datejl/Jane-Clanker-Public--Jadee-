from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from features.staff.workflows import rendering as workflowRendering
from features.staff.workflows import service as workflowService

_PAYMENT_WORKFLOW_KEY = "anrd-payments"
_PAYMENT_SUBJECT_TYPE = "anrd_payment_request"


def _stateForPaymentStatus(status: object) -> str:
    normalized = str(status or "").strip().upper()
    if normalized == "APPROVED":
        return "approved"
    if normalized == "DENIED":
        return "denied"
    if normalized == "NEGOTIATING":
        return "negotiating"
    if normalized == "NEEDS_INFO":
        return "needs-info"
    if normalized == "PENDING":
        return "pending-review"
    return "submitted"


def _paymentDisplayName(requestRow: dict[str, Any]) -> str:
    requestId = int(requestRow.get("requestId") or 0)
    submitterId = int(requestRow.get("submitterId") or 0)
    if requestId > 0 and submitterId > 0:
        return f"Payment #{requestId} ({submitterId})"
    if requestId > 0:
        return f"Payment #{requestId}"
    return "ANRD Payment Request"


def _paymentMetadata(requestRow: dict[str, Any]) -> dict[str, Any]:
    return {
        "requestId": int(requestRow.get("requestId") or 0),
        "submitterId": int(requestRow.get("submitterId") or 0),
        "status": str(requestRow.get("status") or "").strip().upper(),
        "askingPrice": str(requestRow.get("askingPrice") or "").strip(),
        "negotiatedPrice": str(requestRow.get("negotiatedPrice") or "").strip(),
        "reviewMessageId": int(requestRow.get("reviewMessageId") or 0),
        "reviewChannelId": int(requestRow.get("reviewChannelId") or 0),
        "payoutSynced": int(requestRow.get("payoutSynced") or 0),
    }


async def syncPaymentWorkflow(
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
        raise ValueError("Payment request row is missing workflow identifiers.")
    return await workflowService.transitionSubjectRun(
        workflowKey=_PAYMENT_WORKFLOW_KEY,
        subjectType=_PAYMENT_SUBJECT_TYPE,
        subjectId=requestId,
        guildId=guildId,
        stateKey=stateKey or _stateForPaymentStatus(requestRow.get("status")),
        actorId=actorId,
        note=note,
        eventType=eventType,
        displayName=_paymentDisplayName(requestRow),
        metadata=_paymentMetadata(requestRow),
        allowNoopEvent=allowNoopEvent,
    )


async def ensurePaymentWorkflowCurrent(requestRow: dict[str, Any]) -> dict[str, Any]:
    return await syncPaymentWorkflow(
        requestRow,
        actorId=None,
        note="Workflow synchronized from payment request status.",
        eventType="SYNC",
        allowNoopEvent=False,
    )


async def getPaymentWorkflowSummary(requestRow: dict[str, Any]) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_PAYMENT_WORKFLOW_KEY,
        subjectType=_PAYMENT_SUBJECT_TYPE,
        subjectId=int(requestRow.get("requestId") or 0),
    )
    if not run:
        return ""
    latestEvent = await workflowService.getLatestRunEvent(int(run["runId"]))
    return workflowRendering.buildCompactSummary(run, latestEvent)


async def getPaymentWorkflowHistorySummary(requestRow: dict[str, Any], *, limit: int = 3) -> str:
    run = await workflowService.getRunBySubject(
        workflowKey=_PAYMENT_WORKFLOW_KEY,
        subjectType=_PAYMENT_SUBJECT_TYPE,
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


async def reconcilePaymentWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    checked = 0
    changed = 0
    for row in rows:
        requestId = int(row.get("requestId") or 0)
        guildId = int(row.get("guildId") or 0)
        if requestId <= 0 or guildId <= 0:
            continue
        checked += 1
        existingRun = await workflowService.getRunBySubject(
            workflowKey=_PAYMENT_WORKFLOW_KEY,
            subjectType=_PAYMENT_SUBJECT_TYPE,
            subjectId=requestId,
        )
        beforeUpdatedAt = str(existingRun.get("updatedAt") or "").strip() if existingRun else ""
        await ensurePaymentWorkflowCurrent(row)
        afterRun = await workflowService.getRunBySubject(
            workflowKey=_PAYMENT_WORKFLOW_KEY,
            subjectType=_PAYMENT_SUBJECT_TYPE,
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
    "ensurePaymentWorkflowCurrent",
    "getPaymentWorkflowHistorySummary",
    "getPaymentWorkflowSummary",
    "reconcilePaymentWorkflowRows",
    "syncPaymentWorkflow",
]

