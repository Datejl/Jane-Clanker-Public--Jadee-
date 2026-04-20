from __future__ import annotations

from typing import Any, Optional

from features.staff.workflows.bridge import (
    WorkflowSubjectBridge,
    normalizedStatus,
    stateKeyForStatus,
)

_PAYMENT_WORKFLOW_KEY = "anrd-payments"
_PAYMENT_SUBJECT_TYPE = "anrd_payment_request"
_PAYMENT_STATUS_STATES = {
    "APPROVED": "approved",
    "DENIED": "denied",
    "NEGOTIATING": "negotiating",
    "NEEDS_INFO": "needs-info",
    "PENDING": "pending-review",
}


def _stateForPaymentStatus(status: object) -> str:
    return stateKeyForStatus(status, _PAYMENT_STATUS_STATES, default="submitted")


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
        "status": normalizedStatus(requestRow.get("status")),
        "askingPrice": str(requestRow.get("askingPrice") or "").strip(),
        "negotiatedPrice": str(requestRow.get("negotiatedPrice") or "").strip(),
        "reviewMessageId": int(requestRow.get("reviewMessageId") or 0),
        "reviewChannelId": int(requestRow.get("reviewChannelId") or 0),
        "payoutSynced": int(requestRow.get("payoutSynced") or 0),
    }


_paymentBridge = WorkflowSubjectBridge(
    workflowKey=_PAYMENT_WORKFLOW_KEY,
    subjectType=_PAYMENT_SUBJECT_TYPE,
    subjectIdField="requestId",
    displayName=_paymentDisplayName,
    metadata=_paymentMetadata,
    stateForStatus=_stateForPaymentStatus,
    missingIdentifiersMessage="Payment request row is missing workflow identifiers.",
)


async def syncPaymentWorkflow(
    requestRow: dict[str, Any],
    *,
    stateKey: Optional[str] = None,
    actorId: Optional[int] = None,
    note: str = "",
    eventType: str = "STATE_CHANGE",
    allowNoopEvent: bool = False,
) -> dict[str, Any]:
    return await _paymentBridge.sync(
        requestRow,
        stateKey=stateKey,
        actorId=actorId,
        note=note,
        eventType=eventType,
        allowNoopEvent=allowNoopEvent,
    )


async def ensurePaymentWorkflowCurrent(requestRow: dict[str, Any]) -> dict[str, Any]:
    return await _paymentBridge.ensureCurrent(
        requestRow,
        note="Workflow synchronized from payment request status.",
    )


async def getPaymentWorkflowSummary(requestRow: dict[str, Any]) -> str:
    return await _paymentBridge.summary(requestRow)


async def getPaymentWorkflowHistorySummary(requestRow: dict[str, Any], *, limit: int = 3) -> str:
    return await _paymentBridge.historySummary(requestRow, limit=limit)


async def reconcilePaymentWorkflowRows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    return await _paymentBridge.reconcileRows(rows, ensureFn=ensurePaymentWorkflowCurrent)

__all__ = [
    "ensurePaymentWorkflowCurrent",
    "getPaymentWorkflowHistorySummary",
    "getPaymentWorkflowSummary",
    "reconcilePaymentWorkflowRows",
    "syncPaymentWorkflow",
]

