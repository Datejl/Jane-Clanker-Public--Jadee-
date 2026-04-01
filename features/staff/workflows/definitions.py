from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WorkflowStateDefinition:
    key: str
    label: str
    pendingWith: str = ""
    isTerminal: bool = False
    allowedFromKeys: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkflowDefinition:
    key: str
    displayName: str
    subjectType: str
    defaultStateKey: str
    states: tuple[WorkflowStateDefinition, ...]

    def getState(self, stateKey: str) -> WorkflowStateDefinition:
        normalizedKey = str(stateKey or "").strip().lower()
        for state in self.states:
            if state.key == normalizedKey:
                return state
        raise KeyError(f"Unknown workflow state '{stateKey}' for workflow '{self.key}'.")

    def isTransitionAllowed(
        self,
        *,
        fromStateKey: str | None,
        toStateKey: str,
    ) -> bool:
        targetState = self.getState(toStateKey)
        normalizedFromStateKey = str(fromStateKey or "").strip().lower()
        if not targetState.allowedFromKeys:
            return True
        return normalizedFromStateKey in targetState.allowedFromKeys


APPLICATION_REVIEW_WORKFLOW = WorkflowDefinition(
    key="applications",
    displayName="Division Application Review",
    subjectType="division_application",
    defaultStateKey="submitted",
    states=(
        WorkflowStateDefinition(
            "submitted",
            "Submitted",
            pendingWith="system",
            allowedFromKeys=("", "submitted"),
        ),
        WorkflowStateDefinition(
            "pending-review",
            "Pending Review",
            pendingWith="reviewer",
            allowedFromKeys=("", "submitted", "needs-info", "approved", "denied", "pending-review"),
        ),
        WorkflowStateDefinition(
            "needs-info",
            "Needs Info",
            pendingWith="applicant",
            allowedFromKeys=("", "pending-review"),
        ),
        WorkflowStateDefinition(
            "approved",
            "Approved",
            isTerminal=True,
            allowedFromKeys=("", "submitted", "pending-review", "needs-info"),
        ),
        WorkflowStateDefinition(
            "denied",
            "Denied",
            isTerminal=True,
            allowedFromKeys=("", "submitted", "pending-review", "needs-info"),
        ),
    ),
)

RIBBON_REQUEST_WORKFLOW = WorkflowDefinition(
    key="ribbons",
    displayName="Ribbon Request Review",
    subjectType="ribbon_request",
    defaultStateKey="submitted",
    states=(
        WorkflowStateDefinition(
            "submitted",
            "Submitted",
            pendingWith="system",
            allowedFromKeys=("", "submitted"),
        ),
        WorkflowStateDefinition(
            "pending-review",
            "Pending Review",
            pendingWith="reviewer",
            allowedFromKeys=("", "submitted", "needs-info", "pending-review"),
        ),
        WorkflowStateDefinition(
            "needs-info",
            "Needs Info",
            pendingWith="applicant",
            allowedFromKeys=("", "pending-review"),
        ),
        WorkflowStateDefinition(
            "approved",
            "Approved",
            isTerminal=True,
            allowedFromKeys=("", "submitted", "pending-review", "needs-info"),
        ),
        WorkflowStateDefinition(
            "rejected",
            "Rejected",
            isTerminal=True,
            allowedFromKeys=("", "submitted", "pending-review", "needs-info"),
        ),
        WorkflowStateDefinition(
            "canceled",
            "Canceled",
            isTerminal=True,
            allowedFromKeys=("", "submitted", "pending-review", "needs-info"),
        ),
    ),
)

PROJECT_REVIEW_WORKFLOW = WorkflowDefinition(
    key="projects",
    displayName="Department Project Review",
    subjectType="department_project",
    defaultStateKey="pending-approval",
    states=(
        WorkflowStateDefinition(
            "pending-approval",
            "Pending HOD Approval",
            pendingWith="reviewer",
            allowedFromKeys=("", "pending-approval"),
        ),
        WorkflowStateDefinition(
            "approved",
            "Approved",
            pendingWith="creator",
            allowedFromKeys=("", "pending-approval"),
        ),
        WorkflowStateDefinition(
            "submitted",
            "Submitted for Finalization",
            pendingWith="final-reviewer",
            allowedFromKeys=("", "approved"),
        ),
        WorkflowStateDefinition(
            "denied",
            "Denied",
            isTerminal=True,
            allowedFromKeys=("", "pending-approval", "approved", "submitted"),
        ),
        WorkflowStateDefinition(
            "finalized",
            "Finalized",
            isTerminal=True,
            allowedFromKeys=("", "submitted"),
        ),
    ),
)

ORBAT_REQUEST_WORKFLOW = WorkflowDefinition(
    key="orbat-requests",
    displayName="ORBAT Request Review",
    subjectType="orbat_request",
    defaultStateKey="submitted",
    states=(
        WorkflowStateDefinition(
            "submitted",
            "Submitted",
            pendingWith="system",
            allowedFromKeys=("", "submitted"),
        ),
        WorkflowStateDefinition(
            "pending-review",
            "Pending Review",
            pendingWith="reviewer",
            allowedFromKeys=("", "submitted", "needs-info", "pending-review"),
        ),
        WorkflowStateDefinition(
            "needs-info",
            "Needs Info",
            pendingWith="applicant",
            allowedFromKeys=("", "pending-review"),
        ),
        WorkflowStateDefinition(
            "approved",
            "Approved",
            isTerminal=True,
            allowedFromKeys=("", "submitted", "pending-review", "needs-info"),
        ),
        WorkflowStateDefinition(
            "rejected",
            "Rejected",
            isTerminal=True,
            allowedFromKeys=("", "submitted", "pending-review", "needs-info"),
        ),
    ),
)

LOA_REQUEST_WORKFLOW = WorkflowDefinition(
    key="loa-requests",
    displayName="LOA Request Review",
    subjectType="loa_request",
    defaultStateKey="submitted",
    states=(
        WorkflowStateDefinition(
            "submitted",
            "Submitted",
            pendingWith="system",
            allowedFromKeys=("", "submitted"),
        ),
        WorkflowStateDefinition(
            "pending-review",
            "Pending Review",
            pendingWith="reviewer",
            allowedFromKeys=("", "submitted", "needs-info", "pending-review"),
        ),
        WorkflowStateDefinition(
            "needs-info",
            "Needs Info",
            pendingWith="applicant",
            allowedFromKeys=("", "pending-review"),
        ),
        WorkflowStateDefinition(
            "approved",
            "Approved",
            isTerminal=True,
            allowedFromKeys=("", "submitted", "pending-review", "needs-info"),
        ),
        WorkflowStateDefinition(
            "rejected",
            "Rejected",
            isTerminal=True,
            allowedFromKeys=("", "submitted", "pending-review", "needs-info"),
        ),
    ),
)

ANRD_PAYMENT_WORKFLOW = WorkflowDefinition(
    key="anrd-payments",
    displayName="ANRD Payment Review",
    subjectType="anrd_payment_request",
    defaultStateKey="submitted",
    states=(
        WorkflowStateDefinition(
            "submitted",
            "Submitted",
            pendingWith="system",
            allowedFromKeys=("", "submitted"),
        ),
        WorkflowStateDefinition(
            "pending-review",
            "Pending Review",
            pendingWith="reviewer",
            allowedFromKeys=("", "submitted", "needs-info", "negotiating", "pending-review"),
        ),
        WorkflowStateDefinition(
            "negotiating",
            "Negotiating",
            pendingWith="submitter",
            allowedFromKeys=("", "pending-review", "needs-info", "negotiating"),
        ),
        WorkflowStateDefinition(
            "needs-info",
            "Needs Info",
            pendingWith="submitter",
            allowedFromKeys=("", "pending-review", "negotiating"),
        ),
        WorkflowStateDefinition(
            "approved",
            "Approved",
            isTerminal=True,
            allowedFromKeys=("", "pending-review", "negotiating", "needs-info"),
        ),
        WorkflowStateDefinition(
            "denied",
            "Denied",
            isTerminal=True,
            allowedFromKeys=("", "pending-review", "negotiating", "needs-info"),
        ),
    ),
)


_DEFINITIONS: dict[str, WorkflowDefinition] = {
    APPLICATION_REVIEW_WORKFLOW.key: APPLICATION_REVIEW_WORKFLOW,
    ANRD_PAYMENT_WORKFLOW.key: ANRD_PAYMENT_WORKFLOW,
    LOA_REQUEST_WORKFLOW.key: LOA_REQUEST_WORKFLOW,
    ORBAT_REQUEST_WORKFLOW.key: ORBAT_REQUEST_WORKFLOW,
    PROJECT_REVIEW_WORKFLOW.key: PROJECT_REVIEW_WORKFLOW,
    RIBBON_REQUEST_WORKFLOW.key: RIBBON_REQUEST_WORKFLOW,
}


def getWorkflowDefinition(workflowKey: str) -> WorkflowDefinition:
    normalizedKey = str(workflowKey or "").strip().lower()
    definition = _DEFINITIONS.get(normalizedKey)
    if definition is None:
        raise KeyError(f"Unknown workflow definition '{workflowKey}'.")
    return definition


def listWorkflowDefinitions() -> list[WorkflowDefinition]:
    return list(_DEFINITIONS.values())


__all__ = [
    "APPLICATION_REVIEW_WORKFLOW",
    "ANRD_PAYMENT_WORKFLOW",
    "LOA_REQUEST_WORKFLOW",
    "ORBAT_REQUEST_WORKFLOW",
    "PROJECT_REVIEW_WORKFLOW",
    "RIBBON_REQUEST_WORKFLOW",
    "WorkflowDefinition",
    "WorkflowStateDefinition",
    "getWorkflowDefinition",
    "listWorkflowDefinitions",
]
