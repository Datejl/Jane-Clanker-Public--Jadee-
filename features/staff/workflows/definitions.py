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


def _normalizedWorkflowDefinition(definition: WorkflowDefinition) -> WorkflowDefinition:
    if not isinstance(definition, WorkflowDefinition):
        raise TypeError("definition must be a WorkflowDefinition.")

    key = str(definition.key or "").strip().lower()
    displayName = str(definition.displayName or "").strip()
    subjectType = str(definition.subjectType or "").strip().lower()
    defaultStateKey = str(definition.defaultStateKey or "").strip().lower()
    if not key or not displayName or not subjectType or not defaultStateKey:
        raise ValueError("Workflow key, display name, subject type, and default state are required.")
    if not definition.states:
        raise ValueError(f"Workflow '{key}' needs at least one state.")

    states: list[WorkflowStateDefinition] = []
    stateKeys: set[str] = set()
    for rawState in definition.states:
        stateKey = str(rawState.key or "").strip().lower()
        stateLabel = str(rawState.label or "").strip()
        if not stateKey or not stateLabel:
            raise ValueError(f"Workflow '{key}' contains a state without a key or label.")
        if stateKey in stateKeys:
            raise ValueError(f"Workflow '{key}' contains duplicate state '{stateKey}'.")
        stateKeys.add(stateKey)
        states.append(
            WorkflowStateDefinition(
                key=stateKey,
                label=stateLabel,
                pendingWith=str(rawState.pendingWith or "").strip().lower(),
                isTerminal=bool(rawState.isTerminal),
                allowedFromKeys=tuple(
                    str(value or "").strip().lower()
                    for value in rawState.allowedFromKeys
                ),
            )
        )

    if defaultStateKey not in stateKeys:
        raise ValueError(
            f"Workflow '{key}' default state '{defaultStateKey}' is not defined."
        )
    for state in states:
        unknownKeys = {
            value
            for value in state.allowedFromKeys
            if value and value not in stateKeys
        }
        if unknownKeys:
            unknownText = ", ".join(sorted(unknownKeys))
            raise ValueError(
                f"Workflow '{key}' state '{state.key}' allows unknown states: {unknownText}."
            )

    return WorkflowDefinition(
        key=key,
        displayName=displayName,
        subjectType=subjectType,
        defaultStateKey=defaultStateKey,
        states=tuple(states),
    )


def registerWorkflowDefinition(
    definition: WorkflowDefinition,
    *,
    replace: bool = False,
) -> WorkflowDefinition:
    """Register a workflow for service calls and return its normalized definition."""

    normalized = _normalizedWorkflowDefinition(definition)
    existing = _DEFINITIONS.get(normalized.key)
    if existing is not None and not replace:
        if existing == normalized:
            return existing
        raise ValueError(
            f"Workflow definition '{normalized.key}' is already registered."
        )
    _DEFINITIONS[normalized.key] = normalized
    return normalized


def isWorkflowRegistered(workflowKey: str) -> bool:
    return str(workflowKey or "").strip().lower() in _DEFINITIONS


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
    "isWorkflowRegistered",
    "listWorkflowDefinitions",
    "registerWorkflowDefinition",
]
