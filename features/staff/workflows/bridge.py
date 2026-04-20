from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Optional

from features.staff.workflows import rendering as workflowRendering
from features.staff.workflows import service as workflowService
from runtime import normalization

Row = dict[str, Any]
StatusStateMap = Mapping[str, str]


def normalizedStatus(value: object) -> str:
    return str(value or "").strip().upper()


def stateKeyForStatus(
    status: object,
    statesByStatus: StatusStateMap,
    *,
    default: str,
) -> str:
    return statesByStatus.get(normalizedStatus(status), default)


@dataclass(frozen=True)
class WorkflowSubjectBridge:
    workflowKey: str
    subjectType: str
    subjectIdField: str
    displayName: Callable[[Row], str]
    metadata: Callable[[Row], dict[str, Any]]
    stateForStatus: Callable[[object], str]
    missingIdentifiersMessage: str
    guildIdField: str = "guildId"

    def subjectId(self, row: Row) -> int:
        return normalization.toPositiveInt(row.get(self.subjectIdField))

    def guildId(self, row: Row) -> int:
        return normalization.toPositiveInt(row.get(self.guildIdField))

    async def runForRow(self, row: Row) -> Optional[Row]:
        subjectId = self.subjectId(row)
        if subjectId <= 0:
            return None
        return await workflowService.getRunBySubject(
            workflowKey=self.workflowKey,
            subjectType=self.subjectType,
            subjectId=subjectId,
        )

    async def sync(
        self,
        row: Row,
        *,
        stateKey: Optional[str] = None,
        actorId: Optional[int] = None,
        note: str = "",
        eventType: str = "STATE_CHANGE",
        allowNoopEvent: bool = False,
    ) -> Row:
        subjectId = self.subjectId(row)
        guildId = self.guildId(row)
        if subjectId <= 0 or guildId <= 0:
            raise ValueError(self.missingIdentifiersMessage)

        return await workflowService.transitionSubjectRun(
            workflowKey=self.workflowKey,
            subjectType=self.subjectType,
            subjectId=subjectId,
            guildId=guildId,
            stateKey=stateKey or self.stateForStatus(row.get("status")),
            actorId=actorId,
            note=note,
            eventType=eventType,
            displayName=self.displayName(row),
            metadata=self.metadata(row),
            allowNoopEvent=allowNoopEvent,
        )

    async def ensureCurrent(self, row: Row, *, note: str) -> Row:
        return await self.sync(
            row,
            actorId=None,
            note=note,
            eventType="SYNC",
            allowNoopEvent=False,
        )

    async def summary(self, row: Row) -> str:
        run = await self.runForRow(row)
        if not run:
            return ""
        latestEvent = await workflowService.getLatestRunEvent(int(run["runId"]))
        return workflowRendering.buildCompactSummary(run, latestEvent)

    async def historySummary(self, row: Row, *, limit: int = 3) -> str:
        run = await self.runForRow(row)
        if not run:
            return ""
        safeLimit = max(1, min(int(limit or 3), 5))
        rows = await workflowService.listRunEvents(int(run["runId"]), limit=safeLimit)
        if not rows:
            return ""
        return workflowRendering.buildWorkflowEventSummary(rows)

    async def reconcileRows(
        self,
        rows: list[Row],
        *,
        ensureFn: Callable[[Row], Awaitable[Row]],
    ) -> tuple[int, int]:
        checked = 0
        changed = 0
        for row in rows:
            subjectId = self.subjectId(row)
            guildId = self.guildId(row)
            if subjectId <= 0 or guildId <= 0:
                continue
            checked += 1
            existingRun = await self.runForRow(row)
            beforeUpdatedAt = str(existingRun.get("updatedAt") or "").strip() if existingRun else ""
            await ensureFn(row)
            afterRun = await self.runForRow(row)
            afterUpdatedAt = str(afterRun.get("updatedAt") or "").strip() if afterRun else ""
            if existingRun is None or afterUpdatedAt != beforeUpdatedAt:
                changed += 1
        return checked, changed


__all__ = [
    "Row",
    "StatusStateMap",
    "WorkflowSubjectBridge",
    "normalizedStatus",
    "stateKeyForStatus",
]
