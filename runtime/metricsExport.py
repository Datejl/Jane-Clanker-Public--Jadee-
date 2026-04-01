from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from db.sqlite import dbPath as sqliteDbPath


class MetricsExporter:
    def __init__(
        self,
        *,
        botClient: Any,
        taskBudgeter: Any,
        maintenanceCoordinator: Any,
        retryQueue: Any,
        featureFlags: Any,
        webhookHealthWatcher: Any,
        auditStream: Any,
        botStartedAt: datetime,
        getProcessResourceSnapshot: Any,
    ) -> None:
        self.botClient = botClient
        self.taskBudgeter = taskBudgeter
        self.maintenance = maintenanceCoordinator
        self.retryQueue = retryQueue
        self.featureFlags = featureFlags
        self.webhookHealthWatcher = webhookHealthWatcher
        self.auditStream = auditStream
        self.botStartedAt = botStartedAt
        self.getProcessResourceSnapshot = getProcessResourceSnapshot

    @staticmethod
    def _taskState(task: Any) -> str:
        if task is None:
            return "not-started"
        if task.cancelled():
            return "cancelled"
        if task.done():
            return "done"
        return "running"

    @staticmethod
    def _dbSizeBytes() -> int:
        try:
            return int(os.path.getsize(sqliteDbPath))
        except Exception:
            return 0

    async def snapshot(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        uptimeSec = int(max(0.0, (now - self.botStartedAt).total_seconds()))
        budgetSnapshot = await self.taskBudgeter.getBudgeter().snapshot()
        retryStats = await self.retryQueue.getStats()
        webhookHealthStats = self.webhookHealthWatcher.getStats()
        processResources = self.getProcessResourceSnapshot(now)

        return {
            "generatedAt": now.isoformat(),
            "uptimeSec": uptimeSec,
            "bot": {
                "userId": int(getattr(getattr(self.botClient, "user", None), "id", 0) or 0),
                "guildCount": len(getattr(self.botClient, "guilds", []) or []),
                "latencyMs": round(float(getattr(self.botClient, "latency", 0.0) or 0.0) * 1000.0, 2),
            },
            "runtime": {
                "dbSizeBytes": self._dbSizeBytes(),
                "process": processResources,
                "tasks": {
                    "startupMaintenance": self._taskState(self.maintenance.startupMaintenanceTask),
                    "globalOrbatUpdate": self._taskState(self.maintenance.globalOrbatUpdateTask),
                    "retryQueueWorker": self._taskState(getattr(self.retryQueue, "workerTask", None)),
                    "webhookHealthWorker": self._taskState(getattr(self.webhookHealthWatcher, "workerTask", None)),
                },
            },
            "budget": budgetSnapshot,
            "retryQueue": retryStats,
            "webhookHealth": webhookHealthStats,
        }
