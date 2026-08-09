from __future__ import annotations

import asyncio
import unittest

from runtime.maintenance import MaintenanceCoordinator


class MaintenanceLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_background_task_is_restarted(self) -> None:
        coordinator = object.__new__(MaintenanceCoordinator)

        async def _complete() -> None:
            return None

        previousStartup = asyncio.create_task(_complete())
        previousGlobal = asyncio.create_task(_complete())
        await asyncio.gather(previousStartup, previousGlobal)
        coordinator.startupMaintenanceTask = previousStartup
        coordinator.globalOrbatUpdateTask = previousGlobal
        coordinator.runStartupMaintenanceOnce = _complete
        coordinator.runGlobalOrbatUpdateLoop = _complete

        coordinator.ensureBackgroundTasksStarted()

        self.assertIsNot(coordinator.startupMaintenanceTask, previousStartup)
        self.assertIsNot(coordinator.globalOrbatUpdateTask, previousGlobal)
        await coordinator.stopBackgroundTasks()

    async def test_stop_background_tasks_cancels_awaits_and_clears_handles(self) -> None:
        coordinator = object.__new__(MaintenanceCoordinator)
        startupStarted = asyncio.Event()
        globalStarted = asyncio.Event()

        async def _wait(started: asyncio.Event) -> None:
            started.set()
            await asyncio.Event().wait()

        coordinator.startupMaintenanceTask = asyncio.create_task(
            _wait(startupStarted),
            name="test-startup-maintenance",
        )
        coordinator.globalOrbatUpdateTask = asyncio.create_task(
            _wait(globalStarted),
            name="test-global-maintenance",
        )
        tasks = (
            coordinator.startupMaintenanceTask,
            coordinator.globalOrbatUpdateTask,
        )
        await asyncio.gather(startupStarted.wait(), globalStarted.wait())

        await coordinator.stopBackgroundTasks()

        self.assertTrue(all(task.cancelled() for task in tasks))
        self.assertIsNone(coordinator.startupMaintenanceTask)
        self.assertIsNone(coordinator.globalOrbatUpdateTask)


if __name__ == "__main__":
    unittest.main()
