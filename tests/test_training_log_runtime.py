from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from runtime.trainingLogRuntime import TrainingLogRuntime


async def _runCallback(callback):
    return await callback()


def _runtime(*, coordinator=None, taskBudgeter=None) -> TrainingLogRuntime:
    coordinator = coordinator or SimpleNamespace(
        shouldInspectSourceMessage=MagicMock(return_value=True),
        handleSourceMessage=AsyncMock(),
        ensureSummaryPanelAtBottom=AsyncMock(),
        syncRecentMessages=AsyncMock(),
        _lastReadySyncAt=object(),
    )
    taskBudgeter = taskBudgeter or SimpleNamespace(
        runLowPriorityDiscord=AsyncMock(side_effect=_runCallback),
        runBackground=AsyncMock(side_effect=_runCallback),
    )
    return TrainingLogRuntime(
        botClient=SimpleNamespace(wait_until_ready=AsyncMock()),
        configModule=SimpleNamespace(trainingLogStartupSyncDelaySec=0),
        taskBudgeter=taskBudgeter,
        coordinator=coordinator,
    )


class TrainingLogRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_sync_retries_until_coordinator_reports_success(self) -> None:
        attempts = 0
        coordinator = SimpleNamespace(
            ensureSummaryPanelAtBottom=AsyncMock(),
            _lastReadySyncAt=None,
        )

        async def _syncRecentMessages() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 2:
                coordinator._lastReadySyncAt = object()

        coordinator.syncRecentMessages = AsyncMock(side_effect=_syncRecentMessages)
        runtime = _runtime(coordinator=coordinator)

        with patch("runtime.trainingLogRuntime.asyncio.sleep", AsyncMock()) as sleepMock:
            await runtime._runStartupSync()

        self.assertEqual(coordinator.syncRecentMessages.await_count, 2)
        coordinator.ensureSummaryPanelAtBottom.assert_awaited_once()
        sleepMock.assert_awaited_once_with(30)

    async def test_capture_task_is_owned_until_message_processing_finishes(self) -> None:
        runtime = _runtime()
        message = SimpleNamespace(id=123)

        runtime.scheduleCapture(message)
        captureTask = runtime._captureTasks[123]
        await captureTask

        runtime.coordinator.handleSourceMessage.assert_awaited_once_with(message)
        self.assertEqual(runtime._captureTasks, {})

    async def test_start_is_idempotent_and_stop_cancels_owned_tasks(self) -> None:
        runtime = _runtime()
        startupStarted = asyncio.Event()
        captureStarted = asyncio.Event()

        async def _startup() -> None:
            startupStarted.set()
            await asyncio.Event().wait()

        async def _capture(_message) -> None:
            captureStarted.set()
            await asyncio.Event().wait()

        runtime._runStartupSync = _startup
        runtime.coordinator.handleSourceMessage = AsyncMock(side_effect=_capture)

        runtime.start()
        startupTask = runtime._startupSyncTask
        runtime.start()
        runtime.scheduleCapture(SimpleNamespace(id=456))
        captureTask = runtime._captureTasks[456]
        await asyncio.gather(startupStarted.wait(), captureStarted.wait())

        self.assertIs(runtime._startupSyncTask, startupTask)
        await runtime.stop()

        assert startupTask is not None
        self.assertTrue(startupTask.cancelled())
        self.assertTrue(captureTask.cancelled())
        self.assertIsNone(runtime._startupSyncTask)
        self.assertEqual(runtime._captureTasks, {})


if __name__ == "__main__":
    unittest.main()
