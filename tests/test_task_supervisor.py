from __future__ import annotations

import asyncio
import unittest
from unittest.mock import MagicMock

from runtime.taskSupervisor import TaskSupervisor, cancelTasks


class TaskSupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_tasks_waits_for_task_shutdown(self) -> None:
        cleanupFinished = asyncio.Event()

        async def _wait() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                cleanupFinished.set()

        task = asyncio.create_task(_wait())
        await asyncio.sleep(0)
        await cancelTasks(task, None, task)

        self.assertTrue(task.cancelled())
        self.assertTrue(cleanupFinished.is_set())

    async def test_completed_task_is_removed(self) -> None:
        supervisor = TaskSupervisor()

        async def _complete() -> int:
            return 42

        task = supervisor.create(_complete(), name="complete")
        assert task is not None
        self.assertEqual(await task, 42)
        await asyncio.sleep(0)
        self.assertEqual(supervisor.activeCount, 0)

    async def test_crashed_task_result_is_consumed_and_logged(self) -> None:
        logger = MagicMock()
        supervisor = TaskSupervisor(logger=logger)

        async def _crash() -> None:
            raise RuntimeError("boom")

        task = supervisor.create(_crash(), name="crash")
        assert task is not None
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

        logger.exception.assert_called_once_with(
            "Supervised runtime task crashed: %s",
            "crash",
        )
        self.assertEqual(supervisor.activeCount, 0)

    async def test_stop_cancels_tasks_and_rejects_new_work(self) -> None:
        supervisor = TaskSupervisor()
        started = asyncio.Event()

        async def _wait() -> None:
            started.set()
            await asyncio.Event().wait()

        task = supervisor.create(_wait(), name="wait")
        assert task is not None
        await started.wait()
        await supervisor.stop()

        self.assertTrue(task.cancelled())
        self.assertEqual(supervisor.activeCount, 0)
        self.assertIsNone(supervisor.create(_wait(), name="late"))


if __name__ == "__main__":
    unittest.main()
