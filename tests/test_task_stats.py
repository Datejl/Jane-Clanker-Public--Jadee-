from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from runtime import taskStats


class TaskStatsStoreTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _emptyStore() -> taskStats._TaskStatsStore:
        store = taskStats._TaskStatsStore()
        store._loaded = True
        return store

    async def test_records_share_one_delayed_flush_task(self) -> None:
        store = self._emptyStore()
        store._writeSync = MagicMock()
        with (
            patch.object(taskStats, "_flushDirtyCount", return_value=100),
            patch.object(taskStats, "_flushIntervalSec", return_value=300),
        ):
            await asyncio.gather(
                store.record("first", 1.0),
                store.record("second", 2.0),
            )

        flushTask = store._flushTask
        self.assertIsNotNone(flushTask)
        self.assertFalse(flushTask.done())
        await store.shutdown()
        assert flushTask is not None
        self.assertTrue(flushTask.cancelled())

    async def test_shutdown_flushes_dirty_stats_after_cancelling_delay(self) -> None:
        store = self._emptyStore()
        writeSync = MagicMock()
        store._writeSync = writeSync
        with (
            patch.object(taskStats, "_flushDirtyCount", return_value=100),
            patch.object(taskStats, "_flushIntervalSec", return_value=300),
        ):
            await store.record("example", 12.5)
            await store.shutdown()

        writeSync.assert_called_once()
        writtenEntries = writeSync.call_args.args[0]
        self.assertEqual(
            writtenEntries,
            [{"name": "example", "timeMs": 12.5, "amount": 1}],
        )
        self.assertEqual(store._dirtyCount, 0)
        self.assertIsNone(store._flushTask)

    async def test_failed_write_remains_dirty_for_a_later_retry(self) -> None:
        store = self._emptyStore()
        store._writeSync = MagicMock(side_effect=OSError("disk unavailable"))
        with (
            patch.object(taskStats, "_flushDirtyCount", return_value=1),
            patch.object(taskStats.log, "warning"),
        ):
            await store.record("example", 5.0)

        self.assertEqual(store._dirtyCount, 1)


class TaskStatsPathTests(unittest.TestCase):
    def test_relative_stats_path_is_anchored_to_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tempDir:
            repoRoot = Path(tempDir).resolve()
            with (
                patch.object(taskStats, "_REPO_ROOT", repoRoot),
                patch.object(taskStats.config, "runtimeTaskStatsPath", "runtime/data/stats.json"),
            ):
                resolved = taskStats._statsPath()

        self.assertEqual(resolved, repoRoot / "runtime" / "data" / "stats.json")

    def test_absolute_stats_path_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tempDir:
            configured = (Path(tempDir) / "stats.json").resolve()
            with patch.object(taskStats.config, "runtimeTaskStatsPath", str(configured)):
                resolved = taskStats._statsPath()

        self.assertEqual(resolved, configured)


if __name__ == "__main__":
    unittest.main()
