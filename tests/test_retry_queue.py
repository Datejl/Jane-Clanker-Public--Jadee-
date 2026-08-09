from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from runtime.retryQueue import RetryQueueCoordinator


class RetryQueueRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_stale_processing_jobs_are_requeued_as_failed(self) -> None:
        coordinator = RetryQueueCoordinator(
            taskBudgeter=SimpleNamespace(runBackground=AsyncMock()),
            pollIntervalSec=6,
        )
        oldUpdatedAt = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

        with (
            patch(
                "runtime.retryQueue.fetchAll",
                AsyncMock(return_value=[{"jobId": 123, "updatedAt": oldUpdatedAt}]),
            ),
            patch("runtime.retryQueue.execute", AsyncMock()) as executeMock,
        ):
            recovered = await coordinator._recoverStaleProcessingJobs()

        self.assertEqual(recovered, 1)
        executeMock.assert_awaited_once()
        query = executeMock.await_args.args[0]
        params = executeMock.await_args.args[1]
        self.assertIn("SET status = 'FAILED'", query)
        self.assertEqual(params[-1], 123)


if __name__ == "__main__":
    unittest.main()
