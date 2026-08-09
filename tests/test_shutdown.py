from __future__ import annotations

import asyncio
import unittest

from runtime.shutdown import OnceAsyncCleanup


class OnceAsyncCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_callers_share_one_cleanup(self) -> None:
        coordinator = OnceAsyncCleanup()
        calls = 0

        async def _cleanup() -> None:
            nonlocal calls
            calls += 1
            await asyncio.sleep(0)

        await asyncio.gather(coordinator.run(_cleanup), coordinator.run(_cleanup))

        self.assertEqual(calls, 1)
        self.assertTrue(coordinator.started)
        self.assertTrue(coordinator.done)

    async def test_cancelled_waiter_does_not_cancel_cleanup(self) -> None:
        coordinator = OnceAsyncCleanup()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def _cleanup() -> None:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()

        firstWaiter = asyncio.create_task(coordinator.run(_cleanup))
        await started.wait()
        firstWaiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await firstWaiter

        self.assertFalse(coordinator.done)
        release.set()
        await coordinator.run(_cleanup)

        self.assertEqual(calls, 1)
        self.assertTrue(coordinator.done)


if __name__ == "__main__":
    unittest.main()
