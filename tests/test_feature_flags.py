from __future__ import annotations

import unittest
import asyncio
from unittest.mock import AsyncMock

from runtime.featureFlags import FeatureFlagService


class _Config:
    featureFlagDefaults = {
        "closed": False,
    }


class FeatureFlagServiceCachedTests(unittest.TestCase):
    def test_cached_command_check_returns_default_without_database_read(self) -> None:
        service = FeatureFlagService(configModule=_Config())

        enabled, featureKey, cacheHit = service.isCommandEnabledCached(123, "example")

        self.assertTrue(enabled)
        self.assertEqual(featureKey, "example")
        self.assertFalse(cacheHit)

    def test_cached_command_check_respects_config_default(self) -> None:
        service = FeatureFlagService(configModule=_Config())

        enabled, featureKey, cacheHit = service.isCommandEnabledCached(123, "closed")

        self.assertFalse(enabled)
        self.assertEqual(featureKey, "closed")
        self.assertFalse(cacheHit)

    def test_cached_command_check_uses_cached_value(self) -> None:
        service = FeatureFlagService(configModule=_Config())
        service._writeCache(123, "example", False)

        enabled, featureKey, cacheHit = service.isCommandEnabledCached(123, "example")

        self.assertFalse(enabled)
        self.assertEqual(featureKey, "example")
        self.assertTrue(cacheHit)


class FeatureFlagServiceLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_cancels_background_refresh_and_blocks_new_refreshes(self) -> None:
        service = FeatureFlagService(configModule=_Config())
        started = asyncio.Event()

        async def _waitForFlag(*_args) -> bool:
            started.set()
            await asyncio.Event().wait()
            return True

        service.getFlag = AsyncMock(side_effect=_waitForFlag)
        service.refreshCommandFlagCacheSoon(123, "example")
        task = next(iter(service._backgroundRefreshTasks.values()))
        await started.wait()

        await service.stop()

        self.assertTrue(task.cancelled())
        self.assertEqual(service._backgroundRefreshTasks, {})
        self.assertEqual(service._backgroundRefreshes, set())
        service.refreshCommandFlagCacheSoon(123, "later")
        self.assertEqual(service._backgroundRefreshTasks, {})


if __name__ == "__main__":
    unittest.main()
