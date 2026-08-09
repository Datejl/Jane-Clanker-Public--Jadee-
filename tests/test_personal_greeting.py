from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

from db import sqlite as sqliteDb
from runtime.dailyMessage import DailyMessageTrigger
from runtime.textCommands import TextCommandRouter


POTATO_USER_ID = 331660652672319488
GENERAL_CHANNEL_ID = 1525783767971528764


def _message(*, userId: int = POTATO_USER_ID, channelId: int = GENERAL_CHANNEL_ID):
    return SimpleNamespace(
        author=SimpleNamespace(id=userId),
        channel=SimpleNamespace(id=channelId, send=AsyncMock()),
    )


class PersonalGreetingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tempDir = tempfile.TemporaryDirectory()
        self._originalDbPath = sqliteDb.dbPath
        await sqliteDb.closeDb()
        sqliteDb.dbPath = str(Path(self._tempDir.name) / "test.db")
        await sqliteDb.initDb()

    async def asyncTearDown(self) -> None:
        await sqliteDb.closeDb()
        sqliteDb.dbPath = self._originalDbPath
        self._tempDir.cleanup()

    async def test_greets_potato_once_per_central_day(self) -> None:
        message = _message()

        first = await TextCommandRouter.handlePotatoGreeting(
            message,
            nowUtc=datetime(2026, 1, 22, 23, tzinfo=timezone.utc),
        )
        repeated = await TextCommandRouter.handlePotatoGreeting(
            message,
            nowUtc=datetime(2026, 1, 23, 1, tzinfo=timezone.utc),
        )
        nextDay = await TextCommandRouter.handlePotatoGreeting(
            message,
            nowUtc=datetime(2026, 1, 23, 6, tzinfo=timezone.utc),
        )

        self.assertTrue(first)
        self.assertFalse(repeated)
        self.assertTrue(nextDay)
        self.assertEqual(
            message.channel.send.await_args_list,
            [call("good to see you, mom")] * 2,
        )

    async def test_ignores_every_other_user_and_channel(self) -> None:
        wrongUser = _message(userId=1)
        wrongChannel = _message(channelId=2)

        self.assertFalse(await TextCommandRouter.handlePotatoGreeting(wrongUser))
        self.assertFalse(await TextCommandRouter.handlePotatoGreeting(wrongChannel))
        wrongUser.channel.send.assert_not_awaited()
        wrongChannel.channel.send.assert_not_awaited()

    async def test_failed_send_can_retry_that_day(self) -> None:
        message = _message()
        message.channel.send.side_effect = [RuntimeError("temporary failure"), None]
        now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)

        failed = await TextCommandRouter.handlePotatoGreeting(message, nowUtc=now)
        retried = await TextCommandRouter.handlePotatoGreeting(message, nowUtc=now)

        self.assertFalse(failed)
        self.assertTrue(retried)
        self.assertEqual(message.channel.send.await_count, 2)

    async def test_daily_trigger_is_reusable_for_another_small_flow(self) -> None:
        trigger = DailyMessageTrigger(
            key="futureFlow",
            userId=10,
            channelId=20,
            content="hello again",
        )
        message = _message(userId=10, channelId=20)
        now = datetime(2026, 1, 22, 12, tzinfo=timezone.utc)

        self.assertTrue(await trigger.handle(message, now=now))
        self.assertFalse(await trigger.handle(message, now=now))
        message.channel.send.assert_awaited_once_with("hello again")


if __name__ == "__main__":
    unittest.main()
