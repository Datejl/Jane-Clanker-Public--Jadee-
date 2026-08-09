from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from runtime import eventIngest
from runtime.johnEventRuntime import JohnEventCoordinator


async def _runCallback(callback):
    return await callback()


def _message(*, messageId: int = 100) -> SimpleNamespace:
    return SimpleNamespace(
        id=messageId,
        channel=SimpleNamespace(id=200),
        guild=SimpleNamespace(id=300),
        author=SimpleNamespace(id=400),
        reactions=[],
        jump_url=f"https://discord.invalid/{messageId}",
        add_reaction=AsyncMock(),
    )


def _event(*, messageId: int = 100, hostId: int = 500) -> eventIngest.IngestEvent:
    return eventIngest.IngestEvent(
        source="john.eventLog",
        eventType="john.orbatIncrement",
        messageId=messageId,
        hostId=hostId,
        payload={"eventCategory": "shift", "eventTypeRaw": "Public shift"},
    )


def _coordinator(**overrides) -> JohnEventCoordinator:
    taskBudgeter = SimpleNamespace(
        runSheetsThread=AsyncMock(return_value=12),
        runDiscord=AsyncMock(side_effect=_runCallback),
        runLowPriorityDiscord=AsyncMock(side_effect=_runCallback),
    )
    values = {
        "botClient": SimpleNamespace(),
        "configModule": SimpleNamespace(
            johnEventOrbatWritesEnabled=True,
            johnClankerBotId=400,
            johnEventLogChannelId=200,
        ),
        "taskBudgeter": taskBudgeter,
        "orbatSheets": SimpleNamespace(incrementEventCount=MagicMock()),
        "robloxUsersModule": SimpleNamespace(fetchRobloxUser=AsyncMock()),
        "orbatAuditRuntime": SimpleNamespace(sendOrbatChangeLog=AsyncMock()),
        "privateExtensionsEnabled": True,
        "fetchOneFn": AsyncMock(return_value=None),
        "executeFn": AsyncMock(),
        "dispatcher": SimpleNamespace(parse=AsyncMock(return_value=[])),
    }
    values.update(overrides)
    return JohnEventCoordinator(**values)


class JohnEventCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_increment_is_audited_persisted_and_reacted(self) -> None:
        coordinator = _coordinator()
        message = _message()

        await coordinator.handleIngestedEvent(message, _event())

        coordinator.taskBudgeter.runSheetsThread.assert_awaited_once_with(
            coordinator.orbatSheets.incrementEventCount,
            500,
            "shifts",
            1,
        )
        coordinator.orbatAuditRuntime.sendOrbatChangeLog.assert_awaited_once()
        coordinator.execute.assert_awaited_once()
        message.add_reaction.assert_awaited_once_with(coordinator.successEmoji)

    async def test_existing_event_is_not_counted_twice(self) -> None:
        coordinator = _coordinator(fetchOneFn=AsyncMock(return_value={"messageId": 100}))

        await coordinator.handleIngestedEvent(_message(), _event())

        coordinator.taskBudgeter.runSheetsThread.assert_not_awaited()
        coordinator.execute.assert_not_awaited()

    async def test_protected_sheet_error_suspends_later_writes(self) -> None:
        taskBudgeter = SimpleNamespace(
            runSheetsThread=AsyncMock(side_effect=RuntimeError("protected cell or object")),
            runDiscord=AsyncMock(),
            runLowPriorityDiscord=AsyncMock(),
        )
        coordinator = _coordinator(taskBudgeter=taskBudgeter)

        with patch("runtime.johnEventRuntime.log.warning"):
            await coordinator.handleIngestedEvent(_message(messageId=100), _event(messageId=100))
            await coordinator.handleIngestedEvent(_message(messageId=101), _event(messageId=101))

        self.assertTrue(coordinator._sheetWritesSuspended)
        self.assertEqual(taskBudgeter.runSheetsThread.await_count, 1)
        coordinator.execute.assert_not_awaited()

    async def test_missing_discord_mapping_retries_with_roblox_username(self) -> None:
        taskBudgeter = SimpleNamespace(
            runSheetsThread=AsyncMock(side_effect=[0, 24]),
            runDiscord=AsyncMock(side_effect=_runCallback),
            runLowPriorityDiscord=AsyncMock(),
        )
        robloxUsers = SimpleNamespace(
            fetchRobloxUser=AsyncMock(
                return_value=SimpleNamespace(robloxUsername="MappedRobloxUser")
            )
        )
        coordinator = _coordinator(
            taskBudgeter=taskBudgeter,
            robloxUsersModule=robloxUsers,
        )

        await coordinator.handleIngestedEvent(_message(), _event())

        robloxUsers.fetchRobloxUser.assert_awaited_once_with(500, 300)
        self.assertEqual(taskBudgeter.runSheetsThread.await_count, 2)
        self.assertEqual(
            taskBudgeter.runSheetsThread.await_args_list[1].kwargs,
            {"robloxUser": "MappedRobloxUser"},
        )

    async def test_start_is_idempotent_and_stop_cancels_backfill(self) -> None:
        coordinator = _coordinator()
        started = asyncio.Event()

        async def _waitForever() -> None:
            started.set()
            await asyncio.Event().wait()

        coordinator._runStartupBackfill = _waitForever
        coordinator.start()
        firstTask = coordinator._startupBackfillTask
        coordinator.start()
        await started.wait()

        self.assertIs(coordinator._startupBackfillTask, firstTask)
        await coordinator.stop()
        self.assertIsNone(coordinator._startupBackfillTask)
        assert firstTask is not None
        self.assertTrue(firstTask.cancelled())


if __name__ == "__main__":
    unittest.main()
