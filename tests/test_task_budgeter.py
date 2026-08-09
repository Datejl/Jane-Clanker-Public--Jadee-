from __future__ import annotations

import asyncio
import unittest

from runtime import taskBudgeter
from runtime.taskBudgeter import AsyncTaskBudgeter


class TaskBudgeterPriorityTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_priority_leapfrogs_low_priority_waiter(self) -> None:
        budgeter = AsyncTaskBudgeter({"robloxApi": 1})
        firstStarted = asyncio.Event()
        releaseFirst = asyncio.Event()
        order: list[str] = []

        async def firstOp() -> None:
            order.append("first")
            firstStarted.set()
            await releaseFirst.wait()

        async def namedOp(name: str) -> None:
            order.append(name)

        firstTask = asyncio.create_task(budgeter.run("robloxApi", firstOp))
        await firstStarted.wait()

        lowTask = asyncio.create_task(
            budgeter.run("robloxApi", lambda: namedOp("low"), priority=50)
        )
        await asyncio.sleep(0)
        normalTask = asyncio.create_task(
            budgeter.run("robloxApi", lambda: namedOp("normal"), priority=0)
        )
        await asyncio.sleep(0)

        releaseFirst.set()
        await asyncio.gather(firstTask, lowTask, normalTask)

        self.assertEqual(order, ["first", "normal", "low"])

    async def test_interactive_discord_leapfrogs_low_priority_waiter(self) -> None:
        budgeter = AsyncTaskBudgeter({"discordIo": 1})
        firstStarted = asyncio.Event()
        releaseFirst = asyncio.Event()
        order: list[str] = []

        async def firstOp() -> None:
            order.append("first")
            firstStarted.set()
            await releaseFirst.wait()

        async def namedOp(name: str) -> None:
            order.append(name)

        async def runWithPriority(name: str, priority: int) -> None:
            await budgeter.run("discordIo", lambda: namedOp(name), priority=priority)

        firstTask = asyncio.create_task(budgeter.run("discordIo", firstOp))
        await firstStarted.wait()

        lowTask = asyncio.create_task(runWithPriority("low", taskBudgeter._lowPriorityDiscordPriority()))
        await asyncio.sleep(0)
        interactiveTask = asyncio.create_task(runWithPriority("interactive", taskBudgeter._interactiveDiscordPriority()))
        await asyncio.sleep(0)

        releaseFirst.set()
        await asyncio.gather(firstTask, lowTask, interactiveTask)

        self.assertEqual(order, ["first", "interactive", "low"])

    async def test_run_background_marks_nested_external_io_low_priority(self) -> None:
        observed: list[int] = []

        async def backgroundOp() -> None:
            observed.append(taskBudgeter._discordPriority.get())
            observed.append(taskBudgeter._robloxPriority.get())

        await taskBudgeter.runBackground(backgroundOp)

        self.assertEqual(
            observed,
            [
                taskBudgeter._lowPriorityDiscordPriority(),
                taskBudgeter._lowPriorityRobloxPriority(),
            ],
        )

    async def test_priority_context_resets_after_failure(self) -> None:
        originalDiscordPriority = taskBudgeter._discordPriority.get()
        originalRobloxPriority = taskBudgeter._robloxPriority.get()

        async def failingDiscordOp() -> None:
            self.assertEqual(
                taskBudgeter._discordPriority.get(),
                taskBudgeter._lowPriorityDiscordPriority(),
            )
            raise RuntimeError("discord failed")

        async def failingRobloxOp() -> None:
            self.assertEqual(
                taskBudgeter._robloxPriority.get(),
                taskBudgeter._lowPriorityRobloxPriority(),
            )
            raise RuntimeError("roblox failed")

        with self.assertRaises(RuntimeError):
            await taskBudgeter.runLowPriorityDiscord(failingDiscordOp)
        with self.assertRaises(RuntimeError):
            await taskBudgeter.runLowPriorityRoblox(failingRobloxOp)

        self.assertEqual(taskBudgeter._discordPriority.get(), originalDiscordPriority)
        self.assertEqual(taskBudgeter._robloxPriority.get(), originalRobloxPriority)

    async def test_cancelled_in_flight_task_releases_slot_and_is_counted(self) -> None:
        budgeter = AsyncTaskBudgeter({"backgroundJobs": 1})
        started = asyncio.Event()
        release = asyncio.Event()
        order: list[str] = []

        async def blockedOp() -> None:
            order.append("blocked")
            started.set()
            await release.wait()

        async def nextOp() -> None:
            order.append("next")

        blockedTask = asyncio.create_task(budgeter.run("backgroundJobs", blockedOp))
        await started.wait()
        blockedTask.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await blockedTask

        await budgeter.run("backgroundJobs", nextOp)
        snapshot = await budgeter.snapshot()
        stats = snapshot["features"]["backgroundJobs"]
        totals = snapshot["totals"]

        self.assertEqual(order, ["blocked", "next"])
        self.assertEqual(stats["inFlight"], 0)
        self.assertEqual(stats["waiting"], 0)
        self.assertEqual(stats["completed"], 2)
        self.assertEqual(stats["failed"], 0)
        self.assertEqual(stats["canceled"], 1)
        self.assertEqual(totals["completed"], 2)
        self.assertEqual(totals["failed"], 0)
        self.assertEqual(totals["canceled"], 1)


if __name__ == "__main__":
    unittest.main()
