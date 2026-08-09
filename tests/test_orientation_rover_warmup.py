from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from features.staff.sessions import orientationRoverWarmup
from features.staff.sessions.Roblox.robloxModels import RoverLookupResult


class OrientationRoverWarmupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        orientationRoverWarmup._warmupTasks.clear()
        orientationRoverWarmup._warmedUsersBySession.clear()

    async def test_warmup_once_uses_low_priority_for_unresolved_attendees(self) -> None:
        fetched: list[tuple[int, list[int]]] = []
        lowPriorityCalls = 0

        async def runLowPriority(opFactory):
            nonlocal lowPriorityCalls
            lowPriorityCalls += 1
            return await opFactory()

        async def fetchWithFallbacks(discordUserId: int, guildIds: list[int]):
            fetched.append((discordUserId, list(guildIds)))
            return RoverLookupResult(1000 + discordUserId, f"User{discordUserId}")

        with (
            patch.object(
                orientationRoverWarmup.service,
                "getSession",
                AsyncMock(
                    return_value={
                        "sessionId": 42,
                        "sessionType": "orientation",
                        "status": "OPEN",
                        "guildId": 999,
                    }
                ),
            ),
            patch.object(
                orientationRoverWarmup.service,
                "getAttendees",
                AsyncMock(
                    return_value=[
                        {"userId": 10, "examGrade": "FAIL"},
                        {"userId": 11, "examGrade": "PASS"},
                        {
                            "userId": 12,
                            "examGrade": "FAIL",
                            "robloxUserId": 1200,
                            "robloxUsername": "StoredAttendee",
                        },
                    ]
                ),
            ),
            patch.object(
                orientationRoverWarmup.robloxUsers,
                "getStoredRobloxIdentity",
                AsyncMock(return_value=None),
            ),
            patch.object(
                orientationRoverWarmup.bgSpreadsheetQueue,
                "fetchRobloxUserWithFallbacks",
                AsyncMock(side_effect=fetchWithFallbacks),
            ),
            patch.object(
                orientationRoverWarmup.taskBudgeter,
                "runLowPriorityRoblox",
                AsyncMock(side_effect=runLowPriority),
            ),
        ):
            attempted = await orientationRoverWarmup.warmupOrientationRoverLookupsOnce(
                SimpleNamespace(),
                42,
            )

        self.assertEqual(attempted, 2)
        self.assertEqual(lowPriorityCalls, 2)
        self.assertEqual([userId for userId, _guildIds in fetched], [10, 11])
        self.assertTrue(all(999 in guildIds for _userId, guildIds in fetched))

    async def test_warmup_once_skips_internal_stored_identity(self) -> None:
        with (
            patch.object(
                orientationRoverWarmup.service,
                "getSession",
                AsyncMock(
                    return_value={
                        "sessionId": 42,
                        "sessionType": "orientation",
                        "status": "OPEN",
                        "guildId": 999,
                    }
                ),
            ),
            patch.object(
                orientationRoverWarmup.service,
                "getAttendees",
                AsyncMock(return_value=[{"userId": 10}]),
            ),
            patch.object(
                orientationRoverWarmup.robloxUsers,
                "getStoredRobloxIdentity",
                AsyncMock(return_value=RoverLookupResult(1000, "StoredLink")),
            ),
            patch.object(
                orientationRoverWarmup.bgSpreadsheetQueue,
                "fetchRobloxUserWithFallbacks",
                AsyncMock(),
            ) as fetchMock,
            patch.object(
                orientationRoverWarmup.taskBudgeter,
                "runLowPriorityRoblox",
                AsyncMock(),
            ) as lowPriorityMock,
        ):
            attempted = await orientationRoverWarmup.warmupOrientationRoverLookupsOnce(
                SimpleNamespace(),
                42,
            )

        self.assertEqual(attempted, 0)
        fetchMock.assert_not_awaited()
        lowPriorityMock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
