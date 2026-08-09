from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from cogs.staff import cohostCog
from db import sqlite as sqliteDb


class _FakeBot:
    def get_channel(self, channel_id: int):
        return None


class _FakeGuild:
    def __init__(self) -> None:
        self.roles = []


class _FakeRole:
    def __init__(self, roleId: int) -> None:
        self.id = roleId


class _FakeMember:
    def __init__(self, roleIds: list[int]) -> None:
        self.roles = [_FakeRole(roleId) for roleId in roleIds]


class _FakeChannel:
    pass


class _FakeMessage:
    def __init__(self) -> None:
        self.channel = _FakeChannel()
        self.guild = _FakeGuild()


class CohostCogFinalizeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tempDir = tempfile.TemporaryDirectory()
        self._originalDbPath = sqliteDb.dbPath
        await sqliteDb.closeDb()
        sqliteDb.dbPath = str(Path(self._tempDir.name) / "test.db")
        await sqliteDb.initDb()

        self.cog = cohostCog.CohostCog(_FakeBot())
        self.message = _FakeMessage()

    async def asyncTearDown(self) -> None:
        await sqliteDb.closeDb()
        sqliteDb.dbPath = self._originalDbPath
        self._tempDir.cleanup()

    async def _insertRequest(self, *, messageId: int, eventType: str) -> None:
        await sqliteDb.execute(
            """
            INSERT INTO cohost_requests
                (messageId, guildId, channelId, hostId, eventType, collectMinutes, status, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (messageId, 1, 10, 99, eventType, 5, "OPEN", "2026-05-08 15:00:00"),
        )

    async def _insertVolunteer(self, *, messageId: int, userId: int, rank: str) -> None:
        await sqliteDb.execute(
            "INSERT INTO cohost_volunteers (messageId, userId, rank) VALUES (?, ?, ?)",
            (messageId, userId, rank),
        )

    async def test_finalize_request_records_same_user_across_multiple_events(self) -> None:
        await self._insertRequest(messageId=123, eventType="solo")
        await self._insertVolunteer(messageId=123, userId=111, rank="SRO")

        await self._insertRequest(messageId=124, eventType="turbine")
        await self._insertVolunteer(messageId=124, userId=111, rank="SRO")

        with (
            patch.object(cohostCog.CohostCog, "_fetchMessage", AsyncMock(return_value=self.message)),
            patch.object(cohostCog.interactionRuntime, "safeMessageEdit", AsyncMock(return_value=True)) as mockEdit,
            patch.object(cohostCog.interactionRuntime, "safeChannelSend", AsyncMock(return_value=object())) as mockSend,
        ):
            firstResult = await self.cog._finalizeRequest(123)
            secondResult = await self.cog._finalizeRequest(124)

        self.assertEqual(firstResult, "Cohost selection finished for Solo: <@111>")
        self.assertEqual(secondResult, "Cohost selection finished for Turbine: <@111>")
        self.assertEqual(mockEdit.await_count, 2)
        self.assertEqual(mockSend.await_count, 2)

        firstRow = await sqliteDb.fetchOne(
            "SELECT status FROM cohost_requests WHERE messageId = ?",
            (123,),
        )
        secondRow = await sqliteDb.fetchOne(
            "SELECT status FROM cohost_requests WHERE messageId = ?",
            (124,),
        )
        self.assertEqual(firstRow["status"], "FINISHED")
        self.assertEqual(secondRow["status"], "FINISHED")

        historyRows = await sqliteDb.fetchAll(
            """
            SELECT eventType, userId, rank
            FROM cohost_history
            WHERE userId = ?
            ORDER BY historyId
            """,
            (111,),
        )
        self.assertEqual(
            [(row["eventType"], row["userId"], row["rank"]) for row in historyRows],
            [("solo", 111, "SRO"), ("turbine", 111, "SRO")],
        )

    async def test_finalize_request_without_volunteers_closes_request(self) -> None:
        await self._insertRequest(messageId=500, eventType="solo")

        with (
            patch.object(cohostCog.CohostCog, "_fetchMessage", AsyncMock(return_value=self.message)),
            patch.object(cohostCog.interactionRuntime, "safeMessageEdit", AsyncMock(return_value=True)) as mockEdit,
            patch.object(cohostCog.interactionRuntime, "safeChannelSend", AsyncMock(return_value=object())) as mockSend,
        ):
            result = await self.cog._finalizeRequest(500)

        self.assertEqual(result, "No volunteers collected for Solo.")
        self.assertEqual(mockEdit.await_count, 1)
        mockSend.assert_awaited_once()
        self.assertEqual(
            mockSend.await_args.kwargs.get("content"),
            "No volunteers collected for Solo.",
        )

        row = await sqliteDb.fetchOne(
            "SELECT status FROM cohost_requests WHERE messageId = ?",
            (500,),
        )
        self.assertEqual(row["status"], "FINISHED")

    async def test_auto_finish_does_not_cancel_its_own_notifications(self) -> None:
        await self._insertRequest(messageId=501, eventType="solo")
        request = await self.cog._getRequest(501)
        assert request is not None

        with (
            patch.object(
                cohostCog.CohostCog,
                "_fetchMessage",
                AsyncMock(return_value=self.message),
            ),
            patch.object(
                cohostCog.interactionRuntime,
                "safeMessageEdit",
                AsyncMock(return_value=True),
            ) as mockEdit,
            patch.object(
                cohostCog.interactionRuntime,
                "safeChannelSend",
                AsyncMock(return_value=object()),
            ) as mockSend,
        ):
            autoTask = asyncio.create_task(self.cog._autoFinish(501, 0))
            request.autoTask = autoTask
            await autoTask

        self.assertFalse(autoTask.cancelled())
        self.assertIsNone(request.autoTask)
        mockEdit.assert_awaited_once()
        mockSend.assert_awaited_once()
        row = await sqliteDb.fetchOne(
            "SELECT status FROM cohost_requests WHERE messageId = ?",
            (501,),
        )
        self.assertEqual(row, {"status": "FINISHED"})

    async def test_cog_unload_cancels_open_request_timers(self) -> None:
        request = cohostCog.CohostRequest(
            messageId=777,
            guildId=1,
            channelId=10,
            hostId=99,
            eventType="solo",
            collectMinutes=5,
            status="OPEN",
            createdAt=cohostCog.datetime.now(),
        )
        request.autoTask = asyncio.create_task(asyncio.Event().wait())
        self.cog.requests[request.messageId] = request
        self.cog._finalizeLocks[request.messageId] = asyncio.Lock()

        timerTask = request.autoTask
        await self.cog.cog_unload()

        self.assertTrue(timerTask.cancelled())
        self.assertEqual(self.cog.requests, {})
        self.assertEqual(self.cog._finalizeLocks, {})


class CohostRankDetectionTests(unittest.TestCase):
    def test_rank_detection_uses_configured_role_ids(self) -> None:
        self.assertEqual(cohostCog._rankFromMember(_FakeMember([1373445628054736937])), "SRO")
        self.assertEqual(cohostCog._rankFromMember(_FakeMember([1424201132435177492])), "STA")
        self.assertEqual(
            cohostCog._rankFromMember(_FakeMember([1424201132435177492, 1373445628054736937])),
            "SRO",
        )
        self.assertIsNone(cohostCog._rankFromMember(_FakeMember([123])))


if __name__ == "__main__":
    unittest.main()
