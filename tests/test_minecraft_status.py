from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from cogs.community.minecraftCog import (
    MinecraftCog,
    MinecraftStatus,
    buildEmbed,
    parsePlayerCount,
    parseTps,
)


class MinecraftStatusParsingTests(unittest.TestCase):
    def test_parse_standard_player_count(self) -> None:
        self.assertEqual(
            parsePlayerCount(
                "There are 12 of a max of 60 players online: a, b",
                fallbackMaximum=20,
            ),
            (12, 60),
        )

    def test_parse_player_count_falls_back_safely(self) -> None:
        self.assertEqual(parsePlayerCount("unexpected", fallbackMaximum=60), (0, 60))

    def test_parse_last_reported_tps(self) -> None:
        self.assertEqual(
            parseTps("Dim 0 Mean TPS: 18.5; Overall Mean TPS: 19.987"),
            "19.99",
        )
        self.assertEqual(parseTps("unexpected"), "N/A")

    def test_embed_uses_dynamic_player_maximum(self) -> None:
        embed = buildEmbed(MinecraftStatus(True, playerCount=3, maxPlayers=25, tps="20.00"))
        self.assertEqual(embed.fields[1].value, "3/25")


class MinecraftStatusPollingTests(unittest.IsolatedAsyncioTestCase):
    async def test_rcon_failure_reports_offline_without_escaping(self) -> None:
        message = SimpleNamespace(edit=AsyncMock())
        cog = MinecraftCog(SimpleNamespace())
        cog.statusMessageId = 1
        with (
            patch.object(cog, "_resolveStoredMessage", AsyncMock(return_value=message)),
            patch.object(cog, "_queryRconSync", side_effect=ConnectionError("offline")),
            patch.object(cog, "_updateStoredStatus", AsyncMock()) as updateMock,
        ):
            online = await cog._pollOnce()

        self.assertFalse(online)
        self.assertFalse(cog.lastStatus.online)
        message.edit.assert_awaited_once()
        updateMock.assert_awaited_once()

    async def test_successful_poll_updates_message_and_state(self) -> None:
        message = SimpleNamespace(edit=AsyncMock())
        cog = MinecraftCog(SimpleNamespace())
        status = MinecraftStatus(True, playerCount=2, maxPlayers=30, tps="20.00")
        with (
            patch.object(cog, "_resolveStoredMessage", AsyncMock(return_value=message)),
            patch.object(cog, "_queryRconSync", return_value=status),
            patch.object(cog, "_updateStoredStatus", AsyncMock()) as updateMock,
        ):
            online = await cog._pollOnce()

        self.assertTrue(online)
        self.assertEqual(cog.lastStatus, status)
        message.edit.assert_awaited_once()
        updateMock.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
