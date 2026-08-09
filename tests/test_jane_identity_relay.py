from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from db import sqlite as sqliteDb
from features.community.identity import service as janeIdentity


class JaneIdentityRelayTests(unittest.IsolatedAsyncioTestCase):
    def test_relay_headers_survive_hosts_that_strip_authorization(self) -> None:
        headers = janeIdentity.relayRequestHeaders("relay-token", jsonBody=True)

        self.assertEqual(headers["Authorization"], "Bearer relay-token")
        self.assertEqual(headers["X-Jane-Identity-Token"], "relay-token")
        self.assertEqual(headers["Content-Type"], "application/json")

    async def asyncSetUp(self) -> None:
        self._tempDir = tempfile.TemporaryDirectory()
        self._originalDbPath = sqliteDb.dbPath
        await sqliteDb.closeDb()
        sqliteDb.dbPath = str(Path(self._tempDir.name) / "test.db")
        await sqliteDb.initDb()
        janeIdentity._linkAttempts.clear()

    async def asyncTearDown(self) -> None:
        janeIdentity._linkAttempts.clear()
        await sqliteDb.closeDb()
        sqliteDb.dbPath = self._originalDbPath
        self._tempDir.cleanup()

    async def test_oauth_attempt_can_complete_after_memory_loss(self) -> None:
        with (
            patch.object(janeIdentity.config, "janeIdentityEnabled", True, create=True),
            patch.object(janeIdentity.config, "janeIdentityWebEnabled", False, create=True),
            patch.object(janeIdentity.config, "janeIdentityRelayEnabled", True, create=True),
            patch.object(janeIdentity.config, "janeIdentityPublicBaseUrl", "https://memorias.example", create=True),
            patch.object(janeIdentity.config, "janeIdentityRelayApiToken", "relay-token", create=True),
            patch.object(janeIdentity.config, "robloxOAuthClientId", "roblox-client-id", create=True),
            patch.object(janeIdentity.config, "robloxOAuthClientSecret", "roblox-client-secret", create=True),
        ):
            attempt = await janeIdentity.createLinkAttempt(discordUserId=123, guildId=456)

        self.assertTrue(attempt.state)
        self.assertIn("redirect_uri=https%3A%2F%2Fmemorias.example%2Fidentity%2Froblox%2Fcallback", attempt.authorize_url)

        janeIdentity._linkAttempts.clear()

        with (
            patch.object(
                janeIdentity,
                "_exchangeCodeForUserInfo",
                AsyncMock(
                    return_value={
                        "sub": "789",
                        "preferred_username": "LinkedRoblox",
                        "name": "Linked Roblox",
                    }
                ),
            ) as exchangeMock,
            patch.object(
                janeIdentity.robloxUsers,
                "rememberKnownRobloxIdentity",
                AsyncMock(return_value=True),
            ) as rememberMock,
        ):
            result = await janeIdentity.completeRobloxOAuth(code="oauth-code", state=attempt.state)

        self.assertTrue(result.ok)
        self.assertEqual(result.discord_user_id, 123)
        self.assertEqual(result.guild_id, 456)
        self.assertEqual(result.roblox_user_id, 789)
        self.assertEqual(result.roblox_username, "LinkedRoblox")
        exchangeMock.assert_awaited_once()
        rememberMock.assert_awaited_once_with(
            123,
            "LinkedRoblox",
            robloxId=789,
            source="jane-identity:oauth",
            guildId=456,
            confidence=100,
        )

        row = await sqliteDb.fetchOne(
            "SELECT state FROM jane_identity_link_attempts WHERE state = ?",
            (attempt.state,),
        )
        self.assertIsNone(row)


if __name__ == "__main__":
    unittest.main()
