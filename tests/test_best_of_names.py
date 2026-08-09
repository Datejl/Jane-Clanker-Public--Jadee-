from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from cogs.community.bestOfCog import BestOfCog
from features.staff.sessions.Roblox.robloxModels import RoverLookupResult


class BestOfCandidateNameTests(unittest.IsolatedAsyncioTestCase):
    async def test_roblox_name_is_used_when_lookup_succeeds(self) -> None:
        cog = BestOfCog(SimpleNamespace())
        guild = SimpleNamespace(id=10)
        member = SimpleNamespace(id=20, display_name="[MR] Discord Name", name="Discord Name")

        with patch(
            "cogs.community.bestOfCog.robloxUsers.fetchRobloxUser",
            AsyncMock(return_value=RoverLookupResult(30, "RobloxName")),
        ) as lookupMock:
            names = await cog._resolveCandidateDisplayNames(guild, {20: member})

        self.assertEqual(names, {20: "RobloxName"})
        lookupMock.assert_awaited_once_with(20, guildId=10)

    async def test_disabled_lookup_uses_clean_discord_name(self) -> None:
        cog = BestOfCog(SimpleNamespace())
        guild = SimpleNamespace(id=10)
        member = SimpleNamespace(id=20, display_name="[MR] Discord Name", name="Discord Name")

        with (
            patch("cogs.community.bestOfCog.config.bestOfRobloxLookupEnabled", False, create=True),
            patch(
                "cogs.community.bestOfCog.robloxUsers.fetchRobloxUser",
                AsyncMock(),
            ) as lookupMock,
        ):
            names = await cog._resolveCandidateDisplayNames(guild, {20: member})

        self.assertEqual(names, {20: "Discord Name"})
        lookupMock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
