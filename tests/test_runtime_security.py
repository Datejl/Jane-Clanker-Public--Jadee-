from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from runtime.bootstrap import BootstrapCoordinator
from runtime import entrypoint


def _bootstrap(config: object) -> BootstrapCoordinator:
    return BootstrapCoordinator(
        botClient=SimpleNamespace(),
        configModule=config,
        initDbFn=AsyncMock(),
        loadMultiRegistryFn=lambda: {},
        sessionViews=SimpleNamespace(),
        maintenanceCoordinator=SimpleNamespace(),
        taskBudgeter=SimpleNamespace(),
        recruitmentService=SimpleNamespace(),
        helpCommandsModule=SimpleNamespace(),
        pluginRegistry=SimpleNamespace(),
        extensionNames=[],
    )


class UnknownGuildInviteTests(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_guild_invite_is_off_by_default(self) -> None:
        channel = SimpleNamespace(
            permissions_for=lambda _member: SimpleNamespace(create_instant_invite=True),
            create_invite=AsyncMock(return_value=SimpleNamespace(url="https://example.invalid")),
        )
        guild = SimpleNamespace(id=1, me=SimpleNamespace(), text_channels=[channel])

        inviteUrl = await _bootstrap(SimpleNamespace())._createUnknownGuildInvite(guild)

        self.assertEqual(inviteUrl, "")
        channel.create_invite.assert_not_awaited()

    async def test_opted_in_invite_is_bounded(self) -> None:
        channel = SimpleNamespace(
            id=2,
            permissions_for=lambda _member: SimpleNamespace(create_instant_invite=True),
            create_invite=AsyncMock(return_value=SimpleNamespace(url="https://example.invalid")),
        )
        guild = SimpleNamespace(id=1, me=SimpleNamespace(), text_channels=[channel])
        config = SimpleNamespace(
            unknownGuildInviteCreationEnabled=True,
            unknownGuildInviteMaxAgeSec=-1,
            unknownGuildInviteMaxUses=999,
        )

        inviteUrl = await _bootstrap(config)._createUnknownGuildInvite(guild)

        self.assertEqual(inviteUrl, "https://example.invalid")
        channel.create_invite.assert_awaited_once_with(
            max_age=60,
            max_uses=10,
            unique=True,
            reason="Jane unknown-guild diagnostics (explicitly enabled)",
        )


class RuntimeApiLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_raw_sql_api_is_not_registered(self) -> None:
        paths = {str(rule) for rule in entrypoint.app.url_map.iter_rules()}
        self.assertNotIn("/SQL", paths)
        self.assertIn("/enterOrientation", paths)

    async def test_disabled_api_only_runs_discord(self) -> None:
        botClient = SimpleNamespace()
        config = SimpleNamespace(orientationApiEnabled=False)
        with (
            patch.object(entrypoint, "runBotWithStartupRetry", AsyncMock()) as botMock,
            patch.object(entrypoint, "run_api", AsyncMock()) as apiMock,
        ):
            await entrypoint.runRuntimeServices(botClient, "discord-token", configModule=config)

        botMock.assert_awaited_once_with(botClient, "discord-token", configModule=config)
        apiMock.assert_not_awaited()

    async def test_enabled_api_requires_token_before_discord_starts(self) -> None:
        botClient = SimpleNamespace()
        config = SimpleNamespace(orientationApiEnabled=True, orientationApiToken="")
        with patch.object(entrypoint, "runBotWithStartupRetry", AsyncMock()) as botMock:
            with self.assertRaisesRegex(RuntimeError, "ORIENTATION_API_TOKEN"):
                await entrypoint.runRuntimeServices(
                    botClient,
                    "discord-token",
                    configModule=config,
                )

        botMock.assert_not_awaited()

    async def test_api_stops_after_discord_stops(self) -> None:
        botClient = SimpleNamespace()
        config = SimpleNamespace(
            orientationApiEnabled=True,
            orientationApiToken="test",
            orientationApiHost="127.0.0.1",
            orientationApiPort=24003,
        )
        apiStarted = asyncio.Event()

        async def fakeApi(*, host: str, port: int) -> None:
            self.assertEqual((host, port), ("127.0.0.1", 24003))
            apiStarted.set()
            await entrypoint.shutdown_api_event.wait()

        async def fakeBot(*_args, **_kwargs) -> None:
            await apiStarted.wait()

        with (
            patch.object(entrypoint, "runBotWithStartupRetry", side_effect=fakeBot),
            patch.object(entrypoint, "run_api", side_effect=fakeApi),
        ):
            await entrypoint.runRuntimeServices(botClient, "discord-token", configModule=config)

        self.assertTrue(entrypoint.shutdown_api_event.is_set())


if __name__ == "__main__":
    unittest.main()
