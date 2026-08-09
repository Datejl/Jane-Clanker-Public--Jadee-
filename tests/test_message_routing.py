from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from runtime.messageRouting import HumanMessageRouter, MessageRoutingMessages


def _message(content: str = "hello") -> SimpleNamespace:
    return SimpleNamespace(
        content=content,
        author=SimpleNamespace(id=10, bot=False, __str__=lambda self: "Test User"),
        guild=SimpleNamespace(id=20, name="Test Guild"),
        channel=SimpleNamespace(send=AsyncMock()),
    )


def _asyncHandler(callOrder: list[str], name: str, result: bool = False) -> AsyncMock:
    async def _handler(*_args, **_kwargs) -> bool:
        callOrder.append(name)
        return result

    return AsyncMock(side_effect=_handler)


def _buildRouter(
    *,
    token: str = "hello",
    paused: bool = False,
    commandAllowed: bool = True,
    guildAllowed: bool = True,
    orgGate: tuple[bool, str] = (True, ""),
    handlerResults: dict[str, bool] | None = None,
) -> tuple[HumanMessageRouter, SimpleNamespace, SimpleNamespace, list[str]]:
    handlerResults = handlerResults or {}
    callOrder: list[str] = []
    handlerNames = (
        "secrets",
        "allow",
        "mirror-training",
        "help",
        "view-channels",
        "username",
        "purge",
        "pair-db",
        "terminal",
        "shutdown",
        "copy-server",
        "runtime",
        "leaderboard",
        "flag-sync",
        "permission-simulator",
    )
    handlers = {
        name: _asyncHandler(callOrder, name, handlerResults.get(name, False))
        for name in handlerNames
    }
    textRouter = SimpleNamespace(
        noteCopyServerWarningMessage=MagicMock(),
        handlePotatoGreeting=AsyncMock(return_value=False),
        firstLowerToken=MagicMock(return_value=token),
        handleJaneSecrets=handlers["secrets"],
        handleAllowServer=handlers["allow"],
        handleMirrorTrainingHistory=handlers["mirror-training"],
        handleJaneHelp=handlers["help"],
        handleViewAllChannels=handlers["view-channels"],
        handleUsernameToUserId=handlers["username"],
        handleChannelPurge=handlers["purge"],
        handlePairDbNamesCommand=handlers["pair-db"],
        handleJaneTerminal=handlers["terminal"],
        handleShutdown=handlers["shutdown"],
        handleCopyServer=handlers["copy-server"],
        handleJaneRuntime=handlers["runtime"],
        handleBgLeaderboardCommand=handlers["leaderboard"],
        handleJaneFlagSync=handlers["flag-sync"],
        handlePermissionSimulatorCommand=handlers["permission-simulator"],
    )
    sillyCommands = SimpleNamespace(
        maybeHandleSillyMentions=_asyncHandler(callOrder, "silly-mentions"),
        maybeHandleSixtySevenSpam=_asyncHandler(callOrder, "sixty-seven"),
        handleSkinCommand=_asyncHandler(callOrder, "skin"),
        handleKillCommand=_asyncHandler(callOrder, "kill"),
        handleCasinoToggleCommand=_asyncHandler(callOrder, "casino"),
    )

    async def _processCommands(_message) -> None:
        callOrder.append("process-commands")

    botClient = SimpleNamespace(
        process_commands=AsyncMock(side_effect=_processCommands),
        get_context=AsyncMock(return_value=SimpleNamespace(command=None)),
    )
    trainingStats = _asyncHandler(callOrder, "training-stats")
    mirrorAttempt = AsyncMock()
    router = HumanMessageRouter(
        botClient=botClient,
        configModule=SimpleNamespace(),
        pauseController=SimpleNamespace(isPaused=MagicMock(return_value=paused)),
        orgFeatureGateModule=SimpleNamespace(
            isTokenEnabledForGuild=MagicMock(return_value=orgGate)
        ),
        sillyCommandsModule=sillyCommands,
        textCommandRouterProvider=MagicMock(return_value=textRouter),
        trainingStatsHandler=trainingStats,
        hasCohostPermission=MagicMock(return_value=True),
        isCommandExecutionAllowed=MagicMock(return_value=commandAllowed),
        isGuildAllowedForCommands=MagicMock(return_value=guildAllowed),
        mirrorUnapprovedGuildCommandAttempt=mirrorAttempt,
        manualTextCommandTokens={"!copyserver", "!janeterminal"},
        lockedPrefixCommandTokens={"!kill", "!copyserver"},
        messages=MessageRoutingMessages(
            runtimePaused="paused",
            serverNotRecognized="unknown-server",
            organizationFeatureUnavailable="feature-disabled",
            temporaryLock="temporarily-locked",
        ),
    )
    return router, textRouter, botClient, callOrder


class HumanMessageRouterTests(unittest.IsolatedAsyncioTestCase):
    async def test_personal_greeting_is_checked_before_command_routing(self) -> None:
        router, textRouter, _botClient, _callOrder = _buildRouter()
        message = _message()

        await router.handle(message)

        textRouter.handlePotatoGreeting.assert_awaited_once_with(message)

    async def test_active_handler_order_is_preserved(self) -> None:
        router, textRouter, botClient, callOrder = _buildRouter()
        message = _message()

        await router.handle(message)

        textRouter.noteCopyServerWarningMessage.assert_called_once_with(message)
        self.assertEqual(
            callOrder,
            [
                "secrets",
                "allow",
                "mirror-training",
                "silly-mentions",
                "help",
                "view-channels",
                "sixty-seven",
                "skin",
                "kill",
                "casino",
                "username",
                "purge",
                "pair-db",
                "training-stats",
                "terminal",
                "shutdown",
                "copy-server",
                "runtime",
                "leaderboard",
                "flag-sync",
                "permission-simulator",
                "process-commands",
            ],
        )
        botClient.process_commands.assert_awaited_once_with(message)

    async def test_paused_manual_command_runs_allowed_handler_then_reports_pause(self) -> None:
        router, textRouter, botClient, callOrder = _buildRouter(
            token="!copyserver",
            paused=True,
        )
        message = _message("!copyserver")

        await router.handle(message)

        self.assertEqual(callOrder, ["secrets", "copy-server"])
        message.channel.send.assert_awaited_once_with("paused")
        botClient.process_commands.assert_not_awaited()
        textRouter.handleAllowServer.assert_not_awaited()

    async def test_unapproved_manual_command_is_mirrored_and_rejected(self) -> None:
        router, _textRouter, botClient, callOrder = _buildRouter(
            token="!janeterminal",
            guildAllowed=False,
        )
        message = _message("!janeterminal")

        await router.handle(message)
        if router._backgroundTasks:
            await asyncio.gather(*router._backgroundTasks)

        self.assertEqual(callOrder, ["secrets", "allow", "mirror-training"])
        router.mirrorUnapprovedGuildCommandAttempt.assert_awaited_once()
        message.channel.send.assert_awaited_once_with("unknown-server")
        botClient.process_commands.assert_not_awaited()

    async def test_restricted_user_can_still_reach_unlocked_prefix_commands(self) -> None:
        router, _textRouter, botClient, callOrder = _buildRouter(
            token="!ordinary",
            commandAllowed=False,
        )
        message = _message("!ordinary")

        await router.handle(message)

        self.assertEqual(
            callOrder,
            [
                "secrets",
                "allow",
                "mirror-training",
                "silly-mentions",
                "help",
                "view-channels",
                "process-commands",
            ],
        )
        botClient.process_commands.assert_awaited_once_with(message)

    async def test_organization_gate_stops_routing_with_feature_name(self) -> None:
        router, _textRouter, botClient, callOrder = _buildRouter(
            orgGate=(False, "recruitment"),
        )
        message = _message()

        await router.handle(message)

        self.assertEqual(callOrder, ["secrets"])
        message.channel.send.assert_awaited_once_with(
            "feature-disabled (`recruitment`)"
        )
        botClient.process_commands.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
