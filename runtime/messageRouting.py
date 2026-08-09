from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from typing import Any

import discord

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MessageRoutingMessages:
    runtimePaused: str
    serverNotRecognized: str
    organizationFeatureUnavailable: str
    temporaryLock: str


class HumanMessageRouter:
    """Route non-bot messages while preserving Jane's legacy command order."""

    def __init__(
        self,
        *,
        botClient,
        configModule,
        pauseController,
        orgFeatureGateModule,
        sillyCommandsModule,
        textCommandRouterProvider: Callable[[], Any],
        trainingStatsHandler: Callable[[discord.Message], Awaitable[bool]],
        hasCohostPermission: Callable[[discord.Member], bool],
        isCommandExecutionAllowed: Callable[[int], bool],
        isGuildAllowedForCommands: Callable[[int], bool],
        mirrorUnapprovedGuildCommandAttempt: Callable[..., Awaitable[None]],
        manualTextCommandTokens: Collection[str],
        lockedPrefixCommandTokens: Collection[str],
        messages: MessageRoutingMessages,
    ) -> None:
        self.botClient = botClient
        self.config = configModule
        self.pauseController = pauseController
        self.orgFeatureGate = orgFeatureGateModule
        self.sillyCommands = sillyCommandsModule
        self.textCommandRouterProvider = textCommandRouterProvider
        self.trainingStatsHandler = trainingStatsHandler
        self.hasCohostPermission = hasCohostPermission
        self.isCommandExecutionAllowed = isCommandExecutionAllowed
        self.isGuildAllowedForCommands = isGuildAllowedForCommands
        self.mirrorUnapprovedGuildCommandAttempt = mirrorUnapprovedGuildCommandAttempt
        self.manualTextCommandTokens = frozenset(manualTextCommandTokens)
        self.lockedPrefixCommandTokens = frozenset(lockedPrefixCommandTokens)
        self.messages = messages
        self._backgroundTasks: set[asyncio.Task] = set()

    @staticmethod
    def _guildId(message: discord.Message) -> int:
        return int(getattr(getattr(message, "guild", None), "id", 0) or 0)

    async def _sendQuietly(self, message: discord.Message, content: str) -> None:
        try:
            await message.channel.send(content)
        except Exception:
            pass

    async def _passesOrganizationGate(
        self,
        message: discord.Message,
        guildId: int,
        token: str,
    ) -> bool:
        enabled, featureKey = self.orgFeatureGate.isTokenEnabledForGuild(
            self.config,
            guildId,
            token,
        )
        if enabled:
            return True
        await self._sendQuietly(
            message,
            f"{self.messages.organizationFeatureUnavailable} (`{featureKey}`)",
        )
        return False

    def _startBackgroundTask(self, awaitable: Awaitable[None], *, name: str) -> None:
        task = asyncio.create_task(awaitable, name=name)
        self._backgroundTasks.add(task)

        def _done(doneTask: asyncio.Task) -> None:
            self._backgroundTasks.discard(doneTask)
            if doneTask.cancelled():
                return
            try:
                doneTask.result()
            except Exception:
                log.exception("Human message routing background task failed: %s", name)

        task.add_done_callback(_done)

    async def _handlePaused(
        self,
        message: discord.Message,
        *,
        token: str,
        guildId: int,
        textRouter,
    ) -> None:
        if not await self._passesOrganizationGate(message, guildId, token):
            return

        pausedHandlers = {
            "!allowserver": textRouter.handleAllowServer,
            "!copyserver": textRouter.handleCopyServer,
            "!shutdown": textRouter.handleShutdown,
            "!mirrortraininghistory": textRouter.handleMirrorTrainingHistory,
            "!janeterminal": textRouter.handleJaneTerminal,
        }
        handler = pausedHandlers.get(token)
        if handler is not None and await handler(message):
            return

        if token in self.manualTextCommandTokens or token in self.lockedPrefixCommandTokens:
            await self._sendQuietly(message, self.messages.runtimePaused)
            return

        context = await self.botClient.get_context(message)
        if context.command is not None:
            await self._sendQuietly(message, self.messages.runtimePaused)

    async def _rejectUnapprovedGuild(
        self,
        message: discord.Message,
        *,
        token: str,
        guildId: int,
    ) -> None:
        if guildId > 0:
            self._startBackgroundTask(
                self.mirrorUnapprovedGuildCommandAttempt(
                    commandName=token,
                    userLabel=str(message.author),
                    userId=int(message.author.id),
                    guildName=str(getattr(message.guild, "name", "Unknown Server")),
                    guildId=guildId,
                ),
                name=f"unapproved-guild-command-{guildId}-{int(message.author.id)}",
            )
        await self._sendQuietly(message, self.messages.serverNotRecognized)

    async def _handleActive(
        self,
        message: discord.Message,
        *,
        token: str,
        guildId: int,
        textRouter,
    ) -> None:
        if not await self._passesOrganizationGate(message, guildId, token):
            return
        if await textRouter.handleAllowServer(message):
            return
        if await textRouter.handleMirrorTrainingHistory(message):
            return

        if token in self.manualTextCommandTokens and not self.isGuildAllowedForCommands(guildId):
            await self._rejectUnapprovedGuild(message, token=token, guildId=guildId)
            return

        await self.sillyCommands.maybeHandleSillyMentions(message, self.botClient)
        if await textRouter.handleJaneHelp(message):
            return
        if await textRouter.handleViewAllChannels(message):
            return

        if not self.isCommandExecutionAllowed(int(message.author.id)):
            if token in self.lockedPrefixCommandTokens:
                await message.channel.send(self.messages.temporaryLock)
                return
            await self.botClient.process_commands(message)
            return

        if await self.sillyCommands.maybeHandleSixtySevenSpam(message):
            return
        if await self.sillyCommands.handleSkinCommand(
            message,
            self.botClient,
            hasSkinPermission=self.hasCohostPermission,
        ):
            return
        if await self.sillyCommands.handleKillCommand(message, self.botClient):
            return
        if await self.sillyCommands.handleCasinoToggleCommand(message):
            return

        orderedHandlers = (
            textRouter.handleUsernameToUserId,
            textRouter.handleChannelPurge,
            textRouter.handlePairDbNamesCommand,
            self.trainingStatsHandler,
            textRouter.handleJaneTerminal,
            textRouter.handleShutdown,
            textRouter.handleCopyServer,
            textRouter.handleJaneRuntime,
            textRouter.handleBgLeaderboardCommand,
            textRouter.handleJaneFlagSync,
            textRouter.handlePermissionSimulatorCommand,
        )
        for handler in orderedHandlers:
            if await handler(message):
                return
        await self.botClient.process_commands(message)

    async def handle(self, message: discord.Message) -> None:
        textRouter = self.textCommandRouterProvider()
        textRouter.noteCopyServerWarningMessage(message)
        await textRouter.handlePotatoGreeting(message)
        if await textRouter.handleJaneSecrets(message):
            return

        token = textRouter.firstLowerToken(message.content or "")
        guildId = self._guildId(message)
        if self.pauseController.isPaused():
            await self._handlePaused(
                message,
                token=token,
                guildId=guildId,
                textRouter=textRouter,
            )
            return
        await self._handleActive(
            message,
            token=token,
            guildId=guildId,
            textRouter=textRouter,
        )

    async def stop(self) -> None:
        tasks = set(self._backgroundTasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._backgroundTasks.clear()
