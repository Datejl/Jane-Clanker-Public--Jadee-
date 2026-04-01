from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)


class ErrorCoordinator:
    def __init__(
        self,
        *,
        botClient: Any,
        configModule: Any,
        taskBudgeter: Any,
        retryQueue: Any | None = None,
    ) -> None:
        self.botClient = botClient
        self.config = configModule
        self.taskBudgeter = taskBudgeter
        self.retryQueue = retryQueue

    def _errorMirrorUserId(self) -> int | None:
        configured = getattr(self.config, "errorMirrorUserId", 0)
        try:
            parsed = int(configured)
        except (TypeError, ValueError):
            parsed = 0
        if parsed > 0:
            return parsed

        fallbackIds = getattr(self.config, "temporaryCommandAllowedUserIds", []) or []
        for raw in fallbackIds:
            try:
                candidate = int(raw)
            except (TypeError, ValueError):
                continue
            if candidate > 0:
                return candidate
        return None

    @staticmethod
    def _truncateForDiscord(value: str, maxLen: int) -> str:
        if len(value) <= maxLen:
            return value
        if maxLen <= 3:
            return value[:maxLen]
        return value[: maxLen - 3] + "..."

    async def sendErrorMirrorDm(
        self,
        *,
        source: str,
        commandName: str,
        userId: object,
        guildId: object,
        error: Exception,
    ) -> None:
        targetUserId = self._errorMirrorUserId()
        if not targetUserId:
            return

        targetUser = self.botClient.get_user(targetUserId)
        if targetUser is None:
            try:
                targetUser = await self.taskBudgeter.runDiscord(lambda: self.botClient.fetch_user(targetUserId))
            except Exception:
                return
        if targetUser is None:
            return

        tracebackText = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        tracebackText = self._truncateForDiscord(tracebackText.strip() or repr(error), 3400)

        embed = discord.Embed(
            title="Jane Error Mirror",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
            description=f"```py\n{tracebackText}\n```",
        )
        embed.add_field(name="Source", value=source or "unknown", inline=True)
        embed.add_field(name="Command", value=(commandName or "unknown"), inline=True)
        embed.add_field(name="User ID", value=str(userId), inline=True)
        embed.add_field(name="Guild ID", value=str(guildId), inline=True)
        embed.set_footer(text="Mirrored from terminal exception log")

        try:
            await self.taskBudgeter.runDiscord(
                lambda: targetUser.send(
                    content="================ Jane Error Log ================",
                    embed=embed,
                )
            )
        except Exception:
            if self.retryQueue is not None:
                try:
                    await self.retryQueue.enqueue(
                        jobType="error-mirror-dm",
                        payload={
                            "targetUserId": int(targetUserId),
                            "content": "================ Jane Error Log ================",
                            "title": str(embed.title or ""),
                            "description": str(embed.description or ""),
                        },
                        maxAttempts=6,
                        initialDelaySec=10,
                        source="error-coordinator",
                    )
                except Exception:
                    pass
            return

    async def handleAppCommandError(
        self,
        *,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
        safeInteractionSend: Callable[[discord.Interaction, str], Awaitable[None]],
    ) -> None:
        underlying = error.original if isinstance(error, app_commands.CommandInvokeError) else error

        if isinstance(underlying, app_commands.CommandOnCooldown):
            return await safeInteractionSend(
                interaction,
                f"That command is on cooldown. Try again in {underlying.retry_after:.1f}s.",
            )
        if isinstance(underlying, app_commands.MissingPermissions):
            return await safeInteractionSend(
                interaction,
                "You do not have permission to use that command.",
            )
        if isinstance(underlying, app_commands.CheckFailure):
            return await safeInteractionSend(
                interaction,
                "You are not allowed to use that command in this context.",
            )
        if isinstance(underlying, app_commands.TransformerError):
            return await safeInteractionSend(
                interaction,
                "One or more inputs are invalid. Please review your command fields and try again.",
            )

        commandName = ""
        if isinstance(interaction.data, dict):
            commandName = str(interaction.data.get("name") or "")
        log.exception(
            "Unhandled app command error (command=%s, userId=%s, guildId=%s).",
            commandName or "unknown",
            getattr(interaction.user, "id", "unknown"),
            getattr(interaction.guild, "id", "dm"),
        )
        await self.sendErrorMirrorDm(
            source="app-command",
            commandName=commandName or "unknown",
            userId=getattr(interaction.user, "id", "unknown"),
            guildId=getattr(interaction.guild, "id", "dm"),
            error=underlying if isinstance(underlying, Exception) else Exception(str(underlying)),
        )
        await safeInteractionSend(
            interaction,
            "That command failed due to an internal error. Please try again in a moment.",
        )

    async def handlePrefixCommandError(
        self,
        *,
        ctx: commands.Context,
        error: commands.CommandError,
    ) -> None:
        if isinstance(error, commands.CommandNotFound):
            return
        log.exception(
            "Unhandled prefix command error (command=%s, userId=%s, guildId=%s).",
            getattr(getattr(ctx, "command", None), "qualified_name", "unknown"),
            getattr(getattr(ctx, "author", None), "id", "unknown"),
            getattr(getattr(ctx, "guild", None), "id", "dm"),
        )
        await self.sendErrorMirrorDm(
            source="prefix-command",
            commandName=str(getattr(getattr(ctx, "command", None), "qualified_name", "unknown")),
            userId=getattr(getattr(ctx, "author", None), "id", "unknown"),
            guildId=getattr(getattr(ctx, "guild", None), "id", "dm"),
            error=error if isinstance(error, Exception) else Exception(str(error)),
        )
