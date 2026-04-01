from __future__ import annotations

import logging
from typing import Any, Optional

import discord

from . import taskBudgeter

log = logging.getLogger(__name__)

_MISSING = object()
_retrySafeLayerInstalled = False


def isUnknownInteractionError(exc: Exception) -> bool:
    if isinstance(exc, discord.NotFound):
        return True
    if isinstance(exc, discord.HTTPException):
        if getattr(exc, "code", None) == 10062:
            return True
        text = str(exc).lower()
        return "unknown interaction" in text
    return False


async def safeInteractionDefer(
    interaction: discord.Interaction,
    *,
    ephemeral: bool = True,
    thinking: bool = False,
) -> bool:
    if interaction.response.is_done():
        return True
    try:
        await interaction.response.defer(ephemeral=ephemeral, thinking=thinking)
        return True
    except (discord.NotFound, discord.HTTPException):
        return False


async def safeInteractionReply(
    interaction: discord.Interaction,
    *,
    content: Optional[str] = None,
    embed: Optional[discord.Embed] = None,
    embeds: Optional[list[discord.Embed]] = None,
    view: Any = _MISSING,
    ephemeral: bool = True,
    allowedMentions: Any = _MISSING,
) -> bool:
    kwargs: dict[str, Any] = {"ephemeral": ephemeral}
    if content is not None:
        kwargs["content"] = content
    if embed is not None:
        kwargs["embed"] = embed
    if embeds is not None:
        kwargs["embeds"] = embeds
    if view is not _MISSING and isinstance(view, discord.ui.View):
        kwargs["view"] = view
    if allowedMentions is not _MISSING:
        kwargs["allowed_mentions"] = allowedMentions

    async def _dispatch() -> None:
        if interaction.response.is_done():
            await taskBudgeter.runDiscord(lambda: interaction.followup.send(**kwargs))
        else:
            await interaction.response.send_message(**kwargs)

    try:
        await _dispatch()
        return True
    except TypeError as exc:
        # Common case: unsupported kwargs for a specific response path.
        if "view" in kwargs:
            kwargs.pop("view", None)
            try:
                await _dispatch()
                return True
            except Exception:
                pass
        log.warning("Interaction reply type error: %s", exc)
        return False
    except (discord.NotFound, discord.HTTPException):
        return False


async def safeInteractionSendModal(
    interaction: discord.Interaction,
    modal: discord.ui.Modal,
    *,
    expiredMessage: str = "This interaction expired. Please run the command again.",
) -> bool:
    if interaction.response.is_done():
        await safeInteractionReply(
            interaction,
            content=expiredMessage,
            ephemeral=True,
        )
        return False
    try:
        await interaction.response.send_modal(modal)
        return True
    except (discord.NotFound, discord.HTTPException):
        return False


async def safeMessageEdit(message: discord.Message, **kwargs: Any) -> bool:
    try:
        await taskBudgeter.runDiscord(lambda: message.edit(**kwargs))
        return True
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return False


def installRetrySafeInteractionLayer() -> None:
    global _retrySafeLayerInstalled
    if _retrySafeLayerInstalled:
        return

    originalSendMessage = discord.InteractionResponse.send_message
    originalDefer = discord.InteractionResponse.defer
    originalSendModal = discord.InteractionResponse.send_modal

    async def _patchedSendMessage(self: discord.InteractionResponse, *args: Any, **kwargs: Any):
        try:
            return await taskBudgeter.runDiscord(lambda: originalSendMessage(self, *args, **kwargs))
        except (discord.NotFound, discord.HTTPException) as exc:
            if isUnknownInteractionError(exc):
                return None
            raise

    async def _patchedDefer(self: discord.InteractionResponse, *args: Any, **kwargs: Any):
        try:
            return await taskBudgeter.runDiscord(lambda: originalDefer(self, *args, **kwargs))
        except (discord.NotFound, discord.HTTPException) as exc:
            if isUnknownInteractionError(exc):
                return None
            raise

    async def _patchedSendModal(self: discord.InteractionResponse, *args: Any, **kwargs: Any):
        try:
            return await taskBudgeter.runDiscord(lambda: originalSendModal(self, *args, **kwargs))
        except (discord.NotFound, discord.HTTPException) as exc:
            if isUnknownInteractionError(exc):
                return None
            raise

    discord.InteractionResponse.send_message = _patchedSendMessage  # type: ignore[assignment]
    discord.InteractionResponse.defer = _patchedDefer  # type: ignore[assignment]
    discord.InteractionResponse.send_modal = _patchedSendModal  # type: ignore[assignment]

    _retrySafeLayerInstalled = True
