from __future__ import annotations

import logging
from typing import Any

import discord

from . import taskBudgeter

log = logging.getLogger(__name__)


async def _getOwnedWebhook(
    *,
    botClient: discord.Client,
    channel: discord.abc.Messageable | discord.abc.GuildChannel,
    webhookName: str,
    reason: str,
) -> tuple[discord.Webhook | None, discord.Thread | None]:
    if not getattr(botClient, "user", None):
        return None, None

    thread = channel if isinstance(channel, discord.Thread) else None
    webhookHostChannel = channel.parent if isinstance(channel, discord.Thread) else channel
    if not isinstance(webhookHostChannel, discord.TextChannel):
        return None, thread

    me = webhookHostChannel.guild.me
    if me is None or not webhookHostChannel.permissions_for(me).manage_webhooks:
        return None, thread

    webhooks = await taskBudgeter.runDiscord(lambda: webhookHostChannel.webhooks())
    webhook = next(
        (
            hook
            for hook in webhooks
            if hook.user and hook.user.id == botClient.user.id and str(hook.name or "") == webhookName
        ),
        None,
    )
    if webhook is None:
        webhook = await taskBudgeter.runDiscord(
            lambda: webhookHostChannel.create_webhook(
                name=webhookName[:80],
                reason=reason,
            )
        )
    return webhook, thread


async def sendOwnedWebhookMessage(
    *,
    botClient: discord.Client,
    channel: discord.abc.Messageable | discord.abc.GuildChannel,
    webhookName: str,
    embed: discord.Embed | None = None,
    content: str | None = None,
    view: discord.ui.View | None = None,
    username: str | None = None,
    avatarUrl: str | None = None,
    reason: str = "Jane webhook message",
) -> bool:
    try:
        webhook, thread = await _getOwnedWebhook(
            botClient=botClient,
            channel=channel,
            webhookName=webhookName,
            reason=reason,
        )
        if webhook is None:
            return False

        kwargs: dict[str, Any] = {
            "wait": True,
            "allowed_mentions": discord.AllowedMentions(users=False, roles=False, everyone=False),
        }
        if content:
            kwargs["content"] = content
        if embed is not None:
            kwargs["embed"] = embed
        if view is not None:
            kwargs["view"] = view
        if username:
            kwargs["username"] = username[:80]
        if avatarUrl:
            kwargs["avatar_url"] = avatarUrl
        if thread is not None:
            kwargs["thread"] = thread

        sentMessage = await taskBudgeter.runDiscord(lambda: webhook.send(**kwargs))
        if view is not None and hasattr(botClient, "add_view"):
            try:
                messageId = int(getattr(sentMessage, "id", 0) or 0)
                if messageId > 0:
                    botClient.add_view(view, message_id=messageId)
            except Exception:
                pass
        return True
    except Exception:
        log.exception("Failed to send owned webhook message for %s.", webhookName)
        return False


async def editOwnedWebhookMessage(
    *,
    botClient: discord.Client,
    message: discord.Message,
    webhookName: str,
    embed: discord.Embed | None = None,
    content: str | None = None,
    view: discord.ui.View | None = None,
    reason: str = "Jane webhook message edit",
) -> bool:
    try:
        webhook, thread = await _getOwnedWebhook(
            botClient=botClient,
            channel=message.channel,
            webhookName=webhookName,
            reason=reason,
        )
        if webhook is None:
            return False

        kwargs: dict[str, Any] = {
            "allowed_mentions": discord.AllowedMentions(users=False, roles=False, everyone=False),
        }
        if content is not None:
            kwargs["content"] = content
        if embed is not None:
            kwargs["embed"] = embed
        if view is not None:
            kwargs["view"] = view
        if thread is not None:
            kwargs["thread"] = thread

        editedMessage = await taskBudgeter.runDiscord(
            lambda: webhook.edit_message(int(message.id), **kwargs)
        )
        if view is not None and hasattr(botClient, "add_view"):
            try:
                messageId = int(getattr(editedMessage, "id", 0) or 0)
                if messageId > 0:
                    botClient.add_view(view, message_id=messageId)
            except Exception:
                pass
        return True
    except Exception:
        log.exception("Failed to edit owned webhook message for %s.", webhookName)
        return False
