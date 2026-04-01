from __future__ import annotations

import logging
from typing import Any

import discord

from . import taskBudgeter

log = logging.getLogger(__name__)


async def sendOwnedWebhookMessage(
    *,
    botClient: discord.Client,
    channel: discord.abc.Messageable | discord.abc.GuildChannel,
    webhookName: str,
    embed: discord.Embed | None = None,
    content: str | None = None,
    username: str | None = None,
    avatarUrl: str | None = None,
    reason: str = "Jane webhook message",
) -> bool:
    if not getattr(botClient, "user", None):
        return False

    thread = channel if isinstance(channel, discord.Thread) else None
    webhookHostChannel = channel.parent if isinstance(channel, discord.Thread) else channel
    if not isinstance(webhookHostChannel, discord.TextChannel):
        return False

    me = webhookHostChannel.guild.me
    if me is None or not webhookHostChannel.permissions_for(me).manage_webhooks:
        return False

    try:
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

        kwargs: dict[str, Any] = {
            "wait": True,
            "allowed_mentions": discord.AllowedMentions(users=False, roles=False, everyone=False),
        }
        if content:
            kwargs["content"] = content
        if embed is not None:
            kwargs["embed"] = embed
        if username:
            kwargs["username"] = username[:80]
        if avatarUrl:
            kwargs["avatar_url"] = avatarUrl
        if thread is not None:
            kwargs["thread"] = thread

        await taskBudgeter.runDiscord(lambda: webhook.send(**kwargs))
        return True
    except Exception:
        log.exception("Failed to send owned webhook message for %s.", webhookName)
        return False
