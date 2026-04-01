from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

import discord
from discord.ext import commands

import config
from silly import hallService

log = logging.getLogger(__name__)

_starEmoji = "\N{WHITE MEDIUM STAR}"
_skullEmoji = "\N{SKULL}"


def _reactionThreshold() -> int:
    return max(1, int(getattr(config, "hallReactionThreshold", 5) or 5))


def _hallTitleFromType(hallType: str) -> str:
    return "Hall of Fame" if str(hallType or "").upper() == "FAME" else "Hall of Shame"


def _allowedCategoryIds() -> set[int]:
    raw = getattr(config, "hallAllowedCategoryIds", [])
    if not isinstance(raw, (list, tuple, set)):
        return set()
    out: set[int] = set()
    for value in raw:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            out.add(parsed)
    return out


def _categoryIdForSourceChannel(channel: object) -> int:
    if isinstance(channel, discord.Thread):
        parent = channel.parent
        if isinstance(parent, discord.TextChannel):
            return int(parent.category_id or 0)
        if isinstance(parent, discord.ForumChannel):
            return int(parent.category_id or 0)
        return 0
    if isinstance(channel, discord.TextChannel):
        return int(channel.category_id or 0)
    return 0


def _toHallRoute(emoji: discord.PartialEmoji) -> Optional[tuple[str, int, str, str]]:
    emojiName = str(emoji.name or "").strip()
    emojiNameLower = emojiName.lower()

    if emoji.id is None:
        if emojiName == _starEmoji:
            return (
                "FAME",
                int(getattr(config, "hallOfFameChannelId", 0) or 0),
                _starEmoji,
                "Hall of Fame",
            )
        if emojiName == _skullEmoji:
            return (
                "SHAME",
                int(getattr(config, "hallOfShameChannelId", 0) or 0),
                _skullEmoji,
                "Hall of Shame",
            )

    if emojiNameLower == "star":
        return (
            "FAME",
            int(getattr(config, "hallOfFameChannelId", 0) or 0),
            _starEmoji,
            "Hall of Fame",
        )
    if emojiNameLower == "skull":
        return (
            "SHAME",
            int(getattr(config, "hallOfShameChannelId", 0) or 0),
            _skullEmoji,
            "Hall of Shame",
        )
    return None


def _isImageAttachment(attachment: discord.Attachment) -> bool:
    contentType = str(attachment.content_type or "").lower()
    if contentType.startswith("image/"):
        return True
    suffix = Path(str(attachment.filename or "")).suffix.lower()
    return suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}


def _firstImageUrl(message: discord.Message) -> Optional[str]:
    for attachment in message.attachments:
        if _isImageAttachment(attachment):
            return str(attachment.url or attachment.proxy_url or "")
    for embed in message.embeds:
        if embed.image and embed.image.url:
            return str(embed.image.url)
    return None


def _safeDisplayName(author: discord.abc.User) -> str:
    displayName = getattr(author, "display_name", None)
    if isinstance(displayName, str) and displayName.strip():
        return displayName.strip()
    name = getattr(author, "name", None)
    if isinstance(name, str) and name.strip():
        return name.strip()
    return f"user-{int(author.id)}"


def _reactionCount(message: discord.Message, emoji: discord.PartialEmoji) -> int:
    targetKey = str(emoji)
    for reaction in message.reactions:
        if str(reaction.emoji) == targetKey:
            try:
                return int(reaction.count or 0)
            except (TypeError, ValueError):
                return 0
    return 0


def _hallHeaderLine(
    *,
    reactionEmoji: str,
    reactionCount: int,
    sourceChannelId: int,
) -> str:
    safeCount = max(0, int(reactionCount or 0))
    if int(sourceChannelId or 0) > 0:
        return f"{reactionEmoji} **{safeCount}** | <#{int(sourceChannelId)}>"
    return f"{reactionEmoji} **{safeCount}** | #unknown"


def _buildHallEmbed(
    *,
    hallType: str,
    sourceMessage: discord.Message,
) -> discord.Embed:
    isFame = hallType == "FAME"
    color = discord.Color.gold() if isFame else discord.Color.red()
    authorName = _safeDisplayName(sourceMessage.author)
    authorIconUrl = sourceMessage.author.display_avatar.url

    rawText = str(sourceMessage.content or "").strip()
    if len(rawText) > 3300:
        rawText = f"{rawText[:3297]}..."

    descriptionParts: list[str] = []
    if rawText:
        descriptionParts.append(rawText)
    descriptionParts.append(f"[Click to jump to message!]({sourceMessage.jump_url})")

    embed = discord.Embed(
        description="\n\n".join(descriptionParts),
        color=color,
        timestamp=sourceMessage.created_at,
    )
    embed.set_author(name=authorName, icon_url=authorIconUrl)

    imageUrl = _firstImageUrl(sourceMessage)
    if imageUrl:
        embed.set_image(url=imageUrl)
    return embed


class HallCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._dispatchLocks: dict[tuple[int, str], asyncio.Lock] = {}

    async def _resolveGuildChannel(
        self,
        guild: discord.Guild,
        channelId: int,
    ) -> Optional[object]:
        channel = guild.get_channel(int(channelId))
        if channel is None:
            channel = guild.get_thread(int(channelId))
        if channel is not None:
            return channel
        try:
            return await guild.fetch_channel(int(channelId))
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return None

    async def _sendHallPost(
        self,
        *,
        hallTitle: str,
        targetChannel: object,
        sourceChannelId: int,
        reactionEmoji: str,
        reactionCount: int,
        embed: discord.Embed,
    ) -> Optional[int]:
        headerLine = _hallHeaderLine(
            reactionEmoji=reactionEmoji,
            reactionCount=reactionCount,
            sourceChannelId=sourceChannelId,
        )

        useWebhook = bool(getattr(config, "hallUseWebhook", True))
        if useWebhook and isinstance(targetChannel, discord.TextChannel):
            me = targetChannel.guild.me
            if me and targetChannel.permissions_for(me).manage_webhooks:
                try:
                    webhooks = await targetChannel.webhooks()
                    webhook = next(
                        (
                            hook
                            for hook in webhooks
                            if hook.user and self.bot.user and int(hook.user.id) == int(self.bot.user.id) and hook.name == hallTitle
                        ),
                        None,
                    )
                    if webhook is None:
                        webhook = await targetChannel.create_webhook(
                            name=hallTitle,
                            reason="Hall repost output",
                        )
                    sent = await webhook.send(
                        content=headerLine,
                        embed=embed,
                        username=hallTitle,
                        avatar_url=self.bot.user.display_avatar.url if self.bot.user else discord.utils.MISSING,
                        wait=True,
                    )
                    return int(sent.id)
                except Exception:
                    log.exception("Hall repost webhook send failed for %s.", hallTitle)

        try:
            sent = await targetChannel.send(content=headerLine, embed=embed)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return None
        return int(sent.id)

    async def _findOwnedHallWebhook(
        self,
        targetChannel: object,
        *,
        hallTitle: str,
    ) -> Optional[discord.Webhook]:
        if not isinstance(targetChannel, discord.TextChannel):
            return None
        me = targetChannel.guild.me
        if me is None or not targetChannel.permissions_for(me).manage_webhooks:
            return None
        try:
            webhooks = await targetChannel.webhooks()
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return None
        return next(
            (
                hook
                for hook in webhooks
                if hook.user and self.bot.user and int(hook.user.id) == int(self.bot.user.id) and hook.name == hallTitle and bool(hook.token)
            ),
            None,
        )

    async def _updateExistingHallPostCount(
        self,
        *,
        guild: discord.Guild,
        existing: dict,
        hallType: str,
        reactionEmoji: str,
        reactionCount: int,
        sourceChannelId: int,
    ) -> bool:
        targetChannelId = int(existing.get("targetChannelId") or 0)
        postedMessageId = int(existing.get("postedMessageId") or 0)
        if targetChannelId <= 0 or postedMessageId <= 0:
            return False

        targetChannel = await self._resolveGuildChannel(guild, targetChannelId)
        if not isinstance(targetChannel, (discord.TextChannel, discord.Thread)):
            return False

        try:
            postedMessage = await targetChannel.fetch_message(postedMessageId)
        except discord.NotFound:
            return False
        except (discord.Forbidden, discord.HTTPException):
            return True

        headerLine = _hallHeaderLine(
            reactionEmoji=reactionEmoji,
            reactionCount=reactionCount,
            sourceChannelId=sourceChannelId,
        )
        try:
            await postedMessage.edit(content=headerLine)
        except discord.NotFound:
            return False
        except (discord.Forbidden, discord.HTTPException):
            hallTitle = _hallTitleFromType(hallType)
            webhook = await self._findOwnedHallWebhook(targetChannel, hallTitle=hallTitle)
            if webhook is None:
                # Keep the record; we just couldn't edit right now.
                return True
            try:
                await webhook.edit_message(int(postedMessageId), content=headerLine)
            except discord.NotFound:
                return False
            except (discord.Forbidden, discord.HTTPException):
                return True
        return True

    async def _handleHallReactionEvent(
        self,
        payload: discord.RawReactionActionEvent,
        *,
        allowCreate: bool,
    ) -> None:
        if payload.guild_id is None:
            return
        if self.bot.user and int(payload.user_id) == int(self.bot.user.id):
            return

        route = _toHallRoute(payload.emoji)
        if route is None:
            return
        hallType, targetChannelId, reactionEmoji, hallTitle = route
        if targetChannelId <= 0:
            return

        guild = self.bot.get_guild(int(payload.guild_id))
        if guild is None:
            try:
                guild = await self.bot.fetch_guild(int(payload.guild_id))
            except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                return

        sourceChannel = await self._resolveGuildChannel(guild, int(payload.channel_id))
        if sourceChannel is None:
            return
        if int(getattr(sourceChannel, "id", 0)) == int(targetChannelId):
            return
        allowedCategoryIds = _allowedCategoryIds()
        if allowedCategoryIds:
            sourceCategoryId = _categoryIdForSourceChannel(sourceChannel)
            if sourceCategoryId not in allowedCategoryIds:
                return

        if not isinstance(sourceChannel, (discord.TextChannel, discord.Thread)):
            return
        try:
            sourceMessage = await sourceChannel.fetch_message(int(payload.message_id))
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return

        if bool(getattr(config, "hallIgnoreBotMessages", True)) and sourceMessage.author.bot:
            return

        count = _reactionCount(sourceMessage, payload.emoji)

        lockKey = (int(sourceMessage.id), hallType)
        dispatchLock = self._dispatchLocks.setdefault(lockKey, asyncio.Lock())
        try:
            async with dispatchLock:
                existing = await hallService.getHallPost(int(sourceMessage.id), hallType)
                if existing is None:
                    if not allowCreate or count < _reactionThreshold():
                        return
                    targetChannel = await self._resolveGuildChannel(guild, int(targetChannelId))
                    if targetChannel is None:
                        return
                    if int(getattr(targetChannel, "id", 0)) == int(getattr(sourceChannel, "id", 0)):
                        return

                    embed = _buildHallEmbed(hallType=hallType, sourceMessage=sourceMessage)
                    postedMessageId = await self._sendHallPost(
                        hallTitle=hallTitle,
                        targetChannel=targetChannel,
                        sourceChannelId=int(getattr(sourceChannel, "id", 0)),
                        reactionEmoji=reactionEmoji,
                        reactionCount=count,
                        embed=embed,
                    )
                    if not postedMessageId:
                        return
                    await hallService.createHallPost(
                        messageId=int(sourceMessage.id),
                        hallType=hallType,
                        guildId=int(guild.id),
                        sourceChannelId=int(getattr(sourceChannel, "id", 0)),
                        targetChannelId=int(getattr(targetChannel, "id", 0)),
                        sourceAuthorId=int(sourceMessage.author.id),
                        reactionEmoji=reactionEmoji,
                        reactionCount=int(count),
                        postedMessageId=int(postedMessageId),
                    )
                    return

                try:
                    previousCount = int(existing.get("reactionCount") or 0)
                except (TypeError, ValueError):
                    previousCount = -1
                if previousCount == int(count):
                    return

                updated = await self._updateExistingHallPostCount(
                    guild=guild,
                    existing=existing,
                    hallType=hallType,
                    reactionEmoji=reactionEmoji,
                    reactionCount=int(count),
                    sourceChannelId=int(getattr(sourceChannel, "id", 0)),
                )
                if not updated:
                    await hallService.deleteHallPost(int(sourceMessage.id), hallType)
                    if allowCreate and count >= _reactionThreshold():
                        targetChannel = await self._resolveGuildChannel(guild, int(targetChannelId))
                        if targetChannel is None:
                            return
                        if int(getattr(targetChannel, "id", 0)) == int(getattr(sourceChannel, "id", 0)):
                            return
                        embed = _buildHallEmbed(hallType=hallType, sourceMessage=sourceMessage)
                        postedMessageId = await self._sendHallPost(
                            hallTitle=hallTitle,
                            targetChannel=targetChannel,
                            sourceChannelId=int(getattr(sourceChannel, "id", 0)),
                            reactionEmoji=reactionEmoji,
                            reactionCount=count,
                            embed=embed,
                        )
                        if not postedMessageId:
                            return
                        await hallService.createHallPost(
                            messageId=int(sourceMessage.id),
                            hallType=hallType,
                            guildId=int(guild.id),
                            sourceChannelId=int(getattr(sourceChannel, "id", 0)),
                            targetChannelId=int(getattr(targetChannel, "id", 0)),
                            sourceAuthorId=int(sourceMessage.author.id),
                            reactionEmoji=reactionEmoji,
                            reactionCount=int(count),
                            postedMessageId=int(postedMessageId),
                        )
                    return

                await hallService.updateHallPostCount(
                    messageId=int(sourceMessage.id),
                    hallType=hallType,
                    reactionCount=int(count),
                )
        finally:
            self._dispatchLocks.pop(lockKey, None)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent) -> None:
        await self._handleHallReactionEvent(payload, allowCreate=True)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent) -> None:
        await self._handleHallReactionEvent(payload, allowCreate=False)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(HallCog(bot))
