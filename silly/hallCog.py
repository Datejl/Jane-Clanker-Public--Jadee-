from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

import discord
from discord.ext import commands

import config
from runtime import webhooks as runtimeWebhooks
from silly import hallService

log = logging.getLogger(__name__)

_starEmoji = "\N{WHITE MEDIUM STAR}"
_fireEmoji = "\N{FIRE}"
_skullEmoji = "\N{SKULL}"
_cockroachEmoji = "\N{COCKROACH}"
_wiltedRoseEmoji = "\N{WILTED FLOWER}"

_hallEmojiRoutes = {
    _starEmoji: (
        "FAME",
        "hallOfFameChannelId",
        _starEmoji,
        "Hall of Fame",
    ),
    _fireEmoji: (
        "FAME",
        "hallOfFameChannelId",
        _fireEmoji,
        "Hall of Fame",
    ),
    _skullEmoji: (
        "SHAME",
        "hallOfShameChannelId",
        _skullEmoji,
        "Hall of Shame",
    ),
    _cockroachEmoji: (
        "SHAME",
        "hallOfShameChannelId",
        _cockroachEmoji,
        "Hall of Shame",
    ),
    _wiltedRoseEmoji: (
        "SHAME",
        "hallOfShameChannelId",
        _wiltedRoseEmoji,
        "Hall of Shame",
    ),
}

_hallEmojiNameRoutes = {
    "star": _starEmoji,
    "fire": _fireEmoji,
    "skull": _skullEmoji,
    "cockroach": _cockroachEmoji,
    "wilted_rose": _wiltedRoseEmoji,
    "wilted_flower": _wiltedRoseEmoji,
}

_hallEmojiIdRoutes = {
    1381056801969275061: (
        "FAME",
        "hallOfFameChannelId",
        "Hall of Fame",
    ),
    1429185489637871708: (
        "SHAME",
        "hallOfShameChannelId",
        "Hall of Shame",
    ),
}


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


def _toHallRoute(emoji: object) -> Optional[tuple[str, int, str, str]]:
    if isinstance(emoji, str):
        emojiName = str(emoji or "").strip()
        emojiId = None
        emojiDisplay = emojiName
    else:
        emojiName = str(getattr(emoji, "name", "") or "").strip()
        emojiId = getattr(emoji, "id", None)
        emojiDisplay = str(emoji)
    emojiNameLower = emojiName.lower()

    if emojiId is not None:
        route = _hallEmojiIdRoutes.get(int(emojiId))
        if route is not None:
            hallType, channelAttr, hallTitle = route
            return (
                hallType,
                int(getattr(config, channelAttr, 0) or 0),
                emojiDisplay,
                hallTitle,
            )

    if emojiId is None:
        route = _hallEmojiRoutes.get(emojiName)
        if route is not None:
            hallType, channelAttr, reactionEmoji, hallTitle = route
            return (
                hallType,
                int(getattr(config, channelAttr, 0) or 0),
                reactionEmoji,
                hallTitle,
            )

    mappedEmoji = _hallEmojiNameRoutes.get(emojiNameLower)
    if mappedEmoji is not None:
        hallType, channelAttr, reactionEmoji, hallTitle = _hallEmojiRoutes[mappedEmoji]
        return (
            hallType,
            int(getattr(config, channelAttr, 0) or 0),
            reactionEmoji,
            hallTitle,
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


def _reactionBreakdown(message: discord.Message, hallType: str) -> dict[str, int]:
    normalizedHallType = str(hallType or "").upper()
    breakdown: dict[str, int] = {}
    for reaction in message.reactions:
        route = _toHallRoute(reaction.emoji)
        if route is None:
            continue
        reactionHallType, _, reactionEmoji, _ = route
        if reactionHallType != normalizedHallType:
            continue
        try:
            reactionCount = int(reaction.count or 0)
        except (TypeError, ValueError):
            reactionCount = 0
        breakdown[reactionEmoji] = max(0, reactionCount)
    return breakdown


def _primaryHallReaction(
    reactionBreakdown: dict[str, int],
    *,
    fallbackEmoji: str,
) -> tuple[str, int]:
    if not reactionBreakdown:
        return (str(fallbackEmoji or "").strip() or _starEmoji, 0)

    primaryEmoji = str(fallbackEmoji or "").strip()
    primaryCount = -1
    for reactionEmoji, reactionCount in reactionBreakdown.items():
        if reactionCount > primaryCount:
            primaryEmoji = reactionEmoji
            primaryCount = int(reactionCount)
    if not primaryEmoji:
        firstEmoji, firstCount = next(iter(reactionBreakdown.items()))
        return (firstEmoji, int(firstCount))
    return (primaryEmoji, max(0, primaryCount))


def _meetsHallThreshold(reactionBreakdown: dict[str, int]) -> bool:
    threshold = _reactionThreshold()
    return any(int(reactionCount) >= threshold for reactionCount in reactionBreakdown.values())


def _orderedHallBreakdownItems(reactionBreakdown: dict[str, int]) -> list[tuple[str, int]]:
    return sorted(
        ((reactionEmoji, max(0, int(reactionCount))) for reactionEmoji, reactionCount in reactionBreakdown.items()),
        key=lambda item: -int(item[1]),
    )


def _hallHeaderLine(
    *,
    reactionBreakdown: dict[str, int],
    sourceChannelId: int,
    fallbackEmoji: str,
) -> str:
    orderedItems = [
        (reactionEmoji, reactionCount)
        for reactionEmoji, reactionCount in _orderedHallBreakdownItems(reactionBreakdown)
        if int(reactionCount or 0) > 0
    ]
    if not orderedItems:
        orderedItems = [(_primaryHallReaction(reactionBreakdown, fallbackEmoji=fallbackEmoji))]
    reactionSummary = " | ".join(
        f"{reactionEmoji} **{max(0, int(reactionCount or 0))}**"
        for reactionEmoji, reactionCount in orderedItems
    )
    if int(sourceChannelId or 0) > 0:
        return f"{reactionSummary} | <#{int(sourceChannelId)}>"
    return f"{reactionSummary} | #unknown"


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
    embed.set_footer(text=f"Source message ID: {int(sourceMessage.id)}")

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
        reactionBreakdown: dict[str, int],
        fallbackEmoji: str,
        embed: discord.Embed,
    ) -> Optional[int]:
        headerLine = _hallHeaderLine(
            reactionBreakdown=reactionBreakdown,
            sourceChannelId=sourceChannelId,
            fallbackEmoji=fallbackEmoji,
        )

        useWebhook = bool(getattr(config, "hallUseWebhook", True))
        if useWebhook and isinstance(targetChannel, discord.TextChannel):
            sent = await runtimeWebhooks.sendOwnedWebhookMessageDetailed(
                botClient=self.bot,
                channel=targetChannel,
                webhookName=hallTitle,
                content=headerLine,
                embed=embed,
                username=hallTitle,
                avatarUrl=self.bot.user.display_avatar.url if self.bot.user else None,
                reason="Hall repost output",
            )
            if sent is not None:
                return int(sent.id)

        try:
            sent = await targetChannel.send(content=headerLine, embed=embed)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return None
        return int(sent.id)

    async def _findExistingHallPostMessage(
        self,
        targetChannel: object,
        *,
        sourceMessage: discord.Message,
    ) -> Optional[discord.Message]:
        if not isinstance(targetChannel, (discord.TextChannel, discord.Thread)):
            return None

        sourceJumpUrl = str(sourceMessage.jump_url or "").strip()
        if not sourceJumpUrl:
            return None

        try:
            async for candidate in targetChannel.history(limit=2000):
                for embed in candidate.embeds:
                    embedDescription = str(embed.description or "")
                    if sourceJumpUrl and sourceJumpUrl in embedDescription:
                        return candidate
                    footerText = str(embed.footer.text or "").strip()
                    if footerText == f"Source message ID: {int(sourceMessage.id)}":
                        return candidate
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return None
        return None

    async def _recoverExistingHallPost(
        self,
        *,
        guild: discord.Guild,
        hallType: str,
        targetChannel: object,
        sourceMessage: discord.Message,
        sourceChannelId: int,
        reactionBreakdown: dict[str, int],
        fallbackEmoji: str,
    ) -> Optional[dict]:
        recoveredMessage = await self._findExistingHallPostMessage(
            targetChannel,
            sourceMessage=sourceMessage,
        )
        if recoveredMessage is None:
            return None

        primaryReactionEmoji, primaryReactionCount = _primaryHallReaction(
            reactionBreakdown,
            fallbackEmoji=fallbackEmoji,
        )
        recoveredRecord = {
            "messageId": int(sourceMessage.id),
            "hallType": hallType,
            "guildId": int(guild.id),
            "sourceChannelId": int(sourceChannelId),
            "targetChannelId": int(getattr(targetChannel, "id", 0)),
            "sourceAuthorId": int(sourceMessage.author.id),
            "reactionEmoji": primaryReactionEmoji,
            "reactionCount": int(primaryReactionCount),
            "postedMessageId": int(recoveredMessage.id),
        }
        updated = await self._updateExistingHallPostCount(
            guild=guild,
            existing=recoveredRecord,
            hallType=hallType,
            reactionBreakdown=reactionBreakdown,
            fallbackEmoji=fallbackEmoji,
            sourceChannelId=sourceChannelId,
        )
        if not updated:
            return None

        await hallService.createHallPost(
            messageId=int(sourceMessage.id),
            hallType=hallType,
            guildId=int(guild.id),
            sourceChannelId=int(sourceChannelId),
            targetChannelId=int(getattr(targetChannel, "id", 0)),
            sourceAuthorId=int(sourceMessage.author.id),
            reactionEmoji=primaryReactionEmoji,
            reactionCount=int(primaryReactionCount),
            reactionBreakdown=reactionBreakdown,
            postedMessageId=int(recoveredMessage.id),
        )
        return recoveredRecord

    async def _updateExistingHallPostCount(
        self,
        *,
        guild: discord.Guild,
        existing: dict,
        hallType: str,
        reactionBreakdown: dict[str, int],
        fallbackEmoji: str,
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
            reactionBreakdown=reactionBreakdown,
            sourceChannelId=sourceChannelId,
            fallbackEmoji=fallbackEmoji,
        )
        try:
            await postedMessage.edit(content=headerLine)
        except discord.NotFound:
            return False
        except (discord.Forbidden, discord.HTTPException):
            hallTitle = _hallTitleFromType(hallType)
            edited = await runtimeWebhooks.editOwnedWebhookMessage(
                botClient=self.bot,
                message=postedMessage,
                webhookName=hallTitle,
                content=headerLine,
                reason="Hall repost count update",
            )
            if not edited:
                return False
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

        reactionBreakdown = _reactionBreakdown(sourceMessage, hallType)
        primaryReactionEmoji, primaryReactionCount = _primaryHallReaction(
            reactionBreakdown,
            fallbackEmoji=reactionEmoji,
        )

        lockKey = (int(sourceMessage.id), hallType)
        dispatchLock = self._dispatchLocks.setdefault(lockKey, asyncio.Lock())
        async with dispatchLock:
            existing = await hallService.getHallPost(int(sourceMessage.id), hallType)
            if existing is None:
                if not allowCreate or not _meetsHallThreshold(reactionBreakdown):
                    return
                targetChannel = await self._resolveGuildChannel(guild, int(targetChannelId))
                if targetChannel is None:
                    return
                if int(getattr(targetChannel, "id", 0)) == int(getattr(sourceChannel, "id", 0)):
                    return

                recovered = await self._recoverExistingHallPost(
                    guild=guild,
                    hallType=hallType,
                    targetChannel=targetChannel,
                    sourceMessage=sourceMessage,
                    sourceChannelId=int(getattr(sourceChannel, "id", 0)),
                    reactionBreakdown=reactionBreakdown,
                    fallbackEmoji=reactionEmoji,
                )
                if recovered is not None:
                    return

                embed = _buildHallEmbed(hallType=hallType, sourceMessage=sourceMessage)
                postedMessageId = await self._sendHallPost(
                    hallTitle=hallTitle,
                    targetChannel=targetChannel,
                    sourceChannelId=int(getattr(sourceChannel, "id", 0)),
                    reactionBreakdown=reactionBreakdown,
                    fallbackEmoji=reactionEmoji,
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
                    reactionEmoji=primaryReactionEmoji,
                    reactionCount=int(primaryReactionCount),
                    reactionBreakdown=reactionBreakdown,
                    postedMessageId=int(postedMessageId),
                )
                return

            previousBreakdown = hallService.loadReactionBreakdown(existing)
            if previousBreakdown == reactionBreakdown:
                return

            updated = await self._updateExistingHallPostCount(
                guild=guild,
                existing=existing,
                hallType=hallType,
                reactionBreakdown=reactionBreakdown,
                fallbackEmoji=str(existing.get("reactionEmoji") or reactionEmoji),
                sourceChannelId=int(getattr(sourceChannel, "id", 0)),
            )
            if not updated:
                targetChannel = await self._resolveGuildChannel(guild, int(targetChannelId))
                if targetChannel is not None and int(getattr(targetChannel, "id", 0)) != int(getattr(sourceChannel, "id", 0)):
                    recovered = await self._recoverExistingHallPost(
                        guild=guild,
                        hallType=hallType,
                        targetChannel=targetChannel,
                        sourceChannelId=int(getattr(sourceChannel, "id", 0)),
                        sourceMessage=sourceMessage,
                        reactionBreakdown=reactionBreakdown,
                        fallbackEmoji=str(existing.get("reactionEmoji") or reactionEmoji),
                    )
                    if recovered is not None:
                        return

                await hallService.deleteHallPost(int(sourceMessage.id), hallType)
                if allowCreate and _meetsHallThreshold(reactionBreakdown):
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
                        reactionBreakdown=reactionBreakdown,
                        fallbackEmoji=str(existing.get("reactionEmoji") or reactionEmoji),
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
                        reactionEmoji=primaryReactionEmoji,
                        reactionCount=int(primaryReactionCount),
                        reactionBreakdown=reactionBreakdown,
                        postedMessageId=int(postedMessageId),
                    )
                    return

            await hallService.updateHallPostReactionState(
                messageId=int(sourceMessage.id),
                hallType=hallType,
                reactionEmoji=primaryReactionEmoji,
                reactionCount=int(primaryReactionCount),
                reactionBreakdown=reactionBreakdown,
            )

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent) -> None:
        await self._handleHallReactionEvent(payload, allowCreate=True)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent) -> None:
        await self._handleHallReactionEvent(payload, allowCreate=False)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(HallCog(bot))
