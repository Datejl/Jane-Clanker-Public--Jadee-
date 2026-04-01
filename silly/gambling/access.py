from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import ceil

import discord

import config
from silly.gambling import baccarat, diceDuel, roulette, russianRoulette, slots, texasHoldem

gameLabels = {
    "slots": "Slots",
    "blackjack": "Blackjack",
    "roulette": "Roulette",
    "diceduel": "Dice Duel",
    "baccarat": "Baccarat",
    "texasholdem": "Texas Hold'em",
    "russianroulette": "Russian Roulette",
}

gameOrder = (
    "slots",
    "blackjack",
    "roulette",
    "diceduel",
    "baccarat",
    "texasholdem",
    "russianroulette",
)

_defaultDisabledGameKeys = (
    "russianroulette",
)
disabledGameKeys = {
    str(gameKey or "").strip().lower()
    for gameKey in getattr(config, "gamblingDisabledGameKeys", _defaultDisabledGameKeys)
    if str(gameKey or "").strip()
}
disabledGameMessage = "This game is currently unavailable."

nonBlackjackModules = {
    "slots": slots,
    "roulette": roulette,
    "diceduel": diceDuel,
    "baccarat": baccarat,
}

multiUserModules = {
    "russianroulette": russianRoulette,
    "texasholdem": texasHoldem,
}

gamblingButtonCooldownSec = 1
_lastGamblingButtonUseByUserId: dict[int, datetime] = {}
_gamblingAllowedRoleIds = {
    int(roleId)
    for roleId in (
        getattr(config, "middleRankRoleId", 0),
        getattr(config, "highRankRoleId", 0),
    )
    if int(roleId) > 0
}
_gamblingAccessDeniedMessage = "Only MR/HR roles can use `/gambling` and its buttons."
_defaultGamblingAllowedCategoryIds = (
    1373726853289738261,
    1447439162222448681,
)
gamblingAllowedCategoryIds = {
    int(categoryId)
    for categoryId in getattr(config, "gamblingAllowedCategoryIds", _defaultGamblingAllowedCategoryIds)
    if int(categoryId) > 0
}
_gamblingCategoryDeniedMessage = "Gambling is only enabled in approved casino categories."
_gamblingCategoryLockEnabled = bool(getattr(config, "gamblingCategoryLockEnabled", True))


def anrobucks(amount: int) -> str:
    return f"{int(amount)} anrobucks"


def isGameEnabled(gameKey: str) -> bool:
    safeKey = str(gameKey or "").strip().lower()
    return safeKey in gameLabels and safeKey not in disabledGameKeys


def enabledGameKeys() -> tuple[str, ...]:
    return tuple(gameKey for gameKey in gameOrder if isGameEnabled(gameKey))


def defaultGameKey() -> str:
    enabled = enabledGameKeys()
    if enabled:
        return enabled[0]
    return "slots"


def isCategoryLockEnabled() -> bool:
    return bool(_gamblingCategoryLockEnabled)


def setCategoryLockEnabled(enabled: bool) -> bool:
    global _gamblingCategoryLockEnabled
    _gamblingCategoryLockEnabled = bool(enabled)
    return _gamblingCategoryLockEnabled


def toggleCategoryLockEnabled() -> bool:
    return setCategoryLockEnabled(not isCategoryLockEnabled())


def isButtonInteraction(interaction: discord.Interaction) -> bool:
    data = getattr(interaction, "data", None)
    if not isinstance(data, dict):
        return False
    componentType = data.get("component_type")
    try:
        return int(componentType or 0) == int(discord.ComponentType.button.value)
    except (TypeError, ValueError):
        return False


async def sendEphemeralInteractionMessage(interaction: discord.Interaction, content: str) -> None:
    if interaction.response.is_done():
        try:
            await interaction.followup.send(content=content, ephemeral=True)
        except (discord.NotFound, discord.HTTPException):
            return
        return
    try:
        await interaction.response.send_message(content, ephemeral=True)
    except (discord.NotFound, discord.HTTPException):
        return


def _memberHasGamblingRole(member: discord.Member) -> bool:
    if not _gamblingAllowedRoleIds:
        return True
    return any(int(role.id) in _gamblingAllowedRoleIds for role in member.roles)


def _interactionCategoryId(interaction: discord.Interaction) -> int:
    channel = getattr(interaction, "channel", None)
    if isinstance(channel, discord.Thread):
        parent = channel.parent
        if parent is not None:
            return int(getattr(parent, "category_id", 0) or 0)
        return 0
    return int(getattr(channel, "category_id", 0) or 0)


async def enforceGamblingRoleAccess(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        await sendEphemeralInteractionMessage(
            interaction,
            "This command can only be used in a server.",
        )
        return False
    if not isinstance(interaction.user, discord.Member):
        await sendEphemeralInteractionMessage(interaction, _gamblingAccessDeniedMessage)
        return False
    if not _memberHasGamblingRole(interaction.user):
        await sendEphemeralInteractionMessage(interaction, _gamblingAccessDeniedMessage)
        return False
    if isCategoryLockEnabled():
        categoryId = _interactionCategoryId(interaction)
        if categoryId not in gamblingAllowedCategoryIds:
            await sendEphemeralInteractionMessage(interaction, _gamblingCategoryDeniedMessage)
            return False
    return True


def _pruneExpiredButtonCooldownEntries(now: datetime) -> None:
    expiry = timedelta(seconds=max(30, gamblingButtonCooldownSec * 6))
    staleUserIds = [
        userId
        for userId, usedAt in _lastGamblingButtonUseByUserId.items()
        if now - usedAt >= expiry
    ]
    for userId in staleUserIds:
        _lastGamblingButtonUseByUserId.pop(userId, None)


async def enforceGamblingButtonCooldown(interaction: discord.Interaction) -> bool:
    if not isButtonInteraction(interaction):
        return True
    safeUserId = int(getattr(interaction.user, "id", 0) or 0)
    if safeUserId <= 0:
        return False

    now = datetime.now(timezone.utc)
    _pruneExpiredButtonCooldownEntries(now)
    lastUsedAt = _lastGamblingButtonUseByUserId.get(safeUserId)
    if lastUsedAt is not None:
        elapsedSec = (now - lastUsedAt).total_seconds()
        if elapsedSec < gamblingButtonCooldownSec:
            waitSec = max(1, int(ceil(gamblingButtonCooldownSec - elapsedSec)))
            await sendEphemeralInteractionMessage(
                interaction,
                f"Please wait `{waitSec}s` before using another gambling button.",
            )
            return False

    _lastGamblingButtonUseByUserId[safeUserId] = now
    return True


async def safeDeferInteraction(interaction: discord.Interaction) -> None:
    if interaction.response.is_done():
        return
    try:
        await interaction.response.defer(thinking=False)
    except (discord.NotFound, discord.HTTPException):
        return


async def safeEditInteractionMessage(
    interaction: discord.Interaction,
    *,
    embed: discord.Embed,
    view: discord.ui.View,
) -> None:
    if interaction.response.is_done():
        try:
            await interaction.edit_original_response(embed=embed, view=view)
            return
        except (discord.NotFound, discord.HTTPException):
            pass
    else:
        try:
            await interaction.response.edit_message(embed=embed, view=view)
            return
        except (discord.NotFound, discord.HTTPException):
            pass

    sourceMessage = getattr(interaction, "message", None)
    if isinstance(sourceMessage, discord.Message):
        try:
            await sourceMessage.edit(embed=embed, view=view)
        except discord.HTTPException:
            return
