from __future__ import annotations

import discord

import config
from runtime import normalization

TEST_GUILD_ONLY_MESSAGE = "This command is only available in the test servers."


def _normalizeGuildIds(values: object) -> tuple[int, ...]:
    return tuple(normalization.normalizeIntList(values))


def _configuredTestGuildIds() -> tuple[int, ...]:
    configuredGuildIds = getattr(config, "testGuildIds", None)
    if configuredGuildIds is None:
        configuredGuildIds = [getattr(config, "serverIdTesting", 0)]
    return _normalizeGuildIds(configuredGuildIds)


_testGuildIds = _configuredTestGuildIds()
_testGuildIdSet = frozenset(_testGuildIds)
_testGuildObjects = tuple(discord.Object(id=guildId) for guildId in _testGuildIds)


def getGuildAndTestGuildIds(*guildIds: int) -> tuple[int, ...]:
    return _normalizeGuildIds([*guildIds, *_testGuildIds])


def getGuildAndTestGuildObjects(*guildIds: int) -> tuple[discord.Object, ...]:
    return tuple(discord.Object(id=guildId) for guildId in getGuildAndTestGuildIds(*guildIds))


def getTestGuildIds() -> tuple[int, ...]:
    return _testGuildIds


def getTestGuildObjects() -> tuple[discord.Object, ...]:
    return _testGuildObjects


def isTestGuild(guildId: int | None) -> bool:
    parsedGuildId = normalization.toPositiveInt(guildId)
    if parsedGuildId <= 0:
        return False
    return parsedGuildId in _testGuildIdSet


def isInteractionInTestGuild(interaction: discord.Interaction) -> bool:
    return isTestGuild(getattr(interaction, "guild_id", None))
