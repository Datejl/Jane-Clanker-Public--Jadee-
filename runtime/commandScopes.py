from __future__ import annotations

import discord

import config

TEST_GUILD_ONLY_MESSAGE = "This command is only available in the test server."


def _normalizeGuildIds(values: list[int] | tuple[int, ...] | set[int] | None) -> tuple[int, ...]:
    out: list[int] = []
    for rawValue in values or []:
        try:
            guildId = int(rawValue)
        except (TypeError, ValueError):
            continue
        if guildId <= 0 or guildId in out:
            continue
        out.append(guildId)
    return tuple(out)


_testGuildIds = _normalizeGuildIds(
    [
        getattr(config, "serverIdTesting", 0),
    ]
)
_testGuildObjects = tuple(discord.Object(id=guildId) for guildId in _testGuildIds)


def getTestGuildIds() -> tuple[int, ...]:
    return _testGuildIds


def getTestGuildObjects() -> tuple[discord.Object, ...]:
    return _testGuildObjects


def isTestGuild(guildId: int | None) -> bool:
    if guildId is None:
        return False
    try:
        parsedGuildId = int(guildId)
    except (TypeError, ValueError):
        return False
    return parsedGuildId in _testGuildIds


def isInteractionInTestGuild(interaction: discord.Interaction) -> bool:
    return isTestGuild(getattr(interaction, "guild_id", None))
