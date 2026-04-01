from __future__ import annotations

from typing import Any

import discord
from discord import Member, PermissionOverwrite, Permissions
from discord.ext import commands

from config import serverId

_VALID_OVERWRITE_FLAGS = set(Permissions.VALID_FLAGS.keys())


def makeOverwrite(**kwargs: Any) -> PermissionOverwrite:
    safeKwargs = {key: value for key, value in kwargs.items() if key in _VALID_OVERWRITE_FLAGS}
    return PermissionOverwrite(**safeKwargs)


def buildConfiguredRoleOverwrites(
    *,
    bot: commands.Bot,
    configuredOverwrites: dict[int, PermissionOverwrite],
) -> tuple[dict[Any, PermissionOverwrite], discord.Guild | None]:
    guild = bot.get_guild(serverId)
    if guild is None:
        return {}, None

    actualOverwrites: dict[Any, PermissionOverwrite] = {}
    for rawTargetId, overwrite in configuredOverwrites.items():
        targetId = int(rawTargetId)
        target = guild.default_role if targetId == int(serverId) else guild.get_role(targetId)
        if target is None:
            continue
        actualOverwrites[target] = overwrite
    return actualOverwrites, guild


def resolveMemberOverwriteTarget(
    guild: discord.Guild,
    memberOrId: Member | int | None,
) -> Member | None:
    if memberOrId is None:
        return None
    if isinstance(memberOrId, Member):
        if int(memberOrId.guild.id) == int(guild.id):
            return memberOrId
        return guild.get_member(int(memberOrId.id))
    try:
        memberId = int(memberOrId)
    except (TypeError, ValueError):
        return None
    return guild.get_member(memberId)


def addMemberOverwrite(
    actualOverwrites: dict[Any, PermissionOverwrite],
    *,
    guild: discord.Guild,
    memberOrId: Member | int | None,
    overwrite: PermissionOverwrite,
) -> None:
    target = resolveMemberOverwriteTarget(guild, memberOrId)
    if target is not None:
        actualOverwrites[target] = overwrite
