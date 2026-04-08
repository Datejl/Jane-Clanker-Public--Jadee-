from __future__ import annotations

from typing import Optional

import discord

import config


def hasModPerm(member: discord.Member) -> bool:
    roleIds: set[int] = set()
    for raw in (
        getattr(config, "moderatorRoleId", None),
        getattr(config, "bgReviewModeratorRoleId", None),
    ):
        try:
            parsed = int(raw) if raw is not None else 0
        except (TypeError, ValueError):
            parsed = 0
        if parsed > 0:
            roleIds.add(parsed)

    if not roleIds:
        return True
    return any(int(role.id) in roleIds for role in member.roles)


def resolveBgQueuePingRoleId(channel: object) -> int:
    guildId = 0
    if isinstance(channel, discord.Thread):
        parentGuild = getattr(channel, "guild", None)
        guildId = int(parentGuild.id) if isinstance(parentGuild, discord.Guild) else 0
    elif isinstance(channel, discord.TextChannel):
        guildId = int(channel.guild.id)

    try:
        mainGuildId = int(getattr(config, "serverId", 0) or 0)
    except (TypeError, ValueError):
        mainGuildId = 0

    try:
        mainRoleId = int(getattr(config, "moderatorRoleId", 0) or 0)
    except (TypeError, ValueError):
        mainRoleId = 0

    try:
        reviewRoleId = int(getattr(config, "bgReviewModeratorRoleId", 0) or 0)
    except (TypeError, ValueError):
        reviewRoleId = 0
    try:
        adultReviewGuildId = int(getattr(config, "bgCheckAdultReviewGuildId", 0) or 0)
    except (TypeError, ValueError):
        adultReviewGuildId = 0
    try:
        minorReviewGuildId = int(getattr(config, "bgCheckMinorReviewGuildId", 0) or 0)
    except (TypeError, ValueError):
        minorReviewGuildId = 0
    try:
        minorReviewRoleId = int(getattr(config, "bgCheckMinorReviewRoleId", 0) or 0)
    except (TypeError, ValueError):
        minorReviewRoleId = 0

    if guildId > 0 and mainGuildId > 0 and guildId == mainGuildId:
        return mainRoleId if mainRoleId > 0 else reviewRoleId
    if guildId > 0 and adultReviewGuildId > 0 and guildId == adultReviewGuildId:
        return reviewRoleId if reviewRoleId > 0 else mainRoleId
    if guildId > 0 and minorReviewGuildId > 0 and guildId == minorReviewGuildId:
        return minorReviewRoleId
    return reviewRoleId if reviewRoleId > 0 else mainRoleId


def sessionGuild(
    bot: discord.Client,
    session: Optional[dict],
    fallback: Optional[discord.Guild],
) -> Optional[discord.Guild]:
    if isinstance(session, dict):
        try:
            guildId = int(session.get("guildId") or 0)
        except (TypeError, ValueError):
            guildId = 0
        if guildId > 0:
            resolved = bot.get_guild(guildId)
            if resolved is not None:
                return resolved
    return fallback


def canClockIn(member: discord.Member) -> bool:
    roleId = getattr(config, "newApplicantRoleId", None)
    if not roleId:
        return True
    return any(role.id == roleId for role in member.roles)


def clockInDeniedMessage() -> str:
    return (
        "Clock-in is restricted to members who still hold the New Applicant role. "
        "Your account appears to have already completed orientation."
        "If you have not completed an orientation, please create a ticket so our staff may correct this error."
    )


def robloxGroupUrl() -> str:
    groupUrl = getattr(config, "robloxGroupUrl", "") or ""
    if groupUrl:
        return groupUrl
    groupId = getattr(config, "robloxGroupId", 0)
    if groupId:
        return f"https://www.roblox.com/groups/{groupId}"
    return "https://www.roblox.com/communities/"
