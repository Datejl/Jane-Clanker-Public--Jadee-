from __future__ import annotations

from functools import lru_cache

import discord

import config
from runtime import normalization


def toPositiveInt(value: object) -> int:
    return normalization.toPositiveInt(value)


def normalizeRoleIds(raw: object) -> list[int]:
    return normalization.normalizeIntList(raw)


def hasAnyRole(member: discord.Member, roleIds: object) -> bool:
    allowedRoleIds = normalization.normalizeIntSet(roleIds)
    if not allowedRoleIds:
        return False
    return any(int(role.id) in allowedRoleIds for role in member.roles)


def formatRoleIds(roleIds: list[int]) -> str:
    if not roleIds:
        return "none configured"
    return ", ".join(f"`{roleId}`" for roleId in roleIds)


@lru_cache(maxsize=1)
def getCohostAllowedRoleIds() -> set[int]:
    roleIds = getattr(config, "cohostAllowedRoleIds", None)
    if not roleIds:
        return set()
    normalized: set[int] = set()
    for value in roleIds:
        parsed = toPositiveInt(value)
        if parsed > 0:
            normalized.add(parsed)
    return normalized


def hasCohostPermission(member: discord.Member) -> bool:
    allowedRoles = getCohostAllowedRoleIds()
    if not allowedRoles:
        return True
    return hasAnyRole(member, allowedRoles)


@lru_cache(maxsize=1)
def getMiddleHighRankRoleIds() -> set[int]:
    out: set[int] = set()
    for raw in (
        getattr(config, "middleRankRoleId", None),
        getattr(config, "highRankRoleId", None),
    ):
        parsed = toPositiveInt(raw)
        if parsed > 0:
            out.add(parsed)
    return out


def hasMiddleHighRankRole(member: discord.Member) -> bool:
    allowedRoles = getMiddleHighRankRoleIds()
    if not allowedRoles:
        return False
    return hasAnyRole(member, allowedRoles)


def getBgCheckCertifiedRoleIds() -> set[int]:
    out: set[int] = set()
    for raw in (
        getattr(config, "bgCheckCertifiedRoleId", None),
        getattr(config, "bgReviewModeratorRoleId", None),
        getattr(config, "moderatorRoleId", None),
    ):
        parsed = toPositiveInt(raw)
        if parsed > 0:
            out.add(parsed)
    return out


def hasBgCheckCertifiedRole(member: discord.Member) -> bool:
    roleIds = getBgCheckCertifiedRoleIds()
    if not roleIds:
        return False
    return hasAnyRole(member, roleIds)


def hasAdminOrManageGuild(member: discord.Member) -> bool:
    return bool(member.guild_permissions.administrator or member.guild_permissions.manage_guild)


def hasAdministrator(member: discord.Member) -> bool:
    return bool(member.guild_permissions.administrator)


@lru_cache(maxsize=1)
def getTemporaryCommandAllowedUserIds() -> set[int]:
    rawIds = getattr(config, "temporaryCommandAllowedUserIds", [331660652672319488]) or [331660652672319488]
    normalized: set[int] = set()
    for value in rawIds:
        parsed = toPositiveInt(value)
        if parsed > 0:
            normalized.add(parsed)
    return normalized


def isCommandExecutionAllowed(userId: int) -> bool:
    enabled = bool(getattr(config, "temporaryCommandLockEnabled", False))
    if not enabled:
        return True
    allowedIds = getTemporaryCommandAllowedUserIds()
    if not allowedIds:
        return True
    return int(userId) in allowedIds


def clearPermissionCaches() -> None:
    getCohostAllowedRoleIds.cache_clear()
    getMiddleHighRankRoleIds.cache_clear()
    getTemporaryCommandAllowedUserIds.cache_clear()
