from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import discord

import config
from . import taskBudgeter


@dataclass
class ConfigIssue:
    level: str
    key: str
    message: str


def _normalizeSingleId(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _optionalIdKeys() -> set[str]:
    configured = getattr(config, "configSanityOptionalIdKeys", []) or []
    normalized = {str(value).strip() for value in configured if str(value).strip()}
    normalized.update(
        {
            "anrdRoleProbationaryId",
            "anrdRoleContributorId",
            "anrdRoleDeveloperId",
            "anrdRoleSeniorDeveloperId",
            "anrdRoleDevelopmentProjectLeadId",
            "anrdRoleDevelopmentOversightId",
            "anrdRoleDevelopmentCreatorAndDirectorId",
            "anrdFundingBenefactorsRoleId",
        }
    )
    return normalized


def _iterSingleIdKeys() -> list[str]:
    out: list[str] = []
    for name in dir(config):
        if name.startswith("_"):
            continue
        if not name.endswith("Id"):
            continue
        if name.endswith("Ids"):
            continue
        out.append(name)
    return sorted(set(out))


def _iterListIdKeys() -> list[str]:
    out: list[str] = []
    for name in dir(config):
        if name.startswith("_"):
            continue
        if not name.endswith("Ids"):
            continue
        out.append(name)
    return sorted(set(out))


def _classifyIdKey(key: str) -> str:
    lower = key.lower()
    if "channel" in lower:
        return "channel"
    if "role" in lower:
        return "role"
    return "generic"


async def _fetchReachableChannel(bot: discord.Client, channelId: int) -> Optional[discord.abc.GuildChannel]:
    channel = bot.get_channel(channelId)
    if channel is None:
        try:
            channel = await taskBudgeter.runDiscord(lambda: bot.fetch_channel(channelId))
        except Exception:
            channel = None
    if isinstance(channel, discord.abc.GuildChannel):
        return channel
    return None


async def _resolveGuild(bot: discord.Client) -> Optional[discord.Guild]:
    serverId = _normalizeSingleId(getattr(config, "serverId", 0))
    if serverId <= 0:
        return None
    guild = bot.get_guild(serverId)
    if guild is not None:
        return guild
    try:
        fetched = await taskBudgeter.runDiscord(lambda: bot.fetch_guild(serverId))
    except Exception:
        return None
    return fetched if isinstance(fetched, discord.Guild) else None


async def runConfigSanityCheck(bot: discord.Client) -> dict[str, Any]:
    issues: list[ConfigIssue] = []
    optional = _optionalIdKeys()
    guild = await _resolveGuild(bot)
    roleIdSet: set[int] = set()
    if guild is not None:
        try:
            roles = await taskBudgeter.runDiscord(lambda: guild.fetch_roles())
        except Exception:
            roles = list(getattr(guild, "roles", []))
        roleIdSet = {int(role.id) for role in roles}

    # single IDs
    for key in _iterSingleIdKeys():
        value = _normalizeSingleId(getattr(config, key, 0))
        if value <= 0:
            if key not in optional:
                issues.append(ConfigIssue("warning", key, "Missing or non-positive ID."))
            continue

        kind = _classifyIdKey(key)
        if kind == "channel":
            channel = await _fetchReachableChannel(bot, value)
            if channel is None:
                issues.append(ConfigIssue("warning", key, f"Channel {value} is not reachable."))
        elif kind == "role" and roleIdSet:
            if value not in roleIdSet:
                issues.append(
                    ConfigIssue(
                        "warning",
                        key,
                        f"Role {value} was not found in server {getattr(config, 'serverId', 0)}.",
                    )
                )

    # list IDs
    for key in _iterListIdKeys():
        rawValues = getattr(config, key, None)
        if rawValues is None:
            continue
        if not isinstance(rawValues, (list, tuple, set)):
            issues.append(ConfigIssue("warning", key, "Expected a list/tuple/set of IDs."))
            continue
        for idx, value in enumerate(rawValues):
            normalized = _normalizeSingleId(value)
            indexedKey = f"{key}[{idx}]"
            if normalized <= 0:
                if key not in optional:
                    issues.append(ConfigIssue("warning", indexedKey, "Missing or non-positive ID."))
                continue
            kind = _classifyIdKey(key)
            if kind == "role" and roleIdSet and normalized not in roleIdSet:
                issues.append(
                    ConfigIssue(
                        "warning",
                        indexedKey,
                        f"Role {normalized} was not found in server {getattr(config, 'serverId', 0)}.",
                    )
                )

    gamblingApiEnabled = bool(getattr(config, "gamblingApiEnabled", False))
    gamblingApiToken = str(getattr(config, "gamblingApiToken", "") or "").strip()
    gamblingApiTokens = getattr(config, "gamblingApiTokens", []) or []
    hasExtraApiTokens = isinstance(gamblingApiTokens, (list, tuple, set)) and any(
        str(value).strip() for value in gamblingApiTokens
    )
    if gamblingApiEnabled and not gamblingApiToken and not hasExtraApiTokens:
        issues.append(
            ConfigIssue(
                "warning",
                "gamblingApiToken",
                "Gambling API is enabled but no API token is configured.",
            )
        )
    if gamblingApiEnabled and gamblingApiToken in {"change-me", ":durr:"}:
        issues.append(
            ConfigIssue(
                "warning",
                "gamblingApiToken",
                "Gambling API token looks like a placeholder; set a unique secret.",
            )
        )

    warningCount = sum(1 for issue in issues if issue.level == "warning")
    errorCount = sum(1 for issue in issues if issue.level == "error")
    summary = {
        "ok": errorCount == 0,
        "warningCount": warningCount,
        "errorCount": errorCount,
        "issues": [{"level": i.level, "key": i.key, "message": i.message} for i in issues],
    }
    return summary
