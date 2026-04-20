from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

import discord

import config
from db.sqlite import executeReturnId, fetchAll
from features.staff.bgIntelligence import externalSources, scoring
from features.staff.bgflags import service as flagService
from features.staff.orbat import sheets as orbatSheets
from features.staff.sessions import bgRouting, bgScanPipeline, roblox
from features.staff.sessions.bgBuckets import adultBgReviewBucket, normalizeBgReviewBucket
from runtime import taskBudgeter

ProgressCallback = Callable[[str], Awaitable[Any]]


@dataclass(frozen=True)
class FlagRules:
    groupIds: set[int]
    usernames: list[str]
    usernameNotes: dict[str, str]
    usernameSeverities: dict[str, int]
    robloxUserIds: set[int]
    robloxUserNotes: dict[int, str]
    robloxUserSeverities: dict[int, int]
    watchlistUserIds: set[int]
    watchlistNotes: dict[int, str]
    watchlistSeverities: dict[int, int]
    bannedUserIds: set[int]
    bannedUserNotes: dict[int, str]
    bannedUserSeverities: dict[int, int]
    groupKeywords: list[str]
    itemKeywords: list[str]
    itemIds: set[int]
    creatorIds: set[int]
    gameIds: set[int]
    gameKeywords: list[str]
    badgeIds: set[int]
    badgeNotes: dict[int, str]
    accountAgeDays: int


@dataclass
class BgIntelligenceReport:
    discordUserId: int
    discordDisplayName: str
    discordUsername: str
    reviewBucket: str
    reviewBucketSource: str
    identitySource: str = "rover"
    robloxUserId: Optional[int] = None
    robloxUsername: Optional[str] = None
    roverError: Optional[str] = None
    robloxCreated: Optional[str] = None
    robloxAgeDays: Optional[int] = None
    groupSummary: dict[str, Any] | None = None
    groupScanStatus: str = "SKIPPED"
    groupScanError: Optional[str] = None
    connectionScanStatus: str = "SKIPPED"
    connectionScanError: Optional[str] = None
    connectionSummary: dict[str, Any] | None = None
    groups: list[dict[str, Any]] | None = None
    flaggedGroups: list[dict[str, Any]] | None = None
    flagMatches: list[dict[str, Any]] | None = None
    directMatches: list[dict[str, Any]] | None = None
    inventoryScanStatus: str = "SKIPPED"
    inventoryScanError: Optional[str] = None
    inventorySummary: dict[str, Any] | None = None
    flaggedItems: list[dict[str, Any]] | None = None
    gamepassScanStatus: str = "SKIPPED"
    gamepassScanError: Optional[str] = None
    gamepassSummary: dict[str, Any] | None = None
    ownedGamepasses: list[dict[str, Any]] | None = None
    favoriteGameScanStatus: str = "SKIPPED"
    favoriteGameScanError: Optional[str] = None
    favoriteGames: list[dict[str, Any]] | None = None
    flaggedFavoriteGames: list[dict[str, Any]] | None = None
    outfitScanStatus: str = "SKIPPED"
    outfitScanError: Optional[str] = None
    outfits: list[dict[str, Any]] | None = None
    badgeHistoryScanStatus: str = "SKIPPED"
    badgeHistoryScanError: Optional[str] = None
    badgeHistorySample: list[dict[str, Any]] | None = None
    badgeTimelineSummary: dict[str, Any] | None = None
    badgeScanStatus: str = "SKIPPED"
    badgeScanError: Optional[str] = None
    flaggedBadges: list[dict[str, Any]] | None = None
    externalSourceStatus: str = "SKIPPED"
    externalSourceError: Optional[str] = None
    externalSourceMatches: list[dict[str, Any]] | None = None
    externalSourceDetails: list[dict[str, Any]] | None = None
    priorReportSummary: dict[str, Any] | None = None
    privateInventoryDmSent: Optional[bool] = None


def _normalizeIntSet(values: Any) -> set[int]:
    normalized: set[int] = set()
    if not isinstance(values, (list, tuple, set)):
        values = [values]
    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            normalized.add(parsed)
    return normalized


def _ageDaysFromCreated(created: Optional[str]) -> Optional[int]:
    if not created:
        return None
    try:
        createdAt = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
    except ValueError:
        return None
    if createdAt.tzinfo is None:
        createdAt = createdAt.replace(tzinfo=timezone.utc)
    return max(0, (datetime.now(createdAt.tzinfo) - createdAt).days)


def _parseRobloxDate(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _ruleSeverity(rule: dict[str, Any]) -> int:
    try:
        severity = int(rule.get("severity") or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, min(100, severity))


def _directMinimum(ruleType: str, severity: int = 0) -> int:
    normalizedType = str(ruleType or "").strip().lower()
    defaults = {
        "banned_user": 95,
        "watchlist": 88,
        "roblox_user": 82,
        "username": 82,
    }
    defaultMinimum = defaults.get(normalizedType, 0)
    configured = max(0, min(100, int(severity or 0)))
    if normalizedType == "banned_user":
        return max(defaultMinimum, configured)
    if configured > 0:
        return max(20, configured)
    return defaultMinimum


async def _resolveOrbatAgeGroupForUser(userId: int) -> str:
    if orbatSheets is None or not hasattr(orbatSheets, "getOrbatEntry"):
        return ""
    try:
        entry = await taskBudgeter.runSheetsThread(orbatSheets.getOrbatEntry, int(userId))
    except Exception:
        return ""
    if not isinstance(entry, dict):
        return ""
    return str(entry.get("ageGroup") or "").strip()


async def resolveReviewBucket(
    member: discord.Member,
    *,
    guildId: int,
    reviewBucketOverride: str = "auto",
    configModule: Any = config,
) -> tuple[str, str]:
    normalizedOverride = str(reviewBucketOverride or "auto").strip().lower()
    if normalizedOverride and normalizedOverride != "auto":
        return normalizeBgReviewBucket(normalizedOverride), "manual"
    return await bgRouting.classifyBgReviewBucketForMember(
        member,
        configModule=configModule,
        resolveOrbatAgeGroup=_resolveOrbatAgeGroupForUser,
        userId=int(member.id),
        guildId=int(guildId),
    )


def _positiveInt(value: Any) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _roverSourceName(guildId: int, *, configModule: Any = config) -> str:
    parsedGuildId = _positiveInt(guildId)
    mainGuildId = _positiveInt(getattr(configModule, "serverId", 0))
    if parsedGuildId > 0 and mainGuildId > 0 and parsedGuildId == mainGuildId:
        return "main server"
    if parsedGuildId > 0:
        return f"guild {parsedGuildId}"
    return "configured guild"


def _roverIdentitySource(
    *,
    roverGuildId: int,
    scanGuildId: int,
    configModule: Any = config,
) -> str:
    parsedRoverGuildId = _positiveInt(roverGuildId)
    mainGuildId = _positiveInt(getattr(configModule, "serverId", 0))
    if parsedRoverGuildId > 0 and mainGuildId > 0 and parsedRoverGuildId == mainGuildId:
        if _positiveInt(scanGuildId) != parsedRoverGuildId:
            return "rover_main_server"
    return "rover"


async def _lookupRoverForGuild(
    discordUserId: int,
    *,
    guildId: int,
    configModule: Any = config,
) -> tuple[roblox.RoverLookupResult, int]:
    resolvedGuildId = _positiveInt(guildId) or _positiveInt(getattr(configModule, "serverId", 0))
    result = await roblox.fetchRobloxUser(int(discordUserId), guildId=resolvedGuildId or None)
    return result, resolvedGuildId


async def loadFlagRules(*, configModule: Any = config) -> FlagRules:
    groupIds = _normalizeIntSet(getattr(configModule, "robloxFlagGroupIds", []) or [])
    badgeIds = _normalizeIntSet(getattr(configModule, "robloxFlagBadgeIds", []) or [])
    badgeNotes: dict[int, str] = {}
    itemIds: set[int] = set()
    creatorIds: set[int] = set()
    gameIds: set[int] = set()
    gameKeywords: list[str] = []
    usernames: list[str] = []
    usernameNotes: dict[str, str] = {}
    usernameSeverities: dict[str, int] = {}
    robloxUserIds: set[int] = set()
    robloxUserNotes: dict[int, str] = {}
    robloxUserSeverities: dict[int, int] = {}
    watchlistUserIds: set[int] = set()
    watchlistNotes: dict[int, str] = {}
    watchlistSeverities: dict[int, int] = {}
    bannedUserIds: set[int] = set()
    bannedUserNotes: dict[int, str] = {}
    bannedUserSeverities: dict[int, int] = {}
    groupKeywords: list[str] = []
    itemKeywords: list[str] = []
    try:
        accountAgeDays = int(getattr(configModule, "robloxAccountAgeFlagDays", 0) or 0)
    except (TypeError, ValueError):
        accountAgeDays = 0

    rules = await flagService.listRules()
    for rule in rules:
        ruleType = str(rule.get("ruleType", "")).strip().lower()
        value = str(rule.get("ruleValue", "")).strip()
        if not value:
            continue
        if ruleType == "group":
            try:
                groupIds.add(int(value))
            except ValueError:
                continue
        elif ruleType == "username":
            lowered = value.lower()
            usernames.append(lowered)
            note = str(rule.get("note") or "").strip()
            if note:
                usernameNotes[lowered] = note
            severity = _ruleSeverity(rule)
            if severity > 0:
                usernameSeverities[lowered] = severity
        elif ruleType == "roblox_user":
            try:
                robloxUserId = int(value)
            except ValueError:
                continue
            robloxUserIds.add(robloxUserId)
            note = str(rule.get("note") or "").strip()
            if note:
                robloxUserNotes[robloxUserId] = note
            severity = _ruleSeverity(rule)
            if severity > 0:
                robloxUserSeverities[robloxUserId] = severity
        elif ruleType == "watchlist":
            try:
                robloxUserId = int(value)
            except ValueError:
                continue
            watchlistUserIds.add(robloxUserId)
            note = str(rule.get("note") or "").strip()
            if note:
                watchlistNotes[robloxUserId] = note
            severity = _ruleSeverity(rule)
            if severity > 0:
                watchlistSeverities[robloxUserId] = severity
        elif ruleType == "banned_user":
            try:
                robloxUserId = int(value)
            except ValueError:
                continue
            bannedUserIds.add(robloxUserId)
            note = str(rule.get("note") or "").strip()
            if note:
                bannedUserNotes[robloxUserId] = note
            severity = _ruleSeverity(rule)
            if severity > 0:
                bannedUserSeverities[robloxUserId] = severity
        elif ruleType == "keyword":
            lowered = value.lower()
            groupKeywords.append(lowered)
            itemKeywords.append(lowered)
            gameKeywords.append(lowered)
        elif ruleType in {"group_keyword", "group-keyword"}:
            groupKeywords.append(value.lower())
        elif ruleType in {"item_keyword", "item-keyword"}:
            itemKeywords.append(value.lower())
        elif ruleType in {"game_keyword", "game-keyword"}:
            gameKeywords.append(value.lower())
        elif ruleType == "item":
            try:
                itemIds.add(int(value))
            except ValueError:
                continue
        elif ruleType == "creator":
            try:
                creatorIds.add(int(value))
            except ValueError:
                continue
        elif ruleType == "game":
            try:
                gameIds.add(int(value))
            except ValueError:
                continue
        elif ruleType == "badge":
            try:
                badgeId = int(value)
            except ValueError:
                continue
            badgeIds.add(badgeId)
            note = str(rule.get("note") or "").strip()
            if note:
                badgeNotes[badgeId] = note

    return FlagRules(
        groupIds=groupIds,
        usernames=usernames,
        usernameNotes=usernameNotes,
        usernameSeverities=usernameSeverities,
        robloxUserIds=robloxUserIds,
        robloxUserNotes=robloxUserNotes,
        robloxUserSeverities=robloxUserSeverities,
        watchlistUserIds=watchlistUserIds,
        watchlistNotes=watchlistNotes,
        watchlistSeverities=watchlistSeverities,
        bannedUserIds=bannedUserIds,
        bannedUserNotes=bannedUserNotes,
        bannedUserSeverities=bannedUserSeverities,
        groupKeywords=groupKeywords,
        itemKeywords=itemKeywords,
        itemIds=itemIds,
        creatorIds=creatorIds,
        gameIds=gameIds,
        gameKeywords=gameKeywords,
        badgeIds=badgeIds,
        badgeNotes=badgeNotes,
        accountAgeDays=max(0, accountAgeDays),
    )


def _groupKey(group: dict[str, Any]) -> tuple[Optional[int], str]:
    rawId = group.get("id")
    try:
        groupId = int(rawId) if rawId is not None else None
    except (TypeError, ValueError):
        groupId = None
    return groupId, str(group.get("name") or "").strip().lower()


def _buildGroupSummary(groups: list[dict[str, Any]]) -> dict[str, Any]:
    total = 0
    baseRank = 0
    elevatedRank = 0
    ownerRank = 0
    namedRole = 0
    verifiedGroups = 0
    publicEntryGroups = 0
    lockedGroups = 0
    knownMemberCountGroups = 0
    unknownMemberCountGroups = 0
    smallGroups = 0
    midSizeGroups = 0
    largeGroups = 0
    veryLargeGroups = 0
    ranks: list[int] = []
    memberCounts: list[int] = []
    roleNames: set[str] = set()

    def _safeInt(value: Any) -> Optional[int]:
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            return None
        return None

    for group in list(groups or []):
        if not isinstance(group, dict):
            continue
        total += 1
        roleName = str(group.get("role") or "").strip().lower()
        rank = _safeInt(group.get("rank")) or 0
        if rank <= 0 and roleName == "owner":
            rank = 255
        ranks.append(rank)
        if roleName:
            roleNames.add(roleName)
        if rank <= 5:
            baseRank += 1
        if rank >= 100:
            elevatedRank += 1
        if rank >= 255 or roleName == "owner":
            ownerRank += 1
        if roleName and roleName not in {"member", "guest", "rank 1"}:
            namedRole += 1
        if group.get("hasVerifiedBadge") is True:
            verifiedGroups += 1
        if group.get("publicEntryAllowed") is True:
            publicEntryGroups += 1
        if group.get("isLocked") is True:
            lockedGroups += 1
        memberCount = _safeInt(group.get("memberCount"))
        if memberCount is None or memberCount < 0:
            unknownMemberCountGroups += 1
            continue
        knownMemberCountGroups += 1
        memberCounts.append(memberCount)
        if memberCount >= 100_000:
            veryLargeGroups += 1
            largeGroups += 1
        elif memberCount >= 10_000:
            largeGroups += 1
        elif memberCount < 100:
            smallGroups += 1
        else:
            midSizeGroups += 1

    sortedRanks = sorted(ranks)
    sortedMemberCounts = sorted(memberCounts)
    medianRank = sortedRanks[len(sortedRanks) // 2] if sortedRanks else 0
    averageRank = round(sum(ranks) / len(ranks), 1) if ranks else 0.0
    averageMemberCount = int(sum(memberCounts) / len(memberCounts)) if memberCounts else 0
    baseRatio = (baseRank / total) if total else 0.0
    elevatedRatio = (elevatedRank / total) if total else 0.0
    smallRatio = (smallGroups / knownMemberCountGroups) if knownMemberCountGroups else 0.0
    largeRatio = (largeGroups / knownMemberCountGroups) if knownMemberCountGroups else 0.0
    verifiedRatio = (verifiedGroups / total) if total else 0.0
    return {
        "totalGroups": total,
        "baseRankGroups": baseRank,
        "elevatedRankGroups": elevatedRank,
        "ownerRankGroups": ownerRank,
        "namedRoleGroups": namedRole,
        "baseRankRatio": round(baseRatio, 3),
        "elevatedRankRatio": round(elevatedRatio, 3),
        "knownMemberCountGroups": knownMemberCountGroups,
        "unknownMemberCountGroups": unknownMemberCountGroups,
        "smallGroups": smallGroups,
        "midSizeGroups": midSizeGroups,
        "largeGroups": largeGroups,
        "veryLargeGroups": veryLargeGroups,
        "smallGroupRatio": round(smallRatio, 3),
        "largeGroupRatio": round(largeRatio, 3),
        "verifiedGroups": verifiedGroups,
        "verifiedGroupRatio": round(verifiedRatio, 3),
        "publicEntryGroups": publicEntryGroups,
        "lockedGroups": lockedGroups,
        "distinctRoleNames": len(roleNames),
        "highestRank": max(ranks) if ranks else 0,
        "medianRank": medianRank,
        "averageRank": averageRank,
        "smallestKnownGroupMemberCount": sortedMemberCounts[0] if sortedMemberCounts else 0,
        "largestGroupMemberCount": sortedMemberCounts[-1] if sortedMemberCounts else 0,
        "averageKnownMemberCount": averageMemberCount,
    }


def _badgeIdFromSample(badge: dict[str, Any]) -> Optional[int]:
    for key in ("id", "badgeId", "badge_id"):
        try:
            value = badge.get(key)
            if value is not None:
                parsed = int(value)
                if parsed > 0:
                    return parsed
        except (TypeError, ValueError):
            continue
    return None


def _mergeBadgeAwardDates(
    badges: list[dict[str, Any]],
    awardRows: list[dict[str, Any]],
) -> None:
    awardDates: dict[int, str] = {}
    for row in list(awardRows or []):
        if not isinstance(row, dict):
            continue
        badgeId = _badgeIdFromSample(row)
        awardedDate = row.get("awardedDate") or row.get("awarded_date")
        if badgeId is not None and isinstance(awardedDate, str) and awardedDate.strip():
            awardDates[badgeId] = awardedDate.strip()
    if not awardDates:
        return
    for badge in list(badges or []):
        if not isinstance(badge, dict):
            continue
        badgeId = _badgeIdFromSample(badge)
        if badgeId is not None and badgeId in awardDates:
            badge["awardedDate"] = awardDates[badgeId]


def _buildBadgeTimelineSummary(
    badges: list[dict[str, Any]],
    *,
    awardDateStatus: str = "SKIPPED",
    awardDateError: Optional[str] = None,
    historyComplete: bool | None = None,
    historyNextCursor: Optional[str] = None,
) -> dict[str, Any]:
    sampleSize = 0
    awardedDates: list[datetime] = []
    for badge in list(badges or []):
        if not isinstance(badge, dict):
            continue
        sampleSize += 1
        awardedAt = _parseRobloxDate(badge.get("awardedDate"))
        if awardedAt is not None:
            awardedDates.append(awardedAt)

    datedBadges = len(awardedDates)
    coverage = (datedBadges / sampleSize) if sampleSize else 0.0
    summary: dict[str, Any] = {
        "sampleSize": sampleSize,
        "datedBadges": datedBadges,
        "awardDateStatus": str(awardDateStatus or "SKIPPED").upper(),
        "awardDateCoverage": round(coverage, 3),
        "quality": "unknown",
    }
    if historyComplete is not None:
        summary["historyComplete"] = bool(historyComplete)
    if historyNextCursor:
        summary["historyNextCursor"] = str(historyNextCursor)
    if awardDateError:
        summary["awardDateError"] = str(awardDateError)
    if not awardedDates:
        summary.update(
            {
                "oldestAwardedAt": None,
                "newestAwardedAt": None,
                "spanDays": 0,
                "distinctAwardYears": 0,
                "recent7Days": 0,
                "recent30Days": 0,
                "maxSameDayAwards": 0,
                "maxSameDayRatio": 0.0,
            }
        )
        if sampleSize <= 0:
            summary["quality"] = "none"
        elif summary["awardDateStatus"] == "OK":
            summary["quality"] = "undated"
        return summary

    awardedDates.sort()
    now = datetime.now(timezone.utc)
    oldest = awardedDates[0]
    newest = awardedDates[-1]
    spanDays = max(0, (newest - oldest).days)
    distinctYears = len({date.year for date in awardedDates})
    recent7Days = sum(1 for date in awardedDates if 0 <= (now - date).days <= 7)
    recent30Days = sum(1 for date in awardedDates if 0 <= (now - date).days <= 30)
    dayCounts: dict[str, int] = {}
    for date in awardedDates:
        dayKey = date.date().isoformat()
        dayCounts[dayKey] = dayCounts.get(dayKey, 0) + 1
    maxSameDayAwards = max(dayCounts.values()) if dayCounts else 0
    maxSameDayRatio = (maxSameDayAwards / datedBadges) if datedBadges else 0.0

    if datedBadges >= 75 and spanDays >= 1095 and distinctYears >= 3:
        quality = "multi_year_deep"
    elif datedBadges >= 30 and spanDays >= 365 and distinctYears >= 2:
        quality = "established"
    elif datedBadges >= 20 and spanDays <= 14 and maxSameDayRatio >= 0.6:
        quality = "burst_heavy"
    elif datedBadges <= 3:
        quality = "thin"
    else:
        quality = "normal"

    summary.update(
        {
            "quality": quality,
            "oldestAwardedAt": oldest.isoformat(),
            "newestAwardedAt": newest.isoformat(),
            "spanDays": spanDays,
            "distinctAwardYears": distinctYears,
            "recent7Days": recent7Days,
            "recent30Days": recent30Days,
            "maxSameDayAwards": maxSameDayAwards,
            "maxSameDayRatio": round(maxSameDayRatio, 3),
        }
    )
    return summary


def _directMatchesForReport(report: BgIntelligenceReport, rules: FlagRules) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    if report.robloxUserId:
        try:
            robloxUserId = int(report.robloxUserId)
        except (TypeError, ValueError):
            robloxUserId = 0
        if robloxUserId > 0:
            if robloxUserId in rules.bannedUserIds:
                minimumScore = _directMinimum("banned_user", rules.bannedUserSeverities.get(robloxUserId, 0))
                matches.append(
                    {
                        "type": "banned_user",
                        "value": robloxUserId,
                        "minimumScore": minimumScore,
                        "severity": rules.bannedUserSeverities.get(robloxUserId, 0),
                        "note": rules.bannedUserNotes.get(robloxUserId),
                    }
                )
            if robloxUserId in rules.watchlistUserIds:
                minimumScore = _directMinimum("watchlist", rules.watchlistSeverities.get(robloxUserId, 0))
                matches.append(
                    {
                        "type": "watchlist",
                        "value": robloxUserId,
                        "minimumScore": minimumScore,
                        "severity": rules.watchlistSeverities.get(robloxUserId, 0),
                        "note": rules.watchlistNotes.get(robloxUserId),
                    }
                )
            if robloxUserId in rules.robloxUserIds:
                minimumScore = _directMinimum("roblox_user", rules.robloxUserSeverities.get(robloxUserId, 0))
                matches.append(
                    {
                        "type": "roblox_user",
                        "value": robloxUserId,
                        "minimumScore": minimumScore,
                        "severity": rules.robloxUserSeverities.get(robloxUserId, 0),
                        "note": rules.robloxUserNotes.get(robloxUserId),
                    }
                )

    if report.robloxUsername:
        username = str(report.robloxUsername).strip().lower()
        if username in rules.usernames:
            minimumScore = _directMinimum("username", rules.usernameSeverities.get(username, 0))
            matches.append(
                {
                    "type": "username",
                    "value": report.robloxUsername,
                    "minimumScore": minimumScore,
                    "severity": rules.usernameSeverities.get(username, 0),
                    "note": rules.usernameNotes.get(username),
                }
            )
    return matches


def _analyzeGroups(
    *,
    groups: list[dict[str, Any]],
    robloxUsername: Optional[str],
    ageDays: Optional[int],
    created: Optional[str],
    rules: FlagRules,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    flaggedGroups: list[dict[str, Any]] = []
    matches: list[dict[str, Any]] = []

    if ageDays is not None and rules.accountAgeDays > 0 and ageDays < rules.accountAgeDays:
        matches.append(
            {
                "type": "accountAge",
                "value": f"{ageDays} days",
                "created": created,
                "thresholdDays": rules.accountAgeDays,
            }
        )

    for group in groups:
        groupId, groupNameLower = _groupKey(group)
        matchedGroup = False
        if groupId is not None and groupId in rules.groupIds:
            matchedGroup = True
        for keyword in rules.groupKeywords:
            if keyword and groupNameLower and keyword in groupNameLower:
                matchedGroup = True
                matches.append(
                    {
                        "type": "keyword",
                        "value": keyword,
                        "context": "group",
                        "groupId": groupId,
                        "groupName": group.get("name"),
                    }
                )
        if matchedGroup:
            flaggedGroups.append(group)

    if robloxUsername:
        username = robloxUsername.lower()
        for keyword in rules.groupKeywords:
            if keyword and keyword in username:
                matches.append({"type": "keyword", "value": keyword, "context": "username"})

    dedupedGroups: list[dict[str, Any]] = []
    seenGroups: set[tuple[Optional[int], str]] = set()
    for group in flaggedGroups:
        key = _groupKey(group)
        if key in seenGroups:
            continue
        seenGroups.add(key)
        dedupedGroups.append(group)
    return dedupedGroups, matches


def _analyzeFavoriteGames(
    *,
    games: list[dict[str, Any]],
    rules: FlagRules,
) -> list[dict[str, Any]]:
    flaggedGames: list[dict[str, Any]] = []
    seen: set[tuple[Optional[int], Optional[int], str]] = set()
    keywords = [
        str(keyword).strip().lower()
        for keyword in list(rules.gameKeywords or [])
        if str(keyword).strip()
    ]
    for game in games:
        if not isinstance(game, dict):
            continue
        rawUniverseId = game.get("universeId")
        rawPlaceId = game.get("placeId")
        try:
            universeId = int(rawUniverseId) if rawUniverseId is not None else None
        except (TypeError, ValueError):
            universeId = None
        try:
            placeId = int(rawPlaceId) if rawPlaceId is not None else None
        except (TypeError, ValueError):
            placeId = None
        name = str(game.get("name") or "").strip()
        nameLower = name.lower()
        matchType = ""
        matchedKeyword = None
        if universeId is not None and universeId in rules.gameIds:
            matchType = "game"
        elif placeId is not None and placeId in rules.gameIds:
            matchType = "game"
        elif nameLower and keywords:
            for keyword in keywords:
                if keyword in nameLower:
                    matchType = "keyword"
                    matchedKeyword = keyword
                    break
        if not matchType:
            continue
        key = (universeId, placeId, nameLower)
        if key in seen:
            continue
        seen.add(key)
        flaggedGames.append(
            {
                "name": name or None,
                "universeId": universeId,
                "placeId": placeId,
                "matchType": matchType,
                "keyword": matchedKeyword,
            }
        )
    return flaggedGames


async def sendPrivateInventoryNotice(
    member: discord.Member,
    *,
    reviewer: discord.User | discord.Member | None = None,
) -> bool:
    reviewerText = f" by {reviewer.mention}" if reviewer is not None else ""
    content = (
        f"Jane tried to run a Roblox background review{reviewerText}, but your inventory "
        "appears to be private or hidden.\n\n"
        "Please set your Roblox inventory to public, then ask staff to run `/bg-intel` again. "
        "If you already changed it, the next scan should pick it up."
    )
    try:
        await member.send(content)
        return True
    except (discord.Forbidden, discord.NotFound, discord.HTTPException):
        return False


async def _scanExternalSources(
    report: BgIntelligenceReport,
    *,
    configModule: Any = config,
) -> None:
    externalResult = await externalSources.scanExternalSources(
        discordUserId=int(report.discordUserId or 0),
        robloxUserId=int(report.robloxUserId) if report.robloxUserId else None,
        configModule=configModule,
    )
    report.externalSourceStatus = externalResult.status
    report.externalSourceError = externalResult.error
    report.externalSourceMatches = externalResult.matches
    report.externalSourceDetails = externalResult.details


async def _emitProgress(progressCallback: ProgressCallback | None, status: str) -> None:
    if progressCallback is None:
        return
    try:
        await progressCallback(status)
    except Exception:
        log.debug("BG intelligence progress update failed.", exc_info=True)


async def _completeReportScan(
    report: BgIntelligenceReport,
    *,
    guildId: int,
    rules: FlagRules,
    member: discord.Member | None = None,
    notifyPrivateInventory: bool = False,
    reviewer: discord.User | discord.Member | None = None,
    configModule: Any = config,
    progressCallback: ProgressCallback | None = None,
) -> BgIntelligenceReport:
    if not report.robloxUserId:
        await _emitProgress(progressCallback, "No Roblox account found; checking clanning records only...")
        report.groupScanStatus = "NO_ROVER"
        report.connectionScanStatus = "NO_ROVER"
        report.inventoryScanStatus = "NO_ROVER"
        report.gamepassScanStatus = "NO_ROVER"
        report.favoriteGameScanStatus = "NO_ROVER"
        report.outfitScanStatus = "NO_ROVER"
        report.badgeHistoryScanStatus = "NO_ROVER"
        report.badgeScanStatus = "NO_ROVER"
        await _scanExternalSources(report, configModule=configModule)
        report.priorReportSummary = await _loadPriorReportSummary(
            guildId=int(guildId),
            targetUserId=int(report.discordUserId or 0),
            robloxUserId=None,
            limit=5,
        )
        return report

    await _emitProgress(progressCallback, "Pulling Roblox profile and clanning records...")
    profileTask = asyncio.create_task(roblox.fetchRobloxUserProfile(int(report.robloxUserId)))
    externalTask = asyncio.create_task(_scanExternalSources(report, configModule=configModule))

    profile = await profileTask
    if not profile.error:
        if not report.robloxUsername and profile.username:
            report.robloxUsername = profile.username
        report.robloxCreated = profile.created
        report.robloxAgeDays = _ageDaysFromCreated(profile.created)
    report.directMatches = _directMatchesForReport(report, rules)
    await externalTask

    if bool(getattr(configModule, "bgIntelligenceFetchConnectionsEnabled", True)):
        await _emitProgress(progressCallback, "Checking Roblox connection counts...")
        connectionResult = await roblox.fetchRobloxConnectionCounts(int(report.robloxUserId))
        report.connectionSummary = {
            "friends": connectionResult.friends,
            "followers": connectionResult.followers,
            "following": connectionResult.following,
        }
        if connectionResult.error and all(
            value is None
            for value in (connectionResult.friends, connectionResult.followers, connectionResult.following)
        ):
            report.connectionScanStatus = "ERROR"
            report.connectionScanError = connectionResult.error
        else:
            report.connectionScanStatus = "OK" if not connectionResult.error else "PARTIAL"
            report.connectionScanError = connectionResult.error
    else:
        report.connectionScanStatus = "SKIPPED"

    isAdultRoute = report.reviewBucket == adultBgReviewBucket
    if isAdultRoute and bool(getattr(configModule, "bgIntelligenceFetchGroupsEnabled", True)):
        await _emitProgress(progressCallback, "Reading Roblox group membership...")
        groupResult = await roblox.fetchRobloxGroups(int(report.robloxUserId))
        if groupResult.error:
            report.groupScanStatus = "ERROR"
            report.groupScanError = groupResult.error
        else:
            report.groupScanStatus = "OK"
            report.groups = groupResult.groups
            report.groupSummary = _buildGroupSummary(groupResult.groups)
            flaggedGroups, flagMatches = _analyzeGroups(
                groups=groupResult.groups,
                robloxUsername=report.robloxUsername,
                ageDays=report.robloxAgeDays,
                created=report.robloxCreated,
                rules=rules,
            )
            report.flaggedGroups = flaggedGroups
            report.flagMatches = flagMatches
    elif isAdultRoute:
        report.groupScanStatus = "SKIPPED"

    inventoryEnabled = bool(getattr(configModule, "robloxInventoryScanEnabled", True)) and bool(
        getattr(configModule, "bgIntelligenceFetchInventoryEnabled", True)
    )
    if isAdultRoute and inventoryEnabled:
        await _emitProgress(progressCallback, "Reviewing inventory and item values...")
        try:
            inventoryMaxPages = int(
                getattr(
                    configModule,
                    "bgIntelligenceInventoryMaxPages",
                    getattr(configModule, "robloxInventoryScanMaxPages", 5),
                )
            )
        except (TypeError, ValueError):
            inventoryMaxPages = 5
        inventoryResult = await roblox.fetchRobloxInventory(
            int(report.robloxUserId),
            rules.itemIds,
            targetCreatorIds=rules.creatorIds,
            targetKeywords=rules.itemKeywords,
            maxPages=inventoryMaxPages,
            includeValue=True,
        )
        report.inventorySummary = inventoryResult.summary or {}
        if inventoryResult.error:
            isPrivate = bgScanPipeline.isPrivateInventoryStatus(
                inventoryResult.status,
                inventoryResult.error,
            )
            report.inventoryScanStatus = "PRIVATE" if isPrivate else "ERROR"
            report.inventoryScanError = "Inventory is private or hidden." if isPrivate else inventoryResult.error
            if (
                member is not None
                and isPrivate
                and notifyPrivateInventory
                and bool(getattr(configModule, "bgIntelligencePrivateInventoryDmEnabled", True))
            ):
                report.privateInventoryDmSent = await sendPrivateInventoryNotice(member, reviewer=reviewer)
        else:
            report.inventoryScanStatus = "OK"
            report.flaggedItems = inventoryResult.items
    elif isAdultRoute:
        report.inventoryScanStatus = "SKIPPED"

    if isAdultRoute and bool(getattr(configModule, "bgIntelligenceFetchGamepassesEnabled", True)):
        await _emitProgress(progressCallback, "Pricing owned gamepasses...")
        try:
            gamepassMaxPages = int(getattr(configModule, "bgIntelligenceGamepassMaxPages", 0))
        except (TypeError, ValueError):
            gamepassMaxPages = 0
        inventoryGamepassIds = []
        if isinstance(report.inventorySummary, dict):
            inventoryGamepassIds = [
                int(value)
                for value in list(report.inventorySummary.get("ownedGamepassIds") or [])
                if _positiveInt(value) > 0
            ]
        if inventoryGamepassIds:
            gamepassResult = await roblox.fetchRobloxGamepassesByIds(inventoryGamepassIds)
        else:
            gamepassResult = await roblox.fetchRobloxUserGamepasses(
                int(report.robloxUserId),
                maxPages=gamepassMaxPages,
            )
        report.gamepassSummary = gamepassResult.summary or {}
        report.ownedGamepasses = gamepassResult.gamepasses
        if gamepassResult.error:
            isPrivate = bgScanPipeline.isPrivateInventoryStatus(
                gamepassResult.status,
                gamepassResult.error,
            )
            report.gamepassScanStatus = "PRIVATE" if isPrivate else "ERROR"
            report.gamepassScanError = "Gamepass inventory is private or hidden." if isPrivate else gamepassResult.error
        else:
            report.gamepassScanStatus = "OK"
    elif isAdultRoute:
        report.gamepassScanStatus = "SKIPPED"

    if isAdultRoute and bool(getattr(configModule, "bgIntelligenceFetchFavoriteGamesEnabled", True)):
        await _emitProgress(progressCallback, "Checking favorite games...")
        gameResult = await roblox.fetchRobloxFavoriteGames(
            int(report.robloxUserId),
            maxGames=int(getattr(configModule, "bgIntelligenceFavoriteGameMax", 25) or 25),
        )
        if gameResult.error:
            report.favoriteGameScanStatus = "ERROR"
            report.favoriteGameScanError = gameResult.error
        else:
            report.favoriteGameScanStatus = "OK"
            report.favoriteGames = gameResult.games
            report.flaggedFavoriteGames = _analyzeFavoriteGames(games=gameResult.games, rules=rules)
    elif isAdultRoute:
        report.favoriteGameScanStatus = "SKIPPED"

    outfitEnabled = bool(getattr(configModule, "robloxOutfitScanEnabled", True)) and bool(
        getattr(configModule, "bgIntelligenceFetchOutfitsEnabled", True)
    )
    if isAdultRoute and outfitEnabled:
        await _emitProgress(progressCallback, "Checking saved outfits...")
        outfitResult = await roblox.fetchRobloxUserOutfits(
            int(report.robloxUserId),
            maxOutfits=int(getattr(configModule, "bgIntelligenceOutfitMax", 25) or 25),
            maxPages=int(getattr(configModule, "robloxOutfitMaxPages", 20) or 20),
        )
        if outfitResult.error:
            report.outfitScanStatus = "ERROR"
            report.outfitScanError = outfitResult.error
        else:
            report.outfitScanStatus = "OK"
            report.outfits = outfitResult.outfits
    elif isAdultRoute:
        report.outfitScanStatus = "SKIPPED"

    if bool(getattr(configModule, "bgIntelligenceFetchBadgeHistoryEnabled", True)):
        await _emitProgress(progressCallback, "Collecting the full badge timeline...")
        try:
            badgeHistoryMaxPages = int(getattr(configModule, "bgIntelligenceBadgeHistoryMaxPages", 0))
        except (TypeError, ValueError):
            badgeHistoryMaxPages = 0
        badgeHistoryResult = await roblox.fetchRobloxUserBadges(
            int(report.robloxUserId),
            limit=int(getattr(configModule, "bgIntelligenceBadgeHistoryPageSize", 100) or 100),
            maxPages=badgeHistoryMaxPages,
        )
        badgeHistoryComplete = not bool(badgeHistoryResult.nextCursor)
        if badgeHistoryResult.error:
            report.badgeHistoryScanStatus = "ERROR"
            report.badgeHistoryScanError = badgeHistoryResult.error
            if badgeHistoryResult.badges:
                report.badgeHistorySample = badgeHistoryResult.badges
                report.badgeTimelineSummary = _buildBadgeTimelineSummary(
                    badgeHistoryResult.badges,
                    awardDateStatus="SKIPPED",
                    awardDateError=badgeHistoryResult.error,
                    historyComplete=badgeHistoryComplete,
                    historyNextCursor=badgeHistoryResult.nextCursor,
                )
        else:
            report.badgeHistoryScanStatus = "OK"
            report.badgeHistorySample = badgeHistoryResult.badges
            badgeIds = {
                badgeId
                for badgeId in (_badgeIdFromSample(badge) for badge in badgeHistoryResult.badges)
                if badgeId is not None
            }
            if badgeIds:
                awardResult = await roblox.fetchRobloxBadgeAwards(
                    int(report.robloxUserId),
                    badgeIds,
                    batchSize=int(getattr(configModule, "robloxBadgeScanBatchSize", 50) or 50),
                )
                if awardResult.error:
                    if awardResult.badges:
                        _mergeBadgeAwardDates(badgeHistoryResult.badges, awardResult.badges)
                    report.badgeTimelineSummary = _buildBadgeTimelineSummary(
                        badgeHistoryResult.badges,
                        awardDateStatus="PARTIAL" if awardResult.badges else "ERROR",
                        awardDateError=awardResult.error,
                        historyComplete=badgeHistoryComplete,
                        historyNextCursor=badgeHistoryResult.nextCursor,
                    )
                else:
                    _mergeBadgeAwardDates(badgeHistoryResult.badges, awardResult.badges)
                    report.badgeTimelineSummary = _buildBadgeTimelineSummary(
                        badgeHistoryResult.badges,
                        awardDateStatus="OK",
                        historyComplete=badgeHistoryComplete,
                        historyNextCursor=badgeHistoryResult.nextCursor,
                    )
            else:
                report.badgeTimelineSummary = _buildBadgeTimelineSummary(
                    badgeHistoryResult.badges,
                    awardDateStatus="OK",
                    historyComplete=badgeHistoryComplete,
                    historyNextCursor=badgeHistoryResult.nextCursor,
                )
    else:
        report.badgeHistoryScanStatus = "SKIPPED"
        report.badgeTimelineSummary = _buildBadgeTimelineSummary([], awardDateStatus="SKIPPED")

    badgeEnabled = bool(getattr(configModule, "robloxBadgeScanEnabled", True)) and bool(
        getattr(configModule, "bgIntelligenceFetchBadgesEnabled", True)
    )
    if rules.badgeIds and badgeEnabled:
        historyComplete = bool((report.badgeTimelineSummary or {}).get("historyComplete"))
        if report.badgeHistoryScanStatus == "OK" and historyComplete and report.badgeHistorySample:
            await _emitProgress(progressCallback, "Checking configured badge records...")
            report.badgeScanStatus = "OK"
            flaggedBadges: list[dict[str, Any]] = []
            for badge in list(report.badgeHistorySample or []):
                if not isinstance(badge, dict):
                    continue
                badgeId = _badgeIdFromSample(badge)
                if badgeId is None or int(badgeId) not in rules.badgeIds:
                    continue
                entry = {
                    "badgeId": badgeId,
                    "awardedDate": badge.get("awardedDate"),
                }
                note = rules.badgeNotes.get(int(badgeId))
                if note:
                    entry["note"] = note
                flaggedBadges.append(entry)
            report.flaggedBadges = flaggedBadges
        else:
            await _emitProgress(progressCallback, "Checking configured badge records...")
            badgeResult = await roblox.fetchRobloxBadgeAwards(
                int(report.robloxUserId),
                rules.badgeIds,
                batchSize=int(getattr(configModule, "robloxBadgeScanBatchSize", 50) or 50),
            )
            if badgeResult.error:
                report.badgeScanStatus = "ERROR"
                report.badgeScanError = badgeResult.error
            else:
                report.badgeScanStatus = "OK"
                flaggedBadges = []
                for badge in badgeResult.badges:
                    badgeId = badge.get("badgeId")
                    if badgeId is None:
                        continue
                    entry = {
                        "badgeId": badgeId,
                        "awardedDate": badge.get("awardedDate"),
                    }
                    note = rules.badgeNotes.get(int(badgeId))
                    if note:
                        entry["note"] = note
                    flaggedBadges.append(entry)
                report.flaggedBadges = flaggedBadges
    else:
        report.badgeScanStatus = "SKIPPED"

    await _emitProgress(progressCallback, "Loading recent local BG context...")
    report.priorReportSummary = await _loadPriorReportSummary(
        guildId=int(guildId),
        targetUserId=int(report.discordUserId or 0),
        robloxUserId=int(report.robloxUserId) if report.robloxUserId else None,
        limit=5,
    )

    return report


async def buildReport(
    member: discord.Member,
    *,
    guild: discord.Guild,
    reviewBucketOverride: str = "auto",
    robloxUserIdOverride: int | None = None,
    robloxUsernameOverride: str | None = None,
    roverGuildId: int | None = None,
    notifyPrivateInventory: bool = False,
    reviewer: discord.User | discord.Member | None = None,
    configModule: Any = config,
    progressCallback: ProgressCallback | None = None,
) -> BgIntelligenceReport:
    await _emitProgress(progressCallback, "Loading scan rules and review route...")
    reviewBucket, reviewBucketSource = await resolveReviewBucket(
        member,
        guildId=int(guild.id),
        reviewBucketOverride=reviewBucketOverride,
        configModule=configModule,
    )
    rules = await loadFlagRules(configModule=configModule)
    report = BgIntelligenceReport(
        discordUserId=int(member.id),
        discordDisplayName=str(member.display_name),
        discordUsername=str(member),
        reviewBucket=reviewBucket,
        reviewBucketSource=reviewBucketSource,
        groups=[],
        flaggedGroups=[],
        flagMatches=[],
        directMatches=[],
        flaggedItems=[],
        ownedGamepasses=[],
        favoriteGames=[],
        flaggedFavoriteGames=[],
        outfits=[],
        flaggedBadges=[],
    )

    await _emitProgress(progressCallback, "Checking RoVer for the linked Roblox account...")
    roverResult, roverLookupGuildId = await _lookupRoverForGuild(
        int(member.id),
        guildId=_positiveInt(roverGuildId) or int(guild.id),
        configModule=configModule,
    )
    roverSource = _roverSourceName(roverLookupGuildId, configModule=configModule)
    manualRobloxUserId = 0
    try:
        manualRobloxUserId = int(robloxUserIdOverride or 0)
    except (TypeError, ValueError):
        manualRobloxUserId = 0
    manualRobloxUsername = str(robloxUsernameOverride or "").strip()
    if manualRobloxUserId > 0:
        report.identitySource = "manual"
        report.robloxUserId = manualRobloxUserId
        if roverResult.robloxId and int(roverResult.robloxId) == manualRobloxUserId:
            report.robloxUsername = roverResult.robloxUsername
        elif roverResult.robloxId:
            report.roverError = f"RoVer on {roverSource} linked to {roverResult.robloxId}; manual override {manualRobloxUserId} used."
        elif roverResult.error:
            report.roverError = f"Manual override used. RoVer on {roverSource} note: {roverResult.error}"
    elif manualRobloxUsername:
        report.identitySource = "manual_username"
        await _emitProgress(progressCallback, "Resolving the Roblox username override...")
        usernameResult = await roblox.fetchRobloxUserByUsername(manualRobloxUsername)
        report.robloxUserId = usernameResult.robloxId
        report.robloxUsername = usernameResult.robloxUsername or manualRobloxUsername
        if roverResult.robloxId:
            report.roverError = f"RoVer on {roverSource} linked to {roverResult.robloxId}; username override {manualRobloxUsername} used."
        elif usernameResult.error:
            report.roverError = usernameResult.error
        elif roverResult.error:
            report.roverError = f"Username override used. RoVer on {roverSource} note: {roverResult.error}"
    else:
        report.identitySource = _roverIdentitySource(
            roverGuildId=roverLookupGuildId,
            scanGuildId=int(guild.id),
            configModule=configModule,
        )
        report.robloxUserId = roverResult.robloxId
        report.robloxUsername = roverResult.robloxUsername
        report.roverError = roverResult.error

    return await _completeReportScan(
        report,
        guildId=int(guild.id),
        rules=rules,
        member=member,
        notifyPrivateInventory=notifyPrivateInventory,
        reviewer=reviewer,
        configModule=configModule,
        progressCallback=progressCallback,
    )


async def buildReportForDiscordId(
    *,
    guild: discord.Guild,
    discordUserId: int,
    displayMember: discord.Member | None = None,
    roverGuildId: int | None = None,
    robloxUsernameOverride: str | None = None,
    reviewBucketOverride: str = "auto",
    configModule: Any = config,
    progressCallback: ProgressCallback | None = None,
) -> BgIntelligenceReport:
    await _emitProgress(progressCallback, "Loading scan rules and review route...")
    normalizedOverride = str(reviewBucketOverride or "auto").strip().lower()
    if normalizedOverride and normalizedOverride != "auto":
        reviewBucket = normalizeBgReviewBucket(normalizedOverride)
        reviewBucketSource = "manual"
    else:
        reviewBucket = adultBgReviewBucket
        reviewBucketSource = "discord_identity_default"

    rules = await loadFlagRules(configModule=configModule)
    cleanDiscordUserId = int(discordUserId or 0)
    displayName = str(getattr(displayMember, "display_name", "") or "").strip()
    displayUsername = str(displayMember) if displayMember is not None else ""
    report = BgIntelligenceReport(
        discordUserId=cleanDiscordUserId,
        discordDisplayName=displayName or (f"Discord ID {cleanDiscordUserId}" if cleanDiscordUserId > 0 else "Discord User"),
        discordUsername=displayUsername,
        reviewBucket=reviewBucket,
        reviewBucketSource=reviewBucketSource,
        groups=[],
        flaggedGroups=[],
        flagMatches=[],
        directMatches=[],
        flaggedItems=[],
        ownedGamepasses=[],
        favoriteGames=[],
        flaggedFavoriteGames=[],
        outfits=[],
        flaggedBadges=[],
    )

    roverResult = None
    roverLookupGuildId = 0
    roverSource = "configured guild"
    if cleanDiscordUserId > 0:
        await _emitProgress(progressCallback, "Checking RoVer for the linked Roblox account...")
        roverResult, roverLookupGuildId = await _lookupRoverForGuild(
            cleanDiscordUserId,
            guildId=_positiveInt(roverGuildId) or int(guild.id),
            configModule=configModule,
        )
        roverSource = _roverSourceName(roverLookupGuildId, configModule=configModule)
    manualRobloxUsername = str(robloxUsernameOverride or "").strip()
    if manualRobloxUsername:
        report.identitySource = "manual_username"
        await _emitProgress(progressCallback, "Resolving the Roblox username override...")
        usernameResult = await roblox.fetchRobloxUserByUsername(manualRobloxUsername)
        report.robloxUserId = usernameResult.robloxId
        report.robloxUsername = usernameResult.robloxUsername or manualRobloxUsername
        if roverResult is not None and roverResult.robloxId:
            report.roverError = f"RoVer on {roverSource} linked to {roverResult.robloxId}; username override {manualRobloxUsername} used."
        elif usernameResult.error:
            report.roverError = usernameResult.error
        elif roverResult is not None and roverResult.error:
            report.roverError = f"Username override used. RoVer on {roverSource} note: {roverResult.error}"
    else:
        report.identitySource = _roverIdentitySource(
            roverGuildId=roverLookupGuildId,
            scanGuildId=int(guild.id),
            configModule=configModule,
        )
        if roverResult is None:
            report.roverError = "No Discord ID supplied."
        else:
            report.robloxUserId = roverResult.robloxId
            report.robloxUsername = roverResult.robloxUsername
            report.roverError = roverResult.error

    return await _completeReportScan(
        report,
        guildId=int(guild.id),
        rules=rules,
        member=None,
        notifyPrivateInventory=False,
        reviewer=None,
        configModule=configModule,
        progressCallback=progressCallback,
    )


async def buildReportForRobloxIdentity(
    *,
    guild: discord.Guild,
    robloxUserId: int | None = None,
    robloxUsername: str | None = None,
    reviewBucketOverride: str = "auto",
    configModule: Any = config,
    progressCallback: ProgressCallback | None = None,
) -> BgIntelligenceReport:
    await _emitProgress(progressCallback, "Loading scan rules and review route...")
    normalizedOverride = str(reviewBucketOverride or "auto").strip().lower()
    if normalizedOverride and normalizedOverride != "auto":
        reviewBucket = normalizeBgReviewBucket(normalizedOverride)
        reviewBucketSource = "manual"
    else:
        reviewBucket = adultBgReviewBucket
        reviewBucketSource = "roblox_identity_default"

    rules = await loadFlagRules(configModule=configModule)
    report = BgIntelligenceReport(
        discordUserId=0,
        discordDisplayName=str(robloxUsername or robloxUserId or "Roblox User"),
        discordUsername="",
        reviewBucket=reviewBucket,
        reviewBucketSource=reviewBucketSource,
        identitySource="manual",
        groups=[],
        flaggedGroups=[],
        flagMatches=[],
        directMatches=[],
        flaggedItems=[],
        ownedGamepasses=[],
        favoriteGames=[],
        flaggedFavoriteGames=[],
        outfits=[],
        flaggedBadges=[],
    )

    manualRobloxUserId = 0
    try:
        manualRobloxUserId = int(robloxUserId or 0)
    except (TypeError, ValueError):
        manualRobloxUserId = 0
    manualRobloxUsername = str(robloxUsername or "").strip()
    if manualRobloxUserId > 0:
        report.robloxUserId = manualRobloxUserId
        report.robloxUsername = manualRobloxUsername or None
    elif manualRobloxUsername:
        report.identitySource = "manual_username"
        await _emitProgress(progressCallback, "Resolving the Roblox username...")
        usernameResult = await roblox.fetchRobloxUserByUsername(manualRobloxUsername)
        report.robloxUserId = usernameResult.robloxId
        report.robloxUsername = usernameResult.robloxUsername or manualRobloxUsername
        report.roverError = usernameResult.error
    else:
        report.roverError = "No Discord member, Discord ID, or Roblox username supplied."

    return await _completeReportScan(
        report,
        guildId=int(guild.id),
        rules=rules,
        member=None,
        notifyPrivateInventory=False,
        reviewer=None,
        configModule=configModule,
        progressCallback=progressCallback,
    )


def _safeJson(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=True, default=str)
    except Exception:
        return "{}"


async def _loadPriorReportSummary(
    *,
    guildId: int,
    targetUserId: int,
    robloxUserId: int | None,
    limit: int = 5,
) -> dict[str, Any]:
    clauses = ["guildId = ?"]
    params: list[Any] = [int(guildId)]
    targetClauses: list[str] = []
    if int(targetUserId or 0) > 0:
        targetClauses.append("targetUserId = ?")
        params.append(int(targetUserId))
    if robloxUserId is not None and int(robloxUserId or 0) > 0:
        targetClauses.append("robloxUserId = ?")
        params.append(int(robloxUserId))
    if not targetClauses:
        return {
            "totalRecent": 0,
            "rows": [],
            "queueApprovals": 0,
            "queueRejections": 0,
        }

    params.append(max(1, min(int(limit or 5), 10)))
    rows = await fetchAll(
        f"""
        SELECT reportId, targetUserId, robloxUserId, score, band, confidence,
               scored, outcome, hardMinimum, createdAt
        FROM bg_intelligence_reports
        WHERE {" AND ".join(clauses)} AND ({" OR ".join(targetClauses)})
        ORDER BY datetime(createdAt) DESC, reportId DESC
        LIMIT ?
        """,
        tuple(params),
    )

    queueApprovals = 0
    queueRejections = 0
    queueClauses: list[str] = []
    queueParams: list[Any] = []
    if int(targetUserId or 0) > 0:
        queueClauses.append("userId = ?")
        queueParams.append(int(targetUserId))
    if robloxUserId is not None and int(robloxUserId or 0) > 0:
        queueClauses.append("robloxUserId = ?")
        queueParams.append(int(robloxUserId))
    if queueClauses:
        queueRows = await fetchAll(
            f"""
            SELECT UPPER(bgStatus) AS status, COUNT(*) AS total
            FROM attendees
            WHERE {" OR ".join(queueClauses)}
            GROUP BY UPPER(bgStatus)
            """,
            tuple(queueParams),
        )
        for row in queueRows:
            status = str(row.get("status") or "").upper()
            total = int(row.get("total") or 0)
            if status == "APPROVED":
                queueApprovals += total
            elif status == "REJECTED":
                queueRejections += total

    scoredRows = [row for row in rows if int(row.get("scored", 1) or 0) == 1]
    noScoreRows = [row for row in rows if int(row.get("scored", 1) or 0) == 0]
    highRiskRows = [
        row
        for row in scoredRows
        if int(row.get("score") or 0) >= 60 or str(row.get("band") or "").lower() in {"high risk", "escalate"}
    ]
    escalateRows = [
        row
        for row in scoredRows
        if int(row.get("score") or 0) >= 80 or str(row.get("band") or "").lower() == "escalate"
    ]
    lastRow = rows[0] if rows else {}
    return {
        "totalRecent": len(rows),
        "scoredRecent": len(scoredRows),
        "noScoreRecent": len(noScoreRows),
        "highRiskRecent": len(highRiskRows),
        "escalateRecent": len(escalateRows),
        "lastScore": lastRow.get("score") if lastRow else None,
        "lastBand": lastRow.get("band") if lastRow else None,
        "lastOutcome": lastRow.get("outcome") if lastRow else None,
        "lastCreatedAt": lastRow.get("createdAt") if lastRow else None,
        "queueApprovals": queueApprovals,
        "queueRejections": queueRejections,
        "rows": rows,
    }


def reportToDict(report: BgIntelligenceReport) -> dict[str, Any]:
    return {
        "discordUserId": int(report.discordUserId),
        "discordDisplayName": report.discordDisplayName,
        "discordUsername": report.discordUsername,
        "reviewBucket": report.reviewBucket,
        "reviewBucketSource": report.reviewBucketSource,
        "identitySource": report.identitySource,
        "robloxUserId": report.robloxUserId,
        "robloxUsername": report.robloxUsername,
        "roverError": report.roverError,
        "robloxCreated": report.robloxCreated,
        "robloxAgeDays": report.robloxAgeDays,
        "groupSummary": report.groupSummary or {},
        "groupScanStatus": report.groupScanStatus,
        "groupScanError": report.groupScanError,
        "connectionScanStatus": report.connectionScanStatus,
        "connectionScanError": report.connectionScanError,
        "connectionSummary": report.connectionSummary or {},
        "groups": report.groups or [],
        "flaggedGroups": report.flaggedGroups or [],
        "flagMatches": report.flagMatches or [],
        "directMatches": report.directMatches or [],
        "inventoryScanStatus": report.inventoryScanStatus,
        "inventoryScanError": report.inventoryScanError,
        "inventorySummary": report.inventorySummary or {},
        "flaggedItems": report.flaggedItems or [],
        "gamepassScanStatus": report.gamepassScanStatus,
        "gamepassScanError": report.gamepassScanError,
        "gamepassSummary": report.gamepassSummary or {},
        "ownedGamepasses": report.ownedGamepasses or [],
        "favoriteGameScanStatus": report.favoriteGameScanStatus,
        "favoriteGameScanError": report.favoriteGameScanError,
        "favoriteGames": report.favoriteGames or [],
        "flaggedFavoriteGames": report.flaggedFavoriteGames or [],
        "outfitScanStatus": report.outfitScanStatus,
        "outfitScanError": report.outfitScanError,
        "outfits": report.outfits or [],
        "badgeHistoryScanStatus": report.badgeHistoryScanStatus,
        "badgeHistoryScanError": report.badgeHistoryScanError,
        "badgeHistorySample": report.badgeHistorySample or [],
        "badgeTimelineSummary": report.badgeTimelineSummary or {},
        "badgeScanStatus": report.badgeScanStatus,
        "badgeScanError": report.badgeScanError,
        "flaggedBadges": report.flaggedBadges or [],
        "externalSourceStatus": report.externalSourceStatus,
        "externalSourceError": report.externalSourceError,
        "externalSourceMatches": report.externalSourceMatches or [],
        "externalSourceDetails": report.externalSourceDetails or [],
        "priorReportSummary": report.priorReportSummary or {},
        "privateInventoryDmSent": report.privateInventoryDmSent,
    }


async def recordReport(
    *,
    guildId: int,
    channelId: int,
    reviewerId: int,
    report: BgIntelligenceReport,
    riskScore: scoring.RiskScore,
) -> int:
    signalRows = [
        {
            "label": signal.label,
            "points": int(signal.points),
            "kind": signal.kind,
        }
        for signal in list(riskScore.signals or [])
    ]
    return await executeReturnId(
        """
        INSERT INTO bg_intelligence_reports (
            guildId, channelId, reviewerId, targetUserId,
            robloxUserId, robloxUsername, reviewBucket,
            score, band, confidence, scored, outcome, hardMinimum, signalJson, reportJson
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(guildId),
            int(channelId),
            int(reviewerId),
            int(report.discordUserId),
            report.robloxUserId,
            report.robloxUsername,
            str(report.reviewBucket or ""),
            int(riskScore.score),
            str(riskScore.band),
            int(riskScore.confidence),
            1 if bool(riskScore.scored) else 0,
            str(riskScore.outcome or "scored"),
            int(riskScore.hardMinimum or 0),
            _safeJson(signalRows),
            _safeJson(reportToDict(report)),
        ),
    )


async def listRecentReports(
    *,
    guildId: int,
    targetUserId: int | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    normalizedLimit = max(1, min(int(limit or 5), 20))
    if targetUserId is not None and int(targetUserId) > 0:
        return await fetchAll(
            """
            SELECT *
            FROM bg_intelligence_reports
            WHERE guildId = ? AND targetUserId = ?
            ORDER BY datetime(createdAt) DESC, reportId DESC
            LIMIT ?
            """,
            (int(guildId), int(targetUserId), normalizedLimit),
        )
    return await fetchAll(
        """
        SELECT *
        FROM bg_intelligence_reports
        WHERE guildId = ?
        ORDER BY datetime(createdAt) DESC, reportId DESC
        LIMIT ?
        """,
        (int(guildId), normalizedLimit),
    )
