from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone, timedelta

import aiohttp

import config
from runtime import taskBudgeter


@dataclass
class RoverLookupResult:
    robloxId: Optional[int]
    robloxUsername: Optional[str]
    error: Optional[str] = None


@dataclass
class RobloxAcceptResult:
    ok: bool
    status: int
    error: Optional[str] = None


@dataclass
class RobloxGroupsResult:
    groups: list[dict]
    status: int
    error: Optional[str] = None


@dataclass
class RobloxInventoryResult:
    items: list[dict]
    status: int
    error: Optional[str] = None

@dataclass
class RobloxFavoriteGamesResult:
    games: list[dict]
    status: int
    error: Optional[str] = None

@dataclass
class RobloxBadgeAwardsResult:
    badges: list[dict]
    status: int
    error: Optional[str] = None

@dataclass
class RobloxUniverseBadgesResult:
    badges: list[dict]
    status: int
    nextCursor: Optional[str] = None
    error: Optional[str] = None

@dataclass
class RobloxUserProfileResult:
    created: Optional[str]
    status: int
    error: Optional[str] = None

@dataclass
class RobloxOutfitsResult:
    outfits: list[dict]
    status: int
    error: Optional[str] = None

@dataclass
class RobloxOutfitThumbnailsResult:
    thumbnails: list[dict]
    status: int
    error: Optional[str] = None


_httpSession: Optional[aiohttp.ClientSession] = None
_roverCache: dict[tuple[int, int], tuple[datetime, RoverLookupResult]] = {}


def _utcNow() -> datetime:
    return datetime.now(timezone.utc)


def _roverCacheTtlSec() -> int:
    try:
        value = int(getattr(config, "roverCacheTtlSec", 120) or 120)
    except (TypeError, ValueError):
        value = 120
    return max(0, min(value, 3600))


def _roverCacheKey(discordId: int, guildId: Optional[int]) -> tuple[int, int]:
    resolvedGuild = int(guildId or getattr(config, "serverId", 0) or 0)
    return int(discordId), resolvedGuild


def _roverCacheGet(discordId: int, guildId: Optional[int]) -> Optional[RoverLookupResult]:
    ttlSec = _roverCacheTtlSec()
    if ttlSec <= 0:
        return None
    key = _roverCacheKey(discordId, guildId)
    cached = _roverCache.get(key)
    if not cached:
        return None
    cachedAt, cachedValue = cached
    if _utcNow() - cachedAt > timedelta(seconds=ttlSec):
        _roverCache.pop(key, None)
        return None
    return cachedValue


def _roverCacheSet(discordId: int, guildId: Optional[int], result: RoverLookupResult) -> None:
    ttlSec = _roverCacheTtlSec()
    if ttlSec <= 0:
        return
    now = _utcNow()
    if _roverCache:
        cutoff = now - timedelta(seconds=ttlSec)
        staleKeys = [key for key, (cachedAt, _) in _roverCache.items() if cachedAt < cutoff]
        for key in staleKeys:
            _roverCache.pop(key, None)

    maxEntries = int(getattr(config, "roverCacheMaxEntries", 2000) or 2000)
    if maxEntries > 0 and len(_roverCache) >= maxEntries:
        oldestKey = min(_roverCache.items(), key=lambda item: item[1][0])[0]
        _roverCache.pop(oldestKey, None)

    _roverCache[_roverCacheKey(discordId, guildId)] = (now, result)


async def _getHttpSession() -> aiohttp.ClientSession:
    global _httpSession
    if _httpSession is None or _httpSession.closed:
        timeoutSec = int(getattr(config, "robloxHttpTimeoutSec", 10) or 10)
        timeout = aiohttp.ClientTimeout(total=max(3, timeoutSec))
        _httpSession = aiohttp.ClientSession(timeout=timeout)
    return _httpSession


async def closeHttpSession() -> None:
    global _httpSession
    if _httpSession is None:
        return
    if not _httpSession.closed:
        await _httpSession.close()
    _httpSession = None


async def _requestJson(
    method: str,
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    params: Optional[dict] = None,
    timeoutSec: int = 10,
    jsonBody: Optional[dict] = None,
) -> tuple[int, object]:
    session = await _getHttpSession()
    timeout = aiohttp.ClientTimeout(total=max(3, int(timeoutSec or 10)))
    maxRetryCount = max(0, int(getattr(config, "robloxApi429MaxRetries", 2) or 2))
    baseDelaySec = max(0.1, float(getattr(config, "robloxApi429RetryDelaySec", 1.0) or 1.0))

    for attempt in range(maxRetryCount + 1):
        async def _runRequest() -> tuple[int, object, dict[str, str]]:
            async with session.request(
                method.upper(),
                url,
                headers=headers,
                params=params,
                json=jsonBody,
                timeout=timeout,
            ) as resp:
                status = resp.status
                try:
                    payload = await resp.json(content_type=None)
                except Exception:
                    payload = None
                responseHeaders = dict(resp.headers or {})
                return status, payload, responseHeaders

        status, payload, responseHeaders = await taskBudgeter.runRoblox(_runRequest)

        if status != 429 or attempt >= maxRetryCount:
            return status, payload

        retryAfterSec = 0.0
        retryAfterHeader = responseHeaders.get("Retry-After")
        if retryAfterHeader:
            try:
                retryAfterSec = float(retryAfterHeader)
            except (TypeError, ValueError):
                retryAfterSec = 0.0
        if isinstance(payload, dict):
            payloadRetry = payload.get("retry_after") or payload.get("retryAfter")
            if payloadRetry is not None:
                try:
                    retryAfterSec = max(retryAfterSec, float(payloadRetry))
                except (TypeError, ValueError):
                    pass

        await asyncio.sleep(max(baseDelaySec * (attempt + 1), retryAfterSec))

    return 429, None


def _roverUrl(discordId: int, guildId: Optional[int] = None) -> str:
    base = getattr(config, "roverApiBaseUrl", "https://verify.eryn.io/api/user/").rstrip("/")
    if "{discordId}" in base or "{guildId}" in base:
        resolvedGuild = guildId or getattr(config, "serverId", None) or ""
        return base.format(discordId=discordId, guildId=resolvedGuild)
    return f"{base}/{discordId}"


def _extractRobloxFields(payload: dict) -> tuple[Optional[int], Optional[str]]:
    def _pick(obj: dict) -> tuple[Optional[int], Optional[str]]:
        candidates = [
            ("robloxId", obj.get("robloxId")),
            ("roblox_id", obj.get("roblox_id")),
            ("robloxID", obj.get("robloxID")),
            ("id", obj.get("id")),
        ]
        robloxId = None
        for _, value in candidates:
            try:
                if value is not None:
                    robloxId = int(value)
                    break
            except (TypeError, ValueError):
                continue
        username = (
            obj.get("robloxUsername")
            or obj.get("roblox_username")
            or obj.get("cachedUsername")
            or obj.get("username")
        )
        if isinstance(username, str) and not username:
            username = None
        return robloxId, username if isinstance(username, str) else None

    robloxId, username = _pick(payload)
    if robloxId or username:
        return robloxId, username
    data = payload.get("data")
    if isinstance(data, dict):
        return _pick(data)
    return None, None


async def fetchRobloxUser(discordId: int, guildId: Optional[int] = None) -> RoverLookupResult:
    cached = _roverCacheGet(discordId, guildId)
    if cached is not None:
        return cached

    url = _roverUrl(discordId, guildId=guildId)
    headers: dict[str, str] = {}
    apiKey = getattr(config, "roverApiKey", "") or ""
    if apiKey:
        headerName = getattr(config, "roverApiKeyHeader", "Authorization") or "Authorization"
        headerValue = apiKey
        if getattr(config, "roverApiKeyUseBearer", False) and not apiKey.lower().startswith("bearer "):
            headerValue = f"Bearer {apiKey}"
        headers[headerName] = headerValue
        extraHeader = getattr(config, "roverApiKeyHeaderAlt", "") or ""
        if extraHeader:
            headers[extraHeader] = headerValue

    try:
        status, data = await _requestJson("GET", url, headers=headers, timeoutSec=10)
    except Exception as exc:
        result = RoverLookupResult(None, None, error=str(exc))
        _roverCacheSet(discordId, guildId, result)
        return result

    if status != 200 or not isinstance(data, dict):
        result = RoverLookupResult(None, None, error=f"Rover lookup failed ({status}).")
        _roverCacheSet(discordId, guildId, result)
        return result

    if data.get("status") == "error" or data.get("success") is False:
        result = RoverLookupResult(None, None, error=str(data.get("message") or "Rover lookup error."))
        _roverCacheSet(discordId, guildId, result)
        return result

    robloxId, username = _extractRobloxFields(data)

    if not robloxId:
        result = RoverLookupResult(None, username, error="No Roblox account linked via RoVer.")
        _roverCacheSet(discordId, guildId, result)
        return result

    result = RoverLookupResult(robloxId, username)
    _roverCacheSet(discordId, guildId, result)
    return result


async def acceptJoinRequest(robloxUserId: int) -> RobloxAcceptResult:
    groupId = getattr(config, "robloxGroupId", 0)
    return await acceptJoinRequestForGroup(robloxUserId, int(groupId or 0))


async def acceptJoinRequestForGroup(robloxUserId: int, groupId: int) -> RobloxAcceptResult:
    apiKey = getattr(config, "robloxOpenCloudApiKey", "") or ""
    if not groupId or not apiKey:
        return RobloxAcceptResult(False, 0, error="Missing Roblox Open Cloud configuration.")

    url = f"https://apis.roblox.com/cloud/v2/groups/{groupId}/join-requests/{robloxUserId}:accept"
    headers = {"x-api-key": apiKey}

    try:
        # Roblox's accept endpoint expects JSON content-type.
        status, payload = await _requestJson(
            "POST",
            url,
            headers=headers,
            timeoutSec=10,
            jsonBody={},
        )
        if 200 <= status < 300:
            return RobloxAcceptResult(True, status)
    except Exception as exc:
        return RobloxAcceptResult(False, 0, error=str(exc))

    errorMsg = None
    if isinstance(payload, dict):
        errorMsg = payload.get("message") or payload.get("error")
        if not errorMsg and isinstance(payload.get("errors"), list) and payload["errors"]:
            errorMsg = payload["errors"][0].get("message")
    if not errorMsg:
        errorMsg = f"Roblox API error ({status})."

    return RobloxAcceptResult(False, status, error=errorMsg)


async def fetchRobloxGroups(robloxUserId: int) -> RobloxGroupsResult:
    url = f"https://groups.roblox.com/v2/users/{robloxUserId}/groups/roles"
    try:
        status, data = await _requestJson("GET", url, timeoutSec=10)
    except Exception as exc:
        return RobloxGroupsResult([], 0, error=str(exc))

    if status != 200 or not isinstance(data, dict):
        return RobloxGroupsResult([], status, error=f"Roblox groups lookup failed ({status}).")

    rawGroups = data.get("data", [])
    if not isinstance(rawGroups, list):
        return RobloxGroupsResult([], status, error="Roblox groups lookup returned invalid data.")

    groups: list[dict] = []
    for entry in rawGroups:
        group = entry.get("group") if isinstance(entry, dict) else None
        role = entry.get("role") if isinstance(entry, dict) else None
        if not isinstance(group, dict):
            continue
        groupId = group.get("id")
        groupName = group.get("name")
        roleName = role.get("name") if isinstance(role, dict) else None
        rank = role.get("rank") if isinstance(role, dict) else None
        try:
            groupId = int(groupId) if groupId is not None else None
        except (TypeError, ValueError):
            groupId = None
        groups.append(
            {
                "id": groupId,
                "name": groupName,
                "role": roleName,
                "rank": rank,
            }
        )

    return RobloxGroupsResult(groups, status)


def _extractAssetId(item: dict) -> Optional[int]:
    candidates = [
        item.get("assetId"),
        item.get("asset_id"),
        item.get("id"),
    ]
    asset = item.get("asset") if isinstance(item.get("asset"), dict) else None
    if asset:
        candidates.extend([asset.get("id"), asset.get("assetId")])
    for value in candidates:
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _extractAssetName(item: dict) -> Optional[str]:
    for key in ("name", "assetName", "displayName"):
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
    asset = item.get("asset") if isinstance(item.get("asset"), dict) else None
    if asset:
        value = asset.get("name")
        if isinstance(value, str) and value:
            return value
    return None


def _extractCreatorId(item: dict) -> Optional[int]:
    candidates = [
        item.get("creatorId"),
        item.get("creator_id"),
        item.get("creatorTargetId"),
    ]
    creator = item.get("creator") if isinstance(item.get("creator"), dict) else None
    if creator:
        candidates.extend([creator.get("id"), creator.get("creatorId")])
    for value in candidates:
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _extractCreatorName(item: dict) -> Optional[str]:
    creator = item.get("creator") if isinstance(item.get("creator"), dict) else None
    if creator:
        value = creator.get("name")
        if isinstance(value, str) and value:
            return value
    return None


async def fetchRobloxInventory(
    robloxUserId: int,
    targetItemIds: Optional[set[int]] = None,
    targetCreatorIds: Optional[set[int]] = None,
    targetKeywords: Optional[list[str]] = None,
    maxPages: int = 5,
) -> RobloxInventoryResult:
    apiKey = getattr(config, "robloxInventoryApiKey", "") or getattr(config, "robloxOpenCloudApiKey", "")
    if not apiKey:
        return RobloxInventoryResult([], 0, error="Missing Roblox Open Cloud API key for inventory.")

    url = f"https://apis.roblox.com/cloud/v2/users/{robloxUserId}/inventory-items"
    headers = {"x-api-key": apiKey}
    params = {"maxPageSize": "100"}
    items: list[dict] = []
    page = 0
    remaining = set(targetItemIds) if targetItemIds else None
    creatorIds = set(targetCreatorIds) if targetCreatorIds else set()
    keywords = [
        str(value).strip().lower()
        for value in (targetKeywords or [])
        if str(value).strip()
    ]
    probeOnly = remaining is None and not creatorIds and not keywords

    try:
        while True:
            if page >= maxPages:
                break
            status, data = await _requestJson("GET", url, headers=headers, params=params, timeoutSec=10)
            if status != 200 or not isinstance(data, dict):
                detail = None
                if isinstance(data, dict):
                    detail = data.get("message") or data.get("error")
                    if not detail and isinstance(data.get("errors"), list) and data["errors"]:
                        first = data["errors"][0]
                        if isinstance(first, dict):
                            detail = first.get("message") or first.get("error")
                        elif isinstance(first, str):
                            detail = first
                if detail:
                    return RobloxInventoryResult(
                        items,
                        status,
                        error=f"Inventory lookup failed ({status}): {detail}",
                    )
                return RobloxInventoryResult(items, status, error=f"Inventory lookup failed ({status}).")

            if probeOnly:
                return RobloxInventoryResult([], status)

            rawItems = data.get("inventoryItems") or data.get("items") or []
            if isinstance(rawItems, list):
                for raw in rawItems:
                    if not isinstance(raw, dict):
                        continue
                    assetId = _extractAssetId(raw)
                    if assetId is None:
                        continue
                    creatorId = _extractCreatorId(raw)
                    creatorName = _extractCreatorName(raw)
                    assetName = _extractAssetName(raw)
                    assetNameLower = assetName.lower() if isinstance(assetName, str) else ""
                    matchItem = remaining is not None and assetId in remaining
                    matchCreator = creatorId in creatorIds if creatorId is not None else False
                    matchKeyword = False
                    matchedKeyword = None
                    if keywords and assetNameLower:
                        for keyword in keywords:
                            if keyword in assetNameLower:
                                matchKeyword = True
                                matchedKeyword = keyword
                                break
                    if not matchItem and not matchCreator and not matchKeyword:
                        continue

                    if matchItem:
                        matchType = "item"
                    elif matchCreator:
                        matchType = "creator"
                    else:
                        matchType = "keyword"
                    items.append(
                        {
                            "id": assetId,
                            "name": assetName,
                            "creatorId": creatorId,
                            "creatorName": creatorName,
                            "matchType": matchType,
                            "keyword": matchedKeyword,
                        }
                    )
                    if remaining is not None and assetId in remaining:
                        remaining.discard(assetId)

            nextToken = data.get("nextPageToken")
            if remaining is not None and not remaining and not creatorIds:
                break
            if not nextToken:
                break
            params["pageToken"] = nextToken
            page += 1
    except Exception as exc:
        return RobloxInventoryResult(items, 0, error=str(exc))

    return RobloxInventoryResult(items, 200)


async def fetchRobloxFavoriteGames(
    robloxUserId: int,
    maxGames: int = 10,
) -> RobloxFavoriteGamesResult:
    try:
        normalizedUserId = int(robloxUserId)
    except (TypeError, ValueError):
        return RobloxFavoriteGamesResult([], 0, error="Favorite games lookup failed (invalid Roblox user ID).")

    if normalizedUserId <= 0:
        return RobloxFavoriteGamesResult([], 0, error="Favorite games lookup failed (invalid Roblox user ID).")

    requestedLimit = max(1, min(int(maxGames or 10), 100))
    apiLimit = 100
    for candidateLimit in (10, 25, 50, 100):
        if requestedLimit <= candidateLimit:
            apiLimit = candidateLimit
            break
    url = f"https://games.roblox.com/v2/users/{normalizedUserId}/favorite/games"
    params = {"limit": str(apiLimit), "sortOrder": "Desc"}
    headers = {
        "User-Agent": str(
            getattr(
                config,
                "robloxPublicApiUserAgent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Jane-Clanker/1.0",
            )
        ),
        "Accept": "application/json",
    }

    try:
        status, data = await _requestJson(
            "GET",
            url,
            headers=headers,
            params=params,
            timeoutSec=10,
        )
    except Exception as exc:
        return RobloxFavoriteGamesResult([], 0, error=str(exc))

    # Some accounts intermittently reject explicit sortOrder. Retry once without it.
    if status == 400:
        try:
            statusNoSort, dataNoSort = await _requestJson(
                "GET",
                url,
                headers=headers,
                params={"limit": str(apiLimit)},
                timeoutSec=10,
            )
            if statusNoSort == 200:
                status, data = statusNoSort, dataNoSort
        except Exception:
            pass

    def _extractApiError(payload: object) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        rawErrors = payload.get("errors")
        if isinstance(rawErrors, list):
            parts: list[str] = []
            for entry in rawErrors:
                if isinstance(entry, dict):
                    message = entry.get("message")
                    if isinstance(message, str) and message.strip():
                        parts.append(message.strip())
                elif isinstance(entry, str) and entry.strip():
                    parts.append(entry.strip())
            if parts:
                return "; ".join(parts)
        message = payload.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
        return None

    if status != 200 or not isinstance(data, dict):
        apiError = _extractApiError(data)
        if apiError:
            return RobloxFavoriteGamesResult([], status, error=f"Favorite games lookup failed ({status}): {apiError}")
        if status in (400, 403):
            return RobloxFavoriteGamesResult(
                [],
                status,
                error="Favorite games are unavailable for this user (private or Roblox API rejection).",
            )
        return RobloxFavoriteGamesResult([], status, error=f"Favorite games lookup failed ({status}).")

    raw = data.get("data")
    if not isinstance(raw, list):
        return RobloxFavoriteGamesResult([], status, error="Favorite games lookup returned invalid data.")

    games: list[dict] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        universeId = entry.get("universeId")
        placeId = (
            entry.get("rootPlaceId")
            or entry.get("placeId")
            or entry.get("placeID")
        )
        name = entry.get("name")
        try:
            universeId = int(universeId) if universeId is not None else None
        except (TypeError, ValueError):
            universeId = None
        try:
            placeId = int(placeId) if placeId is not None else None
        except (TypeError, ValueError):
            placeId = None
        games.append(
            {
                "name": name if isinstance(name, str) else None,
                "universeId": universeId,
                "placeId": placeId,
            }
        )
        if len(games) >= requestedLimit:
            break

    return RobloxFavoriteGamesResult(games, status)

async def fetchRobloxBadgeAwards(
    robloxUserId: int,
    badgeIds: set[int],
    batchSize: int = 50,
) -> RobloxBadgeAwardsResult:
    if not badgeIds:
        return RobloxBadgeAwardsResult([], 200)

    url = f"https://badges.roblox.com/v1/users/{robloxUserId}/badges/awarded-dates"
    badges: list[dict] = []
    ids = list(badgeIds)

    try:
        for start in range(0, len(ids), batchSize):
            chunk = ids[start : start + batchSize]
            params = {"badgeIds": ",".join(str(b) for b in chunk)}
            status, data = await _requestJson("GET", url, params=params, timeoutSec=10)
            if status != 200 or not isinstance(data, dict):
                return RobloxBadgeAwardsResult(
                    badges, status, error=f"Badge lookup failed ({status})."
                )
            rows = data.get("data")
            if not isinstance(rows, list):
                return RobloxBadgeAwardsResult(
                    badges, status, error="Badge lookup returned invalid data."
                )
            for row in rows:
                if not isinstance(row, dict):
                    continue
                badgeId = row.get("badgeId") or row.get("badge_id") or row.get("id")
                awardedDate = row.get("awardedDate") or row.get("awarded_date")
                try:
                    badgeId = int(badgeId) if badgeId is not None else None
                except (TypeError, ValueError):
                    badgeId = None
                if not badgeId or not awardedDate:
                    continue
                badges.append(
                    {
                        "badgeId": badgeId,
                        "awardedDate": awardedDate,
                    }
                )
    except Exception as exc:
        return RobloxBadgeAwardsResult(badges, 0, error=str(exc))

    return RobloxBadgeAwardsResult(badges, 200)

async def fetchRobloxUserOutfits(
    robloxUserId: int,
    maxOutfits: int = 0,
    page: int = 1,
    itemsPerPage: int = 50,
    editableOnly: bool = True,
    maxPages: int = 20,
) -> RobloxOutfitsResult:
    normalizedMaxOutfits = int(maxOutfits or 0)
    if normalizedMaxOutfits < 0:
        normalizedMaxOutfits = 0

    itemsPerPage = max(1, min(int(itemsPerPage or 50), 50))
    if normalizedMaxOutfits > 0:
        itemsPerPage = min(itemsPerPage, normalizedMaxOutfits)
    maxPages = max(1, int(maxPages or 20))
    url = f"https://avatar.roblox.com/v1/users/{robloxUserId}/outfits"
    outfits: list[dict] = []
    seenOutfitIds: set[int] = set()
    currentPage = max(1, int(page or 1))
    lastStatus = 200

    for _ in range(maxPages):
        params = {
            "page": str(currentPage),
            "itemsPerPage": str(itemsPerPage),
        }
        if editableOnly:
            params["isEditable"] = "true"

        try:
            status, data = await _requestJson("GET", url, params=params, timeoutSec=10)
        except Exception as exc:
            return RobloxOutfitsResult(outfits, 0, error=str(exc))

        lastStatus = int(status or 0)
        if status != 200 or not isinstance(data, dict):
            if outfits:
                # Keep partial results if later pages fail.
                return RobloxOutfitsResult(outfits, status)
            return RobloxOutfitsResult([], status, error=f"Outfit lookup failed ({status}).")

        raw = data.get("data") or data.get("outfits") or []
        if not isinstance(raw, list):
            if outfits:
                return RobloxOutfitsResult(outfits, status)
            return RobloxOutfitsResult([], status, error="Outfit lookup returned invalid data.")

        if not raw:
            break

        for entry in raw:
            if not isinstance(entry, dict):
                continue
            outfitId = entry.get("id") or entry.get("outfitId")
            name = entry.get("name")
            try:
                outfitId = int(outfitId) if outfitId is not None else None
            except (TypeError, ValueError):
                outfitId = None
            if outfitId is None or outfitId in seenOutfitIds:
                continue
            seenOutfitIds.add(outfitId)
            outfits.append(
                {
                    "id": outfitId,
                    "name": name,
                    "isEditable": entry.get("isEditable"),
                    "outfitType": entry.get("outfitType"),
                }
            )
            if normalizedMaxOutfits > 0 and len(outfits) >= normalizedMaxOutfits:
                return RobloxOutfitsResult(outfits, status)

        # If this page was short, we've reached the end.
        if len(raw) < itemsPerPage:
            break
        currentPage += 1

    return RobloxOutfitsResult(outfits, lastStatus)

async def fetchRobloxOutfitThumbnails(
    outfitIds: list[int],
    size: str = "420x420",
    imageFormat: str = "Png",
    isCircular: bool = False,
) -> RobloxOutfitThumbnailsResult:
    if not outfitIds:
        return RobloxOutfitThumbnailsResult([], 200)

    url = "https://thumbnails.roblox.com/v1/users/outfits"
    params = {
        "userOutfitIds": ",".join(str(oid) for oid in outfitIds),
        "size": size,
        "format": imageFormat,
        "isCircular": str(isCircular).lower(),
    }
    try:
        status, data = await _requestJson("GET", url, params=params, timeoutSec=10)
    except Exception as exc:
        return RobloxOutfitThumbnailsResult([], 0, error=str(exc))

    if status != 200 or not isinstance(data, dict):
        return RobloxOutfitThumbnailsResult([], status, error=f"Outfit thumbnail lookup failed ({status}).")

    raw = data.get("data")
    if not isinstance(raw, list):
        return RobloxOutfitThumbnailsResult([], status, error="Outfit thumbnail lookup returned invalid data.")

    thumbs: list[dict] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        targetId = entry.get("targetId")
        try:
            targetId = int(targetId) if targetId is not None else None
        except (TypeError, ValueError):
            targetId = None
        if targetId is None:
            continue
        thumbs.append(
            {
                "id": targetId,
                "imageUrl": entry.get("imageUrl"),
                "state": entry.get("state"),
            }
        )

    return RobloxOutfitThumbnailsResult(thumbs, status)

def _extractBadgeId(entry: dict) -> Optional[int]:
    for key in ("id", "badgeId", "badge_id"):
        value = entry.get(key)
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None

async def fetchRobloxUniverseBadges(
    universeId: int,
    limit: int = 100,
    cursor: Optional[str] = None,
    sortOrder: str = "Asc",
) -> RobloxUniverseBadgesResult:
    url = f"https://badges.roblox.com/v1/universes/{universeId}/badges"
    params = {"limit": str(limit), "sortOrder": sortOrder}
    if cursor:
        params["cursor"] = cursor

    try:
        status, data = await _requestJson("GET", url, params=params, timeoutSec=10)
    except Exception as exc:
        return RobloxUniverseBadgesResult([], 0, error=str(exc))

    if status != 200 or not isinstance(data, dict):
        return RobloxUniverseBadgesResult([], status, error=f"Badge list failed ({status}).")

    raw = data.get("data")
    if not isinstance(raw, list):
        return RobloxUniverseBadgesResult([], status, error="Badge list returned invalid data.")

    badges: list[dict] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        badgeId = _extractBadgeId(entry)
        if badgeId is None:
            continue
        name = entry.get("name")
        stats = entry.get("statistics") if isinstance(entry.get("statistics"), dict) else {}
        awardedCount = stats.get("awardedCount") or stats.get("awardedCountFormatted")
        badges.append(
            {
                "id": badgeId,
                "name": name,
                "awardedCount": awardedCount,
            }
        )

    return RobloxUniverseBadgesResult(
        badges,
        status,
        nextCursor=data.get("nextPageCursor"),
    )

async def fetchRobloxUserProfile(robloxUserId: int) -> RobloxUserProfileResult:
    url = f"https://users.roblox.com/v1/users/{robloxUserId}"
    try:
        status, data = await _requestJson("GET", url, timeoutSec=10)
    except Exception as exc:
        return RobloxUserProfileResult(None, 0, error=str(exc))

    if status != 200 or not isinstance(data, dict):
        return RobloxUserProfileResult(None, status, error=f"Profile lookup failed ({status}).")

    created = data.get("created")
    return RobloxUserProfileResult(created if isinstance(created, str) else None, status)
