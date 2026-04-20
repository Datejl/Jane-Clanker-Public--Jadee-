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
    summary: Optional[dict] = None


@dataclass
class RobloxConnectionCountsResult:
    friends: Optional[int]
    followers: Optional[int]
    following: Optional[int]
    status: int
    error: Optional[str] = None


@dataclass
class RobloxGamepassesResult:
    gamepasses: list[dict]
    status: int
    nextCursor: Optional[str] = None
    error: Optional[str] = None
    summary: Optional[dict] = None

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
class RobloxUserBadgesResult:
    badges: list[dict]
    status: int
    nextCursor: Optional[str] = None
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
    username: Optional[str] = None

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
_robloxCache: dict[str, dict[object, tuple[datetime, object]]] = {}


def _utcNow() -> datetime:
    return datetime.now(timezone.utc)


def _cacheTtlSec(name: str, default: int) -> int:
    try:
        value = int(getattr(config, name, default) or 0)
    except (TypeError, ValueError):
        value = default
    return max(0, value)


def _cacheGet(storeName: str, key: object, *, ttlName: str, defaultTtlSec: int) -> object | None:
    ttlSec = _cacheTtlSec(ttlName, defaultTtlSec)
    if ttlSec <= 0:
        return None
    store = _robloxCache.get(storeName)
    if not store:
        return None
    cached = store.get(key)
    if not cached:
        return None
    cachedAt, value = cached
    if _utcNow() - cachedAt > timedelta(seconds=ttlSec):
        store.pop(key, None)
        return None
    return value


def _cacheSet(storeName: str, key: object, value: object, *, ttlName: str, defaultTtlSec: int) -> None:
    ttlSec = _cacheTtlSec(ttlName, defaultTtlSec)
    if ttlSec <= 0:
        return
    store = _robloxCache.setdefault(storeName, {})
    maxEntries = int(getattr(config, "robloxApiCacheMaxEntries", 5000) or 5000)
    if maxEntries > 0 and len(store) >= maxEntries:
        oldestKey = min(store.items(), key=lambda item: item[1][0])[0]
        store.pop(oldestKey, None)
    store[key] = (_utcNow(), value)


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


async def fetchRobloxUserByUsername(username: str) -> RoverLookupResult:
    cleanUsername = str(username or "").strip()
    if not cleanUsername:
        return RoverLookupResult(None, None, error="Missing Roblox username.")

    url = "https://users.roblox.com/v1/usernames/users"
    body = {
        "usernames": [cleanUsername],
        "excludeBannedUsers": False,
    }
    try:
        status, data = await _requestJson("POST", url, jsonBody=body, timeoutSec=10)
    except Exception as exc:
        return RoverLookupResult(None, None, error=str(exc))

    if status != 200 or not isinstance(data, dict):
        return RoverLookupResult(None, None, error=f"Roblox username lookup failed ({status}).")

    rows = data.get("data")
    if not isinstance(rows, list) or not rows:
        return RoverLookupResult(None, None, error=f"No Roblox user found for `{cleanUsername}`.")

    first = rows[0]
    if not isinstance(first, dict):
        return RoverLookupResult(None, None, error="Roblox username lookup returned invalid data.")

    try:
        robloxId = int(first.get("id"))
    except (TypeError, ValueError):
        robloxId = None
    resolvedUsername = first.get("name") or first.get("displayName") or cleanUsername
    if not robloxId:
        return RoverLookupResult(None, str(resolvedUsername), error="Roblox username lookup did not return a user ID.")
    return RoverLookupResult(robloxId, str(resolvedUsername))


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


def _optionalInt(value: object) -> Optional[int]:
    try:
        if value is not None:
            return int(value)
    except (TypeError, ValueError):
        return None
    return None


def _optionalBool(value: object) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    return None


def _extractGroupOwner(group: dict) -> tuple[Optional[int], Optional[str]]:
    owner = group.get("owner") if isinstance(group.get("owner"), dict) else None
    if not owner:
        return None, None
    ownerId = _optionalInt(owner.get("userId") or owner.get("id"))
    ownerName = owner.get("username") or owner.get("name") or owner.get("displayName")
    return ownerId, ownerName if isinstance(ownerName, str) and ownerName else None


async def fetchRobloxGroups(robloxUserId: int) -> RobloxGroupsResult:
    cacheKey = int(robloxUserId or 0)
    cached = _cacheGet(
        "groups",
        cacheKey,
        ttlName="robloxGroupCacheTtlSec",
        defaultTtlSec=3600,
    )
    if isinstance(cached, RobloxGroupsResult):
        return cached

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
        groupId = _optionalInt(group.get("id"))
        groupName = group.get("name")
        roleName = role.get("name") if isinstance(role, dict) else None
        roleId = _optionalInt(role.get("id")) if isinstance(role, dict) else None
        rank = _optionalInt(role.get("rank")) if isinstance(role, dict) else None
        ownerId, ownerName = _extractGroupOwner(group)
        groups.append(
            {
                "id": groupId,
                "name": groupName,
                "memberCount": _optionalInt(group.get("memberCount")),
                "hasVerifiedBadge": _optionalBool(group.get("hasVerifiedBadge")),
                "isLocked": _optionalBool(group.get("isLocked")),
                "publicEntryAllowed": _optionalBool(group.get("publicEntryAllowed")),
                "ownerId": ownerId,
                "ownerName": ownerName,
                "roleId": roleId,
                "role": roleName,
                "rank": rank,
            }
        )

    result = RobloxGroupsResult(groups, status)
    _cacheSet(
        "groups",
        cacheKey,
        result,
        ttlName="robloxGroupCacheTtlSec",
        defaultTtlSec=3600,
    )
    return result


def _extractAssetId(item: dict) -> Optional[int]:
    candidates = [
        item.get("assetId"),
        item.get("asset_id"),
        item.get("id"),
    ]
    asset = item.get("asset") if isinstance(item.get("asset"), dict) else None
    if asset:
        candidates.extend([asset.get("id"), asset.get("assetId")])
    assetDetails = item.get("assetDetails") if isinstance(item.get("assetDetails"), dict) else None
    if assetDetails:
        candidates.extend([assetDetails.get("assetId"), assetDetails.get("id")])
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
    assetDetails = item.get("assetDetails") if isinstance(item.get("assetDetails"), dict) else None
    if assetDetails:
        for key in ("name", "assetName", "displayName"):
            value = assetDetails.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def _extractInventoryItemType(item: dict) -> str:
    candidates: list[object] = [
        item.get("itemType"),
        item.get("inventoryItemType"),
        item.get("type"),
        item.get("assetType"),
        item.get("assetTypeName"),
    ]
    asset = item.get("asset") if isinstance(item.get("asset"), dict) else None
    if asset:
        candidates.extend(
            [
                asset.get("type"),
                asset.get("assetType"),
                asset.get("assetTypeName"),
            ]
        )
    assetDetails = item.get("assetDetails") if isinstance(item.get("assetDetails"), dict) else None
    if assetDetails:
        candidates.extend(
            [
                assetDetails.get("inventoryItemAssetType"),
                assetDetails.get("assetType"),
                assetDetails.get("assetTypeName"),
            ]
        )
    if isinstance(item.get("gamePassDetails"), dict):
        candidates.append("GAME_PASS")
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for nestedKey in ("type", "name", "displayName"):
                nested = value.get(nestedKey)
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
    return ""


def _isGamepassInventoryItem(item: dict) -> bool:
    itemType = _extractInventoryItemType(item).replace("_", "").replace("-", "").replace(" ", "").lower()
    if "gamepass" in itemType:
        return True
    for key in ("gamePassId", "gamepassId", "game_pass_id", "passId"):
        if item.get(key) is not None:
            return True
    gamepass = item.get("gamePass") if isinstance(item.get("gamePass"), dict) else None
    gamepassDetails = item.get("gamePassDetails") if isinstance(item.get("gamePassDetails"), dict) else None
    return bool(gamepass or gamepassDetails)


def _extractGamepassId(item: dict) -> Optional[int]:
    candidates = [
        item.get("gamePassId"),
        item.get("gamepassId"),
        item.get("game_pass_id"),
        item.get("passId"),
    ]
    gamepass = item.get("gamePass") if isinstance(item.get("gamePass"), dict) else None
    if gamepass:
        candidates.extend([gamepass.get("id"), gamepass.get("gamePassId")])
    gamepassDetails = item.get("gamePassDetails") if isinstance(item.get("gamePassDetails"), dict) else None
    if gamepassDetails:
        candidates.extend([gamepassDetails.get("id"), gamepassDetails.get("gamePassId")])
    if _isGamepassInventoryItem(item):
        candidates.append(_extractAssetId(item))
    for value in candidates:
        try:
            if value is not None:
                parsed = int(value)
                if parsed > 0:
                    return parsed
        except (TypeError, ValueError):
            continue
    return None


def _extractRobuxPrice(item: dict) -> Optional[int]:
    for key in ("price", "priceInRobux", "PriceInRobux", "lowestPrice", "lowestPriceInRobux"):
        value = item.get(key)
        try:
            if value is not None:
                parsed = int(value)
                if parsed >= 0:
                    return parsed
        except (TypeError, ValueError):
            continue
    product = item.get("product") if isinstance(item.get("product"), dict) else None
    if product:
        return _extractRobuxPrice(product)
    return None


async def _fetchCatalogAssetPrices(assetIds: list[int]) -> tuple[dict[int, dict], Optional[str]]:
    uniqueIds = sorted({int(assetId) for assetId in assetIds if int(assetId or 0) > 0})
    if not uniqueIds:
        return {}, None

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": str(
            getattr(
                config,
                "robloxPublicApiUserAgent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Jane-Clanker/1.0",
            )
        ),
    }
    prices: dict[int, dict] = {}
    missingIds: list[int] = []
    for assetId in uniqueIds:
        cached = _cacheGet(
            "asset_prices",
            int(assetId),
            ttlName="robloxAssetPriceCacheTtlSec",
            defaultTtlSec=86400,
        )
        if isinstance(cached, dict):
            prices[int(assetId)] = dict(cached)
        else:
            missingIds.append(int(assetId))
    if not missingIds:
        return prices, None

    errors: list[str] = []
    semaphore = asyncio.Semaphore(8)

    async def _fetchAsset(assetId: int) -> tuple[int, dict | None, Optional[str]]:
        url = f"https://economy.roblox.com/v2/assets/{int(assetId)}/details"
        async with semaphore:
            try:
                status, data = await _requestJson("GET", url, headers=headers, timeoutSec=10)
            except Exception as exc:
                return int(assetId), None, str(exc)
        if status != 200 or not isinstance(data, dict):
            return int(assetId), None, f"Asset price lookup failed ({status})."
        entry = {
            "id": int(assetId),
            "name": data.get("Name") or data.get("name"),
            "price": _extractRobuxPrice(data),
            "isForSale": _optionalBool(
                data.get("IsForSale") if data.get("IsForSale") is not None else data.get("isForSale")
            ),
            "isLimited": _optionalBool(
                data.get("IsLimited") if data.get("IsLimited") is not None else data.get("isLimited")
            ),
            "isLimitedUnique": _optionalBool(
                data.get("IsLimitedUnique") if data.get("IsLimitedUnique") is not None else data.get("isLimitedUnique")
            ),
        }
        return int(assetId), entry, None

    results = await asyncio.gather(
        *[_fetchAsset(assetId) for assetId in missingIds],
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, Exception):
            errors.append(str(result))
            continue
        assetId, entry, error = result
        if error:
            errors.append(error)
            continue
        if not isinstance(entry, dict):
            continue
        prices[int(assetId)] = entry
        _cacheSet(
            "asset_prices",
            int(assetId),
            dict(entry),
            ttlName="robloxAssetPriceCacheTtlSec",
            defaultTtlSec=86400,
        )

    return prices, "; ".join(errors[:3]) or None


def _gamepassHardMaxPages() -> int:
    try:
        configured = int(getattr(config, "bgIntelligenceGamepassHardMaxPages", 100) or 100)
    except (TypeError, ValueError):
        configured = 100
    return max(1, min(configured, 500))


def _inventoryHardMaxPages() -> int:
    try:
        configured = int(getattr(config, "bgIntelligenceInventoryHardMaxPages", 100) or 100)
    except (TypeError, ValueError):
        configured = 100
    return max(1, min(configured, 500))


def _badgeHistoryHardMaxPages() -> int:
    try:
        configured = int(getattr(config, "bgIntelligenceBadgeHistoryHardMaxPages", 100) or 100)
    except (TypeError, ValueError):
        configured = 100
    return max(1, min(configured, 500))


def _badgeAwardLookupConcurrency() -> int:
    try:
        configured = int(getattr(config, "robloxBadgeAwardLookupConcurrency", 1) or 1)
    except (TypeError, ValueError):
        configured = 1
    return max(1, min(configured, 4))


def _badgeAwardLookupDelaySec() -> float:
    try:
        configured = float(getattr(config, "robloxBadgeAwardLookupDelaySec", 0.5) or 0.0)
    except (TypeError, ValueError):
        configured = 0.5
    return max(0.0, min(configured, 5.0))


_PUBLIC_INVENTORY_ASSET_TYPE_IDS: tuple[int, ...] = (
    1, 2, 3, 4, 5, 8, 9, 10, 11, 12, 13, 17, 18, 19, 24,
    27, 28, 29, 30, 31, 32, 38, 40, 41, 42, 43, 44, 45,
    46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 61, 62,
    64, 65, 66, 67, 68, 69, 70, 71, 72, 76, 77,
)


def _publicInventoryAssetTypeIds() -> tuple[int, ...]:
    configured = getattr(config, "bgIntelligencePublicInventoryAssetTypeIds", None)
    if isinstance(configured, (list, tuple, set)):
        values: list[int] = []
        for value in configured:
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                continue
            if parsed > 0 and parsed not in {21, 34}:
                values.append(parsed)
        if values:
            return tuple(values)
    return _PUBLIC_INVENTORY_ASSET_TYPE_IDS


async def _fetchPublicInventoryAssetType(
    robloxUserId: int,
    assetTypeId: int,
    *,
    maxPages: int,
) -> tuple[list[dict], int, Optional[str], bool]:
    url = f"https://inventory.roblox.com/v2/users/{int(robloxUserId)}/inventory/{int(assetTypeId)}"
    headers = {
        "Accept": "application/json",
        "User-Agent": str(
            getattr(
                config,
                "robloxPublicApiUserAgent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Jane-Clanker/1.0",
            )
        ),
    }
    cursor: Optional[str] = None
    rows: list[dict] = []
    status = 200
    complete = True
    try:
        for _ in range(max(1, int(maxPages or 1))):
            params = {"limit": "100", "sortOrder": "Desc"}
            if cursor:
                params["cursor"] = cursor
            status, data = await _requestJson("GET", url, headers=headers, params=params, timeoutSec=10)
            if status != 200 or not isinstance(data, dict):
                detail = None
                if isinstance(data, dict):
                    detail = data.get("message") or data.get("error")
                return rows, int(status or 0), str(detail or f"Inventory asset type lookup failed ({status})."), False
            rawRows = data.get("data")
            if not isinstance(rawRows, list):
                return rows, int(status or 0), "Inventory asset type lookup returned invalid data.", False
            for raw in rawRows:
                if isinstance(raw, dict):
                    raw = dict(raw)
                    raw.setdefault("assetTypeId", int(assetTypeId))
                    rows.append(raw)
            cursor = data.get("nextPageCursor") or data.get("nextCursor")
            if not cursor:
                complete = True
                break
        else:
            complete = not bool(cursor)
    except Exception as exc:
        return rows, 0, str(exc), False
    return rows, int(status or 200), None, complete


async def _fetchPublicInventoryAssets(
    robloxUserId: int,
    *,
    maxPagesPerType: int,
) -> tuple[list[dict], int, Optional[str], bool]:
    rows: list[dict] = []
    errors: list[str] = []
    statuses: list[int] = []
    complete = True
    semaphore = asyncio.Semaphore(8)

    async def _fetch(assetTypeId: int) -> tuple[list[dict], int, Optional[str], bool]:
        async with semaphore:
            return await _fetchPublicInventoryAssetType(
                robloxUserId,
                assetTypeId,
                maxPages=maxPagesPerType,
            )

    results = await asyncio.gather(
        *[_fetch(assetTypeId) for assetTypeId in _publicInventoryAssetTypeIds()],
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, Exception):
            errors.append(str(result))
            complete = False
            continue
        typeRows, status, error, typeComplete = result
        statuses.append(int(status or 0))
        rows.extend(typeRows)
        complete = complete and bool(typeComplete)
        if error and int(status or 0) not in {400, 404}:
            errors.append(error)
    if rows:
        return rows, 200, "; ".join(errors[:3]) or None, complete
    status = max(statuses) if statuses else 0
    return rows, status, "; ".join(errors[:3]) or None, complete


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


def _inventoryMatchEntry(
    raw: dict,
    *,
    remaining: Optional[set[int]],
    creatorIds: set[int],
    keywords: list[str],
) -> tuple[Optional[dict], Optional[int], Optional[int]]:
    if _isGamepassInventoryItem(raw):
        return None, None, _extractGamepassId(raw)
    assetId = _extractAssetId(raw)
    if assetId is None:
        return None, None, None
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
        return None, int(assetId), None

    if matchItem:
        matchType = "item"
    elif matchCreator:
        matchType = "creator"
    else:
        matchType = "keyword"
    if remaining is not None and assetId in remaining:
        remaining.discard(assetId)
    return {
        "id": assetId,
        "name": assetName,
        "creatorId": creatorId,
        "creatorName": creatorName,
        "matchType": matchType,
        "keyword": matchedKeyword,
    }, int(assetId), None


async def _buildPublicInventoryResult(
    robloxUserId: int,
    *,
    targetItemIds: Optional[set[int]],
    targetCreatorIds: Optional[set[int]],
    targetKeywords: Optional[list[str]],
    maxPages: int,
) -> RobloxInventoryResult:
    remaining = set(targetItemIds) if targetItemIds else None
    creatorIds = set(targetCreatorIds) if targetCreatorIds else set()
    keywords = [
        str(value).strip().lower()
        for value in (targetKeywords or [])
        if str(value).strip()
    ]
    try:
        pagesPerType = int(maxPages or 0)
    except (TypeError, ValueError):
        pagesPerType = 0
    if pagesPerType <= 0:
        pagesPerType = max(1, min(int(getattr(config, "bgIntelligencePublicInventoryMaxPagesPerType", 10) or 10), 100))
    rawRows, status, error, complete = await _fetchPublicInventoryAssets(
        int(robloxUserId),
        maxPagesPerType=pagesPerType,
    )
    assetIds: list[int] = []
    gamepassIds: list[int] = []
    items: list[dict] = []
    for raw in rawRows:
        if not isinstance(raw, dict):
            continue
        entry, assetId, gamepassId = _inventoryMatchEntry(
            raw,
            remaining=remaining,
            creatorIds=creatorIds,
            keywords=keywords,
        )
        if assetId is not None:
            assetIds.append(int(assetId))
        if gamepassId is not None:
            gamepassIds.append(int(gamepassId))
        if entry is not None:
            items.append(entry)

    prices, priceError = await _fetchCatalogAssetPrices(assetIds)
    totalValue = 0
    pricedCount = 0
    for details in prices.values():
        price = _extractRobuxPrice(details)
        if price is None:
            continue
        totalValue += int(price)
        pricedCount += 1
    uniqueAssetCount = len(set(assetIds))
    summary = {
        "status": "OK" if rawRows or not error else "ERROR",
        "itemsScanned": len(rawRows),
        "pagesScanned": pagesPerType,
        "assetCount": len(assetIds),
        "uniqueAssetCount": uniqueAssetCount,
        "gamepassCount": len(gamepassIds),
        "uniqueGamepassCount": len(set(gamepassIds)),
        "ownedGamepassIds": sorted(set(gamepassIds)),
        "knownValueRobux": totalValue,
        "pricedAssetCount": pricedCount,
        "unpricedAssetCount": max(0, uniqueAssetCount - pricedCount),
        "gamepassesExcluded": True,
        "complete": complete,
        "requestedAllPages": maxPages <= 0,
        "priceError": priceError,
        "valueSource": "Roblox public inventory and economy asset details current price",
    }
    finalError = error if not rawRows else None
    return RobloxInventoryResult(items, status or 200, error=finalError, summary=summary)


async def _fetchRobloxCount(url: str) -> tuple[Optional[int], int, Optional[str]]:
    try:
        status, data = await _requestJson(
            "GET",
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": str(
                    getattr(
                        config,
                        "robloxPublicApiUserAgent",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Jane-Clanker/1.0",
                    )
                ),
            },
            timeoutSec=10,
        )
    except Exception as exc:
        return None, 0, str(exc)
    if status != 200 or not isinstance(data, dict):
        return None, int(status or 0), f"Count lookup failed ({status})."
    count = _optionalInt(data.get("count"))
    if count is None:
        return None, int(status or 0), "Count lookup returned invalid data."
    return int(count), int(status or 200), None


async def fetchRobloxConnectionCounts(robloxUserId: int) -> RobloxConnectionCountsResult:
    try:
        normalizedUserId = int(robloxUserId)
    except (TypeError, ValueError):
        return RobloxConnectionCountsResult(None, None, None, 0, error="Connection lookup failed (invalid Roblox user ID).")
    if normalizedUserId <= 0:
        return RobloxConnectionCountsResult(None, None, None, 0, error="Connection lookup failed (invalid Roblox user ID).")
    cached = _cacheGet(
        "connection_counts",
        normalizedUserId,
        ttlName="robloxConnectionCacheTtlSec",
        defaultTtlSec=3600,
    )
    if isinstance(cached, RobloxConnectionCountsResult):
        return cached

    baseUrl = f"https://friends.roblox.com/v1/users/{normalizedUserId}"
    results = await asyncio.gather(
        _fetchRobloxCount(f"{baseUrl}/friends/count"),
        _fetchRobloxCount(f"{baseUrl}/followers/count"),
        _fetchRobloxCount(f"{baseUrl}/followings/count"),
    )
    friends, friendStatus, friendError = results[0]
    followers, followerStatus, followerError = results[1]
    following, followingStatus, followingError = results[2]
    errors = [error for error in (friendError, followerError, followingError) if error]
    status = max(friendStatus, followerStatus, followingStatus)
    if errors and friends is None and followers is None and following is None:
        return RobloxConnectionCountsResult(friends, followers, following, status, error="; ".join(errors[:3]))
    result = RobloxConnectionCountsResult(
        friends,
        followers,
        following,
        status or 200,
        error="; ".join(errors[:3]) if errors else None,
    )
    _cacheSet(
        "connection_counts",
        normalizedUserId,
        result,
        ttlName="robloxConnectionCacheTtlSec",
        defaultTtlSec=3600,
    )
    return result


async def fetchRobloxInventory(
    robloxUserId: int,
    targetItemIds: Optional[set[int]] = None,
    targetCreatorIds: Optional[set[int]] = None,
    targetKeywords: Optional[list[str]] = None,
    maxPages: int = 5,
    includeValue: bool = False,
) -> RobloxInventoryResult:
    apiKey = getattr(config, "robloxInventoryApiKey", "") or getattr(config, "robloxOpenCloudApiKey", "")
    cacheKey = None
    if includeValue:
        normalizedKeywords = tuple(
            sorted(str(value).strip().lower() for value in (targetKeywords or []) if str(value).strip())
        )
        cacheKey = (
            "opencloud" if apiKey else "public",
            int(robloxUserId or 0),
            int(maxPages or 0),
            tuple(sorted(int(value) for value in (targetItemIds or set()))),
            tuple(sorted(int(value) for value in (targetCreatorIds or set()))),
            normalizedKeywords,
        )
        cached = _cacheGet(
            "inventory_value",
            cacheKey,
            ttlName="robloxInventoryValueCacheTtlSec",
            defaultTtlSec=21600,
        )
        if isinstance(cached, RobloxInventoryResult):
            return cached
    if not apiKey:
        if includeValue:
            result = await _buildPublicInventoryResult(
                int(robloxUserId),
                targetItemIds=targetItemIds,
                targetCreatorIds=targetCreatorIds,
                targetKeywords=targetKeywords,
                maxPages=maxPages,
            )
            if cacheKey is not None and not result.error:
                _cacheSet(
                    "inventory_value",
                    cacheKey,
                    result,
                    ttlName="robloxInventoryValueCacheTtlSec",
                    defaultTtlSec=21600,
                )
            return result
        return RobloxInventoryResult([], 0, error="Missing Roblox Open Cloud API key for inventory.")

    url = f"https://apis.roblox.com/cloud/v2/users/{robloxUserId}/inventory-items"
    headers = {"x-api-key": apiKey}
    params = {"maxPageSize": "100"}
    items: list[dict] = []
    pageCount = 0
    remaining = set(targetItemIds) if targetItemIds else None
    creatorIds = set(targetCreatorIds) if targetCreatorIds else set()
    keywords = [
        str(value).strip().lower()
        for value in (targetKeywords or [])
        if str(value).strip()
    ]
    probeOnly = remaining is None and not creatorIds and not keywords and not includeValue
    try:
        normalizedMaxPages = int(maxPages or 0)
    except (TypeError, ValueError):
        normalizedMaxPages = 5
    if normalizedMaxPages <= 0:
        pageLimit = _inventoryHardMaxPages()
        requestedAllPages = True
    else:
        pageLimit = max(1, normalizedMaxPages)
        requestedAllPages = False

    totalItemsScanned = 0
    assetIds: list[int] = []
    gamepassIds: list[int] = []
    lastStatus = 200
    nextToken: Optional[str] = None

    def _summary(*, status: str = "OK", priceError: Optional[str] = None) -> dict:
        uniqueAssetIds = sorted(set(assetIds))
        uniqueGamepassIds = sorted(set(gamepassIds))
        return {
            "status": status,
            "itemsScanned": totalItemsScanned,
            "pagesScanned": pageCount,
            "assetCount": len(assetIds),
            "uniqueAssetCount": len(uniqueAssetIds),
            "gamepassCount": len(gamepassIds),
            "uniqueGamepassCount": len(uniqueGamepassIds),
            "ownedGamepassIds": uniqueGamepassIds,
            "knownValueRobux": 0,
            "pricedAssetCount": 0,
            "unpricedAssetCount": len(uniqueAssetIds),
            "gamepassesExcluded": True,
            "complete": not bool(nextToken),
            "nextPageToken": nextToken,
            "requestedAllPages": requestedAllPages,
            "priceError": priceError,
            "valueSource": "Roblox economy asset details current price",
        }

    try:
        while True:
            if pageCount >= pageLimit:
                break
            status, data = await _requestJson("GET", url, headers=headers, params=params, timeoutSec=10)
            lastStatus = int(status or 0)
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
                        summary=_summary(status="ERROR"),
                    )
                return RobloxInventoryResult(
                    items,
                    status,
                    error=f"Inventory lookup failed ({status}).",
                    summary=_summary(status="ERROR"),
                )

            rawItems = data.get("inventoryItems") or data.get("items") or []
            if probeOnly:
                return RobloxInventoryResult([], status)
            if isinstance(rawItems, list):
                for raw in rawItems:
                    if not isinstance(raw, dict):
                        continue
                    totalItemsScanned += 1
                    if _isGamepassInventoryItem(raw):
                        gamepassId = _extractGamepassId(raw)
                        if includeValue and gamepassId is not None:
                            gamepassIds.append(int(gamepassId))
                        continue
                    assetId = _extractAssetId(raw)
                    if assetId is None:
                        continue
                    if includeValue:
                        assetIds.append(int(assetId))
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
            pageCount += 1
            if not includeValue and remaining is not None and not remaining and not creatorIds and not keywords:
                break
            if not nextToken:
                break
            params["pageToken"] = nextToken
    except Exception as exc:
        return RobloxInventoryResult(items, 0, error=str(exc), summary=_summary(status="ERROR"))

    if not includeValue:
        return RobloxInventoryResult(items, lastStatus or 200)

    summary = _summary(status="OK")
    prices, priceError = await _fetchCatalogAssetPrices(assetIds)
    if priceError:
        summary["priceError"] = priceError
    if prices:
        totalValue = 0
        pricedCount = 0
        for details in prices.values():
            price = _extractRobuxPrice(details)
            if price is None:
                continue
            totalValue += int(price)
            pricedCount += 1
        summary["knownValueRobux"] = totalValue
        summary["pricedAssetCount"] = pricedCount
        summary["unpricedAssetCount"] = max(0, int(summary.get("uniqueAssetCount") or 0) - pricedCount)
    result = RobloxInventoryResult(items, lastStatus or 200, summary=summary)
    if cacheKey is not None:
        _cacheSet(
            "inventory_value",
            cacheKey,
            result,
            ttlName="robloxInventoryValueCacheTtlSec",
            defaultTtlSec=21600,
        )
    return result


def _extractGamepassName(item: dict) -> Optional[str]:
    for key in ("name", "gamePassName", "displayName"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    gamepass = item.get("gamePass") if isinstance(item.get("gamePass"), dict) else None
    if gamepass:
        return _extractGamepassName(gamepass)
    return None


async def _fetchPublicGamepasses(
    robloxUserId: int,
    *,
    maxPages: int,
) -> tuple[list[dict], int, Optional[str], bool]:
    rows, status, error, complete = await _fetchPublicInventoryAssetType(
        int(robloxUserId),
        34,
        maxPages=maxPages,
    )
    gamepasses: list[dict] = []
    seenIds: set[int] = set()
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        gamepassId = _extractGamepassId(raw) or _extractAssetId(raw)
        if not gamepassId or int(gamepassId) in seenIds:
            continue
        seenIds.add(int(gamepassId))
        gamepasses.append(
            {
                "id": int(gamepassId),
                "name": _extractGamepassName(raw) or _extractAssetName(raw),
                "price": _extractRobuxPrice(raw),
            }
        )
    return gamepasses, int(status or 0), error, complete


async def _fetchGamepassProductInfo(gamepassId: int) -> tuple[int, dict | None, Optional[str]]:
    cached = _cacheGet(
        "gamepass_product",
        int(gamepassId),
        ttlName="robloxGamepassProductCacheTtlSec",
        defaultTtlSec=86400,
    )
    if isinstance(cached, dict):
        return 200, dict(cached), None

    url = f"https://apis.roblox.com/game-passes/v1/game-passes/{int(gamepassId)}/product-info"
    headers = {
        "Accept": "application/json",
        "User-Agent": str(
            getattr(
                config,
                "robloxPublicApiUserAgent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Jane-Clanker/1.0",
            )
        ),
    }
    try:
        status, data = await _requestJson("GET", url, headers=headers, timeoutSec=10)
    except Exception as exc:
        return 0, None, str(exc)
    if status != 200 or not isinstance(data, dict):
        return int(status or 0), None, f"Gamepass product lookup failed ({status})."
    _cacheSet(
        "gamepass_product",
        int(gamepassId),
        dict(data),
        ttlName="robloxGamepassProductCacheTtlSec",
        defaultTtlSec=86400,
    )
    return int(status or 200), data, None


async def _enrichGamepassPrices(gamepasses: list[dict]) -> Optional[str]:
    missing = [
        int(gamepass["id"])
        for gamepass in gamepasses
        if int(gamepass.get("id") or 0) > 0 and _extractRobuxPrice(gamepass) is None
    ]
    if not missing:
        return None
    errors: list[str] = []
    semaphore = asyncio.Semaphore(8)

    async def _lookup(gamepassId: int) -> tuple[int, dict | None, Optional[str]]:
        async with semaphore:
            return await _fetchGamepassProductInfo(gamepassId)

    results = await asyncio.gather(*[_lookup(gamepassId) for gamepassId in missing], return_exceptions=True)
    byId = {int(gamepass.get("id") or 0): gamepass for gamepass in gamepasses}
    for result in results:
        if isinstance(result, Exception):
            errors.append(str(result))
            continue
        _, payload, error = result
        if error:
            errors.append(error)
            continue
        if not isinstance(payload, dict):
            continue
        gamepassId = _optionalInt(
            payload.get("GamePassId")
            or payload.get("gamePassId")
            or payload.get("TargetId")
            or payload.get("targetId")
            or payload.get("id")
        )
        if not gamepassId:
            continue
        gamepass = byId.get(int(gamepassId))
        if not gamepass:
            continue
        name = payload.get("Name") or payload.get("name")
        if isinstance(name, str) and name.strip() and not gamepass.get("name"):
            gamepass["name"] = name.strip()
        price = _extractRobuxPrice(payload)
        if price is not None:
            gamepass["price"] = int(price)
        productId = _optionalInt(payload.get("ProductId") or payload.get("productId"))
        if productId:
            gamepass["productId"] = int(productId)
    return "; ".join(errors[:3]) or None


async def fetchRobloxGamepassesByIds(gamepassIds: list[int] | set[int] | tuple[int, ...]) -> RobloxGamepassesResult:
    uniqueIds = sorted({int(value) for value in list(gamepassIds or []) if int(value or 0) > 0})
    if not uniqueIds:
        return RobloxGamepassesResult([], 200, summary={
            "status": "OK",
            "pagesScanned": 0,
            "totalGamepasses": 0,
            "pricedGamepasses": 0,
            "unpricedGamepasses": 0,
            "totalRobux": 0,
            "complete": True,
            "valueSource": "Roblox game-pass product-info current price",
        })
    cacheKey = tuple(uniqueIds)
    cached = _cacheGet(
        "gamepasses_by_ids",
        cacheKey,
        ttlName="robloxGamepassCacheTtlSec",
        defaultTtlSec=21600,
    )
    if isinstance(cached, RobloxGamepassesResult):
        return cached
    gamepasses = [{"id": int(gamepassId), "name": None, "price": None} for gamepassId in uniqueIds]
    priceError = await _enrichGamepassPrices(gamepasses)
    totalRobux = 0
    pricedGamepasses = 0
    for gamepass in gamepasses:
        price = _extractRobuxPrice(gamepass)
        if price is None:
            continue
        totalRobux += int(price)
        pricedGamepasses += 1
    summary = {
        "status": "OK",
        "pagesScanned": 0,
        "totalGamepasses": len(gamepasses),
        "pricedGamepasses": pricedGamepasses,
        "unpricedGamepasses": max(0, len(gamepasses) - pricedGamepasses),
        "totalRobux": totalRobux,
        "complete": True,
        "valueSource": "Roblox game-pass product-info current price",
    }
    if priceError:
        summary["priceError"] = priceError
    result = RobloxGamepassesResult(gamepasses, 200, summary=summary)
    _cacheSet(
        "gamepasses_by_ids",
        cacheKey,
        result,
        ttlName="robloxGamepassCacheTtlSec",
        defaultTtlSec=21600,
    )
    return result


async def fetchRobloxUserGamepasses(
    robloxUserId: int,
    *,
    maxPages: int = 0,
) -> RobloxGamepassesResult:
    try:
        normalizedUserId = int(robloxUserId)
    except (TypeError, ValueError):
        return RobloxGamepassesResult([], 0, error="Gamepass inventory lookup failed (invalid Roblox user ID).")
    if normalizedUserId <= 0:
        return RobloxGamepassesResult([], 0, error="Gamepass inventory lookup failed (invalid Roblox user ID).")

    try:
        normalizedMaxPages = int(maxPages or 0)
    except (TypeError, ValueError):
        normalizedMaxPages = 0
    pageLimit = _gamepassHardMaxPages() if normalizedMaxPages <= 0 else max(1, normalizedMaxPages)
    requestedAllPages = normalizedMaxPages <= 0
    cacheKey = (normalizedUserId, normalizedMaxPages)
    cached = _cacheGet(
        "user_gamepasses",
        cacheKey,
        ttlName="robloxGamepassCacheTtlSec",
        defaultTtlSec=21600,
    )
    if isinstance(cached, RobloxGamepassesResult):
        return cached
    url = f"https://inventory.roblox.com/v1/users/{normalizedUserId}/items/GamePass"
    headers = {
        "Accept": "application/json",
        "User-Agent": str(
            getattr(
                config,
                "robloxPublicApiUserAgent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Jane-Clanker/1.0",
            )
        ),
    }
    cursor: Optional[str] = None
    gamepasses: list[dict] = []
    seenIds: set[int] = set()
    pageCount = 0
    status = 200
    complete = True

    try:
        while pageCount < pageLimit:
            params = {"limit": "100", "sortOrder": "Desc"}
            if cursor:
                params["cursor"] = cursor
            status, data = await _requestJson("GET", url, headers=headers, params=params, timeoutSec=10)
            if status != 200 or not isinstance(data, dict):
                errorDetail = ""
                if isinstance(data, dict):
                    message = data.get("message") or data.get("error")
                    if isinstance(message, str) and message.strip():
                        errorDetail = f": {message.strip()}"
                fallbackRows, fallbackStatus, fallbackError, fallbackComplete = await _fetchPublicGamepasses(
                    normalizedUserId,
                    maxPages=pageLimit,
                )
                if fallbackRows:
                    gamepasses = fallbackRows
                    status = fallbackStatus or status
                    cursor = None
                    complete = fallbackComplete
                    break
                return RobloxGamepassesResult(
                    gamepasses,
                    int(status or 0),
                    nextCursor=cursor,
                    error=f"Gamepass inventory lookup failed ({status}){errorDetail}.",
                    summary={
                        "status": "ERROR",
                        "pagesScanned": pageCount,
                        "totalGamepasses": len(gamepasses),
                        "complete": False,
                        "requestedAllPages": requestedAllPages,
                        "fallbackError": fallbackError,
                    },
                )
            rawItems = data.get("data") or data.get("items") or []
            if not isinstance(rawItems, list):
                return RobloxGamepassesResult(
                    gamepasses,
                    int(status or 0),
                    nextCursor=cursor,
                    error="Gamepass inventory lookup returned invalid data.",
                )
            for raw in rawItems:
                if not isinstance(raw, dict):
                    continue
                gamepassId = _extractGamepassId(raw) or _optionalInt(raw.get("id"))
                if not gamepassId or int(gamepassId) in seenIds:
                    continue
                seenIds.add(int(gamepassId))
                gamepasses.append(
                    {
                        "id": int(gamepassId),
                        "name": _extractGamepassName(raw),
                        "price": _extractRobuxPrice(raw),
                    }
                )
            pageCount += 1
            cursor = data.get("nextPageCursor") or data.get("nextPageToken") or data.get("nextCursor")
            if not cursor:
                break
        else:
            complete = not bool(cursor)
    except Exception as exc:
        return RobloxGamepassesResult(gamepasses, 0, nextCursor=cursor, error=str(exc))

    if not gamepasses:
        fallbackRows, fallbackStatus, fallbackError, fallbackComplete = await _fetchPublicGamepasses(
            normalizedUserId,
            maxPages=pageLimit,
        )
        if fallbackRows:
            gamepasses = fallbackRows
            status = fallbackStatus or status
            cursor = None
            complete = fallbackComplete
        elif fallbackError:
            return RobloxGamepassesResult(
                gamepasses,
                fallbackStatus or status,
                nextCursor=cursor,
                error=fallbackError,
                summary={
                    "status": "ERROR",
                    "pagesScanned": pageCount,
                    "totalGamepasses": 0,
                    "complete": False,
                    "requestedAllPages": requestedAllPages,
                },
            )

    priceError = await _enrichGamepassPrices(gamepasses)
    totalRobux = 0
    pricedGamepasses = 0
    for gamepass in gamepasses:
        price = _extractRobuxPrice(gamepass)
        if price is None:
            continue
        totalRobux += int(price)
        pricedGamepasses += 1

    summary = {
        "status": "OK",
        "pagesScanned": pageCount,
        "totalGamepasses": len(gamepasses),
        "pricedGamepasses": pricedGamepasses,
        "unpricedGamepasses": max(0, len(gamepasses) - pricedGamepasses),
        "totalRobux": totalRobux,
        "complete": complete and not bool(cursor),
        "nextPageCursor": cursor,
        "requestedAllPages": requestedAllPages,
        "valueSource": "Roblox game-pass product-info current price",
    }
    if priceError:
        summary["priceError"] = priceError
    result = RobloxGamepassesResult(gamepasses, int(status or 200), nextCursor=cursor, summary=summary)
    _cacheSet(
        "user_gamepasses",
        (normalizedUserId, normalizedMaxPages),
        result,
        ttlName="robloxGamepassCacheTtlSec",
        defaultTtlSec=21600,
    )
    return result


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
    cacheKey = (normalizedUserId, requestedLimit)
    cached = _cacheGet(
        "favorite_games",
        cacheKey,
        ttlName="robloxFavoriteGamesCacheTtlSec",
        defaultTtlSec=3600,
    )
    if isinstance(cached, RobloxFavoriteGamesResult):
        return cached
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

    result = RobloxFavoriteGamesResult(games, status)
    _cacheSet(
        "favorite_games",
        cacheKey,
        result,
        ttlName="robloxFavoriteGamesCacheTtlSec",
        defaultTtlSec=3600,
    )
    return result

async def fetchRobloxBadgeAwards(
    robloxUserId: int,
    badgeIds: set[int],
    batchSize: int = 50,
) -> RobloxBadgeAwardsResult:
    if not badgeIds:
        return RobloxBadgeAwardsResult([], 200)

    url = f"https://badges.roblox.com/v1/users/{robloxUserId}/badges/awarded-dates"
    badges: list[dict] = []
    ids = sorted({int(value) for value in list(badgeIds or set()) if int(value or 0) > 0})
    missingIds: list[int] = []
    for badgeId in ids:
        cached = _cacheGet(
            "badge_awards",
            (int(robloxUserId), int(badgeId)),
            ttlName="robloxBadgeAwardCacheTtlSec",
            defaultTtlSec=86400,
        )
        if isinstance(cached, dict):
            badges.append(dict(cached))
        else:
            missingIds.append(int(badgeId))
    if not missingIds:
        return RobloxBadgeAwardsResult(badges, 200)

    try:
        normalizedBatchSize = int(batchSize or 100)
    except (TypeError, ValueError):
        normalizedBatchSize = 100
    normalizedBatchSize = max(1, min(normalizedBatchSize, 100))
    semaphore = asyncio.Semaphore(_badgeAwardLookupConcurrency())

    async def _fetchChunk(chunk: list[int]) -> tuple[int, list[dict], Optional[str]]:
        params = {"badgeIds": ",".join(str(b) for b in chunk)}
        async with semaphore:
            status, data = await _requestJson("GET", url, params=params, timeoutSec=10)
            delaySec = _badgeAwardLookupDelaySec()
            if delaySec > 0:
                await asyncio.sleep(delaySec)
        if status != 200 or not isinstance(data, dict):
            return int(status or 0), [], f"Badge lookup failed ({status})."
        rows = data.get("data")
        if not isinstance(rows, list):
            return int(status or 0), [], "Badge lookup returned invalid data."
        parsedRows: list[dict] = []
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
            parsedRows.append(
                {
                    "badgeId": badgeId,
                    "awardedDate": awardedDate,
                }
            )
        return int(status or 200), parsedRows, None

    try:
        results = await asyncio.gather(
            *[
                _fetchChunk(missingIds[start : start + normalizedBatchSize])
                for start in range(0, len(missingIds), normalizedBatchSize)
            ],
            return_exceptions=True,
        )
        errors: list[str] = []
        errorStatus = 0
        for result in results:
            if isinstance(result, Exception):
                errors.append(str(result))
                continue
            status, rows, error = result
            if error:
                errorStatus = int(status or errorStatus or 0)
                errors.append(error)
                continue
            badges.extend(rows)
            for row in rows:
                badgeId = row.get("badgeId")
                if badgeId is None:
                    continue
                _cacheSet(
                    "badge_awards",
                    (int(robloxUserId), int(badgeId)),
                    dict(row),
                    ttlName="robloxBadgeAwardCacheTtlSec",
                    defaultTtlSec=86400,
                )
        if errors:
            return RobloxBadgeAwardsResult(
                badges,
                errorStatus,
                error=errors[0],
            )
    except Exception as exc:
        return RobloxBadgeAwardsResult(badges, 0, error=str(exc))

    return RobloxBadgeAwardsResult(badges, 200)

async def fetchRobloxUserBadges(
    robloxUserId: int,
    *,
    limit: int = 100,
    maxPages: int = 2,
) -> RobloxUserBadgesResult:
    try:
        normalizedUserId = int(robloxUserId)
    except (TypeError, ValueError):
        return RobloxUserBadgesResult([], 0, error="Badge history lookup failed (invalid Roblox user ID).")
    if normalizedUserId <= 0:
        return RobloxUserBadgesResult([], 0, error="Badge history lookup failed (invalid Roblox user ID).")

    pageLimit = max(10, min(int(limit or 100), 100))
    try:
        normalizedMaxPages = int(maxPages or 0)
    except (TypeError, ValueError):
        normalizedMaxPages = 2
    if normalizedMaxPages <= 0:
        pageCountLimit = _badgeHistoryHardMaxPages()
    else:
        pageCountLimit = max(1, min(normalizedMaxPages, _badgeHistoryHardMaxPages()))
    cacheKey = (normalizedUserId, pageLimit, pageCountLimit)
    cached = _cacheGet(
        "user_badges",
        cacheKey,
        ttlName="robloxBadgeHistoryCacheTtlSec",
        defaultTtlSec=86400,
    )
    if isinstance(cached, RobloxUserBadgesResult):
        return cached
    url = f"https://badges.roblox.com/v1/users/{normalizedUserId}/badges"
    cursor: Optional[str] = None
    badges: list[dict] = []
    status = 200

    try:
        for _ in range(pageCountLimit):
            params = {"limit": str(pageLimit), "sortOrder": "Desc"}
            if cursor:
                params["cursor"] = cursor
            status, data = await _requestJson("GET", url, params=params, timeoutSec=10)
            if status != 200 or not isinstance(data, dict):
                return RobloxUserBadgesResult(
                    badges,
                    status,
                    nextCursor=cursor,
                    error=f"Badge history lookup failed ({status}).",
                )
            raw = data.get("data")
            if not isinstance(raw, list):
                return RobloxUserBadgesResult(
                    badges,
                    status,
                    nextCursor=cursor,
                    error="Badge history lookup returned invalid data.",
                )
            for entry in raw:
                if not isinstance(entry, dict):
                    continue
                badgeId = _extractBadgeId(entry)
                if badgeId is None:
                    continue
                name = entry.get("name")
                created = entry.get("created")
                updated = entry.get("updated")
                stats = entry.get("statistics") if isinstance(entry.get("statistics"), dict) else {}
                badges.append(
                    {
                        "id": badgeId,
                        "name": name if isinstance(name, str) else None,
                        "created": created if isinstance(created, str) else None,
                        "updated": updated if isinstance(updated, str) else None,
                        "awardedCount": stats.get("awardedCount"),
                    }
                )
            cursor = data.get("nextPageCursor")
            if not cursor:
                break
    except Exception as exc:
        return RobloxUserBadgesResult(badges, 0, nextCursor=cursor, error=str(exc))

    result = RobloxUserBadgesResult(badges, status, nextCursor=cursor)
    _cacheSet(
        "user_badges",
        cacheKey,
        result,
        ttlName="robloxBadgeHistoryCacheTtlSec",
        defaultTtlSec=86400,
    )
    return result

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
    cacheKey = (int(robloxUserId or 0), normalizedMaxOutfits, int(page or 1), itemsPerPage, bool(editableOnly), maxPages)
    cached = _cacheGet(
        "outfits",
        cacheKey,
        ttlName="robloxOutfitCacheTtlSec",
        defaultTtlSec=3600,
    )
    if isinstance(cached, RobloxOutfitsResult):
        return cached
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
                result = RobloxOutfitsResult(outfits, status)
                _cacheSet(
                    "outfits",
                    cacheKey,
                    result,
                    ttlName="robloxOutfitCacheTtlSec",
                    defaultTtlSec=3600,
                )
                return result

        # If this page was short, we've reached the end.
        if len(raw) < itemsPerPage:
            break
        currentPage += 1

    result = RobloxOutfitsResult(outfits, lastStatus)
    _cacheSet(
        "outfits",
        cacheKey,
        result,
        ttlName="robloxOutfitCacheTtlSec",
        defaultTtlSec=3600,
    )
    return result

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
    cacheKey = int(robloxUserId or 0)
    cached = _cacheGet(
        "profiles",
        cacheKey,
        ttlName="robloxProfileCacheTtlSec",
        defaultTtlSec=86400,
    )
    if isinstance(cached, RobloxUserProfileResult):
        return cached

    url = f"https://users.roblox.com/v1/users/{robloxUserId}"
    try:
        status, data = await _requestJson("GET", url, timeoutSec=10)
    except Exception as exc:
        return RobloxUserProfileResult(None, 0, error=str(exc))

    if status != 200 or not isinstance(data, dict):
        return RobloxUserProfileResult(None, status, error=f"Profile lookup failed ({status}).")

    created = data.get("created")
    username = data.get("name")
    result = RobloxUserProfileResult(
        created if isinstance(created, str) else None,
        status,
        username=username if isinstance(username, str) else None,
    )
    _cacheSet(
        "profiles",
        cacheKey,
        result,
        ttlName="robloxProfileCacheTtlSec",
        defaultTtlSec=86400,
    )
    return result
