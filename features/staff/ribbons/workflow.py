from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Optional

import config
from db.sqlite import execute, executeMany, executeReturnId, fetchAll, fetchOne

BASE_DIR = Path(__file__).resolve().parent
ASSET_DIRECTORIES = {
    "sacks": BASE_DIR / "Awards",
    "gorget": BASE_DIR / "Commendations",
    "spbadge": BASE_DIR / "Commendations",
    "commendations": BASE_DIR / "Commendations",
    "corpus": BASE_DIR / "Commendations",
    "ribbons": BASE_DIR / "Ribbons",
}
VALID_ELIGIBILITY_TYPES = {"ROLE_AUTO", "PROOF_REQUIRED", "STAFF_GRANT_ONLY"}


def _normalizeName(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _displayNameFromPath(path: Path) -> str:
    return path.stem.strip()


def _hashBytes(rawBytes: bytes) -> str:
    return hashlib.sha256(rawBytes).hexdigest()


def _assetIdForFile(category: str, path: Path, rawBytes: bytes) -> str:
    fileHash = _hashBytes(rawBytes)
    pathHash = hashlib.sha256(f"{category}:{path.as_posix().lower()}".encode("utf-8")).hexdigest()[:4]
    return f"{fileHash[:12]}-{pathHash}"


def _categorizeCommendation(name: str) -> str:
    lowered = (name or "").lower()
    if "gorget" in lowered:
        return "gorget"
    if lowered.startswith(("mr ", "hr ", "anrocom ")):
        return "corpus"
    if "badge" in lowered:
        return "spbadge"
    return "commendations"


def _scanDirectoryAssets(
    directory: Path,
    defaultCategory: str,
    categoryResolver: Optional[Callable[[str], str]] = None,
) -> list[dict[str, Any]]:
    if not directory.exists():
        return []
    assets: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.png"), key=lambda item: item.name.lower()):
        displayName = _displayNameFromPath(path)
        category = categoryResolver(displayName) if categoryResolver else defaultCategory
        rawBytes = path.read_bytes()
        assets.append(
            {
                "assetId": _assetIdForFile(category, path, rawBytes),
                "displayName": displayName,
                "category": category,
                "filePath": str(path),
                "fileHash": _hashBytes(rawBytes),
            }
        )
    return assets


def scanAssetCatalog() -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    assets.extend(_scanDirectoryAssets(ASSET_DIRECTORIES["sacks"], "sacks"))
    assets.extend(_scanDirectoryAssets(ASSET_DIRECTORIES["ribbons"], "ribbons"))
    assets.extend(
        _scanDirectoryAssets(
            ASSET_DIRECTORIES["commendations"],
            "commendations",
            _categorizeCommendation,
        )
    )
    return assets


def _rulesPath() -> Path:
    rawPath = str(
        getattr(config, "ribbonRulesPath", "configData/ribbons.json")
        or "configData/ribbons.json"
    ).strip()
    if not rawPath:
        rawPath = "configData/ribbons.json"
    path = Path(rawPath)
    if path.is_absolute():
        return path
    configPath = Path(getattr(config, "__file__", __file__)).resolve().parent
    return (configPath / path).resolve()


def loadEligibilityRules() -> dict[str, Any]:
    path = _rulesPath()
    if not path.exists():
        return {
            "defaultEligibilityType": "PROOF_REQUIRED",
            "requestCooldownHours": 24,
            "rules": [],
            "rulesByRibbonId": {},
            "rulesByDisplayName": {},
        }

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    defaultEligibilityType = str(payload.get("defaultEligibilityType") or "PROOF_REQUIRED").strip().upper()
    if defaultEligibilityType not in VALID_ELIGIBILITY_TYPES:
        defaultEligibilityType = "PROOF_REQUIRED"

    try:
        requestCooldownHours = int(payload.get("requestCooldownHours", 24))
    except (TypeError, ValueError):
        requestCooldownHours = 24
    requestCooldownHours = max(0, min(requestCooldownHours, 168))

    rulesByRibbonId: dict[str, dict[str, Any]] = {}
    rulesByDisplayName: dict[str, dict[str, Any]] = {}
    cleanedRules: list[dict[str, Any]] = []

    for rawRule in payload.get("rules") or []:
        if not isinstance(rawRule, dict):
            continue
        ribbonId = str(rawRule.get("ribbonId") or "").strip()
        displayName = str(rawRule.get("displayName") or "").strip()
        if not ribbonId and not displayName:
            continue

        eligibilityType = str(rawRule.get("eligibilityType") or defaultEligibilityType).strip().upper()
        if eligibilityType not in VALID_ELIGIBILITY_TYPES:
            eligibilityType = defaultEligibilityType

        proofType = str(rawRule.get("proofType") or "screenshot").strip().lower()
        try:
            allowedProofCount = int(rawRule.get("allowedProofCount", 1))
        except (TypeError, ValueError):
            allowedProofCount = 1
        allowedProofCount = max(1, min(allowedProofCount, 10))

        requiredRoleIds: list[int] = []
        for value in rawRule.get("requiredRoleIds") or []:
            try:
                roleId = int(value)
            except (TypeError, ValueError):
                continue
            if roleId > 0:
                requiredRoleIds.append(roleId)

        rule = {
            "ribbonId": ribbonId,
            "displayName": displayName,
            "eligibilityType": eligibilityType,
            "proofType": proofType,
            "allowedProofCount": allowedProofCount,
            "requiredRoleIds": sorted(set(requiredRoleIds)),
            "notes": str(rawRule.get("notes") or "").strip(),
        }
        cleanedRules.append(rule)
        if ribbonId:
            rulesByRibbonId[ribbonId] = rule
        if displayName:
            rulesByDisplayName[_normalizeName(displayName)] = rule

    return {
        "defaultEligibilityType": defaultEligibilityType,
        "requestCooldownHours": requestCooldownHours,
        "rules": cleanedRules,
        "rulesByRibbonId": rulesByRibbonId,
        "rulesByDisplayName": rulesByDisplayName,
    }


def resolveRuleForAsset(asset: dict[str, Any], rulesConfig: dict[str, Any]) -> dict[str, Any]:
    assetId = str(asset.get("assetId") or "")
    displayName = str(asset.get("displayName") or "")
    byId = rulesConfig.get("rulesByRibbonId") or {}
    byName = rulesConfig.get("rulesByDisplayName") or {}
    rule = byId.get(assetId) or byName.get(_normalizeName(displayName))
    if rule:
        return rule
    return {
        "ribbonId": assetId,
        "displayName": displayName,
        "eligibilityType": str(rulesConfig.get("defaultEligibilityType") or "PROOF_REQUIRED"),
        "proofType": "screenshot",
        "allowedProofCount": 1,
        "requiredRoleIds": [],
        "notes": "",
    }


async def syncCatalogToDb(discoveredAssets: list[dict[str, Any]]) -> dict[str, int]:
    existingRows = await fetchAll("SELECT * FROM ribbon_assets")
    existingById = {str(row["assetId"]): row for row in existingRows}
    seenIds = {str(asset.get("assetId") or "") for asset in discoveredAssets if str(asset.get("assetId") or "").strip()}

    inserts: list[tuple] = []
    updates: list[tuple] = []
    revivedIds: list[str] = []

    for asset in discoveredAssets:
        assetId = str(asset.get("assetId") or "").strip()
        if not assetId:
            continue
        displayName = str(asset.get("displayName") or "").strip()
        category = str(asset.get("category") or "").strip()
        filePath = str(asset.get("filePath") or "").strip()
        fileHash = str(asset.get("fileHash") or "").strip()

        existing = existingById.get(assetId)
        if not existing:
            inserts.append((assetId, displayName, category, filePath, fileHash))
            continue

        changed = (
            str(existing.get("displayName") or "") != displayName
            or str(existing.get("category") or "") != category
            or str(existing.get("filePath") or "") != filePath
            or str(existing.get("fileHash") or "") != fileHash
        )
        if changed:
            updates.append((displayName, category, filePath, fileHash, assetId))
        if int(existing.get("isRetired") or 0) != 0:
            revivedIds.append(assetId)

    retiredIds = [assetId for assetId in existingById.keys() if assetId not in seenIds]

    if inserts:
        await executeMany(
            """
            INSERT INTO ribbon_assets
                (assetId, displayName, category, filePath, fileHash, isRetired, aliasesJson, updatedAt)
            VALUES (?, ?, ?, ?, ?, 0, '[]', datetime('now'))
            """,
            inserts,
        )
    if updates:
        await executeMany(
            """
            UPDATE ribbon_assets
            SET displayName = ?, category = ?, filePath = ?, fileHash = ?, updatedAt = datetime('now')
            WHERE assetId = ?
            """,
            updates,
        )
    if revivedIds:
        await executeMany(
            """
            UPDATE ribbon_assets
            SET isRetired = 0, updatedAt = datetime('now')
            WHERE assetId = ?
            """,
            [(assetId,) for assetId in revivedIds],
        )
    if retiredIds:
        await executeMany(
            """
            UPDATE ribbon_assets
            SET isRetired = 1, updatedAt = datetime('now')
            WHERE assetId = ?
            """,
            [(assetId,) for assetId in retiredIds],
        )

    return {
        "discovered": len(discoveredAssets),
        "inserted": len(inserts),
        "updated": len(updates),
        "retired": len(retiredIds),
        "revived": len(revivedIds),
    }


async def getActiveAssets() -> list[dict[str, Any]]:
    return await fetchAll(
        """
        SELECT *
        FROM ribbon_assets
        WHERE isRetired = 0
        ORDER BY category, displayName
        """
    )


async def getAssetById(assetId: str) -> Optional[dict[str, Any]]:
    return await fetchOne(
        "SELECT * FROM ribbon_assets WHERE assetId = ?",
        (assetId,),
    )


async def getAssetsByIds(assetIds: list[str]) -> dict[str, dict[str, Any]]:
    normalizedIds = [str(assetId).strip() for assetId in assetIds if str(assetId).strip()]
    if not normalizedIds:
        return {}
    placeholders = ",".join("?" for _ in normalizedIds)
    rows = await fetchAll(
        f"SELECT * FROM ribbon_assets WHERE assetId IN ({placeholders})",
        tuple(normalizedIds),
    )
    return {str(row["assetId"]): row for row in rows}


async def getRibbonProfile(userId: int) -> Optional[dict[str, Any]]:
    return await fetchOne(
        "SELECT * FROM ribbon_profiles WHERE userId = ?",
        (int(userId),),
    )


async def upsertRibbonProfile(
    userId: int,
    *,
    nameplateText: str,
    medalSelection: list[str],
    currentRibbonIds: list[str],
    lastGeneratedImagePath: str = "",
) -> None:
    await execute(
        """
        INSERT INTO ribbon_profiles
            (userId, nameplateText, medalSelectionJson, currentRibbonIdsJson, lastGeneratedImagePath, updatedAt)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(userId) DO UPDATE SET
            nameplateText = excluded.nameplateText,
            medalSelectionJson = excluded.medalSelectionJson,
            currentRibbonIdsJson = excluded.currentRibbonIdsJson,
            lastGeneratedImagePath = excluded.lastGeneratedImagePath,
            updatedAt = datetime('now')
        """,
        (
            int(userId),
            nameplateText or "",
            json.dumps(medalSelection or []),
            json.dumps(currentRibbonIds or []),
            lastGeneratedImagePath or "",
        ),
    )


def parseJsonList(rawValue: Any) -> list[str]:
    if not rawValue:
        return []
    try:
        data = json.loads(str(rawValue))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(data, list):
        return []
    out: list[str] = []
    for value in data:
        normalized = str(value or "").strip()
        if normalized:
            out.append(normalized)
    return out


async def createRibbonRequest(
    *,
    guildId: int,
    channelId: int,
    requesterId: int,
    nameplateText: str,
    medalSelection: list[str],
    addRibbonIds: list[str],
    removeRibbonIds: list[str],
    autoApprovedRibbonIds: list[str],
    needsProofRibbonIds: list[str],
    staffOnlyRibbonIds: list[str],
    currentSnapshot: dict[str, Any],
    status: str = "PENDING",
) -> dict[str, Any]:
    requestId = await executeReturnId(
        """
        INSERT INTO ribbon_requests
            (
                guildId,
                channelId,
                requesterId,
                status,
                nameplateText,
                medalSelectionJson,
                addRibbonIdsJson,
                removeRibbonIdsJson,
                autoApprovedRibbonIdsJson,
                needsProofRibbonIdsJson,
                staffOnlyRibbonIdsJson,
                currentSnapshotJson
            )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(guildId),
            int(channelId),
            int(requesterId),
            str(status or "PENDING").upper(),
            nameplateText or "",
            json.dumps(medalSelection or []),
            json.dumps(addRibbonIds or []),
            json.dumps(removeRibbonIds or []),
            json.dumps(autoApprovedRibbonIds or []),
            json.dumps(needsProofRibbonIds or []),
            json.dumps(staffOnlyRibbonIds or []),
            json.dumps(currentSnapshot or {}),
        ),
    )
    requestCode = f"RR-{requestId}"
    await execute(
        "UPDATE ribbon_requests SET requestCode = ?, updatedAt = datetime('now') WHERE requestId = ?",
        (requestCode, int(requestId)),
    )
    await addRibbonRequestEvent(int(requestId), int(requesterId), "SUBMITTED", "")
    row = await getRibbonRequestById(int(requestId))
    if not row:
        raise RuntimeError("Failed to create ribbon request row.")
    return row


async def getRibbonRequestById(requestId: int) -> Optional[dict[str, Any]]:
    return await fetchOne(
        "SELECT * FROM ribbon_requests WHERE requestId = ?",
        (int(requestId),),
    )


async def listRibbonRequestsForWorkflowReconciliation() -> list[dict[str, Any]]:
    return await fetchAll(
        """
        SELECT *
        FROM ribbon_requests
        ORDER BY datetime(updatedAt) DESC, requestId DESC
        """
    )


async def listRibbonRequestsForReviewViews() -> list[dict[str, Any]]:
    return await fetchAll(
        """
        SELECT *
        FROM ribbon_requests
        WHERE reviewMessageId IS NOT NULL
          AND status IN ('PENDING', 'NEEDS_INFO')
        ORDER BY createdAt DESC
        """
    )


async def getOpenRibbonRequestForUser(userId: int) -> Optional[dict[str, Any]]:
    return await fetchOne(
        """
        SELECT *
        FROM ribbon_requests
        WHERE requesterId = ?
          AND status IN ('PENDING', 'NEEDS_INFO')
        ORDER BY createdAt DESC
        LIMIT 1
        """,
        (int(userId),),
    )


async def getLatestRibbonRequestForUser(userId: int) -> Optional[dict[str, Any]]:
    return await fetchOne(
        """
        SELECT *
        FROM ribbon_requests
        WHERE requesterId = ?
        ORDER BY createdAt DESC
        LIMIT 1
        """,
        (int(userId),),
    )


async def setRibbonRequestReviewMessage(requestId: int, reviewChannelId: int, reviewMessageId: int) -> None:
    await execute(
        """
        UPDATE ribbon_requests
        SET reviewChannelId = ?, reviewMessageId = ?, updatedAt = datetime('now')
        WHERE requestId = ?
        """,
        (int(reviewChannelId), int(reviewMessageId), int(requestId)),
    )


async def setRibbonRequestStatus(
    requestId: int,
    *,
    status: str,
    reviewerId: Optional[int] = None,
    reviewNote: str = "",
) -> None:
    normalizedStatus = str(status or "PENDING").upper()
    if normalizedStatus in {"APPROVED", "REJECTED", "CANCELED"}:
        await execute(
            """
            UPDATE ribbon_requests
            SET status = ?,
                reviewerId = ?,
                reviewNote = ?,
                reviewedAt = datetime('now'),
                updatedAt = datetime('now')
            WHERE requestId = ?
            """,
            (normalizedStatus, reviewerId, reviewNote or "", int(requestId)),
        )
        return
    await execute(
        """
        UPDATE ribbon_requests
        SET status = ?,
            reviewerId = ?,
            reviewNote = ?,
            updatedAt = datetime('now')
        WHERE requestId = ?
        """,
        (normalizedStatus, reviewerId, reviewNote or "", int(requestId)),
    )


async def addRibbonRequestEvent(
    requestId: int,
    actorId: Optional[int],
    eventType: str,
    details: str = "",
) -> None:
    await execute(
        """
        INSERT INTO ribbon_request_events
            (requestId, actorId, eventType, details)
        VALUES (?, ?, ?, ?)
        """,
        (int(requestId), actorId, str(eventType or "").strip().upper(), details or ""),
    )


async def listRibbonProofsByRequest(requestId: int) -> list[dict[str, Any]]:
    return await fetchAll(
        """
        SELECT *
        FROM ribbon_request_proofs
        WHERE requestId = ?
        ORDER BY proofId
        """,
        (int(requestId),),
    )


async def getApprovedProofRibbonIdsForUser(
    userId: int,
    ribbonIds: Optional[list[str]] = None,
) -> set[str]:
    normalizedIds = [str(ribbonId).strip() for ribbonId in (ribbonIds or []) if str(ribbonId).strip()]
    params: list[Any] = [int(userId)]
    whereExtra = ""
    if normalizedIds:
        placeholders = ",".join("?" for _ in normalizedIds)
        whereExtra = f" AND rp.ribbonId IN ({placeholders})"
        params.extend(normalizedIds)

    rows = await fetchAll(
        f"""
        SELECT DISTINCT rp.ribbonId AS ribbonId
        FROM ribbon_request_proofs rp
        JOIN ribbon_requests rr ON rr.requestId = rp.requestId
        WHERE rr.requesterId = ?
          AND rr.status = 'APPROVED'
          AND rp.ribbonId IS NOT NULL
          AND rp.ribbonId != ''
          {whereExtra}
        """,
        tuple(params),
    )
    out: set[str] = set()
    for row in rows:
        ribbonId = str(row.get("ribbonId") or "").strip()
        if ribbonId:
            out.add(ribbonId)
    return out


async def getApprovedProofDisplayNamesForUser(userId: int) -> set[str]:
    rows = await fetchAll(
        """
        SELECT DISTINCT ra.displayName AS displayName
        FROM ribbon_request_proofs rp
        JOIN ribbon_requests rr ON rr.requestId = rp.requestId
        JOIN ribbon_assets ra ON ra.assetId = rp.ribbonId
        WHERE rr.requesterId = ?
          AND rr.status = 'APPROVED'
          AND rp.ribbonId IS NOT NULL
          AND rp.ribbonId != ''
          AND ra.displayName IS NOT NULL
          AND ra.displayName != ''
        """,
        (int(userId),),
    )
    out: set[str] = set()
    for row in rows:
        displayName = str(row.get("displayName") or "").strip()
        if displayName:
            out.add(displayName)
    return out
