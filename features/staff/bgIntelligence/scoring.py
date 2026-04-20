from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from features.staff.sessions import bgBuckets


@dataclass(frozen=True)
class RiskSignal:
    label: str
    points: int
    kind: str = "risk"


@dataclass(frozen=True)
class RiskScore:
    score: int
    band: str
    confidence: int
    confidenceLabel: str
    signals: list[RiskSignal]
    scored: bool = True
    outcome: str = "scored"
    hardMinimum: int = 0


def _cfg(configModule: Any | None, name: str, default: Any) -> Any:
    if configModule is None:
        return default
    return getattr(configModule, name, default)


def _clampInt(value: int, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, int(value)))


def _get(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _count(values: Any) -> int:
    if isinstance(values, (list, tuple, set)):
        return len(values)
    return 0


def _status(value: Any) -> str:
    return str(value or "").strip().upper()


def _dict(source: Any) -> dict:
    return source if isinstance(source, dict) else {}


def _safeInt(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safeFloat(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _band(score: int) -> str:
    if score >= 80:
        return "Escalate"
    if score >= 60:
        return "High Risk"
    if score >= 40:
        return "Manual Review"
    if score >= 20:
        return "Mild Review"
    return "Low Risk"


def _noScore(
    *,
    band: str,
    confidence: int,
    signals: list[RiskSignal],
    outcome: str,
) -> RiskScore:
    finalConfidence = _clampInt(confidence)
    return RiskScore(
        score=0,
        band=band,
        confidence=finalConfidence,
        confidenceLabel=_confidenceLabel(finalConfidence),
        signals=signals or [RiskSignal("Jane did not have enough data to score this account.", 0, "data")],
        scored=False,
        outcome=outcome,
    )


def _confidenceLabel(confidence: int) -> str:
    if confidence >= 75:
        return "High"
    if confidence >= 50:
        return "Medium"
    return "Low"


def _scoreFloor(configModule: Any | None) -> int:
    """Keep scored reports from implying perfect zero-risk certainty."""

    configuredFloor = _safeInt(_cfg(configModule, "bgRiskScoreFloor", 5), default=5)
    return _clampInt(configuredFloor, minimum=0, maximum=19)


def _scoreExternalSources(report: Any) -> tuple[int, int, list[RiskSignal]]:
    status = _status(_get(report, "externalSourceStatus", "SKIPPED"))
    matches = _get(report, "externalSourceMatches", []) or []
    details = _get(report, "externalSourceDetails", []) or []
    error = str(_get(report, "externalSourceError") or "").strip()
    signals: list[RiskSignal] = []
    scoreDelta = 0
    confidenceDelta = 0

    if status in {"", "SKIPPED"}:
        return 0, 0, []
    if status == "ERROR":
        confidenceDelta -= 6
        suffix = f" {error}" if error else ""
        return 0, confidenceDelta, [RiskSignal(f"External safety source scan failed.{suffix}", 0, "data")]
    if status == "PARTIAL":
        confidenceDelta -= 4
        suffix = f" {error}" if error else ""
        signals.append(RiskSignal(f"One external safety source failed, but Jane used the source(s) that responded.{suffix}", 0, "data"))

    if not isinstance(matches, (list, tuple)) or not matches:
        attemptedOkSources = [
            str(row.get("source") or "").strip()
            for row in list(details or [])
            if isinstance(row, dict) and str(row.get("status") or "").strip().upper() == "OK"
        ]
        if attemptedOkSources:
            signals.append(
                RiskSignal(
                    f"External safety source(s) returned no records: {', '.join(attemptedOkSources)}.",
                    -2,
                    "reassuring",
                )
            )
            return -2, confidenceDelta, signals
        return 0, confidenceDelta, signals

    for match in list(matches):
        if not isinstance(match, dict):
            continue
        source = str(match.get("source") or "").strip().lower()
        if source == "tase":
            scoreSum = _safeFloat(match.get("scoreSum"))
            guildCount = _safeInt(match.get("guildCount"))
            pastOffender = bool(match.get("pastOffender"))
            if scoreSum >= 150 or guildCount >= 5:
                points = 40
            elif scoreSum >= 80 or guildCount >= 3:
                points = 32
            elif scoreSum >= 30 or guildCount >= 2:
                points = 22
            elif scoreSum > 0 or guildCount >= 1:
                points = 14
            else:
                points = 8
            if pastOffender:
                points = min(48, points + 8)
            scoreDelta += points
            detailsText = f"score sum {scoreSum:g}" if scoreSum else "record present"
            if guildCount:
                detailsText += f" across {guildCount} server(s)"
            signals.append(RiskSignal(f"TASE matched Discord safety records ({detailsText}).", points))
            typeNames = [
                str(typeName).strip()
                for typeName in list(match.get("typeNames") or [])[:3]
                if str(typeName).strip()
            ]
            if typeNames:
                signals.append(RiskSignal(f"TASE categories: {', '.join(typeNames)}.", 0, "data"))
            if match.get("appealing"):
                signals.append(RiskSignal("TASE marks this record as appealing; review current context carefully.", 0, "data"))
        elif source == "moco-co":
            groupCount = _safeInt(match.get("groupCount"))
            if groupCount >= 10:
                points = 36
            elif groupCount >= 5:
                points = 28
            elif groupCount >= 2:
                points = 20
            elif groupCount >= 1:
                points = 14
            else:
                points = 8
            scoreDelta += points
            groupText = f"{groupCount} flagged/safety group(s)" if groupCount else "record present"
            username = str(match.get("username") or "").strip()
            usernameText = f" for `{username}`" if username else ""
            signals.append(RiskSignal(f"Moco-co matched Roblox safety records{usernameText} ({groupText}).", points))
            if match.get("lastSeen"):
                signals.append(RiskSignal(f"Moco-co last saw this account at `{match.get('lastSeen')}`.", 0, "data"))

    return scoreDelta, confidenceDelta, signals


def scoreReport(
    report: Any,
    *,
    configModule: Any | None = None,
) -> RiskScore:
    """Score a report for review priority.

    This is intentionally deterministic. Jane should explain what she noticed,
    not pretend a black-box model proved anything.
    """

    reviewBucket = bgBuckets.normalizeBgReviewBucket(
        _get(report, "reviewBucket"),
        default=bgBuckets.adultBgReviewBucket,
    )
    signals: list[RiskSignal] = []
    score = int(_cfg(configModule, "bgRiskScoreBase", 20) or 20)
    confidence = 100

    robloxUserId = _get(report, "robloxUserId")
    roverError = str(_get(report, "roverError") or "").strip()
    identitySource = str(_get(report, "identitySource") or "rover").strip().lower()
    hardMinimum = 0
    if identitySource in {"manual", "manual_username"}:
        confidence -= 5
        label = "Manual Roblox username override was used." if identitySource == "manual_username" else "Manual Roblox user ID override was used."
        signals.append(RiskSignal(label, 0, "data"))
    externalScoreDelta, externalConfidenceDelta, externalSignals = _scoreExternalSources(report)
    score += externalScoreDelta
    confidence += externalConfidenceDelta
    signals.extend(externalSignals)
    if not robloxUserId:
        confidence = min(confidence - 35, 35)
        if externalScoreDelta > 0:
            signals.append(
                RiskSignal(
                    "No Roblox account resolved. This score only reflects Discord-linked external safety records.",
                    0,
                    "data",
                )
            )
            if roverError:
                signals.append(RiskSignal(f"RoVer note: {roverError}", 0, "data"))
            finalRawScore = max(score, _scoreFloor(configModule))
            finalScore = _clampInt(finalRawScore)
            finalConfidence = _clampInt(confidence)
            return RiskScore(
                score=finalScore,
                band=_band(finalScore),
                confidence=finalConfidence,
                confidenceLabel=_confidenceLabel(finalConfidence),
                signals=signals,
                scored=True,
                outcome="discord_external_only",
            )
        signals.append(
            RiskSignal(
                "No Roblox account could be scored. Staff need to resolve identity first.",
                0,
                "data",
            )
        )
        if roverError:
            signals.append(RiskSignal(f"RoVer note: {roverError}", 0, "data"))
        return _noScore(
            band="Needs Identity Review",
            confidence=confidence,
            signals=signals,
            outcome="needs_identity",
        )

    directMatches = _get(report, "directMatches", []) or []
    for match in list(directMatches):
        if not isinstance(match, dict):
            continue
        matchType = str(match.get("type") or "").strip().lower()
        value = str(match.get("value") or "").strip()
        valueText = f" `{value}`" if value else ""
        note = str(match.get("note") or "").strip()
        minimumScore = _clampInt(_safeInt(match.get("minimumScore"), default=0), 0, 100)
        if matchType == "banned_user":
            minimumScore = max(95, minimumScore)
            hardMinimum = max(hardMinimum, minimumScore)
            suffix = f" Note: {note}" if note else ""
            signals.append(RiskSignal(f"Hard override: known banned Roblox ID{valueText} matched.{suffix}", minimumScore, "override"))
        elif matchType == "watchlist":
            minimumScore = minimumScore if minimumScore > 0 else 88
            hardMinimum = max(hardMinimum, minimumScore)
            suffix = f" Note: {note}" if note else ""
            signals.append(RiskSignal(f"Hard override: watchlist Roblox ID{valueText} matched.{suffix}", minimumScore, "override"))
        elif matchType == "roblox_user":
            minimumScore = minimumScore if minimumScore > 0 else 82
            hardMinimum = max(hardMinimum, minimumScore)
            suffix = f" Note: {note}" if note else ""
            signals.append(RiskSignal(f"Hard override: exact flagged Roblox ID{valueText} matched.{suffix}", minimumScore, "override"))
        elif matchType == "username":
            minimumScore = minimumScore if minimumScore > 0 else 82
            hardMinimum = max(hardMinimum, minimumScore)
            suffix = f" Note: {note}" if note else ""
            signals.append(RiskSignal(f"Hard override: exact flagged Roblox username{valueText} matched.{suffix}", minimumScore, "override"))

    ageDays = _get(report, "robloxAgeDays")
    try:
        ageDaysInt = int(ageDays) if ageDays is not None else None
    except (TypeError, ValueError):
        ageDaysInt = None
    if ageDaysInt is not None:
        if ageDaysInt < 1:
            score += 62
            signals.append(RiskSignal("Roblox account was created within the last day.", 62))
        elif ageDaysInt < 3:
            score += 38
            signals.append(RiskSignal(f"Roblox account is extremely new ({ageDaysInt} day(s)).", 38))
        elif ageDaysInt < 7:
            score += 28
            signals.append(RiskSignal(f"Roblox account is very new ({ageDaysInt} day(s)).", 28))
        elif ageDaysInt < 30:
            score += 15
            signals.append(RiskSignal(f"Roblox account is new ({ageDaysInt} day(s)).", 15))
        elif ageDaysInt < 100:
            score += 6
            signals.append(RiskSignal(f"Roblox account is under 100 days old ({ageDaysInt} day(s)).", 6))
        elif ageDaysInt >= 1095:
            score -= 8
            signals.append(RiskSignal("Roblox account is over 3 years old.", -8, "reassuring"))
        elif ageDaysInt >= 365:
            score -= 4
            signals.append(RiskSignal("Roblox account is over 1 year old.", -4, "reassuring"))
    elif robloxUserId:
        confidence -= 10
        signals.append(RiskSignal("Roblox account age could not be checked.", 0, "data"))

    flaggedGroups = _get(report, "flaggedGroups", []) or []
    flagMatches = _get(report, "flagMatches", []) or []
    groupStatus = _status(_get(report, "groupScanStatus"))
    if reviewBucket == bgBuckets.adultBgReviewBucket:
        flaggedGroupCount = _count(flaggedGroups)
        if flaggedGroupCount:
            points = min(54, 30 + max(0, flaggedGroupCount - 1) * 8)
            score += points
            signals.append(RiskSignal(f"{flaggedGroupCount} flagged Roblox group(s) matched.", points))
        for match in list(flagMatches or [])[:8]:
            if not isinstance(match, dict):
                continue
            matchType = str(match.get("type") or "").strip().lower()
            context = str(match.get("context") or "").strip().lower()
            if matchType == "username":
                score += 20
                signals.append(RiskSignal("Roblox username matched a flagged username rule.", 20))
            elif matchType == "keyword" and context == "username":
                score += 15
                signals.append(RiskSignal(f"Roblox username matched keyword `{match.get('value')}`.", 15))
            elif matchType == "keyword" and context == "group":
                score += 10
                signals.append(
                    RiskSignal(
                        f"Group keyword `{match.get('value')}` matched {match.get('groupName') or 'a group'}.",
                        10,
                    )
                )
        if groupStatus == "OK" and not flaggedGroupCount and not flagMatches:
            score -= 6
            signals.append(RiskSignal("Group scan found no configured flags.", -6, "reassuring"))
        elif groupStatus in {"", "SKIPPED"} and robloxUserId:
            confidence -= 15
            signals.append(RiskSignal("Group scan did not run.", 0, "data"))
        elif groupStatus not in {"OK", "", "SKIPPED"}:
            confidence -= 20
            signals.append(RiskSignal(f"Group scan status: {groupStatus}.", 0, "data"))

        groupSummary = _dict(_get(report, "groupSummary", {}) or {})
        groupCount = _safeInt(groupSummary.get("totalGroups"), _count(_get(report, "groups", []) or []))
        baseRankGroups = _safeInt(groupSummary.get("baseRankGroups"))
        elevatedRankGroups = _safeInt(groupSummary.get("elevatedRankGroups"))
        ownerRankGroups = _safeInt(groupSummary.get("ownerRankGroups"))
        knownMemberCountGroups = _safeInt(groupSummary.get("knownMemberCountGroups"))
        smallGroups = _safeInt(groupSummary.get("smallGroups"))
        largeGroups = _safeInt(groupSummary.get("largeGroups"))
        veryLargeGroups = _safeInt(groupSummary.get("veryLargeGroups"))
        verifiedGroups = _safeInt(groupSummary.get("verifiedGroups"))
        try:
            baseRatio = float(groupSummary.get("baseRankRatio") or 0)
        except (TypeError, ValueError):
            baseRatio = 0.0
        try:
            smallGroupRatio = float(groupSummary.get("smallGroupRatio") or 0)
        except (TypeError, ValueError):
            smallGroupRatio = 0.0
        try:
            elevatedRatio = float(groupSummary.get("elevatedRankRatio") or 0)
        except (TypeError, ValueError):
            elevatedRatio = 0.0
        if groupStatus == "OK":
            if groupCount >= 50 and not flaggedGroupCount and not flagMatches:
                score -= 5
                signals.append(RiskSignal("Group spread looks established: 50+ groups and no configured group flags.", -5, "reassuring"))
            elif groupCount >= 20 and not flaggedGroupCount and not flagMatches:
                score -= 3
                signals.append(RiskSignal("Group spread looks reasonably established: 20+ groups and no configured group flags.", -3, "reassuring"))
            if knownMemberCountGroups >= 8 and largeGroups >= 5 and not flaggedGroupCount and not flagMatches:
                score -= 3
                signals.append(RiskSignal(f"Group quality looks established: {largeGroups} large public group(s) in the scanned set.", -3, "reassuring"))
            elif knownMemberCountGroups >= 4 and largeGroups >= 2 and not flaggedGroupCount and not flagMatches:
                score -= 2
                signals.append(RiskSignal(f"Group quality includes {largeGroups} large public group(s) in the scanned set.", -2, "reassuring"))
            elif knownMemberCountGroups >= 4 and veryLargeGroups >= 2 and not flaggedGroupCount and not flagMatches:
                score -= 2
                signals.append(RiskSignal(f"Group quality has {veryLargeGroups} very large public group(s).", -2, "reassuring"))
            if verifiedGroups >= 2 and not flaggedGroupCount and not flagMatches:
                score -= 2
                signals.append(RiskSignal(f"Account is in {verifiedGroups} Roblox-verified group(s).", -2, "reassuring"))
            if groupCount >= 15 and baseRatio >= 0.6 and not flaggedGroupCount:
                score -= 2
                signals.append(RiskSignal(f"Most group memberships are base-rank ({baseRankGroups}/{groupCount}).", -2, "reassuring"))
            if ageDaysInt is not None and ageDaysInt >= 365 and groupCount <= 1:
                score += 4
                signals.append(RiskSignal("Older Roblox account has a very thin public group footprint.", 4))
            if ageDaysInt is not None and ageDaysInt < 100 and knownMemberCountGroups >= 8 and smallGroupRatio >= 0.75:
                score += 3
                signals.append(RiskSignal(f"Newer account's group footprint is mostly tiny groups ({smallGroups}/{knownMemberCountGroups}).", 3))
            elif knownMemberCountGroups >= 8 and smallGroupRatio >= 0.75:
                signals.append(RiskSignal(f"Most known group memberships are small groups ({smallGroups}/{knownMemberCountGroups}); verify context manually.", 0, "data"))
            if groupCount >= 10 and elevatedRankGroups >= 8 and elevatedRatio >= 0.35:
                signals.append(RiskSignal(f"Elevated role density is high ({elevatedRankGroups}/{groupCount}); verify context manually.", 0, "data"))
            elif elevatedRankGroups >= 8:
                signals.append(RiskSignal(f"Account has elevated roles in {elevatedRankGroups} group(s); verify context manually.", 0, "data"))
            if ownerRankGroups >= 3:
                signals.append(RiskSignal(f"Account owns or leads {ownerRankGroups} group(s); verify context manually.", 0, "data"))

    flaggedItems = _get(report, "flaggedItems", []) or []
    inventoryStatus = _status(_get(report, "inventoryScanStatus"))
    if reviewBucket == bgBuckets.adultBgReviewBucket:
        itemPoints = 0
        for item in list(flaggedItems or [])[:10]:
            if not isinstance(item, dict):
                continue
            matchType = str(item.get("matchType") or "").strip().lower()
            if matchType in {"item", "creator"}:
                itemPoints += 25
            elif matchType == "keyword":
                itemPoints += 12
            else:
                itemPoints += 15
        if itemPoints:
            itemPoints = min(itemPoints, 45)
            score += itemPoints
            signals.append(RiskSignal(f"{_count(flaggedItems)} flagged inventory item(s) matched.", itemPoints))
        if inventoryStatus == "PRIVATE":
            score += 4
            confidence -= 30
            signals.append(
                RiskSignal(
                    "Inventory is private or hidden. Treat this as incomplete data, not proof.",
                    4,
                    "data",
                )
            )
        elif inventoryStatus == "OK" and not flaggedItems:
            score -= 4
            signals.append(RiskSignal("Inventory scan found no configured item flags.", -4, "reassuring"))
        elif inventoryStatus in {"", "SKIPPED"} and robloxUserId:
            confidence -= 15
            signals.append(RiskSignal("Inventory scan did not run.", 0, "data"))
        elif inventoryStatus not in {"OK", "", "SKIPPED"}:
            confidence -= 15
            signals.append(RiskSignal(f"Inventory scan status: {inventoryStatus}.", 0, "data"))

    flaggedFavoriteGames = _get(report, "flaggedFavoriteGames", []) or []
    favoriteGameStatus = _status(_get(report, "favoriteGameScanStatus"))
    if reviewBucket == bgBuckets.adultBgReviewBucket:
        gamePoints = 0
        for game in list(flaggedFavoriteGames or [])[:10]:
            if not isinstance(game, dict):
                continue
            matchType = str(game.get("matchType") or "").strip().lower()
            gamePoints += 18 if matchType == "game" else 10
        if gamePoints:
            gamePoints = min(gamePoints, 35)
            score += gamePoints
            signals.append(RiskSignal(f"{_count(flaggedFavoriteGames)} flagged favorite game(s) matched.", gamePoints))
        if favoriteGameStatus == "OK" and not flaggedFavoriteGames:
            score -= 2
            signals.append(RiskSignal("Favorite-game scan found no configured flags.", -2, "reassuring"))
        elif favoriteGameStatus in {"", "SKIPPED"} and robloxUserId:
            confidence -= 8
            signals.append(RiskSignal("Favorite-game scan did not run.", 0, "data"))
        elif favoriteGameStatus not in {"OK", "", "SKIPPED"}:
            confidence -= 10
            signals.append(RiskSignal(f"Favorite-game scan status: {favoriteGameStatus}.", 0, "data"))

    outfitStatus = _status(_get(report, "outfitScanStatus"))
    if reviewBucket == bgBuckets.adultBgReviewBucket:
        if outfitStatus in {"", "SKIPPED"} and robloxUserId:
            confidence -= 5
            signals.append(RiskSignal("Outfit scan did not run.", 0, "data"))
        elif outfitStatus not in {"OK", "", "SKIPPED"}:
            confidence -= 8
            signals.append(RiskSignal(f"Outfit scan status: {outfitStatus}.", 0, "data"))

    flaggedBadges = _get(report, "flaggedBadges", []) or []
    badgeStatus = _status(_get(report, "badgeScanStatus"))
    badgeHistoryStatus = _status(_get(report, "badgeHistoryScanStatus"))
    badgeHistorySample = _get(report, "badgeHistorySample", []) or []
    badgeTimelineSummary = _dict(_get(report, "badgeTimelineSummary", {}) or {})
    flaggedBadgeCount = _count(flaggedBadges)
    if flaggedBadgeCount:
        points = min(50, 28 + max(0, flaggedBadgeCount - 1) * 8)
        score += points
        signals.append(RiskSignal(f"{flaggedBadgeCount} flagged badge(s) matched.", points))
    elif badgeStatus == "OK":
        points = -4 if reviewBucket == bgBuckets.adultBgReviewBucket else -8
        score += points
        signals.append(RiskSignal("Badge scan found no configured badge flags.", points, "reassuring"))
    elif badgeStatus in {"", "SKIPPED"}:
        if reviewBucket == bgBuckets.minorBgReviewBucket:
            confidence -= 20
            signals.append(RiskSignal("Badge scan did not run for the -18 route.", 0, "data"))
    elif badgeStatus not in {"OK", "", "SKIPPED"}:
        confidence -= 15
        signals.append(RiskSignal(f"Badge scan status: {badgeStatus}.", 0, "data"))

    badgeHistoryCount = _count(badgeHistorySample)
    if badgeHistoryStatus == "OK":
        awardDateStatus = _status(badgeTimelineSummary.get("awardDateStatus"))
        timelineQuality = str(badgeTimelineSummary.get("quality") or "").strip().lower()
        datedBadges = _safeInt(badgeTimelineSummary.get("datedBadges"))
        spanDays = _safeInt(badgeTimelineSummary.get("spanDays"))
        distinctYears = _safeInt(badgeTimelineSummary.get("distinctAwardYears"))
        maxSameDayAwards = _safeInt(badgeTimelineSummary.get("maxSameDayAwards"))
        try:
            maxSameDayRatio = float(badgeTimelineSummary.get("maxSameDayRatio") or 0)
        except (TypeError, ValueError):
            maxSameDayRatio = 0.0
        if awardDateStatus == "OK" and datedBadges > 0:
            if timelineQuality == "multi_year_deep":
                score -= 8
                signals.append(
                    RiskSignal(
                        f"True badge timeline is deep: {datedBadges} awarded badge(s) across {distinctYears} year(s).",
                        -8,
                        "reassuring",
                    )
                )
            elif timelineQuality == "established":
                score -= 5
                signals.append(
                    RiskSignal(
                        f"True badge timeline looks established: {datedBadges} awarded badge(s) over {spanDays} day(s).",
                        -5,
                        "reassuring",
                    )
                )
            elif datedBadges >= 25:
                score -= 2
                signals.append(RiskSignal(f"True badge timeline has {datedBadges} dated awarded badge(s).", -2, "reassuring"))
            if timelineQuality == "burst_heavy" and ageDaysInt is not None and ageDaysInt >= 100:
                score += 5
                signals.append(
                    RiskSignal(
                        f"Badge timeline is burst-heavy: {maxSameDayAwards} award(s) on one day ({maxSameDayRatio:.0%} of dated sample).",
                        5,
                    )
                )
            if timelineQuality == "thin" and ageDaysInt is not None and ageDaysInt >= 365:
                score += 3
                signals.append(RiskSignal("Older Roblox account has a very thin dated badge timeline.", 3))
        elif awardDateStatus == "ERROR":
            confidence -= 4
            signals.append(RiskSignal("Badge award-date timeline could not be verified.", 0, "data"))
        elif badgeHistoryCount >= 25:
            signals.append(RiskSignal(f"Public badge sample has {badgeHistoryCount} badge(s), but no true award timeline.", 0, "data"))
        if badgeHistoryCount == 0 and ageDaysInt is not None and ageDaysInt >= 365:
            score += 4
            signals.append(RiskSignal("Older Roblox account has no public badges in the sampled badge list.", 4))
    elif badgeHistoryStatus in {"", "SKIPPED"}:
        confidence -= 5
        signals.append(RiskSignal("Public badge-history sample did not run.", 0, "data"))
    elif badgeHistoryStatus not in {"OK", "", "SKIPPED"}:
        confidence -= 8
        signals.append(RiskSignal(f"Public badge-history sample status: {badgeHistoryStatus}.", 0, "data"))

    priorSummary = _dict(_get(report, "priorReportSummary", {}) or {})
    priorReports = _safeInt(priorSummary.get("totalRecent"))
    highRiskRecent = _safeInt(priorSummary.get("highRiskRecent"))
    escalateRecent = _safeInt(priorSummary.get("escalateRecent"))
    noScoreRecent = _safeInt(priorSummary.get("noScoreRecent"))
    queueApprovals = _safeInt(priorSummary.get("queueApprovals"))
    queueRejections = _safeInt(priorSummary.get("queueRejections"))
    if queueRejections > 0:
        points = min(24, 14 + max(0, queueRejections - 1) * 4)
        score += points
        signals.append(RiskSignal(f"Prior Jane BG queue rejection(s) found: {queueRejections}.", points))
    if queueApprovals > 0 and queueRejections <= 0:
        points = -8 if queueApprovals >= 2 else -5
        score += points
        signals.append(RiskSignal(f"Prior Jane BG queue approval(s) found: {queueApprovals}.", points, "reassuring"))
    if escalateRecent > 0:
        points = min(16, 8 + max(0, escalateRecent - 1) * 4)
        score += points
        signals.append(RiskSignal(f"Prior Jane intelligence scan(s) reached Escalate: {escalateRecent}.", points))
    elif highRiskRecent > 0:
        points = min(10, 6 + max(0, highRiskRecent - 1) * 2)
        score += points
        signals.append(RiskSignal(f"Prior Jane intelligence scan(s) were high risk: {highRiskRecent}.", points))
    if noScoreRecent >= 2:
        score += 4
        signals.append(RiskSignal(f"Repeated prior no-score intelligence result(s): {noScoreRecent}.", 4))
    if priorReports > 0:
        signals.append(RiskSignal(f"Prior Jane intelligence reports found: {priorReports}.", 0, "data"))

    failedMajorCategories = 0
    if robloxUserId and ageDaysInt is None:
        failedMajorCategories += 1
    majorStatuses = [groupStatus, inventoryStatus, favoriteGameStatus, badgeStatus]
    for status in majorStatuses:
        if status in {"ERROR", "NO_ROVER"}:
            failedMajorCategories += 1
    if failedMajorCategories >= 2:
        signals.append(
            RiskSignal(
                "Too many major data sources failed. Staff should rerun later or review manually.",
                0,
                "data",
            )
        )
        if hardMinimum <= 0:
            return _noScore(
                band="Insufficient Data",
                confidence=min(confidence, 35),
                signals=signals,
                outcome="insufficient_data",
            )
        confidence = min(confidence, 45)

    if not signals:
        signals.append(RiskSignal("No configured risk signals matched.", 0, "reassuring"))

    finalRawScore = max(score, hardMinimum)
    if hardMinimum <= 0:
        finalRawScore = max(finalRawScore, _scoreFloor(configModule))
    finalScore = _clampInt(finalRawScore)
    finalConfidence = _clampInt(confidence)
    return RiskScore(
        score=finalScore,
        band=_band(finalScore),
        confidence=finalConfidence,
        confidenceLabel=_confidenceLabel(finalConfidence),
        signals=signals,
        scored=True,
        outcome="scored",
        hardMinimum=hardMinimum,
    )


def compactScoreLine(score: RiskScore) -> str:
    if not score.scored:
        return f"Not scored - {score.band} ({score.confidenceLabel} confidence)"
    return f"{score.score}/100 - {score.band} ({score.confidenceLabel} confidence)"


def signalLines(score: RiskScore, *, limit: int = 8) -> list[str]:
    rows: list[str] = []
    for signal in score.signals[: max(1, int(limit or 8))]:
        if signal.kind == "override" and signal.points > 0:
            prefix = f"min {signal.points}"
        elif signal.points > 0:
            prefix = f"+{signal.points}"
        elif signal.points < 0:
            prefix = str(signal.points)
        else:
            prefix = "0"
        rows.append(f"`{prefix}` {signal.label}")
    if len(score.signals) > limit:
        rows.append(f"... and {len(score.signals) - limit} more signal(s)")
    return rows
