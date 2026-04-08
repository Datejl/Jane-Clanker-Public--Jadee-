from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from features.staff.sessions import bgScanPipeline
from features.staff.sessions.bgBuckets import adultBgReviewBucket, normalizeBgReviewBucket


def parseSessionId(customId: str) -> int:
    # format: prefix:action:sessionId
    return int(customId.split(":")[-1])


def isRecentIsoScan(scanAt: Optional[str], cacheDays: int) -> bool:
    if not scanAt:
        return False
    try:
        scanTime = datetime.fromisoformat(scanAt)
    except ValueError:
        return False
    return (datetime.now() - scanTime).days < int(cacheDays)


def bgCandidates(attendees: list[dict], reviewBucket: Optional[str] = None) -> list[dict]:
    candidates = [attendee for attendee in attendees if attendee["examGrade"] == "PASS"]
    if reviewBucket is None:
        return candidates
    normalizedBucket = normalizeBgReviewBucket(reviewBucket)
    return [
        attendee
        for attendee in candidates
        if normalizeBgReviewBucket(attendee.get("bgReviewBucket"), default=adultBgReviewBucket) == normalizedBucket
    ]


def isBgQueueComplete(candidates: list[dict], reviewBucket: Optional[str] = None) -> bool:
    if reviewBucket is not None:
        candidates = bgCandidates(candidates, reviewBucket)
    if not candidates:
        return True
    return all(candidate.get("bgStatus") != "PENDING" for candidate in candidates)


def loadJsonList(value: Optional[str]) -> list:
    if not value:
        return []
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def normalizeIntSet(values: list[str | int]) -> set[int]:
    normalized: set[int] = set()
    for value in values:
        try:
            normalized.add(int(value))
        except (TypeError, ValueError):
            continue
    return normalized


def isPrivateInventoryStatus(status: int, error: Optional[str]) -> bool:
    return bgScanPipeline.isPrivateInventoryStatus(status, error)


