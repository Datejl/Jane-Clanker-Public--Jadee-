from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from features.staff.sessions import bgScanPipeline


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


def bgCandidates(attendees: list[dict]) -> list[dict]:
    return [attendee for attendee in attendees if attendee["examGrade"] == "PASS"]


def isBgQueueComplete(candidates: list[dict]) -> bool:
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


