from __future__ import annotations

import logging
from typing import Any, Optional

import config

log = logging.getLogger(__name__)

_prefix = "division-clockin:"


def normalizeSubdepartmentKey(rawValue: str) -> str:
    normalized = "".join(ch for ch in str(rawValue or "").strip().lower() if ch.isalnum() or ch in {"-", "_"})
    return normalized or "unknown"


def buildDivisionClockinSessionType(subdepartmentKey: str) -> str:
    return f"{_prefix}{normalizeSubdepartmentKey(subdepartmentKey)}"


def extractDivisionClockinSubdepartment(sessionType: str) -> Optional[str]:
    normalized = str(sessionType or "").strip().lower()
    if not normalized.startswith(_prefix):
        return None
    subdepartment = normalized[len(_prefix) :].strip()
    return subdepartment or None


def getDivisionClockinSheetMapping(subdepartmentKey: str) -> Optional[dict[str, Any]]:
    mapping = getattr(config, "divisionClockinSheetMap", {}) or {}
    if not isinstance(mapping, dict):
        return None
    key = normalizeSubdepartmentKey(subdepartmentKey)
    value = mapping.get(key)
    return value if isinstance(value, dict) else None


async def syncDivisionClockinSkeleton(sessionId: int, subdepartmentKey: str, attendeeCount: int) -> dict[str, Any]:
    mapping = getDivisionClockinSheetMapping(subdepartmentKey)
    mapped = mapping is not None
    log.info(
        "Division clock-in skeleton sync requested: session=%s subdepartment=%s attendees=%s mapped=%s",
        int(sessionId),
        normalizeSubdepartmentKey(subdepartmentKey),
        int(attendeeCount),
        mapped,
    )
    return {
        "ok": True,
        "mapped": mapped,
        "mapping": mapping or {},
    }
