from __future__ import annotations

from typing import Any


def _toIntList(values: Any) -> list[int]:
    out: list[int] = []
    for value in values or []:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            out.append(parsed)
    return out


def _toPositiveInt(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _normalizeAppKey(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _isFinal(status: str) -> bool:
    return (status or "").upper() in {"APPROVED", "DENIED"}


def _parseDays(value: str) -> int:
    raw = (value or "").strip().lower()
    if raw.endswith("d"):
        raw = raw[:-1]
    days = int(raw or "7")
    return max(1, min(days, 365))
