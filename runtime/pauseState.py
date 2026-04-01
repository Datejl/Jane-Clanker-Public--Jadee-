from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _nowUtc() -> datetime:
    return datetime.now(timezone.utc)


class PauseController:
    def __init__(self) -> None:
        self._paused = False
        self._pausedAt: datetime | None = None
        self._pausedById = 0
        self._pausedByLabel = ""
        self._resumedAt: datetime | None = None
        self._resumedById = 0
        self._resumedByLabel = ""

    def isPaused(self) -> bool:
        return bool(self._paused)

    def setPaused(self, paused: bool, *, actorId: int = 0, actorLabel: str = "") -> bool:
        normalizedPaused = bool(paused)
        if self._paused == normalizedPaused:
            return self._paused
        self._paused = normalizedPaused
        if normalizedPaused:
            self._pausedAt = _nowUtc()
            self._pausedById = int(actorId or 0)
            self._pausedByLabel = str(actorLabel or "").strip()
        else:
            self._resumedAt = _nowUtc()
            self._resumedById = int(actorId or 0)
            self._resumedByLabel = str(actorLabel or "").strip()
        return self._paused

    def toggle(self, *, actorId: int = 0, actorLabel: str = "") -> bool:
        return self.setPaused(
            not self._paused,
            actorId=actorId,
            actorLabel=actorLabel,
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "paused": bool(self._paused),
            "pausedAt": self._pausedAt.isoformat() if self._pausedAt else "",
            "pausedById": int(self._pausedById or 0),
            "pausedByLabel": str(self._pausedByLabel or ""),
            "resumedAt": self._resumedAt.isoformat() if self._resumedAt else "",
            "resumedById": int(self._resumedById or 0),
            "resumedByLabel": str(self._resumedByLabel or ""),
        }
