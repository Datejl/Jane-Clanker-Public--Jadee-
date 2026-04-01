from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

import config


log = logging.getLogger(__name__)

_cachedPath: str = ""
_cachedMtime: Optional[float] = None
_cachedLayouts: list[dict[str, Any]] = []


def _normalizeKey(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _resolveLayoutsPath() -> Path:
    rawPath = str(
        getattr(config, "departmentOrbatLayoutsPath", "departmentOrbat/layouts.json")
        or "departmentOrbat/layouts.json"
    ).strip()
    basePath = Path(getattr(config, "__file__", __file__)).resolve().parent
    path = Path(rawPath)
    if not path.is_absolute():
        path = basePath / path
    return path


def _normalizeLayouts(rawLayouts: Any) -> list[dict[str, Any]]:
    if not isinstance(rawLayouts, list):
        return []
    return [entry for entry in rawLayouts if isinstance(entry, dict)]


def loadDepartmentLayouts(forceReload: bool = False) -> list[dict[str, Any]]:
    global _cachedPath
    global _cachedMtime
    global _cachedLayouts

    path = _resolveLayoutsPath()
    pathKey = str(path)
    mtime: Optional[float] = None
    if path.exists():
        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = None

    if (
        not forceReload
        and _cachedLayouts
        and _cachedPath == pathKey
        and _cachedMtime == mtime
    ):
        return [dict(entry) for entry in _cachedLayouts]

    loaded: list[dict[str, Any]] = []
    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            loaded = _normalizeLayouts(payload)
        except Exception:
            log.exception("Failed loading department ORBAT layouts from %s.", path)

    # Backward compatibility fallback for older config.py layouts.
    if not loaded:
        loaded = _normalizeLayouts(getattr(config, "departmentOrbatLayouts", []))

    _cachedPath = pathKey
    _cachedMtime = mtime
    _cachedLayouts = loaded
    return [dict(entry) for entry in loaded]


def hasDepartmentLayoutsConfigured() -> bool:
    return bool(loadDepartmentLayouts())


def findLayoutByDivisionKey(divisionKey: str) -> Optional[dict[str, Any]]:
    target = _normalizeKey(divisionKey)
    if not target:
        return None
    for layout in loadDepartmentLayouts():
        if _normalizeKey(layout.get("divisionKey")) == target:
            return layout
    return None
