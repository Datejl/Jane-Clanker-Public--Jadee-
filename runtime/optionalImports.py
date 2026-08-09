from __future__ import annotations

import importlib
from types import ModuleType


def isRequestedModuleMissing(exc: ModuleNotFoundError, moduleName: str) -> bool:
    """Return whether an import failed because the requested module is absent.

    ``ModuleNotFoundError`` is also raised when a module exists but one of its
    dependencies is missing. Optional feature loading should only suppress the
    former; suppressing the latter silently replaces broken production code
    with a fallback and makes deployment failures very difficult to diagnose.
    """

    missingName = str(getattr(exc, "name", "") or "").strip()
    requestedName = str(moduleName or "").strip()
    if not missingName or not requestedName:
        return False
    return requestedName == missingName or requestedName.startswith(f"{missingName}.")


def importOptionalModule(moduleName: str, *, enabled: bool = True) -> ModuleType | None:
    if not enabled:
        return None
    try:
        return importlib.import_module(moduleName)
    except ModuleNotFoundError as exc:
        if isRequestedModuleMissing(exc, moduleName):
            return None
        raise
