from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Callable


def _tryImportModule(moduleName: str, *, enabled: bool = True) -> Any | None:
    if not enabled:
        return None
    try:
        return importlib.import_module(moduleName)
    except ModuleNotFoundError:
        return None


class _DepartmentOrbatSheetsFallback:
    @staticmethod
    def hasConfiguredLayouts() -> bool:
        return False

    @staticmethod
    def touchupAllDepartmentSheets() -> dict[str, object]:
        return {"reason": "private extensions disabled", "results": {}}


class _OrbatSheetsFallback:
    @staticmethod
    def organizeOrbatRows() -> dict[str, object]:
        return {"reason": "private extensions disabled"}

    @staticmethod
    def incrementEventCount(*args, **kwargs) -> int:
        return 0


class _OrbatRoleSyncFallback:
    @staticmethod
    async def syncMemberRoleOrbats(*args, **kwargs) -> dict[str, object]:
        return {"changed": False, "results": []}


def _loadMultiOrbatRegistryFallback() -> dict[str, object]:
    return {}


@dataclass(slots=True)
class PrivateServices:
    privateExtensionsEnabled: bool
    departmentOrbatSheets: Any
    orbatSheets: Any
    orbatRoleSync: Any
    loadMultiOrbatRegistry: Callable[[], dict[str, object]]
    orbatAuditRuntime: Any | None
    serverSafetyService: Any | None
    gitUpdateModule: Any | None
    processControlModule: Any | None


def loadPrivateServices(*, configModule: Any) -> PrivateServices:
    privateExtensionsEnabled = bool(getattr(configModule, "enablePrivateExtensions", True))

    departmentOrbatSheets = _tryImportModule(
        "features.staff.departmentOrbat.sheets",
        enabled=privateExtensionsEnabled,
    ) or _DepartmentOrbatSheetsFallback()
    orbatMultiRegistry = _tryImportModule(
        "features.staff.orbat.multiRegistry",
        enabled=privateExtensionsEnabled,
    )
    orbatRoleSync = _tryImportModule(
        "features.staff.orbat.roleSync",
        enabled=privateExtensionsEnabled,
    ) or _OrbatRoleSyncFallback()
    orbatSheets = _tryImportModule(
        "features.staff.orbat.sheets",
        enabled=privateExtensionsEnabled,
    ) or _OrbatSheetsFallback()

    return PrivateServices(
        privateExtensionsEnabled=privateExtensionsEnabled,
        departmentOrbatSheets=departmentOrbatSheets,
        orbatSheets=orbatSheets,
        orbatRoleSync=orbatRoleSync,
        loadMultiOrbatRegistry=(
            getattr(orbatMultiRegistry, "loadMultiOrbatRegistry", None)
            or _loadMultiOrbatRegistryFallback
        ),
        orbatAuditRuntime=_tryImportModule(
            "runtime.orbatAudit",
            enabled=privateExtensionsEnabled,
        ),
        serverSafetyService=_tryImportModule(
            "features.operations.serverSafety.service",
            enabled=privateExtensionsEnabled,
        ),
        gitUpdateModule=_tryImportModule("runtime.gitUpdate"),
        processControlModule=_tryImportModule("runtime.processControl"),
    )

