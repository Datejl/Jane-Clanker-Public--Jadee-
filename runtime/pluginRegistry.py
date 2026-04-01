from __future__ import annotations

import importlib
from dataclasses import dataclass, asdict
from typing import Any

from runtime import extensionLayout as runtimeExtensionLayout


@dataclass(slots=True)
class PluginManifest:
    extensionName: str
    displayName: str
    category: str
    description: str


class PluginRegistry:
    def __init__(self) -> None:
        self._pluginsByExtension: dict[str, PluginManifest] = {}

    def registerExtension(self, extensionName: str) -> PluginManifest:
        manifest = self._loadManifest(extensionName)
        self._pluginsByExtension[extensionName] = manifest
        return manifest

    def listPlugins(self) -> list[dict[str, Any]]:
        manifests = sorted(self._pluginsByExtension.values(), key=lambda item: (item.category, item.displayName))
        return [asdict(item) for item in manifests]

    def _loadManifest(self, extensionName: str) -> PluginManifest:
        try:
            module = importlib.import_module(extensionName)
        except Exception:
            return self._defaultManifest(extensionName)

        raw = getattr(module, "PLUGIN_MANIFEST", None)
        if isinstance(raw, dict):
            return PluginManifest(
                extensionName=extensionName,
                displayName=str(raw.get("displayName") or extensionName.rsplit(".", 1)[-1]),
                category=str(raw.get("category") or extensionName.split(".", 1)[0]),
                description=str(raw.get("description") or "").strip(),
            )
        return self._defaultManifest(extensionName)

    def _defaultManifest(self, extensionName: str) -> PluginManifest:
        leaf = extensionName.rsplit(".", 1)[-1]
        layer = runtimeExtensionLayout.classifyExtensionLayer(extensionName)
        if layer == "core":
            category = extensionName.split(".", 1)[0]
        else:
            category = f"{layer}-plugins"
        displayName = leaf.replace("Cog", "").replace("cog", "").replace("_", " ").strip().title()
        return PluginManifest(
            extensionName=extensionName,
            displayName=displayName or leaf,
            category=category,
            description="",
        )
