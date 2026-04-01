from __future__ import annotations

import importlib
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


BASE_DIR = Path(__file__).resolve().parent
ENGINE_MODULE = "features.staff.ribbons.ribbonengine_headless"
OUTPUT_DIR = BASE_DIR / "output"


@dataclass
class RibbonEngineStatus:
    available: bool
    reason: str


def _loadEngine():
    return importlib.import_module(ENGINE_MODULE)


def getEngineStatus() -> RibbonEngineStatus:
    try:
        _loadEngine()
        return RibbonEngineStatus(True, "")
    except ModuleNotFoundError as exc:
        message = str(exc)
        if "PIL" in message:
            return RibbonEngineStatus(
                False,
                "Pillow (PIL) is not installed. Install with: pip install Pillow",
            )
        return RibbonEngineStatus(False, message)
    except Exception as exc:
        return RibbonEngineStatus(False, str(exc))


def listAssetsByCategory() -> dict[str, list[str]]:
    status = getEngineStatus()
    if status.available:
        engine = _loadEngine()
        getAllAssetNames = getattr(engine, "_allAssetNames")
        names = getAllAssetNames()
        return {str(key): list(value) for key, value in names.items()}

    categories = {
        "sacks": BASE_DIR / "Awards",
        "gorget": BASE_DIR / "Commendations",
        "spbadge": BASE_DIR / "Commendations",
        "commendations": BASE_DIR / "Commendations",
        "corpus": BASE_DIR / "Commendations",
        "ribbons": BASE_DIR / "Ribbons",
    }
    out: dict[str, list[str]] = {}
    for key, directory in categories.items():
        if not directory.exists():
            out[key] = []
            continue
        names = []
        for filename in sorted(os.listdir(directory), key=str.lower):
            if filename.lower().endswith(".png"):
                names.append(os.path.splitext(filename)[0])
        out[key] = names
    return out


def validateSelection(selectedNames: list[str], strict: bool = False) -> tuple[list[str], list[str]]:
    names = [name.strip() for name in selectedNames if isinstance(name, str) and name.strip()]
    allKnown = set()
    for _, items in listAssetsByCategory().items():
        allKnown.update(items)
    unknown = sorted({name for name in names if name not in allKnown})
    if strict and unknown:
        raise ValueError("Unknown ribbon names: " + ", ".join(unknown))
    known = [name for name in names if name not in set(unknown)]
    return known, unknown


def _buildOutputPath(nameplate: str = "") -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safeName = "".join(ch for ch in (nameplate or "") if ch.isalnum() or ch in {" ", "_", "-"}).strip()
    safeName = safeName.replace(" ", "_")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    suffix = f"_{safeName}" if safeName else ""
    return OUTPUT_DIR / f"ribbon_{stamp}{suffix}.png"


def renderSelection(
    *,
    selectedNames: list[str],
    nameplate: str = "",
    baseImagePath: Optional[str] = None,
    strict: bool = False,
    allowBlankName: bool = False,
    outputPath: Optional[str] = None,
    embedMetadata: bool = True,
) -> dict:
    status = getEngineStatus()
    if not status.available:
        raise RuntimeError(status.reason)

    engine = _loadEngine()
    knownNames, unknown = validateSelection(selectedNames, strict=strict)
    if strict and unknown:
        raise ValueError("Unknown ribbon names: " + ", ".join(unknown))

    requireName = not allowBlankName and not baseImagePath
    renderFunc = getattr(engine, "renderRibbonImage")
    image, info = renderFunc(
        selectedNames=knownNames,
        nameplate=nameplate,
        baseImagePath=baseImagePath,
        requireNameForNew=requireName,
    )

    if outputPath:
        targetPath = Path(outputPath)
    else:
        targetPath = _buildOutputPath(nameplate)
    targetPath.parent.mkdir(parents=True, exist_ok=True)

    saveFunc = getattr(engine, "saveRenderedImage")
    saveFunc(
        image=image,
        outputPath=str(targetPath),
        selectedNames=knownNames,
        nameplate=nameplate,
        embedMetadata=embedMetadata,
    )
    return {
        "path": str(targetPath),
        "usedCount": len(knownNames),
        "unknown": unknown,
        "info": info,
    }


def renderSelectionToTemp(
    *,
    selectedNames: list[str],
    nameplate: str = "",
    baseImagePath: Optional[str] = None,
    strict: bool = False,
    allowBlankName: bool = False,
    embedMetadata: bool = True,
) -> dict:
    tempFile = tempfile.NamedTemporaryFile(prefix="jane_ribbon_", suffix=".png", delete=False)
    tempFile.close()
    return renderSelection(
        selectedNames=selectedNames,
        nameplate=nameplate,
        baseImagePath=baseImagePath,
        strict=strict,
        allowBlankName=allowBlankName,
        outputPath=tempFile.name,
        embedMetadata=embedMetadata,
    )

