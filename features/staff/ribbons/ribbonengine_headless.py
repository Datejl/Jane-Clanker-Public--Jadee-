import argparse
import datetime
import json
import os
import sys
from typing import Iterable, Optional

from PIL import Image, PngImagePlugin


# -------------------------------
# Part coordinates (top-left positions)
PART_COORDS = {
    "corpus": (8, 16),
    "nametape": (13, 31),
    "sacks": (14, 62),
    "commendations": (8, 25),
    "ribbons": (80, 33),
    "gorget": (43, 0),
    "spbadge": (90, 59),
}

# -------------------------------
# Medal layout (pockets)
POCKET_COL_SPACING = 15
POCKET_RIGHT_OFFSET = 71
POCKET_X_OFFSET = -14

# Fine-tuning offsets
CORPUS_X_OFFSET = 0
RIBBONS_RIGHT_ALIGN_OFFSET = -2

# Medal name lists (filenames without .png)
AWARD_MEDAL_NAMES = {
    "Diamond Medal",
    "Galaxy Medal",
    "Quantum Medal",
}
BONUS_MEDAL_NAMES = {
    "Teto Medal",
    "Teto Medal Shiny",
    "ANROSOC Medal",
}

CHARACTER_ALIASES = {" ": "Space", ".": "Period"}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RIBBONS_DIR = os.path.join(BASE_DIR, "Ribbons")
COMMENDATIONS_DIR = os.path.join(BASE_DIR, "Commendations")
AWARDS_DIR = os.path.join(BASE_DIR, "Awards")
CHARACTERS_DIR = os.path.join(BASE_DIR, "Characters")


def listPngs(directory: str):
    if not os.path.isdir(directory):
        raise FileNotFoundError(f"Missing folder: {directory}")
    items = []
    for filename in sorted(os.listdir(directory), key=str.lower):
        if filename.lower().endswith(".png"):
            name = os.path.splitext(filename)[0]
            items.append({"name": name, "path": os.path.join(directory, filename)})
    return items


def loadRibbonGroups():
    groups = {
        "sacks": listPngs(AWARDS_DIR),
        "gorget": [],
        "spbadge": [],
        "commendations": [],
        "corpus": [],
        "ribbons": listPngs(RIBBONS_DIR),
    }
    for item in listPngs(COMMENDATIONS_DIR):
        lowerName = item["name"].lower()
        if "gorget" in lowerName:
            groups["gorget"].append(item)
        elif lowerName.startswith(("mr ", "hr ", "anrocom ")):
            groups["corpus"].append(item)
        elif "badge" in lowerName:
            groups["spbadge"].append(item)
        else:
            groups["commendations"].append(item)
    return groups


def loadRibbonImage(item):
    path = item.get("path")
    if not path:
        raise FileNotFoundError("Missing ribbon image path.")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing ribbon image: {path}")
    with Image.open(path) as img:
        return img.convert("RGBA")


def loadCharacterImage(ch):
    token = CHARACTER_ALIASES.get(ch, ch)
    path = os.path.join(CHARACTERS_DIR, f"{token}.png")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing character image: {path}")
    with Image.open(path) as img:
        return img.convert("RGBA")


def _defaultOutputName(nameplate: str):
    rawName = nameplate.strip()
    safeName = "".join(ch for ch in rawName if ch.isalnum() or ch in (" ", "_", "-")).strip()
    safeName = safeName.replace(" ", "_")
    datePrefix = datetime.date.today().strftime("%Y-%m-%d")
    if safeName:
        return f"{datePrefix}_{safeName}.png"
    return f"{datePrefix}.png"


def _safeLoad(item, missingAssets: set[str]):
    try:
        return loadRibbonImage(item)
    except Exception:
        missingAssets.add(item.get("name", "Unknown"))
        return None


def _selectedItems(items, selectedNames: set[str]):
    return [item for item in items if item["name"] in selectedNames]


def _buildPocketCenters(baseX: int, selectedCount: int):
    left = baseX
    middle = baseX + POCKET_COL_SPACING
    right = baseX + (POCKET_COL_SPACING * 2)
    # One medal: prioritize the center pocket first.
    if selectedCount == 1:
        return [middle, left, right]
    # Two or more medals: fill left-to-right.
    return [left, middle, right]


def renderRibbonImage(
    selectedNames: Iterable[str],
    nameplate: str = "",
    baseImagePath: Optional[str] = None,
    requireNameForNew: bool = True,
):
    selectedNames = [name.strip() for name in selectedNames if isinstance(name, str) and name.strip()]
    selectedSet = set(selectedNames)
    ribbonGroups = loadRibbonGroups()

    if baseImagePath:
        if not os.path.exists(baseImagePath):
            raise FileNotFoundError(f"Missing base image: {baseImagePath}")
        with Image.open(baseImagePath) as image:
            baseImg = image.convert("RGBA")
    else:
        if requireNameForNew and not nameplate.strip():
            raise ValueError("Nametape cannot be blank for a new image.")
        baseImg = Image.new("RGBA", (128, 128), (255, 255, 255, 0))

    usedSlots = {
        "sacks": set(),
        "corpus": set(),
        "gorget": set(),
        "spbadge": set(),
        "commendations": set(),
        "ribbons": set(),
    }

    nameplateImg = None
    nameplateWidth = 31
    nameplatePath = os.path.join(CHARACTERS_DIR, "Nameplate.png")
    if os.path.exists(nameplatePath):
        with Image.open(nameplatePath) as img:
            nameplateImg = img.convert("RGBA")
            nameplateWidth = nameplateImg.size[0]

    missingAssets: set[str] = set()

    # Awards / Bonus medals (pocket layout)
    selectedMedals = _selectedItems(ribbonGroups["sacks"], selectedSet)
    awardMedals = [item for item in selectedMedals if item["name"] in AWARD_MEDAL_NAMES]
    bonusMedals = [item for item in selectedMedals if item["name"] in BONUS_MEDAL_NAMES]
    awardMedals = awardMedals[:3]
    bonusMedals = bonusMedals[:3]

    if awardMedals or bonusMedals:
        nametapeCenterX = PART_COORDS["nametape"][0] + (nameplateWidth // 2)
        leftCenterX = nametapeCenterX
        rightCenterX = nametapeCenterX + POCKET_RIGHT_OFFSET
        yTop = PART_COORDS["sacks"][1]

        leftSlotX = leftCenterX + POCKET_X_OFFSET
        rightSlotX = rightCenterX + POCKET_X_OFFSET

        pocketCentersLeft = _buildPocketCenters(leftSlotX, len(awardMedals))
        pocketCentersRight = _buildPocketCenters(rightSlotX, len(bonusMedals))

        for item, cx in zip(awardMedals, pocketCentersLeft):
            piece = _safeLoad(item, missingAssets)
            if piece is None:
                continue
            w, _ = piece.size
            name = item["name"]
            if name not in usedSlots["sacks"]:
                baseImg.paste(piece, (int(cx - w / 2), yTop), piece)
                usedSlots["sacks"].add(name)

        for item, cx in zip(bonusMedals, pocketCentersRight):
            piece = _safeLoad(item, missingAssets)
            if piece is None:
                continue
            w, _ = piece.size
            name = item["name"]
            if name not in usedSlots["sacks"]:
                baseImg.paste(piece, (int(cx - w / 2), yTop), piece)
                usedSlots["sacks"].add(name)

    # Gorgets
    for item in ribbonGroups["gorget"]:
        name = item["name"]
        if name in selectedSet and name not in usedSlots["gorget"]:
            piece = _safeLoad(item, missingAssets)
            if piece is not None:
                baseImg.paste(piece, PART_COORDS["gorget"], piece)
                usedSlots["gorget"].add(name)

    # Special badges
    for item in ribbonGroups["spbadge"]:
        name = item["name"]
        if name in selectedSet and name not in usedSlots["spbadge"]:
            piece = _safeLoad(item, missingAssets)
            if piece is not None:
                baseImg.paste(piece, PART_COORDS["spbadge"], piece)
                usedSlots["spbadge"].add(name)

    # Commendations
    selectedComm = _selectedItems(ribbonGroups["commendations"], selectedSet)
    maxPerRow = 4
    yStart = PART_COORDS["commendations"][1]
    secondRow = False
    rowCount = 0

    while selectedComm:
        rowCount += 1
        if rowCount >= 2:
            secondRow = True
        row = selectedComm[:maxPerRow]
        selectedComm = selectedComm[maxPerRow:]

        rowImages = []
        totalWidth = 0
        rowHeight = 0
        for item in row:
            piece = _safeLoad(item, missingAssets)
            if piece is None:
                continue
            w, h = piece.size
            rowImages.append((item, piece, w, h))
            totalWidth += w
            rowHeight = max(rowHeight, h)

        if not rowImages:
            continue

        ribbonAreaWidth = 43
        ribbonAreaX = PART_COORDS["commendations"][0]
        rowCenter = ribbonAreaX + ribbonAreaWidth // 2
        if len(row) == 1:
            xCursor = rowCenter - totalWidth // 2 - 1
        elif len(row) == 4:
            xCursor = rowCenter - totalWidth // 2 + 1
        else:
            xCursor = rowCenter - totalWidth // 2

        for item, piece, w, _ in rowImages:
            name = item["name"]
            if name not in usedSlots["commendations"]:
                baseImg.paste(piece, (xCursor, yStart), piece)
                xCursor += w - 1
                usedSlots["commendations"].add(name)
        yStart -= rowHeight - 1

    # Corpus commendations
    selectedCorpus = _selectedItems(ribbonGroups["corpus"], selectedSet)
    if selectedCorpus:
        maxPerRow = 4
        yStart = PART_COORDS["corpus"][1]
        if not secondRow:
            yStart += 3

        while selectedCorpus:
            row = selectedCorpus[:maxPerRow]
            selectedCorpus = selectedCorpus[maxPerRow:]

            rowImages = []
            totalWidth = 0
            rowHeight = 0
            for item in row:
                piece = _safeLoad(item, missingAssets)
                if piece is None:
                    continue
                w, h = piece.size
                rowImages.append((item, piece, w, h))
                totalWidth += w
                rowHeight = max(rowHeight, h)

            if not rowImages:
                continue

            ribbonAreaX = PART_COORDS["corpus"][0]
            ribbonAreaWidth = 43
            rowCenter = ribbonAreaX + ribbonAreaWidth // 2
            if len(row) == 1:
                xCursor = rowCenter - totalWidth // 2 - 1 + CORPUS_X_OFFSET
            elif len(row) == 4:
                xCursor = rowCenter - totalWidth // 2 + 1 + CORPUS_X_OFFSET
            else:
                xCursor = rowCenter - totalWidth // 2 + CORPUS_X_OFFSET

            for item, piece, w, _ in rowImages:
                name = item["name"]
                if name not in usedSlots["corpus"]:
                    baseImg.paste(piece, (xCursor, yStart), piece)
                    xCursor += w - 1
                    usedSlots["corpus"].add(name)
            yStart -= rowHeight

    # Ribbons layout
    selectedRibbons = _selectedItems(ribbonGroups["ribbons"], selectedSet)
    yStart = PART_COORDS["ribbons"][1]
    rowNumber = 1
    while selectedRibbons:
        if rowNumber <= 4:
            maxInRow = 4
            alignRight = False
        elif rowNumber == 5:
            maxInRow = 3
            alignRight = True
        else:
            maxInRow = 2
            alignRight = True

        row = selectedRibbons[:maxInRow]
        selectedRibbons = selectedRibbons[maxInRow:]

        rowImages = []
        totalWidth = 0
        rowHeight = 0
        for item in row:
            piece = _safeLoad(item, missingAssets)
            if piece is None:
                continue
            w, h = piece.size
            rowImages.append((item, piece, w, h))
            totalWidth += w
            rowHeight = max(rowHeight, h)

        if not rowImages:
            rowNumber += 1
            continue

        ribbonAreaWidth = 43
        ribbonAreaX = PART_COORDS["ribbons"][0]
        widthWithSpacing = totalWidth - max(len(rowImages) - 1, 0)
        if alignRight:
            rightEdge = ribbonAreaX + ribbonAreaWidth - 1
            xCursor = rightEdge - widthWithSpacing + RIBBONS_RIGHT_ALIGN_OFFSET
        else:
            xCursor = ribbonAreaX + (ribbonAreaWidth - widthWithSpacing) // 2 + RIBBONS_RIGHT_ALIGN_OFFSET

        for item, piece, w, _ in rowImages:
            name = item["name"]
            if name not in usedSlots["ribbons"]:
                baseImg.paste(piece, (xCursor, yStart), piece)
                xCursor += w - 1
                usedSlots["ribbons"].add(name)

        yStart -= rowHeight - 1
        rowNumber += 1

    # Nametape
    if nameplate.strip():
        npX, npY = PART_COORDS["nametape"]
        if nameplateImg is None:
            if not os.path.exists(nameplatePath):
                raise FileNotFoundError(f"Missing nameplate image: {nameplatePath}")
            with Image.open(nameplatePath) as img:
                nameplateImg = img.convert("RGBA")
        baseImg.paste(nameplateImg, (npX, npY), nameplateImg)

        name = nameplate.upper()
        letters = []
        totalWidth = 0
        letterSpacing = 1
        for ch in name:
            try:
                letterImg = loadCharacterImage(ch)
            except FileNotFoundError:
                if ch == " ":
                    letters.append((None, 2))
                    totalWidth += 2
                continue
            w, _ = letterImg.size
            letters.append((letterImg, w))
            totalWidth += w
        if letters:
            totalWidth += letterSpacing * (len(letters) - 1)
            startX = npX + (nameplateImg.size[0] - totalWidth) // 2
            for index, (letterImg, w) in enumerate(letters):
                if letterImg is not None:
                    baseImg.paste(letterImg, (startX, npY + 1), letterImg)
                startX += w
                if index < len(letters) - 1:
                    startX += letterSpacing

    return baseImg, {
        "missingAssets": sorted(missingAssets),
        "selectedCount": len(selectedSet),
        "usedSlots": {k: sorted(v) for k, v in usedSlots.items()},
    }


def saveRenderedImage(image, outputPath: str, selectedNames: Iterable[str], nameplate: str, embedMetadata: bool = True):
    os.makedirs(os.path.dirname(outputPath) or ".", exist_ok=True)
    if embedMetadata and outputPath.lower().endswith(".png"):
        metadata = {
            "ribbons": sorted(set(selectedNames)),
            "nameplate": nameplate.strip(),
        }
        pnginfo = PngImagePlugin.PngInfo()
        pnginfo.add_text("ribbonengine", json.dumps(metadata, separators=(",", ":")))
        image.save(outputPath, pnginfo=pnginfo)
    else:
        image.save(outputPath)


def _loadSelectionJson(path: str):
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("Selection JSON must be an object.")
    ribbons = data.get("ribbons", [])
    nameplate = data.get("nameplate", "")
    if not isinstance(ribbons, list):
        raise ValueError("Selection JSON key 'ribbons' must be a list.")
    if not isinstance(nameplate, str):
        raise ValueError("Selection JSON key 'nameplate' must be a string.")
    clean = [name.strip() for name in ribbons if isinstance(name, str) and name.strip()]
    return clean, nameplate


def _allAssetNames():
    groups = loadRibbonGroups()
    names = {}
    for category, items in groups.items():
        names[category] = [item["name"] for item in items]
    return names


def _validateSelectionNames(selectedNames: Iterable[str], strict: bool):
    allNames = set()
    for _, categoryNames in _allAssetNames().items():
        allNames.update(categoryNames)
    unknown = sorted({name for name in selectedNames if name not in allNames})
    if unknown and strict:
        raise ValueError("Unknown ribbon names: " + ", ".join(unknown))
    return unknown


def main():
    parser = argparse.ArgumentParser(description="Headless ANRO Ribbon Engine renderer.")
    parser.add_argument("--output", help="Output PNG path. Defaults to date+name format in project folder.")
    parser.add_argument("--nameplate", default="", help="Nameplate text.")
    parser.add_argument("--select", action="append", default=[], help="Ribbon/award/commendation name. Repeat flag for multiple.")
    parser.add_argument("--selection-json", dest="selectionJson", help="Path to JSON file with {'ribbons': [...], 'nameplate': '...'}")
    parser.add_argument("--base-image", dest="baseImage", help="Optional base image path to render onto.")
    parser.add_argument("--allow-blank-name", dest="allowBlankName", action="store_true", help="Allow blank nameplate when no base image is provided.")
    parser.add_argument("--strict", action="store_true", help="Fail when unknown selection names are provided.")
    parser.add_argument("--list", action="store_true", help="List available asset names grouped by category and exit.")
    parser.add_argument("--no-metadata", dest="noMetadata", action="store_true", help="Do not embed ribbon metadata into PNG text metadata.")
    args = parser.parse_args()

    if args.list:
        names = _allAssetNames()
        for category in ("sacks", "gorget", "spbadge", "commendations", "corpus", "ribbons"):
            print(f"[{category}]")
            for name in names.get(category, []):
                print(name)
            print("")
        return 0

    selectedNames = list(args.select)
    jsonNameplate = ""
    if args.selectionJson:
        jsonSelected, jsonNameplate = _loadSelectionJson(args.selectionJson)
        selectedNames.extend(jsonSelected)
    selectedNames = [name.strip() for name in selectedNames if name.strip()]
    selectedNames = list(dict.fromkeys(selectedNames))

    nameplate = args.nameplate if args.nameplate else jsonNameplate
    outputPath = args.output or os.path.join(BASE_DIR, _defaultOutputName(nameplate))
    requireNameForNew = (not args.allowBlankName) and (not args.baseImage)

    try:
        unknown = _validateSelectionNames(selectedNames, strict=args.strict)
        if unknown:
            print("Warning: unknown selection names ignored: " + ", ".join(unknown), file=sys.stderr)
            unknownSet = set(unknown)
            selectedNames = [name for name in selectedNames if name not in unknownSet]

        image, info = renderRibbonImage(
            selectedNames=selectedNames,
            nameplate=nameplate,
            baseImagePath=args.baseImage,
            requireNameForNew=requireNameForNew,
        )
        saveRenderedImage(
            image=image,
            outputPath=outputPath,
            selectedNames=selectedNames,
            nameplate=nameplate,
            embedMetadata=not args.noMetadata,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Saved: {outputPath}")
    if info["missingAssets"]:
        print("Missing assets: " + ", ".join(info["missingAssets"]), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


