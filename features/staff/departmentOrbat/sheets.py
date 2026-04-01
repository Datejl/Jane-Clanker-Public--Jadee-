from __future__ import annotations

import calendar
import logging
import time
from datetime import date, datetime, timezone
from typing import Any, Optional

import config
from features.staff.anrdPayments import sheets as anrdPaymentSheets
from features.staff.departmentOrbat.layouts import loadDepartmentLayouts
from features.staff.departmentOrbat.sectionHeaders import (
    isManagedSection as _isManagedSection,
    isSectionAwareLayout as _isSectionAwareLayout,
    isSectionHeaderTextMatch as _isSectionHeaderTextMatch,
    normalizeKey as _normalizeKey,
    sectionHeaderAliasMap as _sectionHeaderAliasMap,
    sectionHeaderNames as _sectionHeaderNames,
)
from features.staff.recruitment import sheets as recruitmentSheets
from features.staff.orbat.a1 import columnIndex
from features.staff.orbat.multiEngine import getMultiOrbatEngine


_engine = getMultiOrbatEngine()
log = logging.getLogger(__name__)


def _isGoogleRateLimitError(exc: Exception) -> bool:
    current: Exception | None = exc
    while current is not None:
        resp = getattr(current, "resp", None)
        status = getattr(resp, "status", None)
        if status == 429 or getattr(current, "status_code", None) == 429:
            return True
        current = current.__cause__ if isinstance(current.__cause__, Exception) else None
    errorText = str(exc or "").upper()
    return "RATE_LIMIT_EXCEEDED" in errorText or "QUOTA EXCEEDED" in errorText


def _runWithRateLimitRetry(label: str, fn) -> tuple[Any, Optional[Exception]]:
    maxAttempts = max(1, int(getattr(config, "orbatMaintenanceMaxAttempts", 3) or 3))
    retryBaseDelaySec = float(getattr(config, "orbatMaintenanceRetryBaseDelaySec", 20) or 20)
    for attempt in range(1, maxAttempts + 1):
        try:
            return fn(), None
        except Exception as exc:
            if _isGoogleRateLimitError(exc) and attempt < maxAttempts:
                delaySec = retryBaseDelaySec * attempt
                log.warning(
                    "%s rate-limited on attempt %d/%d; retrying in %.1fs.",
                    label,
                    attempt,
                    maxAttempts,
                    delaySec,
                )
                time.sleep(delaySec)
                continue
            return None, exc
    return None, RuntimeError(f"{label} failed with unknown error.")


def _usernameSortTuple(value: Any) -> tuple[str, str]:
    raw = str(value or "").strip()
    return (_normalizeKey(raw), raw.casefold())


def _allDepartmentLayouts() -> list[dict[str, Any]]:
    return loadDepartmentLayouts()


def hasConfiguredLayouts() -> bool:
    return bool(_allDepartmentLayouts())


def _findLayoutByDivisionKey(divisionKey: str) -> Optional[dict[str, Any]]:
    target = _normalizeKey(divisionKey)
    if not target:
        return None
    for layout in _allDepartmentLayouts():
        if _normalizeKey(layout.get("divisionKey")) == target:
            return layout
    return None


def _parseDefaultColumnValues(rawDefaults: Any) -> dict[str, str]:
    if not isinstance(rawDefaults, dict):
        return {}
    out: dict[str, str] = {}
    for col, value in rawDefaults.items():
        letter = str(col or "").strip().upper()
        if not letter:
            continue
        out[letter] = str(value or "")
    return out


def _sheetName(sheetKey: str) -> str:
    return _engine.getSheetName(sheetKey)


def _readUsernameRows(sheetKey: str, usernameCol: str, startRow: int, endRow: int) -> list[str]:
    if endRow < startRow:
        return []
    rows = _engine.getValues(
        sheetKey,
        f"{_sheetName(sheetKey)}!{usernameCol}{startRow}:{usernameCol}{endRow}",
    )
    out: list[str] = []
    total = endRow - startRow + 1
    for idx in range(total):
        row = rows[idx] if idx < len(rows) else []
        out.append(str(row[0]).strip() if row else "")
    return out


def _resolveSectionBlocks(
    layout: dict[str, Any],
    sheetKey: str,
    startRow: int,
    usernameCol: str,
) -> list[dict[str, Any]]:
    sectionStart, sectionEnd = _memberRowsRange(layout, sheetKey, startRow, usernameCol)
    if sectionEnd < sectionStart:
        return []

    headerNames = _sectionHeaderNames(layout)
    if not headerNames:
        return []
    aliasMap = _sectionHeaderAliasMap(layout, headerNames)

    forcedHeaderRowsRaw = layout.get("sectionHeaderRows") or {}
    forcedHeaderRows: list[tuple[int, str]] = []
    if isinstance(forcedHeaderRowsRaw, dict):
        for rawName, rawRow in forcedHeaderRowsRaw.items():
            headerName = str(rawName or "").strip()
            if not headerName:
                continue
            try:
                headerRow = int(rawRow or 0)
            except (TypeError, ValueError):
                headerRow = 0
            if headerRow <= 0:
                continue
            if sectionStart <= headerRow <= sectionEnd:
                forcedHeaderRows.append((headerRow, headerName))
        if forcedHeaderRows:
            forcedHeaderRows.sort(key=lambda item: item[0])
            headerRows = forcedHeaderRows
        else:
            headerRows = []
    else:
        headerRows = []

    usernames = _readUsernameRows(sheetKey, usernameCol, sectionStart, sectionEnd)
    if not headerRows:
        seenCounts: dict[str, int] = {name: 0 for name in headerNames}
        for offset, username in enumerate(usernames):
            matchedHeaders: list[str] = []
            for headerName in headerNames:
                aliases = aliasMap.get(headerName, [headerName])
                if any(_isSectionHeaderTextMatch(alias, username) for alias in aliases):
                    matchedHeaders.append(headerName)
            if not matchedHeaders:
                continue
            matchedHeaders.sort(
                key=lambda item: (
                    seenCounts.get(item, 0),
                    headerNames.index(item),
                )
            )
            selectedHeader = matchedHeaders[0]
            seenCounts[selectedHeader] = seenCounts.get(selectedHeader, 0) + 1
            headerRows.append((sectionStart + offset, selectedHeader))

    if not headerRows:
        return []

    blocks: list[dict[str, Any]] = []
    for idx, (headerRow, headerName) in enumerate(headerRows):
        nextHeaderRow = headerRows[idx + 1][0] if idx + 1 < len(headerRows) else None

        dataStartRow = headerRow + 1
        if dataStartRow <= sectionEnd:
            firstCell = usernames[dataStartRow - sectionStart] if dataStartRow - sectionStart < len(usernames) else ""
            if not firstCell:
                dataStartRow += 1

        dataEndRow = (nextHeaderRow - 1) if nextHeaderRow else sectionEnd
        if dataEndRow >= dataStartRow and dataEndRow <= sectionEnd:
            lastCell = usernames[dataEndRow - sectionStart] if dataEndRow - sectionStart < len(usernames) else ""
            if not lastCell:
                dataEndRow -= 1

        blocks.append(
            {
                "headerName": headerName,
                "headerRow": headerRow,
                "nextHeaderRow": nextHeaderRow,
                "dataStartRow": dataStartRow,
                "dataEndRow": dataEndRow,
                "sectionStartRow": sectionStart,
                "sectionEndRow": sectionEnd,
            }
        )
    return blocks


def _ensureSectionSpacerRows(layout: dict[str, Any], sheetKey: str, startRow: int, usernameCol: str) -> int:
    if not _isSectionAwareLayout(layout):
        return 0
    if layout.get("enforceSectionSpacers", True) is False:
        return 0

    headerNames = _sectionHeaderNames(layout)
    if not headerNames:
        return 0
    aliasMap = _sectionHeaderAliasMap(layout, headerNames)

    sectionStart, sectionEnd = _memberRowsRange(layout, sheetKey, startRow, usernameCol)
    if sectionEnd < sectionStart:
        return 0

    sheetId = _engine.getSheetTabId(sheetKey)
    inserted = 0
    maxPasses = 8
    for _ in range(maxPasses):
        changed = False
        usernames = _readUsernameRows(sheetKey, usernameCol, sectionStart, sectionEnd)
        rowIndex = sectionStart
        while rowIndex <= sectionEnd:
            username = usernames[rowIndex - sectionStart] if rowIndex - sectionStart < len(usernames) else ""
            isHeader = False
            for headerName in headerNames:
                aliases = aliasMap.get(headerName, [headerName])
                if any(_isSectionHeaderTextMatch(alias, username) for alias in aliases):
                    isHeader = True
                    break
            if not isHeader:
                rowIndex += 1
                continue

            # Blank row before header.
            if rowIndex > sectionStart:
                prevValue = usernames[rowIndex - 1 - sectionStart] if rowIndex - 1 - sectionStart < len(usernames) else ""
                if prevValue:
                    _engine.batchUpdateRequests(
                        sheetKey,
                        [
                            {
                                "insertDimension": {
                                    "range": {
                                        "sheetId": sheetId,
                                        "dimension": "ROWS",
                                        "startIndex": rowIndex - 1,
                                        "endIndex": rowIndex,
                                    },
                                    "inheritFromBefore": True,
                                }
                            }
                        ],
                    )
                    inserted += 1
                    sectionEnd += 1
                    changed = True
                    break

            # Blank row after header.
            if rowIndex < sectionEnd:
                nextValue = usernames[rowIndex + 1 - sectionStart] if rowIndex + 1 - sectionStart < len(usernames) else ""
                if nextValue:
                    _engine.batchUpdateRequests(
                        sheetKey,
                        [
                            {
                                "insertDimension": {
                                    "range": {
                                        "sheetId": sheetId,
                                        "dimension": "ROWS",
                                        "startIndex": rowIndex,
                                        "endIndex": rowIndex + 1,
                                    },
                                    "inheritFromBefore": True,
                                }
                            }
                        ],
                    )
                    inserted += 1
                    sectionEnd += 1
                    changed = True
                    break
            else:
                _engine.batchUpdateRequests(
                    sheetKey,
                    [
                        {
                            "insertDimension": {
                                "range": {
                                    "sheetId": sheetId,
                                    "dimension": "ROWS",
                                    "startIndex": rowIndex,
                                    "endIndex": rowIndex + 1,
                                },
                                "inheritFromBefore": True,
                            }
                        }
                    ],
                )
                inserted += 1
                sectionEnd += 1
                changed = True
                break
            rowIndex += 1
        if not changed:
            break
    return inserted


def _memberRowsRange(layout: dict[str, Any], sheetKey: str, startRow: int, usernameCol: str) -> tuple[int, int]:
    preserveTrailingVisualRow = bool(layout.get("preserveTrailingVisualRow"))
    explicitEndRow = int(layout.get("membersEndRow") or 0)
    if explicitEndRow >= startRow:
        adjustedEndRow = explicitEndRow - 1 if preserveTrailingVisualRow else explicitEndRow
        if adjustedEndRow < startRow:
            return startRow, startRow - 1
        return startRow, adjustedEndRow

    scanEndRow = max(startRow, int(layout.get("membersScanEndRow") or (startRow + 500)))
    rows = _engine.getValues(
        sheetKey,
        f"{_sheetName(sheetKey)}!{usernameCol}{startRow}:{usernameCol}{scanEndRow}",
    )
    if not rows:
        return startRow, startRow - 1

    # Section-aware layouts should not stop on short blank runs because
    # headers are intentionally separated by spacer rows.
    if _isSectionAwareLayout(layout):
        lastNonEmptyRow = startRow - 1
        for offset, row in enumerate(rows):
            value = str(row[0]).strip() if row else ""
            if value:
                lastNonEmptyRow = startRow + offset
        if preserveTrailingVisualRow and lastNonEmptyRow >= startRow:
            lastNonEmptyRow -= 1
        if lastNonEmptyRow < startRow:
            return startRow, startRow - 1
        return startRow, lastNonEmptyRow

    stopEmptyRun = max(1, int(layout.get("membersStopEmptyRun") or 3))

    lastMemberRow = startRow - 1
    emptyRun = 0
    for offset, row in enumerate(rows):
        rowIndex = startRow + offset
        value = str(row[0]).strip() if row else ""
        if value:
            lastMemberRow = rowIndex
            emptyRun = 0
            continue

        if lastMemberRow >= startRow:
            emptyRun += 1
            if emptyRun >= stopEmptyRun:
                break

    if lastMemberRow < startRow:
        return startRow, startRow - 1
    if preserveTrailingVisualRow:
        lastMemberRow -= 1
        if lastMemberRow < startRow:
            return startRow, startRow - 1
    return startRow, lastMemberRow


def _findOrAppendMemberRow(layout: dict[str, Any], sheetKey: str, startRow: int, usernameCol: str, robloxUsername: str) -> int:
    if _isSectionAwareLayout(layout):
        return _findOrAppendMemberRowInSection(layout, sheetKey, startRow, usernameCol, robloxUsername)

    sectionStart, sectionEnd = _memberRowsRange(layout, sheetKey, startRow, usernameCol)
    if sectionEnd < sectionStart:
        return sectionStart

    rows = _engine.getValues(
        sheetKey,
        f"{_sheetName(sheetKey)}!{usernameCol}{sectionStart}:{usernameCol}{sectionEnd}",
    )
    target = robloxUsername.strip().lower()

    firstBlankRow: Optional[int] = None
    for offset, row in enumerate(rows):
        rowIndex = sectionStart + offset
        value = str(row[0]).strip() if row else ""
        if value.lower() == target:
            return rowIndex
        if firstBlankRow is None and not value:
            firstBlankRow = rowIndex

    if firstBlankRow is not None:
        return firstBlankRow
    return sectionEnd + 1


def _findMemberRowByUsername(
    layout: dict[str, Any],
    sheetKey: str,
    startRow: int,
    usernameCol: str,
    robloxUsername: str,
) -> Optional[int]:
    sectionStart, sectionEnd = _memberRowsRange(layout, sheetKey, startRow, usernameCol)
    if sectionEnd < sectionStart:
        return None

    rows = _engine.getValues(
        sheetKey,
        f"{_sheetName(sheetKey)}!{usernameCol}{sectionStart}:{usernameCol}{sectionEnd}",
    )
    target = str(robloxUsername or "").strip().lower()
    if not target:
        return None
    for offset, row in enumerate(rows):
        value = str(row[0]).strip() if row else ""
        if value.lower() == target:
            return sectionStart + offset
    return None


def _findOrAppendMemberRowInSection(
    layout: dict[str, Any],
    sheetKey: str,
    startRow: int,
    usernameCol: str,
    robloxUsername: str,
) -> int:
    blocks = _resolveSectionBlocks(layout, sheetKey, startRow, usernameCol)
    if not blocks:
        sectionStart, sectionEnd = _memberRowsRange(layout, sheetKey, startRow, usernameCol)
        if sectionEnd < sectionStart:
            return sectionStart
        return sectionEnd + 1

    targetSection = str(layout.get("insertSectionHeader") or "").strip()
    targetNorm = _normalizeKey(targetSection) if targetSection else ""
    targetBlock = None
    for block in blocks:
        if targetNorm and _normalizeKey(block["headerName"]) == targetNorm:
            targetBlock = block
            break
    if targetBlock is None:
        for block in blocks:
            if "employee" in _normalizeKey(block["headerName"]):
                targetBlock = block
                break
    if targetBlock is None:
        targetBlock = blocks[0]

    dataStartRow = int(targetBlock["dataStartRow"])
    dataEndRow = int(targetBlock["dataEndRow"])
    nextHeaderRow = targetBlock.get("nextHeaderRow")

    if dataEndRow >= dataStartRow:
        usernames = _readUsernameRows(sheetKey, usernameCol, dataStartRow, dataEndRow)
        target = robloxUsername.strip().lower()
        firstBlankRow: Optional[int] = None
        for offset, username in enumerate(usernames):
            rowIndex = dataStartRow + offset
            value = str(username).strip()
            if value.lower() == target:
                return rowIndex
            if firstBlankRow is None and not value:
                firstBlankRow = rowIndex
        if firstBlankRow is not None:
            return firstBlankRow

    insertRow = (int(nextHeaderRow) - 1) if nextHeaderRow else max(dataStartRow, dataEndRow + 1)
    sheetId = _engine.getSheetTabId(sheetKey)
    _engine.batchUpdateRequests(
        sheetKey,
        [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheetId,
                        "dimension": "ROWS",
                        "startIndex": insertRow - 1,
                        "endIndex": insertRow,
                    },
                    "inheritFromBefore": True,
                }
            }
        ],
    )
    return insertRow


def _formatRows(
    sheetKey: str,
    *,
    startRow: int,
    endRow: int,
    startCol: str,
    endCol: str,
    boldCols: list[str],
    fontFamily: str,
    fontSize: int,
    columnBorderOverrides: Optional[list[dict[str, Any]]] = None,
) -> int:
    if endRow < startRow:
        return 0
    sheetId = _engine.getSheetTabId(sheetKey)
    startColIndex = columnIndex(startCol) - 1
    endColIndexExclusive = columnIndex(endCol)
    if startColIndex < 0 or endColIndexExclusive <= startColIndex:
        return 0

    baseRange = {
        "sheetId": sheetId,
        "startRowIndex": startRow - 1,
        "endRowIndex": endRow,
        "startColumnIndex": startColIndex,
        "endColumnIndex": endColIndexExclusive,
    }
    black = {"red": 0.0, "green": 0.0, "blue": 0.0}

    requests: list[dict[str, Any]] = [
        {
            "repeatCell": {
                "range": baseRange,
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "fontFamily": str(fontFamily or "Abel"),
                            "fontSize": int(fontSize or 10),
                            "bold": False,
                        }
                    }
                },
                "fields": "userEnteredFormat.textFormat.fontFamily,userEnteredFormat.textFormat.fontSize,userEnteredFormat.textFormat.bold",
            }
        },
        {
            "updateBorders": {
                "range": baseRange,
                "top": {"style": "SOLID", "color": black},
                "bottom": {"style": "SOLID", "color": black},
                "left": {"style": "SOLID", "color": black},
                "right": {"style": "SOLID", "color": black},
                "innerHorizontal": {"style": "SOLID", "color": black},
                "innerVertical": {"style": "SOLID", "color": black},
            }
        },
    ]

    def _normalizeBorderStyle(styleValue: Any, fallback: str) -> str:
        style = str(styleValue or "").strip().upper()
        allowed = {"DOTTED", "DASHED", "SOLID", "SOLID_MEDIUM", "SOLID_THICK", "DOUBLE"}
        return style if style in allowed else fallback

    for boldCol in boldCols:
        colLetter = str(boldCol or "").strip().upper()
        colIndex = columnIndex(colLetter) - 1
        if colIndex < 0:
            continue
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheetId,
                        "startRowIndex": startRow - 1,
                        "endRowIndex": endRow,
                        "startColumnIndex": colIndex,
                        "endColumnIndex": colIndex + 1,
                    },
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            }
        )

    # Optional per-column border overrides for special visual requirements.
    # This applies after the base border request above.
    # We intentionally keep this data-driven via config.
    if isinstance(columnBorderOverrides, list):
        for override in columnBorderOverrides:
            if not isinstance(override, dict):
                continue
            startColOverride = str(override.get("startColumn") or "").strip().upper()
            endColOverride = str(override.get("endColumn") or "").strip().upper()
            startIdxOverride = columnIndex(startColOverride) - 1
            endIdxOverrideExclusive = columnIndex(endColOverride)
            if startIdxOverride < 0 or endIdxOverrideExclusive <= startIdxOverride:
                continue
            overrideRange = {
                "sheetId": sheetId,
                "startRowIndex": startRow - 1,
                "endRowIndex": endRow,
                "startColumnIndex": startIdxOverride,
                "endColumnIndex": endIdxOverrideExclusive,
            }
            requests.append(
                {
                    "updateBorders": {
                        "range": overrideRange,
                        "top": {"style": _normalizeBorderStyle(override.get("topStyle"), "SOLID"), "color": black},
                        "bottom": {"style": _normalizeBorderStyle(override.get("bottomStyle"), "SOLID"), "color": black},
                        "left": {"style": _normalizeBorderStyle(override.get("leftStyle"), "SOLID"), "color": black},
                        "right": {"style": _normalizeBorderStyle(override.get("rightStyle"), "SOLID"), "color": black},
                        "innerHorizontal": {
                            "style": _normalizeBorderStyle(override.get("innerHorizontalStyle"), "SOLID"),
                            "color": black,
                        },
                        "innerVertical": {
                            "style": _normalizeBorderStyle(override.get("innerVerticalStyle"), "SOLID"),
                            "color": black,
                        },
                    }
                }
            )

    _engine.batchUpdateRequests(sheetKey, requests)
    return endRow - startRow + 1


def _formatSectionDataRows(layout: dict[str, Any], sheetKey: str, startRow: int) -> int:
    usernameCol = str(layout.get("usernameColumn") or "A").strip().upper()
    blocks = _resolveSectionBlocks(layout, sheetKey, startRow, usernameCol)
    if not blocks:
        return 0

    touched = 0
    columnBorderOverrides = layout.get("columnBorderOverrides")
    for block in blocks:
        if not _isManagedSection(layout, str(block.get("headerName") or "")):
            continue
        dataStartRow = int(block.get("dataStartRow") or 0)
        dataEndRow = int(block.get("dataEndRow") or 0)
        if dataStartRow <= 0 or dataEndRow < dataStartRow:
            continue
        touched += _formatRows(
            sheetKey,
            startRow=dataStartRow,
            endRow=dataEndRow,
            startCol=str(layout.get("formatStartColumn") or "A"),
            endCol=str(layout.get("formatEndColumn") or "K"),
            boldCols=[str(col) for col in (layout.get("boldColumns") or ["A"])],
            fontFamily=str(layout.get("fontFamily") or "Abel"),
            fontSize=int(layout.get("fontSize") or 10),
            columnBorderOverrides=columnBorderOverrides if isinstance(columnBorderOverrides, list) else None,
        )
    return touched


def _resolveSortColumns(
    layout: dict[str, Any],
    usernameCol: str,
    rankCol: str,
) -> Optional[dict[str, int | str]]:
    sortStartCol = str(layout.get("sortStartColumn") or layout.get("formatStartColumn") or "A").strip().upper()
    sortEndCol = str(layout.get("sortEndColumn") or layout.get("formatEndColumn") or "K").strip().upper()
    startColIndex = columnIndex(sortStartCol)
    endColIndex = columnIndex(sortEndCol)
    if startColIndex <= 0 or endColIndex <= 0 or endColIndex < startColIndex:
        return None

    usernameOffset = columnIndex(usernameCol) - startColIndex
    rankOffset = columnIndex(rankCol) - startColIndex
    width = endColIndex - startColIndex + 1
    if usernameOffset < 0 or usernameOffset >= width or rankOffset < 0 or rankOffset >= width:
        return None

    return {
        "sortStartCol": sortStartCol,
        "sortEndCol": sortEndCol,
        "startColIndex": startColIndex,
        "endColIndex": endColIndex,
        "usernameOffset": usernameOffset,
        "rankOffset": rankOffset,
        "width": width,
    }


def _readSortMatrix(
    sheetKey: str,
    sortStartCol: str,
    sortEndCol: str,
    startRow: int,
    endRow: int,
    width: int,
) -> list[list[str]]:
    if endRow < startRow or width <= 0:
        return []

    totalRows = endRow - startRow + 1
    matrixRaw = _engine.getValues(
        sheetKey,
        f"{_sheetName(sheetKey)}!{sortStartCol}{startRow}:{sortEndCol}{endRow}",
    )

    matrix: list[list[str]] = []
    for idx in range(totalRows):
        source = matrixRaw[idx] if idx < len(matrixRaw) else []
        padded = [str(value) for value in source[:width]]
        if len(padded) < width:
            padded.extend([""] * (width - len(padded)))
        matrix.append(padded)
    return matrix


def _splitRowsByUsername(matrix: list[list[str]], usernameOffset: int) -> tuple[list[list[str]], list[list[str]]]:
    populatedRows: list[list[str]] = []
    blankRows: list[list[str]] = []
    for row in matrix:
        username = str(row[usernameOffset] or "").strip()
        if username:
            populatedRows.append(row)
        else:
            blankRows.append(row)
    return populatedRows, blankRows


def _buildRankUsernameSortedMatrix(
    matrix: list[list[str]],
    *,
    usernameOffset: int,
    rankOffset: int,
    rankOrderMap: dict[str, int],
    rankDefault: int,
) -> tuple[list[list[str]], int, bool]:
    populatedRows, blankRows = _splitRowsByUsername(matrix, usernameOffset)
    if len(populatedRows) < 2:
        return matrix, len(populatedRows), False

    sortedPopulated = sorted(
        populatedRows,
        key=lambda row: (
            rankOrderMap.get(_normalizeKey(row[rankOffset]), rankDefault),
            _usernameSortTuple(row[usernameOffset]),
        ),
    )
    sortedMatrix = sortedPopulated + blankRows
    return sortedMatrix, len(sortedPopulated), sortedMatrix != matrix


def _resolveSectionSortLockedTailRule(layout: dict[str, Any], sectionNameNorm: str) -> Optional[dict[str, Any]]:
    rawRules = layout.get("sectionSortLockedTailRules") or {}
    if not isinstance(rawRules, dict):
        return None
    for rawSectionName, rawRule in rawRules.items():
        if _normalizeKey(rawSectionName) != sectionNameNorm:
            continue
        if isinstance(rawRule, dict):
            return rawRule
        if isinstance(rawRule, list):
            return {"anyNonEmptyColumns": rawRule}
    return None


def _rowHasAnyNonEmptyConfiguredColumn(
    row: list[str],
    configuredColumns: list[str],
    sortStartColIndex: int,
) -> bool:
    for rawCol in configuredColumns:
        colLetter = str(rawCol or "").strip().upper()
        colIndexAbs = columnIndex(colLetter)
        if colIndexAbs <= 0:
            continue
        offset = colIndexAbs - sortStartColIndex
        if offset < 0 or offset >= len(row):
            continue
        if str(row[offset] or "").strip():
            return True
    return False


def _rowMatchesRequiredColumnValues(
    row: list[str],
    requiredColumnValues: dict[str, Any],
    sortStartColIndex: int,
) -> bool:
    if not requiredColumnValues:
        return True
    for rawCol, rawExpected in requiredColumnValues.items():
        colLetter = str(rawCol or "").strip().upper()
        colIndexAbs = columnIndex(colLetter)
        if colIndexAbs <= 0:
            continue
        offset = colIndexAbs - sortStartColIndex
        cellValue = str(row[offset] or "").strip() if 0 <= offset < len(row) else ""

        expectedValues: list[str]
        if isinstance(rawExpected, list):
            expectedValues = [str(item).strip() for item in rawExpected if str(item).strip()]
        else:
            expectedText = str(rawExpected).strip()
            expectedValues = [expectedText] if expectedText else []

        if not expectedValues:
            continue
        expectedNormSet = {_normalizeKey(item) for item in expectedValues}
        if _normalizeKey(cellValue) not in expectedNormSet:
            return False
    return True


def _splitPinnedBottomRows(
    matrix: list[list[str]],
    *,
    usernameOffset: int,
    sortStartColIndex: int,
    lockRule: dict[str, Any],
) -> tuple[list[list[str]], list[list[str]]]:
    anyNonEmptyColumnsRaw = lockRule.get("anyNonEmptyColumns") if isinstance(lockRule, dict) else None
    if not isinstance(anyNonEmptyColumnsRaw, list) or not anyNonEmptyColumnsRaw:
        return matrix, []

    configuredColumns = [str(col).strip().upper() for col in anyNonEmptyColumnsRaw if str(col).strip()]
    if not configuredColumns:
        return matrix, []
    requiredColumnValuesRaw = lockRule.get("requiredColumnValues") if isinstance(lockRule, dict) else None
    requiredColumnValues = requiredColumnValuesRaw if isinstance(requiredColumnValuesRaw, dict) else {}

    pinnedRows: list[list[str]] = []
    normalRows: list[list[str]] = []
    for row in matrix:
        username = str(row[usernameOffset] or "").strip()
        if username and _rowHasAnyNonEmptyConfiguredColumn(row, configuredColumns, sortStartColIndex):
            if _rowMatchesRequiredColumnValues(row, requiredColumnValues, sortStartColIndex):
                pinnedRows.append(row)
                continue
        normalRows.append(row)

    if not pinnedRows:
        return matrix, []
    return normalRows, pinnedRows


def _writeSortMatrix(
    sheetKey: str,
    sortStartCol: str,
    sortEndCol: str,
    startRow: int,
    endRow: int,
    matrix: list[list[str]],
) -> None:
    _engine.batchUpdateValues(
        sheetKey,
        [
            {
                "range": f"{_sheetName(sheetKey)}!{sortStartCol}{startRow}:{sortEndCol}{endRow}",
                "values": matrix,
            }
        ],
    )


def _sortMemberRows(
    layout: dict[str, Any],
    sheetKey: str,
    *,
    startRow: int,
    endRow: int,
) -> int:
    if _isSectionAwareLayout(layout):
        return _sortSectionMemberRows(layout, sheetKey, startRow=startRow)

    if endRow < startRow:
        return 0

    rankOrder = layout.get("rankOrder") or []
    if not isinstance(rankOrder, list) or not rankOrder:
        return 0

    usernameCol = str(layout.get("usernameColumn") or "A").strip().upper()
    rankCol = str(layout.get("rankColumn") or "").strip().upper()
    if not rankCol:
        return 0

    sortContext = _resolveSortColumns(layout, usernameCol, rankCol)
    if not sortContext:
        return 0

    sortStartCol = str(sortContext["sortStartCol"])
    sortEndCol = str(sortContext["sortEndCol"])
    usernameOffset = int(sortContext["usernameOffset"])
    rankOffset = int(sortContext["rankOffset"])
    width = int(sortContext["width"])
    matrix = _readSortMatrix(
        sheetKey,
        sortStartCol,
        sortEndCol,
        startRow,
        endRow,
        width,
    )
    if not matrix:
        return 0

    rankOrderMap = {_normalizeKey(name): index for index, name in enumerate(rankOrder)}
    rankDefault = len(rankOrderMap) + 100

    sortedMatrix, sortedCount, changed = _buildRankUsernameSortedMatrix(
        matrix,
        usernameOffset=usernameOffset,
        rankOffset=rankOffset,
        rankOrderMap=rankOrderMap,
        rankDefault=rankDefault,
    )
    if not changed:
        return 0

    _writeSortMatrix(sheetKey, sortStartCol, sortEndCol, startRow, endRow, sortedMatrix)
    return sortedCount


def _sortSectionMemberRows(layout: dict[str, Any], sheetKey: str, *, startRow: int) -> int:
    usernameCol = str(layout.get("usernameColumn") or "A").strip().upper()
    rankCol = str(layout.get("rankColumn") or "").strip().upper()
    if not rankCol:
        return 0

    sortContext = _resolveSortColumns(layout, usernameCol, rankCol)
    if not sortContext:
        return 0

    sortStartCol = str(sortContext["sortStartCol"])
    sortEndCol = str(sortContext["sortEndCol"])
    startColIndex = int(sortContext["startColIndex"])
    usernameOffset = int(sortContext["usernameOffset"])
    rankOffset = int(sortContext["rankOffset"])
    width = int(sortContext["width"])

    blocks = _resolveSectionBlocks(layout, sheetKey, startRow, usernameCol)
    if not blocks:
        return 0

    globalRankOrder = layout.get("rankOrder") or []
    globalRankOrderMap = {_normalizeKey(name): index for index, name in enumerate(globalRankOrder)}
    sectionRankOrderRaw = layout.get("sectionRankOrder") or {}
    sectionRankOrder = {
        _normalizeKey(section): (ranks if isinstance(ranks, list) else [])
        for section, ranks in sectionRankOrderRaw.items()
    } if isinstance(sectionRankOrderRaw, dict) else {}

    totalSorted = 0
    for block in blocks:
        if not _isManagedSection(layout, str(block.get("headerName") or "")):
            continue
        dataStartRow = int(block.get("dataStartRow") or 0)
        dataEndRow = int(block.get("dataEndRow") or 0)
        if dataEndRow < dataStartRow or dataStartRow <= 0:
            continue

        matrix = _readSortMatrix(
            sheetKey,
            sortStartCol,
            sortEndCol,
            dataStartRow,
            dataEndRow,
            width,
        )
        if not matrix:
            continue

        sectionNameNorm = _normalizeKey(block.get("headerName"))
        localRankList = sectionRankOrder.get(sectionNameNorm) or []
        localRankOrderMap = {
            _normalizeKey(name): index for index, name in enumerate(localRankList)
        } if localRankList else globalRankOrderMap
        rankDefault = len(localRankOrderMap) + 100

        lockRule = _resolveSectionSortLockedTailRule(layout, sectionNameNorm)
        sortableMatrix, lockedTailRows = _splitPinnedBottomRows(
            matrix,
            usernameOffset=usernameOffset,
            sortStartColIndex=startColIndex,
            lockRule=lockRule or {},
        )
        sortedSortable, sortedCount, _ = _buildRankUsernameSortedMatrix(
            sortableMatrix,
            usernameOffset=usernameOffset,
            rankOffset=rankOffset,
            rankOrderMap=localRankOrderMap,
            rankDefault=rankDefault,
        )
        sortedMatrix = sortedSortable + lockedTailRows
        if sortedMatrix == matrix:
            continue

        _writeSortMatrix(sheetKey, sortStartCol, sortEndCol, dataStartRow, dataEndRow, sortedMatrix)
        totalSorted += sortedCount
    return totalSorted


def _writeSingleRowValues(sheetKey: str, row: int, columnValues: dict[str, str]) -> None:
    updates = [
        {
            "range": f"{_sheetName(sheetKey)}!{col}{row}:{col}{row}",
            "values": [[value]],
        }
        for col, value in columnValues.items()
    ]
    if updates:
        _engine.batchUpdateValues(sheetKey, updates)


def _addMonthsToDate(baseDate: date, monthsOffset: int) -> date:
    totalMonths = (baseDate.year * 12) + (baseDate.month - 1) + int(monthsOffset)
    targetYear = totalMonths // 12
    targetMonth = (totalMonths % 12) + 1
    maxDay = calendar.monthrange(targetYear, targetMonth)[1]
    targetDay = min(baseDate.day, maxDay)
    return date(targetYear, targetMonth, targetDay)


def _formatDateByRuleStyle(targetDate: date, style: str) -> str:
    styleNorm = _normalizeKey(style)
    if styleNorm in {"mondaydot", "monddot", "monthdaydot", "mond"}:
        return f"{targetDate.strftime('%b')} {targetDate.day}."
    return targetDate.isoformat()


def _applyUpsertDateColumnRules(
    layout: dict[str, Any],
    sheetKey: str,
    row: int,
    *,
    created: bool,
) -> None:
    rawRules = layout.get("upsertDateColumns") or []
    if not isinstance(rawRules, list) or not rawRules:
        return

    nowDate = datetime.now(timezone.utc).date()
    emptyCheckColumns: set[str] = set()
    for rawRule in rawRules:
        if not isinstance(rawRule, dict):
            continue
        col = str(rawRule.get("column") or "").strip().upper()
        if not col:
            continue
        if bool(rawRule.get("setWhenEmptyOnly", False)):
            emptyCheckColumns.add(col)
    existingRowValues = (
        _engine.readRowColumns(
            sheetKey,
            row=row,
            columnMap={col: col for col in sorted(emptyCheckColumns)},
        )
        if emptyCheckColumns
        else {}
    )
    valuesToWrite: dict[str, str] = {}
    for rawRule in rawRules:
        if not isinstance(rawRule, dict):
            continue
        col = str(rawRule.get("column") or "").strip().upper()
        if not col:
            continue

        onlyWhenCreated = bool(rawRule.get("onlyWhenCreated", False))
        if onlyWhenCreated and not created:
            continue

        setWhenEmptyOnly = bool(rawRule.get("setWhenEmptyOnly", False))
        if setWhenEmptyOnly and str(existingRowValues.get(col, "")).strip():
            continue

        try:
            monthsOffset = int(rawRule.get("monthsOffset", rawRule.get("months", 0)) or 0)
        except (TypeError, ValueError):
            monthsOffset = 0

        style = str(rawRule.get("formatStyle") or rawRule.get("format") or "iso")
        targetDate = _addMonthsToDate(nowDate, monthsOffset)
        valuesToWrite[col] = _formatDateByRuleStyle(targetDate, style)

    if valuesToWrite:
        _writeSingleRowValues(sheetKey, row, valuesToWrite)


def _resolveLayoutSheetContext(layout: dict[str, Any]) -> Optional[dict[str, Any]]:
    sheetKey = str(layout.get("sheetKey") or "").strip()
    if not sheetKey:
        return None
    usernameCol = str(layout.get("usernameColumn") or "A").strip().upper()
    startRow = max(1, int(layout.get("membersStartRow") or 5))
    return {
        "sheetKey": sheetKey,
        "usernameCol": usernameCol,
        "startRow": startRow,
    }


def _organizeLayoutRowsThroughMemberRow(
    layout: dict[str, Any],
    sheetKey: str,
    usernameCol: str,
    startRow: int,
    memberRow: int,
) -> tuple[int, int]:
    _, memberEndRow = _memberRowsRange(layout, sheetKey, startRow, usernameCol)
    if memberEndRow < memberRow:
        memberEndRow = memberRow
    return _organizeLayoutRows(
        layout,
        sheetKey,
        startRow=startRow,
        endRow=memberEndRow,
    )


def _formatLayoutRows(layout: dict[str, Any], sheetKey: str, startRow: int, endRow: int) -> int:
    if _isSectionAwareLayout(layout):
        return _formatSectionDataRows(layout, sheetKey, startRow)
    return _formatRows(
        sheetKey,
        startRow=startRow,
        endRow=endRow,
        startCol=str(layout.get("formatStartColumn") or "A"),
        endCol=str(layout.get("formatEndColumn") or "K"),
        boldCols=[str(col) for col in (layout.get("boldColumns") or ["A"])],
        fontFamily=str(layout.get("fontFamily") or "Abel"),
        fontSize=int(layout.get("fontSize") or 10),
        columnBorderOverrides=(
            layout.get("columnBorderOverrides")
            if isinstance(layout.get("columnBorderOverrides"), list)
            else None
        ),
    )


def _organizeLayoutRows(
    layout: dict[str, Any],
    sheetKey: str,
    *,
    startRow: int,
    endRow: int,
) -> tuple[int, int]:
    sortedRows = _sortMemberRows(
        layout,
        sheetKey,
        startRow=startRow,
        endRow=endRow,
    )
    touchedRows = _formatLayoutRows(layout, sheetKey, startRow, endRow)
    return sortedRows, touchedRows


def upsertDivisionMemberByRobloxUsername(
    divisionKey: str,
    robloxUsername: str,
    initialRank: Optional[str] = None,
) -> dict[str, Any]:
    layout = _findLayoutByDivisionKey(divisionKey)
    if not layout:
        return {"ok": False, "reason": "layout-not-configured"}

    username = str(robloxUsername or "").strip()
    if not username:
        return {"ok": False, "reason": "missing-username"}

    context = _resolveLayoutSheetContext(layout)
    if context is None:
        return {"ok": False, "reason": "missing-sheet-key"}

    sheetKey = str(context["sheetKey"])
    usernameCol = str(context["usernameCol"])
    startRow = int(context["startRow"])
    spacerRowsAdded = _ensureSectionSpacerRows(layout, sheetKey, startRow, usernameCol)
    row = _findOrAppendMemberRow(layout, sheetKey, startRow, usernameCol, username)
    currentUsername = _engine.readRowColumns(
        sheetKey,
        row=row,
        columnMap={"username": usernameCol},
    ).get("username", "")
    created = _normalizeKey(currentUsername) != _normalizeKey(username)

    if created:
        defaults = _parseDefaultColumnValues(layout.get("defaultColumnValues"))
        valuesToWrite: dict[str, str] = dict(defaults)
        rankCol = str(layout.get("rankColumn") or "").strip().upper()
        requestedRank = str(initialRank or "").strip()
        if rankCol and requestedRank:
            valuesToWrite[rankCol] = requestedRank
        valuesToWrite[usernameCol] = username
        _writeSingleRowValues(sheetKey, row, valuesToWrite)
    else:
        _engine.batchUpdateValues(
            sheetKey,
            [
                {
                    "range": f"{_sheetName(sheetKey)}!{usernameCol}{row}:{usernameCol}{row}",
                    "values": [[username]],
                }
            ],
        )
    _applyUpsertDateColumnRules(layout, sheetKey, row, created=created)

    sortedRows, _ = _organizeLayoutRowsThroughMemberRow(
        layout,
        sheetKey,
        usernameCol,
        startRow,
        row,
    )
    return {
        "ok": True,
        "row": row,
        "sheetKey": sheetKey,
        "sortedRows": sortedRows,
        "spacerRowsAdded": spacerRowsAdded,
    }


def syncDivisionMemberRankByRobloxUsername(
    divisionKey: str,
    robloxUsername: str,
    targetRank: str,
    organizeAfter: bool = True,
) -> dict[str, Any]:
    layout = _findLayoutByDivisionKey(divisionKey)
    if not layout:
        return {"ok": False, "reason": "layout-not-configured"}

    username = str(robloxUsername or "").strip()
    rankValue = str(targetRank or "").strip()
    if not username:
        return {"ok": False, "reason": "missing-username"}
    if not rankValue:
        return {"ok": False, "reason": "missing-target-rank"}

    context = _resolveLayoutSheetContext(layout)
    if context is None:
        return {"ok": False, "reason": "missing-sheet-key"}

    sheetKey = str(context["sheetKey"])
    usernameCol = str(context["usernameCol"])
    rankCol = str(layout.get("rankColumn") or "").strip().upper()
    if not rankCol:
        return {"ok": False, "reason": "missing-rank-column"}
    startRow = int(context["startRow"])

    _ensureSectionSpacerRows(layout, sheetKey, startRow, usernameCol)
    row = _findOrAppendMemberRow(layout, sheetKey, startRow, usernameCol, username)
    rowSnapshot = _engine.readRowColumns(
        sheetKey,
        row=row,
        columnMap={"username": usernameCol, "rank": rankCol},
    )
    currentUsername = rowSnapshot.get("username", "")
    created = _normalizeKey(currentUsername) != _normalizeKey(username)
    if created:
        defaults = _parseDefaultColumnValues(layout.get("defaultColumnValues"))
        valuesToWrite: dict[str, str] = dict(defaults)
        valuesToWrite[usernameCol] = username
        _writeSingleRowValues(sheetKey, row, valuesToWrite)
    else:
        _engine.batchUpdateValues(
            sheetKey,
            [
                {
                    "range": f"{_sheetName(sheetKey)}!{usernameCol}{row}:{usernameCol}{row}",
                    "values": [[username]],
                }
            ],
        )

    currentRank = str(rowSnapshot.get("rank", "")).strip()
    rankUpdated = _normalizeKey(currentRank) != _normalizeKey(rankValue)
    if rankUpdated:
        _engine.batchUpdateValues(
            sheetKey,
            [
                {
                    "range": f"{_sheetName(sheetKey)}!{rankCol}{row}:{rankCol}{row}",
                    "values": [[rankValue]],
                }
            ],
        )

    sortedRows = 0
    touchedRows = 0
    if organizeAfter:
        sortedRows, touchedRows = _organizeLayoutRowsThroughMemberRow(
            layout,
            sheetKey,
            usernameCol,
            startRow,
            row,
        )

    return {
        "ok": True,
        "divisionKey": str(layout.get("divisionKey") or divisionKey),
        "sheetKey": sheetKey,
        "row": row,
        "created": created,
        "rankUpdated": rankUpdated,
        "previousRank": currentRank,
        "targetRank": rankValue,
        "sortedRows": sortedRows,
        "rows": touchedRows,
    }


def touchupDivisionSheet(divisionKey: str) -> dict[str, Any]:
    layout = _findLayoutByDivisionKey(divisionKey)
    if not layout:
        return {"ok": False, "reason": "layout-not-configured"}

    context = _resolveLayoutSheetContext(layout)
    if context is None:
        return {"ok": False, "reason": "missing-sheet-key"}

    sheetKey = str(context["sheetKey"])
    startRow = int(context["startRow"])
    usernameCol = str(context["usernameCol"])
    spacerRowsAdded = _ensureSectionSpacerRows(layout, sheetKey, startRow, usernameCol)
    _, endRow = _memberRowsRange(layout, sheetKey, startRow, usernameCol)
    if endRow < startRow:
        return {"ok": True, "rows": 0, "sheetKey": sheetKey, "spacerRowsAdded": spacerRowsAdded}

    sortedRows, touchedRows = _organizeLayoutRows(
        layout,
        sheetKey,
        startRow=startRow,
        endRow=endRow,
    )
    return {
        "ok": True,
        "sheetKey": sheetKey,
        "rows": touchedRows,
        "sortedRows": sortedRows,
        "startRow": startRow,
        "endRow": endRow,
        "spacerRowsAdded": spacerRowsAdded,
    }


def touchupAllDepartmentSheets() -> dict[str, Any]:
    results: dict[str, Any] = {}
    updated = 0
    failures = 0
    if getattr(config, "deptSpreadsheetId", ""):
        recruitmentResult, recruitmentError = _runWithRateLimitRetry(
            "Department ORBAT touchup for ANRORS",
            recruitmentSheets.touchupRecruitmentRows,
        )
        if recruitmentError is not None:
            failures += 1
            results["ANRORS"] = {"ok": False, "reason": "exception", "error": str(recruitmentError)}
            log.exception("Department ORBAT touchup failed for ANRORS.")
        else:
            if isinstance(recruitmentResult, dict):
                results["ANRORS"] = {"ok": True, **recruitmentResult}
            else:
                results["ANRORS"] = {"ok": True, "result": str(recruitmentResult)}
            updated += 1
    for layout in _allDepartmentLayouts():
        divisionKey = str(layout.get("divisionKey") or "").strip()
        if not divisionKey:
            continue
        result, divisionError = _runWithRateLimitRetry(
            f"Department ORBAT touchup for {divisionKey}",
            lambda key=divisionKey: touchupDivisionSheet(key),
        )
        if divisionError is not None:
            failures += 1
            result = {"ok": False, "reason": "exception", "error": str(divisionError)}
            log.exception("Department ORBAT touchup failed for %s.", divisionKey)
        results[divisionKey] = result
        if result.get("ok"):
            updated += 1
    if any(_normalizeKey((layout or {}).get("divisionKey")) == _normalizeKey("ANRD") for layout in _allDepartmentLayouts()):
        anrdPaymentResult, anrdPaymentError = _runWithRateLimitRetry(
            "Department ORBAT touchup for ANRD payment manager",
            anrdPaymentSheets.touchupPaymentManagerSorting,
        )
        anrdEntry = results.get("ANRD")
        if not isinstance(anrdEntry, dict):
            anrdEntry = {"ok": anrdPaymentError is None}
            results["ANRD"] = anrdEntry
        if anrdPaymentError is not None:
            failures += 1
            anrdEntry["ok"] = False
            anrdEntry["paymentManager"] = {
                "ok": False,
                "reason": "exception",
                "error": str(anrdPaymentError),
            }
            log.exception("Department ORBAT touchup failed for ANRD payment manager.")
        else:
            if isinstance(anrdPaymentResult, dict):
                anrdEntry["paymentManager"] = {"ok": True, **anrdPaymentResult}
            else:
                anrdEntry["paymentManager"] = {"ok": True, "result": str(anrdPaymentResult)}
    return {"updatedSheets": updated, "failedSheets": failures, "results": results}

