import time
from typing import Optional, Dict, Any

import config
from features.staff.orbat.a1 import cellRange, columnIndex, indexToColumn
from features.staff.orbat.engineFacade import createEngineServiceFacade
from features.staff.orbat.multiEngine import getMultiOrbatEngine


_ORBAT_SHEET_KEY = "generalStaff"
_engine = getMultiOrbatEngine()
_serviceFacade = None
_rowLookupCache: dict[str, tuple[float, dict[str, int]]] = {}


def _columnToIndex(col: str) -> int:
    return columnIndex(col)


def _maxColumnIndex(columns: list[str]) -> int:
    return max((_columnToIndex(c) for c in columns if c), default=1)


def _getService():
    global _serviceFacade
    if _serviceFacade is None:
        _serviceFacade = createEngineServiceFacade(_engine, _ORBAT_SHEET_KEY)
    return _serviceFacade


def _sheetName() -> str:
    return _engine.getSheetName(_ORBAT_SHEET_KEY)


def _spreadsheetId() -> str:
    sheetId = _engine.getSpreadsheetId(_ORBAT_SHEET_KEY)
    if not sheetId:
        raise RuntimeError("Missing ORBAT spreadsheet ID.")
    return sheetId


def _range(col: str, row: int) -> str:
    return cellRange(_sheetName(), col, row)


def _getColumns() -> Dict[str, str]:
    return {
        "discordId": getattr(config, "orbatColumnDiscordId", "A") or "",
        "robloxUser": getattr(config, "orbatColumnRobloxUser", "B"),
        "rank": getattr(config, "orbatColumnRank", "C"),
        "clearance": getattr(config, "orbatColumnClearance", "D"),
        "status": getattr(config, "orbatColumnStatus", "E"),
        "loaInfo": getattr(config, "orbatColumnLoaInfo", "G"),
        "department": getattr(config, "orbatColumnDepartment", "F"),
        "notes": getattr(config, "orbatColumnNotes", "H"),
        "mic": getattr(config, "orbatColumnMic", "") or "",
        "timezone": getattr(config, "orbatColumnTimezone", "N"),
        "ageGroup": getattr(config, "orbatColumnAgeGroup", "O"),
        "shifts": getattr(config, "orbatColumnShifts", "I"),
        "otherEvents": getattr(config, "orbatColumnOtherEvents", "J"),
        "total": getattr(config, "orbatColumnTotal", "K"),
        "allTimeShifts": getattr(config, "orbatColumnAllTimeShifts", "L"),
        "allTime": getattr(config, "orbatColumnAllTime", "M"),
        "strikes": getattr(config, "orbatColumnStrikes", "P"),
    }


_ORBAT_VISIBLE_COLUMN_KEYS = (
    "robloxUser",
    "rank",
    "clearance",
    "status",
    "loaInfo",
    "department",
    "notes",
    "shifts",
    "otherEvents",
    "total",
    "allTimeShifts",
    "allTime",
    "mic",
    "timezone",
    "ageGroup",
    "strikes",
)

_ORBAT_COUNTER_COLUMN_KEYS = (
    "shifts",
    "otherEvents",
    "total",
    "allTimeShifts",
    "allTime",
)

_ORBAT_PRESERVED_COLUMN_KEYS = (
    *_ORBAT_COUNTER_COLUMN_KEYS,
    "strikes",
)


def _configuredColumnLetters(columns: Dict[str, str]) -> list[str]:
    return [
        str(col or "").strip().upper()
        for col in list(columns.values())
        if str(col or "").strip()
    ]


def _configuredReadRange(columns: Dict[str, str]) -> str:
    endColumn = indexToColumn(_maxColumnIndex(_configuredColumnLetters(columns)))
    return f"{_sheetName()}!A:{endColumn}"


def _columnValues(rows: list[list[str]], columnLetter: str) -> list[str]:
    index = _columnToIndex(columnLetter) - 1
    if index < 0:
        return []
    return [row[index] if index < len(row) else "" for row in rows]


def _rowColumnValue(row: list[str], columnLetter: str) -> str:
    index = _columnToIndex(columnLetter) - 1
    if index < 0 or index >= len(row):
        return ""
    return row[index]


def _dataStyleColumnBounds(columns: Dict[str, str]) -> tuple[int, int]:
    indices = [
        _columnToIndex(columns.get(key, ""))
        for key in _ORBAT_VISIBLE_COLUMN_KEYS
        if columns.get(key, "")
    ]
    indices = [index for index in indices if index > 0]
    if not indices:
        return 0, 0
    return min(indices) - 1, max(indices)


def _lookupCacheTtlSec() -> float:
    return float(getattr(config, "orbatRowLookupCacheTtlSec", 120) or 120)


def _invalidateLookupCaches() -> None:
    _rowLookupCache.clear()


def _getCachedRowLookup(
    service,
    *,
    columnLabel: str,
    columnLetter: str,
    normalizeValue,
) -> dict[str, int]:
    cacheKey = f"{columnLabel}:{columnLetter}"
    now = time.monotonic()
    cached = _rowLookupCache.get(cacheKey)
    if cached and (now - cached[0]) <= _lookupCacheTtlSec():
        return cached[1]

    sheetId = _spreadsheetId()
    values = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=sheetId, range=f"{_sheetName()}!{columnLetter}:{columnLetter}")
        .execute()
        .get("values", [])
    )

    lookup: dict[str, int] = {}
    for idx, row in enumerate(values, start=1):
        if not row:
            continue
        raw = str(row[0]).strip()
        if not raw:
            continue
        key = normalizeValue(raw)
        if key and key not in lookup:
            lookup[key] = idx
    _rowLookupCache[cacheKey] = (now, lookup)
    return lookup


def _findRowByDiscordIdWithService(service, discordId: int) -> Optional[int]:
    columns = _getColumns()
    col = columns["discordId"]
    if not col:
        return None
    lookup = _getCachedRowLookup(
        service,
        columnLabel="discordId",
        columnLetter=col,
        normalizeValue=lambda value: str(value).strip(),
    )
    return lookup.get(str(discordId))


def _findRowByRobloxUserWithService(service, robloxUser: str) -> Optional[int]:
    columns = _getColumns()
    col = columns["robloxUser"]
    lookup = _getCachedRowLookup(
        service,
        columnLabel="robloxUser",
        columnLetter=col,
        normalizeValue=lambda value: str(value).strip().lower(),
    )
    return lookup.get(robloxUser.strip().lower())


def findRowByDiscordId(discordId: int) -> Optional[int]:
    service = _getService()
    return _findRowByDiscordIdWithService(service, discordId)


def findRowByRobloxUser(robloxUser: str) -> Optional[int]:
    service = _getService()
    return _findRowByRobloxUserWithService(service, robloxUser)


def getOrbatEntry(discordId: int, robloxUser: Optional[str] = None) -> Optional[Dict[str, Any]]:
    service = _getService()
    columns = _getColumns()
    row = _findRowByDiscordIdWithService(service, discordId)
    if not row and robloxUser:
        row = _findRowByRobloxUserWithService(service, robloxUser)
    if not row:
        return None

    ranges = []
    for key in (
        "robloxUser",
        "rank",
        "clearance",
        "department",
        "notes",
        "mic",
        "timezone",
        "ageGroup",
        "status",
        "loaInfo",
    ):
        col = columns.get(key, "")
        if col:
            ranges.append(_range(col, row))
    sheetId = _spreadsheetId()
    data = (
        service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=sheetId, ranges=ranges)
        .execute()
        .get("valueRanges", [])
    )

    def _getValue(idx: int) -> str:
        try:
            values = data[idx].get("values", [])
            if not values or not values[0]:
                return ""
            return str(values[0][0])
        except Exception:
            return ""

    def _findIndex(key: str) -> int:
        col = columns.get(key, "")
        if not col:
            return -1
        try:
            return ranges.index(_range(col, row))
        except ValueError:
            return -1

    return {
        "robloxUser": _getValue(_findIndex("robloxUser")),
        "rank": _getValue(_findIndex("rank")),
        "clearance": _getValue(_findIndex("clearance")),
        "department": _getValue(_findIndex("department")),
        "notes": _getValue(_findIndex("notes")),
        "mic": _getValue(_findIndex("mic")),
        "timezone": _getValue(_findIndex("timezone")),
        "ageGroup": _getValue(_findIndex("ageGroup")),
        "status": _getValue(_findIndex("status")),
        "loaInfo": _getValue(_findIndex("loaInfo")),
        "row": row,
    }

def _rankLetter(rank: str) -> str:
    if not rank:
        return ""
    return rank.strip().split(" ", 1)[0].strip().upper()


def _rankTier(rank: str) -> int:
    letter = _rankLetter(rank)
    tiers = {
        "J": 10,
        "I": 9,
        "H": 8,
        "G": 7,
        "F": 6,
        "E": 5,
        "D": 4,
        "C": 3,
        "B": 2,
        "A": 1,
        "0": 0,
    }
    return tiers.get(letter, -1)


def _sectionForRank(rank: str) -> Optional[str]:
    letter = _rankLetter(rank)
    if letter in {"J", "I", "H", "G"}:
        return "ANROCOM"
    if letter == "F":
        return "HIGH"
    if letter in {"E", "D", "C"}:
        return "MIDDLE"
    if letter == "B":
        return "JUNIOR"
    return None

def _departmentSortKey(value: str) -> str:
    cleaned = str(value or "").strip().lower()
    return cleaned or "zzzz"

def _isVacant(value: str) -> bool:
    return str(value or "").strip().lower() == "--vacant--"

def _normalizeDepartmentName(value: str) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())

def _highRankDepartmentIndex(value: str) -> int:
    normalized = _normalizeDepartmentName(value)
    if "internal" in normalized:
        return 0
    if "community" in normalized:
        return 1
    if "training" in normalized or "qualification" in normalized:
        return 2
    if "logistic" in normalized or "operation" in normalized:
        return 3
    if "middle" in normalized:
        return 4
    if "general" in normalized:
        return 99
    return 50


def _getSheetId(service) -> int:
    return _engine.getSheetTabId(_ORBAT_SHEET_KEY)

def _hexToColor(value: str) -> Optional[dict]:
    raw = value.strip().lstrip("#")
    if len(raw) != 6:
        return None
    try:
        r = int(raw[0:2], 16) / 255.0
        g = int(raw[2:4], 16) / 255.0
        b = int(raw[4:6], 16) / 255.0
    except ValueError:
        return None
    return {"red": r, "green": g, "blue": b}


def _defaultBandingColors() -> tuple[dict, dict]:
    primaryHex = getattr(config, "orbatBandingPrimaryHex", "") or ""
    secondaryHex = getattr(config, "orbatBandingSecondaryHex", "") or ""
    primary = _hexToColor(primaryHex) if primaryHex else None
    secondary = _hexToColor(secondaryHex) if secondaryHex else None
    if primary and secondary:
        return primary, secondary
    return (
        {"red": 0.95, "green": 0.95, "blue": 0.95},
        {"red": 0.85, "green": 0.85, "blue": 0.85},
    )


def _resolveBandingColors(meta: dict, sheetName: str) -> tuple[dict, dict]:
    primaryHex = getattr(config, "orbatBandingPrimaryHex", "") or ""
    secondaryHex = getattr(config, "orbatBandingSecondaryHex", "") or ""
    primary = _hexToColor(primaryHex) if primaryHex else None
    secondary = _hexToColor(secondaryHex) if secondaryHex else None
    if primary and secondary:
        return primary, secondary
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") != sheetName:
            continue
        banded = sheet.get("bandedRanges", [])
        for entry in banded:
            band = entry.get("bandedRange", {})
            rowProps = band.get("rowProperties", {})
            primary = rowProps.get("firstBandColor")
            secondary = rowProps.get("secondBandColor")
            if primary and secondary:
                return primary, secondary
    return _defaultBandingColors()


def _bandingOverlaps(rangeA: dict, rangeB: dict) -> bool:
    if rangeA.get("sheetId") != rangeB.get("sheetId"):
        return False
    aStartRow = rangeA.get("startRowIndex", 0)
    aEndRow = rangeA.get("endRowIndex", aStartRow)
    bStartRow = rangeB.get("startRowIndex", 0)
    bEndRow = rangeB.get("endRowIndex", bStartRow)
    if aEndRow <= bStartRow or bEndRow <= aStartRow:
        return False
    aStartCol = rangeA.get("startColumnIndex", 0)
    aEndCol = rangeA.get("endColumnIndex", aStartCol)
    bStartCol = rangeB.get("startColumnIndex", 0)
    bEndCol = rangeB.get("endColumnIndex", bStartCol)
    return not (aEndCol <= bStartCol or bEndCol <= aStartCol)


def _applySectionBanding(service, headers: Dict[str, int], totalRows: int) -> None:
    if not headers:
        return
    sheetName = _sheetName()
    meta = service.spreadsheets().get(spreadsheetId=_spreadsheetId()).execute()
    sheetId = None
    bandedRanges = []
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") != sheetName:
            continue
        sheetId = int(props.get("sheetId"))
        bandedRanges = sheet.get("bandedRanges", [])
        break
    if sheetId is None:
        return

    primaryColor, secondaryColor = _resolveBandingColors(meta, sheetName)
    bounds = _sectionBounds(headers, totalRows)
    columns = _getColumns()
    startColumnIndex, endColumnIndex = _dataStyleColumnBounds(columns)
    if endColumnIndex <= startColumnIndex:
        return

    requests = []
    for entry in bandedRanges:
        band = entry.get("bandedRange", {})
        bandId = band.get("bandedRangeId")
        if not bandId:
            continue
        requests.append({"deleteBanding": {"bandedRangeId": bandId}})

    if requests:
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=_spreadsheetId(),
                body={"requests": requests},
            ).execute()
        except Exception:
            pass

    manualRequests = []
    for startRow, endRow in bounds.values():
        if endRow < startRow:
            continue
        for rowIdx in range(startRow, endRow + 1):
            isPrimary = (rowIdx - startRow) % 2 == 0
            color = primaryColor if isPrimary else secondaryColor
            manualRequests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheetId,
                            "startRowIndex": rowIdx - 1,
                            "endRowIndex": rowIdx,
                            "startColumnIndex": startColumnIndex,
                            "endColumnIndex": endColumnIndex,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": color}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                }
            )
    if manualRequests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=_spreadsheetId(),
            body={"requests": manualRequests},
        ).execute()

def _applyRowTextStyle(
    service,
    startRow: int,
    endRow: Optional[int] = None,
) -> None:
    if startRow <= 0:
        return
    if endRow is None:
        endRow = startRow
    if endRow < startRow:
        endRow = startRow
    sheetGid = _getSheetId(service)
    fontSize = int(getattr(config, "orbatRowFontSize", 13) or 13)
    bold = bool(getattr(config, "orbatRowBold", True))
    startColumnIndex, endColumnIndex = _dataStyleColumnBounds(_getColumns())
    if endColumnIndex <= startColumnIndex:
        return
    request = {
        "repeatCell": {
            "range": {
                "sheetId": sheetGid,
                "startRowIndex": startRow - 1,
                "endRowIndex": endRow,
                "startColumnIndex": startColumnIndex,
                "endColumnIndex": endColumnIndex,
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "bold": bold,
                        "fontSize": fontSize,
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat.bold,userEnteredFormat.textFormat.fontSize",
        }
    }
    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=_spreadsheetId(),
            body={"requests": [request]},
        ).execute()
    except Exception:
        # Formatting should never block functional writes.
        pass

def _coerceNumber(value: Any) -> float:
    try:
        return float(str(value or "").replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0

def _coerceInt(value: Any) -> int:
    return int(round(_coerceNumber(value)))


def _defaultCounterColumnValues(columns: Dict[str, str]) -> Dict[str, Any]:
    return {
        columns[key]: 0
        for key in _ORBAT_COUNTER_COLUMN_KEYS
        if columns.get(key)
    }


def _readRowValuesByKeys(
    service,
    sheetId: str,
    columns: Dict[str, str],
    row: int,
    keys: tuple[str, ...],
) -> Dict[str, Any]:
    ranges: list[str] = []
    rangeColumns: list[str] = []
    for key in keys:
        col = columns.get(key, "")
        if not col:
            continue
        ranges.append(_range(col, row))
        rangeColumns.append(col)
    if not ranges:
        return {}
    fetched = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=sheetId,
            ranges=ranges,
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
        .get("valueRanges", [])
    )
    out: Dict[str, Any] = {}
    for idx, col in enumerate(rangeColumns):
        value = ""
        try:
            values = fetched[idx].get("values", [])
            if values and values[0]:
                value = values[0][0]
        except Exception:
            value = ""
        out[col] = value
    return out


def _loadRangeValues(service, rangeA1: str) -> list[list[str]]:
    return (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=_spreadsheetId(), range=rangeA1)
        .execute()
        .get("values", [])
    )

def _loadRangeValuesWithOptions(service, rangeA1: str, valueRenderOption: str = "UNFORMATTED_VALUE") -> list[list[str]]:
    return (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=_spreadsheetId(),
            range=rangeA1,
            valueRenderOption=valueRenderOption,
        )
        .execute()
        .get("values", [])
    )


def _findSectionHeaders(colB: list[str]) -> Dict[str, int]:
    headers: Dict[str, int] = {}
    for idx, raw in enumerate(colB, start=1):
        text = str(raw or "").strip().lower()
        if not text:
            continue
        if "anro command personnel" in text:
            headers["ANROCOM"] = idx
        elif "high rank personnel" in text:
            headers["HIGH"] = idx
        elif "middle rank personnel" in text:
            headers["MIDDLE"] = idx
        elif "junior personnel" in text:
            headers["JUNIOR"] = idx
    return headers


def _sectionBounds(headers: Dict[str, int], totalRows: int) -> Dict[str, tuple[int, int]]:
    ordered = sorted(headers.items(), key=lambda item: item[1])
    bounds: Dict[str, tuple[int, int]] = {}
    for idx, (name, row) in enumerate(ordered):
        start = row + 1
        end = totalRows
        if idx + 1 < len(ordered):
            end = ordered[idx + 1][1] - 1
        bounds[name] = (start, end)
    return bounds


def _writeRow(
    service,
    sheetId: str,
    columns: Dict[str, str],
    valuesByColumn: Dict[str, str],
    row: int,
) -> None:
    data = [
        {
            "range": _range(col, row),
            "values": [[value]],
        }
        for col, value in valuesByColumn.items()
        if col
    ]
    try:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=sheetId,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": data,
            },
        ).execute()
    except Exception as exc:
        # If discordId column is protected, retry without it.
        if columns["discordId"] and "protected cell" in str(exc).lower():
            data = [d for d in data if not d["range"].startswith(f"{_sheetName()}!{columns['discordId']}")]
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=sheetId,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": data,
                },
            ).execute()
        else:
            raise
    _applyRowTextStyle(service, row)


def _upsertOrbatRowSimple(
    discordId: int,
    robloxUser: str,
    rank: str,
    clearance: str,
    department: str,
    notes: str,
    mic: str,
    timezone: str,
    ageGroup: str,
    status: str,
    loaInfo: str = "",
) -> int:
    service = _getService()
    columns = _getColumns()
    sheetId = _spreadsheetId()

    row = _findRowByRobloxUserWithService(service, robloxUser)
    if row is None:
        row = _findRowByDiscordIdWithService(service, discordId)

    valuesByColumn = {
        columns["robloxUser"]: robloxUser,
        columns["rank"]: rank,
        columns["clearance"]: clearance,
        columns["department"]: department,
        columns["notes"]: notes,
        columns["mic"]: mic,
        columns["timezone"]: timezone,
        columns["ageGroup"]: ageGroup,
        columns["status"]: status,
        columns["loaInfo"]: loaInfo,
    }
    if columns["discordId"]:
        valuesByColumn[columns["discordId"]] = str(discordId)

    if row:
        _writeRow(service, sheetId, columns, valuesByColumn, row)
        _invalidateLookupCaches()
        return row

    valuesByColumn.update(_defaultCounterColumnValues(columns))

    maxCol = _maxColumnIndex(list(valuesByColumn.keys()))
    rowValues = [""] * maxCol
    for col, value in valuesByColumn.items():
        idx = _columnToIndex(col)
        if idx <= 0:
            continue
        rowValues[idx - 1] = value

    response = (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=sheetId,
            range=f"{_sheetName()}!A:A",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [rowValues]},
        )
        .execute()
    )
    updatedRange = response.get("updates", {}).get("updatedRange", "")
    if "!" in updatedRange:
        rowPart = updatedRange.split("!")[1]
        rowDigits = "".join(ch for ch in rowPart if ch.isdigit())
        if rowDigits.isdigit():
            rowNum = int(rowDigits)
            _applyRowTextStyle(service, rowNum)
            _invalidateLookupCaches()
            return rowNum
    _invalidateLookupCaches()
    return 0


def upsertOrbatRow(
    discordId: int,
    robloxUser: str,
    rank: str,
    clearance: str,
    department: str,
    notes: str,
    mic: str,
    timezone: str,
    ageGroup: str,
    status: str,
    loaInfo: str = "",
) -> int:
    service = _getService()
    columns = _getColumns()
    sheetId = _spreadsheetId()
    section = _sectionForRank(rank)

    values = _loadRangeValues(service, _configuredReadRange(columns))
    nameValues = _columnValues(values, columns["robloxUser"])
    rankValues = _columnValues(values, columns["rank"])
    departmentValues = _columnValues(values, columns["department"])

    headers = _findSectionHeaders(nameValues)
    if not section or section not in headers:
        return _upsertOrbatRowSimple(
            discordId,
            robloxUser,
            rank,
            clearance,
            department,
            notes,
            mic,
            timezone,
            ageGroup,
            status,
            loaInfo,
        )

    bounds = _sectionBounds(headers, max(len(nameValues), 1))
    startRow, endRow = bounds[section]
    if endRow < startRow:
        endRow = startRow

    # Find existing row by Roblox username (case-insensitive).
    targetLower = robloxUser.strip().lower()
    existingRow = None
    for idx, value in enumerate(nameValues, start=1):
        if str(value or "").strip().lower() == targetLower:
            existingRow = idx
            break
    if existingRow is None:
        existingRow = _findRowByDiscordIdWithService(service, discordId)

    # If existing row is outside the target section, delete it so we can reinsert.
    needsBanding = False
    preservedValuesByColumn: Dict[str, Any] = {}
    if existingRow:
        if section == "ANROCOM" and startRow <= existingRow <= endRow:
            valuesByColumn = {
                columns["robloxUser"]: robloxUser,
                columns["rank"]: rank,
                columns["clearance"]: clearance,
                columns["department"]: department,
                columns["notes"]: notes,
                columns["mic"]: mic,
                columns["timezone"]: timezone,
                columns["ageGroup"]: ageGroup,
                columns["status"]: status,
                columns["loaInfo"]: loaInfo,
            }
            if columns["discordId"]:
                valuesByColumn[columns["discordId"]] = str(discordId)
            _writeRow(service, sheetId, columns, valuesByColumn, existingRow)
            return existingRow

        preservedValuesByColumn = _readRowValuesByKeys(
            service,
            sheetId,
            columns,
            existingRow,
            _ORBAT_PRESERVED_COLUMN_KEYS,
        )

        sheetGid = _getSheetId(service)
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheetId,
            body={
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheetGid,
                                "dimension": "ROWS",
                                "startIndex": existingRow - 1,
                                "endIndex": existingRow,
                            }
                        }
                    }
                ]
            },
        ).execute()
        needsBanding = True

        # Reload data after deletion.
        values = _loadRangeValues(service, _configuredReadRange(columns))
        nameValues = _columnValues(values, columns["robloxUser"])
        rankValues = _columnValues(values, columns["rank"])
        departmentValues = _columnValues(values, columns["department"])
        headers = _findSectionHeaders(nameValues)
        bounds = _sectionBounds(headers, max(len(nameValues), 1))
        startRow, endRow = bounds.get(section, (startRow, endRow))

    # Build ordered entries in target section.
    entries: list[dict] = []
    vacantRowsToDelete: list[int] = []
    for rowIdx in range(startRow, endRow + 1):
        name = nameValues[rowIdx - 1] if rowIdx - 1 < len(nameValues) else ""
        if not name:
            continue
        if _isVacant(name):
            if section in {"MIDDLE", "JUNIOR"}:
                vacantRowsToDelete.append(rowIdx)
                continue
        rankCell = rankValues[rowIdx - 1] if rowIdx - 1 < len(rankValues) else ""
        deptCell = departmentValues[rowIdx - 1] if rowIdx - 1 < len(departmentValues) else ""
        entries.append(
            {
                "row": rowIdx,
                "rankTier": _rankTier(rankCell),
                "name": str(name),
                "departmentKey": _departmentSortKey(deptCell),
                "departmentIndex": _highRankDepartmentIndex(deptCell),
                "nameKey": "zzzz" if _isVacant(name) else str(name).strip().lower(),
            }
        )

    if vacantRowsToDelete:
        sheetGid = _getSheetId(service)
        deleteRequests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheetGid,
                        "dimension": "ROWS",
                        "startIndex": rowIdx - 1,
                        "endIndex": rowIdx,
                    }
                }
            }
            for rowIdx in sorted(vacantRowsToDelete, reverse=True)
        ]
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheetId,
            body={"requests": deleteRequests},
        ).execute()
        needsBanding = True

        values = _loadRangeValues(service, _configuredReadRange(columns))
        nameValues = _columnValues(values, columns["robloxUser"])
        rankValues = _columnValues(values, columns["rank"])
        departmentValues = _columnValues(values, columns["department"])
        headers = _findSectionHeaders(nameValues)
        bounds = _sectionBounds(headers, max(len(nameValues), 1))
        startRow, endRow = bounds.get(section, (startRow, endRow))
        entries = []
        for rowIdx in range(startRow, endRow + 1):
            name = nameValues[rowIdx - 1] if rowIdx - 1 < len(nameValues) else ""
            if not name:
                continue
            if _isVacant(name) and section in {"MIDDLE", "JUNIOR"}:
                continue
            rankCell = rankValues[rowIdx - 1] if rowIdx - 1 < len(rankValues) else ""
            deptCell = departmentValues[rowIdx - 1] if rowIdx - 1 < len(departmentValues) else ""
            entries.append(
                {
                    "row": rowIdx,
                    "rankTier": _rankTier(rankCell),
                    "name": str(name),
                    "departmentKey": _departmentSortKey(deptCell),
                    "nameKey": "zzzz" if _isVacant(name) else str(name).strip().lower(),
                }
            )

    newTier = _rankTier(rank)
    newName = robloxUser.lower()
    newDepartment = _departmentSortKey(department)
    newDepartmentIndex = _highRankDepartmentIndex(department)

    insertRow = endRow + 1
    for entry in entries:
        if section == "HIGH":
            if newDepartmentIndex < entry["departmentIndex"]:
                insertRow = entry["row"]
                break
            if newDepartmentIndex == entry["departmentIndex"]:
                if newDepartment < entry["departmentKey"]:
                    insertRow = entry["row"]
                    break
                if newDepartment == entry["departmentKey"] and newName < entry["nameKey"]:
                    insertRow = entry["row"]
                    break
        else:
            if newTier > entry["rankTier"]:
                insertRow = entry["row"]
                break
            if newTier == entry["rankTier"] and newName < entry["nameKey"]:
                insertRow = entry["row"]
                break

    # If we can reuse a vacant row at the insertion point, do it.
    canReuseVacant = False
    if insertRow <= endRow and insertRow - 1 < len(nameValues):
        if str(nameValues[insertRow - 1]).strip().lower() == "--vacant--":
            canReuseVacant = True

    if not canReuseVacant:
        sheetGid = _getSheetId(service)
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheetId,
            body={
                "requests": [
                    {
                        "insertDimension": {
                            "range": {
                                "sheetId": sheetGid,
                                "dimension": "ROWS",
                                "startIndex": insertRow - 1,
                                "endIndex": insertRow,
                            },
                            "inheritFromBefore": True,
                        }
                    }
                ]
            },
        ).execute()
        needsBanding = True

    valuesByColumn = {
        columns["robloxUser"]: robloxUser,
        columns["rank"]: rank,
        columns["clearance"]: clearance,
        columns["department"]: department,
        columns["notes"]: notes,
        columns["mic"]: mic,
        columns["timezone"]: timezone,
        columns["ageGroup"]: ageGroup,
        columns["status"]: status,
        columns["loaInfo"]: loaInfo,
    }
    if columns["discordId"]:
        valuesByColumn[columns["discordId"]] = str(discordId)
    if preservedValuesByColumn:
        valuesByColumn.update(preservedValuesByColumn)
    else:
        valuesByColumn.update(_defaultCounterColumnValues(columns))

    _writeRow(service, sheetId, columns, valuesByColumn, insertRow)
    if needsBanding:
        values = _loadRangeValues(service, _configuredReadRange(columns))
        nameValues = _columnValues(values, columns["robloxUser"])
        headers = _findSectionHeaders(nameValues)
        _applySectionBanding(service, headers, max(len(nameValues), 1))
    return insertRow


def organizeOrbatRows() -> dict:
    service = _getService()
    sheetId = _spreadsheetId()
    columns = _getColumns()

    values = _loadRangeValuesWithOptions(service, _configuredReadRange(columns))
    if not values:
        return {"sections": 0, "updated": 0}

    nameValues = _columnValues(values, columns["robloxUser"])
    rankValues = _columnValues(values, columns["rank"])
    departmentValues = _columnValues(values, columns["department"])
    headers = _findSectionHeaders(nameValues)
    if not headers:
        return {"sections": 0, "updated": 0}

    bounds = _sectionBounds(headers, max(len(nameValues), 1))
    sectionOrder = [section for section, _ in sorted(headers.items(), key=lambda item: item[1])]
    sheetName = _sheetName()

    columnLetters = [
        columns.get("discordId", ""),
        columns.get("robloxUser", ""),
        columns.get("rank", ""),
        columns.get("clearance", ""),
        columns.get("status", ""),
        columns.get("loaInfo", ""),
        columns.get("department", ""),
        columns.get("notes", ""),
        columns.get("mic", ""),
        columns.get("timezone", ""),
        columns.get("ageGroup", ""),
        columns.get("shifts", ""),
        columns.get("otherEvents", ""),
        columns.get("total", ""),
        columns.get("allTimeShifts", ""),
        columns.get("allTime", ""),
        columns.get("strikes", ""),
    ]
    columnLetters = [col for col in columnLetters if col]
    columnIndices = {col: _columnToIndex(col) - 1 for col in columnLetters}

    updatedSections = 0
    for section in sectionOrder:
        if section == "ANROCOM":
            continue
        values = _loadRangeValuesWithOptions(service, _configuredReadRange(columns))
        nameValues = _columnValues(values, columns["robloxUser"])
        rankValues = _columnValues(values, columns["rank"])
        departmentValues = _columnValues(values, columns["department"])
        headers = _findSectionHeaders(nameValues)
        bounds = _sectionBounds(headers, max(len(nameValues), 1))
        if section not in bounds:
            continue
        startRow, endRow = bounds[section]

        emptyRows = []
        vacantRows = []
        for rowIdx in range(startRow, endRow + 1):
            if rowIdx - 1 >= len(nameValues):
                break
            nameCell = nameValues[rowIdx - 1]
            if not str(nameCell or "").strip():
                emptyRows.append(rowIdx)
                continue
            if section in {"MIDDLE", "JUNIOR"} and _isVacant(nameCell):
                vacantRows.append(rowIdx)

        rowsToDelete = sorted(set(emptyRows + vacantRows), reverse=True)
        if rowsToDelete:
            sheetGid = _getSheetId(service)
            deleteRequests = [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheetGid,
                            "dimension": "ROWS",
                            "startIndex": rowIdx - 1,
                            "endIndex": rowIdx,
                        }
                    }
                }
                for rowIdx in rowsToDelete
            ]
            service.spreadsheets().batchUpdate(
                spreadsheetId=sheetId,
                body={"requests": deleteRequests},
            ).execute()

            values = _loadRangeValuesWithOptions(service, _configuredReadRange(columns))
            nameValues = _columnValues(values, columns["robloxUser"])
            rankValues = _columnValues(values, columns["rank"])
            departmentValues = _columnValues(values, columns["department"])
            headers = _findSectionHeaders(nameValues)
            bounds = _sectionBounds(headers, max(len(nameValues), 1))
            startRow, endRow = bounds.get(section, (startRow, endRow))

        entries: list[dict] = []
        for rowIdx in range(startRow, endRow + 1):
            if rowIdx - 1 >= len(values):
                break
            rowValues = values[rowIdx - 1]
            name = _rowColumnValue(rowValues, columns["robloxUser"])
            if not name:
                continue
            if _isVacant(name) and section in {"MIDDLE", "JUNIOR"}:
                continue
            rankCell = rankValues[rowIdx - 1] if rowIdx - 1 < len(rankValues) else ""
            deptCell = departmentValues[rowIdx - 1] if rowIdx - 1 < len(departmentValues) else ""
            valuesByCol = {}
            for col, idx in columnIndices.items():
                if idx < len(rowValues):
                    valuesByCol[col] = rowValues[idx]
                else:
                    valuesByCol[col] = ""
            entries.append(
                {
                    "row": rowIdx,
                    "nameKey": "zzzz" if _isVacant(name) else str(name).strip().lower(),
                    "rankTier": _rankTier(rankCell),
                    "departmentKey": _departmentSortKey(deptCell),
                    "departmentIndex": _highRankDepartmentIndex(deptCell),
                    "values": valuesByCol,
                }
            )

        if len(entries) < 2:
            continue

        if section == "HIGH":
            sortedEntries = sorted(
                entries,
                key=lambda e: (e["departmentIndex"], e["departmentKey"], e["nameKey"]),
            )
        else:
            sortedEntries = sorted(entries, key=lambda e: (-e["rankTier"], e["nameKey"]))

        currentOrder = [(e["departmentKey"], e["nameKey"], e["rankTier"]) for e in entries]
        sortedOrder = [(e["departmentKey"], e["nameKey"], e["rankTier"]) for e in sortedEntries]
        if currentOrder == sortedOrder:
            continue

        targetRows = [e["row"] for e in entries]
        data = []
        for idx, entry in enumerate(sortedEntries):
            row = targetRows[idx]
            for col, value in entry["values"].items():
                data.append(
                    {
                        "range": f"{sheetName}!{col}{row}",
                        "values": [[value]],
                    }
                )
        try:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=sheetId,
                body={"valueInputOption": "USER_ENTERED", "data": data},
            ).execute()
        except Exception as exc:
            if columns.get("discordId") and "protected cell" in str(exc).lower():
                protectedCol = columns.get("discordId")
                filtered = [d for d in data if not d["range"].startswith(f"{sheetName}!{protectedCol}")]
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=sheetId,
                    body={"valueInputOption": "USER_ENTERED", "data": filtered},
                ).execute()
            else:
                raise
        if targetRows:
            _applyRowTextStyle(service, min(targetRows), max(targetRows))

        updatedSections += 1

    values = _loadRangeValues(service, _configuredReadRange(columns))
    nameValues = _columnValues(values, columns["robloxUser"])
    headers = _findSectionHeaders(nameValues)
    bounds = _sectionBounds(headers, max(len(nameValues), 1))
    _applySectionBanding(service, headers, max(len(nameValues), 1))

    _invalidateLookupCaches()
    return {"sections": len(bounds), "updated": updatedSections}


def updateLoaStatus(
    discordId: int,
    status: str,
    loaInfo: str,
    robloxUser: Optional[str] = None,
) -> int:
    service = _getService()
    columns = _getColumns()
    sheetId = _spreadsheetId()
    row = _findRowByDiscordIdWithService(service, discordId)
    if not row and robloxUser:
        row = _findRowByRobloxUserWithService(service, robloxUser)
    if not row:
        return 0
    data = [
        {"range": _range(columns["status"], row), "values": [[status]]},
        {"range": _range(columns["loaInfo"], row), "values": [[loaInfo]]},
    ]
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheetId,
        body={"valueInputOption": "USER_ENTERED", "data": data},
    ).execute()
    _applyRowTextStyle(service, row)
    return row


def incrementEventCount(
    discordId: int,
    columnKey: str,
    delta: int = 1,
    robloxUser: Optional[str] = None,
) -> int:
    service = _getService()
    columns = _getColumns()
    column = columns.get(columnKey, "")
    if not column:
        return 0
    sheetId = _spreadsheetId()
    row = _findRowByDiscordIdWithService(service, discordId)
    if not row and robloxUser:
        row = _findRowByRobloxUserWithService(service, robloxUser)
    if not row:
        return 0

    targetRanges: dict[str, str] = {"event": _range(column, row)}
    totalCol = columns.get("total", "")
    allTimeCol = columns.get("allTime", "")
    allTimeShiftsCol = columns.get("allTimeShifts", "")
    if totalCol:
        targetRanges["total"] = _range(totalCol, row)
    if allTimeCol:
        targetRanges["allTime"] = _range(allTimeCol, row)
    if columnKey == "shifts" and allTimeShiftsCol:
        targetRanges["allTimeShifts"] = _range(allTimeShiftsCol, row)

    orderedKeys = list(targetRanges.keys())
    orderedRanges = [targetRanges[key] for key in orderedKeys]
    fetched = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=sheetId,
            ranges=orderedRanges,
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
        .get("valueRanges", [])
    )

    currentValues: dict[str, int] = {}
    for idx, key in enumerate(orderedKeys):
        rawValue = ""
        try:
            values = fetched[idx].get("values", [])
            if values and values[0]:
                rawValue = values[0][0]
        except Exception:
            rawValue = ""
        currentValues[key] = _coerceInt(rawValue)

    updatesByRange: dict[str, Any] = {
        targetRanges["event"]: currentValues.get("event", 0) + int(delta),
    }
    for key in ("total", "allTime", "allTimeShifts"):
        rangeA1 = targetRanges.get(key, "")
        if rangeA1:
            updatesByRange[rangeA1] = currentValues.get(key, 0) + int(delta)

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheetId,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": rangeA1, "values": [[value]]}
                for rangeA1, value in updatesByRange.items()
            ],
        },
    ).execute()
    _applyRowTextStyle(service, row)
    return row

