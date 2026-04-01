from __future__ import annotations

import re
from typing import Any, Optional

import config
from features.staff.departmentOrbat.layouts import loadDepartmentLayouts
from features.staff.orbat.a1 import cellRange, columnIndex, columnRange, rowRange
from features.staff.orbat.multiEngine import getMultiOrbatEngine


_engine = getMultiOrbatEngine()
_anrdSheetKey = "dept_anrd"
_paymentSectionHeaders = (
    "ANRD Payment Manager",
    "Funding Sources",
    "Developer Payment",
    "Contributor Payment",
)


def _sheetName() -> str:
    return _engine.getSheetName(_anrdSheetKey)


def _normalize(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _toInt(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    match = re.search(r"-?\d+", text.replace(",", ""))
    if not match:
        return default
    try:
        return int(match.group(0))
    except (TypeError, ValueError):
        return default


def _usernameSortTuple(value: Any) -> tuple[str, str]:
    raw = str(value or "").strip()
    return (_normalize(raw), raw.casefold())


def _range(col: str, row: int) -> str:
    return cellRange(_sheetName(), col, row)


def _rangeRow(startCol: str, endCol: str, row: int) -> str:
    return rowRange(_sheetName(), startCol, endCol, row)


def _readColumn(col: str, startRow: int, endRow: int) -> list[str]:
    if endRow < startRow:
        return []
    values = _engine.getValues(
        _anrdSheetKey,
        columnRange(_sheetName(), col, startRow, endRow),
    )
    out: list[str] = []
    span = endRow - startRow + 1
    for idx in range(span):
        row = values[idx] if idx < len(values) else []
        out.append(str(row[0]).strip() if row else "")
    return out


def _findPaymentManagerHeaderRow(scanStartRow: int = 1, scanEndRow: Optional[int] = None) -> Optional[int]:
    startRow = max(1, int(scanStartRow or 1))
    configuredEnd = int(getattr(config, "anrdPaymentManagerScanEndRow", 260) or 260)
    endRow = max(startRow, int(scanEndRow or max(configuredEnd, 600)))
    colA = _readColumn("A", startRow, endRow)
    target = _normalize("ANRD Payment Manager")
    for offset, value in enumerate(colA):
        if _normalize(value) == target:
            return startRow + offset
    return None


def _findMemberRank(robloxUsername: str) -> tuple[Optional[str], Optional[int]]:
    startRow = int(getattr(config, "anrdMembersStartRow", 3) or 3)
    configuredEndRow = int(getattr(config, "anrdMembersEndRow", 49) or 49)
    paymentHeaderRow = _findPaymentManagerHeaderRow()
    scanEndRow = paymentHeaderRow - 1 if paymentHeaderRow and paymentHeaderRow > startRow else max(configuredEndRow, startRow)
    endRow = max(configuredEndRow, scanEndRow)
    if endRow < startRow:
        return None, None
    rows = _engine.getValues(
        _anrdSheetKey,
        f"{_sheetName()}!A{startRow}:C{endRow}",
    )
    target = _normalize(robloxUsername)
    if not target:
        return None, None
    span = endRow - startRow + 1
    for idx in range(span):
        rowIndex = startRow + idx
        row = rows[idx] if idx < len(rows) else []
        usernameCell = str(row[0]).strip() if len(row) > 0 else ""
        if _normalize(usernameCell) != target:
            continue
        rankCell = str(row[2]).strip() if len(row) > 2 else ""
        return rankCell, rowIndex
    return None, None


def _findSection(sectionName: str) -> Optional[dict[str, Any]]:
    startRow = int(getattr(config, "anrdPaymentManagerStartRow", 52) or 52)
    endRow = int(getattr(config, "anrdPaymentManagerScanEndRow", 260) or 260)
    if endRow < startRow:
        return None

    headerNorm = _normalize(sectionName)
    headerRow: Optional[int] = None
    actualStartRow = startRow
    actualEndRow = endRow
    colA = _readColumn("A", actualStartRow, actualEndRow)
    for offset, value in enumerate(colA):
        if _normalize(value) == headerNorm:
            headerRow = actualStartRow + offset
            break
    if headerRow is None:
        # Fallback if sections shifted beyond configured rows.
        actualStartRow = 1
        actualEndRow = max(endRow, 600)
        colA = _readColumn("A", actualStartRow, actualEndRow)
        for offset, value in enumerate(colA):
            if _normalize(value) == headerNorm:
                headerRow = actualStartRow + offset
                break
    if headerRow is None:
        return None

    knownNorms = {_normalize(name) for name in _paymentSectionHeaders}
    nextHeaderRow: Optional[int] = None
    for row in range(headerRow + 1, actualEndRow + 1):
        value = colA[row - actualStartRow] if row - actualStartRow < len(colA) else ""
        if _normalize(value) in knownNorms:
            nextHeaderRow = row
            break
    sectionEndRow = (nextHeaderRow - 1) if nextHeaderRow else actualEndRow

    subHeaderName = "Source" if _normalize(sectionName) == _normalize("Funding Sources") else "Member"
    subHeaderRow: Optional[int] = None
    for row in range(headerRow + 1, sectionEndRow + 1):
        value = colA[row - actualStartRow] if row - actualStartRow < len(colA) else ""
        if _normalize(value) == _normalize(subHeaderName):
            subHeaderRow = row
            break
    if subHeaderRow is None:
        subHeaderRow = headerRow + 1
    dataStartRow = subHeaderRow + 1
    if dataStartRow > sectionEndRow:
        dataStartRow = sectionEndRow

    return {
        "sectionName": sectionName,
        "headerRow": headerRow,
        "subHeaderRow": subHeaderRow,
        "dataStartRow": dataStartRow,
        "dataEndRow": sectionEndRow,
        "nextHeaderRow": nextHeaderRow,
    }


def _findOrCreateSectionRow(section: dict[str, Any], robloxUsername: str) -> tuple[Optional[int], dict[str, Any], bool]:
    dataStartRow = int(section["dataStartRow"])
    dataEndRow = int(section["dataEndRow"])
    nextHeaderRow = section.get("nextHeaderRow")
    if dataStartRow > dataEndRow:
        insertRow = int(nextHeaderRow) - 1 if nextHeaderRow else dataStartRow
        sheetId = _engine.getSheetTabId(_anrdSheetKey)
        _engine.batchUpdateRequests(
            _anrdSheetKey,
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
        section = dict(section)
        section["dataStartRow"] = insertRow
        section["dataEndRow"] = insertRow
        if nextHeaderRow:
            section["nextHeaderRow"] = int(nextHeaderRow) + 1
        return insertRow, section, True

    usernames = _readColumn("A", dataStartRow, dataEndRow)
    target = _normalize(robloxUsername)
    firstBlank: Optional[int] = None
    for offset, value in enumerate(usernames):
        row = dataStartRow + offset
        if _normalize(value) == target:
            return row, section, False
        if firstBlank is None and not str(value).strip():
            firstBlank = row
    if firstBlank is not None:
        return firstBlank, section, False

    insertRow = int(nextHeaderRow) - 1 if nextHeaderRow else dataEndRow + 1
    sheetId = _engine.getSheetTabId(_anrdSheetKey)
    _engine.batchUpdateRequests(
        _anrdSheetKey,
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
    section = dict(section)
    section["dataEndRow"] = int(section["dataEndRow"]) + 1
    if nextHeaderRow:
        section["nextHeaderRow"] = int(nextHeaderRow) + 1
    return insertRow, section, True


def _sectionWidth(sectionName: str) -> str:
    if _normalize(sectionName) == _normalize("Funding Sources"):
        return "F"
    return "G"


def _anrdDeveloperRankOrderMap() -> tuple[dict[str, int], int]:
    rankList: list[str] = []
    layouts = loadDepartmentLayouts()
    for layout in layouts:
        if _normalize((layout or {}).get("divisionKey")) != _normalize("ANRD"):
            continue
        sectionRankOrder = (layout or {}).get("sectionRankOrder") or {}
        membersOrder = sectionRankOrder.get("Members") if isinstance(sectionRankOrder, dict) else None
        if isinstance(membersOrder, list) and membersOrder:
            rankList = [str(item).strip() for item in membersOrder if str(item).strip()]
            break
        configuredRankOrder = (layout or {}).get("rankOrder") or []
        if isinstance(configuredRankOrder, list) and configuredRankOrder:
            rankList = [str(item).strip() for item in configuredRankOrder if str(item).strip()]
            break

    if not rankList:
        fallback = getattr(config, "anrdDeveloperEligibleRanks", []) or []
        rankList = [str(item).strip() for item in fallback if str(item).strip()]

    orderMap = {_normalize(name): index for index, name in enumerate(rankList)}
    defaultIndex = len(orderMap) + 100
    return orderMap, defaultIndex


def _sortSectionRowsByUsername(section: dict[str, Any]) -> bool:
    dataStartRow = int(section.get("dataStartRow") or 0)
    dataEndRow = int(section.get("dataEndRow") or 0)
    sectionName = str(section.get("sectionName") or "").strip()
    if dataStartRow <= 0 or dataEndRow < dataStartRow:
        return False

    usernames = _readColumn("A", dataStartRow, dataEndRow)
    nonEmptyRows = [dataStartRow + idx for idx, value in enumerate(usernames) if str(value).strip()]
    if len(nonEmptyRows) < 2:
        return False

    sortStartRow = min(nonEmptyRows)
    sortEndRow = max(nonEmptyRows)
    endCol = _sectionWidth(sectionName)
    width = columnIndex(endCol)
    if width <= 0:
        return False

    if _normalize(sectionName) == _normalize("Developer Payment"):
        rowsRaw = _engine.getValues(
            _anrdSheetKey,
            f"{_sheetName()}!A{sortStartRow}:B{sortEndRow}",
        )
        rankOrderMap, rankDefault = _anrdDeveloperRankOrderMap()
        sortKeyCol = "Z"
        sortKeyValues: list[list[Any]] = []
        hasAnyDifference = False
        lastKey: Optional[int] = None
        lastUsernameNorm: Optional[str] = None

        for idx in range(sortEndRow - sortStartRow + 1):
            source = rowsRaw[idx] if idx < len(rowsRaw) else []
            username = str(source[0]).strip() if len(source) > 0 else ""
            rank = str(source[1]).strip() if len(source) > 1 else ""
            if username:
                keyValue = rankOrderMap.get(_normalize(rank), rankDefault)
                usernameNorm = _normalize(username)
            else:
                keyValue = rankDefault + 10000
                usernameNorm = ""
            sortKeyValues.append([keyValue])
            if lastKey is not None:
                if keyValue < lastKey or (keyValue == lastKey and usernameNorm < (lastUsernameNorm or "")):
                    hasAnyDifference = True
            lastKey = keyValue
            lastUsernameNorm = usernameNorm

        if not hasAnyDifference:
            return False

        _engine.batchUpdateValues(
            _anrdSheetKey,
            [
                {
                    "range": f"{_sheetName()}!{sortKeyCol}{sortStartRow}:{sortKeyCol}{sortEndRow}",
                    "values": sortKeyValues,
                }
            ],
        )

        sheetId = _engine.getSheetTabId(_anrdSheetKey)
        startColIndex = columnIndex("A") - 1
        endColIndex = columnIndex(sortKeyCol)
        keyColIndex = columnIndex(sortKeyCol) - 1
        userColIndex = columnIndex("A") - 1
        _engine.batchUpdateRequests(
            _anrdSheetKey,
            [
                {
                    "sortRange": {
                        "range": {
                            "sheetId": sheetId,
                            "startRowIndex": sortStartRow - 1,
                            "endRowIndex": sortEndRow,
                            "startColumnIndex": startColIndex,
                            "endColumnIndex": endColIndex,
                        },
                        "sortSpecs": [
                            {"dimensionIndex": keyColIndex, "sortOrder": "ASCENDING"},
                            {"dimensionIndex": userColIndex, "sortOrder": "ASCENDING"},
                        ],
                    }
                }
            ],
        )

        clearValues = [[""] for _ in range(sortEndRow - sortStartRow + 1)]
        _engine.batchUpdateValues(
            _anrdSheetKey,
            [
                {
                    "range": f"{_sheetName()}!{sortKeyCol}{sortStartRow}:{sortKeyCol}{sortEndRow}",
                    "values": clearValues,
                }
            ],
        )
        return True

    sheetId = _engine.getSheetTabId(_anrdSheetKey)
    startColIndex = columnIndex("A") - 1
    endColIndex = columnIndex(endCol)
    _engine.batchUpdateRequests(
        _anrdSheetKey,
        [
            {
                "sortRange": {
                    "range": {
                        "sheetId": sheetId,
                        "startRowIndex": sortStartRow - 1,
                        "endRowIndex": sortEndRow,
                        "startColumnIndex": startColIndex,
                        "endColumnIndex": endColIndex,
                    },
                    "sortSpecs": [
                        {
                            "dimensionIndex": startColIndex,
                            "sortOrder": "ASCENDING",
                        }
                    ],
                }
            }
        ],
    )
    return True


def _isContributorRank(rank: str) -> bool:
    configured = getattr(config, "anrdContributorRanks", []) or []
    allowed = {_normalize(item) for item in configured}
    return _normalize(rank) in allowed


def _isDeveloperRank(rank: str) -> bool:
    configured = getattr(config, "anrdDeveloperEligibleRanks", []) or []
    allowed = {_normalize(item) for item in configured}
    return _normalize(rank) in allowed


def _isUnlimitedDeveloperRank(rank: str) -> bool:
    configured = getattr(config, "anrdDeveloperUnlimitedRanks", []) or []
    allowed = {_normalize(item) for item in configured}
    return _normalize(rank) in allowed


def _updateContributorLimitStatus(section: dict[str, Any]) -> None:
    subHeaderRow = int(section.get("subHeaderRow") or 0)
    dataStartRow = int(section.get("dataStartRow") or 0)
    dataEndRow = int(section.get("dataEndRow") or 0)
    if subHeaderRow <= 0 or dataStartRow <= 0 or dataEndRow < dataStartRow:
        return
    monthlyCap = int(getattr(config, "anrdContributorMonthlyCap", 2000) or 2000)
    formula = f"=IF(SUM(D{dataStartRow}:D{dataEndRow})>={monthlyCap},TRUE,FALSE)"
    _engine.batchUpdateValues(
        _anrdSheetKey,
        [
            {"range": _range("G", subHeaderRow), "values": [[formula]]},
        ],
    )


def applyApprovedPaymentRequest(robloxUsername: str, approvedAmount: int) -> dict[str, Any]:
    username = str(robloxUsername or "").strip()
    amount = int(approvedAmount or 0)
    if not username:
        return {"ok": False, "reason": "missing-username"}
    if amount <= 0:
        return {"ok": False, "reason": "invalid-amount"}

    rank, rankRow = _findMemberRank(username)
    if not rank:
        return {"ok": False, "reason": "rank-not-found"}

    rankNorm = _normalize(rank)
    if _isContributorRank(rank):
        sectionName = "Contributor Payment"
    elif _isDeveloperRank(rank):
        sectionName = "Developer Payment"
    else:
        return {"ok": False, "reason": f"rank-not-eligible:{rankNorm}"}

    section = _findSection(sectionName)
    if not section:
        return {"ok": False, "reason": f"section-not-found:{_normalize(sectionName)}"}

    row, section, createdRow = _findOrCreateSectionRow(section, username)
    if row is None:
        return {"ok": False, "reason": "row-upsert-failed"}

    rowValues = _engine.getValues(
        _anrdSheetKey,
        _rangeRow("C", "D", row),
    )
    existingAmount = _toInt(rowValues[0][0], 0) if rowValues and len(rowValues[0]) > 0 else 0
    existingPaid = _toInt(rowValues[0][1], 0) if rowValues and len(rowValues[0]) > 1 else 0
    newAmount = existingAmount + amount

    updates: list[dict[str, Any]] = [
        {"range": _range("A", row), "values": [[username]]},
        {"range": _range("B", row), "values": [[str(rank).strip()]]},
        {"range": _range("C", row), "values": [[newAmount]]},
        {"range": _range("E", row), "values": [[f"=C{row}-D{row}"]]},
    ]
    if existingPaid <= 0:
        updates.append({"range": _range("D", row), "values": [[0]]})

    if _normalize(sectionName) == _normalize("Developer Payment"):
        if _isUnlimitedDeveloperRank(rank):
            updates.append({"range": _range("F", row), "values": [["N/A"]]})
            updates.append({"range": _range("G", row), "values": [["FALSE"]]})
            capReached = False
            capValue = None
        else:
            capValue = int(getattr(config, "anrdDeveloperMonthlyCap", 1200) or 1200)
            updates.append({"range": _range("F", row), "values": [[capValue]]})
            updates.append({"range": _range("G", row), "values": [[f"=IF(D{row}>=F{row},FALSE,TRUE)"]]})
            capReached = existingPaid >= capValue
    else:
        capValue = int(getattr(config, "anrdContributorMonthlyCap", 2000) or 2000)
        capReached = False

    _engine.batchUpdateValues(_anrdSheetKey, updates)
    _sortSectionRowsByUsername(section)

    if _normalize(sectionName) == _normalize("Contributor Payment"):
        _updateContributorLimitStatus(section)

    return {
        "ok": True,
        "section": sectionName,
        "row": row,
        "membersRankRow": rankRow,
        "rank": str(rank).strip(),
        "newAmount": newAmount,
        "amountPaid": existingPaid,
        "cap": capValue,
        "capReached": capReached,
        "rowCreated": createdRow,
    }


def touchupPaymentManagerSorting() -> dict[str, Any]:
    sections = ("Funding Sources", "Developer Payment", "Contributor Payment")
    touched = 0
    found = 0
    for sectionName in sections:
        section = _findSection(sectionName)
        if not section:
            continue
        found += 1
        if _sortSectionRowsByUsername(section):
            touched += 1
    return {
        "sectionsFound": found,
        "sectionsSorted": touched,
    }

