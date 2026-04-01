from __future__ import annotations


def normalizeColumn(col: str) -> str:
    return str(col or "").strip().upper()


def columnIndex(col: str) -> int:
    value = normalizeColumn(col)
    result = 0
    for ch in value:
        if not ("A" <= ch <= "Z"):
            continue
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result


def indexToColumn(index1: int) -> str:
    if index1 <= 0:
        return "A"
    out = ""
    index = int(index1)
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        out = chr(65 + remainder) + out
    return out


def cellRange(sheetName: str, col: str, row: int) -> str:
    colLetter = normalizeColumn(col)
    rowIndex = int(row)
    return f"{sheetName}!{colLetter}{rowIndex}:{colLetter}{rowIndex}"


def rowRange(sheetName: str, startCol: str, endCol: str, row: int) -> str:
    startLetter = normalizeColumn(startCol)
    endLetter = normalizeColumn(endCol)
    rowIndex = int(row)
    return f"{sheetName}!{startLetter}{rowIndex}:{endLetter}{rowIndex}"


def columnRange(sheetName: str, col: str, startRow: int, endRow: int) -> str:
    colLetter = normalizeColumn(col)
    startIndex = int(startRow)
    endIndex = int(endRow)
    return f"{sheetName}!{colLetter}{startIndex}:{colLetter}{endIndex}"
