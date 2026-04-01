from __future__ import annotations

import re
from typing import Any


def normalizeKey(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def sectionHeaderNames(layout: dict[str, Any]) -> list[str]:
    raw = layout.get("sectionHeaders") or []
    if isinstance(raw, list):
        out = [str(item).strip() for item in raw if str(item).strip()]
        if out:
            return out
    return []


_genericSectionHeaderTokens = {
    "personnel",
    "member",
    "members",
    "staff",
    "team",
    "section",
    "department",
    "dept",
    "employee",
    "employees",
    "rank",
    "ranks",
}


def _headerTokens(value: Any) -> list[str]:
    text = str(value or "").strip().lower()
    if not text:
        return []
    return [token for token in re.split(r"[^a-z0-9]+", text) if token]


def _tokensMatchWithGenericSuffix(base: list[str], candidate: list[str]) -> bool:
    if not base or len(candidate) < len(base):
        return False
    if candidate[: len(base)] != base:
        return False
    suffix = candidate[len(base) :]
    return bool(suffix) and all(token in _genericSectionHeaderTokens for token in suffix)


def isSectionHeaderTextMatch(expected: str, actual: str) -> bool:
    expectedNorm = normalizeKey(expected)
    actualNorm = normalizeKey(actual)
    if not expectedNorm or not actualNorm:
        return False
    if expectedNorm == actualNorm:
        return True

    expectedTokens = _headerTokens(expected)
    actualTokens = _headerTokens(actual)
    if not expectedTokens or not actualTokens:
        return False
    if expectedTokens == actualTokens:
        return True

    # Allow controlled compatibility for labels like:
    # - "Retired" <-> "Retired Personnel"
    # but avoid broad prefix matches like:
    # - "Probation" matching "Probationary Inspector".
    if _tokensMatchWithGenericSuffix(expectedTokens, actualTokens):
        return True
    if _tokensMatchWithGenericSuffix(actualTokens, expectedTokens):
        return True
    return False


def sectionHeaderAliasMap(layout: dict[str, Any], headerNames: list[str]) -> dict[str, list[str]]:
    aliasMap: dict[str, list[str]] = {name: [name] for name in headerNames}
    rawAliases = layout.get("sectionHeaderAliases") or {}
    if not isinstance(rawAliases, dict):
        return aliasMap

    normalizedToHeader = {normalizeKey(name): name for name in headerNames}
    for rawHeader, rawAliasValues in rawAliases.items():
        mappedHeader = normalizedToHeader.get(normalizeKey(rawHeader))
        if not mappedHeader:
            continue
        aliasValues: list[str] = []
        if isinstance(rawAliasValues, list):
            aliasValues = [str(item).strip() for item in rawAliasValues if str(item).strip()]
        elif str(rawAliasValues or "").strip():
            aliasValues = [str(rawAliasValues).strip()]
        for alias in aliasValues:
            if alias not in aliasMap[mappedHeader]:
                aliasMap[mappedHeader].append(alias)
    return aliasMap


def isSectionAwareLayout(layout: dict[str, Any]) -> bool:
    return len(sectionHeaderNames(layout)) > 0


def isManagedSection(layout: dict[str, Any], headerName: str) -> bool:
    raw = layout.get("managedSectionHeaders") or []
    if not isinstance(raw, list) or not raw:
        return True
    headerNorm = normalizeKey(headerName)
    if not headerNorm:
        return False
    for item in raw:
        itemNorm = normalizeKey(item)
        if not itemNorm:
            continue
        if headerNorm == itemNorm or headerNorm.startswith(itemNorm) or itemNorm.startswith(headerNorm):
            return True
    return False

