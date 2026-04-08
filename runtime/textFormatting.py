from __future__ import annotations

from typing import Iterable


def clipText(
    value: object,
    limit: int,
    *,
    suffix: str = "...",
    emptyText: str = "",
    strip: bool = False,
) -> str:
    text = str(value or "")
    if strip:
        text = text.strip()
    if not text:
        return str(emptyText or "")

    safeLimit = max(0, int(limit))
    if len(text) <= safeLimit:
        return text

    suffixText = str(suffix or "")
    if safeLimit <= len(suffixText):
        return suffixText[:safeLimit]
    return f"{text[: safeLimit - len(suffixText)]}{suffixText}"


def joinLinesAndClip(
    lines: Iterable[object],
    limit: int,
    *,
    separator: str = "\n",
    suffix: str = "...",
    emptyText: str = "",
    strip: bool = False,
) -> str:
    return clipText(
        separator.join(str(line or "") for line in lines),
        limit,
        suffix=suffix,
        emptyText=emptyText,
        strip=strip,
    )
