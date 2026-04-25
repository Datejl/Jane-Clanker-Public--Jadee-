from __future__ import annotations

from collections.abc import Iterable


def toInt(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def toPositiveInt(value: object, default: int = 0) -> int:
    parsed = toInt(value, default)
    return parsed if parsed > 0 else int(default)


def _iterValues(values: object) -> Iterable[object]:
    if values is None:
        return ()
    if isinstance(values, (str, bytes)):
        return (values,)
    if isinstance(values, Iterable):
        return values
    return (values,)


def normalizeIntList(values: object, *, positiveOnly: bool = True) -> list[int]:
    out: list[int] = []
    for value in _iterValues(values):
        parsed = toInt(value, 0)
        if positiveOnly and parsed <= 0:
            continue
        if parsed not in out:
            out.append(parsed)
    return out


def normalizeIntSet(values: object, *, positiveOnly: bool = True) -> set[int]:
    return set(normalizeIntList(values, positiveOnly=positiveOnly))


def parseDiscordUserId(value: object) -> int:
    text = str(value or "").strip()
    if text.startswith("<@") and text.endswith(">"):
        text = text.removeprefix("<@").removeprefix("!").removesuffix(">")
    if not text.isdigit():
        return 0
    return toPositiveInt(text)


def commandParts(content: object) -> tuple[str, str]:
    stripped = str(content or "").strip()
    if not stripped:
        return "", ""
    parts = stripped.split(maxsplit=1)
    token = parts[0].lower()
    rest = parts[1].strip() if len(parts) > 1 else ""
    return token, rest


def tokenAt(content: object, index: int, default: str = "") -> str:
    if index < 0:
        return default
    parts = str(content or "").strip().split()
    if index >= len(parts):
        return default
    return parts[index].lower()
