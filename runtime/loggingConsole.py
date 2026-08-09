from __future__ import annotations

import logging
import os
import re
import sys
from typing import TextIO

_EXACT_LOGGER_LABELS = {
    "root": "jane",
    "runtime.entrypoint": "jane",
    "runtime.bootstrap": "bootstrap",
    "runtime.maintenance": "maintenance",
    "runtime.gamblingApi": "gambling-api",
    "features.staff.applications.cogMixins.configMixin": "applications",
    "features.staff.ribbons.cogMixin": "ribbons",
    "cogs.staff.divisionClockinCog": "division-clockin",
    "cogs.staff.anrdPaymentCog": "anrd-payment",
    "cogs.community.eventCog": "events",
    "discord.client": "discord",
    "discord.gateway": "discord",
    "hypercorn.error": "orientation-api",
    "hypercorn.access": "orientation-api",
}
_CAMEL_CASE_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")
_NOISY_MESSAGES = {
    "logging in using static token",
}
_LEVEL_LABELS = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARN",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "FATAL",
}
_LEVEL_MARKERS = {
    logging.DEBUG: "·",
    logging.INFO: "›",
    logging.WARNING: "!",
    logging.ERROR: "×",
    logging.CRITICAL: "×",
}
_ASCII_LEVEL_MARKERS = {
    logging.DEBUG: ".",
    logging.INFO: ">",
    logging.WARNING: "!",
    logging.ERROR: "x",
    logging.CRITICAL: "x",
}
_RESET = "\x1b[0m"
_DIM = "\x1b[2m"
_BOLD = "\x1b[1m"
_CYAN = "\x1b[36m"
_BRIGHT_CYAN = "\x1b[96m"
_YELLOW = "\x1b[33m"
_RED = "\x1b[31m"
_BRIGHT_RED = "\x1b[91m"
_GRAY = "\x1b[90m"
_BANNER_SHOWN = False


def _toKebab(value: str) -> str:
    text = str(value or "").replace("_", "-").strip("-")
    text = _CAMEL_CASE_PATTERN.sub("-", text)
    return text.lower()


def _shortLoggerName(name: str) -> str:
    normalized = str(name or "root").strip() or "root"
    exact = _EXACT_LOGGER_LABELS.get(normalized)
    if exact:
        return exact

    parts = [part for part in normalized.split(".") if part]
    if not parts:
        return "jane"

    leaf = parts[-1]
    for suffix in ("Cog", "Mixin"):
        if leaf.endswith(suffix):
            leaf = leaf[: -len(suffix)]

    if parts[0] in {"runtime", "cogs"}:
        return _toKebab(leaf or parts[-1])
    if parts[0] == "features" and len(parts) >= 2:
        feature = _toKebab(parts[1])
        leafLabel = _toKebab(leaf or parts[-1])
        if leafLabel in {"cog", "service", "views", feature}:
            return feature
        return f"{feature}:{leafLabel}"
    if parts[0] == "silly":
        return f"silly:{_toKebab(leaf or parts[-1])}"
    return _toKebab(leaf or parts[-1]) or "jane"


def _environmentColorMode() -> str:
    configured = str(os.getenv("JANE_CONSOLE_COLOR", "auto") or "auto").strip().lower()
    if configured in {"1", "always", "on", "true", "yes"}:
        return "always"
    if configured in {"0", "never", "off", "false", "no"}:
        return "never"
    return "auto"


def _supportsColor(stream: TextIO) -> bool:
    mode = _environmentColorMode()
    if mode == "always":
        return True
    if mode == "never" or os.getenv("NO_COLOR") is not None:
        return False
    if str(os.getenv("TERM", "") or "").strip().lower() == "dumb":
        return False
    isatty = getattr(stream, "isatty", None)
    return bool(callable(isatty) and isatty())


def _supportsBoxDrawing(stream: TextIO) -> bool:
    encoding = str(getattr(stream, "encoding", "") or "utf-8")
    try:
        "╭─╮│╰╯".encode(encoding)
    except (LookupError, UnicodeEncodeError):
        return False
    return True


def _levelStyle(levelNumber: int) -> str:
    if levelNumber >= logging.CRITICAL:
        return _BOLD + _BRIGHT_RED
    if levelNumber >= logging.ERROR:
        return _RED
    if levelNumber >= logging.WARNING:
        return _YELLOW
    if levelNumber >= logging.INFO:
        return _BRIGHT_CYAN
    return _GRAY


class ConsoleNoiseFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = str(record.getMessage() or "").strip().lower()
        return message not in _NOISY_MESSAGES


class JaneConsoleFormatter(logging.Formatter):
    default_time_format = "%H:%M:%S"
    default_msec_format = None

    def __init__(self, *, useColor: bool = False, useUnicode: bool = True) -> None:
        super().__init__()
        self.useColor = bool(useColor)
        self.useUnicode = bool(useUnicode)

    def _color(self, text: str, style: str) -> str:
        if not self.useColor:
            return text
        return f"{style}{text}{_RESET}"

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, self.datefmt)
        level = _LEVEL_LABELS.get(record.levelno, record.levelname.upper()[:5])
        markerMap = _LEVEL_MARKERS if self.useUnicode else _ASCII_LEVEL_MARKERS
        marker = markerMap.get(record.levelno, "·" if self.useUnicode else ".")
        divider = "│" if self.useUnicode else "|"
        component = _shortLoggerName(record.name)[:20]

        plainPrefix = f"{timestamp}  {marker} {level:<5} {component:<20} {divider} "
        coloredPrefix = (
            self._color(timestamp, _DIM)
            + "  "
            + self._color(f"{marker} {level:<5}", _levelStyle(record.levelno))
            + " "
            + self._color(f"{component:<20}", _CYAN)
            + " "
            + self._color(divider, _DIM)
            + " "
        )
        continuation = " " * (len(plainPrefix) - 2) + self._color(divider, _DIM) + " "

        message = str(record.getMessage() or "")
        if record.exc_info:
            exceptionText = self.formatException(record.exc_info)
            message = f"{message}\n{exceptionText}" if message else exceptionText
        elif record.stack_info:
            stackText = self.formatStack(record.stack_info)
            message = f"{message}\n{stackText}" if message else stackText

        lines = message.splitlines() or [""]
        rendered = coloredPrefix + lines[0]
        if len(lines) > 1:
            rendered += "\n" + "\n".join(continuation + line for line in lines[1:])
        return rendered


def _bannerLines(*, stream: TextIO) -> list[str]:
    title = "JANE CLANKER"
    if _supportsBoxDrawing(stream):
        tagline = "waking up the machinery — mind the loose bolts."
        details = f"pid {os.getpid()}  ·  Python {sys.version_info.major}.{sys.version_info.minor}"
        width = max(len(title), len(tagline), len(details)) + 6
        top = "╭" + ("─" * (width - 2)) + "╮"
        bottom = "╰" + ("─" * (width - 2)) + "╯"

        def _content(value: str) -> str:
            return "│  " + value.ljust(width - 6) + "  │"
    else:
        tagline = "waking up the machinery - mind the loose bolts."
        details = f"pid {os.getpid()}  |  Python {sys.version_info.major}.{sys.version_info.minor}"
        width = max(len(title), len(tagline), len(details)) + 6
        top = "+" + ("-" * (width - 2)) + "+"
        bottom = "+" + ("-" * (width - 2)) + "+"

        def _content(value: str) -> str:
            return "|  " + value.ljust(width - 6) + "  |"
    return [top, _content(title), _content(tagline), _content(details), bottom]


def showStartupBanner(*, stream: TextIO = sys.stdout, useColor: bool | None = None) -> None:
    global _BANNER_SHOWN
    if _BANNER_SHOWN:
        return
    _BANNER_SHOWN = True

    colorEnabled = _supportsColor(stream) if useColor is None else bool(useColor)
    lines = _bannerLines(stream=stream)
    if colorEnabled:
        lines[0] = f"{_DIM}{lines[0]}{_RESET}"
        lines[1] = f"{_BOLD}{_BRIGHT_CYAN}{lines[1]}{_RESET}"
        lines[2] = f"{_CYAN}{lines[2]}{_RESET}"
        lines[3] = f"{_DIM}{lines[3]}{_RESET}"
        lines[4] = f"{_DIM}{lines[4]}{_RESET}"
    stream.write("\n" + "\n".join(lines) + "\n\n")
    stream.flush()


def configureConsoleLogging(
    *,
    level: int = logging.INFO,
    stream: TextIO = sys.stdout,
    showBanner: bool = True,
) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    useColor = _supportsColor(stream)
    useUnicode = _supportsBoxDrawing(stream)
    if showBanner:
        showStartupBanner(stream=stream, useColor=useColor)

    handler = logging.StreamHandler(stream)
    handler.name = "jane-console"
    handler.setLevel(level)
    handler.setFormatter(
        JaneConsoleFormatter(useColor=useColor, useUnicode=useUnicode)
    )
    handler.addFilter(ConsoleNoiseFilter())
    root.addHandler(handler)

    for loggerName in ("discord", "discord.client", "discord.gateway"):
        logger = logging.getLogger(loggerName)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(level)
