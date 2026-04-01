from __future__ import annotations

import logging
import re
import sys

_EXACT_LOGGER_LABELS = {
    "root": "system",
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
}
_CAMEL_CASE_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")
_NOISY_MESSAGES = {
    "logging in using static token",
}


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
        return "system"

    leaf = parts[-1]
    for suffix in ("Cog", "Mixin"):
        if leaf.endswith(suffix):
            leaf = leaf[: -len(suffix)]

    if parts[0] == "runtime":
        return _toKebab(leaf or parts[-1])
    if parts[0] == "cogs":
        return _toKebab(leaf or parts[-1])
    if parts[0] == "features" and len(parts) >= 2:
        feature = _toKebab(parts[1])
        leafLabel = _toKebab(leaf or parts[-1])
        if leafLabel in {"cog", "service", "views", feature}:
            return feature
        return f"{feature}:{leafLabel}"
    if parts[0] == "silly":
        return f"silly:{_toKebab(leaf or parts[-1])}"
    return _toKebab(leaf or parts[-1]) or "system"


class ConsoleNoiseFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = str(record.getMessage() or "").strip().lower()
        return message not in _NOISY_MESSAGES


class JaneConsoleFormatter(logging.Formatter):
    default_time_format = "%H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, self.datefmt)
        level = record.levelname.upper()
        if level == "WARNING":
            level = "WARN"
        level = level[:5].ljust(5)
        component = _shortLoggerName(record.name)[:18].ljust(18)
        prefix = f"[{timestamp}] {level} {component}"
        message = str(record.getMessage() or "")
        if "\n" in message:
            message = message.replace("\n", "\n" + (" " * len(prefix)) + " | ")
        rendered = f"{prefix} | {message}"
        if record.exc_info:
            rendered = f"{rendered}\n{self.formatException(record.exc_info)}"
        elif record.stack_info:
            rendered = f"{rendered}\n{self.formatStack(record.stack_info)}"
        return rendered


def configureConsoleLogging(*, level: int = logging.INFO) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(JaneConsoleFormatter())
    handler.addFilter(ConsoleNoiseFilter())
    root.addHandler(handler)

    for loggerName in ("discord", "discord.client", "discord.gateway"):
        logger = logging.getLogger(loggerName)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(level)

