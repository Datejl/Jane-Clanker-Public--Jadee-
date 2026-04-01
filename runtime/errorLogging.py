from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

_HANDLER_NAME = "jane-general-errors"
_DEFAULT_LOG_DIRNAME = "logs"
_DEFAULT_LOG_FILENAME = "general-errors.log"
_DEFAULT_MAX_BYTES = 2 * 1024 * 1024
_DEFAULT_BACKUP_COUNT = 5
_EXCEPTION_HOOK_INSTALLED = False


def _repoRoot() -> Path:
    return Path(__file__).resolve().parent.parent


def _configuredLogDir(configModule: Any) -> Path:
    configured = str(getattr(configModule, "generalErrorLogDir", "") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return _repoRoot() / _DEFAULT_LOG_DIRNAME


def generalErrorLogPath(configModule: Any) -> Path:
    return _configuredLogDir(configModule) / _DEFAULT_LOG_FILENAME


class GeneralErrorFormatter(logging.Formatter):
    default_time_format = "%Y-%m-%d %H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, self.datefmt)
        level = record.levelname.upper()
        if level == "WARNING":
            level = "WARN"
        message = str(record.getMessage() or "")
        header = f"[{timestamp}] {level:<5} {record.name}"
        rendered = f"{header}\n{message}"
        if record.exc_info:
            rendered = f"{rendered}\n{self.formatException(record.exc_info)}"
        elif record.stack_info:
            rendered = f"{rendered}\n{self.formatStack(record.stack_info)}"
        return f"{rendered}\n{'-' * 90}"


def configureGeneralErrorLogging(*, configModule: Any, level: int = logging.WARNING) -> Path:
    root = logging.getLogger()
    for handler in root.handlers:
        if getattr(handler, "name", "") == _HANDLER_NAME:
            existingPath = getattr(handler, "baseFilename", "")
            return Path(existingPath) if existingPath else generalErrorLogPath(configModule)

    logPath = generalErrorLogPath(configModule)
    logPath.parent.mkdir(parents=True, exist_ok=True)

    maxBytes = int(getattr(configModule, "generalErrorLogMaxBytes", _DEFAULT_MAX_BYTES) or _DEFAULT_MAX_BYTES)
    backupCount = int(
        getattr(configModule, "generalErrorLogBackupCount", _DEFAULT_BACKUP_COUNT) or _DEFAULT_BACKUP_COUNT
    )
    handler = RotatingFileHandler(
        filename=logPath,
        maxBytes=max(1024, maxBytes),
        backupCount=max(1, backupCount),
        encoding="utf-8",
    )
    handler.name = _HANDLER_NAME
    handler.setLevel(level)
    handler.setFormatter(GeneralErrorFormatter())
    root.addHandler(handler)
    logging.captureWarnings(True)
    return logPath


def installGlobalExceptionHooks() -> None:
    global _EXCEPTION_HOOK_INSTALLED
    if _EXCEPTION_HOOK_INSTALLED:
        return
    _EXCEPTION_HOOK_INSTALLED = True

    def _logUnhandled(excType: type[BaseException], excValue: BaseException, excTraceback: Any) -> None:
        if issubclass(excType, KeyboardInterrupt):
            return
        logging.getLogger("runtime.general-errors").error(
            "Unhandled exception reached sys.excepthook.",
            exc_info=(excType, excValue, excTraceback),
        )

    def _logThreadException(args: threading.ExceptHookArgs) -> None:
        logging.getLogger("runtime.general-errors").error(
            "Unhandled thread exception (thread=%s).",
            getattr(getattr(args, "thread", None), "name", "unknown"),
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    def _logUnraisable(args: Any) -> None:
        logging.getLogger("runtime.general-errors").error(
            "Unraisable exception encountered (object=%r).",
            getattr(args, "object", None),
            exc_info=(
                getattr(args, "exc_type", Exception),
                getattr(args, "exc_value", Exception("unraisable exception")),
                getattr(args, "exc_traceback", None),
            ),
        )

    sys.excepthook = _logUnhandled
    if hasattr(threading, "excepthook"):
        threading.excepthook = _logThreadException
    if hasattr(sys, "unraisablehook"):
        sys.unraisablehook = _logUnraisable


def installAsyncioExceptionLogging(loop: asyncio.AbstractEventLoop) -> None:
    if getattr(loop, "_jane_general_error_handler_installed", False):
        return

    previousHandler = loop.get_exception_handler()

    def _handleAsyncioException(activeLoop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        exception = context.get("exception")
        message = str(context.get("message") or "Unhandled asyncio exception.")
        task = context.get("task")
        future = context.get("future")
        transport = context.get("transport")

        details: list[str] = [message]
        if task is not None:
            details.append(f"task={task!r}")
        if future is not None and future is not task:
            details.append(f"future={future!r}")
        if transport is not None:
            details.append(f"transport={transport!r}")

        logging.getLogger("runtime.general-errors").error(
            "Asyncio loop exception: %s",
            " | ".join(details),
            exc_info=(type(exception), exception, exception.__traceback__) if exception is not None else None,
        )

        if previousHandler is not None:
            previousHandler(activeLoop, context)
            return
        activeLoop.default_exception_handler(context)

    loop.set_exception_handler(_handleAsyncioException)
    setattr(loop, "_jane_general_error_handler_installed", True)


def currentProcessLogSummary(*, configModule: Any) -> str:
    path = generalErrorLogPath(configModule)
    return os.fspath(path)
