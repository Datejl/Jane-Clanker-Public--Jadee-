from __future__ import annotations

import asyncio
import logging
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from discord.ext import commands

from runtime.errors import ErrorCoordinator, ErrorMirrorLogHandler


class ErrorCoordinatorPrefixTests(unittest.IsolatedAsyncioTestCase):
    def _coordinator(self) -> ErrorCoordinator:
        coordinator = ErrorCoordinator(
            botClient=SimpleNamespace(),
            configModule=SimpleNamespace(errorMirrorUserId=0),
            taskBudgeter=SimpleNamespace(),
        )
        coordinator.sendErrorMirrorDm = AsyncMock()  # type: ignore[method-assign]
        return coordinator

    async def test_prefix_check_failure_is_not_mirrored(self):
        coordinator = self._coordinator()

        await coordinator.handlePrefixCommandError(
            ctx=SimpleNamespace(),
            error=commands.CheckFailure("global check failed"),
        )

        coordinator.sendErrorMirrorDm.assert_not_awaited()

    async def test_prefix_command_not_found_is_not_mirrored(self):
        coordinator = self._coordinator()

        await coordinator.handlePrefixCommandError(
            ctx=SimpleNamespace(),
            error=commands.CommandNotFound("missing"),
        )

        coordinator.sendErrorMirrorDm.assert_not_awaited()


class ErrorMirrorLogHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def _drain_loop(self) -> None:
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    async def test_error_record_is_mirrored(self):
        coordinator = SimpleNamespace(sendLoggedErrorMirrorDm=AsyncMock())
        handler = ErrorMirrorLogHandler(
            coordinator=coordinator,  # type: ignore[arg-type]
            loop=asyncio.get_running_loop(),
        )
        record = logging.LogRecord(
            name="tests.error-mirror",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="failed %s",
            args=("thing",),
            exc_info=None,
            func=None,
            sinfo=None,
        )

        handler.emit(record)
        await self._drain_loop()

        coordinator.sendLoggedErrorMirrorDm.assert_awaited_once()
        kwargs = coordinator.sendLoggedErrorMirrorDm.await_args.kwargs
        self.assertEqual(kwargs["loggerName"], "tests.error-mirror")
        self.assertEqual(kwargs["levelName"], "ERROR")
        self.assertEqual(kwargs["message"], "failed thing")

    async def test_skipped_record_is_not_mirrored(self):
        coordinator = SimpleNamespace(sendLoggedErrorMirrorDm=AsyncMock())
        handler = ErrorMirrorLogHandler(
            coordinator=coordinator,  # type: ignore[arg-type]
            loop=asyncio.get_running_loop(),
        )
        record = logging.LogRecord(
            name="tests.error-mirror",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="skip me",
            args=(),
            exc_info=None,
            func=None,
            sinfo=None,
        )
        setattr(record, "skipErrorMirrorDm", True)

        handler.emit(record)
        await self._drain_loop()

        coordinator.sendLoggedErrorMirrorDm.assert_not_awaited()

    async def test_transient_non_google_record_is_not_mirrored(self):
        coordinator = SimpleNamespace(sendLoggedErrorMirrorDm=AsyncMock())
        handler = ErrorMirrorLogHandler(
            coordinator=coordinator,  # type: ignore[arg-type]
            loop=asyncio.get_running_loop(),
        )
        record = logging.LogRecord(
            name="tests.error-mirror",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="connection reset by peer",
            args=(),
            exc_info=None,
            func=None,
            sinfo=None,
        )

        handler.emit(record)
        await self._drain_loop()

        coordinator.sendLoggedErrorMirrorDm.assert_not_awaited()

    async def test_google_warning_record_is_mirrored(self):
        coordinator = SimpleNamespace(sendLoggedErrorMirrorDm=AsyncMock())
        handler = ErrorMirrorLogHandler(
            coordinator=coordinator,  # type: ignore[arg-type]
            loop=asyncio.get_running_loop(),
        )
        record = logging.LogRecord(
            name="features.staff.recruitment.outputs",
            level=logging.WARNING,
            pathname=__file__,
            lineno=1,
            msg="Google Sheets sync failed: quota exceeded",
            args=(),
            exc_info=None,
            func=None,
            sinfo=None,
        )

        handler.emit(record)
        await self._drain_loop()

        coordinator.sendLoggedErrorMirrorDm.assert_awaited_once()
        kwargs = coordinator.sendLoggedErrorMirrorDm.await_args.kwargs
        self.assertEqual(kwargs["title"], "Jane Google/Sheets Alert")
        self.assertEqual(kwargs["levelName"], "WARNING")

    async def test_google_transient_record_is_mirrored(self):
        coordinator = SimpleNamespace(sendLoggedErrorMirrorDm=AsyncMock())
        handler = ErrorMirrorLogHandler(
            coordinator=coordinator,  # type: ignore[arg-type]
            loop=asyncio.get_running_loop(),
        )
        try:
            raise RuntimeError("googleapiclient IncompleteRead: connection reset by peer")
        except RuntimeError as exc:
            excInfo = (type(exc), exc, exc.__traceback__)
        record = logging.LogRecord(
            name="features.staff.orbat.multiEngine",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="Google Sheets operation failed",
            args=(),
            exc_info=excInfo,
            func=None,
            sinfo=None,
        )

        handler.emit(record)
        await self._drain_loop()

        coordinator.sendLoggedErrorMirrorDm.assert_awaited_once()
        kwargs = coordinator.sendLoggedErrorMirrorDm.await_args.kwargs
        self.assertEqual(kwargs["title"], "Jane Google/Sheets Alert")


if __name__ == "__main__":
    unittest.main()
