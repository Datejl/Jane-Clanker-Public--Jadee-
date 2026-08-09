from __future__ import annotations

import http.client
import unittest
from unittest.mock import patch

from features.staff.orbat.multiEngine import MultiOrbatEngine
from features.staff.orbat.multiRegistry import MultiOrbatSheetConfig


class _FakeExecute:
    def __init__(self, result=None, exc: Exception | None = None) -> None:
        self._result = result
        self._exc = exc

    def execute(self):
        if self._exc is not None:
            raise self._exc
        return self._result


class _FakeValues:
    def __init__(self, result=None, exc: Exception | None = None) -> None:
        self._result = result
        self._exc = exc

    def get(self, **_kwargs):
        return _FakeExecute(result=self._result, exc=self._exc)


class _FakeSpreadsheets:
    def __init__(self, result=None, exc: Exception | None = None) -> None:
        self._values = _FakeValues(result=result, exc=exc)

    def values(self):
        return self._values


class _FakeService:
    def __init__(self, result=None, exc: Exception | None = None) -> None:
        self._spreadsheets = _FakeSpreadsheets(result=result, exc=exc)

    def spreadsheets(self):
        return self._spreadsheets


class MultiOrbatEngineRetryTests(unittest.TestCase):
    def test_get_values_rebuilds_service_after_transient_transport_error(self) -> None:
        sheet = MultiOrbatSheetConfig(
            key="test",
            displayName="Test",
            spreadsheetId="sheet-123",
            sheetName="Sheet1",
        )
        engine = MultiOrbatEngine(registry={"test": sheet})
        failingService = _FakeService(
            exc=ConnectionAbortedError(
                10053,
                "An established connection was aborted by the software in your host machine",
            )
        )
        healthyService = _FakeService(result={"values": [["ok"]]})
        services = [failingService, healthyService]

        with (
            patch.object(engine, "_throttle", return_value=None),
            patch("features.staff.orbat.multiEngine.time.sleep", return_value=None),
            patch.object(engine, "_getService", side_effect=lambda _sheet: services.pop(0)),
        ):
            values = engine.getValues("test", "Sheet1!A:A")

        self.assertEqual(values, [["ok"]])

    def test_get_values_retries_incomplete_read(self) -> None:
        sheet = MultiOrbatSheetConfig(
            key="test",
            displayName="Test",
            spreadsheetId="sheet-123",
            sheetName="Sheet1",
        )
        engine = MultiOrbatEngine(registry={"test": sheet})
        failingService = _FakeService(
            exc=http.client.IncompleteRead(b"partial", 10)
        )
        healthyService = _FakeService(result={"values": [["ok"]]})
        services = [failingService, healthyService]

        with (
            patch.object(engine, "_throttle", return_value=None),
            patch("features.staff.orbat.multiEngine.time.sleep", return_value=None),
            patch.object(engine, "_getService", side_effect=lambda _sheet: services.pop(0)),
        ):
            values = engine.getValues("test", "Sheet1!A:A")

        self.assertEqual(values, [["ok"]])


if __name__ == "__main__":
    unittest.main()
