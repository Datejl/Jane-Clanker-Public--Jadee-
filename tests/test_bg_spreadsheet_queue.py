from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from features.staff.sessions import bgSpreadsheetQueue
from features.staff.sessions.Roblox.robloxModels import RoverLookupResult


class BgSpreadsheetQueueTests(unittest.IsolatedAsyncioTestCase):
    def test_delete_extra_rows_request_keeps_header_and_written_rows(self) -> None:
        request = bgSpreadsheetQueue._deleteExtraRowsRequest(
            sheetId=0,
            existingRowCount=100,
            desiredRowCount=4,
        )

        self.assertEqual(
            request,
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": 0,
                        "dimension": "ROWS",
                        "startIndex": 4,
                        "endIndex": 100,
                    }
                }
            },
        )

    def test_delete_extra_rows_request_skips_when_no_excess_rows_exist(self) -> None:
        self.assertIsNone(
            bgSpreadsheetQueue._deleteExtraRowsRequest(
                sheetId=0,
                existingRowCount=4,
                desiredRowCount=4,
            )
        )

    def test_sheet_values_use_human_inventory_labels(self) -> None:
        row = bgSpreadsheetQueue.BgSpreadsheetRow(
            discord_id=123,
            roblox_user="SomeUser",
            inventory=bgSpreadsheetQueue.inventoryLabelPrivate,
        )

        self.assertEqual(row.sheet_values(), ["123", "SomeUser", "Private"])

    def test_setup_requests_add_jane_intel_column_when_missing(self) -> None:
        requests = bgSpreadsheetQueue._bgSpreadsheetSetupRequests(
            sheet={
                "properties": {
                    "sheetId": 7,
                    "title": "Sheet1",
                    "gridProperties": {"rowCount": 10, "columnCount": 11},
                }
            },
            rowCount=10,
            columnCount=11,
        )

        self.assertIn(
            {
                "appendDimension": {
                    "sheetId": 7,
                    "dimension": "COLUMNS",
                    "length": 1,
                }
            },
            requests,
        )
        self.assertTrue(
            any(
                request.get("setDataValidation", {}).get("range", {}).get("startColumnIndex") == 11
                for request in requests
            )
        )

    def test_setup_requests_preserve_table_dropdown_metadata(self) -> None:
        requests = bgSpreadsheetQueue._bgSpreadsheetSetupRequests(
            sheet={
                "properties": {
                    "sheetId": 7,
                    "title": "Sheet1",
                    "gridProperties": {"rowCount": 1000, "columnCount": 12},
                },
                "tables": [
                    {
                        "tableId": "table-1",
                        "range": {
                            "startRowIndex": 0,
                            "endRowIndex": 91,
                            "startColumnIndex": 0,
                            "endColumnIndex": 12,
                        },
                        "columnProperties": [{"columnIndex": 11, "columnName": "Jane Intel"}],
                    }
                ],
            },
            rowCount=91,
            columnCount=12,
        )

        self.assertFalse(any("setDataValidation" in request for request in requests))
        tableRequest = next(request["updateTable"] for request in requests if "updateTable" in request)
        self.assertEqual(tableRequest["fields"], "range")
        self.assertEqual(tableRequest["table"]["range"]["endRowIndex"], 91)
        self.assertNotIn("columnProperties", tableRequest["table"])

    def test_setup_requests_delete_ticketed_no_highlight(self) -> None:
        requests = bgSpreadsheetQueue._bgSpreadsheetSetupRequests(
            sheet={
                "properties": {
                    "sheetId": 7,
                    "title": "Sheet1",
                    "gridProperties": {"rowCount": 10, "columnCount": 12},
                },
                "conditionalFormats": [
                    {
                        "ranges": [{"startColumnIndex": 8, "endColumnIndex": 9}],
                        "booleanRule": {
                            "condition": {
                                "type": "TEXT_EQ",
                                "values": [{"userEnteredValue": "No"}],
                            }
                        },
                    }
                ],
            },
            rowCount=10,
            columnCount=12,
        )

        self.assertIn(
            {"deleteConditionalFormatRule": {"sheetId": 7, "index": 0}},
            requests,
        )

    def test_bg_intel_sheet_cell_value_links_to_report(self) -> None:
        value = bgSpreadsheetQueue._bgIntelSheetCellValue(
            status="Needs review",
            messageUrl="https://discord.com/channels/1/2/3",
        )

        self.assertEqual(
            value,
            '=HYPERLINK("https://discord.com/channels/1/2/3","Needs review")',
        )

    def test_bg_intel_sheet_status_prioritizes_manual_review(self) -> None:
        status = bgSpreadsheetQueue._bgIntelSheetStatus(
            SimpleNamespace(robloxUserId=123, inventoryScanStatus="PRIVATE"),
            SimpleNamespace(score=40, scored=True, outcome="scored"),
        )

        self.assertEqual(status, "Needs review")

    def test_bg_intel_sheet_status_handles_private_inventory(self) -> None:
        status = bgSpreadsheetQueue._bgIntelSheetStatus(
            SimpleNamespace(robloxUserId=123, inventoryScanStatus="PRIVATE"),
            SimpleNamespace(score=5, scored=True, outcome="scored"),
        )

        self.assertEqual(status, "Private inventory")

    def test_bg_intel_sheet_status_handles_missing_identity(self) -> None:
        status = bgSpreadsheetQueue._bgIntelSheetStatus(
            SimpleNamespace(robloxUserId=None, inventoryScanStatus="SKIPPED"),
            SimpleNamespace(score=0, scored=False, outcome="needs_identity"),
        )

        self.assertEqual(status, "Missing identity")

    def test_sheet_properties_falls_back_to_first_tab_for_default_sheet_name(self) -> None:
        sheetId, sheetName, rowCount = bgSpreadsheetQueue._sheetPropertiesFromMetadata(
            {
                "sheets": [
                    {
                        "properties": {
                            "sheetId": 0,
                            "title": "BGC Template",
                            "gridProperties": {"rowCount": 250},
                        }
                    }
                ]
            },
            "Sheet1",
        )

        self.assertEqual(sheetId, 0)
        self.assertEqual(sheetName, "BGC Template")
        self.assertEqual(rowCount, 250)

    def test_sheet_properties_rejects_missing_explicit_tab(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Missing Tab"):
            bgSpreadsheetQueue._sheetPropertiesFromMetadata(
                {
                    "sheets": [
                        {
                            "properties": {
                                "sheetId": 0,
                                "title": "BGC Template",
                                "gridProperties": {"rowCount": 250},
                            }
                        }
                    ]
                },
                "Missing Tab",
            )

    async def test_send_bg_spreadsheet_change_log_uses_generic_audit_payload(self) -> None:
        result = bgSpreadsheetQueue.BgSpreadsheetResult(
            spreadsheet_id="sheet-123",
            title="Orientation 2026-05-08",
            sheet_name="Sheet1",
            rows=[
                bgSpreadsheetQueue.BgSpreadsheetRow(discord_id=1, roblox_user="alpha", inventory="private"),
                bgSpreadsheetQueue.BgSpreadsheetRow(discord_id=2, roblox_user="bravo", inventory="public"),
            ],
        )

        with patch.object(
            bgSpreadsheetQueue.orbatAuditRuntime,
            "sendOrbatChangeLog",
            AsyncMock(),
        ) as auditMock:
            await bgSpreadsheetQueue.sendBgSpreadsheetChangeLog(
                SimpleNamespace(),
                result=result,
                change="Created orientation BGC spreadsheet.",
                authorizedBy="<@99>",
                requestedBy="<@44>",
                requestMessageUrl="https://discord.com/channels/1/2/3",
                details="Session: 42",
            )

        auditMock.assert_awaited_once()
        kwargs = auditMock.await_args.kwargs
        self.assertEqual(kwargs["title"], "Spreadsheet Change")
        self.assertEqual(kwargs["spreadsheetId"], "sheet-123")
        self.assertEqual(kwargs["sheetName"], "Sheet1")
        self.assertEqual(kwargs["label"], "Orientation 2026-05-08")
        self.assertEqual(kwargs["requestedBy"], "<@44>")
        self.assertEqual(kwargs["requestMessageUrl"], "https://discord.com/channels/1/2/3")
        self.assertIn("Rows: 2", kwargs["details"])
        self.assertIn("Inventory private: 1", kwargs["details"])
        self.assertIn("Session: 42", kwargs["details"])

    async def test_build_rows_for_attendees_uses_stored_roblox_username(self) -> None:
        with (
            patch.object(bgSpreadsheetQueue.robloxUsers, "fetchRobloxUser", AsyncMock()) as roverMock,
            patch.object(bgSpreadsheetQueue.robloxInventory, "fetchRobloxInventory", AsyncMock(return_value=SimpleNamespace(error=None, status=200))),
        ):
            rows = await bgSpreadsheetQueue.buildRowsForAttendees(
                [
                    {
                        "userId": 10,
                        "robloxUserId": 123,
                        "robloxUsername": "StoredUser",
                    }
                ],
                sourceGuild=SimpleNamespace(id=999),
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].discord_id, 10)
        self.assertEqual(rows[0].roblox_user, "StoredUser")
        self.assertEqual(rows[0].inventory, bgSpreadsheetQueue.inventoryLabelPublic)
        roverMock.assert_not_awaited()

    async def test_build_rows_for_user_ids_falls_back_to_configured_rover_guild(self) -> None:
        async def roverSideEffect(discordId: int, guildId: int | None = None):
            if guildId == 999:
                return RoverLookupResult(None, None, error="No Roblox account linked via RoVer.")
            return RoverLookupResult(321, "FallbackUser")

        with (
            patch.object(bgSpreadsheetQueue.robloxUsers, "getStoredRobloxIdentity", AsyncMock(return_value=None)),
            patch.object(bgSpreadsheetQueue.config, "serverId", 111),
            patch.object(bgSpreadsheetQueue.robloxUsers, "fetchRobloxUser", AsyncMock(side_effect=roverSideEffect)),
            patch.object(bgSpreadsheetQueue.robloxInventory, "fetchRobloxInventory", AsyncMock(return_value=SimpleNamespace(error=None, status=200))),
        ):
            rows = await bgSpreadsheetQueue.buildRowsForUserIds(
                [10],
                sourceGuild=SimpleNamespace(id=999),
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].roblox_user, "FallbackUser")

    async def test_build_rows_for_user_ids_uses_stored_identity_before_rover(self) -> None:
        with (
            patch.object(
                bgSpreadsheetQueue.robloxUsers,
                "getStoredRobloxIdentity",
                AsyncMock(return_value=RoverLookupResult(654, "StoredLink")),
            ) as storedMock,
            patch.object(bgSpreadsheetQueue.robloxUsers, "fetchRobloxUser", AsyncMock()) as roverMock,
            patch.object(bgSpreadsheetQueue.robloxInventory, "fetchRobloxInventory", AsyncMock(return_value=SimpleNamespace(error=None, status=200))),
        ):
            rows = await bgSpreadsheetQueue.buildRowsForUserIds(
                [10],
                sourceGuild=SimpleNamespace(id=999),
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].roblox_user, "StoredLink")
        storedMock.assert_awaited_once_with(10)
        roverMock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
