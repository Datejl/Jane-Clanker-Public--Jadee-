from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from features.staff.recruitment import sheets


class RecruitmentSheetWriterTests(unittest.TestCase):
    def test_build_all_time_write_value_extends_formula(self) -> None:
        self.assertEqual(
            sheets._buildAllTimeWriteValue("443", "=425+6+6+6", 6),
            "=(425+6+6+6)+6",
        )

    def test_build_all_time_write_value_skips_formula_when_delta_is_zero(self) -> None:
        self.assertIsNone(sheets._buildAllTimeWriteValue("443", "=425+6+6+6", 0))

    def test_build_approved_log_batch_data_writes_all_time_for_formula_rows(self) -> None:
        header = {
            "robloxUsername": "B",
            "rsRank": "C",
            "monthly": "D",
            "allTime": "E",
            "patrols": "F",
            "quota": "G",
        }
        rows = [23]
        currentByRow = {
            23: {
                "rsRank": "Recruitment Manager",
                "monthly": "30",
                "allTime": "443",
                "allTimeFormula": "=425+6+6+6",
                "patrols": "0",
                "quota": "Incomplete",
            }
        }
        updatesByRow = {
            23: {
                "robloxUsername": "JcJcJc_2012",
                "pointsDelta": 6,
                "patrolDelta": 1,
                "hostedPatrolDelta": 0,
            }
        }

        batchData, touchedRows = sheets._buildApprovedLogBatchData(
            header,
            rows,
            currentByRow,
            updatesByRow,
        )

        self.assertEqual(touchedRows, [23])
        self.assertTrue(
            any(
                item["range"].split("!")[-1] == "E23:E23"
                and item["values"] == [["=(425+6+6+6)+6"]]
                for item in batchData
            )
        )

    def test_load_header_map_uses_configured_profile_columns_without_sheet_scan(self) -> None:
        service = MagicMock()
        service.spreadsheets.side_effect = AssertionError("sheet scan should not be required")
        sheetConfig = SimpleNamespace(
            rowModel={
                "identity": {"robloxUserColumn": "B"},
                "pointColumns": {
                    "monthly": "D",
                    "allTime": "E",
                    "patrols": "F",
                },
                "profileColumns": {
                    "rank": "C",
                    "quota": "G",
                    "status": "H",
                    "loaExpiration": "I",
                    "notes": "J",
                },
            }
        )

        with patch.object(sheets._engine, "getSheetConfig", return_value=sheetConfig):
            header = sheets._loadHeaderMap(service)

        self.assertEqual(
            header,
            {
                "robloxUsername": "B",
                "monthly": "D",
                "allTime": "E",
                "patrols": "F",
                "rsRank": "C",
                "quota": "G",
                "status": "H",
                "loaExpiration": "I",
                "notes": "J",
            },
        )

    def test_find_row_by_roblox_username_reuses_cached_lookup(self) -> None:
        sheets._invalidateRowLookupCaches()
        valuesApi = MagicMock()
        valuesApi.batchGet.return_value.execute.return_value = {
            "valueRanges": [
                {"values": [[], ["Members"], ["AlphaUser"], ["BravoUser"]]},
                {"values": [[], [""], ["Recruiter"], ["Recruitment Manager"]]},
            ]
        }
        spreadsheetsApi = MagicMock()
        spreadsheetsApi.values.return_value = valuesApi
        service = MagicMock()
        service.spreadsheets.return_value = spreadsheetsApi

        with (
            patch.object(sheets, "_spreadsheetId", return_value="sheet-id"),
            patch.object(sheets, "_sheetName", return_value="ANRORS"),
            patch.object(sheets.config, "recruitmentRowLookupCacheTtlSec", 120, create=True),
        ):
            first = sheets._findRowByRobloxUsername(service, "B", "C", "AlphaUser")
            second = sheets._findRowByRobloxUsername(service, "B", "C", "AlphaUser")

        self.assertEqual(first, 3)
        self.assertEqual(second, 3)
        self.assertEqual(valuesApi.batchGet.call_count, 1)
        sheets._invalidateRowLookupCaches()

    def test_load_approved_log_current_rows_reads_compact_row_blocks(self) -> None:
        header = {
            "rsRank": "C",
            "monthly": "D",
            "allTime": "E",
            "patrols": "F",
            "quota": "G",
        }
        batchGetCalls = []

        def batchGet(**kwargs):
            batchGetCalls.append(kwargs)
            if kwargs.get("valueRenderOption") == "FORMULA":
                valueRanges = [
                    {"values": [["Recruiter", "1", "=5+1", "2", "Incomplete"], ["Recruiter", "3", "7", "4", "Complete"]]},
                    {"values": [["Manager", "8", "=10", "6", "Complete"]]},
                ]
            else:
                valueRanges = [
                    {"values": [["Recruiter", "1", "6", "2", "Incomplete"], ["Recruiter", "3", "7", "4", "Complete"]]},
                    {"values": [["Manager", "8", "10", "6", "Complete"]]},
                ]
            return SimpleNamespace(execute=lambda: {"valueRanges": valueRanges})

        valuesApi = MagicMock()
        valuesApi.batchGet.side_effect = batchGet
        spreadsheetsApi = MagicMock()
        spreadsheetsApi.values.return_value = valuesApi
        service = MagicMock()
        service.spreadsheets.return_value = spreadsheetsApi

        with patch.object(sheets, "_sheetName", return_value="ANRORS"):
            currentRows = sheets._loadApprovedLogCurrentRows(service, "sheet-id", header, [23, 24, 30])

        self.assertEqual(
            [call["ranges"] for call in batchGetCalls],
            [["ANRORS!C23:G24", "ANRORS!C30:G30"], ["ANRORS!C23:G24", "ANRORS!C30:G30"]],
        )
        self.assertEqual(currentRows[23]["allTime"], "6")
        self.assertEqual(currentRows[23]["allTimeFormula"], "=5+1")
        self.assertEqual(currentRows[24]["monthly"], "3")
        self.assertEqual(currentRows[30]["rsRank"], "Manager")
        self.assertEqual(currentRows[30]["allTimeFormula"], "=10")

    def test_row_selection_mask_only_marks_requested_rows(self) -> None:
        self.assertEqual(
            sheets._rowSelectionMask(10, 15, [15, 10, 15, 99]),
            ["1", "", "", "", "", "1"],
        )

    def test_resolve_approved_log_rows_refreshes_locations_after_insert(self) -> None:
        service = MagicMock()
        header = {"robloxUsername": "B", "rsRank": "C"}
        aggregate = {
            "newuser": {"robloxUsername": "NewUser", "pointsDelta": 1},
            "existinguser": {
                "robloxUsername": "ExistingUser",
                "pointsDelta": 2,
            },
        }

        with (
            patch.object(sheets, "_insertMissingMemberRow", return_value=10) as insertRow,
            patch.object(sheets, "_spreadsheetId", return_value="sheet-id"),
            patch.object(
                sheets,
                "_loadWritableMemberRowsByUsername",
                return_value={"newuser": 10, "existinguser": 31},
            ) as reloadRows,
        ):
            updatesByRow = sheets._resolveApprovedLogRows(
                service,
                header,
                aggregate,
                {"existinguser": 30},
            )

        insertRow.assert_called_once_with(service, header, "NewUser")
        reloadRows.assert_called_once_with(service, "sheet-id", header)
        self.assertEqual(sorted(updatesByRow), [10, 31])
        self.assertEqual(updatesByRow[31]["robloxUsername"], "ExistingUser")

    def test_apply_approved_logs_batch_scopes_cleanup_to_touched_rows(self) -> None:
        service = MagicMock()
        header = {
            "robloxUsername": "B",
            "rsRank": "C",
            "monthly": "D",
            "allTime": "E",
            "patrols": "F",
            "quota": "G",
        }
        updatesByRow = {
            10: {"robloxUsername": "Alpha", "pointsDelta": 1},
            30: {"robloxUsername": "Bravo", "pointsDelta": 1},
        }
        batchData = [
            {"range": "ANRORS!D10:D10", "values": [[1]]},
            {"range": "ANRORS!D30:D30", "values": [[1]]},
        ]

        with (
            patch.object(sheets, "_getService", return_value=service),
            patch.object(sheets, "_spreadsheetId", return_value="sheet-id"),
            patch.object(sheets, "_getSheetTabId", return_value=77),
            patch.object(sheets, "_loadHeaderMap", return_value=header),
            patch.object(
                sheets,
                "_aggregateApprovedLogUpdates",
                return_value={"alpha": {}, "bravo": {}},
            ),
            patch.object(sheets, "_loadWritableMemberRowsByUsername", return_value={}),
            patch.object(sheets, "_resolveApprovedLogRows", return_value=updatesByRow),
            patch.object(sheets, "_loadApprovedLogCurrentRows", return_value={}),
            patch.object(
                sheets,
                "_buildApprovedLogBatchData",
                return_value=(batchData, [10, 30]),
            ),
            patch.object(sheets, "_zeroFillColumnRange", return_value=("D", "F")),
            patch.object(sheets, "_fillEmptyCellsWithZero") as zeroFill,
            patch.object(sheets, "_applyRecruitmentRowsFormattingForRowSet") as formatRows,
            patch.object(sheets, "_applyRecruitmentRowsFormatting") as formatRange,
        ):
            result = sheets.applyApprovedLogsBatch(
                [{"robloxUsername": "Alpha", "pointsDelta": 1}],
                organizeAfter=False,
            )

        self.assertEqual(result["updatedRows"], 2)
        zeroFill.assert_called_once_with(
            service,
            10,
            30,
            "D",
            "F",
            rowUsernames=["1"] + ([""] * 19) + ["1"],
        )
        formatRows.assert_called_once_with(service, [10, 30], sheetId=77)
        formatRange.assert_not_called()


if __name__ == "__main__":
    unittest.main()
