from __future__ import annotations

import unittest

from features.staff.sessions import bgSpreadsheetQueue, bgSpreadsheetRouting


class BgSpreadsheetRoutingTests(unittest.TestCase):
    def test_clean_orientation_host_name_strips_prefix_and_optional_suffix(self) -> None:
        self.assertEqual(
            bgSpreadsheetRouting._cleanOrientationHostName("[HOST] potater [LOA]"),
            "potater",
        )
        self.assertEqual(
            bgSpreadsheetRouting._cleanOrientationHostName("[HOST] potater"),
            "potater",
        )

    def test_orientation_spreadsheet_date_uses_sheet_title_when_available(self) -> None:
        result = bgSpreadsheetQueue.BgSpreadsheetResult(title="Orientation 2026-05-05")

        self.assertEqual(
            bgSpreadsheetRouting._orientationSpreadsheetDateText(result),
            "5/5/2026",
        )


if __name__ == "__main__":
    unittest.main()
