from __future__ import annotations

import unittest
from datetime import datetime

from features.staff.cohost import selection


class CohostSelectionTests(unittest.TestCase):
    def test_select_prefers_configured_sro_role_then_history(self) -> None:
        volunteers = [
            selection.VolunteerCandidate(userId=101, rank="STA", joinedAt=datetime(2026, 5, 17, 10, 0, 0)),
            selection.VolunteerCandidate(userId=202, rank="SRO", joinedAt=datetime(2026, 5, 17, 10, 1, 0)),
            selection.VolunteerCandidate(userId=303, rank="SRO", joinedAt=datetime(2026, 5, 17, 10, 2, 0)),
        ]
        history = [
            selection.CohostHistoryEntry(userId=202, eventType="solo", selectedAt=datetime(2026, 5, 15)),
        ]

        selected = selection.selectCohosts("solo", volunteers, history, slots=2)

        self.assertEqual([entry.userId for entry in selected], ["303", "202"])

    def test_select_uses_sta_before_unranked(self) -> None:
        volunteers = [
            selection.VolunteerCandidate(userId=101, rank="", joinedAt=datetime(2026, 5, 17, 10, 0, 0)),
            selection.VolunteerCandidate(userId=202, rank="STA", joinedAt=datetime(2026, 5, 17, 10, 1, 0)),
        ]

        selected = selection.selectCohosts("grid", volunteers, [], slots=1)

        self.assertEqual([entry.userId for entry in selected], ["202"])


if __name__ == "__main__":
    unittest.main()
