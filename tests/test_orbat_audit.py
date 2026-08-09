from __future__ import annotations

import unittest

from runtime import orbatAudit


class OrbatAuditTests(unittest.TestCase):
    def test_build_role_sync_change_log_for_recruitment_sync(self) -> None:
        payload = orbatAudit.buildRoleSyncChangeLog(
            memberMention="<@123>",
            result={
                "syncType": "recruitment.anrorsPlacement",
                "robloxUsername": "potatergaming",
                "created": True,
                "moved": True,
                "updated": False,
                "section": "Managers",
                "hasAnrorsMemberRole": True,
                "hasAnrorsRmPlusRole": True,
            },
        )

        self.assertIsNotNone(payload)
        self.assertEqual(payload["sheetKey"], "recruitment")
        self.assertEqual(payload["requestedBy"], "automatic role sync")
        self.assertEqual(payload["authorizedBy"], "Discord role sync")
        self.assertEqual(payload["change"], "Updated Recruitment ORBAT from Discord role sync.")
        self.assertIn("User: <@123>", payload["details"])
        self.assertIn("Roblox: potatergaming", payload["details"])
        self.assertIn("created row", payload["details"])
        self.assertIn("moved row", payload["details"])

    def test_build_role_sync_change_log_for_department_sync(self) -> None:
        payload = orbatAudit.buildRoleSyncChangeLog(
            memberMention="<@456>",
            result={
                "syncType": "department.anrdRankByRole",
                "divisionKey": "ANRD",
                "sheetKey": "dept_anrd",
                "robloxUsername": "builderman",
                "created": False,
                "rankUpdated": True,
                "previousRank": "Contributor",
                "targetRank": "Senior Developer",
                "matchedRoleId": 789,
            },
        )

        self.assertIsNotNone(payload)
        self.assertEqual(payload["sheetKey"], "dept_anrd")
        self.assertEqual(payload["divisionKey"], "ANRD")
        self.assertEqual(payload["requestedBy"], "automatic role sync")
        self.assertEqual(payload["change"], "Updated ANRD Department ORBAT from Discord role sync.")
        self.assertIn("Previous rank: Contributor", payload["details"])
        self.assertIn("Target rank: Senior Developer", payload["details"])
        self.assertIn("<@&789>", payload["details"])

    def test_build_discord_message_url(self) -> None:
        url = orbatAudit.buildDiscordMessageUrl(1, 2, 3)
        self.assertEqual(url, "https://discord.com/channels/1/2/3")
        self.assertEqual(orbatAudit.buildDiscordMessageUrl(0, 2, 3), "")

    def test_build_role_sync_change_log_returns_none_when_nothing_changed(self) -> None:
        payload = orbatAudit.buildRoleSyncChangeLog(
            memberMention="<@999>",
            result={
                "syncType": "recruitment.anrorsPlacement",
                "robloxUsername": "quietuser",
                "created": False,
                "moved": False,
                "updated": False,
            },
        )

        self.assertIsNone(payload)


if __name__ == "__main__":
    unittest.main()
