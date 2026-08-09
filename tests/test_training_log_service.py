from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from features.staff.trainingLog import trainingLogService


class TrainingLogServiceSqlTests(unittest.IsolatedAsyncioTestCase):
    def _coordinator(self) -> trainingLogService.TrainingLogCoordinator:
        config = SimpleNamespace(
            organizationProfiles={
                "ANRO": {
                    "primaryGuildId": 10,
                    "guildIds": [10, 20],
                    "trainingResultsChannelId": 111,
                }
            }
        )
        return trainingLogService.TrainingLogCoordinator(
            botClient=SimpleNamespace(),
            configModule=config,
            taskBudgeter=SimpleNamespace(),
            recruitmentService=SimpleNamespace(),
            webhookModule=SimpleNamespace(),
        )

    async def test_export_listing_pushes_org_status_and_limit_into_sql(self) -> None:
        coordinator = self._coordinator()
        captured: dict[str, object] = {}

        async def fakeFetchAll(query: str, params: tuple = ()) -> list[dict]:
            captured["query"] = query
            captured["params"] = params
            return []

        with patch.object(trainingLogService, "fetchAll", fakeFetchAll):
            rows = await coordinator.listTrainingExportRows(orgKey="ANRO", onlyReady=True, limit=5)

        self.assertEqual(rows, [])
        query = str(captured["query"])
        self.assertIn("logs.sourceChannelId = ?", query)
        self.assertIn("logs.sourceGuildId IN (?,?)", query)
        self.assertIn("UPPER(COALESCE(exports.exportStatus, 'PENDING')) = 'READY'", query)
        self.assertIn("LIMIT ?", query)
        self.assertEqual(captured["params"], (111, 10, 20, 5))


if __name__ == "__main__":
    unittest.main()
