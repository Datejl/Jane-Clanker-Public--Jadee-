from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from db import sqlite as sqliteDb
from features.staff.bgItemReview import service as itemReviewService
from features.staff.bgItemReview import spreadsheetSync as itemReviewSpreadsheetSync
from features.staff.bgItemReview import workflow as itemReviewWorkflow


class BgItemReviewDisputeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tempDir = tempfile.TemporaryDirectory()
        self._originalDbPath = sqliteDb.dbPath
        await sqliteDb.closeDb()
        sqliteDb.dbPath = str(Path(self._tempDir.name) / "test.db")
        await sqliteDb.initDb()

    async def asyncTearDown(self) -> None:
        await sqliteDb.closeDb()
        sqliteDb.dbPath = self._originalDbPath
        self._tempDir.cleanup()

    async def test_bg_intel_dispute_creates_queue_entry_with_reference_context(self) -> None:
        async def fakeFetchCatalogAssetPrices(assetIds: list[int]):
            details = {
                200: {
                    "id": 200,
                    "name": "Disputed Shirt",
                    "price": 75,
                    "creatorId": 10,
                    "creatorName": "Maker",
                    "assetTypeName": "Shirt",
                },
                100: {
                    "id": 100,
                    "name": "Original Shirt",
                    "price": 60,
                    "creatorId": 11,
                    "creatorName": "Source Maker",
                    "assetTypeName": "Shirt",
                },
            }
            return ({assetId: details[assetId] for assetId in assetIds if assetId in details}, None)

        async def fakeValidateVisuals(assetIds: list[int]):
            rows = []
            for assetId in assetIds:
                rows.append(
                    {
                        "assetId": int(assetId),
                        "thumbnailUrl": f"https://cdn.example/{int(assetId)}.png",
                        "thumbnailState": "completed",
                        "thumbnailHash": f"hash-{int(assetId)}",
                        "validationState": "VALID",
                    }
                )
            return rows

        fakeChannel = SimpleNamespace(id=321)
        fakeSentMessage = SimpleNamespace(id=654)
        report = SimpleNamespace(
            discordUserId=44,
            robloxUserId=55,
            robloxUsername="TargetUser",
        )
        flaggedItem = {
            "id": 200,
            "name": "Disputed Shirt",
            "creatorId": 10,
            "creatorName": "Maker",
            "itemType": "Shirt",
            "matchType": "visual",
            "matchMode": "thumbnail_hash",
            "referenceItemId": 100,
        }

        with (
            patch.object(itemReviewWorkflow, "_queueEnabled", return_value=True),
            patch.object(itemReviewWorkflow, "_queueChannelId", return_value=321),
            patch.object(itemReviewWorkflow, "_resolveChannel", AsyncMock(return_value=fakeChannel)),
            patch.object(
                itemReviewWorkflow.runtimeWebhooks,
                "sendOwnedWebhookMessageDetailed",
                AsyncMock(return_value=fakeSentMessage),
            ),
            patch.object(
                itemReviewWorkflow.robloxAssets,
                "fetchCatalogAssetPrices",
                AsyncMock(side_effect=fakeFetchCatalogAssetPrices),
            ),
            patch.object(
                itemReviewWorkflow.robloxAssets,
                "validateRobloxAssetVisualReferences",
                AsyncMock(side_effect=fakeValidateVisuals),
            ),
        ):
            result = await itemReviewWorkflow.queueBgIntelDisputedItem(
                SimpleNamespace(),
                guildId=1,
                reviewerId=77,
                report=report,
                flaggedItem=flaggedItem,
                reportId=88,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["created"])
        queueId = int(result["queueId"])
        queueRow = await itemReviewService.getQueueEntry(queueId)
        self.assertIsNotNone(queueRow)
        self.assertEqual(queueRow["reviewChannelId"], 321)
        self.assertEqual(queueRow["reviewMessageId"], 654)

        context = json.loads(queueRow["contextJson"])
        self.assertEqual(context["kind"], "bg_intel_dispute")
        self.assertEqual(context["reportId"], 88)
        self.assertIn("Thumbnail similarity", context["flagBasis"])
        self.assertEqual(context["referenceItem"]["id"], 100)

        embed = await itemReviewWorkflow._buildQueueEmbed(dict(queueRow))
        fieldMap = {field.name: field.value for field in embed.fields}
        self.assertIn("Queue Context", fieldMap)
        self.assertIn("Flag Basis", fieldMap)
        self.assertIn("Matched Against", fieldMap)
        self.assertIn("Original Shirt", fieldMap["Matched Against"])
        self.assertIn("Disputed Shirt", fieldMap["Disputed Item"])
        self.assertEqual(embed.thumbnail.url, "https://cdn.example/100.png")
        self.assertEqual(embed.image.url, "https://cdn.example/200.png")

    async def test_update_queue_status_does_not_overwrite_final_decision(self) -> None:
        queueId = await itemReviewService.createQueueEntry(
            guildId=1,
            sessionId=0,
            assetId=12345,
            assetName="Test Item",
            itemType="Hat",
            creatorId=678,
            creatorName="Maker",
            priceRobux=10,
            thumbnailHash="hash",
            thumbnailUrl="https://cdn.example/item.png",
            thumbnailState="completed",
            sourceUserId=111,
            sourceRobloxUserId=222,
            sourceRobloxUsername="Target",
            queuedByReviewerId=333,
        )

        first = await itemReviewService.updateQueueStatus(
            queueId,
            status=itemReviewService.STATUS_SAFE,
            reviewerId=444,
        )
        second = await itemReviewService.updateQueueStatus(
            queueId,
            status=itemReviewService.STATUS_FLAGGED,
            reviewerId=555,
            note="late",
        )

        row = await itemReviewService.getQueueEntry(queueId)
        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(row["status"], itemReviewService.STATUS_SAFE)
        self.assertEqual(row["reviewedBy"], 444)

    async def test_rejected_attendee_inventory_review_is_removed(self) -> None:
        result = await itemReviewWorkflow.queueRejectedAttendeeInventory(
            SimpleNamespace(),
            session={"guildId": 1},
            attendee={"userId": 111, "robloxUsername": "Target"},
            reviewerId=222,
        )

        rows = await itemReviewService.listQueueCounts(guildId=1)

        self.assertEqual(result["created"], 0)
        self.assertEqual(result["existing"], 0)
        self.assertEqual(result["known"], 0)
        self.assertEqual(result["errors"], 0)
        self.assertIn("removed", str(result["reason"]).lower())
        self.assertEqual(rows["total"], 0)

    async def test_denied_spreadsheet_sync_is_removed(self) -> None:
        result = await itemReviewSpreadsheetSync.syncDeniedSpreadsheetRows(
            SimpleNamespace(),
            guildId=1,
            lookbackDays=5,
        )

        self.assertFalse(result["enabled"])
        self.assertEqual(result["files"], 0)
        self.assertEqual(result["created"], 0)
        self.assertIn("removed", str(result["reason"]).lower())


if __name__ == "__main__":
    unittest.main()
