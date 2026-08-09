from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from features.staff.bgItemReview import workflow as itemReviewWorkflow


class BgItemReviewRestoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_restore_persistent_views_is_disabled_for_old_item_review_messages(self) -> None:
        bot = SimpleNamespace(add_view=Mock())

        with (
            patch.object(
                itemReviewWorkflow.service,
                "listOpenQueueEntries",
                AsyncMock(return_value=[]),
            ) as listEntries,
            patch.object(itemReviewWorkflow, "refreshQueueMessage", AsyncMock()) as refreshMessage,
        ):
            result = await itemReviewWorkflow.restorePersistentViews(bot)

        listEntries.assert_not_awaited()
        refreshMessage.assert_not_awaited()
        bot.add_view.assert_not_called()
        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()
