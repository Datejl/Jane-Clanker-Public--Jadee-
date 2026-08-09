from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from runtime.webhookHealth import WebhookHealthWatcher


class WebhookHealthWatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_check_cleans_missing_hub_and_event_rows(self) -> None:
        auditStream = SimpleNamespace(logEvent=AsyncMock())
        watcher = WebhookHealthWatcher(
            botClient=SimpleNamespace(get_channel=lambda _channelId: None),
            taskBudgeter=SimpleNamespace(runDiscord=AsyncMock()),
            auditStream=auditStream,
            checkIntervalSec=600,
        )

        hubRows = [
            {
                "messageId": 111,
                "guildId": 222,
                "channelId": 333,
                "divisionKey": "niru",
            }
        ]
        eventRows = [
            {
                "eventId": 444,
                "guildId": 555,
                "channelId": 666,
                "messageId": 777,
                "title": "test",
            }
        ]

        with (
            patch("runtime.webhookHealth.fetchAll", AsyncMock(side_effect=[hubRows, eventRows])),
            patch.object(watcher, "_messageState", AsyncMock(side_effect=["missing", "missing"])),
            patch(
                "runtime.webhookHealth.applicationsService.deleteHubMessage",
                AsyncMock(),
            ) as deleteHubMessageMock,
            patch(
                "runtime.webhookHealth.eventService.markScheduledEventDeleted",
                AsyncMock(),
            ) as deleteEventMock,
        ):
            summary = await watcher.runCheck()

        self.assertEqual(summary, {"checked": 2, "missing": 2, "errors": 0})
        deleteHubMessageMock.assert_awaited_once_with(111)
        deleteEventMock.assert_awaited_once_with(444)
        self.assertEqual(auditStream.logEvent.await_count, 2)

        firstCall = auditStream.logEvent.await_args_list[0].kwargs
        secondCall = auditStream.logEvent.await_args_list[1].kwargs
        firstDetails = firstCall["details"]
        secondDetails = secondCall["details"]
        self.assertTrue(firstDetails["cleanupApplied"])
        self.assertTrue(secondDetails["cleanupApplied"])
        self.assertEqual(firstCall["severity"], "INFO")
        self.assertEqual(secondCall["severity"], "INFO")
        self.assertEqual(firstCall["action"], "Application hub message reference cleaned")
        self.assertEqual(secondCall["action"], "Scheduled event reference cleaned")

    async def test_run_check_logs_inaccessible_without_cleanup(self) -> None:
        auditStream = SimpleNamespace(logEvent=AsyncMock())
        watcher = WebhookHealthWatcher(
            botClient=SimpleNamespace(get_channel=lambda _channelId: None),
            taskBudgeter=SimpleNamespace(runDiscord=AsyncMock()),
            auditStream=auditStream,
            checkIntervalSec=600,
        )

        hubRows = [
            {
                "messageId": 111,
                "guildId": 222,
                "channelId": 333,
                "divisionKey": "niru",
            }
        ]

        with (
            patch("runtime.webhookHealth.fetchAll", AsyncMock(side_effect=[hubRows, []])),
            patch.object(watcher, "_messageState", AsyncMock(return_value="inaccessible")),
            patch(
                "runtime.webhookHealth.applicationsService.deleteHubMessage",
                AsyncMock(),
            ) as deleteHubMessageMock,
        ):
            summary = await watcher.runCheck()

        self.assertEqual(summary, {"checked": 1, "missing": 1, "errors": 0})
        deleteHubMessageMock.assert_not_awaited()
        auditStream.logEvent.assert_awaited_once()
        self.assertEqual(
            auditStream.logEvent.await_args.kwargs["action"],
            "Application hub message inaccessible",
        )
        self.assertEqual(auditStream.logEvent.await_args.kwargs["severity"], "WARN")

    async def test_run_check_dedupes_cleaned_hub_rows_for_same_division(self) -> None:
        auditStream = SimpleNamespace(logEvent=AsyncMock())
        watcher = WebhookHealthWatcher(
            botClient=SimpleNamespace(get_channel=lambda _channelId: None),
            taskBudgeter=SimpleNamespace(runDiscord=AsyncMock()),
            auditStream=auditStream,
            checkIntervalSec=600,
        )

        hubRows = [
            {
                "messageId": 111,
                "guildId": 222,
                "channelId": 333,
                "divisionKey": "lo",
            },
            {
                "messageId": 112,
                "guildId": 222,
                "channelId": 333,
                "divisionKey": "lo",
            },
        ]

        with (
            patch("runtime.webhookHealth.fetchAll", AsyncMock(side_effect=[hubRows, []])),
            patch.object(watcher, "_messageState", AsyncMock(side_effect=["missing", "missing"])),
            patch(
                "runtime.webhookHealth.applicationsService.deleteHubMessage",
                AsyncMock(),
            ) as deleteHubMessageMock,
        ):
            summary = await watcher.runCheck()

        self.assertEqual(summary, {"checked": 2, "missing": 2, "errors": 0})
        self.assertEqual(deleteHubMessageMock.await_count, 2)
        auditStream.logEvent.assert_awaited_once()
        kwargs = auditStream.logEvent.await_args.kwargs
        self.assertEqual(kwargs["severity"], "INFO")
        self.assertEqual(kwargs["details"]["divisionKey"], "lo")
        self.assertTrue(kwargs["details"]["cleanupApplied"])


if __name__ == "__main__":
    unittest.main()
