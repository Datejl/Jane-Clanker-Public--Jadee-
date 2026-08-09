from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from cogs.staff.anrdPaymentCog import AnrdPaymentCog


class AnrdPaymentCogTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_approved_payment_to_orbat_posts_audit_log(self) -> None:
        cog = AnrdPaymentCog(SimpleNamespace())
        requestRow = {
            "requestId": 7,
            "submitterId": 101,
            "guildId": 202,
            "reviewChannelId": 303,
            "reviewMessageId": 404,
            "status": "APPROVED",
            "askingPrice": "250",
            "negotiatedPrice": "",
        }

        with (
            patch("cogs.staff.anrdPaymentCog.paymentService.isPaymentPayoutSynced", AsyncMock(return_value=False)),
            patch("cogs.staff.anrdPaymentCog.paymentService.getPaymentRequest", AsyncMock(return_value=requestRow)),
            patch("cogs.staff.anrdPaymentCog.config.nonRecruitmentOrbatWritesEnabled", True),
            patch.object(cog, "resolveSubmitterRobloxUsername", AsyncMock(return_value=("builderman", None))),
            patch.object(cog, "resolveSubmitterMember", AsyncMock(return_value=None)),
            patch(
                "cogs.staff.anrdPaymentCog.taskBudgeter.runSheetsThread",
                AsyncMock(
                    return_value={
                        "ok": True,
                        "section": "Developer Payment",
                        "row": 12,
                        "rank": "Developer",
                        "rowCreated": True,
                    }
                ),
            ),
            patch("cogs.staff.anrdPaymentCog.paymentService.markPaymentPayoutSynced", AsyncMock()),
            patch("cogs.staff.anrdPaymentCog.orbatAuditRuntime.sendOrbatChangeLog", AsyncMock()) as auditMock,
        ):
            ok, details = await cog.syncApprovedPaymentToOrbat(
                7,
                authorizedBy="<@555>",
            )

        self.assertTrue(ok)
        self.assertIn("row 12 updated", details)
        auditMock.assert_awaited_once()
        kwargs = auditMock.await_args.kwargs
        self.assertEqual(kwargs["title"], "Spreadsheet Change")
        self.assertEqual(kwargs["change"], "Updated ANRD payment spreadsheet from approved payment request.")
        self.assertEqual(kwargs["requestedBy"], "<@101>")
        self.assertEqual(kwargs["authorizedBy"], "<@555>")
        self.assertEqual(
            kwargs["requestMessageUrl"],
            "https://discord.com/channels/202/303/404",
        )
        self.assertEqual(kwargs["sheetKey"], "dept_anrd")
        self.assertIn("Amount: 250", kwargs["details"])
        self.assertIn("Action: created payout row", kwargs["details"])


if __name__ == "__main__":
    unittest.main()
