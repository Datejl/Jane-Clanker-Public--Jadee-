from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiohttp.client_exceptions import ClientOSError

from runtime import interaction


class InteractionRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def test_transient_transport_error_detects_bad_record_mac(self) -> None:
        exc = ClientOSError(
            1,
            "[SSL: SSLV3_ALERT_BAD_RECORD_MAC] sslv3 alert bad record mac (_ssl.c:2648)",
        )

        self.assertTrue(interaction._isTransientTransportError(exc))

    async def test_safe_channel_send_swallows_transient_transport_error(self) -> None:
        channel = SimpleNamespace(send=AsyncMock())
        exc = ClientOSError(
            1,
            "[SSL: SSLV3_ALERT_BAD_RECORD_MAC] sslv3 alert bad record mac (_ssl.c:2648)",
        )

        with patch.object(
            interaction.taskBudgeter,
            "runInteractiveDiscord",
            AsyncMock(side_effect=exc),
        ):
            result = await interaction.safeChannelSend(channel, content="hello")

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
