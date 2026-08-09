from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from quart import Quart

from API import EndPoints


class OrientationApiParsingTests(unittest.TestCase):
    def test_parse_clock_in_payload_rejects_bad_shapes(self) -> None:
        self.assertIsNone(EndPoints.parseClockInPayload(None))
        self.assertIsNone(EndPoints.parseClockInPayload({}))
        self.assertIsNone(
            EndPoints.parseClockInPayload(
                {
                    "sessionId": "not-a-number",
                    "sessionPassword": "secret",
                    "sessionUserId": 10,
                }
            )
        )
        self.assertIsNone(
            EndPoints.parseClockInPayload(
                {
                    "sessionId": 5,
                    "sessionPassword": 123,
                    "sessionUserId": 10,
                }
            )
        )

    def test_parse_clock_in_payload_accepts_positive_ids(self) -> None:
        self.assertEqual(
            EndPoints.parseClockInPayload(
                {
                    "sessionId": "5",
                    "sessionPassword": "secret",
                    "sessionUserId": 10,
                }
            ),
            (5, "secret", 10),
        )


class OrientationApiRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = Quart(__name__)
        EndPoints.registerRoutes(self.app)
        self.client = self.app.test_client()

    async def test_route_rejects_missing_token_before_reading_payload(self) -> None:
        EndPoints.configureApi(botClient=SimpleNamespace(), token="expected-token")

        response = await self.client.post("/enterOrientation", json={})

        self.assertEqual(response.status_code, 403)
        self.assertEqual((await response.get_json())["error"], "unauthorized")

    async def test_route_rejects_invalid_payload_without_crashing(self) -> None:
        EndPoints.configureApi(botClient=SimpleNamespace(), token="expected-token")

        response = await self.client.post(
            "/enterOrientation",
            headers={"X-API-TOKEN": "expected-token"},
            json={"sessionId": "bad"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual((await response.get_json())["error"], "invalid-request")

    async def test_successful_clock_in_schedules_message_refresh(self) -> None:
        botClient = SimpleNamespace()
        EndPoints.configureApi(botClient=botClient, token="expected-token")
        with (
            patch.object(
                EndPoints,
                "attemptClockIn",
                AsyncMock(return_value={"status": "ADDED", "attendeeCount": 4}),
            ) as clockInMock,
            patch.object(
                EndPoints,
                "requestSessionMessageUpdate",
                AsyncMock(),
            ) as updateMock,
        ):
            response = await self.client.post(
                "/enterOrientation",
                headers={"X-API-TOKEN": "expected-token"},
                json={
                    "sessionId": 7,
                    "sessionPassword": "secret",
                    "sessionUserId": 11,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue((await response.get_json())["ok"])
        clockInMock.assert_awaited_once_with(7, 11, "secret")
        updateMock.assert_awaited_once_with(bot=botClient, sessionId=7, delaySec=0.5)


if __name__ == "__main__":
    unittest.main()
