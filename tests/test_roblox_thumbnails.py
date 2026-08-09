from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from features.staff.sessions.Roblox import robloxThumbnails


class RobloxThumbnailTests(unittest.IsolatedAsyncioTestCase):
    async def test_badge_thumbnail_uses_badge_supported_default_size(self) -> None:
        calls: list[dict] = []

        async def fakeRequestJson(*args, **kwargs):
            calls.append(dict(kwargs.get("params") or {}))
            return (
                200,
                {
                    "data": [
                        {
                            "targetId": 123,
                            "state": "Completed",
                            "imageUrl": "https://cdn.example/badge.png",
                        }
                    ]
                },
            )

        with (
            patch.object(robloxThumbnails, "_cacheGet", Mock(return_value=None)),
            patch.object(robloxThumbnails, "_cacheSet", Mock()),
            patch.object(robloxThumbnails, "_requestJson", fakeRequestJson),
        ):
            url = await robloxThumbnails.fetchRobloxThumbnailUrl("badge", 123)

        self.assertEqual(url, "https://cdn.example/badge.png")
        self.assertEqual(calls[0]["size"], "150x150")

    async def test_thumbnail_lookup_retries_without_return_policy_on_bad_request(self) -> None:
        calls: list[dict] = []

        async def fakeRequestJson(*args, **kwargs):
            params = dict(kwargs.get("params") or {})
            calls.append(params)
            if "returnPolicy" in params:
                return 400, {"errors": [{"message": "unsupported returnPolicy"}]}
            return (
                200,
                {
                    "data": [
                        {
                            "targetId": 456,
                            "state": "Completed",
                            "imageUrl": "https://cdn.example/group.png",
                        }
                    ]
                },
            )

        with (
            patch.object(robloxThumbnails, "_cacheGet", Mock(return_value=None)),
            patch.object(robloxThumbnails, "_cacheSet", Mock()),
            patch.object(robloxThumbnails, "_requestJson", fakeRequestJson),
        ):
            url = await robloxThumbnails.fetchRobloxThumbnailUrl("group", 456)

        self.assertEqual(url, "https://cdn.example/group.png")
        self.assertIn("returnPolicy", calls[0])
        self.assertNotIn("returnPolicy", calls[1])
