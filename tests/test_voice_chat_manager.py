from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import discord

from features.staff.voiceChat import voiceChatManager


class _FakeVoiceChannel:
    def __init__(self, channel_id: int) -> None:
        self.id = int(channel_id)

    async def delete(self) -> None:
        response = SimpleNamespace(status=404, reason="Not Found")
        raise discord.NotFound(response, {"message": "Unknown Channel", "code": 10003})


class _FakeGuild:
    def __init__(self, channel: _FakeVoiceChannel) -> None:
        self._channel = channel

    def get_channel(self, channel_id: int):
        if int(channel_id) == int(self._channel.id):
            return self._channel
        return None


class _FakeBot:
    def __init__(self, guild: _FakeGuild) -> None:
        self._guild = guild

    def get_guild(self, _guild_id: int):
        return self._guild


class VoiceChatManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_tqual_static_channels_are_not_managed_for_auto_delete(self) -> None:
        for index, name in enumerate(("TQUAL Stage", "TQUAL Trainings VC"), start=1):
            channel = SimpleNamespace(
                id=10_000 + index,
                name=name,
                category_id=voiceChatManager.voiceChannelCreationCategory,
            )

            self.assertTrue(voiceChatManager.isPermanentChannel(channel))
            self.assertFalse(voiceChatManager.isManagedVoiceChannel(channel))

    async def test_delete_voice_channel_treats_unknown_channel_as_already_deleted(self) -> None:
        liveChannel = _FakeVoiceChannel(123)
        bot = _FakeBot(_FakeGuild(liveChannel))

        with (
            patch.object(voiceChatManager, "VoiceChannel", _FakeVoiceChannel),
            patch.object(voiceChatManager, "isManagedVoiceChannel", return_value=True),
            patch.object(voiceChatManager, "handleDeletedVoiceChannel", AsyncMock()) as deletedMock,
            patch.object(voiceChatManager, "_rebalanceVoiceChatCategory", AsyncMock()) as rebalanceMock,
            patch.object(voiceChatManager, "_safeEphemeral", AsyncMock()) as safeEphemeralMock,
        ):
            deleted = await voiceChatManager.deleteVoiceChannel(
                bot=bot,
                voiceChannel=liveChannel,
                interaction=None,
            )

        self.assertFalse(deleted)
        deletedMock.assert_awaited_once_with(bot, liveChannel)
        rebalanceMock.assert_awaited_once_with(bot)
        safeEphemeralMock.assert_awaited_once_with(None, "Voice chat was already deleted.")


if __name__ == "__main__":
    unittest.main()
