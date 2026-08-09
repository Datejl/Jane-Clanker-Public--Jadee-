from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from features.staff.sessions import sessionControls


class SessionControlLockTests(unittest.TestCase):
    def tearDown(self) -> None:
        sessionControls._finishingSessionIds.clear()

    def test_finishing_claim_is_single_owner_until_released(self) -> None:
        self.assertTrue(sessionControls._claimFinishingSession(123))
        self.assertFalse(sessionControls._claimFinishingSession(123))

        sessionControls._releaseFinishingSession(123)

        self.assertTrue(sessionControls._claimFinishingSession(123))


class _FakeMember:
    def __init__(self, user_id: int, *, administrator: bool = False, manage_guild: bool = False) -> None:
        self.id = int(user_id)
        self.guild_permissions = SimpleNamespace(
            administrator=administrator,
            manage_guild=manage_guild,
        )


class SessionControlPermissionTests(unittest.TestCase):
    def test_session_host_can_manage_session_controls(self) -> None:
        interaction = SimpleNamespace(user=SimpleNamespace(id=10))
        session = {"hostId": 10}

        self.assertTrue(sessionControls._canManageSessionControls(interaction, session))

    def test_server_manager_can_manage_session_controls(self) -> None:
        interaction = SimpleNamespace(user=_FakeMember(20, manage_guild=True))
        session = {"hostId": 10}

        with patch.object(sessionControls.discord, "Member", _FakeMember):
            self.assertTrue(sessionControls._canManageSessionControls(interaction, session))

    def test_unrelated_member_cannot_manage_session_controls(self) -> None:
        interaction = SimpleNamespace(user=_FakeMember(20))
        session = {"hostId": 10}

        with patch.object(sessionControls.discord, "Member", _FakeMember):
            self.assertFalse(sessionControls._canManageSessionControls(interaction, session))


class SessionControlRecoveryTests(unittest.TestCase):
    def test_recovery_snapshot_reads_orientation_message_embed(self) -> None:
        embed = SimpleNamespace(
            title="Orientation Session",
            description="Click the button below.\nThis session has attendee limit of 35.",
            fields=[
                SimpleNamespace(name="Certification Type", value="Orientation"),
                SimpleNamespace(name="Host", value="<@111>"),
                SimpleNamespace(
                    name="Attendees (35)",
                    value=(
                        "1. <@222>  -  :o: Not Graded\n"
                        "2. <@333>  -  :white_check_mark: Passed\n"
                        "3. <@444>  -  :x: Failed"
                    ),
                ),
            ],
            footer=SimpleNamespace(text="Status: FULL"),
        )
        interaction = SimpleNamespace(
            guild=SimpleNamespace(id=10),
            guild_id=10,
            channel=SimpleNamespace(id=20),
            user=SimpleNamespace(id=999),
            message=SimpleNamespace(id=30, embeds=[embed], channel=SimpleNamespace(id=20)),
        )

        snapshot = sessionControls._recoverableOrientationSnapshotFromMessage(interaction, 123)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["sessionId"], 123)
        self.assertEqual(snapshot["guildId"], 10)
        self.assertEqual(snapshot["channelId"], 20)
        self.assertEqual(snapshot["messageId"], 30)
        self.assertEqual(snapshot["hostId"], 111)
        self.assertEqual(snapshot["maxAttendeeLimit"], 35)
        self.assertEqual(snapshot["status"], "FULL")
        self.assertEqual(
            snapshot["attendeeGrades"],
            [(222, "NOT_GRADED"), (333, "PASS"), (444, "FAIL")],
        )


if __name__ == "__main__":
    unittest.main()
