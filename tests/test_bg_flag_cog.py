from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from cogs.staff import bgFlagCog
from db import sqlite as sqliteDb
from features.staff.bgflags import service as flagService


class _FakeMember:
    def __init__(self, guild_id: int, role_ids: list[int] | None = None) -> None:
        self.guild = SimpleNamespace(id=int(guild_id))
        self.roles = [SimpleNamespace(id=int(role_id)) for role_id in (role_ids or [])]


class BgFlagPermissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_require_mod_permission_allows_anyone_in_open_guild(self) -> None:
        interaction = SimpleNamespace(
            user=_FakeMember(1475880785947918441),
            guild=SimpleNamespace(id=1475880785947918441),
        )

        with (
            patch.object(bgFlagCog.discord, "Member", _FakeMember),
            patch.object(bgFlagCog.config, "bgFlagOpenGuildIds", [1475880785947918441], create=True),
            patch.object(bgFlagCog.interactionRuntime, "safeInteractionReply", AsyncMock()) as replyMock,
        ):
            allowed = await bgFlagCog._requireModPermission(interaction)

        self.assertTrue(allowed)
        replyMock.assert_not_awaited()

    async def test_require_mod_permission_still_blocks_non_mods_elsewhere(self) -> None:
        interaction = SimpleNamespace(
            user=_FakeMember(123),
            guild=SimpleNamespace(id=123),
        )

        with (
            patch.object(bgFlagCog.discord, "Member", _FakeMember),
            patch.object(bgFlagCog.interactionRuntime, "safeInteractionReply", AsyncMock()) as replyMock,
        ):
            allowed = await bgFlagCog._requireModPermission(interaction)

        self.assertFalse(allowed)
        replyMock.assert_awaited_once()
        self.assertEqual(replyMock.await_args.kwargs["content"], "Mods only.")

    async def test_require_mod_permission_allows_configured_review_role(self) -> None:
        interaction = SimpleNamespace(
            user=_FakeMember(123, role_ids=[1475881349909975191]),
            guild=SimpleNamespace(id=123),
        )

        with (
            patch.object(bgFlagCog.discord, "Member", _FakeMember),
            patch.object(bgFlagCog.config, "bgFlagOpenGuildIds", [], create=True),
            patch.object(bgFlagCog.interactionRuntime, "safeInteractionReply", AsyncMock()) as replyMock,
        ):
            allowed = await bgFlagCog._requireModPermission(interaction)

        self.assertTrue(allowed)
        replyMock.assert_not_awaited()


class BgFlagProposalUiTests(unittest.IsolatedAsyncioTestCase):
    async def test_add_flag_dropdown_excludes_removed_rule_types(self) -> None:
        removed = {"username", "roblox_user", "watchlist", "banned_user", "group_keyword", "item_keyword"}
        values = set(bgFlagCog._addableFlagTypeValues)

        self.assertFalse(values & removed)
        self.assertIn("keyword", values)
        self.assertIn("item", values)

    async def test_proposal_decision_only_rejects_when_not_flag_outnumbers_flag(self) -> None:
        self.assertEqual(
            bgFlagCog._proposalDecision(
                {
                    bgFlagCog.flagService.PROPOSAL_VOTE_FLAG: 2,
                    bgFlagCog.flagService.PROPOSAL_VOTE_NOT_FLAG: 2,
                }
            ),
            "",
        )
        self.assertEqual(
            bgFlagCog._proposalDecision(
                {
                    bgFlagCog.flagService.PROPOSAL_VOTE_FLAG: 3,
                    bgFlagCog.flagService.PROPOSAL_VOTE_NOT_FLAG: 2,
                }
            ),
            "",
        )
        self.assertEqual(
            bgFlagCog._proposalDecision(
                {
                    bgFlagCog.flagService.PROPOSAL_VOTE_FLAG: 1,
                    bgFlagCog.flagService.PROPOSAL_VOTE_NOT_FLAG: 2,
                }
            ),
            bgFlagCog.flagService.PROPOSAL_STATUS_REJECTED,
        )

    async def test_cog_load_restores_open_proposal_webhook_buttons(self) -> None:
        bot = SimpleNamespace(add_view=Mock())
        cog = bgFlagCog.BgFlagCog(bot)

        with patch.object(
            bgFlagCog.flagService,
            "listOpenProposalsWithMessages",
            AsyncMock(return_value=[{"proposalId": 42, "messageId": 1234}]),
        ):
            await cog.cog_load()

        bot.add_view.assert_called_once()
        args, kwargs = bot.add_view.call_args
        self.assertIsInstance(args[0], bgFlagCog.BgFlagProposalVoteView)
        self.assertEqual(kwargs["message_id"], 1234)

    async def test_proposal_embed_includes_item_thumbnail(self) -> None:
        proposal = {
            "proposalId": 10,
            "ruleType": "item",
            "ruleValue": "12345",
            "severity": 50,
            "status": flagService.PROPOSAL_STATUS_OPEN,
        }

        with (
            patch.object(
                bgFlagCog.flagService,
                "proposalVoteCounts",
                AsyncMock(return_value={flagService.PROPOSAL_VOTE_FLAG: 1, flagService.PROPOSAL_VOTE_NOT_FLAG: 0}),
            ),
            patch.object(
                bgFlagCog.robloxAssets,
                "fetchRobloxAssetThumbnails",
                AsyncMock(
                    return_value=SimpleNamespace(
                        thumbnails=[
                            {
                                "id": 12345,
                                "imageUrl": "https://cdn.example/item.png",
                                "state": "Completed",
                            }
                        ]
                    )
                ),
            ) as thumbnailMock,
        ):
            embed = await bgFlagCog._buildProposalEmbed(proposal)

        thumbnailMock.assert_awaited_once_with([12345])
        self.assertEqual(embed.thumbnail.url, "https://cdn.example/item.png")

    async def test_proposal_embed_includes_external_rule_thumbnails(self) -> None:
        cases = [
            ("group", "1001", "group", "https://cdn.example/group.png"),
            ("badge", "2002", "badge", "https://cdn.example/badge.png"),
            ("game", "3003", "game", "https://cdn.example/game.png"),
        ]

        with (
            patch.object(
                bgFlagCog.flagService,
                "proposalVoteCounts",
                AsyncMock(return_value={flagService.PROPOSAL_VOTE_FLAG: 1, flagService.PROPOSAL_VOTE_NOT_FLAG: 0}),
            ),
            patch.object(bgFlagCog.robloxThumbnails, "fetchRobloxThumbnailUrl", AsyncMock()) as thumbnailMock,
        ):
            for index, (ruleType, ruleValue, expectedKind, expectedUrl) in enumerate(cases, start=1):
                thumbnailMock.return_value = expectedUrl
                embed = await bgFlagCog._buildProposalEmbed(
                    {
                        "proposalId": index,
                        "ruleType": ruleType,
                        "ruleValue": ruleValue,
                        "severity": 50,
                        "status": flagService.PROPOSAL_STATUS_OPEN,
                    }
                )
                self.assertEqual(embed.thumbnail.url, expectedUrl)
                thumbnailMock.assert_awaited_with(expectedKind, int(ruleValue))

    async def test_proposal_embed_skips_keyword_thumbnail_lookup(self) -> None:
        proposal = {
            "proposalId": 10,
            "ruleType": "keyword",
            "ruleValue": "suspicious",
            "severity": 50,
            "status": flagService.PROPOSAL_STATUS_OPEN,
        }

        with (
            patch.object(
                bgFlagCog.flagService,
                "proposalVoteCounts",
                AsyncMock(return_value={flagService.PROPOSAL_VOTE_FLAG: 1, flagService.PROPOSAL_VOTE_NOT_FLAG: 0}),
            ),
            patch.object(bgFlagCog.robloxAssets, "fetchRobloxAssetThumbnails", AsyncMock()) as assetMock,
            patch.object(bgFlagCog.robloxThumbnails, "fetchRobloxThumbnailUrl", AsyncMock()) as thumbnailMock,
        ):
            embed = await bgFlagCog._buildProposalEmbed(proposal)

        self.assertIsNone(embed.thumbnail.url)
        assetMock.assert_not_awaited()
        thumbnailMock.assert_not_awaited()

    async def test_proposal_embed_survives_thumbnail_lookup_failure(self) -> None:
        proposal = {
            "proposalId": 10,
            "ruleType": "group",
            "ruleValue": "12345",
            "severity": 50,
            "status": flagService.PROPOSAL_STATUS_OPEN,
        }

        with (
            patch.object(
                bgFlagCog.flagService,
                "proposalVoteCounts",
                AsyncMock(return_value={flagService.PROPOSAL_VOTE_FLAG: 1, flagService.PROPOSAL_VOTE_NOT_FLAG: 0}),
            ),
            patch.object(
                bgFlagCog.robloxThumbnails,
                "fetchRobloxThumbnailUrl",
                AsyncMock(side_effect=RuntimeError("thumbnail outage")),
            ),
        ):
            embed = await bgFlagCog._buildProposalEmbed(proposal)

        self.assertIsNone(embed.thumbnail.url)


class BgFlagProposalServiceTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_proposal_vote_counts_track_latest_vote_per_reviewer(self) -> None:
        proposalId = await flagService.createProposal(
            guildId=1,
            ruleType="keyword",
            ruleValue="bad keyword",
            note="review this",
            proposedBy=10,
            severity=50,
        )

        await flagService.upsertProposalVote(
            proposalId,
            voterId=20,
            vote=flagService.PROPOSAL_VOTE_FLAG,
        )
        await flagService.upsertProposalVote(
            proposalId,
            voterId=20,
            vote=flagService.PROPOSAL_VOTE_NOT_FLAG,
        )
        await flagService.upsertProposalVote(
            proposalId,
            voterId=21,
            vote=flagService.PROPOSAL_VOTE_NOT_FLAG,
        )

        counts = await flagService.proposalVoteCounts(proposalId)

        self.assertEqual(counts[flagService.PROPOSAL_VOTE_FLAG], 1)
        self.assertEqual(counts[flagService.PROPOSAL_VOTE_NOT_FLAG], 2)
        self.assertEqual(counts["total"], 3)

    async def test_create_proposal_creates_rule_and_initial_flag_vote(self) -> None:
        proposalId = await flagService.createProposal(
            guildId=1,
            ruleType="item",
            ruleValue="12345",
            note="reviewed item",
            proposedBy=10,
            severity=75,
        )

        rules = await flagService.listRules("item")
        proposal = await flagService.getProposal(proposalId)
        counts = await flagService.proposalVoteCounts(proposalId)

        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["ruleValue"], "12345")
        self.assertEqual(rules[0]["severity"], 75)
        self.assertEqual(proposal["status"], flagService.PROPOSAL_STATUS_OPEN)
        self.assertEqual(proposal["resultingRuleId"], rules[0]["ruleId"])
        self.assertEqual(counts[flagService.PROPOSAL_VOTE_FLAG], 1)
        self.assertEqual(counts[flagService.PROPOSAL_VOTE_NOT_FLAG], 0)

    async def test_validated_visual_references_ignore_legacy_review_queue_rows(self) -> None:
        ruleId = await flagService.addRule(
            "item",
            "111",
            "active rule",
            createdBy=10,
            severity=50,
        )
        await sqliteDb.execute(
            """
            INSERT INTO bg_item_visual_refs (
                assetId,
                sourceRuleId,
                sourceRuleCount,
                assetName,
                assetTypeId,
                assetTypeName,
                visualCategory,
                thumbnailHash,
                colorSignature,
                colorSignatureVersion,
                hashSize,
                thumbnailState,
                validationState,
                lastValidatedAt
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                111,
                ruleId,
                1,
                "Active Shirt",
                11,
                "Shirt",
                "classic_shirt",
                "activehash",
                '{"v":3,"bins":[],"mode":"detailed"}',
                flagService._visualColorSignatureVersion(),
                flagService._visualHashSize(),
                "completed",
                "VALID",
            ),
        )
        await sqliteDb.execute(
            """
            INSERT INTO bg_item_review_queue (
                guildId,
                assetId,
                assetName,
                thumbnailHash,
                thumbnailState,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, 222, "Legacy flagged item", "legacyhash", "completed", "FLAGGED"),
        )

        with patch.object(
            flagService.robloxAssets,
            "fetchRobloxAssetVisualSignatures",
            AsyncMock(return_value=({222: {"thumbnailHash": "newlegacyhash"}}, None)),
        ) as signatureMock:
            references = await flagService.getValidatedItemVisualReferences(ensureSynced=True)

        self.assertEqual(set(references.keys()), {111})
        self.assertEqual(references[111]["thumbnailHash"], "activehash")
        signatureMock.assert_not_awaited()

    async def test_item_visual_reference_sync_stores_type_metadata(self) -> None:
        await flagService.addRule(
            "item",
            "333",
            "shirt rule",
            createdBy=10,
            severity=50,
        )

        async def fakeValidate(assetIds):
            return [
                {
                    "assetId": int(assetId),
                    "thumbnailHash": f"hash-{int(assetId)}",
                    "colorSignature": '{"v":3,"bins":[],"mode":"detailed"}',
                    "colorSignatureVersion": flagService._visualColorSignatureVersion(),
                    "hashSize": flagService._visualHashSize(),
                    "thumbnailUrl": f"https://cdn.example/{int(assetId)}.png",
                    "thumbnailState": "completed",
                    "validationState": "VALID",
                    "lastValidatedAt": "2026-01-01T00:00:00+00:00",
                }
                for assetId in assetIds
            ]

        async def fakePrices(assetIds):
            return {
                int(assetId): {
                    "name": f"Classic Shirt {int(assetId)}",
                    "assetTypeId": 11,
                    "assetTypeName": "Shirt",
                    "creatorId": 99,
                    "creatorType": "User",
                }
                for assetId in assetIds
            }, None

        with (
            patch.object(flagService.robloxAssets, "validateRobloxAssetVisualReferences", AsyncMock(side_effect=fakeValidate)),
            patch.object(flagService.robloxAssets, "fetchCatalogAssetPrices", AsyncMock(side_effect=fakePrices)),
        ):
            summary = await flagService.syncItemVisualReferences(force=True)
            references = await flagService.getValidatedItemVisualReferences(ensureSynced=False)

        self.assertEqual(summary["metadataCount"], 1)
        self.assertEqual(summary["categoryCounts"], {"classic_shirt": 1})
        self.assertEqual(references[333]["assetName"], "Classic Shirt 333")
        self.assertEqual(references[333]["assetTypeId"], 11)
        self.assertEqual(references[333]["assetTypeName"], "Shirt")
        self.assertEqual(references[333]["visualCategory"], "classic_shirt")

    async def test_open_proposal_restore_list_excludes_and_closes_expired_votes(self) -> None:
        proposalId = await flagService.createProposal(
            guildId=1,
            ruleType="keyword",
            ruleValue="expired keyword",
            note="old vote",
            proposedBy=10,
            severity=50,
        )
        await flagService.setProposalMessage(proposalId, channelId=123, messageId=456)
        await sqliteDb.execute(
            "UPDATE bg_flag_proposals SET createdAt = datetime('now', '-25 hours') WHERE proposalId = ?",
            (proposalId,),
        )

        rows = await flagService.listOpenProposalsWithMessages()
        proposal = await flagService.getProposal(proposalId)

        self.assertEqual(rows, [])
        self.assertEqual(proposal["status"], flagService.PROPOSAL_STATUS_CLOSED)

    async def test_open_proposal_restore_list_keeps_fresh_votes(self) -> None:
        proposalId = await flagService.createProposal(
            guildId=1,
            ruleType="keyword",
            ruleValue="fresh keyword",
            note="new vote",
            proposedBy=10,
            severity=50,
        )
        await flagService.setProposalMessage(proposalId, channelId=123, messageId=456)

        rows = await flagService.listOpenProposalsWithMessages()

        self.assertEqual([row["proposalId"] for row in rows], [proposalId])

    async def test_close_expired_proposal_keeps_backing_rule(self) -> None:
        proposalId = await flagService.createProposal(
            guildId=1,
            ruleType="keyword",
            ruleValue="old keyword",
            note="reviewed keyword",
            proposedBy=10,
            severity=50,
        )
        proposal = await flagService.getProposal(proposalId)
        ruleId = int(proposal["resultingRuleId"])
        await sqliteDb.execute(
            "UPDATE bg_flag_proposals SET createdAt = datetime('now', '-25 hours') WHERE proposalId = ?",
            (proposalId,),
        )

        closed = await flagService.closeExpiredProposal(proposalId)
        rule = await flagService.getRule(ruleId)
        proposal = await flagService.getProposal(proposalId)

        self.assertTrue(closed)
        self.assertIsNotNone(rule)
        self.assertEqual(proposal["status"], flagService.PROPOSAL_STATUS_CLOSED)
        self.assertEqual(proposal["resultingRuleId"], ruleId)

    async def test_reject_proposal_removes_backing_rule(self) -> None:
        proposalId = await flagService.createProposal(
            guildId=1,
            ruleType="keyword",
            ruleValue="bad keyword",
            note="reviewed keyword",
            proposedBy=10,
            severity=50,
        )
        proposal = await flagService.getProposal(proposalId)
        ruleId = int(proposal["resultingRuleId"])

        rejected = await flagService.rejectProposal(proposalId, resolvedBy=20)
        rules = await flagService.listRules("keyword")
        proposal = await flagService.getProposal(proposalId)

        self.assertTrue(rejected)
        self.assertEqual(rules, [])
        self.assertEqual(proposal["status"], flagService.PROPOSAL_STATUS_REJECTED)
        self.assertEqual(proposal["resultingRuleId"], ruleId)


if __name__ == "__main__":
    unittest.main()
