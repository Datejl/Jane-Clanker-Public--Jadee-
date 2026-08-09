from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from db import sqlite as sqliteDb
from features.staff.workflows import (
    WorkflowDefinition,
    WorkflowStateDefinition,
    WorkflowSubjectBridge,
    ensureRun,
    getWorkflowDefinition,
    isWorkflowRegistered,
    listRunEvents,
    registerWorkflowDefinition,
    transitionSubjectRun,
)


class WorkflowExtensionTests(unittest.TestCase):
    def _definition(self, key: str) -> WorkflowDefinition:
        return WorkflowDefinition(
            key=key,
            displayName="Future Review",
            subjectType="future_request",
            defaultStateKey="submitted",
            states=(
                WorkflowStateDefinition(
                    key="submitted",
                    label="Submitted",
                    pendingWith="reviewer",
                    allowedFromKeys=("", "submitted"),
                ),
                WorkflowStateDefinition(
                    key="done",
                    label="Done",
                    isTerminal=True,
                    allowedFromKeys=("submitted",),
                ),
            ),
        )

    def test_custom_definition_can_register_for_normal_service_lookup(self) -> None:
        key = f"future-{uuid4().hex}"

        registered = registerWorkflowDefinition(self._definition(key))

        self.assertTrue(isWorkflowRegistered(key))
        self.assertIs(getWorkflowDefinition(key), registered)
        self.assertEqual(registered.getState("DONE").label, "Done")

    def test_register_is_idempotent_but_rejects_a_different_duplicate(self) -> None:
        key = f"future-{uuid4().hex}"
        definition = self._definition(key)

        first = registerWorkflowDefinition(definition)
        second = registerWorkflowDefinition(definition)

        self.assertIs(first, second)
        with self.assertRaisesRegex(ValueError, "already registered"):
            registerWorkflowDefinition(
                WorkflowDefinition(
                    key=key,
                    displayName="Different Flow",
                    subjectType="future_request",
                    defaultStateKey="queued",
                    states=(WorkflowStateDefinition("queued", "Queued"),),
                )
            )

    def test_definition_validation_catches_unknown_transitions(self) -> None:
        key = f"future-{uuid4().hex}"

        with self.assertRaisesRegex(ValueError, "unknown states"):
            registerWorkflowDefinition(
                WorkflowDefinition(
                    key=key,
                    displayName="Broken Flow",
                    subjectType="future_request",
                    defaultStateKey="submitted",
                    states=(
                        WorkflowStateDefinition(
                            "submitted",
                            "Submitted",
                            allowedFromKeys=("missing",),
                        ),
                    ),
                )
            )

    def test_workflow_bridge_is_available_from_the_public_package(self) -> None:
        bridge = WorkflowSubjectBridge(
            workflowKey="applications",
            subjectType="division_application",
            subjectIdField="applicationId",
            displayName=lambda row: str(row.get("name") or ""),
            metadata=lambda row: dict(row),
            stateForStatus=lambda _status: "submitted",
            missingIdentifiersMessage="missing",
        )

        self.assertEqual(bridge.subjectId({"applicationId": "42"}), 42)
        self.assertEqual(bridge.guildId({"guildId": "7"}), 7)


class WorkflowExtensionServiceTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_registered_workflow_uses_the_normal_service_calls(self) -> None:
        key = f"future-{uuid4().hex}"
        registerWorkflowDefinition(
            WorkflowDefinition(
                key=key,
                displayName="Future Review",
                subjectType="future_request",
                defaultStateKey="submitted",
                states=(
                    WorkflowStateDefinition(
                        "submitted",
                        "Submitted",
                        pendingWith="reviewer",
                        allowedFromKeys=("", "submitted"),
                    ),
                    WorkflowStateDefinition(
                        "done",
                        "Done",
                        isTerminal=True,
                        allowedFromKeys=("submitted",),
                    ),
                ),
            )
        )

        created = await ensureRun(
            workflowKey=key,
            subjectType="future_request",
            subjectId=42,
            guildId=7,
            displayName="Future Request 42",
        )
        finished = await transitionSubjectRun(
            workflowKey=key,
            subjectType="future_request",
            subjectId=42,
            guildId=7,
            stateKey="done",
            actorId=99,
            note="Finished by a future caller.",
        )
        events = await listRunEvents(int(finished["runId"]))

        self.assertEqual(created["currentStateKey"], "submitted")
        self.assertEqual(finished["currentStateKey"], "done")
        self.assertEqual(int(finished["isTerminal"]), 1)
        self.assertEqual(len(events), 2)
