from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from features.operations.serverSafety import paths as serverSafetyPaths
from runtime import backups, entrypoint, errorLogging, processControl
from tools import setup_jane_identity_tunnel as identityTunnelSetup


class EnvironmentPathTests(unittest.TestCase):
    def test_env_is_found_beside_entrypoint_without_relying_on_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as tempDir:
            repoRoot = Path(tempDir)
            scriptPath = repoRoot / "bot.py"
            scriptPath.write_text("", encoding="utf-8")
            expected = repoRoot / ".env"
            expected.write_text("EXAMPLE=1\n", encoding="utf-8")

            with patch.dict(os.environ, {"JANE_ENV_PATH": ""}, clear=False):
                resolved = entrypoint.resolveEnvironmentPath(str(scriptPath))

        self.assertEqual(resolved, expected.resolve())

    def test_relative_env_override_is_resolved_from_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tempDir:
            repoRoot = Path(tempDir)
            scriptPath = repoRoot / "bot.py"
            scriptPath.write_text("", encoding="utf-8")
            expected = repoRoot / "private" / "jane.env"
            expected.parent.mkdir()
            expected.write_text("EXAMPLE=1\n", encoding="utf-8")

            with patch.dict(os.environ, {"JANE_ENV_PATH": "private/jane.env"}, clear=False):
                resolved = entrypoint.resolveEnvironmentPath(str(scriptPath))

        self.assertEqual(resolved, expected.resolve())


class RuntimeDataPathTests(unittest.TestCase):
    def test_relative_log_and_backup_paths_are_anchored_to_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tempDir:
            repoRoot = Path(tempDir).resolve()
            with (
                patch.object(errorLogging, "_repoRoot", return_value=repoRoot),
                patch.object(backups, "_repoRoot", return_value=repoRoot),
            ):
                logPath = errorLogging.generalErrorLogPath(
                    SimpleNamespace(generalErrorLogDir="var/jane/logs")
                )
                backupPath = backups._backupDir(
                    SimpleNamespace(dbBackupDir="var/jane/backups")
                )

        self.assertEqual(logPath, repoRoot / "var" / "jane" / "logs" / "general-errors.log")
        self.assertEqual(backupPath, repoRoot / "var" / "jane" / "backups")

    def test_relative_server_snapshot_path_is_anchored_to_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tempDir:
            repoRoot = Path(tempDir).resolve()
            config = SimpleNamespace(serverSafetySnapshotDir="var/jane/snapshots")
            with patch.object(serverSafetyPaths, "_repoRoot", return_value=repoRoot):
                resolved = serverSafetyPaths.snapshotDir(config)

        self.assertEqual(resolved, repoRoot / "var" / "jane" / "snapshots")

class ProcessControlPortabilityTests(unittest.TestCase):
    def test_supervisor_restart_uses_a_process_exit(self) -> None:
        with patch.dict(os.environ, {"JANE_SUPERVISOR_MANAGED": "1"}, clear=False):
            with self.assertRaises(SystemExit) as raised:
                processControl.relaunchCurrentProcess(scriptPath=__file__)

        self.assertEqual(raised.exception.code, 75)


class TerminationHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_sigterm_requests_a_clean_bot_close(self) -> None:
        class _Bot:
            closed = False

            def is_closed(self) -> bool:
                return self.closed

            async def close(self) -> None:
                self.closed = True

        class _LoopProxy:
            callback = None
            removed = False

            def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
                self.loop = loop

            def add_signal_handler(self, _signal: int, callback) -> None:
                self.callback = callback

            def remove_signal_handler(self, _signal: int) -> None:
                self.removed = True

            def create_task(self, coroutine, *, name: str):
                return self.loop.create_task(coroutine, name=name)

        bot = _Bot()
        loopProxy = _LoopProxy(asyncio.get_running_loop())
        with patch.object(entrypoint.asyncio, "get_running_loop", return_value=loopProxy):
            removeHandler = entrypoint.installTerminationHandler(bot)

        assert loopProxy.callback is not None
        loopProxy.callback()
        await asyncio.sleep(0)
        removeHandler()
        entrypoint.shutdown_api_event.clear()

        self.assertTrue(bot.closed)
        self.assertTrue(loopProxy.removed)


class IdentityRunnerPortabilityTests(unittest.TestCase):
    def test_linux_tailscale_runner_is_executable_shell_script(self) -> None:
        with tempfile.TemporaryDirectory() as tempDir:
            root = Path(tempDir)
            with (
                patch.object(identityTunnelSetup, "_usesPowerShellRunner", return_value=False),
                patch.object(Path, "chmod") as chmod,
            ):
                runner = identityTunnelSetup.writeTailscaleRunner(
                    root,
                    Path("/usr/bin/tailscale"),
                    "127.0.0.1",
                    8791,
                    443,
                )

            content = runner.read_text(encoding="utf-8")

        self.assertEqual(runner.suffix, ".sh")
        self.assertTrue(content.startswith("#!/usr/bin/env sh\nset -eu\n"))
        self.assertIn("funnel --bg --yes --https=443", content)
        chmod.assert_called_once()

    def test_windows_tailscale_runner_keeps_powershell_format(self) -> None:
        with tempfile.TemporaryDirectory() as tempDir:
            root = Path(tempDir)
            with patch.object(identityTunnelSetup, "_usesPowerShellRunner", return_value=True):
                runner = identityTunnelSetup.writeTailscaleRunner(
                    root,
                    Path("tailscale.exe"),
                    "127.0.0.1",
                    8791,
                    443,
                )

            content = runner.read_text(encoding="utf-8")

        self.assertEqual(runner.suffix, ".ps1")
        self.assertIn("$ErrorActionPreference", content)


if __name__ == "__main__":
    unittest.main()
