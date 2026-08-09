from __future__ import annotations

import asyncio
import logging

import discord

log = logging.getLogger(__name__)


class TrainingLogRuntime:
    """Own training-log startup sync and per-message capture tasks."""

    def __init__(
        self,
        *,
        botClient,
        configModule,
        taskBudgeter,
        coordinator,
    ) -> None:
        self.botClient = botClient
        self.config = configModule
        self.taskBudgeter = taskBudgeter
        self.coordinator = coordinator
        self._startupSyncTask: asyncio.Task | None = None
        self._captureTasks: dict[int, asyncio.Task] = {}

    async def _runStartupSync(self) -> None:
        await self.botClient.wait_until_ready()
        delaySec = max(
            0.0,
            float(getattr(self.config, "trainingLogStartupSyncDelaySec", 90) or 0),
        )
        if delaySec > 0:
            await asyncio.sleep(delaySec)
        await self.taskBudgeter.runLowPriorityDiscord(
            lambda: self.coordinator.ensureSummaryPanelAtBottom()
        )

        maxAttempts = 3
        retryDelaySec = 30
        for attempt in range(1, maxAttempts + 1):
            log.info(
                "Training log startup sync attempt %s/%s beginning.",
                attempt,
                maxAttempts,
            )
            await self.taskBudgeter.runLowPriorityDiscord(
                lambda: self.coordinator.syncRecentMessages()
            )
            if getattr(self.coordinator, "_lastReadySyncAt", None) is not None:
                log.info("Training log startup sync completed.")
                return
            if attempt < maxAttempts:
                log.warning(
                    "Training log startup sync did not complete on attempt %s/%s. "
                    "Retrying in %ss.",
                    attempt,
                    maxAttempts,
                    retryDelaySec,
                )
                await asyncio.sleep(retryDelaySec)
        log.warning("Training log startup sync gave up after %s attempt(s).", maxAttempts)

    def start(self) -> None:
        if self._startupSyncTask is not None and not self._startupSyncTask.done():
            return
        task = asyncio.create_task(
            self._runStartupSync(),
            name="training-log-backfill",
        )
        self._startupSyncTask = task

        def _doneCallback(doneTask: asyncio.Task) -> None:
            try:
                doneTask.result()
            except asyncio.CancelledError:
                log.info("Training log backfill task was cancelled.")
            except Exception:
                log.exception("Training log backfill task crashed.")

        task.add_done_callback(_doneCallback)

    def scheduleCapture(self, message: discord.Message) -> None:
        if not self.coordinator.shouldInspectSourceMessage(message):
            return
        try:
            messageId = int(getattr(message, "id", 0) or 0)
        except (TypeError, ValueError):
            messageId = 0
        if messageId <= 0:
            return

        async def _runner() -> None:
            try:
                await self.taskBudgeter.runBackground(
                    lambda: self.coordinator.handleSourceMessage(message)
                )
            except Exception:
                log.exception("Training log capture failed for message %s.", messageId)
            finally:
                current = self._captureTasks.get(messageId)
                if current is asyncio.current_task():
                    self._captureTasks.pop(messageId, None)

        self._captureTasks[messageId] = asyncio.create_task(
            _runner(),
            name=f"training-log-capture-{messageId}",
        )

    async def stop(self) -> None:
        tasks = set(self._captureTasks.values())
        if self._startupSyncTask is not None:
            tasks.add(self._startupSyncTask)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._captureTasks.clear()
        self._startupSyncTask = None
