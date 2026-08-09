from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable
from typing import Any

log = logging.getLogger(__name__)


async def cancelTasks(*tasks: asyncio.Task | None) -> None:
    """Cancel tasks and wait until none of them can keep running after teardown."""

    currentTask = asyncio.current_task()
    activeTasks = {
        task
        for task in tasks
        if task is not None and task is not currentTask
    }
    for task in activeTasks:
        if not task.done():
            task.cancel()
    if activeTasks:
        await asyncio.gather(*activeTasks, return_exceptions=True)


class TaskSupervisor:
    """Own short-lived runtime tasks and consume every terminal result."""

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self.log = logger or log
        self._tasks: set[asyncio.Task] = set()
        self._stopping = False

    @property
    def activeCount(self) -> int:
        return len(self._tasks)

    def create(self, awaitable: Awaitable[Any], *, name: str) -> asyncio.Task | None:
        if self._stopping:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            return None

        task = asyncio.create_task(awaitable, name=name)
        self._tasks.add(task)

        def _done(doneTask: asyncio.Task) -> None:
            self._tasks.discard(doneTask)
            if doneTask.cancelled():
                return
            try:
                doneTask.result()
            except Exception:
                self.log.exception("Supervised runtime task crashed: %s", name)

        task.add_done_callback(_done)
        return task

    async def stop(self) -> None:
        self._stopping = True
        tasks = set(self._tasks)
        await cancelTasks(*tasks)
        self._tasks.clear()
