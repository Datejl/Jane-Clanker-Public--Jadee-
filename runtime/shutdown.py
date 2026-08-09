from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable


class OnceAsyncCleanup:
    """Run one cleanup coroutine and let every caller await the same result."""

    def __init__(self, *, taskName: str = "runtime-cleanup") -> None:
        self.taskName = taskName
        self._task: asyncio.Task[None] | None = None

    @property
    def started(self) -> bool:
        return self._task is not None

    @property
    def done(self) -> bool:
        return self._task is not None and self._task.done()

    async def run(self, cleanupFactory: Callable[[], Awaitable[None]]) -> None:
        if self._task is None:
            self._task = asyncio.create_task(cleanupFactory(), name=self.taskName)
        await asyncio.shield(self._task)
