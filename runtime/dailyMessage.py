from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, tzinfo
from typing import Any

from db import sqlite as sqliteDb

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DailyMessageTrigger:
    """Send one small message per configured calendar day for an exact user/channel."""

    key: str
    userId: int
    channelId: int
    content: str
    timezoneInfo: tzinfo = timezone.utc

    def __post_init__(self) -> None:
        cleanKey = str(self.key or "").strip()
        cleanContent = str(self.content or "").strip()
        if not cleanKey:
            raise ValueError("Daily message key cannot be blank.")
        if int(self.userId or 0) <= 0:
            raise ValueError("Daily message userId must be positive.")
        if int(self.channelId or 0) <= 0:
            raise ValueError("Daily message channelId must be positive.")
        if not cleanContent:
            raise ValueError("Daily message content cannot be blank.")
        object.__setattr__(self, "key", cleanKey)
        object.__setattr__(self, "userId", int(self.userId))
        object.__setattr__(self, "channelId", int(self.channelId))
        object.__setattr__(self, "content", cleanContent)

    @property
    def settingKey(self) -> str:
        return f"dailyMessage:{self.key}:lastDay"

    def matches(self, message: Any) -> bool:
        return (
            int(getattr(getattr(message, "author", None), "id", 0) or 0) == self.userId
            and int(getattr(getattr(message, "channel", None), "id", 0) or 0) == self.channelId
        )

    def dayKey(self, now: datetime | None = None) -> str:
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        return current.astimezone(self.timezoneInfo).date().isoformat()

    async def claim(self, dayKey: str) -> bool:
        cleanDayKey = str(dayKey or "").strip()
        if not cleanDayKey:
            raise ValueError("Daily message day key cannot be blank.")

        async def _claim(db) -> bool:
            cursor = await db.execute(
                """
                INSERT INTO bot_settings (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value
                WHERE COALESCE(bot_settings.value, '') <> excluded.value
                """,
                (self.settingKey, cleanDayKey),
            )
            try:
                return int(cursor.rowcount or 0) == 1
            finally:
                await cursor.close()

        return await sqliteDb.runWriteTransaction(_claim)

    async def release(self, dayKey: str) -> None:
        await sqliteDb.execute(
            "DELETE FROM bot_settings WHERE key = ? AND value = ?",
            (self.settingKey, str(dayKey or "").strip()),
        )

    async def handle(
        self,
        message: Any,
        *,
        now: datetime | None = None,
    ) -> bool:
        """Handle a message and return whether the daily response was sent."""

        if not self.matches(message):
            return False

        dayKey = self.dayKey(now)
        try:
            claimed = await self.claim(dayKey)
        except Exception:
            log.exception("Could not claim daily message trigger %s.", self.key)
            return False
        if not claimed:
            return False

        try:
            await message.channel.send(self.content)
        except Exception:
            log.exception("Could not send daily message trigger %s.", self.key)
            try:
                await self.release(dayKey)
            except Exception:
                log.exception("Could not release failed daily message trigger %s.", self.key)
            return False
        return True


__all__ = ["DailyMessageTrigger"]
