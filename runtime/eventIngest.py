from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Protocol

import discord

import config

_hostMentionRegex = re.compile(r"<@!?(\d+)>")


@dataclass
class IngestEvent:
    source: str
    eventType: str
    messageId: int
    hostId: Optional[int]
    payload: dict


class EventIngestAdapter(Protocol):
    name: str

    async def tryParse(self, message: discord.Message) -> Optional[IngestEvent]:
        ...


def _extractHostIdFromLine(line: str, guild: discord.Guild) -> int | None:
    mentionMatch = _hostMentionRegex.search(line)
    if mentionMatch:
        return int(mentionMatch.group(1))

    raw = line.split(":", 1)[-1].strip()
    if raw.startswith("@"):
        raw = raw[1:]
    raw = re.sub(r"\[[^\]]+\]", "", raw).strip()
    if not raw:
        return None
    target = raw.lower()
    matches = [
        member
        for member in guild.members
        if member.display_name.lower() == target or member.name.lower() == target
    ]
    if len(matches) == 1:
        return int(matches[0].id)
    return None


def _extractHostId(message: discord.Message) -> int | None:
    if not message.guild or not message.content:
        return None
    for line in message.content.splitlines():
        line = line.strip()
        if line.lower().startswith("host:"):
            return _extractHostIdFromLine(line, message.guild)
    return None


def _extractEventType(message: discord.Message) -> str:
    if not message.content:
        return ""
    for line in message.content.splitlines():
        line = line.strip()
        if line.lower().startswith("type of event:"):
            return line.split(":", 1)[-1].strip()
    return ""


class JohnEventLogAdapter:
    name = "john.eventLog"

    async def tryParse(self, message: discord.Message) -> Optional[IngestEvent]:
        johnBotId = getattr(config, "johnClankerBotId", None)
        eventLogChannelId = getattr(config, "johnEventLogChannelId", None)
        if not johnBotId or not eventLogChannelId:
            return None
        if message.author.id != int(johnBotId):
            return None
        if message.channel.id != int(eventLogChannelId):
            return None

        content = message.content or ""
        if not content.strip():
            return None
        if "recruit" in content.lower():
            return None

        hostId = _extractHostId(message)
        eventTypeRaw = _extractEventType(message)
        eventType = eventTypeRaw.lower()
        isShift = "shift" in eventType or "shit" in eventType
        eventCategory = "shift" if isShift else "other"

        return IngestEvent(
            source=self.name,
            eventType="john.orbatIncrement",
            messageId=int(message.id),
            hostId=hostId,
            payload={
                "eventCategory": eventCategory,
                "eventTypeRaw": eventTypeRaw,
            },
        )


class EventIngestDispatcher:
    def __init__(self, adapters: Optional[list[EventIngestAdapter]] = None):
        self._adapters: list[EventIngestAdapter] = list(adapters or [])

    def register(self, adapter: EventIngestAdapter) -> None:
        self._adapters.append(adapter)

    async def parse(self, message: discord.Message) -> list[IngestEvent]:
        events: list[IngestEvent] = []
        for adapter in self._adapters:
            try:
                parsed = await adapter.tryParse(message)
            except Exception:
                continue
            if parsed is not None:
                events.append(parsed)
        return events
