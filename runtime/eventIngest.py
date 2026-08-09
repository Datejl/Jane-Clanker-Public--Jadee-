from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Protocol

import discord

import config

_hostMentionRegex = re.compile(r"<@!?(\d+)>")
_trainingEventTitleRegex = re.compile(
    r"\b(?:"
    r"training|trainings|train|trained|"
    r"exam|exams|examination|examinations|"
    r"orientation|orientations|"
    r"cert|certs|certification|certifications|"
    r"qualification|qualifications|tqual|"
    r"lecture|lectures|class|classes"
    r")\b",
    re.IGNORECASE,
)
_tqualEventTitleRegex = re.compile(
    r"\b(?:"
    r"grid|emergency|turbine|solo|supervisor"
    r")\b",
    re.IGNORECASE,
)


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


def classifyJohnEventCategory(eventTypeRaw: str) -> str | None:
    title = str(eventTypeRaw or "").strip()
    if not title:
        return None

    normalizedTitle = title.casefold()
    if "recruit" in normalizedTitle:
        return None
    if "shift" in normalizedTitle:
        return "shift"
    if _trainingEventTitleRegex.search(title):
        return None
    if _tqualEventTitleRegex.search(title):
        return None
    return "other"


class JohnEventLogAdapter:
    name = "john.eventLog"

    def __init__(self, *, configModule=config) -> None:
        self.config = configModule

    async def tryParse(self, message: discord.Message) -> Optional[IngestEvent]:
        johnBotId = getattr(self.config, "johnClankerBotId", None)
        eventLogChannelId = getattr(self.config, "johnEventLogChannelId", None)
        if not johnBotId or not eventLogChannelId:
            return None
        if message.author.id != int(johnBotId):
            return None
        if message.channel.id != int(eventLogChannelId):
            return None

        content = message.content or ""
        if not content.strip():
            return None

        hostId = _extractHostId(message)
        eventTypeRaw = _extractEventType(message)
        eventCategory = classifyJohnEventCategory(eventTypeRaw)
        if eventCategory is None:
            return None

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
