from __future__ import annotations

from typing import Sequence

import discord

from features.staff.honorGuard import rendering as honorGuardRendering
from features.staff.honorGuard import service as honorGuardService


class HonorGuardEventClockinAdapter:
    async def createSession(
        self,
        guildId: int,
        channelId: int,
        hostId: int,
        maxAttendeeLimit: int = 30,
        **kwargs,
    ) -> int:
        return await honorGuardService.createEventClockinSession(
            guildId=int(guildId),
            channelId=int(channelId),
            hostId=int(hostId),
            maxAttendeeLimit=int(maxAttendeeLimit),
            eventType=str(kwargs.get("eventType") or "").strip(),
            eventTitle=str(kwargs.get("eventTitle") or "").strip(),
            eventDate=str(kwargs.get("eventDate") or "").strip(),
            hostRobloxUsername=str(kwargs.get("hostRobloxUsername") or "").strip(),
            coHostUserIds=list(kwargs.get("coHostUserIds") or []),
            supervisorUserIds=list(kwargs.get("supervisorUserIds") or []),
            scheduleEventId=str(kwargs.get("scheduleEventId") or "").strip(),
            notes=str(kwargs.get("notes") or "").strip(),
            createdBy=int(kwargs.get("createdBy") or hostId or 0),
        )

    async def setSessionMessageId(self, sessionId: int, messageId: int) -> None:
        await honorGuardService.setEventClockinMessageId(int(sessionId), int(messageId))

    async def getSession(self, sessionId: int) -> dict | None:
        return await honorGuardService.getEventClockinSession(int(sessionId))

    async def listOpenSessions(self) -> list[dict]:
        return await honorGuardService.listOpenEventClockinSessions()

    async def listAttendees(self, sessionId: int) -> list[dict]:
        return await honorGuardService.listEventClockinAttendees(int(sessionId))

    async def addAttendee(self, sessionId: int, userId: int) -> None:
        await honorGuardService.addEventClockinAttendee(int(sessionId), int(userId))

    async def removeAttendee(self, sessionId: int, userId: int) -> None:
        await honorGuardService.removeEventClockinAttendee(int(sessionId), int(userId))

    async def updateSessionStatus(self, sessionId: int, status: str) -> None:
        await honorGuardService.updateEventClockinStatus(int(sessionId), str(status))

    def normalizeSession(self, session: dict) -> dict:
        return {
            "sessionId": int(session.get("sessionId") or 0),
            "guildId": int(session.get("guildId") or 0),
            "channelId": int(session.get("channelId") or 0),
            "messageId": int(session.get("messageId") or 0),
            "hostId": int(session.get("hostId") or 0),
            "status": str(session.get("status") or "OPEN").upper(),
            "eventRecordId": int(session.get("eventRecordId") or 0),
            "eventType": str(session.get("eventType") or "").strip(),
            "eventTitle": str(session.get("eventTitle") or "").strip(),
            "eventDate": str(session.get("eventDate") or "").strip(),
            "hostRobloxUsername": str(session.get("hostRobloxUsername") or "").strip(),
            "maxAttendeeLimit": int(session.get("maxAttendeeLimit") or 30),
            "coHostUserIds": list(session.get("coHostUserIds") or []),
            "supervisorUserIds": list(session.get("supervisorUserIds") or []),
            "scheduleEventId": str(session.get("scheduleEventId") or "").strip(),
            "notes": str(session.get("notes") or "").strip(),
        }

    def buildEmbed(self, session: dict, attendees: Sequence[dict]) -> discord.Embed:
        return honorGuardRendering.buildEventClockinEmbed(
            self.normalizeSession(session),
            list(attendees),
        )
