from __future__ import annotations

import time

from dataclasses import dataclass
from typing import Any
from db.sqlite import execute, executeReturnId


from db.sqlite import execute, executeReturnId, fetchOne, fetchAll, executeMany

@dataclass(slots=True, frozen=True)
class HonorGuardConfig:
    enabled: bool
    reviewChannelId: int
    logChannelId: int
    archiveChannelId: int
    spreadsheetId: str
    memberSheetName: str
    scheduleSheetName: str
    archiveSheetName: str


@dataclass(slots=True, frozen=True)
class HonorGuardScaffoldStatus:
    config: HonorGuardConfig
    plannedDbTables: tuple[str, ...]
    plannedModules: tuple[str, ...]
    nextMilestones: tuple[str, ...]


def _normalizePositiveInt(value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def loadHonorGuardConfig(*, configModule: Any) -> HonorGuardConfig:
    return HonorGuardConfig(
        enabled=bool(getattr(configModule, "honorGuardEnabled", False)),
        reviewChannelId=_normalizePositiveInt(getattr(configModule, "honorGuardReviewChannelId", 0)),
        logChannelId=_normalizePositiveInt(getattr(configModule, "honorGuardLogChannelId", 0)),
        archiveChannelId=_normalizePositiveInt(getattr(configModule, "honorGuardArchiveChannelId", 0)),
        spreadsheetId=str(getattr(configModule, "honorGuardSpreadsheetId", "") or "").strip(),
        memberSheetName=str(getattr(configModule, "honorGuardMemberSheetName", "") or "").strip(),
        scheduleSheetName=str(getattr(configModule, "honorGuardScheduleSheetName", "") or "").strip(),
        archiveSheetName=str(getattr(configModule, "honorGuardArchiveSheetName", "") or "").strip(),
    )


def buildScaffoldStatus(*, configModule: Any) -> HonorGuardScaffoldStatus:
    return HonorGuardScaffoldStatus(
        config=loadHonorGuardConfig(configModule=configModule),
        plannedDbTables=(
            "hg_submissions",
            "hg_submission_events",
            "hg_point_awards",
            "hg_attendance_records",
            "hg_sentry_logs",
            "hg_quota_cycles",
            "hg_event_records",
        ),
        plannedModules=(
            "cogs.staff.honorGuardCog",
            "features.staff.honorGuard.service",
            "features.staff.honorGuard",
        ),
        nextMilestones=(
            "Finalize DB schema ownership and add tables.",
            "Build approval workflow for manual awards and sentry logs.",
            "Add event clock-in flow and point calculation logic.",
            "Wire approved records into the Honor Guard sheet adapter.",
        ),
    )

async def createPointAwardSubmission(
    guildId: int,
    channelId: int,
    submitterId: int,
    awardedUserId: int,
    reason: str,
    eventPoints: int,
    quotaPoints: int = 0,
) -> int:
    return await executeReturnId(
        """
        INSERT INTO hg_point_awards
            (guildId, channelId, messageId, submitterId, awardedUserId, reason, eventPoints, quotaPoints, status)
        VALUES (?, ?, 0, ?, ?, ?, ?, ?, 'PENDING')
        """,
        (
            guildId,
            channelId,
            submitterId,
            awardedUserId,
            reason,
            eventPoints,
            quotaPoints,
        ),
    )


async def getPointAwardSubmission(submissionId: int) -> dict | None:
    return await fetchOne(
        """
        SELECT * FROM hg_point_awards WHERE id = ?
        """,
        (submissionId,),
    )

async def setPointAwardMessageId(submissionId: int, messageId: int) -> None:
    await execute(
        """
        UPDATE hg_point_awards SET messageId = ? WHERE id = ?
        """,
        (messageId, submissionId),
    )

async def updatePointAwardStatus(
    submissionId: int,
    status: str,
    reviewerId: int,
    note: str | None = None,
    threadId: int | None = None,
) -> None:
    await execute(
        """
        UPDATE hg_point_awards
        SET status = ?, reviewerId = ?, reviewNote = ?, reviewThreadId = ?
        WHERE id = ?
        """,
        (status, reviewerId, note, threadId, submissionId),
    )
########################################
#           SOLO SENTRY LOGS           #
########################################

async def soloSentryRequest(
        guildId: int,
        channelId: int,
        messageId: int,
        submitterId: int,
        startTime: int,
        endTime: int,
        evidenceAttachmentUrl1: str | None = None,
        evidenceAttachmentUrl2: str | None = None,
) -> int:
    return await executeReturnId(
        """
        INSERT INTO hg_sentry_logs
            (guildId, channelId, messageId, submitterId, startTime, endTime, evidenceAttachmentUrl1, evidenceAttachmentUrl2, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
        """,
        (
            guildId,
            channelId,
            messageId,
            submitterId,
            startTime,
            endTime,
            evidenceAttachmentUrl1,
            evidenceAttachmentUrl2,
        ),
    )

async def getSoloSentrySubmission(requestId: int) -> dict | None:
    return await fetchOne(
        """
        SELECT * FROM hg_sentry_logs WHERE id = ?
        """,
        (requestId,),
    )

async def setSoloSentryMessageId(requestId: int, messageId: int) -> None:
    await execute(
        """
        UPDATE hg_sentry_logs SET messageId = ? WHERE id = ?
        """,
        (messageId, requestId),
    )

async def updateSoloSentryStatus(
    requestId: int,
    status: str,
    reviewerId: int,
    points: int,
    note: str | None = None,
    threadId: int | None = None,
) -> None:
    await execute(
        """
        UPDATE hg_sentry_logs
        SET status = ?, reviewerId = ?, eventPoints = ?, reviewNote = ?, reviewThreadId = ?
        WHERE id = ?
        """,
        (status, reviewerId, points, note, threadId, requestId),
    )

########################################
#              EVENT LOGS              #
########################################

async def stringToList(inputString: str) -> list[str]:
    return [item.strip() for item in inputString.split(",") if item.strip()]

async def listToString(inputList: list[str]) -> str:
    return ",".join(inputList)

async def scheduleEvent(
    guildId: int,
    channelId: int,
    announcementMessageId: int,
    name: str,
    description: str,
    type: str,
    time: str,
    hostId: int,
    cohostsString: str,
    supervisorsString: str,
) -> int:
    eventId = await executeReturnId(
        """
        INSERT INTO hg_event(guildId, channelId, announcementMessageId, name, description, type, time, hostId, cohostsString, supervisorsString, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'SCHEDULED')
        """,
    (guildId, channelId, announcementMessageId, name, description, type, time, hostId, cohostsString, supervisorsString),
    )
    return eventId

async def getEventId(eventId: int) -> dict | None:
    return await fetchOne(
        """
        SELECT * FROM hg_event WHERE eventId = ?
        """,
        (eventId,),
    )

async def setAnnouncementMessageId(eventId: int, announcementMessageId: int) -> None:
    await execute(
        """
        UPDATE hg_event SET announcementMessageId = ? WHERE eventId = ?
        """,
        (announcementMessageId, eventId),
    )

async def updateEventStatus(
    eventId: int,
    status: str,
) -> None:
    await execute(
        """
        UPDATE hg_event
        SET status = ?
        WHERE eventId = ?
        """,
        (status, eventId),
    )

#########################################
#           EVENT ACTIVATION            #
#########################################

async def activateEvent(eventId: int, clockInMessageId: int) -> None:
    await execute(
        """
        UPDATE hg_event
        SET status = 'ACTIVE', clockInMessageId = ?, startTime = strftime('%s', 'now')
        WHERE eventId = ?
        """,
        (clockInMessageId, eventId),
    )

async def setClockInMessageId(eventId: int, clockInMessageId: int) -> None:
    await execute(
        """
        UPDATE hg_events SET clockInMessageId = ? WHERE eventId = ?
        """,
        (clockInMessageId, eventId),
    )

########################################
#          EVENT ARCHIVATION           #
########################################

async def archiveEvent(eventId: int) -> None:
    kwargs = await fetchOne(
        """        
        SELECT * FROM hg_events WHERE eventId = ?
        """,
        (eventId,),
    )
    await execute(
        """
        DELETE FROM hg_events WHERE eventId = ?
        """,
        (eventId,),
    )
    durationMinutes = round((kwargs['startTime'] - time.time()) / 60)
    await execute(
        """
        INSERT INTO hg_event_archive (eventId, name, description, type, time, hostId, cohostsString, supervisorsString, durationMinutes, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ARCHIVED')
        """,
        (kwargs['eventId'], kwargs['name'], kwargs['description'], kwargs['type'], kwargs['time'], kwargs['hostId'], kwargs['cohostsString'], kwargs['supervisorsString'], durationMinutes),
    )
