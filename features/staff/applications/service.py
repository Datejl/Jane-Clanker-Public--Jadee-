import json
from typing import Optional, Dict, List

from db.sqlite import execute, executeReturnId, fetchAll, fetchOne


def _jsonText(value, default):
    return json.dumps(value if value is not None else default)


def _isMissingColumnError(exc: Exception, columnName: str) -> bool:
    return f"no such column: {str(columnName or '').strip().lower()}" in str(exc).lower()


async def createDivisionApplication(
    guildId: int,
    divisionKey: str,
    applicantId: int,
    answers: Dict,
    proofMessageUrl: Optional[str],
    proofAttachments: List[str],
    reviewChannelId: int,
) -> Dict:
    applicationId = await executeReturnId(
        """
        INSERT INTO division_applications
            (guildId, divisionKey, applicantId, status, answersJson, proofMessageUrl, proofAttachmentsJson, reviewChannelId)
        VALUES (?, ?, ?, 'PENDING', ?, ?, ?, ?)
        """,
        (
            guildId,
            divisionKey,
            applicantId,
            _jsonText(answers, {}),
            proofMessageUrl or "",
            _jsonText(proofAttachments, []),
            reviewChannelId,
        ),
    )
    appCode = f"APP-{applicationId}"
    await execute(
        "UPDATE division_applications SET appCode = ? WHERE applicationId = ?",
        (appCode, applicationId),
    )
    await addApplicationEvent(applicationId, applicantId, "SUBMITTED", "")
    row = await getApplicationById(applicationId)
    if not row:
        raise RuntimeError("Failed to create application row.")
    return row


async def getApplicationById(applicationId: int) -> Optional[Dict]:
    return await fetchOne(
        "SELECT * FROM division_applications WHERE applicationId = ?",
        (applicationId,),
    )


async def getApplicationByCode(appCode: str) -> Optional[Dict]:
    return await fetchOne(
        "SELECT * FROM division_applications WHERE appCode = ?",
        (appCode,),
    )


async def setApplicationReviewMessage(
    applicationId: int,
    reviewChannelId: int,
    reviewMessageId: int,
) -> None:
    await execute(
        """
        UPDATE division_applications
        SET reviewChannelId = ?, reviewMessageId = ?, updatedAt = datetime('now')
        WHERE applicationId = ?
        """,
        (reviewChannelId, reviewMessageId, applicationId),
    )


async def updateApplicationAnswers(
    applicationId: int,
    answers: Dict,
) -> None:
    await execute(
        """
        UPDATE division_applications
        SET answersJson = ?, updatedAt = datetime('now')
        WHERE applicationId = ?
        """,
        (_jsonText(answers, {}), applicationId),
    )


async def setApplicationStatus(
    applicationId: int,
    status: str,
    reviewerId: Optional[int] = None,
    reviewNote: Optional[str] = None,
) -> None:
    normalizedStatus = str(status or "").strip().upper()
    finalized = normalizedStatus in {"APPROVED", "DENIED"}
    try:
        if finalized:
            await execute(
                """
                UPDATE division_applications
                SET status = ?, reviewerId = ?, reviewNote = ?,
                    reviewedAt = datetime('now'),
                    updatedAt = datetime('now'),
                    closedAt = datetime('now')
                WHERE applicationId = ?
                """,
                (normalizedStatus, reviewerId, reviewNote or "", applicationId),
            )
        else:
            await execute(
                """
                UPDATE division_applications
                SET status = ?, reviewerId = ?, reviewNote = ?,
                    updatedAt = datetime('now'),
                    closedAt = NULL
                WHERE applicationId = ?
                """,
                (normalizedStatus, reviewerId, reviewNote or "", applicationId),
            )
    except Exception as exc:
        if not (_isMissingColumnError(exc, "reviewedAt") or _isMissingColumnError(exc, "closedAt")):
            raise
        try:
            if finalized:
                await execute(
                    """
                    UPDATE division_applications
                    SET status = ?, reviewerId = ?, reviewNote = ?,
                        updatedAt = datetime('now'),
                        closedAt = datetime('now')
                    WHERE applicationId = ?
                    """,
                    (normalizedStatus, reviewerId, reviewNote or "", applicationId),
                )
            else:
                await execute(
                    """
                    UPDATE division_applications
                    SET status = ?, reviewerId = ?, reviewNote = ?,
                        updatedAt = datetime('now'),
                        closedAt = NULL
                    WHERE applicationId = ?
                    """,
                    (normalizedStatus, reviewerId, reviewNote or "", applicationId),
                )
        except Exception as fallbackExc:
            if not _isMissingColumnError(fallbackExc, "closedAt"):
                raise
            await execute(
                """
                UPDATE division_applications
                SET status = ?, reviewerId = ?, reviewNote = ?, updatedAt = datetime('now')
                WHERE applicationId = ?
                """,
                (normalizedStatus, reviewerId, reviewNote or "", applicationId),
            )


async def incrementReopenCount(applicationId: int) -> None:
    await execute(
        """
        UPDATE division_applications
        SET reopenedCount = COALESCE(reopenedCount, 0) + 1, updatedAt = datetime('now')
        WHERE applicationId = ?
        """,
        (applicationId,),
    )


async def addApplicationEvent(
    applicationId: int,
    actorId: Optional[int],
    eventType: str,
    details: str,
) -> None:
    await execute(
        """
        INSERT INTO division_application_events
            (applicationId, actorId, eventType, details)
        VALUES (?, ?, ?, ?)
        """,
        (applicationId, actorId, eventType, details or ""),
    )


async def listPendingApplications(divisionKey: Optional[str] = None) -> List[Dict]:
    if divisionKey:
        return await fetchAll(
            """
            SELECT *
            FROM division_applications
            WHERE divisionKey = ? AND status IN ('PENDING', 'NEEDS_INFO')
            ORDER BY createdAt DESC
            """,
            (divisionKey,),
        )
    return await fetchAll(
        """
        SELECT *
        FROM division_applications
        WHERE status IN ('PENDING', 'NEEDS_INFO')
        ORDER BY createdAt DESC
        """,
    )


async def listApplicationsForReviewViews() -> List[Dict]:
    return await fetchAll(
        """
        SELECT *
        FROM division_applications
        WHERE reviewMessageId IS NOT NULL
          AND status IN ('PENDING', 'NEEDS_INFO')
        ORDER BY createdAt DESC
        """
    )


async def listApplicationsForWorkflowReconciliation() -> List[Dict]:
    return await fetchAll(
        """
        SELECT *
        FROM division_applications
        WHERE status IN ('PENDING', 'NEEDS_INFO', 'APPROVED', 'DENIED')
        ORDER BY applicationId DESC
        """
    )


async def hasActiveApplication(guildId: int, divisionKey: str, applicantId: int) -> bool:
    row = await fetchOne(
        """
        SELECT applicationId
        FROM division_applications
        WHERE guildId = ? AND divisionKey = ? AND applicantId = ?
          AND status IN ('PENDING', 'NEEDS_INFO')
        LIMIT 1
        """,
        (guildId, divisionKey, applicantId),
    )
    return row is not None


async def activeApplicationCount(guildId: int, divisionKey: str, applicantId: int) -> int:
    row = await fetchOne(
        """
        SELECT COUNT(*) AS countValue
        FROM division_applications
        WHERE guildId = ? AND divisionKey = ? AND applicantId = ?
          AND status IN ('PENDING', 'NEEDS_INFO')
        """,
        (guildId, divisionKey, applicantId),
    )
    if not row:
        return 0
    return int(row.get("countValue") or 0)


async def lastApplicationTimestamp(guildId: int, divisionKey: str, applicantId: int) -> Optional[str]:
    row = await fetchOne(
        """
        SELECT createdAt
        FROM division_applications
        WHERE guildId = ? AND divisionKey = ? AND applicantId = ?
        ORDER BY createdAt DESC
        LIMIT 1
        """,
        (guildId, divisionKey, applicantId),
    )
    if not row:
        return None
    return row.get("createdAt")


async def statsForDivision(divisionKey: Optional[str], days: int) -> Dict[str, int]:
    days = max(1, int(days))
    timeExpr = f"-{days} days"
    params = (timeExpr,)
    whereDivision = ""
    if divisionKey:
        whereDivision = "AND divisionKey = ?"
        params = (timeExpr, divisionKey)

    rows = await fetchAll(
        f"""
        SELECT status, COUNT(*) AS countValue
        FROM division_applications
        WHERE datetime(createdAt) >= datetime('now', ?)
          {whereDivision}
        GROUP BY status
        """,
        params,
    )
    out = {"PENDING": 0, "NEEDS_INFO": 0, "APPROVED": 0, "DENIED": 0}
    for row in rows:
        status = str(row.get("status") or "")
        if status not in out:
            out[status] = 0
        out[status] = int(row.get("countValue") or 0)
    out["TOTAL"] = sum(value for key, value in out.items() if key != "TOTAL")
    return out


async def saveHubMessage(
    messageId: int,
    guildId: int,
    channelId: int,
    divisionKey: str,
) -> None:
    await execute(
        """
        INSERT OR REPLACE INTO division_hub_messages
            (messageId, guildId, channelId, divisionKey)
        VALUES (?, ?, ?, ?)
        """,
        (messageId, guildId, channelId, divisionKey),
    )


async def listHubMessages() -> List[Dict]:
    return await fetchAll(
        "SELECT * FROM division_hub_messages ORDER BY createdAt DESC",
    )


async def listHubMessagesForGuild(guildId: int) -> List[Dict]:
    return await fetchAll(
        """
        SELECT *
        FROM division_hub_messages
        WHERE guildId = ?
        ORDER BY createdAt DESC
        """,
        (int(guildId),),
    )


async def deleteHubMessage(messageId: int) -> None:
    await execute(
        "DELETE FROM division_hub_messages WHERE messageId = ?",
        (int(messageId),),
    )


async def getLatestHubMessageForDivision(guildId: int, divisionKey: str) -> Optional[Dict]:
    return await fetchOne(
        """
        SELECT *
        FROM division_hub_messages
        WHERE guildId = ? AND divisionKey = ?
        ORDER BY createdAt DESC
        LIMIT 1
        """,
        (guildId, divisionKey),
    )


async def listHubMessagesForDivision(guildId: int, divisionKey: str) -> List[Dict]:
    return await fetchAll(
        """
        SELECT *
        FROM division_hub_messages
        WHERE guildId = ? AND divisionKey = ?
        ORDER BY createdAt DESC
        """,
        (guildId, divisionKey),
    )


async def listActiveApplicationsForGuild(guildId: int) -> List[Dict]:
    return await fetchAll(
        """
        SELECT *
        FROM division_applications
        WHERE guildId = ?
          AND status IN ('PENDING', 'NEEDS_INFO')
        ORDER BY createdAt DESC
        """,
        (int(guildId),),
    )


def _divisionOpenSettingKey(guildId: int, divisionKey: str) -> str:
    return f"divisionAppsOpen:{guildId}:{divisionKey}"


async def isDivisionOpen(guildId: int, divisionKey: str) -> bool:
    row = await fetchOne(
        "SELECT value FROM bot_settings WHERE key = ?",
        (_divisionOpenSettingKey(guildId, divisionKey),),
    )
    if not row:
        return True
    return str(row.get("value") or "1").strip() != "0"


async def setDivisionOpen(guildId: int, divisionKey: str, isOpen: bool) -> None:
    await execute(
        """
        INSERT INTO bot_settings (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (_divisionOpenSettingKey(guildId, divisionKey), "1" if isOpen else "0"),
    )
