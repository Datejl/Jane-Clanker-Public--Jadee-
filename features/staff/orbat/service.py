from typing import Optional, List, Dict
from db.sqlite import fetchOne, fetchAll, execute, executeReturnId


async def createOrbatRequest(
    guildId: int,
    channelId: int,
    submitterId: int,
    robloxUser: str,
    mic: str,
    timezone: str,
    ageGroup: str,
    notes: Optional[str],
    inferredRank: Optional[str],
    inferredClearance: Optional[str],
    inferredDepartment: Optional[str],
) -> int:
    requestId = await executeReturnId(
        """
        INSERT INTO orbat_requests
        (guildId, channelId, submitterId, robloxUser, mic, timezone, ageGroup, notes,
         inferredRank, inferredClearance, inferredDepartment, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
        """,
        (
            guildId,
            channelId,
            submitterId,
            robloxUser,
            mic,
            timezone,
            ageGroup,
            notes or "",
            inferredRank,
            inferredClearance,
            inferredDepartment,
        ),
    )
    return requestId


async def getOrbatRequest(requestId: int) -> Optional[Dict]:
    return await fetchOne("SELECT * FROM orbat_requests WHERE requestId = ?", (requestId,))


async def setOrbatMessageId(requestId: int, messageId: int) -> None:
    await execute(
        "UPDATE orbat_requests SET messageId = ? WHERE requestId = ?",
        (messageId, requestId),
    )


async def updateOrbatStatus(
    requestId: int,
    status: str,
    reviewerId: Optional[int] = None,
    reviewNote: Optional[str] = None,
    sheetRow: Optional[int] = None,
    threadId: Optional[int] = None,
) -> None:
    await execute(
        """
        UPDATE orbat_requests
        SET status = ?, reviewedBy = ?, reviewedAt = datetime('now'),
            reviewNote = ?, sheetRow = COALESCE(?, sheetRow),
            threadId = COALESCE(?, threadId)
        WHERE requestId = ?
        """,
        (status, reviewerId, reviewNote, sheetRow, threadId, requestId),
    )


async def listOrbatPendingStatuses() -> List[Dict]:
    return await fetchAll(
        "SELECT * FROM orbat_requests WHERE status IN ('PENDING', 'NEEDS_INFO')",
    )


async def listOrbatRequestsForWorkflowReconciliation() -> List[Dict]:
    return await fetchAll(
        """
        SELECT *
        FROM orbat_requests
        ORDER BY datetime(reviewedAt) DESC, requestId DESC
        """
    )


async def createLoaRequest(
    guildId: int,
    channelId: int,
    submitterId: int,
    startDate: str,
    endDate: str,
    reason: Optional[str],
) -> int:
    requestId = await executeReturnId(
        """
        INSERT INTO loa_requests
        (guildId, channelId, submitterId, startDate, endDate, reason, status)
        VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
        """,
        (guildId, channelId, submitterId, startDate, endDate, reason or ""),
    )
    return requestId


async def getLoaRequest(requestId: int) -> Optional[Dict]:
    return await fetchOne("SELECT * FROM loa_requests WHERE requestId = ?", (requestId,))


async def setLoaMessageId(requestId: int, messageId: int) -> None:
    await execute(
        "UPDATE loa_requests SET messageId = ? WHERE requestId = ?",
        (messageId, requestId),
    )


async def updateLoaStatus(
    requestId: int,
    status: str,
    reviewerId: Optional[int] = None,
    reviewNote: Optional[str] = None,
    threadId: Optional[int] = None,
) -> None:
    await execute(
        """
        UPDATE loa_requests
        SET status = ?, reviewedBy = ?, reviewedAt = datetime('now'),
            reviewNote = ?, threadId = COALESCE(?, threadId)
        WHERE requestId = ?
        """,
        (status, reviewerId, reviewNote, threadId, requestId),
    )


async def listLoaPendingStatuses() -> List[Dict]:
    return await fetchAll(
        "SELECT * FROM loa_requests WHERE status IN ('PENDING', 'NEEDS_INFO')",
    )


async def listLoaRequestsForWorkflowReconciliation() -> List[Dict]:
    return await fetchAll(
        """
        SELECT *
        FROM loa_requests
        ORDER BY datetime(reviewedAt) DESC, requestId DESC
        """
    )
