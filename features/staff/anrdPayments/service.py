from __future__ import annotations

from typing import Optional, Dict, List

from db.sqlite import execute, executeReturnId, fetchOne, fetchAll


async def createPaymentRequest(
    guildId: int,
    channelId: int,
    submitterId: int,
    workSummary: str,
    proof: str,
    askingPrice: str,
) -> int:
    return await executeReturnId(
        """
        INSERT INTO anrd_payment_requests
            (guildId, channelId, submitterId, workSummary, proof, askingPrice, status)
        VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
        """,
        (
            int(guildId),
            int(channelId),
            int(submitterId),
            str(workSummary or "").strip(),
            str(proof or "").strip(),
            str(askingPrice or "").strip(),
        ),
    )


async def getPaymentRequest(requestId: int) -> Optional[Dict]:
    return await fetchOne(
        "SELECT * FROM anrd_payment_requests WHERE requestId = ?",
        (int(requestId),),
    )


async def setPaymentReviewMessage(
    requestId: int,
    reviewChannelId: int,
    reviewMessageId: int,
) -> None:
    await execute(
        """
        UPDATE anrd_payment_requests
        SET reviewChannelId = ?, reviewMessageId = ?, updatedAt = datetime('now')
        WHERE requestId = ?
        """,
        (int(reviewChannelId), int(reviewMessageId), int(requestId)),
    )


async def updatePaymentDecision(
    requestId: int,
    *,
    status: str,
    reviewerId: Optional[int],
    reviewNote: Optional[str],
    negotiatedPrice: Optional[str] = None,
) -> None:
    statusValue = str(status or "").strip().upper()
    finalized = statusValue in {"APPROVED", "DENIED"}
    if finalized:
        await execute(
            """
            UPDATE anrd_payment_requests
            SET status = ?,
                reviewerId = ?,
                reviewNote = ?,
                negotiatedPrice = ?,
                reviewedAt = datetime('now'),
                updatedAt = datetime('now')
            WHERE requestId = ?
            """,
            (
                statusValue,
                int(reviewerId) if reviewerId is not None else None,
                str(reviewNote).strip() if reviewNote else None,
                str(negotiatedPrice).strip() if negotiatedPrice else None,
                int(requestId),
            ),
        )
        return

    await execute(
        """
        UPDATE anrd_payment_requests
        SET status = ?,
            reviewerId = ?,
            reviewNote = ?,
            negotiatedPrice = COALESCE(?, negotiatedPrice),
            updatedAt = datetime('now')
        WHERE requestId = ?
        """,
        (
            statusValue,
            int(reviewerId) if reviewerId is not None else None,
            str(reviewNote).strip() if reviewNote else None,
            str(negotiatedPrice).strip() if negotiatedPrice else None,
            int(requestId),
        ),
    )


async def listOpenPaymentRequests() -> List[Dict]:
    return await fetchAll(
        """
        SELECT *
        FROM anrd_payment_requests
        WHERE status IN ('PENDING', 'NEGOTIATING', 'NEEDS_INFO')
          AND reviewMessageId IS NOT NULL
          AND reviewMessageId > 0
        """
    )


async def listPaymentRequestsForWorkflowReconciliation() -> List[Dict]:
    return await fetchAll(
        """
        SELECT *
        FROM anrd_payment_requests
        ORDER BY datetime(updatedAt) DESC, requestId DESC
        """
    )


async def applySubmitterFinalPriceDecision(
    requestId: int,
    *,
    accepted: bool,
    actorId: int,
    finalPrice: str,
) -> None:
    if accepted:
        await execute(
            """
            UPDATE anrd_payment_requests
            SET status = 'APPROVED',
                reviewNote = ?,
                reviewedAt = datetime('now'),
                updatedAt = datetime('now')
            WHERE requestId = ?
            """,
            (
                f"Submitter accepted final price {str(finalPrice).strip()}.",
                int(requestId),
            ),
        )
        return

    await execute(
        """
        UPDATE anrd_payment_requests
        SET status = 'NEGOTIATING',
            reviewNote = ?,
            updatedAt = datetime('now')
        WHERE requestId = ?
        """,
        (
            f"Submitter rejected final price {str(finalPrice).strip()}.",
            int(requestId),
        ),
    )


async def isPaymentPayoutSynced(requestId: int) -> bool:
    row = await fetchOne(
        "SELECT payoutSynced FROM anrd_payment_requests WHERE requestId = ?",
        (int(requestId),),
    )
    if not row:
        return False
    try:
        return int(row.get("payoutSynced") or 0) == 1
    except (TypeError, ValueError):
        return False


async def markPaymentPayoutSynced(requestId: int) -> None:
    await execute(
        """
        UPDATE anrd_payment_requests
        SET payoutSynced = 1,
            updatedAt = datetime('now')
        WHERE requestId = ?
        """,
        (int(requestId),),
    )
