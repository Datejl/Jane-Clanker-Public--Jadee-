from __future__ import annotations

import re
from typing import Optional

from db.sqlite import execute, fetchOne

defaultBalance = 1000
pointsToDollarRate = 5
_requestIdPattern = re.compile(r"^[A-Za-z0-9._:-]{1,96}$")


def _sanitizePositiveInt(value: object, fallback: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return int(fallback)
    return parsed if parsed > 0 else int(fallback)


def _sanitizeRequestId(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not _requestIdPattern.fullmatch(text):
        return ""
    return text


def _rowToWallet(row: Optional[dict], userId: int) -> dict:
    if not row:
        return {
            "userId": int(userId),
            "balance": int(defaultBalance),
            "gamesPlayed": 0,
            "totalLost": 0,
        }
    return {
        "userId": int(row.get("userId") or userId),
        "balance": _sanitizePositiveInt(row.get("balance"), 0),
        "gamesPlayed": max(0, int(row.get("gamesPlayed") or 0)),
        "totalLost": max(0, int(row.get("totalLost") or 0)),
    }


async def ensureWallet(userId: int) -> dict:
    safeUserId = int(userId)
    existing = await fetchOne(
        """
        SELECT userId, balance, gamesPlayed, totalLost
        FROM silly_gambling_wallets
        WHERE userId = ?
        """,
        (safeUserId,),
    )
    if existing:
        return _rowToWallet(existing, safeUserId)

    # Lazy-create wallet on first use so command handlers can call this
    # unconditionally without a separate "signup" step.
    await execute(
        """
        INSERT OR IGNORE INTO silly_gambling_wallets (userId, balance, gamesPlayed, totalLost, updatedAt)
        VALUES (?, ?, 0, 0, datetime('now'))
        """,
        (safeUserId, int(defaultBalance)),
    )
    created = await fetchOne(
        """
        SELECT userId, balance, gamesPlayed, totalLost
        FROM silly_gambling_wallets
        WHERE userId = ?
        """,
        (safeUserId,),
    )
    return _rowToWallet(created, safeUserId)


async def getWallet(userId: int) -> dict:
    return await ensureWallet(int(userId))


async def applyLossBet(userId: int, betAmount: int) -> dict | None:
    safeUserId = int(userId)
    stake = _sanitizePositiveInt(betAmount, 0)
    if stake <= 0:
        return None

    wallet = await ensureWallet(safeUserId)
    balance = int(wallet.get("balance") or 0)
    if balance < stake:
        return None

    # Bets are a pure debit path in this economy: spend reduces balance,
    # increments games played, and adds to lifetime loss totals.
    await execute(
        """
        UPDATE silly_gambling_wallets
        SET balance = balance - ?,
            gamesPlayed = gamesPlayed + 1,
            totalLost = totalLost + ?,
            updatedAt = datetime('now')
        WHERE userId = ?
        """,
        (stake, stake, safeUserId),
    )
    return await getWallet(safeUserId)


async def applyWalletCredit(userId: int, dollarAmount: int) -> dict | None:
    safeUserId = int(userId)
    payout = _sanitizePositiveInt(dollarAmount, 0)
    if payout <= 0:
        return None

    await ensureWallet(safeUserId)
    await execute(
        """
        UPDATE silly_gambling_wallets
        SET balance = balance + ?,
            updatedAt = datetime('now')
        WHERE userId = ?
        """,
        (payout, safeUserId),
    )
    return await getWallet(safeUserId)


async def applyWorkPayout(userId: int, payoutAmount: int = 10) -> dict | None:
    return await applyWalletCredit(userId, payoutAmount)


def pointsToDollars(points: int, conversionRate: int = pointsToDollarRate) -> int:
    safePoints = _sanitizePositiveInt(points, 0)
    safeRate = _sanitizePositiveInt(conversionRate, pointsToDollarRate)
    if safePoints <= 0 or safeRate <= 0:
        return 0
    # One-way conversion helper for external API credits.
    return int(safePoints * safeRate)


async def applyPointsCredit(
    userId: int,
    points: int,
    *,
    conversionRate: int = pointsToDollarRate,
) -> dict | None:
    dollars = pointsToDollars(points, conversionRate)
    if dollars <= 0:
        return None
    return await applyWalletCredit(userId, dollars)


async def getApiCreditRecord(requestId: str) -> dict | None:
    safeRequestId = _sanitizeRequestId(requestId)
    if not safeRequestId:
        return None
    return await fetchOne(
        """
        SELECT requestId, userId, points, directDollars, creditedDollars, conversionRate, source, createdAt
        FROM silly_gambling_api_credits
        WHERE requestId = ?
        """,
        (safeRequestId,),
    )


async def recordApiCreditRecord(
    *,
    requestId: str,
    userId: int,
    points: int,
    directDollars: int,
    creditedDollars: int,
    conversionRate: int,
) -> None:
    safeRequestId = _sanitizeRequestId(requestId)
    if not safeRequestId:
        return

    safeUserId = int(userId)
    safePoints = max(0, int(points or 0))
    safeDirectDollars = max(0, int(directDollars or 0))
    safeCreditedDollars = max(0, int(creditedDollars or 0))
    safeConversionRate = max(1, int(conversionRate or pointsToDollarRate))

    # Request IDs are treated as idempotency keys via INSERT OR IGNORE.
    await execute(
        """
        INSERT OR IGNORE INTO silly_gambling_api_credits
        (requestId, userId, points, directDollars, creditedDollars, conversionRate, source, createdAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            safeRequestId,
            safeUserId,
            safePoints,
            safeDirectDollars,
            safeCreditedDollars,
            safeConversionRate,
            "",
        ),
    )
