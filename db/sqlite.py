import aiosqlite
import asyncio
import logging
import os
import sqlite3
from pathlib import Path
from typing import Awaitable, Callable, Optional, TypeVar

from . import schema as sqliteSchema

# JANE_DB_PATH lets the test bot point at a separate SQLite file for testing
_repoRoot = Path(__file__).resolve().parent.parent
_dbPathOverride = (os.getenv("JANE_DB_PATH") or "").strip()
if _dbPathOverride:
    _dbPathCandidate = Path(_dbPathOverride)
    if not _dbPathCandidate.is_absolute():
        _dbPathCandidate = _repoRoot / _dbPathCandidate
    dbPath = str(_dbPathCandidate)
else:
    dbPath = str(_repoRoot / "bot.db")
_dbConn: Optional[aiosqlite.Connection] = None
_dbConnInitLock = asyncio.Lock()
_dbOperationLock = asyncio.Lock()
_dbWriteLock = asyncio.Lock()
log = logging.getLogger(__name__)
_schemaVersionTarget = sqliteSchema.SCHEMA_VERSION
_T = TypeVar("_T")
_sqliteBusyTimeoutMs = 60_000
_sqliteLockRetryDelaysSec = (0.25, 0.75, 1.5, 3.0, 5.0)


async def _prepareConnection(db: aiosqlite.Connection) -> None:
    # Connection-scoped pragmas
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.execute(f"PRAGMA busy_timeout={_sqliteBusyTimeoutMs};")


def _isDatabaseLocked(exc: BaseException) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return "database is locked" in message or "database table is locked" in message


def _querySummary(query: str) -> str:
    return " ".join(str(query or "").split())[:180]


async def _rollbackQuietly(db: aiosqlite.Connection) -> None:
    try:
        await db.rollback()
    except Exception:
        log.debug("SQLite rollback after failed operation also failed.", exc_info=True)


async def _runWithLockedDatabaseRetries(
    operationName: str,
    query: str,
    callback: Callable[[], Awaitable[_T]],
    *,
    rollback: Callable[[], Awaitable[None]] | None = None,
) -> _T:
    maxAttempts = len(_sqliteLockRetryDelaysSec) + 1
    for attempt in range(maxAttempts):
        try:
            return await callback()
        except sqlite3.OperationalError as exc:
            if rollback is not None:
                await rollback()
            if not _isDatabaseLocked(exc) or attempt >= maxAttempts - 1:
                raise
            delaySec = _sqliteLockRetryDelaysSec[attempt]
            log.warning(
                "SQLite %s hit a locked database; retrying in %.2fs (%d/%d). query=%s",
                operationName,
                delaySec,
                attempt + 1,
                maxAttempts - 1,
                _querySummary(query),
            )
            await asyncio.sleep(delaySec)
    raise RuntimeError("unreachable sqlite retry state")


async def _getConnection() -> aiosqlite.Connection:
    global _dbConn
    if _dbConn is not None:
        return _dbConn
    async with _dbConnInitLock:
        if _dbConn is not None:
            return _dbConn
        db = await aiosqlite.connect(dbPath, timeout=_sqliteBusyTimeoutMs / 1000)
        try:
            await _prepareConnection(db)
        except BaseException:
            await db.close()
            raise
        db.row_factory = aiosqlite.Row
        _dbConn = db
        return _dbConn

async def _initializeSchema() -> None:
    db = await _getConnection()
    async with _dbWriteLock:
        await sqliteSchema.applySchema(
            db,
            logger=log,
        )


async def initDb() -> None:
    """Initialize Jane's schema, retrying transient SQLite lock failures safely."""

    async def _initialize() -> None:
        try:
            await _initializeSchema()
        except BaseException:
            db = _dbConn
            if db is not None:
                await _rollbackQuietly(db)
            raise

    await _runWithLockedDatabaseRetries(
        "schema initialization",
        "Jane SQLite schema",
        _initialize,
    )


async def _runCommittedWrite(
    operationName: str,
    query: str,
    callback: Callable[[aiosqlite.Connection], Awaitable[_T]],
) -> _T:
    async def _write() -> _T:
        async with _dbOperationLock:
            db = await _getConnection()
            async with _dbWriteLock:
                try:
                    result = await callback(db)
                    await db.commit()
                    return result
                except BaseException:
                    await _rollbackQuietly(db)
                    raise

    return await _runWithLockedDatabaseRetries(operationName, query, _write)

async def fetchOne(query: str, params: tuple = ()):
    async def _fetch() -> Optional[dict]:
        async with _dbOperationLock:
            db = await _getConnection()
            async with db.execute(query, params) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    return await _runWithLockedDatabaseRetries("fetchOne", query, _fetch)

async def fetchAll(query: str, params: tuple = ()):
    async def _fetch() -> list[dict]:
        async with _dbOperationLock:
            db = await _getConnection()
            async with db.execute(query, params) as cur:
                rows = await cur.fetchall()
                return [dict(r) for r in rows]

    return await _runWithLockedDatabaseRetries("fetchAll", query, _fetch)

async def execute(query: str, params: tuple = ()):
    async def _execute(db: aiosqlite.Connection) -> None:
        cursor = await db.execute(query, params)
        await cursor.close()

    return await _runCommittedWrite("execute", query, _execute)

async def executeReturnId(query: str, params: tuple = ()) -> int:
    async def _execute(db: aiosqlite.Connection) -> int:
        cursor = await db.execute(query, params)
        try:
            return cursor.lastrowid
        finally:
            await cursor.close()

    return await _runCommittedWrite("executeReturnId", query, _execute)

async def executeMany(query: str, paramsSeq: list[tuple]) -> None:
    if not paramsSeq:
        return

    async def _execute(db: aiosqlite.Connection) -> None:
        cursor = await db.executemany(query, paramsSeq)
        await cursor.close()

    return await _runCommittedWrite("executeMany", query, _execute)


async def runWriteTransaction(callback: Callable[[aiosqlite.Connection], Awaitable[_T]]) -> _T:
    async def _write() -> _T:
        async with _dbOperationLock:
            db = await _getConnection()
            async with _dbWriteLock:
                try:
                    await db.execute("BEGIN IMMEDIATE")
                    result = await callback(db)
                    await db.commit()
                    return result
                except BaseException:
                    await _rollbackQuietly(db)
                    raise

    return await _runWithLockedDatabaseRetries("runWriteTransaction", "BEGIN IMMEDIATE", _write)

async def closeDb() -> None:
    global _dbConn
    async with _dbOperationLock:
        if _dbConn is None:
            return
        async with _dbConnInitLock:
            if _dbConn is None:
                return
            await _dbConn.close()
            _dbConn = None
