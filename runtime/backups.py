from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db import sqlite as sqliteDb


def _safeName(value: str) -> str:
    cleaned = "".join(ch for ch in str(value or "").strip() if ch.isalnum() or ch in {"-", "_"})
    return cleaned[:40] if cleaned else "manual"


def _backupDir(configModule: Any) -> Path:
    configured = str(getattr(configModule, "dbBackupDir", "") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return (Path(__file__).resolve().parent.parent / "backups").resolve()


def _dbPath() -> Path:
    return Path(sqliteDb.dbPath).resolve()


def _nowStamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _makeBackupFilePath(configModule: Any, label: str) -> Path:
    directory = _backupDir(configModule)
    directory.mkdir(parents=True, exist_ok=True)
    fileName = f"bot_{_nowStamp()}_{_safeName(label)}.db"
    return directory / fileName


def _sqliteBackupFile(srcPath: Path, dstPath: Path) -> None:
    srcConn = sqlite3.connect(str(srcPath))
    dstConn = sqlite3.connect(str(dstPath))
    try:
        srcConn.backup(dstConn)
    finally:
        dstConn.close()
        srcConn.close()


async def createBackup(configModule: Any, *, label: str = "manual") -> Path:
    src = _dbPath()
    dst = _makeBackupFilePath(configModule, label)
    await sqliteDb.closeDb()
    await sqliteDb._dbConnInitLock.acquire()  # type: ignore[attr-defined]
    try:
        _sqliteBackupFile(src, dst)
    finally:
        sqliteDb._dbConnInitLock.release()  # type: ignore[attr-defined]
    await sqliteDb.initDb()
    return dst


async def listBackups(configModule: Any, *, limit: int = 25) -> list[Path]:
    directory = _backupDir(configModule)
    if not directory.exists():
        return []
    files = [path for path in directory.glob("*.db") if path.is_file()]
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return files[: max(1, min(200, int(limit or 25)))]


async def restoreBackup(configModule: Any, *, backupFileName: str) -> Path:
    directory = _backupDir(configModule)
    source = (directory / backupFileName).resolve()
    if not source.exists() or not source.is_file():
        raise FileNotFoundError(f"Backup file not found: {backupFileName}")
    if source.parent != directory:
        raise ValueError("Invalid backup path")

    target = _dbPath()
    safetyCopy = _makeBackupFilePath(configModule, "pre_restore")

    await sqliteDb.closeDb()
    await sqliteDb._dbConnInitLock.acquire()  # type: ignore[attr-defined]
    try:
        if target.exists():
            shutil.copy2(target, safetyCopy)
        shutil.copy2(source, target)
        walPath = target.with_suffix(target.suffix + "-wal")
        shmPath = target.with_suffix(target.suffix + "-shm")
        if walPath.exists():
            walPath.unlink(missing_ok=True)
        if shmPath.exists():
            shmPath.unlink(missing_ok=True)
    finally:
        sqliteDb._dbConnInitLock.release()  # type: ignore[attr-defined]
    await sqliteDb.initDb()
    return target
