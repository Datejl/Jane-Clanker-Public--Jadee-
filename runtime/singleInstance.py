from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import TextIO


def _readExistingPid(handle: TextIO) -> int:
    try:
        handle.seek(0)
        rawValue = str(handle.read() or "").strip()
        return int(rawValue) if rawValue else 0
    except (OSError, ValueError):
        return 0


def _lockHandle(handle: TextIO) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlockHandle(handle: TextIO) -> None:
    if os.name == "nt":
        import msvcrt

        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            return
        return

    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        return


class SingleInstanceLock:
    def __init__(self, lockPath: Path):
        self.lockPath = Path(lockPath)
        self._handle: TextIO | None = None
        self._ownerPid = 0
        self._windowsMutexHandle = None

    def _windowsMutexName(self) -> str:
        resolved = str(self.lockPath.resolve()).lower()
        digest = hashlib.sha1(resolved.encode("utf-8", errors="ignore")).hexdigest()
        return f"Local\\JaneClanker_{digest}"

    def acquire(self) -> tuple[bool, int]:
        if self._handle is not None:
            return True, int(self._ownerPid or 0)

        if os.name == "nt":
            import ctypes

            kernel32 = ctypes.windll.kernel32
            mutexName = self._windowsMutexName()
            handle = kernel32.CreateMutexW(None, False, mutexName)
            if not handle:
                return False, 0
            errorCode = int(kernel32.GetLastError() or 0)
            if errorCode == 183:
                kernel32.CloseHandle(handle)
                return False, 0
            self._windowsMutexHandle = handle
            self._ownerPid = int(os.getpid())
            return True, int(self._ownerPid or 0)

        self.lockPath.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lockPath.open("a+", encoding="utf-8")
        existingPid = _readExistingPid(handle)
        try:
            handle.seek(0)
            _lockHandle(handle)
        except OSError:
            handle.close()
            return False, int(existingPid or 0)

        ownerPid = int(os.getpid())
        handle.seek(0)
        handle.truncate(0)
        handle.write(str(ownerPid))
        handle.flush()
        self._handle = handle
        self._ownerPid = ownerPid
        return True, ownerPid

    def release(self) -> None:
        if os.name == "nt":
            handle = self._windowsMutexHandle
            self._windowsMutexHandle = None
            self._ownerPid = 0
            if handle:
                import ctypes

                ctypes.windll.kernel32.CloseHandle(handle)
            return

        handle = self._handle
        self._handle = None
        self._ownerPid = 0
        if handle is None:
            return
        try:
            handle.seek(0)
            handle.truncate(0)
            handle.flush()
        except OSError:
            pass
        _unlockHandle(handle)
        try:
            handle.close()
        except OSError:
            return
