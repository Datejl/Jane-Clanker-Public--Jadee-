import threading
import time

import config


_installLock = threading.Lock()
_isInstalled = False
_rateLock = threading.Lock()
_nextAllowedAt = 0.0


def _isRateLimitError(exc: Exception) -> bool:
    current: Exception | None = exc
    while current is not None:
        resp = getattr(current, "resp", None)
        status = getattr(resp, "status", None)
        if status == 429 or getattr(current, "status_code", None) == 429:
            return True
        current = current.__cause__ if isinstance(current.__cause__, Exception) else None
    return False


def _reserveRateSlot() -> None:
    global _nextAllowedAt
    minIntervalSec = float(getattr(config, "googleSheetsMinRequestIntervalSec", 0.05) or 0.05)
    if minIntervalSec <= 0:
        return
    with _rateLock:
        now = time.monotonic()
        waitSec = max(0.0, _nextAllowedAt - now)
        target = max(now, _nextAllowedAt) + minIntervalSec
        _nextAllowedAt = target
    if waitSec > 0:
        time.sleep(waitSec)


def installGoogleSheetsRuntime() -> None:
    global _isInstalled
    if _isInstalled:
        return
    with _installLock:
        if _isInstalled:
            return

        from googleapiclient.http import HttpRequest

        originalExecute = HttpRequest.execute

        def patchedExecute(request: HttpRequest, *args, **kwargs):
            maxAttempts = max(1, int(getattr(config, "googleSheetsMaxAttempts", 3) or 3))
            retryBaseSec = float(getattr(config, "googleSheetsRetryBaseSec", 1.5) or 1.5)
            for attempt in range(1, maxAttempts + 1):
                _reserveRateSlot()
                try:
                    return originalExecute(request, *args, **kwargs)
                except Exception as exc:
                    if not _isRateLimitError(exc) or attempt >= maxAttempts:
                        raise
                    time.sleep(retryBaseSec * attempt)
            return originalExecute(request, *args, **kwargs)

        HttpRequest.execute = patchedExecute
        _isInstalled = True

