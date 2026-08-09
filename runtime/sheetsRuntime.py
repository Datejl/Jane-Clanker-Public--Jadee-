import threading
import time

import config
from runtime import transientNetwork


_installLock = threading.Lock()
_isInstalled = False
_rateLock = threading.Lock()
_nextAllowedAt = 0.0


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
                    if not transientNetwork.isRetryableHttpOrNetworkError(exc) or attempt >= maxAttempts:
                        raise
                    time.sleep(retryBaseSec * attempt)
            return originalExecute(request, *args, **kwargs)

        HttpRequest.execute = patchedExecute
        _isInstalled = True
