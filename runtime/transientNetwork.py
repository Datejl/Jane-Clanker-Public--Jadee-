from __future__ import annotations

import http.client
import socket
import ssl
from collections.abc import Iterable

try:
    import aiohttp
except Exception:  # pragma: no cover - aiohttp is expected with discord.py.
    aiohttp = None  # type: ignore[assignment]


_RATE_LIMIT_HTTP_STATUSES = frozenset({429})
_TRANSIENT_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
_TRANSIENT_NETWORK_TEXT = (
    "temporary failure in name resolution",
    "could not resolve host",
    "name or service not known",
    "nodename nor servname provided",
    "no address associated with hostname",
    "cannot connect to host",
    "could not connect to server",
    "failed to connect to",
    "network is unreachable",
    "connection timed out",
    "operation timed out",
    "timed out",
    "timeout",
    "connection refused",
    "connection reset by peer",
    "connection reset",
    "connection aborted",
    "connection was aborted",
    "server disconnected",
    "session is closed",
    "remote end closed connection",
    "unexpected eof",
    "temporarily unavailable",
    "incomplete read",
    "incompleteread",
    "chunked",
    "chunk",
    "bad record mac",
    "wrong version number",
    "cipher operation failed",
    "tls",
    "ssl",
    "temporary failure resolving",
)
_TRANSIENT_EXCEPTION_CLASS_NAMES = {
    "BadStatusLine",
    "ClientConnectorDNSError",
    "ClientConnectorError",
    "IncompleteRead",
    "RemoteDisconnected",
    "ServerDisconnectedError",
}


def walkExceptionChain(exc: BaseException | None) -> Iterable[BaseException]:
    seen: set[int] = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _coerceHttpStatus(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def exceptionHasHttpStatus(exc: BaseException | None, statuses: Iterable[int]) -> bool:
    wanted = frozenset(int(status) for status in statuses)
    if not wanted:
        return False
    for current in walkExceptionChain(exc):
        resp = getattr(current, "resp", None)
        candidates = (
            getattr(resp, "status", None),
            getattr(current, "status_code", None),
            getattr(current, "status", None),
        )
        if any(_coerceHttpStatus(candidate) in wanted for candidate in candidates):
            return True
    return False


def exceptionHasRateLimitHttpStatus(exc: BaseException | None) -> bool:
    return exceptionHasHttpStatus(exc, _RATE_LIMIT_HTTP_STATUSES)


def exceptionHasRetryableHttpStatus(exc: BaseException | None) -> bool:
    return exceptionHasHttpStatus(exc, _TRANSIENT_HTTP_STATUSES)


def isRetryableHttpOrNetworkError(
    exc: BaseException | None,
    *,
    includeTransport: bool = True,
) -> bool:
    if exceptionHasRetryableHttpStatus(exc):
        return True
    return includeTransport and isLikelyTransientNetworkError(exc)


def isLikelyTransientNetworkError(exc: BaseException | None) -> bool:
    for current in walkExceptionChain(exc):
        if aiohttp is not None and isinstance(current, aiohttp.ClientError):
            return True
        if isinstance(
            current,
            (
                http.client.IncompleteRead,
                http.client.RemoteDisconnected,
                http.client.BadStatusLine,
                ssl.SSLError,
                socket.gaierror,
                socket.timeout,
                TimeoutError,
                ConnectionError,
            ),
        ):
            return True
        if current.__class__.__name__ in _TRANSIENT_EXCEPTION_CLASS_NAMES:
            return True
        text = str(current).lower()
        if any(pattern in text for pattern in _TRANSIENT_NETWORK_TEXT):
            return True
    return False


def textLooksLikeTransientNetworkError(value: object) -> bool:
    text = str(value or "").lower()
    return any(pattern in text for pattern in _TRANSIENT_NETWORK_TEXT)


def textLooksLikeRateLimitError(value: object) -> bool:
    text = str(value or "").upper()
    return "RATE_LIMIT_EXCEEDED" in text or "QUOTA EXCEEDED" in text
