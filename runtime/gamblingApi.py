from __future__ import annotations

import asyncio
import errno
import hmac
import logging
import os
import re
import secrets
from typing import Any

from aiohttp import web

from silly import gamblingService

log = logging.getLogger(__name__)

_requestIdPattern = re.compile(r"^[A-Za-z0-9._:-]{1,96}$")


def _toPositiveInt(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _sanitizeRequestId(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not _requestIdPattern.fullmatch(text):
        return ""
    return text


class GamblingApiServer:
    def __init__(
        self,
        *,
        configModule: Any,
        metricsProvider: Any | None = None,
    ):
        self.config = configModule
        self.metricsProvider = metricsProvider
        self.app: web.Application | None = None
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self.started = False
        self.creditLock = asyncio.Lock()
        self.requestSemaphore = asyncio.Semaphore(self._maxConcurrency())

    def isEnabled(self) -> bool:
        return bool(getattr(self.config, "gamblingApiEnabled", False))

    def _host(self) -> str:
        return str(getattr(self.config, "gamblingApiHost", "127.0.0.1") or "127.0.0.1").strip()

    def _port(self) -> int:
        return max(1, int(getattr(self.config, "gamblingApiPort", 8787) or 8787))

    def _maxConcurrency(self) -> int:
        return max(1, int(getattr(self.config, "gamblingApiMaxConcurrency", 8) or 8))

    def _conversionRate(self) -> int:
        return max(1, int(getattr(self.config, "gamblingPointsToDollarRate", 5) or 5))

    def _configuredTokens(self) -> list[str]:
        tokens: list[str] = []

        primary = str(getattr(self.config, "gamblingApiToken", "") or "").strip()
        if primary:
            tokens.append(primary)

        extra = getattr(self.config, "gamblingApiTokens", []) or []
        if isinstance(extra, (list, tuple, set)):
            for value in extra:
                token = str(value or "").strip()
                if token:
                    tokens.append(token)

        envToken = str(os.getenv("JANE_GAMBLING_API_TOKEN", "") or "").strip()
        if envToken:
            tokens.append(envToken)

        unique: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            if token in seen:
                continue
            seen.add(token)
            unique.append(token)
        return unique

    def _allowedIpSet(self) -> set[str]:
        raw = getattr(self.config, "gamblingApiAllowedIps", []) or []
        if not isinstance(raw, (list, tuple, set)):
            return set()
        return {str(value).strip() for value in raw if str(value).strip()}

    def _extractProvidedToken(self, request: web.Request) -> str:
        authHeader = str(request.headers.get("Authorization", "") or "").strip()
        if authHeader.lower().startswith("bearer "):
            return authHeader[7:].strip()
        return str(request.headers.get("X-Jane-Api-Token", "") or "").strip()

    def _isAuthorized(self, request: web.Request) -> bool:
        validTokens = self._configuredTokens()
        if not validTokens:
            return False
        provided = self._extractProvidedToken(request)
        if not provided:
            return False
        return any(hmac.compare_digest(provided, token) for token in validTokens)

    def _isIpAllowed(self, request: web.Request) -> bool:
        allowedIpSet = self._allowedIpSet()
        if not allowedIpSet:
            return True
        remote = str(request.remote or "").strip()
        return bool(remote) and remote in allowedIpSet

    def _resolveRequestId(self, request: web.Request, body: dict | None = None) -> str:
        bodyData = body if isinstance(body, dict) else {}
        for candidate in (
            request.headers.get("X-Request-Id"),
            request.headers.get("Idempotency-Key"),
            bodyData.get("requestId"),
        ):
            safe = _sanitizeRequestId(candidate)
            if safe:
                return safe
        return f"req_{secrets.token_urlsafe(10)}"

    def _walletPayload(self, wallet: dict) -> dict[str, int]:
        return {
            "balance": int(wallet.get("balance") or 0),
            "gamesPlayed": int(wallet.get("gamesPlayed") or 0),
            "totalLost": int(wallet.get("totalLost") or 0),
        }

    def _creditMath(self, *, points: int, directDollars: int) -> tuple[int, int, int]:
        conversionRate = self._conversionRate()
        fromPointsDollars = gamblingService.pointsToDollars(points, conversionRate)
        creditedDollars = int(fromPointsDollars + directDollars)
        return conversionRate, fromPointsDollars, creditedDollars

    async def _json(self, requestId: str, payload: dict, *, status: int = 200) -> web.Response:
        envelope = {"requestId": requestId, **payload}
        return web.json_response(envelope, status=status)

    async def _error(
        self,
        requestId: str,
        *,
        code: str,
        message: str,
        status: int,
        extra: dict | None = None,
    ) -> web.Response:
        payload = {
            "ok": False,
            "error": {
                "code": code,
                "message": message,
            },
        }
        if isinstance(extra, dict):
            payload.update(extra)
        return await self._json(requestId, payload, status=status)

    async def _parseJsonBody(self, request: web.Request, requestId: str) -> tuple[dict | None, web.Response | None]:
        try:
            body = await request.json()
        except Exception:
            return None, await self._error(
                requestId,
                code="invalid-json",
                message="Request body must be valid JSON.",
                status=400,
            )
        if not isinstance(body, dict):
            return None, await self._error(
                requestId,
                code="invalid-body",
                message="Request body must be a JSON object.",
                status=400,
            )
        return body, None

    async def _authorizeRequest(self, request: web.Request, requestId: str) -> web.Response | None:
        if not self._isIpAllowed(request):
            return await self._error(
                requestId,
                code="ip-not-allowed",
                message="Caller IP is not allowed.",
                status=403,
            )
        if not self._isAuthorized(request):
            return await self._error(
                requestId,
                code="unauthorized",
                message="Missing or invalid API token.",
                status=401,
            )
        return None

    async def _health(self, request: web.Request) -> web.Response:
        requestId = self._resolveRequestId(request)
        authError = await self._authorizeRequest(request, requestId)
        if authError is not None:
            return authError

        return await self._json(
            requestId,
            {
                "ok": True,
                "service": "jane-gambling-api",
                "version": "v1",
                "conversionRate": self._conversionRate(),
                "maxConcurrency": self._maxConcurrency(),
            },
        )

    async def _runtimeMetrics(self, request: web.Request) -> web.Response:
        requestId = self._resolveRequestId(request)
        authError = await self._authorizeRequest(request, requestId)
        if authError is not None:
            return authError
        if self.metricsProvider is None:
            return await self._error(
                requestId,
                code="metrics-unavailable",
                message="Runtime metrics provider is not configured.",
                status=503,
            )
        try:
            metrics = await self.metricsProvider()
        except Exception as exc:
            return await self._error(
                requestId,
                code="metrics-error",
                message=f"Failed to collect runtime metrics ({exc.__class__.__name__}).",
                status=500,
            )
        return await self._json(
            requestId,
            {
                "ok": True,
                "metrics": metrics if isinstance(metrics, dict) else {},
            },
        )

    async def _convert(self, request: web.Request) -> web.Response:
        requestId = self._resolveRequestId(request)
        authError = await self._authorizeRequest(request, requestId)
        if authError is not None:
            return authError

        points = _toPositiveInt(request.query.get("points", 0))
        dollars = gamblingService.pointsToDollars(points, self._conversionRate())
        return await self._json(
            requestId,
            {
                "ok": True,
                "points": points,
                "conversionRate": self._conversionRate(),
                "dollars": dollars,
            },
        )

    async def _wallet(self, request: web.Request) -> web.Response:
        requestId = self._resolveRequestId(request)
        authError = await self._authorizeRequest(request, requestId)
        if authError is not None:
            return authError

        userId = _toPositiveInt(request.match_info.get("userId", 0))
        if userId <= 0:
            return await self._error(
                requestId,
                code="invalid-userId",
                message="userId must be a positive integer.",
                status=400,
            )

        async with self.requestSemaphore:
            wallet = await gamblingService.getWallet(userId)

        return await self._json(
            requestId,
            {
                "ok": True,
                "userId": userId,
                "wallet": self._walletPayload(wallet),
            },
        )

    def _requestIdConflict(self, existing: dict, *, userId: int, points: int, directDollars: int) -> bool:
        return any(
            (
                int(existing.get("userId") or 0) != int(userId),
                int(existing.get("points") or 0) != int(points),
                int(existing.get("directDollars") or 0) != int(directDollars),
                int(existing.get("conversionRate") or 0) != int(self._conversionRate()),
            )
        )

    async def _credit(self, request: web.Request) -> web.Response:
        provisionalRequestId = self._resolveRequestId(request)
        authError = await self._authorizeRequest(request, provisionalRequestId)
        if authError is not None:
            return authError

        body, bodyError = await self._parseJsonBody(request, provisionalRequestId)
        if bodyError is not None:
            return bodyError
        assert isinstance(body, dict)

        requestId = self._resolveRequestId(request, body)
        userId = _toPositiveInt(body.get("userId"))
        if userId <= 0:
            return await self._error(
                requestId,
                code="invalid-userId",
                message="userId must be a positive integer.",
                status=400,
            )

        points = _toPositiveInt(body.get("points"))
        directDollars = _toPositiveInt(body.get("dollars"))
        conversionRate, fromPointsDollars, creditedDollars = self._creditMath(
            points=points,
            directDollars=directDollars,
        )

        if creditedDollars <= 0:
            return await self._error(
                requestId,
                code="no-credit-amount",
                message="Provide positive points and/or dollars.",
                status=400,
            )

        idempotencyKey = _sanitizeRequestId(body.get("requestId") or request.headers.get("Idempotency-Key"))

        async with self.requestSemaphore:
            async with self.creditLock:
                if idempotencyKey:
                    existing = await gamblingService.getApiCreditRecord(idempotencyKey)
                    if existing is not None:
                        if self._requestIdConflict(
                            existing,
                            userId=userId,
                            points=points,
                            directDollars=directDollars,
                        ):
                            return await self._error(
                                requestId,
                                code="request-id-reused",
                                message="Idempotency key was already used with different parameters.",
                                status=409,
                            )

                        wallet = await gamblingService.getWallet(userId)
                        return await self._json(
                            requestId,
                            {
                                "ok": True,
                                "idempotent": True,
                                "requestKey": idempotencyKey,
                                "creditedDollars": int(existing.get("creditedDollars") or 0),
                                "fromPoints": int(existing.get("points") or 0),
                                "fromPointsDollars": gamblingService.pointsToDollars(
                                    int(existing.get("points") or 0),
                                    int(existing.get("conversionRate") or conversionRate),
                                ),
                                "fromDirectDollars": int(existing.get("directDollars") or 0),
                                "conversionRate": int(existing.get("conversionRate") or conversionRate),
                                "wallet": self._walletPayload(wallet),
                            },
                        )

                wallet = await gamblingService.applyWalletCredit(userId, creditedDollars)
                if not wallet:
                    return await self._error(
                        requestId,
                        code="wallet-update-failed",
                        message="Unable to update wallet.",
                        status=500,
                    )

                if idempotencyKey:
                    await gamblingService.recordApiCreditRecord(
                        requestId=idempotencyKey,
                        userId=userId,
                        points=points,
                        directDollars=directDollars,
                        creditedDollars=creditedDollars,
                        conversionRate=conversionRate,
                    )

        log.info(
            "Gambling API credit: userId=%s points=%s directDollars=%s creditedDollars=%s idempotencyKey=%s",
            userId,
            points,
            directDollars,
            creditedDollars,
            idempotencyKey or "none",
        )

        return await self._json(
            requestId,
            {
                "ok": True,
                "idempotent": False,
                "requestKey": idempotencyKey or "",
                "userId": userId,
                "creditedDollars": creditedDollars,
                "fromPoints": points,
                "fromPointsDollars": fromPointsDollars,
                "fromDirectDollars": directDollars,
                "conversionRate": conversionRate,
                "wallet": self._walletPayload(wallet),
            },
        )

    async def start(self) -> None:
        if self.started:
            return
        if not self.isEnabled():
            log.info("Gambling API disabled.")
            return
        if not self._configuredTokens():
            log.warning("Gambling API not started: no configured token.")
            return
        host = self._host()
        if host not in {"127.0.0.1", "::1", "localhost"} and not self._allowedIpSet():
            log.warning(
                "Gambling API is exposed on %s:%s with token-only auth (no IP allow-list).",
                host,
                self._port(),
            )

        self.requestSemaphore = asyncio.Semaphore(self._maxConcurrency())
        self.app = web.Application(client_max_size=32 * 1024)
        self.app.router.add_get("/api/gambling/health", self._health)
        self.app.router.add_get("/api/gambling/convert", self._convert)
        self.app.router.add_get("/api/gambling/wallet/{userId}", self._wallet)
        self.app.router.add_post("/api/gambling/credit", self._credit)
        self.app.router.add_get("/api/runtime/metrics", self._runtimeMetrics)

        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host=self._host(), port=self._port())
        try:
            await self.site.start()
        except OSError as exc:
            winError = int(getattr(exc, "winerror", 0) or 0)
            errNo = int(getattr(exc, "errno", 0) or 0)
            portInUseErrnos = {int(errno.EADDRINUSE), 48, 98, 10048}
            portInUse = winError == 10048 or errNo in portInUseErrnos
            try:
                await self.runner.cleanup()
            except Exception:
                pass
            self.site = None
            self.runner = None
            self.app = None
            if portInUse:
                log.warning(
                    "Gambling API not started: %s:%s is already in use.",
                    self._host(),
                    self._port(),
                )
                return
            raise
        self.started = True
        log.info(
            "Gambling API started on http://%s:%s (maxConcurrency=%s)",
            self._host(),
            self._port(),
            self._maxConcurrency(),
        )

    async def stop(self) -> None:
        if not self.started:
            return
        try:
            if self.site is not None:
                await self.site.stop()
        finally:
            self.site = None
        try:
            if self.runner is not None:
                await self.runner.cleanup()
        finally:
            self.runner = None
            self.app = None
            self.started = False
        log.info("Gambling API stopped.")
