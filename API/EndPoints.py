from __future__ import annotations

import hmac
import logging
from typing import Any

import discord
from quart import Quart, Response, jsonify, request

from features.staff.sessions.service import attemptClockIn
from features.staff.sessions.viewRuntime import requestSessionMessageUpdate


log = logging.getLogger(__name__)

_apiBotClient: discord.Client | None = None
_apiToken = ""


def configureApi(*, botClient: discord.Client | None, token: str) -> None:
    global _apiBotClient, _apiToken
    _apiBotClient = botClient
    _apiToken = str(token or "").strip()


def validateToken(requestToken: object) -> bool:
    candidate = str(requestToken or "").strip()
    return bool(_apiToken and candidate and hmac.compare_digest(candidate, _apiToken))


def _positiveInt(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def parseClockInPayload(requestData: object) -> tuple[int, str, int] | None:
    if not isinstance(requestData, dict):
        return None

    sessionId = _positiveInt(requestData.get("sessionId"))
    userId = _positiveInt(requestData.get("sessionUserId"))
    rawPassword = requestData.get("sessionPassword")
    password = rawPassword if isinstance(rawPassword, str) else ""
    if sessionId <= 0 or userId <= 0 or not password:
        return None
    return sessionId, password, userId


async def enterOrientation() -> Response:
    if not validateToken(request.headers.get("X-API-TOKEN")):
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    requestData = await request.get_json(silent=True)
    parsed = parseClockInPayload(requestData)
    if parsed is None:
        return jsonify({"ok": False, "error": "invalid-request"}), 400
    sessionId, password, userId = parsed

    if _apiBotClient is None:
        return jsonify({"ok": False, "error": "discord-client-unavailable"}), 503

    clockInResult = await attemptClockIn(sessionId, userId, password)
    resultText = str(clockInResult.get("status") or "").upper()
    responseBody: dict[str, Any] = {
        "ok": resultText == "ADDED",
        "result": resultText,
        "attendees": clockInResult.get("attendeeCount"),
    }

    if resultText == "ADDED":
        try:
            await requestSessionMessageUpdate(
                bot=_apiBotClient,
                sessionId=sessionId,
                delaySec=0.5,
            )
        except Exception:
            log.exception(
                "Orientation API could not schedule a message refresh for session %s.",
                sessionId,
            )
        return jsonify(responseBody), 200

    # Preserve the legacy endpoint's non-success status for existing clients.
    return jsonify(responseBody), 418


def registerRoutes(app: Quart) -> None:
    if "enterOrientation" not in app.view_functions:
        app.add_url_rule(
            "/enterOrientation",
            endpoint="enterOrientation",
            view_func=enterOrientation,
            methods=["POST"],
        )
