from __future__ import annotations

import random
from datetime import datetime, timezone

from .common import trimRoundText

chamberCount = 6


def createState() -> dict:
    state = {
        "participants": [],
        "bulletCount": 1,
        "loadedChambers": [],
        "chamberCursor": 0,
        "shotsFired": 0,
        "mutedCount": 0,
        "lastVictimId": 0,
        "lastChamber": 0,
        "lastFired": False,
        "lastShotAt": "",
    }
    _reseedLoadedChambers(state)
    return state


def warningText() -> str:
    return ":warning: WARNING: If it fires, you are muted for 30 minutes."


def promptText() -> str:
    return (
        "Press **Configure** to set bullets, then **Pull Trigger**.\n"
        f"{warningText()}"
    )


def _sanitizeBulletCount(value: int) -> int:
    safeValue = int(value or 1)
    if safeValue < 1:
        return 1
    if safeValue > chamberCount:
        return chamberCount
    return safeValue


def _reseedLoadedChambers(state: dict) -> list[int]:
    bulletsLoaded = _sanitizeBulletCount(int(state.get("bulletCount") or 1))
    chamberPool = list(range(1, chamberCount + 1))
    chosen = sorted(random.sample(chamberPool, bulletsLoaded))
    state["loadedChambers"] = chosen
    return chosen


def _normalizeLoadedChambers(state: dict) -> list[int]:
    raw = state.get("loadedChambers") or []
    try:
        loaded = sorted({int(value) for value in raw if 1 <= int(value) <= chamberCount})
    except (TypeError, ValueError):
        loaded = []
    bulletsLoaded = bulletCount(state)
    if len(loaded) != bulletsLoaded:
        return _reseedLoadedChambers(state)
    state["loadedChambers"] = loaded
    return loaded


def bulletCount(state: dict) -> int:
    return _sanitizeBulletCount(int(state.get("bulletCount") or 1))


def setBulletCount(state: dict, value: int) -> int:
    safeBulletCount = _sanitizeBulletCount(value)
    state["bulletCount"] = safeBulletCount
    state["chamberCursor"] = 0
    _reseedLoadedChambers(state)
    return safeBulletCount


def cycleBulletCount(state: dict) -> int:
    current = bulletCount(state)
    nextValue = 1 if current >= chamberCount else current + 1
    return setBulletCount(state, nextValue)


def settingsText(state: dict) -> str:
    participants = list(state.get("participants") or [])
    bulletsLoaded = bulletCount(state)
    shotsFired = int(state.get("shotsFired") or 0)
    mutedCount = int(state.get("mutedCount") or 0)
    return (
        f"Bullets: `{bulletsLoaded}/{chamberCount}` | "
        f"Next Chamber: `{(int(state.get('chamberCursor') or 0) % chamberCount) + 1}/{chamberCount}` | "
        f"Participants: `{len(participants)}` | "
        f"Shots: `{shotsFired}` | "
        f"Mutes: `{mutedCount}` | "
        f"Penalty: `30m mute on fire`"
    )


def actionLabel() -> str:
    return "Pull Trigger"


def configLabel() -> str:
    return "Configure"


def quickLabel() -> str:
    return "Roster"


def isParticipant(state: dict, userId: int) -> bool:
    participants = set(int(value) for value in (state.get("participants") or []))
    return int(userId) in participants


def toggleParticipant(state: dict, userId: int) -> bool:
    safeUserId = int(userId)
    participants = [int(value) for value in (state.get("participants") or [])]
    participantSet = set(participants)
    if safeUserId in participantSet:
        participants = [value for value in participants if value != safeUserId]
        joined = False
    else:
        participants.append(safeUserId)
        joined = True
    state["participants"] = participants
    return joined


def rosterMentions(state: dict) -> str:
    participants = [int(value) for value in (state.get("participants") or [])]
    if not participants:
        return "(no participants)"
    return ", ".join(f"<@{userId}>" for userId in participants[:20])


def pullTrigger(state: dict, userId: int) -> dict:
    safeUserId = int(userId)
    if not isParticipant(state, safeUserId):
        state["participants"] = [*list(state.get("participants") or []), safeUserId]

    shotsFired = int(state.get("shotsFired") or 0) + 1
    cursor = int(state.get("chamberCursor") or 0) % chamberCount
    chamber = cursor + 1
    state["chamberCursor"] = (cursor + 1) % chamberCount
    bulletsLoaded = bulletCount(state)
    loadedChambers = _normalizeLoadedChambers(state)
    fired = chamber in loadedChambers
    state["shotsFired"] = shotsFired
    state["lastVictimId"] = safeUserId
    state["lastChamber"] = chamber
    state["lastFired"] = bool(fired)
    state["lastShotAt"] = datetime.now(timezone.utc).isoformat()
    if fired:
        state["mutedCount"] = int(state.get("mutedCount") or 0) + 1

    return {
        "victimId": safeUserId,
        "chamber": chamber,
        "fired": bool(fired),
        "bulletCount": bulletsLoaded,
    }


def formatShotResult(
    *,
    userId: int,
    chamber: int,
    bulletCount: int,
    fired: bool,
    timeoutApplied: bool = False,
    timeoutFailureReason: str = "",
) -> str:
    if not fired:
        outcome = f"<@{int(userId)}> pulled the trigger. **click.** No timeout applied."
    elif timeoutApplied:
        outcome = f"<@{int(userId)}> pulled the trigger. **BANG.** Timed out for 30 minutes."
    else:
        reason = timeoutFailureReason or "timeout failed"
        outcome = f"<@{int(userId)}> pulled the trigger. **BANG.** Could not apply timeout ({reason})."
    return trimRoundText(
        (
            f"Loaded: `{_sanitizeBulletCount(bulletCount)}/{chamberCount}` | Chamber: `{int(chamber)}/{chamberCount}`\n"
            f"{outcome}\n"
            f"{warningText()}"
        )
    )
