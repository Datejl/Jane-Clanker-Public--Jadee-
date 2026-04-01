from __future__ import annotations

import random

from .common import trimRoundText


def createState() -> dict:
    return {"mode": "Standard"}


def promptText() -> str:
    return "Place a bet, choose mode, then press **Roll**."


def settingsText(state: dict) -> str:
    return f"Mode: `{str(state.get('mode') or 'Standard')}`"


def actionLabel() -> str:
    return "Roll"


def configLabel(state: dict) -> str:
    return f"Mode: {str(state.get('mode') or 'Standard')}"


def quickLabel() -> str:
    return "Quick Pick"


def cycleConfig(state: dict) -> None:
    mode = str(state.get("mode") or "Standard")
    state["mode"] = "Press" if mode == "Standard" else "Standard"


def randomizeConfig(state: dict) -> None:
    state["mode"] = random.choice(["Standard", "Press"])


def resolveRound(state: dict) -> str:
    mode = "Press" if str(state.get("mode") or "").lower() == "press" else "Standard"
    maxRolls = 10 if mode == "Press" else 8

    dieA = random.randint(1, 6)
    dieB = random.randint(1, 6)
    comeOut = dieA + dieB
    lines = [f"Come-out: `{dieA}+{dieB}={comeOut}`"]

    if comeOut in {2, 3, 12}:
        lines.append("Come-out loss.")
        point = None
    elif comeOut in {7, 11}:
        point = random.choice([4, 5, 6, 8, 9, 10])
        lines.append(f"Point moved to `{point}`.")
    else:
        point = comeOut
        lines.append(f"Point set to `{point}`.")

    rollCount = 0
    touchedPoint = False
    while rollCount < maxRolls:
        d1 = random.randint(1, 6)
        d2 = random.randint(1, 6)
        total = d1 + d2
        rollCount += 1
        lines.append(f"Roll {rollCount}: `{d1}+{d2}={total}`")

        if point is None:
            if total == 7:
                lines.append("Round settled.")
                break
            continue

        if total == point:
            touchedPoint = True
            lines.append("Point hit, rolling continues.")
            continue
        if total == 7:
            lines.append("Seven-out.")
            break
    else:
        lines.append("Round timeout.")

    if touchedPoint and "Seven-out." not in lines:
        lines.append("Seven-out posted at close.")
    lines.append(f"Mode: **{mode}**")
    lines.append("House wins the round.")
    # Keep the full round log so roll numbering always starts at 1 for this interaction.
    return trimRoundText("\n".join(lines))
