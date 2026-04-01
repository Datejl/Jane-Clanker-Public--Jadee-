from __future__ import annotations

import random
from typing import Callable

from .common import trimRoundText

rouletteRed = {
    1, 3, 5, 7, 9, 12, 14, 16, 18,
    19, 21, 23, 25, 27, 30, 32, 34, 36,
}
rouletteBaseBets: list[tuple[str, str, Callable[[int], bool]]] = [
    ("Red", "pays 1:1", lambda n: n in rouletteRed),
    ("Black", "pays 1:1", lambda n: n != 0 and n not in rouletteRed),
    ("Even", "pays 1:1", lambda n: n != 0 and n % 2 == 0),
    ("Odd", "pays 1:1", lambda n: n % 2 == 1),
    ("1-18", "pays 1:1", lambda n: 1 <= n <= 18),
    ("19-36", "pays 1:1", lambda n: n >= 19),
    ("Dozen 1", "pays 2:1", lambda n: 1 <= n <= 12),
    ("Dozen 2", "pays 2:1", lambda n: 13 <= n <= 24),
    ("Dozen 3", "pays 2:1", lambda n: 25 <= n <= 36),
]


def createState() -> dict:
    options = _buildOptions()
    return {"options": options, "index": 0}


def promptText() -> str:
    return "Place a bet, pick roulette bet type, then press **Spin**."


def settingsText(state: dict) -> str:
    return f"Bet type: `{currentBet(state)[0]}`"


def actionLabel() -> str:
    return "Spin"


def configLabel(state: dict) -> str:
    return f"Bet: {currentBet(state)[0][:28]}"


def quickLabel() -> str:
    return "Quick Pick"


def _buildOptions() -> list[tuple[str, str, Callable[[int], bool]]]:
    straight = random.randint(0, 36)
    column = random.randint(1, 3)
    splitBase = random.randint(1, 35)
    if splitBase % 3 == 0:
        splitBase -= 1
    splitPair = (splitBase, splitBase + 1)
    dynamic = [
        (f"Straight {straight}", "pays 35:1", lambda n, target=straight: n == target),
        (
            f"Column {column}",
            "pays 2:1",
            lambda n, col=column: n != 0 and ((n - 1) % 3) + 1 == col,
        ),
        (
            f"Split {splitPair[0]}/{splitPair[1]}",
            "pays 17:1",
            lambda n, pair=splitPair: n in pair,
        ),
    ]
    return [*rouletteBaseBets, *dynamic]


def currentBet(state: dict) -> tuple[str, str, Callable[[int], bool]]:
    options = state.get("options") or _buildOptions()
    state["options"] = options
    idx = int(state.get("index") or 0)
    idx = max(0, min(idx, len(options) - 1))
    state["index"] = idx
    return options[idx]


def cycleConfig(state: dict) -> None:
    options = state.get("options") or _buildOptions()
    idx = int(state.get("index") or 0)
    idx = (idx + 1) % len(options)
    state["options"] = options
    state["index"] = idx


def randomizeConfig(state: dict) -> None:
    options = _buildOptions()
    state["options"] = options
    state["index"] = random.randint(0, len(options) - 1)


def _color(number: int) -> str:
    if number == 0:
        return "Green"
    return "Red" if number in rouletteRed else "Black"


def resolveRound(state: dict) -> str:
    betName, betPayout, betFn = currentBet(state)

    pocket = random.randint(0, 36)
    startPocket = pocket
    track = [pocket]
    momentum = random.randint(3, 8)
    for _ in range(random.randint(8, 15)):
        move = max(1, random.randint(1, momentum))
        pocket = (pocket + move) % 37
        momentum = max(1, momentum - random.choice([0, 1]))
        track.append(pocket)

    finalSpin = track[-1]
    if betFn(finalSpin):
        step = random.choice([-1, 1])
        for jump in range(1, 37):
            candidate = (finalSpin + (step * jump)) % 37
            if not betFn(candidate):
                finalSpin = candidate
                track[-1] = finalSpin
                break

    return trimRoundText(
        (
            f"Bet: **{betName}** ({betPayout})\n"
            f"Start pocket: `{startPocket}`\n"
            f"Track: `{' -> '.join(str(x) for x in track[-5:])}`\n"
            f"Result: **{finalSpin} {_color(finalSpin)}**\n"
            "Spin complete."
        )
    )
