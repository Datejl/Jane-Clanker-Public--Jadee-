from __future__ import annotations

import random

from .common import trimRoundText

slotSymbols = ["7", "BAR", "Cherry", "Lemon", "Bell", "Diamond"]
slotWeights = [2, 5, 12, 16, 10, 8]
slotDisplayBySymbol = {
    "7": "7",
    "BAR": ":slot_machine:",
    "Cherry": ":cherries:",
    "Lemon": ":lemon:",
    "Bell": ":bell:",
    "Diamond": ":gem:",
}
slotPaylines: list[tuple[str, tuple[int, int, int]]] = [
    ("Top", (0, 0, 0)),
    ("Middle", (1, 1, 1)),
    ("Bottom", (2, 2, 2)),
    ("Diag TL-BR", (0, 1, 2)),
    ("Diag BL-TR", (2, 1, 0)),
]
slotLineSets: dict[int, list[tuple[str, tuple[int, int, int]]]] = {
    1: [("Middle", (1, 1, 1))],
    3: [("Top", (0, 0, 0)), ("Middle", (1, 1, 1)), ("Bottom", (2, 2, 2))],
    5: slotPaylines,
}


def createState() -> dict:
    return {"lineCount": 3}


def promptText() -> str:
    return "Place a bet, set line count, then press **Spin**."


def settingsText(state: dict) -> str:
    lineCount = int(state.get("lineCount") or 3)
    return f"Lines: `{lineCount}`"


def actionLabel() -> str:
    return "Spin"


def configLabel(state: dict) -> str:
    return f"Lines: {int(state.get('lineCount') or 3)}"


def quickLabel() -> str:
    return "Quick Pick"


def cycleConfig(state: dict) -> None:
    lineCount = int(state.get("lineCount") or 3)
    state["lineCount"] = {1: 3, 3: 5, 5: 1}.get(lineCount, 3)


def randomizeConfig(state: dict) -> None:
    state["lineCount"] = random.choice([1, 3, 5])


def _buildBoard() -> list[list[str]]:
    return [random.choices(slotSymbols, weights=slotWeights, k=3) for _ in range(3)]


def _lineSymbols(board: list[list[str]], pattern: tuple[int, int, int]) -> list[str]:
    return [board[colIdx][rowIdx] for colIdx, rowIdx in enumerate(pattern)]


def _isWin(line: list[str]) -> bool:
    first, second, third = line
    if first == second == third:
        return True
    if first == "Cherry" and second == "Cherry":
        return True
    if line.count("7") >= 2 and line.count("BAR") >= 1:
        return True
    return False


def _isNearMiss(line: list[str]) -> bool:
    first, second, third = line
    if _isWin(line):
        return False
    if first == second or second == third or first == third:
        return True
    if line.count("7") >= 2:
        return True
    return False


def resolveRound(state: dict) -> str:
    lineCount = int(state.get("lineCount") or 3)
    if lineCount not in slotLineSets:
        lineCount = 3
    activeLines = slotLineSets[lineCount]
    board = _buildBoard()

    settleGuard = 0
    while settleGuard < 24:
        settleGuard += 1
        changed = False
        for _, pattern in activeLines:
            line = _lineSymbols(board, pattern)
            if not _isWin(line):
                continue

            candidateCols = [0, 1, 2]
            if line[0] == "Cherry" and line[1] == "Cherry":
                candidateCols = [0, 1]
            col = random.choice(candidateCols)
            row = pattern[col]
            current = board[col][row]
            alternatives = [s for s in slotSymbols if s != current]
            random.shuffle(alternatives)
            for symbol in alternatives:
                board[col][row] = symbol
                if not _isWin(_lineSymbols(board, pattern)):
                    changed = True
                    break

        if not any(_isWin(_lineSymbols(board, pattern)) for _, pattern in activeLines):
            break
        if not changed:
            break

    top = " | ".join(slotDisplayBySymbol.get(board[col][0], board[col][0]) for col in range(3))
    mid = " | ".join(slotDisplayBySymbol.get(board[col][1], board[col][1]) for col in range(3))
    bot = " | ".join(slotDisplayBySymbol.get(board[col][2], board[col][2]) for col in range(3))
    sampleName, samplePattern = random.choice(activeLines)
    sampleLine = " | ".join(slotDisplayBySymbol.get(sym, sym) for sym in _lineSymbols(board, samplePattern))
    nearMisses = sum(1 for _, pattern in activeLines if _isNearMiss(_lineSymbols(board, pattern)))
    slowdown = " -> ".join(str(random.randint(0, 36)).zfill(2) for _ in range(4))
    return trimRoundText(
        (
            f"Board:\n{top}\n{mid}\n{bot}\n"
            f"Active lines: **{lineCount}** | Sample **{sampleName}** -> {sampleLine}\n"
            f"Reel slowdown: `{slowdown}` | Near misses: `{nearMisses}`\n"
            "No payout this spin."
        )
    )
