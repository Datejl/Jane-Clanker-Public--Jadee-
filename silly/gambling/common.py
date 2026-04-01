from __future__ import annotations

import random
from typing import Any

cardRanks = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
cardSuits = ["S", "H", "D", "C"]
suitDisplayByCode = {
    "S": ":spades:",
    "H": ":heart:",
    "D": ":diamonds:",
    "C": ":clubs:",
}


def sanitizeBet(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def trimRoundText(text: str, maxLen: int = 780) -> str:
    clean = str(text or "").strip()
    if len(clean) <= maxLen:
        return clean
    return f"{clean[: maxLen - 1].rstrip()}..."


def drawCard() -> tuple[str, str]:
    return random.choice(cardRanks), random.choice(cardSuits)


def buildShoe(decks: int) -> list[tuple[str, str]]:
    shoe = [(rank, suit) for _ in range(max(1, decks)) for rank in cardRanks for suit in cardSuits]
    random.shuffle(shoe)
    return shoe


def drawFromShoe(shoe: list[tuple[str, str]]) -> tuple[str, str]:
    if not shoe:
        shoe.extend(buildShoe(4))
    return shoe.pop()


def cardText(card: tuple[str, str]) -> str:
    return f"{card[0]}{suitDisplayByCode.get(card[1], card[1])}"


def blackjackValue(rank: str) -> int:
    if rank == "A":
        return 11
    if rank in {"K", "Q", "J"}:
        return 10
    return int(rank)


def blackjackTotal(cards: list[tuple[str, str]]) -> int:
    total = sum(blackjackValue(rank) for rank, _ in cards)
    aces = sum(1 for rank, _ in cards if rank == "A")
    while total > 21 and aces > 0:
        total -= 10
        aces -= 1
    return total


def baccaratValue(rank: str) -> int:
    if rank == "A":
        return 1
    if rank in {"10", "J", "Q", "K"}:
        return 0
    return int(rank)


def baccaratTotal(cards: list[tuple[str, str]]) -> int:
    return sum(baccaratValue(rank) for rank, _ in cards) % 10
