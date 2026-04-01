from __future__ import annotations

import random

from .common import blackjackTotal, buildShoe, cardText, drawCard, drawFromShoe, trimRoundText


def createState() -> dict:
    return {
        "active": False,
        "stake": 0,
        "shoe": [],
        "player": [],
        "dealer": [],
    }


def promptText() -> str:
    return "Place a bet to start a hand, then use **Hit** or **Stand**."


def settingsText(_: dict) -> str:
    return "Hand play"


def _buildDealerWinningHand(visibleCard: tuple[str, str], playerTotal: int) -> list[tuple[str, str]]:
    targetMin = max(17, min(21, int(playerTotal)))
    bestTie: list[tuple[str, str]] | None = None

    for _ in range(8000):
        handSize = random.randint(2, 5)
        candidate = [visibleCard, *[drawCard() for _ in range(handSize - 1)]]
        total = blackjackTotal(candidate)
        if total > 21:
            continue
        if total >= targetMin and total >= playerTotal:
            return candidate
        if total == playerTotal and bestTie is None:
            bestTie = candidate

    if bestTie is not None:
        return bestTie

    fallback = [visibleCard, drawCard()]
    while blackjackTotal(fallback) < 17 and len(fallback) < 5:
        fallback.append(drawCard())
    return fallback


def _formatRound(state: dict, *, hideDealerHole: bool, note: str) -> str:
    player = list(state.get("player") or [])
    dealer = list(state.get("dealer") or [])
    stake = int(state.get("stake") or 0)

    playerTotal = blackjackTotal(player)
    playerText = ", ".join(cardText(card) for card in player) or "(none)"

    if hideDealerHole:
        if dealer:
            dealerText = f"{cardText(dealer[0])}, ?"
            dealerTotalText = "?"
        else:
            dealerText = "(none)"
            dealerTotalText = "0"
    else:
        dealerText = ", ".join(cardText(card) for card in dealer) or "(none)"
        dealerTotalText = str(blackjackTotal(dealer))

    return trimRoundText(
        (
            f"Bet: `{stake} anrobucks`\n"
            f"Your hand: **{playerText}** (`{playerTotal}`)\n"
            f"Dealer hand: **{dealerText}** (`{dealerTotalText}`)\n"
            f"{note}"
        )
    )


def startRound(state: dict, stake: int) -> str:
    state["active"] = True
    state["stake"] = int(stake)
    state["shoe"] = buildShoe(random.randint(4, 8))

    shoe = state["shoe"]
    player = [drawFromShoe(shoe), drawFromShoe(shoe)]
    dealer = [drawFromShoe(shoe), drawFromShoe(shoe)]

    if blackjackTotal(player) == 21:
        player[1] = drawFromShoe(shoe)
        if blackjackTotal(player) == 21:
            player.append(drawFromShoe(shoe))

    state["player"] = player
    state["dealer"] = dealer
    return _formatRound(state, hideDealerHole=True, note="Your move: **Hit** or **Stand**.")


def hit(state: dict) -> tuple[bool, str]:
    if not bool(state.get("active")):
        return False, "Place a blackjack bet first."

    shoe = list(state.get("shoe") or [])
    if not shoe:
        shoe = buildShoe(4)
    player = list(state.get("player") or [])
    player.append(drawFromShoe(shoe))
    state["shoe"] = shoe
    state["player"] = player

    if blackjackTotal(player) >= 21:
        return True, stand(state)
    return False, _formatRound(state, hideDealerHole=True, note="Your move: **Hit** or **Stand**.")


def stand(state: dict) -> str:
    if not bool(state.get("active")):
        return "Place a blackjack bet first."

    player = list(state.get("player") or [])
    playerTotal = blackjackTotal(player)

    if playerTotal > 21:
        note = "You bust. Dealer wins."
    else:
        dealer = list(state.get("dealer") or [])
        if dealer:
            dealer = _buildDealerWinningHand(dealer[0], playerTotal)
        state["dealer"] = dealer
        if blackjackTotal(dealer) == playerTotal:
            note = "Push. House rules award the dealer."
        else:
            note = "Dealer wins."

    state["active"] = False
    return _formatRound(state, hideDealerHole=False, note=note)
