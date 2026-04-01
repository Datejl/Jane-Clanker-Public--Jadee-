from __future__ import annotations

import random

from .common import baccaratTotal, baccaratValue, buildShoe, cardText, drawFromShoe, trimRoundText


def createState() -> dict:
    return {"wager": "Player"}


def promptText() -> str:
    return "Place a bet, choose wager side, then press **Deal**."


def settingsText(state: dict) -> str:
    return f"Wager side: `{str(state.get('wager') or 'Player')}`"


def actionLabel() -> str:
    return "Deal"


def configLabel(state: dict) -> str:
    return f"Wager: {str(state.get('wager') or 'Player')}"


def quickLabel() -> str:
    return "Quick Pick"


def cycleConfig(state: dict) -> None:
    wager = str(state.get("wager") or "Player")
    state["wager"] = {"Player": "Banker", "Banker": "Tie", "Tie": "Player"}.get(wager, "Player")


def randomizeConfig(state: dict) -> None:
    state["wager"] = random.choice(["Player", "Banker", "Tie"])


def _bankerShouldDraw(total: int, playerThirdValue: int | None) -> bool:
    if playerThirdValue is None:
        return total <= 5
    if total <= 2:
        return True
    if total == 3:
        return playerThirdValue != 8
    if total == 4:
        return 2 <= playerThirdValue <= 7
    if total == 5:
        return 4 <= playerThirdValue <= 7
    if total == 6:
        return 6 <= playerThirdValue <= 7
    return False


def _tryCorrection(hand: list[tuple[str, str]], shoe: list[tuple[str, str]], targetFn) -> bool:
    if not shoe:
        return False
    for card in random.sample(shoe, k=min(len(shoe), 72)):
        trial = [*hand, card]
        if targetFn(baccaratTotal(trial)):
            hand.append(card)
            shoe.remove(card)
            return True
    return False


def resolveRound(state: dict) -> str:
    wager = str(state.get("wager") or "Player").title()
    if wager not in {"Player", "Banker", "Tie"}:
        wager = "Player"

    shoe = buildShoe(random.randint(6, 8))
    player = [drawFromShoe(shoe), drawFromShoe(shoe)]
    banker = [drawFromShoe(shoe), drawFromShoe(shoe)]
    playerTotal = baccaratTotal(player)
    bankerTotal = baccaratTotal(banker)

    if playerTotal < 8 and bankerTotal < 8:
        playerThird = None
        if playerTotal <= 5:
            playerThird = drawFromShoe(shoe)
            player.append(playerThird)
            playerTotal = baccaratTotal(player)
        playerThirdValue = baccaratValue(playerThird[0]) if playerThird else None
        if _bankerShouldDraw(bankerTotal, playerThirdValue):
            banker.append(drawFromShoe(shoe))
            bankerTotal = baccaratTotal(banker)

    if wager == "Player" and playerTotal > bankerTotal:
        fixed = _tryCorrection(banker, shoe, lambda total: total >= playerTotal)
        bankerTotal = baccaratTotal(banker)
        if not fixed and bankerTotal < playerTotal:
            bankerTotal = playerTotal
    elif wager == "Banker" and bankerTotal > playerTotal:
        fixed = _tryCorrection(player, shoe, lambda total: total > bankerTotal)
        playerTotal = baccaratTotal(player)
        if not fixed and playerTotal <= bankerTotal:
            playerTotal = (bankerTotal + random.randint(1, 3)) % 10
    elif wager == "Tie" and playerTotal == bankerTotal:
        fixed = _tryCorrection(banker, shoe, lambda total, target=playerTotal: total != target)
        bankerTotal = baccaratTotal(banker)
        if not fixed and bankerTotal == playerTotal:
            bankerTotal = 8 if bankerTotal == 9 else bankerTotal + 1

    def winner(p: int, b: int) -> str:
        if p > b:
            return "Player"
        if b > p:
            return "Banker"
        return "Tie"

    winningSide = winner(playerTotal, bankerTotal)
    if winningSide == wager:
        if wager == "Player":
            playerTotal = max(0, playerTotal - 1)
            if playerTotal >= bankerTotal:
                bankerTotal = min(9, playerTotal + 1)
        elif wager == "Banker":
            bankerTotal = max(0, bankerTotal - 1)
            if bankerTotal >= playerTotal:
                playerTotal = min(9, bankerTotal + 1)
        else:
            bankerTotal = 8 if bankerTotal == 9 else bankerTotal + 1
        winningSide = winner(playerTotal, bankerTotal)

    return trimRoundText(
        (
            f"Wager: **{wager}**\n"
            f"Player: **{', '.join(cardText(card) for card in player)}** (`{playerTotal}`)\n"
            f"Banker: **{', '.join(cardText(card) for card in banker)}** (`{bankerTotal}`)\n"
            f"Winner: **{winningSide}**"
        )
    )
