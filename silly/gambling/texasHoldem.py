from __future__ import annotations

import random
from itertools import combinations

from .common import cardRanks, cardSuits, cardText, trimRoundText

houseBotSeat = "HOUSE_BOT"
houseBotName = "DealerBot"

rankValueByCode = {
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7": 7,
    "8": 8,
    "9": 9,
    "10": 10,
    "J": 11,
    "Q": 12,
    "K": 13,
    "A": 14,
}

categoryNames = {
    8: "Straight Flush",
    7: "Four of a Kind",
    6: "Full House",
    5: "Flush",
    4: "Straight",
    3: "Three of a Kind",
    2: "Two Pair",
    1: "One Pair",
    0: "High Card",
}


def createState() -> dict:
    return {
        "participants": [],
        "handsPlayed": 0,
        "lastWinnerId": 0,
        "lastMargin": "",
    }


def promptText() -> str:
    return "Join the table, then press **Deal Hand**."


def settingsText(state: dict) -> str:
    participants = list(state.get("participants") or [])
    handsPlayed = int(state.get("handsPlayed") or 0)
    return f"Participants: `{len(participants)}` | Hands: `{handsPlayed}`"


def actionLabel() -> str:
    return "Deal Hand"


def configLabel() -> str:
    return "Join/Leave"


def quickLabel() -> str:
    return "Roster"


def isParticipant(state: dict, userId: int) -> bool:
    safeUserId = int(userId)
    return safeUserId in {int(value) for value in (state.get("participants") or [])}


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
    return ", ".join(f"<@{userId}>" for userId in participants[:25])


def _buildDeck() -> list[tuple[str, str]]:
    deck = [(rank, suit) for suit in cardSuits for rank in cardRanks]
    random.shuffle(deck)
    return deck


def _straightHigh(rankValues: list[int]) -> int:
    unique = sorted(set(rankValues), reverse=True)
    if len(unique) != 5:
        return 0
    if unique == [14, 5, 4, 3, 2]:
        return 5
    if unique[0] - unique[-1] == 4:
        return unique[0]
    return 0


def _scoreFive(cards: list[tuple[str, str]]) -> tuple[int, tuple[int, ...]]:
    ranks = [rankValueByCode[rank] for rank, _ in cards]
    suits = [suit for _, suit in cards]
    rankCounts: dict[int, int] = {}
    for value in ranks:
        rankCounts[value] = int(rankCounts.get(value, 0)) + 1

    isFlush = len(set(suits)) == 1
    straightHigh = _straightHigh(ranks)

    if isFlush and straightHigh:
        return 8, (straightHigh,)

    counts = sorted(((count, rank) for rank, count in rankCounts.items()), reverse=True)

    if counts[0][0] == 4:
        quadRank = counts[0][1]
        kicker = max(rank for rank in ranks if rank != quadRank)
        return 7, (quadRank, kicker)

    if counts[0][0] == 3 and counts[1][0] == 2:
        return 6, (counts[0][1], counts[1][1])

    if isFlush:
        return 5, tuple(sorted(ranks, reverse=True))

    if straightHigh:
        return 4, (straightHigh,)

    if counts[0][0] == 3:
        tripRank = counts[0][1]
        kickers = sorted((rank for rank in ranks if rank != tripRank), reverse=True)
        return 3, (tripRank, *kickers)

    if counts[0][0] == 2 and counts[1][0] == 2:
        highPair = max(counts[0][1], counts[1][1])
        lowPair = min(counts[0][1], counts[1][1])
        kicker = max(rank for rank in ranks if rank not in {highPair, lowPair})
        return 2, (highPair, lowPair, kicker)

    if counts[0][0] == 2:
        pairRank = counts[0][1]
        kickers = sorted((rank for rank in ranks if rank != pairRank), reverse=True)
        return 1, (pairRank, *kickers)

    return 0, tuple(sorted(ranks, reverse=True))


def _bestScore(cards: list[tuple[str, str]]) -> tuple[int, tuple[int, ...]]:
    best: tuple[int, tuple[int, ...]] | None = None
    for combo in combinations(cards, 5):
        score = _scoreFive(list(combo))
        if best is None or score > best:
            best = score
    return best or (0, ())


def _describeScore(score: tuple[int, tuple[int, ...]]) -> str:
    category = int(score[0])
    tie = tuple(score[1] or ())
    name = categoryNames.get(category, "Hand")
    if not tie:
        return name
    if category in {8, 4}:
        return f"{name} ({tie[0]} high)"
    if category == 7:
        return f"{name} ({tie[0]}s)"
    if category == 6:
        return f"{name} ({tie[0]} over {tie[1]})"
    if category == 5:
        return f"{name} ({tie[0]} high)"
    if category == 3:
        return f"{name} ({tie[0]}s)"
    if category == 2:
        return f"{name} ({tie[0]} & {tie[1]})"
    if category == 1:
        return f"{name} ({tie[0]}s)"
    return f"{name} ({tie[0]} high)"


def _winMarginMetric(botScore: tuple[int, tuple[int, ...]], runnerScore: tuple[int, tuple[int, ...]]) -> tuple[int, int, int]:
    botCategory, botTie = int(botScore[0]), tuple(botScore[1] or ())
    runnerCategory, runnerTie = int(runnerScore[0]), tuple(runnerScore[1] or ())
    categoryGap = botCategory - runnerCategory
    firstDiff = 1
    for idx in range(max(len(botTie), len(runnerTie))):
        botValue = int(botTie[idx] if idx < len(botTie) else 0)
        runnerValue = int(runnerTie[idx] if idx < len(runnerTie) else 0)
        if botValue != runnerValue:
            firstDiff = max(1, botValue - runnerValue)
            break
    totalSpread = sum(
        int(botTie[idx] if idx < len(botTie) else 0) - int(runnerTie[idx] if idx < len(runnerTie) else 0)
        for idx in range(max(len(botTie), len(runnerTie)))
    )
    return categoryGap, firstDiff, max(1, totalSpread)


def _dealCandidate(humanSeats: list[int]) -> dict:
    seats: list[int | str] = [*humanSeats, houseBotSeat]
    deck = _buildDeck()
    holes: dict[int | str, list[tuple[str, str]]] = {seat: [deck.pop(), deck.pop()] for seat in seats}
    board = [deck.pop() for _ in range(5)]
    scores = {seat: _bestScore([*holes[seat], *board]) for seat in seats}
    return {"holes": holes, "board": board, "scores": scores}


def _bestHumanSeat(humanSeats: list[int], scores: dict[int | str, tuple[int, tuple[int, ...]]]) -> int:
    ranked = sorted(humanSeats, key=lambda seat: scores.get(seat, (0, ())), reverse=True)
    return int(ranked[0])


def _findBarelyWinningCandidate(humanSeats: list[int], maxAttempts: int = 6000) -> tuple[dict, int, tuple[int, int, int], int]:
    bestCandidate: dict | None = None
    bestRunner = 0
    bestMetric: tuple[int, int, int] | None = None
    attempts = 0

    while attempts < maxAttempts:
        attempts += 1
        candidate = _dealCandidate(humanSeats)
        scores = dict(candidate.get("scores") or {})
        botScore = scores.get(houseBotSeat, (0, ()))
        runnerSeat = _bestHumanSeat(humanSeats, scores)
        runnerScore = scores.get(runnerSeat, (0, ()))

        if botScore <= runnerScore:
            continue
        if any(scores.get(seat) == botScore for seat in humanSeats):
            continue

        metric = _winMarginMetric(botScore, runnerScore)
        if bestMetric is None or metric < bestMetric:
            bestCandidate = candidate
            bestRunner = runnerSeat
            bestMetric = metric
            if metric[0] == 0 and metric[1] == 1:
                break

    if bestCandidate is not None and bestMetric is not None:
        return bestCandidate, bestRunner, bestMetric, attempts

    # Fallback: keep simulating until a strict bot win is found.
    while True:
        attempts += 1
        candidate = _dealCandidate(humanSeats)
        scores = dict(candidate.get("scores") or {})
        botScore = scores.get(houseBotSeat, (0, ()))
        runnerSeat = _bestHumanSeat(humanSeats, scores)
        runnerScore = scores.get(runnerSeat, (0, ()))
        if botScore <= runnerScore:
            continue
        if any(scores.get(seat) == botScore for seat in humanSeats):
            continue
        metric = _winMarginMetric(botScore, runnerScore)
        return candidate, runnerSeat, metric, attempts


def resolveRound(state: dict, *, requesterId: int) -> str:
    safeRequesterId = int(requesterId)
    participants = [int(value) for value in (state.get("participants") or [])]
    if safeRequesterId not in participants:
        participants.append(safeRequesterId)
    participants = list(dict.fromkeys(participants))
    if len(participants) > 7:
        participants = participants[:7]

    candidate, runnerSeat, marginMetric, attempts = _findBarelyWinningCandidate(participants)
    board = list(candidate.get("board") or [])
    holes = dict(candidate.get("holes") or {})
    scores = dict(candidate.get("scores") or {})

    state["participants"] = participants
    state["handsPlayed"] = int(state.get("handsPlayed") or 0) + 1
    state["lastWinnerId"] = 0
    state["lastMargin"] = f"gap={marginMetric[0]}/{marginMetric[1]}"

    lines: list[str] = []
    lines.append(f"Board: {' '.join(cardText(card) for card in board)}")
    lines.append("")
    for seat in participants:
        hand = list(holes.get(seat) or [])
        score = scores.get(seat, (0, ()))
        lines.append(
            f"<@{seat}>: {' '.join(cardText(card) for card in hand)}"
            f" | {_describeScore(score)}"
        )
    botHand = list(holes.get(houseBotSeat) or [])
    botScore = scores.get(houseBotSeat, (0, ()))
    lines.append(
        f"{houseBotName}: {' '.join(cardText(card) for card in botHand)}"
        f" | {_describeScore(botScore)}"
    )
    lines.append("")
    lines.append(f"Winner: **{houseBotName}**")
    lines.append(f"Runner-up: <@{runnerSeat}>")
    lines.append(f"Simulation attempts: `{attempts}`")

    return trimRoundText("\n".join(lines))
