from __future__ import annotations

import json
from datetime import datetime, timezone

import discord


def parsePollOptions(pollRow: dict) -> list[str]:
    raw = str(pollRow.get("optionsJson") or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    options: list[str] = []
    for value in data:
        text = str(value or "").strip()
        if text:
            options.append(text)
    return options


def parsePollSelection(row: dict) -> list[int]:
    raw = str(row.get("optionIndexesJson") or "").strip()
    if raw:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, list):
            selections: list[int] = []
            for value in data:
                try:
                    parsed = int(value)
                except (TypeError, ValueError):
                    continue
                if parsed >= 0 and parsed not in selections:
                    selections.append(parsed)
            if selections:
                return selections
    try:
        parsed = int(row.get("optionIndex") or 0)
    except (TypeError, ValueError):
        return []
    return [parsed] if parsed >= 0 else []


def tallyPollVotes(optionCount: int, voteRows: list[dict]) -> list[int]:
    tallies = [0] * max(0, int(optionCount))
    for row in voteRows:
        for optionIndex in parsePollSelection(row):
            if 0 <= optionIndex < len(tallies):
                tallies[optionIndex] += 1
    return tallies


def buildPollEmbed(pollRow: dict, voteRows: list[dict], resultsForCreator: bool = False) -> discord.Embed:
    options = parsePollOptions(pollRow)
    tallies = tallyPollVotes(len(options), voteRows)
    totalVotes = sum(tallies)
    status = str(pollRow.get("status") or "OPEN").strip().upper()
    pollId = int(pollRow.get("pollId") or 0)
    creatorId = int(pollRow.get("creatorId") or 0)
    anonymous = bool(int(pollRow.get("anonymous") or 0))
    multiSelect = bool(int(pollRow.get("multiSelect") or 0))
    hideResultsUntilClosed = bool(int(pollRow.get("hideResultsUntilClosed") or 0))
    messageResultsToCreator = bool(int(pollRow.get("messageResultsToCreator") or 0))
    showResults = (status == "CLOSED" and not messageResultsToCreator) or not hideResultsUntilClosed or resultsForCreator
    
    embed = discord.Embed(
        title=f"Poll #{pollId}",
        description=str(pollRow.get("question") or "").strip() or "Untitled poll",
        color=discord.Color.green() if status == "OPEN" else discord.Color.dark_grey(),
    )

    lines: list[str] = []
    for index, option in enumerate(options, start=1):
        if showResults:
            votes = tallies[index - 1]
            percentage = 0 if totalVotes <= 0 else round((votes / totalVotes) * 100)
            lines.append(f"`{index}.` {option} - **{votes}** vote(s) ({percentage}%)")
        else:
            lines.append(f"`{index}.` {option}")
    embed.add_field(name="Options", value="\n".join(lines) if lines else "(none)", inline=False)
    embed.add_field(name="Status", value=status.title(), inline=True)
    if showResults:
        embed.add_field(name="Total Votes", value=str(totalVotes), inline=True)
    else:
        embed.add_field(name="Results", value="Hidden until the poll closes", inline=True)
    embed.add_field(
        name="Created By",
        value="Anonymous" if anonymous else (f"<@{creatorId}>" if creatorId > 0 else "Unknown"),
        inline=True,
    )
    embed.add_field(name="Voting Mode", value="Multi-select" if multiSelect else "Single-select", inline=True)

    closesAtRaw = str(pollRow.get("closesAt") or "").strip()
    if closesAtRaw:
        try:
            closesAt = datetime.fromisoformat(closesAtRaw)
        except ValueError:
            closesAt = None
        if closesAt is not None:
            if closesAt.tzinfo is None:
                closesAt = closesAt.replace(tzinfo=timezone.utc)
            embed.add_field(
                name="Closes",
                value=f"{discord.utils.format_dt(closesAt, 'F')}\n{discord.utils.format_dt(closesAt, 'R')}",
                inline=False,
            )
    gateRaw = str(pollRow.get("roleGateIdsJson") or "").strip()
    if gateRaw and gateRaw != "[]":
        try:
            gateIds = json.loads(gateRaw)
        except json.JSONDecodeError:
            gateIds = []
        if isinstance(gateIds, list):
            roleMentions = " ".join(f"<@&{int(roleId)}>" for roleId in gateIds if int(roleId) > 0)
            if roleMentions:
                embed.add_field(name="Voting Access", value=roleMentions, inline=False)
    embed.set_footer(text="Vote changes replace your previous vote.")
    return embed
