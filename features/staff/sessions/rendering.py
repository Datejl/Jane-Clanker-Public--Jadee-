import discord
from typing import Dict, List

_DISCORD_FIELD_VALUE_MAX = 1024
_FLAG_CLEAR_EMOJI = "<:bg_clear:1463951975946256436>"
_FLAG_HIT_EMOJI = ":no_entry:"


def _buildCappedListFieldValue(
    lines: List[str],
    *,
    emptyText: str = "None",
    overflowNoun: str = "item(s)",
) -> str:
    if not lines:
        return emptyText

    keptLines: list[str] = []
    currentLen = 0
    for idx, line in enumerate(lines):
        lineText = str(line)
        if len(lineText) > _DISCORD_FIELD_VALUE_MAX:
            lineText = lineText[: _DISCORD_FIELD_VALUE_MAX - 3] + "..."

        addLen = len(lineText) + (1 if keptLines else 0)
        if currentLen + addLen <= _DISCORD_FIELD_VALUE_MAX:
            keptLines.append(lineText)
            currentLen += addLen
            continue

        remaining = len(lines) - idx
        suffix = f"\n... and {remaining} more {overflowNoun}."
        while keptLines and (len("\n".join(keptLines)) + len(suffix)) > _DISCORD_FIELD_VALUE_MAX:
            keptLines.pop()
            remaining += 1
            suffix = f"\n... and {remaining} more {overflowNoun}."

        if not keptLines:
            return f"... and {remaining} more {overflowNoun}."
        return "\n".join(keptLines) + suffix

    return "\n".join(keptLines)


def gradeIcon(grade: str) -> str:
    if grade == "PASS":
        return ":white_check_mark: Passed"
    if grade == "FAIL":
        return ":x: Failed"
    return ":o: Not Graded"


def bgIcon(bg: str) -> str:
    if bg == "APPROVED":
        return "BG Approved :white_check_mark:"
    if bg == "REJECTED":
        return "BG Rejected :x:"
    return "BG Pending"


def bgReviewIcon(bg: str) -> str:
    if bg == "APPROVED":
        return "Pass :white_check_mark:"
    if bg == "REJECTED":
        return "Fail :x:"
    return "Pending"


def inventoryIcon(status: str | None) -> str:
    normalized = str(status or "").strip().upper()
    if normalized == "OK":
        return "Inventory :white_check_mark:"
    return "Inventory :x:"


def inventoryReviewIcon(status: str | None) -> str:
    normalized = str(status or "").strip().upper()
    if normalized == "OK":
        return ":white_check_mark:"
    return ":x:"


def _isFlagged(flagged: object) -> bool:
    if flagged is None:
        return False
    if isinstance(flagged, bool):
        return flagged
    if isinstance(flagged, (int, float)):
        return int(flagged) != 0
    text = str(flagged).strip().lower()
    if not text:
        return False
    if text in {"0", "false", "none", "null", "no"}:
        return False
    return True


def flaggedReviewIcon(flagged: object) -> str:
    return _FLAG_HIT_EMOJI if _isFlagged(flagged) else _FLAG_CLEAR_EMOJI


def _bgQueueInventoryText(status: str | None) -> str:
    # Treat all non-OK states (PRIVATE, ERROR, NO_ROVER, unknown) as not-clear.
    icon = ":white_check_mark:" if status == "OK" else ":x:"
    return f"Inventory {icon}"


def _bgQueueBgText(bgStatus: str | None) -> str:
    if bgStatus == "APPROVED":
        return "BG Approved :white_check_mark:"
    if bgStatus == "REJECTED":
        return "BG Rejected :x:"
    return "BG Pending"


def _bgQueueFlaggedText(flagged: object) -> str:
    return f"Flagged {flaggedReviewIcon(flagged)}"


def buildSessionEmbed(
    session: Dict,
    hostMention: str,
    attendees: List[Dict],
    showBg: bool = False,
) -> discord.Embed:
    title = "Certification Session" if session["sessionType"] != "orientation" else "Orientation Session"
    embed = discord.Embed(title=title, description="Click the ? button below to join the session!")

    embed.add_field(name="Certification Type", value=session["sessionType"].title(), inline=False)
    embed.add_field(name="Host", value=hostMention, inline=False)

    lines = []
    for i, attendee in enumerate(attendees, start=1):
        userLine = f"{i}. <@{attendee['userId']}>  -  {gradeIcon(attendee['examGrade'])}"
        if showBg:
            userLine += f"  -  {bgIcon(attendee['bgStatus'])}"
        lines.append(userLine)

    embed.add_field(
        name=f"Attendees ({len(attendees)})",
        value=_buildCappedListFieldValue(lines, overflowNoun="attendee(s)"),
        inline=False,
    )

    status = session["status"]
    embed.set_footer(text=f"Status: {status}")
    return embed


def buildGradingEmbed(
    session: Dict,
    host: discord.Member,
    attendeeUserId: int,
    index: int,
    total: int,
) -> discord.Embed:
    embed = discord.Embed(
        title="Grading",
        description=f"Grading attendee **{index}/{total}**.\nUse Pass/Fail below.",
    )
    embed.add_field(name="Host", value=host.mention, inline=False)
    embed.add_field(name="Now Grading", value=f"<@{attendeeUserId}>", inline=False)
    embed.set_footer(text="This panel is only visible to you.")
    return embed


def buildBgQueueEmbed(
    session: Dict,
    attendees: List[Dict],
    claimsByUserId: Dict[int, int] | None = None,
) -> discord.Embed:
    embed = discord.Embed(
        title="Background Check Queue",
        description="Use queue controls below to open attendee review panels.\n`Inventory  |  BG`",
    )
    lines = []
    for i, attendee in enumerate(attendees, start=1):
        userId = int(attendee["userId"])
        claimOwnerId = None
        if claimsByUserId:
            claimOwnerId = claimsByUserId.get(userId)
        headerLine = f"{i}. <@{userId}>"
        statusParts = [
            _bgQueueInventoryText(attendee.get("robloxInventoryScanStatus")),
            _bgQueueBgText(attendee.get("bgStatus")),
        ]
        if _isFlagged(attendee.get("robloxFlagged")):
            statusParts.append(_bgQueueFlaggedText(attendee.get("robloxFlagged")))
        statusLine = f"   {'  |  '.join(statusParts)}"
        if claimOwnerId:
            lines.append(f"{headerLine}\n{statusLine}\n   Claimed <@{claimOwnerId}>")
        else:
            lines.append(f"{headerLine}\n{statusLine}")
    embed.add_field(
        name=f"People ({len(attendees)})",
        value=_buildCappedListFieldValue(lines, overflowNoun="attendee(s)"),
        inline=False,
    )
    embed.set_footer(text="Mods: open attendee panels to approve/reject. Queue auto-reposts every 5 minutes.")
    return embed
