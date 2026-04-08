from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import discord

from runtime import textFormatting as textFormattingRuntime


def _parseDbTime(rawValue: Any) -> Optional[datetime]:
    text = str(rawValue or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _discordTimestamp(rawValue: Any, style: str = "R") -> str:
    parsed = _parseDbTime(rawValue)
    if parsed is None:
        return "unknown"
    return f"<t:{int(parsed.timestamp())}:{style}>"


def _waitingLabel(run: dict[str, Any]) -> str:
    pendingWith = str(run.get("pendingWith") or "").strip().lower()
    isTerminal = int(run.get("isTerminal") or 0) != 0
    if isTerminal:
        return "Closed"
    if pendingWith == "final-reviewer":
        return "Awaiting final reviewer"
    if pendingWith == "reviewer":
        return "Awaiting reviewer"
    if pendingWith == "creator":
        return "Awaiting creator submission"
    if pendingWith == "submitter":
        return "Awaiting requester"
    if pendingWith == "applicant":
        return "Awaiting applicant"
    if pendingWith == "system":
        return "Awaiting system routing"
    return "In progress"


def buildCompactSummary(
    run: Optional[dict[str, Any]],
    latestEvent: Optional[dict[str, Any]] = None,
) -> str:
    if not run:
        return "No workflow run recorded."

    currentLabel = str(run.get("currentStateLabel") or run.get("currentStateKey") or "Unknown").strip()
    lines = [
        f"**Stage:** {currentLabel}",
        f"**State:** {_waitingLabel(run)}",
        f"**Updated:** {_discordTimestamp(run.get('updatedAt'), 'R')}",
    ]

    if latestEvent:
        eventNote = str(latestEvent.get("note") or "").strip()
        actorId = int(latestEvent.get("actorId") or 0)
        actorText = f"<@{actorId}>" if actorId > 0 else "system"
        if eventNote:
            lines.append(f"**Last Change:** {eventNote} ({actorText})")
        else:
            toLabel = str(latestEvent.get("toStateLabel") or latestEvent.get("toStateKey") or currentLabel).strip()
            lines.append(f"**Last Change:** moved to {toLabel} ({actorText})")

    return textFormattingRuntime.joinLinesAndClip(lines, 1024)


def clipEmbedValue(value: object, *, limit: int = 1024) -> str:
    return textFormattingRuntime.clipText(
        value,
        limit,
        emptyText="(none)",
        strip=True,
    )


def buildWorkflowEventSummary(rows: list[dict[str, Any]], *, limit: int = 1024) -> str:
    if not rows:
        return ""

    lines: list[str] = []
    for row in reversed(rows):
        toLabel = str(row.get("toStateLabel") or row.get("toStateKey") or "Unknown").strip()
        actorId = int(row.get("actorId") or 0)
        actorText = f"<@{actorId}>" if actorId > 0 else "system"
        note = str(row.get("note") or "").strip()
        transitionText = f"{toLabel} - {note}" if note else toLabel
        lines.append(f"{_discordTimestamp(row.get('createdAt'))}: {transitionText} ({actorText})")
    return textFormattingRuntime.joinLinesAndClip(lines, limit)


def buildReviewStatusBlock(
    *,
    statusText: str,
    workflowSummary: str = "",
    reviewerNote: str = "",
) -> str:
    lines = [f"**Status:** {str(statusText or 'Unknown').strip()}"]
    summaryText = str(workflowSummary or "").strip()
    if summaryText:
        lines.append(summaryText)
    noteText = str(reviewerNote or "").strip()
    if noteText:
        lines.append(f"**Reviewer Note:** {noteText}")
    return clipEmbedValue("\n".join(lines))


def addReviewWorkflowFields(
    embed: discord.Embed,
    *,
    statusText: str,
    workflowSummary: str = "",
    workflowHistorySummary: str = "",
    reviewerNote: str = "",
    statusFieldName: str = "Review Status",
    historyFieldName: str = "Workflow History",
) -> discord.Embed:
    embed.add_field(
        name=statusFieldName,
        value=buildReviewStatusBlock(
            statusText=statusText,
            workflowSummary=workflowSummary,
            reviewerNote=reviewerNote,
        ),
        inline=False,
    )
    historyText = str(workflowHistorySummary or "").strip()
    if historyText:
        embed.add_field(
            name=historyFieldName,
            value=clipEmbedValue(historyText),
            inline=False,
        )
    return embed


__all__ = [
    "addReviewWorkflowFields",
    "buildCompactSummary",
    "buildWorkflowEventSummary",
    "buildReviewStatusBlock",
    "clipEmbedValue",
]
