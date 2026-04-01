import discord
from typing import Dict

from features.staff.workflows import rendering as workflowRendering


def _statusLabel(status: str) -> str:
    if status == "APPROVED":
        return ":white_check_mark: Approved"
    if status == "REJECTED":
        return ":x: Rejected"
    if status == "NEEDS_INFO":
        return ":warning: Needs Info"
    return ":hourglass: Pending"


def buildOrbatRequestEmbed(
    request: Dict,
    submitterMention: str,
    *,
    workflowSummary: str = "",
    workflowHistorySummary: str = "",
) -> discord.Embed:
    embed = discord.Embed(
        title=f"ORBAT Request #{request['requestId']}",
    )
    embed.add_field(name="User", value=submitterMention, inline=False)
    embed.add_field(name="Roblox User", value=request["robloxUser"], inline=False)
    embed.add_field(name="Rank", value=request.get("inferredRank") or "Unknown", inline=True)
    embed.add_field(name="Clearance", value=request.get("inferredClearance") or "Unknown", inline=True)
    embed.add_field(name="Department", value=request.get("inferredDepartment") or "Unknown", inline=True)
    embed.add_field(name="Mic", value=request.get("mic") or "Unknown", inline=True)
    embed.add_field(name="Time Zone", value=request.get("timezone") or "Unknown", inline=True)
    embed.add_field(name="Age Group", value=request.get("ageGroup") or "Unknown", inline=True)
    notes = request.get("notes") or "None"
    embed.add_field(name="Notes", value=notes, inline=False)
    workflowRendering.addReviewWorkflowFields(
        embed,
        statusText=_statusLabel(request["status"]),
        workflowSummary=workflowSummary,
        workflowHistorySummary=workflowHistorySummary,
        reviewerNote=str(request.get("reviewNote") or "").strip(),
    )
    return embed


def buildLoaRequestEmbed(
    request: Dict,
    submitterMention: str,
    *,
    workflowSummary: str = "",
    workflowHistorySummary: str = "",
) -> discord.Embed:
    embed = discord.Embed(
        title=f"LOA Request #{request['requestId']}",
    )
    embed.add_field(name="User", value=submitterMention, inline=False)
    embed.add_field(name="Start", value=request["startDate"], inline=True)
    embed.add_field(name="End", value=request["endDate"], inline=True)
    reason = request.get("reason") or "None"
    embed.add_field(name="Reason", value=reason, inline=False)
    workflowRendering.addReviewWorkflowFields(
        embed,
        statusText=_statusLabel(request["status"]),
        workflowSummary=workflowSummary,
        workflowHistorySummary=workflowHistorySummary,
        reviewerNote=str(request.get("reviewNote") or "").strip(),
    )
    return embed

