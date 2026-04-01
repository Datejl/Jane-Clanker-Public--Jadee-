import json
from datetime import datetime, timezone
from typing import Dict

import discord

from features.staff.workflows import rendering as workflowRendering


def _statusIcon(status: str) -> str:
    state = (status or "").upper()
    if state == "APPROVED":
        return ":white_check_mark: Approved"
    if state == "DENIED":
        return ":x: Denied"
    if state == "NEEDS_INFO":
        return ":warning: Needs Info"
    return ":hourglass_flowing_sand: Pending"


def _answersText(raw: str) -> str:
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict) or not payload:
        return "No answers captured."
    lines = []
    for key, value in payload.items():
        label = str(key or "").strip()
        text = str(value or "").strip() or "(blank)"
        lines.append(f"**{label}**\n{text}")
    return "\n\n".join(lines)


def buildApplicationHubEmbed(division: Dict) -> discord.Embed:
    title = str(division.get("displayName") or division.get("key") or "Division Application")
    description = str(division.get("description") or "Click Apply to submit your application.")
    embed = discord.Embed(title=title, description=description)
    requirements = str(division.get("requirements") or "").strip()
    if requirements:
        embed.add_field(name="Requirements", value=requirements, inline=False)
    whatYouDo = str(division.get("whatYouDo") or "").strip()
    if whatYouDo:
        embed.add_field(name="What You Do", value=whatYouDo, inline=False)
    imageUrl = str(division.get("bannerUrl") or "").strip()
    if imageUrl:
        embed.set_image(url=imageUrl)
    return embed


def _submittedTimestamp(rawValue: str) -> str:
    if not rawValue:
        return "Unknown"
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(rawValue)
    except ValueError:
        try:
            parsed = datetime.strptime(rawValue, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return str(rawValue)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    unix = int(parsed.timestamp())
    return f"<t:{unix}:f> (<t:{unix}:R>)"


def buildReviewEmbed(
    application: Dict,
    division: Dict,
    *,
    workflowSummary: str = "",
    workflowHistorySummary: str = "",
) -> discord.Embed:
    divisionName = str(division.get("displayName") or application.get("divisionKey") or "Application").strip()
    embed = discord.Embed(title=f"Application Review - {divisionName}")
    embed.add_field(name="Applicant", value=f"<@{application['applicantId']}> (`{application['applicantId']}`)", inline=False)
    embed.add_field(name="Division", value=divisionName, inline=True)
    embed.add_field(name="Submitted", value=_submittedTimestamp(str(application.get("createdAt") or "")), inline=True)
    workflowRendering.addReviewWorkflowFields(
        embed,
        statusText=_statusIcon(str(application.get("status") or "PENDING")),
        workflowSummary=workflowSummary,
        workflowHistorySummary=workflowHistorySummary,
        reviewerNote=str(application.get("reviewNote") or "").strip(),
    )
    proofMessageUrl = str(application.get("proofMessageUrl") or "").strip()
    if proofMessageUrl:
        embed.add_field(name="Proof", value=f"[Open proof message]({proofMessageUrl})", inline=False)
    rawProofAttachments = str(application.get("proofAttachmentsJson") or "[]")
    try:
        proofAttachments = json.loads(rawProofAttachments)
    except json.JSONDecodeError:
        proofAttachments = []
    if isinstance(proofAttachments, list) and proofAttachments:
        urls = [str(value).strip() for value in proofAttachments if str(value).strip()]
        if urls:
            preview = "\n".join(f"[Attachment {index + 1}]({url})" for index, url in enumerate(urls[:8]))
            if len(urls) > 8:
                preview += f"\n...and {len(urls) - 8} more"
            embed.add_field(name="Proof Attachments", value=workflowRendering.clipEmbedValue(preview), inline=False)
    answersText = _answersText(str(application.get("answersJson") or "{}"))
    embed.add_field(name="Answers", value=workflowRendering.clipEmbedValue(answersText), inline=False)
    return embed

