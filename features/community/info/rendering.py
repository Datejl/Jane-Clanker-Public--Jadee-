from __future__ import annotations

from datetime import datetime, timezone

import discord

from runtime import textFormatting as textFormattingRuntime


def _discordTimestamp(value: datetime | None, style: str = "f") -> str:
    if value is None:
        return "Unknown"
    dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return discord.utils.format_dt(dt, style=style)


def _clip(text: str, maxLen: int) -> str:
    return textFormattingRuntime.clipText(text, maxLen)


def buildUserInfoEmbed(member: discord.Member) -> discord.Embed:
    roleMentions = [role.mention for role in reversed(member.roles[1:]) if not role.is_default()]
    rolesText = ", ".join(roleMentions[:15]) if roleMentions else "(none)"
    if len(roleMentions) > 15:
        rolesText = f"{rolesText}, +{len(roleMentions) - 15} more"

    embed = discord.Embed(
        title=f"User Info - {member.display_name}",
        color=member.color if int(member.color.value) > 0 else discord.Color.blurple(),
    )
    embed.add_field(name="Mention", value=member.mention, inline=True)
    embed.add_field(name="User ID", value=f"`{member.id}`", inline=True)
    embed.add_field(name="Bot", value="Yes" if member.bot else "No", inline=True)
    embed.add_field(name="Account Created", value=_discordTimestamp(member.created_at, "F"), inline=False)
    embed.add_field(name="Joined Server", value=_discordTimestamp(member.joined_at, "F"), inline=False)
    embed.add_field(name="Top Role", value=member.top_role.mention if not member.top_role.is_default() else "@everyone", inline=True)
    embed.add_field(name="Nickname", value=member.nick or "(none)", inline=True)
    embed.add_field(name="Role Count", value=str(max(0, len(member.roles) - 1)), inline=True)
    embed.add_field(name="Roles", value=_clip(rolesText, 1024), inline=False)
    embed.set_thumbnail(url=member.display_avatar.url)
    return embed


def buildServerInfoEmbed(guild: discord.Guild) -> discord.Embed:
    ownerText = f"<@{guild.owner_id}>" if guild.owner_id else "Unknown"
    systemChannelText = f"<#{guild.system_channel.id}>" if guild.system_channel else "(none)"
    description = guild.description or "No server description set."

    embed = discord.Embed(
        title=f"Server Info - {guild.name}",
        description=_clip(description, 2048),
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Server ID", value=f"`{guild.id}`", inline=True)
    embed.add_field(name="Owner", value=ownerText, inline=True)
    embed.add_field(name="Created", value=_discordTimestamp(guild.created_at, "F"), inline=True)
    embed.add_field(name="Verification", value=str(guild.verification_level).replace("_", " ").title(), inline=True)
    embed.add_field(name="System Channel", value=systemChannelText, inline=True)
    embed.add_field(name="Boost Tier", value=str(guild.premium_tier), inline=True)
    embed.add_field(name="Banner", value="Set" if guild.banner else "Not set", inline=True)
    embed.add_field(name="Vanity URL", value=guild.vanity_url_code or "(none)", inline=True)
    embed.add_field(name="Preferred Locale", value=guild.preferred_locale or "Unknown", inline=True)
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    return embed


def buildServerStatsEmbed(
    guild: discord.Guild,
    *,
    latestSnapshot: dict | None = None,
    daySnapshot: dict | None = None,
    weekSnapshot: dict | None = None,
    memberActivityRows: list[dict] | None = None,
    channelActivityRows: list[dict] | None = None,
) -> discord.Embed:
    totalMembers = guild.member_count or len(guild.members)
    botCount = sum(1 for member in guild.members if member.bot)
    humanCount = max(0, totalMembers - botCount)
    textChannels = len(guild.text_channels)
    voiceChannels = len(guild.voice_channels)
    stageChannels = len(guild.stage_channels)
    forumChannels = len(guild.forums)
    categoryCount = len(guild.categories)

    embed = discord.Embed(
        title=f"Server Stats - {guild.name}",
        color=discord.Color.green(),
    )
    embed.add_field(name="Members", value=str(totalMembers), inline=True)
    embed.add_field(name="Humans", value=str(humanCount), inline=True)
    embed.add_field(name="Bots", value=str(botCount), inline=True)
    embed.add_field(name="Text Channels", value=str(textChannels), inline=True)
    embed.add_field(name="Voice Channels", value=str(voiceChannels), inline=True)
    embed.add_field(name="Stage Channels", value=str(stageChannels), inline=True)
    embed.add_field(name="Forum Channels", value=str(forumChannels), inline=True)
    embed.add_field(name="Categories", value=str(categoryCount), inline=True)
    embed.add_field(name="Roles", value=str(len(guild.roles)), inline=True)
    embed.add_field(name="Emojis", value=str(len(guild.emojis)), inline=True)
    embed.add_field(name="Stickers", value=str(len(guild.stickers)), inline=True)
    embed.add_field(name="Boosts", value=str(guild.premium_subscription_count or 0), inline=True)

    def _delta(current: int, previousRow: dict | None, key: str) -> str:
        if not previousRow:
            return "n/a"
        try:
            previous = int(previousRow.get(key) or 0)
        except (TypeError, ValueError):
            return "n/a"
        delta = current - previous
        sign = "+" if delta >= 0 else ""
        return f"{sign}{delta}"

    growthLines = [
        f"24h members: `{_delta(totalMembers, daySnapshot, 'memberCount')}`",
        f"7d members: `{_delta(totalMembers, weekSnapshot, 'memberCount')}`",
        f"24h humans: `{_delta(humanCount, daySnapshot, 'humanCount')}`",
        f"7d humans: `{_delta(humanCount, weekSnapshot, 'humanCount')}`",
    ]
    if latestSnapshot:
        growthLines.append(
            f"Last snapshot: {_discordTimestamp(_parseSnapshotTime(str(latestSnapshot.get('capturedAt') or '')), 'R')}"
        )
    embed.add_field(name="Growth Snapshots", value="\n".join(growthLines), inline=False)

    joins = sum(int(row.get("joinCount") or 0) for row in (memberActivityRows or []))
    leaves = sum(int(row.get("leaveCount") or 0) for row in (memberActivityRows or []))
    activityLines = [
        f"Joins (7d): `{joins}`",
        f"Leaves (7d): `{leaves}`",
        f"Net (7d): `{joins - leaves:+d}`",
    ]
    trendLines: list[str] = []
    for row in (memberActivityRows or [])[-5:]:
        activityDate = str(row.get("activityDate") or "").strip()
        joinCount = int(row.get("joinCount") or 0)
        leaveCount = int(row.get("leaveCount") or 0)
        trendLines.append(f"`{activityDate}` +{joinCount} / -{leaveCount}")
    embed.add_field(
        name="Join / Leave Activity",
        value="\n".join(activityLines + ([""] if trendLines else []) + trendLines),
        inline=False,
    )

    topChannels: list[str] = []
    for row in (channelActivityRows or [])[:5]:
        channelId = int(row.get("channelId") or 0)
        messageCount = int(row.get("messageCount") or 0)
        mention = f"<#{channelId}>" if channelId > 0 else "`unknown-channel`"
        topChannels.append(f"{mention} - `{messageCount}` messages")
    embed.add_field(
        name="Most Active Channels (7d)",
        value="\n".join(topChannels) if topChannels else "(no activity tracked yet)",
        inline=False,
    )

    roleCounts: list[tuple[str, int]] = []
    for role in guild.roles:
        if role.is_default():
            continue
        count = sum(1 for member in guild.members if role in member.roles)
        if count <= 0:
            continue
        roleCounts.append((role.name, count))
    roleCounts.sort(key=lambda row: (-row[1], row[0].lower()))
    roleLines = [f"`{name}` - `{count}`" for name, count in roleCounts[:10]]
    embed.add_field(
        name="Per-Role Population",
        value="\n".join(roleLines) if roleLines else "(no populated roles)",
        inline=False,
    )
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    return embed


def _parseSnapshotTime(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
