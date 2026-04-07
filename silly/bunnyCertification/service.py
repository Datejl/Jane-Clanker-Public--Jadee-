from datetime import datetime, timedelta
from typing import Optional, Any

import discord
from discord import Interaction, Member, File, Embed
from discord.ext import commands
import runtime.interaction as interactionRuntime
from db.sqlite import execute, fetchOne
from silly.bunnyCertification.assetHandler import getAsset
import config

bunnyCertRoleId = getattr(config, "bunnyCertificationRoleId")
serverId = getattr(config, "serverId")

solutions = {
    "hatch": 1,
    "whiskers": 2,
    "218.67": 3,
    "daffodil": 4,
    "biblical fluffy easter bunny": 5,
    "AT FIRST LIGHT, WHAT WAS SEALED IS OPENED, AND WHAT WAS HIDDEN BRINGS LIFE WHERE NONE WAS EXPECTED.": 6,
}

lastStage = 6
cooldownInMinutes = 1
_loggingGuildId = 1486749078984589442
_loggingChannelId = 1489383453324738771

CONGRATULATIONS_MESSAGE = """
Congratulations, you are now bunny certified. You may share this knowledge with the world or keep it to yourself. You can make this common or keep it rare. It's entirely up to you now.
-# *dm wainui a screenshot of this cause he wants to know how you figured everything out*
"""

_embedFieldLimit = 1024


def _truncateEmbedFieldValue(value: str, *, limit: int = _embedFieldLimit) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text or "*empty*"

    suffix = "..."
    safeLimit = max(1, int(limit) - len(suffix))
    return f"{text[:safeLimit]}{suffix}"


def _buildEmbed(userName: str, correct: bool, answer: str, currentStage: int) -> Embed:
    embed = Embed()
    embed.title = f"{'Correct' if correct else 'Incorrect'} Answer Submitted"
    embed.add_field(
        name=str(userName or "Unknown user")[:256],
        value=_truncateEmbedFieldValue(answer),
        inline=False,
    )
    embed.set_footer(text=f"Current Stage: {currentStage}")
    embed.colour = discord.Color.green() if correct else discord.Color.dark_red()
    return embed


async def _safeEphemeral(interaction: Interaction, message: str, embed=None, file=None) -> None:
    await interactionRuntime.safeInteractionReply(
        interaction, content=message, embed=embed, file=file, ephemeral=True,
    )

async def _loglogsahur(channel, member: Member, correct: bool, answer: str, stage: int) -> None:
    if channel is None:
        return
    await channel.send(
        embed=_buildEmbed(userName=member.name, correct=correct, answer=answer, currentStage=stage),
        content=f"{member.id} submitted:",
    )

async def _getRow(userId: int) -> Optional[dict]:
    return await fetchOne(
        """
        SELECT currentStageId, lastTry
        FROM bunny_certification
        WHERE userId = ?
        """,
        (userId,)
    )

async def _insertUser(userId: int) -> None:
    await execute(
        """
        INSERT INTO bunny_certification
            (userId, currentStageId, lastTry)
        VALUES (?, 1, datetime('now'))""",
        (userId,)
    )

async def _updateUser(userId: int, *, stageId: Optional[int] = None, updateLastTry: bool = False) -> None:
    if stageId is not None and updateLastTry:
        await execute(
            """
            UPDATE bunny_certification
            SET currentStageId = ?, lastTry = datetime('now')
            WHERE userId = ?
            """,
            (stageId, userId)
        )
    elif stageId is not None:
        await execute(
            """
            UPDATE bunny_certification
            SET currentStageId = ?
            WHERE userId = ?
            """,
            (stageId, userId)
        )
    elif updateLastTry:
        await execute(
            """
            UPDATE bunny_certification
            SET lastTry = datetime('now')
            WHERE userId = ?
            """,
            (userId,)
        )


async def _releaseAsset(bot: commands.Bot, interaction: Interaction, userId: int, stageId: int) -> None:
    asset = getAsset(stageId)
    if not asset:
        await _safeEphemeral(interaction, "Sorry, there was an internal error.")
        return
    if isinstance(asset, File):
        await bot.get_user(userId).send(file=asset)
        await _safeEphemeral(interaction, "Check your DMs.")
    else:
        await _safeEphemeral(interaction, asset)


async def handleCode(bot: commands.Bot, interaction: Interaction, code: str) -> None:
    userId = interaction.user.id
    member: Member = interaction.user
    loggingGuild = bot.get_guild(_loggingGuildId)
    loggingChannel = loggingGuild.get_channel(_loggingChannelId) if loggingGuild is not None else None

    if code == "reset-6677":
        await _updateUser(userId, stageId=0)
        await _safeEphemeral(interaction, "Reset to stage 0.")
        return

    row = await _getRow(userId)

    if row and row["lastTry"]:
        lastTry = datetime.fromisoformat(str(row["lastTry"]))
        if datetime.now() <= lastTry + timedelta(minutes=cooldownInMinutes):
            await _safeEphemeral(interaction, "Sorry, try again later.")
            return

    nextStage = solutions.get(code)
    currentStage = row["currentStageId"] if row else 0

    if nextStage is None:
        if row:
            await _updateUser(userId, updateLastTry=True)
        await _safeEphemeral(interaction, "Sorry, I don't recognize that code.")
        await _loglogsahur(loggingChannel, member, correct=False, answer=code, stage=currentStage)
        return

    if nextStage == 1 or not row:
        if not row:
            await _insertUser(userId)
        else:
            await _updateUser(userId, stageId=1, updateLastTry=True)
        await _releaseAsset(bot, interaction, userId, stageId=1)
        await _loglogsahur(loggingChannel, member, correct=True, answer=code, stage=currentStage)
        return

    if nextStage - currentStage != 1:
        await _updateUser(userId, updateLastTry=True)
        await _safeEphemeral(interaction, "Trying to skip ahead are we?")
        return

    if nextStage == lastStage:
        await _updateUser(userId, stageId=nextStage, updateLastTry=True)
        await _safeEphemeral(interaction, CONGRATULATIONS_MESSAGE)
        role = bot.get_guild(serverId).get_role(bunnyCertRoleId)
        await member.add_roles(role)
        return

    await _updateUser(userId, stageId=nextStage, updateLastTry=True)
    await _releaseAsset(bot, interaction, userId, stageId=nextStage)
    await _loglogsahur(loggingChannel, member, correct=True, answer=code, stage=currentStage)
