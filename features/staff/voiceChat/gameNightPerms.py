from __future__ import annotations

from discord import Member, PermissionOverwrite
from discord.ext import commands

from features.staff.voiceChat.shiftPerms import getShiftPerms


def getGameNightPerms(
    cohost1: Member | int | None,
    cohost2: Member | int | None,
    bot: commands.Bot,
) -> dict[object, PermissionOverwrite]:
    return getShiftPerms(cohost1=cohost1, cohost2=cohost2, bot=bot)

