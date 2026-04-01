from __future__ import annotations

import io
import asyncio
import re
from datetime import datetime, timezone
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands

from runtime import cogGuards as runtimeCogGuards
from runtime import interaction as interactionRuntime

_invalidFilenameChars = re.compile(r"[^a-zA-Z0-9._-]+")


def _sanitizeFilenamePart(value: str) -> str:
    normalized = _invalidFilenameChars.sub("-", str(value or "").strip()).strip(" .-_")
    return normalized or "channel"


def _archiveFilename(channelName: str, archivedAt: datetime) -> str:
    stamp = archivedAt.strftime("%Y-%m-%d_%H-%M-%S")
    return f"{_sanitizeFilenamePart(channelName)}-{stamp}.txt"


def _archiveFilePath(filename: str) -> Path:
    repoRoot = Path(__file__).resolve().parents[2]
    archiveDir = repoRoot / "silly" / "archives"
    archiveDir.mkdir(parents=True, exist_ok=True)
    return archiveDir / filename


def _writeArchiveHeader(handle: io.TextIOBase, channel: discord.TextChannel, archivedBy: discord.abc.User) -> None:
    handle.write("Jane Channel Archive\n")
    handle.write("====================\n")
    handle.write(f"Channel: #{channel.name} ({channel.id})\n")
    handle.write(f"Guild: {channel.guild.name} ({channel.guild.id})\n")
    handle.write(f"Archived by: {archivedBy} ({archivedBy.id})\n")
    handle.write(f"Archived at (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
    handle.write("\n")


def _writeArchivedMessage(handle: io.TextIOBase, message: discord.Message) -> None:
    createdAt = message.created_at
    if createdAt.tzinfo is None:
        createdAt = createdAt.replace(tzinfo=timezone.utc)
    createdAtText = createdAt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    authorText = f"{message.author} ({message.author.id})"
    handle.write(f"[{createdAtText}] {authorText}\n")

    content = (message.content or "").replace("\r\n", "\n").replace("\r", "\n")
    if content:
        handle.write(content + "\n")

    if message.attachments:
        attachmentLinks: list[str] = []
        for attachment in message.attachments:
            link = str(getattr(attachment, "url", "") or "").strip()
            if not link:
                link = str(getattr(attachment, "proxy_url", "") or "").strip()
            if link:
                attachmentLinks.append(link)
        if attachmentLinks:
            handle.write("[attachments]\n")
            for link in attachmentLinks:
                handle.write(f"- {link}\n")

    if message.embeds:
        handle.write(f"[embeds] {len(message.embeds)}\n")

    handle.write("\n")


class ArchiveCog(runtimeCogGuards.InteractionGuardMixin, commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @app_commands.command(name="archive", description="Archive this channel into a .txt export.")
    @app_commands.default_permissions(administrator=True)
    @app_commands.guild_only()
    async def archive(self, interaction: discord.Interaction) -> None:
        if await self._requireAdministrator(interaction) is None:
            return

        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await self._safeReply(interaction, "Use this in a standard text channel.")
            return

        await interactionRuntime.safeInteractionDefer(interaction, ephemeral=True, thinking=True)

        everyoneRole = channel.guild.default_role
        lockApplied = False
        try:
            overwrite = channel.overwrites_for(everyoneRole)
            overwrite.send_messages = False
            overwrite.add_reactions = False
            if hasattr(overwrite, "send_messages_in_threads"):
                overwrite.send_messages_in_threads = False
            await channel.set_permissions(
                everyoneRole,
                overwrite=overwrite,
                reason=f"Channel archived by {interaction.user} ({interaction.user.id})",
            )
            lockApplied = True
        except (discord.Forbidden, discord.HTTPException):
            lockApplied = False

        archivedAt = datetime.now(timezone.utc)
        filename = _archiveFilename(channel.name, archivedAt)
        outputPath = _archiveFilePath(filename)

        messageCount = 0
        with outputPath.open("w", encoding="utf-8") as handle:
            _writeArchiveHeader(handle, channel, interaction.user)
            afterMessage = None
            while True:
                try:
                    batch = [
                        msg
                        async for msg in channel.history(
                            limit=100,
                            oldest_first=True,
                            after=afterMessage,
                        )
                    ]
                except discord.HTTPException as exc:
                    if getattr(exc, "status", None) == 429:
                        retryAfter = float(getattr(exc, "retry_after", 5.0) or 5.0)
                        await asyncio.sleep(max(1.0, retryAfter))
                        continue
                    raise

                if not batch:
                    break
                for message in batch:
                    _writeArchivedMessage(handle, message)
                    messageCount += 1
                afterMessage = batch[-1]

        guildFileLimit = int(getattr(channel.guild, "filesize_limit", 8 * 1024 * 1024) or 8 * 1024 * 1024)
        outputSize = outputPath.stat().st_size
        uploadAttempted = False
        uploadSucceeded = False

        if outputSize <= guildFileLimit:
            uploadAttempted = True
            try:
                await channel.send(
                    content=(
                        f"Archive completed for <#{channel.id}> by {interaction.user.mention}.\n"
                        f"Messages archived: {messageCount}."
                    ),
                    file=discord.File(str(outputPath), filename=filename),
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )
                uploadSucceeded = True
            except (discord.Forbidden, discord.HTTPException):
                uploadSucceeded = False

        if uploadSucceeded:
            try:
                outputPath.unlink(missing_ok=True)
            except OSError:
                pass
            await self._safeReply(
                interaction,
                f"Archive complete. Channel lock {'applied' if lockApplied else 'could not be applied'}.",
            )
            return

        sizeMiB = outputSize / (1024 * 1024)
        localNotice = (
            f"Archive completed for <#{channel.id}> by {interaction.user.mention}.\n"
            f"Messages archived: {messageCount}.\n"
            f"Archive file saved locally: `{outputPath.name}` ({sizeMiB:.2f} MiB)."
        )
        if uploadAttempted:
            localNotice += "\nDiscord file upload failed; local save retained."
        else:
            localNotice += "\nFile exceeded channel upload limit; local save retained."

        try:
            await channel.send(
                localNotice,
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
        except (discord.Forbidden, discord.HTTPException):
            pass
        await self._safeReply(
            interaction,
            (
                f"Archive complete. Upload not posted in channel. Local file saved at:\n`{outputPath}`\n"
                f"Channel lock {'applied' if lockApplied else 'could not be applied'}."
            ),
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ArchiveCog(bot))
