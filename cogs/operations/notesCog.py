from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands

from features.operations.notes import service as notesService
from runtime import cogGuards as runtimeCogGuards
from runtime import commandScopes as runtimeCommandScopes


def _normalizeSubjectKey(subjectType: str, subject: str) -> str:
    normalizedType = str(subjectType or "").strip().upper()
    raw = str(subject or "").strip()
    if normalizedType == "USER":
        if raw.startswith("<@") and raw.endswith(">"):
            raw = "".join(ch for ch in raw if ch.isdigit())
    return raw[:120]


class NotesCog(runtimeCogGuards.InteractionGuardMixin, commands.Cog):

    @app_commands.command(name="notes-add", description="Add an internal assistant note.")
    @app_commands.guilds(*runtimeCommandScopes.getTestGuildObjects())
    @app_commands.describe(subject_type="What kind of thing this note is about.", subject="User ID, division key, or process label.", content="Internal note text.")
    @app_commands.rename(subject_type="subject-type")
    @app_commands.choices(
        subject_type=[
            app_commands.Choice(name="User", value="USER"),
            app_commands.Choice(name="Division", value="DIVISION"),
            app_commands.Choice(name="Process", value="PROCESS"),
        ]
    )
    async def notesAdd(
        self,
        interaction: discord.Interaction,
        subject_type: app_commands.Choice[str],
        subject: str,
        content: str,
    ) -> None:
        member = await self._requireAdminOrManageGuild(interaction)
        if member is None:
            return
        if not await self._requireTestGuild(interaction):
            return
        subjectKey = _normalizeSubjectKey(subject_type.value, subject)
        if not subjectKey or not str(content or "").strip():
            return await self._safeReply(interaction, "Subject and content are required.")
        noteId = await notesService.addNote(
            guildId=int(member.guild.id),
            subjectType=subject_type.value,
            subjectKey=subjectKey,
            content=str(content or "").strip(),
            createdBy=int(member.id),
        )
        await self._safeReply(interaction, f"Saved note `{noteId}` for `{subject_type.value}:{subjectKey}`.")

    @app_commands.command(name="notes-list", description="List internal assistant notes.")
    @app_commands.guilds(*runtimeCommandScopes.getTestGuildObjects())
    @app_commands.describe(subject_type="What kind of thing the notes are about.", subject="User ID, division key, or process label.")
    @app_commands.rename(subject_type="subject-type")
    @app_commands.choices(
        subject_type=[
            app_commands.Choice(name="User", value="USER"),
            app_commands.Choice(name="Division", value="DIVISION"),
            app_commands.Choice(name="Process", value="PROCESS"),
        ]
    )
    async def notesList(
        self,
        interaction: discord.Interaction,
        subject_type: app_commands.Choice[str],
        subject: str,
    ) -> None:
        member = await self._requireAdminOrManageGuild(interaction)
        if member is None:
            return
        if not await self._requireTestGuild(interaction):
            return
        subjectKey = _normalizeSubjectKey(subject_type.value, subject)
        rows = await notesService.listNotes(
            guildId=int(member.guild.id),
            subjectType=subject_type.value,
            subjectKey=subjectKey,
            limit=10,
        )
        if not rows:
            return await self._safeReply(interaction, "No notes found.")
        lines = []
        for row in rows:
            lines.append(
                f"`{row.get('noteId')}` by <@{int(row.get('createdBy') or 0)}> at `{row.get('updatedAt')}`\n"
                f"{str(row.get('content') or '').strip()[:250]}"
            )
        await self._safeReply(interaction, "\n\n".join(lines[:10]))

    @app_commands.command(name="notes-delete", description="Delete an internal assistant note.")
    @app_commands.guilds(*runtimeCommandScopes.getTestGuildObjects())
    @app_commands.rename(note_id="note-id")
    async def notesDelete(self, interaction: discord.Interaction, note_id: int) -> None:
        member = await self._requireAdminOrManageGuild(interaction)
        if member is None:
            return
        if not await self._requireTestGuild(interaction):
            return
        await notesService.deleteNote(noteId=int(note_id), guildId=int(member.guild.id))
        await self._safeReply(interaction, f"Deleted note `{int(note_id)}` if it existed.")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(NotesCog())
