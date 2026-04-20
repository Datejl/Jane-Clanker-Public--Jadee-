from __future__ import annotations

import asyncio
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import discord
from discord.http import Route

log = logging.getLogger(__name__)

_excludedPathParts = {
    ".git",
    ".venv",
    "__pycache__",
    "backups",
    "localOnly",
}
_excludedExtensions = {
    ".7z",
    ".db",
    ".dll",
    ".exe",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".pdf",
    ".pkl",
    ".png",
    ".pyc",
    ".so",
    ".sqlite",
    ".webp",
    ".zip",
}


@dataclass(frozen=True, slots=True)
class RepoLineCount:
    lines: int
    files: int


def _isCountedPath(path: str) -> bool:
    candidate = Path(str(path or ""))
    if any(part in _excludedPathParts for part in candidate.parts):
        return False
    return candidate.suffix.casefold() not in _excludedExtensions


def _gitTrackedFiles(repoRoot: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=str(repoRoot),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _fallbackFiles(repoRoot: Path) -> list[str]:
    files: list[str] = []
    for path in repoRoot.rglob("*"):
        if not path.is_file():
            continue
        try:
            relPath = path.relative_to(repoRoot).as_posix()
        except ValueError:
            continue
        files.append(relPath)
    return files


def countRepoTextLines(repoRoot: Path) -> RepoLineCount:
    root = Path(repoRoot).resolve()
    files = _gitTrackedFiles(root) or _fallbackFiles(root)
    totalLines = 0
    countedFiles = 0
    for relPath in files:
        if not _isCountedPath(relPath):
            continue
        path = root / relPath
        if not path.is_file():
            continue
        try:
            data = path.read_bytes()
        except OSError:
            continue
        text = data.decode("utf-8", errors="ignore")
        totalLines += sum(1 for line in text.splitlines() if line.strip())
        countedFiles += 1
    return RepoLineCount(lines=totalLines, files=countedFiles)


def buildJaneBio(count: RepoLineCount) -> str:
    return (
        f"Helping ANRO with `{int(count.lines):,}` lines of code across `{int(count.files):,}` files.\n\n"
        "Developed by a_very_tired_potato and koulmoir for use in ANRO operations."
    )


def _allowedGuildIds(configModule: Any) -> set[int]:
    out: set[int] = set()
    for rawValue in list(getattr(configModule, "allowedCommandGuildIds", []) or []):
        try:
            guildId = int(rawValue)
        except (TypeError, ValueError):
            continue
        if guildId > 0:
            out.add(guildId)
    return out


async def _setCurrentMemberBio(
    *,
    botClient: discord.Client,
    taskBudgeter: Any,
    guildId: int,
    bio: str,
) -> bool:
    route = Route("PATCH", "/guilds/{guild_id}/members/@me", guild_id=int(guildId))
    try:
        await taskBudgeter.runDiscord(lambda: botClient.http.request(route, json={"bio": bio}))
        return True
    except discord.HTTPException:
        log.warning("Failed to update Jane bio in guild %s.", int(guildId), exc_info=True)
        return False


async def updateJaneBioOnStartup(
    *,
    botClient: discord.Client,
    configModule: Any,
    taskBudgeter: Any,
    repoRoot: str | Path,
) -> dict[str, int | str]:
    await botClient.wait_until_ready()
    count = await asyncio.to_thread(countRepoTextLines, Path(repoRoot))
    bio = buildJaneBio(count)
    allowedGuildIds = _allowedGuildIds(configModule)

    attempted = 0
    updated = 0
    skipped = 0
    for guild in list(getattr(botClient, "guilds", []) or []):
        guildId = int(getattr(guild, "id", 0) or 0)
        if guildId <= 0:
            continue
        if allowedGuildIds and guildId not in allowedGuildIds:
            skipped += 1
            continue
        attempted += 1
        if await _setCurrentMemberBio(
            botClient=botClient,
            taskBudgeter=taskBudgeter,
            guildId=guildId,
            bio=bio,
        ):
            updated += 1

    log.info(
        "Jane startup bio update complete: updated=%s attempted=%s skipped=%s lines=%s files=%s.",
        updated,
        attempted,
        skipped,
        int(count.lines),
        int(count.files),
    )
    return {
        "updated": updated,
        "attempted": attempted,
        "skipped": skipped,
        "lines": int(count.lines),
        "files": int(count.files),
        "bio": bio,
    }
