from __future__ import annotations

import asyncio
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import discord

from db.sqlite import dbPath as sqliteDbPath
from runtime import bgQueueCommand as runtimeBgQueueCommand
from runtime import helpMenu as runtimeHelpMenu


class TextCommandRouter:
    def __init__(
        self,
        *,
        botClient: Any,
        configModule: Any,
        sessionService: Any,
        sessionViews: Any,
        taskBudgeter: Any,
        helpCommandsModule: Any,
        permissionsModule: Any,
        maintenanceCoordinator: Any,
        botStartedAt: datetime,
        formatUptime: Callable[[Any], str],
        discordTimestamp: Callable[[datetime, str], str],
        getProcessResourceSnapshot: Callable[[datetime], dict[str, str]],
        sendRuntimeWebhookMessage: Callable[[discord.Message, discord.Embed], Any],
        sendTerminalWebhookMessage: Callable[[discord.Message, str], Any],
        hasCohostPermission: Callable[[discord.Member], bool],
        orbatWeeklyScheduleConfig: Callable[[], tuple[int, int, int]],
        gitUpdateCoordinator: Any | None = None,
        generalErrorLogPath: str = "",
    ) -> None:
        self.botClient = botClient
        self.config = configModule
        self.sessionService = sessionService
        self.sessionViews = sessionViews
        self.taskBudgeter = taskBudgeter
        self.helpCommands = helpCommandsModule
        self.permissions = permissionsModule
        self.maintenance = maintenanceCoordinator
        self.botStartedAt = botStartedAt
        self.formatUptime = formatUptime
        self.discordTimestamp = discordTimestamp
        self.getProcessResourceSnapshot = getProcessResourceSnapshot
        self.sendRuntimeWebhookMessage = sendRuntimeWebhookMessage
        self.sendTerminalWebhookMessage = sendTerminalWebhookMessage
        self.hasCohostPermission = hasCohostPermission
        self.orbatWeeklyScheduleConfig = orbatWeeklyScheduleConfig
        self.gitUpdateCoordinator = gitUpdateCoordinator
        self.generalErrorLogPath = str(generalErrorLogPath or "").strip()

    def _formatIsoTimestampOrNever(self, rawValue: object) -> str:
        rawText = str(rawValue or "").strip()
        if not rawText:
            return "`never`"
        try:
            parsed = datetime.fromisoformat(rawText)
        except ValueError:
            return f"`{rawText}`"
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return self.discordTimestamp(parsed.astimezone(timezone.utc), "f")

    def firstLowerToken(self, content: str) -> str:
        stripped = content.strip()
        if not stripped:
            return ""
        return stripped.split(maxsplit=1)[0].lower()

    def _formatTerminalTime(self, rawValue: object) -> str:
        rawText = str(rawValue or "").strip()
        if not rawText:
            return "never"
        try:
            parsed = datetime.fromisoformat(rawText)
        except ValueError:
            return rawText
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def _janeTerminalAllowedUserId(self) -> int:
        try:
            configured = int(
                getattr(self.config, "janeTerminalAllowedUserId", 0)
                or getattr(self.config, "errorMirrorUserId", 0)
                or 0
            )
        except (TypeError, ValueError):
            configured = 0
        return configured if configured > 0 else 0

    def _readGeneralErrorLogTail(self, *, maxLines: int = 10, maxChars: int = 900) -> list[str]:
        logPathText = str(self.generalErrorLogPath or "").strip()
        if not logPathText:
            return ["(general error log path unavailable)"]
        logPath = Path(logPathText)
        if not logPath.exists():
            return [f"(log file missing: {logPath.name})"]
        try:
            lines = logPath.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            return [f"(failed to read {logPath.name})"]

        filtered = [line.rstrip() for line in lines if line.strip() and not set(line.strip()) <= {"-"}]
        if not filtered:
            return [f"(no entries in {logPath.name})"]

        tailLines = filtered[-maxLines:]
        clipped: list[str] = []
        remainingChars = maxChars
        for line in tailLines:
            compactLine = line[:180]
            if len(compactLine) + 1 > remainingChars:
                break
            clipped.append(compactLine)
            remainingChars -= len(compactLine) + 1
        return clipped or ["(log tail truncated)"]

    def _dbPath(self) -> Path:
        return Path(sqliteDbPath)

    def _buildJaneTerminalContent(self) -> str:
        now = datetime.now(timezone.utc)
        uptime = self.formatUptime(now - self.botStartedAt)
        processResources = self.getProcessResourceSnapshot(now)
        latencyValue = float(getattr(self.botClient, "latency", 0.0) or 0.0)
        latencyText = f"{round(latencyValue * 1000)} ms" if math.isfinite(latencyValue) else "unavailable"

        gitStats: dict[str, Any] = {}
        if self.gitUpdateCoordinator is not None:
            try:
                gitStats = dict(self.gitUpdateCoordinator.getStats())
            except Exception:
                gitStats = {}

        gitCheckText = self._formatTerminalTime(gitStats.get("lastCheckAt"))
        gitUpdateText = self._formatTerminalTime(gitStats.get("lastUpdateAt"))
        gitResultText = str(gitStats.get("lastResult") or "idle").strip() or "idle"

        lines = [
            f"Jane Terminal :: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"status      ONLINE",
            f"uptime      {uptime}",
            f"ping        {latencyText}",
            f"guilds      {len(self.botClient.guilds)}",
            f"cogs        {len(self.botClient.cogs)}",
            f"rss         {processResources.get('rss', 'unavailable')}",
            f"dbSize      {((self._dbPath().stat().st_size / (1024 * 1024)) if self._dbPath().exists() else 0.0):.2f} MB",
            f"gitCheck    {gitCheckText}",
            f"gitUpdate   {gitUpdateText}",
            f"gitResult   {gitResultText}",
            "-" * 54,
            "general-errors tail",
        ]
        lines.extend(self._readGeneralErrorLogTail())

        body = "\n".join(lines)
        if len(body) > 1900:
            body = body[:1897] + "..."
        return f"```ansi\n{body}\n```"

    async def handleJaneHelp(self, message: discord.Message) -> bool:
        if message.author.bot or not message.content:
            return False
        if not message.guild or not isinstance(message.author, discord.Member):
            return False

        token = self.firstLowerToken(message.content or "")
        if token != ":)help":
            return False

        if message.guild.me and message.channel.permissions_for(message.guild.me).manage_messages:
            try:
                await message.delete()
            except Exception:
                pass

        sections = self.helpCommands.buildHelpSections(
            self.botClient.tree,
            guild=message.guild,
        )
        if bool(getattr(self.config, "temporaryCommandLockEnabled", False)):
            allowedIds = sorted(self.permissions.getTemporaryCommandAllowedUserIds())
            restrictionText = (
                "Temporary command lock is ON. Most commands are restricted to: "
                + (", ".join(f"`{userId}`" for userId in allowedIds) if allowedIds else "`(none configured)`")
            )
            if sections:
                overviewSection = dict(sections[0])
                overviewItems = list(overviewSection.get("items") or [])
                overviewItems.insert(
                    0,
                    {
                        "name": "Temporary Command Lock",
                        "description": restrictionText,
                        "permission": "Applies bot-wide until the rollout lock is disabled.",
                    },
                )
                overviewSection["items"] = overviewItems
                sections[0] = overviewSection

        view = runtimeHelpMenu.HelpMenuView(
            openerId=int(message.author.id),
            helpCommandsModule=self.helpCommands,
            sections=sections,
            currentSectionKey="overview",
        )
        await message.channel.send(
            embed=view.buildEmbed(),
            view=view,
            allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False),
        )
        return True

    async def handleJaneRuntime(self, message: discord.Message) -> bool:
        if message.author.bot or not message.content:
            return False

        token = self.firstLowerToken(message.content or "")
        if token != "?janeruntime":
            return False

        if not message.guild or not isinstance(message.author, discord.Member):
            return False

        member = message.author
        allowed = (
            member.id == message.guild.owner_id
            or member.guild_permissions.manage_guild
            or member.guild_permissions.administrator
            or self.hasCohostPermission(member)
        )
        if not allowed:
            try:
                await message.channel.send("You do not have permission to use this command.")
            except Exception:
                pass
            return True

        if message.guild.me and message.channel.permissions_for(message.guild.me).manage_messages:
            try:
                await message.delete()
            except Exception:
                pass

        now = datetime.now(timezone.utc)
        uptime = self.formatUptime(now - self.botStartedAt)
        startedAt = self.discordTimestamp(self.botStartedAt, "s")
        loop = asyncio.get_running_loop()

        def taskState(task: asyncio.Task | None) -> str:
            if task is None:
                return "not started"
            if task.cancelled():
                return "cancelled"
            if task.done():
                return "done"
            return "running"

        embed = discord.Embed(
            title="Jane Runtime",
            color=discord.Color.blurple(),
            timestamp=now,
        )
        embed.add_field(name="Ping", value=f"{round(self.botClient.latency * 1000)} ms", inline=True)
        embed.add_field(name="Uptime", value=uptime, inline=True)
        embed.add_field(name="Started", value=startedAt, inline=False)
        embed.add_field(name="Guilds", value=str(len(self.botClient.guilds)), inline=True)
        embed.add_field(name="Users Cached", value=str(len(self.botClient.users)), inline=True)
        embed.add_field(name="Cogs", value=str(len(self.botClient.cogs)), inline=True)
        nowUtc = datetime.now(timezone.utc)
        weeklyHour, weeklyMinute, weeklyWeekday = self.orbatWeeklyScheduleConfig()
        nextWeekly = self.maintenance.nextWeeklyRunAfter(
            nowUtc,
            weeklyHour,
            weeklyMinute,
            weeklyWeekday,
        )
        autoRecruitmentPayout = bool(self.maintenance.automaticRecruitmentPayoutEnabled())
        nextPayoutText = (
            self.discordTimestamp(self.maintenance.nextRecruitmentPayoutRun(nowUtc), "s")
            if autoRecruitmentPayout
            else "manual-only (disabled)"
        )
        embed.add_field(
            name="Background Tasks",
            value=(
                f"startupMaintenance: `{taskState(self.maintenance.startupMaintenanceTask)}`\n"
                f"globalOrbatUpdate: `{taskState(self.maintenance.globalOrbatUpdateTask)}`"
            ),
            inline=False,
        )
        embed.add_field(
            name="Next Scheduled Checks",
            value=(
                f"weeklyOrbat: {self.discordTimestamp(nextWeekly, 's')}\n"
                f"recruitmentPayout: {nextPayoutText}"
            ),
            inline=False,
        )
        gitStats = {}
        if self.gitUpdateCoordinator is not None:
            try:
                gitStats = dict(self.gitUpdateCoordinator.getStats())
            except Exception:
                gitStats = {}
        if gitStats:
            lastCheckAt = str(gitStats.get("lastCheckAt") or "").strip()
            gitLines = [
                f"lastPull: {self._formatIsoTimestampOrNever(lastCheckAt)}",
                f"lastUpdate: {self._formatIsoTimestampOrNever(gitStats.get('lastUpdateAt'))}",
            ]
            embed.add_field(
                name="Most Recent Git Pull",
                value="\n".join(gitLines),
                inline=False,
            )
        budgetSnapshot = await self.taskBudgeter.getBudgeter().snapshot()
        budgetTotals = budgetSnapshot.get("totals", {}) if isinstance(budgetSnapshot, dict) else {}
        featureStats = budgetSnapshot.get("features", {}) if isinstance(budgetSnapshot, dict) else {}
        queueTelemetry = self.sessionViews.getRuntimeQueueTelemetry()
        pendingBackgroundTasks = (
            int(queueTelemetry.get("bgQueueUpdateActiveTasks", 0))
            + int(queueTelemetry.get("sessionUpdateActiveTasks", 0))
            + int(queueTelemetry.get("bgQueueRepostActiveTasks", 0))
        )
        embed.add_field(
            name="Background Job Telemetry",
            value=(
                f"queueDepth: `{int(budgetTotals.get('waiting', 0))}`\n"
                f"pendingTasks: `{int(budgetTotals.get('pending', 0)) + pendingBackgroundTasks}`\n"
                f"avgOpLatency: `{float(budgetTotals.get('avgLatencyMs', 0.0)):.2f} ms`"
            ),
            inline=False,
        )
        if isinstance(featureStats, dict) and featureStats:
            lines: list[str] = []
            for featureName in sorted(featureStats.keys()):
                stats = featureStats.get(featureName)
                if not isinstance(stats, dict):
                    continue
                lines.append(
                    f"{featureName}: q={int(stats.get('waiting', 0))} "
                    f"in={int(stats.get('inFlight', 0))} "
                    f"lat={float(stats.get('avgLatencyMs', 0.0)):.1f}ms"
                )
            if lines:
                embed.add_field(
                    name="Budgeted Features",
                    value="\n".join(lines[:8]),
                    inline=False,
                )
        if isinstance(self.maintenance.lastConfigSanitySummary, dict):
            warningCount = int(self.maintenance.lastConfigSanitySummary.get("warningCount", 0) or 0)
            errorCount = int(self.maintenance.lastConfigSanitySummary.get("errorCount", 0) or 0)
            embed.add_field(
                name="Config Sanity",
                value=f"errors: `{errorCount}` | warnings: `{warningCount}`",
                inline=True,
            )
        embed.add_field(
            name="Runtime",
            value=f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} | discord.py {discord.__version__}",
            inline=False,
        )
        processResources = self.getProcessResourceSnapshot(now)
        embed.add_field(
            name="Process Resources",
            value=(
                f"pid: `{processResources['pid']}`\n"
                f"cpu(avg): `{processResources['cpuPercent']}`\n"
                f"ram(rss): `{processResources['rss']}`\n"
                f"threads: `{processResources['threads']}`"
            ),
            inline=False,
        )
        dbSizeMb = 0.0
        try:
            dbSizeMb = self._dbPath().stat().st_size / (1024 * 1024)
        except Exception:
            dbSizeMb = 0.0
        embed.add_field(name="DB Size", value=f"{dbSizeMb:.2f} MB", inline=True)
        embed.add_field(
            name="Loop Time",
            value=f"{loop.time():.2f}",
            inline=True,
        )

        sentViaWebhook = await self.sendRuntimeWebhookMessage(message, embed)
        if not sentViaWebhook:
            await message.channel.send(embed=embed)
        return True

    async def handleJaneTerminal(self, message: discord.Message) -> bool:
        if message.author.bot or not message.content:
            return False

        token = self.firstLowerToken(message.content or "")
        if token != "!janeterminal":
            return False

        if not message.guild or not isinstance(message.author, discord.Member):
            return True

        allowedUserId = self._janeTerminalAllowedUserId()
        if allowedUserId <= 0 or int(message.author.id) != allowedUserId:
            if message.guild.me and message.channel.permissions_for(message.guild.me).manage_messages:
                try:
                    await message.delete()
                except Exception:
                    pass
            return True

        if message.guild.me and message.channel.permissions_for(message.guild.me).manage_messages:
            try:
                await message.delete()
            except Exception:
                pass

        terminalContent = self._buildJaneTerminalContent()
        sentViaWebhook = await self.sendTerminalWebhookMessage(message, terminalContent)
        if not sentViaWebhook:
            await message.channel.send(terminalContent)
        return True

    async def handleBgCheckCommand(self, message: discord.Message) -> bool:
        if message.author.bot or not message.content:
            return False
        if not message.guild or not isinstance(message.author, discord.Member):
            return False

        token = self.firstLowerToken(message.content or "")
        if token not in {"?bgcheck", "?bg-check"}:
            return False

        if not self.permissions.hasBgCheckCertifiedRole(message.author):
            await message.channel.send("You do not have permission to start background-check queues.")
            return True

        pendingRoleId = getattr(self.config, "pendingBgRoleId", None)
        try:
            pendingRoleIdInt = int(pendingRoleId) if pendingRoleId else 0
        except (TypeError, ValueError):
            pendingRoleIdInt = 0
        if pendingRoleIdInt <= 0:
            await message.channel.send("Pending Background Check role is not configured.")
            return True

        sourceGuildId = getattr(self.config, "bgCheckSourceGuildId", None) or getattr(self.config, "serverId", None) or message.guild.id
        try:
            sourceGuildIdInt = int(sourceGuildId)
        except (TypeError, ValueError):
            sourceGuildIdInt = int(message.guild.id)

        progress = runtimeBgQueueCommand.BgQueueProgressReporter(
            channel=message.channel,
            sourceGuildId=sourceGuildIdInt,
            totalSteps=5,
        )
        await progress.start("Resolving the source server and pending BG role...")

        sourceGuild = self.botClient.get_guild(sourceGuildIdInt)
        if sourceGuild is None:
            await progress.update(
                stepIndex=1,
                detail="Source guild is not available to Jane right now.",
                failed=True,
            )
            await message.channel.send("Source guild is not available to Jane right now.")
            return True

        pendingRole = sourceGuild.get_role(pendingRoleIdInt)
        if pendingRole is None:
            await progress.update(
                stepIndex=1,
                detail="Pending Background Check role could not be found in the source server.",
                failed=True,
            )
            await message.channel.send("Pending Background Check role could not be found in the source server.")
            return True

        try:
            pendingMembers = await runtimeBgQueueCommand.collectPendingMembers(
                sourceGuild,
                pendingRole,
                pendingRoleIdInt,
                progress,
            )
            if not pendingMembers:
                await progress.update(
                    stepIndex=2,
                    detail="No members currently have the Pending Background Check role.",
                    pendingCount=0,
                    failed=True,
                )
                await message.channel.send("No members currently have the Pending Background Check role.")
                return True

            await progress.update(
                stepIndex=3,
                detail="Creating the BG queue session and attendee list...",
                pendingCount=len(pendingMembers),
            )

            bgQueueChannel = await runtimeBgQueueCommand.resolveBgQueueChannel(
                self.botClient,
                self.config,
                message.channel,
            )
            if bgQueueChannel is None:
                await progress.update(
                    stepIndex=3,
                    detail="BG queue channel is not configured or inaccessible.",
                    pendingCount=len(pendingMembers),
                    failed=True,
                )
                await message.channel.send("BG queue channel is not configured or inaccessible.")
                return True

            if message.guild.me and message.channel.permissions_for(message.guild.me).manage_messages:
                try:
                    await message.delete()
                except Exception:
                    pass

            sessionId = await self.sessionService.createSession(
                guildId=int(sourceGuild.id),
                channelId=int(bgQueueChannel.id),
                messageId=int(message.id),
                sessionType="bg-check",
                hostId=int(message.author.id),
                password=os.urandom(8).hex(),
            )
            attendeeUserIds = [int(member.id) for member in pendingMembers]
            await self.sessionService.addAttendeesBulk(
                int(sessionId),
                attendeeUserIds,
                examGrade="PASS",
            )

            await progress.update(
                stepIndex=4,
                detail=f"Posting the queue panel in <#{bgQueueChannel.id}>...",
                pendingCount=len(pendingMembers),
            )
            await self.sessionViews.postBgQueue(self.botClient, sessionId, sourceGuild)
            await progress.update(
                stepIndex=5,
                detail=(
                    f"Background-check queue created for `{len(pendingMembers)}` member(s) "
                    f"in <#{bgQueueChannel.id}>.\n"
                    "Initial Roblox flag scans will continue in the background."
                ),
                pendingCount=len(pendingMembers),
                finished=True,
            )
        except Exception as exc:
            await progress.update(
                stepIndex=5,
                detail=f"Queue creation failed: `{exc.__class__.__name__}`",
                pendingCount=None,
                failed=True,
            )
            raise

        return True

    async def handleBgLeaderboardCommand(self, message: discord.Message) -> bool:
        if message.author.bot or not message.content:
            return False
        if not message.guild or not isinstance(message.author, discord.Member):
            return False

        token = self.firstLowerToken(message.content or "")
        if token not in {"?bgleaderboard", "?bg-leaderboard"}:
            return False

        if not self.permissions.hasBgCheckCertifiedRole(message.author):
            await message.channel.send("You do not have permission to view the background-check leaderboard.")
            return True

        rows = await self.sessionService.getBgReviewLeaderboard(limit=15)
        if not rows:
            await message.channel.send("No background-check actions are logged yet.")
            return True

        if message.guild.me and message.channel.permissions_for(message.guild.me).manage_messages:
            try:
                await message.delete()
            except Exception:
                pass

        lines: list[str] = []
        for idx, row in enumerate(rows, start=1):
            reviewerId = int(row.get("reviewerId") or 0)
            approvals = int(row.get("approvals") or 0)
            rejections = int(row.get("rejections") or 0)
            total = int(row.get("total") or (approvals + rejections))
            if reviewerId <= 0:
                continue
            lines.append(
                f"{idx}. <@{reviewerId}>  |  Approved: `{approvals}`  |  Rejected: `{rejections}`  |  Total: `{total}`"
            )
        if not lines:
            await message.channel.send("No background-check actions are logged yet.")
            return True

        embed = discord.Embed(
            title="Background Check Leaderboard",
            description="\n".join(lines),
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text="Counts are based on logged approve/reject decisions.")
        await message.channel.send(
            embed=embed,
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        return True

    def _permissionSimulatorGuildAllowed(self, guildId: int) -> bool:
        configured = getattr(self.config, "permissionSimulatorGuildIds", None) or [getattr(self.config, "serverId", 0)]
        allowedIds: set[int] = set()
        for raw in configured:
            try:
                parsed = int(raw)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                allowedIds.add(parsed)
        if not allowedIds:
            return False
        return int(guildId) in allowedIds

    def _likelyCommandAccess(self, member: discord.Member, commandPath: str) -> str:
        path = str(commandPath or "").strip().lower()
        if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
            return "Likely allowed (admin/manage-server bypass)."
        if path.startswith("/orientation"):
            roleId = int(getattr(self.config, "instructorRoleId", 0) or 0)
            hasRole = any(int(role.id) == roleId for role in member.roles) if roleId > 0 else False
            return "Likely allowed." if hasRole else "Likely denied (missing instructor role)."
        if path.startswith("/recruitment"):
            roleId = int(getattr(self.config, "recruiterRoleId", 0) or 0)
            hasRole = any(int(role.id) == roleId for role in member.roles) if roleId > 0 else False
            return "Likely allowed." if hasRole else "Likely denied (missing recruiter role)."
        if path.startswith("/bg-flag"):
            roleIds = self.permissions.getBgCheckCertifiedRoleIds()
            hasRole = any(int(role.id) in roleIds for role in member.roles)
            return "Likely allowed." if hasRole else "Likely denied (missing BG-certified role)."
        if path.startswith("/schedule-event"):
            mr = int(getattr(self.config, "middleRankRoleId", 0) or 0)
            hr = int(getattr(self.config, "highRankRoleId", 0) or 0)
            hasRole = any(int(role.id) in {mr, hr} for role in member.roles if int(role.id) > 0)
            return "Likely allowed." if hasRole else "Likely denied (missing MR/HR role)."
        if path.startswith("/archive") or path.startswith("/best-of") or path.startswith("/jail") or path.startswith("/unjail"):
            return "Likely denied (admin/manage-server required)."
        return "Permission depends on command-specific checks."

    async def handlePermissionSimulatorCommand(self, message: discord.Message) -> bool:
        if message.author.bot or not message.content:
            return False
        if not message.guild or not isinstance(message.author, discord.Member):
            return False

        token = self.firstLowerToken(message.content or "")
        if token not in {"?perm-sim", "?permsim"}:
            return False

        if not self._permissionSimulatorGuildAllowed(int(message.guild.id)):
            await message.channel.send("Permission simulator is only enabled in the test server.")
            return True
        if not (message.author.guild_permissions.administrator or message.author.guild_permissions.manage_guild):
            await message.channel.send("You do not have permission to use this command.")
            return True

        parts = str(message.content or "").strip().split(maxsplit=2)
        if len(parts) < 2:
            await message.channel.send("Usage: `?perm-sim /command-path [@user]`")
            return True
        commandPath = str(parts[1] or "").strip()
        if not commandPath.startswith("/"):
            commandPath = f"/{commandPath.lstrip('/')}"

        targetMember = message.author
        mentions = list(message.mentions)
        if mentions:
            mentioned = mentions[0]
            if isinstance(mentioned, discord.Member):
                targetMember = mentioned
            else:
                resolved = message.guild.get_member(int(mentioned.id))
                if resolved is not None:
                    targetMember = resolved

        hint = self.helpCommands.slashPermissionHint(commandPath)
        likely = self._likelyCommandAccess(targetMember, commandPath)
        roleIds = ", ".join(str(int(role.id)) for role in targetMember.roles if not role.is_default()) or "(none)"

        embed = discord.Embed(
            title="Permission Simulator (Hidden/Test)",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
            description=f"Command: `{commandPath}`\nTarget: {targetMember.mention}",
        )
        embed.add_field(name="Policy Hint", value=hint, inline=False)
        embed.add_field(name="Likely Result", value=likely, inline=False)
        embed.add_field(name="Target Roles", value=roleIds[:1000], inline=False)
        await message.channel.send(embed=embed)
        return True
