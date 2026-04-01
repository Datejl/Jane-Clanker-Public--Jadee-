from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import discord

from silly import gamblingService
from silly.gambling import access, blackjack, russianRoulette, texasHoldem
from silly.gambling.common import sanitizeBet, trimRoundText


class GamblingTableView(discord.ui.View):
    def __init__(
        self,
        cog: Any,
        *,
        userId: int,
        selectedGame: str,
    ):
        super().__init__(timeout=3600)
        self.cog = cog
        self.userId = int(userId)
        self.selectedGame = selectedGame if access.isGameEnabled(selectedGame) else access.defaultGameKey()
        self.lastResult = ""

        self.roundActive = False
        self.pendingStake = 0
        self.roundBalanceBefore = 0
        self.tableEnded = False
        self.restarted = False

        self.blackjackState = blackjack.createState()
        self.gameState = {}
        self.texasHoldemState = texasHoldem.createState()
        self.russianRouletteState = russianRoulette.createState()
        if self.selectedGame in access.nonBlackjackModules:
            self.gameState = access.nonBlackjackModules[self.selectedGame].createState()
        if self._isNoBetTable():
            # Multi-user tables do not use currency betting controls.
            for item in (self.bet10Btn, self.bet50Btn, self.bet100Btn, self.allInBtn):
                self.remove_item(item)

    def _isBlackjackTable(self) -> bool:
        return self.selectedGame == "blackjack"

    def _isRussianRouletteTable(self) -> bool:
        return self.selectedGame == "russianroulette"

    def _isTexasHoldemTable(self) -> bool:
        return self.selectedGame == "texasholdem"

    def _isNoBetTable(self) -> bool:
        return self.selectedGame in access.multiUserModules

    @staticmethod
    def _interactionUserId(interaction: discord.Interaction) -> int:
        return int(getattr(interaction.user, "id", 0) or 0)

    def _multiUserModule(self):
        return access.multiUserModules.get(self.selectedGame)

    def _multiUserState(self) -> dict | None:
        if self._isRussianRouletteTable():
            return self.russianRouletteState
        if self._isTexasHoldemTable():
            return self.texasHoldemState
        return None

    def _nonBlackjackModule(self):
        return access.nonBlackjackModules.get(self.selectedGame)

    def _roundInProgress(self) -> bool:
        if self._isBlackjackTable():
            return bool(self.blackjackState.get("active"))
        return bool(self.roundActive)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not access.isGameEnabled(self.selectedGame):
            await interaction.response.send_message(access.disabledGameMessage, ephemeral=True)
            return False
        if not await access.enforceGamblingRoleAccess(interaction):
            return False
        if self._isRussianRouletteTable() and self.tableEnded:
            data = getattr(interaction, "data", None)
            customId = str((data or {}).get("custom_id", "")) if isinstance(data, dict) else ""
            if customId != "gambling-table-refresh":
                await interaction.response.send_message(
                    "This Russian Roulette round has ended. Use **Start New Round**.",
                    ephemeral=True,
                )
                return False
        if self._isNoBetTable():
            if bool(getattr(interaction.user, "bot", False)):
                await interaction.response.send_message("Bots cannot join this table.", ephemeral=True)
                return False
            if not await access.enforceGamblingButtonCooldown(interaction):
                return False
            return True
        if int(getattr(interaction.user, "id", 0) or 0) != self.userId:
            await interaction.response.send_message(f"Only <@{self.userId}> can play this table.", ephemeral=True)
            return False
        if not await access.enforceGamblingButtonCooldown(interaction):
            return False
        return True

    async def _resolveGuildMember(self, interaction: discord.Interaction) -> discord.Member | None:
        if interaction.guild is None:
            return None
        if isinstance(interaction.user, discord.Member):
            return interaction.user
        safeUserId = int(getattr(interaction.user, "id", 0) or 0)
        if safeUserId <= 0:
            return None
        member = interaction.guild.get_member(safeUserId)
        if member is not None:
            return member
        try:
            return await interaction.guild.fetch_member(safeUserId)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    def tablePrompt(self) -> str:
        if self._isRussianRouletteTable():
            return russianRoulette.promptText()
        if self._isTexasHoldemTable():
            return texasHoldem.promptText()
        if self._isBlackjackTable():
            return blackjack.promptText()
        module = self._nonBlackjackModule()
        if module is None:
            return "Place a bet to begin."
        return module.promptText()

    def tableSettingsText(self) -> str:
        if self._isRussianRouletteTable():
            return russianRoulette.settingsText(self.russianRouletteState)
        if self._isTexasHoldemTable():
            return texasHoldem.settingsText(self.texasHoldemState)
        if self._isBlackjackTable():
            return blackjack.settingsText(self.blackjackState)
        module = self._nonBlackjackModule()
        if module is None:
            return "N/A"
        return module.settingsText(self.gameState)

    def _syncControls(self, *, balance: int) -> None:
        self.refreshBtn.label = "Refresh"
        self.refreshBtn.style = discord.ButtonStyle.primary
        self.refreshBtn.disabled = False

        if self._isRussianRouletteTable() and self.tableEnded:
            self.actionBtn.label = "Round Ended"
            self.actionBtn.style = discord.ButtonStyle.secondary
            self.actionBtn.disabled = True

            self.configBtn.label = "Round Ended"
            self.configBtn.style = discord.ButtonStyle.secondary
            self.configBtn.disabled = True

            self.quickBtn.label = "Round Ended"
            self.quickBtn.style = discord.ButtonStyle.secondary
            self.quickBtn.disabled = True

            self.refreshBtn.label = "Start New Round"
            self.refreshBtn.style = discord.ButtonStyle.success
            self.refreshBtn.disabled = self.restarted
            return

        disableBets = balance <= 0 or self._roundInProgress() or self._isNoBetTable()
        self.bet10Btn.disabled = disableBets
        self.bet50Btn.disabled = disableBets
        self.bet100Btn.disabled = disableBets
        self.allInBtn.disabled = disableBets

        multiUserModule = self._multiUserModule()
        if multiUserModule is not None:
            self.actionBtn.label = multiUserModule.actionLabel()
            self.actionBtn.style = (
                discord.ButtonStyle.danger if self._isRussianRouletteTable() else discord.ButtonStyle.success
            )
            self.actionBtn.disabled = False

            if self._isRussianRouletteTable():
                bulletsLoaded = russianRoulette.bulletCount(self.russianRouletteState)
                self.configBtn.label = f"Bullets: {bulletsLoaded}/6"
            else:
                self.configBtn.label = multiUserModule.configLabel()
            self.configBtn.style = discord.ButtonStyle.secondary
            self.configBtn.disabled = False

            self.quickBtn.label = multiUserModule.quickLabel()
            self.quickBtn.style = discord.ButtonStyle.secondary
            self.quickBtn.disabled = False
            return

        if self._isBlackjackTable():
            active = bool(self.blackjackState.get("active"))
            self.actionBtn.label = "Hit"
            self.actionBtn.style = discord.ButtonStyle.success
            self.actionBtn.disabled = not active

            self.configBtn.label = "Stand"
            self.configBtn.style = discord.ButtonStyle.secondary
            self.configBtn.disabled = not active

            self.quickBtn.label = "Table Rules"
            self.quickBtn.style = discord.ButtonStyle.secondary
            self.quickBtn.disabled = True
            return

        module = self._nonBlackjackModule()
        if module is None:
            self.actionBtn.label = "Action"
            self.actionBtn.disabled = True
            self.configBtn.label = "Configure"
            self.configBtn.disabled = True
            self.quickBtn.label = "Quick"
            self.quickBtn.disabled = True
            return

        self.actionBtn.label = module.actionLabel()
        self.actionBtn.style = discord.ButtonStyle.success
        self.actionBtn.disabled = not self.roundActive

        self.configBtn.label = module.configLabel(self.gameState)[:80]
        self.configBtn.style = discord.ButtonStyle.secondary
        self.configBtn.disabled = False

        self.quickBtn.label = module.quickLabel()
        self.quickBtn.style = discord.ButtonStyle.secondary
        self.quickBtn.disabled = False

    async def _startConfiguredRound(self, interaction: discord.Interaction, stake: int) -> None:
        walletBefore = await gamblingService.getWallet(self.userId)
        self.roundBalanceBefore = int(walletBefore.get("balance") or 0)

        if await gamblingService.applyLossBet(self.userId, stake) is None:
            await interaction.response.send_message("Unable to process that bet right now.", ephemeral=True)
            return

        self.pendingStake = int(stake)
        self.roundActive = True
        self.lastResult = trimRoundText(f"Bet locked: `{access.anrobucks(stake)}`.\n{self.tablePrompt()}")
        await self.refresh(interaction)

    async def _resolveConfiguredRound(self, interaction: discord.Interaction) -> None:
        if not self.roundActive:
            await interaction.response.send_message("Place a bet first.", ephemeral=True)
            return

        module = self._nonBlackjackModule()
        if module is None:
            await interaction.response.send_message("This table is not available.", ephemeral=True)
            return

        roundText = module.resolveRound(self.gameState)
        walletAfter = await gamblingService.getWallet(self.userId)
        stake = int(self.pendingStake)

        self.roundActive = False
        self.pendingStake = 0
        self.lastResult = trimRoundText(
            (
                f"Bet: `{access.anrobucks(stake)}` | Payout: `{access.anrobucks(0)}`\n"
                f"{roundText}\n"
                f"Balance: `{access.anrobucks(self.roundBalanceBefore)}` -> `{access.anrobucks(int(walletAfter.get('balance') or 0))}`"
            )
        )
        await self.refresh(interaction)

    async def _playWithBet(self, interaction: discord.Interaction, betAmount: int) -> None:
        if self._isNoBetTable():
            actionLabel = "Pull Trigger" if self._isRussianRouletteTable() else "Deal Hand"
            gameLabel = access.gameLabels.get(self.selectedGame, "This table")
            await interaction.response.send_message(
                f"{gameLabel} does not use table bets. Use **Join/Leave** then **{actionLabel}**.",
                ephemeral=True,
            )
            return

        stake = sanitizeBet(betAmount)
        if stake <= 0:
            await interaction.response.send_message("Bet must be at least `1 anrobucks`.", ephemeral=True)
            return
        if self._roundInProgress():
            await interaction.response.send_message("Finish the current round before placing a new bet.", ephemeral=True)
            return

        wallet = await gamblingService.getWallet(self.userId)
        balance = int(wallet.get("balance") or 0)
        if balance <= 0:
            self.lastResult = f"Balance is `{access.anrobucks(0)}`."
            await self.refresh(interaction)
            return
        if stake > balance:
            await interaction.response.send_message(
                f"You only have `{access.anrobucks(balance)}` available.",
                ephemeral=True,
            )
            return

        if self._isBlackjackTable():
            if await gamblingService.applyLossBet(self.userId, stake) is None:
                await interaction.response.send_message("Unable to process that bet right now.", ephemeral=True)
                return
            self.lastResult = blackjack.startRound(self.blackjackState, stake)
            await self.refresh(interaction)
            return

        await self._startConfiguredRound(interaction, stake)

    async def refresh(self, interaction: discord.Interaction) -> None:
        wallet = await gamblingService.getWallet(self.userId)
        self._syncControls(balance=int(wallet.get("balance") or 0))

        embed = self.cog.buildTableEmbed(
            userId=self.userId,
            wallet=wallet,
            selectedGame=self.selectedGame,
            lastResult=self.lastResult or self.tablePrompt(),
            settingsText=self.tableSettingsText(),
        )
        await access.safeEditInteractionMessage(interaction, embed=embed, view=self)

    @discord.ui.button(label="Bet 10 anrobucks", style=discord.ButtonStyle.secondary, row=0)
    async def bet10Btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._playWithBet(interaction, 10)

    @discord.ui.button(label="Bet 50 anrobucks", style=discord.ButtonStyle.secondary, row=0)
    async def bet50Btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._playWithBet(interaction, 50)

    @discord.ui.button(label="Bet 100 anrobucks", style=discord.ButtonStyle.secondary, row=0)
    async def bet100Btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._playWithBet(interaction, 100)

    @discord.ui.button(label="All In", style=discord.ButtonStyle.danger, row=0)
    async def allInBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        wallet = await gamblingService.getWallet(self.userId)
        await self._playWithBet(interaction, int(wallet.get("balance") or 0))

    @discord.ui.button(label="Action", style=discord.ButtonStyle.success, row=1)
    async def actionBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self._isRussianRouletteTable():
            await access.safeDeferInteraction(interaction)
            member = await self._resolveGuildMember(interaction)
            if member is None:
                self.lastResult = "Russian Roulette can only be used in a server channel."
                await self.refresh(interaction)
                return

            result = russianRoulette.pullTrigger(self.russianRouletteState, int(member.id))
            timeoutApplied = False
            timeoutFailureReason = ""
            fired = bool(result.get("fired"))
            if fired:
                timeoutUntil = datetime.now(timezone.utc) + timedelta(minutes=30)
                try:
                    await member.edit(
                        timed_out_until=timeoutUntil,
                        reason="Russian Roulette: loaded chamber fired.",
                    )
                    timeoutApplied = True
                except discord.Forbidden:
                    timeoutFailureReason = "missing permissions"
                except discord.HTTPException:
                    timeoutFailureReason = "api error"

            self.lastResult = russianRoulette.formatShotResult(
                userId=int(member.id),
                chamber=int(result.get("chamber") or 1),
                bulletCount=int(result.get("bulletCount") or 1),
                fired=fired,
                timeoutApplied=timeoutApplied,
                timeoutFailureReason=timeoutFailureReason,
            )
            if fired:
                self.tableEnded = True
                self.lastResult = trimRoundText(
                    f"{self.lastResult}\nRound ended. Press **Start New Round** to post a new table."
                )
            await self.refresh(interaction)
            return
        if self._isTexasHoldemTable():
            await access.safeDeferInteraction(interaction)
            safeUserId = self._interactionUserId(interaction)
            if safeUserId <= 0:
                self.lastResult = "Unable to identify this user."
                await self.refresh(interaction)
                return
            self.lastResult = texasHoldem.resolveRound(self.texasHoldemState, requesterId=safeUserId)
            await self.refresh(interaction)
            return

        if self._isBlackjackTable():
            if not bool(self.blackjackState.get("active")):
                await interaction.response.send_message("Place a blackjack bet first.", ephemeral=True)
                return
            _, text = blackjack.hit(self.blackjackState)
            self.lastResult = text
            await self.refresh(interaction)
            return

        await self._resolveConfiguredRound(interaction)

    @discord.ui.button(label="Configure", style=discord.ButtonStyle.secondary, row=1)
    async def configBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self._isNoBetTable():
            safeUserId = self._interactionUserId(interaction)
            if safeUserId <= 0:
                await interaction.response.send_message("Unable to identify this user.", ephemeral=True)
                return
            multiUserModule = self._multiUserModule()
            state = self._multiUserState()
            if multiUserModule is None:
                await interaction.response.send_message("This table is not available.", ephemeral=True)
                return
            if state is None:
                await interaction.response.send_message("This table is not available.", ephemeral=True)
                return
            if self._isRussianRouletteTable():
                if safeUserId != self.userId:
                    await interaction.response.send_message(
                        "Only the table owner can change bullet load.",
                        ephemeral=True,
                    )
                    return
                bulletsLoaded = russianRoulette.cycleBulletCount(state)
                self.lastResult = (
                    f"Revolver load set to `{bulletsLoaded}/6` bullet(s).\n"
                    f"{russianRoulette.warningText()}"
                )
                await self.refresh(interaction)
                return
            joined = multiUserModule.toggleParticipant(state, safeUserId)
            gameLabel = access.gameLabels.get(self.selectedGame, "table")
            if joined:
                self.lastResult = f"<@{safeUserId}> joined the {gameLabel} table."
            else:
                self.lastResult = f"<@{safeUserId}> left the {gameLabel} table."
            await self.refresh(interaction)
            return

        if self._isBlackjackTable():
            if not bool(self.blackjackState.get("active")):
                await interaction.response.send_message("Place a blackjack bet first.", ephemeral=True)
                return
            self.lastResult = blackjack.stand(self.blackjackState)
            await self.refresh(interaction)
            return

        module = self._nonBlackjackModule()
        if module is None:
            await interaction.response.send_message("This table is not available.", ephemeral=True)
            return

        module.cycleConfig(self.gameState)
        if not self.roundActive:
            self.lastResult = self.tablePrompt()
        await self.refresh(interaction)

    @discord.ui.button(label="Quick Pick", style=discord.ButtonStyle.secondary, row=1)
    async def quickBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self._isNoBetTable():
            multiUserModule = self._multiUserModule()
            state = self._multiUserState()
            if multiUserModule is None:
                await interaction.response.send_message("This table is not available.", ephemeral=True)
                return
            if state is None:
                await interaction.response.send_message("This table is not available.", ephemeral=True)
                return
            self.lastResult = trimRoundText("Current roster:\n" + multiUserModule.rosterMentions(state))
            await self.refresh(interaction)
            return

        if self._isBlackjackTable():
            await interaction.response.send_message("Use Hit or Stand during blackjack hands.", ephemeral=True)
            return

        module = self._nonBlackjackModule()
        if module is None:
            await interaction.response.send_message("This table is not available.", ephemeral=True)
            return

        module.randomizeConfig(self.gameState)
        if not self.roundActive:
            self.lastResult = self.tablePrompt()
        await self.refresh(interaction)

    @discord.ui.button(
        label="Refresh",
        style=discord.ButtonStyle.primary,
        row=2,
        custom_id="gambling-table-refresh",
    )
    async def refreshBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self._isRussianRouletteTable() and self.tableEnded:
            await access.safeDeferInteraction(interaction)
            channel = interaction.channel
            if channel is None or not hasattr(channel, "send"):
                self.lastResult = trimRoundText(
                    f"{self.lastResult}\nUnable to post a new round in this channel."
                )
                await self.refresh(interaction)
                return

            newTableView = GamblingTableView(self.cog, userId=self.userId, selectedGame="russianroulette")
            wallet = await gamblingService.getWallet(self.userId)
            newTableEmbed = self.cog.buildTableEmbed(
                userId=self.userId,
                wallet=wallet,
                selectedGame="russianroulette",
                lastResult=newTableView.tablePrompt(),
                settingsText=newTableView.tableSettingsText(),
            )
            try:
                await channel.send(
                    content=f"{interaction.user.mention} started a new **{access.gameLabels['russianroulette']}** round.",
                    embed=newTableEmbed,
                    view=newTableView,
                )
            except discord.HTTPException:
                self.lastResult = trimRoundText(
                    f"{self.lastResult}\nFailed to post a new round."
                )
                await self.refresh(interaction)
                return

            self.restarted = True
            self.lastResult = trimRoundText(
                f"{self.lastResult}\nNew round posted below."
            )
            await self.refresh(interaction)
            return
        await self.refresh(interaction)
