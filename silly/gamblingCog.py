from __future__ import annotations

from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

from silly import gamblingService
from silly.gambling import access
from silly.gambling.hubViews import GamblingHubView

# Re-exported for hidden runtime toggle command in silly/commands.py.
isCategoryLockEnabled = access.isCategoryLockEnabled
setCategoryLockEnabled = access.setCategoryLockEnabled
toggleCategoryLockEnabled = access.toggleCategoryLockEnabled


class GamblingCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def buildHubEmbed(self, *, userId: int, wallet: dict, lastStatus: str) -> discord.Embed:
        balance = int(wallet.get("balance") or 0)
        gamesPlayed = int(wallet.get("gamesPlayed") or 0)
        totalLost = int(wallet.get("totalLost") or 0)
        embed = discord.Embed(
            title="Casino Hub",
            description="Choose a game below to open a public table in this channel.",
            color=discord.Color.dark_blue(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Player", value=f"<@{int(userId)}>", inline=True)
        embed.add_field(name="Balance", value=f"`{access.anrobucks(balance)}`", inline=True)
        embed.add_field(name="Games Played", value=str(gamesPlayed), inline=True)
        embed.add_field(name="Total Wagered", value=f"`{access.anrobucks(totalLost)}`", inline=True)
        embed.add_field(name="Status", value=lastStatus, inline=False)
        embed.set_footer(text="Open a table, then use game controls on that table.")
        return embed

    def buildTableEmbed(
        self,
        *,
        userId: int,
        wallet: dict,
        selectedGame: str,
        lastResult: str,
        settingsText: str,
    ) -> discord.Embed:
        balance = int(wallet.get("balance") or 0)
        gamesPlayed = int(wallet.get("gamesPlayed") or 0)
        totalLost = int(wallet.get("totalLost") or 0)
        gameLabel = access.gameLabels.get(selectedGame, "Slots")
        embed = discord.Embed(
            title=f"{gameLabel} Table",
            description="Place bets and use the game controls below.",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc),
        )
        if selectedGame == "russianroulette":
            embed.description = (
                "Multi-user table. Configure bullets, join, then pull trigger.\n"
                "Warning: if the chamber fires, you are muted for 30 minutes."
            )
        if selectedGame == "texasholdem":
            embed.description = "Multi-user table. Join, then deal a hand."
        embed.add_field(name="Player", value=f"<@{int(userId)}>", inline=True)
        embed.add_field(name="Balance", value=f"`{access.anrobucks(balance)}`", inline=True)
        embed.add_field(name="Games Played", value=str(gamesPlayed), inline=True)
        embed.add_field(name="Total Wagered", value=f"`{access.anrobucks(totalLost)}`", inline=True)
        embed.add_field(name="Settings", value=settingsText, inline=True)
        embed.add_field(name="Last Round", value=lastResult, inline=False)
        if selectedGame in {"russianroulette", "texasholdem"}:
            embed.set_footer(text="Any member can join this table.")
        else:
            embed.set_footer(text=f"Only this player can use this table: {userId}")
        return embed

    @app_commands.command(name="gambling", description="Open the casino game hub.")
    async def gambling(self, interaction: discord.Interaction) -> None:
        if not await access.enforceGamblingRoleAccess(interaction):
            return
        userId = int(getattr(interaction.user, "id", 0) or 0)
        wallet = await gamblingService.getWallet(userId)
        view = GamblingHubView(self, userId=userId)
        embed = self.buildHubEmbed(
            userId=userId,
            wallet=wallet,
            lastStatus="Select a game below to post a table.",
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        try:
            view.hubMessage = await interaction.original_response()
        except discord.HTTPException:
            view.hubMessage = None


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(GamblingCog(bot))
