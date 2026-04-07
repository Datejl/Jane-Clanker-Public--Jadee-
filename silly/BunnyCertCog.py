
from discord.ext import commands
from discord import app_commands, Interaction

from silly.bunnyCertification.service import handleCode


class BunnyCertCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="bunny-cert")
    @app_commands.guild_only()
    async def redeem_bunny_cert(
            self,
            interaction: Interaction,
            code: str
    ):
        await handleCode(self.bot, interaction, code)


async def setup(bot):
    await bot.add_cog(BunnyCertCog(bot))