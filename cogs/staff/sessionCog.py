import discord
from discord.ext import commands
from discord import app_commands
from features.staff.clockins import ClockinEngine, OrientationClockinAdapter
from features.staff.sessions.views import SessionView, updateSessionMessage
from runtime import interaction as interactionRuntime
import config

class SessionsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._orientationAdapter = OrientationClockinAdapter()
        self._orientationEngine = ClockinEngine(bot, self._orientationAdapter)

    async def _safeEphemeral(self, interaction: discord.Interaction, message: str) -> None:
        await interactionRuntime.safeInteractionReply(
            interaction,
            content=message,
            ephemeral=True,
        )

    def canStart(self, member: discord.Member) -> bool:
        if config.instructorRoleId is None:
            return True
        return any(r.id == config.instructorRoleId for r in member.roles)

    @app_commands.command(name="orientation", description="Start an orientation session.")
    @app_commands.describe(password="Password attendees must enter to join.")
    async def orientation(self, interaction: discord.Interaction, password: str):
        if not interaction.guild or not interaction.channel:
            return await self._safeEphemeral(interaction, "This command can only be used inside a server channel.")
        if not isinstance(interaction.user, discord.Member):
            return await self._safeEphemeral(interaction, "This command can only be used inside a server channel.")
        if not self.canStart(interaction.user):
            return await self._safeEphemeral(interaction, "You do not have permission to start orientation sessions.")

        # Create placeholder message first so we can store messageId
        await self._safeEphemeral(interaction, "Creating orientation session...")

        channel = interaction.channel
        host = interaction.user
        guildId = int(interaction.guild_id or interaction.guild.id)
        sessionId = await self._orientationEngine.createSession(
            guildId=guildId,
            channelId=int(channel.id),
            hostId=int(host.id),
            sessionType="orientation",
            password=password,
            messageId=0,
        )
        session = await self._orientationEngine.getSession(int(sessionId))
        tempEmbed = (
            self._orientationAdapter.buildEmbed(session, [])
            if session is not None
            else discord.Embed(
                title="Orientation Session",
                description="Click the \u2705 button below to join the session!",
            )
        )
        try:
            tempMessage = await channel.send(embed=tempEmbed, view=SessionView(int(sessionId)))
        except discord.Forbidden:
            await self._orientationEngine.updateSessionStatus(int(sessionId), "CANCELED")
            return await self._safeEphemeral(interaction, "I do not have permission to send messages in this channel.")
        except discord.HTTPException:
            await self._orientationEngine.updateSessionStatus(int(sessionId), "CANCELED")
            return await self._safeEphemeral(interaction, "I could not create the orientation session message.")
        await self._orientationEngine.setSessionMessageId(int(sessionId), int(tempMessage.id))

        # Update message with correct view custom_ids
        await updateSessionMessage(self.bot, int(sessionId))

        await self._safeEphemeral(interaction, f"Password: ||{password}||")

async def setup(bot: commands.Bot):
    await bot.add_cog(SessionsCog(bot))


