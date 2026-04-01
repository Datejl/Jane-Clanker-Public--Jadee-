from __future__ import annotations

from typing import Any

import discord

from runtime import interaction as interactionRuntime


def configuredRoleMenus(configModule: Any) -> dict[str, dict[str, Any]]:
    raw = getattr(configModule, "publicRoleMenus", {}) or {}
    return raw if isinstance(raw, dict) else {}


def menuConfig(configModule: Any, menuKey: str) -> dict[str, Any] | None:
    menus = configuredRoleMenus(configModule)
    row = menus.get(str(menuKey or "").strip())
    return row if isinstance(row, dict) else None


class RoleMenuSelect(discord.ui.Select):
    def __init__(self, *, configModule: Any, menuKey: str):
        self.configModule = configModule
        self.menuKey = str(menuKey or "").strip()
        row = menuConfig(configModule, self.menuKey) or {}
        roleIds = [int(roleId) for roleId in (row.get("roleIds") or []) if int(roleId) > 0]
        options = [
            discord.SelectOption(label=f"Role {roleId}", value=str(roleId))
            for roleId in roleIds[:25]
        ]
        super().__init__(
            placeholder=str(row.get("placeholder") or "Choose your roles")[:150],
            min_values=0,
            max_values=max(1, len(options)) if options else 1,
            options=options or [discord.SelectOption(label="No roles configured", value="0")],
            disabled=not bool(options),
            custom_id=f"public-role-menu:{self.menuKey}",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interactionRuntime.safeInteractionReply(
                interaction,
                content="This menu can only be used inside a server.",
                ephemeral=True,
            )
        row = menuConfig(self.configModule, self.menuKey) or {}
        roleIds = [int(roleId) for roleId in (row.get("roleIds") or []) if int(roleId) > 0]
        selectedRoleIds = {int(value) for value in self.values if str(value).isdigit()}

        me = interaction.guild.me or interaction.guild.get_member(int(getattr(interaction.client.user, "id", 0) or 0))
        if me is None:
            return await interactionRuntime.safeInteractionReply(
                interaction,
                content="Jane could not resolve her member state here.",
                ephemeral=True,
            )

        addRoles: list[discord.Role] = []
        removeRoles: list[discord.Role] = []
        for roleId in roleIds:
            role = interaction.guild.get_role(roleId)
            if role is None or role.managed or role.is_default():
                continue
            if role.position >= me.top_role.position:
                continue
            if roleId in selectedRoleIds and role not in interaction.user.roles:
                addRoles.append(role)
            if roleId not in selectedRoleIds and role in interaction.user.roles:
                removeRoles.append(role)

        try:
            if addRoles:
                await interaction.user.add_roles(*addRoles, reason="Public role menu opt-in.")
            if removeRoles:
                await interaction.user.remove_roles(*removeRoles, reason="Public role menu opt-out.")
        except (discord.Forbidden, discord.HTTPException):
            return await interactionRuntime.safeInteractionReply(
                interaction,
                content="I could not update your roles.",
                ephemeral=True,
            )

        await interactionRuntime.safeInteractionReply(
            interaction,
            content=f"Updated roles for menu `{self.menuKey}`.",
            ephemeral=True,
        )


class RoleMenuView(discord.ui.View):
    def __init__(self, *, configModule: Any, menuKey: str):
        super().__init__(timeout=None)
        self.add_item(RoleMenuSelect(configModule=configModule, menuKey=menuKey))
