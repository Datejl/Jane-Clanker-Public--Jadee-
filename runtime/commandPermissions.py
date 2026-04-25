from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, TypeVar

import discord
from discord import app_commands


_PERMISSION_ATTR = "__jane_permission_level__"
_PERMISSION_EXTRA = "janePermissionLevel"
_PATH_HINTS: dict[str, str] = {}
_T = TypeVar("_T")


@dataclass(frozen=True)
class PermissionPolicy:
    key: str
    label: str
    denyMessage: str


@dataclass(frozen=True)
class PermissionCheckResult:
    allowed: bool
    level: str = ""
    message: str = ""


_ADMINISTRATOR = PermissionPolicy(
    key="administrator",
    label="Administrator only.",
    denyMessage="Administrator permission required.",
)
_GENERAL_STAFF = PermissionPolicy(
    key="general_staff",
    label="General Staff (MR/HR) or administrator/manage-server required.",
    denyMessage="General Staff (MR/HR) or administrator/manage-server required.",
)
_POLICIES = {
    _ADMINISTRATOR.key: _ADMINISTRATOR,
    _GENERAL_STAFF.key: _GENERAL_STAFF,
}


def normalizeCommandPath(value: object) -> str:
    text = str(value or "").strip().lower()
    while text.startswith("/"):
        text = text[1:]
    return " ".join(part for part in text.replace(".", " ").split() if part)


def _policy(level: str) -> PermissionPolicy:
    policy = _POLICIES.get(str(level or "").strip().lower())
    if policy is None:
        raise ValueError(f"Unknown Jane permission level: {level!r}")
    return policy


def _setPermission(target: _T, level: str) -> _T:
    policy = _policy(level)
    extras = getattr(target, "extras", None)
    if isinstance(extras, dict):
        extras[_PERMISSION_EXTRA] = policy.key

    callback = getattr(target, "callback", None)
    if callback is not None:
        setattr(callback, _PERMISSION_ATTR, policy.key)
    else:
        setattr(target, _PERMISSION_ATTR, policy.key)
    return target


def _decorator(level: str, target: _T | None = None) -> Callable[[_T], _T] | _T:
    def apply(inner: _T) -> _T:
        return _setPermission(inner, level)

    if target is None:
        return apply
    return apply(target)


def administrator(target: _T | None = None) -> Callable[[_T], _T] | _T:
    return _decorator(_ADMINISTRATOR.key, target)


def generalStaff(target: _T | None = None) -> Callable[[_T], _T] | _T:
    return _decorator(_GENERAL_STAFF.key, target)


# Short aliases for cogs that prefer compact decorators.
admin = administrator
adminOnly = administrator
general = generalStaff
generalStaffOnly = generalStaff


def _commandPermissionLevel(command: object) -> str:
    current = command
    while current is not None:
        extras = getattr(current, "extras", None)
        if isinstance(extras, dict):
            level = str(extras.get(_PERMISSION_EXTRA) or "").strip()
            if level:
                return level

        callback = getattr(current, "callback", None)
        level = str(getattr(callback, _PERMISSION_ATTR, "") or "").strip()
        if level:
            return level

        level = str(getattr(current, _PERMISSION_ATTR, "") or "").strip()
        if level:
            return level

        current = getattr(current, "parent", None)
    return ""


def _walkInteractionCommand(tree: app_commands.CommandTree, interaction: discord.Interaction) -> object | None:
    data = interaction.data if isinstance(interaction.data, dict) else {}
    rootName = str(data.get("name") or "").strip()
    if not rootName:
        return None

    command = None
    guild = getattr(interaction, "guild", None)
    try:
        if guild is not None:
            command = tree.get_command(rootName, guild=guild, type=discord.AppCommandType.chat_input)
    except TypeError:
        try:
            command = tree.get_command(rootName, guild=guild) if guild is not None else None
        except TypeError:
            command = None
    if command is None:
        try:
            command = tree.get_command(rootName, type=discord.AppCommandType.chat_input)
        except TypeError:
            try:
                command = tree.get_command(rootName)
            except TypeError:
                command = None
    if command is None:
        return None

    options = data.get("options")
    while isinstance(options, list) and options:
        first = options[0]
        if not isinstance(first, dict):
            break
        optionType = int(first.get("type") or 0)
        if optionType not in {1, 2}:
            break
        optionName = str(first.get("name") or "").strip()
        getChild = getattr(command, "get_command", None)
        if not optionName or not callable(getChild):
            break
        child = getChild(optionName)
        if child is None:
            break
        command = child
        options = first.get("options")

    return command


def resolveInteractionCommand(
    interaction: discord.Interaction,
    *,
    tree: app_commands.CommandTree | None = None,
) -> object | None:
    command = getattr(interaction, "command", None)
    if command is not None:
        return command
    if tree is None:
        client = getattr(interaction, "client", None)
        tree = getattr(client, "tree", None)
    if isinstance(tree, app_commands.CommandTree):
        return _walkInteractionCommand(tree, interaction)
    return None


def _configuredRoleId(configModule: Any, key: str) -> int:
    try:
        parsed = int(getattr(configModule, key, 0) or 0)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _hasAnyRole(member: discord.Member, roleIds: set[int]) -> bool:
    if not roleIds:
        return False
    return any(int(role.id) in roleIds for role in getattr(member, "roles", []))


def _isAdministrator(member: discord.Member) -> bool:
    return bool(member.guild_permissions.administrator)


def _isAdminOrManageGuild(member: discord.Member) -> bool:
    return bool(member.guild_permissions.administrator or member.guild_permissions.manage_guild)


def _isGeneralStaff(configModule: Any, member: discord.Member) -> bool:
    roleIds = {
        roleId
        for roleId in (
            _configuredRoleId(configModule, "middleRankRoleId"),
            _configuredRoleId(configModule, "highRankRoleId"),
        )
        if roleId > 0
    }
    return _isAdminOrManageGuild(member) or _hasAnyRole(member, roleIds)


def checkInteraction(
    configModule: Any,
    interaction: discord.Interaction,
    *,
    command: object | None = None,
    tree: app_commands.CommandTree | None = None,
) -> PermissionCheckResult:
    resolvedCommand = command or resolveInteractionCommand(interaction, tree=tree)
    level = _commandPermissionLevel(resolvedCommand)
    if not level:
        return PermissionCheckResult(True)

    policy = _policy(level)
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None:
        return PermissionCheckResult(False, level=level, message="This command can only be used in a server channel.")

    if policy.key == _ADMINISTRATOR.key:
        allowed = _isAdministrator(member)
    elif policy.key == _GENERAL_STAFF.key:
        allowed = _isGeneralStaff(configModule, member)
    else:
        allowed = False

    if allowed:
        return PermissionCheckResult(True, level=level)
    return PermissionCheckResult(False, level=level, message=policy.denyMessage)


def permissionHintForCommand(command: object) -> str:
    level = _commandPermissionLevel(command)
    if not level:
        return ""
    return _policy(level).label


def recordCommandPath(path: object, command: object) -> None:
    normalizedPath = normalizeCommandPath(path)
    hint = permissionHintForCommand(command)
    if normalizedPath and hint:
        _PATH_HINTS[normalizedPath] = hint


def permissionHintForPath(path: object) -> str:
    return _PATH_HINTS.get(normalizeCommandPath(path), "")
