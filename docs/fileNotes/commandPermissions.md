# Central Command Permissions

[`runtime/commandPermissions.py`](../../runtime/commandPermissions.py) provides small code-side decorators for Jane's common command gates.

Use these when a slash command only needs one of the common access levels. This avoids repeating Discord.py permission syntax and keeps the actual permission logic centralized.

## Current Levels

- `@commandPermissions.administrator`
  Allows Discord administrators.
- `@commandPermissions.generalStaff`
  Allows General Staff: MR/HR, plus administrator/manage-server.

## Example

```python
from runtime import commandPermissions


@app_commands.command(name="example-admin", description="Admin-only example.")
@commandPermissions.administrator
async def exampleAdmin(self, interaction: discord.Interaction) -> None:
    ...


@app_commands.command(name="example-staff", description="General-staff example.")
@commandPermissions.generalStaff
async def exampleStaff(self, interaction: discord.Interaction) -> None:
    ...
```

The decorator can sit above or below `@app_commands.command`; it handles either order.

## Behavior

The global slash-command interaction check reads the decorator metadata before the command body runs. Commands without one of these decorators keep their existing behavior.

This does not replace contextual checks that depend on command inputs, ownership, selected records, or buttons. Examples include "project creator only", "poll creator only", and "view someone else's ribbon profile".
