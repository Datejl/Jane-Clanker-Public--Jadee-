# Plugin Layout

Jane's extension loading is split into three layers:

- `core`
  Existing built-in cogs and legacy `silly` extensions that ship with the main repo.
- `plugins.public`
  Optional public-safe extensions that can exist in the public repo.
- `plugins.private`
  Optional private-only extensions that should exist only in private deployments.

The current startup flow loads:

1. built-in core extensions from `runtime/extensionLayout.py`
2. any extra extensions listed in `config.extraExtensionNames`
3. any optional modules listed in:
   - `plugins/public/extensionList.py`
   - `plugins/private/extensionList.py`

Private extensions are only loaded when `config.enablePrivateExtensions` is truthy.

Destructive private actions should also require:

- `ENABLE_DESTRUCTIVE_COMMANDS=1`
- an allowed user
- an allowed guild from `config.destructiveCommandGuildIds`
- surviving the shared destructive-action cooldown

This keeps Jane's current layout stable while giving the repo a clear place to move public-safe and private-only extensions over time.
