# textCommands.py

[`runtime/textCommands.py`](../../runtime/textCommands.py) is where a lot of Jane's hidden/manual text-command logic lives.

If slash commands are the public front desk, this file is more like the weird side door staff keeps using anyway.

## What Lives Here

- `?janeRuntime`
- `!janeterminal`
- `!copyserver`
- `!shutdown`
- some BG-check utility text commands
- helper methods for hidden/runtime-only workflows

## Main Class

### `TextCommandRouter`

This is the actual router for the manual text commands.

It gets built once from [`bot.py`](../../bot.py) and then reused when `on_message` needs it.

Good methods to know:

- `firstLowerToken(...)`
  tiny helper that figures out which text command is being called

- `handleJaneRuntime(...)`
  builds the runtime/status embed

- `handleJaneTerminal(...)`
  builds the hidden read-only terminal view

- `handleCopyServer(...)`
  starts the whole copyserver flow

- `handleShutdown(...)`
  hidden lead-dev shutdown command

## Copyserver Notes

The copyserver code in this file is pretty dense now.

The big moving pieces are:

- preview + confirmation UI
- pinned snapshot / backup state
- pause / retry / auto-retry
- progress message editing
- final allowlist + dev-role handling

If you are trying to understand `!copyserver`, start with:

- `CopyServerConfirmView`
- `handleCopyServer(...)`
- the state helpers in [`runtime/copyServerState.py`](../../runtime/copyServerState.py)
- the restore stack in [`features/operations/serverSafety/snapshotRestore.py`](../../features/operations/serverSafety/snapshotRestore.py)

## Things To Be Careful About

- This file mixes "small utility command" logic with one very large workflow (`!copyserver`).
  So it is easy to accidentally break a simple command while touching the big one.

- A lot of these commands are intentionally hidden or restricted.
  If you touch the allowlist logic, double-check who can still run the command afterward.

- Webhook-authored messages behave differently from normal bot-authored messages.
  Copyserver status edits are one place where that matters a lot.

## Good Small Edits Here

- improve one status line
- improve one denial message
- add one missing log/warning
- extract one ugly little repeated helper
- add docs for one handler you touched
