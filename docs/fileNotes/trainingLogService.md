# trainingLogService.py

[`features/staff/trainingLog/trainingLogService.py`](../../features/staff/trainingLog/trainingLogService.py) coordinates training-result capture, mirroring, summaries, and host stats.

It is created in [`bot.py`](../../bot.py) as `TrainingLogCoordinator` and is used from startup backfill, `on_message`, and the text-command path.

For the operational runbook, see [Training Log Mirror](../features/trainingLogMirror.md).

## Main Entry Points

- `handleSourceMessage(...)`
  Captures a new training result message from the configured source channel.

- `syncRecentMessages(...)`
  Backfills recent source-channel history for every training-enabled organization profile.

- `runManualMirrorBackfillOnce(...)`
  Manual wrapper used by `!mirrortraininghistory`.

- `refreshSummaryPanel(...)`
  Rebuilds the archive-channel summary panel.

- `handleTrainingStats(...)`
  Handles `?trainingstats` / `?hoststats`.

## Data Flow

The normal flow is:

1. source message arrives in a configured training-results channel
2. `parseSourceMessage(...)` extracts the event type, host, passes, and fails
3. `_upsertParsedLog(...)` writes `training_result_logs`
4. `_ensureMirrorMessage(...)` sends or edits an archive-channel mirror
5. `refreshSummaryPanel(...)` updates the summary panel when needed

Backfill uses the same capture path, but defers summary refresh until each org scan finishes.

Each captured row also gets a `training_result_export_state` row. That state caches the host Roblox identity and marks whether the row is ready for the future training-results sheet export.

## Message Parsing

The parser supports:

- `Orientation Results`
- `Grid Certification Training Session Completed`
- `Grid Certification Examination Session Completed`
- `Emergency Certification Training Session Completed`
- `Emergency Certification Examination Session Completed`
- `Turbine Certification Session Completed`
- `Solo Certification Session Completed`
- `Supervisor Certification Session Completed`

It reads both message content and embed content. Host lines, pass attendees, and fail attendees are normalized before storage.

Orientation messages are only accepted from Jane-like authors. Certification messages are accepted from the source channel after parsing.

## Organization Profiles

Most channel IDs are read through `runtime/orgProfiles.py`:

- `trainingResultsChannelId`
- `trainingArchiveChannelId`
- `trainingLogBackfillDays`
- `trainingLogStartupMirrorNewRows`
- `trainingSummaryWebhookName`
- `trainingMirrorWebhookName`

Feature gating uses `anro-training-logs`.

This distinction matters for housekeeping: changing ANRORS recruitment logging does not necessarily change training/orientation logging.

## Stored State

The main table is `training_result_logs`.

Summary-panel message state is persisted through the recruitment/settings service with keys derived from:

- `trainingLogSummaryMessageId`
- `trainingLogSummaryChannelId`

Each org gets its own derived key.

## Things To Be Careful About

- Do not change parser titles casually. The accepted strings are effectively a data contract with posted training result messages.
- Mirrored messages are matched by stored message IDs and by footer recovery. Keep the `Source message ID` footer stable unless you migrate old mirrors.
- Backfill can scan a lot of Discord history. Keep the cooldown and locks in mind.
- Restart backfill may mirror recent missing rows only after it builds the archive index. If the index read fails, it stores rows but avoids creating mirror messages.
- `?trainingstats` matches hosts by mention, ID, and normalized display labels. Name matching can be fuzzy, so avoid making it more aggressive without checking false positives.
