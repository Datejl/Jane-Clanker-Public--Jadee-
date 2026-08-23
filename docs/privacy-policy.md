# Jane Clanker | Privacy Policy

_Last updated: 2026-08-23_

Jane Clanker ("Jane", "the bot", "we") is a Discord bot operated privately for the ANRO community. This policy describes what data Jane receives from Discord, why she needs it, how it is stored, and how a user can request removal.

Jane is not a commercial product. She is not sold, licensed, or offered as a service to other communities. She is invited to a small number of Discord servers operated by ANRO staff and used only for the internal operation of that community.

## 1. Who runs Jane

Jane is operated by ANRO staff. Questions about this policy or data requests can be sent to the operator by opening a ticket in the ANRO Discord server.

## 2. What data Jane receives

Because Jane is a Discord bot, everything she sees is delivered to her by the Discord API. Specifically, Jane requests the following Privileged Gateway Intents:

- **Server Members Intent**: receive member join, leave, and update events for the servers she is in.
- **Message Content Intent**: read the text, attachments, and embeds of messages in channels she can see.

She does not request the Presence Intent.

Concretely, Jane may receive and process:

- Discord user IDs, usernames, display names, and avatars of members in servers where Jane is present.
- Server (guild) IDs, channel IDs, role IDs, and role assignments for those servers.
- Message content, message IDs, attachment metadata, and embed contents from channels Jane can read.
- Voice channel presence (join/leave/move) in channels Jane monitors, for session and clock-in features.
- Roblox account identifiers voluntarily submitted by members through Jane's `/verify` flow.
- Command inputs and interaction payloads sent by users when they invoke Jane's slash commands.

Jane does **not** collect or store: email addresses, IP addresses, phone numbers, payment information, government identifiers, or Discord passwords. Jane does not receive that data from Discord in the first place.

## 3. Why Jane needs each intent

### Server Members Intent

- Send onboarding / welcome messages when a new member joins.
- Apply, remove, or refresh member roles as part of the verification (Jane Identity) flow.
- Update internal rosters (ORBAT, honor guard, division rosters) when members join, leave, or change roles.
- Clean up records when a member leaves the server.
- Populate staff-facing member lookup features (background flags, notes, reminders) with current usernames and roles.

Without the Server Members Intent, Jane cannot receive `GUILD_MEMBER_ADD`, `GUILD_MEMBER_REMOVE`, or `GUILD_MEMBER_UPDATE` events and these workflows break.

### Message Content Intent

- Legacy text-command prefix handlers (`!` and `?` commands) that predate slash commands.
- The training-log mirror, which parses training-result messages posted by another authorized bot and mirrors normalized entries into an archive channel.
- The "best-of" (starboard-style) feature, which needs message text to build the mirrored embed when a message crosses the configured reaction threshold.
- Auto-moderation and message-routing helpers that inspect message text for known spam or unsafe link patterns.
- Suggestion, poll, and reminder features that read the message body the user typed.

Without the Message Content Intent, Discord delivers empty `content` fields on most messages and these features stop working.

## 4. How Jane stores data

- Jane stores state in a local SQLite database (`bot.db`) on the machine that runs the bot. That machine is operated by ANRO staff and is not shared with third parties.
- Some feature state (roster snapshots, configuration) is also stored in local JSON files alongside the bot.
- Logs of bot activity are written to local files for debugging and are rotated on the host.
- Jane does **not** send member data to any third-party analytics, advertising, or AI-training service.
- Jane does **not** use member data to train machine-learning or AI models.

Roblox verification uses the Roblox public API to confirm account ownership when a member runs `/verify`. Only the returned Roblox user ID and username are stored, and only for members who chose to verify.

## 5. Message content handling

- Message content is used to power the features listed in section 3.
- Full message bodies are only persisted when a feature explicitly needs them: the training-log mirror, the best-of archive, and moderation audit records. Everything else processes the message in memory and discards it once the handler is done.
- Message content is never sold, shared with third parties, or used to train ML/AI models.

## 6. User opt-out

A user can opt out of message-content-based features at any time:

- Open an ANRO Discord ticket to add the user to Jane's message-content opt-out list. Once added, Jane will discard message events from that user without processing them for content-based features.
- A user can also stop interacting with Jane by removing Jane's ability to see the channel, or by leaving the server.

A user can remove their stored Roblox link at any time by running `/unlink` in Discord.

A user can request full deletion of any records Jane holds about them by contacting the operator. We will remove records that are not required to be retained for community-safety purposes (for example, active moderation actions) within a reasonable time.

## 7. Data retention

- Verification links, roster entries, and configuration are kept for as long as the member is part of the community, and are removed on request or on member departure where feature logic allows.
- Message-content-derived records (training-log archive, best-of archive, moderation audit) are kept for as long as the community needs them for operational history. They can be deleted on request.
- Local backups of the database are rotated on the host.

## 8. Children

Jane is used inside communities that follow Discord's Terms of Service, including the minimum age requirement. Jane does not knowingly collect data from anyone under Discord's minimum age.

## 9. Changes to this policy

If this policy changes, the updated version will be committed to the Jane repository and the "Last updated" date at the top will be revised. Material changes will also be announced in the ANRO Discord server.

## 10. Contact

Discord: open a ticket in the ANRO Discord server. (https://discord.gg/666zF5bYfR)
