# BG Intelligence

This is Jane's standalone background-check report command.

## Command

- `/bg-intel`
  Runs a one-user Roblox background intelligence report.

  Staff can scan a Discord member, a Discord ID, or a Roblox username.

  If RoVer is missing or stale, staff can provide a manual Roblox username override.

  Jane always runs the full scan route. There is no reduced-check command option.

The command is staff-only. Jane allows BG-certified reviewers and server managers/admins to use it.

The public output posts in the channel where the command was used as Jane. Expand controls are locked to the reviewer who ran the scan, and they update the current message to show one public-safe expanded section at a time.

For Discord targets, Jane checks the main server from `config.serverId` first. If the user is present there, Jane uses that guild for the RoVer lookup, then continues the scan in the channel where `/bg-intel` was run.

## What It Checks

Jane currently looks at:

- RoVer link status
- Discord ID lookup status
- Roblox friend, follower, and following counts
- manual Roblox username override status
- exact flagged Roblox user IDs
- watchlist / known-banned Roblox user IDs
- Roblox account creation date / age
- configured flagged Roblox groups
- configured group and username keywords
- group quality context like member-count shape, large groups, verified groups, and rank spread
- configured inventory item / creator / item keyword flags
- current catalog Robux value for visible non-gamepass inventory assets
- current Robux value for visible owned gamepasses
- configured favorite game / favorite game keyword flags
- outfit scan availability and outfit count
- configured badge flags
- full public badge award timeline quality, up to the configured hard page cap
- public badge timeline graph, when awarded dates are available
- optional external safety-source records from TASE and Moco-co
- known Roblox clanning-group records from configured flags and external sources
- prior Jane BG queue and BG intelligence records for internal scoring/audit context
- whether inventory is private or hidden

Outfits are shown as context/completeness only. Jane does not score avatar aesthetics, because "vibes-based justice" is how goblins get promoted.

## The Score

The score is a review-priority score, not a guilty/not-guilty machine.

Jane starts from a small base score, adds points for configured risk signals, subtracts a little for clean scans, then reports:

- `Low Risk`
- `Mild Review`
- `Manual Review`
- `High Risk`
- `Escalate`

She also shows confidence separately. Low confidence usually means "Jane could not see enough data", not "this user is secretly evil".

Scored reports have a tiny floor, currently `5/100`, so Jane does not imply absolute zero risk just because every visible scan was clean.

Private inventory is treated as incomplete data. Jane gives it only a tiny score bump and lowers confidence, because a private inventory by itself is not proof of anything.

Some rules are hard minimums instead of normal math:

- `banned_user` forces at least `95/100`
- `watchlist` forces at least `88/100`
- `roblox_user` forces at least `82/100`
- `username` exact matches also force at least `82/100`

That means Jane will not do silly math like "known banned user, but the account is old, so probably fine." Bonk. Direct hits stay direct hits.

Direct account rules can also have custom severity. In `/bg-flag`, the optional severity field is treated as the minimum score for `watchlist`, `roblox_user`, and `username` rules. Blank severity keeps Jane's default. `banned_user` can be raised above `95/100`, but not lowered below it.

Jane can also return `Not scored` instead of a number:

- `Needs Identity Review` means Jane could not resolve a Roblox account and did not find Discord-side external safety records to score.
- `Insufficient Data` means too many major data sources failed and the result would be fake precision.

If Jane cannot resolve a Roblox account but does find Discord-side external safety records, she still returns a score. That score is based only on those external records and should be treated as an identity-review prompt, not a complete Roblox background result.

The embed also shows `Data Completeness`, which is the quick "what did Jane actually see?" section. This is separate from confidence so reviewers do not have to reverse-engineer missing API calls from the signal list.

External sources are treated as extra evidence, not gospel in a funny hat. TASE checks Discord-side safety records when Jane has a Discord user ID. Moco-co checks Roblox-side safety records when Jane has a Roblox user ID. If either API key is missing, Jane marks that source as skipped and continues the rest of the report.

Jane also does a little bit of "normalcy" scoring now:

- A broad, clean group spread can lower risk a little.
- Mostly base-rank group memberships can lower risk a little.
- Large or Roblox-verified groups can lower risk a little when there are no configured flags.
- Mostly tiny groups on a newer account can raise risk a tiny bit.
- A true multi-year awarded badge timeline can lower risk a little.
- A thin or burst-heavy awarded badge timeline can raise risk a little.
- Prior Jane queue approvals can lower risk a little.
- Prior queue rejections or prior high-risk Jane scans can raise risk.

These are intentionally small weights. They are context, not destiny. Tiny goblin math is still math.

## Calibration Notes

The public bot ecosystem does not publish much exact math, because apparently everyone likes keeping the wizard knobs hidden. Jane's numbers are calibrated from the public behavior we can actually see:

- SpiderEye advertises profile data, badges, groups, inventory context, external records, watchlists, and badge analytics.
- RoNexus public examples show a clean older account around `8/100`, and a hard-flagged external-record account around `82/100`.
- Aunto and Double Counter treat detected alt / banned-alt links as enforcement-level signals, not tiny hints.
- Bloxlink-style verification restrictions commonly use account age and group membership gates, so Jane treats truly brand-new Roblox accounts as much more review-worthy than merely young accounts.

Jane's matching behavior:

- Clean, established accounts should usually land in the low single digits or low teens.
- Brand-new accounts under one day old now land around `High Risk` even before other context.
- Accounts under a week old usually land around `Manual Review`.
- Exact flagged accounts start at `82/100`, matching the public "critical hard flag" shape.
- Watchlist and banned-user hits stay above that because those are stronger than fuzzy score math.
- TASE and Moco-co matches can add enough weight to push a scan into manual/high-risk review, but they do not auto-reject anyone by themselves.

The public overview is intentionally short: high-level account identity, score, confidence, aggregate scan summaries, inventory and gamepass value totals, and a badge timeline graph when Jane has awarded badge dates. Expanded sections stay public-safe and scoped to one section at a time. Signal explanations, Roblox connection details, clanning-source details, configured item flags, and badge-history completeness live in the expanded section instead of the overview.

Discord does not allow interactive buttons inside the embed body itself. Jane keeps the message clean with one section picker below the embed instead of a large grid of section buttons.

Jane caches expensive Roblox reads in memory so repeated scans do not re-walk the same heavy data immediately. Badge award-date lookups are intentionally paced to avoid Roblox 429s, and asset/gamepass value lookups run behind small concurrency caps. When inventory already returns owned gamepass IDs, Jane prices those IDs directly instead of making a separate gamepass inventory listing call.

The pages are:

- overview
- detection summary
- profile
- connections
- groups
- inventory
- gamepasses
- favorite games
- outfits
- badges
- clanning record

## Inventory Privacy DM

If `/bg-intel` sees a private or hidden inventory, Jane can DM the user and ask them to make it public.

There is no queue state attached to this command, so the rescan path is simple: staff runs `/bg-intel` again after the user fixes privacy.

If staff scanned a Roblox-only target without a Discord member, Jane cannot DM anyone about private inventory.

The older queue-specific inventory retry button may still exist while the old queue exists, but this command does not depend on it.

## Audit Trail

Every successful `/bg-intel` scan writes a lightweight audit row to `bg_intelligence_reports`.

This stores:

- who ran the scan
- who was scanned
- the routed review bucket
- the Roblox account Jane found
- score, band, confidence, and whether the scan was actually scored
- no-score outcome and hard minimum, when relevant
- signal JSON
- report JSON
- prior report context, when available

It does not store an approval/rejection. The future Google Sheet queue should own reviewer decisions.

## Config

The main toggles live in `config.py`:

- `bgRiskScoreBase`
- `bgRiskScoreFloor`
- `bgIntelligenceFetchGroupsEnabled`
- `bgIntelligenceFetchConnectionsEnabled`
- `bgIntelligenceFetchInventoryEnabled`
- `bgIntelligenceFetchGamepassesEnabled`
- `bgIntelligenceFetchBadgesEnabled`
- `bgIntelligenceFetchFavoriteGamesEnabled`
- `bgIntelligenceFetchOutfitsEnabled`
- `bgIntelligenceFetchBadgeHistoryEnabled`
- `bgIntelligenceExternalSourcesEnabled`
- `bgIntelligenceTaseEnabled`
- `bgIntelligenceTaseApiBaseUrl`
- `bgIntelligenceTaseApiToken`
- `bgIntelligenceTaseTimeoutSec`
- `bgIntelligenceMocoEnabled`
- `bgIntelligenceMocoApiBaseUrl`
- `bgIntelligenceMocoApiKey`
- `bgIntelligenceMocoTimeoutSec`
- `bgIntelligenceFavoriteGameMax`
- `bgIntelligenceOutfitMax`
- `bgIntelligenceInventoryMaxPages`
- `bgIntelligenceInventoryHardMaxPages`
- `bgIntelligencePublicInventoryMaxPagesPerType`
- `bgIntelligenceGamepassMaxPages`
- `bgIntelligenceGamepassHardMaxPages`
- `bgIntelligenceBadgeHistoryPageSize`
- `bgIntelligenceBadgeHistoryMaxPages`
- `bgIntelligenceBadgeHistoryHardMaxPages`
- `bgIntelligencePrivateInventoryDmEnabled`
- `robloxApiCacheMaxEntries`
- `robloxProfileCacheTtlSec`
- `robloxGroupCacheTtlSec`
- `robloxConnectionCacheTtlSec`
- `robloxFavoriteGamesCacheTtlSec`
- `robloxOutfitCacheTtlSec`
- `robloxInventoryValueCacheTtlSec`
- `robloxGamepassCacheTtlSec`
- `robloxAssetPriceCacheTtlSec`
- `robloxGamepassProductCacheTtlSec`
- `robloxBadgeHistoryCacheTtlSec`
- `robloxBadgeAwardCacheTtlSec`
- `robloxBadgeAwardLookupConcurrency`
- `robloxBadgeAwardLookupDelaySec`

Secrets should stay in `.env`:

- `TASE_API_TOKEN`
- `MOCO_API_KEY`

If those are blank, Jane does not call that service. She will still run the normal Roblox checks.

Flag rules come from the same BG flag manager as the existing scan code:

- `/bg-flag`

Supported rule types are still:

- `group`
- `username`
- `roblox_user`
- `watchlist`
- `banned_user`
- `keyword`
- `group_keyword`
- `item_keyword`
- `item`
- `creator`
- `badge`
- `game`
- `game_keyword`

The optional severity field mostly matters for direct-user rules. For example, a watchlist item can be severity `45` for "manual review please" or `90` for "this is practically an escalation." Non-direct rules may store severity for future use, but Jane does not currently score group/item/badge rules from that field.

## Safe Edit Notes

- Do not make the score auto-reject users. Keep it as triage.
- Do not merge this command into the old queue unless the queue rewrite explicitly wants that.
- Keep the scoring explanations readable. If staff cannot tell why Jane gave a score, the score is not useful.
- Be careful adding new Roblox API calls. This command is interactive, so slow scans feel bad fast.
- Do not score avatar aesthetics. Outfits are useful for availability/context, not suspicion by themselves.
- Treat public badge history as complete only when Roblox stops returning a next-page cursor. Score awarded dates, not badge creation dates, when making timeline claims.
- Keep prior-record scoring gentle. A previous scan can be stale or based on old rules.
