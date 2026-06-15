# Honor Guard

This doc is a coordination note for Jane's Honor Guard work.

It should not be read as a ranking of anyone's branch or a decision that one person's implementation should replace another person's work. The active `feat-honorguard` branches from jojoa and Datejl are the current workflow exploration. The private repo and public `main` branch are integration targets, and they may lag behind or contain partial manual merges.

The goal is to keep everyone coding toward the same HG flow without silently discarding useful branch work.

## Active Branches

- `jojoa/Jane-Clanker-Public`, branch `feat-honorguard`
  - event create flow
  - solo sentry and point award work
  - early event manage and finish direction
- `Datejl/Jane-Clanker-Public--Jadee-`, branch `feat-honorguard`
  - based on jojoa's branch
  - newer attendee edit dropdown work
  - early finish UI work for event point editing
- `aVeryTiredPotato/Jane-Clanker-Public`, branch `main`
  - public sync target
  - not guaranteed to contain the newest HG workflow
- private Jane repo
  - production integration target
  - keeps private config, credentials, and deployment details out of public

When these disagree, assume the active feature branches may contain newer HG workflow decisions. Check them before changing the public or private version of HG.

## Current Workflow Goal

The current HG direction is:

1. Create an event.
2. Let people clock in.
3. Let the host or allowed supervisors manage the event while it is active.
4. Finish the event.
5. Review and edit each person's quota points and event points before final submission.
6. Submit the result for review.
7. Bulk sync approved results to the sheet.

This is the main point that should guide future work. HG does not want every normal event attendee to become a separate manual log. Normal hosted events should create attendance records through the event flow.

## Event Manage Flow

The manage flow should let an allowed manager select attendees and then take clear actions.

Expected actions:

- remove an attendee
- change an attendee to co-host
- change an attendee to supervisor
- change a co-host or supervisor back to a normal attendee

The active branch work is already moving toward a dropdown/select based UI for this. That direction is worth preserving unless HG explicitly asks for a different interaction model.

Open permission detail:

- hosts can manage their own event
- supervisors should be able to manage if HG wants that operationally
- admins or HG command roles can manage as an override

## Finish Flow

The finish flow should not immediately write final points.

Expected behavior:

- Jane calculates the default quota points and event points from the event type, role, duration, and attendance.
- The finish view shows the calculated result.
- Staff can edit per-person quota points and event points before submitting.
- The submitted result goes through review before sheet sync.

This matters for late joins, early leaves, extra credit, and manual corrections.

## Exam Flow

Exams probably need a separate finish path.

The current idea is to treat exam grading more like Orientation grading:

- attendees may have pass/fail or grading outcomes
- hosts, co-hosts, and supervisors may earn points based on graded attendee counts
- NCO exam screen-assist points need to be handled cleanly

Do not force exams through the exact same finish UI as a normal training if that makes the grading awkward.

## Sheet Sync

The sheet should remain the live HG member tracker unless HG intentionally decides to move member state into Jane's database later.

Jane's database should store:

- event workflow state
- submission and review state
- attendance results
- sentry logs
- manual point awards
- sync/audit history

The sheet adapter should move toward bulk updates, similar to recruitment. Updating the sheet once per person is slower, noisier, and more likely to hit API pain during larger events.

## Public And Private Repo Rule

Public-safe HG work should stay public when possible.

When bringing HG work between the active branches, public `main`, and private Jane:

- compare the active feature branch behavior first
- preserve the staff-facing workflow when it reflects the current HG plan
- adapt internals for private config, secrets, deployment, and shared services
- do not overwrite jojoa or Datejl's current workflow direction without checking the branch and asking if needed
- if schemas disagree, write a small migration or adapter plan instead of silently choosing one

Private repo constraints still matter. Secrets, private channel IDs, credentials, and private-only modules should not leak into public.

## Where It Lives

- `cogs/staff/honorGuardCog.py`
- `cogs/staff/honorGuardViews.py`
- `features/staff/honorGuard/service.py`
- `features/staff/honorGuard/sheets.py`
- `features/staff/honorGuard/outputs.py`
- `features/staff/honorGuard/rendering.py`
- `features/staff/clockins/honorGuardAdapter.py`
- `db/sqlite.py`
- `config.py`

## Core Point Model

Honor Guard has:

- `quota points`
- `promotion points`

Promotion points are tracked as:

- `event points`
- `awarded points`

The system also cares about three broad groups:

- `enlisted`
  `Jr Guardsman`, `Guardsman`
- `nco`
  `Sr Guardsman`, `Patrol Sergeant`
- `officer`
  `Parade Officer+`

The important behavior split is:

- enlisted members get normal event credit by attending
- officers do not get points for just attending
- officers get points by hosting, co-hosting, supervising, grading, or other officer-specific work
- NCOs can earn points both ways

## Logging Paths

HG currently has three main paths.

### Solo Sentry

Solo sentry is individually logged by the member.

Expected behavior:

- one log per user per day
- 30 minutes required
- evidence attachments required
- manual review required
- earns 1 quota point
- earns 1 promotion event point

Do not fully automate acceptance for solo sentry. HG wants review here for fraud resistance.

### Event Clock-In

Trainings, orientations, lectures, inspections, tryouts, JGEs, NCO exams, and similar hosted events should use an event record plus attendance flow.

That means:

- the host creates the event record
- attendees join through a clock-in style flow
- event attendance records are generated from that flow
- staff should not have to submit individual manual logs for normal hosted events

The clock-in flow needs HG-specific handling for:

- host, co-host, and supervisor attribution
- event-type-specific point rules
- late-join and early-leave point adjustments
- per-person edits before final review

### Manual Point Awards

Manual point awards are for awarded points and exceptions.

Examples:

- dev work
- document writing
- special officer-awarded extra credit

These should go through an approval flow and then sync into the member sheet as awarded promotion points, not event attendance points.

## Point Rules

These are the working rules unless HG changes them again.

### Enlisted Attendance

- quota points: `1` per event
- exception: `gamenight` gives `0.5` quota points
- promotion event points come from attendance-based event logic

### Officer Hosting, Supervising, And Co-Hosting

Officers should not receive points for just attending.

They receive promotion points for:

- `gamenight` host: `1`
- `orientation` supervisor/manager from start to finish: `2`
- `training` or `lecture` host: `3`
- `Honor Guard-wide tryout` host: `6`
- `inspection` host or attend: `8`

For exams:

- `JGE`
  `0.75` per graded attendee, rounded up
- `NCO exam`
  `1.5` per graded attendee, rounded up
- `NCO exam` screen-assist co-host
  `2` points even without grading
- `NCO exam` screen-assist plus grading
  `2` plus personal graded-attendee points

Co-hosts and supervisors should receive points like attendees unless the specific event rule says otherwise.

### NCO Behavior

NCOs are the mixed case.

They can:

- receive attendance-style quota credit
- receive hosting, supervising, or co-hosting promotion credit

## Promotion And Status Rules

### Guardsman

- `15` promotion points
- passed `Junior Guardsman Exam`

### Senior Guardsman

- `50` promotion points
- passed `NCO exam`
- active status required unless `Retired` or `LoA`

### Special Status Notes

- some officers retire into `Senior Guardsman`
- those should not be blindly auto-demoted
- if someone has an excuse status or is new, their activity status should be `N/A`

## Quota Cycle Rules

Quota resets are bi-weekly.

Status logic:

- `>= 4` quota points at reset: `Active`
- `< 4` quota points at reset and no excuse status: `Inactive`
- `8` quota points before reset: may be marked `Active` early

Do not automate kicks just because someone has `0` quota points. Leave that as a manual staff action unless HG intentionally asks Jane to own it later.

## Database Meanings

These are the intended meanings for the current private-repo tables. If the active branch schema differs, reconcile the behavior before replacing either side.

### `hg_submissions`

Generic approval queue for things that need human review before sheet sync.

Examples:

- manual point awards
- solo sentry submissions
- event records awaiting review
- future manual exceptions

### `hg_submission_events`

Audit trail for submission state changes.

Examples:

- created
- approved
- rejected
- synced to sheet

This is not the same thing as hosted event attendance.

### `hg_point_awards`

Approved manual awarded-point records.

These are durable accounting rows for awarded points, not the live review queue itself.

### `hg_attendance_records`

Per-user attendance results from event clock-ins.

These should be generated from event flows, not typed in one-by-one for normal hosted events.

### `hg_sentry_logs`

Solo-sentry-only records.

This should stay separate from generic event attendance.

### `hg_event_records`

Hosted event records.

This is the main event-level object for:

- event type
- host
- attendee count
- archive sync
- schedule removal
- host-stat updates

### `hg_quota_cycles`

Cycle history and reset history.

This is useful for:

- recording reset windows
- recording who ran the reset
- capturing metadata about the cycle

It is not required to make clock-ins work in phase 1.

## Sheet Side Effects

When a hosted event is approved and finalized, Jane should:

1. sync the relevant member point deltas
2. append the event to the archive sheet
3. remove or mark the event from the schedule sheet
4. increment the host's event-host stats

That archive, schedule, and host-stat work is part of the real workflow.

## Current Priority

The most useful next implementation order is:

1. keep solo sentry and point award flows working
2. finish event create and attendee clock-in
3. finish event manage with attendee removal and role changes
4. finish per-person point editing before review submission
5. bulk sheet sync for approved results
6. build the exam-specific finish flow
7. add manual quota-cycle tooling
8. consider quota automation later

## Practical Rule

When branch code, docs, and private backend are not aligned:

- active HG feature branch workflow wins for staff-facing UX unless HG says otherwise
- private repo constraints win for secrets, production safety, and shared deployment behavior
- sheet state remains the live member tracker unless HG intentionally changes that
- schema differences need reconciliation, not silent replacement
