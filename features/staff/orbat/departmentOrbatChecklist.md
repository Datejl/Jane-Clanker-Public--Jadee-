# Department ORBAT Rollout Checklist

This tracks each Department ORBAT tab through the same implementation path:

- Config key added
- Columns mapped
- Row organization rules mapped
- Logging hooks wired
- Application approval tie-in wired
- Validation and edge cases added
- Smoke-tested

## RECRUITMENT (current focus)

Status: `Wired (smoke test pending)`

- [x] Config key added
- [x] Columns mapped
- [x] Row organization rules mapped
- [x] Logging hooks wired
- [x] Application approval tie-in wired (optional; set `seedOrbatOnApprove: true` in `configData/divisions.json`)
- [x] Validation and edge cases added
- [ ] Smoke-tested in live Discord flow after latest changes

Notes:

- Recruitment ORBAT updates already run from approved recruitment/recruitment-patrol logs.
- Orientation bonus reconciliation also syncs recruitment points.
- New optional application hook can auto-seed approved applicants into Recruitment ORBAT with zero points/patrols.
- ANRORS is currently handled through this recruitment path (not yet migrated to department layout engine).

## Department ORBAT Tabs

Status: `In Progress`

- [x] ANRD (layout + startup/weekly touchup + app-seed wired)
- [x] CE (layout + startup/weekly touchup + app-seed wired)
- [x] LOGISTICS / LO (layout + startup/weekly touchup + app-seed wired)
- [x] TQUAL (layout + startup/weekly touchup + app-seed wired)
- [x] NIRI (renamed from R&D; layout + app-seed wired with default rank `Researcher`)
- [x] ANLD (renamed from LORE; app key + layout mapped)
- [ ] PROPAGANDA (division present; layout mapping pending)
- [x] A&A (layout + startup/weekly touchup + app-seed wired)
- [x] MSB (layout + startup/weekly touchup + app-seed wired)

## Next Validation Pass

- [ ] Smoke-test approval seeding for ANRD, CE, LO, TQUAL, NIRI, ANLD in live Discord flow
- [ ] Smoke-test startup + weekly maintenance run for all mapped tabs
- [ ] Decide whether to migrate ANRORS from recruitment-specific path into department layout engine
