# R2 archive selection — one config list rules (per table, never one merged lake table)

**Date:** 2026-09-30
**Status:** **implemented 2026-09-30 (S1-S5; CHG-464..467)** — certified with
the next full gate. Operator requirement + approved design 2026-09-30 ("One
list rules it"); supersedes DEC-059's "one lake table" clause — recorded as
**DEC-060**. Test-first, one CHG per commit.

## Requirement (operator, 2026-09-30)

> "EOD: I don't want to save one lake table (candles + features together); I
> want a facility where I could choose what table I want to save and not any
> other, so that via config I would select those tables to save on Cloudflare
> R2 at EOD. My local SSD will have all data and tables — I am talking about
> Cloudflare R2, which is the object store."

## What exists today

- `EOD_TABLES` (env / `--tables`; the production deck already requires it) —
  the tables the EOD controller manages: it verifies each day's landing in R2
  and extends local retention while a day is unverified.
- `table.datalake.enabled` (per table; ~19 table definitions carry `true`) —
  which tables the Fluss tiering job copies to R2.
- The copy engine is the Flink tiering service; EOD verifies
  (`EOD_OFFLOAD=lake` only reads evidence).
- **Gap:** no single config says "archive exactly these tables and nothing
  else". Today's definitions say "archive almost everything", and DEC-059
  recorded "one lake table" for the merged design.

## Design (approved: "One list rules it")

1. **One list is the authority.** `EOD_TABLES` means *the R2 archive set* — the
   tables archived to R2, and nothing else. The EOD controller already
   consumes it, so the guard scope coincides with the archive set (correct:
   archived tables are the ones promised and verified).
2. **Sync step** (`r2-archive-sync`; dry-run default, `--apply` to execute):
   reads the list + live per-table flags; enables exactly the listed tables;
   disables any unlisted table that is on; reports legacy tables that refuse
   the ALTER (created before the cluster gained `datalake.format`) and exits
   non-zero so nothing is skipped silently. Prints the full diff.
3. **Fail-closed guard (gate preflight).** The preflight already reads the live
   catalog; it gains one rule: **no live table outside the list may be set to
   archive** (unreadable options or a mismatch → FAIL; an unset list uses the
   tool's documented default and says so in the log).
4. **Definitions default to OFF.** The ~19 table definitions stop asserting
   `table.datalake.enabled=true`; the sync step turns archiving on only from
   the list. A fresh stack archives nothing until the list says so.
5. **Local SSD unchanged.** This switch controls only the R2 copy; local
   retention stays per its own policy (raw 3 d, `candle_live` 60 s, etc.).
6. **Per table, never merged.** The merged live table (DEC-059 / Wave B), if
   ever built, is one selectable entry among others and never becomes a merged
   lake table; candle and feature data stay separately selectable.

## Slices (test-first; one CHG record per commit)

- **S1 — selection core + tests.** Pure logic: (list × live flags) →
  enable / disable / legacy-blocked; failing-first unit tests + a pinned
  dry-run output shape.
- **S2 — sync tool + tests.** Multi-table ALTER loop reusing `EnableTiering`'s
  proven single-table call; script wrapper (bash -n/shellcheck clean); a
  refusing test for the legacy case; runs on the dev stack (dry-run then
  apply where legal).
- **S3 — gate guard + tests.** Preflight rule + unit tests with a fake reader;
  live check on dev; fail-closed when options are unreadable.
- **S4 — definitions default-off + docs.** ~19 DDL edits + manifest regen +
  contract/dossier/runbook text (the lake ops runbook gains the selection
  section); update the tests that pinned the old default.
- **S5 — dev verification + change record.** Sync on dev (legacy tables report
  recreate-required — expected; nothing outside the list is ever enabled);
  CHG record; tracker updates.

## Verification

- Per slice: its failing-first tests → green; `make static-check`.
- Live: sync dry-run/apply on the dev stack (legacy tables → recreate-required
  is the honest result there); guard reads the live catalog.
- Final: full `make gate` covering Wave A + this change + the hygiene fixes.
- Scope boundary: a *fresh* table + the tiering service are needed for an
  enabled-table copy proof; the real-VM tiering proof stays out of scope
  (recorded FACTs). This plan's claim is the selection contract and the
  fail-closed enforcement, not that parquet lands on production R2.

## Closure (2026-09-30, CHG-464..467)

- **S1** `CHG-464`: selection core (`r2_archive_selection.py`) + 12 tests
  (failing-first).
- **S2** `CHG-465`: `EodControllerTool tiering` (`--list` / `--set on|off`) +
  `r2_archive_sync.py` (dry-run default, `--apply`); live-proven on dev —
  `candle_closed` enabled then disabled again (both ALTER directions with
  readback), `raw_table_1` refused (stale Iceberg table name — recreate the
  table or archive/move the stale objects aside first, never delete) with the
  sync reporting it and exiting 1.
- **S3** `CHG-466`: fail-closed preflight guard; live line
  `OK archive selection: 0 of 28 tables enabled, none outside the configured
  list (EOD_TABLES unset -> nothing may be enabled)`; scoped preflight PASS.
- **S4** `CHG-467`: 18 definitions ship `enabled=false` (opt-in), manifest
  regenerated, contract/runbook/dossier updated (the runbook gains the
  selection section), docs-audit all green (truth 769/559/638).
- **S5** (this closure): dev readback shows the as-found state (all disabled;
  `--tables candle_closed` dry-run: `enable candle_closed`); the ddl-apply
  image is rebuilt for the final gate.
- **Caveat (recorded):** the copy engine remains the Fluss tiering job — the
  real-VM parquet proof stays out of scope (recorded FACTs); this change is the
  selection contract and its enforcement.

## Risks / rollback

- Legacy tables cannot be enabled without a one-time recreate (reported, not
  silent).
- Disabling archiving stops future copies; existing R2 objects are never
  deleted by this tooling.
- Rollback: re-run the sync with the previous list, or set the flags back via
  the same ALTER; DDL/doc changes revert with their commits.

## Wave B note

Wave B (merged candle+feature live table) is **not implemented** and stays
staged behind its own plan doc. That plan must preserve this decision:
per-table archive choice, no merged lake table.
