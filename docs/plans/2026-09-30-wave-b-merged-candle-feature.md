# Wave B — merged candle+feature table (staged, flags off)

**Date:** 2026-09-30
**Status:** operator-accepted design (DEC-059, 2026-09-30); implementing in
staged slices, **rollout flags default off**, cutover/decommission NOT in scope.
Constrained by **DEC-060**: the merged live table is one selectable archive
entry; it never forces a merged lake table.

**Progress:** **W-B1 landed** (`CHG-468`: DDL 35 + manifest 28 + contract test +
audit pins) · **W-B2 landed** (`CHG-469`: `MergedCandleRow` + the terminal-seal
rule + `MERGED_CANDLE_FEATURES_ENABLED=false`) · **W-B3 landed** (`CHG-470`: the
host's dual-write — forming row per live cadence + sealed row at close, KV
upsert sink, `MERGED_CANDLE_TABLE`, all behind the flag; compute 650/0/18) ·
**W-B4 landed** (`CHG-472`: `isSealed`/`isForming` reader predicates) ·
**W-B5 pending (live smoke) — now unblocked**: the dev scratch table exists
(`CHG-474`, the CHG-350 probe route; live-proof create + upsert, ZK catalog
29 tables) and the certifying gate passed 19/19 on tree `9b6a8f30`
(`logs/soak/monday-gates-20260930-193447`, certificate
`logs/tracker-14/gate-certificate-20260930.md`). Remaining: the ~200 s
dual-write smoke; until it runs the merged path is code- and test-complete but
**live-unproven**.

**Decision note (2026-09-30):** the final `make gate` ran **before** W-B5 (the
smoke needs the live table creation, an operator-visible step; the flag-off gate
does not depend on it) — **PASS, 19/19**. W-B5 follows on the certified tree.

## Goal (DEC-059)

Collapse `candle_live` + `candle_closed` + `feature_values` into **one KV
table** keyed `(instrument_token, tf, window_start)` carrying the candle
columns + `features MAP<INT,DOUBLE>` (DEC-057 encoding) + `sealed BOOLEAN`:

- **One writer** — the strategy host (Fluss 1.0 upserts are full-row; no
  partial update). Forming-row upserts on the live cadence; the final **sealed**
  write at close with the close-cadence features.
- **Sealed rows are never rewritten** (late ticks are dropped after seal).
- **Readers**: finished rows via `sealed = true`; the now-view reads the forming
  row.
- **Retention**: 3 d (already the era's local window) + lake opt-in per DEC-060.
- Feature add/remove stays one registry line + one pin line (RETIRED to remove;
  ids never reused) — inside the merged row, no DDL churn.

Why: the duplicate candle copy + separate feature store was the measured
inefficiency (smoke #3, 200 s: `candle_live` 357 MB + `candle_closed` 76 MB +
`feature_values` 73 MB) and every consumer already joined on the same key.

## Verify-first map (W-B0 — recorded at slice start)

| Piece | Where (to pin at slice start) |
|---|---|
| Candle forming/closed emission | `MultiTimeframeAggregateFunction` (live/closed side outputs, `LIVE_TAG`/`CLOSED_TAG`) |
| Candle sinks | Signal job sink wiring (`candle_live`, `candle_closed`) |
| Feature computation + stored layer | strategy host + feature side output (`FEATURE_ROWS`) behind `FEATURE_LAYER_ENABLED` |
| Feature registry | `FeatureRegistry.FEATURES` + `feature-registry-pins.tsv` (append-only, DEC-057) |
| Reader paths | `FlussCandleFetcher`/context provider; dashboards/probes read `candle_closed` |

## Slices (test-first; one CHG per commit; all runtime paths behind a flag)

1. **W-B1 — merged table DDL.** New DDL `35_candle_features.sql`: KV, PK
   `(instrument_token, tf, window_start)`, candle columns (mirroring
   `candle_closed`'s column contract) + `features MAP<INT,DOUBLE>` +
   `sealed BOOLEAN`, 3 d TTL, bucket key `instrument_token`, lake options
   present but `enabled=false` (DEC-060 opt-in). Manifest regen; column-
   agreement test; contract/dossier text.
2. **W-B2 — row assembly (pure).** `MERGED_CANDLE_FEATURES_ENABLED` (default
   false) + a row builder: forming row (candle snapshot + latest features +
   `sealed=false`), sealed row (final candle + close-cadence features +
   `sealed=true`), late-tick drop after seal. Unit tests, no cluster.
3. **W-B3 — writer wiring (flag off by default).** Dual-write from the host
   path: forming upsert on the live cadence, sealed write at close; the
   existing sinks stay untouched while the flag is off. Tests: cadence, seal
   idempotence (never rewritten), flag-off inertness. **No reader cutover.**
4. **W-B4 — readers + docs.** Sealed-filter helper for finished-row reads;
   dossier/contract updates; DEC-060 note (one selectable archive entry).
5. **W-B5 — live smoke + closure.** 200 s dual-write smoke on dev (smoke-first
   rule), evidence, CHG; **cutover (readers switch, old tables read-only then
   dropped) is a separate, later operator decision** with its own evidence.

## Verification

- Per slice: failing-first tests → green; `make static-check`.
- W-B5 smoke: same pipeline path, 200 s, prove merged rows grow with
  `sealed=false` on the cadence and `sealed=true` at close; existing tables
  unchanged (dual-write).
- Final: one full `make gate` covering Wave A + R2 selection + Wave B (and the
  2026-09-30 hygiene fixes).

## Risks / rollback

- One-writer coupling (host path) — accepted in DEC-059; a fallback writer is
  explicitly out of scope.
- The merged table's lake story stays opt-in (DEC-060) — enabling it is a list
  entry, not a side effect of the merge.
- Rollback: the flag (default off) makes every runtime path inert; DDL/docs
  revert with their commits.

## Out of scope (explicit)

- Live cutover and dropping `candle_live`/`candle_closed`/`feature_values`.
- Reader migration in dashboards/probes beyond the helper in W-B4.
- Production deploy (recorded FACT boundaries).
