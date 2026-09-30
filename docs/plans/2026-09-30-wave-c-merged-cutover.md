# Wave C — merged candle+feature cutover (readers switch, legacy tables dropped)

**Date:** 2026-09-30
**Status:** **scope doc — awaiting operator approval** (per the wave protocol).
Operator said to tackle the cutover (DEC-059's remaining step); this doc fixes the
code map, slices, tests and decisions before any production-code edit.

**Constraints**
- DEC-059: one writer (the strategy host), merged KV `candle_features`; DEC-060:
  merged table is one *selectable* archive entry, never a merged lake table.
- Acceptance bar for every slice: low latency + high throughput + **less state
  growth** (the whole point: stop writing 3 copies of the same information).
- Rollout flags stay explicit; cutover is a deliberate flip, rollback = flip back.
- Production deploy remains unprovable from this host (FACT-019/020) — dev proof
  only; the production flip is a separate operator action with its own evidence.
- Wave B is complete and live-proven (`CHG-475`); the legacy path is still
  authoritative today (dual-write was only ever transient).

**Progress:** **W-C1 landed** (`CHG-476`: context reader switch — config +
sealed-only decode, 109/0/0 scoped, mutation-checked; full gate 19/19 —
`logs/soak/monday-gates-20260930-204958`) · **W-C2 landed** (`CHG-477`: writer
consolidation switch — `LEGACY_CANDLE_SINKS_ENABLED` + per-branch guard
filters, sink UIDs stay, config 82/0/0 + UID pins 4/0/0, mutation-checked) ·
**W-C3 landed** (`CHG-478`: merged-table preflight contract + inclusion rule,
144/0/0 unit scoped + 5/0/0 drill, mutation-checked) · W-C4..W-C5 pending.

**Cadence (revised 2026-09-30, operator):** scoped checks per slice; **one full
`make gate` at the cutover** (W-C5a certifying run) and one final full run after
the W-C7 drop. The interrupted W-C2 gate run
(`logs/soak/monday-gates-20260930-211832`) was aborted deliberately — not a
repository failure.

## Verify-first census (tree `a68e54ff`, re-verified 2026-09-30)

**Writers (legacy):**
| What | Where |
|---|---|
| `candle_live` KV upsert sink (1 s snapshot) + `candle_closed` sink | `SignalJob.java:287-309` via `MultiTimeframeSinks.sinkLive/sinkClosed` |
| `candle_closed` first-write-wins filter | `MultiTimeframeSinks.java:218-236` — **KeyedProcessFunction with `ValueState<Boolean>`** (durable, checkpointed) |
| `feature_values` sink (stored layer, off on dev) | `SignalJob.java:348-364` behind `FEATURE_LAYER_ENABLED` |
| Registry entries (existence-checked at boot; NOT owned) | `DdlBootstrap.java:628/636/652` (`OWNED_TABLES` at `:281-282` does not include them) |
| DDL corpus | `32_candle_live.sql`, `33_candle_closed.sql`, `34_feature_values.sql` |

**Writers (merged, live-proven CHG-475):** `SignalJob.java:366-386` (host side output → `candle-features-sink`), flag `MERGED_CANDLE_FEATURES_ENABLED`.

**Readers (legacy):**
| What | Where |
|---|---|
| Strategy context fetch (exact-PK lookup) | `FlussCandleFetcher.java:54-66` → `config.candleClosedTable()`; column indexes match DDL 33 (`:37`) |
| Context fetch factory | `ContextProvider.java:112-116` |
| W-B4 predicates ready to use | `MergedCandleRows.isSealed/isForming` |
| Live verification probe | `fluss-probes/CandleVerify.java:155` (looks up `candle_closed`) |
| Ops panels (metric task names) | `o2-provision.py:381-393, 618, 642-648, 800, 1093` (`candle_closed_sink:_Writer`, `candle_live_sink:_Writer`) |
| Docs | `01-runbooks.md:459-520` env table; dossiers/contracts; `day_run.py:73` comment |

**Preflight contracts:** `TableContractValidator.java:154/175/196` validate candle_live/candle_closed/feature_values at startup (`SignalJob.java:780-808`). **The merged table has no contract validation yet** — a cutover gap to close.

**Pins that follow the DDL corpus (self-guarding tests):** `schema_manifest.json` (28), `ddl_apply_smoke.py:102` `APPLIED_TABLES=28` + docs-audit copies, `AppliedTablesPinTest`, foundation truth line (`769/560/651`).

## End state (definition of cutover done)
1. The host writes **only** `candle_features` (forming + sealed); `candle_live`/`candle_closed`/`feature_values` receive nothing.
2. The context reader reads `candle_features` **sealed rows only**; forming rows read as "window not finished".
3. Preflight validates the merged table contract (fail-closed) and no longer requires the legacy tables.
4. DDL 32/33/34, registry entries, pins, probes and panels are retired.
5. On dev the three legacy tables are dropped; the 19-step gate certifies the code.

## Slices (each: failing-first tests, one CHG, one commit; full `make gate` per slice — approved D3)

Ordering reflects **D2 = drop immediately**: the live flip and drop come before the
code decommission commit, so the final commit lands with the tables already gone.

1. **W-C1 — context reader switch.** New config `CANDLE_CONTEXT_TABLE` (default `candle_closed`) + `CANDLE_CONTEXT_SEALED_ONLY` (default false). `FlussCandleFetcher` reads the configured table and, when sealed-only, treats a `sealed=false`/absent row as miss (W-B4 predicate; column prefix is identical by DDL-35 design). Tests: fetcher decodes merged rows; forming row → miss; flag-off = byte-identical legacy behavior.
2. **W-C2 — writer consolidation.** New config `LEGACY_CANDLE_SINKS_ENABLED` (default true; fail fast if false while merged is off). In `SignalJob`, when false, insert a drop-filter **before** the two legacy sinks — the operators (uid + `ValueState`) stay in the graph so savepoint/checkpoint restore stays compatible; rollback = flip back. Tests: config validation; graph UID dump unchanged; flag-off = today's wiring; flag-on = legacy sinks receive 0.
3. **W-C3 — preflight switch.** Add `validateMergedCandleTable` (17 columns, PK, bucket key, KV v2 per DDL 35); `SignalJob` validates the merged table whenever writer or reader uses it, and skips legacy validation when legacy sinks are off and the reader is off legacy. Tests: validator agreement test + startup paths.
4. **W-C4 — feature-layer retirement (D1 approved).** Retire `FEATURE_ROWS` side output + `feature-values-sink` + `FeatureValuesColumns` + `validateFeatureValuesTable` + `FEATURE_LAYER_ENABLED`/`FEATURE_TABLE` config and tests; features live only inside the merged row. Note: the deployed dev graph already runs with the flag off, so deleting the gated operator does not change the live graph shape. Public behavior: feature add/remove stays registry + pin only, unchanged.
5. **W-C6 — dev cutover flip + smoke.** Deploy with `MERGED_CANDLE_FEATURES_ENABLED=true`, `LEGACY_CANDLE_SINKS_ENABLED=false`, `CANDLE_CONTEXT_TABLE=candle_features`, `CANDLE_CONTEXT_SEALED_ONLY=true`; same fake-feed recipe as `CHG-475` but now verify: merged rows grow, legacy tables **flat**, context fetch hits (`compute.context.*` counters) served by sealed merged rows. Evidence + CHG. (Full gate.)
6. **W-C7 — legacy table drop (D2 approved: immediate).** Dev-only `Admin.dropTable` for `candle_live`/`candle_closed`/`feature_values` through a sanctioned probe, right after the flip smoke passes; record the drop + live catalog readback. Irreversible — the raw table remains the source of truth for dev reproduction.
7. **W-C5 — decommission commit.** Remove DDL 32/33/34, regenerate the manifest, delete the `candle_live`/`candle_closed`/`feature_values` registry entries (keep `candle_features`), flip code defaults to the merged end-state (merged writer on, legacy wiring deleted — accepting fail-closed restore for pre-cutover checkpoints on dev), and let the pin tests walk the updates (`APPLIED_TABLES`, docs-audit copies, foundation truth, dossier/contract text, DEC-060 examples → `candle_features`); migrate `CandleVerify` and the o2-provision panels to the merged sink/context metrics; refresh the runbook env table and the day-runner note. (Full gate — the certifying one.)

## Risks / rollback
- **Savepoint compatibility** — the closed-sink filter's `ValueState` and all UIDs are preserved by the W-C2 drop-filter approach; conditional wiring is explicitly rejected (restore would fail closed).
- **Eager sink-table resolution (measured 2026-09-30, W-C3)** — `FlussSinkBuilder.build()` resolves its table at graph-build time, so the guarded legacy sinks still require `candle_live`/`candle_closed` to exist while they deploy (fine through W-C6). After the W-C7 drop, no fresh submit may happen before W-C5a deletes the legacy wiring — both happen back-to-back in the same session; the already-running job is unaffected until next submit.
- **Reader staleness window** — order matters: W-C1 must land before W-C2 is flipped (reader first, then writer stop); the doc's slice order enforces it.
- **Drops are irreversible** — W-C7 is separated by a soak window; until then rollback is a flag flip.
- **DDL hazard** — corpus edits stay reconciled proposals (no live apply implied); the live drops are the dev-testing step only.
- **Archiving** — `EOD_TABLES` currently unset; after cutover the only candle archive candidate is `candle_features` (opt-in stays manual).

## Decisions (approved by the operator, 2026-09-30)
- **D1 — retire `feature_values` fully in this wave.** (W-C4 deletes the stored-layer path and its DDL/config.)
- **D2 — drop the legacy dev tables immediately** after the flip smoke passes (no soak window). The slice order was re-arranged (flip → drop → decommission) so the irreversible step happens before the final code decommission commit; rollback before the drop is a flag flip.
- **D3 — full `make gate` after every slice** (seven certifying runs; no scoped shortcuts).
