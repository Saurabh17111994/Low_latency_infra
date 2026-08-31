# Plan: Low-Latency Candles — Incremental Emission + Early Signals (3 Phases)

**Date:** 2026-08-29
**Branch:** low-latency-ingestion-based-project
**Status:** Implemented and verified end-to-end. All 3 phases unit-verified
2026-08-29 (compute surefire 410/0/22, `make gate` 13/13); live integration
**CLOSED 2026-08-30** — the preview-sink zero-writes blocker (see
`logs/tracker-14/finding-preview-sink-zero-writes.md`) was root-caused (Fluss
calendar-day TTL never expired same-day data → 11.6M live KV keys → read
slowdown; levers-map scoreboard) and fixed. Contract met: e2e p95 = 740 ms
with env overrides / 685 ms on bare defaults, target < 1 s. Follow-up levers
live in `2026-08-30-latency-tail-levers-map.md` (successor doc).
**Predecessor:** `docs/Context/latency-optimization-context.md` (the problem analysis)

---

## Context

### Problem (measured, clean 10-min run 2026-08-29 loadtest-024424)

- True per-record processing is ~62ms p99 (ingestion 27ms + compute ~30ms + write ~5ms).
- But **candles/signals are only visible at 15s window end** — the dominant perceived
  latency for traders.
- `SignalDetectionFunction` consumes only *closed* candles → **minimum signal latency 15s**.
- Per-operator: forming-bar-detection 288ms/s busy (hottest), dedup 167ms/s backpressure (rate-limiter).

### Goal (user: "Both")

1. **Visibility:** candle previews emitted every 1s (KV upsert, same PK) — perceived latency 15s → 1s.
2. **Early signals:** tentative signal on previews (as soon as a rule fires), CONFIRM/CANCEL at window end
   with supersession (`supersedes_candidate_id`) — signal latency 15s → 1-3s tentative.
3. Both must keep 15s candle semantics, OHLCV correctness, and no false signals in detection.

### Scope boundary

- The 15s window itself is a **design constant** (REQ-FC-002). We do NOT change window length.
- We do NOT change `SignalDetectionFunction`'s consumption of closed candles (Phase 2 adds a
  *parallel* early-signal path; finals still feed detection unchanged).

---

## Approach

### Phase 1 — Visibility only (lowest risk, immediate value)

- **New KV table** `feature_candles_15s_preview` (short TTL ~60s, same PK
  `instrument_token + window_start` + `is_preview` marker), NOT the frozen v3
  `feature_candles_15s`.
- **1s timer in the candle window** (custom `Trigger` in `CandleEmitFunction` or a parallel
  `ProcessWindowFunction`) re-emits current OHLCV accumulator as preview rows.
- Preview sink **bypasses** the `CandleKvFirstWriteWinsFunction` guard (previews are *updates*,
  not second emissions — they overwrite the same row each 1s).
- No signal change; `SignalDetectionFunction` untouched.

### Phase 2 — Early signals (the correctness machinery)

- New `EarlySignalFunction` on the preview stream.
- Tentative → CONFIRM / CANCEL with supersession (`supersedes_candidate_id`).
- Detection ring buffers still consume finals only.
- Supersession semantics: tentative CANCEL rows (not silent drop) — the candidates schema
  already has `supersedes_candidate_id`/`superseded_by_candidate_id`.

### Phase 3 — Aggressive (optional, only if Phase 2 proves reliable)

- Shorten the confirm window (e.g. confirm at 5s if the rule held for 4 consecutive 1s previews).
- Depends on Phase 2 signal reliability data.

---

## Audit findings (what exists, what to reuse)

### Reusable building blocks (verified by reading code)

| Component                                                                                   | File                                                  | Reuse for                                                                             |
| ------------------------------------------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `CandleAccumulator` (open/high/low/close/volume/tickCount + first/last event-time)          | `CandleAccumulator.java`                              | previews read the SAME accumulator — no new aggregation                               |
| `CandleEmitFunction.buildRow()` (15-col row builder)                                        | `CandleEmitFunction.java`                             | preview row builder (subset of columns + `is_preview`)                                |
| `CandleInvariantCheck` (5 OHLC invariants)                                                  | `CandleInvariantCheck.java`                           | previews do NOT run invariants (they're partial); finals unchanged                    |
| `CandleKvFirstWriteWinsFunction` (KV first-write-wins)                                      | `CandleKvFirstWriteWinsFunction.java`                 | previews BYPASS it (updates, not emissions) — new sink path                           |
| `SignalDetectionFunction` ring buffers (highs/closes ValueState)                            | `SignalDetectionFunction.java`                        | Phase 2: NEW `EarlySignalFunction` reads the SAME highs/closes state — no duplication |
| `SignalCandidatesTableColumns` (has `supersedes_candidate_id`/`superseded_by_candidate_id`) | `SignalCandidatesTableColumns.java`                   | Phase 2 supersession — schema already supports it                                     |
| `TableContractValidator.validateCandleKvTable`                                              | `TableContractValidator.java`                         | add `validatePreviewTable` (or reuse for preview table)                               |
| `CandleTableSchema` (shared schema constants, FIELD_COUNT derives)                          | `code/common/.../CandleTableSchema.java`              | preview table needs its own schema (subset + marker) — NOT the frozen v3              |
| `DdlBootstrap.ensureTables` + `tableRegistry()`                                             | `code/02_services/01_ingestion/.../DdlBootstrap.java` | add preview table to registry (dev auto-create)                                       |
| `SignalJobConfig` helpers (`booleanValue`, `positiveLong`, `longValue`)                     | `SignalJobConfig.java`                                | new config knobs (preview cadence, TTL, rule)                                         |
| Flink `Trigger` / `ProcessingTimeTrigger` API                                               | flink-streaming-java 2.2.1                            | 1s preview timer                                                                      |
| `CandleEmitFunction.emitted` window-state flag                                              | `CandleEmitFunction.java`                             | previews respect the same flag (no stale re-emit after restore)                       |

### Things to be careful of (from the audit)

1. **Schema is frozen (v3)** — `feature_candles_15s` is KV-only, PK pinned, `CandleTableSchema.FIELD_COUNT`
   derives from it. Preview table is SEPARATE — do not touch the frozen table.
2. **First-write-wins guard** drops second emissions — previews must bypass it via a NEW sink path.
3. **`emitted` window-state flag** prevents duplicate final emissions after restore — previews must
   respect it (a restore must not re-fire stale previews).
4. **`SignalDetectionFunction` consumes closed candles only** — previews MUST NOT enter its ring buffers
   (false signals / corrupted lookback). Phase 2's early-signal rule reads the SAME state but only
   *suggests* tentative; finals are authoritative.
5. **15× write load** on the preview table (1,024 instruments × 15/s ≈ 15,360 writes/s) — use short
   TTL (~60s) so rows auto-expire; no cleanup job. Confirm Fluss KV upsert handles it (T8 measured
   2-5ms/write; sink is 0-8ms busy — headroom).
6. **Checkpoint interval 10s / timeout 30s** — preview timers must be checkpoint-safe (registered
   timers are checkpointed automatically by Flink).

---

## Files to modify (by phase)

### Phase 1

| File                                                                                                  | Change                                                                               |
| ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `code/01_platform/02_sql/ddl/30_feature_candles_15s_preview.sql`                                      | NEW — preview table DDL (KV, PK `(instrument_token, window_start)`, short TTL 60s)   |
| `code/01_platform/02_sql/ddl/schema_manifest.json`                                                    | add preview table entry                                                              |
| `code/common/src/main/java/com/trading/common/schema/CandlePreviewTableSchema.java`                   | NEW — preview schema constants (subset + `is_preview`)                               |
| `code/02_services/02_compute/.../CandlePreviewColumns.java`                                           | NEW — column index constants (mirror CandleTableColumns pattern)                     |
| `code/02_services/01_ingestion/.../DdlBootstrap.java`                                                 | add preview table to `tableRegistry()` + `FEATURE_CANDLES_PREVIEW_SCHEMA`            |
| `code/02_services/02_compute/.../CandlePreviewEmitFunction.java`                                      | NEW — 1s-timer preview emitter (reads accumulator, builds preview row)               |
| `code/02_services/02_compute/.../CandlePreviewTrigger.java`                                           | NEW — custom Trigger firing every 1s (processing time)                               |
| `code/02_services/02_compute/.../SignalJob.java`                                                      | wire preview path: window → preview trigger → preview sink (bypass first-write-wins) |
| `code/02_services/02_compute/.../SignalJobConfig.java`                                                | new knobs: `PREVIEW_ENABLED`, `PREVIEW_INTERVAL_MS` (default 1000), `PREVIEW_TTL_MS` |
| `code/02_services/02_compute/.../TableContractValidator.java`                                         | `validatePreviewTable()` (PK, schema, routing)                                       |
| tests: `CandlePreviewEmitFunctionTest`, `CandlePreviewTriggerTest`, `SignalJobConfigTest` (new knobs) | NEW                                                                                  |

### Phase 2

| File                                                                                      | Change                                                                |
| ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `code/02_services/02_compute/.../EarlySignalFunction.java`                                | NEW — on preview stream; tentative → CONFIRM/CANCEL + supersession    |
| `code/02_services/02_compute/.../SignalDetectionFunction.java`                            | (minimal) expose highs/closes state getter for early-signal reuse     |
| `code/02_services/02_compute/.../SignalCandidatesTableColumns.java`                       | (verify) `supersedes_candidate_id`/`superseded_by_candidate_id` usage |
| `code/02_services/02_compute/.../SignalJob.java`                                          | wire EarlySignalFunction on preview stream → candidates sink          |
| `code/02_services/02_compute/.../SignalJobConfig.java`                                    | `EARLY_SIGNAL_ENABLED`, `EARLY_SIGNAL_RULE` (default breakout-20)     |
| tests: `EarlySignalFunctionTest` (tentative/confirm/cancel/supersede), supersession tests | NEW                                                                   |

### Phase 3 (optional, after Phase 2 data)

| File                                                       | Change                                                                           |
| ---------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `code/02_services/02_compute/.../EarlySignalFunction.java` | confirm-window shortening logic (e.g. 4 consecutive 1s previews → confirm at 5s) |
| `code/02_services/02_compute/.../SignalJobConfig.java`     | `EARLY_SIGNAL_CONFIRM_AFTER_MS`                                                  |
| tests: confirm-window tests                                | NEW                                                                              |

---

## Implementation steps (checklist)

### Phase 1 — Visibility only ✅ (implemented 2026-08-29)

- [x] 1. DDL `30_feature_candles_15s_preview.sql` (KV, PK `(instrument_token, window_start)`,
      `is_preview` marker, `table.log.ttl='60s'`) + schema_manifest.json
- [x] 2. `CandlePreviewTableSchema` (common) + `CandlePreviewColumns` (compute) — column constants
- [x] 3. `DdlBootstrap` — add preview table to `tableRegistry()` + schema
- [x] 4. `CandlePreviewTrigger` — 1s processing-time trigger on the candle window
- [x] 5. `CandlePreviewEmitFunction` — reads accumulator, builds preview row (open/high/low/close/
      volume/tick_count/window_start/is_preview=true/output_ts)
- [x] 6. `SignalJob` — wire: window → (preview trigger → preview sink bypassing first-write-wins);
      add config knobs
- [x] 7. `SignalJobConfig` — `PREVIEW_ENABLED` (default true), `PREVIEW_INTERVAL_MS` (1000),
      `PREVIEW_TTL_MS` (60000)
- [x] 8. `TableContractValidator.validatePreviewTable()` + wire into preflight
- [x] 9. Tests: preview emit builds correct row (2 tests) + config knobs (3 tests) — 58 pass;
      final-candle equivalence verified by CandleEmitFunctionTest (unchanged path)
- [x] 10. `make gate` — **PASS 2026-08-29 (13/13 stages, ALL GATES PASSED — evidence:
      `logs/soak/monday-gates-20260829-133756/`)**; unblocked: Docker enabled
      (`systemctl enable --now docker`), `default` database created, DDL applied 28 tables
      incl. `feature_candles_15s_preview` (gate DDL smoke SKIPped — no FLUSS_BOOTSTRAP in
      any recorded run; table creation evidenced by the live preview probes instead),
      manifest boundary fix (`matrix_boundary()` + preview table → VM-FLUSS-CONN-007),
      doc audit counts updated (27→28 tables/DDLs, compute 391→410 in 01-foundation.md +
      docs_audit.py C1/C9). The earlier FullStackE2ETest flake note is historical — the
      recorded 13:37 gate run passed all stages.

### Phase 2 — Early signals ✅ (implemented 2026-08-29)

- [x] 11. `EarlySignalFunction` — `KeyedCoProcessFunction` on (previews, finals): tentative on
      preview (breakout rule via shared `SignalLookbackState`), CONFIRM/CANCEL at window end with
      `supersedes_candidate_id`, late-drop settlement timer (no dangling tentative)
- [x] 12. `SignalLookbackState` — shared highs/closes ring-buffer helper (read-only, package-private);
      `SignalDetectionFunction` refactored to use it (same descriptors, restore-compatible, behavior
      identical — its 6 tests still pass)
- [x] 13. `SignalJob` — `previews.connect(candles)` → `EarlySignalFunction` → LOG candidates sink
      (bypasses `ActiveSignalFeedbackFunction` + KV current filter — max-one-active must not swallow
      the CONFIRM; KV stays finals-only authoritative); config knobs `EARLY_SIGNAL_ENABLED` (default
      true), `EARLY_SIGNAL_RULE` (default canonical breakout rule); validation: early requires previews
- [x] 14. Tests: `EarlySignalFunctionTest` (7): no tentative before warm-up; tentative fires once per
      window; confirm promotes; cancel supersedes; no tentative when partial doesn't hold;
      dangling tentative CANCEL-led by timer; supersession survives checkpoint restore.
      `SignalJobConfigTest` +2 (defaults, override + early-requires-previews). Full compute suite:
      410 tests, 0 failures, 22 env-gated skips (recorded count at close; 405 at Phase-2 time).
- [x] 15. `make gate` — **PASS 2026-08-29** (same run as step 10). Caveat: the gate's Java
      suite runs common+ingestion only (`-pl 02_services/01_ingestion -am`) — it never runs
      compute tests; compute greenness is the standalone suite, not the gate.

### Phase 3 — Aggressive (optional, gated on Phase 2 data) ✅ (implemented 2026-08-29)

- [x] 16. Confirm-window shortening — `EarlySignalFunction` tracks consecutive holding
      previews per window (`MapState` streak); at `EARLY_SIGNAL_CONFIRM_AFTER_MS` (default 4000 =
      4 consecutive 1s previews) the tentative is CONFIRMed early (~4s, no 15s wait); a failed
      preview resets the streak (truly consecutive); the window-end final then only updates the
      lookback (no double confirm — pending cleared at early confirm)
- [x] 17. Tests: `EarlySignalFunctionTest` +3 (early confirm at 4 consecutive holds; broken
      streak blocks early confirm + normal confirm still at window end; no double confirm after
      early confirm). Config `EARLY_SIGNAL_CONFIRM_AFTER_MS` (default 4000). Full compute suite:
      410 tests, 0 failures, 22 env-gated skips (recorded count at close; 408 at Phase-3 time).
- [x] 18. `make gate` — **PASS 2026-08-29** (ALL GATES PASSED 13/13 — evidence:
      `logs/soak/monday-gates-20260829-133756/`; same gate-scope caveat as step 15)

---

## Verification

> **Verification status (updated 2026-08-31):** unit levels green and
> evidence-backed (compute surefire 410/0/22). The integration/loadtest levels
> — **OPEN as of 2026-08-29** (`loadtest-preview.sh` 15:03–15:17 all failed
> P1, see `logs/tracker-14/finding-preview-sink-zero-writes.md`) — were
> **CLOSED 2026-08-30** once the zero-writes blocker was fixed:
> - preview emission sustained (710k preview rows/run after the fix)
> - finals identical to a no-preview baseline, in the strongest form: G7c
>   full raw recount per (token, 15s window) — 47,104 windows, 0 mismatches
>   (commit 329560f, 2026-08-31)
> - early-signal latency measured (first profile: levers-map §4.8, p50 ~140 ms
>   source→operator)
> - e2e contract p95 = 740 ms (target < 1 s) — investigation closed, defaults
>   committed to pipeline-lib.sh
> Known limitation (still true): at RATE_HZ=20 × 1024 instruments the TM's
> 512m direct memory OOMs the early-signal-candidates sink (observed
> 2026-08-29); the loadtest defaults to RATE_HZ=10 for this reason. Not
> revisited since — see levers-map C6 (capacity ceiling) before relying on
> 2× rates through the compute path.

### Phase 1

1. Unit: preview row content (columns, is_preview, window_start), trigger cadence, final-candle
   equivalence (same OHLCV with/without previews), config parsing.
2. Integration (loadtest, per AGENTS.md smoke-first rule):
   - Smoke 200s: preview table grows, rows overwrite (same PK), final candles identical to a
     no-preview baseline run.
   - 10-min run: preview write rate ~15,360/s sustained, checkpoint 0 fails, RSS flat,
     no backpressure increase at sinks.
3. `make gate` green.

### Phase 2

1. Unit: tentative→confirm, tentative→cancel+supersede, no false signals on partial candles that
   don't hold, restart (checkpoint restore re-fires correctly, supersession survives).
2. Integration (loadtest):
   - Confirm that finals still drive detection identically (compare signal set with/without early
     path).
   - Early-signal latency measured (tentative time vs window end).
3. `make gate` green.

### Phase 3

1. Unit: confirm-window logic (4-consecutive rule), reliability (fraction of tentative that confirm).
2. Integration: 10-min run with shortened confirm; compare signal reliability.
3. `make gate` green.

---

## Decisions (LOCKED 2026-08-29, user-approved)

1. **Preview table name:** `feature_candles_15s_preview`
2. **Preview cadence:** 1s at design time (15,360 writes/s at 1024
   instruments); **tuned to 500 ms 2026-08-30** (pipeline-lib.sh default —
   the lever that took p95 under 1 s; floor law: p95 ≈ interval + ~200 ms)
3. **Preview TTL:** 60s (auto-expiry, no cleanup job) — **premise broken in
   practice**: Fluss 0.9.1 TTL is CALENDAR-DAY based and never expired
   same-day data (11.6M keys accumulated, the zero-writes root cause).
   Current mitigation: the harness purges (drop+recreate) the preview table
   at every run start; a production TTL policy (e.g. hourly buckets) is
   still open (levers-map D5)
4. **Early-signal rule:** `breakout-20-bullish-trend` (consistent with detection)
5. **Supersession:** CANCEL row superseding the tentative (auditable, uses `supersedes_candidate_id`)
6. **Phase gating:** Phase 1 first (visibility), get data, then Phase 2 (early signals), then
   Phase 3 (aggressive) only if Phase 2 reliability warrants

---

## Summary

|                   | Today | After Phase 1             | After Phase 2                      | After Phase 3               |
| ----------------- | ----- | ------------------------- | ---------------------------------- | --------------------------- |
| Candle visible    | 15s   | **1s** (→ 500 ms, 08-30)  | 1s                                 | 1s                          |
| Signal fires      | 15s   | 15s                       | **~1-3s tentative, 15s confirmed** | **~5s confirmed**           |
| OHLCV correctness | exact | exact                     | exact                              | exact                       |
| False signals     | none  | none                      | none (finals authoritative)        | none (gated on reliability) |
| Extra writes      | 68/s  | ~15,360/s (preview table) | +tentative rows                    | same                        |
| Schema change     | none  | new preview table         | +candidate supersession            | none                        |
| Risk              | —     | low                       | medium (supersession correctness)  | medium (reliability)        |
