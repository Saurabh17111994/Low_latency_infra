# Segment Build Contract — Compute

## Boundary

The Signal Flink job consumes `raw_table_1`, performs bounded fingerprint deduplication, builds multi-timeframe candles (15 s / 30 s / 1 m / 3 m / 5 m / 15 m), and passes live (forming) and closed candle events directly to the strategy host in the same job. **(Rewritten 2026-09-27: the 15 s single-timeframe candle and forming-bar path is RETIRED — 2026-09-05 cutover, batch 3; the current path is `candle_features` → strategy host — see `08_implementation/04-signal-job.md`. Wave C W-C5a 2026-09-30 retired the two old tables, DEC-059.)**

**Tier-scoped deployment (current testing phase):** the current phase builds and validates the Signal job on the approved 1,024-instrument / single-connection envelope (20,480 ticks/s at 20 Hz per instrument). The 3,000-instrument / 50,000 ticks/s variable baseline remains the deferred production target (`PERF-PROD-60000-001`; `PERF-PROD-90000-001` retired with the peak campaign, DEC-036); per-instrument windowing, dedup, and candle logic are identical across envelopes — only the load/acceptance profile differs.

## State (DEC-038 — authoritative → Fluss, transient/recovery → Flink)

The Signal job's state splits into three categories:

**Authoritative durable state — Fluss:** candles (`candle_features` KV, PK `(instrument_token, tf, window_start)`, one row per window: terminal `sealed=true` row at close only — closed-only storage since CHG-486 (2026-10-01), the forming state stays in Flink memory, 3 d + lake opt-in per DEC-060) and current signal state (`Signal_Candidates_current` KV). The fingerprint-dedup set is NOT Fluss state: per DEC-054 it is an operator-local per-token count window (`DEDUP_WINDOW_ENTRIES` = 200, ~10 s horizon), intentionally not checkpointed. Durable state survives a Flink restart independently of a large Flink checkpoint. **(Rewritten 2026-09-27: the `feature_candles_15s` single-TF KV candle table is RETIRED — 2026-09-05 cutover.)**

**Transient execution state — Flink working state:** the multi-timeframe per-instrument candle accumulators (one per timeframe), the strategy-host per-instrument heap state (strategy rings/setups — intentional amnesia, a restore rebuilds from replayed candles), and the operator-local dedup count window, so the hot path performs no Fluss round trip per tick. **(Updated 2026-09-27: the 15 s window accumulator, the `candle-emitted` flag, the forming-bar stack, and the Fluss dedup working cache are RETIRED — 2026-09-05 cutover / DEC-054.)**

**Recovery state — Flink checkpoint:** source offsets, watermarks, event-time timers, and minimal execution metadata. The checkpoint is intentionally small and is not a second complete copy of Fluss-owned business state.

Production checkpoints/savepoints use encrypted S3. The exact state backend and connector versions are pinned and tested.

**Restart contract:** restore the compact Flink checkpoint → verify Fluss authoritative-state availability and compatibility → rehydrate only the working state actually needed → resume. Fluss unavailability/incompatibility fails the job closed (no silent replay); deterministic continuation and safe degradation are preserved.

## Event-time and finalization

The deployed watermark, allowed-lateness, and source-idleness values are configuration parameters, not universal protocol constants. The default profile is bounded out-of-orderness of 500 milliseconds (single-timeline rule 2026-08-30, measured), allowed lateness of five seconds, and source idleness of fifteen seconds. Each value may be changed only through a tested deployment profile. A source without a verified event timestamp cannot advance the watermark.

**(Multi-TF update 2026-09-27; Wave C W-C5a 2026-09-30: this finalization contract applies per timeframe to `candle_features` sealed rows; the forming row of an open window is not final.)** A candle is final from its first write: the final row emits at first window fire (watermark ≥ `window_end`), an `emitted` window-state flag makes any allowed-lateness re-trigger a no-op (late-within-lateness folds into the accumulator and is counted, never re-written), and no correction/update row exists in MVP. The "final after `window_end + allowed_lateness`" phrasing means the finalization boundary — the candle is not corrected after that point. The finalization contract SHALL separately name the watermark out-of-orderness bound, source-partition idleness threshold, allowed/finalization delay, source split identity, reconnect/reassignment behavior, and late-event classification. The term `allowed lateness` SHALL not imply correction/update rows.

## Deduplication horizon

The `dedup_horizon` is the maximum supported append retry, connector replay/rewind, checkpoint restore rewind, broker replay, and approved operational replay interval, plus a documented safety margin. `DEDUP_TTL_MS` SHALL be exactly `60000` (1 minute) in MVP; deployment SHALL reject any other value. (Changed 2026-08-28, CHG-116: broker docs rule out replay on reconnect — `sub` = live data only, reconnect = resubscribe + ack; the duplicate horizon is checkpoint restore rewind ≤10s; 60s = 6× margin.) The implementation SHALL report accepted event rate, dedup entries, serialized entry bytes, physical backend/checkpoint bytes, and restore duration at the variable 50,000 ticks/s average baseline. (The 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045.) **(SUPERSEDED 2026-09-03, DEC-054: the dedup bound is an operator-local per-token count window — `DEDUP_WINDOW_ENTRIES` = 200, ~10 s horizon — intentionally not checkpointed; `DEDUP_TTL_MS` is no longer read anywhere in the compute service.)**

## Typed handoff to Business Logic

Compute SHALL expose both typed live (forming) candle events and typed closed-candle events to the strategy host within the Signal job. Each event includes instrument, window boundaries (`tf`, `window_start`, `window_end`), source schema/configuration versions, deterministic ordering metadata, and event/processing timestamps. The strategy host SHALL not reconstruct these events by reading the candle table back. **(Updated 2026-09-27: `portfolio_id` removed with the ranking scope, CHG-005; the live event is the `candle_features` forming-row stream — the host reads the per-tick `LIVE_TICK_TAG` feed when `MULTITF_FAST_LIVE_FEED=true`, the default. Updated 2026-10-01 (CHG-484): the feed carries every timeframe's forming row per accepted trade tick, so strategies see all six evolving candles in memory; only the FIFTEEN_S forming row is stored per tick.)**

## Outputs

- `candle_features` rows (KV upsert, PK `(instrument_token, tf, window_start)`, one row per window: forming then sealed, 3 d + lake opt-in)
- Typed in-job live/closed candle events to the strategy host
- `Signal_Candidates` LOG + `Signal_Candidates_current` KV when the strategy host runs (`STRATEGY_HOST_ENABLED=true`)
- `Execution_Intent` LOG when `EXECUTION_INTENT_ENABLED=true`
- Invalid, duplicate, late, watermark, checkpoint, and backpressure metrics

~~`Trade_Decisions` are immutable instruction records published by the Signal job~~ — **REMOVED 2026-08-15 (CHG-005 — decisions out of scope, not deferred).**

## Guarantees

Exactly-once applies only to the pinned integration-tested Flink state/sink boundary. It does not imply cross-table atomic visibility or broker-call exactly-once.

**State-boundary acceptance:** (1) large durable Signal state is observable in Fluss; (2) the Flink checkpoint remains bounded and does not duplicate the full durable state; (3) restart restores compact Flink state and rehydrates from Fluss without full raw-history replay; (4) Fluss state unavailability or incompatibility causes safe degradation; (5) checkpoint failures remain safe.

## Acceptance

Out-of-order, duplicate, identical-legitimate-event, empty/invalid window, late discard, deterministic replay, checkpoint restore, backpressure, node/process restart, and workload-envelope tests pass.

## Requirement traceability

- Functional: `REQ-FC-001` through `REQ-FC-013`
- Cross-cutting: `03-non-functional.md` §§3.1–3.3, 3.5, 3.8; `04-data.md` §§4.1, 4.3–4.7; `05-interfaces.md` §§5.2–5.3, 5.11; `06-operational.md` §§6.2–6.5, 6.10

See `../02_requirements/02-functional/03-compute.md`.
