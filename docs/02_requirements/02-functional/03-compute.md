# 02.3 — Compute

## Purpose

The Signal Flink job consumes `raw_table_1`, performs bounded best-effort deduplication, assigns event-time semantics, emits final MVP candles, and passes closed-candle plus forming-bar state to Business Logic within the same job. It does not read feature tables back from Fluss for signal generation.

**Tier-scoped deployment (current testing phase):** the current phase builds and validates Compute on the approved 1,024-instrument / single-connection envelope (20,480 ticks/s at 20 Hz per instrument). The 3,000-instrument / 50,000 ticks/s variable baseline remains the deferred production target; `PERF-PROD-60000-001` and the 3,000-instrument acceptance rows (AC-FC-007/011, NFR-PERF-002) are not part of this phase's acceptance. (`PERF-PROD-90000-001` and the 90,000 ticks/s peak are retired, DEC-036; the 60,000 ticks/s gate is current, DEC-045.) Windowing, dedup, and candle logic are envelope-independent — only the load/acceptance profile differs.

## Constraints

- Compute SHALL NOT read `feature_candles_15s` or any other feature table back from Fluss for signal generation — normal feature computation stays in-process within the Signal job. This prohibition does not cover the explicitly designated Fluss-authoritative state tables (dedup state table, `feature_candles_15s` KV, `Signal_Candidates_current` KV — DEC-038), which SHALL be read/written only for externalized state management, hydration, recovery, and authoritative state access; temporary Flink → Fluss → Flink round trips merely to compute a feature remain prohibited.
- Deduplication SHALL NOT use `seq_no` as a required key, ordering field, or completeness assertion. Fingerprint-based dedup is best-effort only.
- The deployed `DEDUP_TTL_MS` SHALL be exactly `60000` (1 minute). Deployment SHALL reject any other value.
- `CANDLE_WINDOW_MS` SHALL be exactly `15000` (15 seconds). Deployment SHALL reject any other value.
- `CHECKPOINT_INTERVAL_MS` SHALL be exactly `10000` (10 seconds). `CHECKPOINT_TIMEOUT_MS` SHALL be exactly `30000` (30 seconds). `MAX_CONCURRENT_CHECKPOINTS` SHALL be exactly `1`. Deployment SHALL reject any other values.
- `MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT` SHALL be exactly `1`. Do not forward another active candidate for that instrument. Structural, not a tunable: `Signal_Candidates_current` (DDL 23) is keyed by `instrument_token`, so supersession overwrites the single current row in place — the ranking-era `PlatformConfig` constant was removed 2026-09-14 (P6-665, after CHG-005 removed its consumer).
- A late event received after final window emission SHALL be discarded and measured in MVP. Correction rows, backfill, or update of a prior candle SHALL NOT be emitted.
- Empty windows and windows containing only invalid or late-discarded events SHALL produce no candle row.
- Candle finalization SHALL use the tested watermark + allowed-lateness boundary. The implementation SHALL NOT claim exactly-once correctness for cross-table visibility, broker calls, or independent Fluss sinks without a version-specific test.
- Production checkpoints SHALL use durable S3 storage. Checkpoint interval, timeout, concurrent count, state backend, and restart strategy SHALL be exact-version configuration, not undocumented defaults.
- Compute SHALL NOT add an unbounded custom queue. Backpressure is Flink-managed with source lag, watermark lag, operator busy/idle time, and sink latency exposed as metrics.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-CMP-001 | TCP preserves order within each Arrow WebSocket connection, and the `raw_table_1` append order is sufficient for deterministic event-time replay under an identical input snapshot. | ASM-001 |
| ASM-CMP-002 | The pinned Flink version's RocksDB (or equivalent managed state backend) and S3 checkpoint storage behave as specified under the workload envelope. | REQ-FC-008 |
| ASM-CMP-003 | The configured watermark out-of-orderness (default 5 s), allowed lateness (default 5 s), and source idleness (default 15 s) are sufficient for the tested broker stream profile. | REQ-FC-004 |
| ASM-CMP-004 | The Fluss connector provides per-partition source offset, watermark, and idleness semantics compatible with the event-time contract. | REQ-FC-004, RISK-008 |
| ASM-CMP-005 | Fingerprint collisions and identical-legitimate-event collapses remain within the measured and accepted rate under production workload. | RISK-001 |
| ASM-CMP-006 | The dedup state TTL covers the worst-case ingestion retry, connector replay/rewind, checkpoint restore rewind, broker replay, and approved operational replay interval plus documented safety margin. | REQ-FC-012 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Best-effort deduplication:** Fingerprint-based dedup may collapse identical legitimate events or miss semantically duplicate packets. The dedup state is bounded by TTL. Metrics distinguish accepted events, dedup hits, and estimated collision risk.
- **Final-on-emission candles:** A candle is final once the watermark passes `window_end + allowed_lateness`. No provisional candle is emitted. No correction or backfill candle is written for that window in MVP.
- **Late events are discarded:** Events arriving after finalization are counted and measured but do not produce a new or updated row. This limitation remains visible in metrics and documentation.
- **Empty windows produce no row:** A 15-second window with zero eligible trades emits no candle. Downstream consumers must not assume a row exists for every window.
- **Deterministic replay is input-bound:** Replay determinism is relative to an identical ordered input snapshot, fingerprint algorithm/version, and configuration version. Different arrival order, fingerprint collisions, missing external state, or changed configuration may produce different results.
- **Checkpoint restore is compact + rehydrated or safe-degraded (DEC-038):** Flink restores only its small checkpoint (source offsets, watermarks, window/lateness timers, in-flight accumulators, working-cache metadata); large durable Signal business state is verified against and rehydrated from Fluss. If Fluss state is unavailable or incompatible, the job enters a safe degraded state (or fails closed at startup). **(Ranking/reservation restore semantics REMOVED 2026-08-15, CHG-005 — out of scope, not deferred.)**

## Out of Scope

The following capabilities are explicitly NOT owned by Compute:

- **Market data ingestion, broker connection, and packet decoding:** Owned by Ingestion.
- **Logical deduplication stronger than bounded fingerprint:** The fingerprint dedup is best-effort. Exact deduplication would require broker sequence IDs, which are not available.
- **Candle correction, backfill, or update rows:** Deferred; not in MVP scope.
- **Feature columns beyond OHLCV candles (250+ features, market context, pattern-feature libraries):** Deferred; not in MVP scope.
- **CEP (Complex Event Processing):** MVP SHALL NOT use CEP. No `flink-cep` dependency, CEP operator, CEP job, CEP table, or `org.apache.flink.cep` import is permitted in MVP.
- **Signal detection, strategy evaluation, and candidate creation:** Owned by Business Logic within the same Signal job.
- ~~**Ranking, portfolio reservation, and instruction publication:**~~ — **REMOVED 2026-08-15 (CHG-005).**
- **Broker order submission, execution, and Arrow REST integration:** Owned by the Executor.
- **Postback capture, fill lifecycle, and position projection:** Owned by Action Capture.
- **Babysitter position monitoring and action emission:** Owned by the Babysitter Flink job.
- **Reading feature tables or strategy tables back from Fluss for computation:** All feature and strategy computation stays in-process within the Signal job. The designated Fluss-authoritative state tables (dedup KV, `feature_candles_15s` KV, `Signal_Candidates_current` KV — DEC-038) are read/written only for externalized state management, hydration, recovery, and authoritative state access — never to compute a feature that could stay in-process.

## REQ-FC-001: MVP scope

MVP computes 15-second event-time OHLCV candles and the forming-bar state required by Business Logic. Advanced feature columns, market-context features, and current-price Babysitter inputs are deferred until a separate phase with its own schema and tests.

The `feature_candles_15s` table name fixes the deployed MVP granularity at 15 seconds. A different granularity requires a new versioned table and migration; it is not a runtime-only toggle.

## REQ-FC-002: Source and event classification

Compute reads only accepted rows from `raw_table_1`. It SHALL:

1. Use **trades + quotes** for OHLCV candle state. Every accepted row updates the forming candle's open/high/low/close from `last_price_paise` — `raw_table_1` schema v2 (R-054/R-231) carries no bid/ask columns, so quote rows contribute via their last-price field; bid/ask-driven OHLC returns only with quote columns in a future schema v3. Volume and `tick_count` increment only on `tick_type = 'TRADE'` rows with `last_qty > 0`.
2. Key state by `instrument_token` and preserve connection/fingerprint metadata for diagnostics.
3. Exclude invalid rows (price ≤ 0, negative volume) from aggregation while preserving their raw audit and reason metrics.

## REQ-FC-003: Fingerprint deduplication

Compute SHALL use a bounded state keyed by the versioned `event_fingerprint` and its scope. It SHALL retain the first accepted event within the configured TTL and count later candidates as dedup hits.

The deduplication guarantee is best-effort. It may collapse identical legitimate events or fail to identify semantically duplicate packets. It SHALL NOT use `seq_no` as a required key or deterministic ordering field.

Dedup state TTL is **exactly 1 minute (60000 ms)**. The deployment SHALL reject any other value.

`CANDLE_WINDOW_MS` SHALL be exactly `15000` in MVP. Deployment SHALL reject any other value.

## REQ-FC-004: Event-time and watermarks

- Event time is UTC epoch milliseconds from the verified broker timestamp.
- Default watermark strategy is bounded out-of-orderness of 5 seconds, configurable only with a tested deployment profile.
- Default allowed lateness is 5 seconds after the watermark, configurable with a tested profile.
- Source idleness is 15 seconds by default and must be configured per source partition.
- A source without a valid timestamp cannot advance the event-time watermark.

A late event within allowed lateness may affect an in-memory window before emission. A late event after emission is discarded in MVP and counted; it does not create a correction row or update a prior candle.

## REQ-FC-005: Candle aggregation

For each instrument and 15-second event-time window containing at least one eligible accepted row (trades + quotes; volume/tick_count only from `TRADE` rows with `last_qty > 0`):

| Field                       | Rule                                                                                         |
| --------------------------- | -------------------------------------------------------------------------------------------- |
| `open`                      | Lowest event-time accepted row in the window; ties use deterministic fingerprint ordering     |
| `high`                      | Maximum accepted row price                                                                    |
| `low`                       | Minimum accepted row price                                                                    |
| `close`                     | Highest event-time accepted row in the window; ties use deterministic fingerprint ordering    |
| `volume`                    | Sum of accepted trade quantities with `last_qty > 0` (quote rows and zero-quantity trades contribute nothing) |
| `tick_count`                | Count of accepted `TRADE` rows with `last_qty > 0` after deduplication                        |
| `window_start`/`window_end` | UTC epoch-millisecond window boundaries                                                       |
| `output_ts`                 | Processing-time instant the final row was emitted (DDL column name; ack-path timestamp is not measured in MVP) |

The implementation SHALL define the exact tie ordering in the fingerprint specification. It SHALL NOT use incomparable per-connection sequence values.

Empty windows and windows containing only invalid/late-discarded events produce no row.

## REQ-FC-006: Final-on-emission policy

A candle is final from its first write: it emits at first window fire (watermark ≥ `window_end`), an `emitted` window-state flag makes any allowed-lateness re-trigger a no-op (late-within-lateness folds into the accumulator and is counted, never re-written), and the output is written through the tested Fluss sink boundary. `window_end + allowed_lateness` is the finalization/cleanup boundary — the candle is never corrected or updated after that point. Subsequent events for that window are discarded in MVP.

The discard metric SHALL include instrument, window, lateness, and reason. A future correction policy requires a new versioned table/contract and is outside MVP.

## REQ-FC-007: Forming-bar state handoff

Within the same Signal Flink job, Compute SHALL expose a typed in-job event to Business Logic whenever an eligible trade updates the current forming bar. The event includes instrument, window boundaries, current OHLCV accumulator, event timestamp, fingerprint, and source metadata.

Business Logic SHALL consume this state directly. It SHALL NOT wait for `feature_candles_15s`. **(The no-Fluss-round-trip ranking clause is REMOVED 2026-08-15, CHG-005.)**

## REQ-FC-008: Checkpoint boundary and state ownership (DEC-038)

**State ownership:** large durable Signal business state lives in Fluss; Flink retains only the small working state needed for active processing plus the minimal recovery state needed to restart. The current Signal path's large state is the fingerprint dedup set; candle output and signal current-state already live in Fluss KV. The Signal job SHALL use RocksDB or the version-pinned equivalent managed state and durable S3 checkpoint storage in production. Checkpoint interval, timeout, concurrent checkpoint count, state backend, and restart strategy SHALL be exact-version configuration, not undocumented defaults.

Exactly-once may be claimed only for the state/sink boundary proven by the version-specific integration suite. The requirement does not make independent table visibility or broker REST calls exactly-once.

**Checkpoint role:** the Flink checkpoint SHALL contain source progress, watermarks, timers, in-flight window accumulators, and minimal working/recovery context — it SHALL NOT be a second complete copy of Fluss-owned Signal business state.

**Restore:** the job SHALL restore its compact checkpoint, SHALL verify Fluss authoritative-state availability and compatibility, and SHALL rehydrate only the working state it needs before resuming. If Fluss state is unavailable, incompatible, or cannot be verified, the job SHALL fail closed / enter a safe degraded state that prevents new instructions. Restore SHALL NOT require a full raw-history replay merely to reconstruct durable hot state.

## REQ-FC-009: Backpressure

Compute SHALL use Flink-managed backpressure and expose source lag, watermark lag, operator busy/idle time, sink latency, checkpoint duration/size, pending records, and restart counts. It SHALL not add an unbounded custom queue.

Sustained backpressure that threatens the 100 ms decision SLO, checkpoint timeout, or safe state continuity SHALL alert and cause the order path to halt according to the Executor contract.

## REQ-FC-011: Finalization and source-partition contract

MVP SHALL use an explicit final-only candle contract. No provisional candle is emitted. A single final candle is emitted when the tested finalization condition is met. The requirement SHALL name separately:

- watermark out-of-orderness bound;
- source-partition idleness threshold;
- allowed/finalization delay;
- source split identity;
- reconnect and reassignment behavior;
- late-event classification.

The term `allowed lateness` SHALL not imply correction/update rows in MVP. Events received after finalization are discarded and measured.

## REQ-FC-012: Deduplication horizon and state budget

Deployment SHALL define `dedup_horizon` as the maximum supported append retry, connector replay/rewind, checkpoint restore rewind, broker replay, and approved operational replay interval, plus a documented safety margin. `DEDUP_TTL` shorter than this horizon or unbounded SHALL be rejected.

The implementation SHALL report accepted event rate, dedup entries, serialized entry bytes, physical backend/checkpoint bytes, cleanup progress, and restore duration. Acceptance SHALL include state-growth evidence at the variable 50,000 ticks/s average baseline. (The 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045.)

## REQ-FC-013: Typed closed-candle handoff

Compute SHALL expose both typed closed-candle events and forming-bar events to Business Logic within the Signal job. Each event includes instrument, `portfolio_id` or routing scope when applicable, window boundaries, source schema/configuration versions, deterministic ordering metadata, and event/processing timestamps. Business Logic SHALL not reconstruct these events by reading the candle table back.

## REQ-FC-010: Metrics and acceptance

Required metrics include source throughput, dedup hits, dedup state size, invalid events by reason, late events, candle throughput, watermark lag, forming-bar update rate, source/sink latency, checkpoint duration/size, restore count, state corruption/recovery events, source split/idleness state, finalization delay, and state-growth bytes.

Acceptance tests SHALL prove:

1. Event-time window assignment with out-of-order fixtures.
2. Fingerprint duplicate and identical-legitimate-event behavior.
3. Invalid/empty-window handling.
4. Final-only emission and late-data discard.
5. Deterministic tie ordering from an identical input snapshot.
6. Checkpoint restore without state duplication within the tested boundary.
7. Backpressure and checkpoint stability at 50,000 ticks per second (3,000 instruments; ≈16.7 ticks/s/instrument average).
8. Safe-halt when state continuity or checkpoint health becomes uncertain.
9. Source idleness, reconnect, reassignment, and watermark recovery.
10. Dedup TTL rejection and state-growth evidence.
