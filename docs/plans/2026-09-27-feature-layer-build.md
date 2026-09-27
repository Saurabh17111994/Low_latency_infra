# Feature layer build — DEC-056 + DEC-057

- Status: in progress (slice 1).
- Decisions: DEC-056 (shared in-process registry, one stored row per window), DEC-057
  (storage encoding `MAP<INT,DOUBLE>`, append-only ids).
- Evidence: storage spike `logs/feature-spike-20260927/REPORT.md` (CHG-345); latency baseline
  p50 49 ms / p99 93 ms (`docs/plans/2026-09-26-signal-source-latency-tuning.md`).

## Architecture

| Piece | Where | Notes |
|---|---|---|
| Registry | `com.trading.compute.feature.FeatureRegistry` | Static list; each entry = one line: id, name, cadence (TICK/CLOSE), declared timeframes, computer factory. Ids are **append-only** — never renumbered, never reused (DEC-057). |
| Per-instrument state | `PerInstrumentFeatures` (one per host slot) | `double[] tickLatest` (TICK, tf-independent) + `double[feature][tf]` close values + one computer per (CLOSE feature, tf). A CLOSE value computed for one timeframe never leaks into another's row. All allocation happens at slot creation; per-update work is O(1) and allocation-free. |
| Compute point | Strategy host subtask (`StrategyHostFunction`) | Features update on the live tick (input 1) and closed candle (input 2) the host already receives. Every strategy reads the same `latest` values — no per-strategy recomputation. |
| Stored row | `feature_values` (proposal DDL 34) | PK `(instrument_token, tf, window_start)`, single `features MAP<INT,DOUBLE>` column. One row per non-empty window per tf, written on candle close. |
| Writer | Separate operator after the host output | Builds the row for the closing tf from `snapshot(tf)` and sinks it via `FlussSink` + `RowDataSerializationSchema` (the MAP path is pinned by `FeatureMapConversionTest`). Batching = one row per closed window per instrument; ~308 rows/s at 6 TFs. |
| Flags | `FEATURE_LAYER_ENABLED` (default OFF), `FEATURE_TABLE` (default `feature_values`) | Compose anchor + `.env` dev values + `rollout-savepoint.sh` forwarding, same pattern as the multi-TF flags (CHG-344). |
| Metrics | `compute.features.*` | Updates by cadence, snapshot rows emitted, per-tf row age. The `compute.latency.tick_to_strategy` histogram stays the KPI. |

## Slices

| Slice | Content | Verify |
|---|---|---|
| S1 (CHG-347) | Registry + SPI + `PerInstrumentFeatures` + first computers (`last_price`, `sma_close_20`, `rsi_close_14`); pure Java, no Flink | Unit tests: registry validation, tick/close routing, snapshot contents, SMA/RSI math, allocation-free update loop shape |
| S2 (CHG-348) | Host wiring: update features in `processElement1/2`, expose a read-only `FeatureView` to strategies, metrics | Host harness tests (existing `StrategyHostFunctionTest` pattern) + no behavior change for N7 (side-by-side) |
| S3a (CHG-349) | DDL 34 proposal + `FeatureValuesColumns` + DDL-agreement pin + host `FEATURE_ROWS` side output (off by default) | Offline: agreement tests + emission tests; `make ddl` validate clean |
| S3b (CHG-350) | Fluss sink wiring in `SignalJob` + startup table validator + flags (`FEATURE_LAYER_ENABLED`, `FEATURE_TABLE`) + rollout forwarding + dev table + smoke | Offline tests + dev smoke: rows appear with expected keys; re-close upserts the same PK |
| S4 (CHG-351) | Re-measure `tick→strategy` vs p50 49 / p99 93 ms; dossier + `03-non-functional.md` notes | Bounded profiling run with the feed on; evidence under `logs/` |

Progress: S1 landed (CHG-347); S2 landed (CHG-348 — features wired into `StrategyHostFunction`,
shared `FeatureView`, fail-open counters; compute suite 572 run / 0 failures / 18 env-gated skips);
S3a landed (CHG-349 — DDL proposal + column contract + host side output, no sink/cluster yet).

## How to add or remove a feature (the only procedure)

Append-only ids (DEC-057); the guards are runtime validation, a pin ledger, and this section.

| Action | Steps | Touches nothing else |
|---|---|---|
| **Add** | 1. Append one `FeatureDef` at the end of `FeatureRegistry.FEATURES` with the next id + `ACTIVE`. 2. Append its `<id>\t<name>` line to `code/02_services/02_compute/src/test/resources/feature-registry-pins.tsv`. 3. Run `FeatureRegistryPinTest` + `PerInstrumentFeaturesTest`. | No DDL, no sink, no strategy, no rollout flag. |
| **Remove** | Change that line's status to `RETIRED`; keep the id and name. | Old stored rows keep their meaning; new rows stop carrying it. |
| **Never** | Renumber, rename, reorder or reuse an id (old rows are keyed by id). A rename is a new feature + a retired old one. | `FeatureRegistryPinTest` fails with the fix in the message. |

Current facility: `FeatureRegistryDump` prints the ledger; `FeatureRegistryDump --full` adds
status/cadence/timeframes. The registry Javadoc at
`code/02_services/02_compute/src/main/java/com/trading/compute/feature/FeatureRegistry.java`
is the canonical in-code copy of this table.

## Rules

- Adding a feature = one registry line (new id = current `SIZE`); removing one = mark the entry
  retired but keep the id reserved (old rows keep their key meaning).
- TICK features: one computer per instrument, copied into every declared tf's row.
  CLOSE features: one computer per (feature, tf).
- No per-update allocation (no boxing, no streams) inside `PerInstrumentFeatures`.
- The stored layer is fail-open: a write failure must never block or fail the signal path
  (the writer operator isolates and counts, same pattern as strategy isolation).

## Risks

| Risk | Guard |
|---|---|
| Registry edit renumbers ids silently | Static validation (ids 0..N-1 in order, unique names) + a test that pins the current id/name map |
| Feature state grows with instrument slices | Per-instrument state is bounded by registry size; footprint checked in S2 against the 13–20 MB/subtask estimate |
| Writer backpressure reaches the host | Separate operator with its own sink client; host output is a plain forward |
| Dev flag not forwarded by rollout | `JOB_ENV_NAMES` pin test (same as CHG-344) |
