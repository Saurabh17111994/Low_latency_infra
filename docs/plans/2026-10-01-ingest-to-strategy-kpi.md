# Platform-speed KPI: ingestion accept → strategy host read (`compute.latency.ingest_to_strategy`)

**Date:** 2026-10-01
**Status:** implemented — CHG-491 (commit `f3a574e9`, 2026-10-01): the strategy host emits `compute.latency.ingest_to_strategy`; real-broker smoke PASS (evidence `logs/chg-491/smoke-kpi.txt`, `logs/stage-profile-20261001-152327/`). **(Updated 2026-10-01: this line previously said "nothing implemented"; the work landed the same day.)**
**Approved idea (operator, 2026-10-01):** "start the stopwatch when the data arrives at our
front door, not at the broker's label" — keep the broker-anchored number as the business age,
add a second, ingestion-anchored number that shows the platform's true speed.

**Inputs (read-only, 2026-10-01):**
- Real run `logs/stage-profile-20261001-142318` — S1–S12 stage ladder + `tick_to_strategy`
  aggregated from TM Prom (290 106 samples): p50 528–563 ms, p99 1 650–1 751 ms; S2+S3+S4
  = 0+1+7 ms p50; S6 `ingest_to_monitor` 24/51 ms; Flink tracker (source→operator)
  19/255.5 ms. Smoke `tick_to_strategy` p50 460–474; fake-feed baseline 46/83/95 ms.
- `CandleAccumulator.java:51-63` — `lastIngestTs` observability seam already exists and is
  maintained per accepted close-setting tick (`CandleAggregateFunction.java:96-102`; tests
  `CandleAggregateFunctionTest.lastIngestTs*` already pin it).
- Dossier `docs/08_implementation/04-signal-job.md` §"Latency KPIs — which number means what"
  (L454-468); probe `code/01_platform/04_scripts/latency_probe.py` (CHG-357).

## 1. Why

The only strategy-path KPI (`compute.latency.tick_to_strategy`) is anchored at the broker's
event time, which the token feed stamps at **whole-second resolution** (`token_slot.go:245-251`,
`LTTmsFromSeconds` ×1000 — no sub-second bits exist). On the real feed the value reads
~0.5 s p50 / ~1.7 s p99 before any of our code runs; the platform's own hops are
single-digit-to-tens of ms (`ingest_to_monitor` 24/51; tracker 19/255).

Consequence: there is no metric answering **"how fast are we, from the moment we accept a
tick to the moment a strategy can act on it?"** This plan adds exactly that metric. It is
measurement-only: no write path, no storage, no DDL.

## 2. The two numbers (end state)

| Metric | Start | Stop | Answers | Owner |
|---|---|---|---|---|
| `compute.latency.tick_to_strategy` (keep, unchanged) | broker label (whole seconds) | host read | signal age incl. feed clock | business |
| `compute.latency.ingest_to_strategy` (new) | `raw.ingest_ts` (Java accept, ms) | host read | platform speed | us / ops |

Both stop at the same point (host read of the canonical `FIFTEEN_S` forming row, one update
per tick), so they are comparable side by side. Neither is a sum of the other's parts.

## 3. Design

1. **Probe field on the forming row.** The live row is in-memory + network only — its only
   consumer is the strategy host (`SignalJob.java:307/336`; no Fluss sink since W-C5a).
   Append one trailing `BIGINT ingest_ts` (index 15) to `CandleLiveColumns`; the first 15
   columns stay pinned to the merged-table prefix (`35_candle_features.sql`, guarded by
   `CandleLiveColumnsAgreementTest`). The DDL file is untouched; nothing is stored.
2. **Value source.** `CandleAccumulator.lastIngestTs` (already tracked, sentinel
   `Long.MIN_VALUE` = unknown, never fabricated). `buildLiveRow`
   (`MultiTimeframeAggregateFunction.java:290-310`) writes it into the row.
3. **Host histogram.** New `compute.latency.ingest_to_strategy` at the existing KPI site
   (`StrategyHostFunction.java:228-232`, update block L296-305): canonical `FIFTEEN_S` row
   only, one update per tick, guard `probe > 0 && now >= probe` — same shape as
   `IngestLatencyMonitorFunction.java:39-44`. `tick_to_strategy` stays byte-identical.
4. **Probe script.** Third entry in `latency_probe.py` `METRICS` (same `strategy-host ->`
   vertex selector) so the number is captured in every run's evidence.
5. **Wording fix (measurement honesty).** `stage_profiler.py` S7/S9 source strings and the
   profiler README call the tracker "event time → feature row"; it is Flink's built-in
   latency **marker** (source → operator, processing time). Correct the words only.

Considered and rejected:
- a separate `CandleLiveTickColumns` layout class — duplicates 15 types/names for one probe
  field;
- computing the metric in the aggregator — misses the aggregator→host hop, which is exactly
  the tail worth measuring;
- carrying the Go SDK receive time (`goReceivedMs`) instead of `ingest_ts` — needs
  proto+row changes for only ~1–3 ms more coverage; available later if ever needed.

## 4. Change map

| # | File | Change | Guard |
|---|---|---|---|
| 1 | `signaljob/CandleLiveColumns.java` | add `INGEST_TS=15`, `FIELD_COUNT=16`, `DDL_FIELD_COUNT=15`; append to `TYPE_ROOTS`/`COLUMN_NULLABLE_IN_DDL`/`NAMES`/`ROW_TYPE_INFO`/`checkIndex`; Javadoc: first 15 = merged-DDL pin, index 15 = in-memory probe | itself (static drift guard) |
| 2 | test `CandleLiveColumnsAgreementTest.java` | pin loops to `DDL_FIELD_COUNT` (first 15 vs `35_candle_features.sql`); new assertions: probe name/type/not-null at index 15, index 15 absent from the DDL (`COLUMN_NAMES.get(15) != cols.get(15).name`) | failing-first |
| 3 | `signaljob/MultiTimeframeAggregateFunction.java` (`buildLiveRow`) | set probe from `acc.lastIngestTs` (slow/mirror path too) | `MultiTimeframeAggregateFunctionTest` (fast + mirror; `TestRawRows.withIngestTs`) |
| 4 | `signaljob/StrategyHostFunction.java` | new histogram + guarded canonical-tick update | `StrategyHostFunctionTest` (recording MetricGroup proxy, pattern in `RawValidationFunctionMetricsTest`) |
| 5 | `04_scripts/latency_probe.py` + `tests/test_latency_probe.py` | third metric entry + coverage | probe tests |
| 6 | `docs/08_implementation/04-signal-job.md` (L454-468) | third KPI row; correct tracker wording | `make docs-audit` |
| 7 | `06_stage_profiler/stage_profiler.py` (L1405/L1444) + `README.md` (L72-74) | tracker label wording | `test_stage_profiler.py` |
| 8 | `docs/05_deployment/change-records/CHG-491.md` | record + evidence | — |

Not touched: `35_candle_features.sql` or any DDL, any Fluss table/sink, `SignalJob` wiring,
operator state / `CandleAccumulator`, `tick_to_strategy`, existing metric names.

## 5. Semantics + expected acceptance

- **Start:** `raw.ingest_ts` = the ingestion service's accept wall-clock (same host clock
  domain as the TMs today; a multi-VM deploy needs NTP or the guard skips skewed samples).
- **Stop:** host read of the canonical `FIFTEEN_S` forming row (same stop as `tick_to_strategy`).
- **Covers:** admission/decode/fingerprint → Fluss append+ack → Flink source read →
  raw-validation → dedup → aggregation → host read. **Does not cover:** broker → our socket
  (unmeasurable from our side) or the strategy fan-out work (the Flink tracker and sink
  metrics cover those legs separately).
- **Cadence:** one sample per tick, canonical row only (not six). With
  `MULTITF_FAST_LIVE_FEED=false` (kill switch) the same probe rides the mirror rows; the
  value is the age of the newest close-setting tick at snapshot emission.
- **Expected (to be confirmed by smoke, not assumed):** p50 in the **35–50 ms** band on both
  fake and real feed — feed-independent by construction; p99 follows the in-job/checkpoint
  tail (calm ≈ 80–100 ms; checkpoint spikes are the known CT-2 territory). That tail becomes
  the number to optimize from then on; if the real feed's p99 is worse, it is now
  attributable instead of hidden behind the feed clock.

## 6. Verification (smoke-first, per repo protocol)

1. Failing-first per slice: agreement test fails before the layout change; live-row carry
   test fails before `buildLiveRow`; host guard test fails before the histogram.
2. Compute module suite module-locally: `cd code/02_services/02_compute && mvn -o test`
   (dossier L1005; compute is outside the root reactor — `make test` does not cover it).
3. Probe unit tests + `make static-check` + `make docs-audit`.
4. **Smoke (required before any long run):** fake-feed profiler smoke (~200–300 s) with a
   `latency_probe.py` capture — the new metric present on all 8 subtasks, p50 in band, and
   no change in `ingest_to_monitor` / `tick_to_strategy`.
5. Real-feed smoke + main inside market hours (if the current window has closed, next
   session; `ALLOW_OFFHOURS_REAL=1` is wiring-only and is not evidence). Capture probe TSVs,
   run-meta, and the stage report.
6. CHG-491 with all evidence; dossier updated in the same commit.

## 7. Risks

- The layout-pin edit is a contract change; the agreement test is the guard and the DDL file
  is not edited. Live rows are never persisted, so there is no storage/schema impact.
- Upgrade: one extra field on a generic side-output row, no operator-state change
  (savepoint-compatible; both producers and the host ship in one image).
- Multi-VM clock skew: guarded skip + NTP prerequisite recorded in the metric doc.
- `acc.lastIngestTs` on cross-TF late drops: the canonical `FIFTEEN_S` sample is the accepted
  tick's own accept time; non-canonical rows may carry the last accepted tick's value — fine
  for a probe, noted in the Javadoc.

## 8. Non-goals

- Changing the broker anchoring of `tick_to_strategy` (it stays the business number).
- Adding the KPI to the stage-profiler report (the probe carries it; optional follow-up).
- Any write-path, storage, or DDL work.
