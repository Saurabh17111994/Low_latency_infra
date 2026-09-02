# Plan: Stage-by-Stage Latency & Throughput Detection Harness

- **Date**: 2026-09-01
- **Status**: PROPOSED — awaiting approval to execute
- **Goal parent**: throughput >15k ticks/s sustained, end-to-end latency <4 s
  (see docs/Investigations/2026-09-01-throughput-degradation.md)
- **Repo**: streaming_project_New

## Aim

Name the exact pipeline stage where throughput collapses and where time is
spent, with aligned per-stage measurements taken during (a) a fresh/healthy
window and (b) a degraded window. The stage whose in-rate diverges from its
out-rate — or whose latency/watermark-lag jumps — is the bottleneck, and
becomes the single target of the next fix.

## Design principles

1. **Measure, don't instrument**: use Flink's native per-operator metrics
   (numRecordsIn/Out, latency histograms, watermarks, busy/backpressure/idle)
   and Fluss's native counters. No per-record timestamps, no code changes in
   the data path — custom instrumentation would add overhead to the very
   path under test and could distort the result.
2. **One aligned timeline**: every source sampled at the same cadence (5 s),
   written to one run directory. Offline analysis compares stage-by-stage.
3. **Diagnostic artifacts are offline files (TSV/JSONL) by design** — they are
   never in the data path and cannot affect latency or throughput. Production
   data path contains no TSV/JSONL (proto/Arrow → Fluss native → Flink
   internal → Fluss tables); nothing to replace there.
4. **Fail-closed**: harness aborts if the job is not RUNNING or the feed is
   not at the expected rate, so captures are never silently invalid.
5. **Small, testable, reusable**: the harness is a script in
   code/01_platform/04_scripts/ with a unit-testable parser, reusing
   pipeline-lib.sh conventions (bash -n clean, py_compile clean).

## Stages

### Stage A — capture harness (build)

**Deliverable**: `code/01_platform/04_scripts/stage-capture.sh`
(+ parser module + unit tests).

One run directory per capture: `logs/tracker-14/stage-capture-<timestamp>/`
with:

| File | Content | Source |
|---|---|---|
| `stages.tsv` | per 5 s: epoch, phase, per-operator numRecordsIn/Out (SUM across subtasks), busy/backpressured/idle ms per subtask | JM REST `/jobs/{jid}/vertices/{vid}/subtasks/metrics` |
| `latency.tsv` | per 5 s: per-operator latency-histogram p50/p90/p99 (metrics.latency.interval=1s already enabled) | TM :9249 Prometheus |
| `watermark-lag.tsv` | per 5 s: per-source-subtask current watermark + max event-time seen → event-time lag per stage | JM REST + TM Prometheus |
| `ingestion.tsv` | per 10 s: tick.ththroughput, append ack latencies, pending records | ingestion JVM otlp payload log (existing) |
| `fluss-side.tsv` | per 10 s: raw_table_1 append rate (coordinator/tablet side) + feature_candles_15s / preview / candidates sink arrival rates | Fluss tablet metrics / logs |
| `flink-checkpoints.jsonl` | every checkpoint: id, status, e2e duration, size, per-operator start-delay/alignment | JM REST |
| `run-meta.txt` | RATE_HZ, job id, TM memory config, checkpoint config, host load snapshot | env + REST |

**Acceptance**: dry-run against a live RUNNING job produces all files with
sane shapes; parser unit tests green (fixture-based, no live dependency);
bash -n clean; docs-audit unaffected.

### Stage B — baseline capture (fresh window)

Run: RATE_HZ=10, load start, capture minutes 0-3 (the window where the job
historically sustains ~12k/s).

**Deliverable**: baseline dataset + a one-page summary table: per stage,
in-rate, out-rate, per-record cost (busy-time/rate), latency percentiles,
watermark lag.

**Acceptance**: capture completed with job RUNNING throughout; every stage
row populated; baseline table committed with the dataset path.

### Stage C — degraded capture (same run, later window)

Same load run, continue capture at minutes 5-15 (degraded regime).

**Deliverable**: comparison table (fresh vs degraded) per stage; **the
divergence list** — stages where in-rate vs out-rate or latency/lag diverge
between windows, ranked.

**Acceptance**: the bottleneck stage(s) named with numbers, not adjectives.
If ALL stages flow evenly and only the source stalls (as aggregate data
suggests), that itself is a named result: the throttle is at the source's
emit/fetch boundary or its input buffers, and Stage E targets it.

### Stage D — end-to-end latency

Read feature_candles_15s_preview (preview rows carry window_start) during
both windows; compute tick→preview-row latency distribution (max event-time
in raw vs read time of the preview row). Same for early-signal candidates.

**Deliverable**: latency histogram per window; explicit answer to "is
tick→preview < 4 s achievable at 10 Hz?" (final candles are floored at the
15 s window by definition — measured, but reported separately).

### Stage E — verdict + fix target

**Deliverable**: update
docs/Investigations/2026-09-01-throughput-degradation.md with: named
bottleneck, evidence, chosen fix (one of: config, single-operator code,
infrastructure), and re-measurement criteria. Fix implementation and its
validation are a separate follow-up plan (not this one).

## What this plan deliberately does NOT do

- No code changes in the SignalJob data path.
- No custom per-record instrumentation (native metrics only).
- No serialization/format changes anywhere (data path has no TSV/JSONL).
- No capacity tuning experiments (RocksDB buffers, checkpoint contract) —
  those follow the verdict, one lever at a time.
- No production-sizing claims — single-box findings only.

## Risks / open questions

- Flink 2.2's busy/backpressure metrics have shown inconsistent shapes via
  REST (empty arrays on some vertices) — Stage A must verify per-vertex
  availability first and fall back to TM Prometheus for those metrics.
- Latency histograms are source-to-operator (Flink latency markers), not
  tick-to-table; Stage D fills the true end-to-end gap.
- TM Prometheus scrape of ~80 operators × histograms every 5 s is ~200 KB/scrape —
  verified fine earlier (the drill already scrapes at this cadence).
- If degradation refuses to reproduce in a clean session (it has been
  intermittent), Stage C retries up to 3 times before declaring
  "needs longer soak" and reports which hypothesis that supports.

## Sequencing note (user-directed)

Investigation order agreed with the user: let all running tests complete →
document evidence (done: Investigations doc) → build this plan → execute
Stages A-E → only then change anything.
