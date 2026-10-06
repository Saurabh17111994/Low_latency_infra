# p99 ≤ 50 ms — integrated program

Status: active · created 2026-10-06 · supersedes the *sequencing* of the fragmented
arm-by-arm trackers listed under "Program ledger" (they keep their evidence; this file
owns the order, the decision rules, and the acceptance gate).

### Requirement Status

- Classification: optimization (primary) + trading event flow + observability (secondary).
- Readiness: **Ready with assumptions** (2 assumptions, 4 open questions, 0 blockers).
- Evidence scope: measured arms C0b / C0d / C0e (2026-10-04/06), per-hop latency markers,
  ingestion OTLP client metrics, tablet logs and data-dir inspection, live config reads.
  Limitation: a single dev stack on one host; production feed and production hardware unmeasured.
- Routed specialist domains: Flink runtime/latency instrumentation, Fluss read/write path.

## Executive Summary

The program has been run as a sequence of single 900 s arms, each testing one config idea.
Three arms produced medians of 75.5 / 42.0 / 68.0 ms on the same KPI, so the per-arm noise is
of the same size as the effects being chased. That, plus a transit model that turned out to be
wrong by an order of magnitude (measured ~4 ms, believed 30–40 ms), means the program's real
problem is **attribution**, not a shortage of candidate levers.

This plan states one latency budget with named, measurable legs; requires every proposed change
to be attached to a leg with a before/after instrument reading; fixes the experiment design
(noise floor, repeats, run hygiene) before any further optimisation; and defines one acceptance
gate that all work must pass. Parallel non-latency tracks (ordering safety, storage, production
readiness, measurement coverage) are listed so they stop competing with the latency track for
the single dev stack.

## Background

**Goal (operator, m03368):** *"but i want lo latency from tick i got from broker till my strategy
host reading it"* — i.e. the host-read KPIs are the operator's primary target.

**Contract:** `docs/plans/2026-09-30-p99-50ms-normal-path.md:24-47` declares **S1** = p99 of
`compute.latency.ingest_to_monitor` ≤ 50 ms, and a **companion** = `tick_to_strategy` p99 ≤ 50 ms
**on the ms test feed**; it explicitly excludes `tick_to_strategy` on the production feed because
that feed's event time is epoch-seconds × 1000 (`code/01_platform/04_scripts/latency_probe.py:9-11`).

**KPI validity is set by the anchor, not by the broker** (corrected 2026-10-06 after the operator's
note that the harness fake broker and the real broker cannot be compared on the same KPI):

| KPI | anchor | fake ms feed | real broker feed |
|---|---|---|---|
| `ingest_to_monitor` | our `raw.ingest_ts` | valid | **valid** |
| `ingest_to_strategy` | our `raw.ingest_ts` | valid | **valid** |
| `tick_to_strategy` | feed event time | valid (ms precision) | **invalid** (event time is epoch-seconds × 1000, 0–1000 ms quantization) |

So `ingest_to_strategy` is the one KPI that is measurable on both feeds and covers the whole path
(accept → dedup → aggregator → host read). It is the natural primary target; `ingest_to_monitor` is
the declared S1 contract and the same measurement minus the aggregator/host legs, while
`tick_to_strategy` stays a test-feed-only companion.

**Measured now (C0e, 2026-10-06, 48 snapshots / 889 s, full topology, ms feed, F1=2 F2=2, certified
checkpoint profile, deck writer stopped):**

| KPI | median | max | snapshots ≥50 ms | windows failing |
|---|---|---|---|---|
| `ingest_to_monitor` (S1) | 68.0 | 463 | 48/48 | 15/15 |
| `tick_to_strategy` (operator primary) | 84.0 | 470 | 48/48 | 15/15 |
| `ingest_to_strategy` | 72.5 | 467 | 48/48 | 15/15 |

### The budget (the integration artifact)

Every leg below is measured, and each names the instrument that measures it. A lever is only
admissible if it lands on a leg with an identified owner.

| # | Leg | Current | Instrument | Owner |
|---|---|---|---|---|
| L0 | broker ts → ingestion accept | ~18 ms | none ours (production feed ±1000 ms quantization) | feed, out of scope |
| L1 | accept → tablet ack | p50 12–24 / p90 17–48 / p99 **23–60, noisy across identical runs** | OTLP `append.latency.ms` → `main/stages/ingestion.tsv` | ingestion client + tablet write path |
| L2 | Fluss → Flink reader | 2 ms, `recordsLag` 0 | prom `fluss_client_scanner_*_fetchLatencyMs` / `_recordsLag` | Flink Fluss source |
| L3 | source → dedup | p50 1.0 / p99 cum 2.7–6.5 | prom `flink_taskmanager_job_task_latency_*` | DAG transit |
| L4 | dedup → aggregator | p50 1.0–1.2 / p99 hop +1.3–2.3 (**C0e's +40.6 did not reproduce — OQ6**) | same | aggregator (state, keys, timers) |
| L5 | aggregator → strategy host | p50 1.8–2.0 / p99 +2.0–3.5 | same | DAG transit |
| L6 | sinks | p50 0–1, p99 538–777 (suspect) | same — **sparse-stream marker artifact, unproven** | sinks |
| **L7** | **ack → record emitted by the Flink source** | **≤ ~15 ms p99 — corrected 2026-10-06 (Task 3c); the 65–69 ms first booked here was mis-attributed.** Fluss's own source gauge reads a 1–5 ms record age *at fetch* (max 17 of 49 snapshots), `pendingRecords` ≈ 0 (48/49), in-DAG ≤ 8 ms p99. The ~65 ms actually sits **upstream of the log read**, in the ingestion client→tablet publish path | prom `currentFetchEventTimeLag` + `pendingRecords` (Fluss/Flink native, already captured in every arm since 2026-09-26); `FlussVisibilityProbe` (independent reader) | **moved to L1 — Task 3b** |

Total in-DAG transit (L3–L5) is **~4 ms at p50**; the previous "3–5 hops × 10 ms buffer = 30–40 ms"
model is dead at `BUFFER_TIMEOUT_MS=2`.

Definition control: `ingest_ts` is stamped inside `TypedFlussRowConverter.append()`
(`code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TypedFlussRowConverter.java:151-169`),
after `RawTickWriter.write()` takes `acceptTime`
(`code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java:180-190`),
so `ingest_to_monitor` **understates** accept→monitor. It is not a definitional inflation.

## Functional Requirements

1. **FR1** Every latency change must name the budget leg (L0–L7) it targets, the instrument that
   measures that leg, and the before/after readings. Changes without attribution are not run.
2. **FR2** Each measurement arm must record: full or reduced topology with the flag/vertex pair,
   which feed drove the pipeline, whether the deck's live ingestion writer was running, tablet
   `kv.snapshot.interval`, checkpoint profile, F1/F2 values, and the presence-gate verdict.
3. **FR3** Leg L7 must have a dedicated instrument before any lever that targets it is proposed.
4. **FR4** Every arm must end with the stack restored to its pre-arm state (deck containers back,
   config reverted) and verified.
5. **FR5** A change is accepted only if it passes the acceptance gate (Task 9) including the cost
   guards; a KPI gain with a cost regression beyond the guards is rejected.

## Non-functional Requirements

- **NFR1 (noise floor)** The per-KPI run-to-run spread must be measured before it is used to judge
  anything. Until measured, treat ~±25 ms on medians as the working assumption (75.5 / 42.0 / 68.0
  across C0b / C0d / C0e).
- **NFR2 (minimum detectable effect)** A change is "material" only if its effect exceeds 1.5 × the
  measured noise floor, and it must reproduce in a second identical arm.
- **NFR3 (cost guards)** Throughput parity ±2 %; backpressure ≤ 100 ms/s; append p99 no worse than
  +20 % of the same-day control; disk growth no worse than the same-day control; all checkpoints
  COMPLETED.
- **NFR4 (storage)** Disk must stay above 20 % free during any arm (measured free space: 316 GB;
  `raw_table_1` grew +179 MB/min at the dev rate and +405 MB/min with a second live writer).
- **NFR5 (measurement honesty)** A KPI is only quoted with: n samples, capture span, feed type,
  topology, and any truncation caveat.

## Architecture

Nothing in the data path changes in Phase 0/1. The work is: (a) an instrument inventory for L7,
(b) attribution of L4 and L7, (c) a lever chosen from attribution, (d) certification.

- Measurement ownership: Flink native latency markers (already wired —
  `code/01_platform/01_docker/docker-compose.yml:253` sets `metrics.latency.interval: 1000`;
  `code/01_platform/04_scripts/pipeline-lib.sh:1118` passes `-Dmetrics.latency.interval=2000` default)
  cover L3–L6 only; they start at *emission* from the source, so **L7 is structurally invisible to
  them**. That is why L7 has no instrument.
- Probes live in `code/01_platform/04_scripts/fluss-probes/` and are compiled/run by
  `code/01_platform/04_scripts/stage-capture.sh` (probe compile block ~:689, sampling loop ~:700,
  `timeout ${PROBE_TIMEOUT_S:-20}`). Existing: `FlussReadLagProbe` (passive, writes
  `main/stages/read-lag.tsv`), `FlussKvProbe`, `FlussReadabilityProbe`, `FlussTableStatsProbe`.
- Harness: `code/01_platform/06_stage_profiler/stage-profile.sh` (smoke gate + main capture;
  `STRATEGY_HOST_ENABLED` / `STRATEGIES` / `MULTITF_ENABLED` reach the `flink run` client, not a
  container — `code/01_platform/04_scripts/stage-capture.sh:179`).
- Scorer: `/tmp/kpi.py` (worst-subtask `quantile="0.99"` per snapshot → 60 s window max);
  per-hop: `/tmp/hops.py`. Both are throwaway and must move into the repo (Task 1).

## Data Model

No schema change is part of this program. Relevant columns/keys: `raw_table_1.ingest_ts` (L1/L7
anchor, `code/01_platform/02_sql/ddl/02_raw_table_1.sql`), `Signal_Candidates_current.evaluation_ts`
(already used by the versioned merge engine, CHG-547), and the KPI histograms
`compute.latency.{ingest_to_monitor,tick_to_strategy,ingest_to_strategy}`
(`code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StrategyHostFunction.java:252-258`,
`.../IngestLatencyMonitorFunction.java:29-48`).

## Interfaces

- Config keys under test: `FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS` (F1), `BUFFER_TIMEOUT_MS` (F2),
  `UNALIGNED_CHECKPOINTS`, `CHANGELOG_STATE_BACKEND`, `CHECKPOINT_INTERVAL_MS`,
  `INGESTION_MAX_BATCH_RECORDS` / `INGESTION_MAX_BATCH_WAIT_MS` (batching is forbidden by design on
  the tick path — `docs/08_implementation/03-ingestion.md:153,157` — so it is a lever only for bulk
  paths), tablet `kv.snapshot.interval` and `log.flush.offset.checkpoint-interval`.
- Probe CLI contract (existing): `java -cp <cp> FlussKvProbe <table> <window_ms> <tokens_csv>
  [bootstrap] [tf]`; `FlussReadLagProbe <database> <table> [bootstrap]` prints
  `<epoch_ms> <table> <partitions> <buckets> <logEndSum>`; exit codes 0/1/2/3 = complete / no sample /
  unusable input / partial.
- Gate: `main/stage-capture.log` must not end with the prom-staleness failure; the smoke presence
  gate (`<run>/smoke-presence.json`) must have `"failures": []`.

## Failure Handling

- **Prom endpoint death mid-capture** (seen in C0e: `last prom scrape was 26s ago ... TM prom endpoint
  died before capture end`) truncates the latency series. Treat the run as partial, quote n, and do
  not compare a truncated tail against a full one.
- **Gate false positives from a reduced topology**: a missing `STRATEGY_HOST_ENABLED=true` silently
  deploys a 3-vertex job with no candle sink, so every read probe returns nothing (this cost four runs
  on 2026-10-04). The fail-fast topology + read-path gates now land in `c7d4acf0`
  (CHG-544/CHG-545, `code/01_platform/04_scripts/tests/test_stage_capture_topology_gate.py`) — so this
  failure mode is closed; the remaining Task 2 work is the metadata block.
- **Run contamination**: the deck's `01_docker-ingestion-1` writes to the same table
  (`ARROW_FEED=token`, `RAW_TABLE_NAME=raw_table_1`, `FLUSS_BOOTSTRAP=fluss-coordinator:9123`) and
  made C0d's `tick_to_strategy` a mix of ms and whole-second timestamps. Stop it (or run off-hours),
  and record its state in the arm metadata.
- **Restore discipline**: an arm that stops containers must restart and health-verify them.

## Edge Cases

- Feed with whole-second timestamps ⇒ `tick_to_strategy` invalid; only the ms test feed counts.
- Sparse streams (candidates ≈3 rows/s, candle_features ~1.5 writes/s) make downstream latency
  markers look enormous (L6 p99 538–777 ms) — do not read those as transit.
- Checkpoint-coincident samples: spikes spaced ~55–59 s align with the checkpoint cadence
  (CHG-543 measured unaligned 618 wave samples / 359 ms spike vs aligned-only 0 / 115 ms); attribute
  a spike to a checkpoint only with the checkpoint's own timeline, not by spacing alone.
- `SC12` churn is already fixed (CHG-460, `4f065d30`): checkpoint `state_size` 0.58–0.76 MB. Do not
  re-open it as a suspect.

## Performance Targets

- **PT1** `ingest_to_strategy` p99 ≤ 50 ms (per 60 s window, worst subtask) in ≥14 of 15 windows —
  the cross-feed primary (candidate, OQ1).
- **PT1b** `ingest_to_monitor` p99 ≤ 50 ms, same windows — the declared S1 contract.
- **PT2** `tick_to_strategy` p99 ≤ 50 ms in ≥14 of 15 windows, **ms test feed only**, full topology.
- **PT3** No cost-guard regression (NFR3).
- **PT4** Attribution requirements met for every accepted change (FR1).

## Observability

- Already available: OTLP ingestion stages (`main/stages/ingestion.tsv`), Flink native latency
  markers, Fluss client scanner/writer gauges (`fetchLatencyMs`, `recordsLag`, `timeMsBetweenPoll`,
  `pollIdleRatio`, `sendLatencyMs`, `recordsPerBatch`), 5 s prom snapshots, probe TSVs, tablet logs.
- **Missing (this program adds):** an instrument for L7; the KPI scorers in-repo; a stated noise
  floor per KPI; a run-metadata block (FR2) present in every arm's `run-meta.txt`.

## Acceptance Criteria

| ID | Criterion | Requirement |
|---|---|---|
| AC1 | PT1 achieved in 2 consecutive clean arms with the deck writer stopped and the presence gate PASS | FR5, PT1 |
| AC2 | PT2 achieved under the same conditions | PT2 |
| AC3 | All NFR3 cost guards green in the accepted arm | FR5, NFR3 |
| AC4 | Every adopted change carries a named leg with before/after instrument readings | FR1 |
| AC5 | Each arm records FR2 metadata and ends with the stack restored (FR4) | FR2, FR4 |
| AC6 | Noise floor documented per KPI and every accepted effect exceeds 1.5 × it | NFR1, NFR2 |

## Test Strategy

- **Noise floor**: 2 back-to-back identical arms (same config, same conditions), score both, report
  the per-KPI spread. This is Task 1's deliverable and gates all later comparisons.
- **Instrument test**: the L7 probe must be validated against a known condition before use — e.g. it
  must show ~0 ms when the pipeline is stopped and non-zero when a writer is active.
- **Regression**: `make test` (covers `common` + `01_ingestion`) for any code change; the stage
  presence gate for every arm; `python3 code/01_platform/04_scripts/change_control_check.py` for any
  change record.
- **Chaos/failure**: not part of this program (M3.x items remain in
  `docs/plans/2026-10-03-measurement-coverage.md`).

## Risks

- **Attribution stall**: L7 may turn out to be a Flink connector/plugin limitation rather than a
  config lever; the mitigation is the L7 probe's split (Fluss-reader-visible vs Flink-source-emitted)
  which localises the delay to one side before any code change is attempted.
- **Noise**: without the noise floor, a lever could be adopted on noise. Mitigation: Tasks 1 and 9.
- **Single dev stack**: every arm blocks the cluster; storing non-latency tracks behind a serial
  queue (this plan) is the mitigation. Nothing runs during a `make gate`.
- **Storage**: an arm at the dev rate plus a live writer grew `raw_table_1` at +405 MB/min; long arms
  need the writer stopped (NFR4).
- **Unproven instruments**: the sink p99 markers and the read-lag absolute delta (which disagrees
  with `scanner recordsLag = 0`) are unresolved; do not build a decision on either until Task 3.

## Open Questions

| # | Question | Impact | Owner |
|---|---|---|---|
| OQ1 | Is PT1 (`ingest_to_strategy`, the only KPI valid on both feeds) the formal primary, with S1/PT1b and PT2 as supporting gates? | phase order and which number is reported at the end | operator |
| OQ2 | Why does `read-lag.tsv` (`logEndSum` − source consumed) show a ~65k–95k record offset that grows, while the scanner reports `recordsLag = 0`? Is the probe's absolute value meaningful? | measurement validity for L2/L7 | Task 3 |
| OQ3 | Are the sink latency markers (p99 538–777 ms) real or sparse-stream artifacts? | whether L6 is a target | Task 3 |
| OQ4 | Is the ~65–69 ms L7 gap on the Fluss side (reader visibility) or inside the Flink source (read→emit batching)? | which repository the fix lives in | **Answered 2026-10-06 (Task 3a): not Fluss-side** — a plain reader sees a record 9 ms p50 after `ingest_ts` (2 ms floor) while the source polls every 2.1 ms with 0 records lag, so the remaining ~55 ms sits between the source's fetch and its emission (Task 3b) |
| OQ5 | S11 (`closeread.tsv` window-close → first read, declared SLO p99 ≤ 75 ms) measures 1 258–9 576 ms with 100 % of windows "violating" — but the sampler only polls every ~17 s, so can this path measure 75 ms at all? | whether a declared SLO is verifiable by the current instrument | measurement track (M-items) |
| OQ6 | Did the C0e aggregator p99 hop (+40.6 ms) have a cause, or was it one run's tail? Task 1's two arms measured +1.3/+2.3 ms on the same config, so the standing term is gone — decide whether to re-open it only if it recurs | whether F4/hop work is still motivated | operator / Task 4 |
| OQ7 | The 64 MB writer buffer pool, the pinned 64 KiB batch, the 1 ms linger and per-record submits produce ~1 600 requests/s per ingestion JVM (`FlussClientAdapter.connect()`). **Which sub-interval of accept→ack owns the ~55–62 ms p99**: pool wait, Sender queue, send/RTT, server append, or response handling? Also: what exactly does `append.pending.records` (standing 530–776) count? | which knob (if any) is worth an arm, and whether the read-side program can reach PT1 at all | **Task 3b** |

## Implementation Roadmap

### Task 1: Establish the noise floor and put the instruments in the repo
**Why:** every later comparison is unfounded without it (NFR1/NFR2).
**Files:** new `code/01_platform/04_scripts/kpi_windows.py`, new `code/01_platform/04_scripts/hop_budget.py`,
new `code/01_platform/04_scripts/tests/test_p99_instruments.py` (one fixture file covering both
scorers); evidence in `logs/tracker-14/`.
**Depends on:** none.

- [x] Port `/tmp/kpi.py` (worst-subtask p99 per snapshot → 60 s windows) and `/tmp/hops.py`
      (per-hop p50/p99 increments) into the repo, with the usage contract in their docstring.
- [x] Add a self-check test on a fixture (synthetic prom snapshots with hand-computed windows) so the
      scorer's arithmetic is pinned. `pytest code/01_platform/04_scripts/tests/test_p99_instruments.py`
      → 6 passed. The ported scorers reproduce every recorded arm number exactly: C0b
      `ingest_to_strategy` 85.0/364.0, `ingest_to_monitor` 75.5/114.0, `tick_to_strategy` 93.5/370.1;
      C0d 63.0/654.0, 42.0/464.0, 81.0/665.0; C0e 72.5/467.0, 68.0/463.0, 84.0/470.0; C0e hop
      increments p50 +1.0/+1.2/+1.8 and p99 cumulative 6.5/47.2/50.7 with `fetchLatencyMs` 2/4 —
      i.e. the in-repo instruments are the same arithmetic the earlier records were built on.
- [ ] Run 2 identical arms (`SMOKE_S=200 MAIN_S=900`, F1+F2, certified profile, deck writer stopped,
      full topology) and score both; record the per-KPI spread as the noise floor. **Done
      2026-10-06 12:56** (bg task `b753af2ad`, driver archived at
      `logs/tracker-14/20261006-task1-noise-floor-attachments/task1-noise.sh`; arms
      `logs/stage-profile-task1-noise-1006-120852-a{1,2}`; both rc=0, gate PASS, 49 snapshots each,
      deck `01_docker-ingestion-1` stopped for both and restored healthy after).
- [x] Record the noise floor in this file and in `docs/plans/2026-10-03-p99-50ms-combined-plan.md`
      (full record: `logs/tracker-14/20261006-task1-noise-floor.md`).

**Noise floor (2 identical arms, same config, same conditions):**

| KPI (worst-subtask p99, 60 s windows) | arm 1 median | arm 2 median | delta | arm 1 max | arm 2 max |
|---|---|---|---|---|---|
| `ingest_to_strategy` (PT1) | 79.0 | 73.0 | **6.0** | 109.1 | 109.0 |
| `ingest_to_monitor` (PT1b / S1) | 72.0 | 68.0 | **4.0** | 77.0 | 74.0 |
| `tick_to_strategy` (PT2, ms feed) | 91.0 | 83.0 | **8.0** | 119.0 | 118.0 |

- **Noise floor = 8 ms** ⇒ a material effect must exceed **12 ms** (1.5 × the floor, NFR2) *and*
  reproduce in a second arm; compare **medians**, never `max` (C0e monitor max was 463 against
  74–77 here on the same config).
- ~~**L7 is confirmed as the only material term**~~ **CORRECTED 2026-10-06 (Task 3c) — the 65–69 ms
  first booked here is not L7.** Subtracting the *p99 of the whole in-DAG chain* from a KPI that is
  itself a p99 is a valid bound only if the ack p99 in the same window is small; in arm 1 the
  ingestion client's own accept→ack p99 was **62 ms in those same windows**, so the subtraction
  charged L7 with a tail that belongs to L1. Native instruments then closed the question from the
  other end: at fetch the newest record's own age is 1–5 ms, the reader queue is empty, and the chain
  is ≤ 8 ms — leaving ≲15 ms for the whole post-append path. **The p99 budget is spent before the
  record reaches the log**, i.e. in L1/Task 3b — and this is also why every read-side lever
  (F1 fetch window, F2 buffer timeout, certified checkpoint profile, `kv.snapshot.interval`) failed to
  move PT1 reproducibly: they are all downstream of the term that owns the tail.
- **The C0e aggregator p99 hop did not reproduce** (+1.3 / +2.3 ms here vs +40.6 ms in C0e) — see OQ6.
- **L1 (append ack) is itself noisy**: p99 median 60 (arm 1) vs 33 (arm 2) vs 23 (C0e). L1 claims need
  repeats. **Corrected 2026-10-06: L1 *is* the SLO carrier** — the same-window comparison above shows
  the client's own accept→ack p99 tracking the KPI's scale (arm 1: ack 62 / KPI 72; arm 2: ack 35 /
  KPI 68; C0e: ack 26 / KPI 68), while the whole post-append path measures ≲15 ms. The KPI's own
  stability (67–77 in 147/147 snapshots across three unlike arms) and its weak correlation with the
  ack p99 (Pearson r 0.46 / 0.29 / 0.16) say the two are *different windows of one slow path*, not
  two independent effects — a standing queue with a tail, not a load response.
- Cost guards green in both arms: throughput 4 962 / 5 019 rows/s, backpressure ≤ 85 ms/s, busiest
  operator ≤ 103 ms/s, 15 / 16 aligned checkpoints, GC ≥15 ms at 11 / 7 events (max 30.2 / 35.9 ms),
  `raw_table_1` +187.09 / +175.32 MB/min with the deck writer stopped.

### Task 2: Remove the known sources of false readings
**Why:** several arms were invalidated by harness/hygiene defects, not by the platform.
**Files:** `code/01_platform/04_scripts/stage-capture.sh` (`run-meta.txt` block, ~:233-256),
`code/01_platform/06_stage_profiler/stage-profile.sh` (deck-writer control), plus its test.
**Depends on:** none.

- [x] fail-fast topology gate (flag true + vertex absent ⇒ exit 1) + read-path expectation —
      committed in `c7d4acf0` (CHG-544/CHG-545, test present). `run-meta.txt` already carries
      `rate_hz`, `checkpoint_interval_ms`, `probe_table`/`probe_tokens`, `topology_branches`,
      `topology_operators`, `topology_flags_requested`.
- [x] Extend the FR2 metadata block with what is still missing: feed type (ms test feed vs production),
      deck `01_docker-ingestion-1` state (running/stopped), tablet `kv.snapshot.interval`,
      `UNALIGNED_CHECKPOINTS` / `CHANGELOG_STATE_BACKEND`, and the F1/F2 values in force.
      **Landed (CHG-550):** `feed_type`, `unaligned_checkpoints`, `changelog_state_backend`,
      `scanner_fetch_wait_max_time_ms`, `buffer_timeout_ms` from the caller env, plus two read back
      from the running stack — `deck_ingestion_state` (`docker ps -a` on
      `01_docker-ingestion-1`, which doubled C0d's growth rate while up) and
      `tablet_kv_snapshot_interval` (`docker exec … grep '^kv.snapshot.interval'
      /opt/fluss/conf/server.yaml`, dev 1m vs certified 0s). `stage-profile.sh` now passes
      `FEED_TYPE="$FEED"` into capture.
- [x] Make the prom-staleness failure mark the run `partial` in the same file instead of only failing
      the phase. **Landed (CHG-550):** `partial=yes` + `partial_reason=prom-stale:<gap>s>budget<N>s`,
      so "truncated" cannot be misread as "never measured".
- [x] Add a test asserting the extended metadata block is written when the arm starts.
      `test_run_meta_records_the_arm_configuration` (hermetic stubs, including the two
      docker-derived fields); `test_stale_prom_marks_the_run_partial` covers the partial marking and
      `test_prom_endpoint_dead_at_capture_start_fails_before_capturing` pins the START gate as the
      owner of dead-at-start (rc 1 before the timed window). File: 8 passed.
- [x] Fix the prom-staleness budget so it cannot false-fail a healthy run. Evidence: C0e
      (`logs/stage-profile-armC0e-clean-1006-105008`) exited 1 on
      `!! FAIL: last prom scrape was 26s ago (budget 25s at interval 5s; …)` while its capture was
      complete (48 snapshots, gate PASS, `stage-capture: duration reached (900s)`). The budget is
      `max(CAPTURE_INTERVAL_S × 5, 25)` (`code/01_platform/04_scripts/stage-capture.sh:1198-1208`)
      but the *observed* snapshot spacing is probe-bound, not sleep-bound: measured 17 s median and
      24 s max in the Task 1 arms (16–23 s in C0e) — 25 s is inside the noise of a single
      trailing iteration. Derive the budget from the run's own observed spacing (e.g.
      `max(25, 2 × observed_max_gap)`), which still catches a genuinely dead TM (a dead TM produces
      no further snapshots, so its gap grows without bound) and stops flagging healthy runs. The
      Task 1 arms exited 0 on the same cadence, i.e. the check is borderline rather than wrong in
      intent — fix it with Task 2 so a run's `rc` keeps meaning something.
      **Landed (CHG-550):** the budget is now `max(25, 2 × observed_max_gap)` computed from the
      epochs in the run's own `prom-*.txt` names, and the four values behind the verdict
      (`prom_snapshots`, `prom_observed_max_gap_s`, `prom_staleness_budget_s`, `prom_final_gap_s`)
      are appended to `run-meta.txt` whichever way the run ends.

### Task 3: Attribute L7 (ack → Flink-source emit) and re-check L2/L6 instruments
**Why:** L7 is the largest unexplained term — measured ~65–69 ms on the Task 1 noise-floor arms, i.e.
~10× the whole in-DAG chain — and the sink/read-lag numbers are unproven.
**Files:** new `code/01_platform/04_scripts/fluss-probes/FlussVisibilityProbe.java`; wire it into
`code/01_platform/04_scripts/stage-capture.sh`; findings in `logs/tracker-14/`.
**Depends on:** Task 1.

- [x] Write a Fluss **client** reader probe that polls `raw_table_1` in a tight loop and, per sample,
      prints the newest `ingest_ts` seen and the wall-clock when it became visible — a stopwatch on
      `ack → any-reader-visible`, independent of Flink. Landed as
      `code/01_platform/04_scripts/fluss-probes/FlussVisibilityProbe.java` (subscribe-at-log-end, so
      it only ever sees fresh records; fetch wait pinned to the job's 2 ms). **Two launch rules
      learned the hard way on 2026-10-06:** a reader probe must be started with
      `--add-opens=java.base/java.nio=ALL-UNNAMED` (the Fluss client's shaded Arrow cannot
      initialise `MemoryUtil` on JDK 17; without it the probe dies at the first record with
      `ExceptionInInitializerError` and exits 0 with zero samples — metadata-only probes such as
      `FlussReadLagProbe` do not need it, which is why the omission was invisible), and the probe
      must be added to `PROBES` in `code/01_platform/04_scripts/tests/test_fluss_probes.py` or its
      source is never compiled by the suite.
- [x] Validate it on a known condition (pipeline idle ⇒ no rows; writer active ⇒ rows visible within
      the sampling period). Verified: with the deck writer stopped and the table flat, nothing is
      readable at log end; with the fleet feeding, both probe runs collected records
      (`__END__ 108720` / `72694`).
- [x] Run one arm with it alongside the existing probes; compare `ack→visible` against L1's p99 (23 ms)
      and against the monitor KPI (L3-cumulative). This splits L7 into "Fluss side" (OQ4 → F3 tablet
      work, server source on disk at
      `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/fluss/fluss-_1.0.0/Fluss_v_1.0.0/`)
      and "Flink source side" (read→emit batching in the job's source operator). **Done 2026-10-06
      (bg `ba0627fe7`, run `logs/stage-profile-visprobe-1006-185947`, record
      `logs/tracker-14/20261006-task3a-visibility-probe.md`): the Fluss half is small.** A plain
      reader sees a record **9 ms p50 after `ingest_ts`, 2 ms floor** (newest_age p90 22–24, p99 38;
      two consecutive probes, 1 678 + 1 112 samples, 181 414 records) — the same order as the
      ingestion client's own accept→ack (11–12 ms p50) in the same window, i.e. visibility after the
      ack costs ~nothing. The Flink scanner's own gauges agree (0 records lag, poll every 2.1 ms) and
      so do the native markers (whole in-DAG chain ≤ 8 ms p99). Only the record-carried KPI
      disagrees (68–72 ms) — and it is the only instrument whose clock starts before the record is in
      the source's hands. **⇒ OQ4 answered: the remaining ~55 ms is between the source's fetch and its
      emission, i.e. Flink-source-side.**
- [x] **Task 3c (2026-10-06, done, no arm needed — the instrument was already on disk):** the
      Fluss source's own reader gauge `currentFetchEventTimeLag` has been captured in **every** arm
      since 2026-09-26 (`code/01_platform/04_scripts/stage-capture.sh` scrapes the
      `flink_taskmanager_job_task_operator_` family). Bytecode-verified semantics
      (`org.apache.fluss.flink.source.reader.FlinkSourceSplitReader.forLogRecords`: offsets 0 and
      335–350): `System.currentTimeMillis() − max(ScanRecord.timestamp())` over the fetched batch, i.e.
      **the age of the newest record at the instant the reader fetched it**, updated per fetch.
      Result on the Task 1 arms (49 snapshots each, same windows as the KPI): worst-subtask value
      **1–5 ms, max 17 ms, 0/49 ≥ 30 ms**; `pendingRecords` **0.0 in 48/49** (one 24). Against the
      same-window KPI (arm 1: `ingest_to_monitor` p99 72; arm 2: 68) and the client's own
      `append.latency.ms` p99 (62 / 35), the post-append path is ≲15 ms and L1 owns the rest.
      **⇒ OQ4 is re-answered: neither Fluss-side nor Flink-source-side — upstream of both.** The
      earlier Task 3a conclusion (2026-10-06, same day) came from comparing a *p50* reader age (9 ms)
      against the *p99* KPI; the probe's own p99 was 38 ms (max 105–142), which already carried the
      write-path tail. Record: `logs/tracker-14/20261006-l7-write-path-attribution.md`.
- [ ] **Task 3b (revised — the write path, not the source):** split L1's accept→ack into intervals we
      can act on, using what the client already exposes plus one small histogram in our own writer:
      (i) zero-code: probe the Fluss client jar for the writer-buffer keys and their defaults
      (`client.writer.buffer.memory-size`/`page-size` — the repo relies on a "64MB pool" per the
      `FlussClientAdapter.connect()` comment) and establish what `append.pending.records` counts;
      (ii) one change in `code/02_services/01_ingestion/`: histogram the pool/buffer wait
      (`RawTickWriter.submitAppend` entry → return) separately from send→ack, beside the existing
      `append.latency.ms`. Then decide whether the lever is a client knob (batch/pool sizing) or a
      tablet/write-path item. Nothing in this task touches the DAG.
- [ ] Explain or discard the `read-lag.tsv` absolute delta (OQ2) and decide whether L6 markers are
      artifacts (OQ3); record both verdicts.

### Task 4: Attribute L4 (aggregator p99 hop +40.6 ms)
**Why:** it is the only in-DAG term that reaches the tens of ms; it drives the host-read KPIs.
**Files:** read-only analysis; evidence in `logs/tracker-14/`.
**Depends on:** Task 1.

- [ ] From the accepted arms, correlate the aggregator hop's p99 with: subtask key distribution,
      checkpoint records in `main/stages/cp-phases-detail.jsonl`, `sendLatencyMs` of the
      candle_features sink, and TM GC pauses.
- [ ] Decide whether the p99 hop is checkpoint-coincident, key-skew-driven, or a plain queueing tail,
      and record the verdict with numbers.
- [ ] Propose exactly one lever for it (or conclude "no lever"), with the leg and instrument named.

### Task 5: Decide F1+F2 adoption on repeatable evidence
**Why:** C0d suggested a gain that C0e did not reproduce; the decision is currently unsettled.
**Depends on:** Task 1 (noise floor).

- [ ] Run the F1+F2 vs default pair again with the Task 1 noise floor available, same-day, back-to-back.
- [ ] Apply NFR2 (effect > 1.5 × noise floor, reproduced twice) and record adopt / reject with numbers.

### Task 6: Implement the single chosen lever
**Why:** one attributed lever at a time keeps attribution intact.
**Files:** depends on Task 3/4 outcome (config key, DAG change, or a Fluss/Flink-side fix);
change record `CHG-…` + `docs/08_implementation/04-signal-job.md` or `02-schema-storage.md` as applicable.
**Depends on:** Task 3 or Task 4, plus Task 5 if the lever is F1/F2.

- [ ] Implement the lever named by attribution, with the leg and before/after instrument readings.
- [ ] Add/update the regression test that protects it.
- [ ] Record `make test` (or the exact reason it could not run) and the change record.

### Task 7: Certify
**Why:** convert a single-arm result into a repeatable one.
**Depends on:** Task 6.

- [ ] Run 2 consecutive clean 900 s arms (full topology, ms feed, deck writer stopped, cost guards
      instrumented) and score both with the Task 1 scorer.
- [ ] Confirm AC1–AC3 on both; if either arm fails a window, treat the change as unproven and return to
      attribution rather than adding another change on top.

### Task 8: Adopt in the dev profile and record the decision
**Why:** the result must not live only in the harness invocation.
**Files:** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java`
(F1 at `:1373`, F2 at `:1377`), `code/01_platform/04_scripts/pipeline-lib.sh:1165,1190`,
`docs/plans/2026-10-03-p99-50ms-combined-plan.md`, this file.
**Depends on:** Task 7.

- [ ] Apply the certified values as defaults (or record why they stay opt-in).
- [ ] Update the plan trackers and write the change record.

### Task 9: Verify acceptance criteria

- [ ] AC1, AC2 from the Task 7 arms; AC3 from their cost-guard output.
- [ ] AC4, AC5, AC6 from the arm metadata, records, and noise-floor section.
- [ ] `make gate` green after adoption — **never run while another arm is in flight**.

## Program ledger (what this file does not replace)

| Document | Owns |
|---|---|
| `docs/plans/2026-10-03-p99-50ms-combined-plan.md` | the arm-by-arm evidence (C0b…C0e, C2b, C2c, deferred levers) |
| `docs/plans/2026-09-30-p99-50ms-normal-path.md` | the SLO definitions (S1 + companion) |
| `docs/plans/2026-10-03-measurement-coverage.md` | M1–M6 coverage gaps for the other seven SLOs |
| `docs/plans/2026-10-04-versioned-merge-adoption-audit.md` | ordering-safety adoption (CHG-547 done; `Position_State` + gateway projections pending) |
| `docs/plans/2026-10-04-overengineering-audit.md` | code-deletion findings (nothing material) |
| `docs/plans/2026-09-22-fluss-1.0-upgrade.md`, `…-native-adoption.md` | Fluss 1.0 upgrade/adoption backlog |

Parallel non-latency tracks (serialised behind the latency track on the single dev stack):
ordering safety (`Position_State` versioned merge, `Positions` version space, stale runbook SQL),
storage policy (`raw_table_1` retention vs +179 MB/min, `Signal_Candidates_current` 7 d changelog,
`candle_features` 3 d→1 d, `ingestion_quarantine` TTL, idle `.index`/`.timeindex` ~460 MB),
production readiness (real-VM deploy unproven, node-local changelog base path), M1–M6 coverage,
plan-ledger sweep, `logs/` policy.
