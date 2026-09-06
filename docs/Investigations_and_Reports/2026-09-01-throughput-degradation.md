# Investigation: Pipeline Throughput Degradation

- **Date**: 2026-09-01
- **Status**: RESOLVED (2026-09-02) for the ~10k/s feed tier — root causes isolated and fixed; sustained ~9,950/s at 1024 tokens, C2 drill PASS. The 15k/s capacity target itself remains OPEN (feed-bound; see Resolution)
- **Related**: CHG-120 (C2 drill), tracker-14 Block 1
- **Machine**: single host, 16 cores / 15.5 GB RAM, NVMe SSD; all services dockerized on one box
  (Flink JM+TM, Fluss coordinator+tablet, MinIO, OpenObserve, otel-collector, ingestion JVM, faketool)

## Aim / Goal

The platform must sustain:

| Target | Value |
|---|---|
| Throughput | **> 15,000 ticks/s** sustained |
| End-to-end latency | **< 4 s** (tick to preview/early signal; final candles are bounded below by the 15 s window — 15 s is the physical floor for final outputs, not an infrastructure limit) |

Today neither target is met. This investigation exists to find what limits and
degrades throughput, with evidence, so the fix targets the real cause.

## The Issue We Are Solving

The Flink SignalJob cannot consume the feed rate:

- Feed into Fluss (ingestion-acked): **10,240 ticks/s** — verified working at full rate.
- Job consumption, fresh start: **~12,000/s for the first 1-2 minutes** (pipeline is capable).
- Job consumption, steady state after minutes: **~1,600-3,000/s**, degrading further under sustained load (~900/s in the worst measured window).
- Because the job falls behind, the watermark lags minutes behind event time,
  windows never close, and the C2 drill's G7c parity gate fails with tens of
  thousands of "no final candle" mismatches. (G7 raw recount in every run:
  **0 duplicates, 0 late rows, no data loss across TM kills** — correctness is
  proven; capacity is the open problem.)

**Signature of the failure**: fast when fresh, decays within minutes, and the
decay persists regardless of checkpoint settings.

## What We Have Done (chronological, with evidence)

1. **Watermark / zero-candle bug — SOLVED (separate issue, earlier today).**
   Wall-clock idle marking added to `SourceIdleWatchdogGenerator`
   (Flink pauses idleness clocks under backpressure, FLINK-35886). 12/12 unit
   tests; candles/previews/signals emit after the fix. Documented in CHG-120
   and docs/08_implementation/25-partitioned-source-watermark-rule.md.

2. **RocksDB state on container overlay filesystem — REAL DEFECT, FIXED, but NOT the throughput cause.**
   - Evidence: 2.1 GB of RocksDB state under `/tmp/flink-rocksdb` on docker
     overlayfs (no volume mounted); JFR profile showed task-thread native time
     concentrated in `RocksDB.put/delete/Checkpoint.createCheckpoint`.
   - Fix: named volume `flink-rocksdb` mounted at exactly
     `/tmp/flink-rocksdb` (docker-compose.yml, CHG-120).
   - Verdict: post-fix throughput unchanged (~3k/s). Necessary hygiene, not the cap.

3. **Checkpoint frequency — RULED OUT.**
   - 10 s interval (default): job eventually restarts on checkpoint timeout
     (durations 7-40 s, growing).
   - 30 s interval / 60 s timeout: job stays RUNNING (smoke PASS at 10 Hz),
     throughput still degrades to ~2k/s.
   - Checkpoints effectively disabled (10-min interval, 5-min window):
     throughput **~900/s average — WORSE**, not better.
   - Conclusion: checkpoints amplify the damage but are not the primary cause.
     Notably the no-checkpoint run being worst suggests the slowdown is in the
     record path itself once the job is behind.

4. **Network buffer starvation — RULED OUT.** TM network memory 256m → 512m
   tested under 10 Hz load: no throughput change. Default reverted to 256m;
   env parameterization kept.

5. **Stale-state / environment accumulation — RULED OUT.** Pruned 14 GB of
   retained checkpoints + 672 MB of dead RocksDB dirs (SIGKILLed TMs cannot
   clean up), restarted the whole stack, clean-slate 10 Hz run: ~1.6-2.8k/s.
   Same degraded regime.

6. **Log tiering / remote reads — RULED OUT.** raw_table_1 has zero tiered
   segments; only 5 remote files exist (unrelated tables, none written after
   18:00). The source reads local segments only.

7. **Individually ruled out earlier** (with evidence): WAL fsync (WAL disabled,
   0-byte .log files), GC (clean, no Full GC), disk saturation (w_await ~0.7 ms,
   14+ cores idle), ingestion feed (full 10,240/s acked into Fluss), Kryo
   serialization (InternalTypeInfo throughout).

8. **Profiling artifacts** (for future reference): `/tmp/prof2.jfr` (pre-volume-fix),
   `/tmp/prof3.jfr` (contaminated — job had restarted), `/tmp/prof4.jfr`
   (post-fix, degraded steady state: Java CPU samples ≈ zero on task threads;
   native samples in RocksDB.put 384 / delete 81 / createCheckpoint 70 vs
   ~2,286 epoll waits; the few hot Java threads were the Prometheus reporter
   HTTP threads).

## What We Know (current state of knowledge)

**Established facts:**
- The pipeline CAN process ~12k/s when fresh — code path and operators are fast enough in principle.
- Degradation begins within minutes of sustained load; it is tied to the job
  being behind (backlog), not to time alone (fresh runs after cleanup restart fast).
- Once degraded, source subtasks sit ~98-99% backpressured, downstream operators
  idle, and the Fluss client shows `requestsInFlight=0` with fetch latency only
  5-11 ms — the source is NOT waiting on Fluss; it is throttled before it fetches.
- Checkpoint barriers stall at `active-signal-feedback` (up to ~7 s alignment)
  waiting on the idle `position-state` source (0 records in/out, forever;
  `WatermarkStrategy.noWatermarks()` — never marked idle).
- ~80 RocksDB instances (keyed operators × 8 subtasks) share 2.2 GB managed
  memory and one disk; each checkpoint costs a fixed 7-20 s and grows with uptime.
- Backlog → watermark minutes behind → no final candles → drill G7c FAIL.
  Correctness (no loss / no duplicates across TM kill) is proven repeatedly.

**Leading suspects (not yet verified):**
1. **Backpressure propagation throttling source fetches excessively** — the
   source is backpressured 99% while downstream operators are idle; the credit
   flow may be starving the wrong way (records queued in network buffers while
   operators starve).
2. **Idle position-state source / barrier interaction** — an input that never
   emits, never marked idle, may interact badly with credit-based flow control
   and barrier propagation.
3. **Fluss tablet serving under backlog** — untested directly in a degraded
   window; must confirm ingestion delivery rate and tablet read latency
   DURING degradation (all earlier measurements were from healthy windows).

## What Is Yet To Do

1. **Verify ingestion→Fluss delivery during a degraded window** (~10 min):
   capture `tick.throughput` and append latencies while the job is degraded.
   Splits "Fluss serving slow" from "Flink consuming slow".
2. **Measure tablet-side fetch handling during degradation**: tablet logs /
   metrics for the raw_table_1 buckets the source lags on.
3. **Test with the position-state source removed** (temporary experiment job):
   if degradation disappears, the idle-input/feedback path is the cause.
4. **RocksDB tuning experiment** (write-buffer sizing, compaction threads,
   fewer/sharper state instances) — capacity work, separately gated.
5. **Idle-marking for the position-state source** (code change, mirrors the
   raw-source watchdog fix) — candidate fix regardless of #3's outcome.
6. Re-run C2 main drill once throughput sustains ≥ feed rate at 4 Hz (short
   exposure) and then 10 Hz, to close CHG-120 Block 1 with G7c PASS.
7. Capacity ramp 4k → 15k+ with latency measurement (tick → preview/early
   signal) once the above is stable.

## Honest Caveats

- Two earlier claims of "fix validated" (overlayfs volume, checkpoint contract)
  were premature: both were real improvements to stability/hygiene, but neither
  moved the throughput ceiling. The evidence above supersedes those claims.
- The 12k/s "fresh" number is measured only over 1-2 minutes; sustaining it is
  not yet demonstrated.
- Single-machine results; production sizing (60k/s gate in DEC-045) will need
  more than this box and is explicitly out of scope for this investigation.

## Stage-throughput-latency detection result (2026-09-01 22:48–23:08)

Harness: `code/01_platform/04_scripts/stage-capture.sh` + `stage_capture_parse.py`
(unit-tested, `tests/test_stage_capture_parse.py`, 5/5). Evidence:
`logs/tracker-14/stage-capture-20260901-224849` (15 min, 5 s sampling, 10 Hz
feed = 10,240/s, cp 30 s/60 s) and `...-230629` (90 s with real
busyTimeMsPerSecond). Harness provenance: this report's stage-capture design (the standalone plan
doc was removed in the 2026-09-06 closed-plan sweep).

### Measured

| Window | Source out rate | Latency p50 (sinks) |
|---|---|---|
| Minutes 0–3 ("fresh") | **1,832/s** | 4.1–5.5 s |
| Minutes 5–15 ("degraded") | **1,689/s** | 5.8–8.2 s |

**Correction to the earlier model:** under instrumented measurement there is
NO 12k/s fresh phase and no time decay — the cap is a steady ~1.5–1.8k/s from
minute 0. The earlier "12k/s in the first 1–2 minutes" did not reproduce; it
was likely burst-peak sampling (5 s counter deltas reach 12–14k/s peaks) or a
differently-scoped measurement. Supersedes the "fresh → decay" narrative in
the earlier sections.

### Time-share per operator (90 s capture, real busyTimeMsPerSecond)

| Operator | busy ms/s | bpress ms/s | idle ms/s |
|---|---|---|---|
| Source: raw-table-1 | 1 | **999** | 0 |
| fingerprint-dedup | **478** | 404 | 118 |
| forming-bar-builder | **539** | 102 | 359 |
| candle-15s | **481** | 0 | 519 |
| candle-preview-15s | **467** | 0 | 533 |
| forming-bar-detection | 283 | 0 | 717 |
| forming-bar-writer | 282 | 0 | 718 |
| early-signal | 196 | 0 | 804 |
| all sinks + position-state | 0–1 | 0 | ~1000 |

hardBackPressured = 0 on every operator (soft backpressure only).

### Verdict (Stage E)

**The bottleneck is downstream per-record compute in the stateful mid-pipeline
chain — not the source, not Fluss, not checkpoints, not network.**

Evidence chain:
1. Source: 99.9% backpressured, ~0 busy, 0 idle — throttled by its own output
   buffer, i.e. by downstream demand; Fluss fetch healthy (5–11 ms).
2. No single saturated operator, but the chain sums to ~2.7 s busy per second
   per pipeline (per-record aggregate busy ≈ 13 ms at ~212 rec/s/subtask).
   On a 16-core box that mathematically caps sustained throughput at
   ~1.2–1.7k/s — exactly what is observed.
3. JFR (prof2/prof4): task-thread time is dominated by native
   `RocksDB.put`/`delete` — the busy time is state access, not JVM compute.
4. Ruled out again: checkpoints (zero-rate windows do not align with trigger
   timestamps; no-cp runs were slower), network buffers (hard bpress = 0;
   512 MB test no effect), disk volume (fixed 2026-09-01, no ceiling change),
   RocksDB write-stall log entries (none; 80 instances, 340 MB).

### Single fix target

Per-record RocksDB state-access cost, led by fingerprint-dedup (~2.3 ms per
record busy). First lever to test: RocksDB per-instance memory — ~80
instances share 2.2 GB managed memory (~27 MB each) → very small write
buffers → flush churn on every put. Action: raise
`state.backend.rocksdb.memory.managed` (or per-instance `write-buffer-size`
/ `max_write_buffer_number`) and re-run the capture; success = busy ms/s
drops and the steady cap rises materially. Second lever if that is not
enough: reduce state writes per record in fingerprint-dedup /
forming-bar / candle operators.

### Measurement caveats

- 5 s REST counter sampling aliases against the ~10 s metric refresh —
  per-sample rates alternate stale/fresh; only window averages are valid
  (verified: all operators freeze on the same samples; averages preserved).
- Latency percentiles are source→operator (Flink latency markers), not
  tick→table end-to-end; Stage D (preview-table timestamps) still pending.
- One instrumented run; re-run after any config change with the same harness.

## Fix validated (2026-09-01 23:24–23:34)

Change: `taskmanager.memory.managed.fraction` 0.4 → 0.6 (flink-conf, docker-compose
TM; TM process 6g → 7g). Managed RocksDB pool 2.42 GB → 3.63 GB (~45 MB/instance).
Pinned by RuntimeOptionsTest (fraction 0.6, 11/11). Correction discovered en
route: `TASK_MANAGER_MEMORY_MANAGED_SIZE` env is an embedded/local-only
passthrough — in cluster mode it does NOT resize the TM pool; the flink-conf
fraction is authoritative (documented in compose + SignalJob.java).

Same capture harness, same 10 Hz feed (10,240/s), evidence
`logs/tracker-14/stage-capture-20260901-232436`:

| Metric | Before (0.4) | After (0.6) |
|---|---|---|
| Source out rate | ~1,690/s (16%) | **~9,950/s (97%)** |
| fingerprint-dedup busy | 478 ms/s | 227–285 ms/s |
| forming-bar-builder busy | 539 ms/s | 147–183 ms/s |
| Source time-share | 999 ms/s backpressured | 586–733 ms/s idle (caught up) |
| Latency p50 (candle sinks) | 4.1–5.5 s | 0.13–0.24 s |
| Latency p95 (candle sinks) | 10.6–14.7 s | ~2.2 s |

Root cause CONFIRMED: RocksDB write-buffer/memory starvation from the 0.4
managed fraction (~25 MB/instance across ~80 instances). Restoring per-instance
headroom removed the per-record put cost (~2.3 ms → ~1.1 ms at fingerprint).
Throughput is now feed-bound, not pipeline-bound, at 10 Hz.

Next: C2 main drill (G7c) at 10 Hz under the fixed memory; then capacity ramp
toward 15k/s (feed-rate increase, harness re-measure).


## Resolution (2026-09-02)

Two root causes were isolated with evidence (not the initially suspected
ingestion side):

1. **Watermark idleness starved window closing under backpressure** — the
   `withIdleness` clock pauses when backpressured, so windows never closed
   and the pipeline ground to ~900-3,000/s. Fixed by the wall-clock
   idle-marking watchdog (`SourceIdleWatchdogGenerator`, CHG-120
   zero-emission fix; `SourceIdleWatchdogGeneratorTest` 7→12).
2. **RocksDB managed-memory fraction mis-sizing** — corrected 0.4→0.6
   with the fix validated at **9,950/s sustained** at the full 10,240/s
   feed (1024 tokens), overlayfs + checkpoint-contract hardening alongside.

Closure evidence: main-mode C2 drill `tm-kill-full-load-20260902-121511`
PASS (G7c 33,792 windows / 0 mismatches; F4 settlement 0 orphans;
single kill-induced restart, recovery 52s; drain gate completed). Full
history: CHG-120 + CHG-121 change records; five-fix saga incl. the
event-horizon checker fix documented in CHG-121.

**Still open (moved to backlog):** the >15,000 ticks/s target — the
faketool feed is the current bound at ~10,240/s; a capacity ramp
(increased rate / 3072 tokens) has not been attempted. Also open: preview
p95 tail (58s burst post-kill, expected catch-up) and Stage-D
broker→table end-to-end latency measurement.
