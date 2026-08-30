# System Quality Lever Map — Latency Tail Investigation and Beyond

> **Date:** 2026-08-30
> **Status:** Active investigation — GC-telemetry run in progress
> **Owner:** measurement→attribute→fix loop (tracker-14 evidence chain)
> **Prerequisite reading:** `2026-08-29-low-latency-candles-plan.md` (the feature),
> `performance-audit-2026-08-26.md` (the audit that started this)

---

## 1. What this document is

A complete map of every lever for making the system **truly low latency,
high throughput, and low memory** — organized by goal, so work can be
batched (one instrumentation upgrade → one run → many answers) instead of
played one-by-one. It started as the latency-tail investigation map
(~90-second bursts) and was extended 2026-08-30 to cover the full quality
goal, because several efficiency levers (serialization, allocator churn,
state backend tuning) were known but undocumented.

**What this document is NOT:** a commitment to do all of it. Items are
ordered by information value; stop when the goal is met — the requirement
is p95 < 1 s at production tick rates with headroom, not a perfect system.

---

## 2. Context — where we are (2026-08-30)

### 2.1 The feature and the requirement

The pipeline ingests ticks (fake broker @ 10 Hz × 1024 instruments =
10,240 ticks/s), computes 15-second candles with **low-latency previews**
(1 s refresh cadence), and emits early trading signals (TENTATIVE →
CONFIRM/CANCEL). The user requirement: **end-to-end latency broker→feature
table < 1 second**.

Current status vs that requirement (wm500 run, 2026-08-30):

| Metric | Value | Verdict |
|---|---|---|
| p50 | 340 ms | ✅ |
| p95 | 2,766 ms | ❌ (tail) |
| p99 | 4,400 ms | ❌ (tail) |
| Throughput | source 10,127/s, zero loss, ~2.5× headroom | ✅ |

### 2.2 The single-timeline rule (user decision, do not revisit)

Preview AND final candles share ONE watermark setting
(`WATERMARK_OUT_OF_ORDER_MS=500` since the 2026-08-30 fix, default changed
in `SignalJobConfig.java`). Late-beyond-wait ticks are excluded from BOTH
equally. This guarantees live decisions and backtests see the same tick
set. The "previews act now / finals correct later" split was rejected as a
false-backtesting risk. Revisit the 500ms value ONLY with measured
real-feed lateness percentiles — never guess upward.

### 2.3 The mystery: ~90-second latency bursts

In the wm500 evidence run, latency spikes of 4–6 s occur every ~88–102 s
(measured at t+110, 200, 299, 401, 489, 580, 669, 760, 850 into the run).
During a burst, ALL preview rows spike together — but no operator is
individually slow.

### 2.4 What has been RULED OUT (with evidence, wm500 run)

| Suspect | Evidence against |
|---|---|
| Flink operator compute | busy/idle metrics steady through bursts — no operator stalls, max busy 38% |
| Checkpoint barriers | checkpoints run every 10 s (2.6–7.8 s duration); bursts are ~90 s apart |
| Fluss tablet server | tablet log nearly silent during the run window |
| ActiveSignal log flooding | steady ~800 lines/min, no burst correlation |
| Harness metric polling | 15 s cadence ≠ 90 s bursts |

### 2.5 What the current GC-telemetry run will answer

Instrumentation added 2026-08-30 (three harness bugs found and fixed along
the way — see §6):

- **TM JVM GC logging** via `FLINK_ENV_JAVA_OPTS` in docker-compose (⚠
  see §6.3 — setting this env var has a non-obvious failure mode)
- **Ingestion JVM GC logging** (`-Xlog:gc*,safepoint` in pipeline-lib.sh)
- **Live checkpoint history capture** (REST `/jobs/{id}/checkpoints`
  history is TRIMMED after job cancel — live polling is the only reliable
  record)
- **Burst attribution analysis** in `holistic-analyze.py`: burst seconds
  (p95>2s per 1s slice) cross-checked against GC pauses (±3 s) and slow
  checkpoints (±8 s), prints a machine verdict

The run in progress (1-min smoke + 10-min main, ~6-7 burst cycles) either
names GC/checkpoints as the culprit or eliminates both — either outcome
narrows the field.

### 2.6 Investigation scoreboard (updated 2026-08-30, post A1/A2/A3/A5 batch)

The ~90 s burst CAUSE is now **attributed: host disk saturation** (A2:
9/9 burst-aligned intervals >80% busy). The remaining open question is
WHICH process saturates the disk (A2b run in flight; tablet/tiering is the
prime suspect per A4).

| Suspect | Verdict | Evidence (runs 035546, 131315) |
|---|---|---|
| GC — TM JVM | ❌ ruled out | 424 pauses, max 25 ms, none ≥100 ms |
| GC — ingestion JVM | ❌ ruled out | 70 pauses, max 16 ms |
| Slow checkpoints | ❌ ruled out | 0 slow checkpoints across both runs |
| CPU throttling | ❌ ruled out | no CPU quota exists (nr_periods=0) |
| Ingestion stages | ❌ ruled out | all 7 stages <50 ms p99 end-to-end |
| RocksDB flush/compaction | ❌ ruled out | memtables max 1.1 MB, 0/10 flushes near bursts |
| Network/transport | ❌ (indirect) | ipc/routing p99 0–4 ms inside ingestion stages |
| **Host disk saturation** | ✅ **ROOT CAUSE FOUND & FIXED (2026-08-30)** | Fluss tablet's LogTieringTask copies rolled segments to remote.data.dir — on this single-NVMe dev box = read+write same disk at 150–200 MB/s (3/3 spikes burst-aligned, run 145108). Fix: `remote.log.task-interval-duration: 0s` (bench-only, via FLUSS_REMOTE_LOG_TASK_INTERVAL env) + 17 GB remote-data purged. First post-fix run: **0 bursts, 0 tiering copies, 0 await spikes, tablet reads 200→0.1 MB/s** |
| Which process writes | ✅ **NAMED: Fluss tablet (tiering)** | A2b v3 (cgroup io.stat) named it definitively. TM writes 17 MB/s max remain (checkpoints) — secondary, not burst-aligned post-fix |
| Run-2 preview-volume anomaly | ✅ **EXPLAINED & FIXED** | Not swap: Fluss TTL is CALENDAR-DAY based, so the preview table's "60s TTL" never expired same-day data. 11.6M accumulated live KV keys slowed upserts progressively across runs (722k→63k→15k→0 preview rows) AND starved the analyzer's 30s from-earliest read. Fix: `pipeline_purge_preview_table` (drop+recreate at run start, pipeline-lib.sh) |
| **Assertive guard** | ✅ **G6 added & proven (2026-08-30)** | holistic-analyze.py G6a (p95 ≤ 1000ms), G6b (no burst second p95 > 3s), G6c (no tablet read-storm burst alignment). Non-zero exit on failure; `LATENCY_GUARD_OFF=1` escape hatch. Proven on bug-injected copy: 10× latency injection → G6a+G6b fire, rc=1; broken measurement → fires; guard-off → suppressed |
| Production relevance | ℹ️ | Dev-box artifact largely: prod tiering targets R2 over network (no local-disk copy). Residual lessons transfer: KV table growth discipline (calendar-day TTL), preview purge between bench runs, guard thresholds |
| **Verify-2 result (2026-08-30 16:25)** | 🔶 **tiering fix held; SECOND cause exposed** | Tiering: 0 copies, tablet reads 200→38 MB/s ✅. Purge: preview emission restored 15k→710k rows, end staleness 892ms ✅. **But bursts returned WITH full preview volume** (39 burst-s, p95=2,523ms) — G6a+G6b fired (first live catch). New fingerprint: tablet WRITE spikes 17 MB/s, **10/11 burst-aligned** → suspect the preview KV write path itself (RocksDB flush/compaction on tablet, or KV changelog) stalls under full upsert volume. Cross-check: verify-1 had crippled previews (15k) and ZERO bursts — bursts track preview volume, not tiering |
| Guard propagation bug | ✅ FIXED | `python3 ... \| tee` swallowed the analyzer's non-zero exit (run printed GUARD FAILED but exited 0). Fixed with `set -o pipefail` + explicit rc check → run now exits non-zero on guard failure |
| **Checkpoint = 3rd cause (2026-08-30 17:35)** | ✅ CONFIRMED & FIXED | Two harness bugs found first: checkpoints.jsonl dedupe collapsed 70→1 line (awk `$1` identical on all JSON lines) + analyzer units bug (epoch vs offset — 0/70 overlap was impossible, corrected 43/70). 76/85 bursts at +2–4s after checkpoint trigger (inside its ~2.5s window). Fix: CHECKPOINT_INTERVAL_MS parameterized; 10s→60s cut bursts 58→3, p95 2857→1120ms. 120s gained nothing (1,150ms) — floor reached |
| **Latency floor law (2026-08-30 18:27)** | ✅ MEASURED | With stalls gone, e2e p95 ≈ emission-interval + fixed pipeline cost (~200ms): 1s interval → 1,150ms; 2s → 2,215ms. Timer-driven preview makes p95 < interval structurally impossible |
| **FINAL RESULT (2026-08-30 18:47)** | ✅ **TARGET MET: p95=740ms** | PREVIEW_INTERVAL_MS=500 + CHECKPOINT_INTERVAL_MS=60000: p50=346 / **p95=740 / p99=2396**, staleness 414ms, 2.87M rows. G6a passes. G6b still fires: 10 burst-seconds ≈ every 60s at 3–3.6s = the per-checkpoint emission freeze (~3s each) — next lever if p99 matters: unaligned checkpoints |

**Status legend:** ✅ done/verified · 🔶 partial/in-flight · ❌ not started ·
(deprioritized marks explain why)

---

## 3. The lever list (the point of this document)

### Group A — burst root-cause levers (current hunt)

If the GC run doesn't fully explain the bursts:

| # | Lever | How to measure | What it decides |
|---|---|---|---|
| A1 | **RocksDB compaction** | RocksDB stats in TM metrics (already exposed via Prometheus reporter on :9249 — we never looked); compaction stall counters | If compaction pauses → tune compaction style / state backend config | ✅ **RESOLVED 2026-08-30 (run 131315): NOT the cause.** Memtables max 1.1 MB, flushes ~1 MB every ~32 s, 0/10 flush events near a burst — far too small to saturate NVMe. A1 sampler + analyzer wired into harness (tm-prom-rocksdb.tsv). |
| A2 | **Host disk I/O** | `iostat -x` sample loop during run (add to harness, 5 s cadence, same as metrics) | If disk saturation → move checkpoint/RocksDB dirs to faster disk or reduce write amplification | 🔶 **STRONG SUSPECT (2 runs): disk saturation correlates — run 131315: 9/9 aligned; run 133729: 1/2 aligned with bursts 4× fewer.** Sampler v2 wired (busy% + await ms/op + queue depth). A2b points at Flink TM as heaviest writer (15–17 MB/s spikes). Awaiting 2-run v2 confirmation. |
| A3 | **Ingestion writer flush cycles** | Fluss writer metrics (flush latency, pending bytes) via existing OTEL pipeline — may just need the metric names | If flush stalls → writer batch/linger tuning | ✅ **RESOLVED 2026-08-30 (run 131315): NOT the cause.** All 7 staged latencies pristine (decode p99 14 ms, fluss_ack 14 ms, end-to-end 29 ms); ticks reach Fluss in <50 ms p99. Payload→java.out bench mode wired (dead collector port). Ingestion fully exonerated. |
| A4 | **Fluss tablet timing** (kv snapshot / log segment roll / tiering upload) | Tablet server metrics + log timestamps — currently only inferred from quiet logs | If tablet cycles → table config (snapshot interval, segment size) | 🔶 WEAKENED (run 133729): tablet writes max 3.7 MB/s — unlikely to saturate NVMe alone. minio (tiering destination) still unsampled; A2b v2 covers it. |
| A5 | **Docker/host CPU scheduling** | Container CPU throttling counters (`/sys/fs/cgroup` cpu.stat) sampled during run | If throttled → CPU limits in compose or pin CPUs | ✅ **RESOLVED 2026-08-30: NOT the cause.** nr_periods=0 — no CPU quota configured on the TM; CFS throttling structurally impossible. Sampler wired (tm-throttle.tsv). |
| A6 | **Network jitter** | Bridge send timestamps vs ingestion receive timestamps (clock-stamp in proto if missing) | If network → bridge/network config | ❌ Not started — **deprioritized**: A3 staged latencies (ipc/routing ~0–4 ms) already show the host transport path is clean. Only revisit if A2b verdict contradicts. |

**Batching move (the "instead of one by one" decision):** A1 + A2 + A5 are
pure harness additions — no production code changes. ONE upgrade, ONE run,
all three answered together. A3 needs a small metric export; A4/A6 are
progressively more invasive. Do A1+A2+A5 as a batch before touching
anything invasive.

### Group B — latency levers beyond bursts (after p95 is fixed)

| # | Lever | Status |
|---|---|---|
| B1 | Watermark wait (500 ms) | ✅ Done 2026-08-30 (p95 5.7s→2.8s). Revisit only with real feed |
| B2 | Preview interval (1 s) | Already at REQ-FC-002 spec; lower = more rows, not faster decisions |
| B3 | Checkpoint interval (10 s, 3–4 s duration — barriers in flight 30–40% of the time) | ✅ **RESOLVED 2026-08-30: checkpoints ruled out as burst cause** (0 slow checkpoints across 2 instrumented runs). Interval tuning remains a general lever but is NOT needed for the tail. |
| B4 | Operator parallelism | Currently 8 source parallelism / mostly 1 elsewhere, 1 TM × 10 slots. Headroom exists; only if throughput grows |

### Group C — production-readiness levers (separate workstream, all open)

| # | Lever | What's missing |
|---|---|---|
| C1 | **Real-feed measurement** | Tick lateness percentiles on real broker → set watermark to just above p99.9 (provisional 500 ms gets re-validated) |
| C2 | **Failover/recovery test** | Kill TM mid-run at full load → preview correctness during recovery? recovery time? data gap? |
| C3 | **Long soak (hours)** | State growth, memory leaks, RocksDB degradation over time — 15-min runs cannot see this |
| C4 | **Backtest-parity check** | Mechanical test that preview candle and final candle saw identical tick sets on a recorded run (single-timeline rule deserves a test, not just design intent) |
| C5 | **KV point-lookup flakiness** | Known intermittent Fluss 0.9.1 issue — file upstream or work around; currently untracked |
| C6 | **Capacity ceiling** | At what tick rate does it break? Current headroom 2.5× measured, but no explicit breaking-point test |

### Group D — memory & CPU efficiency levers (low-memory goal)

These reduce allocation, serialization, or state cost. None is urgent at
current load (operators ≤38% busy), but together they define how far the
system scales on the same hardware before needing more machines.

| # | Lever | Evidence / how to check | Expected effect |
|---|---|---|---|
| D1 | **POJO serialization for CandleAccumulator** | ⚠ OBSERVED in JM logs: `CandleAccumulator is not public so it cannot be used as a POJO type and must be processed as GenericType` (logged every job submit). GenericType = Kryo serialization = slower + larger state. Fix: make the class public with public fields. | Less CPU per record, smaller RocksDB state, cheaper checkpoints |
| D2 | **GC tuning / collector choice** | GC logs now captured (2026-08-30). If pauses are the burst cause: G1 tuning (region size, pause-target) or switch collector (ZGC/shenandoah = sub-ms pauses, some throughput cost) | ✅ **MEASURED 2026-08-30 (2 runs): GC is healthy and NOT the burst cause.** TM: 424 pauses, max 25 ms; ingestion: 70 pauses, max 16 ms. No tuning needed at current load; GC logging stays in harness as a standing baseline. |
| D3 | **Operator object-churn audit** | Flink metrics: `numRecordsOutPerSecond` already tracked; add allocation profiling (JFR on TM for 60 s during a run) if GC frequency looks allocation-driven | Lower GC frequency = fewer pauses |
| D4 | **RocksDB state tuning** | Default block cache/write-buffer sizes on a 2.2 GB heap TM; check `state.backend.rocksdb.memory` config vs actual state size (candles state = 1024 tokens × open window) | Less memory pressure, fewer compactions |
| D5 | **Preview row volume** | 1,013 preview rows/s at 1 s cadence × 1024 tokens — the preview table grows ~87M rows/day at test rates. Consider TTL/compaction policy on the preview table if it is query-only for live decisions | Bounds storage + read amplification |
| D6 | **Ingestion JVM footprint** | 2 g heap + 1 g direct, but idle-processing load is light; measure actual live-set during a run (GC logs now show it) — right-size before production replicas | 🔶 Partial: GC logging wired (gc.log in evidence every run); live-set analysis not yet done — one offline pass over captured gc.logs, no new run needed. |
| D7 | **Fluss writer batching** (relates to A3) | `FLUSS_WRITER_BATCH_SIZE_BYTES=0` = every tick is its own append (lowest latency, highest RPC overhead). At production rates this trade may flip — measure RPC count/s | Throughput headroom at modest latency cost |

### Group E — throughput scaling levers (high-throughput goal)

Current: 10,127 events/s with ~2.5× headroom measured (source idle ~75%).
If production rate grows (more instruments, higher Hz, more tables):

| # | Lever | Trigger condition |
|---|---|---|
| E1 | **Source parallelism** (currently 8; matches raw_table_1 buckets) | Source busy-time consistently >60% |
| E2 | **Operator parallelism beyond 1** (candles, preview, signal currently parallelism 1) | When a single operator's busy-time saturates; requires key-by already in place (it is) |
| E3 | **Multiple TMs / slots** (1 TM × 10 slots now; 152 tasks already run on it) | CPU throttling observed (A5) or slot exhaustion |
| E4 | **Network buffers** (`taskmanager.memory.network.*` currently 128 MB) | Backpressure originating at network exchange, not compute (check backpressure metrics per vertex) |
| E5 | **Fluss table bucket count** (raw_table_1 buckets = 8, fixing max source parallelism) | When E1 is capped at 8 and still saturated — needs table recreate, plan carefully |

### Group F — data quality levers (is the data right?)

Distinct from performance: these verify the pipeline produces *correct*
data, not just fast data. A wrong candle that arrives in 300 ms is worse
than a right one that arrives in 2 s.

| # | Lever | Current state | What's missing |
|---|---|---|---|
| F1 | **Zero-loss verification** (tick in broker → tick in raw table) | ✅ Strong: from-earliest LOG reads + source throughput vs input rate already cross-checked in every measurement run | Automate as a standing assertion on production feed (not just test harness) |
| F2 | **Dedup correctness** (dedup operator drops only true duplicates) | Dedup operator exists (`DEDUP_TTL_MS=60000`); never independently audited | A/B test: inject known duplicates via faketool, assert exactly those are dropped |
| F3 | **Late-tick accounting** (ticks dropped beyond watermark wait) | Mechanism exists (single-timeline rule); dropped-tick count is NOT measured | Add a counter: late-beyond-wait ticks per window — the number that defines backtest/live parity loss |
| F4 | **Signal settlement correctness** (TENTATIVE→CONFIRM/CANCEL) | ✅ Measured per run (settlement balance 8100/8049 in latest run, remainder = open windows, expected) | Assert no TENTATIVE is orphaned after grace period |
| F5 | **Preview↔final candle parity** | Design intent (single-timeline rule); overlaps lever C4 | Mechanical test: same window's preview final row == final candle row, field by field |
| F6 | **Tick sanity/validation** (bad prices, zero volume, crossed markets) | `raw-validation` operator exists; validation rules coverage unreviewed | Review rules; add rejection counters (how many ticks rejected, by which rule) |
| F7 | **Schema contract enforcement** | ✅ `TableContractValidator` fails closed at job start (schema v2, 15 cols) | Broker-side schema drift is not covered — manifest fingerprint is the natural gate (see G3) |
| F8 | **Clock/ordering sanity** (event-time monotonicity per instrument) | `ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000` guards future skew; backward disorder handled by watermark | A periodic assertion that per-token event time never jumps backward beyond the watermark budget |

### Group G — data observability levers (can we SEE the data's health?)

Infrastructure observability (Prometheus/OpenObserve) covers the
*pipeline's* health. Data observability covers the *data's* health —
volumes, freshness, distributions, schema — so degradation is caught by
alerts, not by a downstream trading decision going wrong.

| # | Lever | Current state | What's missing |
|---|---|---|---|
| G1 | **Volume monitoring** (events/s per table, per instrument) | Measured in test harness only | Continuous production monitor + alert on rate drop (e.g. >50% below rolling median = broker feed problem) |
| G2 | **Freshness monitoring** (age of newest row per table) | Freshness measured per run (p50=841ms staleness at run end) | Continuous freshness SLO with alert (e.g. alert if newest preview > 5 s old) |
| G3 | **Schema/broker drift detection** | ⚠ OBSERVED: ingestion logs `bridge manifest_fingerprint mismatch` WARNs — the signal exists but nothing acts on it | Escalate fingerprint mismatch to a hard alert (instrument set changed = silent data gap) |
| G4 | **Distribution drift** (price/volume ranges per instrument) | Nothing — candles computed but never profiled | Nightly job: per-instrument min/max/mean vs trailing baseline; flag breakouts as possible bad ticks |
| G5 | **Table-level health** (row counts, partition/bucket balance) | Ad-hoc via LOG reads | Scheduled check: expected-rows vs actual-rows per window (ties into F1) |
| G6 | **Alert routing** (who gets told, how fast) | Nothing production-facing | Even a single webhook/email on F1/F3/G1/G2/G3 violations is enough for now |
| G7 | **Runbook per alert** (what to DO when it fires) | Partial (docs/06_operations) | One paragraph per observable failure mode: feed loss, staleness, drift, orphaned signals |

**Design principle for F/G:** prefer levers that reuse evidence already
being produced (LOG reads, manifest fingerprints, settlement balances,
freshness numbers) — most of these are alerts wired to existing signals,
not new systems.

---

## 4. Recommended sequencing

1. **Now:** GC-telemetry run verdict (in progress)
2. ~~**Next batch:** A1 + A2 + A5 harness upgrade → one run answers all three~~ **DONE 2026-08-30** — A1/A2/A3/A5 all answered in one run (see scoreboard); disk saturation confirmed as the cause
3. **Then (NEXT INVESTIGATION):** 2 consecutive 10-min runs with v2 samplers
   (fixed distroless coverage + disk await/queue depth). Decision matrix:
   - TM spikes + disk await high + burst-aligned → fix = checkpoint/state
     write pacing (unaligned checkpoints, `state.backend.rocksdb`
     write-buffer throttling, or async checkpoint tuning), then assertive guard
   - minio/openobserve revealed as the writer → isolate their volumes
     (separate disk / tmpfs) or throttle tiering upload cadence
   - disk clean but bursts persist → the tail is NOT disk; next suspect is
     Flink network-buffer credit recursion (E4) measured via backpressure
     trace (`/jobs/{id}/backpressure`), or accept tail as faketool-artifact
     (validate on real feed, C1)
   - Then: one targeted fix + verification run + assertive guard (the
     analyzer's disk/burst correlation verdict becomes a FAILING exit once
     the fix lands — regression protection)
4. **Cheap and confirmed-broken:** D1 (POJO fix) — the JM log literally
   flags it every submit; one-line visibility change + measurement A/B
5. **Parallel-safe workstream:** C2, C3, C4 are correctness tests — each one
   run, no new instrumentation, can be done any time
6. **Before production feed:** G3 (act on the fingerprint mismatch warnings
   already appearing in logs) and F3 (late-tick counter) — both are cheap
   and directly protect trading decisions; F1/G1/G2 become production
   monitors at the same time
7. **When load grows:** Group E (scaling) — not before; measuring now would
   be premature optimization
8. **Blocked on real feed:** C1, C6 final numbers; D7's batching tradeoff
   also needs real-rate data

### Efficiency principles (why Groups D/E are ordered last)

- No operator is compute-bound today (max 38% busy) — CPU work is not the
  bottleneck, so serialization/allocator wins (D1, D3) are headroom, not
  relief.
- Memory pressure only matters if it produces pauses — which is exactly
  what the GC run will tell us. D2/D4 follow the verdict, not precede it.
- Scale-out (E) multiplies cost; efficiency (D) reduces it; both only
  matter when the measured load demands them. The 2.5× headroom means
  neither is urgent at test rates.

## 5. Where the evidence lives

- `logs/tracker-14/holistic-measure-20260830-014108` — 5 s watermark baseline (KEEP)
- `logs/tracker-14/holistic-measure-20260830-021926` — v2 instrumented baseline (last_event_ts)
- `logs/tracker-14/holistic-measure-20260830-030012` — wm500 run, current best numbers + burst analysis source
- `logs/tracker-14/loadtest-20260829-192648` — committed 18/18 verification evidence (KEEP)
- Harness: `code/01_platform/04_scripts/` — `pipeline-lib.sh` (shared),
  `holistic-measure.sh` (gate+measure), `holistic-analyze.py` (report+burst
  attribution), `test-pipeline-lib.sh` (19 guards)

## 6. Hard-won gotchas (do not rediscover these)

1. **String constants inline at compile time** — changing `ROW_SCHEMA_VERSION` in common requires `mvn -pl common install` + **clean** rebuild of compute.
2. **REST checkpoint history is trimmed after job cancel** — only ~10 of ~90 entries survive; poll live during runs (now guarded by `capture_checkpoint_history`).
3. **`FLINK_ENV_JAVA_OPTS` replaces image defaults** — Flink's `config.sh` only reads `env.java.opts.all` from config.yaml if `FLINK_ENV_JAVA_OPTS` is UNSET. Setting it (e.g. for GC logging) silently drops the `--add-opens` flags Fluss's Arrow deserialization requires → every source subtask dies with `ExceptionInInitializerError: MemoryUtil`. The compose file now carries the full default flag set + GC logging, with a warning comment.
4. **Compose YAML merge keys replace whole maps** — a service-level `environment:` block does NOT merge with the `x-flink-common` anchor's environment; it replaces it. Use `<<: *flink-common-env` (anchor now defined on the env map itself).
5. **Job submit races TM registration** — after the hygiene TM restart, the TM needs up to ~60 s to re-register; submitting before that = `NoResourceAvailableException` → RESTARTING. Guarded in `pipeline-lib.sh` (B5: poll `/taskmanagers`, assert non-empty array — note the response has no `numRegisteredTMs` field).
6. **LogScanner double-delivery** — every Fluss log row appears twice in from-earliest reads; always dedupe.
7. **Event-time vs wall-clock** — signal rows carry event-time stamps (eval_ts=window_end), NOT wall-clock; preview `output_ts` IS wall-clock. Latency = `output_ts − last_event_ts` per row (schema v2).
8. **Never pipe loadtest scripts through `grep | head`** — SIGPIPE kills the pipeline; use bg_run with output redirect.
9. **`docker compose` always needs `--env-file .env --env-file secrets.env`.**
10. **Standing rule:** every fixed bug gets a mechanical guard so it cannot recur (comment-only fixes are not fixes).

### 6.1 Observed-but-unfixed warnings (candidates, not confirmed problems)

- `CandleAccumulator` GenericType serialization warning (→ lever D1) —
  logged by the JobManager on every job submit since at least 2026-08-29.
- Fluss `NettyServerHandler` idle-connection warnings on tablet teardown —
  cosmetic so far, watch for recurrence under load.
- Faketool logged 653 `non-json frame` events during the wm500 run — the
  bridge sends proto frames on the sub channel; harmless at current scale
  but worth understanding before production (protocol audit).

## 7. The loop (methodology)

Every question in this document is worked the same way:

```
measure (harness run with the right instrumentation)
  → attribute (correlate evidence: bursts vs GC vs checkpoints vs I/O)
  → fix (smallest config/code change that addresses the attributed cause)
  → guard (mechanical assertion so the bug cannot recur)
  → re-measure (verify the number moved, nothing else regressed)
```

Verification ≠ measurement: `loadtest-preview.sh` fails fast (gate);
`holistic-measure.sh` keeps collecting (measurement). Smoke must pass
before any main run (user rule, enforced in the scripts).
