# System Quality Lever Map — Latency Tail Investigation and Beyond

> **Date:** 2026-08-30 (levers refreshed 2026-08-31 PM post-lake-migration)
> **Status:** Target MET (p95 = 740 ms) — investigation closed 2026-08-30;
> this map now tracks follow-up levers only
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

Current status vs that requirement (final combo run 2026-08-30 18:47,
PREVIEW_INTERVAL_MS=500 + CHECKPOINT_INTERVAL_MS=60000 — now the
pipeline-lib.sh defaults):

| Metric | Value | Verdict |
|---|---|---|
| p50 | 346 ms | ✅ |
| **p95** | **740 ms** | ✅ **TARGET MET** |
| p99 | 2,396 ms | 🔶 accepted (per-checkpoint emission freeze ~3 s, documented) |
| Throughput | source 10,127/s, zero loss, ~2.5× headroom | ✅ |

Baseline re-run with the new defaults (no env overrides): p95 = 685 ms —
the defaults hold without per-run tuning. Full latency picture:
ingestion broker→raw table ~15–50 ms p99; preview path p50 346 / p95 740 /
p99 2,396 ms; final candle window-close→committed p50 1,164 / p95 1,260 ms
(by design: 500 ms out-of-order allowance + ~500 ms watermark advance +
write; tunable via watermark if ever needed).

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

### 2.5 What the GC-telemetry run answered (closed 2026-08-30)

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

Verdict: GC eliminated (TM max pause 25 ms, ingestion 16 ms). The run's
instrumentation (GC logs, live checkpoint capture, burst attribution in the
analyzer) stays in the harness as standing baseline. The burst hunt
continued through A1–A5 and closed with three root causes — see the
scoreboard.

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
| **Defaults encoded (2026-08-30 19:0x)** | ✅ COMMITTED (9274ad8) | pipeline-lib.sh defaults now PREVIEW_INTERVAL_MS:-500, CHECKPOINT_INTERVAL_MS:-60000; env vars override. Guards 19/19 pass. Baseline re-run with bare defaults: p95=685ms |
| **G6b redesign (2026-08-30)** | ✅ COMMITTED | Flat 3s/4s thresholds nagged on the documented checkpoint tail. Final design: exempt checkpoint-window seconds [trigger−1s, end+6s] (measured post-end lag 0.5–5.0s + 1s bucket rounding). Guard catches NEW stall types; the known checkpoint tail is G6a's job. Also fixed: analyzer live re-read was overwriting the rows evidence file (two runs' evidence lost that way) |
| **Final-candle path (2026-08-30)** | ✅ MEASURED (dd17230) | Analyzer v-final-candle: window-close→committed p50 1,164 / p95 1,260 / p99 1,275 ms (n=48,128). output_ts is field index **13** of 15 (index 12 is config_version) |
| **State-recovery drill (2026-08-30)** | ✅ **ZERO DATA LOSS** | Savepoint (11 MB, dedup 1,063,976 firsts/0 dups) → stop → restore → job RUNNING → dedup preserved (831,998 firsts/0 dups), post-restore checkpoint 91 MB. Full final-table continuity read: **1024/1024 candles in every 15s window across the restart**. 3 rollout-savepoint.sh bugs found by the live drill (compose wrapper missing --env-file pair; empty JOB_ID → rc=22; pipefail+empty-sed → rc=1) — fixed in 72c9743. Lesson: recovery tooling must be drilled live, not read |
| **EOD controller test (2026-08-30 22:13)** | ✅ 8/8 PASS (6c48efd) | 6 guards (fail-closed offload=none, lease fencing rc=5, clean-state no-ops, idempotency, reconcile stability, purge hygiene) + 120s smoke + 600s main cycle via mock executor. Encodes measured semantics: leases persist in eod_offload_state across process death (purge between subtests); 2nd run on VERIFIED day = no-op. Real MinIO/R2 offload still untested (needs sidecars + lake tiering session) |
| **F2/F3/F5 data-quality audit (2026-08-31)** | ✅ **ALL EXACT (329560f)** | Live A/B injection: smoke gate 200/200 dups + 20/20 lates dropped exactly; full run 1,200 dups / 120 lates all in raw, counters exact within the sampling window (round 6 fired post-teardown — expected, now capped at 4 rounds). G7c parity: full raw recount (7.53M rows, audit-mode sanitized reader) vs 47,104 (token,window) final candles — **0 mismatches**; LATEST-mode startup skip tolerated by design. Guards proven against a bug-injected evidence copy: all 3 tamper classes fired, G7c named the exact damaged window. 4 harness bugs found: binary `payload_hash` column broke line parsing (~23% rows silently dropped — false data-loss alarm), stale LogFullRead.class ignored the new audit mode, counter-vs-sent comparison ignored the sampling window, post-teardown injection round |
| **G3 fingerprint drift (2026-08-31)** | ✅ **NATIVE FIX + GATE** | Root cause of the long-observed `manifest_fingerprint mismatch` WARNs: NOT broker drift — the bench handed the bridge 1,024 tokens (env) while Java loaded the full 2,431-row CSV (manifest path). Two configs for one set = guaranteed false alarm on every bridge event. Native fix (no patchwork): Java's `startBridge` now writes the loaded manifest set into the child's `ARROW_INSTRUMENT_TOKENS` — the bridge's own env-override path consumes it, so both sides hash the identical set by construction. Harness passes only `INSTRUMENT_MANIFEST_PATH` (the 1,024-row slice). New G8 run-gate (both phases' java.out must show zero mismatch lines) + G9/G10 guard tests (wiring + tamper-proof). Live proof: zero mismatches both phases, G6/G7 all green, p95 722ms, counter exactness now perfect (sent=raw=counter 800/800/800, 80/80/80 — the 4-round cap eliminated the post-teardown round) |
| **D6 applied + combined verify (2026-08-31)** | ✅ **512m JVM VERIFIED LIVE** | Right-sized ingestion JVM (-Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m in pipeline-lib.sh + loadtest-run.sh; G11 guard pins both scripts identical). Verify run 20260831-100737: live-set max **62 MB** (8× headroom), e2e **p50=331 / p95=689ms**, all G6/G7/G8 guards PASS — no regression from the 4× heap cut. D6 leak-alarm guard (warm-half live-set >200 MB fails the run) added to the analyzer. 2GB RAM freed for the future 3,000-instrument scale test |
| **Signal-path latency (2026-08-31)** | ✅ **FIRST MEASUREMENT** | Flink latency histograms (`metrics.latency.interval=2000` — enabled all along, never sampled) now captured per run (tm-prom-latency.tsv) and reported per operator by the analyzer. First profile: early_signal p50=139ms / p95=3.6s; signal_detection p50=136ms / p95=2.7s; sink writers p95 1.7–3.5s. Medians healthy; the tail shares the known checkpoint/GC causes (§4.8). Two guards born: F4 (orphaned-TENTATIVE after 30s grace — settlement must never silently drop) and F8 (backward event-time jumps must equal late rows — exact reconciliation 80=80) |

**Status legend:** ✅ done/verified · 🔶 partial/in-flight · ❌ not started ·
(deprioritized marks explain why)

---

## 3. The lever list (the point of this document)

### Group A — burst root-cause levers (current hunt)

All closed (hunt finished 2026-08-30 — three root causes, three fixes;
kept for the record of how each suspect was eliminated):

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
| B2 | Preview interval | ✅ **Now 500 ms (default in pipeline-lib.sh, 2026-08-30)** — this is what took p95 under 1 s (floor law: p95 ≈ interval + ~200 ms). Going lower needs event-driven emission (code change), not a smaller timer |
| B3 | Checkpoint interval | ✅ **RESOLVED & APPLIED: 60 s (default in pipeline-lib.sh)**. 10 s→60 s cut bursts 58→3, p95 2,857→1,120 ms; 120 s gained nothing. Checkpoints WERE a real cause (76/85 bursts at +2–4 s after trigger); first ruled out on the wm500 run because a harness dedupe bug collapsed 70 checkpoints to 1 |
| B4 | Operator parallelism | Currently 8 source parallelism / mostly 1 elsewhere, 1 TM × 10 slots. Headroom exists; only if throughput grows |
| B5 | p99 tail (per-checkpoint ~3 s emission freeze) | ✅ **INVESTIGATED & CLOSED (2026-08-31, 4-run series): ACCEPT the tail.** Unaligned checkpoints falsified (zero effect — backpressure ~4%, alignment ≤46 ms/task); phase gauges cleared barrier arrival (≤59 ms/task) and alignment as causes; residual = checkpoint I/O (TM's 24 MB/s state-upload bursts, once per 60 s, single-NVMe dev artifact). p95 contract met: 687–723 ms typical (1 outlier at 1,132 ms in 4 runs). Reopen only with a real p99 requirement — first lever then: disk separation (RocksDB localdir / checkpoint dir / Fluss data on different disks) |

#### B5 detailed experiment plan — p99 tail (drafted 2026-08-31; Phase 1
#### EXECUTED — results below)

Goal: cut e2e p99 (1,840 ms, run 20260831-100737) by removing the
per-checkpoint ~3 s emission freeze (7 burst-seconds per 11-min run).

**Codebase audit findings (2026-08-31) — two of the proposed levers are
already settled:**

| Item | Status | Evidence |
|---|---|---|
| RocksDB state backend | ✅ already active | compose `STATE_BACKEND: rocksdb` default; TM logs show `RocksDBKeyedStateBackend` |
| **#4 Incremental checkpoints** | ✅ **already ON** | pinned in `SignalJob.applyRuntimeOptions` (`CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true`, rocksdb branch) AND `state.backend.incremental: "true"` in compose FLINK_PROPERTIES. Checkpoint sizes 27→84→110→96 MB confirm incremental (not identical full copies) |
| **#1 Unaligned checkpoints** | ❌ OFF — implementable | `execution.checkpointing.unaligned` key verified in the pinned flink-dist-2.2.1 jar. No config/code sets it today |
| Backpressure during bursts | ~zero | run-4 evidence: max 75 ms/s (7.5%) per second, mean ~40 ms/s; **no spike at checkpoint triggers** (42 ms during ±3 s vs 37 ms otherwise) |
| Barrier alignment cost | ~zero | completed checkpoint `alignment_buffered: 0`; `processed_data` 1.6 MB (tiny) |

**Consequence — the honest hypothesis:** unaligned checkpoints only help
when barriers cannot travel because of backpressure. With ~4% backpressure
and zero alignment cost, the evidence predicts **unaligned will NOT fix
the tail**. The freeze is more likely the checkpoint **sync phase**
blocking task threads (RocksDB snapshot / memtable flush, or a sink's
barrier-triggered flush) — which unaligned does not change. This plan
therefore runs the cheap flag experiment as a *falsification test*, with
instrumentation to attribute the true mechanism if it fails.

**Phase 0 — mechanism isolation instrument (harness-only change, no job
rebuild):**

1. `capture_checkpoint_history` (pipeline-lib.sh) currently stores a slim
   projection (id/trigger/duration/size/status). Enhance it to ALSO store,
   per checkpoint, the per-task sync/async/alignment/duration breakdown
   from the REST history (`tasks` map is populated while the job is live —
   it was empty `{}` in the post-cancel evidence). New file:
   `checkpoints-detail.jsonl`.
2. Analyzer: report `sync_ms` vs `async_ms` vs `alignment_ms` p50/p95 per
   operator for completed checkpoints, and correlate burst-seconds with
   the dominant phase. This answers WHICH code path freezes emission.

**Phase 1 — the unaligned experiment (one bench run, ~27 min):**

1. `pipeline_submit_job` gains an env-gated flag:
   `UNALIGNED_CHECKPOINTS=true` → append
   `-Dexecution.checkpointing.unaligned=true` to the `flink run` command
   (same pattern as `-Dmetrics.latency.interval`; build it as a shell
   array so the backslash-continued command stays G4-clean). Default OFF —
   baseline behavior unchanged unless explicitly requested.
2. Run: `UNALIGNED_CHECKPOINTS=true SMOKE_S=150 MAIN_S=660 bash
   holistic-measure.sh`.
3. Compare against run 20260831-100737 (identical settings otherwise):

| Metric | Baseline (run-4) | Success = | If unchanged = |
|---|---|---|---|
| burst-seconds (G6b raw count) | 7 | → ~0 | alignment hypothesis falsified |
| e2e p99 | 1,840 ms | < 1,500 ms | sync-phase hypothesis confirmed |
| checkpoint end_to_end_duration p95 | ~2.9 s | small change expected | — |
| checkpoint size | 84–110 MB | may grow (in-flight buffers persisted) | watch for unbounded growth |

4. Expected outcome per the audit: bursts UNCHANGED → mechanism is the
   sync phase, close the "unaligned" lever with evidence (B5 row updated),
   and Phase 2 decides.

**Phase 2 — only if Phase 1 confirms sync-phase (decision point, needs
its own mini-plan):**

| Candidate lever | What it attacks | Cost |
|---|---|---|
| Fluss/preview sink barrier flush cost | If the detail breakdown names a sink operator's sync time | connector config investigation; no known knob yet |
| RocksDB memtable flush on snapshot (`state.backend.rocksdb.*`) | If keyed operators dominate sync time | tuning risk vs ~1×/min freeze |
| Accept the tail (status quo) | p99 = 1.8 s with p95 contract met at 689 ms | zero; B5 stays deferred |

**Risks / notes:**
- Unaligned + EXACTLY_ONCE is supported (this job's mode) — no conflict.
- With near-zero backpressure, unaligned's checkpoint-size penalty should
  also be near-zero (little in-flight data to persist) — if size DOES grow,
  that itself is diagnostic (in-flight buffers were not the visible ones).
- Guard impact: none — G6b already counts burst-seconds and is exempting
  checkpoint windows; the experiment only changes what the job does inside
  those windows.
- Both phases reuse the existing bench harness end-to-end (guards G6/G7/
  G8/F4/F8/D6 all run as usual, so the experiment run doubles as a
  regression check).

**B5 RESULTS (2026-08-31, run `holistic-measure-20260831-111021`,
UNALIGNED_CHECKPOINTS=true):**

| Metric | Baseline (run-4, aligned) | Unaligned | Verdict |
|---|---|---|---|
| e2e p50 | 331 ms | 343 ms | noise |
| e2e p95 | 689 ms | 723 ms | noise (both < 1s contract) |
| **e2e p99** | 1,840 ms | **2,058 ms** | **no improvement — lever FALSIFIED** |
| burst-seconds | 7 | 10 | unchanged mechanism |
| checkpoints overlapping bursts | — | 6/13 | still correlated |

Unaligned was confirmed ACTIVE (REST checkpoint config
`unaligned_checkpoints: true`; checkpoints recorded as
`UNALIGNED_CHECKPOINT` type) — this is a true negative, not a wiring
failure. Mechanism, exactly as the backpressure audit predicted: with
~4% backpressure and `alignment_buffered: 0` there was nothing for the
barriers to overtake. **The "unaligned" lever is CLOSED with evidence.**

The run's burst attribution answered the mechanism question as a side
effect: **the heaviest disk writer at burst time is the TaskManager
(max 24 MB/s, spikes aligned with bursts)** — bursts are the TM's
checkpoint state + sink flush hitting the single NVMe. Remaining
ambiguity: RocksDB state upload vs Fluss sink writer flush (both are TM
writes). The phase-gauge sampler added in this session
(`tm-prom-cp-phases.tsv`, checkpointStartDelayNanos /
checkpointAlignmentTime per task) resolves this on the NEXT run.

**B5 harness findings (both guarded now):**
- Flink 2.2's REST checkpoint history has an EMPTY `tasks` map for
  regular checkpoints (savepoints only) — the first phase-breakdown
  implementation read it and would have silently produced nothing.
  Fixed: TM Prometheus vertex gauges; analyzer FAILS the run when
  `B5_EXPECT_PHASES=1` and the gauge file is absent/empty.
- G12 guard suite (test-pipeline-lib.sh, 4 checks): unaligned flag wiring
  present, defaults OFF, phase sampler present, analyzer fail-fast
  present. 29/29 guards pass.
- Editing a shell script while its process runs corrupts execution (bash
  re-reads the shifted file at the old byte offset — this run's measure
  script died at the final analysis step with a spurious syntax error).
  Evidence was intact; the analyzer was run manually. **Rule: never edit
  harness scripts mid-run.** (Gotcha #24.)

**B5 FINAL VERDICT (2026-08-31, four-run series): CLOSED — accept the
tail.**

The confirmation runs executed (phase gauges active, baseline config):

| Run | e2e p50 | e2e p95 | e2e p99 | Result |
|---|---|---|---|---|
| Baseline (100737) | 331 ms | 689 ms | 1,840 ms | PASS |
| Unaligned (111021) | 343 ms | 723 ms | 2,058 ms | PASS (lever dead) |
| Phases (114311) | 348 ms | **1,132 ms** | 2,607 ms | G6a FAILED |
| Stability re-run (12xxxx) | 335 ms | **687 ms** | 1,903 ms | PASS |

- **p95 contract: MET, typical ~690 ms.** The 1,132 ms run is a one-off
  outlier in four (disk busy 79% that run vs 72% on the re-run — a
  host-level hiccup, not stack degradation: the immediate re-run at
  identical config returned to 687 ms). Run-to-run p95 variance on this
  single-host dev box is real and worth knowing: quote "p95 ≈ 700 ms
  (worst observed 1.1 s in 4 runs)".
- **Phase gauges cleared both barrier suspects:** every task reaches its
  barrier in ≤59 ms (start-delay) and aligns in ≤46 ms. Totals 881 ms /
  342 ms summed across 19 tasks — nowhere near the ~3 s freeze. The p99
  tail is **checkpoint I/O** (RocksDB state upload + sink flush = the
  TM's 24 MB/s burst-aligned writes), a bounded once-per-60 s event.
- **Decision: accept.** Fixing p99 on this box means disk separation or
  code changes for a dev-only artifact while the contract metric passes.
  Production relevance is low (prod state goes to S3/network, not the
  same NVMe as everything else). Reopen only if a p99 requirement ever
  materializes — the first lever to try then is separating RocksDB
  localdir / checkpoint dir / Fluss data onto different disks.
| B6 | p95 < 500 ms | ❌ Deferred by choice. Structurally impossible with timer-driven emission at 500 ms interval (floor law). Would need event-driven preview emission — a code change, estimate before ever starting |
| B4 | Operator parallelism | Currently 8 source parallelism / mostly 1 elsewhere, 1 TM × 10 slots. Headroom exists; only if throughput grows |

### Group C — production-readiness levers (separate workstream, all open)

| # | Lever | What's missing |
|---|---|---|
| C1 | **Real-feed measurement** | Tick lateness percentiles on real broker → set watermark to just above p99.9 (provisional 500 ms gets re-validated) |
| C2 | **Failover/recovery test** | ✅ **DONE 2026-08-30 (savepoint→stop→restore drill, zero data loss — see scoreboard)**. Caveat: this was a graceful savepoint drill, not a kill -9 mid-burst. A TM-kill-at-full-load drill remains open if that failure mode matters |
| C3 | **Long soak (hours)** | State growth, memory leaks, RocksDB degradation over time — 15-min runs cannot see this |
| C4 | **Backtest-parity check** | ✅ **DONE 2026-08-31 via F5's stronger form (commit 329560f):** the G7c audit proves final candles == full raw recount per (token, window) — since previews and finals share one watermark (single-timeline rule, §2.2), tick-set parity holds by construction with evidence. The literal preview-final-row vs final-candle-row comparison was intentionally NOT built: preview trigger timing is event-time driven inside the window, so preview-last ≠ final by design (see scoreboard F2/F3/F5 entry) |
| C5 | **KV point-lookup flakiness** | ✅ **DONE 2026-08-31 (CHG-119, work-around):** root cause verified against Fluss 0.9.1 source — lookups retry transients internally up to `client.lookup.max-retries` (default `Integer.MAX_VALUE`) with **no per-request timeout on inflight RPCs** (`ServerConnection` TODO), so a transient that outlasts the caller's `.get(timeout)` (measured ~5s leader-election settle vs default 2s `GATEWAY_REQUEST_TIMEOUT_MS`) surfaces as an intermittent `TimeoutException`/`UNAVAILABLE`. Blast radius (verified by grep): hot ingestion/signal path does **not** use point lookups (dedup moved to RocksDB, CHG-022); the only production consumers are the execution gateway's two raw-client stores. Fix: caller-side bounded retry (`BoundedRetry`, 3 × caller-timeout + 2 × 200ms ≈ 6.4s budget > 5s settle) in `FlussControlStateStore` (lookup) and `FlussProjectionLedgerStore` (lookup+put); internal loop deliberately left unbounded (it is the recovery mechanism; bounding it would convert recoverable outages into failures). Guard tests: `BoundedRetryTest` (5) + `FlussControlStateStoreFlakyLookupTest` (2) — transient-then-success recovers, permanent transient fails fast after exactly 3 attempts. Gateway suite 79/79. Upstream filing left open (work-around sufficient) |
| C6 | **Capacity ceiling** | At what tick rate does it break? Current headroom 2.5× measured, but no explicit breaking-point test |

### Group D — memory & CPU efficiency levers (low-memory goal)

These reduce allocation, serialization, or state cost. None is urgent at
current load (operators ≤38% busy), but together they define how far the
system scales on the same hardware before needing more machines.

| # | Lever | Evidence / how to check | Expected effect |
|---|---|---|---|
| D1 | **POJO serialization for CandleAccumulator** | ✅ **FIXED 2026-08-30 (commit 39adae2): full POJO (public class + public fields)**. JM no longer logs the GenericType warning. A/B latency measurement not done — fix was free and safe; measure only if D-group ever becomes urgent |
| D2 | **GC tuning / collector choice** | GC logs now captured (2026-08-30). If pauses are the burst cause: G1 tuning (region size, pause-target) or switch collector (ZGC/shenandoah = sub-ms pauses, some throughput cost) | ✅ **MEASURED 2026-08-30 (2 runs): GC is healthy and NOT the burst cause.** TM: 424 pauses, max 25 ms; ingestion: 70 pauses, max 16 ms. No tuning needed at current load; GC logging stays in harness as a standing baseline. |
| D3 | **Operator object-churn audit** | Flink metrics: `numRecordsOutPerSecond` already tracked; add allocation profiling (JFR on TM for 60 s during a run) if GC frequency looks allocation-driven | Lower GC frequency = fewer pauses |
| D4 | **RocksDB state tuning** | Default block cache/write-buffer sizes on a 2.2 GB heap TM; check `state.backend.rocksdb.memory` config vs actual state size (candles state = 1024 tokens × open window) | Less memory pressure, fewer compactions |
| D5 | **Preview row volume** | ✅ **PARTIALLY FIXED 2026-08-30:** the real problem was Fluss's CALENDAR-DAY TTL never expiring same-day data (11.6 M keys accumulated). Harness now purges (drop+recreate) the preview table at every run start (`pipeline_purge_preview_table`). Remaining: decide if a production TTL policy (e.g. hourly buckets) is needed once preview runs continuously | Bounds storage + read amplification |
| D6 | **Ingestion JVM footprint** | ✅ **DONE + APPLIED 2026-08-31:** offline analysis (17 runs) put the ingestion live-set at **64–90 MB post-GC** on a 2,048 MB heap — 32× oversized. Right-sized to **512 MB heap + 512 MB direct** in `pipeline-lib.sh` + `loadtest-run.sh` (G11 guard keeps both scripts' flags identical). **Verified live (run 20260831-100737, 512m): live-set max 62 MB, e2e p95 689 ms, all G6/G7 guards PASS — no regression.** The D6 guard in the analyzer now fails any run whose warm-half live-set exceeds 200 MB (leak alarm). TM JVM: live-set ~1,010–1,115 MB on 2,270 MB heap (56% — healthy) — left as-is. | Done; watch the D6 guard on future runs |
| D7 | **Fluss writer batching** (relates to A3) | `FLUSS_WRITER_BATCH_SIZE_BYTES=0` = every tick is its own append (lowest latency, highest RPC overhead). At production rates this trade may flip — measure RPC count/s | Throughput headroom at modest latency cost |

### Group E — throughput scaling levers (high-throughput goal)

Current: 10,127 events/s with ~2.5× headroom measured (source idle ~75%).
If production rate grows (more instruments, higher Hz, more tables):

| # | Lever | Trigger condition |
|---|---|---|
| E1 | **Source parallelism** (currently 8; raw_table_1 v3 now has **16 buckets** — CHG-117 recreate — so headroom to raise before E5) | Source busy-time consistently >60% |
| E2 | **Operator parallelism beyond 1** (candles, preview, signal currently parallelism 1) | When a single operator's busy-time saturates; requires key-by already in place (it is) |
| E3 | **Multiple TMs / slots** (1 TM × 10 slots now; 152 tasks already run on it) | CPU throttling observed (A5) or slot exhaustion |
| E4 | **Network buffers** (`taskmanager.memory.network.*` currently 128 MB) | Backpressure originating at network exchange, not compute (check backpressure metrics per vertex) |
| E5 | **Fluss table bucket count** (raw_table_1 v3 = **16 buckets** since the 2026-08-31 recreate, CHG-117 — max source parallelism cap is now 16, not 8) | When E1 is raised to 16 and still saturated — needs another drop/recreate via `fluss-repair/RawTableAdmin.java` (archive the lake prefix first: see `06_operations/07-lake-archive-ops.md`) |

### Group F — data quality levers (is the data right?)

Distinct from performance: these verify the pipeline produces *correct*
data, not just fast data. A wrong candle that arrives in 300 ms is worse
than a right one that arrives in 2 s.

| # | Lever | Current state | What's missing |
|---|---|---|---|
| F1 | **Zero-loss verification** (tick in broker → tick in raw table) | ✅ Strong: from-earliest LOG reads + source throughput vs input rate already cross-checked in every measurement run | Automate as a standing assertion on production feed (not just test harness) |
| F2 | **Dedup correctness** (dedup operator drops only true duplicates) | ✅ **DONE 2026-08-31 (commit 329560f):** faketool injects verbatim-resend duplicates (`-inject-dups`); smoke gate asserts live counter delta == injected (200/200); full audit asserts counter == dups ingested inside the counter sampling window (1000/1000) AND raw-table fingerprint-extras == sent (1200/1200) | Standing guard in harness (G7a, `holistic-analyze.py`) |
| F3 | **Late-tick accounting** (ticks dropped beyond watermark wait) | ✅ **DONE 2026-08-31 (commit 329560f):** `CandleLateDrop` counter (`compute.candles.late.dropped`) existed in code; now injected-audited — smoke gate 20/20 live; full audit counter 100/100 in sampling window, 120/120 in raw | Standing guard in harness (G7b) |
| F4 | **Signal settlement correctness** (TENTATIVE→CONFIRM/CANCEL) | ✅ **DONE 2026-08-31 (§4.8 F4 guard):** a TENTATIVE whose window closed >30 s before run end MUST have a CONFIRM/CANCEL partner — 0 orphans across all runs; harness asserts it every run | Production-feed equivalent still to wire (with G6) |
| F5 | **Preview↔final candle parity** | ✅ **DONE 2026-08-31 (commit 329560f), stronger than planned:** instead of preview-vs-final (preview trigger timing differs by design), the audit does a FULL RAW RECOUNT — every final candle's tick_count+volume vs a from-earliest raw-table recount per (token, 15s window): 47,104 windows, 0 mismatches. Proven against a bug-injected evidence copy (guards fired on all 3 tamper classes) | Standing guard in harness (G7c) |
| F6 | **Tick sanity/validation** (bad prices, zero volume, crossed markets) | ✅ **DONE 2026-08-31:** the operator already exported `compute.invalid.rows` + 8 per-reason `byReason` counters since inception — never sampled (gotcha-#22 pattern, again). Harness now captures them (`tm-prom-invalid.tsv`); the analyzer reports in-window deltas and FAILS the run on any non-zero rejection (clean bench feed ⇒ rejections = validation-rule regression or corrupt feed). G13 wiring guards (32/32). Tamper-proven: fabricated rising counters → "F6: 45 raw rows rejected... by-reason={'validity-state': 42}" fires, rc=1; zero-delta passes | Standing guard per run (F6, `holistic-analyze.py`) |
| F7 | **Schema contract enforcement** | ✅ `TableContractValidator` fails closed at job start (schema v2, 15 cols) | Broker-side schema drift is not covered — manifest fingerprint is the natural gate (see G3) |
| F8 | **Clock/ordering sanity** (event-time monotonicity per instrument) | ✅ **HARNESS GUARD DONE 2026-08-31 (§4.8 F8):** per-token backward event-time jumps in log order must equal the late-row count — exact reconciliation (80 = 80) every run. `ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000` guards future skew | Continuous production assertion still open (with G6) |

### Group G — data observability levers (can we SEE the data's health?)

Infrastructure observability (Prometheus/OpenObserve) covers the
*pipeline's* health. Data observability covers the *data's* health —
volumes, freshness, distributions, schema — so degradation is caught by
alerts, not by a downstream trading decision going wrong.

| # | Lever | Current state | What's missing |
|---|---|---|---|
| G1 | **Volume monitoring** (events/s per table, per instrument) | 🔶 **First rule 2026-08-31 (CHG-118):** `SIGNAL-warn-source-volume-drop` fires when the source runs < 5,000 rec/s (50% of design) for 5 min — covers the partial-degradation gap that source-stalled (0 rec/s only) missed. Routed to the G6 consumer | Relative >50% drop vs rolling median (needs an O2 recording rule — v0.91.5 lacks it, revisit on upgrade); per-instrument granularity; per-table rules beyond raw_table_1 |
| G2 | **Freshness monitoring** (age of newest row per table) | Freshness measured per run (p50=841ms staleness at run end) | Continuous freshness SLO with alert (e.g. alert if newest preview > 5 s old) |
| G3 | **Schema/broker drift detection** | ✅ **DONE 2026-08-31 (native fix + G8 gate):** root cause was config skew — bridge env carried a 1,024-token slice while Java loaded the full 2,431-row CSV, so every bridge event tripped the cross-check. Native fix: `startBridge` hands the Go child EXACTLY Java's loaded manifest set via `ARROW_INSTRUMENT_TOKENS` (child-env handoff, one source of truth — the manifest slice). Bench scripts no longer pass a second token config. New G8 gate fails any run on a mismatch line in java.out (both phases); G9/G10 guard tests prove the wiring + gate fires on tampered logs. Live proof run: zero mismatches, G6/G7 all pass, p95 722ms | Escalating to fail-closed halt in production is now safe (no false alarms) — wire when production monitors land (G6) |
| G4 | **Distribution drift** (price/volume ranges per instrument) | Nothing — candles computed but never profiled | Nightly job: per-instrument min/max/mean vs trailing baseline; flag breakouts as possible bad ticks |
| G5 | **Table-level health** (row counts, partition/bucket balance) | **Lake side covered 2026-08-31:** `lake-guard.sh` (cron-able, negative-test-proven) checks yesterday's day-folder objects + iceberg manifests daily — see `06_operations/07-lake-archive-ops.md`. Fluss-side still ad-hoc via LOG reads | Scheduled expected-rows vs actual-rows per window on the live tables (ties into F1) |
| G6 | **Alert routing** (who gets told, how fast) | ✅ **DONE 2026-08-31 (dev form, CHG-118):** all 47 O2 alert rules route to one webhook (`dev-webhook`) whose consumer now persists every delivery durably (JSONL on the alert-store volume, classified crit/error/warn × ing/signal/infra, queryable via `/alerts`/`/stats`) — the CHG-093 stdout receiver is retired. Mechanical guard: `make alert-routing-test` (end-to-end probe + malformed-delivery negative proof). Production = repoint the destination at a real pager; record format is the contract (`06_operations/03-alert-routing.md`) | Production pager destination (when the 4-VM stack lands); F1/G2 production monitors still to wire as rules |
| G7 | **Runbook per alert** (what to DO when it fires) | Partial (docs/06_operations); the lake/tiering failure modes got a full runbook + known-failure-mode ledger 2026-08-31 (`06_operations/07-lake-archive-ops.md`, M-1…M-18) | One paragraph each for feed loss, staleness, drift, orphaned signals (candle-path alerts) |

**Design principle for F/G:** prefer levers that reuse evidence already
being produced (LOG reads, manifest fingerprints, settlement balances,
freshness numbers) — most of these are alerts wired to existing signals,
not new systems.

---

## 4. Recommended sequencing

1. ~~GC-telemetry run verdict~~ **DONE 2026-08-30** — GC eliminated.
2. ~~A1 + A2 + A5 batch~~ **DONE** — all answered; disk saturation named,
   tiering fixed, then checkpoint cause found and fixed (scoreboard).
3. ~~Target p95 < 1 s~~ **DONE 2026-08-30** — p95 = 740 ms (740 with env
   overrides, 685 with bare defaults); defaults committed. Investigation
   CLOSED.
4. ~~D1 POJO fix~~ **DONE 2026-08-30** (39adae2).
5. ~~C2 recovery drill~~ **DONE 2026-08-30** — zero data loss; rollout
   tooling drilled live and fixed (72c9743).
6. ~~F2/F3/F5 data-quality audit~~ **DONE 2026-08-31 (commit 329560f)** —
   injection-based dedup/late-drop guards + full raw recount parity
   (47,104 windows, 0 mismatches), proven against bug-injected evidence.
   C4 closed with it (stronger form). Evidence:
   `logs/tracker-14/holistic-measure-20260831-002628`.
7. ~~Lake tiering / real EOD offload test~~ **DONE 2026-08-31 (CHG-117)** —
   continuous R2 iceberg tiering is live on the recreated raw_table_1 v3
   (event_day daily partitions, 16 buckets, `table.datalake.freshness` back
   at the 5 min default — the bench-only `0s` knob died with the old table);
   EOD lake verification drilled live (day 2026-08-31 COMMITTED → VERIFIED
   on R2 evidence). Ops surface + bug ledger M-1…M-18:
   `06_operations/07-lake-archive-ops.md`. The v3 recreate also clears
   E5's old bucket cap (8 → 16, above).
8. ~~Signal-path latency measurement~~ **DONE 2026-08-31** — see §4.8 (first
   profile + F4/F8 guards).
9. ~~G3 fingerprint mismatch~~ **DONE 2026-08-31 (native fix + G8 gate,
   commit 65da1e2)** — see scoreboard. Remaining before production feed:
   G3's fail-closed halt wiring (safe now — no false alarms) and F1/G1/G2
   as production monitors
10. **When load grows:** scale test 1,024 → 3,000 instruments (box's 16 GB
   RAM may cap this; C6), then Group E (scaling) — not before
11. **Blocked on real feed:** C1 final watermark numbers; D7's batching
   tradeoff also needs real-rate data

### Efficiency principles (why Groups D/E are ordered last)

- No operator is compute-bound today (max 38% busy) — CPU work is not the
  bottleneck, so serialization/allocator wins (D1, D3) are headroom, not
  relief.
- Memory pressure only matters if it produces pauses — which is exactly
  what the GC run will tell us. D2/D4 follow the verdict, not precede it.
- Scale-out (E) multiplies cost; efficiency (D) reduces it; both only
  matter when the measured load demands them. The 2.5× headroom means
  neither is urgent at test rates.

### 4.8 Signal-path latency (first measurement, 2026-08-31)

The signal tables carry event-time stamps only (no wall clock), so the
only honest wall-clock measurement is Flink's built-in source→operator
latency tracking — enabled all along (`metrics.latency.interval=2000`) but
never sampled until 2026-08-31 (`tm-prom-latency.tsv`, 15 s cadence).
First profile (run 20260831-100737, 512 m ingestion JVM, 10 Hz × 1024):

| operator | p50 | p95 | p99 |
|---|---|---|---|
| early_signal | 139 ms | 3.6 s | 5.7 s |
| signal_detection | 136 ms | 2.7 s | 3.0 s |
| early_signal_candidates_sink Writer | 185 ms | 3.5 s | 5.7 s |
| candle_preview_15s | 90 ms | 2.7 s | 4.0 s |

Reading: medians are healthy (~140 ms), the p95/p99 tail is dominated by
the same tail that hits e2e (checkpoints + GC + writer batching). Note the
numbers are source→operator, so the candle-path p95 (~2.7 s) includes
window-close wait that is by design (15 s windows); the interesting
tail-work is already covered by D2–D5. Two new guards came out of this
work: F4 (a TENTATIVE whose window closed >30 s before run end MUST have
a CONFIRM/CANCEL partner — 0 orphans across all runs) and F8 (per-token
backward event-time jumps in log order must equal the late-row count —
exact reconciliation: 80 = 80).

## 5. Where the evidence lives

- `logs/tracker-14/holistic-measure-20260830-014108` — 5 s watermark baseline (KEEP)
- `logs/tracker-14/holistic-measure-20260830-021926` — v2 instrumented baseline (last_event_ts)
- `logs/tracker-14/holistic-measure-20260830-030012` — wm500 run, current best numbers + burst analysis source
- `logs/tracker-14/loadtest-20260829-192648` — committed 18/18 verification evidence (KEEP)
- `logs/tracker-14/holistic-measure-20260830-142542` and `-145108` —
  tiering-fix verification runs (first post-fix zero-burst evidence)
- `logs/tracker-14/holistic-measure-20260830-173514` — final combo run,
  p95 = 740 ms (the target-met evidence)
- `logs/rollout/rollout-signal-job-compute-20260830-*.log` — recovery
  drill: savepoint, restore, post-restore verification
- `logs/eod-test/eod-test-20260830-221347.log` — EOD controller test 8/8
  (guards + smoke + main)
- `logs/tracker-14/holistic-measure-20260831-002628` — F2/F3/F5 audit run:
  injection gate + G7 evidence (raw recount 7.53M rows, 47,104-window
  parity, 0 mismatches). Raw evidence file is the sanitized audit format
  (965 MB) — NOT the 6.3 GB full-row format of earlier runs
- `logs/tracker-14/holistic-measure-20260831-022833` — G3 native-fix proof
  run: zero fingerprint mismatches both phases (java.out has the token
  handoff line), G6/G7 all pass, p95 722ms
- ⚠ runs 140907 and 173514's rows files were destroyed by an analyzer bug
  (live re-read overwrote them) — fixed 9274ad8; later runs intact
- Harness: `code/01_platform/04_scripts/` — `pipeline-lib.sh` (shared),
  `holistic-measure.sh` (gate+measure), `holistic-analyze.py` (report+burst
  attribution+G6 guards+final-candle path), `test-pipeline-lib.sh` (19
  guards), `rollout-savepoint.sh` (deploy/savepoint/restore drill),
  `eod-controller-test.sh` (EOD machinery), `eod_controller.py` (EOD CLI)

## 6. Hard-won gotchas (do not rediscover these)

1. **String constants inline at compile time** — changing `ROW_SCHEMA_VERSION` in common requires `mvn -pl common install` + **clean** rebuild of compute.
2. **REST checkpoint history is trimmed after job cancel** — only ~10 of ~90 entries survive; poll live during runs (now guarded by `capture_checkpoint_history`).
3. **`FLINK_ENV_JAVA_OPTS` replaces image defaults** — Flink's `config.sh` only reads `env.java.opts.all` from config.yaml if `FLINK_ENV_JAVA_OPTS` is UNSET. Setting it (e.g. for GC logging) silently drops the `--add-opens` flags Fluss's Arrow deserialization requires → every source subtask dies with `ExceptionInInitializerError: MemoryUtil`. The compose file now carries the full default flag set + GC logging, with a warning comment.
4. **Compose YAML merge keys replace whole maps** — a service-level `environment:` block does NOT merge with the `x-flink-common` anchor's environment; it replaces it. Use `<<: *flink-common-env` (anchor now defined on the env map itself).
5. **Job submit races TM registration** — after the hygiene TM restart, the TM needs up to ~60 s to re-register; submitting before that = `NoResourceAvailableException` → RESTARTING. Guarded in `pipeline-lib.sh` (B5: poll `/taskmanagers`, assert non-empty array — note the response has no `numRegisteredTMs` field).
6. **LogScanner double-delivery** — every Fluss log row appears twice in from-earliest reads; always dedupe.
7. **Event-time vs wall-clock** — signal rows carry event-time stamps (eval_ts=window_end), NOT wall-clock; preview `output_ts` IS wall-clock. Latency = `output_ts − last_event_ts` per row (schema v2).
8. **Never pipe loadtest scripts through `grep | head`** — SIGPIPE kills the pipeline; use bg_run with output redirect.
9. **`docker compose` always needs `--env-file .env --env-file secrets.env`** — in manual commands AND in script wrappers; a wrapper that omits them aborts interpolating AWS_SECRET_ACCESS_KEY (rollout-savepoint.sh bug 1 of 3, found live).
10. **`set -o pipefail` + `set -e` traps:** `sed -n 's/../../p'` exits 1 on empty input and `curl -f` exits 22 on HTTP 4xx — both kill scripts via assignment status. Use `{ ...; } 2>/dev/null || true` around probes.
11. **Comments must NEVER sit inside backslash-continued commands** — a `#` on a continuation line comments out the rest (G4/G4b guard in test-pipeline-lib.sh).
12. **Fluss duration configs need units** — `remote.log.task-interval-duration: 0` is invalid; `0s` is valid.
13. **LogFullRead needs** `--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`, and long reads need run_ms ≥ 600000.
14. **Checkpoints JSONL dedupe must key on `"id": N`** — keying on `$1` of a JSON line collapses every checkpoint into one (this bug hid the checkpoint/burst correlation for two runs).
15. **Standing rule:** every fixed bug gets a mechanical guard so it cannot recur (comment-only fixes are not fixes).

### 6.2 More gotchas from the F2/F3/F5 audit (2026-08-31)

16. **Binary columns break line-based reads** — Fluss prints VARBINARY
    (`payload_hash`, `raw_payload`) as raw bytes: `0x0A` splits lines,
    `0x2C` shifts comma-split fields. Never parse full-row toString of a
    table with binary columns; use a sanitized field projection (audit
    mode in LogFullRead reads columns by INDEX and prints only ASCII-safe
    fields). Symptom: silent row loss (~23%) → false data-loss alarms.
17. **Always recompile embedded Java helpers** — `LogFullRead.class` was
    cached from an earlier run and silently ignored the new `audit` mode
    argument (fell back to plain mode). The analyzer now regenerates +
    recompiles on every call (~2 s).
18. **Counter exactness needs a sampling window** — Prometheus counters
    only cover what ingested BEFORE the last sample; injection rounds
    firing after the final sample land in raw but never in the counters.
    G7a/G7b now compare counter delta vs dups ingested before the last
    sample timestamp, not vs total sent. Injection is capped
    (`INJECT_MAX_ROUNDS=4`) so rounds don't fire during teardown drift.
19. **LATEST startup mode skips the backlog by design** — the source
    subscribes at LATEST (2026-08-29 decision: measure the live path
    only), so the first 15s window after job start is legitimately partial
    (e.g. 142/150 ticks) and pre-startup windows have no candle at all.
    G7c tolerates the contiguous startup prefix; a gap AFTER the first
    present candle is still a failure.
20. **Two configs for one token set = guaranteed fingerprint false alarms**
    (G3, 2026-08-31) — the bridge's `ARROW_INSTRUMENT_TOKENS` env and
    Java's `INSTRUMENT_MANIFEST_PATH` can drift apart silently. The native
    fix is ownership: Java loads the manifest and hands the exact set to
    the child in `startBridge`; the harness configures ONE input. Any
    mismatch line in java.out is now real drift and fails the run (G8).
21. **A Fluss "row" starts with `(` — anything else on reader stdout is an
    error line** (2026-08-31) — running the analyzer offline (stack down)
    makes LogFullRead print connection errors to stdout; counting those as
    rows let a re-analysis overwrite 300 MB of preview evidence with 91
    error lines. Both readers now keep only `(`-prefixed lines before
    deciding whether to overwrite the evidence file.
22. **Flink latency histograms live in labels, not metric names**
    (2026-08-31) — `flink_..._latency{...task_name="op -> x",...,
    quantile="0.5",}`: the operator name is the `task_name` label's first
    path segment and `quantile` is a label (not the first one). The job
    already ran with `metrics.latency.interval=2000` since the beginning —
    the data existed all along, nobody sampled it.
23. **Power loss tears Fluss log segments** (2026-08-31) — a reboot
    mid-write left a truncated record batch (EOF: expected 48 bytes, got
    37) and the tablet server crash-looped on replay. Recovery for
    disposable dev data: wipe the fluss data volumes, recreate the
    `default` database, re-run the ddl-apply contract. A clean shutdown
    avoids it entirely.
24. **Never edit a shell script while its process is running** (2026-08-31)
    — bash reads scripts incrementally by BYTE OFFSET; an edit shifts the
    file under the interpreter and the process resumes parsing mid-token
    (observed: a spurious `syntax error near unexpected token (' at a line
    that is valid — the B5 run died at its final analysis step). Edit
    BEFORE launch or AFTER exit; measurement evidence on disk is never
    affected, only the live process.

### 6.1 Observed-but-unfixed warnings (candidates, not confirmed problems)

- ~~`CandleAccumulator` GenericType serialization warning~~ **FIXED
  2026-08-30** (D1, commit 39adae2).
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
