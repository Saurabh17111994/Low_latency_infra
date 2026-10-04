# p99 ≤ 50 ms — full combined plan (2026-10-03)

**One program, one combined change set, one certification round.** This consolidates the
remaining p99 work: the two measured env levers (fetch window, output-buffer flush), the new
append-ack-tail fix found by the 2026-10-03 diagnosis, and the final combined certification +
production rollout. It supersedes the remaining W1/W2 adoption and W5 items of
`docs/plans/2026-09-30-p99-50ms-normal-path.md`; the completed W3 checkpoint work
(CHG-541/542/543) and the read-only diagnosis (`2026-10-03-baseline-100ms-diagnosis.md`) are
inputs.

## 0. Live tracker

- [x] **C0** Baseline freeze — current tree, defaults (fetch 20 / buffer 10):
  `logs/stage-profile-20261003-w3e3-main`; decomposition recorded in the diagnosis note.
- [x] **C1** Round A — **F1+F2 combined** on the current tree (fetch 2 / buffer 2) + passive
  F3 probe capture; smoke 200 s → 900 s — done 2026-10-03,
  `logs/stage-profile-20261003-c1` (job `510367d3911714c43c2f006b18f7860b`, smoke+main PASS,
  4,867 rows/s, 95/95 COMPLETED aligned checkpoints, 0 errors; `fetchLatencyMs`=2 /
  `timeMsBetweenPoll`=2 live). **Worst-subtask p99 (60 s bins): tick 85 / ingest 69**
  (per-snapshot medians 83 / 67) vs C0 105 / 85 → **improved ~16–18 ms, not ≤ 50**; cost:
  tablet CPU 20 → 61 %, TM 122 → 238 %; append tail p50 21 / p99 54 (C0 14 / 38). F3 probe
  verdict below.
- [ ] **C2** F3 — pick the append-tail lever from the probe, apply, smoke → 900 s.
  **Ingestion-only arm DONE 2026-10-04** (`logs/o2-ingestion-only-20261004/`):
  3 min fleet-only (phase1) then 3 min with SignalJob `4a4fa074ea3f1ff98ebc4048b1376ec1`
  (phase2), same load (~1 622 ticks/s × 3 instances ≈ 4.9 k/s). Append tail is
  **identical in both phases** — p99 med 59 (phase1, no SignalJob) vs 60 (phase2),
  p50 med 23 / 22, pending ~600 both, same good/bad oscillation and period
  (evidence: `logs/tracker-14/20261004-c2-ingestion-only-arm.md`). The bad state
  is **not** caused by the SignalJob/reader load → route to the server/write path
  (L1). Also NOT tablet CPU (median 2.9 %, max 67 %, over the whole run) and NOT
  remote-log tiering (`FLUSS_REMOTE_LOG_TASK_INTERVAL=0s` in the live deck).
  **Lead tested and CLOSED 2026-10-04 — 0 s vs 1 m A/B, full topology.** The four
  2026-10-04 runs before this one had omitted `STRATEGY_HOST_ENABLED=true`, so they
  deployed a 3-vertex job (no candle sink) and their read-gate failures were that,
  not the platform (see the invocation-trap bullet in F3). Re-run with the operator
  recipe, tablet flipped `FLUSS_KV_SNAPSHOT_INTERVAL=0s` between the arms and
  restored to `1m` afterwards (verified in `/opt/fluss/conf/server.yaml`):
  `logs/stage-profile-20261004-095336` (1 m) and `logs/stage-profile-20261004-095955`
  (0 s), both smoke gates **PASS**, same 3×811 @2 Hz fleet, same topology
  (`multi_tf=on strategy_host=on`). The 1 m tablet logged 186 snapshot completions
  over its life at 73–116 ms each. Append p99, 12 samples per arm:
  **1 m — 2/12 ≥ 50 ms (median 26.5, max 67); 0 s — 0/12 ≥ 50 ms (median 25.5,
  max 46)**; p50 medians 14 vs 12. Inside the 1 m arm the two >50 ms samples sat in
  10 s windows holding **1 and 5** snapshot completions, while the busiest windows
  (**7 and 9** completions) reported p99 26 and 23 ms → the tail does not track
  snapshot work. **Verdict: the tablet KV snapshot is NOT the bad-state cause.** 0 s
  is not a fix — it also removes KV durability (recovery degrades to plain log
  replay) — so dev stays at `1m`. At n=12 the 2-sample difference is inside the
  run-to-run variance; the honest reading is "no mechanism found", not "0 s helps".
  **Next C2 candidate:** `log.flush.offset.checkpoint-interval` (default `PT1M`:
  recovery-point offset checkpoint + directory fsync on a 60 s cadence, silent in the
  tablet log) and the raw write path/disk itself — and any resolution of a 60–90 s
  oscillation needs an arm of ≥ 900 s, not 200 s.
- [x] **C0b** 900 s baseline at defaults, full topology (2026-10-04) — the exact pass
  method on a run long enough to see the oscillation: `logs/stage-profile-20261004-101739`
  (job `ba4dcb435415b43520115c137acc5818`; `STRATEGY_HOST_ENABLED=true`
  `STRATEGIES=n7-range-breakout-v1`, fetch 20 / buffer 10, smoke 200 s PASS →
  main 900 s; `multi_tf=on strategy_host=on execution_intent=off`). KPIs scored from the
  50 Prometheus snapshots in `main/stages/prom-*.txt` (10:24:34 → 10:39:26 IST, 892 s,
  8 subtasks, `quantile="0.99"` per subtask): **worst-subtask p99 median 76 / max 114 ms
  for `ingest_to_monitor`; median 94 / max 370 ms for `tick_to_strategy`** →
  **15 of 15 60 s windows FAIL for both KPIs** (SLO ≤ 50 ms). `ingest_to_strategy` follows
  `tick_to_strategy` (median 85, max 364) — same stall, platform-side.
  The worst subtask **rotates across all 8** subtasks and the median-across-subtasks p99
  (73 / 90 ms) sits at the worst-subtask value → a **uniform, systemic elevation, not a
  hot key or straggler**. Cost checks all clean: throughput parity 4 867 → 4 841 rows/s
  (99.5 %), backpressure max 52 ms/s (median 0), busiest operator 6 %, 14/14 checkpoints
  COMPLETED (e2e 281–450 ms, p50 390), tablet CPU 25.4–25.9 %, append ack p50 16 / p90 32 /
  p99 42 ms, growth raw_table_1 +179 MB/min / candle_features +6.79 MB/min.
  **New lead (hypothesis, untested):** the five >200 ms windows each contain a sample taken
  3–8 s after a checkpoint (`:04` s every minute — 10:25:07, 10:29:07, 10:31:12, 10:32:07,
  10:35:07, 10:39:10), while samples ≥ 9 s after a checkpoint read 84–99 ms → consistent
  with a short post-checkpoint stall, but snapshots are 16–23 s apart so this is **not**
  proof; it needs a dedicated arm (checkpoint-interval change, or per-subtask stall timing).
- [x] **C2b** tablet offset-checkpoint A/B (launched 2026-10-04 11:01 IST,
  bg task `b8809d8ac`, driver `/tmp/ckpt-ab.sh`, log `/tmp/ckpt-ab.log`): two 900 s
  **fleet-only** captures (`SKIP_JOB=1`, the new fleet-only mode, CHG-545) with
  `log.flush.offset.checkpoint-interval` = `PT1M` (default) vs `PT10M`, tablet recreated
  between arms and restored to `PT1M` at the end, with a 1 s poller
  (`/tmp/ckpt-poller.sh`) sampling the recovery-point/high-watermark checkpoint file
  mtimes. Runs: `logs/stage-profile-ckpt-PT1M-1004-110120` (first) and
  `logs/stage-profile-ckpt-PT10M-*`. Scored with `/tmp/arm_report.py` (aligned
  append-latency p50/p90/p99 per 10 s bucket, all three ingestion instances). Observed
  before the arm: the recovery-point checkpoint is rewritten at exact +60 s, the
  high-watermark checkpoint every ~5 s.
  **Attempt 1 outcome (2026-10-04):** the `PT1M` control arm is valid — 900 s fleet-only,
  **22 of 90 aligned samples with worst p99 ≥ 50 ms**, 212 checkpoint-write events over
  912 s (recovery-point every 60 s, high-watermark every ~5 s), scored into
  `logs/stage-profile-ckpt-PT1M-1004-110120-score.txt`. The `PT10M` arm is **invalid**:
  the tablet never started, because the compose default I added was `PT1M` and Fluss's
  parser (`TimeUtils.parseDuration`) rejects ISO-8601 — `Could not parse value 'PT1M' for
  key 'log.flush.offset.checkpoint-interval'`, 16 restarts at exit 2 (see the revision
  section of `CHG-546`). `C0c` (arm 1) aborted at its preflight on the same crash loop, so
  **both the A/B comparison and arm 1 are still open**. Fix: default is now `60s`, guarded
  by `test_08_local_compose_f8_snapshots.py::test_duration_knob_defaults_are_parser_legal`.
  **Attempt 2 outcome (2026-10-04, FINAL — lever ruled out):** driver `/tmp/ckpt-ab2.sh`,
  values `60s` vs `600s`, one 900 s fleet-only arm each on the same recreated tablet
  generation, tablet restored to `60s` afterwards. The treatment was real: the 1 s poller
  counted **16 recovery-point writes** in the `60s` arm (`logs/stage-profile-ckpt2-60s-113512`,
  first writes at +0/+84/+144/+204 s) against **2** in the `600s` arm
  (`logs/stage-profile-ckpt2-600s-115208`, 601 s apart), with 181 high-watermark writes in
  both arms. Result: `60s` → **37/90** samples with worst p99 ≥ 50 ms, `600s` → **22/90**.
  The attempt-1 `60s` control also scored **22/90**, so the `600s` value sits exactly at the
  low end of the `60s` range (22–37) ⇒ **no readable effect: the tablet's offset checkpoint
  (flush + directory fsync) is NOT the source of the ~60 s wave.** Both tablet periodicities
  are now ruled out (KV snapshot 2026-10-04 morning, offset checkpoint here), which points
  away from the tablet: the three ingestion JVMs oscillate **in phase** and were all started
  within seconds of each other, so a time-based *client-side* periodicity fits the shape
  better than any server event. Standing candidate: **JFR chunk rotation** — profile
  recording is on inside the measured window. Next arm: the same 900 s fleet-only capture
  with JFR **off** (`INGESTION_JAVA_TOOL_OPTIONS`, `stage-profile.sh:80`) — one variable, no
  tablet change, no job.
  **Measurement lesson:** the threshold count is noisy run-to-run (identical `60s` config
  scored 22/90 and 37/90); one 900 s arm cannot resolve differences inside ~22–37, so read
  the per-sample p99 median (or repeat) before claiming movement.
  **Harness lesson (2026-10-04, wasted one cycle):** the first tablet-fix attempt never
  ran — in the background shell (`zsh`) an unquoted `$DC` holding `"docker compose …"` is
  **not word-split**, so the whole string was executed as one filename
  (`zsh: no such file or directory …`, `compose-up rc=127`); only the driver scripts
  (`sh`, POSIX word-splitting) apply config. The same script still exited 0 and printed
  its done-marker, so a green task status proved nothing — always assert the *effect*
  (container state, rendered config, absence of the parse error), not the script's exit.
- [x] **C0c** checkpoint-profile arm (**started 2026-10-04 12:09:58**, bg task `bdd54bc03`,
  driver `/tmp/arm1b.sh`; it began automatically once `C2b` wrote `C2-AB2-DONE`): the
  **exact C0b recipe, one variable changed** — the harness
  defaults ran `UNALIGNED_CHECKPOINTS=true` + `CHANGELOG_STATE_BACKEND=false`
  (`pipeline-lib.sh:1159-1160`), while the **production-certified** profile is
  aligned-only (CHG-543) + the changelog state backend (CHG-541). Arm: full topology
  (`STRATEGY_HOST_ENABLED=true STRATEGIES=n7-range-breakout-v1`, fetch 20 / buffer 10,
  smoke 200 s → main 900 s) with `CHANGELOG_STATE_BACKEND=true UNALIGNED_CHECKPOINTS=false`.
  Hypothesis: the five >200 ms windows come from the checkpoint path — the changelog
  backend exists precisely because the synchronous RocksDB snapshot blocks every stateful
  subtask **400–600 ms per checkpoint** at the 10 s cadence (`SignalJob.java:577-590`), and
  CHG-543 measured 618 wave samples / 359 ms spike unaligned vs 0 / 115 ms aligned-only.
  Prerequisites verified before queueing: TM plugin `dstl-dfs` mounted, TM keys
  `state.changelog.storage: filesystem` + base-path, `MAX_CONCURRENT_CHECKPOINTS=1`,
  `STATE_BACKEND=rocksdb` (all satisfied). Scored with `/tmp/kpi.py` (worst-subtask
  `quantile="0.99"` per snapshot → 60 s windows anchored at the first sample; **validated
  against C0b: reproduces median 76 / max 114 / 49-of-50 ≥ 50 ms / 15-of-15 windows
  failing for `ingest_to_monitor` and median 94 / max 370 / 15-of-15 for
  `tick_to_strategy`**). JFR is deliberately left on so the only delta vs C0b is the two
  flags.
  **Outcome (2026-10-04 12:31, FINAL — checkpoint profile ruled out as the lever):** the
  arm completed exit 0 (57 min) into
  `logs/stage-profile-arm1b-changelog-aligned-1004-120920`, harness rc 0, presence gate
  PASS, **16 checkpoint phases** sampled in `main/stages/cp-phases-detail.jsonl`. Scored
  with the C0b-validated `/tmp/kpi.py`:
  | KPI | C0b (dev profile) | C0c (changelog + aligned) |
  |---|---|---|
  | `ingest_to_monitor` | median 76 / max 114 | median **85** / max 118 |
  | `tick_to_strategy` | median 94 / max **370** | median **101** / max 139 |
  | `ingest_to_strategy` | median 85 / max **364** | median **90** / max 128 |
  | `ingest_to_monitor` windows | 49/50 ≥ 50 ms, 15/15 failing | 49/50, 15/15 failing |
  | `tick_to_strategy` windows | 50/50, 15/15 failing | 50/50, 15/15 failing |
  The pre-committed materiality rule (≥20 % lower median **and** fewer failing windows) is
  **not met**: medians are ~8–10 % *worse* and the failing-window count is identical. So
  the checkpoint path does **not** explain the ~76–101 ms floor — that is structural
  (source fetch window + 3–5 serialised 10 ms output-buffer hops), which is what F1/F2/F4
  address. Real, separate gain worth keeping: the giant spikes disappear in the certified
  profile (max 370 → 139 and 364 → 128, −62 % / −65 %), so **aligned-only + changelog
  backend stays the standing recipe for tail hygiene** — it just is not the p99-floor
  lever. Evidence record: `logs/tracker-14/20261004-c0c-changelog-aligned-arm.md`.

### Deferred levers — read-only sweep 2026-10-04 (no changes made)

Recorded so they are not re-derived; each needs its own arm/CHG before adoption.

- **Ingestion micro-batching is off** — `INGESTION_MAX_BATCH_RECORDS=1`,
  `INGESTION_MAX_BATCH_WAIT_MS=0` (`PlatformConfig.java:31-32`) → ~4.9 k append RPC/s, each
  paying the full RTT + server queue. Batching 5–10 records / ≤1 ms also makes tablet
  batches big enough for zstd to work, which is a `raw_table_1` **byte** win as well (DDL
  v5 note: at ~1 row/batch compression "has almost nothing to work with", ~508 B/row).
- **JFR profile-mode recording runs inside the measured window** — TM
  `-XX:StartFlightRecording=...duration=900s` from TM start and ingestion
  `INGESTION_JAVA_TOOL_OPTIONS` JFR 1800 s (`stage-profile.sh:80`). Off for latency arms
  (keep it on for the C0c comparison so only one variable moves).
- **Storage growth is Fluss-side, not Flink-side** — Flink RocksDB state is 4.3 MB/subtask
  and checkpoints 0.58–0.76 MB; the growth is `raw_table_1` +179.01 MB/min (3 d TTL ⇒
  ~770 GB steady state at the dev rate, ~7.7 TB at the production 10× rate) against
  **316 GB free** on the host disk (`/dev/nvme0n1p2`; tablet data dir is the docker volume
  `01_docker_fluss-tablet-data`) → ~29 h of continuous 2 Hz ingest fills it.
  `Signal_Candidates_current`'s 1.9 GB is ~100 % `log-N` changelog segments (16 × ~116 MB,
  `kv-N` dirs are tiny) with a 7 d `table.log.ttl` ⇒ ~44 GB steady state.
  `candle_features` +6.79 MB/min (3 d ⇒ ~29 GB). `ingestion_quarantine` 338 MB with no TTL.
- **Idle index preallocation** — every log bucket carries a preallocated `.index` (10 MB) +
  `.timeindex` (10 MB) even with an empty `.log` (seen on
  `trade_instruction_state-24/log-0`); nine near-idle tables hold ~460 MB of disk.
- **Cheap config alignments not yet done** — dev checkpoints every 60 s
  (`pipeline-lib.sh:1158`) vs the production pin `PlatformConfig.CHECKPOINT_INTERVAL_MS=10_000`;
  dev `FLUSS_KV_SNAPSHOT_INTERVAL=1m` (`.env:152`) vs the certified `0s`.
- **Health/diagnostic leads** — `01_docker-ingestion-1`'s readiness healthcheck
  (`test -f /tmp/ingestion.ready`) failed 4 of 5 recent probes (the service keeps clearing
  the ready file: `IngestionService.java:2049-2065`, clear at `:2244`); the o2
  `bridge.slot` anomaly (`capacity_used_percent` 100, `last_frame_age_ms` growing) is still
  unexplained. Root-cause both before trusting later arms.
  Full record: `logs/tracker-14/20261004-900s-baseline-defaults.md`.
- [x] **C2c** JFR A/B — the profiler is ruled out as the source of the ~60 s wave
  (2026-10-04, bg task `b541eb8df` + treatment proof `b913a9054`, driver `/tmp/jfr-ab.sh`,
  scorer `/tmp/jfrs.py`). After both tablet periodicities (C2, C2b) and the checkpoint
  profile (C0c) were eliminated, the last named candidate was the JFR profile recording
  that runs inside every measured window: `INGESTION_JAVA_TOOL_OPTIONS` (`stage-profile.sh:80`)
  starts `-XX:StartFlightRecording=settings=profile,duration=1800s` **plus** an
  `-Xlog:gc*,safepoint` file log in all three harness ingestion JVMs at once (the TM and
  tablet recordings are `duration=900s` and expired long ago). Design: two 300 s
  **fleet-only** captures (`SKIP_JOB=1`, CHG-545) differing in exactly one variable — ON =
  the stock harness flags, OFF = `INGESTION_JAVA_TOOL_OPTIONS=" "` (a space, because
  `:-` treats an empty value as unset). Runs `logs/stage-profile-jfr-on-1004-143023` and
  `logs/stage-profile-jfr-off-1004-143023`.
  **Treatment proven**: the 60 s OFF re-run inspected the live containers and every harness
  ingestion JVM carried `JAVA_TOOL_OPTIONS= ` (empty) — no JFR, no gc-log.
  **Result**: ON **1 of 30** aligned samples with worst p99 ≥ 50 ms (worst 53 ms at
  09:02:35), OFF **6 of 60** (worst 61 ms at 09:12:16); typical p50 8–13 / p99 12–35 on
  both sides.
  **Verdict: JFR is exonerated** — removing the profiler does not remove the spikes, so no
  earlier p99 number in this program was an artifact of our own recording. Side observation:
  in fleet-only mode the wave is weaker and intermittent (bad samples ~70–190 s apart, not a
  clean 60 s beat) than in the full-topology runs, so the SignalJob's own read load amplifies
  the tail rather than causing it.
  Full record: `logs/tracker-14/20261004-jfr-ab-verdict.md`.
- [ ] **C3** Final combined certification — **F1+F2+F3 all on**, 900 s, exact pass method,
  then `make gate`.
- [ ] **C4** Production adoption — defaults + deck env, deploy window, live verification.
- [ ] **C5** (conditional) F4 hop reduction — only if the C3 round still misses 50 ms.

## 1. Goal / success criterion

**S1:** every 60 s snapshot's **worst-subtask p99 ≤ 50 ms** for both
`compute.latency.ingest_to_monitor` and `compute.latency.tick_to_strategy` over the C3
certification round, with no regression in throughput, checkpoints, or state growth.

## 2. Where we are (measured 2026-10-03, current tree, defaults)

| KPI | p50 | p99 |
|---|---|---|
| `tick_to_strategy` | 47–50 | 95–104 |
| `ingest_to_monitor` | 36–37 | 79–84 |
| `ingest_to_strategy` | 42–44 | 85–91 |

p99 composition (three structural parts, partially overlapping):

| Part | p99 contribution | Status |
|---|---|---|
| Ingestion append ack tail | ~40–55 ms | **not addressed — F3** |
| Source fetch window | up to 20 ms (cap fully consumed every poll) | **F1** (measured) |
| In-DAG transit over 3–4 hops | ~30–38 ms (10 ms buffer timeout/hop) | **F2** (measured) |

Already fixed and not touched again: checkpoint spikes (W3: 359 → 115 ms tick max,
618 → 0 wave samples), Flink's original 100 ms per-hop buffer (→ 10 ms, 2026-09-26).

## 3. The complete fix set

### F1 — Source fetch window 20 → 2 ms (`FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS`)
- **Measured (2026-09-30, W1):** alone: ingest p99 75 → 71, tick 93 → 89; combined with F2 it
  is part of tick p50 50.5 → 31 / p99 99 → 71, ingest p50 37 → 23 / p99 81 → 58.
- **Cost:** request rate ×~30 (~3 k/s at the 60 k/s production rate) — watch Fluss CPU/tablet.
- **Adoption:** default 20 → 2 in `SignalJobConfig.java:1373` + `pipeline-lib.sh:1165`
  (env override remains the instant rollback).

### F2 — Output-buffer flush 10 → 2 ms (`BUFFER_TIMEOUT_MS`)
- **Measured (2026-09-30, W2 marginal over W1):** tick p99 med −18 ms, ingest p99 med −13 ms.
- **Cost:** more flush events; trial checkpoint e2e med 75 ms (control 40 ms) — must stay
  within the checkpoint budget in C1/C3.
- **Adoption:** default 10 → 2 in `SignalJobConfig.java:1377` + `pipeline-lib.sh:1190`.

### F3 — Ingestion append ack tail (new; the largest remaining part)
Measured facts (2026-10-03, current tree):

| Fact | Value |
|---|---|
| Append call | submit = ack — one blocking call, identical histograms |
| Client batch-timeout | **1 ms** (already tuned, O-2) |
| Client batch-size | 64 KiB pinned, dynamic sizing off — does not bind at this rate |
| Buckets | 16, key `instrument_token` → ~304 rows/s per bucket at 4.9 k rows/s |
| Append latency (client-observed) | p50 14 / p90 29–47 / p99 **38–68** ms |
| Pending records | ~635 standing; memory pool 66.8/67.1 MB free; zero backpressure |
| Server | tablet defaults; no ack/flush tuning in the deck |

At ~304 rows/s per bucket, the 1 ms timer effectively fires per record — the append path is
close to a per-record RPC, and its ack tail (38–68 ms p99) is the target.

**Probe result (2026-10-03, C1 + the five historical arms) — client knobs ruled out:**
- `client.writer.batch-timeout` is already 1 ms and `client.writer.batch-size` is pinned
  64 KiB with dynamic sizing off; at ~304 rows/s per bucket the timer fires per record —
  **no client-side knob left**.
- The append latency **oscillates inside every run** between a good state (p50 ≈ 10,
  p99 ≈ 20–27 ms) and a bad state (p50 ≈ 20–33, p99 ≈ 50–66 ms) with a ~60–90 s period;
  the KPI p99 is set by the bad state. Eliminating it would put ingest ≈ 35–40 and tick
  ≈ 50–55 (then F4 covers the tick residual).
- The tablet runs continuous KV snapshots (`Safety_Halt_Requests` table 18 + a per-run
  table, ~100–180 ms each) — measured, but **not correlated** with the bad windows
  (0–10 snapshots per 10 s in both states).
- **Answered 2026-10-04** by the 0 s vs 1 m A/B under the full topology (C2 above):
  the 1 m arm's two >50 ms samples sat in 10 s windows holding 1 and 5 snapshot
  completions, while windows with 7–9 completions stayed at p99 23–26 ms; 0 s removed
  those two heat spikes (0/12 vs 2/12 ≥ 50 ms) but left the p50 floor and the
  bad-state drift unchanged. Snapshot cadence does not set this tail.
- Run-to-run variance is large: append p99 medians 38 / 43 / 45 / 54 / 62 across nominally
  same configs; the old-tree W1+W2 (58 / 71 KPI, append 14 / 43) is the best measured state.
- The pre-job windows (fleet writing, no SignalJob) sit in the good state (p50 12–13) —
  the bad state appears with the full platform load (readers + compute writers + snapshots).
- **Corrected 2026-10-04 by the ingestion-only arm:** the pre-job windows in that
  earlier probe were good-state by timing luck. Under a controlled 3-min no-job
  phase the fleet alone reproduces the full oscillation (p99 19–68, med 59) — the
  bad state does **not** require readers/compute/snapshots from the platform.
- **Invocation trap (2026-10-04, now gated):** the branch flags reach only the
  `flink run` client, which defaults them to false, so a capture that omits
  `STRATEGY_HOST_ENABLED=true` silently deploys a 3-vertex job — no candle sink,
  `candle_features` empty, every read probe 0 rows, and the failure surfaces 3.5 min
  later as bare `S8.feature_read: 0 < 1 samples`. `stage-capture.sh` now fails fast on
  a requested-vs-deployed topology mismatch and warns when a deployed candle branch is
  not yet readable (`code/01_platform/04_scripts/tests/test_stage_capture_topology_gate.py`).
  Recipe: `logs/tracker-14/2026-10-03-chg526-rerun-latency-state-growth.md:6-9`.
- **Open question for C2:** is the bad state caused by the platform's other load
  (then: load isolation / server tuning) or by the raw write path/disk itself
  (then: write-path investigation)? A short ingestion-only arm (no SignalJob) answers it.

**Lever menu (chosen from the probe):**

| Lever | Kind | Use when |
|---|---|---|
| L1 server ack/queue config (`FLUSS_PROPERTIES`) | server | probe shows server-bound processing |
| L2 client sender/batching (`client.writer.*`) | client | probe shows client-bound queueing |
| L3 `FLUSS_WRITERS` > 1 (probe-only today) | producer model | single-writer bound — needs its own CHG |
| L4 batch-size/timeout sweep | diagnostic | discriminating test only |

**Target:** append p99 ≤ 20 ms.

### F4 — Structural hop reduction (conditional margin)
- Remove one shuffle hop (~5–10 ms). Design only; used **only if C3 misses 50 ms**.
- W4 (CPU / slot isolation) is closed — not reproducible.

## 4. Combined expected outcome

| State | tick p99 | ingest p99 | Source |
|---|---|---|---|
| C0 — today | ~100 | ~82 | measured |
| + F1+F2 | 71 | 58 | measured 2026-09-30 (old tree) |
| + F3 (target) | ~48–55 | ~35–45 | estimate — C3 measures |
| + F4 (if needed) | −5–10 | −5–10 | design |

## 5. Execution program

1. **C1 — Round A (combined F1+F2 on the current tree).** Env: fetch 2 / buffer 2; everything
   else unchanged (10 s checkpoints, changelog backend, aligned-only). Smoke 200 s with
   end-to-end data-flow check → 900 s `stage-capture.sh` with `INGESTION_JAVA_OUT` (append
   metrics) and the F3 probe. Evidence: `logs/stage-profile-<ts>-c1/`.
2. **C2 — F3.** Apply the probe-selected lever; smoke → 900 s; measure the append tail alone.
   If the probe shows the tail is not on the KPI path (visible-before-ack), close F3 with the
   evidence and re-derive the residual instead.
3. **C3 — Final combined certification.** F1+F2+F3 all on; 900 s; exact pass method
   (worst subtask per snapshot, both KPIs, 60 windows); cost checks; then `make gate`
   (19 steps) for the code changes. This is the certification attempt.
4. **C4 — Production adoption.** Code defaults for F1/F2 (and F3's chosen value) + CHG(s) +
   dossier rows + deck env where applicable; deploy in a market-closed window; live
   verification on the next session; rollback = env override lines (fetch 20 / buffer 10 /
   F3 value back) without a redeploy.
5. **C5** — only if C3 misses: F4 design → apply → re-run C3.

**Approvals:** (1) this combined scope — now; (2) before any code/default change (F1/F2/F3)
+ CHG; (3) before the production deploy. No silent default flips.

## 6. Verification method

- **Smoke first:** every 900 s run is preceded by a 200 s smoke that shows data flow
  (table growth / read counters / VALID snapshots) and is confirmed PASSING.
- **Pass method:** worst-subtask per snapshot, both KPIs, ≤ 50 ms on every window.
- **Cost checks:** checkpoint duration/e2e, Fluss request rate + tablet CPU, throughput parity
  (4.9 k rows/s dev; 60 k/s production re-check), state growth, backpressure stays 0.
- **Evidence:** `logs/stage-profile-<ts>-*/` + gate logs; one CHG per code change; dossier
  updates in the same change.

## 7. Risks / rollback

| Risk | Mitigation |
|---|---|
| F1 raises Fluss request rate ~30× | watch tablet CPU/queue in C1/C3; revert env instantly |
| F2 raises checkpoint e2e (trial 75 vs 40 ms) | checkpoint budget check in C1/C3; revert env |
| F3 lever unknown until probe | probe first; server lever = property revert; client lever = value revert |
| Dev is a single TM | label dev-only; production verification is the C4 live check |

Rollback for every lever is a value change (env override), no topology change.

## 8. Out of scope

- W4 CPU/slot isolation (closed), the signal-sink `batchQueueTimeMs` ≈ 100 ms constant
  (downstream of the KPI), checkpoint interval/settings, DDL/schema/bucket changes (frozen),
  TTL, feed timestamp-fidelity (separate item).
