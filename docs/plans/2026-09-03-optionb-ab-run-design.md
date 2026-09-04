# Option B A/B Run Design — bounded source drain vs stock (controlled comparison)

Status: AWAITING APPROVAL (no runs started)
Date: 2026-09-03
Branch: low-latency-ingestion-based-project (uncommitted worktree /tmp/flink-221-exp)

## 1. What is being tested (the causal chain from diag7)

diag7b (run 20260903-121002, job 1b68d3c5, source avg 5,344/s at 10Hz feed) proved:

1. The source drain loop calls `canEmitBatchOfRecords.check()` per poll
   = `!hasMail() && taskIsAvailable()`.
2. Non-urgent mailbox mail (200ms auto-watermark timer, latency/resume
   wakeups) arrives ~5/s/subtask → the drain gate-stops every ~200ms at
   **27-55 records** despite a backed-up source.
3. The reader-side 2-slot FutureCompletingBlockingQueue fills faster than
   the chopped drain empties it → **70% of Fluss fetcher puts block,
   median 2.3s** (qsize=2 cap=2).
4. Fetch cadence == put-block time == the throughput ceiling.

Option B (this experiment): when `pipeline.source.bounded-drain.enabled=true`,
the drain yields ONLY to URGENT mailbox mail (checkpoint barrier/cancel/stop)
and continues past non-urgent mail up to a configurable record quantum per
emitNext call. Claim: the queue stops saturating, put-block → 0, fetch cadence
rises, sustained throughput rises toward the 10,240/s feed.

## 2. The change (built, unit-tested, NOT deployed)

6 files in flink-runtime/flink-core (release-2.2.1 worktree, HEAD 450c63e9618):

| File | Change |
|---|---|
| PipelineOptions.java | +3 ConfigOptions: `pipeline.source.bounded-drain.enabled` (bool, default false), `.max-ms` (int, default 5 — the HARD per-drain time budget), `.max-records` (int, default 4096 — secondary guard) |
| TaskMailbox.java | +`hasUrgentMail()` interface method |
| TaskMailboxImpl.java | +impl: `hasNewUrgentMail \|\| batchHead.isUrgent()` (covers queue flag AND mailbox-thread putFirst fast-path to batch head) |
| MailboxProcessor.java | +`hasUrgentMail()` delegation |
| StreamTask.java | +`boundedDrainEnabled` (final, read once from job config in ctor); getCanEmitBatchOfRecords returns bounded checker `!hasUrgentMail() && taskIsAvailable()` when on, STOCK lambda when off |
| SourceOperator.java | +quantum counter + nanoTime start reset per emitNext; boundedDrainQuantumExceeded() = elapsed>=maxMs (HARD) OR counter>=maxRecords (secondary); config read in setup() from job config; IllegalArgumentException if max-ms<=0 OR max-records<=0 when enabled |

New test: TaskMailboxHasUrgentMailTest (6 tests, all green standalone).

Bytecode: recompiled `--release 11` (major 55 = matches dist). Dist swap jar
built: `/tmp/bd-dist/flink-dist-2.2.1-optb.jar` sha `5c1a0ca2…`
(stock `4c1afceccc6081c8…` pristine from container).

No duplicate-class shadowing risk (verified): the 6 changed packages exist ONLY
in flink-dist; connector-files duplicates only connector-base/sink packages.

## 3. Deployment mechanics for the A/B (TM-only swap)

The changed classes execute on the TaskManager (task threads). The JobManager
loads flink-dist for submission but does not run source drains. Prior diag
used TM-only swaps; JM class version is irrelevant to behavior. Still, both
containers mount the same image; to keep JM/TM identical (avoid any
classpath-order confusion) we swap flink-dist in BOTH containers.

Swap procedure (per arm):
```
docker cp /opt/flink/lib/flink-dist-2.2.1.jar <host backup>   # pristine captured already
docker cp <arm-jar> 01_docker-flink-jobmanager-1:/opt/flink/lib/flink-dist-2.2.1.jar
docker cp <arm-jar> 01_docker-flink-taskmanager-1:/opt/flink/lib/flink-dist-2.2.1.jar
docker restart 01_docker-flink-jobmanager-1 01_docker-flink-taskmanager-1
# wait for JM REST /overview + TM registration (B5)
```

Stock arm uses the STOCK dist (flag off is belt-and-braces; the optb jar with
flag off is byte-identical behavior — the flag gates every behavior change).

## 4. Controlled A/B protocol (ONE comparison, per user req #7)

Arm order: **Stock first, then Option B** on the SAME host, SAME config, SAME
fresh tables, SAME DURATION_S, SAME RATE_HZ=10 (10,240/s feed).

Per arm, exact sequence (drives stage-a2-baseline.sh primitives):

1. Ensure host uptime >= MIN_UPTIME_S (600s) — power-cut guard.
2. Purge raw + preview tables (fresh, no backlog — measures steady state).
   NOTE: ingestion restart after purge is handled by the runner (pipeline
   restarts ingestion with the fresh table).
3. `pipeline_preflight` (validates jars/bridge/manifest/ports, restarts TM
   fresh, waits registration).
4. faketool feed (10Hz x 1024 = 10,240/s), ingestion, SignalJob submit.
5. Stock arm: submit with NO bounded-drain flags (pure stock dist).
   Option B arm: submit with
   `-Dpipeline.source.bounded-drain.enabled=true`
   `-Dpipeline.source.bounded-drain.max-ms=5`
   `-Dpipeline.source.bounded-drain.max-records=4096`.
6. Capture DURATION_S (default 720s = 12 min) via stage-capture.
7. Collect: stages.tsv (per-operator numRecordsIn/Out, busy/backpressured/idle),
   consumer-read.tsv (read lag), watermark-lag.tsv, checkpoints.jsonl,
   ingestion.tsv + java.out counters, prom snapshots, io-latency.
8. Cleanup (cancel job, stop feed+ingestion).

Arm A/B = 2 runs, ~25-30 min wall each incl. preflight + purge + submit.

## 5. Metrics and gates (from the 10 acceptance criteria)

Primary throughput: SignalJob source vertex numRecordsIn avg/s (stages.tsv
numRecordsIn delta / epoch delta) — G25 floor is 8192/s (80% of 10,240/s).

Secondary (causal-chain validation):
- Fluss source fetch cadence / put-block: NOT instrumented in this run
  (diag7 jar not deployed). Proxy: if throughput rises, the queue no longer
  saturates; put-block is the proven mechanism. Full chain instrumentation
  would need the connector-files diag classes — deferred unless A/B is
  inconclusive.
- Data age / latency: consumer-read.tsv read_lag_ms, watermark-lag.tsv,
  io-latency.tsv p50/p95/p99.
- Backlog: consumer-read.tsv last_event_ts vs output_ts gap.
- Checkpoint health: checkpoints.jsonl completed count, duration, no failures
  (EXACTLY_ONCE preserved; urgent barrier mail still preempts).
- Watermark cadence: watermark-lag.tsv (the 200ms timer mail is deferred ≤1
  quantum; watermark still emits at next drain return — lag should be ≤ one
  drain quantum ~ ms, not seconds).
- No-loss/no-dup: Signal_Candidates rows vs source records (existing G7c
  end-state integrity + dedup state evidence).

## 6. Success criteria (req #6/#7 — claim success ONLY if)

Do NOT claim Option B successful merely because throughput increases. Success
requires the conjunction:

- higher sustained throughput (source avg >= stock arm; G25 floor 8192/s on the
  Option B arm) AND
- lower/stable latency (tick p50/p95/p99/max, data age p50/p95/p99/max do NOT
  regress; ideally fall) AND
- low data age + backlog near zero AND
- correct checkpoints/watermarks: checkpoint trigger delay p50/p95/p99/max not
  inflated; checkpoint completion/timeout healthy; watermark progression intact;
  SourceIdleWatchdog unchanged (15s/60s thresholds) AND
- zero loss (Signal_Candidates rows match source records within dedup semantics)
  AND zero duplicates.

Report the causal chain changed:
BEFORE: mailbox chop -> 27-55 records/drain -> queue full -> fetcher blocked
        (put-block median 2.3s) -> low throughput / high data age.
AFTER:  bounded drain (5ms/4096 quantum) -> larger drain quanta -> queue stays
        available -> fetcher blocking -> 0 -> throughput rises -> data age falls.
If the expected chain does not change, do NOT claim the fix solved the root
cause.

Any arm that fails G25 is a POISONED baseline (per runner semantics) — rerun
that arm, do not compare across a poisoned baseline.

## 7b. Metric report (mandatory, all together — per user approval)

Throughput: source records/s, downstream records/s, sustained rate, backlog
growth. Latency/freshness: tick latency p50/p95/p99/max where available, data
age p50/p95/p99/max, oldest-record age. Source mechanism: drain records per
emitNext, drain duration, queue occupancy, queue-full put blocking, fetch
cadence/rate (needs the diag instrumentation — see note). Correctness/control:
checkpoint trigger delay p50/p95/p99/max, checkpoint completion/timeout,
watermark progression, SourceIdleWatchdog behavior, loss=0, duplicates=0.

## 7. Risk controls / rollback

- Opt-in flag default OFF; stock behavior byte-identical when off (verified
  by code path + unit tests).
- If Option B regresses (throughput down / latency up / checkpoint breakage),
  swap the STOCK dist back and rerun stock to confirm baseline — one restart.
- The experiment changes NO persistent config; table purge is the only
  destructive step (already standard for every stage run).
- Worktree /tmp/flink-221-exp is isolated; master checkout untouched.
- After the A/B: restore the stock dist in both containers (pristine jar
  preserved at /tmp/bd-dist/flink-dist-2.2.1-stock.jar).

## 8. Preconditions checked before starting

- [x] Live table raw_table_1-112, ingestion healthy, no SignalJob running.
- [x] Stock dist pristine sha captured (4c1afceccc…).
- [x] Optb dist built (5c1a0ca2…) + 6 unit tests green.
- [x] No duplicate-class shadowing (only flink-dist owns the 6 packages).
- [x] Host uptime gate + purge/ingestion-restart handled by runner.
- [ ] USER APPROVAL to start the A/B runs (this document).

## 9. Post-A/B actions (after results reviewed, NOT now)

- Report before/after (req #7) with the metrics above.
- Only if the causal chain is proven: quantum sweep (req #4) as a SEPARATE
  approved experiment (e.g. max-records 512/4096/16384, maybe a time budget).
- Decision on making it permanent (needs upstream discussion + default-off
  remains the safe posture; the flag stays experimental).

## Appendix: exact files/artifacts

- Sources: /tmp/flink-221-exp/{flink-core,flink-runtime}/.../ (git diff clean, 6 files + 1 test)
- Compiled classes: /tmp/bd-classes (major 55)
- Dist jars: /tmp/bd-dist/flink-dist-2.2.1-{stock,optb}.jar
- Harness: code/01_platform/04_scripts/{pipeline-lib,stage-a2-baseline}.sh
- Prior evidence: logs/tracker-14/stage-a2-baseline-20260903-121002/ANALYSIS-diag7.md
