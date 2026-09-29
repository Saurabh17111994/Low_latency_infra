# Stage profiler — one test, one command, one run, one report

Greenfield: **every file in this folder is new.** No existing file is edited,
refactored, or migrated. The profiler *drives the existing pipeline as a black
box* (same images, jars, tables, endpoints) and *reads the native
instrumentation already in it* — it changes nothing on the data path.

## The one command

```
bash code/01_platform/06_stage_profiler/stage-profile.sh
```

One run does everything:

```
preflight  -> stack checks (Fluss ready, fresh TM, image stamp, tables purged)
bring-up   -> fake broker + 3 ingestion containers (3x811) + SignalJob
warm-up    -> 45 s, raw path must progress
SMOKE      -> short aligned capture; feeds the presence gate
presence   -> FAIL-CLOSED: every stage must have samples, or the run stops
MAIN       -> full aligned capture (default 900 s)
report     -> profile.md + profile.tsv + presence.json in the evidence dir
teardown   -> cancel job, remove containers, drain tick counts
```

Defaults: full universe (`NSE_CM_EQUITY.csv`, 2 433 stocks), `RATE_HZ=20`
(48 660 ticks/s), `SMOKE_S=120`, `MAIN_S=900`. Overrides: `NSE_PATH`,
`RATE_HZ`, `SMOKE_S`, `MAIN_S`, `OUT`.

Evidence: `logs/stage-profile-<timestamp>/`.

## Real broker mode (`FEED=real`, 2026-09-26)

Same ladder, same report — the data comes from the real broker DataStream
(`wss://ds.arrow.trade`, `ARROW_FEED=token`, mode `full`) instead of the fake
broker:

```
FEED=real INGESTION_CONTAINERS=1 \
  bash code/01_platform/06_stage_profiler/stage-profile.sh
```

- **One container, N slots.** The bridge caps one connection at 1 024 tokens
  (`subscription_plan.go`), so the 2 433-stock universe runs as
  `slots = ceil(rows/1024)` = 3 bridge connections inside that one container
  (3 × 811), with `ARROW_HFT_MULTI_CONNECTION_APPROVED=true` (dev-only policy
  gate; production rejects multi-connection). The full `NSE_PATH` file is used
  unfiltered — no per-slice manifests, no token filtering.
- **Market hours only.** Outside 09:15–15:30 IST the DataStream re-sends stale
  snapshots, which the ingestion staleness gate (`ARROW_MAX_EVENT_AGE_MS`)
  drops; `FEED=real` therefore refuses to start outside the session unless
  `ALLOW_OFFHOURS_REAL=1`.
- **Off-hours wiring dry run** (no capture, no presence claims):
  `FEED=real INGESTION_CONTAINERS=1 ALLOW_OFFHOURS_REAL=1 BRINGUP_ONLY=1 …`
  starts the fleet, waits for the readiness marker and for **all** slot
  subscription confirmations, then tears down.
- Real cadence in `full` mode is ≈1 Hz per token (measured 2026-09-24), so
  throughput is not comparable with the 2 Hz fake ladder; the report title and
  the capture `rate_hz` label say "real broker DataStream full".

## What it measures (S1..S9)

| Step | Boundary | Latency source | Throughput source |
|---|---|---|---|
| S1 | fake broker -> Java accept | raw rows `ingest_ts - event_time` (bounded sample) | bridge tick counts + `tick.throughput` delta |
| S2 | Go bridge internal (recv -> emit) | `stage.ipc_latency` | bridge emitted ticks |
| S3 | Java decode / batch / route | `stage.decode_latency`, `stage.batching_latency`, `stage.routing_latency` | `tick.throughput` delta |
| S4 | Java -> Fluss append ack | `stage.fluss_submit_latency`, `stage.fluss_ack_latency`, `append.latency.ms` | append count delta |
| S5 | ingestion end-to-end (event time -> ack) | `stage.end_to_end_latency` | append count delta |
| S6 | raw table -> Flink post-dedup | `compute.latency.ingest_to_monitor` summary quantiles | raw source records delta |
| S7 | Flink per-operator | tracker latency percentiles per operator | `numRecordsIn/Out` delta |
| S8 | Flink -> feature tables (window close -> readable) | first-seen minus window end on candle polls | sink records delta + feature row counts |
| S9 | whole path (event time -> feature row) | last-operator tracker latency | raw rows/s vs feature rows/s |

Honesty rules (non-negotiable): Java histograms carry p50/p90/p99 — no p95;
values are reported **per container**, never pooled; a metric that is absent is
printed as `—`, never as 0; the presence gate refuses a run with an empty leg.

Session rule: the IST clock decides `MULTITF_SESSION_BYPASS` (default **true**
when the 09:15–15:30 IST session is closed), exactly as `holistic-measure.sh`
does, so the candle/signal path aggregates off-hours. Production never
bypasses; `session.txt` in the evidence dir records the decision.

## Deliberate non-goals (the "do not touch" contract)

- No edits to `holistic-measure.sh`, `stage-soak-e2e.sh`, `stage-capture.sh`,
  `pipeline-lib.sh`, `holistic-analyze.py`, the Makefile, any existing test,
  any Java/Go/Flink code, any DDL.
- No live step inside `make gate` (a 20-minute live run is not a gate step).
  The offline half (`stage_profiler.py`) is unit-tested in this folder.
- No `make stage-profile` target — that would edit the existing Makefile. Add
  it later only on explicit approval.

Reuse is by invocation only: the fleet starts with the same `docker run`
command lines the 2026-09-04/05 full-universe soaks proved, and collection
invokes the existing Fluss probes the same way `stage-capture.sh` does.

## Layout + evidence

```
code/01_platform/06_stage_profiler/
  stage-profile.sh      the one command (orchestrator)
  stage_profiler.py     pure core: java.out OTLP parsing, percentiles,
                        presence gate, report render
  RawSampleReader.java  S1 bounded tail sample of partitioned raw_table_1
                        (new: the stock FlussPrefixReader cannot read a
                        partitioned LOG table in Fluss 1.0)
  tests/                offline unit tests (36, no stack needed)
```

Per-subtask checkpoint phases come from `04_scripts/cp_phase_capture.py`, run
by `stage-profile.sh` for the capture window (CT-1 of
`docs/plans/2026-09-29-checkpoint-tail-remediation.md`); the summary endpoint's
phase fields are null, and the details endpoint is only fetchable while the job
is alive.

```
logs/stage-profile-<ts>/
  smoke/                smoke phase evidence + the presence gate verdict
  main/                 main phase evidence
    capture/j1-{0..2}/java.out   Java OTLP logs, one per container
    capture/raw-sample.jsonl     S1 sample rows (RawSampleReader)
    capture/ticks/               per-container tick counts (teardown)
    stages/                      stage-capture.sh evidence (prom, probes, ...)
                                 + cp-phases-detail.jsonl (per-checkpoint,
                                   per-subtask phases; cp_phase_capture.py)
  profile.md profile.tsv presence.json    the single report
```

## Offline use (no stack needed)

```
python3 stage_profiler.py stages                 # print the S1..S9 registry
python3 stage_profiler.py java <java.out>        # one container's Java stages
python3 stage_profiler.py presence --phase DIR   # smoke gate on any phase dir
python3 stage_profiler.py report --phase DIR --out DIR2 --title T
CHECK_ONLY=1 bash stage-profile.sh               # validate inputs, start nothing
```

The report reads any directory with the same shape; it was validated offline
against `logs/soak-e2e-20260905-181803` before the first live run.
