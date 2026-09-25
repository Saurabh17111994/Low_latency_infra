# Single end-to-end stage profiler — native latency + throughput (full universe)

**Created:** 2026-09-25 · **Rev 3** (single-test restructure; supersedes Rev 2's two-change
split) · **Status:** DRAFT — awaiting operator approval · root `AGENTS.md` wave discipline.

## 1. The ask — one test

One command, one run, one report:

```
make stage-profile
```

runs the whole chain at the **full 2 433-stock NSE universe** —

```
fake broker → Go bridge/SDK → Java ingestion → Fluss raw_table_1 → Flink → Fluss feature tables
```

— and produces a **step-by-step profile** (latency + throughput per step) plus the
**end-to-end** latency and throughput, from native instrumentation only.

- `holistic-measure.sh` and `stage-soak-e2e.sh` **are not modified** — they keep their current
  jobs (candle-quality gates; full-universe correctness).
- No Java / Go / Flink / DDL changes — every meter already exists (§3).
- Existing gate suites are **not migrated** — changes are strictly additive (§8).

## 2. Why the profiler is a new single test (not an edit of the two harnesses)

| Candidate | Why not the profiler |
|---|---|
| `holistic-measure.sh` | It is a candle-quality harness: injection gates, G7 dedup/late/parity, preview semantics, hard-locked to 1 024 tokens; extending it drags quality-gate semantics and its 55-test suite into a profiling change. |
| `stage-soak-e2e.sh` | It is the full-universe **correctness** run (tick losslessness, N7, O2 evidence); its 33-test suite is extraction-anchor based, so moving its code forces a test migration. |

Both stay as-is. The profiler reuses their shared foundations: `pipeline-lib.sh` (one
bring-up path), `stage-capture.sh` (the aligned per-stage collector built for exactly this),
and the Fluss probes. **One new thin orchestrator + one new report; nothing else moves.**

## 3. Verified native sources (no new meters needed)

| Step | Latency (native) | Throughput (native) |
|---|---|---|
| S1 broker → Java accept | raw rows `ingest_ts − event_time` (bounded sample; exclude injected late ticks — none here, no injection) | bridge tick counts per container + Java `tick.throughput` delta |
| S2 Go bridge internal | `stage.ipc_latency` (Go emit − Go recv) | Go emit rate |
| S3 Java decode/batch/route | `stage.decode_latency`, `stage.batching_latency`, `stage.routing_latency` | `tick.throughput` delta |
| S4 Java → raw ack | `stage.fluss_submit_latency` / `stage.fluss_ack_latency` / `append.latency.ms` | append histogram count delta |
| S5 ingestion e2e | `stage.end_to_end_latency` (broker eventTime → ack) | append count delta |
| S6 raw → Flink post-dedup | `compute.latency.ingest_to_monitor` histogram (exact, every tick) | raw source read delta |
| S7 Flink per-operator | tracker `latencyP50/P95/P99` per operator | per-operator read/write deltas |
| S8 Flink → feature tables | sink tracker percentiles + window-close→committed / settlement (row stamps) | sink write deltas + feature row counts |
| S9 whole path (e2e) | tick eventTime → feature row `output_ts` (`closed-read.tsv` / bounded sample) | raw rows/s vs feature rows/s |

Line evidence: `OtlpMetricsEmitter` L309-314 + `IngestionService` L1072-1075/L1353-1364;
`IngestLatencyMonitorFunction` L24-47; raw DDL L82-103; `stage-capture.sh` L1-35 (outputs);
`holistic-analyze.py` L81/L128/L914-948/L1621 (the percentile methodology to reuse).
Java histograms carry p50/**p90**/p99 (labeled as such); Flink carries p50/95/99. Percentiles
are reported per container (never pooled).

## 4. The single test — flow

```
make stage-profile
  ├─ preflight        (lib): Fluss ready, fresh TM, loadgen image fresh, purge raw + candles + signals
  ├─ bring-up         (lib fleet): 1 faketool (fake broker) + 3 ingestion containers (3×811) + SignalJob
  ├─ warm-up          45 s; raw-path progress > 0
  ├─ SMOKE            120 s aligned capture (stage-capture) + feed counts
  │    └─ presence gate  FAIL-CLOSED: every step in §3 has samples; raw + feature tables grow;
  │                      job RUNNING; else stop (no confident-looking profile)
  ├─ MAIN             900 s aligned capture (stage-capture, all 3 java.out, monitor histogram)
  ├─ report           profile.md + stage-profile.tsv + stage-timeline.tsv
  └─ teardown         (lib) cancel job, remove fleet + faketool, drain tick counts
```

Defaults: full NSE file (`$ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv`,
2 433 rows), `RATE_HZ=20` (48 660 ticks/s — the point the 2026-09-04/05 soaks already ran),
`SMOKE_S=120`, `MAIN_S=900`. Overrides: `NSE_PATH` (e.g. the 1 024 CSV for quick checks),
`RATE_HZ`, phase lengths. Evidence: `logs/stage-profile-<ts>/`.

The report answers, in one table per run:

```
STEP  BOUNDARY                     THROUGHPUT      p50    p90    p95    p99    n
S1    broker -> Java accept        ... ticks/s     ...ms  ...    -      ...
...
S5    ingestion e2e                ... appends/s   ...
S9    e2e tick -> feature row      ... rows/s      ...
```

## 5. Components — what is new, what is extended (all additive)

| File | Change | Why |
|---|---|---|
| `code/01_platform/04_scripts/stage-profile.sh` | **NEW** — orchestrator: fleet bring-up via lib, smoke gate, capture, report, teardown; shellcheck-clean | the single test |
| `code/01_platform/04_scripts/stage_profile.py` | **NEW** — merge + percentile math + presence check + report; pure functions, runs standalone over any evidence dir | offline-testable profiling report |
| `code/01_platform/04_scripts/stage-capture.sh` | **EXTEND (strictly additive):** `INGESTION_JAVA_OUT` accepts multiple paths (singular still works); scrape `compute.latency.ingest_to_monitor` quantiles + source lag; bounded raw-lag sample leg (`raw-lag.tsv`) | reuse the aligned collector for all 3 containers + missing S1/S6 sources |
| `code/01_platform/04_scripts/pipeline-lib.sh` | **EXTEND (strictly additive):** new `pipeline_start_ingestion_fleet` (universe CSV → 3×811 slice manifests, per-container logs/readiness, divisibility fail-closed) + cleanup reaps fleet containers; **existing functions, names and literals untouched** | one shared bring-up; no 1 024-lock workaround |
| `Makefile` + `docs/commands/COMMANDS.md` | add `make stage-profile` (+ usage) | single entry point |
| `code/01_platform/04_scripts/tests/test_stage_profile_wave*.py` | **NEW** — registry/report golden tests, presence gate, script guards | joins gate step 3 automatically |
| `test-pipeline-lib.sh` | **ADDITIVE guards** for the fleet function (existing 131 guards untouched) | gate step 0a keeps pinning the lib |

**Not touched:** Java ingestion, Go bridge, Flink job, DDL/schema, `holistic-measure.sh`,
`stage-soak-e2e.sh`, `holistic-analyze.py`, `gate` step count (`GATE_TOTAL=19`).

## 6. Commit slices (ONE change, ONE certification)

1. **Lib fleet function** + additive guards. Existing suites must stay green by construction.
2. **Collector legs** (multi-java.out, monitor histogram, bounded raw-lag sample) + tests.
3. **`stage_profile.py`** + offline golden tests (fed from the existing
   `logs/tracker-14/holistic-measure-20260923-*` and `logs/soak-e2e-*` dirs).
4. **`stage-profile.sh`** + Makefile/COMMANDS + new pytest.
5. **Live certification + docs/CHG:** quick 1 024 smoke → full-universe 120 s smoke → 900 s
   main → one repeat → budgets pinned → `make gate` (scoped first, full to certify) →
   `03-ingestion.md` / `10-observability.md` / `11-testing-and-release.md` updates.

Thresholds: first full run is **record-only**; budgets are pinned from its percentiles and
enabled in a follow-up commit inside the same change (budgets that fail the run when a step
regresses, same rc contract as the existing guard family).

## 7. Verification plan

1. **Offline:** golden tests; `stage_profile.py` replayed over existing evidence dirs —
   overlap with already-recorded numbers must match.
2. **Quick run:** `NSE_PATH=…/NSE_CM_EQUITY (1024).csv RATE_HZ=10` smoke+main (~6 min) — proves
   the whole flow cheaply (the mandatory smoke for the long run).
3. **Full-universe smoke:** 120 s, 3×811 @20 Hz — every step present, ticks uniform, then stop.
4. **Certification run:** 900 s @20 Hz (48 660 ticks/s) → `profile.md` with S1–S9.
5. **Repeat** once for variance, then pin budgets.
6. `make gate` scoped `--steps` first, full to certify; evidence under `logs/`.

## 8. Existing-test impact (verified)

**None of the existing suites is modified by this change.** It is additive by design:

| Existing suite | Why it stays green |
|---|---|
| `test_pipeline_lib_hardening.py` (39) | legacy `pipeline_start_ingestion` and its literal mount/names are untouched; the fleet function is new |
| `test-pipeline-lib.sh` (131 guards, gate 0a) | existing guards pin existing functions; new guards are added alongside |
| `test_stage_capture_parse.py` (13) / `test_stage_capture_wave38.py` (17) | the collector interface stays backward-compatible (singular `INGESTION_JAVA_OUT`, same files/columns); new columns/files are additive |
| `test_stage_soak_e2e_wave32.py` (33) | soak script untouched |
| `test_holistic_*` (55+7+62+11) | holistic untouched |
| `test_gate_harness_guards_wave36.py` (10) | Makefile/COMMANDS additions are new entries, not renames |

Rule for the implementation: **if any "additive" change turns out not to be additive, the
slice stops and this plan returns for review** — no silent test migration.

## 9. Risks

- **Runtime:** ~20 min per full run (smoke + main + warm-up/teardown); repeat runs optional.
- **Raw-lag sampling (S1):** bounded sample windows (size recorded in the report), not a full
  table read (43.8 M rows at 20 Hz/900 s); cross-checked against `end_to_end − append`.
- **Resource:** 3 ingestion JVMs (512 MB) + faketool + Flink TM — proven on this box by the
  2026-09-04/05 soaks; no prod state touched.
- **Presence-gate strictness:** thresholds (sample counts per step) start conservative and are
  tuned with the first baseline; it fails runs, never fabricates rows.

## 10. Approvals / out of scope

- Approvals: this plan; the live dev-stack runs; commits. One change, one certification.
- Out of scope: modifying holistic/soak, production envelope (DEC-037), Executor/Arrow API
  hop, tiering/lake perf, any new Flink operator or schema change.

**Done = `make stage-profile` produces the full-universe step-by-step + e2e profile from
native sources; the smoke gate refuses a run with a missing step; the gate is green with the
new tests inside it.**
