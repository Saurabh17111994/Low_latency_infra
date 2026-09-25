# Signal raw-source latency tuning (S5→S6)

**Date:** 2026-09-26
**Status:** implement + smoke-verify (operator go-ahead 2026-09-26)
**Baseline evidence:** `logs/stage-profile-20260926-002322/profile.md` (2433 instruments @ 20 Hz, 900 s)

## 1. Problem (measured, not inferred)

S6 (`raw_table_1 → Flink post-dedup`) p50 = **316 ms** against S5 (ingestion
end-to-end) 27 ms. Decomposition from the raw Prom snapshots in
`logs/stage-profile-20260926-002322/main/stages/prom-*.txt`:

| term | p50 | evidence |
|---|---|---|
| Java accept → Fluss append ack | ~11 ms | `append.latency.ms` |
| **waiting inside Fluss + source fetch queue** | **~260 ms** | source `pendingRecords`/`recordsLag` 0–958 records; `currentEmitEventTimeLag` 81–648 ms; `pollIdleRatio` 0.13–0.5 |
| source emit → dedup (one shuffle hop) | ~45 ms | Flink latency tracker (`fingerprint_dedup____ingest_latency_monitor`) |
| dedup + monitor compute | ~1–2 ms | `busyTimeMsPerSecond` ≈ 50/1000 |

The job is **not** compute-bound: every operator is 88–95 % idle
(`busyTimeMsPerSecond` 8–116 of 1000), backpressure ≈ 0 everywhere, and no
operator after the source holds pending records. The cost is cadence:
(a) the Fluss log scanner's fetch chunk (job sets no scanner options →
client defaults 16 MiB request / 1 MiB per bucket / 500 ms wait) and
(b) Flink's output-buffer flush timeout (nothing in the repo sets it →
default 100 ms) at every keyBy shuffle hop.

## 2. Change — native knobs only, no topology/state change

| knob | old (default) | new | set in |
|---|---|---|---|
| `execution.buffer-timeout` | 100 ms (Flink) | **10 ms** | `SignalJob.applyRuntimeOptions` (typed `ExecutionOptions.BUFFER_TIMEOUT`) |
| `client.scanner.log.fetch.max-bytes` | 16 MiB (Fluss) | **512 KiB** | `SignalJob.flussSourceConfiguration` (typed `ConfigOptions`) |
| `client.scanner.log.fetch.max-bytes-for-bucket` | 1 MiB (Fluss) | **128 KiB** | same |
| `client.scanner.log.fetch.wait-max-time` | 500 ms (Fluss) | **20 ms** | same |

Env seam (declared in `ConfigKeys`): `BUFFER_TIMEOUT_MS`,
`FLUSS_SCANNER_FETCH_MAX_BYTES`, `FLUSS_SCANNER_FETCH_MAX_BYTES_FOR_BUCKET`,
`FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS`. Defaults carry the tuned values;
`pipeline-lib.sh` passthrough is a follow-up (defaults run unchanged through
the existing submit path).

## 3. Expected effect

- S6 p50: 316 ms → **~60–110 ms**; floor of this architecture ~40–60 ms.
- Per-hop tracker deltas: ~40–55 ms → ~5–10 ms (sinks 198 → ~70–100 ms p50).
- Throughput parity required: ≥ 48.4k rows/s post-dedup at 20 Hz.
- p99 tails (599–978 ms) likely need a separate checkpoint review (10 s
  interval, maxConcurrent 1) if they persist.

## 4. Verification plan

1. Unit: `SourceFetchLatencyTuningTest` — defaults, env overrides, fail-closed
   pair (`perBucket ≤ maxBytes`), the Fluss Configuration keys, and
   `execution.buffer-timeout` in the Flink Configuration.
2. Smoke (this workstream): `PHASES=smoke SMOKE_S=200 bash
   code/01_platform/06_stage_profiler/stage-profile.sh`; compare S6/S7 plus
   source `recordsLag` / `currentEmitEventTimeLag` against the baseline.
3. Full 900 s run for the record once the smoke passes.

## 5. Risk + rollback

- Rollback = env values back to the old defaults (or revert the commit);
  no checkpoint-format, UID, or schema change — client fetch sizing and one
  runtime option only.
- Cost: more fetch requests and more frequent buffer flushes. The measured
  job had > 85 % idle CPU in every operator and a Fluss tier that appends
  48.5k rows/s at p50 11 ms — the additional RPCs are inside that budget.
- Revert trigger: smoke shows throughput loss or S6 regression.

## 6. Verification results (2026-09-26 — both runs green)

Smoke `logs/stage-profile-20260926-015049` (200 s) + record run
`logs/stage-profile-20260926-015722` (900 s), full universe @ 20 Hz:

| metric | baseline 002322 (900 s) | smoke (200 s) | record 015722 (900 s) |
|---|---|---|---|
| S6 p50 / p95 / p99 (ms) | 316 / 543 / 599 | 26 / 41 / 49 | 27 / 43 / 55 |
| S7 slowest operator p50 / p99 (ms) | 198 / 978 | 23 / 1068 | 15 / 1134 |
| S9 whole path p50 / p99 (ms) | 198 / 978 | 23 / 1068 | 21.5 / 1124 |
| S6 post-dedup throughput (rows/s) | 48 398 | 48 658 | 48 831 |
| S1 / S5 p50 (ms) | 18 / 27 | 15 / 26 | 17 / 21 |
| source `pendingRecords` max per snapshot | 1 673 | 92 | 21 129 (one startup transient; ≤ 608 after) |
| tracker hops p50: dedup / aggregator / live / closed (ms) | 37–45 / 76–94 / 116–148 / 172–198 | 4–6 / 10 / 14–18 / 18–23 | 4–5 / 9–10 / 13–16 / 18–21.5 |

- Attribution: every shuffle hop fell by ~40–50 ms — the buffer timeout's
  average wait (100→10 ms) — and the source backlog collapsed (final
  snapshots 0.0 pending on all 8 subtasks) — the scanner fetch cap.
- Throughput parity holds (48.4k → 48.8k rows/s post-dedup), upstream stages
  at parity or better.
- The residual p99 tail (~1.1 s at the sinks) is checkpoint-dominated:
  15 completed checkpoints in the 900 s run, p50 680 ms / max 1 916 ms
  end-to-end, interval 60 s in the submitted job (the submit path defaults
  `CHECKPOINT_INTERVAL_MS=60000` in `pipeline-lib.sh` while compose/dossier
  pin 10000 — observed as-is, recorded here, not changed by this workstream)
  — a separate lever (interval / max-concurrent / alignment), not this change.
- Verification: `SourceFetchLatencyTuningTest` 6/6; full compute module suite
  green (76 report files, 0 failures); common `ConfigGuardTest` green;
  profiler 36/36 offline; `docs-audit` C6 re-measured compute 532→538.
