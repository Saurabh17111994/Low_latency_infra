# Measurement coverage — what the certification proves and what it does not

**Date:** 2026-10-03
**Status:** diagnosis recorded 2026-10-03 (deep pass over every measurement facility vs the full
production path and the platform SLO table). Planning only — items start per item with the
operator's go-ahead; each adopted KPI lands with probe/test + CHG record + evidence under `logs/`
per the repo rules.
**Origin:** the question "does the certification measure everything we need?" raised during the L4
changelog certification (`logs/stage-profile-20261003-l4-cert`). Answer: it is a deep but narrow
slice — ingestion → Flink → feature/signal tables, dev single-node, synthetic 2 Hz feed.
**Standing goal (operator):** low latency + high throughput + less state growth + data correctness —
and a measurement picture complete enough that no SLO boundary is invisible.
**Baseline evidence for this diagnosis:**

- `docs/08_implementation/10-observability.md` §SLO boundaries (the 7 declared SLOs) and §Required
  telemetry by area (the declared telemetry catalog).
- Stage profiler registry S1–S12 (`code/01_platform/06_stage_profiler/stage_profiler.py`).
- `code/01_platform/04_scripts/holistic-measure.sh` (signal preview/tentative/settlement latency).
- `docs/08_implementation/22-failure-chaos-suite.md` (slot/TM/tablet/VM kill drills).
- Soak tooling (`stage-soak-e2e.sh`, `soak-monitor.sh`, `soak-headroom.sh`, `soak-reconnect-loop.sh`).

**Already tracked elsewhere — that plan stays the authority (not re-opened here):**

| Aspect | Authority |
|---|---|
| Real-broker (DataStream) feed profiling — market-hours full-universe measurement pending | `docs/plans/2026-09-26-real-broker-datastream-profiling.md` |
| Execution paper/sandbox hop readiness (H2-1…H2-5 landed; H3-2, P4-4 pending) | `docs/plans/2026-09-27-execution-mode-hop-paper-sandbox.md` |
| Readability + state-growth facility (S10–S12, CHG-461) | `docs/plans/2026-09-30-readability-and-state-growth-measurement.md` |
| Ingest → strategy KPI extraction method | `docs/plans/2026-10-01-ingest-to-strategy-kpi.md` |
| Latency levers W1–W6 (buffer, fetch window, materialization) | `docs/plans/2026-09-30-p99-50ms-normal-path.md` |
| Checkpoint tail CT-1…CT-7 | `docs/plans/2026-09-29-checkpoint-tail-remediation.md` |

## Next actions (in order)

1. **Wave 0 (P0 — cheapest, highest value):** M2.1 load ramp → M1.2 decision→readable KPI →
   M4.1 cold SSD/RocksDB read → M1.1 execution-chain timing. Each: smoke first, then the run;
   evidence in `logs/`.
2. **Wave 1 (P1):** M3.1 chaos under load with recovery-time KPIs → M4.2 KV lookup latency →
   M5.1 hours-long state trend → M6.1 data-level end-to-end verification.
3. **Wave 2 (P2):** M1.3–M1.5, M2.2, M3.2–M3.3, M4.3–M4.5, M5.2–M5.3, M6.2; M2.3 multi-node last
   (needs the 4-VM rig).

## 0. Live tracker

#### M1 - Execution last mile (path coverage)

- [ ] **M1.1** Execution-chain timing KPI — signal row committed → gateway → executor/Nautilus →
  broker ack/fill, per-hop p50/p99 plus end-to-end. Why: the trading path ends at broker ack;
  today's KPIs stop at the signal table. How: reuse the H2-5 paper sandbox path
  (`code/01_platform/04_scripts/t9_order_sandbox.py`) with timestamps at each hop; executor/gateway
  telemetry seam (`TelemetrySink`) already exists. Evidence: dated drill log with percentiles.
- [ ] **M1.2** Decision → readable KPI — candle close → `Signal_Candidates` row first-read by a
  fresh reader, percentile per window. Why: "decision committed" is a declared SLO boundary;
  `consumer-read.tsv` gives ~1.5 s read lag but no percentile KPI. How: point the
  `FlussReadabilityProbe` closed leg (or a dedicated probe) at `Signal_Candidates`; join with
  holistic-measure settlement latency. Evidence: profiler report row + run log.
- [ ] **M1.3** Arrow REST round trip — order request start → verified broker response, reported
  separately per the SLO table. How: sandbox probe over the paper path.
- [ ] **M1.4** Safe-halt latency — uncertainty detected → calls blocked < 5 s. How: execution-gate
  fault injection on the sandbox path; measure gate transition time.
- [ ] **M1.5** EOD / lake path — market close → verified manifest < 30 min; Iceberg commit timing.
  How: eod-controller evidence with timing; not in the latency certification.

#### M2 - Load and condition coverage

- [ ] **M2.1** Load ramp / headroom curve — same certification at `RATE_HZ=10` and `RATE_HZ=20`
  (≈24k / 48k ticks/s vs the 50k/s SLO baseline; 60k gate) and find the saturation point. Why:
  today's runs are 4.9k ticks/s — 10× below the declared SLO load. How: existing profiler knob;
  2 arms, smoke first. Evidence: p99-vs-load table + headroom point.
- [ ] **M2.2** Session-on certification — `MULTITF_SESSION_BYPASS=false` (production behavior).
  Why: off-hours runs bypass session windows and exercise a different aggregation path.
- [L] **M2.3** Multi-node topology — 4-VM rig: overlay-network latency, shared storage, clock skew.
  Needs the live rig; single-node numbers do not transfer automatically.

#### M3 - Failure and recovery coverage

- [ ] **M3.1** Chaos under load with recovery-time KPIs — run chaos tests 1–4 while the profiler
  capture is live; record failure-detected → processing-resumed per scenario (< 30 s SLO). Why: the
  suite currently yields PASS/SKIP, not SLO evidence.
- [ ] **M3.2** Checkpoint failure/expiry under stress — inject storage stalls; verify recovery and
  no data loss. Why: the certification shows 0 failures by design.
- [ ] **M3.3** Broker disconnect/reconnect — reconnect time + acknowledged loss; `soak-reconnect-loop.sh`
  exists as the driver.

#### M4 - Storage and read paths

- [ ] **M4.1** Cold SSD/RocksDB read — tablet restart with cold cache, then read old windows; KV
  lookup p99 + log first-read on flushed data. Why: every current read is hot (cache), so SSD
  worst-case is invisible.
- [ ] **M4.2** KV lookup latency percentiles — add lookup latency (not staleness) for
  `Signal_Candidates` / `candle_features` by key to the profiler report.
- [ ] **M4.3** Backlog / replay read speed — catch-up rows/s from earliest offset after downtime;
  time-to-head.
- [ ] **M4.4** Tablet disk I/O pressure — flush/compaction stall metrics (disk util, stall counts)
  in the state sampler; detect I/O-induced latency spikes.
- [ ] **M4.5** Recovery read speed at scale — recurring metric for the large-table restart path
  (the 22-min recovery class), not only the one-off incident record.

#### M5 - Time and resources

- [ ] **M5.1** Hours-long state trend — soak ≥ 4 h with the S12 series + checkpoint size/duration
  trend; catches slow leaks and compaction effects a 900 s run cannot.
- [ ] **M5.2** Memory/GC leak trend — TM/JM/ingestion heap + GC over hours.
- [ ] **M5.3** Efficiency per row at 50k/s — CPU/memory per row at the SLO load for capacity planning.

#### M6 - Correctness as certification

- [ ] **M6.1** Data-level end-to-end verification — counts in/out, dedup accuracy, OHLC correctness,
  feature values, signal rules as certification pass/fail (wire `CandleVerify`, `FeatureSpikeProbe`,
  signal checks into the run).
- [ ] **M6.2** Restore correctness at scale — both directions re-proven per certification, not only
  in standalone drills.

**Roll-up**

| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| M1 - Execution last mile (path coverage) | 5 | 0 | 0 | 5 | 0 | 0 | 0 |
| M2 - Load and condition coverage | 3 | 0 | 0 | 2 | 1 | 0 | 0 |
| M3 - Failure and recovery coverage | 3 | 0 | 0 | 3 | 0 | 0 | 0 |
| M4 - Storage and read paths | 5 | 0 | 0 | 5 | 0 | 0 | 0 |
| M5 - Time and resources | 3 | 0 | 0 | 3 | 0 | 0 | 0 |
| M6 - Correctness as certification | 2 | 0 | 0 | 2 | 0 | 0 | 0 |
| **Total** | **21** | **0** | **0** | **20** | **1** | **0** | **0** |

## Overview

**What this plan is.** The gap register from the 2026-10-03 measurement diagnosis: every declared
SLO boundary and production-path segment that today's certification does not cover, with the
cheapest closure path for each. It does not replace the plans listed above; it routes work into them
where they already own an item.

**Coverage today (for context).** The stage profiler covers broker → bridge → ingestion → Fluss raw
→ Flink (dedup/candles/features) → feature tables, plus checkpoint sync/e2e, table readability,
backpressure, watermark lag and state growth (S1–S12). `holistic-measure.sh` separately covers
signal preview/tentative/settlement latency. The chaos suite, soak tooling and correctness probes
exist but are not wired into the certification's pass/fail.

**The gap in one line.** Today's certification is a deep but narrow slice: the ingestion → Flink →
table path, on a single dev node, at 1/10th of the SLO load, on hot data, with no execution chain
and no failure injection.

**Success criteria**

| # | Criterion | Proof |
|---|---|---|
| S1 | Every Wave-0 item produces a dated evidence record with the metric defined (boundary, target, pass/fail) | `logs/` evidence + plan marker |
| S2 | No measurement changes the data path — probes are read-only and fail fast when broken | probe design note + run log |
| S3 | Every adopted KPI appears in the standard report (profiler table or its own report) | report diff |
| S4 | This tracker stays green (`plan_tracker.py --check`) | tool output |

**Working rules**

1. One new measurement per round — never mixed with a lever test.
2. Smoke before any long run (repo rule); a measurement run counts as a run.
3. Probes read-only; no data-path edits. Enabled-but-broken probes fail fast (a silent gap is worse
   than no capture).
4. A new KPI adopted as pass/fail = probe/test + CHG + doc update + `make gate`.
5. The related plans above stay the authority for their own items; this plan only adds what they do
   not already own.
