# Baseline ~100 ms diagnosis (post-checkpoint fixes) — read-only

**Status:** diagnosis only — no code, config, or topology changed. Operator request 2026-10-03.
**Sources:** `logs/stage-profile-20261003-w3e3-main` (aligned-only, 900 s, 10 s checkpoints,
**defaults** fetch-wait 20 ms / buffer-timeout 10 ms), `logs/stage-profile-20261003-w3hybrid-main`
(cross-check), the W1/W2 trial rounds and the frozen extraction in
`docs/plans/2026-09-30-p99-50ms-normal-path.md`.

## 1. The "~100 ms" is the p99 tail, not a fixed delay

Per-subtask distribution (latest snapshot; 8 strategy-host subtasks, tight across subtasks):

| KPI | p50 | p95 | p99 | p99.9 |
|---|---|---|---|---|
| `tick_to_strategy` | 47–50 | 83–88 | 95–104 | 106–114 |
| `ingest_to_monitor` | 36–37 | 69–73 | 79–84 | 89–94 |
| `ingest_to_strategy` (new KPI) | 42–44 | 75–79 | 85–91 | 93–100 |

The SLO (S1) is the **worst-subtask p99 per 60 s snapshot ≤ 50 ms** — i.e. the target sits at
~half of the current p99; the p50 is already inside the target.

## 2. Chain decomposition (measured)

**Pre-Flink legs (per tick):**

| Leg | p50 | p99 | Notes |
|---|---|---|---|
| feed → Java accept (S1) | 4–5 | 13–21 | broker/accept |
| Java accept → Fluss append ack (S4) | 17 | 43 | ingestion's own histogram: append p99 **38–68 ms** across the run; `append.pending.records` ≈ **635 standing**; batching latency ≈ 0 (linger 1 ms — not a lever) |
| Fluss visible → source emit | — | up to **20** | the fetch window is **fully consumed**: `fetchLatencyMs` = 20 ms and `timeMsBetweenPoll` = 20 ms on every raw subtask, every snapshot; `pollIdleRatio` ≈ 0.5 |

**In-DAG (cumulative source → operator latency markers, p50 / p99):**

| Operator | p50 | p99 |
|---|---|---|
| fingerprint-dedup → ingest-latency-monitor | 5.0 | 10.7 |
| multi-tf-aggregator | 10.5 | 19.4 |
| strategy-host (KPI point) | 16–21 | 30–38 |
| sinks (downstream of the KPI — out of scope) | 21–21.5 | 37–38 |

Per-hop p50 ≈ 5 ms ≈ **half the 10 ms output-buffer timeout** (Flink's default was 100 ms; the
job pins 10 ms since 2026-09-26 — that fix is what removed the original per-hop 100 ms).

## 3. Where the p99 comes from — three structural contributors

1. **Ingestion append tail ~40–55 ms** (S4/S5 p99; ingestion append histogram p99 38–68 ms;
   ~635 records standing pending).
2. **Source fetch window up to 20 ms** (cap fully consumed at every poll).
3. **In-DAG transit p99 ~30–38 ms** (3–4 hops × 10 ms buffer timeout worst case).

Not saturation: back-pressure 0 ms/s on all tasks, max task busy 55 %, host PSI-cpu p50 3.6 /
p99 6.5, disk util p50 4 % (w_await p99 8 ms, occasional spikes only). The tail is structural
(per-hop waits + fetch window + append ack), not resource exhaustion.

## 4. Separate constant — do not confuse with the KPI

`fluss_client_writer_client_id_batchQueueTimeMs` ≈ **100 ms** on the compute signal writer is a
standing sink-queue constant, flat and uncorrelated with the KPI (recorded in the plan's problem
statement). It is downstream of the KPI point and out of scope.

## 5. Levers — measured but NOT active today

| Lever | Measured effect (900 s rounds) | State |
|---|---|---|
| W1 `FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS` 20 → 2 | alone: ingest p99 med 75 → 71, tick 93 → 89 | trial only — default still 20 |
| W2 `BUFFER_TIMEOUT_MS` 10 → 2 (marginal over W1) | tick p99 med −18, ingest p99 med −13 | trial only — default still 10 |
| **W1+W2 (2/2, 2026-09-30)** | **tick p50 50.5 → 31, p99 med 99 → 71; ingest p50 37 → 23, p99 med 81 → 58** | measured, never adopted |
| W4 CPU / slot isolation | closed — not reproducible (unit misread) | — |
| W5 hop reduction | design only, last resort | — |

## 6. Reading

- The two ready env levers (W1+W2) are the **biggest available step** and already measured to cut
  the p99 by ~25–30 %; they are simply not adopted (defaults 20/10).
- **After W1+W2 the residual p99 ≈ 58–71 vs the ≤ 50 target.** The expected remaining driver is
  the **ingestion append tail** (~40–55 ms) plus residual per-hop/scheduling jitter. W5's hop
  removal (~5–10 ms) alone likely cannot close that gap — the ingestion append path needs its own
  probe (writer ack concurrency/batching, `append.pending.records` ≈ 635 standing, Fluss server
  ack).
- **Recommendation (no action taken):** re-run W1+W2 on the current tree (post Wave A/B/C +
  checkpoint fixes) as the next lever round, then re-derive the residual before designing W5.

**Caveats:** all numbers are the dev single-TM deck at 4.9 k rows/s; `tick_to_strategy` is
event-time based (the feed timestamp-fidelity item stays a separate non-goal); the fetch-window
lever's cost (request rate ×~30, ~3 k/s at the 60 k/s production rate) must be re-checked at
production load before adoption.
