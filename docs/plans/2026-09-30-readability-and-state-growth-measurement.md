# Readability matrix + full-platform state growth — measurement scope

**Date:** 2026-09-30
**Status:** operator-approved 2026-09-30 (three decisions below); implemented in
CHG-461 (probes + stage-capture wiring + profiler report). Smoke evidence lands
in `logs/chg461-smoke-*`; the 900 s round waits for the operator's release.
**Supersedes as the measured surface:** the mixed-TF, 5 s-quantized S8 reading
and the operator-side-only S9 reading (both stay in the report for continuity).

**Standing goal (operator, 2026-09-30):** every change is judged on three axes
together — **low latency + high throughput + less state growth**. The facility
is the three measurement faces of that goal: the readability matrix (latency),
the rows/s + window/s legs (throughput), and the state meter (state growth,
platform-wide and per timeframe).

## Operator decisions (2026-09-30)

1. **"Readable" = a fresh reader sees the row.** The clock stops when an
   independent reader (our probe) first sees the row in the Fluss table — not
   when some specific consumer (strategy, executor, dashboard) reads it. One
   definition for every timeframe and every feature; consumer-specific legs
   stay separately measured (`compute.latency.tick_to_strategy`).
2. **SLO: p99 ≤ 75 ms in every window of every timeframe** — not only the 15 s
   grid. For the live leg a sample is scored in the timeframe window it was
   taken in; for the close-cadenced legs one stored row *is* one window, so the
   row's close→read latency is that window's score. Because windows nest, the
   15 s grid is the strictest check for the per-tick legs; per-TF scoring is
   what makes the closed/feature legs honest (each TF closes on its own
   schedule).
3. **State meter covers the full platform**: the Flink job (checkpoint
   `state_size`, changelog, RocksDB, heap), the Fluss tables (row counts +
   tablet bytes), and the service containers — ingestion included.

## Why (what did not exist)

| Surface | Before | Problem |
|---|---|---|
| Live candles | `consumer-read.tsv` (15 s TF only, 5 s poll) | no per-TF breakdown |
| Closed candles | S8 = first-seen − window_end | mixed across TFs; quantized by the 5 s KV poll (the real number was hidden) |
| Features | nothing (S9 is a sink-tracker metric) | `feature_values` readability never observed |
| State | per-checkpoint `state_size` + ad-hoc `du` | no series per operator/table/layer |

## The matrix

| leg | start event | end event | source |
|---|---|---|---|
| live candles, per TF | tick (`last_event_time` of the row) | fresh reader sees the row | `liveread.tsv` — KV sweep of `candle_live`, 6 TFs × token sample, ~1 s |
| closed candles, per TF | window end | first read | `closeread.tsv` — `candle_closed` log tail, first sighting per `(token, tf, window)` |
| features, per TF × feature id | window end | first read | `featureread.tsv` — `feature_values` log tail; feature ids from the row's map (0 `last_price`, 1 `sma_close_20`, 2 `rsi_close_14`) |
| strategy path (existing) | tick event time | strategy host read | `compute.latency.tick_to_strategy` (TM Prom) |

The closed/feature legs are **log tails at sub-ms resolution** — the probe
subscribes to the requested buckets from the current log end and timestamps
each record's first sighting — replacing the old 5 s KV poll quantization.
Two of the 16 buckets are sampled by default (`READ_PROBE_BUCKETS=0,1`): the
matrix is a per-timeframe readability measurement, not a census, and a
full-table live tail (~29 k rows/s) would perturb the latency being measured.

## State growth (long format, `state-growth.tsv`)

`epoch_ms \t layer \t key \t value`, sampled every 15 s (every 3rd capture
tick), with the tablet `du` on every 2nd sample:

| layer | key | source |
|---|---|---|
| `flink.changelog.bytes` | job dir | `du -sk /checkpoints/changelog/*` in the TM |
| `flink.rocksdb.bytes` | operator dir (subtasks summed in the report) | `du -sk /tmp/flink-rocksdb/*` in the TM |
| `fluss.tablet.bytes` | table dir | `du -sk /tmp/fluss/data/*/*` in the tablet |
| `fluss.table.rows` | table | `Admin.getTableStats` (`FlussTableStatsProbe`, one RPC per table) |

Checkpoint `state_size` already arrives per checkpoint in
`flink-checkpoints.jsonl`; the report shows all of it together (S12 + the
growth table). A failed sample is a missing tick + a loud warn — never a
made-up zero.

### Per timeframe (`state-tf.tsv`, `state-tf-live.tsv`)

The same way the latency matrix breaks every number down by timeframe, state
is measured per timeframe:

- **`candle_closed` / `feature_values`** — the readability probe's tails now
  subscribe **all buckets** and count every record they see per timeframe
  (cumulative rows + changelog bytes), writing a snapshot every 5 s. The
  latency log stays bucket-sampled (2 of 16 by default) — counts are exact,
  latency stays cheap.
- **`candle_live`** — a periodic KV-snapshot census (`FlussTableStatsProbe
  … tf-census`, every 60 s) counts the CURRENT stored rows per timeframe; the
  live table is TTL-bounded, so this is the live-state size the log-record
  count cannot show. Bytes stay unknown (`-1` → rendered as a placeholder).

The report adds a **Per-timeframe state growth** table (rows first/last,
rows/min, last bytes per table × timeframe).

## Report + gate wiring

- `stage_profiler.py`: new stages **S10** (live readability), **S11**
  (closed + feature close→read), **S12** (state growth), plus per-TF and
  per-feature detail tables and the per-window SLO scoring. Pure functions
  (`parse_liveread`, `live_window_stats`, `parse_closeread`,
  `parse_featureread`, `close_window_stats`, `parse_state_growth`,
  `state_growth_series`, `growth_rate`) are unit-pinned.
- Presence gate: `S10.live_read`, `S11.closed_read`, `S11.feature_read`,
  `S12.state_growth` are required for the smoke — an empty measurement leg
  refuses the main phase (same fail-closed posture as S1–S9).
- `stage-capture.sh`: compiles `FlussReadabilityProbe` + `FlussTableStatsProbe`,
  starts the long-lived readability probe with the capture (stopped, bounded,
  after the loop), samples state growth in the tick loop, declares the new
  outputs (readability TSVs are WARN-only on emptiness mid-run; the smoke
  presence gate is the hard check).

## Evidence plan

1. Smoke (200 s, quiet host) → presence gate green + first real per-TF numbers
   + state-growth series. → `logs/chg461-smoke-*`.
2. W3-d 900 s verdict round (operator-held) — unchanged scope, now with the
   matrix attached.
3. 900 s measurement round for the matrix when the operator releases it.

## First smoke (2026-09-30, `logs/chg461-smoke-20260930-150031`) — what it caught

All three new legs produced data; the gate failed on exactly one leg and the
report exposed two measurement defects, both now fixed:

- **Feature leg = 0 rows because the job flag is off by default.** The gate
  failed closed (`S11.feature_read: 0 < 1`) — correct behaviour: the job
  defaults `FEATURE_LAYER_ENABLED=false`, so the stored feature layer had
  never been written. Measurement smokes must set
  `FEATURE_LAYER_ENABLED=true` (purge of `feature_values` is not needed: the
  tail starts at the current log end).
- **The live sweep blocked the scanner poll loop** (`FlussReadabilityProbe`
  ran all three legs in one loop; 72 sequential lookups ≈ 7 s per sweep), so
  close→read timestamps were quantized/inflated up to ~7.6 s (15 s TF p50
  4.2 s before the fix — an artifact, not pipeline latency). Fix: one thread
  per leg + fan-out lookups (one RTT round per sweep, P6-086 discipline).
- **Live-leg finding (real, not an artifact):** live staleness p50 768 ms /
  p95 1116 / p99 2291 (1800 samples, 32 windows). The live mirror emits each
  (key, tf) row ~2×/s by design, so the fresh-reader view of a live candle is
  ~0.5–1 s old even with a perfect pipeline; the 75 ms target cannot apply to
  this leg as designed. Operator decision needed: keep the cadence and target
  the live leg as a freshness bound, or raise the emission to per-tick
  (write load ≈ 29 k rows/s — the pre-W3-d steady state).
- **State growth (valid, kept):** changelog **+1.56 MB/min** (vs ~20 MB/min
  before W3-d), RocksDB dirs ~flat (+0.03…0.06 MB/min), per-table rows and
  tablet bytes per table all tracked; raw-table disk grows at the feed's own
  write rate (+150.85 MB/min during a heavy smoke).

Smoke #2 (`logs/chg461-smoke2-*`, feature layer on, refactored probe) is the
one that certifies the facility.

## Second smoke (2026-09-30, `logs/chg461-smoke2-20260930-150730`) — certified

**EXIT=0, presence gate green** (S10 15 192 / S11.closed 8 652 /
S11.feature 8 652 / S12 304 samples). Clean matrix, every timeframe:

| leg | p50 | p95 | p99 | reading |
|---|---|---|---|---|
| live candles (tick → fresh-reader visible) | 645 ms | 1103 ms | 1158 ms | the ~2 Hz/key mirror emission cadence — a design floor, not pipeline lag |
| closed candles (window close → first read) | 898 ms | 1016 ms | 1024 ms | watermark/close detection + sink + visibility, tight and repeatable |
| features (window close → first read) | 899 ms | 1021 ms | 1023 ms | same close path as the closed candle (emitted from it), as expected |

Interpretation for the 75 ms SLO (operator decision item):

- The **per-tick legs** (strategy path) are the 75 ms-class legs: this smoke's
  `tick_to_strategy` p99 med = 83 ms (ingest p99 med = 69 ms), unchanged by
  the facility.
- The **stored-data legs** carry deliberate batching: the live mirror emits
  each (key, tf) row ~2×/s, and a closed window becomes readable only after
  the watermark passes it (~0.9–1.0 s). 0 of their windows pass 75 ms **by
  construction**; the numbers are now exact and stable, so the operator can
  decide per leg: keep the cadence (and set a class-appropriate target), or
  redesign (per-tick live emission ≈ 29 k rows/s; prompt close instead of
  watermark close).
- Feature content caveat: in a 200 s smoke only `last_price` (id 0) is
  populated — `sma_close_20` needs 20 closed windows (5 h for the 15 m TF)
  and `rsi_close_14` fifteen; their rows appear once the warm-up exists.

Facility cost check: pipeline KPIs stayed in family with smoke #1/CHG-460
(tick p99 med 83 vs 95/80; ingest 69 vs 75/64) — the probes do not distort the
measurement they take. State growth identical in shape to smoke #1
(changelog +1.54 MB/min, RocksDB ~flat) plus the now-tracked `feature_values`
rows (0 → 68 124).

## Open items

- Close-cadenced SLO: scored against 75 ms from day one; if the first honest
  numbers show the whole class is milliseconds-to-seconds away, the target and
  its engineering cost get a separate decision (operator).
- Heap-structure gauges (candle slots/rings, dedup windows, feature rings,
  strategy host state) are **not** covered by S12 — they need a small job-code
  change; deferred until the managed/disk/table series are in use.

## Architecture options under operator review (2026-09-30 — NOT decided)

Operator questions: (a) build a 1 s candle table first, then derive the
multi-TF candles + features from it; (b) one raw table + per-TF derived
tables (OHLCV + features) on the Fluss changelog. Acceptance bar for every
option, stated by the operator: **low latency + high throughput + less state
growth**. Nothing below is approved or scheduled.

**1 s base first (roll-up chain).** Two readings — 1 s as an extra TF, or 1 s
as the intermediate that feeds 15 s…15 m. Appraisal for this repo:
- State: the smoke ran 2 433 instruments. 1 s history ≈ 2 433 rows/s ≈ 0.2 B
  rows/day; all six current TFs together ≈ 27 M rows/day, so a 1 s store is
  ~8× the whole multi-TF set — against the state bar unless aggressively
  TTL'd / lake-offloaded.
- Latency: deriving via Fluss adds a durable write + read hop; the job
  already consumes `raw_table_1` directly (SignalJob Javadoc line 35), so a
  1 s intermediate puts a second Fluss round trip in front of every TF.
- Correctness: Fluss changelogs carry no watermarks; window completion would
  have to be synthesized across the hop, and a late 1 s row lands after the
  larger row was already sealed — the failure mode the current tick-watermark
  close cannot show.
- Throughput: not the constraint (~2.4 k writes/s vs the ~29 k upserts/s live
  mirror already running).
- Verdict so far: derive TFs from ticks in the same job (today's design); add
  1 s only as a same-job sibling output if a consumer needs it, never as the
  intermediate for the other TFs.

**One raw table + per-TF derived tables.** Already the shape of the system:
`raw_table_1` (LOG, 9 d `table.log.ttl`, daily partitions, EOD Iceberg
offload) → `FlussSource` → multi-TF aggregator → derived candle tables. The
Fluss changelog is the per-table log: LOG tables stream append facts, KV
tables stream an upsert changelog; subscribers receive a whole table's log
and filter client-side; the log carries no watermarks. Open layout choices:
- One table keyed `(instrument_token, tf, window_start)` (plus `features` +
  `sealed`, single writer — previous clarification) vs six per-TF tables.
  Same total rows; per-TF tables add DDL/bucket/sink/lake-table overhead and
  buy per-TF retention and subscribe-only-your-TF. Default remains one
  TF-keyed table; split only for a concrete retention/isolation need.
- Features in the same row as the candle (merged, sealed flag) so there is no
  duplicate candle copy and one writer per table.

**Measured state levers (smoke #3, 200 s):** `raw_table_1` +144 MB/min (9 d
retention × feed rate — the dominant term), `candle_live` +88 MB/min (2 Hz
mirror churn; `table.log.ttl` covers the changelog only, KV rows do not
expire), `candle_closed` +2.6 MB/min, `feature_values` +0.5 MB/min. State is
won with retention/offload, write cadence, and one-copy merges — not by
adding tables or intermediates.
