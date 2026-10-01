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

**Smoke/main invocation recipe (learned 2026-09-30):** the phase needs
`FEATURE_LAYER_ENABLED=true STRATEGY_HOST_ENABLED=true
STRATEGIES=n7-range-breakout-v1` — the job's config guard rejects the feature
layer without the host, and the host without a non-empty strategy list — and an
**absolute `OUT`** (the fleet containers bind-mount it; a relative path fails
docker create with exit 125). Attempts `smoke4`/`smoke4b`/`smoke4c` failed on
those two invocation errors and carry no capture evidence.

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

## Locked decisions — 50-question round (2026-09-30)

Operator answered all 50 questions; these are the working decisions for the
merge/EOL/cadence/retention work. The merged-table contract decision is recorded
as **DEC-059**. Items marked **OPEN** still need one clarification round before
implementation.

### Merge / table design (Q1–Q12, Q20, Q48–Q49)

| Q | Decision |
|---|---|
| 1 | Merge `candle_live` + `candle_closed` + `feature_values` into one table. Feature add/remove friction is the open concern → the MAP + append-only registry path answers it (add = one registry line + pin line + computer; remove = `RETIRED`; ids never reused; no DDL/table/sink change). **OPEN**: operator confirmation. |
| 2 | Single writer = strategy host. |
| 3 | `sealed BOOLEAN` marks finality. |
| 4 | Sealed rows are never rewritten; late ticks dropped/counted. |
| 5 | `MAP<INT,DOUBLE>` features (DEC-057). |
| 6 | Forming rows carry tick-cadence features only; sma/rsi null until seal. |
| 7 | Feature column present/empty when the layer is off. |
| 8 | Staged dual-write cutover. |
| 9 | All consumers migrate: strategy/signal, EOD/lake, UI/dashboards, replay/backtest. |
| 10 | Old tables read-only, dropped after N days. |
| 11 | **Resolved (later round): one lake table** (candles + features together) — native tiering, no split step. (The earlier answer, separate lake tables, was overridden.) |
| 12 | Merged retention **3 d + lake**. |
| 20 | Merged forming rows serve the now-view. **Superseded 2026-10-01 (CHG-486): closed-only storage — the live now-view is the strategy host's in-memory view, never the table.** |
| 48 | One TF-keyed table (not six). |
| 49 | sma/rsi null until computable. |

### Live end-of-life / cadence (Q13–Q27, Q50)

| Q | Decision |
|---|---|
| 13 | Live EOL = merged freeze (no separate live table in the merged world). |
| 14 | kv.ttl 15 min is a **fallback-only** value (if the merge is deferred); moot in the merged world — the rows are the history. Closed 2026-09-30 (later round). |
| 15 | kv.ttl semantics verified by an isolated test first (fallback path only). |
| 16 | Compaction/churn investigation included in the state work. |
| 17 | Verify no consumer reads sealed rows from `candle_live` before cutover. |
| 18 | Quiet-instrument rows may expire (fallback semantics). |
| 19 | Rows-vs-churn split confirmed first (census fix + 900 s). |
| 21 | Stored live leg is freshness-bound, not 75 ms-class. |
| 22 | Live cadence **1 Hz/key** (halves churn; stored now-view floor ≈1 s). **Superseded for storage 2026-10-01 (CHG-486): no live-cadence write.** |
| 23 | Per-tick, if ever enabled: **smallest TF only** (load answer below). |
| 24 | Single global cadence. |
| 25 | One record per key per emission (no batching) — request-rate watch item. |
| 26 | All TFs keep live rows. **Superseded for storage 2026-10-01 (CHG-486): no stored live rows — all six TFs are live in Flink memory only.** |
| 27 | **Revised (later round): keep today's watermark close** — prompt close not adopted (no two-phase flags, no grace). |
| 50 | **Resolved: strategies act on the in-memory per-tick forming context** (~26 ms p50 path); stored forming rows (1 Hz) serve UI/dashboards/external readers. **Superseded for storage 2026-10-01 (CHG-486): Fluss stores closed candles only — no stored forming rows exist.** |

### Q23 load answer — per-tick emission

| Option | Writes @ smoke feed (2 433 instr × 2 Hz) | Writes @ busy feed (50 ticks/s/instr) | Note |
|---|---|---|---|
| today 2 Hz/key | ~29 k/s | ~29 k/s | fixed, bounded |
| chosen 1 Hz/key | ~15 k/s | ~15 k/s | half churn |
| per-tick, all TFs | ~29 k/s | ~730 k/s (~25×) | feed-proportional firehose |
| per-tick, smallest TF only | ~5 k/s | ~122 k/s | near-constant share |

Recommendation stands: **smallest TF only** if per-tick is ever enabled.

### Retention / platform (Q34–Q38)

| Q | Decision |
|---|---|
| 34 | `raw_table_1` retention **3 d + lake**. |
| 35 | Longest Fluss replay window needed ≤24 h. |
| 36 | Controller guard first, then lower retention. |
| 37 | Decide from smoke extrapolation (no real-feed sizing round) — caveat: the smoke is a 2 Hz fake broker; revisit when real rates exist. |
| 38 | Backfills older than retention come from the lake. |

### p99 round (Q28–Q33)

| Q | Decision |
|---|---|
| 28 | p99 work starts after these decisions. |
| 29 | Tail instrumentation on all five suspects (source fetch/serde, dedup/window state, aggregator/host compute, GC pauses, network/contention). |
| 30 | Per-stage p99 histograms — yes. |
| 31 | Bar = **p99 ≤75 ms in every window of every TF**, strict — applied to the per-tick legs; stored legs are class-scored per Q21. |
| 32 | No throughput/state regression accepted while chasing p99. |
| 33 | GC/container tuning allowed with before/after evidence. |

### Process (Q39–Q47)

| Q | Decision |
|---|---|
| 39 | 900 s released: census fix + 200 s smoke first. |
| 40 | Smoke first — yes. |
| 41 | Prepared commits — done (`c5031a40`, `d5ac945c`). |
| 42 | `make gate` after the next implementation wave. |
| 43 | Records: this scope doc + DEC-059. |
| 44 | Plan doc first, then implement. |
| 45 | Trackers updated now. |
| 46 | No 1 s intermediate. |
| 47 | No 1 s output TF. |

### Open items — resolved (2026-09-30, later round)

| Item | Resolution |
|---|---|
| Lake (Q11) | **One lake table** (candles + features) — native tiering. |
| Prompt close (Q27) | **Not adopted** — watermark close stays (measured ~0.9–1.3 s). |
| Forming rows for strategies (Q50) | **In-memory per-tick context only** — closed-only storage (CHG-486, 2026-10-01): no stored forming rows; the table holds sealed candles. |
| kv.ttl (Q14) | Fallback-only value; moot after the merge. |
| Per-tick scope (Q23) | **Smallest TF only**, if ever enabled. |
| Feature add/remove (Q1) | MAP + append-only registry path explained (one registry line + one pin line; `RETIRED` to remove) — **awaiting operator confirmation**. |

## Wave C post-cutover round (2026-10-01) — latency + state growth

**Run:** `logs/wave-c-stage-profile-20261001-065919`
(`stage-profile.sh`: smoke 200 s presence **PASS** → main 900 s; 2 Hz x 2 433
instruments; strategy host `n7-range-breakout-v1`; harness repair for the
merged world in CHG-483). First attempt
(`logs/wave-c-stage-profile-20261001-065729`) stopped at job submit on a stale
pre-W-C5a `compute.jar` (the gate runs `mvn test`, not `package`) — rebuilt.

| axis | pre-Wave-C (`wave-a-a1-main-20260930-170808`) | post-Wave-C | reading |
|---|---|---|---|
| S4 append ack p99 | 63 ms | 54 ms | unchanged/improved |
| S5 ingest e2e p99 | 71 ms | 66 ms | unchanged/improved |
| S6 raw → post-dedup p99 | 76 ms | 81 ms | same class |
| S7/S9 worst Flink tracker p99 | 1 120 ms | 42.8 ms | improved in this round (single snapshot) |
| S11 close → read log tail p50 | 775 ms | 710 ms | same/improved |
| S11 close → read log tail p99 | 2 502 ms | 8 689 ms | tail spikes on FIFTEEN_S/THIRTY_S/ONE_M — finding (a) |
| S8 close → read KV first-sight (p50/p99) | quantized 5 s poll | 511 / 1 176 ms | sealed rows readable within ~1.2 s |
| S10 live p50, FIFTEEN_S | 756 ms | **232 ms** | per-tick fast feed |
| S10 live p50, THIRTY_S…FIFTEEN_M | ~750 ms (every TF) | 15.8 s / 30.8 s / 89.9 s / 132.8 s / 188.9 s | **finding (b)**: no forming rows above FIFTEEN_S |
| candle-side tablet bytes | candle_live 47.25 + candle_closed 2.85 = **50.10 MB/min** | candle_features **45.04 MB/min** | −10 % bytes, one table/writer instead of three |
| Flink RocksDB dirs | +0.40 MB/min | +0.13 MB/min | −68 % (changelog backend differs across runs — not compared) |
| raw_table_1 | +141.79 MB/min | +146.81 MB/min | feed-bound, unchanged |

Per-TF forming churn (cumulative records over the main): FIFTEEN_S 4 534 364
(97.2 %), THIRTY_S 72 990, ONE_M 36 495, THREE_M 12 165, FIVE_M 7 299,
FIFTEEN_M 2 433 — i.e. the merged writer emits FIFTEEN_S forming rows per tick
(~300 k versions/min equivalent) and every other TF **once per window at seal**.
(Pre-CHG-486 measurement; the closed-only storage resolution below removes the
per-tick forming share.)

**Finding (a) — log-tail p99 ~8.7 s on the smallest TFs.** The KV first-sighting
leg (S8) shows sealed rows readable in ~1.2 s p99, so the log-tail spikes are a
scan-delivery tail now that the tail carries FIFTEEN_S per-tick churn
interleaved with sealed rows. Consumers that tail the merged changelog
(archive/EOD subscribers) inherit it — watch item, not a strategy-path issue.

**Finding (b) — merged now-view coverage conflict (recorded in
`01-foundation.md`).** Q22/Q26/Q50 require stored forming rows for all six TFs
at ~1 Hz/key; post-cutover the strategy host (the only writer) consumed only
the smallest TF per tick and the aggregator's all-TF `LIVE_TAG` snapshot stream
had no consumer. **Strategy half resolved 2026-10-01 (CHG-484):** the fast feed
now carries every TF's forming row per tick — strategies see all six evolving
candles in memory. **Storage half resolved 2026-10-01 (operator decision,
CHG-486): closed-only storage** — no forming rows are written at all; the table
holds one sealed row per closed window, and the live now-view is the strategy
host's in-memory view. Smoke evidence
(`logs/chg486-smoke-20261001-111805`, 120 s, presence PASS): FIFTEEN_S writes
+8,658 rows/min vs +301,888/min in the CHG-484 smoke (~35x); zero unsealed rows
observed; live-path metrics unchanged.

Note: the p99 ≤ 75 ms per-window SLO is **not** met by any stored leg by
construction (watermark close, Q27 — measured close→read ≈0.7–1.2 s); the
per-tick strategy path remains the 75 ms-class leg.
