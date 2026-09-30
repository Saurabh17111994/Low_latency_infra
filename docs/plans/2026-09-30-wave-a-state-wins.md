# Wave A — state wins without schema change (cadence, raw retention, compaction)

**Date:** 2026-09-30
**Status:** operator-directed order approved 2026-09-30 ("Wave A first, then the
merged table as staged Wave B"). A1 verify-first complete; A2/A3 verify-first in
progress. Every item is judged on the three axes (low latency + high throughput
+ less state growth) with the CHG-461 meters; any non-winner is reverted.
`make gate` after the wave's code changes (operator Q42); runs follow the
standing smoke-before-long-run rule.

## Baseline — today's certified 900 s (`logs/chg460-main-20260930-163401`)

| Series | Baseline 900 s |
|---|---|
| `raw_table_1` | **+150.28 MB/min** (9 d retention; 9 d ≈ 1.9 TB at smoke feed) |
| `candle_live` | **+85.45 MB/min** (vs `candle_closed` +2.77 MB/min for the identical +257 898 rows → ≈97 % churn/versions/log) |
| `candle_closed` | +2.77 MB/min |
| Flink changelog | +0.93 MB/min (post-W3-d) |
| Live freshness (fresh reader) | p50 327 / p95 815 / p99 857 ms |
| Closed readability | p50 1 026 / p99 1 525 ms |
| Live-mirror emissions | 29 247 rows/s (measured) |
| Ingest / e2e / tick p99 | 71 / 69 / 87–93 ms |

## A1 — live mirror cadence: effective 2 Hz/key → 1 Hz/key (env-first)

**Verify-first finding (2026-09-30):** `MULTITF_LIVE_SNAPSHOT_INTERVAL_MS`
(`SignalJobConfig.java:181`) already defaults to **1000 ms**, yet the effective
emission rate is **~2 Hz/key**: `maybeScanLiveMirrors`
(`MultiTimeframeAggregateFunction.java:731-767`) runs **two independent
steplines** per slot — a processing-time due marker and an event-time
(watermark) due marker, both advancing by `liveSnapshotIntervalMs`
(`:444`, `:450`). 14 598 keys × 2 = 29 196/s, matching the measured
29 247 rows/s. **"1 Hz/key" (operator Q22) therefore means interval
`2000` ms** (two steplines per 2 s = 1 Hz/key), *not* 1000 ms — setting 1000
would be a no-op.

**Trial change:** env-only `MULTITF_LIVE_SNAPSHOT_INTERVAL_MS=2000` added to the
W3-d 900 s protocol. No tracked file is touched in the trial round.
**Expected:** `candle_live` growth ≈ halved (~42–45 MB/min); live freshness
floor ≈ p50 0.5–0.7 s / p99 ≈1.3–1.5 s (freshness-bound class, operator Q21);
emissions ≈ 14.6 k rows/s; KPIs/throughput unchanged.
**If it wins:** adopt `2000` as the default in `SignalJobConfig` — code change
with a failing-first test pin + CHG; `make gate` at wave close (p99-plan rule:
"env-only trial; the winning value lands as code").
**Wrong if:** growth does not roughly halve, the live leg degrades beyond the
agreed freshness class, or KPIs/throughput regress. **Revert:** drop the env.

## A2 — `raw_table_1` retention: 9 d → 3 d (guard first)

**Verify-first (open):**
- DDL `02_raw_table_1.sql`: `table.log.ttl = 9d` + `auto-partition.num-retention
  = 9`; the P4-001 header requires the EOD VERIFIED buffer and forbids lowering
  "without the controller guard in place".
- Guard: `EodControllerTool` (common; extend/delete-block path; tests exist) —
  operator Q36: guard behavior verified first.
- Apply path: determine whether `table.log.ttl` / `auto-partition.num-retention`
  are **ALTERable on Fluss 1.0.0** or create-time-only (DEC-052: create-time
  options adopt via recreate). A recreate of `raw_table_1` carries
  ingestion/offset implications — if ALTER is unsupported, A2 becomes a staged
  recreate plan, not a config flip.
**Expected:** +150.28 → ~50 MB/min (3 d ≈ 650 GB vs 1.9 TB).
**Not started until the apply path and guard are verified.**

## A3 — compaction investigation (read-only first)

**Question:** the ≈97 % churn share in `candle_live` (and the raw log growth) —
is it bounded by compaction or unbounded? Inventory: Fluss tablet KV/compaction
options, log-segment sizing/TTL, snapshot settings; measure with the S12 series
(`fluss.tablet.bytes`, `flink.changelog.bytes`, RocksDB dirs).
**Deliverable:** a short findings note; if a safe lever exists, a config trial
judged on the meters; otherwise document why cadence/retention are the only
levers.

## Measurement protocol (per item)

1. **200 s smoke** — feature layer on (the presence gate requires
   `S11.feature_read`, as in today's precedent); confirm data flow + the lever's
   expected signature (for A1: emissions ≈ halved, rows still growing).
2. **900 s round** — the certified baseline protocol (W1+W2 envs, 10 s
   checkpoints, feature layer off, quiet host) → three-axis verdict vs the
   baseline table above.
3. Adopt or revert; **one lever per round**; evidence under `logs/`; CHG per
   change; gate at wave close.

## Risks

- **A1:** stored live now-view floor becomes ~1 s (operator-approved class
  change, Q21). Strategies are unaffected (in-memory tick path, Q50); no
  consumer needing sub-second stored freshness is known.
- **A2:** retention shorter than the EOD VERIFIED path risks tick loss before
  offload verification — hence guard-first; the recreate path may cost more
  than the disk win justifies (defer if so).
- **A3:** no safe lever found — acceptable; the investigation still bounds the
  problem.

## Rollback

- **A1:** drop the env (trial) / revert the default change commit.
- **A2:** restore 9 d + num-retention 9 through the verified apply path.
- **A3:** none unless a trial is explicitly opened.
