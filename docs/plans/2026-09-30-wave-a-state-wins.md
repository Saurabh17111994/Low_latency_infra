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

## A2 — `raw_table_1` retention: 9 d → 3 d (guard-first **reconciliation**, not a bare ALTER)

**Verify-first (2026-09-30, read-only):**
- **ALTER is the apply path, no recreate.** `table.log.ttl` and
  `table.auto-partition.num-retention`/`num-precreate` are on Fluss 1.0.0's
  alterable list; the log.ttl ALTER is proven accepted **and enforced** (A1/A2/
  A2b probes GREEN 2026-09-25; `CHG-307`). The guard itself extends retention
  with exactly that one ALTER (`EodControllerTool` → `Admin.alterTable`), and
  `RawTableAlter`/`EnableTiering` show the one-shot tool pattern for live
  option changes.
- **The guard's floor rule fights a 3 d TTL.** `EodPlanner.plan`:
  `protectedBound = max(3rd-most-recent trading day end + TTL, earliest
  unverified day end + TTL)`; extension fires when `margin < EOD_SAFETY_FLOOR`
  (default **7 d**; `EodRetentionPolicy.MIN_COMPLETE_TRADING_DAYS = 3`).
  Today's 9 d TTL gives ~8 d margin → quiet. A 3 d TTL gives ~2 d margin
  < 7 d → the controller extends to `TTL + EOD_EXTENSION` (30 d) **immediately**
  — and the weekend math (Friday end + 3 d = Tuesday 00:00) makes the
  "3 complete trading days" rule fire at any floor above ~30 min on Mondays.
  So 3 d is only stable with a guard change; the operator already re-scoped the
  contract in Q36 ("replay window ≤ 24 h; backfills from lake").
- **Guard runtime:** on this PC EOD runs as a one-shot
  (`make eod-controller ARGS="..."`, compose service `eod-controller`,
  `restart: no`); the always-on scheduler is a production-deck service
  (`docker-stack.yml`: `EOD_AT`/`EOD_ZONE`/`EOD_TABLES`/`EOD_OFFLOAD`). The
  extend path is exercised in CHG-307's drills; the 3 d reconciliation is a
  code+env change, testable offline.
- **Live check owed before the ALTER:** `table.datalake.enabled` on the live
  `raw_table_1` must be true (else there is no offload) + the live ZK options
  (`9d`/`9` expected).

**Proposed reconciliation (operator ack before the live ALTER):**
- Keep "unverified day always extends"; replace the 3rd-most-recent-day floor
  with a runway rule tied to the TTL (extend only when an **unverified** day's
  bound is closer than a small runway; verified days never extend) — the
  weekend/steady-state cases then stay quiet at 3 d.
- Set `EOD_SAFETY_FLOOR` for a 3 d deployment (no steady-state firing) and
  update the `EodRetentionPolicy`/`02_raw_table_1.sql` contract text
  (7 d/9 d → 3 d).
- Failing-first tests: Friday run → quiet; stuck-unverified day → extension
  with runway; holiday weekend case.

**Apply once acked:** ALTER `raw_table_1` (`table.log.ttl=3d`,
`table.auto-partition.num-retention=3`); verify in-force (ZK) + post-ALTER
write/read (the next round exercises both). Record the steady-state
projection (≈650 GB vs ≈1.9 TB at the synthetic 2 Hz feed) — the growth
**rate** is unchanged, only the ceiling moves, so no 900 s round can show it.

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
