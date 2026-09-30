# Wave A — state wins without schema change (cadence, raw retention, compaction)

**Date:** 2026-09-30
**Status:** operator-directed order approved 2026-09-30 ("Wave A first, then the
merged table as staged Wave B"). **A1 landed (`CHG-462`); A2 landed (`CHG-463`);
A3 findings recorded** — wave code complete, `make gate` at close. Every item is
judged on the three axes (low latency + high throughput + less state growth)
with the CHG-461 meters; any non-winner is reverted.
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

**Smoke (2026-09-30 17:01 IST, `logs/wave-a-a1-smoke-20260930-170130`): GREEN.**
Presence zero failures; all legs populated (S10 live 15 048 samples, S11 closed
+ features 8 652 + 8 652, S12 304 + 516 series). The lever landed: live-mirror
emissions **14 603 rows/s = exactly half** the 29 247 baseline (interval env
reached the job). Smoke-window `candle_live` +47.66 MB/min (vs +85.45 in the
feature-off 900 s baseline; smoke had the feature layer on — the 900 s round is
the verdict). Live staleness p50 1505 / p99 1610 ms — the expected freshness
class for a 2 s stepline.

**Result (900 s, 2026-09-30): PASS — landed as CHG-462.**
`candle_live` **+85.45 → +47.25 MB/min (−45 %)**; `candle_closed` +2.85
(unchanged); `raw_table_1` +141.79 (feed); Flink changelog +0.87; ingest e2e
p99 71 ms and tick→strategy p99 80–88 ms (vs 86–93) — no latency/throughput
cost; live-read staleness p50 749 / p99 2 126 ms (freshness class, Q21). The
landing is the code default flip (`SignalJobConfig` 1000 → 2000 ms + test
pins; failing-first 2 failures before, 79/79 after; full compute suite
638/0/18-skipped). Evidence `logs/wave-a-a1-main-20260930-170808`.
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
- **Live check (2026-09-30): DONE.** The live `raw_table_1` reported
  `ttl=9d num-retention=9 partitions=[event_day]` and
  `table.datalake.enabled=false` — the dev table predates the lake options
  (recreation-only; `EnableTiering.java` exists for exactly this). The DDL
  carries `datalake.enabled=true`/iceberg/5min/auto-compaction
  (`02_raw_table_1.sql:169-172`), so fresh tables are correct; the dev lake
  path and the real tiering-service proof stay tracked in the native-adoption
  plan — not claimed here.

**Reconciliation implemented (2026-09-30, CHG-463) — operator accepted:**
- Keep "unverified day always extends"; replace the 3rd-most-recent-day floor
  with a runway rule tied to the TTL (extend only when an **unverified** day's
  bound is closer than a small runway; verified days never extend) — the
  weekend/steady-state cases then stay quiet at 3 d.
- Set `EOD_SAFETY_FLOOR` for a 3 d deployment (no steady-state firing) and
  update the `EodRetentionPolicy`/`02_raw_table_1.sql` contract text
  (7 d/9 d → 3 d).
- Failing-first tests: Friday run → quiet; stuck-unverified day → extension
  with runway; holiday weekend case.

**Applied (2026-09-30, `CHG-463`):**
- Guard: verified days never extend (3-day floor retired); unverified days
  extend when margin < runway; defaults `EOD_TTL=3d`, `EOD_SAFETY_FLOOR=1d`;
  the extension now raises **both** expiry clocks (`table.log.ttl` +
  `auto-partition.num-retention` on partitioned tables — a partition ages out
  by GC as well as TTL, so the old TTL-only ALTER could not protect it).
- Failing-first: 4 red (default 1d, null bound for all-verified,
  oldest-unverified bound, weekend quiet) → green; `Eod*` 70/70;
  `make test` 768 + 559 green; `static-check` 0; gate test 4/4.
- Live ALTER via `fluss-repair/SetRawRetention.java`:
  `before: ttl=9d num-retention=9 datalake.enabled=false` →
  `ALTER OK: ttl=3d num-retention=3 changes=2`; guard status
  `RESULT=OK EXIT=0`; stats probe reads 4 738 673 rows.
- Post-ALTER write/read rides the wave-close `make gate` (the dev feed was
  idle at apply time: `FEED_STALLED`/BACKOFF). Steady-state projection
  ≈650 GB vs ≈1.9 TB — rate unchanged, ceiling moved; no 900 s round can
  show it.

## A3 — compaction investigation (read-only first)

**Question:** the ≈97 % churn share in `candle_live` (and the raw log growth) —
is it bounded by compaction or unbounded? Inventory: Fluss tablet KV/compaction
options, log-segment sizing/TTL, snapshot settings; measure with the S12 series
(`fluss.tablet.bytes`, `flink.changelog.bytes`, RocksDB dirs).
**Deliverable:** a short findings note; if a safe lever exists, a config trial
judged on the meters; otherwise document why cadence/retention are the only
levers.

**Findings (2026-09-30, read-only):**
- `candle_live`'s **log** side is bounded: 60 s `table.log.ttl` (DDL 32), so
  its log segments age out on their own; the ≈97 % churn share sits on the
  **KV/LSM** side (upsert replay into the KV store).
- Fluss 1.0 exposes **no compaction-scheduling knob**: the tablet config
  surface (`FLUSS_PROPERTIES`) carries `log.segment.file-size`,
  `log.retention.check-interval`, `remote.log.task-interval-duration`,
  `kv.snapshot.interval` — memory/sizing knobs, not compaction cadence; no
  `kv.ttl` server default (seal-time deletes are deferred).
- Therefore the only safe levers on the churn meters are **write cadence**
  (A1, landed) and **retention/ceiling** (A2, landed); a multi-hour series
  could quantify KV boundedness but would turn no runtime knob. **No trial
  opened** — acceptable per the A3 plan line.

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
