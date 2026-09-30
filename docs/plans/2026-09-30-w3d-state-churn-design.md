# W3-d design note — state-churn cut (timer-driven changelog volume)

- **Status:** implemented (CHG-460, commit `4f065d30`): scan-driven live mirrors replace the
  per-key live timers; failing-first test + mutation check green, full compute suite 638/0.
  Smoke + 900 s round pending. Operator approved the design pass and the implementation
  2026-09-30.
- **Filed:** 2026-09-30
- **Supersedes:** the earlier one-line W3-d framing ("shrink candle structs / fingerprint
  strings") — **that premise is obsolete** (see "Correction" below).
- **Evidence base:** `logs/w3ac-main-20260930-124510`, `logs/ct45-main-20260930-131344`,
  `logs/chg458-smoke-20260930-134645`, TM RocksDB dirs + changelog payloads (read-only),
  pinned 2.2.1 bytecode/metrics.

## 1. Correction — where the checkpoint state is NOT

- `MultiTimeframeState` (6 accumulators + 6 rings × 15 closed candles) is **heap-only by
  design** — "intentional amnesia, not checkpointed" (`MultiTimeframeAggregateFunction`
  line 124: plain `HashMap<Long, Slot>`).
- `FingerprintDedupFunction` "requests no managed state at all" — its fingerprint windows
  are plain fields, not checkpoints.
- Therefore the growing checkpoint `state_size` is **not** the candle/dedup structures and
  cannot be reduced by shrinking their encodings.

## 2. Measured evidence (what actually grows)

| Measurement | Value |
|---|---|
| Checkpoint `state_size` growth | ~1.0–1.7 MB **per 10 s checkpoint**, linear; smoke 2.4 → 42.6 MB in 200 s; main round ~94 MB med / 183 MB max (900 s) |
| Changelog base path growth | ~18–21 MB/min; 70 MB per 4-min smoke; 129 MB in ~6 min of a main round |
| Live-snapshot emissions (`compute.candles.live.emitted`) | **29 211 rows/s** over the 895 s window (≈ 6 TFs × 2 timer kinds × ~2 400 active keys) |
| Live-tick emissions (`compute.candles.live.tick.emitted`) | 4 869/s (the tick path; unaffected by this design) |
| Managed state inventory (whole signal job) | aggregate-function timers; `mtf_candle_closed_written` (ValueState, 68 k keys in the smoke); `strategy_host_emitted_ids(_v2)` (tiny). Nothing else. |
| Registry WARNs ("state is not in tracking") | 457–465 per round, intra-run (persist after the per-phase wipe ⇒ not a stale-file artifact) |

## 3. Root cause

`MultiTimeframeAggregateFunction` schedules **per-key live-snapshot timers** —
one processing-time and one event-time timer per key, re-registered every
`liveSnapshotIntervalMs` (1 s) for the whole job lifetime (lines 417–430 and 744–757):
on each fire it re-registers itself (`timestamp + interval`). With ~2 400 active keys that
is **~4 900 timer fires + ~4 900 registrations per second**, each a managed-state mutation
captured by the changelog since the last materialization. That volume is what the
checkpoint `state_size` reports and what the FS changelog writes: ~20 MB/min of pure timer
bookkeeping. Window-close event timers (~300/s) and sink/strategy flag writes are the
remaining, much smaller contributors.

## 4. Proposed change

**Phase 1 (core — the cut): operator-scope live mirrors, no per-key timers.**

The operator already owns every key's state on the heap (`slots` map), so the live mirror
needs no per-key timers at all:

1. **Processing-time mirror:** one processing-time timer **per operator subtask** at 1 s
   cadence; on fire, iterate `slots` and emit the live rows via the existing
   `emitLiveForTimerSlot` (whose per-key context is available directly from the scan).
2. **Event-time mirror:** on watermark advance — the code already runs a per-tick
   watermark-driven scan (`lastEvictedWatermark`, line 148) — emit for each key whose
   `nextLiveEventTimer` is due (≤ watermark), using the same emission function.
3. Remove the per-key `registerProcessingTimeTimer` / `registerEventTimeTimer` calls for
   the live mirror (window-close and session-close timers stay unchanged).

**Expected effect:** timer mutations collapse from ~4 900/s to ~2/s per subtask + window
timers (~300/s job-wide) ⇒ changelog volume target **<3 MB/min (≥85 % cut)**, checkpoint
`state_size` near-flat instead of 94 MB, disk per run down ~10×, registry/CPU spikes down.

**Phase 2 (optional, only if the gap demands it): window-close timer batching.**
All pending windows are already on the heap (`pending` maps); register the *earliest*
event timer per key and close all due windows on fire (scan-on-fire). Correctness-critical:
closure/lateness semantics — separate design + tests if Phase 1 leaves the tail.

**Phase 3 (optional): bound `mtf_candle_closed_written`.** 68 k keys in a 4-minute smoke,
growing with every closed candle and never pruned — a monotonic state growth. Prune flags
once they are older than the lateness horizon (watermark-based). Correctness-adjacent;
separate CHG.

## 5. Correctness argument (Phase 1)

- The live mirror is a **1 s upsert cadence per key** (diagnostic/live path; the signal
  path is the live-tick emission and the closed-candle path, both untouched).
- Timer condition `fire at lastEmit + 1 s` becomes `emit when watermark/proc-time ≥
  lastEmit + 1 s` in a scan — the same condition, evaluated on the same triggers.
- Per-key row contents are produced by the same `emitLiveForTimerSlot`; only the driver
  changes (scan instead of timer). Cross-key order changes, which is irrelevant for keyed
  upserts; per-key order is preserved by the scan.
- No candle math, no window boundaries, no sink schemas change.

## 6. Risks

- Live-mirror cadence jitter under scan: bounded by scan cost over ~2 400 keys/subtask
  (cheap: map iteration + existing emission); verify via emission counters in the round.
- Missed emission for a key with no new events (scan must still emit the 1 s mirror —
  the per-key timer used to fire without events; the scan has the same property).
- Watermark stall ⇒ event mirror stalls (same as today: event timers need watermarks).
- Phase 2/3 carry genuinely semantic risk — deliberately separated.

## 7. Test plan (failing-first, per repo discipline)

1. Unit (compute module): a test that drives N keys for M seconds and asserts live-mirror
   emissions per key per second equal the timer-driven reference; must fail before the
   change on the "no per-key timers" property (e.g., assert registration sites are the
   operator-scope only).
2. Static pin (gate step 3): the aggregate function must not re-introduce per-key 1 s
   timer registrations (count/context guard on the live-mirror sites).
3. Scoped suite: `mvn -o package` in `02_compute` (637+ tests).
4. Smoke (200 s): flow + emission counters within ±5 % + changelog volume metric.
5. 900 s round: five-axis verdict vs baseline; primary metric = changelog MB/min and
   checkpoint `state_size` series; guardrails = rows/s, cps 100 %, live counters, candle
   row counts.
6. `make gate` when the operator un-defers (job-graph/state changes fall under the gate).

## 8. Rollback

Phase 1 is a self-contained change in `MultiTimeframeAggregateFunction` (timer sites +
scan helpers): revert = restore the two registration blocks; rebuild jar; no state/schema
migration either way (timers are ephemeral; heap state is amnesia by design).
