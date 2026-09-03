# Candle-chain heap plan (2026-09-03) — remove per-tick database cost from all 5 hot stages

## 1. Goal and pass bar

Step-2 proof (job `f0f1374b`, 720 s, new dedup): source **4,930/s**, dedup workload
4.4% (was 52%). The jam moved downstream. Per-task gauges + subtask split (even,
no straggler) + disk signature (87% busy, small writes, CPU 73% idle) + code
op-count agree: 5 stages each pay database reads+writes **per tick**, and the
builder→detection chain (1.5 ms/tick/worker) predicts the measured ceiling
almost exactly (645/s/worker × 8 ≈ 5,100/s ≈ 4,900 observed).

Fix all 5 at once with the proven dedup trick (per-key plain-memory state, no
managed state). Predicted ceiling ≈ 20,000/s (peak). ONE implementation, ONE
proof run.

| Gate (720 s proof, 10 Hz feed + injected repeats) | Bar |
|---|---|
| Source avg | ≥ 8,000/s (stretch: 20 Hz peak run same day if headroom ≥ 2×) |
| builder/detection/candle/preview/writer workload | each < 25% |
| Source backpressure | < 20% |
| Checkpoints | p50 ≤ ~20 s, 100% success |
| Zero loss, clean portion | sink-in = feed − injected dups |
| Injected repeats | `compute_dedup_duplicates` Δ == injected total |
| Tick age | p50 < 5 s |

## 2. What changes (5 operators) and what is reused untouched

The key review-saver: all MATH is pure functions already and is reused
byte-for-byte. Only state-holding moves (database → heap):

| Reused unchanged | Used by |
|---|---|
| `CandleAggregateFunction.add` (no-decode hot path) | builder, candle window, preview |
| `CandleEmitFunction.buildRow` + `CandleInvariantCheck` + quarantine row builder | candle window |
| `CandlePreviewEmitFunction.buildRow` | preview |
| `FormingBarRowMapper.toRow` | writer |
| `CandleLateDrop.OUTPUT` tag + `CounterFunction` | candle window late path |
| `CandleKvFirstWriteWinsFunction`, all sinks, early-signal, signal-detection | untouched |

### OP1 — forming-bar-builder → heap (same class, rewritten state)
- Per-key heap: `windowStart` + `CandleAccumulator`, via the SAME `aggregate.add`.
- Same emit: one `FormingBar` (record, shared reference) to main + `PERSIST_OUTPUT`
  side output (tag kept, downstream wiring unchanged).
- Restore: starts empty; rebuilds from live ticks (one forming window partial
  per key per restore — accepted trade, §5).
- Guards: per-key single acc (nothing to bound); global key-count cap
  fail-closed (same G-DEDUP-2 shape).
- Uid: `forming-bar-builder-v2` (fail closed on old checkpoints).

### OP2 — forming-bar-detection → heap (same class, rewritten state)
- Per-key heap: `currentWindow`, `firedWindow`, two `ArrayDeque<Long>` trimmed
  to `formingLookbackCandles` (structural bound, dedup-Window pattern).
- Evaluation ORDER preserved exactly (current-window update before evaluate;
  strictly-prior guard; fire-once; warm gate).
- Restore: empty; re-warms in lookback×15 s. Uid `forming-bar-detection-v2`.

### OP3 — forming-bar-writer → heap (same class, rewritten state)
- Per-key heap: latest `FormingBar` + pending-timer timestamp. Processing-time
  batch timer preserved (`formingBarWriteBatchMs`); coalesce + clear-on-emit
  preserved; `toRow` reused.
- Restore: empty; at most one batched write per key delayed to next batch.
- Uid `forming-bar-wins`… `forming-bar-writer-v2`.

### OP4 — candle-15s window → heap window function (type change: window → `KeyedProcessFunction`)
- Per-key heap: `windowStart` + `CandleAccumulator` + `emitted` flag + retained
  last-window acc until watermark passes end+allowedLateness (late-update
  counting preserved).
- Semantics preserved, item by item: epoch alignment `(t/w)*w`; event-time
  timer at window end = FIRE_AND_PURGE; beyond-lateness ticks → `CandleLateDrop.OUTPUT`
  side output (downstream counter untouched); late-but-within-lateness ticks
  after emit → counted no-op (same as emitted-flag path); invariant check +
  quarantine side output via reused builders; empty windows emit nothing
  (timer registered on first tick only — same as today).
- Restore: current window partial per key per restore (accepted trade, §5).
- Uid `candle-15s-v2` (mandatory: operator type changes, old state unreadable).

### OP5 — candle-preview window → heap preview function (type change)
- Per-key heap: `windowStart` + `CandleAccumulator`; event-time interval timer
  anchored at `window.start + previewIntervalMs`, re-armed `time + interval`
  (same cadence as `CandlePreviewTrigger`); emit via reused preview `buildRow`.
- Behavior note (improvement, §4 item 3): beyond-lateness ticks are silently
  dropped by Flink today (no side output on the preview window); heap version
  drops + counts them (`compute.candles.preview.late.dropped`). Silent→counted
  is observable-only, no downstream effect (previews never feed detection —
  wiring comment in SignalJob).
- Restore: same partial-window trade. Uid `candle-preview-15s-v2`.

## 3. Out of scope (audited, deliberately untouched)
- early-signal (inputs ~900/s, rate-decoupled from ticks; 32% today → ~35% at
  20 k ticks/s — fine), signal-detection (29/s in), kv-first-write-wins
  (29/s), all sinks (~4%, starved), raw-validation (chained, pure),
  source/fetch (exonerated twice).

## 4. Improvements found during the audit (beyond the 5 rewrites)
1. Detection's write-before-warm-check becomes free with heap (order kept, no
   semantic change) — the wasted write disappears structurally.
2. Builder's double emit is already the shared immutable reference — no extra
   allocation; no change needed (checked, not assumed).
3. Preview late-drop silent→counted (above) — closes an observability hole the
   audit found; no behavior change downstream.
4. Writer's per-tick whole-`FormingBar` database write disappears with heap —
   this was the hidden half of OP3's 36.6%.
5. Step-3 repeats test folds INTO the single proof run (injection ≈ 800 rows /
   720 s — negligible load effect; speed + dup-drop gates in one run, one
   12-minute cost instead of two).

## 5. Accepted trade (the only one) and why it is safe
After a crash/upgrade restart, each key's currently-forming window loses its
pre-restart ticks (one partial candle per key per restart). Completed candles,
signals, saved tables, dedup exactness are unaffected. Restarts are rare
(failures/upgrades only); sources resume from checkpointed offsets so nothing
is double-counted — the window simply covers fewer ticks that once. The old
code paid ~1.5 ms/tick/worker for exactness nobody consumes downstream at
restarts (kv-first-write-wins already guards re-emission effects).

## 6. Tests (mirror the dedup precedent: contract + guardrail per operator)
- OP1/OP2/OP3: harness contract tests (window alignment, lookback trim bound,
  fire-once, coalesce batching, warm gate, restore-empty rebuild) + global-cap
  trip tests where a cap exists.
- OP4: window-EQUIVALENCE tests vs `TumblingEventTimeWindows` semantics on
  scripted feeds (alignment, watermark firing, late within/beyond lateness,
  multi-window advance, empty windows emit nothing) + emit/quarantine paths.
- OP5: cadence-equivalence tests (interval-anchored fires, re-arm, late-drop
  counting) + row-content test via reused `buildRow`.
- Existing tests for reused math (`CandleAggregateFunctionTest`,
  `CandleEmitFunctionTest`, `CandlePreview*Test`, late-drop, quarantine,
  kv-fww) stay green unchanged — they pin the reused code.
- `SignalJobOperatorUidTest`: 5 uid bumps (`-v2`).
- Full compute + common suites green (pre-existing manifest-count failure
  excluded — separate fix).

## 7. Validation (single round, no iteration)
1. Per-operator tests + full suites (above).
2. `mvn package`, jar content check (no `*State` descriptors requested by the
   5 operators — assert via a unit test that open() touches no managed state:
   Sierra-style `getState` never called; harness `numKeyedStateEntries()==0`
   per operator test, as dedup's test does).
3. ONE 720 s proof run (10 Hz + F2/F3 injection shape), §1 gates. Stretch: same-day
   20 Hz peak run only if headroom ≥ 2×.
4. Rollback: prior `compute.jar` restore + restart (jar backed up before swap);
   first deploy starts WITHOUT restore (new uids fail closed on old
   checkpoints by design).

## 9. Restore-related tests that MUST change (found in self-review, 2026-09-03)

- `CandleRocksDbRestoreIntegrationTest.rocksDbBackendRestoresDedupWindowAndSinksAcrossWorkers`
  WILL break: it restores MID-window and asserts exact `tickCount` (w23 closes
  with count 1 combining pre+post-restore ticks; w24 with 4). With heap windows
  the pre-restore ticks are gone — w23 never fires. Rework to the empty-restart
  contract (same pattern as the dedup savepoint IT): already-written LOG keys
  unchanged (Fluss table persists across phases — that assertion survives
  untouched), pending mid-window windows restart from post-restore ticks,
  strict restore still succeeds. Its RocksDB-artifact assertions (store dir,
  checkpoint SST files) stay: event-time timers + early-signal/detection/kv
  state keep the backend non-empty — verified by suite run, not assumed.
- `FormingBarRehydrationIntegrationTest` — SAFE, no change: tests the Fluss KV
  store directly, no Flink operators involved.
- `SignalJobSavepointRestoreIntegrationTest` (dedup empty-restart) — SAFE, no
  change: asserts job-level strict restore + dedup rebuild, both preserved.
- `SignalJobOperatorUidTest` — 5 uid bumps (`-v2` each, §2).
- `CompatFlinkCheckpointRescaleIntegrationTest` — generic rescale, no candle
  operators; suite will confirm, no planned change.

## 10. Fallback (explicit, pre-approved alternative)

If OP4's equivalence tests prove hairy during implementation: ship OP1/OP2/OP3/OP5
(heap, low risk) and keep OP4 windowed (exact). Predicted ceiling then ≈ 10 k/s
(candle window becomes the cap) — passes the 8 k bar with headroom but not the
20 k peak. OP4 becomes a second step with its own proof run. Default is the full
5-operator plan above; fallback needs no new approval to take (it is strictly
less change), but the 20 k prediction is withdrawn with it.

## 11. Risks and mitigations
| Risk | Mitigation |
|---|---|
| Timer/watermark mismatch in OP4/OP5 vs Flink windows | Same watermark strategy; harness tests advance watermarks explicitly; equivalence tests §6 |
| Allowed-lateness edge differs | Dedicated late within/beyond tests; late counters compared in proof |
| Preview cadence drift | Same event-time anchoring; cadence test |
| Fix under-delivers (e.g. 12 k not 20 k) | Dose table attributes per stage; remaining limiter named by same gauges, not guessed; fallback §10 |
| Restore-trade wider than stated | §9 names every restore test + its fate; suite run proves the list complete (any red test not listed stops the line) |
| Change too big to review | Math reused untouched (§2 table); review surface = state-holding only, ~5 small files |

## 12. Implementation record (2026-09-03, all OP1-OP5 shipped)

Status: all five operators rewritten to heap state and wired; full compute suite
green: Tests run: 439, Failures: 0, Errors: 0, Skipped: 22 (gated ITs skipped —
they need the dev Fluss cluster + COMPUTE_INT_TEST_P6 gate; the RocksDB restore
IT was reworked to the empty-restart contract, §9).

New/changed files (main):
- `HeapCandleEmitFunction.java` (OP4, replaces `keyBy→window→aggregate→
  CandleEmitFunction`; uid `candle-15s-v2`)
- `HeapPreviewFunction.java` (OP5, replaces preview window branch; uid
  `candle-preview-15s-v2`)
- `FormingBarBuilderFunction.java` (OP1, heap slots; uid `forming-bar-builder-v2`)
- `FormingBarDetectionFunction.java` (OP2, heap slots + lookback rings; uid
  `forming-bar-detection-v2`)
- `FormingBarWriterFunction.java` (OP3, heap coalesce buffer; only the
  processing-time flush timer stays managed; uid `forming-bar-writer-v2`)
- `SignalJob.java` — wires the heap operators; uids bumped (G-CHAIN-3)

Per-operator fail-fast guards (user directive: if it fails, we know WHY):
- G-CHAIN-1 structural bounds: lookback rings trim to the lookback; preview
  accumulator dropped at its window's end fire; candle closed-window marker
  evicted past end+lateness.
- G-CHAIN-2 global caps fail closed (never silent eviction): 65 536 slots per
  operator (64× headroom over the 1024-instrument universe), ≤16 pending candle
  closes per key, ≤8 live preview windows per key.
- G-CHAIN-3 uid bumps so pre-redesign checkpoints fail closed on restore
  (no allowNonRestoredState) instead of silently attaching old window state to
  heap operators.
- Loud restore no-ops: `compute.candles.restored_timer_noop`,
  `compute.candles.previews.restored_timer_noop` count restored timers with no
  heap slot; heap no-op counters assert in tests (never silent).
- Deliberate-edge counters: candle late updates, candle late drops, preview
  late drops (`compute.candles.previews.late.dropped`).

Tests added/changed (all green, suite 441/0/0/22): builder 10, detection
14, writer 8, `HeapCandleEmitFunctionTest` 10 (differential vs real window
operator), `HeapPreviewFunctionTest` 8 (differential vs preview window operator
+ interval-guard fail-fast), `SignalJobOperatorUidTest` (4 uid bumps; NO
preview entries — that env has no preview table so the branch is absent),
`CandleRocksDbRestoreIntegrationTest` reworked (empty-restart contract: 48 rows,
w23 absent after restore, w24 exact).

Review cross-check (2026-09-03, code-review skill) established and fixed three
material findings: (1) two preview UID entries (one duplicated) would have
failed the gated UID contract test, which pins the previews-off graph —
removed with a NOTE; (2) a detection empty-slot cleanup that could never fire
(reject path implies a non-fresh slot, admit path fills the rings) — removed as
dead code, replaced with an honest boundedness test (one slot per unseen key,
rings trimmed, global cap bounds); (3) a preview `interval > windowMs`
misconfiguration orphaned accumulators until the live-window cap killed the job
mid-run (the old operator silently degraded) — now fails fast at `open()` with
both key names in the message, covered by a test. Also removed the now-unused
`TumblingEventTimeWindows` import from `SignalJob`. Deliberate keeps: old
window/trigger/emit classes stay as the differential-test oracle (and share
`buildRow`); the preview late-drop-after-purge is a documented improvement over
the oracle (counted, never resurrected). Residual risk: the four gated ITs
(uid/restore/rescale/savepoint) only run under COMPUTE_INT_TEST_P6 + dev
cluster — unit-covered, cluster-unproven until the proof run.

Perf probe (harness, in-process): CHAIN[builder] p99=10.5µs,
CHAIN[candle] p99=2.8µs, CHAIN[preview] p99=2.2µs — all < 50µs budget.

Next: (1) approval for the single 12-min proof run (RATE_HZ=10 + injected
repeats); (2) rebuild jar (host mvn -o package); (3) per-stage dose verdict vs
the §7 predicted ~20 k/s ceiling.

## 13. Proof-run verdict (2026-09-03, job f92ddaa7, 12-min @10Hz + injections)

Run `logs/tracker-14/stage-a2-baseline-20260903-193144`, feed 10,240/s,
faketool INJECT 4 rounds x (200 dups + 20 late-90s). G24 OK, G25 OK
(source avg 10,249/s >= 8,192/s floor), no FAILURE.txt.

| Gate | Result |
|---|---|
| Source throughput | 10,249/s (100% of feed; step-2 run: 4,930/s) |
| Per-op busy (was step-2) | SOURCE 1.7 / DEDUP 2.4 / CANDLE 2.7 / BUILDER 3.8 / DETECT 3.6 / WRITER 4.2 / PREVIEW 3.9 % (was 32-48% x5) — all idle ~96%, bp 0.0% |
| Checkpoints | 330/330 COMPLETED, 0 failed, p50 1.7s p95 2.0s max 2.3s (gate: p50 <= 20s) |
| Data age | p50 197ms p95 489ms p99 697ms max 992ms (gate: p50 < 5s) |
| Duplicates | dedup dropped 597 = 3 in-window rounds x 200 (round 1 fired pre-capture); no repeat leaked downstream |
| Candle equality | 47,104 closes = 46 windows x 1024 keys EXACT; quarantine 0 |

Dose verdict vs §7: every predicted limiter is gone — no operator above 5%
busy at full feed. The ~20 k/s ceiling itself is NOT yet proven (10 Hz feed
cannot load past 10,240/s); the remaining proof is a 20 Hz tier run.
