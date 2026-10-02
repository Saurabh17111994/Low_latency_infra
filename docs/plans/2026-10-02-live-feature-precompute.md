# Live feature precompute — scope plan

**Date:** 2026-10-02
**Status:** **approved 2026-10-02; implemented and runtime-verified** (unit: 694 fresh
reports / 0 failures / 0 errors / 21 env-gated skips; mutation check caught by 5 tests;
`make static-check` 0 failures. Runtime: smoke + clean 900 s A/B with a same-jar ON/OFF
control — strategy-host tail S7/S9 p99 412.8 → 39.4 ms, ingestion equal-or-better, state
growth identical; production restored on the new jar) — CHG-526 filed; `make gate`
certification running.
**Operator decision (2026-10-02):** every registered feature's live value is
materialized on every incoming tick, irrespective of whether any strategy reads
it; Fluss storage stays closed-only (one sealed row per closed window).

## Goal

Make "the board prints every feature every tick" true for **all** cadences:

- TICK and MARKET features: already materialized per tick (unchanged).
- CLOSE features (`sma_close_20`, `rsi_close_14`, all future ones): the live
  value (as-if-closed-now) is computed and stored per acceptable tick, once,
  and shared by every strategy of that instrument.

Mapped to the standing goal:

| Goal | How this design serves it |
|---|---|
| Low latency | computation happens on the host tick path before the fan-out; a strategy read becomes a plain field read; no I/O, no locks, no timers |
| High throughput | O(1) per declared feature, preallocated primitive arrays, no boxing/allocation; computed once per instrument per tick, reused by N strategies; registry precomputes per-TF id arrays so no registry scan on the hot path |
| Less state growth | live slab is heap-only, fixed-size, allocated once; no Flink managed state, no checkpoint/RocksDB content, no per-tick Fluss writes |
| Data correctness | live and closed slots are **physically separate arrays**; the storage snapshot reads closed slots only; pure deterministic previews; NaN/absent semantics preserved |

## Behavior (after this change)

1. **Slot creation (once per instrument):** allocate the live slab
   (`double[registrySize][]` for CLOSE features) next to the existing arrays.
2. **On every forming event (per tick, per timeframe):** after
   `onFormingCandle` stores the forming candle, loop that timeframe's declared
   CLOSE feature ids (`FeatureRegistry.closeIdsFor(tf)`) and store
   `computer.previewOnForming(...)` into the live slab. `onTick`/`onMarket`
   loops are unchanged.
3. **On window close:** `onClosedCandle` finalizes the closed value, sets the
   live slot equal to it, and `snapshot(tf, out)` builds the sealed Fluss row
   from closed slots only.
4. **Strategy reads:** `latest(id, tf)` = closed value (unchanged);
   `latestLive(id, tf)` = precomputed live value (fallback: closed value when
   no forming candle exists). No strategy API change.
5. **Registry:** unchanged and append-only. A new CLOSE feature automatically
   joins steps 2–3 via its declaration (one registry line + pin line).

## Code map (current lines at `93ebd16b` + tonight's tree)

| File | Area | Current lines | Change |
|---|---|---|---|
| `code/02_services/02_compute/src/main/java/com/trading/compute/feature/PerInstrumentFeatures.java` | arrays | 34–51 | add `closeLive` slab (same shape as `closeLatest`) |
| | constructor | 53–97 | allocate/NaN-fill the slab (once per slot) |
| | `onFormingCandle` | 149–166 | store live values for `closeIdsFor(tf)` |
| | `latestLive` | 175–202 | return the slab value; fallback to closed |
| | `onClosedCandle` | 204–228 | after close, copy closed → live for that tf |
| | `snapshot` | 235–249 | **unchanged** (closed slots only) |
| `code/02_services/02_compute/src/main/java/com/trading/compute/feature/FeatureComputer.java` | preview contract | 66–80 | no signature change |
| `code/02_services/02_compute/src/main/java/com/trading/compute/feature/FeatureRegistry.java` | `closeIdsFor` routing | 274–276 | already exists; used, not changed |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StrategyHostFunction.java` | slot creation | 668 | pass the kill-switch flag into the constructor |
| | `updateFeaturesOnTick` / `updateFeaturesOnClose` / `emitSealedMergedRow` | 906–942 / 949–967 / 996–1008 | no logic change (the precompute hangs off `onFormingCandle`) |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java` | env parsing | flag pattern e.g. 219 | add `FEATURE_LIVE_PRECOMPUTE` (default **true**) |
| Tests (module test tree) | `feature/PerInstrumentFeaturesTest.java`, `FeatureComputersTest.java`, `MarketFeaturesTest.java`, `signaljob/StrategyHostFeaturesTest.java` | — | new + existing |

## Cost model

Per instrument per tick (fast feed = one forming event per timeframe):

```
cost = TICK_COUNT + MARKET_COUNT + Σ_tf closeCount(tf)
```

- Today: 1 + 52 + (SMA on 4 TFs + RSI on 3 TFs) = **60** small ops/tick.
- 100 CLOSE features on all 6 TFs (hypothetical): **600** ops/tick.
  At 2 Hz × 2 433 instruments ≈ 2.9 M ops/s; at HFT 50 ticks/s/instrument
  ≈ 73 M ops/s. This is the deliberate always-on trade — measured by the
  profiler before any production enablement.
- Memory: `closeFeatures × 6 × 8 B` per instrument → today ≈ 2.1 KB,
  100 features ≈ 4.8 KB, ×2 433 instruments ≈ **12 MB**.

## Build slices

1. `PerInstrumentFeatures`: live slab + per-forming-event computation +
   `latestLive` repoint + close sync. Registry and storage untouched.
2. Kill switch `FEATURE_LIVE_PRECOMPUTE` (default true → precompute;
   false → today's compute-on-read fallback), plumbed through `SignalJobConfig`.
3. Unit tests (failing-first).
4. Runtime evidence: smoke, then full profiler, compared with the
   pre-change baseline `logs/stage-profile-20261002-210543`.
5. Docs: `docs/08_implementation/04-signal-job.md` + CHG record; then gate.

## Tests (failing-first)

- `liveSlabFilledForEveryDeclaredCloseFeature` — after `onFormingCandle`, the
  live slab holds the preview for every CLOSE feature declared on that tf;
  undeclared tf stays NaN.
- `latestLiveEqualsClosedValueAtClose` — the last precomputed live value equals
  `latest()` after `onClosedCandle` (storage-equivalence proof).
- `storageSnapshotNeverCarriesLiveValues` — advance the forming candle after a
  close; `snapshot(tf)` output is unchanged.
- `liveValuesFollowEveryFormingEvent` — value moves as the forming candle moves.
- `killSwitchFallsBackToComputeOnRead` — flag off → identical values, no slab.
- Existing suites stay green (`PerInstrumentFeaturesTest`,
  `MarketFeaturesTest`, `FeatureRegistryPinTest`).
- Implementation note: no new per-feature failure test — the precompute loop runs
  inside the host's existing guarded feature update, and
  `StrategyHostFeaturesTest.invalidLiveTimeframeIsCountedAndDoesNotBlockDelivery`
  already pins the isolation contract.

## Evidence plan

- Engineering: module tests → smoke → full `make gate` + `make static-check`.
- Runtime: `PHASES=smoke` 1 024-token smoke, then full 900 s 2 433-instrument
  fake-feed profiler (same command as tonight). Compare S4–S9 latency, GC
  summary, container CPU/mem, checkpoint `state_size` curve, sealed-row counts
  and per-TF state growth against the baseline.
- Record: `logs/tracker-14/2026-10-02-live-feature-precompute*.md`.

## Risks and guards

| Risk | Guard |
|---|---|
| Always-on CPU scales with registry × rate | kill switch; profiler A/B; appendix-only registry (reviewed per add) |
| A live value leaks into storage | separate arrays; snapshot reads closed only; dedicated test |
| Hot-path allocation/GC | slab allocated at slot creation; computation allocation-free |
| Restore semantics | live slab is transient heap state, rebuilt on replay; no checkpoint change |
| Feature failure blocks the tick | existing isolation contract: counted, never thrown into the fan-out |

## Out of scope

- Strategies consuming the features (N7 live feature usage) — separate change.
- DDL/table/sink/storage format — untouched.
- New feature definitions — none.

## Approval

Operator sign-off required before implementation.
