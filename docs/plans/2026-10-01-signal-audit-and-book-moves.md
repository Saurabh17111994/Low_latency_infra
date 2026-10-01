# Scope — signal-row audit + book-move firing (2026-10-01)

- **Status:** implemented 2026-10-02 (CHG-504 audit, CHG-505 market rows, CHG-506
  restore fix) — code + tests green, full compute suite 678/0/0; live smoke
  **PASS** on the restored job (24,444 market rows, 0 strategy failures,
  evidence `logs/tracker-14/20261002-signal-market-tick-live-smoke.md`); the v2
  audit on a live fire remains unit-verified until the first live N7 signal.
  Landing note:
  the market row reuses the **existing** `LIVE_TICK_TAG` fast side output
  instead of a new `MARKET_TICK_TAG` + union — a union is an operator with no
  pinned UID, while reusing the tag keeps the job graph byte-identical with the
  flag on *or* off (strictly better for restore; the host already discriminates
  rows by `TF`). Candle-prefix fields are zero-defaulted rather than null (the
  layout's NOT NULL contract on the transport), never read on a market row.
  The restore smoke also exposed and fixed an upgrade-restore defect (CHG-506):
  unaligned checkpoints replay pre-market-section 16-column rows; the host now
  skips their decode and counts `compute.market.row.legacy` instead of crashing.
- **Source:** operator request 2026-10-01 ("Why did this signal fire?" audit on
  the signal row; strategy firing on book moves alone).
- **DDL freeze note:** no schema change is proposed. The audit rides the
  existing free-STRING `score_inputs` column; a future DDL change can formalize
  it into typed columns once the freeze lifts.

## Current state (code-verified)

| Surface | Today | Gap |
|---|---|---|
| `Signal_Candidates.score_inputs` | N7 stamps 7 fields: `tf, side, n7WindowStart, levelHigh, levelLow, n7Range, triggerPrice` (N7RangeBreakoutStrategy.scoreInputs). Nothing reads it. | The market/feature state the strategy evaluated is not on the row — you cannot answer "why did it fire" from the row alone. |
| Market snapshot | 42 values + 2 change clocks captured on every accepted tick (trade or quote) in `MultiTimeframeAggregateFunction.updateMarketSnapshot`. Only the canonical `FIFTEEN_S` forming row carries the 44-column section. | — |
| Forming-row emission | Only accepted TRADE ticks emit forming rows (`emitLiveTick`/`fastLive`). Quote ticks update the snapshot and return. | A book-only move never reaches the host → a strategy cannot evaluate or fire on it. |
| Host dispatch | `StrategyHostFunction.processElement1` decodes the market section into the per-slot `MarketSnapshot`, hands `StrategyView` to `onLiveTick` / `onClosedCandle` / `onContextReady`. | No market-update callback. |
| N7 fire paths | live (`evaluate` on each new trade price) + arm-time cross-stream fire in `onClosedCandle`. | The audit must be stamped on both paths. |

## Item 1 — "Why did this signal fire?" audit on the signal row

**Design.** `score_inputs` becomes a versioned, self-contained audit JSON
(`v:2`), built by the strategy with a shared helper
(`SignalAuditJson`) so every strategy's audit is parseable the same way:

```json
{"v":2,
 "strategy":{"id":"n7-range-breakout-v1","tf":"15m","side":"BUY",
             "windowStart":...,"levelHigh":...,"levelLow":...,
             "range":...,"triggerPrice":...},
 "fire":{"path":"live","detectionTs":...,"evaluationTs":...},
 "candle":{"windowStart":...,"windowEnd":...,"open":...,"high":...,
           "low":...,"close":...,"volume":...},
 "market":{"bid":[{"px":...,"qty":...,"ord":...}, ... 5 levels],
           "ask":[... 5 levels],
           "stats":{"totalBuyQty":...,"totalSellQty":...,"dayOpen":...,
                    "dayHigh":...,"dayLow":...,"prevClose":...,"vwap":...,
                    "openInterest":...,"oiDayHigh":...,"oiDayLow":...,
                    "lowerLimit":...,"upperLimit":...},
           "derived":{"spread":...,"microprice":...,"depthImbalance":...,
                      "totalBidQty":...,"totalAskQty":...},
           "clocks":{"statsChangedAt":...,"depthChangedAt":...,
                     "statsAgeMs":...,"depthAgeMs":...}}}
```

- Size ≈ **1.0–1.3 KB per signal row** (signals are rare; bounded).
- Never-seen values render as `null` (the 0 = not provided convention is kept
  distinguishable); NaN derived values render as `null`.
- N7 gains view-aware overrides (`onLiveTick(RowData, StrategyView, Collector)`,
  `onClosedCandle(RowData, StrategyView, Collector)`) to capture the market at
  fire time; the audit is stamped on both fire paths.
- Alternative offered to the operator: a **compact** block (L1 + derived +
  clocks only, ≈400 B) instead of the full snapshot.

**Tests.** JSON schema pin (fields, `v`, null/NaN handling); N7 audit present
on both fire paths; unchanged `formation_snapshot_ref`; existing N7 tests
updated to the v2 shape.

## Item 2 — strategy firing on book moves alone

**Design.**

1. **Aggregator** — `updateMarketSnapshot` reports whether the snapshot
   changed; on a **non-trade tick whose market changed** (depth or stats),
   emit a market-only row on the **existing** `LIVE_TICK_TAG` fast side output
   (`CandleLiveColumns` layout, `TF = "MKT"` sentinel, identity + market
   section + clocks; candle-prefix fields zero-defaulted — the callback
   contract says only the market section is meaningful). Landing choice: no new
   tag, no union operator, no graph change.
2. **Host** — a `"MKT"` row refreshes `slot.market` and calls the new
   `SignalStrategy.onMarketUpdate(RowData market, StrategyView view,
   Collector<RowData> out)` (**default no-op**). `onLiveTick` is *not* called,
   so N7 and the stub strategy are byte-identical in behavior.
3. **Wiring / flag** — `STRATEGY_MARKET_TICK_ENABLED` (default **false**,
   consistent with the rollout-flag convention; requires
   `STRATEGY_HOST_ENABLED`, which itself requires `MULTITF_ENABLED`). The flag
   gates the emission only; the host branch is always present and the job
   graph is byte-identical with the flag on or off (no new operator at all).
   Inert when `MULTITF_FAST_LIVE_FEED=false` (the 1 s mirror already carries
   the market section).
4. **Semantics** — fire when either change clock advances on a tick that emits
   no live row. Depth-only is a one-line narrowing if preferred.

**Tests.** Aggregator: book change → exactly one market row; repeated identical
book → none; trade tick → none (the live row already carries the section).
Host: market row → `onMarketUpdate` once, `onLiveTick` zero, view refreshed.
A test strategy that emits a signal from `onMarketUpdate` proves firing on a
book move end-to-end; the default no-op proves existing strategies unchanged.

**Perf.** ≤1 row per quote tick that actually changes the book (feed depth
≈1 Hz/instrument); no per-key timers, no new managed state.

## Out of scope

- No DDL/schema change (freeze): audit uses `score_inputs`, no new columns.
- No concrete book strategy rule (capability + tests only). A real
  book-imbalance strategy would be a follow-up request.
- Order-flow dynamics (deferred earlier).

## Risks / rollback

- `score_inputs` v2: nothing reads it today; consumers must key on `v`.
  Rollback = revert (rows are immutable; old rows keep v1/legacy shape).
- Flag off = unchanged graph and no market-row emission; flag on changes the
  aggregator's runtime emission only (no operator, no UID, no state), so a
  restore is unaffected in both directions.
- Audit row size bounded (~1.3 KB); no impact on the hot trade path (signals
  are rare).

## Why these are the native choices

- **Full audit snapshot:** the 42 values + 2 clocks are *already* captured
  natively on every tick (CHG-502) and already ride the canonical row —
  stamping them is serialization only, no new capture path and no new state.
  It is the exact state the strategy read through `StrategyView`, including
  the freshness clocks. The snapshot is a merge of trade+quote ticks
  ("latest non-null wins" per field) and is **not** re-derivable from a single
  raw tick, so a subset audit is lossy. Honest caveat: JSON-in-`score_inputs`
  is the bridge under the DDL freeze; the native end state is typed columns,
  and the `v` field makes that migration mechanical.
- **Either-clock trigger:** the platform already models the market as two
  groups with two change clocks; `MarketView.marketChangedAt()` is the native
  "the market moved" expression. Firing on depth only would silently drop
  legitimate stats updates (OI, day stats, limits); a strategy that wants
  book-only semantics checks `depthChangedAt` itself. Same cost, nothing
  pre-filtered away.
- **Flag default off:** every rollout flag in this repo defaults off, and the
  off state must keep the job graph byte-identical so checkpoint/restore is
  untouched. The landing goes further than the draft: the market row rides the
  existing side output, so the graph is byte-identical even with the flag on
  (a union would have added an operator with no pinned UID). Dev `.env`
  already runs the strategy path, so the live smoke can prove the feature;
  production changes only after a drill. The callback default is a no-op, so
  even flag-on cannot change N7/stub behavior.

## Decisions (operator-approved 2026-10-02)

1. Audit depth: **full snapshot** (accepted; CHG-504).
2. Book-move trigger: **depth or stats change** (recommended, approved with
   the implementation go-ahead; CHG-505).
3. Flag default **off** + enabled in dev `.env` for the live smoke; production
   deck stays off until validated (approved).
