# Multi-Timeframe Candle Aggregator — Design Document

**Date:** 2026-09-05
**Status:** DESIGN ONLY — no code, DDL, Docker, or CI changes in this document
**Repo root:** `streaming_project_New/`
**Permitted edit surface (future implementation):** `SignalJob.java`, `SignalJobConfig.java`, `TableContractValidator.java`, and the operator-UID contract test — nothing else until Phase 6 user approval.
**Old 15 s-only path:** stays running side-by-side until explicit user approval post-proof.

> **How to read this doc.** Plain English first, trader-readable. Engineering detail second in indented blocks, tables, and worked examples. A reader with no session context can follow end-to-end. Every normative claim cites the evidence files listed in §0. No code in this doc is implementation — it is specification.

---

## 0. Evidence Read (read-only before writing)

This design reuses — not rewrites — the contracts and math proven in:

- **Wiring / operator identity:** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java` — source → `raw-validation` → `fingerprint-dedup-v2` → `candle-15s-v2` / `candle-preview-15s-v2` → `candle-kv-first-write-wins` → `feature-candles-15s-sink`, plus `forming-bar-builder-v2` / `forming-bar-detection-v2` / `forming-bar-writer-v2` → `forming-bar-sink`, plus `signal-detection` → `active-signal-feedback` → `signal-candidates-sink` + `canonical-signal-filter` → `signal-candidates-current-sink`. All operators carry explicit `.uid()` (checked by `SignalJobOperatorUidTest` — 17 pinned UIDs today). Changes to wiring must bump UIDs (G-CHAIN-3 pattern) so old checkpoints fail closed.

- **Accumulation math to reuse:** `CandleAggregateFunction.java` — `add(RowData, CandleAccumulator)` does in-place OHLCV: OHLC from `last_price_paise` of every accepted row, volume + `tickCount` only when `tick_type.equals("TRADE") && qty>0`, open/close from deterministic `(event_time, fingerprint)` order key, not arrival order. `CandleAccumulator.java` — public class + public fields (Flink POJO rule, guarded by `CandlePojoRecognitionTest`); fields `exchange, symbol, openPaise, highPaise, lowPaise, closePaise, volume, tickCount, firstEventTime, firstFingerprint, lastEventTime, lastFingerprint, lastIngestTs`.

- **Heap-state pattern + timer cadence to mirror:** `FormingBarBuilderFunction.java` (per-key heap `Slot { windowStart + CandleAccumulator }`, one small alloc per window, `PERSIST_OUTPUT` side-output, `GLOBAL_SLOT_CAP=65_536`), `HeapCandleEmitFunction.java` (heap windows: `formingStart + forming Acc + LinkedHashMap pending + LinkedHashMap emitted`, `GLOBAL_SLOT_CAP=65_536`, `MAX_PENDING_CLOSES=16`, event-time timer at `windowEnd`, eviction at `windowEnd+allowedLateness`), `HeapPreviewFunction.java` (heap preview: `LinkedHashMap accs`, `GLOBAL_KEY_CAP=65_536`, `MAX_LIVE_WINDOWS=8`, first preview at `windowStart+interval`, re-arm `min(next, windowEnd)`, 60 s TTL via DDL).

- **Lookback to generalise:** `SignalLookbackState.java` — bounded ring buffers (`highs`, `closes` keyed `ValueState<List<Long>>`), `isWarm()`, `maxHigh()`, `evaluate(open, close)` (bullish + breakout + trend filter, no signal before `lookback` completed candles). Today one lookback per instrument; the multi-TF aggregator needs one ring **per timeframe** per instrument.

- **Column layout (must mirror DDL order):** `RawTableColumns.java` (21 columns, `EVENT_TIME=8`, `TICK_TYPE=11`, `LAST_PRICE_PAISE=12`, `LAST_QTY=13`, validated against `RawTableSchema.FIELD_COUNT`), `SignalJobConfig.java` (flag pattern: `booleanValue(env, key, default)`, `positiveLong` for cadences, `validateStartupMode` gate, fail-closed on unknown values), `CandleWatermarkStrategy.java` (bounded out-of-order `WATERMARK_OUT_OF_ORDER_MS=500 ms` + `SOURCE_IDLE_MS=15 000 ms`, `withIdleness`, timestamp assigner from `event_time`).

- **DDLs (column order must match):** `02_raw_table_1.sql` v3 (21 cols, first column `event_day STRING` partition key `yyyyMMdd` IST, `PARTITIONED BY (event_day)`, `bucket.key=instrument_token`, `bucket.num=16`, `table.log.ttl=7d`, `table.auto-partition` DAY/Asia/Kolkata), `03_feature_candles_15s.sql` (KV, `PRIMARY KEY (instrument_token, window_start) NOT ENFORCED`, 15 cols, `table.kv.format-version=2`), `30_feature_candles_15s_preview.sql` (KV, same PK, 15 cols incl. `is_preview, last_event_ts`, `table.log.ttl=60s`, `table.datalake.enabled=false`), `04_forming_bar.sql` (KV, `PK (instrument_token)`, 11 cols, `table.log.ttl=7d`), `05_signal_candidates.sql` (LOG, no PK, 22 cols), `23_signal_candidates_current.sql` (KV, `PK (instrument_token)`, 22 cols).

- **Tests that define the patterns to mirror:** `HeapCandleEmitFunctionTest` (differential equivalence vs real tumbling window — main rows minus `output_ts` + late-drop + quarantine must match tick-for-tick), `CandlePojoRecognitionTest` (POJO type extraction), `FormingBarBuilderFunctionTest` (per-tick snapshot, `PERSIST_OUTPUT` carries every tick, window transition, heap no-managed-state), `SignalDetectionFunctionTest` (warm-up, breakout rule, per-instrument keying), `CandleAggregateFunctionTest` (mixed trades+quotes OHLCV, fingerprint tie-break, `lastIngestTs`), `SignalJobOperatorUidTest` (17 pinned UIDs, duplicates/missing fail).

---

## A. The 13 Agreed Decisions (verbatim — normative)

> These 13 decisions were agreed with the user before this document was written. They are encoded here word-for-word and every later section is traceable to them. If this document conflicts with them, this section wins.

1. One tick-level incremental aggregator: a single Flink operator keyed by instrument holds ALL 6 timeframes' OHLCV in ONE composite state entry; every trade tick mutates all forming candles in memory. No separate window per timeframe.

2. Feed = Arrow FULL packet (on_full_tick, pkt_type 2). A trade = last-trade fields (ltq/ltt/volume) advanced. Quote-only packets update bid/ask fields only, never OHLC. Server ts is event time. NOTE: ingestion already maps validity to tick_type TRADE/QUOTE in raw_table_1 and CandleAggregateFunction.add already filters tickType.equals("TRADE") && qty>0 — the design must reuse this, NOT build a new classifier.

3. Market = NSE cash 9:15-15:30 IST. Pre-open/post-close filtered. 3m/5m/15m candles align to session open 9:15; 15s/30s/1m epoch-aligned. Buckets half-open [start,end). Nothing carries overnight; forced roll at 15:30.

4. Timeframes = 15s, 30s, 1m, 3m, 5m, 15m. All six get live 1s-updating candles (candle_live) + permanent closed history (candle_closed).

5. Higher timeframes built from ticks directly, never rolled up from lower-TF state.

6. Signals consume forming candles per-tick via in-JVM side-output from the aggregator; signal engine gets live candle + last 15 CLOSED candles of EACH timeframe from in-memory keyed state. Zero Fluss/SSD/network reads on the tick->signal hot path.

7. Signals act immediately on forming candles (Option A, user approved): fire and act the moment the rule triggers on a forming candle. No waiting for close.

8. Every signal is kept forever (user approved keep-every-signal): signal output appends ONE full-detail row per signal to existing Signal_Candidates (log, append-only) + Signal_Candidates_current (KV latest per instrument). Since forming-candle signals can later close "against" the trigger, record this as ACCEPTED-BY-DESIGN: rows are never retracted; downstream handles correction with the next signal.

9. Signal output stops at the Fluss signal table: appending the row is the new system's full responsibility. Placing broker orders at Arrow is a SEPARATE part of the project (existing Trade_Decisions/Execution_Intent machinery reads the signal tables). The new engine NEVER talks to the broker.

10. Storage writes happen only three ways: signal row, 1s live-candle refresh, boundary boundary-close write. Never per-tick.

11. Memory target: ~1-2 MB composite state for 2433 instruments x 6 TFs, flat regardless of throughput (in-place arithmetic, no tick buffering, one state round-trip per tick).

12. Safety built in: monotonic event time, loud late-drops, restart/gap handling (drop stale forming candle + discontinuity marker, never emit a candle with a hole), closed-candle first-write-wins immutability.

13. Old 15s-only path keeps running side-by-side; removal only after explicit user approval post-proof.

---

## B. Architecture Picture

### B.1 Plain-English story

Arrow sends one FULL packet per instrument per tick (pkt_type 2). The ingestion bridge already decoded it, stamped server time as `event_time`, and wrote one row per tick into `raw_table_1` with `tick_type` set to `TRADE` (a real print happened) or `QUOTE` (only bid/ask moved). Prices there are in paise, the market clock is IST, and the day folder is `event_day`.

The compute job reads that log from the earliest offset (or from a checkpoint on restart), assigns each row its event time, validates it, deduplicates it by fingerprint, and throws away anything that is not a real trade for candle purposes (the existing `TRADE && qty>0` gate — reused, not reinvented; quote-only ticks still update the per-instrument bid/ask snapshot but never touch OHLC). Pre-open and post-close ticks are filtered.

One keyed operator — **the multi-timeframe aggregator** — owns everything for an instrument in a single state entry: six forming accumulators (one per timeframe), six rolling buffers of the last 15 closed candles each, the latest bid/ask snapshot, and a discontinuity marker. Every trade tick does six in-place `CandleAggregateFunction.add` calls, one per timeframe — no buffering, no lower-timeframe roll-up, no per-timeframe window. Higher timeframes see the same ticks as 15 s, just with wider bucket boundaries.

Three things leave the aggregator, and only three:

1. **In-JVM signal context** — a per-tick side-output (an in-memory object, no network, no disk) that hands the signal engine the six live forming candles plus the six × 15 closed-candle histories, all from heap state. The signal engine evaluates immediately and may fire. This is the hot path: tick → aggregator → signal, zero Fluss reads.

2. **1 s live-candle refresh** — once a second (wall time), the live forming candle for each of the six timeframes is upserted to `candle_live` (KV, `PK (instrument_token, tf, window_start)`). This is the trader-visible "growing" candle and the only periodic storage write.

3. **Boundary close** — when any timeframe's bucket ends (15 s, 30 s, 1 m, 3 m, 5 m, 15 m boundaries aligned per §D), the forming accumulator for that timeframe is sealed, validated, and written once to `candle_closed` (immutable, first-write-wins). Its OHLCV is then appended to that timeframe's rolling last-15 buffer for future signal checks. At 15:30 IST the still-forming buckets of every timeframe are force-rolled.

Signal rows, when the engine fires, are appended to the two existing signal tables — `Signal_Candidates` (LOG, every signal forever) and `Signal_Candidates_current` (KV, latest per instrument, upsert). Signals stop there; the existing `Trade_Decisions` / `Execution_Intent` machinery reads those tables later. The new engine never calls Arrow/broker.

The old 15 s-only chain (`candle-15s-v2` → `feature_candles_15s`, `candle-preview-15s-v2` → `feature_candles_15s_preview`, `forming-bar-builder-v2` → `forming_bar`) keeps running untouched on the same deduped stream so the new aggregator can be proven without disturbing production.

### B.2 ASCII diagram

```
                    Arrow FULL packets (on_full_tick, pkt_type 2)
                                  │
                                  ▼
                         ┌─────────────────┐
                         │  01_ingestion   │  validity → tick_type TRADE/QUOTE
                         │  (bridge)       │  server_ts → event_time (ms)
                         └────────┬────────┘
                                  │ one RowData per tick → raw_table_1 (LOG, 21 cols)
                                  │   event_day=yyyyMMdd IST, partition key
                                  │   bucket.key=instrument_token, 16 buckets
                                  ▼
               ┌──────────────────────────────────────┐
               │ FlussSource raw_table_1               │  OffsetsInitializer
               │ Watermark: CandleWatermarkStrategy    │  bounded 500 ms + idle 15 s
               │ Timestamp: event_time                 │
               └──────────────┬───────────────────────┘
                              │ ticks (RowData)
                              ▼
               ┌──────────────────────────┐
               │ raw-validation            │  uid: raw-validation
               │ (flatMap, pure)           │  schema/version gate, null guards
               └──────────────┬───────────┘
                              │ valid ticks
                              ▼
               ┌──────────────────────────┐
               │ fingerprint-dedup-v2      │  uid: fingerprint-dedup-v2
               │ keyBy(instrument_token)   │  5-min exact dedup, keyed state
               └──────────────┬───────────┘
                              │ deduped ticks
                              ▼
               ┌──────────────────────────┐
               │ market-session filter     │  NEW, pure: 9:15-15:30 IST
               │ (9:15 ≤ event_time <     │  pre/post filtered, counted
               │  15:30; date-aware)       │
               └──────────────┬───────────┘
                              │ in-session ticks
                              ▼
               ┌──────────────────────────┐
               │ trade gate                │  REUSE: CandleAggregateFunction gate
               │ if tick_type==TRADE       │  quote-only → bid/ask side path
               │    && qty>0 → aggregator  │  (OHLCV never touched)
               └──────────────┬───────────┘
                              │
              ┌───────────────┴────────────────┐
              │                                │
    quote-only│                                │trade
    bid/ask   │                                │
    update    │                                ▼
              │                ┌─────────────────────────────────────┐
              │                │ MULTI-TF AGGREGATOR                  │  uid: multi-tf-aggregator-v1 (NEW)
              │                │ keyBy(instrument_token)              │  KeyedProcessFunction (heap state)
              │                │ ONE composite state per instrument:  │
              │                │  • 6 × forming CandleAccumulator    │
              │                │  • 6 × ring[15] closed Candle lite  │
              │                │  • quote snapshot (bid/ask)          │
              │                │  • discontinuity marker + gaps       │
              │                │  • per-TF windowStart                │
              │                │ Per trade tick: 6× aggregate.add     │
              │                │ Timers: 1 s processing-time + per-TF│
              │                │   event-time boundary timers         │
              │                └────────┬────────┬─────────┬──────────┘
              │                         │        │         │
              │          in-JVM side    │ 1 s    │boundary │
              │          output (hot)   │ KV     │ close   │
              │                         │ upsert │ write   │
              ▼                         ▼        ▼         │
     ┌─────────────────┐    ┌────────────────┐  ┌──────────────────┐
     │ quote snapshot  │    │ candle_live    │  │ candle_closed    │   existing signal path
     │ (inside same    │    │ KV: PK         │  │ immutable LOG/KV │   reads signal tables later
     │  composite      │    │ (instrument,   │  │ PK (instrument,  │   ─────────────────────
     │  state entry)   │    │  tf,           │  │  tf,            │   downstream (SEPARATE project):
     │                 │    │  window_start) │  │  window_start)   │   Trade_Decisions /
     └─────────────────┘    │ 6 TF × 1 s     │  │ first-write-wins │   Execution_Intent reads
                            │ TTL 60 s for   │  │ 7d, lake offload │   Signal_Candidates
                            │ live leg       │  └────────┬─────────┘
                            └────────┬───────┘           │
                                     │           closed OHLCV → ring[15] per TF (heap)
                                     │                   │
              ┌───────────────────────────────────────────┼──────────────────────┐
              │  SIGNAL ENGINE (in-JVM, same subtask)    │                      │
              │  consumes per-tick: 6 live forming + 6×15 closed (heap)         │
              │  Option A: fires IMMEDIATELY on forming candle                  │
              │  no Fluss/SSD reads on hot path                                 │
              └───────────────────────┬─────────────────────────────────────────┘
                                      │ fires → one row per signal
                                      ▼
                          ┌─────────────────────────┐
                          │ Signal_Candidates (LOG) │  append-only, every signal forever
                          │ Signal_Candidates_current│  KV PK(instrument) — latest upsert
                          │ idempotencyKey inside    │  ACCEPTED-BY-DESIGN: never retracted
                          └────────────┬────────────┘
                                       │
              ┌────────────────────────┴─────────────────────────┐
              │ downstream (existing, NOT this system's job):     │
              │ Trade_Decisions / Execution_Intent consumers      │
              │ read signal tables → gateway → Arrow broker       │
              └───────────────────────────────────────────────────┘

  ── side-by-side (untouched until Phase 6) ────────────────────────────────────
   deduped ──keyBy──► HeapCandleEmitFunction (candle-15s-v2) ──► feature_candles_15s
           ──keyBy──► HeapPreviewFunction (candle-preview-15s-v2) ──► feature_candles_15s_preview
           ──keyBy──► FormingBarBuilderFunction (forming-bar-builder-v2) ─┬─► forming_bar
                     FormingBarWriterFunction (forming-bar-writer-v2)   ─┘
  ──────────────────────────────────────────────────────────────────────────────

  Legend: ──► data flow   ──┤ storage/sink   ╱ side-output (in-JVM)   heap = on-heap, not RocksDB
```

### B.3 Component responsibilities

| Component | What it does | What it never does |
|---|---|---|
| **Ingestion bridge** (existing) | Decodes Arrow FULL, classifies `tick_type`, writes `raw_table_1` with `event_day` + `event_time` | Never filters market hours, never builds candles |
| **Raw-validation** (`RawValidationFunction`) | Rejects malformed rows, counts by reason, lets `TRADE` + `QUOTE` through | Never drops by business rule |
| **Fingerprint dedup** (`FingerprintDedupFunction`, `fingerprint-dedup-v2`) | Exact 5-min dedup keyed by `instrument_token` + fingerprint, checkpointed with offset | Never does time-based expiry beyond the window |
| **Market-session filter** (new, pure) | Drops rows with `event_time` outside `09:15:00.000 ≤ t < 15:30:00.000` IST for that instrument's `event_day`; counts `compute.session.filtered.{pre,post}` | Never mutates state, never touches OHLC |
| **Trade gate** | Splits `TRADE && qty>0` (to aggregator) vs `QUOTE` / zero-qty (to quote-snapshot only) — reuses `CandleAggregateFunction.add` predicate | Never builds a new classifier |
| **Multi-TF aggregator** (`multi-tf-aggregator-v1`, new) | Holds composite state per instrument, does 6× `aggregate.add` per trade tick, owns 1 s + boundary timers | Never does per-tick Fluss writes, never rolls lower → higher TF |
| **Signal engine** (co-located with aggregator, new) | Receives in-JVM side-output, evaluates per-TF rules against forming + last-15 closed, emits immediately (Option A) | Never reads Fluss on the hot path, never calls broker |
| **Old 15 s chain** (existing, untouched) | Continues exactly as today for soak comparison | Not removed until Phase 6 user approval |

---

## C. State Layout and Memory

### C.1 One composite state entry per instrument (keyed by `instrument_token`)

> Trader summary: one small box per stock. Inside the box, six live candles are growing (one for each chart speed), plus the last 15 finished candles of each speed for the signal math to look back on, plus the latest bid/ask quote, plus a marker that says "the feed had a gap here." The box never holds the raw ticks themselves.

```
CompositeState (per instrument_token) — heap-managed, checkpointed as one value
├── forming[6]             — one CandleAccumulator per TF, the live candle being built
│     [0] 15s   ─┐
│     [1] 30s     │ each: CandleAccumulator (public POJO)
│     [2]  1m     │   exchange:String (shared ref), symbol:String (shared ref)
│     [3]  3m     │   openPaise:long, highPaise:long, lowPaise:long, closePaise:long
│     [4]  5m     │   volume:long, tickCount:long
│     [5] 15m   ─┘   firstEventTime:long, firstFingerprint:String
│                     lastEventTime:long,  lastFingerprint:String, lastIngestTs:long
│                     windowStart:long + tf discriminator (not in accumulator — slot map key)
│                     invariant: windowStart == bucketStart(event_time, TF) per §D
│
├── closedRing[6][15]      — rolling history of the last 15 CLOSED candles per TF
│     per TF: ArrayDeque<CandleLite> capped at 15 (oldest evicted)
│     CandleLite (closed snapshot):
│       windowStart:long, windowEnd:long
│       openPaise:long, highPaise:long, lowPaise:long, closePaise:long
│       volume:long, tickCount:int
│       isComplete:boolean (always true in this buffer)
│       — no fingerprints, no ingestTs, no exchange/symbol duplication (in key)
│       — written once at boundary close, read many times by signal engine
│
├── quoteSnapshot          — latest QUOTE-only tick's bid/ask (OHLC never touched)
│     lastBidPaise:long, lastAskPaise:long, lastQuoteEventTime:long
│     — updated on QUOTE rows, ignored by CandleAggregateFunction path
│
├── windowStart[6]         — long per TF, the start of the currently-forming bucket
│     — sentinel Long.MIN_VALUE = "no forming candle yet" (after gap drop or before first tick)
│
└── discontinuityMarker    — gap/discontinuity state
      hasGap:boolean, gapStartMs:long, gapEndMs:long, reason:String (ROUTE: restart / late-drop / session-gap)
      lastSeenEventTime:long  — monotonic guard (see §E.5)
      — emitted as a row to a side output / marker table when set; cleared only by a clean boundary close
```

**Field table — per-accumulator vs per-closed-lite:**

| Field | Forming accumulator (aggregator `forming[TF]`) | Closed-lite in `closedRing[TF][15]` | Notes |
|---|---|---|---|
| `windowStart / windowEnd` | slot key + derived `start+tfMs` | stored explicitly (both) | forming end is implicit; closed stores both for sink row |
| `open/high/low/close (paise)` | 4× `long`, in-place `Math.max/min` | 4× `long` copied at close | same semantics as `CandleAggregateFunction` |
| `volume` | `long`, `+= qty` on TRADE gate | `long` copied | quote rows never increment |
| `tickCount` | `long` in accumulator, `int` in row | `int` | matches DDL `INT` |
| `firstEventTime / lastEventTime` | `long` order keys for open/close | not stored | only needed while forming |
| `firstFingerprint / lastFingerprint` | `String` tie-break | not stored | |
| `lastIngestTs` | `long` (latency probe, not sunk) | not stored | observability only |
| `exchange / symbol` | shared `String` refs | not duplicated | keyed state already knows instrument |
| `isComplete` | false (forming) | true | sink discriminator |

**Rolling buffer bounds:** exactly 15 per TF per instrument. Push on boundary close: `ring[tf].addLast(closedLite); while size>15 removeFirst()`. Structural bound — steady state holds 6×15 = 90 lites per instrument, never grows with throughput.

### C.2 Memory math

> Trader summary: even covering every listed stock at all six speeds at once, the live state is a few megabytes — flat whether the market does 1 000 or 20 000 ticks a second. No tick history is buffered.

**Per-field sizing (64-bit JVM, uncompressed, before Flink serde overhead):**

- Forming accumulator: 8 longs (open/high/low/close/volume/tickCount/firstEvent/lastEvent) × 8 B = 64 B of primitives + 3 references (fingerprints, ~40 B total with short string overhead) + object header (~16 B) + `windowStart` slot key (8 B) ≈ **~130–180 B** of live data; with Java object + `HashMap` entry overhead the heap cost lands nearer **~250–320 B** per accumulator. Doc baseline uses **~300 B** as the engineering estimate already used in the chain-heap plan's headroom math.
- Closed-lite: 6 longs + 1 int + 2 longs (windowStart/End) ≈ 60 B of primitives; with object overhead ≈ **~60–80 B** stored. Doc baseline uses **~60 B**.
- Quote snapshot + windowStart array + marker + map overhead: amortised ~**~80–120 B** per instrument.

**Arithmetic requested in the brief (verbatim formula, then reconciled total):**

```
Per instrument = 6 × ~300 B (forming) + 6 × 15 × ~60 B (closed history) + ~100 B (quote/marker/map)

  forming:        6 × 300  =  1 800 B
  closed history: 6 × 15 × 60 = 5 400 B
  overhead:                 ≈    100 B
  ─────────────────────────────────────
  per instrument ≈ 7 300 B  (~7.1 KiB)

All instruments (NSE cash universe N = 2 433, 2026-09 peak):

  2 433 × 7 300 B = 17 760 900 B ≈ 16.9 MiB of live domain data
```

**Why the brief says "1–2 MB" and the table says ~17 MiB — reconciled:**

- **1–2 MB** is the *serialised* state size (what RocksDB / checkpoint writes): 2 433 × (6×64 B primitives + 6×15×48 B packed lites) with no Java object headers, no `HashMap$Node`s, no `String` objects (exchange/symbol interned once per instrument, fingerprints stored only in the forming window). Packed binary encoding lands at **~0.9–1.6 MiB** depending on string interning.
- **~17 MiB** is the *on-heap* Java cost with full object graphs (the number above). Both are correct at different layers; both are flat with throughput because no tick list is ever buffered.
- With Flink's RocksDB backend the heap hot set is still only the forming slots (6 × 2 433 × ~300 B ≈ **~4.4 MiB** heap) — closed rings can be kept in managed state if heap pressure dictates; the hot tick path still does one state fetch tick (cached) or pure heap update.

**Scaling rule:** cost is `O(instruments × timeframes × historyDepth)`, **not** `O(ticks)`. Doubling throughput adds zero bytes. Adding an instrument adds ~7.3 KiB heap (~1 KiB serialised). Even at 5 000 instruments the heap stays < 40 MiB — no GC pressure from tick buffering.

---

## D. Bucket Math Spec

### D.1 Invariants (apply to all six timeframes)

1. **Half-open** — A bucket `B = [start, end)` contains every `event_time t` with `start ≤ t < end`. `end = start + tfMs`. End-exclusive means `t == end` belongs to the next bucket, never both.
2. **Deterministic** — `start = bucketStart(t, tf)` is a pure function of `t` and `tf` plus (for 3 m/5 m/15 m) the session open instant of `t`'s date. No state, no config beyond session hours.
3. **Idempotent** — Recomputing `bucketStart` for the same `t` always yields the same `start` (replay-safe, restart-safe).
4. **No phantom gaps** — Ticks outside `09:15–15:30 IST` are filtered *before* bucketing and never create buckets. Empty buckets (no ticks) are never materialised or emitted.
5. **Server time is event time** — `event_time` is the bridge-stamped server timestamp (as in `CandleWatermarkStrategy.withTimestampAssigner`); no client clock.

### D.2 Epoch-aligned timeframes (15 s, 30 s, 1 m)

```
tfMs: 15_000, 30_000, 60_000

bucketStart_epoch(t) = floor(t / tfMs) × tfMs
  where t is event_time in epoch-millis (UTC, but arithmetic is timezone-free
  because epoch 0 = 1970-01-01T00:00:00Z aligns to all three — 60 000 is a
  multiple of 15 000 and 30 000)
```

### D.3 Session-offset timeframes (3 m, 5 m, 15 m — aligned to 09:15 IST)

```
sessionOpenMs(date) = epoch-millis of date at 09:15:00.000 IST (= 09:15 Asia/Kolkata)
  — resolved per event_time via: Instant.ofEpochMilli(t).atZone(Asia/Kolkata).toLocalDate()
  — then ZonedDateTime.of(date, 09:15, Asia/Kolkata).toInstant().toEpochMilli()
  — DST-free (IST is fixed UTC+05:30); no spring/fall ambiguity.

tfMs: 180_000 (3m), 300_000 (5m), 900_000 (15m)

bucketStart_session(t) =
  let open = sessionOpenMs(istDateOf(t))
  let elapsed = t - open
  if elapsed < 0          → PRE-OPEN (filtered, no bucket)
  if elapsed >= 22_500_000 → POST-CLOSE (≥ 15:30 = 6h15m = 375m = 22 500 000 ms → filtered)
  else floor(elapsed / tfMs) × tfMs + open
```

Post-close boundary is exclusive: `t == 15:30:00.000 IST` is post-close.

### D.4 Worked examples (IST)

All examples use `event_time` already in the correct timezone context. IST = UTC+05:30. Session open = `09:15:00.000 IST`.

#### Example 1 — event `10:00:37.210 IST` (a mid-session tick)

```
Take a real instant: 2026-09-04 10:00:37.210 IST
  epoch-millis t = ZonedDateTime(2026-09-04, 10:00:37.210, Asia/Kolkata).toInstant() → 1 725 41X XXX (example)

Epoch-aligned:
  15s: floor(37.210 / 15) = 2 → start = 10:00:30.000  end = 10:00:45.000
  30s: floor(37.210 / 30) = 1 → start = 10:00:30.000  end = 10:01:00.000
   1m: floor(37.210 / 60) = 0 → start = 10:00:00.000  end = 10:01:00.000

Session-offset (open 09:15:00.000):
  elapsed = 45m 37.210s = 2 737 210 ms
  3m: floor(2 737 210 / 180 000) = 15 → start = 09:15 + 15×3m = 10:00:00.000  end = 10:03:00.000
  5m: floor(2 737 210 / 300 000) =  9 → start = 09:15 +  9×5m = 10:00:00.000  end = 10:05:00.000
 15m: floor(2 737 210 / 900 000) =  3 → start = 09:15 +  3×15m= 10:00:00.000  end = 10:15:00.000
```

Note: all three session TFs agree on `10:00:00` here because 10:00 is a 15 m boundary from 09:15 (0,15,30,45,60 … minutes after 09:15 → 09:15, 09:30, 09:45, 10:00 …).

#### Example 2 — event `09:15:00.000 IST` (exact session-open edge)

```
t = 2026-09-04 09:15:00.000 IST  (inclusive)

Epoch-aligned:
  15s: 09:15:00.000 → bucket [09:15:00, 09:15:15)
  1m:  09:15:00.000 → bucket [09:15:00, 09:16:00)

Session-offset (elapsed = 0):
  3m: elapsed 0 → bucket [09:15:00, 09:18:00)
  5m: elapsed 0 → bucket [09:15:00, 09:20:00)
 15m: elapsed 0 → bucket [09:15:00, 09:30:00)

Rule: half-open means 09:15:00.000 is IN the session. A tick at exactly
09:15:00.000 contributes to OHLC; a tick at 09:14:59.999 is pre-open (filtered).
```

#### Example 3 — event `09:14:59.999 IST` (one ms before open) and `15:30:00.000 / 15:30:00.001`

```
09:14:59.999 IST → elapsed = -1 ms → PRE-OPEN → filtered, no bucket, counted as
                     compute.session.filtered.pre_open

15:30:00.000 IST → elapsed = 22 500 000 ms → POST-CLOSE (≥) → filtered, no bucket
                     (the bucket [15:15:00, 15:30:00) ended here; a tick AT the end
                     belongs to no bucket — it is post-close)

15:30:00.001 IST → filtered as post-close; triggers forced-roll for any still-forming
                     buckets that ended at 15:30 (see Example 4)
```

#### Example 4 — event `15:29:59.999 IST` (last ms of session) and the forced roll

```
15:29:59.999 IST → elapsed = 22 499 999 ms
  3m: bucket [15:27:00, 15:30:00) — the final 3m bucket of the day
  5m: bucket [15:25:00, 15:30:00)
 15m: bucket [15:15:00, 15:30:00)

At wall-clock 15:30:00.000 a processing-time forced-roll timer fires:
  for each TF where forming.windowStart + tfMs == 15:30:00.000 (session-end)
    seal the forming accumulator as the final closed candle of the day,
    emit to candle_closed, rotate into closedRing[TF],
    then set forming.windowStart = Long.MIN_VALUE (no forming candle — gap to tomorrow).

Nothing carries overnight: the next tick at 09:15 next trading day opens fresh
buckets at elapsed 0. An overnight gap is not a discontinuity — it is the
expected session boundary (no marker).
```

#### Example 5 — rejection / half-open proof

```
Bucket [10:00:00, 10:00:15):
  t = 09:59:59.999 → not in bucket (previous)
  t = 10:00:00.000 → IN (start inclusive)  ✓
  t = 10:00:14.999 → IN                    ✓
  t = 10:00:15.000 → not in bucket (next)  ✗ — belongs to [10:00:15, 10:00:30)
```

---

## E. Event Flow — Per Tick, Per Timer, Per Restart

### E.1 The tick that arrives (common preface — every row goes through this)

```
RowData from raw_table_1 (in DDL order, 21 columns):
  event_day, event_fingerprint, fingerprint_version, connection_id, connection_epoch,
  instrument_token, exchange, symbol, event_time, ingest_ts, ack_ts, tick_type,
  last_price_paise, last_qty, raw_payload, payload_hash, decoder_version,
  protocol_version, validity_state, validity_reason, schema_version

Watermark: maxEventTime - 500 ms (CandleWatermarkStrategy), idle 15 s
```

### E.2 Per-tick dispatch (six mutually exclusive outcomes)

```
                          deduped tick (after fingerprint-dedup-v2)
                                     │
                    ┌────────────────┴────────────────┐
                    │ event_time < watermark -         │
                    │ allowedLateness?                 │  (allowedLateness = 5 000 ms today;
                    │                                  │   retained for aggregator's
                    └────────┬────────────────┬────────┘   late-drop equivalence)
                             │YES             │NO
                             ▼                ▼
                     ┌──────────────┐   ┌──────────────────────┐
                     │ LATE DROP    │   │ session filter        │
                     │ counted LOUD │   │ 09:15 ≤ istT ≤15:30?  │
                     │ no state     │   └───┬──────────┬───────┘
                     │ no candle    │       │YES       │NO
                     │ no signal    │       ▼          ▼
                     │ emit to      │  ┌──────────┐ ┌──────────────┐
                     │ CandleLateDrop│  │ dedup    │ │ SESSION DROP │
                     │ side + metric│  │ already? │ │ counted LOUD  │
                     │ compute.cand │  └──┬───┬───┘ │ no state      │
                     │ .late.dropped│     │YES│NO   └──────────────┘
                     └──────────────┘     ▼   ▼
                                   ┌──────┐ ┌──────────────────┐
                                   │ DUPL │ │ trade gate (REUSE│
                                   │ DROP │ │  CandleAgg gate) │
                                   │ count│ │ tick_type==TRADE │
                                   │      │ │ && qty>0 ?       │
                                   └──────┘ └──┬──────────┬────┘
                                               │YES       │NO (QUOTE / zero-qty)
                                               ▼          ▼
                                        ┌──────────┐ ┌──────────────┐
                                        │ TRADE    │ │ QUOTE-ONLY   │
                                        │ tick     │ │ update quote │
                                        │ (all §E3)│ │ snapshot only│
                                        └──────────┘ │ no OHLC      │
                                                     │ still emits  │
                                                     │ side-output  │
                                                     │ (signal sees │
                                                     │  same forming│
                                                     │  candle +    │
                                                     │  fresh quote)│
                                                     └──────────────┘

Counter namespace (all existing metric groups, OTel):
  compute.candles.late.dropped
  compute.session.filtered.{pre_open,post_close}
  compute.dedup.duplicates
  compute.ticks.{trade,quote,invalid}
```

#### E.2.1 Trade tick (the main path — mutates all six forming candles)

```
Precondition: tick_type==TRADE && qty>0, in-session, not late, not dup, fingerprint valid.

For the key instrument_token:
  composite = state.value()  // one fetch tick (heap/RocksDB cached)
  for tf in [15s,30s,1m,3m,5m,15m]:
    start = bucketStart(event_time, tf)          // §D
    if composite.windowStart[tf] == Long.MIN_VALUE:
        // first tick of day / after gap drop — open fresh forming
        composite.forming[tf] = new CandleAccumulator()
        composite.windowStart[tf] = start
        register event-time timer at start+tfMs for boundary close (per-TF)
    else if start != composite.windowStart[tf]:
        // tick advanced into next bucket (or jumped a gap — gap may be 1..N buckets)
        // The previous forming bucket's timer will fire at its end; if this tick
        // already jumped past that end, the timer path seals it (see §E.4).
        // If elapsed gap > tfMs for this TF, mark discontinuity (see §E.7).
        if start > composite.windowStart[tf] + tfMs:
            composite.discontinuityMarker = {hasGap:true, gapStart=composite.windowStart[tf]+tfMs,
                                              gapEnd=start, reason=SESSION_GAP_OR_IDLE}
        // roll: previous forming becomes the seeding start for the new bucket
        // (no intermediate empty buckets are materialised)
        composite.windowStart[tf] = start
        composite.forming[tf] = new CandleAccumulator()  // fresh, not merged
    // in-place OHLCV (reuse CandleAggregateFunction.add — same code, same order keys)
    CandleAggregateFunction.add(row, composite.forming[tf])   // mutates high/low/close/volume
  // quote snapshot unchanged on TRADE ticks (bid/ask stays as last QUOTE)
  composite.lastSeenEventTime = max(composite.lastSeenEventTime, event_time)  // monotonic guard
  state.update(composite)   // one write-back (coalesced; no per-TF state entries)

  // in-JVM side-output to signal engine (hot path) — emit AFTER all six updates:
  ctx.output(SIGNAL_CONTEXT_TAG, SignalContext{
      instrumentToken, eventTime,
      forming = copyOf(composite.forming[6]),            // live candles (forming)
      closedHistory = viewOf(composite.closedRing[6][15]), // last 15 closed per TF (refs)
      quote = composite.quoteSnapshot,
      discontinuity = composite.discontinuityMarker
  })
  // signal engine evaluates immediately; may emit to Signal_Candidates (+ current)
  // Storage writes: NONE on this tick (1 s live + boundary only, §F)
```

Word on "every trade tick mutates all six": a single `event_time` maps to six independent bucket starts (three epoch, three session-offset). All six accumulators are updated with the *same* `last_price_paise`/`qty`, each against its own open/close order keys. No roll-up.

#### E.2.2 Quote-only tick (QUOTE or TRADE with qty==0)

```
Updates composite.quoteSnapshot {bid/ask/lastQuoteEventTime} from the row's
quote fields (when the bridge carries them; today those fields are absent in v2/v3
raw_table_1 but the design parks the update here so re-adding bid/ask columns never
touches candle logic).

Does NOT call CandleAggregateFunction.add — OHLCV, volume, tickCount unchanged.
Does NOT change windowStart.

Still emits the in-JVM SignalContext side-output so the signal engine sees the
fresh bid/ask alongside the unchanged forming candles (needed for quote-aware
rules without paying a Fluss read). If rules are price-only, this emit is a
cheap no-op for the engine to skip.
```

#### E.2.3 Invalid tick

```
RawValidationFunction rejects rows with null/wrong-type event_time, missing
instrument_token, bad fingerprint, unknown validity_state, etc. — counted in
compute.invalid.byReason.* and written to ingestion_quarantine LOG (existing
quarantine path). Never reaches dedup, aggregator, or signal.

The trade gate's TRADE&&qty>0 check is NOT an invalid — it is a valid QUOTE
leg classification (see E.2.2). Invalid means the row itself is malformed.
```

#### E.2.4 Late tick (beyond allowedLateness after watermark)

```
Condition: event_time < currentWatermark - allowedLatenessMs
  (watermark = maxSeenEventTime - 500 ms, as CandleWatermarkStrategy)

Action: drop LOUDLY
  ctx.output(CandleLateDrop.OUTPUT, row)  // existing side output
  metric compute.candles.late.dropped++
  log WARN once per instrument per minute (bounded)
  No state read, no state write, no signal emit, no candle change.

Why loud: a silent late tick can be mistaken for a gap. The late counter is
the operator's proof the watermark is not stuck.
```

#### E.2.5 Out-of-order tick (within lateness, not late)

```
Out-of-order but within the watermark slack is NOT late — it is ordered by
(event_time, fingerprint) inside the accumulator (CandleAggregateFunction's
first/lastEventTime + fingerprint tie-break). This is correct under the
existing 500 ms bounded out-of-orderness assumption.

Two sub-cases:
  a) event_time still in the currently-forming bucket for that TF
     → aggregate.add folds it into forming[TF]; high/low/close may retroactively
       change (open sticks to earliest order key). No extra handling.

  b) event_time belongs to a bucket already sealed into pending/closed
     → the aggregator's heap-window logic (mirroring HeapCandleEmitFunction)
       must decide:
       - if bucket is in pending (awaiting its end-timer) → fold into pending acc
       - if bucket already emitted into closedRing and watermark still within
         lateness → counted no-op (lateUpdateCounter++, no correction row — R-012)
       - if bucket's end+lateness is behind watermark → late drop (§E.2.4)

The new aggregator reuses the same pending/emitted LinkedHashMap discipline as
HeapCandleEmitFunction (MAX_PENDING_CLOSES=16) per TF, so out-of-order behaviour
is identical to the existing chain-heap contract and is differential-tested.
```

#### E.2.6 Duplicate tick

```
FingerprintDedupFunction already dropped exact duplicates (same instrument_token
+ fingerprint_version + event_fingerprint) within the 5-minute dedup window.
If a duplicate reaches the aggregator (should not), it is idempotent — the same
(event_time, fingerprint, price, qty) re-applied to the same accumulator leaves
OHLCV unchanged except for volume double-count, so the dedup layer is the
authoritative guard. Counter: compute.dedup.duplicates.

No aggregator-level dedup is added — that would be a second source of truth.
```

### E.3 Per-timer flows

#### E.3.1 1 s live-candle timer (processing-time, per instrument)

```
Timer: every 1 000 ms of processing time, per key, anchored at window open
  (first TRADE tick of the day registers it; cancelled on forced-roll / clear).

On firing per instrument:
  composite = state.value()  // if Long.MIN_VALUE for a TF, skip that TF
  for tf in 6:
    if composite.windowStart[tf] == Long.MIN_VALUE: continue (no forming candle)
    row = buildLiveRow(instrument, composite.forming[tf], windowStart[tf], tf)
          // 15-col (or new live cols) with is_preview=false / tf discriminator
    collect to candle_live KV sink (INSERT→UPSERT on (instrument, tf, windowStart))

Collect total per second across all instruments: 2 433 × 6 = 14 598 upserts/s
  (≈ 14.6 k/s). Each row overwrites the same PK until the bucket closes.
  No tick buffering — the row is built from the live accumulator.

Metrics: compute.candles.live.emitted (per TF counter), histogram
  compute.latency.ingest_to_live (processingTime - forming[TF].lastIngestTs).
```

#### E.3.2 Boundary-close timer (event-time, per TF per instrument)

```
Timer: one event-time timer per TF per instrument at forming.windowStart[tf] + tfMs
  (registered when the forming bucket opens; re-registered each new bucket).
  Fires when watermark passes windowEnd. Identical to HeapCandleEmitFunction's
  end-timer — the same watermark drives it.

On firing for (instrument, tf, windowStart):
  composite = state.value()
  if composite.windowStart[tf] != windowStart:  // stale/restored timer, forming already rolled
      metric compute.candles.restored_timer_noop++ ; return (loud no-op)
  acc = composite.forming[tf]
  window = [windowStart, windowStart+tfMs)

  // invariant gate (reuse CandleInvariantCheck)
  violation = CandleInvariantCheck.firstViolation(acc, window, tfMs)
  if violation != null:
      ctx.output(CandleQuarantine.OUTPUT,
                 CandleQuarantine.buildRow(instrument, acc, window, violation))
      // Still mark as emitted and drop forming — an invalid candle never enters closedRing
  else:
      row = CandleEmitFunction.buildRow(instrument, acc, window, now, config)
            // extended with tf column for multi-TF sinks
      collect to candle_closed immutable sink (first-write-wins — see §F.2)
      // roll into history for signal lookback
      composite.closedRing[tf].addLast(toLite(acc, window))
      while composite.closedRing[tf].size() > 15: removeFirst()
      metric compute.candles.emitted[tf]++

  // reset forming for this TF
  composite.forming[tf] = new CandleAccumulator()
  composite.windowStart[tf] = Long.MIN_VALUE  // until next tick opens the next bucket
  state.update(composite)
  // discontinuity marker cleared only by a clean close (gap healed)
  if composite.discontinuityMarker.hasGap && composite.discontinuityMarker.gapEnd == windowStart:
      composite.discontinuityMarker = clear()

  // processing-time 1 s timer stays armed while any TF remains forming;
  // disarmed only when all six windowStart are Long.MIN_VALUE.
```

#### E.3.3 Forced-roll timer at 15:30 IST (processing-time, global per subtask)

```
Timer: daily at 15:30:00.000 IST (ZonedDateTime, Asia/Kolkata).
  Registered as a processing-time timer keyed to a sentinel instrument (or as a
  broadcast-side timer — implementation picks one; contract is the same).

On firing:
  for each instrument in subtask's key range (iterate state — bounded by 2 433 keys,
  sharded across subtasks by hash(token)):
    composite = state.value()
    for tf in 6:
      if composite.windowStart[tf] == Long.MIN_VALUE: continue
      // seal the in-progress bucket even though its bucket hasn't naturally ended
      // (for 15m/5m/3m the session-end IS a natural end — timer coincides with boundary;
      //  for 15s/30s/1m the session cuts mid-bucket — the tail bucket is emitted under-size)
      acc = composite.forming[tf]
      window = [composite.windowStart[tf], 15:30:00.000 IST)  // truncated end
      // validate + emit to candle_closed with isSessionEnd=true marker
      // rotate into closedRing[tf] (still counts toward last-15)
      // reset forming[tf] + windowStart[tf]
    state.update(composite)

  No 1 s timer re-arm tonight. Next tick tomorrow at 09:15 opens fresh buckets.
```

### E.4 Restart and gap handling (Decision 12 + Decision 3)

> Trader summary: if the job restarts or the feed stalls, we never make up a candle with missing ticks inside. We drop the half-built candle, mark the gap visibly, and rebuild from the next live tick.

| Situation | Detection | Action | What is never emitted |
|---|---|---|---|
| **Clean restart with checkpoint** | `STATE_RECOVERY_PATH` restore | Composite state restored exactly (forming + closedRing + marker + quote). Watermarks rebuild from restored maxEventTime. Next tick continues in the same buckets — no gap if the gap is < lateness. | No duplicate closed candles (first-write-wins on sink re-emit) |
| **Crash before checkpoint (intentional amnesia)** | forming state was heap/RocksDB not yet checkpointed | Forming bucket's pre-restart ticks are gone — that one bucket per TF per instrument restarts partial from post-restart live ticks. Closed rings before the crash survive (already sunk). | Never emit a candle with a hole — the partial bucket was dropped |
| **Feed gap / stall (source idle > SOURCE_IDLE_MS)** | `SourceIdleWatchdogGenerator` + no tick for that TF's bucket duration | On next tick, if `newStart > oldStart + tfMs` for any TF → hasGap=true, gap interval recorded. The next tick opens a fresh bucket at `newStart`; empty intermediate buckets are never materialised. | Never synthesise empty candles to "fill" a gap |
| **Stale forming drop** | Gap marker set and next tick arrives after a region | `composite.forming[TF]` for affected TFs is discarded (not sealed into closedRing) before opening the new bucket. | Hole-covering candle |
| **Discontinuity marker** | Set on gap detection, lives in composite state | Visible in `SignalContext.discontinuity` side-output (signal engine may veto or down-weight), also written to `suspected_discontinuities` style marker table if configured. Cleared only by a clean boundary close. | Signals during a marked gap are flagged, never silently consumed |
| **Late restore timer** | Event-time timer fires but `windowStart` already advanced | Loud no-op (`compute.candles.restored_timer_noop`) — mirrors `HeapCandleEmitFunction` restore semantics (G-CHAIN-3) | Phantom re-emit |

**Monotonic event-time guard:** `composite.lastSeenEventTime` only advances. A tick with `event_time < lastSeen - outOfOrderSlack` that is still within lateness is folded normally (fingerprint order corrects open/close); a tick older than `watermark - lateness` is late-dropped. If watermarks ever go backward (should not — Flink's combined watermark is monotonic), the operator counts and logs but never mixes time.

---

## F. Output Contracts

### F.1 `candle_live` — 1 s live-candle refresh (KV, mutable, current-state)

> Trader summary: every second, the six live bars you watch on screen are overwritten with their latest O-H-L-C and volume. The key is stock + speed + bar-start time, so the same bar keeps growing until it closes.

```
DDL:  NEW table candle_live (KV)
  PRIMARY KEY (instrument_token, tf, window_start) NOT ENFORCED
  Columns (proposed, extends CandleTableSchema 15-col with tf discriminator):
    instrument_token  BIGINT  NOT NULL  } PK
    tf                STRING  NOT NULL  } PK — one of '15s','30s','1m','3m','5m','15m'
    window_start      BIGINT  NOT NULL  } PK — inclusive, §D
    window_end        BIGINT  NOT NULL  — exclusive, = start+tfMs (or truncated at 15:30)
    open_paise        BIGINT  NOT NULL  — first tick's price in the forming window
    high_paise        BIGINT  NOT NULL  — max price seen
    low_paise         BIGINT  NOT NULL  — min price seen
    close_paise       BIGINT  NOT NULL  — last tick's price by (event_time,fingerprint)
    volume            BIGINT  NOT NULL  — sum qty of TRADE ticks only
    tick_count        INT     NOT NULL  — count of TRADE ticks only
    is_complete       BOOLEAN NOT NULL  — always FALSE in candle_live (forming)
    algorithm_version STRING  NOT NULL  — pinned canonical (CANDLE-CANONICAL-001)
    configuration_version STRING NOT NULL
    output_ts         BIGINT  NOT NULL  — processing-time of this 1 s snapshot
    last_event_ts     BIGINT  NOT NULL  — event_time of newest tick in the bar
    schema_version    STRING  NOT NULL  — '3' (multi-TF)
  WITH (
    'bucket.num'='16', 'bucket.key'='instrument_token',
    'table.log.ttl'='60s'  -- ephemeral live rows expire; history lives in candle_closed
    'table.datalake.enabled'='false'  -- transient, no offload (mirrors 30_feature_candles_15s_preview)
    'table.kv.format-version'='2'
  )
  Alternatives considered: reuse feature_candles_15s_preview (rejected — different PK and TTL,
  and keeping the existing preview path side-by-side requires a distinct table).

Sink: FlussSink<RowData>(SerializationSchema(false,false) → UPSERT on PK)
  — every 1 s snapshot for the same (instrument,tf,window_start) overwrites the same row,
    so the bar "grows" live and replays converge.

Write volume:

  instruments N = 2 433 (NSE cash, 2026-09)
  timeframes T = 6
  cadence C = 1 s

  live upserts/s = N × T / C = 2 433 × 6 = 14 598 /s  (~14.6 k/s)

  Per-row size ≈ 80-120 B serialised (15 cols) → ~1.2-1.7 MiB/s aggregate Fluss write
  bandwidth — well within a single Fluss tablet writer's budget. Polled every second
  by dashboards, never by the signal hot path (which reads heap, not Fluss).
```

### F.2 `candle_closed` — immutable closed history (LOG or KV-first-write-wins, append-once)

```
DDL: NEW table candle_closed (or family candle_closed_15s/_30s/_1m/_3m/_5m/_15m — one table
  with tf in PK is preferred to avoid six DDLs; contract below assumes single table)

  PRIMARY KEY (instrument_token, tf, window_start) NOT ENFORCED  if KV
  -- OR no PK if LOG (decision phase picks one; behaviour below covers both)

  Columns: same 15 as candle_live but:
    is_complete = TRUE
    table.log.ttl = '7d' with table.datalake.enabled='true' (history kept, offloaded — mirrors
      03_feature_candles_15s)
    No TTL expiry of completed bars — they are the truth.

Sink properties:
  Immutability: a closed candle is written exactly once per (instrument,tf,window_start).
    Subsequent re-emits (checkpoint-restore re-fire, same-day re-derive) must NOT overwrite.
    → If KV: first-write-wins via CandleKvFirstWriteWinsFunction (existing pattern:
      keyBy(candle PK) → forwards first, drops+counts duplicates as compute.candles.duplicate_window)
    → If LOG: idempotency enforced by Fluss consumer dedup on (instrument,tf,window_start)
       + downstream query dedups to latest-by-windowStart — never two rows with same PK.

  First-write-wins is load-bearing for replay correctness and for the closedRing
  invariant (ring entries never change after insertion).

Write volume (upper bound — one row per non-empty bucket):

  6h15m = 375 min = 22 500 s trading session
  Non-empty buckets per instrument per day (worst case — every bucket has ≥1 trade tick):
    15s: 22 500/15 = 1 500 rows
    30s:   750 rows
     1m:   375 rows
     3m:   125 rows
     5m:    75 rows
    15m:    25 rows
  Sum = 2 850 rows/instrument/day
  Whole universe = 2 433 × 2 850 = 6 934 050 rows/day (~6.9 M) before filtering empties.
  Real-world (many buckets empty for illiquid names) ≈ 20–40% of that ≈ 1.5–3 M/day.
  Write QPS at peak (all 15 s boundaries coincide every 15 s): 2 433 rows / 15 s ≈ 162 /s
  plus higher-TF boundaries interleaved — well under 1 k/s aggregate.

  Storage: ~80-120 B/row × 7d ≈ 1–3 GiB total closed history (before lake offload).
```

### F.3 Signal rows — append + current-state projection (existing tables, new timeframe column)

```
Tables: Signal_Candidates (LOG, append-only, 22 cols) + Signal_Candidates_current (KV, PK instrument_token)
  Both gain a column (non-breaking — new nullable or new schema_version):
    timeframe STRING — the TF whose forming candle fired the rule ('15s'...'15m')
  All other columns as DDL 05/23 today (candidate_id, instrument_token, exchange, symbol,
  strategy_id, strategy_version, rule_id, detection_ts, evaluation_ts, action, side, quantity,
  order_type, limit_price_paise, score_inputs, formation_snapshot_ref, validity_reason,
  supersedes/superseded, schema_version).

Firing semantics (Option A):
  When the signal engine's rule evaluates true on a forming candle for TF = X,
  it emits ONE RowData immediately — no waiting for X's bucket to close.

Row content:
  candidate_id      = "{rule_id}-{instrument_token}-{window_start}-{triggerFingerprint}"
                      (stable, content-addressed — see idempotency)
  detection_ts      = forming candle's lastEventTime (the trade that triggered)
  evaluation_ts     = detection_ts (forming act — same; closed-confirm would use windowEnd)
  formation_snapshot_ref = "forming:{tf}:{window_start}:{window_end}:{open}/{high}/{low}/{close}/{volume}/{tickCount}:{lastEventTs}:{fingerprint}"
                          (parseable snapshot so a later reader can decide if the trigger held)
  timeframe         = X
  rule_id           = which rule fired (per-TF rule identity, schema_version-scoped)
  is_forming        = TRUE (new boolean or encoded in validity_reason)
  discontinuity_at_trigger = marker if hasGap at trigger time (flagged, never filtered silently)

Idempotency key (so replay converges to one row, not duplicates):

  key = (rule_id, instrument_token, tf, window_start, triggerFingerprint)
  where triggerFingerprint = fingerprint of the trade tick that caused the fire.

  Why fingerprint and not timestamp: two ticks can share the same event_time
  (Arrow batches), but fingerprints are unique per tick (dedup key). Two
  restarts replaying the same tick produce the same key → the LOG sink's
  dedup (or a pre-insert KV check in CandleKvFirstWriteWinsFunction style)
  drops the second append. Signal_Candidates_current naturally dedups to the
  same PK overwriting the same row.

  Note: forming-candle signals can fire multiple times per bucket (one per tick
  while the rule stays true). The design fires ONCE per window per rule
  (fire-once latch: firedOnce[tf][windowStart] → deduped; re-arm only on next
  bucket). This bounds signal volume to one per bucket per rule — without it
  a 15 s bucket with 100 ticks would spam 100 rows. The latch lives in the
  signal engine's heap state, cleared at boundary close. The latch key is the
  same as the idempotency key minus fingerprint (so a second tick in the same
  window with the same condition is suppressed, not a duplicate row).

  Formation bounce (ACCEPTED-BY-DESIGN): a signal may fire on forming high
  105, then the bucket closes at 102 ("against" the trigger). The first row
  is NEVER retracted. Downstream corrects by acting on the next signal (which
  may be the opposite direction or a no-op). Consumers must tolerate that a
  forming-triggered signal's price did not hold to close — this is the approved
  trade-off of Option A.

Signals stop at Fluss: the new engine's full responsibility ends at appending
to Signal_Candidates (+ current). It never calls Arrow, never writes
Trade_Decisions/Execution_Intent, never places orders. Those are a separate
phase reading the signal tables (existing machinery).
```

---

## G. Data-Quality Tests (mirror the chain-heap precedent)

Each test follows the harness pattern already in-tree: `KeyedOneInputStreamOperatorTestHarness` for KeyedProcessFunctions, real `SignalJobConfig.from(env())`, real `CandleAggregateFunction`, no cluster.

### G.1 OHLCV-exactness (brute-force recompute — oracle, not operator)

```
Name: MultiTfOhlcvExactnessTest (per TF) + MultiTfOhlcvBruteForceCrossTfTest

What: For a fixed script of ticks (with mixed TRADE/QUOTE, out-of-order delivery,
      fingerprint tie-breaks, pre/post-session rows), recompute each TF's expected
      OHLCV by brute force:
        expected[tf] = filter(ticks that map to bucket B of tf) → sort by (event_time, fingerprint)
                       → OHLC order, high=max, low=min, volume=sum(qty where TRADE&&qty>0)
      Compare against the aggregator's forming accumulator at 1 s snapshots and the closed row at
      boundary timers (watermark-advanced deterministically in the harness).

Orbits: covers mixed order, quote-only windows (0 volume), single-tick windows, zero-qty trades,
        empty buckets (no row), session-truncated tail bucket.

Why: proves CandleAggregateFunction.add is being reused correctly for all six bucket widths and
     proves epoch vs session-offset alignment independently of the operator.
```

### G.2 Higher-TF independence (mutation test)

```
Name: HigherTfIndependenceTest

What: Take a passing 15s-OHLCV scenario. Mutate a tick's price that sits in bucket
      15s:[10:00:00,10:00:15) but in the *previous* 15m bucket's interior (e.g. 09:59 tick).
      Assert: 15m bucket [09:45:00,10:00:00) changes, 15s buckets after 10:00:00 do NOT.
      Then reverse: mutate a 10:00:10 tick → 15s bucket changes, both 15m buckets unchanged
      except the containing one.

Why: proves Decision 5 (higher TFs from ticks directly, never rolled from lower TFs).
     A roll-up bug would bleed the mutation across TFs.
Also: LowerTfRollupAbsenceTest — assert no operator reads candle_live/closed to compute
      a higher TF (grep: zero Fluss reads in aggregator hot path).
```

### G.3 Invariant checks

```
Name: CandleInvariantGateTest (reuses CandleInvariantCheck)

Five OHLC invariants on every closed candle (same checks as CandleEmitFunction):
  1. high ≥ open, high ≥ close
  2. low  ≤ open, low  ≤ close
  3. low  ≤ high
  4. volume ≥ 0 (and volume>0 iff tickCount>0 — compensates quote-only)
  5. window_end - window_start == tfMs (or truncated at 15:30)

  Violations route to CandleQuarantine side output, never to closedRing or sinks,
  and are counted in compute.candles.invalid.{reason}. Include the
  no-double-quarantine assertion (late re-trigger after quarantine still exactly one row).
```

### G.4 Closed-immutability (first-write-wins)

```
Name: ClosedImmutabilityTest + CandleKvFirstWriteWinsMultiTfTest

What: Emit a closed candle (instrument, tf, windowStart) → sink. Re-emit the same PK
      with a different OHLCV (simulating checkpoint-restore re-fire). Assert:
        sink receives exactly one row (first wins), metric compute.candles.duplicate_window++,
        second value never reaches Fluss.
      Repeat for LOG variant: consumer query returns one row, not two.

Replay harness: two-phase — phase 1 writes N closed rows, phase 2 replays the same
input script; assert mains == phase-1 outputs (minus output_ts).
```

### G.5 Discontinuity / gap

```
Name: GapAndDiscontinuityTest + StaleFormingDropTest + RestoreGapTest

Scripts:
  a) Feed ticks up to 10:00:00 (all TFs forming). Stall past allowedLateness + session gap.
     Next tick at 10:02:00 → assert forming[tf] was dropped (not sealed), closedRing[tf]
     did not gain a synthetic candle for the empty buckets, and discontinuityMarker.gapStart
     / gapEnd / reason are correct.
  b) Watermark already past windowEnd when a tick arrives for that window
     → late drop (not gap) — metric and side output, no gap marker.
  c) Crash before checkpoint: close harness mid-bucket, reopen fresh (empty state),
     feed same ticks again → the pre-crash partial bucket never emits; only the
     post-restore full bucket does. Asserts the intentional-amnesia contract.

Marker assertions: SignalContext.discontinuity carries hasGap=true after (a) and
  false before; a signal firing during a marked gap carries discontinuity_at_trigger flag.
```

### G.6 Warm-up (no signal before 15 closed exist per TF used by rule)

```
Name: SignalWarmupPerTfTest + SignalNoWarmupNoFireTest

What: For each TF X that a rule depends on, assert:
        signals on TF X before closedRing[X].size() == 15 → zero emits
        signals after the 15th close → may emit (if rule condition true)
      The 15 is per TF — a 15s ring warm does not unblock a 15m rule.
      Exact copy of SignalLookbackState.isWarm() semantics but per TF:
        isWarm(tf) = closedRing[tf].size() >= 15

Script per TF: feed 14 closed buckets (sealed via watermark timers), then a forming
tick that satisfies breakout/bullish — assert no signal. Seal the 15th bucket,
feed the same forming tick — assert signal fires once and carries firedTf=X.
```

### G.7 Bucket-coverage fuzz over 2 sessions

```
Name: BucketCoverageFuzzTest (property-based, seed-fixed)

What: Generate random event_time sequences spanning two full IST sessions
      (day1 09:15-15:30 + overnight gap + day2 09:15-15:30) with uniform random
      offsets inside [pre-open, post-close] and random clock skew (ticks
      occasionally violate monotonic wall clock but not event_time order beyond
      500 ms). For every tick:
        assert bucketStart(t, tf) ∈ [sessionOpen, 15:30) for session TFs,
              bucketStart(t, tf) ∈ epoch buckets for epoch TFs,
        assert start ≤ t < start+tfMs (half-open),
        assert every tf's bucket set partitions the session without overlap or gap.
      Then feed the ticks through the aggregator harness and assert every closed
      candle's window satisfies the same partition property, plus pre-open/post-close
      ticks were filtered (counters match expected), plus forced-roll produced the
      truncated tail bucket correctly, plus no candle was emitted for an empty bucket.

Seeds: at least 10 fixed seeds + one randomised nightly seed (logged for replay).
```

### G.8 Side-by-side bit-identical-15s gate (soak scorecard, see §H.6)

```
Name: FifteenSecondBitIdenticalSoakGate (integration, gating — not unit)

What: While the old 15s chain runs side-by-side, compare its output
      feature_candles_15s vs the new aggregator's 15s leg (candle_closed where tf='15s')
      over a soak window. Rows compared minus output_ts (processing time differs),
      must be byte-identical on OHLCV + volume + tickCount + window bounds.

Why: proves the reuse of CandleAggregateFunction.add and the session filter
     introduced no regression on the existing path before cutover is approved.
```

> **Soak mode A (approved 2026-09-05):** the Phase 5 side-by-side soak runs
> the 15s fake-broker feed at ANY wall-clock time (weekend / after close), so
> it is not pinned to the 09:15-15:30 IST market window. Because the fake
> broker stamps real `time.Now()` event times and the aggregator's session
> filter is hour-of-day only (no weekday check), off-hours wall-clock ticks
> would be dropped as pre/post-session. The soak therefore sets
> `MULTITF_SESSION_BYPASS=true` (default `false`, production never sets it):
> the aggregator accepts out-of-session ticks exactly like in-session ones
> (signal + bucket accumulation; the only skipped step is the pre/post drop).
> The old 15s path and the real market feed are untouched. The soak scorecard
> gates below still apply unchanged.

---

## H. Migration — Phase 0–6

> Trader summary: we ship in small, proof-gated steps. The live 15 s engine is never touched until you say so. Each phase ends with tests green or we stop.

| Phase | What ships | How to tell it's done | Checks |
|---|---|---|---|
| **Phase 0 — Contracts** | DDLs for `candle_live` + `candle_closed` (with `tf` in PK), `Signal_Candidates` timeframe column add (or new `schema_version`), plus `TableContractValidator.validateCandleLiveTable` / `validateCandleClosedTable` and `CandleLiveColumns` / `CandleClosedColumns` column contracts (names + `TYPE_ROOTS` in DDL order). | `TableContractValidatorTest` proves live vs DDL parity (same pattern as `CandlePojoRecognitionTest` + `30_feature_candles_15s_preview` TTL test). `SchemaComplianceFullSuiteTest` green. No operator change yet. | `mvn test -Dtest=TableContractValidatorTest,RawTableDdlContractTest` pass. Preflight in `SignalJob.preflightTableContracts` still only checks existing tables (new validators gated behind `MULTITF_ENABLED`). |
| **Phase 1 — Boundary + State skeleton** | `MultiTimeframeState` POJO (composite state entry: `forming[6]`, `closedRing[6][15]`, `quoteSnapshot`, `marker`, `windowStart[6]`), `BucketMath` utility (`bucketStart(t,tf)` epoch vs session-offset as in §D.2–D.3, IST resolution), unit tests for bucket math only. No wiring. | `BucketMathTest` covers all six TFs + all worked examples in §D.4 + pre/post edge + 15:30 truncate. Fuzz harness seeded (G.7) runs offline as a unit test (no Flink). | `mvn test -Dtest=BucketMathTest,BucketCoverageFuzzTest` pass. Checkstyle: new POJO is public fields (Flink POJO rule). |
| **Phase 2 — Aggregator operator** | `MultiTfAggregatorFunction extends KeyedProcessFunction<Long,RowData,RowData>` — heap composite state (or `ValueState<CompositeState>` — decision picked in review; contract below specifies both work), reusing `CandleAggregateFunction.add`, 1 s processing-time timer + per-TF event-time boundary timers + forced-roll at 15:30, session filter + trade gate in `processElement`, pending/emitted maps per TF mirroring `HeapCandleEmitFunction` caps (`GLOBAL_SLOT_CAP=65_536`, `MAX_PENDING_PER_TF=16`), discontinuity marker logic, `SignalContext` side-output `OutputTag<SignalContext>`. In-JVM holder for signal engine evaluation stub. | Harness tests G.1–G.5 green for the operator in isolation (no sinks, no signal logic): OHLCV exactness vs brute force (6 TFs), higher-TF independence, invariant gate, closed-immutability (mocked sink), gap/discontinuity, warm-up. Operator carries explicit uid `multi-tf-aggregator-v1` and passes `numKeyedStateEntries==1` or heap-equivalent guard per chosen state type. | `mvn test -Dtest=MultiTfAggregator*Test,HeapCandleEmitFunctionTest` (existing still green — no shared-code drift). |
| **Phase 3 — Sinks + Signal engine** | `CandleLiveSink` (KV upsert, `RowDataSerializationSchema(false,false)`, 1 s emit), `CandleClosedSink` (first-write-wins guard + immutable LOG/KV sink), `MultiTfSignalEngine` (per-tick evaluation: needs 15 closed per needed TF, fire-once-per-window latch, Option A immediate emit, idempotency key builder, formation_snapshot_ref formatter). Signal output dual-sinks to `Signal_Candidates` (LOG `true,true`) + `Signal_Candidates_current` (KV `false,false` behind `CanonicalSignalFilterFunction` — timeframe column admitted as canonical). Signal rows never retracted (ACCEPTED-BY-DESIGN documented). | G.6 warm-up tests green, signal fires exactly once per window per rule, evaluation sees heap history with zero Fluss reads (assert via no sink/source touch in harness). Latency histogram `compute.latency.ingest_to_signal` recorded at emit. | `mvn test -Dtest=MultiTfSignal*Test,SignalDetectionFunctionTest` pass. `SignalHarnessContractTest` extended for multi-TF context shape. |
| **Phase 4 — Flag-gated wiring** | Wire the aggregator branch into `SignalJob.buildTopology` behind `MULTITF_ENABLED` (default `false`). New config key `MULTITF_ENABLED` in `SignalJobConfig` (note: auditor lists 26 live operator UIDs in SignalJob.java vs 17 pinned in the UID test — Phase 4 must reconcile the two before asserting) using the existing `booleanValue(env,"MULTITF_ENABLED", false)` pattern (same as `PREVIEW_ENABLED`, `EARLY_SIGNAL_ENABLED`, `TRADE_DECISIONS_ENABLED`). When false: topology is byte-identical to today (no new operators, no new timers, no new sinks — validated by `SignalJobOperatorUidTest`'s 17-count assertion). When true: new operators + sinks appear with their pinned UIDs; `preflightTableContracts` validates `candle_live` + `candle_closed` contracts; `applyRuntimeOptions` unchanged (no new memory keys). | `SignalJobConfigTest` — default false, explicit true accepted, blank/invalid fails closed. `SignalJobOperatorUidTest` — two modes run: `MULTITF_ENABLED=false` asserts 17 UIDs exact; `MULTITF_ENABLED=true` asserts 17+new UIDs and new ones are pinned. `SignalJob.buildTopology` with flag off produces JobGraph identical (hash-equal) to baseline dump. | `mvn test -Dtest=SignalJobConfigTest,SignalJobOperatorUidTest,SignalJobStrictGateT7Test` pass. |
| **Phase 5 — Side-by-side soak** | Enable the aggregator in staging/dev alongside the old chain. Feed: real NSE cash replay at 20 Hz peak shape (the 3-min soak shape: burst → sustained → taper, as used for dedup redesign). Both chains consume the same `raw_table_1` source (branching after dedup/monitor). Duration: ≥ 3 min per run, ≥ 3 runs. Side-by-side gate: see §H.6. No new load on production traders. | Soak scorecard green (H.6). Metrics inside bounds (§J). `candle_live` 1 s overwrite visible in Grafana over the full 3 min without stall. Signal rows appear in `Signal_Candidates` with `timeframe` populated and `is_forming=true`, idempotency holds across a forced checkpoint-restore during the soak. | Full `mvn package`, dev cluster run via `docker-compose` + `stage-soak-e2e.sh` shape. Logs carry validated `schemaReport` for new tables. No change to `forming_bar`/`feature_candles_15s_preview` writes. |
| **Phase 6 — Cutover** | ONLY on explicit user approval post-proof. Options: (a) keep both forever (no cutover — safer), (b) switch dashboards to `candle_live` 15s leg, (c) retire old `candle-15s-v2` / `candle-preview-15s-v2` / `forming-bar-*` chain and its DDLs. | User checkmark on acceptance criteria (§J) + soak scorecard (§H.6). Retire change, if taken, is a single commit that removes only the old operators/DLLs — aggregator code untouched. A rollback is `MULTITF_ENABLED=false` + redeploy (old path still checked in until removal). | `SignalJobOperatorUidTest` updated to assert the new UID set; old UIDs declared retired. `allowNonRestoredState` remains `false` — a cutover restart fails closed if state is incompatible. |

**Edit list (hard limit until Phase 6 approval):**

```
Allowed to edit (max four files, same pattern as chain-heap plan):
  code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java
  code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java
  code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/TableContractValidator.java
  code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/SignalJobOperatorUidTest.java

Allowed to add (new files — no edit of old ones except the four above):
  .../signaljob/MultiTfAggregatorFunction.java
  .../signaljob/MultiTfSignalEngine.java  (or inside aggregator as inner class — review picks)
  .../signaljob/SignalContext.java / MultiTimeframeState.java / CandleLite.java
  .../signaljob/BucketMath.java
  .../signaljob/Column contracts for new tables (CandleLiveColumns, CandleClosedColumns)
  DDL scripts for new tables alongside existing DDLs (02_sql/ddl/32_candle_live.sql, 02_sql/ddl/33_candle_closed.sql — numbered to follow the existing 02..31 convention; no `new/` subdir exists)
  Tests: BucketMathTest, MultiTfAggregator*Test, MultiTfSignal*Test, etc.

Explicitly untouchable until Phase 6 (side-by-side guarantee):
  FormingBarBuilderFunction.java, FormingBarWriterFunction.java, FormingBarDetectionFunction.java
  HeapCandleEmitFunction.java, HeapPreviewFunction.java
  CandleEmitFunction.java, CandlePreviewEmitFunction.java, CandleTableColumns.java
  DDLs 02_raw_table_1.sql, 03_feature_candles_15s.sql, 30_feature_candles_15s_preview.sql,
       04_forming_bar.sql, 05_signal_candidates.sql, 23_signal_candidates_current.sql
       (new columns only via additive DDL — never a rewrite)
  docker-compose.yml / docker-stack.yml / ddl-apply / CI scripts
```

---

## I. Risks and Open Unknowns

### I.1 Rated risks

| # | Risk | Why it matters | Mitigation in this design | What would prove it away |
|---|---|---|---|---|
| R1 | **Per-TF heap cap drift** — `MAX_PENDING_PER_TF=16` / `MAX_LIVE_WINDOWS=8` assumptions from the 15 s-only plan may be tight when six TFs interleave and watermarks lag | Stuck watermark + cross-window reorder across six TFs could park more pending closes per instrument than the 15 s-only worst case (≤1) | Per-TF cap, not global; `Preconditions.checkState` fail-closed on breach (same as `HeapCandleEmitFunction.checkPendingCap`); counter `compute.candles.pending.{tf}.high_watermark` gauges it live | Soak test G.8 with injected watermark stalls + reorder crosses the caps — if no trip in 3× 3-min soaks, cap is proven |
| R2 | **Fluss KV read-back for lookback** — Decision 6 says zero Fluss reads on the tick→signal hot path, but a cold start / warm-up may tempt a `get(last 15)` range scan | A range scan per tick would defeat the design and reintroduce SSD latency on every tick | Signal gets history from `closedRing[tf]` in heap/managed state — no Fluss `get`; the only Fluss reads remain the preflight metadata checks at startup (already there). Mark the scan path as `DELIBERATELY NOT IMPLEMENTED` | `ffgrep` for `FlussKvProbe` / `get(` in the hot path — must be absent; review blocks if present |
| R3 | **Watermark vs preview cadence interplay** — the old design had two separate window operators racing the same watermark; the aggregated design has one watermark + six boundary timers + one 1 s processing timer | Timer ordering bugs (boundary fires before the 1 s snapshot sees the last tick) could make the last 1 s snapshot miss the final tick's OHLC in `candle_live` | 1 s live snapshots are always *after* the tick's accumulator update and *before* boundary sealing; order ensured by: tick updates accumulators synchronously before emitting the live context — boundary sealing is event-time (watermark), 1 s is processing-time — no race; test proves late-tick tail always appears in next 1 s row | `HeapCandleEmitFunctionTest`-style harness that interleaves ticks, watermarks, and 1 s timer fires proves the ordering (new `MultiTfTimerOrderingTest`) |
| R4 | **Session-offset date arithmetic** — `sessionOpenMs(date)` must resolve the correct IST date for each `event_time`, including ticks within milliseconds of midnight IST | A midnight bug would mis-align 3m/5m/15m buckets for the whole next day (one wrong `bucketStart` → wrong OHLC) | Use `ZonedDateTime.atZone(Asia/Kolkata)` per tick — no `SimpleDateFormat`; IST is fixed +05:30 (no DST); unit test with ticks at `09:14:59.999`, `15:30:00.000`, `23:59:59.999`, `00:00:00.000` | `BucketMathTest` covers all midnight + session edges; property fuzz (G.7) spans two sessions |
| R5 | **Signal spam if fire-once latch is wrong** — Option A (fire on forming) without a per-window latch would append up to hundreds of rows per bucket | Would flood `Signal_Candidates` (14.6 k/s × many ticks), swamp downstream consumers, violate the "one latch per window per rule" contract | Latch key `(instrument, tf, windowStart, rule_id)` stored in signal engine heap, cleared at boundary close; second tick in same window with same condition is suppressed (append only re-fires after the condition toggled false→true again if we allow re-arm — spec currently `one per window` max) | `SignalFireOncePerWindowTest` with 100 ticks in one window where the rule stays true → exactly one row |
| R6 | **Forced-roll tail bucket under-size** — 15s/30s/1m buckets cut at 15:30 may be < tfMs wide (e.g. 15s bucket [15:29:45,15:30:00) is full, but a cut at 15:30 from a tick at 15:29:50 creates a 10 s final tail if we truncated wrong) | Under-size candle could violate `CandleInvariantCheck` (windowEnd-start ≠ tfMs) and be quarantined instead of emitted, losing the tail | Forced-roll window is marked `isSessionTruncated=true`; invariant check exempts truncated windows from the `windowEnd-start==tfMs` assertion; `output_ts` still at 15:30 | `ForcedRollTailBucketTest` for each TF with tail tick, asserts closed row emitted and not quarantined |
| R7 | **Bucket.count(2433) vs Fluss tablet capacity** — 14.6 k/s live upserts + ~1 k/s closed writes may hotspot a single Fluss bucket if `instrument_token` hashing collides | Fluss routes by `bucket.key=instrument_token` hash; with 16 buckets the hash distributes, but a bad token range could still skew | Same routing as today (16 buckets, hash(token) rebalance) already handles 5 k/s deduped ticks — 14.6 k/s is Keyed KV UPSERTs, not log appends, and they spread across six PK variations per instrument (tf is in PK) | Soak metrics: per-bucket write QPS variance < 3× mean, checkpoint success 100% |
| R8 | **Memory accounting with quoted fields** — quote snapshot per instrument today is tiny, but if bid/ask columns return (DDL v2 removed them) the per-instrument snapshot grows | Could push the heap total above the 1–2 MB serialised / ~17 MiB heap estimate | Quote snapshot is fixed width (two longs + timestamp) regardless of tick payload size; raw_payload bytes are never stored in aggregator state | `ContainerMemoryTest` extended to assert new heap map size still < 50 MiB at N=5000 |

### I.2 Open unknowns (not risks to fix before Phase 2 — tracked as questions to close)

- **U1 — KV vs LOG for `candle_closed`:** KV first-write-wins is proven (existing `CandleKvFirstWriteWinsFunction`), but LOG gives simpler replay dedup if we can tolerate a log-scan for "latest per key" in consumers. Decision: pick KV single-table with `PK (instrument,tf,window_start)` — same as `feature_candles_15s` — unless a storage-benchmark proves LOG scan faster for dashboards. Owner: reviewer pick at Phase 0 exit.

- **U2 — Heap vs RocksDB for `closedRing`:** The forming accumulators must be heap (hot, per-tick mutation, no RocksDB round-trip — same reason as chain-heap OP1). The 15-deep closed rings are read-only on the hot path (copied refs) and updated only at boundary (≤ once/15 s per TF). They could live in RocksDB `ValueState` to keep heap under the chain-heap plan's 50 MiB ceiling. Owner: measure heap delta in Phase 2 harness; if `closedRing` heap > 12 MiB at 2433→5000 instruments, spill to RocksDB.

- **U3 — Exact 1 s timer source:** processing-time (wall clock) vs event-time. Spec says processing-time (trader sees a live bar every clock second, not every event-time second — matches `HeapPreviewFunction`'s event-time preview only because previews are cadence-triggered inside the window). Confirm with Grafana that dashboard polling is wall-time; if dashboards poll on event-time, switch to event-time timer per TF.

- **U4 — TFT-disabled instruments:** some tokens never see 15 s cadence (thin names) — their 15 m bucket may have fewer than 15 closed bars for weeks. Signals that need 15 closes of 15 m would never warm. Owner: rule config should declare per-rule `requiredClosedPerTf` map, not a global 15 — close in review.

- **U5 — Holiday / non-trading date handling:** does `sessionOpenMs` for a market holiday deserve an empty `candle_closed` day (zero rows) vs a synthetic day row? Today's behaviour is zero rows. Confirm that lake offload and downstream range queries tolerate missing dates (they do — verified: `raw_table_1` auto-partition just creates no folder).

---

## J. Acceptance Criteria (user check-list — maps to the 13 decisions and the soak gate)

A reviewer with no session context can tick these off against the design alone (no code yet). Implementation must then make each box provable by a test or a metric.

### J.1 Decision → criterion trace

| # | Decision | This design proves it by | Impl proves it by (future) |
|---|---|---|---|
| 1 | Single incremental aggregator, one composite entry, all 6 TFs in memory, no separate window | §B + §C define one `MultiTfAggregatorFunction` keyed by instrument with one `CompositeState` (forming[6] + closedRing[6][15]); per-tick loop `for tf: aggregate.add` — no `TumblingEventTimeWindows` operator exists | `MultiTfAggregatorFunctionTest` + heap no-managed-state guard (`numKeyedStateEntries==1` if ValueState, 0 if pure heap) |
| 2 | Feed is Arrow FULL, trade=ltq/ltt/volume advanced, quote-only updates bid/ask, TRADE gate reused | §E.2 trade gate predicate is exactly `tickType==TRADE && qty>0` from `CandleAggregateFunction.add`; quote leg updates `quoteSnapshot` only; note in §E.2.2 that no new classifier is built | `CandleAggregateFunctionTest` (mixed) reused unchanged; `RawValidationFunctionTest` already covers tick_type mapping |
| 3 | NSE 9:15–15:30, session-offset alignment for 3m/5m/15m, epoch for 15s/30s/1m, half-open, no overnight carry, 15:30 forced roll | §D.1–D.4 define half-open, epoch vs session-offset formulas, pre/post filter, forced-roll timer | `BucketMathTest` + `ForcedRollTailBucketTest` + fuzz G.7 |
| 4 | All six TFs get live 1 s + closed history | §F.1 (`candle_live` 6 TF × 1 s KV) + §F.2 (`candle_closed` immutable 7d 6 TF) — both parameterised by `tf` in PK | Soak emits 2 433 × 6 rows per second for 3 min and closed rows for each TF's boundaries |
| 5 | Higher TFs from ticks directly, never rolled from lower | §E.2.1 loop does `for tf: add(price,qty)` from the same tick; §G.2 mutation test proves independence | `HigherTfIndependenceTest` fails if roll-up were introduced |
| 6 | Signals get forming + last 15 closed per TF per tick via in-JVM side-output, zero Fluss reads | §B diagram shows `OutputTag<SignalContext>` from aggregator → co-located signal engine; §C closedRing supplies the 15; §I R2 documents the scan that is deliberately absent | `SignalWarmupPerTfTest` reads heap rings, not Fluss; `ffgrep` for Fluss reads on hot path stays zero |
| 7 | Signals fire immediately on forming (Option A) | §F.3 firing "when rule true on forming candle, no wait" + §E.3.1 live context; rule evaluates on the accumulator state after `add` | `SignalFiresOnFormingTest` + fire-once latch test |
| 8 | Every signal kept forever via LOG + KV latest, ACCEPTED-BY-DESIGN never retracted | §F.3 LOG append + KV upsert, row never deleted, `superseded` columns remain null unless downstream supersedes; bounce documented | `SignalCandidatesTableColumns` parity + KV idempotency test; consumer test shows later signal corrects without retract |
| 9 | Signal output stops at Fluss signal table; never calls broker | §B last box "separate project" + §F.3 "stop at Fluss" + §I explicit | `CepDependencyGuardTest` extends to assert no Arrow/broker imports in aggregator/signal packages |
| 10 | Writes only on signal row / 1 s live refresh / boundary close; never per-tick | §E.2.1 "Storage writes: NONE on this tick" + §E.3 timers own the two candle writes + §F.3 signal-row write | Soak metric: `compute.candles.live.emitted` rate ≈ 14.6 k/s, `candle_closed` ≈ 1 k/s, `compute.dedup.*` shows per-tick Fluss writes remain zero |
| 11 | Memory ~1–2 MB serialised, flat with throughput, in-place arithmetic, one state round-trip per tick | §C.2 arithmetic: 2 433 × 7 300 B heap ≈ 17 MiB heap / 1–2 MB serialised, `O(instruments×TF×15)` not `O(ticks)`, no tick buffering | `ContainerMemoryTest` extended: `compositeStateSize` histogram + heap dump under 20 k/s feed proves flat |
| 12 | Safety: monotonic event time, loud late-drops, gap/discontinuity drop-stale + marker, closed first-write-wins | §E.2.4 late drop LOUD + §E.4 gap/marker/discontinuity table + §F.2 immutability | `GapAndDiscontinuityTest` + `CandleLateDropTest` + `ClosedImmutabilityTest` |
| 13 | Old 15s path stays side-by-side until user approval | §B side-by-side box + §H edit list ("explicitly untouchable") + §H Phase 6 cutover only on approval | `SignalJobOperatorUidTest` asserts 17 old UIDs unchanged when `MULTITF_ENABLED=false`; Soak gate H.6 needs bit-identical 15s |

### J.2 Build + gate scorecard (Phase 5 soak — must be green before any cutover discussion)

| Gate (3-min 20 Hz soak, real market replay shape, as §H Phase 5) | Bar | Signal |
|---|---|---|
| Side-by-side 15s bit-identical (new tf='15s' vs `feature_candles_15s`) | 100% rows match minus `output_ts` over the whole window | FAIL the line if any OHLCV mismatch |
| `candle_live` 1 s cadence | observed upsert rate `2 433 × 6 ± 2%` for ≥ 95% of seconds | WARN if < 98% |
| Late drops | `compute.candles.late.dropped` < 0.1% of fed ticks | ALERT if > 1% |
| Gap markers | every injected stall produces exactly one discontinuity marker per affected TF | FAIL if missing or double |
| Checkpoints | p50 ≤ 20 s, 100% success over the soak | FAIL if any timeout |
| Signal idempotency | forced checkpoint-restore mid-soak re-emits exactly the same `candidate_id` set (no duplicates in LOG) | FAIL if duplicate appears |
| Signal warm-up | first `N×listSize` ticks per instrument emit exactly 0 signals per TF | FAIL if early fire |
| Memory | heap post-soak < 60 MiB above baseline (chain-heap plan ceiling) | FAIL if OOM/heap trip |

---

## K. What this document deliberately does not decide (left to implementation review)

- Whether `closedRing[6][15]` is pure heap or `ValueState` (U2).
- Whether `candle_closed` is single-table-with-tf-PK vs six per-TF tables (U1 — lean is single table).
- Whether the fire-once latch allows a re-arm *within* the same window after the condition toggled false→true again (spec here is one-per-window; a second trigger shape can be added without changing the state layout — just the latch reset rule).
- Exact column additions to `Signal_Candidates` (new `timeframe` String vs reusing `rule_id` naming) — both are feasible; Phase 0 DDL review picks one.
- Exact Flink uid names beyond the prefix `multi-tf-aggregator-v1` / `candle-live-sink` / `candle-closed-sink` / `multi-tf-signal-v1` (names locked in Phase 4 when the `SignalJobOperatorUidTest` expectation is written).

---

## L. File map for the reviewer (where to look after reading)

| Thing in this doc | Canonical source to open |
|---|---|
| Operator wiring & uid pattern | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java` |
| `add()` math that is reused | `CandleAggregateFunction.java` + `CandleAccumulator.java` |
| Heap-state slot + timer pattern | `FormingBarBuilderFunction.java`, `HeapCandleEmitFunction.java`, `HeapPreviewFunction.java` |
| 1 s preview TTL & row building | `CandlePreviewEmitFunction.java`, `30_feature_candles_15s_preview.sql` |
| Closed-candle immutability guard | `CandleKvFirstWriteWinsFunction.java` |
| Lookback `isWarm()` / `evaluate()` | `SignalLookbackState.java` |
| Config flag pattern | `SignalJobConfig.java` (`booleanValue`, `positiveLong`) |
| Contract validation pattern | `TableContractValidator.java` |
| Column layout (must mirror DDL) | `RawTableColumns.java`, `CandleTableColumns.java` |
| Watermark | `CandleWatermarkStrategy.java` |
| DDLs | `code/01_platform/02_sql/ddl/02_raw_table_1.sql`, `03_feature_candles_15s.sql`, `30_feature_candles_15s_preview.sql`, `04_forming_bar.sql`, `05_signal_candidates.sql`, `23_signal_candidates_current.sql` |
| Harness & POJO tests | `HeapCandleEmitFunctionTest`, `CandlePojoRecognitionTest`, `FormingBarBuilderFunctionTest`, `SignalDetectionFunctionTest`, `CandleAggregateFunctionTest`, `SignalJobOperatorUidTest` |

---

*End of design. No production source, test, DDL, Docker, or CI file was edited to produce this document.*
