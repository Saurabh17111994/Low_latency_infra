# N7 range-contraction entry signal — design (2026-09-05)

## Purpose

The user's entry-signal method is now the N7 pattern. N7 means Narrow Range 7
or NR7. It replaces the old 20-candle breakout rule and the Phase-3
placeholder multi-TF producer rule as the signal source for the new chain.

This document records the approved N7 semantics before implementation. It
matches the plan approved in-conversation on 2026-09-05, Option B.

## Scope

- Add an N7 signal operator to the compute job.
- The operator consumes the multi-TF aggregator's two existing outputs.
  They are the completed candle stream and the live forming candle stream.
- The operator emits Signal_Candidates rows.
- The old chain's 20-candle breakout rule and the Phase-3 placeholder
  multi-TF producer rule are removed.

## Definitions

- **Candle range**: high minus low in paise.
- **N7 candle**: a completed candle whose range is strictly smaller than the
  range of each of the previous 6 completed candles of the same timeframe.
  Strict means no tie. An equal range does not count.
- **N7 setup**: the N7 candle's high and low. They act as breakout levels
  while the setup is active.
- **Setup lifetime**: a setup arms when its N7 candle completes. A newer N7
  candle of the same timeframe replaces the setup. Nothing else disarms it.
- **Breakout**: price trades strictly above the setup high, or strictly below
  the setup low.
- **D-006, higher-timeframe priority**: when more than one timeframe of one
  instrument breaches in the same evaluation, only the highest-timeframe
  breach emits a signal. A single lower-timeframe breach still fires alone.

## Behavior

The operator is a KeyedCoProcessFunction keyed by instrument_token.

Input 1 is the live forming candle stream at a 1s cadence. It drives the
breakout check on every new trade price.

Input 2 is the completed candle stream. It drives the N7 ring and setup
arming.

Warm-up. Each timeframe keeps the last 7 completed candles of that timeframe.
No setup can arm until 7 completed candles exist. The operator needs no
other warm-up state.

Arming. When a completed candle arrives, the operator appends its range to
the timeframe ring. If the ring now holds 7 candles and the newest candle is
strictly the narrowest of the 7, the operator arms a new setup from that
candle's high and low. The setup replaces any older setup of that timeframe.

Breakout check. Each live forming candle row carries the current trade price
and a last-trade timestamp. When a live row shows a price newer than the last
evaluated price for that instrument, the operator checks every armed setup of
every timeframe against that one price. This is the single evaluation point.
It keeps all timeframes coherent because all forming candles of one
instrument share the same last trade price at the same instant. It also makes
D-006 a single decision, not a cross-row race.

If one or more setups breach, the operator picks the highest timeframe among
the breached setups and emits one Signal_Candidates row for it. Rows are
action=ENTRY with side=BUY when price breaks above a high, or side=SELL when
price breaks below a low.

Fire-once per setup. Once a setup has fired in either direction, it never
fires again. A new setup of the same timeframe arms with a clean slate.

Cross-stream ordering safety. The two inputs have no cross-stream order
guarantee. A live row can arrive before the completed candle that arms the
setup it breaches. To close this gap, arming a setup also checks the last
known trade price of that instrument. If that price already breaches the new
setup and the price is newer than the setup candle's own window end, the
operator fires immediately. A price inside the setup candle's own window can
never breach it, so this check cannot false-fire on the arming candle itself.

Intentional amnesia and replay safety. The operator mirrors the multi-TF
aggregator's memory model. The closed-candle rings and the last-price fields
live on the heap and rebuild from live rows after restore. The armed setups
and their fired flags are managed keyed state. They survive
checkpoint-restore so a replay never re-fires a setup that already emitted.
The emit decision, the fired-flag write, and the transactional Fluss LOG sink
commit under the same checkpoint barrier. A restored replay therefore
converges to exactly-once rows. This matches the retired producer's contract.

State bounding. The heap holds at most 7 candles per timeframe per
instrument. The managed state holds at most one armed setup per timeframe
per instrument. Both are structural caps, not time-based.

Sinks. The operator emits into the existing signal dual-sink. They are the
Signal_Candidates LOG, append-only, and the Signal_Candidates_current KV
projection through the canonical signal filter. This mirrors the retired
multi-TF producer exactly.

## Rule identity

- strategy_id: the canonical strategy id simple-breakout.
- rule_id: n7-range-breakout-v1.
- schema_version: 2.
- The rule id joins the canonical filter's admitted set.

## Operator identity

- name n7-signal, uid n7-signal-v1.
- The uid is new so a checkpoint that carries the retired producer's state
  fails closed instead of attaching to the new operator.

## Config

- MULTITF_ENABLED gates the whole branch. N7 lives inside it.
- N7_RULE_ID overrides the rule id. Default n7-range-breakout-v1.
- MULTITF_SIGNAL_CONTEXT_ENABLED default flips to false. The per-tick
  signal-context photocopy no longer feeds any job branch after the producer
  is removed. Default-on would copy 6 forming accumulators and 6 closed
  rings on every tick into the void. The aggregator still supports the
  side-output for its direct-construction tests and for a future consumer.

## Metrics

- compute.n7.signal.emitted: rows emitted.
- compute.n7.signal.suppressed: lower-timeframe breaches dropped by D-006,
  and fired-setup repeats.
- compute.n7.setup.armed: new setups armed.

## Out of scope

- No DDL change. Rows go into the existing 22-column signal tables.
- No change to the aggregator's candle math, sinks, or SIGNAL_TAG side output.
- No change to the old 15s path in this step. The old-chain removal is a
  separate approved change.
- The forming-bar placeholder rule stays until its own removal decision.
