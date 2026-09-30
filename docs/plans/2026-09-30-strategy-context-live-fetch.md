# Live-candle strategy context — on-demand old-data fetch inside the signal job

**Date:** 2026-09-30
**Status:** operator-approved scope (decisions #1–#5 below). Items start only when their
window opens; contract changes are approved at their own item.
**Inputs (read-only, 2026-09-30):** full code review of the strategy path; native APIs
verified in the pinned jars (`fluss-client-1.0.0`, `flink-streaming-java-2.2.1`);
DEC-040 / DEC-054 / DEC-056 / DEC-057; DDLs `32_candle_live.sql`, `33_candle_closed.sql`,
`34_feature_values.sql`.

## Requirement (hard constraints, operator 2026-09-30)

1. **Live candle, never close-wait (hard).** A decision that needs old data must fire while
   the window is still forming — never deferred to the candle close.
2. **Dynamic need.** Any strategy may fetch old data at evaluation time; no static
   allow-list of strategies and no pre-registered window list.
3. **The signal job owns the context.** The job itself computes/keeps it — no separate job,
   no separate context table, no mirror stream.
4. **Memory budget (approved).** Context cache ≤ **8 MB per subtask** (configurable);
   ≤ 32 in-flight fetches per subtask; 100 ms fetch timeout + cooldown retry; if a strategy
   needs more than ~500 windows per (instrument, timeframe), derive and keep **scalars**
   instead of raw slices.
5. **Where values live (approved).** Broadly reusable scalars → `FeatureRegistry` /
   `feature_values` (one registry line, shared, externally visible); private on-demand
   slices → the context cache (memory only). The same value never lives in both places.

## Why this shape (discovered by code reading)

A request/response loop that leaves the host and returns as a stream is **impossible** in
the pinned Flink: the DataStream iteration API is absent from
`flink-streaming-java-2.2.1.jar` (no `IterativeStream` classes), and a plain union-back
would be a job-graph cycle. Feeding old data into a strategy that must fire on the live
candle therefore has exactly one acyclic native shape: **fetch inside the host operator**
(a `ContextProvider`), complete the future off-thread, and wake the host on its own
mailbox with a keyed processing-time timer (native; same mechanism as
`MultiTimeframeAggregateFunction.java:420,755`).

## Current state (verified in code, for the record)

| Fact | Where |
|---|---|
| Host is a two-input keyed process function with `open()` | `StrategyHostFunction.java:58-59,167` |
| Slot = strategy instances + shared per-instrument features; cap 65 536/subtask | `StrategyHostFunction.java:262-290`, `:70` |
| Strategies are per-(token, ruleId) heap objects; default-overload contract pattern | `SignalStrategy.java:32-83` |
| N7 keeps its own 7-candle rings per TF | `N7RangeBreakoutStrategy.java:61,67` |
| `SIGNAL_TAG` (15-candle context) exists but is disabled by default and has no consumer; per-tick deep copies when on | `MultiTimeframeSignalContext.java:9-16`, `MultiTimeframeAggregateFunction.java:73,98,652-666` |
| Features: 3 registered, computed once per instrument, allocation-free | `FeatureRegistry.java`, `PerInstrumentFeatures.java:24-56` |
| `candle_closed`: immutable (first-write-wins), PK (token, tf, window_start), 7 d + Iceberg | `33_candle_closed.sql` |
| `candle_live`: 60 s log TTL, ~1 s upserts — never a decision source | `32_candle_live.sql` |
| `feature_values` = PROPOSAL only; feature layer default off | `34_feature_values.sql`, DEC-056/057 |
| Sync Fluss on the hot path is a proven failure (checkpoint-1 stall); the sanctioned future is async lookup + partial caching | DEC-040 |
| Native lookup machinery present and unused: `Lookuper.lookup(InternalRow) → CompletableFuture<LookupResult>`; keys via `GenericRow`; `client.lookup.queue-size / max-batch-size / max-inflight-requests / batch-timeout` | `fluss-client-1.0.0.jar` |
| Adding a sink raised 250–500 ms tail bursts (feature-layer S4) → this design adds **no** sink | dossier `04-signal-job.md` |

**Alternatives rejected (audit trail):** feedback loop via `AsyncDataStream` → graph cycle,
no iteration API in 2.2.1; close-boundary deferral → violates constraint #1; separate
context job/table/mirror → violates #3 and re-risks the sink-tail finding; synchronous
lookups on the hot path → DEC-040 forbidden; reusing `SIGNAL_TAG` → disabled default,
per-tick deep-copy cost, no consumer.

## 0. Live tracker

#### P1 - design lock

- [x] **C0** Design + API verification locked (this document; 2026-09-30).

#### P2 - build (each item starts only when its window opens)

- [~] **C1** `ContextProvider` + `ContextView` skeleton (no consumer yet)
- [~] **C2** Contract additions + host wiring (default callbacks, last-live snapshot, timer wake-up)
- [~] **C3** First consumer proves live-candle firing end-to-end
- [~] **C4** Compact context derivation + warm-up (T3)

#### P3 - certification

- [ ] **C5** Certification: tests + gate + no-regression on the 50 ms protocol + docs

**Roll-up**

| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| P1 - design lock | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| P2 - build (each item starts only when its window opens) | 4 | 0 | 4 | 0 | 0 | 0 | 0 |
| P3 - certification | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| **Total** | **6** | **1** | **4** | **1** | **0** | **0** | **0** |

## Overview

**Success criteria**

| # | Criterion | Proof |
|---|---|---|
| S1 | A decision needing old data fires on a **live forming candle**: miss → no emit; fetch completes; emit on a live tick of the same window **before its close**; candidate-id dedup intact | E2E test + live smoke |
| S2 | No regression: strategies that do not fetch behave identically; the tick-path KPIs (`ingest_to_monitor`, `tick_to_strategy`) stay within the certified 50 ms-protocol thresholds | existing suites + smoke + 900 s run |
| S3 | Memory bounded: cache ≤ 8 MB/subtask with eviction, ≤ 32 in-flight, timeout/cooldown metrics; no unbounded growth | unit caps tests + run metrics |
| S4 | Native and non-invasive: no new sinks/tables/operators, no UID change, no DDL apply; code changes carry failing-first tests + CHG + green `make gate` | gate certificate + CHG |

**Working rules** (house rules): failing-first test → change → full `make gate` → then
measure; smoke before any long run; no code edits while a gate runs; evidence under
`logs/`, never edited; one CHG per commit region; go/no-go after each item.

**Non-goals:** close-boundary deferral; separate context job/table/mirror; arbitrary
ad-hoc query API beyond the declared slice/range forms; reading `candle_live`; history
older than the 7 d `candle_closed` TTL (lake path out of scope); any change to dedup,
candle, or checkpoint semantics.

## Design

### Component

`ContextProvider` (new class, `signaljob` package) owned by `StrategyHostFunction`, one
per subtask:

- `open()`: build the Fluss `Configuration` (same pattern as
  `SignalJob.flussSourceConfiguration`), `ConnectionFactory.createConnection(...)`,
  `getTable(TablePath.of(db, "candle_closed"))`,
  `newLookup().lookupBy("instrument_token","tf","window_start").createLookuper()`.
- `close()`: close the connection (native bounded close exists).
- A read-only `ContextView` handed to strategies, backed by the provider.

### Flow (live candle, no close-wait)

1. A live tick arrives; the host dispatches to strategies.
2. A strategy asks the `ContextView` for old data (`candle(tf, windowStart)` /
   `slice(tf, count)` forms).
3. **Hit** → value returned → the strategy decides **on the live candle**.
4. **Miss** → the provider schedules one fetch (single-flight per key set), marks the slot
   pending, and returns "not ready"; the strategy does not fire this tick.
5. The host registers a one-shot keyed processing-time timer (now + ~2 ms) the first time
   a slot goes pending.
6. The Fluss future completes on a client thread → it writes **only** into a concurrent
   arrival map (never operator state, never strategy code).
7. The timer (or the next live tick, whichever comes first) consumes the arrival **on the
   mailbox thread**, loads rows into the heap cache, and invokes the new default callback
   `onContextReady(...)` with the current live snapshot → the strategy decides **on the
   live forming candle**. While still pending the timer re-arms; at the 100 ms timeout the
   value is marked absent (counted) and retried after a cooldown.
8. Re-evaluation is idempotent: candidate ids are deterministic and the host's existing
   emitted-id dedup prevents duplicates.

### Goal mapping

| Goal | How |
|---|---|
| Low latency | Tick path never blocks, never RPCs; fetch completes in ms; timer wake-up ≈ ms; cache hits are local |
| High throughput | Only misses fetch; Fluss client batches concurrent lookups; demand is per-need, not per-tick |
| Low memory | 8 MB/subtask cap + LRU eviction; derive→keep scalars; deep history stays in Fluss (7 d + lake) |
| Native | Fluss `Lookuper`/`CompletableFuture`/`GenericRow`; Flink keyed processing-time timers; default-method contract extension |
| Correctness | Only immutable `candle_closed` is cached; pure side-effect-free reads; single-flight + caps + cooldown; timeout ⇒ absent + counted; deterministic re-eval + dedup; no new Flink state (heap rebuilt on demand) |

### Rollout flag

`STRATEGY_CONTEXT_ENABLED` (default **false**): with it off the provider is not created and
the `ContextView` reports "disabled" — zero cost, certified state untouched.

## Items

### C1 — ContextProvider + ContextView skeleton (no consumer)

**GIVES YOU** — the fetch machinery, bounded and observable, with zero behavior change.
**FIT** — new classes in `signaljob`; wired behind `STRATEGY_CONTEXT_ENABLED=false`; unit
tests use a fake connection/lookuper (repo precedent: gateway stubs, ingestion
`FakeConnection`).
**COST** — one Fluss connection per subtask when enabled; heap cache ≤ 8 MB.
**ACTION** — failing-first unit tests (hit/miss, single-flight, timeout, cooldown, caps,
metrics, mailbox-thread safety) → implement → smoke with the flag on but no consumer →
tick KPI unchanged → CHG.
**WRONG IF** — any tick-latency regression, any operator/strategy state touched off the
mailbox thread, or caps not enforced; revert.

### C2 — Contract additions + host wiring

**GIVES YOU** — strategies can receive fetched data on the live path without changing
existing behavior.
**FIT** — `SignalStrategy` gains a default `onContextReady(...)` no-op (same pattern as the
DEC-056 feature overloads); the host keeps a minimal last-live snapshot (token,
last_event_time, close/high/low, fingerprint) only while a slot has a pending request;
timer registration in `processElement1`; re-dispatch on timer.
**COST** — a small per-slot snapshot while pending (rare); one timer while pending.
**ACTION** — pin existing strategies' behavior with tests; add the re-entrancy contract
(note in `SignalStrategy` Javadoc: repeated live evaluations of the same
`(token, last_event_time)` must be idempotent) → smoke → CHG.
**WRONG IF** — strategies that do not fetch change behavior; revert.

### C3 — First consumer proves live-candle firing end-to-end

**GIVES YOU** — the proof of hard constraint #1.
**FIT** — a test-only strategy first (no production config change); any real rule later —
the API is dynamic, no registration list.
**COST** — test code + one live smoke.
**ACTION** — prove: miss → no emit; fetch completes; emit on a live tick of the same
window **before its close**; dedup intact; fetch latency metrics recorded.
**WRONG IF** — the only way to fire is at the close; stop and re-scope (the design failed).

### C4 — Compact context derivation + warm-up (T3)

**GIVES YOU** — small retained scalars (prior-day levels, opening range, …) and a warm-up
path that makes first live needs usually a cache hit.
**FIT** — `scalar(name)` retention + deterministic derivation from fetched history;
optional warm-up on first touch/close; memory audit against decision #4.
**COST** — small heap; no persistence (rebuilt on demand).
**WRONG IF** — budget exceeded or derivation non-deterministic; revert to raw slices.

### C5 — Certification

**GIVES YOU** — the capability certified with no regressions.
**FIT** — full `make gate`; one 900 s run under the 50 ms protocol (both KPIs within the
certified thresholds); dossier rows (`docs/08_implementation/04-signal-job.md`); CHG; this
tracker closed.
**ACTION** — run → extract → update docs → operator sign-off.
**WRONG IF** — mixed results reported as pass; forbidden.

**BLOCKED (2026-09-30; raised on the planned 900 s run):** C5's acceptance is "both KPIs
within the certified thresholds" — i.e. the W6 thresholds of
`docs/plans/2026-09-30-p99-50ms-normal-path.md`, which **do not exist yet** (W1–W5
unimplemented; the certified reference today is the post-CT-4A 100 ms-target baseline,
p99 ≈ 70–93 ms). A 900 s run now could only produce a no-regression statement, never C5 —
and the context smokes so far ran at 60 s checkpoints / changelog-off, which is not the
certified protocol either. C5 starts only after W6 certifies the thresholds; the same
certification run then carries the context no-regression evidence on the final path.
C1–C4 are landed (code + tests + functional smokes); the capability itself is proven live
(see CHG-447's probe evidence).

## Risks

| Risk | Mitigation |
|---|---|
| Timer overhead while pending | timers exist only while a slot is pending; measured in C1/C2; rare by construction |
| Client-thread code touching operator/strategy state | protocol: arrivals go to a concurrent map only; concurrency test + code review gate |
| Request storms from a strategy | single-flight per key set + ≤ 32 in-flight + cooldown + counters |
| Cache memory | 8 MB cap + LRU + per-key row cap + metrics; >~500 windows ⇒ scalars (decision #4) |
| Strategy idempotency | contract note + deterministic candidate ids + existing emitted-id dedup |
| Restore behavior | no new Flink state; heap cache rebuilt on demand; pending requests simply re-issued |
| Interaction with the 50 ms workstream | no new sinks/operators; C5 runs the 50 ms protocol before closure |

## Operator approval points

1. Approve this scope (now). — required before C1 starts.
2. C2 (contract change) and C3 (first consumer): explicit go before implementation.
3. Production adoption (flag on in a real config): separate approval; no silent flips.

## Evidence map

| Step | Evidence |
|---|---|
| C0 | this document (code + API verification) |
| C1/C2 | unit tests + smoke run + CHG |
| C3 | E2E test + live smoke + fetch latency metrics |
| C4 | memory audit + unit tests |
| C5 | 900 s run + `make gate` certificate + dossier diff |

## Rollback

- C1/C2: revert the commit + `make gate`; with `STRATEGY_CONTEXT_ENABLED=false` the
  certified behavior is untouched.
- C3/C4: remove the consumer / revert the derivation; no state migration either way.
