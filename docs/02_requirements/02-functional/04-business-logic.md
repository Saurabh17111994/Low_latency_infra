# 02.4 — Business Logic

## Purpose

Business Logic is a stateful operator inside the Signal Flink job. It consumes Compute's in-job closed-candle and forming-bar events, detects patterns, and creates immutable candidates. It never calls a broker and never mutates lifecycle or position state. **(Updated 2026-10-01: closed-candle events are multi-timeframe `candle_features` sealed rows (15 s/30 s/1 m/3 m/5 m/15 m); forming events are the in-memory per-tick forming context — no stored forming row exists (DEC-059/Wave C, CHG-486).)** **Ranking/Reservations/Decisions (Slice 3) is REMOVED from scope 2026-08-15 (CHG-005, not deferred) — see REQ-SS-005/006/009/010 below.**

> **MVP scope (Slice 2.1, DEC-034, implemented 2026-08-10):** the implemented subset is closed-candle signal detection → `Signal_Candidates` records per REQ-SS-003 (immutable append). **Scope update 2026-08-15 (CHG-005): Ranking/Reservations/Decisions (Slice 3) is REMOVED — the ranking, reservation, and decision requirements below (REQ-SS-005/006/009/010, REQ-RNK-*) are out of scope, not postponed. Forming-bar detection (REQ-SS-002) is implemented 2026-08-16.** **(Updated 2026-10-01: closed-candle detection runs per timeframe on `candle_features` sealed rows (15 s/30 s/1 m/3 m/5 m/15 m); forming-bar detection runs on the in-memory per-tick forming context — the stored `forming_bar` table is RETIRED (2026-09-05) and no forming row is stored (CHG-486).)**

## Constraints

- Business Logic SHALL NOT call a broker, submit an order, or interact with Arrow REST. Order execution is owned by the Executor.
- Business Logic SHALL NOT mutate lifecycle (`Order_Lifecycle`) or position (`Positions`) state. These are owned by Action Capture and the position projector.
- Business Logic SHALL NOT publish an executable instruction when the portfolio reservation view is missing, stale, conflicting, or contains unresolved `UNKNOWN` state. **(REMOVED with ranking 2026-08-15, CHG-005 — reservation state is out of scope.)**
- Business Logic SHALL NOT reuse `instruction_id` for different quantity, side, symbol, price, strategy version, or trade context. A changed winning parameter creates a new `instruction_id`.
- Business Logic SHALL NOT treat cross-table visibility as atomic ordering. Executor is authoritative for submission eligibility and supersession sequencing.
- `Signal_Candidates` LOG records SHALL NOT be updated. Corrections are new records with an explicit supersession relation. The companion `Signal_Candidates_current` KV projection IS updated in place: supersession overwrites the per-instrument row (RE-SCOPED 2026-08-13).
- ~~The ranking/reservation operator SHALL be a serialized scope per `portfolio_id`~~ — **REMOVED 2026-08-15 (CHG-005).**

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-BL-001 | The Compute operator delivers typed closed-candle and forming-bar events in a deterministic order within each instrument key group after deduplication. **(Updated 2026-10-01: closed-candle events are `candle_features` sealed rows, multi-TF; forming events are the in-memory per-tick forming context — no stored forming row, CHG-486.)** | REQ-FC-013, REQ-SS-007 |
| ASM-BL-002 | ~~The in-job portfolio reservation state interface~~ — **REMOVED 2026-08-15 (CHG-005).** | REQ-SS-001, REQ-SS-009 |
| ASM-BL-003 | Executor respects the supersession contract: a replacement instruction carrying `supersedes_instruction_id` is held or rejected until the predecessor is terminally disposed or explicitly reconciled. | REQ-SS-010 |
| ASM-BL-004 | Fingerprint collisions at the dedup stage do not cause candidate identity collisions or spurious supersession at the Business Logic layer. | RISK-001 |
| ASM-BL-005 | The strategy versions, configuration hashes, and scoring parameters are deterministic and replayable from the checkpointed state. | REQ-SS-007 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Forming-bar detection fires on incomplete data:** Patterns may fire on the forming bar before window close. This enables low-latency entry but means a signal may be invalidated later if the bar reverses. The operator defines one-shot, repeatable, or updated detection semantics per strategy. **(Updated 2026-10-01: the forming bar is the in-memory per-tick forming context — no stored forming row exists (DEC-059/Wave C, CHG-486).)**
- **Immutable instructions are never corrected in-place:** A `Signal_Candidates` LOG record is never updated; the `Signal_Candidates_current` KV row is overwritten in place by supersession. If parameters change, a new candidate and new `instruction_id` are created with a supersession relation. The old candidate remains as audit evidence.
- ~~**Reservations are conservative:**~~ **REMOVED 2026-08-15 (CHG-005).**
- **Deterministic replay is bounded:** Replay determinism is guaranteed only under identical ordered input, fingerprint version, strategy version, and configuration version. Different arrival order or missing external state may produce different results. (**Lifecycle/reservation snapshot clause REMOVED 2026-08-15, CHG-005.**)
- ~~**In-operator ranking with no Fluss round trip:**~~ **REMOVED 2026-08-15 (CHG-005).**
- **Same winner, unchanged parameters = audit only:** A repeated evaluation of the same setup with identical executable content produces only an audit record, not a new instruction.

## Out of Scope

The following capabilities are explicitly NOT owned by Business Logic:

- **Candle computation, event-time watermarking, and deduplication:** Owned by Compute within the same Signal job.
- **Broker order submission, execution, gate management, and Arrow REST integration:** Owned by the Executor.
- **Postback capture, fill lifecycle, and position projection:** Owned by Action Capture.
- **Position monitoring and position-action emission:** Owned by the Babysitter Flink job.
- **Order lifecycle state mutation (`Order_Lifecycle`):** Owned by Action Capture. (Business Logic's reservation-read for eligibility is REMOVED 2026-08-15, CHG-005.)
- **Strategy authoring, backtesting, or configuration UI:** Deferred; not in MVP scope.
- ~~**ML-based ranking or dynamic weight adjustment:**~~ **REMOVED 2026-08-15 (CHG-005).**
- **Multi-broker or multi-account strategy routing:** Deferred; not in MVP scope.

## REQ-SS-001: State and ownership

The operator SHALL maintain versioned state for:

- Per-instrument closed-candle ring buffer
- Current forming-bar accumulator **(Updated 2026-10-01: in-memory per tick; never a stored row.)**
- Active setup descriptors
- Candidate/evaluation identity
- ~~Portfolio reservation view received through the tested in-job/materialized state interface~~ **(REMOVED 2026-08-15, CHG-005)**

State restoration SHALL be checkpointed with the Signal job. (The reservation-state suppression clause is REMOVED 2026-08-15, CHG-005.)

## REQ-SS-002: Forming-bar detection

Patterns MAY fire on the forming bar as soon as a verified condition is met. Every candidate includes instrument, strategy, rule, event timestamp, formation state, entry parameters, and the strategy version/configuration hash. **(Updated 2026-10-01: the forming bar is the in-memory per-tick forming context — no stored `forming_bar` table or forming row exists (RETIRED 2026-09-05; DEC-059/Wave C, CHG-486).)**

The operator SHALL define whether a setup is one-shot, repeatable after invalidation, or updated. A repeated evaluation of the same setup is audit-only unless it creates a new immutable instruction under the instruction lifecycle rules.

## REQ-SS-003: Candidate audit

Every detected candidate, selected or rejected, SHALL append an immutable `Signal_Candidates` record with:

- `candidate_id`
- `instruction_id` when an instruction is created, otherwise null
- `trade_context_id` when known
- Instrument, strategy, rule, action, price, quantity, product, and order type
- Event and evaluation timestamps
- Score inputs and strategy/configuration version
- Detection reason and validity state

A candidate record is never updated. Corrections are new records with an explicit supersession relation.

## REQ-SS-004: Immutable instruction lifecycle

`instruction_id` is immutable. A candidate whose winning parameters change SHALL create a new instruction with a new `instruction_id`; the prior instruction becomes `SUPERSEDED` or `CANCELLED` in execution-owned state before a replacement can be submitted.

The Signal job SHALL not reuse `instruction_id` for different quantity, side, symbol, price, strategy version, or trade context. Exact identifier encoding is versioned and collision-tested.

## REQ-SS-005: Portfolio reservation — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** The reservation state machine (`RESERVED`/`SUBMITTING`/`PENDING`/`OPEN`/`RELEASE_PENDING`/`RELEASED`/`UNKNOWN`) and conservative capacity model are out of scope; the `Portfolio_Reservations` table is not part of the current system.

## REQ-SS-006: Ranking handoff — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** There is no in-operator Ranking function in the current system.

## REQ-SS-007: Deterministic replay

Replay determinism is defined relative to an identical ordered input snapshot, fingerprint algorithm/version, and strategy/configuration version. It is not promised across different arrival order, fingerprint collisions, missing external state, or changed configuration. (**Lifecycle/reservation snapshot clause REMOVED 2026-08-15, CHG-005.**)

## REQ-SS-009: Portfolio-keyed ranking and reservation topology — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** There is no `portfolio_id` repartition or serialized ranking/reservation scope in the current system.

## REQ-SS-010: Reservation lifecycle and supersession — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** Reservation lifecycle state is out of scope. (The Executor-side supersession contract for replacement instructions remains an Executor concern where applicable.)

## REQ-SS-011: Candidate and evaluation bounds

Business Logic SHALL define candidate expiry, invalidation, repeat/evaluation triggers, maximum active candidates per instrument and portfolio, timer bounds, and cleanup behavior. Every evaluation SHALL have a stable `evaluation_id` and source state/version snapshot.

## REQ-SS-008: Acceptance

Tests SHALL prove pattern detection on forming bars, closed-candle handoff, one-shot/repeat behavior, immutable instruction creation, supersession, missing-state suppression, restart restore, candidate audit completeness, bounded candidate state, and deterministic replay under a fixed input snapshot. **(Global capacity and reservation transitions are REMOVED 2026-08-15, CHG-005.)**
