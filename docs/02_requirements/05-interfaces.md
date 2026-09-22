# 05 — Interface Requirements

## Interface policy

All interfaces are versioned contracts. Exact Arrow/Fluss protocol values are evidence-gated until official artifacts or sandbox captures pass. External calls are at-least-once/uncertain until reconciled; Flink exactly-once is limited to tested state/sink boundaries.

## Constraints

- All interfaces SHALL carry a schema/protocol version. An implicit or unversioned interface is not ready for implementation or live-money release.
- External calls SHALL be treated as at-least-once or uncertain until reconciled. No external broker or REST call SHALL be described as exactly-once from Flink checkpointing.
- Exactly-once SHALL be claimed only for version-pinned, tested Flink state/sink boundaries. Independent non-Flink writes and cross-table visibility SHALL NOT be described as exactly-once without a connector test.
- Cross-table atomic visibility SHALL NOT be claimed without a version-pinned connector test proving the specific behavior. Consumers SHALL tolerate partial visibility and reconcile using stable IDs.
- A modified instruction under the same `instruction_id` is a contract violation. Executor SHALL halt, quarantine, and alert; it SHALL NOT submit the modified instruction.
- No postback SHALL infer a broker-order mapping from proximity, timing, or overloaded IDs. Postbacks without a unique verified mapping are quarantined.
- A latency SLO SHALL name the exact pair of boundary events it measures. `commit` alone is not a sufficient boundary definition.
- Secrets, credentials, tokens, and original payload bytes SHALL NOT be transmitted through observability interfaces. Redaction SHALL occur at the emitting component.
- Every interface SHALL have a compatibility test covering malformed input, unknown versions, duplicate delivery, out-of-order events, partial writes, retries, timeout/unknown outcomes, restart, changelog gaps, authorization failure, and supersession races.
- Safety-halt request consumption SHALL NOT replace Executor's final pre-call gate, fencing, and health checks. The safety-halt interface and the pre-call gate are separate, independently enforced safeguards.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-IF-001 | Arrow postbacks expose `broker_order_id`, lifecycle status, and the submitted `remarks` value via the order-updates WebSocket (`wss://order-updates.arrow.trade`). | **Validated** — ASM-002 confirmed. |
| ASM-IF-002 | Fluss `partial_update` and FULL changelog behavior match Fluss 1.0.0 for KV projection interfaces. | Pinned version (1.0.0) confirmed to support these features. Integration test pending. |
| ASM-IF-003 | Arrow REST `POST /order/regular` returns deterministic order-submission responses with enough evidence to correlate broker order identity. | **Validated** — Response includes `orderNo` (system order ID). `GET /order/{id}` returns `exchangeOrderID`, full lifecycle. |
| ASM-IF-004 | Fluss 1.0.0 supports the required binary payload type (`BYTES`), KV state tables, changelog images, replication, retention extension, and lake tiering. | Partially validated — feature set exercised by gate suites on 1.0.0 images; dedicated interface test pending. |
| ASM-IF-005 | `client_order_ref` fits within Arrow's 16-character `remarks` field and is echoed reliably. | **Validated** — RISK-004 closed. Arrow docs confirm max 16 chars, echoed in REST responses and WS order updates. |
| ASM-IF-006 | Fluss connector atomic visibility semantics are per-sink, not cross-sink, for the Signal job → Fluss output interfaces. | RISK-008 |
| ASM-IF-007 | Flink and Fluss do not propagate distributed trace headers without version-specific evidence. Correlation IDs and audit IDs remain mandatory regardless. | REQ-OBS-001 |
| ASM-IF-008 | The pre-production clean break permits replacing incompatible interface contracts. After go-live, interface evolution follows the schema compatibility and rollback contract. | RISK-011 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Partial cross-table visibility:** A single Flink checkpoint commits source offsets and all sinks, but atomic visibility across multiple LOG and KV tables is not assumed. Consumers downstream of the Signal job tolerate partial visibility and reconcile using stable IDs, versions, and the Executor's duplicate guard.
- **At-least-once external calls with reconciliation:** Broker REST calls and postback delivery are at-least-once or uncertain. Unknown outcomes are never automatically retried. Reconciliation is mandatory before release or retry.
- **In-process interfaces are not network contracts:** The Compute → forming-bar handoff is a typed in-job state/event boundary, not a Fluss round trip. It shares the Signal job checkpoint and is not subject to network interface versioning. (**The Business Logic ↔ Ranking interface is REMOVED 2026-08-15, CHG-005.**)
- **Broker REST is not exactly-once:** Arrow REST calls are protected by the durable attempt protocol, gate verification, and post-call reconciliation — not by Flink checkpointing semantics.
- **Trace propagation is capability-gated:** Distributed tracing is used where supported but not required for MVP. Correlation IDs and audit IDs are mandatory on every interface regardless of tracing capability.
- **Protocol values are now confirmed:** Arrow WS binary format, REST contract, and postback WS format are validated from Go SDK and REST API docs. These are no longer hypotheses.
- **Safety-halt is layered, not delegated:** The safety-halt control interface provides an additional safety layer. It does not replace Executor's own pre-call gate, fencing, and health verification.

## Out of Scope

The following capabilities are explicitly NOT owned by the Interface Requirements layer:

- **Actual broker protocol field definitions, packet schemas, and decoder versions:** Now confirmed from Arrow Go SDK (`arrow/streams.go`, `arrow/market.go`) and REST API docs.
- **Arrow REST endpoint, authentication, payload format, and response schema values:** Now confirmed from REST API docs (`orders`, `order-data`). Executor calls Arrow directly; no OpenAlgo.
- **Candle computation, deduplication, event-time watermarking, signal detection:** Owned by the Signal Flink job. (**Ranking logic REMOVED 2026-08-15, CHG-005.**)
- **Broker order execution, gate management, and reconciliation logic:** Owned by the Executor.
- **Postback capture, fill audit, lifecycle projection, and position projection logic:** Owned by Action Capture.
- **Babysitter position monitoring and action emission logic:** Owned by the Babysitter Flink job.
- **EOD controller orchestration and manifest creation:** Owned by the EOD controller.
- **Observability backend, dashboard, and alert configuration:** Owned by the observability layer.
- **Physical DDL generation, application, and schema lifecycle management:** Owned by the schema lifecycle process.

## 5.1 Ingestion → Fluss

- **Protocol:** evidence-approved Fluss Java client
- **Table:** `raw_table_1` LOG
- **Direction:** write
- **Guarantee:** at-least-once
- **Payload:** original packet bytes plus normalized typed fields, hash, decoder/protocol version, fingerprint/version, timestamps, and validity state
- **Failure:** bounded retry under pinned client policy; uncertainty is counted and alerting/readiness is affected

Ingestion never claims raw logical deduplication. Compute performs bounded fingerprint deduplication; the durable dedup set is Fluss-authoritative (DEC-038).

## 5.2 Fluss → Signal Flink job

- **Source:** version-pinned Fluss connector
- **Table:** `raw_table_1`
- **Timestamp:** verified UTC `event_time`
- **State key:** instrument plus fingerprint scope
- **Watermark:** tested bounded out-of-orderness profile

The job filters eligible trades, deduplicates best-effort, emits final candles, and passes forming-bar state in-process to Business Logic. It does not read feature tables back for strategy execution.

## 5.3 Signal job → Fluss

The Signal job writes:

- `feature_candles_15s` final KV upsert rows (PK `(instrument_token, window_start)` — sole candle output, 2026-08-13 conversion)
- `Signal_Candidates` immutable LOG rows
- ~~`Ranking_Results` immutable LOG rows~~ — **REMOVED 2026-08-15 (CHG-005)**
- ~~Immutable `Trade_Decisions` instruction records~~ — **REMOVED 2026-08-15 (CHG-005)**

One checkpoint covers the job, but no cross-table atomic visibility is promised without a connector test. Consumers must tolerate partial visibility and use IDs/reconciliation.

## 5.4 In-operator Business Logic ↔ Ranking — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** There is no in-operator ranking interface in the current system.

## 5.5 Action Capture → audit and projections

Action Capture consumes broker postbacks and writes independently:

1. Immutable `Fills` event/audit.
2. `Order_Lifecycle` KV projection.
3. `Positions` projection for uniquely correlated fill events.
4. `Postback_Quarantine` for missing/ambiguous/invalid events.

These writes are not atomic unless proven. A durable projection/reconciliation worker retries incomplete projections. No postback may infer a mapping from proximity or overloaded IDs.

## 5.6 Signal/Action state → Babysitter

Babysitter consumes the versioned `Positions` changelog. It may also consume lifecycle/audit projections needed for freshness, but it does not own them. MVP emits no actions.

Future actions are immutable `Position_Actions` events with `action_id`, `position_id`, `trade_context_id`, typed action fields, source version, expiry, reason, and configuration version.

## 5.7 Instructions/actions → Executor

Executor consumes ~~immutable `Trade_Decisions`~~ (**REMOVED 2026-08-15, CHG-005**) and, after MVP, `Position_Actions`. For each event it verifies schema/version, identity, expiry, ~~reservation~~ (**REMOVED 2026-08-15, CHG-005**), gate state, and duplicate request hash.

A modified instruction under the same `instruction_id` is a contract violation and causes halt/quarantine. Executor writes only execution-owned state: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, and `Execution_Audit`.

## 5.8 Executor → Arrow REST

- **Endpoint:** `POST /order/regular` (place), `PATCH /order/regular/{id}` (modify), `DELETE /order/regular/{id}` (cancel), `GET /order/{id}` (detail), `GET /user/orders` (order book), `GET /user/trades` (trade book)
- **Auth:** `appID` + `token` headers; token from `/auth/app/authenticate-token` (24hr TTL)
- **Request:** `{exchange, symbol, quantity, transactionType, order, product, price, validity, remarks (≤16 chars), mpp}`
- **Response:** `{status:"success", data:{orderNo, requestTime}}`
- **Order lifecycle (from WS postbacks):** PENDING → OPEN → COMPLETE (filled) / CANCELLED / REJECTED
- **Rate limit:** 10 req/sec
- **Guarantee:** The required semantic contract is: prepare durable attempt before call, verify gate epoch immediately before call, record request hash and `client_order_ref`, treat ambiguous response/timeout/crash window as `UNKNOWN`, halt and reconcile before retry, never claim REST exactly-once from Flink checkpointing. Arrow REST SHALL return enough evidence to correlate broker order identity, acceptance/rejection, and status.

## 5.9 Gate/resume interface

Gate transitions are authenticated control operations recorded in `Execution_Audit`. Resume requires broker/order reconciliation, position/fill reconciliation, changelog continuity, job/checkpoint health, resolution of unknown attempts, and a single-operator (Saurabh, DEC-044) authorized approval for the same gate epoch/evidence hash.

## 5.10 Observability interface

All components emit structured logs, metrics, and traces/correlation IDs to OpenObserve using the supported ingestion contract. Trace propagation is capability-gated; correlation IDs and audit IDs are mandatory even if distributed tracing is unavailable.

## 5.11 Compatibility and acceptance

Every interface has a schema/protocol version and compatibility test. Tests cover malformed input, unknown versions, duplicate delivery, out-of-order events, partial writes, retries, timeout/unknown outcomes, restart, changelog gaps, authorization failure, supersession races, safety-halt propagation, fencing, and rollback. The exact version matrix is owned by Platform and Execution teams and is a live-money release blocker until pinned.

## 5.12 Portfolio ranking and reservation — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** There is no `portfolio_id` ranking/reservation interface in the current system.

## 5.13 Safety-halt control

Authorized components publish durable `Safety_Halt_Requests`. Executor consumes them idempotently, validates scope/source/version, applies or rejects the halt, advances the gate epoch for an applied halt, and writes immutable audit evidence. This interface does not replace Executor's final pre-call gate/fencing/health checks.

## 5.14 Action Capture projection ledger

Action Capture records durable projection progress per `postback_event_id`. Audit, lifecycle, and position writes remain independently recoverable. Restart resumes every non-complete record idempotently; duplicate immutable events cannot create duplicate projection effects.

## 5.15 Timing and visibility vocabulary

Interfaces SHALL distinguish event receipt, append submission, append acknowledgement, reader visibility, checkpoint commit, and Executor receipt. A latency SLO SHALL name the exact pair of events it measures. `commit` alone is not a sufficient boundary definition.
