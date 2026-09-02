# 09 — Acceptance Traceability Matrix

## Purpose

This matrix maps every requirement ID to at least one binary acceptance test. Each row cross-references the owning requirement file for detailed criteria. Individual requirement files retain their prose acceptance sections; this matrix provides the centralized implementation checklist.

An evidence-gated requirement (marked `EVIDENCE-BLOCKED`) remains blocked until the exact external behavior and version are proven in the specified test.

## Coverage model

| Coverage type | Meaning |
| --- | --- |
| **Full** | The acceptance test verifies every SHALL statement in the requirement. |
| **Partial** | The test verifies some SHALL statements; uncovered criteria are listed. |
| **Indirect** | The requirement is validated through a parent/cross-domain test. |

Every SHALL statement in every requirement MUST have a specific test or evidence record. A requirement with Partial or Indirect coverage MUST list the uncovered criteria so the gap is explicit.

## Status vocabulary

| Status | Meaning |
| --- | --- |
| `NOT_IMPLEMENTED` | No test exists |
| `EVIDENCE_BLOCKED` | External/runtime facts unproven |
| `FAILED` | Test exists but does not pass |
| `PASSED` | Test passes under declared conditions |
| `SKIPPED` | Deferred with explicit approval |

## Acceptance matrix

### Ingestion

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-ING-001` | REQ-ING-001 | Full | — | Golden packet corpus | Decode matches byte-for-byte; unknown versions quarantined without corruption | `test/ingestion/golden-packets/` report | `PASSED` (2026-08-21) — full fake-broker acceptance `ING-E2E-001` (`FullStackE2ETest`, evidence `logs/ing-e2e-001/`): fake HFT broker → Go bridge → IngestionService → live Fluss; forced disconnect + reconnect epoch appends resume, zero errors; corpus `golden_corpus_test.go` + `go test -count=1` ok; Java `GoldenCorpusPayloadHashTest` 3/3 + `BridgeEventParserTest` 5/5; approved LIVE frames `logs/broker-md-001/marketdata-capture-20260813.jsonl` (BROKER-MD-001) |
| `AC-ING-002` | REQ-ING-001, REQ-ING-002 | Full | — | Unknown protocol version | Readiness false, critical alert, no typed stream entry | `test/ingestion/protocol-version/` report | `EVIDENCE_BLOCKED` — SDK rejects unknown packet types at decode (Go test asserts no emission, PASS 2026-08-21 re-run); Java `IngestionService` quarantines unknown `feed` (`UNKNOWN_VERSION`) proven by `BrokerQuarantineTest` 2/2 PASS (`mvn -o test`, 2026-08-21); remaining: readiness (false)/critical-alert integration E2E (wiring to `/readyz` + alert emission) |
| `AC-ING-003` | REQ-ING-005 | Full | — | Original bytes round-trip | Payload hash matches; lossless | `test/ingestion/round-trip/` report | `PASSED` (2026-08-21) — full-stack Fluss round-trip `ING-E2E-001` PASS on live dev Fluss (`logs/ing-e2e-001/`): DdlBootstrap provisioned `raw_table_1` v2, schema verified (schemaId=1), ticks appended with reconnect-epoch growth, zero errors; `PayloadHashValidatorTest` 11/11 + `GoldenCorpusPayloadHashTest` 3/3; wire-losslessness ING-TCP-001 (3,072 sink rows = 3,072 ticks, 0 lost; `logs/tracker-14/losslessness-validation-20260813.md`) |
| `AC-ING-004` | REQ-ING-005, REQ-ING-006 | Full | — | Approved typed normalization fixtures | Typed columns match reference; timestamps in UTC ms | `test/ingestion/normalization/` report | `NOT_IMPLEMENTED` |
| `AC-ING-005` | REQ-ING-004 | Full | — | Reconnect with subscription | Manifest completeness restored; all instruments re-subscribed | `test/ingestion/reconnect/` report | `PASSED` (2026-08-21) — Go ING-RES-001 100-cycle reconnect + Java fake-broker integration `ING-E2E-001` PASS live: forced disconnect → in-process reconnect → second ACTIVE `subscription_ack` → tick-count grows past pre-reconnect baseline (manifest-loaded instruments re-subscribe; `logs/ing-e2e-001/` + `logs/res001-real-backoff/res001-real-backoff-20260813.out`) |
| `AC-ING-006` | REQ-ING-009 | Full | — | Duplicate packet stream | False-positive and false-negative documented; dedup metric reports both | `test/ingestion/fingerprint/` report | `NOT_IMPLEMENTED` |
| `AC-ING-007` | REQ-ING-012 | Full | — | variable 50,000 ticks/s average baseline (90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045) | Memory below configured bound; bounded backlog; no acknowledged packet loss | `test/ingestion/backpressure/` report | `NOT_IMPLEMENTED` |
| `AC-ING-008` | REQ-ING-012, REQ-PF-009 | Full | — | variable 50,000 ticks/s average baseline for full session (≈16.7 ticks/s/instrument average; 90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045) | Saturation behavior defined; checkpoint stable; recovery bounded | `test/capacity/stress-per-instrument/` report | `NOT_IMPLEMENTED` |
| `AC-ING-009` | REQ-ING-013, REQ-ING-014 | Full | — | Credential rotation, exhaustion, shutdown, clock-skew, observability failure | Readiness transitions correctly; graceful shutdown completes; no silent drop | `test/ingestion/operational/` report | `NOT_IMPLEMENTED` |
| `AC-ING-010` | REQ-ING-003 | Full | — | Connection lifecycle fixtures: reconnect, restart, slot reassignment | `connection_id` stable per principal+slot; `connection_epoch` monotonically increasing; new scope on instrument reassignment | `test/ingestion/connection-identity/` report | `NOT_IMPLEMENTED` |
| `AC-ING-011` | REQ-ING-007 | Full | — | Mixed LTPC/Full packet stream | Trades classified for candle aggregation; quotes/depth preserved but excluded from OHLCV; invalid events rejected | `test/ingestion/tick-classification/` report | `NOT_IMPLEMENTED` |
| `AC-ING-012` | REQ-ING-008 | Full | — | Duplicate fingerprint stream with forced Fluss retry | At-least-once boundary documented; duplicates appended; no silent elimination claimed | `test/ingestion/delivery-semantics/` report | `NOT_IMPLEMENTED` |
| `AC-ING-013` | REQ-ING-010, REQ-ING-011 | Full | — | Connection drop, heartbeat timeout, unknown version, decode failure burst | `suspected_discontinuities` record created per event type; invalid/unknown packets quarantined with reason; readiness false for affected stream | `test/ingestion/discontinuity-quarantine/` report | `NOT_IMPLEMENTED` |
| `AC-ING-014` | REQ-ING-015 | Partial | — | Log format validation; secrets redaction | All required metrics emitted with bounded cardinality; structured logs include identity fields; raw packets and credentials absent from logs | `test/ingestion/telemetry/` report | `NOT_IMPLEMENTED` |
| `AC-ING-015` | REQ-ING-016 | Indirect | — | Covered by AC-ING-001 through AC-ING-014 above | Aggregate acceptance gates proven | `test/ingestion/acceptance-summary/` report | `NOT_IMPLEMENTED` |

### Storage

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-FLS-001` | REQ-FLS-001, REQ-FLS-013 | Full | — | 3-node replication, one VM loss | Quorum maintained; data available; no write loss | `test/storage/replication-one-vm/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-002` | REQ-FLS-008, REQ-FLS-015 | Full | — | Identical instruction_id with changed content | Contract violation quarantined; no mutation applied | `test/storage/immutable-instruction/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-003` | REQ-FLS-009 | Full | — | Partial-update with stale version | Stale update rejected | `test/storage/stale-update/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-004` | REQ-FLS-005, REQ-FLS-011 | Full | — | 3-day live retention; EOD manifest unverified | Source data not expired while manifest unverified; retention extends | `test/storage/retention-safety/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-005` | REQ-FLS-011 | Full | — | EOD manifest verification | Counts, ranges, hashes/checksums match; retry on failure; alert on expiry margin | `test/storage/eod-manifest/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-006` | REQ-FLS-012 | Full | — | Schema migration (additive field) | Consumer compatibility validated; replay passes; rollback plan exercised | `test/storage/schema-migration/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-007` | REQ-FLS-010, REQ-FLS-013 | Full | — | Quarantine rebuild | `Postback_Quarantine` and `suspected_discontinuities` rebuildable from source audit | `test/storage/quarantine-rebuild/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-008` | REQ-FLS-014 | Full | — | Projection ledger recovery | `Postback_Projection_Ledger` resumes incomplete records idempotently after restart | `test/storage/projection-ledger/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-009` | REQ-FLS-016 | Full | — | Cross-scope write attempt | Rejected; audit captured | `test/storage/scope-isolation/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-010` | REQ-FLS-013 | Full | — | Policy-controlled encrypted audit retrieval (one-year minimum) | Record reconstructable from cold storage | `test/storage/audit-retrieval/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-011` | REQ-FLS-003 | Full | — | DDL → requirement parity scan for all 21 tables (plan-time count, 2026-08-10 — now 24) | Every table has DECIDED, EVIDENCE-GATED, or NOT APPLICABLE type status; DDL matches declared type and owner | `test/storage/table-type-decision/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-012` | REQ-FLS-014 | Full | — | ~~`Portfolio_Reservations`~~ + `Safety_Halt_Requests` state lifecycle (**reservation half REMOVED 2026-08-15, CHG-005**) | ~~Reservation transitions legal~~ (**REMOVED 2026-08-15, CHG-005**); halt idempotent and scoped; DDL keys/columns match contract | `test/storage/reservation-halt-state/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-013` | REQ-FLS-002 | Indirect | — | Covered by AC-FLS-011 (DDL parity scan validates LOG/KV/audit class assignment) | Table class assignment matches declared guarantee | `test/storage/table-type-decision/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-014` | REQ-FLS-004 | Full | — | Cross-domain identity fixture: each table with its declared identity fields | No `order_id` in any table; each domain uses correct identity set; schema version present on every table | `test/storage/identity-fields/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-015` | REQ-FLS-006 | Full | — | Normal candle session; CANDLE_WINDOW_MS=15000 | KV upsert output (last-write-wins, no row growth); OHLCV fields match aggregation; no late corrections written; retention 2 calendar days (candle-table exception, user decision 2026-08-16) | `test/storage/candle-log/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-016` | REQ-FLS-007 | Full | — | Candidate event stream (**ranking stream REMOVED 2026-08-15, CHG-005**) | `Signal_Candidates` immutable; ~~`Ranking_Results` and strategy/ranking version fields~~ (**REMOVED 2026-08-15, CHG-005**) | `test/storage/strategy-ranking-audit/` report | `NOT_IMPLEMENTED` |
| `AC-FLS-017` | REQ-FLS-017 | Full | — | DEC-038 state-ownership contract per Fluss-owned Signal table (dedup, candle, forming bar, current signal state) | Each Fluss-owned table defines owner, keys/bucket.key, update semantics, TTL/cleanup mechanism (no per-key TTL in Fluss 0.9.1), rebuild source, versioning, restart/rehydration, and consistency/fail-closed rule; Flink checkpoints are not a duplicate copy of Fluss-owned state | `test/storage/dec038-state-ownership/` report | `NOT_IMPLEMENTED` |

### Compute

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-FC-001` | REQ-FC-004 | Full | — | Out-of-order event-time fixtures | Window assignment correct; late events classified correctly | `test/compute/event-time/` report | `NOT_IMPLEMENTED` |
| `AC-FC-002` | REQ-FC-003 | Full | — | Duplicate and identical-legitimate-event stream | Dedup count correct; false positive/negative documented | `test/compute/dedup/` report | `NOT_IMPLEMENTED` |
| `AC-FC-003` | REQ-FC-006 | Full | — | Empty windows; invalid-event-only windows | No row emitted for empty/invalid windows | `test/compute/empty-window/` report | `NOT_IMPLEMENTED` |
| `AC-FC-004` | REQ-FC-006, REQ-FC-011 | Full | — | Late events after finalization | Discarded and counted; no correction row | `test/compute/final-only/` report | `NOT_IMPLEMENTED` |
| `AC-FC-005` | REQ-FC-005 | Full | — | Identical input snapshot | Deterministic tie ordering; same OHLCV output | `test/compute/deterministic-ties/` report | `NOT_IMPLEMENTED` |
| `AC-FC-006` | REQ-FC-008 | Full | — | Checkpoint restore | No state duplication within tested boundary; dedup/window/forming-bar state restored | `test/compute/checkpoint-restore/` report | `NOT_IMPLEMENTED` |
| `AC-FC-007` | REQ-FC-009, REQ-PF-009 | Full | — | variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) | Backpressure bounded; checkpoint stable | `test/capacity/signal-throughput/` report | `NOT_IMPLEMENTED` |
| `AC-FC-008` | REQ-FC-009 | Full | — | Checkpoint or state continuity uncertain | Order path safely halted per Executor contract | `test/compute/safe-halt/` report | `NOT_IMPLEMENTED` |
| `AC-FC-009` | REQ-FC-004 | Full | — | Source idleness, reconnect, reassignment | Watermark recovers; no stale window finalization | `test/compute/source-idleness/` report | `NOT_IMPLEMENTED` |
| `AC-FC-010` | REQ-FC-012 | Full | — | Dedup TTL shorter than declared horizon | Configuration rejected at deployment | `test/compute/dedup-ttl-reject/` report | `NOT_IMPLEMENTED` |
| `AC-FC-011` | REQ-FC-012 | Full | — | variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) | State growth documented; checkpoint size bounded; cleanup progress observed | `test/compute/state-growth/` report | `NOT_IMPLEMENTED` |
| `AC-FC-012` | REQ-FC-001 | Full | — | Verification scan: no advanced feature columns, no CEP, no current-price inputs | Only 15s OHLCV + forming-bar state in Compute output; `feature_candles_15s` table name fixed; granularity change requires migration | `test/compute/mvp-scope/` report | `NOT_IMPLEMENTED` |
| `AC-FC-013` | REQ-FC-002 | Full | — | Mixed trade/quote stream | Trades+quotes update forming candle OHLC from `last_price_paise` (schema v2 has no bid/ask columns); volume and tick_count increment only on TRADE rows (`last_qty > 0`); invalid rows excluded with reason metric | `test/compute/source-classification/` report | `NOT_IMPLEMENTED` |
| `AC-FC-014` | REQ-FC-007 | Full | — | Typed forming-bar handoff → Business Logic receives event | Forming-bar event contains instrument, window boundaries, current OHLCV, event timestamp, fingerprint; no Fluss round trip | `test/compute/forming-bar-handoff/` report | `NOT_IMPLEMENTED` |
| `AC-FC-015` | REQ-FC-013 | Full | — | Typed closed-candle + forming-bar events delivered in-job | Both event types carry instrument, portfolio_id, window boundaries, schema/config versions, deterministic ordering metadata | `test/compute/closed-candle-handoff/` report | `NOT_IMPLEMENTED` |
| `AC-FC-016` | REQ-FC-010 | Indirect | — | Covered by AC-FC-001 through AC-FC-015 above | All required metrics present; all acceptance criteria proven | `test/compute/acceptance-summary/` report | `NOT_IMPLEMENTED` |

### Business Logic

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-SS-001` | REQ-SS-002 | Full | — | Forming-bar pattern trigger | Correct detection; candidate created with strategy/version | `test/signal/forming-bar/` report | `NOT_IMPLEMENTED` |
| `AC-SS-002` | REQ-SS-002, REQ-SS-007 | Full | — | Identical input snapshot | Same pattern detection result; deterministic replay | `test/signal/deterministic-replay/` report | `NOT_IMPLEMENTED` |
| `AC-SS-003` | REQ-SS-003 | Full | — | All candidate evaluations | Candidate audit record written for every evaluation (selected or rejected) | `test/signal/candidate-audit/` report | `NOT_IMPLEMENTED` |
| `AC-SS-004` | REQ-SS-004 | Full | — | Changed winning parameters | New instruction_id created; prior instruction superseded/cancelled | `test/signal/instruction-lifecycle/` report | `NOT_IMPLEMENTED` |
| `AC-SS-005` | REQ-SS-005, REQ-SS-010 | Full | — | ~~Reservation transition sequence~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Legal transitions accepted; illegal transitions rejected; stale version rejected~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-SS-006` | REQ-SS-009, REQ-RNK-008 | Full | — | Concurrent candidates across instruments | Global capacity limit enforced; no overbooking | `test/signal/global-capacity/` report | `NOT_IMPLEMENTED` |
| `AC-SS-007` | REQ-SS-001 | Full | — | ~~Missing/stale lifecycle or reservation state~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Instruction publication suppressed~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-SS-008` | REQ-SS-001, REQ-FC-008 | Full | — | Signal job restart | Candidate state restored consistently (**reservation/instruction state REMOVED 2026-08-15, CHG-005**) | `test/signal/restart-restore/` report | `PASSED (2026-09-02)` — proven live by the C2 TM-kill-at-full-load drill `tm-kill-full-load-20260902-121511` at 10,240 ticks/s: checkpoint restore + candidate/tentative state survived the kill; every in-run tentative settled (F4: 0 orphans) and candle parity was exact (G7c 33,792/0); EarlySignalFunctionTest 18/18 pins the semantics (checkpoint-restore survival, marker reconciliation). Evidence: drill dir + CHG-121. Prior `NOT_IMPLEMENTED` superseded. |
| `AC-SS-009` | REQ-SS-010 | Full | — | Replacement instruction with unresolved predecessor | Executor rejects/holds replacement; predecessor disposal required first | `test/signal/supersession/` report | `NOT_IMPLEMENTED` |
| `AC-SS-010` | REQ-SS-011 | Full | — | Max active candidates exceeded | Oldest/invalid candidates cleaned up; new evaluation bounded | `test/signal/candidate-bounds/` report | `NOT_IMPLEMENTED` |
| `AC-SS-011` | REQ-SS-006 | Full | — | ~~Code + topology verification scan~~ (**REMOVED 2026-08-15, CHG-005 — ranking out of scope**) | ~~No Fluss source on `Signal_Candidates` in ranking code; no separate ranking deployment~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-SS-012` | REQ-SS-008 | Indirect | — | Covered by AC-SS-001 through AC-SS-011 above | All acceptance criteria: patterns, replay, audit, lifecycle, restore, bounds (**reservation/capacity REMOVED 2026-08-15, CHG-005**) | `test/signal/acceptance-summary/` report | `NOT_IMPLEMENTED` |

### Ranking

> **REMOVED 2026-08-15 (CHG-005, not deferred):** ranking is out of scope; the AC-RNK rows below are retained for cross-reference with REMOVED annotations on each row.

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-RNK-001` | REQ-RNK-002 | Full | — | ~~Null/NaN/non-finite score input~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Candidate rejected with reason~~ (**REMOVED 2026-08-15, CHG-005 — ranking/decisions out of scope, not deferred**) | — | `NOT_IMPLEMENTED` |
| `AC-RNK-002` | REQ-RNK-003 | Full | — | ~~Identical composite score~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Deterministic tie-break~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-RNK-003` | REQ-RNK-004 | Full | — | ~~Same winner, unchanged parameters~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Audit-only reevaluation~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-RNK-004` | REQ-RNK-004 | Full | — | ~~Different winner~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Reservation transition~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-RNK-005` | REQ-RNK-005, REQ-SS-010 | Full | — | ~~Stale reservation state~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Instruction publication suppressed~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-RNK-006` | REQ-RNK-006 | Full | — | ~~variable 50,000 ticks/s baseline~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Decision p99 <100 ms~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-RNK-007` | REQ-RNK-009 | Full | — | ~~Every evaluation~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Ranking evidence record~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-RNK-008` | REQ-RNK-001 | Full | — | ~~No separate ranking deployment~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Verified within Signal job~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |
| `AC-RNK-009` | REQ-RNK-007, REQ-RNK-008 | Full | — | ~~Repartition + serialization verification~~ (**REMOVED 2026-08-15, CHG-005**) | ~~Candidates repartitioned by portfolio_id~~ (**REMOVED 2026-08-15, CHG-005**) | — | `NOT_IMPLEMENTED` |

### Action Capture

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-AC-001` | REQ-AC-002 | Full | — | Duplicate postback delivery | Duplicate logged; logical projection idempotent | `test/action-capture/duplicate/` report | `NOT_IMPLEMENTED` |
| `AC-AC-002` | REQ-AC-002, REQ-AC-013 | Full | — | Out-of-order postbacks without broker sequence | State precedence enforced; no regression of terminal state | `test/action-capture/out-of-order/` report | `NOT_IMPLEMENTED` |
| `AC-AC-003` | REQ-AC-003 | Full | — | Missing broker_order_id and client_order_ref | Quarantined; never inferred from symbol/quantity/proximity | `test/action-capture/missing-ids/` report | `NOT_IMPLEMENTED` |
| `AC-AC-004` | REQ-AC-005 | Full | — | Terminal-state regression attempt (CANCELLED → FILLED) | Rejected; order moved to UNKNOWN; alert raised | `test/action-capture/terminal-regression/` report | `NOT_IMPLEMENTED` |
| `AC-AC-005` | REQ-AC-006 | Full | — | Crash between audit append and lifecycle projection | Restart resumes; projection completed idempotently | `test/action-capture/independent-write-crash/` report | `NOT_IMPLEMENTED` |
| `AC-AC-006` | REQ-AC-011 | Full | — | Restart with incomplete ledger records | All non-complete records resumed idempotently | `test/action-capture/projection-recovery/` report | `NOT_IMPLEMENTED` |
| `AC-AC-007` | REQ-AC-008 | Full | — | Rejected broker order | Terminal lifecycle state (**reservation-release clause REMOVED 2026-08-15, CHG-005**) | `test/action-capture/rejected-order/` report | `NOT_IMPLEMENTED` |
| `AC-AC-008` | REQ-AC-007 | Full | — | First correlated fill | position_id minted; position state OPEN | `test/action-capture/position-creation/` report | `NOT_IMPLEMENTED` |
| `AC-AC-009` | REQ-AC-007 | Full | — | Partial fills on same position | Cumulative quantity correct; average fill price correct | `test/action-capture/partial-fills/` report | `NOT_IMPLEMENTED` |
| `AC-AC-010` | REQ-AC-007 | Full | — | Multi-order trade_context | trade_context_id groups correctly across orders | `test/action-capture/trade-context/` report | `NOT_IMPLEMENTED` |
| `AC-AC-011` | REQ-AC-012 | Full | — | Position closure and re-entry | Closed position FLAT; re-entry creates new position_id | `test/action-capture/reentry/` report | `NOT_IMPLEMENTED` |
| `AC-AC-012` | REQ-AC-012 | Full | — | Correction/bust fill | Position arithmetic corrected; audit trail complete | `test/action-capture/correction/` report | `NOT_IMPLEMENTED` |
| `AC-AC-013` | REQ-AC-013 | Full | — | Policy-controlled audit reconstruction (one-year minimum) | All postback, lifecycle, and position state reconstructable from immutable audit | `test/action-capture/audit-reconstruction/` report | `NOT_IMPLEMENTED` |
| `AC-AC-014` | REQ-AC-001 | Full | — | Evidence artifacts: endpoint, auth, payload schema, status vocabulary verified; unknown versions quarantined | Original bytes + payload hash retained; decoder/schema version recorded; no guessed fields | `test/action-capture/evidence-gated-intake/` report | `EVIDENCE_BLOCKED` |
| `AC-AC-015` | REQ-AC-004 | Full | — | Postback delivery → Fills append with all required identity, correlation, payload, and provenance fields | One immutable row per delivery; no in-place mutation; quarantine reason on uncorrelated events | `test/action-capture/immutable-fills/` report | `NOT_IMPLEMENTED` |
| `AC-AC-016` | REQ-AC-009 | Full | — | Fluss unavailable; schema mismatch; projection backlog above policy | Readiness false on each condition; memory bounded; pending writes bounded | `test/action-capture/backpressure-readiness/` report | `NOT_IMPLEMENTED` |
| `AC-AC-017` | REQ-AC-010 | Indirect | — | Covered by AC-AC-001 through AC-AC-016 above | All required metrics present; all acceptance criteria: duplicates, out-of-order, missing IDs, terminal regression, crash recovery, projection rebuild, position lifecycle, multi-order context, reentry, corrections, audit retrieval | `test/action-capture/acceptance-summary/` report | `NOT_IMPLEMENTED` |

### Executor

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-EXE-001` | REQ-EXE-005 | Full | — | Crash before broker call | No broker order submitted; attempt state PREPARED | `test/executor/crash-before-call/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-002` | REQ-EXE-005 | Full | — | Crash after broker acceptance, before durable ack | No duplicate broker order; attempt state UNKNOWN; gate halted | `test/executor/crash-after-accept/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-003` | REQ-EXE-005 | Full | — | Timeout/ambiguous broker response | Mark UNKNOWN; halt within 5 seconds; no auto retry | `test/executor/unknown-outcome/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-004` | REQ-EXE-001, REQ-EXE-002 | Full | — | Restart with missing/corrupt state | Defaults to HALTED | `test/executor/restart-corrupt/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-005` | REQ-EXE-003 | Full | — | Single-operator (Saurabh, DEC-044) approval | Same gate epoch + evidence hash required; authenticated operator identity (saurabh); a second approval is not required and not checked; automatic resume rejected | `test/executor/single-operator-resume/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-006` | REQ-EXE-008, REQ-EXE-012 | Full | — | Two concurrent Executor instances | One submits; other blocked by fencing; no dual submission | `test/executor/concurrent-fencing/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-007` | REQ-EXE-011 | Full | — | Duplicate safety-halt request | Idempotent; applied once; stale request rejected and audited | `test/executor/safety-halt-idempotent/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-008` | REQ-EXE-011 | Full | — | Cross-scope halt request | Rejected; audit captured | `test/executor/safety-halt-scope/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-009` | REQ-EXE-006 | Full | — | Missing correlation | Quarantined; halts affected flow; no inferred mapping | `test/executor/correlation-quarantine/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-010` | REQ-EXE-004 | Full | — | Modified instruction row under existing instruction_id | Contract violation; halt, quarantine, alert | `test/executor/instruction-mutation/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-011` | REQ-EXE-005 | Full | — | Broker rejection | Terminal failure (**reservation-release clause REMOVED 2026-08-15, CHG-005**) | `test/executor/broker-rejection/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-012` | REQ-EXE-013 | Full | — | Arrow REST reconciliation query | List recent orders, fills, positions with defined consistency delay | `test/executor/reconciliation-capability/` report | `EVIDENCE_BLOCKED` |
| `AC-EXE-013` | REQ-EXE-010 | Full | — | Policy-controlled audit reconstruction (one-year minimum) | Every money-moving call reconstructable from immutable audit | `test/executor/audit-reconstruction/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-014` | REQ-EXE-010 | Full | — | NotImplementedError scaffold replaced | All gates pass | Code review + test suite | `NOT_IMPLEMENTED` |
| `AC-EXE-015` | REQ-EXE-007 | Indirect | — | Covered by AC-EXE-014 (MVP no-op; future action gate passes through same attempt/fencing/reconciliation protocol as entry) | — | `test/executor/scaffold/` report | `NOT_IMPLEMENTED` |
| `AC-EXE-016` | REQ-EXE-009 | Partial | Arrow REST latency/status, consumer lag, security events | — | Gate state/epoch, halt latency, attempts by phase/outcome, unknown outcomes, duplicate suppressions, reconciliation duration, mapping/quarantine, approval events | `test/executor/health-observability/` report | `NOT_IMPLEMENTED` |

### Babysitter

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-BB-001` | REQ-BB-001 | Full | — | Two-job deployment | Signal and Babysitter jobs both running; separate checkpoints | `test/babysitter/two-job-topology/` report | `NOT_IMPLEMENTED` |
| `AC-BB-002` | REQ-BB-003 | Full | — | All fixtures | Zero position actions emitted in MVP | `test/babysitter/zero-action/` report | `NOT_IMPLEMENTED` |
| `AC-BB-003` | REQ-BB-002 | Full | — | Valid and invalid Position changelog | Schema/version validated; continuity verified | `test/babysitter/input-handling/` report | `NOT_IMPLEMENTED` |
| `AC-BB-004` | REQ-BB-006 | Full | — | Checkpoint restore | Source offset and observation state restored; no duplicate actions | `test/babysitter/checkpoint-restore/` report | `NOT_IMPLEMENTED` |
| `AC-BB-005` | REQ-BB-006 | Full | — | Changelog discontinuity | Becomes not ready; no action emitted | `test/babysitter/discontinuity/` report | `NOT_IMPLEMENTED` |
| `AC-BB-006` | REQ-BB-005 | Full | — | Stale position state | Action creation suppressed; alert emitted | `test/babysitter/stale-state/` report | `NOT_IMPLEMENTED` |
| `AC-BB-007` | REQ-BB-007 | Full | — | Backpressure | Bounded under workload; consumer lag within limits | `test/babysitter/backpressure/` report | `NOT_IMPLEMENTED` |
| `AC-BB-008` | REQ-BB-008 | Indirect | — | Covered by AC-BB-001 through AC-BB-007 above | Two-job deployment, input handling, restore, discontinuity, stale state, backpressure, zero actions | `test/babysitter/acceptance-summary/` report | `NOT_IMPLEMENTED` |
| `AC-BB-009` | REQ-BB-004 | Indirect | — | Covered by AC-BB-002 (MVP zero-action test validates deferred state; future action contract gated on structured action approval) | — | `test/babysitter/future-action-gate/` report | `NOT_IMPLEMENTED` |

### Observability

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-OBS-001` | REQ-OBS-001 | Full | — | All components | Structured JSON logs emitted; secrets/raw payloads redacted | `test/observability/log-format/` report | `NOT_IMPLEMENTED` |
| `AC-OBS-002` | REQ-OBS-002 | Full | — | Metric cardinality bounds | High-cardinality labels bounded; no unbounded per-instrument metric explosion | `test/observability/metric-cardinality/` report | `NOT_IMPLEMENTED` |
| `AC-OBS-003` | REQ-OBS-003 | Full | — | Each critical alert condition | Alert fires, acknowledged, escalation path exercised | `test/observability/alert-delivery/` report | `NOT_IMPLEMENTED` |
| `AC-OBS-004` | REQ-OBS-008 | Full | — | OpenObserve outage | Ingestion/Action Capture continue bounded capture; Executor halts new calls | `test/observability/backend-outage/` report | `NOT_IMPLEMENTED` |
| `AC-OBS-005` | REQ-OBS-004 | Full | — | All health dimensions | Liveness, readiness, job health, trading readiness distinguished | `test/observability/health-states/` report | `NOT_IMPLEMENTED` |
| `AC-OBS-006` | REQ-OBS-003 | Full | — | Unauthorized gate resume attempt | Alert fires; rejected; audited | `test/observability/unauthorized-resume-alert/` report | `NOT_IMPLEMENTED` |
| `AC-OBS-007` | REQ-FLS-011 | Full | — | EOD manifest verification failure | Critical storage alert fires; retention extended | `test/observability/eod-expiry-alert/` report | `NOT_IMPLEMENTED` |
| `AC-OBS-008` | REQ-OBS-007 | Full | — | Live-money acceptance gate reconstruction | Every gate provable from observability/audit evidence | `test/observability/gate-reconstruction/` report | `NOT_IMPLEMENTED` |
| `AC-OBS-009` | REQ-OBS-005 | Full | — | Each SLO metric at variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) | p50/p95/p99 reported with unit, UTC clock source, workload, duration, versions; decision latency excludes broker REST | `test/observability/slo-measurement/` report | `NOT_IMPLEMENTED` |
| `AC-OBS-010` | REQ-OBS-006 | Full | — | Access control verification on audit endpoints | Money-moving logs, gate, attempt, mapping, postback, reconciliation, approval data immutable and role-restricted; access logged | `test/observability/audit-access/` report | `NOT_IMPLEMENTED` |

### Platform / Runtime

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-PF-001` | REQ-PF-009 | Full | — | variable 50,000 ticks/s average baseline full session (≈16.7 ticks/s/instrument average; 90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045) | Decision p99 <100 ms | `test/platform/perf-per-instrument/` report | `NOT_IMPLEMENTED` |
| `AC-PF-002` | REQ-PF-009 | Full | — | variable 50,000 ticks/s average baseline for ≥30 min (≈16.7 ticks/s/instrument average; 90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045) | Bounded backlog; no acknowledged loss | `test/platform/perf-sustained/` report | `NOT_IMPLEMENTED` |
| `AC-PF-003` | REQ-PF-009 | Full | — | variable 50,000 ticks/s average baseline for full session (≈16.7 ticks/s/instrument average; 90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045) | Bounded saturation; checkpoint stable; recovery within threshold | `test/platform/perf-full-session/` report | `NOT_IMPLEMENTED` |
| `AC-PF-004` | REQ-PF-002, REQ-PF-011 | Full | — | One workload VM loss at variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) | Data recovery <30s; safe halt <5s; sustained workload post-recovery | `test/platform/one-vm-loss/` report | `NOT_IMPLEMENTED` |
| `AC-PF-005` | REQ-PF-009 | Full | — | Crash-window injection | No duplicate broker order | `test/platform/crash-window/` report | `NOT_IMPLEMENTED` |
| `AC-PF-006` | REQ-PF-009 | Full | — | EOD full-volume manifest | Verification <30 min target | `test/platform/eod-manifest-perf/` report | `NOT_IMPLEMENTED` |
| `AC-PF-007` | REQ-PF-010 | Full | — | Network exposure scan | No public Fluss tablet/RPC ports; only authorized endpoints exposed | `test/platform/security-exposure/` report | `NOT_IMPLEMENTED` |
| `AC-PF-008` | REQ-PF-010, REQ-PF-012 | Full | — | All sensitive traffic paths | TLS or equivalent authenticated encrypted transport active | `test/platform/security-tls/` report | `NOT_IMPLEMENTED` |
| `AC-PF-009` | REQ-PF-004 | Full | — | Secret rotation (broker, Arrow REST, S3, OpenObserve) | Expired credentials cause readiness degradation/alert; not silent failure | `test/platform/secret-rotation/` report | `NOT_IMPLEMENTED` |
| `AC-PF-010` | REQ-PF-004 | Full | — | Least-privilege identity | Each service accesses only authorized resources | `test/platform/least-privilege/` report | `NOT_IMPLEMENTED` |
| `AC-PF-011` | REQ-PF-010 | Full | — | Unauthorized gate/resume attempt | Rejected, audited, alerted | `test/platform/unauthorized-gate/` report | `NOT_IMPLEMENTED` |
| `AC-PF-012` | REQ-PF-005 | Full | — | Encrypted storage for Fluss volumes, S3 checkpoints, lake/audit, Executor state | At-rest encryption verified for each | `test/platform/storage-encryption/` report | `NOT_IMPLEMENTED` |
| `AC-PF-013` | REQ-PF-010 | Full | — | Image pinning, SBOM, vulnerability scan | No `latest` tags; SBOM present; vulnerability policy met | `test/platform/image-compliance/` report | `NOT_IMPLEMENTED` |
| `AC-PF-014` | REQ-PF-010 | Full | — | Compromised credential recovery | Rotation completes; affected flow halted; reconciliation required | `test/platform/credential-compromise/` report | `NOT_IMPLEMENTED` |
| `AC-PF-015` | REQ-PF-001 | Full | — | Production manifest scan | Exact image digests/versions pinned for Fluss, Flink, Arrow REST, OpenObserve, all project services; `latest` absent; version ranges absent | `test/platform/exact-versions/` report | `NOT_IMPLEMENTED` |
| `AC-PF-016` | REQ-PF-003 | Full | — | Network isolation scan | Compose: isolated bridge only; Production: encrypted overlay, only authorized operator/UI/API ingress; Fluss tablet/RPC/checkpoint/service ports not publicly exposed | `test/platform/networking/` report | `NOT_IMPLEMENTED` |
| `AC-PF-017` | REQ-PF-006 | Full | — | Service inventory scan | Fluss coordinator+tablets, Flink control+workers, Ingestion, Signal+Babysitter submitter, Action Capture, Executor, OpenObserve all deployed; Executor fencing active | `test/platform/service-topology/` report | `NOT_IMPLEMENTED` |
| `AC-PF-018` | REQ-PF-007 | Partial | Startup dependency declarations alone not sufficient | — | Schemas present; Fluss quorum healthy; Signal/Babysitter RUNNING+checkpointing; Executor durable; contracts/credentials valid; changelog + observability healthy before ENABLED | `test/platform/startup-readiness/` report | `NOT_IMPLEMENTED` |
| `AC-PF-019` | REQ-PF-008 | Partial | Money-moving rollback behavior; schema-breaking migration approval | — | Rolling/canary only with proven compatibility; gate halted before money-moving change; rollback preserves readability and defaults to halted | `test/platform/deployment-rollback/` report | `NOT_IMPLEMENTED` |

### Non-functional

| Acceptance ID | Requirement | Coverage type | Uncovered criteria | Fixture / Workload | Threshold | Evidence Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AC-NFR-001` | NFR-PERF-002 | Full | — | Broker packet receive → raw append ack | p99 <50 ms target (evidence-gated) | `test/nfr/ingestion-latency/` report | `EVIDENCE_BLOCKED` |
| `AC-NFR-002` | NFR-PERF-002 | Full | — | Trigger tick → instruction commit | p99 <100 ms at variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) | `test/nfr/decision-latency/` report | `NOT_IMPLEMENTED` |
| `AC-NFR-003` | NFR-PERF-002 | Full | — | Failure detection → data processing resumed | <30 s | `test/nfr/recovery-time/` report | `NOT_IMPLEMENTED` |
| `AC-NFR-004` | NFR-PERF-002 | Full | — | Uncertainty detection → gate blocks new calls | <5 s | `test/nfr/safe-halt-time/` report | `NOT_IMPLEMENTED` |
| `AC-NFR-005` | NFR 3.4.1 | Full | — | Object-lock enforcement on test prefix (R2 bucket locks — indefinite rule via Cloudflare API; the S3 Object Lock API is not implemented on R2, 2026-08-14) | Write prevented after lock; delete rejected | `test/nfr/audit-worm/` report | `EVIDENCE_BLOCKED` (mechanism pinned 2026-08-14: R2 bucket locks; needs a Cloudflare API token to apply/verify) |
| `AC-NFR-006` | NFR 3.4.1 | Full | — | Legal hold freeze/release cycle | All versions preserved during hold; release restores normal lifecycle | `test/nfr/audit-legal-hold/` report | `EVIDENCE_BLOCKED` |
| `AC-NFR-007` | NFR 3.4.1 | Full | — | Key rotation without audit loss | Old keys decrypt existing data; new keys encrypt future audit | `test/nfr/audit-key-rotation/` report | `EVIDENCE_BLOCKED` |
| `AC-NFR-008` | NFR 3.4.1 | Full | — | Retrieval SLA measurement | Single record reconstructable <15 min from cold storage | `test/nfr/audit-retrieval-sla/` report | `EVIDENCE_BLOCKED` |
| `AC-NFR-009` | NFR 3.4.1 | Full | — | Export-and-reconstruct integrity | Hash chain verifiable end-to-end | `test/nfr/audit-reconstruction-integrity/` report | `EVIDENCE_BLOCKED` |
| `AC-NFR-010` | NFR 3.4.1 | Full | — | Unauthorized deletion attempt | Rejected; immutable evidence event created | `test/nfr/audit-deletion-rejection/` report | `EVIDENCE_BLOCKED` |
| `AC-NFR-011` | NFR 3.10 | Full | — | Each failure scenario | RPO reported per boundary; fault/detect/block/recovery times recorded | `test/nfr/rpo-boundaries/` report | `NOT_IMPLEMENTED` |
| `AC-NFR-012` | NFR 3.11 | Full | — | Each state item | Cardinality bound, serialized size, checkpoint contribution, restore size/time documented | `test/nfr/state-budgets/` report | `NOT_IMPLEMENTED` |

## Coverage summary

132 requirements are covered by 152 acceptance tests. Some tests intentionally cover multiple requirements; no requirement may remain unmapped.

| Domain | Requirements | Acceptance IDs | Ratio |
| --- | ---: | ---: | --- |
| Ingestion | 16 | 15 | 1:0.9 |
| Storage | 17 | 17 | 1:1.0 |
| Compute | 13 | 16 | 1:1.2 |
| Business Logic | 11 | 12 | 1:1.1 |
| Ranking | 9 | 9 | 1:1.0 |
| Action Capture | 13 | 17 | 1:1.3 |
| Executor | 13 | 16 | 1:1.2 |
| Babysitter | 8 | 9 | 1:1.1 |
| Observability | 8 | 10 | 1:1.3 |
| Platform | 12 | 19 | 1:1.6 |
| Non-functional | 12 | 12 | 1:1.0 |
| **Total** | **132** | **152** | **1:1.1** |

## Summary

| Domain | Total ACs | Passed | Failed | Evidence Blocked | Not Implemented |
| --- | --- | --- | --- | --- | --- |
| Ingestion | 15 | 0 | 0 | 1 | 11 |
| Storage | 17 | 0 | 0 | 0 | 17 |
| Compute | 16 | 0 | 0 | 0 | 16 |
| Business Logic | 12 | 0 | 0 | 0 | 12 |
| Ranking | 9 | 0 | 0 | 0 | 9 |
| Action Capture | 17 | 0 | 0 | 1 | 16 |
| Executor | 16 | 0 | 0 | 1 | 15 |
| Babysitter | 9 | 0 | 0 | 0 | 9 |
| Observability | 10 | 0 | 0 | 0 | 10 |
| Platform | 19 | 0 | 0 | 0 | 19 |
| Non-functional | 12 | 0 | 0 | 7 | 5 |
| **Total** | **152** | **0** | **0** | **10** | **139** |

---

## Traceability rule

When a requirement changes, the corresponding row SHALL be reconciled. No acceptance ID may be removed without deprecation evidence. New requirements SHALL add rows to this matrix before being marked implementation-ready.

Partial or Indirect coverage rows SHALL list the uncovered criteria explicitly. Every SHALL statement in every requirement MUST resolve to a specific test or evidence record. A requirement that is fully covered by another domain's test (e.g., a storage requirement validated by an executor test) SHALL use `Indirect` coverage and reference the exact covering acceptance ID.
