# 01 — System Context and Scope

## 1.1 Purpose

The Streaming Trading Data Platform ingests live market data, computes multi-timeframe event-time candles and strategy signals with Apache Flink, stores streaming events and operational state in Apache Fluss, submits approved immutable instructions through the Executor via Arrow's REST API, captures broker postbacks independently, and preserves eligible history in encrypted Apache Iceberg/S3 storage.

The system has two distinct safety postures:

- **Data path:** may recover automatically and tolerate bounded, explicitly measured data loss or lateness.
- **Order path:** must halt new money-moving calls whenever state, correlation, or broker outcome is uncertain. Resumption requires reconciliation and single-operator (Saurabh, DEC-044) authorization.

## 1.2 Actors and ownership

| Actor/component      | Owns                                                                                                                                                         | Does not own                                                 |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------ |
| Ingestion            | Broker connection, protocol decoding, normalized typed tick placement, original packet preservation, bounded fingerprinting, suspected discontinuity records | Candle computation, strategy, order placement                |
| Fluss storage        | Tables, DDL, distribution, retention, changelog, replication, lake tiering                                                                                   | Strategy or broker calls                                     |
| Signal Flink job     | Computes/operates: dedup, multi-timeframe event-time candles, strategy-host signal detection, candidates — durable candle/signal state is Fluss-authoritative (DEC-038); dedup is operator-local (DEC-054); Flink keeps bounded working + recovery state. **Ranking/Reservations/Decisions REMOVED 2026-08-15 (CHG-005); the 15 s candle/forming-bar path RETIRED 2026-09-05** | Broker REST calls, authoritative fill capture                |
| Action Capture       | Broker postback intake, immutable postback audit, order-lifecycle projection, identity correlation quarantine                                                | Strategy, broker submission                         |
| Babysitter Flink job | Position-management evaluation; no-op in MVP; future structured actions                                                                                      | New entry strategy, lifecycle authority, direct broker calls |
| Executor             | Changelog intake, durable order gate, attempt ledger, ID mapping, reconciliation, controlled execution state, Arrow REST calls                             | Strategy scoring, authoritative fill capture                 |
| Position state       | Correlated fill-derived position aggregate                                                                                                                   | Raw order lifecycle authority                                |
| OpenObserve          | Operational logs, metrics, traces, alert delivery                                                                                                            | Trading decisions                                            |

## 1.3 Topology

```text
Arrow market-data stream
  → Ingestion
  → raw_table_1 LOG
  → Signal Flink job
      ├─ bounded fingerprint deduplication
      ├─ multi-timeframe event-time candle state
      ├─ candidate audit
          → Executor durable order gate
          → Arrow REST
          → broker

Arrow broker postback stream
  → Action Capture
      ├─ immutable postback audit
      ├─ order-lifecycle projection
      └─ correlation quarantine when identity is missing/ambiguous
          → Position projection from correlated fills
              ↕
          Babysitter Flink job (MVP no-op)
              → structured position action
              → Executor gate and Arrow REST

Eligible immutable events → EOD Iceberg/S3
All components → OpenObserve
```

There are exactly two Flink jobs in MVP: the Signal job and the Babysitter job. **(Ranking was never a deployment — it is REMOVED from scope 2026-08-15, CHG-005.)**

## 1.4 Identity model

The following identities SHALL remain distinct:

| Identity               | Created by                              | Purpose                                                                            |
| ---------------------- | --------------------------------------- | ---------------------------------------------------------------------------------- |
| `instruction_id`       | Signal job                              | Immutable platform decision; changed parameters create a new instruction           |
| `client_order_ref`     | Executor                                | Deterministic broker-facing reference; length and echo behavior are evidence-gated |
| `broker_order_id`      | Broker/Arrow REST response                | Broker-authoritative order identity                                                |
| `trade_context_id`     | Signal/position projection              | Groups an entry and its related child orders                                       |
| `position_id`          | Position projection when exposure opens | Stable position aggregate for fills, trim, exit, and re-entry relationships        |
| `execution_attempt_id` | Executor                                | One durable attempt to submit one instruction or structured position action        |

A missing or ambiguous mapping is quarantined and cannot be retried as a new order automatically.

## 1.5 Scope

### MVP in scope

- One broker integration, subject to evidence-gated protocol validation
- Supported NSE and MCX instrument manifest
- Original broker packet preservation and normalized raw tick log
- Bounded best-effort tick fingerprint deduplication
- Event-time 15-second candles with configurable watermark and allowed lateness
- Forming-bar signal detection
- Immutable candidate records **(ranking and instruction records REMOVED 2026-08-15, CHG-005)**
- Durable Executor gate, attempt ledger, identity mapping, reconciliation, and Arrow REST handoff
- Independent postback capture and order-lifecycle projection
- Separate position projection
- Checkpointed Babysitter no-op
- Operational logs, metrics, traces, health checks, and all MVP safety alerts
- EOD Iceberg/S3 offload with verified manifest and retention safety buffer
- Local Docker Compose and production Docker Swarm definitions (09 v1 4 VMs Manager+Worker → v2 7 VMs Manager-ONLY + Workers, same stack)

### Explicit non-goals for MVP

- Multi-broker support
- BSE and currency derivatives
- Strategy authoring, backtesting, P&L, win-rate, or business analytics (ML ranking REMOVED 2026-08-15, CHG-005)
- Real Babysitter actions; they are Phase 4.3+ after the structured action contract is proven
- Kubernetes
- Automatic gap backfill during live ingestion
- Automatic resumption of money-moving calls after uncertain state

## 1.7 Implementation clarification: scope identities

Every attempt, correlation mapping, lifecycle record, position, gate, and audit event SHALL carry the applicable scope: (**Instructions and reservations REMOVED 2026-08-15, CHG-005.**)

- `account_scope_id`: broker/account isolation boundary.
- ~~`portfolio_id`: ranking, reservation, and capacity boundary~~ — **REMOVED 2026-08-15 (CHG-005).**
- `execution_partition_id`: Executor fencing boundary when one account has multiple execution partitions.

A missing or mismatched scope is a contract violation. Scope isolation SHALL be tested so that a halt, mapping, or fence in one scope cannot affect another. (**Reservation-scope clause REMOVED 2026-08-15, CHG-005.**)

## 1.8 Implementation clarification: safety posture

Component health SHALL be separate from trading readiness. A data-path component MAY continue bounded evidence capture during an observability degradation when durable storage and local buffering remain healthy. The Executor SHALL halt new money-moving calls when mandatory execution audit, safety control, fencing, reconciliation, or continuity evidence is unavailable.

## 1.9 Implementation clarification: requirement proof

Every mandatory requirement SHALL link to an acceptance ID, owner, evidence artifact, workload or fixture, and binary threshold. An evidence-gated requirement remains blocked until the exact external behavior and version are proven.

## 1.6 Evidence-gated dependencies

Platform and Execution teams SHALL provide and test:

- Arrow market-data packet corpus and protocol documentation
- Arrow postback fields, ordering, replay, and identity behavior
- Arrow REST request/response/error contract
- Broker-facing `client_order_ref` length and echo behavior
- Exact Flink, Fluss, Java, Python, SDK, and Arrow REST versions
- Version-specific connector semantics for checkpoints, sinks, changelogs, partial updates, replication, and recovery

Until evidence passes, the relevant capability remains blocked for live-money use.
