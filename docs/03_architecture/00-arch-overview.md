# Architecture Overview — Streaming Trading Data Platform

## Status and authority

This document describes the target Phase 4.2 architecture. It is derived from:

1. Executable implementation and tests
2. Active decisions in [04-decisions.md](../01_project/04-decisions.md)
3. Reconciled DDLs under [code/01_platform/02_sql/ddl/](../../code/01_platform/02_sql/ddl/)
4. Build contracts under [../04_contracts/](../04_contracts/)
5. Requirements under [../02_requirements/](../02_requirements/)

The architecture is **blocked for live-money use** until the evidence-gated Arrow protocols, exact software versions, Flink/Fluss connector semantics, DDLs, and acceptance tests pass. Unknown external behavior is not represented as an implementation fact.

## System purpose

The platform ingests supported NSE and MCX market data, computes event-time 15-second candles and forming-bar signals in Apache Flink, ranks active setups in the same Signal job, publishes immutable instructions through Apache Fluss, and submits approved instructions through a durable Executor calling Arrow's REST API directly.

It independently captures broker postbacks, builds order-lifecycle and fill-derived position state, runs a checkpointed Babysitter no-op in MVP, and offloads eligible immutable history to encrypted Iceberg/S3 storage.

The platform has two safety postures:

- **Data path:** may recover automatically when correctness state is verifiable and may tolerate only bounded, measured data gaps or lateness.
- **Order path:** halts new money-moving calls whenever state, identity, changelog continuity, or broker outcome is uncertain. Resume requires reconciliation and a single-operator (Saurabh, DEC-044) authenticated approval.

## Scope and non-goals

MVP includes one evidence-approved broker integration, the supported NSE/MCX instrument manifest, raw packet preservation, bounded best-effort fingerprinting, event-time candles, forming-bar detection, immutable candidates, durable execution state, independent postback capture, fill-derived positions, Babysitter no-op wiring, operational observability, and verified EOD offload. **(In-operator ranking and immutable ranking/instruction records REMOVED 2026-08-15, CHG-005.)**

MVP does not include multi-broker support, BSE/currency derivatives, strategy authoring, backtesting, business analytics, Kubernetes, automatic live gap backfill, or automatic order-path resume. Real Babysitter actions are deferred until the structured action contract and safety tests are approved. **(ML ranking REMOVED 2026-08-15, CHG-005.)**

## System at a glance

```text
Arrow market-data WebSocket
  → Ingestion
  → raw_table_1 LOG
  → Signal Flink job
      ├─ bounded fingerprint deduplication
      ├─ event-time 15-second candle state
      ├─ forming-bar detection
      ├─ candidate audit
      ├─ Signal_Candidates LOG
      ├─ Signal_Candidates_current KV
          → immutable execution intent
          → Nautilus Execution Service
              ├─ OMS · risk · portfolio/position engine · reconciliation
              ├─ custom gate · fencing · Fluss projections
              └─ ExecutionClient → localhost go-arrow bridge
                  → Arrow REST (POST /order/regular)
                  → broker

Arrow broker postback stream
  → go-arrow bridge
  → Nautilus Execution Service
      ├─ immutable Fills LOG
      ├─ Order_Lifecycle KV
      ├─ Postback_Projection_Ledger
      ├─ correlation quarantine
      └─ fill-derived Positions KV
          ↕
      Babysitter Flink job (MVP strict no-op)
          → future structured Position_Actions
          → custom gate → Nautilus OMS

Safety controls:
  Signal/capture path/platform health/operators
      → Safety_Halt_Requests
      → custom execution control layer
          → Nautilus gate enforcement (idempotent scoped action)

Eligible immutable events → EOD controller → verified Iceberg/S3 manifest
All components → OpenObserve plus local durable execution audit
```

There are exactly two Flink jobs in MVP: the Signal job and the Babysitter job. **(Ranking was never a deployment — REMOVED 2026-08-15, CHG-005.)**

## Component ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| Ingestion | Broker connection, decode, normalization, packet preservation, fingerprinting, discontinuity evidence | Candles, strategy, broker orders |
| Fluss | Tables, DDL, distribution, replication, retention, changelog, lake tiering | Strategy rules and broker calls |
| Signal Flink job | Computes/operates: dedup, candles, forming bars, candidates — durable dedup/candle/signal state is Fluss-owned (DEC-038); Flink keeps only bounded working + recovery state (**ranking/reservations/instructions REMOVED 2026-08-15, CHG-005**) | Arrow REST calls and authoritative fills |
| Nautilus Execution Service | Authoritative order lifecycle, fills, positions, PnL, reconciliation, risk, and event processing; projects execution state to Fluss | Strategy and market-data ingestion |
| go-arrow bridge | Arrow authentication, order REST, order-update WebSocket, and broker reads | Strategy, gate decisions, and position arithmetic |
| Execution control/projection glue | Fluss intent intake, gate, attempts, correlation, fencing, quarantine, and projections | Independent OMS or position arithmetic |
| Action Capture | Historical name for the capture path now implemented within the Nautilus Execution Service | Strategy and independent order submission |
| Position projector | Historical name for the fill-derived projection now provided by Nautilus and materialized into Fluss | Raw order lifecycle authority |
| Babysitter | Position observation; no-op in MVP; future structured actions | New entry signals, lifecycle authority, direct broker calls |
| Executor | Historical name for the order path now implemented by Nautilus plus custom execution control glue | Strategy, direct Arrow REST calls, independent OMS or position authority |
| EOD controller | Manifest creation, verification, retry/backoff, retention extension, expiry protection. NSE offload after 4 PM IST, MCX after 11:30 PM IST | Broker connection or strategy decisions |
| Arrow REST | Broker order entry and management (`https://edge.arrow.trade`) | Fluss consumption, strategy, fill capture, gate decisions |
| OpenObserve | Operational telemetry and alert delivery | Trading decisions or durable execution evidence |

## Identity and state boundaries

The following identities remain distinct:

- `candidate_id`: detected setup audit record
- `instruction_id`: immutable platform execution request
- `execution_attempt_id`: one submission attempt
- `client_order_ref`: deterministic broker-facing attempt reference
- `broker_order_id`: broker-authoritative order identity
- `trade_context_id`: entry and related position-management chain
- `position_id`: fill-derived exposure aggregate
- `postback_event_id`: platform-captured postback delivery
- `action_id`: future immutable structured position action
- ~~`reservation_id`: portfolio capacity reservation~~ — **REMOVED 2026-08-15 (CHG-005)**
- `halt_request_id`: durable safety-halt request

### Scope identities and isolation

Three canonical scope identities enforce isolation across accounts, portfolios, and execution partitions:

| Scope | Purpose | Carried by |
| --- | --- | --- |
| `account_scope_id` | Broker/account isolation boundary | Gates, attempts, mappings, positions, lifecycle, halt requests, audit |
| ~~`portfolio_id`~~ | ~~Ranking, reservation, and capacity boundary~~ | ~~Reservations, candidate evaluation, instruction context~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `execution_partition_id` | Fenced Executor ownership boundary | Execution gate, fencing token, attempt state |

A missing, mismatched, stale, or unauthorized cross-scope operation fails closed and is audited. Scope isolation tests must prove that a halt, mapping, or fence in one scope cannot affect another. **(Reservation-scope clause REMOVED 2026-08-15, CHG-005.)**

- ~~**MVP (2026-07-23):** `portfolio_id` is singular…~~ — **REMOVED 2026-08-15 (CHG-005).**

`Order_Lifecycle` is keyed by `broker_order_id`. `Positions` is keyed by `position_id`. `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, and `Execution_Audit` are separate Executor-owned state. A generic `order_id` is prohibited across domains.

## Deployment environments

| Environment | Topology | Purpose |
| --- | --- | --- |
| Local/integration | Docker Compose, single host | Development, deterministic tests, component integration |
| Production | Docker Swarm, v1 4 VMs → v2 7 VMs | v1: 3× Manager+Worker + 1 O2 (baseline). v2: 3× Manager ONLY + N≥3 Workers + 1 O2 (drained, same stack). Each VM: 250 GB SSD starting allocation. Final CPU, RAM, SSD IOPS/throughput, and network bandwidth are `EVIDENCE-BLOCKED` until `PERF-PROD-60000-001` and `FAIL-VM-LOSS-60000-001` pass. |

Compose is not evidence of production HA. Production must prove three-node Fluss replication/quorum, anti-co-location (all three replicas of any critical Fluss/Flink role across separate VMs), encrypted S3 checkpoints, one-workload-VM loss tolerance, safe halt under five seconds, and data recovery under thirty seconds at 50,000 ticks/s (3,000 instruments). A cloud provider's uptime claim is not proof of application, broker-route, or order-path availability.

## Architecture references

- Project context and ownership: [02-system-context.md](../01_project/02-system-context.md)
- Quality targets and guarantees: [03-quality-targets.md](../01_project/03-quality-targets.md)
- Active decisions: [04-decisions.md](../01_project/04-decisions.md)
- Functional requirements: [02_requirements/02-functional/](../02_requirements/02-functional/)
- Data and interfaces: [04-data.md](../02_requirements/04-data.md), [05-interfaces.md](../02_requirements/05-interfaces.md)
- Operational requirements: [06-operational.md](../02_requirements/06-operational.md)
- Build contracts: [00-index.md](../04_contracts/00-index.md)
- Detailed Nautilus operating model (ownership, topology, boundary contracts, trade flows,
  position management, migration roadmap, non-goals):
  [05-execution-core.md](../08_implementation/05-execution-core.md#recommended-operating-model)

## Recovery and durability architecture

### RPO per durable boundary

Recovery Point Objective is defined per boundary, not as a single platform claim:

| Boundary | RPO target | Recovery source |
| --- | --- | --- |
| Raw accepted packets | Measured gap between last ack and broker stream position post-reconnect | Ingestion uncertainty counters |
| Immutable instructions | Checkpoint-committed instructions not yet visible to Executor | Signal job checkpoint + Fluss replay |
| Postback audit | Postbacks received but not durably appended | Action Capture pending buffer + broker replay |
| Executor attempts/audit | Durable attempt state not yet reflected in audit LOG | `Execution_Attempts` KV + `Execution_Audit` LOG |
| Projections (lifecycle/positions) | Projection ledger incomplete records | `Postback_Projection_Ledger` + immutable postback audit |
| EOD data | Trading days with unverified or missing manifest | EOD controller manifest state + Fluss source retention |

### Failure timeline model

Every failure test records:

1. Fault injection time
2. Detector threshold and detection time
3. Safety-gate block time (order path)
4. Recovery start time
5. Recovery completion time
6. Source catch-up time (data path)
7. Resume approval time (order path)

Data-path recovery and order-path safe-halt are separate clocks. The five-second safe-halt target applies to the complete fault→gate-block interval including detection delay.

## State capacity architecture and ownership (DEC-038)

**State ownership boundary (2026-08-14):** Fluss is the authoritative durable hot-state layer for the Signal job's large business state; Flink owns only the small working state needed for active processing plus the minimal recovery/checkpoint state needed to restart. The Signal job performs computation; Fluss owns the durable business state; Flink owns small working/recovery state. The Flink checkpoint is intentionally small and is not a second copy of Fluss-owned business state.

Every managed and durable state category must have a defined capacity budget for production readiness. The **checkpoint contribution** column distinguishes Flink-checkpointed state (small) from Fluss-owned durable state (large):

| State category | Cardinality bound | Serialized size/entry | Checkpoint contribution | Owner / cleanup |
| --- | --- | --- | --- | --- |
| Fingerprint dedup | entries = rate × dedup_horizon | ~64 B fingerprint + metadata | **Not a full copy** — Flink keeps only a bounded working cache; the authoritative set is a Fluss KV state table (DEC-038) | Fluss KV (authoritative) + Flink cache; expiry column/cleanup path (no per-key TTL in Fluss 0.9.1 — mechanism must be tested) |
| Candle/forming-bar windows | instruments × (allowed_lateness + window_size) / window_size | Per-instrument window accumulator | Small in-flight accumulator + `emitted` flag; final rows already Fluss KV | Flink (transient) + `feature_candles_15s` KV (durable) |
| Active candidates | configurable max per instrument × instruments | Per-candidate record ~1 KB | Small; output already Fluss LOG/KV | Flink (working) + `Signal_Candidates`/`_current` (durable) |
| ~~Portfolio reservations~~ | ~~max concurrent × portfolios~~ | ~~Per-reservation record ~512 B~~ | ~~Included in Signal checkpoint~~ | **REMOVED 2026-08-15 (CHG-005)** |
| Execution attempts | active + reconciliation window | Per-attempt record ~1 KB | N/A (KV durable state) | Fluss KV durable state |
| Projection ledger | incomplete records | Per-ledger entry ~256 B | N/A (KV durable state) | Fluss KV durable state |
| Postback quarantine | unresolved records | Per-quarantine entry ~2 KB | N/A (LOG durable state) | Fluss LOG durable state |
| Suspected discontinuities | operational investigation window | Per-discontinuity ~512 B | N/A (LOG durable state) | Fluss LOG durable state |

All bounds that depend on external configuration (instruments, portfolios, rate) must be workload-validated at the variable 50,000 ticks/s average baseline with every instrument capped at 20 ticks/s. (The 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045.) State categories without a measured bound are evidence-gated until measurement. The dedup checkpoint contribution above is a target — the actual checkpoint size after externalization must be measured, not asserted (DEC-038).

## Required logical state inventory

The architecture mandates these logical tables before physical DDL generation:

| Table | Type | Writer |
| --- | --- | --- |
| `raw_table_1` | LOG | Ingestion |
| `feature_candles_15s` | KV (PK `instrument_token, window_start` — sole candle output, 2026-08-13) | Signal job |
| `forming_bar` | KV (PK `instrument_token`) | Signal job |
| `Signal_Candidates` | LOG | Signal job |
| `Signal_Candidates_current` | KV | Signal job |
| `Ranking_Results` | ~~LOG~~ | ~~Signal job~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `Trade_Decisions` | ~~Immutable feed~~ | ~~Signal job~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `Portfolio_Reservations` | ~~KV/logical state~~ | ~~Signal job~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `Fills` | LOG | Nautilus Execution Service projection glue |
| `Order_Lifecycle` | KV | Nautilus Execution Service projection glue |
| `Positions` | KV | Nautilus Execution Service projection glue |
| `Postback_Projection_Ledger` | KV | Nautilus Execution Service projection glue |
| `Postback_Quarantine` | LOG | Nautilus Execution Service projection glue |
| `Execution_Gate` | KV | Custom execution control glue |
| `Execution_Attempts` | KV | Custom execution control glue |
| `Order_Correlation` | KV | Custom execution control glue |
| `Execution_Audit` | LOG | Nautilus service + custom control projection |
| `Safety_Halt_Requests` | KV | Authorized components |
| `suspected_discontinuities` | LOG | Ingestion |
| `ingestion_quarantine` | LOG | Ingestion |
| `instruments` | Manifest | Operators |
| `Position_Actions` | Future LOG | Babysitter (post-MVP) |

## Architecture acceptance checklist

Before architecture is considered implementation-ready:

- [ ] All required tables appear in state inventory, pipeline diagrams, and ownership matrices.
- [ ] `Postback_Projection_Ledger` and `Safety_Halt_Requests` appear consistently. (`Portfolio_Reservations` REMOVED 2026-08-15, CHG-005.)
- [ ] `account_scope_id`, `portfolio_id`, and `execution_partition_id` are defined and consistently used.
- [ ] EOD controller has a named owner and durable restart behavior.
- [ ] RPO is defined per boundary; failure timeline includes detection delay.
- [ ] State capacity budgets exist or are evidence-gated for every category.
- [ ] Approved audit-retention controls are described or explicitly evidence-gated; the current minimum target is one year.
- [ ] Transport encryption is mandatory for all sensitive paths.
- [ ] No stale name (separate Ranking job — REMOVED 2026-08-15 CHG-005, generic `order_id`, TLS "where supported") remains.
- [ ] All relative Markdown links resolve.
- [ ] MVP alert groups are owned and have defined response lifecycle.
- [ ] Requirements-to-architecture traceability matrix is current.
