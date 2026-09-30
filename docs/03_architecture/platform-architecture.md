# Docker Platform Architecture (MVP)

## Purpose and environment boundary

This file describes the Phase 4.2 runtime target. Docker Compose is for local development, integration, and deterministic tests. Production uses a separate Docker Swarm definition (09 v1 4 VMs Manager+Worker → v2 7 VMs Manager-ONLY + Workers, same stack via role labels) and requires HA, security, capacity, and recovery evidence. The Compose topology must never be presented as production proof.

## Local topology

```text
Arrow market-data stream
  → ingestion
  → Fluss raw_table_1 LOG
  → Signal Flink job
      ├─ candles (keyed by instrument_token)
      ├─ candidates (keyed by instrument_token)
      ├─ repartition by portfolio_id

          → Executor durable gate
          → Arrow REST (POST /order/regular)
          → broker

Arrow postback stream
  → action-capture
      ├─ Fills LOG
      ├─ Order_Lifecycle KV
      ├─ Positions KV
      ├─ Postback_Projection_Ledger
      └─ Postback_Quarantine LOG
          → Babysitter Flink job (MVP zero actions)

Safety: Signal/Action Capture/platform/operators → Safety_Halt_Requests → Executor
Fluss immutable events → EOD controller → verified Iceberg/S3
All services → OpenObserve
```

## Required deployables

| Service/deployable | Responsibility |
| --- | --- |
| Fluss coordinator/tablets | Metadata, storage, replication, changelogs, lake tiering |
| Flink control/workers | Run the Signal and Babysitter jobs |
| Ingestion | Evidence-approved market stream decode and raw append |
| Nautilus Execution Service + projection glue | Postback evidence, Nautilus OMS/position/reconciliation, and Fluss execution projections |
| EOD controller | Manifest creation, verification, retry/backoff, retention extension |
| Custom execution control + Nautilus ExecutionClient | Durable gate, attempts, mappings, fencing, safety-halt consumption, and bridge commands |
| OpenObserve | Logs, metrics, traces, alerts |
| Operators/reconciliation control | Authenticated gate reconciliation and single-operator (Saurabh, DEC-044) approval |

The Signal job contains Compute and Business Logic. **(Ranking is REMOVED 2026-08-15, CHG-005 — it was never a service.)** Babysitter is a separate Flink job and a strict no-op in MVP.

## Required logical state

The deployment must provision or reconcile these logical tables before readiness:

- Market: `raw_table_1`, `candle_features`, `suspected_discontinuities`, `instruments` (`feature_candles_15s` + `forming_bar` RETIRED 2026-09-05; `candle_live`/`candle_closed`/`feature_values` RETIRED 2026-09-30, merged into `candle_features` — DEC-059/Wave C W-C5a)
- Strategy: `Signal_Candidates`, `Signal_Candidates_current`, `Execution_Intent` (**`Ranking_Results`, `Trade_Decisions`, `Portfolio_Reservations` REMOVED 2026-08-15, CHG-005**)
- Postback/position: `Fills`, `Order_Lifecycle`, `Positions`, `Postback_Quarantine`, `Postback_Projection_Ledger`
- Execution: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`, `Safety_Halt_Requests`
- EOD: EOD controller durable manifest state
- Future only: `Position_Actions`

Every table has an explicit schema version, owner, retention policy, writer/column ownership, and tested DDL. Startup does not enable order placement merely because a container is healthy.

## Production Swarm topology

Production topology is config-driven (Option B, 09 DECISION 2026-08-20 v1→v2): **v1 (4 VMs)** — three Swarm Manager+Worker VMs host Fluss replicas/quorum and Flink workload capacity with anti-co-location, plus a fourth dedicated observability VM; **v2 (7 VMs)** — three Swarm Manager-ONLY VMs (drained) form the Raft control plane (quorum 2/3) and three+ dedicated Worker VMs host the workloads, same `docker-stack.yml` via role labels (`role=manager/worker`, `flink`, `fluss`), no hostname pin. The observability VM hosts telemetry services and is not required for order-safety correctness.

Production requirements include:

- Immutable image digests and pinned Java/Python/Flink/Fluss/protocol versions
- Three-node Fluss replication/quorum across workload VMs (v1: the Manager+Worker VMs; v2: the Worker VMs) — Fluss data placement is independent of Swarm manager split
- Encrypted S3 checkpoints/savepoints
- Durable replicated Fluss volumes
- Encrypted Iceberg/audit storage
- Swarm secrets and least-privilege identities
- Encrypted overlay/TLS-protected cross-host transport mandatory for all sensitive paths (broker, Arrow REST, S3, operator control, secret delivery, money-moving/state traffic)
- Executor single-owner fencing per `execution_partition_id`
- EOD controller service or scheduled job owning manifest lifecycle and retention extension
- Explicit resource, placement, health, restart, update, rollback, and shutdown policies

## Readiness sequence

Services may start concurrently, but production readiness is dependency-driven:

1. S3 checkpoint/lake access and secrets validate.
2. Fluss quorum, replication, tablets, and required schemas are healthy.
3. Flink control/workers are healthy.
4. EOD controller state is durable and manifests from previous trading dates are verified or retryable.
5. Signal and Babysitter jobs run and checkpoint.
6. Ingestion and Action Capture pass protocol/schema and subscription checks.
7. Executor validates durable state, mappings, changelog continuity, safety-halt health, Arrow REST reachability, and starts `HALTED`.
8. Reconciliation completes.
9. Single authorized operator `saurabh` (DEC-044) approve the same gate epoch/evidence hash before `ENABLED`.

Liveness, readiness, job health, trading readiness, and durability readiness are separate health dimensions.

## Capacity and failure gates

The production-like environment must test:

- variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) for a full session with trigger-tick-to-instruction p99 below 100 ms
- One workload VM loss at the per-instrument production rate
- Data-path recovery under 30 seconds for accepted scenarios
- Safe-halt under five seconds
- Full-volume EOD manifest verification under 30 minutes target
- No duplicate broker orders in crash-window injection

Evidence must report p50/p95/p99, workload, duration, UTC clock source/offset, exact versions, and restart/failure inclusion.

## Run and change controls

Local commands may create ignored `.env` secrets, start Compose, apply reconciled DDLs, and run focused tests. Production changes require a change record, prechecks, gate halted, reconciliation, compatibility proof, rollback criteria, and post-deployment verification.

Schema-breaking clean-break migrations are permitted only before live-money release and require destructive-change approval plus reset/replay evidence. Any uncertain rollback defaults to `HALTED`.

## References

- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Operational requirements: `../02_requirements/06-operational.md`
- Architecture overview: `./00-arch-overview.md`
- Build contracts: `../04_contracts/09-platform-runtime.md`, `../04_contracts/07-executor.md`
