# Operational Strategy

## Status and authority

This document defines the operating model for the Phase 4.2 platform. It follows:

- `../01_project/02-system-context.md`
- `../01_project/03-quality-targets.md`
- `../02_requirements/06-operational.md`
- `../03_architecture/platform-architecture.md`
- `../04_contracts/09-platform-runtime.md`

The platform is blocked for live-money operation until all evidence-gated release criteria pass. Exact endpoint paths, probe commands, and version-specific CLI syntax are configuration/implementation inputs and must not be invented here. The authoritative health definitions, dashboard panels, alert rules, and thresholds are in [`../08_implementation/10-observability.md`](../08_implementation/10-observability.md).

## Operating environments

| Environment | Runtime | Operating purpose |
| --- | --- | --- |
| Local/development | Docker Compose | Development, debugging, deterministic integration |
| Acceptance | Compose or production-like harness | Contract, workload, failure, recovery, and security tests |
| Production | Four-VM Docker Swarm | Three workload/HA VMs plus one observability VM |

Local commands and single-host health checks are not production HA evidence.

## Startup and readiness

Services may start concurrently. Readiness is dependency-driven, not determined by container order or a listening TCP port.

1. Validate Swarm secrets, encrypted networks, persistent volumes, S3 checkpoint/lake access, and image digests.
2. Verify Fluss coordinator/tablets, quorum, replication, anti-co-location, and required versioned schemas.
3. Verify Flink control/workers and encrypted checkpoint storage.
4. Deploy the Signal and Babysitter Flink jobs from pinned artifacts; confirm running and checkpointing.
5. Verify Ingestion protocol/decoder compatibility, instrument manifest, required subscriptions, append acknowledgements, clock offset, and telemetry.
6. Verify Action Capture postback schema/protocol, correlation dependencies, projection readiness, and telemetry.
7. Start Executor with gate `HALTED`; verify durable execution state, identity mappings, changelog continuity, Arrow REST contract/readiness, fencing, and telemetry.
8. Reconcile broker orders, fills, positions, attempts, and incomplete projections. (**Reservations REMOVED 2026-08-15, CHG-005.**)
9. Require the single-operator (Saurabh, DEC-044) authenticated approval of the same gate epoch/evidence hash before `ENABLED`.

A process may be live while not ready. Startup never automatically enables order placement.

## Health during operation

Use the health dimensions and dashboard/alert definitions in [`../08_implementation/10-observability.md`](../08_implementation/10-observability.md). The Executor may be process-healthy while trading is halted. OpenObserve cannot authorize orders.

## Ownership during operation

| Operational area | Owner | Required behavior |
| --- | --- | --- |
| Market ingress | Ingestion | Preserve raw packets, bounded memory, protocol/quarantine evidence |
| Stream compute | Signal job | Dedup, event-time candles, forming bars (**ranking/instructions REMOVED 2026-08-15, CHG-005**) |
| Postbacks | Action Capture | Preserve evidence, correlate, project lifecycle/positions, quarantine ambiguity |
| Position observation | Babysitter | Consume `Positions`; emit zero actions in MVP |
| Money-moving calls | Executor | Enforce gate, attempts, mappings, reconciliation, fencing |
| Storage durability | Platform/Storage | Quorum, replication, checkpoints, EOD manifest, retention gate |
| Telemetry | Platform/Operations | Metrics, logs, alerts, audit reconstruction |

## Normal operating posture

- Raw ingestion is at-least-once; ingestion does not remove duplicate packets.
- Compute deduplication is bounded best-effort fingerprinting, not exact identity.
- Flink exactly-once claims stop at the pinned tested state/sink boundary.
- Broker REST effects and independent projections require durable idempotency/reconciliation.
- Unknown order outcomes retain capacity, halt the affected order path, and cannot be blindly retried.
- Original packets and postbacks are retained as evidence; secrets and payloads are never logged.
- EOD offload must be verified before expiry; at least three complete trading days remain live.

## Shutdown and maintenance posture

Planned maintenance:

1. Halt new money-moving calls and record gate epoch/reason.
2. Drain or reconcile in-flight attempts and projections. (**Reservations REMOVED 2026-08-15, CHG-005.**)
3. Checkpoint Flink jobs and verify durable state.
4. Drain accepted ingestion/postback work within bounded deadlines.
5. Stop or update services according to the approved change plan.
6. Verify readiness, continuity, replication, storage, and telemetry after restart.
7. Keep gate `HALTED` until reconciliation and single-operator (Saurabh, DEC-044) approval complete.

Forced termination creates an audit event and requires reconciliation before resumption.

## Operating evidence

Operations records UTC timestamps, workload, software/configuration versions, gate transitions, checkpoint IDs, offsets, replication state, projection status, EOD manifests, alert acknowledgements, incident actions, operator approvals, and RPO/RTO per failure scenario. No broader guarantee is claimed than tested.

## References

- Runbooks: `./01-runbooks.md`
- Policy-controlled audit store (R2 bucket locks): `./06-audit-store.md`
- R2 lake archive tiering (raw_table_1 daily partitions, tiering job, lake guard, per-day restore): `./07-lake-archive-ops.md`
- G6 alert routing (O2 → dev-webhook → durable alert-consumer, selftest guard): `./03-alert-routing.md`
- Observability, dashboards, alerts, and thresholds: `../08_implementation/10-observability.md`
- DR: `./04-dr-plan.md`
- Maintenance: `./05-maintenance.md`
