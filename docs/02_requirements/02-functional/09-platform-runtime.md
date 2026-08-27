# 02.9 — Platform and Runtime

## Environments

| Environment       | Orchestrator   | Purpose                                                                 |
| ----------------- | -------------- | ----------------------------------------------------------------------- |
| Local/integration | Docker Compose | Single-host development, component integration, and deterministic tests |
| Production        | Docker Swarm   | v1: 4 VMs (3× Manager+Worker + 1 O2); v2: 7 VMs (3× Manager ONLY + N≥3 Workers + 1 O2, same stack) |

Compose behavior SHALL not be presented as proof of production HA.

## Constraints

- Production manifests SHALL pin exact immutable image versions/digests. `latest`, version ranges, and unpinned tags are prohibited.
- Production SHALL use Docker Swarm secrets with least-privilege service identities. Secrets SHALL NOT be embedded in images, stack files, logs, command lines, or environment dumps.
- TLS or an equivalent authenticated encrypted transport SHALL be mandatory for broker, Arrow REST, S3, operator control, secret delivery, and cross-host money-moving/state traffic. "Where supported" is not sufficient.
- Flink checkpoints/savepoints SHALL use encrypted, versioned S3 storage. Local volumes are not production checkpoint durability.
- Fluss data volumes SHALL be durable per workload node with tested replication/recovery. Three-node replication/quorum SHALL prevent replica co-location on a single VM.
- Money-moving audit records SHALL be encrypted at rest in the lake tier and retained for at least one year or longer under the approved data-retention policy.
- Only explicitly required operator/UI/API endpoints SHALL be exposed through controlled ingress/firewall rules. Internal RPC, checkpoint, and service ports SHALL NOT be publicly exposed.
- The observability VM SHALL NOT be required for order-safety correctness. Loss of the observability VM SHALL NOT authorize orders or erase execution audit.
- Schema-breaking clean-break migrations are allowed only before live-money release and require destructive-change approval and reset/replay plan.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-PF-001 | The production Swarm (v1: 4 VMs Manager+Worker; v2: 7 VMs with N≥3 Workers, load on the Worker VMs) can sustain the normal variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) while one HA VM is unavailable, with manager capacity isolated from workload contention. | ASM-005, RISK-010 |
| ASM-PF-002 | Docker Swarm secrets, encrypted overlay/TLS, S3 checkpoints, role-label placement, and three-node Fluss placement can be operated on the production Swarm (v1 4 VMs; v2 7 VMs) without changing the 3-manager Raft quorum when workers are added. | ASM-009 |
| ASM-PF-003 | S3 `ap-south-1` can complete verified EOD offload of a full trading day within 30 minutes. | ASM-006 |
| ASM-PF-004 | Loss of any one workload VM at normal load is detected and the safe-halt completes within five seconds. | REQ-PF-002, RISK-003 |
| ASM-PF-005 | Fluss data recovery under 30 seconds after a clean VM restart is achievable at the normal workload. | REQ-PF-002 |
| ASM-PF-006 | Docker Swarm provides a fencing/leadership mechanism sufficient for single-active-owner enforcement per `execution_partition_id`. | ASM-005, REQ-EXE-008, REQ-PF-006 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Compose is not production HA:** Local Docker Compose proves component integration and deterministic tests. It does not prove production replication, quorum, TLS, secret management, or VM-loss tolerance.
- **Observability VM is not critical-path:** The observability VM hosts OpenObserve. Its loss does not halt orders or erase audit. However, Executor independently halts when mandatory execution audit delivery is unavailable.
- **New order placement is disabled at startup:** Infrastructure services may start concurrently, but new orders wait for the full readiness chain: schemas, replication, running/checkpointing jobs, explicitly enabled gate, valid broker credentials, and healthy observability.
- **Rollback defaults to halted:** Money-moving changes begin with the gate halted, reconciliation complete, and controlled enablement. Rollback preserves schema/state readability and defaults to halted when uncertain.
- **Pre-production clean break:** Schema-breaking migrations are permitted before live money. After go-live, schema changes follow the compatibility, replay, and rollback contract.

## Out of Scope

The following capabilities are explicitly NOT owned by Platform/Runtime:

- **Market data ingestion, broker connection, and packet decoding:** Owned by Ingestion.
- **Candle computation, signal detection, strategy evaluation:** Owned by the Signal Flink job. **(Ranking and instruction publication REMOVED 2026-08-15, CHG-005.)**
- **Broker order submission, execution, and gate management:** Owned by the Executor.
- **Postback capture, fill lifecycle, and position projection:** Owned by Action Capture.
- **Babysitter position monitoring and action emission:** Owned by the Babysitter Flink job.
- **EOD controller orchestration and manifest creation:** Owned by the EOD controller.
- **Observability backend, dashboard, and alert configuration:** Owned by the observability layer and operations.
- **Kubernetes deployment:** Deferred. Production is Docker Swarm.
- **Multi-broker or multi-region deployment:** Deferred; not in MVP scope.
- **Automatic live-gap backfill or historical replay:** Deferred; not in MVP scope.

## REQ-PF-001: Exact versions

Production manifests SHALL pin exact immutable image versions/digests for Fluss, Flink, Arrow REST, OpenObserve, and all project services. `latest` and version ranges are prohibited. Exact Java, Python, protocol/SDK, connector, and plugin versions are release inputs.

## REQ-PF-002: Production Swarm placement

v1: the three workload VMs are Manager+Worker and SHALL host Fluss replicas/quorum and Flink capacity with constraints preventing replica co-location; v2: M1-3 are Manager ONLY (drained), W1-3 (+W4+) are Workers. The observability VM (O1, outside Swarm) SHALL not be required for order-safety correctness. Placement uses role labels (`role=manager/worker`, `flink`, `fluss`), not hostnames, so `W4` joins without stack redesign. Resources reserve 2CPU/2GB per manager in v1. Placement, resources, update order, restart policy, health checks, and rollback are explicit in the stack definition. See 09 v1→v2.

Loss of any one workload VM SHALL be tested at 50,000 ticks/s variable average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average). The test proves replica/quorum behavior, Flink recovery from S3 checkpoints, bounded backlog, data recovery under 30 seconds, and safe order halt under five seconds.

## REQ-PF-003: Networking

Local Compose uses an isolated bridge. Production uses encrypted Swarm overlay networks or equivalent TLS-protected cross-host transport. Only explicitly required operator/UI/API endpoints are exposed through controlled ingress/firewall rules. Fluss tablet, internal RPC, checkpoint, and service ports are not publicly exposed.

## REQ-PF-004: Secrets and identity

Local development may use ignored `.env` files. Production SHALL use Docker Swarm secrets with least-privilege service identities. Secrets are not embedded in images, stack files, logs, command lines, or environment dumps.

Rotation/revocation procedures cover broker credentials, Arrow REST keys, S3 credentials, OpenObserve credentials, TLS material, and operator identities. Rotation tests include expired/revoked credentials and confirm alerting/readiness behavior.

## REQ-PF-005: Persistent storage

- Flink checkpoints/savepoints use encrypted, versioned S3 storage.
- Fluss data uses durable volumes per workload node with tested replication/recovery.
- Iceberg/audit uses encrypted S3 with immutable/versioned lifecycle policy compatible with the approved audit-retention period.
- OpenObserve uses dedicated observability storage and its failure cannot authorize orders or erase execution audit.

Local named volumes are not production durability.

## REQ-PF-006: Service topology

Required deployables are Fluss coordinator/quorum and tablets, Flink control/workers, Ingestion, Signal/Babysitter job submitter, Action Capture/position projector, Executor, and OpenObserve. Executor supports fencing/single active owner for each account/order partition.

## REQ-PF-007: Startup and readiness

Infrastructure may start concurrently, but services become ready only after dependency-specific readiness checks. New order placement remains disabled until:

- Required schemas/versions exist.
- Fluss quorum and replication are healthy.
- Signal and Babysitter jobs are running/checkpointing.
- Executor state is durable and gate is explicitly enabled through approved process.
- Broker/Arrow REST contracts and credentials are valid.
- Changelog continuity and observability are healthy.

Startup dependency declarations alone do not satisfy readiness.

## REQ-PF-008: Deployment and rollback

Deployments use rolling/canary policy only where state/schema compatibility is proven. Money-moving changes begin with the gate halted, reconciliation complete, and controlled enablement. Rollback must preserve schema/state readability and default to halted when uncertain.

Schema-breaking clean-break migrations are allowed only before live-money release and require destructive-change approval and reset/replay plan.

## REQ-PF-009: Capacity acceptance

The production-like Swarm environment SHALL pass:

1. variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) for a full session with decision p99 <100 ms.
2. One workload VM loss at the per-instrument production rate.
3. EOD full-volume manifest verification under 30 minutes target.
4. No duplicate broker order in crash-window injection.

## REQ-PF-010: Security acceptance

Tests cover network exposure, TLS, secret scanning/redaction, rotation/revocation, least privilege, unauthorized gate/resume attempts, encrypted storage, audit access, image pinning/SBOM/vulnerability policy, and recovery from a compromised credential.

## REQ-PF-011: N+1 resource and recovery budget

The production-like Swarm environment SHALL document per-VM CPU, memory, network, disk, Flink slots, Fluss tablet/quorum capacity, checkpoint bandwidth, and catch-up service rate. After loss of any one workload VM, the remaining placement SHALL sustain the declared 50,000 ticks/s average-baseline profile within the declared backlog, checkpoint, durability, and recovery thresholds. (The 90,000 ticks/s peak profile is retired, DEC-036; 60,000 ticks/s gate, DEC-045.)

The one-VM test SHALL report detection, safe halt, job restore, source catch-up, steady-state recovery, Fluss re-replication, and maximum backlog separately.

## REQ-PF-012: Sensitive transport encryption

TLS or an equivalent authenticated encrypted transport SHALL be mandatory for broker, Arrow REST, S3, operator control, secret delivery, and cross-host money-moving/state traffic. Exceptions require explicit scope, data classification, isolation, risk owner, expiry, and approval; “where supported” is not sufficient for production readiness.
