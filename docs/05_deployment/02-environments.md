# Deployment Environments

## Environment matrix

| Environment | Orchestrator | Failure domain | State/storage | Purpose | Money-moving calls |
| --- | --- | --- | --- | --- | --- |
| Local development | Docker Compose, one host | Single host | Named volumes and local test storage | Fast development and debugging | Disabled by default; simulation only |
| Integration/acceptance | Compose or production-like harness | Controlled test failures | Test Fluss, encrypted test S3 | Contract, compatibility, workload, and recovery evidence | Sandbox/simulated only |
| Production | Docker Swarm, v1 4 VMs → v2 7 VMs | v1: 3× Manager+Worker + 1 observability; v2: 3× Manager-ONLY + N≥3 Workers + 1 observability (same stack) | Replicated Fluss, encrypted S3, durable audit | Live platform | Blocked until release gates pass |

Environment definitions must remain separate. Local Compose settings must never be used as evidence for production HA, security, durability, or capacity.

## Production placement

### Workload VMs

v1: The three workload VMs are Manager+Worker and host:

- A ZooKeeper ensemble node (one per VM; 3-node ensemble, quorum 2-of-3; Fluss metadata store — required by Fluss 0.9.1 and still by 1.0.0 — and Flink JobManager HA leadership)
- Fluss coordinator/tablet capacity and three-node replication/quorum (LOG tables; KV tables are single-replica in Fluss 0.9.1 and 1.0.0 (standby replicas are an opt-in promotion aid only) — durability via Fluss remote storage + rebuild from audit (Flink checkpoints hold only small working/recovery state — DEC-038))
- Flink JobManager (HA standby + leader via ZooKeeper)/TaskManager workload capacity according to the proven placement plan
- Ingestion, Action Capture, Executor, and job deployment control as assigned by the Swarm stack

Fluss replicas cannot co-locate on one workload VM. All three replicas of any critical Fluss/Flink role SHALL be placed across separate workload VMs via anti-co-location constraints. The placement plan must specify resources, update order, restart policy, shutdown grace, health checks, and persistent volume ownership.

### Observability VM

The dedicated observability VM hosts OpenObserve and related telemetry storage/collection. Its loss must not authorize orders, erase local durable execution audit, or make the order path appear healthy. Telemetry loss is itself observable and can block live-money readiness.

## Shared dependencies

Production readiness requires:

- Encrypted, versioned S3 for Flink checkpoints/savepoints and Iceberg/audit storage
- Docker Swarm secrets and least-privilege identities
- Pinned immutable image digests and exact dependency versions
- Version-reconciled Fluss schemas and connector configuration
- Broker/Arrow REST protocol evidence and valid credentials
- ZooKeeper ensemble quorum (2-of-3) health and Fluss quorum/replication health
- Signal and Babysitter jobs running and checkpointing
- Executor durable state, fencing, changelog continuity, and known gate state
- OpenObserve delivery or an approved durable telemetry degradation mode

## Readiness dimensions

A deployment reports separately:

- **Liveness:** process/event loop responds.
- **Readiness:** mandatory dependencies and data flow are available.
- **Job health:** required Flink jobs are running and checkpointing.
- **Trading readiness:** Executor gate is `ENABLED`, state is known, and reconciliation is clean.
- **Durability readiness:** replication, checkpoints, offload, audit retention, and recovery posture pass.

A healthy container is not sufficient for any higher readiness state.

## Local Compose expectations

Local Compose may include one Fluss coordinator/tablet, Flink control/workers, ingestion, Action Capture, Executor, OpenObserve, and job submission components. It is intended for deterministic development and sandbox testing.

Local configuration:

- Uses ignored `.env` only for development secrets.
- Uses test/sandbox broker credentials.
- Must not use production audit, checkpoint, or live-money credentials.
- May use local named volumes, which are not production durability evidence.
- Starts with the Executor gate `HALTED` unless a test explicitly controls a simulated gate.

## Production startup order

1. Validate Swarm secrets, network, S3, and durable storage access.
2. Validate ZooKeeper ensemble quorum (2-of-3), then Fluss quorum, replication, tablets, and required schemas.
3. Start Flink control/workers (JobManager HA leader elected via ZooKeeper) and verify checkpoint + HA metadata storage.
4. Deploy Signal and Babysitter jobs from pinned artifacts.
5. Verify ingestion manifest/subscriptions and Action Capture protocol readiness.
6. Start Executor with gate `HALTED`; verify durable state, mappings, continuity, Arrow REST, and telemetry.
7. Complete reconciliation and verify all unknown attempts are resolved. (**Reservations REMOVED 2026-08-15, CHG-005.**)
8. Obtain the single-operator (Saurabh, DEC-044) authenticated approval for the same evidence hash/epoch.
9. Enable only the approved gate epoch.

Startup dependencies and health checks never automatically enable order placement.

## Failure and maintenance behavior

- Loss of any one workload VM is tested at the variable 50,000 ticks/s average baseline (90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045); ZooKeeper ensemble holds quorum (2-of-3) through that loss.
- ZooKeeper quorum loss, Fluss quorum degradation, checkpoint failure, changelog discontinuity, or uncertain Executor state halts new money-moving calls. A single ZooKeeper node loss is tolerated while quorum holds.
- Broker/authentication failure makes affected services not ready and alerts operations.
- Planned maintenance begins with the gate halted, drains or reconciles attempts, checkpoints jobs, and verifies durable state.
- Forced termination creates an audit event and requires reconciliation before resumption.

## Environment acceptance

Acceptance must prove the variable 50,000 ticks/s average baseline profile (90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045), one-VM loss, bounded backlog, checkpoint restore, data recovery under 30 seconds for accepted cases, safe-halt under five seconds, EOD manifest verification, security controls, and audit reconstruction.

## References

- Architecture: `../03_architecture/03-networking.md`, `../03_architecture/platform-architecture.md`
- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Operational requirements: `../02_requirements/06-operational.md`
