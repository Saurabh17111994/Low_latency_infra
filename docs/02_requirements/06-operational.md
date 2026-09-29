# 06 — Operational Requirements

## 6.1 Operating modes

Local operations use Docker Compose. Production operations use Docker Swarm across v1: 4 VMs (3× Manager+Worker + 1 O2) → v2: 7 VMs (3× Manager ONLY + N≥3 Workers + 1 O2) per 09 v1→v2. Every runbook identifies its environment; local commands are never presented as production procedures.

## Constraints

- A process start or container health check SHALL NOT automatically enable order placement. The Executor gate SHALL begin `HALTED` and require explicit reconciliation plus single-operator (Saurabh, DEC-044) approval to reach `ENABLED`.
- Automatic gate resume SHALL be prohibited. Any mechanism that enables order placement without the single-operator (Saurabh, DEC-044) authenticated approval for the same gate epoch and evidence hash is a release-blocking defect.
- Planned maintenance that affects money-moving state SHALL first halt new money-moving calls and record the gate epoch and reason before proceeding.
- No source trading day SHALL expire before its EOD manifest is verified. Retention SHALL extend automatically while the manifest is unverified, retryable, or under reconciliation. At least three complete trading days SHALL remain live even after successful verification.
- RPO and RTO claims SHALL be reported per failure scenario with measured evidence. Broader claims than the tested scenarios are prohibited.
- Secret rotation/revocation procedures SHALL be documented and tested for broker, Arrow REST, S3, OpenObserve, TLS, and operator credentials. Secrets SHALL NOT be embedded in images, runbooks, logs, or command lines.
- Runbooks SHALL distinguish fault injection, detector threshold, detection timestamp, safety-gate block, recovery start, recovery completion, source catch-up, and resume approval as separate events. Safe-halt and data-recovery targets apply to the complete declared boundary.
- A named service or scheduled job SHALL own EOD manifest creation, verification, retry/backoff, retention extension, expiry protection, and storage-pressure alerts. Its state and ownership SHALL be durable.
- Local Compose commands SHALL NOT be presented as production procedures. Every runbook identifies its environment.
- Unauthorized or mismatched gate approvals SHALL be rejected, audited, and alerted. The rejection event is immutable and retained under the approved execution-audit policy.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-OPS-001 | The production Swarm (v1: 4 VMs Manager+Worker with 2CPU/2GB manager reserved per VM; v2: 7 VMs with 3 Manager-ONLY + N≥3 Workers) can be operated with encrypted overlay, TLS, Swarm secrets, role-label placement, and three-node Fluss placement, adding worker VMs without changing the 3-manager Raft quorum. | ASM-005, ASM-009 |
| ASM-OPS-002 | S3 `ap-south-1` is accessible, supports versioning/lifecycle policies, and can complete verified EOD offload within 30 minutes. | ASM-006 |
| ASM-OPS-003 | Fluss three-node replication/quorum and Flink S3 checkpoints operate as documented in the pinned version matrix. | ASM-008 |
| ASM-OPS-004 | ~~OpenAlgo is reachable from the production Swarm network~~ (obsolete — OpenAlgo removed per DEC-006). Arrow REST (`https://edge.arrow.trade`) is reachable from the production Swarm network and exposes deterministic order-submission and reconciliation endpoints. | ASM-007 |
| ASM-OPS-005 | Single authorized operator `saurabh` (DEC-044) are available for the single-operator (Saurabh, DEC-044) gate resume protocol on demand. | RISK-012 |
| ASM-OPS-006 | Brokers provide sufficient reconciliation capability (query/list recent orders, client references, broker order IDs, fills, positions) with defined consistency delay. | REQ-EXE-013 |
| ASM-OPS-007 | Security incidents can be detected and will halt affected order flow within the five-second safe-halt target. | RISK-003 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Executor starts HALTED:** Every fresh installation, every restart with unverifiable state, and every planned maintenance cycle begins with the gate halted. The platform does not assume clean state on startup.
- **Readiness-driven startup order:** Infrastructure services may start concurrently, but service readiness is gated on dependency-specific checks. New order placement waits for the full readiness chain: schemas, replication, running/checkpointing jobs, explicitly enabled gate, valid broker credentials, and healthy observability.
- **Maintenance halts gate first:** Planned maintenance that could affect money-moving correctness halts new orders, records the gate epoch and reason, drains in-flight attempts, checkpoints jobs, and verifies durable state before proceeding.
- **RPO/RTO per scenario, not a single platform claim:** Recovery Point Objective and Recovery Time Objective are measured per failure scenario (VM loss, checkpoint corruption, S3 interruption, quorum degradation, broker disconnect, crash window). A single aggregate HA claim is not sufficient.
- **Paper trading validates but does not waive live-money gates:** Simulated or paper trading may be used for validation, but all live-money release gates must still pass with evidence.
- **Unauthorized approvals are immutable audit events:** Every approval attempt — successful, rejected, mismatched, or unauthorized — is recorded in `Execution_Audit` and retained for at least one year or longer under approved policy.
- **Local Compose is development-only:** Docker Compose proves component integration and deterministic tests. It does not prove production HA, replication, quorum, TLS, secret management, or VM-loss tolerance.

## Out of Scope

The following capabilities are explicitly NOT in MVP operational scope:

- **Kubernetes deployment and operations:** Production is Docker Swarm.
- **Multi-broker operational procedures:** MVP supports exactly one evidence-approved broker integration.
- **Multi-region disaster recovery:** MVP covers single-region operations with S3 durability.
- **Automatic order-path resume after uncertainty:** Explicitly prohibited. Resume is always manual, single-operator (Saurabh, DEC-044), and reconciliation-gated.
- **Automatic live-gap backfill operations:** Deferred; not in MVP scope.
- **Business analytics operations (P&L, win rate, trader dashboards):** Deferred; not in MVP scope.
- **End-user trading alerts and notification operations:** Deferred. Operational alerts remain in scope.
- **Babysitter action monitoring and management:** MVP is a strict no-op; post-MVP action operations are deferred.

## 6.2 Startup

Production startup order is readiness-driven:

1. S3 checkpoint/lake and Swarm secret access validated.
2. Fluss quorum, tablets, replication, and schemas healthy.
3. Flink control/workers healthy.
4. Signal and Babysitter jobs deployed from pinned artifacts and checkpoint-compatible state.
5. Ingestion and Action Capture connect using verified protocol/schema versions.
6. Executor starts `HALTED`, validates durable state, mappings, continuity, Arrow REST, and observability.
7. Reconciliation completes; the authorized single operator (Saurabh, DEC-044) approves the same gate epoch/evidence hash before `ENABLED`.

A process start or container health check never automatically enables order placement.

## 6.3 Health checks

Each service exposes liveness and readiness. Operations additionally monitor Flink job/checkpoint health, Fluss replication/quorum, Executor trading readiness/gate state, storage/offload readiness, broker subscription completeness, projection lag, and observability delivery.

Readiness failure removes traffic/work ownership where safe. Order-path uncertainty also halts the gate.

## 6.4 Shutdown and maintenance

Planned shutdown first halts new money-moving calls, records gate epoch/reason, drains/reconciles in-flight attempts, checkpoints jobs, drains accepted ingestion/postbacks within bounded deadlines, and verifies durable state before stopping storage.

Forced termination creates an audit/alert and requires reconciliation before resume.

DDL, version, checkpoint/savepoint, retention, secret, network, and capacity changes use approved change records, prechecks, rollback criteria, and post-deployment verification.

## 6.5 Backup, recovery, and disaster recovery

- Flink checkpoints/savepoints: encrypted versioned S3.
- Fluss: three-node replicated data across workload VMs.
- Immutable audit/lake: encrypted S3; execution/order/fill/gate audit retained for at least one year or longer under approved policy.
- Operational KV projections: rebuildable from immutable audit or tested backup.

Required exercises include process restart, tablet/worker loss, any one workload VM loss, checkpoint corruption/unavailability, S3 interruption, Fluss quorum degradation, observability loss, broker disconnect, and Executor crash windows.

Data processing resumes under 30 seconds for accepted failure scenarios. Order safe-halt completes under five seconds. RPO/RTO are reported per scenario; no broader claim is made than tested.

## 6.6 EOD offload operations

At EOD the operator/service verifies manifests, counts, source ranges, schema versions, hashes/checksums, and Iceberg commits. Failures retry with backoff and extend Fluss retention. Critical alerts fire on failed verification, insufficient expiry margin, S3 unavailability, or storage pressure.

No source day expires until its manifest is verified, and at least three complete trading days remain live.

## 6.7 Gate halt and resume runbook

Halt records reason, detection timestamp, scope, epoch, affected attempts (and, pre-2026-08-15, reservations — REMOVED with CHG-005), and evidence. Reconciliation checks broker orders, Arrow REST responses, mappings, fills/positions, changelog continuity, Signal job/checkpoints, and every unknown attempt.

Resume requires a single-operator (Saurabh, DEC-044) authenticated approval of the same evidence hash and epoch. Unauthorized approvals or a mismatched epoch/evidence hash are rejected, audited, and alerted. Automatic resume is prohibited.

## 6.8 Secrets and security operations

Production credentials are Swarm secrets. Rotation/revocation is documented and tested for broker, Arrow REST, S3, OpenObserve, TLS, and operator credentials. Logs and support bundles run redaction checks. Audit access is role-controlled and reviewed.

Security incidents halt affected order flow, rotate credentials, preserve evidence, and require reconciliation/single-operator (Saurabh, DEC-044) resume.

## 6.9 Observability and alert response

MVP runbooks cover all order-safety, streaming-health, storage-safety, and security alerts defined in `02-functional/08-observability.md`. Every critical alert has owner role, acknowledgement target, diagnostic evidence, halt behavior, escalation, remediation, and closure evidence. Due dates remain TBD until assigned; live-money release cannot proceed with unowned critical alerts.

Runbook coverage (kept in sync with the runbook index in `../06_operations/`):

| Runbook | Runbook ID | Alert family / scope |
| --- | --- | --- |
| [`../06_operations/01-runbooks.md`](../06_operations/01-runbooks.md) — Operational runbooks | — | Order-safety, streaming-health, storage-safety, and security alerts (gate halt, reconciliation/resume, checkpoint failure, replay incident, cluster, schema-preflight, credential incident) |
| [`../06_operations/02-ingestion-alerting.md`](../06_operations/02-ingestion-alerting.md) — Ingestion alerting contract | — | Ingestion alerts (`ING-*`) |
| [`../06_operations/04-dr-plan.md`](../06_operations/04-dr-plan.md) — Disaster recovery plan | — | Recovery and VM-loss exercise procedures |
| [`../06_operations/05-maintenance.md`](../06_operations/05-maintenance.md) — Maintenance and change operations | — | Planned maintenance and change records |
| [`../06_operations/06-audit-store.md`](../06_operations/06-audit-store.md) — Policy-controlled audit store (R2 bucket locks) | `OPS-AUDIT-STORE-001` | Storage-safety: approved audit-retention policy and immutable storage (NFR 3.4.1 / AC-NFR-005); provisioning and validation (`audit_r2.py`) |

## 6.11 Control-plane operations

The platform SHALL define versioned, authenticated, idempotent interfaces for halt, reconciliation start/completion, approval, approval expiry/revocation, quarantine disposition, manual unknown-attempt disposition, gate inspection, and audit retrieval. Every command includes scope, actor, request ID, expected epoch/version, evidence hash where applicable, authorization result, and immutable audit outcome.

## 6.12 Failure timeline evidence

Failure runbooks SHALL distinguish fault injection, detector threshold, detection timestamp, safety-gate block, recovery start, recovery completion, source catch-up, and resume approval. Safe-halt and data-recovery targets apply to the complete declared boundary, not only the interval after an unspecified detector fires.

## 6.13 EOD controller ownership

A named service or scheduled job SHALL own EOD manifest creation, verification, retry/backoff, retention extension, expiry protection, storage-pressure alerts, and manual reconciliation. Its state and ownership SHALL be durable and included in the schema/operations acceptance tests. A slot that passed with no recorded success SHALL be caught up within the same trading day (the controller's lease keeps the re-fire idempotent), and the scheduler's health signal SHALL distinguish a dead scheduler from a live one whose day is not yet archived.

## 6.10 Release gates

No live-money deployment until:

- Broker/Arrow evidence and exact version matrix are approved.
- DDL/requirements/contracts/tests agree.
- Critical risks are closed.
- Throughput, latency, recovery, one-VM, checkpoint, replication, offload, security, and crash-window tests pass.
- Executor scaffold is replaced and durable gate/attempt/reconciliation behavior passes.
- Dashboards, alerts, runbooks, rollback, and audit retrieval are operational.

Paper/simulated trading may validate the system but does not waive live-money gates.
