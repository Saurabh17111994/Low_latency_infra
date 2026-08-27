# Release Strategy

## Status and authority

This document defines the deployment and release strategy for MVP Phase 4.2. It is derived from the active project decisions, requirements, architecture, and build contracts:

- `../01_project/06-delivery-scope.md`
- `../02_requirements/00-index.md`
- `../02_requirements/06-operational.md`
- `../03_architecture/platform-architecture.md`
- `../04_contracts/00-index.md`

The platform is **blocked for live-money release** until the evidence-gated protocol, exact-version, schema, safety, capacity, recovery, security, and observability gates pass. Paper or simulated trading does not waive those gates.

## Environment policy

| Environment | Orchestrator | Purpose | Order placement |
| --- | --- | --- | --- |
| Local/development | Docker Compose | Fast development and component integration | Disabled by default; simulation only unless explicitly approved |
| Integration/acceptance | Docker Compose or production-like test harness | Deterministic contract, fault, workload, and recovery tests | Simulated/sandbox only |
| Production | Four-VM Docker Swarm | Three workload/HA VMs plus one observability VM | Disabled until all live-money gates pass |

Compose is not proof of production HA. Production placement, replication, checkpoint recovery, security, and failure tolerance must be tested on the Swarm topology.

## MVP release scope

MVP Phase 4.2 includes:

- Evidence-approved Arrow market-data ingestion with original packet preservation
- `raw_table_1` and reconciled Fluss schemas
- Bounded best-effort fingerprint deduplication
- Event-time 15-second OHLCV candles and final-on-emission behavior
- Forming-bar Business Logic and immutable candidate audit
- ~~In-operator Ranking and immutable ranking audit~~ — **REMOVED 2026-08-15 (CHG-005)**
- ~~Portfolio reservation gates and immutable `Trade_Decisions`~~ — **REMOVED 2026-08-15 (CHG-005)**
- Durable Executor gate, attempts, mappings, reconciliation, fencing, and Arrow REST handoff
- Independent postback capture, `Fills`, `Order_Lifecycle`, and fill-derived `Positions`
- Checkpointed Babysitter job that emits zero actions
- Operational logs, metrics, health, alerts, and durable execution audit
- Verified EOD Iceberg/S3 offload with retention safety buffer
- Local Compose and production Swarm definitions (09 v1 4 VMs Manager+Worker; v2 7 VMs Manager-ONLY + Workers)

Deferred capabilities include real Babysitter actions, advanced features, multi-broker support, BSE/currency derivatives, Kubernetes, automatic live gap backfill, backtesting, and business analytics. (**Market-context and ML ranking REMOVED 2026-08-15, CHG-005.**)

## Release stages

### Stage 0 — Evidence and version freeze

Before implementation is accepted:

1. Approve Arrow packet/postback artifacts or captured sandbox corpus.
2. Prove required broker identities, status values, timestamps, reference echo, limits, and response behavior.
3. Pin immutable versions/digests for Flink, Fluss, Java, Python, connectors, broker protocol/SDK, Arrow REST, OpenObserve, and project images.
4. Record the version matrix and compatibility classification.

Unknown external behavior remains a blocker and is never filled with a plausible value.

### Stage 1 — Local contract validation

Use Compose to validate:

- Reconciled DDL/schema parity
- Golden packet decoding and raw-byte preservation
- Ingestion, Flink, Action Capture, Executor, and Babysitter interfaces
- Identity and ownership rules
- Duplicate, out-of-order, malformed, partial-write, restart, and quarantine behavior
- Safe-halt and single-operator (Saurabh, DEC-044) resume in simulation

### Stage 2 — Production-like acceptance

Use the production Swarm topology (09 v1 4 VMs; v2 7 VMs) to validate:

- variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) for a full session
- One workload VM loss at the per-instrument production rate
- p50/p95/p99 SLO reporting with exact workload/version context
- One workload VM loss at the normal baseline
- S3 checkpoint/savepoint recovery
- Fluss replication/quorum and bounded backlog
- EOD manifest verification and retention protection
- Security, observability, and audit reconstruction

### Stage 3 — Controlled production enablement

Production begins with:

1. Gate `HALTED`.
2. Reconciled broker orders, fills, positions, and attempts. (**Reservations REMOVED 2026-08-15, CHG-005.**)
3. Verified changelog continuity and checkpoint health.
4. Verified observability and alert ownership.
5. Single-operator (Saurabh, DEC-044) authenticated approval of the same gate epoch/evidence hash.
6. A documented rollback and halt procedure available to operators.

Automatic enablement and automatic resume are prohibited.

## Release gates

Live-money deployment requires all of the following:

- Critical risks closed with evidence
- Exact protocol/version matrix approved
- DDL, requirements, contracts, tests, and architecture consistent
- Crash-window tests produce no duplicate broker order
- Unknown outcomes halt within five seconds and cannot retry automatically
- Restart with unverifiable Executor state defaults to `HALTED`
- Fencing prevents concurrent active Executors
- variable 50,000 ticks/s average-baseline workload tests passes (90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045)
- One-workload-VM failure posture is proven
- Data recovery target under 30 seconds is met for accepted scenarios
- EOD offload and three-day retention safety are proven
- Approved-policy encrypted audit retrieval is proven for at least one year
- Security, secret rotation, least privilege, and unauthorized-control tests pass
- Dashboards, alerts, runbooks, rollback, and gate approval evidence are operational

## Change-control rule

Every deployment-affecting change records:

- Requirements/contracts/DDL/interfaces affected
- Version and schema compatibility classification
- State and savepoint impact
- Gate/halt requirement
- Prechecks and acceptance tests
- Rollback criteria and recovery path
- Post-deployment verification

Schema-breaking clean-break migration is allowed only before live-money release and requires destructive-change approval plus reset/replay evidence.

## Local development commands

The following are illustrative local workflow commands; exact scripts are implementation-owned and must be verified against the repository:

```bash
make env
make up
make ddl
make logs
make down
```

`make clean` or volume deletion is destructive and must not be used against production data.

## References

- Operational requirements: `../02_requirements/06-operational.md`
- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Quality targets: `../01_project/03-quality-targets.md`
- Deployment contract: `../04_contracts/09-platform-runtime.md`
