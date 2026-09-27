# Requirements — Index

## Status

| Field              | Value                                                                       |
| ------------------ | --------------------------------------------------------------------------- |
| Classification     | Production live-money streaming platform requirements                       |
| Readiness          | **Blocked for live money** until the release gates below pass               |
| Migration posture  | Pre-production clean break; no compatibility with stale schemas is required |
| Evidence owners    | Platform Team and Execution Team                                            |
| Evidence due dates | TBD; must be assigned before implementation starts                          |

## Authority

Use the repository authority order defined in [`../01_project/00-index.md`](../01_project/00-index.md): implementation and tests, active project decisions, DDLs, contracts, then detailed requirements. A requirement that depends on unverified Arrow, Flink, or Fluss behavior is an **evidence-gated requirement**, not an established fact.

## Production baseline

The requirements in this directory enforce these decisions:

- One signal Flink job performs multi-timeframe candle computation and strategy-host signal detection. **(In-operator ranking and instruction publication REMOVED 2026-08-15, CHG-005; the 15 s candle/forming-bar path RETIRED 2026-09-05.)**
- One separate Babysitter Flink job is a checkpointed no-op in MVP.
- Order correlation uses `instruction_id`, `client_order_ref`, and `broker_order_id`.
- No broker tick or postback sequence is assumed.
- Tick deduplication is bounded and best-effort using a versioned event fingerprint.
- Ingestion preserves original broker packet bytes plus normalized typed columns.
- Emitted candles are final in MVP; later records are discarded and measured.
- Exactly-once claims are limited to version-pinned, tested Flink boundaries. Broker calls and independent writes are not described as exactly-once.
- Executor owns durable gate, attempt, correlation, reconciliation, and execution-audit state.
- Instructions are immutable. Changed parameters create a new `instruction_id`.
- Position state is a separate aggregate keyed by `position_id` and linked by `trade_context_id`.
- MVP Babysitter emits no actions. Future actions use a versioned structured schema.
- Local development uses Docker Compose; production uses Docker Swarm (09 v1 4 VMs Manager+Worker → v2 7 VMs Manager-ONLY + Workers).
- Production checkpoints/savepoints use S3; Fluss uses three-node replication across workload VMs.
- Eligible live source tables retain at least three complete trading days and do not expire data while EOD offload is unverified.
- Money-moving audit records are immutable, encrypted, integrity-verifiable, and retained for at least one year or longer under approved policy.

## Reading map

| Layer | File                                                 | Purpose                                                           |
| ----- | ---------------------------------------------------- | ----------------------------------------------------------------- |
| 1     | [`01-context-and-scope.md`](01-context-and-scope.md) | Actors, boundaries, scope, and non-goals                          |
| 2     | [`02-functional/`](02-functional/)                   | Component behavior and ownership                                  |
| 3     | [`03-non-functional.md`](03-non-functional.md)       | Performance, availability, durability, consistency, and security  |
| 4     | [`04-data.md`](04-data.md)                           | Logical data model, identity, ownership, retention, and evolution |
| 5     | [`05-interfaces.md`](05-interfaces.md)               | Component interfaces and delivery semantics                       |
| 6     | [`06-operational.md`](06-operational.md)             | Deployment, startup, health, recovery, and release gates          |
| 7     | [`07-requirement-authoring-template.md`](07-requirement-authoring-template.md) | Standard owner/state/failure/acceptance structure for requirements |
| 8     | [`08-reading-guide.md`](08-reading-guide.md)          | Human/LLM navigation, canonical scope, timestamps, and safety vocabulary |
| 9     | [`09-acceptance-matrix.md`](09-acceptance-matrix.md)  | Requirement-to-test traceability; 152 acceptance IDs across all domains (corrected 2026-08-16 — was 151)  |

## Requirement conventions

- **SHALL** is mandatory; **SHOULD** is recommended; **MAY** is optional.
- Every mandatory behavior requires an owner and a binary acceptance test.
- Numeric targets must identify boundary, percentile, workload, duration, and clock source.
- Unknown protocol details are written as blockers with an evidence owner; plausible values are not promoted to contracts.
- Schema names in this directory must match reconciled DDLs before implementation is considered ready.

## Live-money release gate

Live-money order placement SHALL remain disabled until all of the following are complete:

1. Critical risks in `../01_project/05-risks-and-assumptions.md` are closed with evidence.
2. Official Arrow artifacts or sandbox captures prove required fields, limits, response semantics, and correlation behavior.
3. Exact Flink, Fluss, Java, Python, broker SDK/protocol, and Arrow REST versions are pinned.
4. Version-specific Flink/Fluss source, sink, changelog, partial-update, checkpoint, and replication tests pass.
5. Crash-window tests prove no duplicate broker order under unknown submission outcomes.
6. Safe-halt occurs within five seconds and requires successful reconciliation plus single-operator (Saurabh, DEC-044) approval to resume.
7. One workload VM can fail while the normal workload remains within the documented durability posture.
8. Throughput, latency, recovery, offload, security, and observability acceptance gates pass.

Paper or simulated trading may be used for validation, but it does not waive any live-money gate.
