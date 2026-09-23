# 03 — Non-functional Requirements

## Constraints

- The data path MAY auto-recover when correctness state is verifiable. The order path SHALL NOT resume automatically after uncertainty — automated blind retry after an unknown broker outcome is prohibited.
- No money-moving call SHALL occur without an `ENABLED` gate and matching current epoch. A gate check that is skipped, stale, or bypassed is a release-blocking defect.
- One immutable `instruction_id` maps to one immutable request definition. A repeated `instruction_id` with different content is a contract violation, not a retry.
- `instruction_id`, `client_order_ref`, and `broker_order_id` SHALL remain distinct and be durably mapped before reconciliation is considered complete.
- Order lifecycle and position lifecycle SHALL remain separate aggregates. A position projection SHALL NOT be inferred from a single order lifecycle row.
- Older or out-of-order postbacks SHALL NOT regress authoritative lifecycle or position state. Stale updates SHALL be rejected using versioned source event checks.
- No source data SHALL expire before verified EOD offload plus the minimum three-day live buffer. Retention extension SHALL be automatic while the manifest is unverified.
- A single authenticated approval by the authorized operator (Saurabh, DEC-044) SHALL approve every post-reconciliation gate resume. An unauthorized approval or a mismatched epoch/evidence hash SHALL NOT enable the gate. A second approval is not required and not checked (DEC-044).
- TLS or equivalent authenticated encrypted transport SHALL be mandatory for broker, Arrow REST, S3, operator control, secret delivery, and cross-host money-moving/state traffic. "Where supported" is not sufficient.
- Unencrypted cross-host secrets or money-moving data are prohibited.
- Secrets, tokens, original payload bytes, and unnecessary account identifiers SHALL be redacted from logs and metrics. Secrets SHALL NOT be embedded in images, stack files, logs, or environment dumps.
- Broad "HA" claims without tests are prohibited. Every availability, recovery, RPO, and RTO claim requires a named failure scenario and measured evidence.
- High-cardinality metric labels SHALL use bounded aggregation, sampling, or structured logs with retention policy. Uncontrolled label cardinality is a production defect.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-NFR-001 | TCP preserves order within each Arrow WebSocket connection, and the `raw_table_1` append order is sufficient for deterministic event-time replay. | ASM-001 |
| ASM-NFR-002 | Arrow postbacks expose `broker_order_id`, lifecycle status, and the submitted `remarks` value for correlation. | ASM-002 |
| ASM-NFR-003 | The production baseline (v1: 4 VMs Manager+Worker; v2: 7 VMs with N≥3 Workers) can sustain the normal variable 50,000 ticks/s average baseline while one HA VM is unavailable, using the three workload VMs (v2: Worker VMs) as the load-bearing capacity with managers isolated from workload contention (the 90,000 ticks/s peak campaign is retired, DEC-036; 60,000 ticks/s gate, DEC-045). | ASM-005, RISK-010 |
| ASM-NFR-004 | S3 `ap-south-1` can complete verified EOD offload of a full trading day within 30 minutes. | ASM-006 |
| ASM-NFR-005 | ~~OpenAlgo exposes deterministic REST order-submission responses~~ (obsolete — OpenAlgo removed per DEC-006). Arrow REST `POST /order/regular` returns deterministic order-submission responses and enough evidence to correlate broker order identity. | ASM-007 |
| ASM-NFR-006 | The selected Fluss version supports BYTES payload, KV state tables, changelog images, three-node replication (LOG tables; KV tables are single-replica in Fluss 1.0.0 (standby replicas are an opt-in promotion aid only) — durability via Fluss remote storage + rebuild from audit (Flink checkpoints hold only small working/recovery state — DEC-038)), retention extension, and lake tiering properties. | ASM-008 |
| ASM-NFR-007 | Docker Swarm secrets, encrypted overlay/TLS, S3 checkpoints, role-label placement, and three-node Fluss placement can be operated on the production Swarm (v1 4 VMs Manager+Worker; v2 7 VMs Manager-ONLY + Workers) without changing the 3-manager Raft quorum. | ASM-009 |
| ASM-NFR-008 | The approved audit-retention policy, currently at least one year, is acceptable for the applicable live-money jurisdiction and account model. | ASM-010 |
| ASM-NFR-009 | Fluss connector atomic visibility semantics are per-sink, not cross-sink. Consumers tolerate partial visibility when reading multiple LOG and KV tables from the same checkpoint boundary. | RISK-008 |
| ASM-NFR-010 | The pre-production clean break permits replacing all stale physical DDLs without preserving compatibility with old consumers. | RISK-011 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Data path vs. order path separation:** The data path may auto-recover and tolerate bounded gaps. The order path halts on uncertainty and requires reconciliation plus single-operator (Saurabh, DEC-044) approval to resume. This is the foundational safety posture of the platform.
- **At-least-once delivery with bounded dedup:** Ingestion, Fluss LOG tables, and Executor intake are at-least-once. Duplicate events may exist. Bounded fingerprint-based dedup in the Signal job is best-effort, not exact.
- **Partial visibility across tables:** A single Flink checkpoint commits source offsets and sinks, but cross-table atomic visibility is not assumed. Consumers tolerate partial visibility and reconcile using stable IDs.
- **Exactly-once is bounded:** Exactly-once is claimed only for Flink managed state and version-pinned, tested sink boundaries. Broker calls and independent non-Flink writes are not described as exactly-once.
- **Pre-production clean break:** Stale schemas, outdated DDLs, and incompatible old consumer compatibility are not preserved. After go-live, schema changes follow the compatibility, replay, and rollback contract.
- **RPO is per-boundary, not a single platform claim:** Recovery Point Objective is defined separately for raw packets, immutable instructions, postback audit, Executor attempts/audit, projections, and EOD data.
- **Health is multidimensional:** A single green/red indicator cannot represent the platform. Liveness, readiness, job health, and trading readiness are separate dimensions.
- **Audit retention is policy-gated:** Money-moving audit records are immutable and encrypted at rest in the lake tier. Audit access is role-restricted and itself logged. Deletion before the approved retention period requires policy change, applicable legal-hold release, and single-operator (Saurabh, DEC-044) authorization.
- **Slow-Fluss ingestion policy** (`EVIDENCE-GATE-ING-BUFFER-001`) **resolved by capacity:** Fluss ingests up to 1-2 million ticks/s and the platform’s theoretical cap ceiling is 60,000 ticks/s (3,000 × 20; sustained gate 60,000 per DEC-045), so no durable local SSD buffer or controlled subscription pause is required. Bounded pending-append limits (50,000 records / `min(64MiB, 10% container memory)` bytes) remain as the defensive backpressure bound; indefinite in-memory buffering and silent data loss remain prohibited. An affected instrument becomes not-ready when the ...

## Out of Scope

The following capabilities are explicitly NOT in MVP scope:

- **Multi-broker support:** MVP supports exactly one evidence-approved broker integration.
- **BSE and currency derivatives:** MVP covers NSE and MCX market-data scope only.
- **250+ feature columns and pattern-feature libraries:** MVP computes 15-second OHLCV candles plus forming-bar state. Advanced features are deferred.
- ~~**ML-based ranking or dynamic weight adjustment:** MVP uses static, versioned weight configuration~~ — **REMOVED 2026-08-15 (CHG-005).**
- **Strategy authoring, backtesting, or configuration UI:** MVP uses hardcoded or configuration-file strategy definitions.
- **Charting and end-user notification features:** MVP includes operational alerts only; end-user trading alerts are not in scope.
- **Business analytics (P&L, win rate, trader dashboards):** Deferred.
- **Kubernetes deployment:** Production is Docker Swarm.
- **Automatic live-gap backfill and real-time gap reconciliation:** Deferred; not in MVP scope.
- **Babysitter position-management actions:** MVP is a strict checkpointed no-op. Real actions are Phase 4.3+.

## 3.1 Performance

### NFR-PERF-000: Fixed configuration constants

The following configuration values SHALL be enforced at startup. Deployment SHALL reject any deviation:

| Constant | Required Value | Enforcement |
| --- | --- | --- |
| `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT` | `65` | Java max heap equals 65% of the container memory limit |
| `NON_HEAP_MEMORY_RESERVE_PERCENT` | `35` | Container limit minus Java max heap must be at least 35% |
| `CONTAINER_MEMORY_ALERT_PERCENT` | `85` | Emit critical alert at or above 85% total container memory for 60 consecutive seconds |
| `BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC` | `20` | Synthetic baseline average only; not a live-feed or per-instrument scheduling guarantee |
| `BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC` | `30` | Workload generator rejects profiles that exceed 30 for any instrument |
| `DEDUP_TTL_MS` | `60000` | One minute; startup fails for any other value |
| `CANDLE_WINDOW_MS` | `15000` | Fifteen seconds; startup fails for any other value |
| `CHECKPOINT_INTERVAL_MS` | `10000` | Signal and Babysitter jobs use this interval |
| `CHECKPOINT_TIMEOUT_MS` | `30000` | Signal and Babysitter jobs use this timeout |
| `MAX_CONCURRENT_CHECKPOINTS` | `1` | Signal and Babysitter jobs use this value |
| `MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT` | `1` | Do not forward another active candidate for that instrument. Enforced structurally, not by a constant: the `Signal_Candidates_current` KV projection holds one current row per instrument (primary key `instrument_token`, DDL 23). The ranking-era `PlatformConfig` constant was removed 2026-09-14 (P6-665; its consumer went with CHG-005) |
| `INGESTION_MAX_BATCH_RECORDS` | `1` (validated 1..1000) | Append each tick immediately |
| `INGESTION_MAX_BATCH_WAIT_MS` | `0` (validated 0..100) | Do not wait for a batch |
| `MAX_PENDING_APPEND_RECORDS` | `50000` (validated 100..1000000) | Stop accepting at limit |
| `MAX_PENDING_APPEND_BYTES` | `min(67108864, floor(container_memory_limit_bytes × 0.10))` | Stop accepting at limit |
| `PENDING_APPEND_WARNING_PERCENT` | `80` | Emit warning and set readiness false at 80% |
| `POSITION_ACTIONS_ENABLED` | `false` | Hard-coded for MVP; startup fails if set to `true` |

### NFR-PERF-001: Workload envelope

The active instrument manifest is fixed at **3,000 instruments** for a trading session. Runtime manifest changes require a controlled restart. The production baseline is **50,000 ticks/s on average** (≈16.7 ticks/s/instrument over the declared window); arrivals are variable, and each instrument is capped at **20 ticks/s**. The capacity-peak campaign at **60,000 ticks/s** is RETIRED (DEC-036); the theoretical cap ceiling (3,000 × 20) remains a generator stress bound only.

Final machine sizing (CPU, RAM, disk I/O, network bandwidth) is evidence-gated by `PERF-PROD-60000-001` and the one-workload-VM-loss test.

| Scenario | Rate | Duration | Required evidence |
| --- | ---: | ---: | --- |
| Variable baseline | 50,000 ticks/s average | Full production-equivalent trading session | p99 latency, checkpoint, loss, and recovery SLOs |
| Capacity peak | ~~60,000 ticks/s~~ RETIRED (DEC-036); theoretical cap ceiling for generator stress only | — | No peak-capacity acceptance evidence required |

Tests use the full 3,000-instrument production manifest, connection count, subscription mode, packet-size/type distribution, and exact software versions.

### NFR-PERF-002: Latency SLOs

| Boundary                                                           | Target                                                                    |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------:|
| Broker packet receive → raw append acknowledgement                 | p99 <50 ms target (≤ 20 ms transport linger), evidence-gated against actual protocol/client           |
| Trigger tick consumed by Signal job → immutable instruction commit | **p99 <100 ms** at the 50,000 ticks/s variable baseline (3,000 instruments). Single release target; internal stage timings are diagnostic only. |
| Instruction commit → Executor receipt                              | Report p50/p95/p99; release threshold set after pinned connector baseline |
| Arrow REST call start → verified broker response                     | Report separately; no unverified fixed SLA                                |
| Failure detection → data processing resumed                        | <30 s                                                                     |
| Uncertainty detection → gate blocks new calls                      | <5 s                                                                      |
| Market close → verified EOD manifest                               | <30 min target at full-volume baseline                                    |

Every report includes p50/p95/p99, UTC clock source/offset, sample duration, failures/restarts included, workload, and exact versions. Candle window waiting is reported separately.

## 3.2 Availability and recovery

- Production SHALL tolerate loss of any one workload VM at the normal workload without violating the tested durability posture.
- Fluss uses three-node replication/quorum (LOG tables) and anti-co-location across workload VMs; Fluss metadata/coordination runs on a 3-node ZooKeeper ensemble (quorum 2-of-3) required by Fluss 1.0.0, which also serves Flink JobManager HA leadership.
- Flink checkpoints/savepoints use durable encrypted S3 storage.
- Data path recovers automatically when correctness state is verifiable.
- Order path never resumes automatically after uncertainty.
- Executor restart with unverifiable state defaults to `HALTED`.

RPO/RTO evidence is produced per failure scenario; broad “HA” claims without tests are prohibited.

## 3.3 Delivery and consistency

| Boundary                            | Contract                                                                        |
| ----------------------------------- | ------------------------------------------------------------------------------- |
| Broker stream → Ingestion           | Evidence-gated delivery; treated as at-least-once with possible gaps            |
| Ingestion → raw LOG                 | At-least-once; no exact raw dedup claim                                         |
| Compute dedup                       | Bounded best-effort fingerprint; false positives/negatives documented           |
| Flink managed state/sinks           | Exactly-once only where pinned integration tests prove the boundary             |
| Multiple Fluss outputs              | Partial visibility allowed unless tested transaction semantics prove otherwise  |
| Fluss instruction/action → Executor | At-least-once delivery; durable identity/request-hash guard                     |
| Executor → Arrow REST / broker          | At-least-once or unknown; durable attempt + reconciliation prevents blind retry |
| Postback projections                | At-least-once input; idempotent/versioned materialization and reconciliation    |

## 3.4 Durability and retention

- Eligible source/audit tables retain at least three complete trading days in Fluss.
- Retention automatically extends while the relevant EOD manifest is unverified, retryable, or under reconciliation.
- Execution, gate, order, fill, correlation, approval, and position-action audit records are encrypted in lake storage and retained for at least one year or longer under approved policy.
- EOD commits include counts, ranges, schema versions, hashes/checksums, and verification state.
- Operational state is rebuildable from immutable audit or has a tested backup/restore contract.

### 3.4.1 Approved audit-retention controls

Execution, gate, order, fill, correlation, approval, and position-action audit records SHALL be retained for at least one year or longer under approved policy in encrypted lake storage with the following minimum controls:

| Control | Requirement | Status |
| --- | --- | --- |
| WORM/immutability | WORM-equivalent immutable storage on the approved audit prefix. **2026-08-14: the configured store is Cloudflare R2, which does not implement the S3 Object Lock API but does support bucket locks — prefix retention rules via the Cloudflare dashboard/Wrangler/API.** | `OPEN` (R2 bucket-lock rule not yet applied, 2026-08-14) |
| Legal hold | Freeze/unfreeze procedure documented; hold preserves all versions within the hold scope; tested | `EVIDENCE-BLOCKED` |
| Key rotation | Audit encryption keys rotate on the same schedule as production credential rotation; previous keys remain available for decryption | `EVIDENCE-BLOCKED` |
| Access review | Audit-role access reviewed at least quarterly; every access is logged as an immutable audit event | `OPEN` |
| Retrieval SLA | Any single audit record or logical transaction reconstructable within 15 minutes from cold storage | `EVIDENCE-BLOCKED` |
| Export format | Audit exports use the reconciled Iceberg table format with content-hash verification; Parquet is an acceptable secondary format | `OPEN` |
| Reconstruction integrity | Every audit event carries a content hash; EOD manifest hashes chain to individual event hashes; a reconstruction integrity test SHALL verify the hash chain end-to-end. **2026-08-14: `AuditHashChain` (event→manifest→root hash chain, unit-tested) is implemented; the EOD-manifest end-to-end proof still needs the EOD controller (SCH-23)** | `EVIDENCE-BLOCKED` (hash-chain core implemented 2026-08-14) |
| Deletion | Audit deletion before the approved retention period is prohibited unless an approved retention-policy change, applicable legal-hold release, and single-operator (Saurabh, DEC-044) authorization are recorded as immutable deletion-evidence events. **2026-08-14: `AuditDeletionControl` implements this — single authorized operator `saurabh` (DEC-044) plus, inside the window, an approved policy change and a legal-hold release; every attempt emits an immutable deletion-evidence event** | `IMPLEMENTED` (2026-08-14, `AuditDeletionControl`) |

Compliance acceptance SHALL include: object-lock enforcement on a test prefix, legal-hold freeze/release cycle, key rotation without audit loss, access-review audit trail, retrieval SLA measurement, export-and-reconstruct integrity, and unauthorized-deletion rejection. **2026-08-14: on Cloudflare R2 the object-lock enforcement acceptance maps to 'bucket lock' rules (indefinite, audit prefix) applied via the Cloudflare dashboard/Wrangler/API; `audit_r2.py validate` verifies bucket/lifecycle/object-I/O via the S3 API and reads bucket-lock state via the Cloudflare API (CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID).**

## 3.5 State and safety invariants

1. One immutable `instruction_id` maps to one immutable request definition.
2. No money-moving call occurs without an `ENABLED` gate and matching current epoch.
3. Unknown broker outcome remains reserved, halts, and is reconciled before retry/release.
4. `instruction_id`, `client_order_ref`, and `broker_order_id` remain distinct and durably mapped.
5. Order lifecycle and position lifecycle are separate aggregates.
6. Older/out-of-order postbacks cannot regress authoritative lifecycle/position state.
7. No source data expires before verified EOD offload plus the minimum live buffer.
8. The authorized single operator (Saurabh, DEC-044) approves every post-reconciliation resume.

## 3.6 Security and privacy

- TLS is required for broker, S3, observability, and cross-host production traffic where supported; unencrypted cross-host secrets or money-moving data are prohibited.
- Data at rest is encrypted for Fluss volumes, S3 checkpoints, lake/audit, secrets, and Executor state.
- Local development uses ignored `.env`; production uses Docker Swarm secrets.
- Least-privilege identities separate ingestion, Flink, Action Capture, Executor, observability, and operators.
- Logs/traces redact credentials, tokens, raw payloads, and unnecessary account identifiers.
- Audit access is role-restricted and itself audited.
- Credential rotation/revocation, unauthorized resume, secret exposure, and expired-token scenarios are tested and alerted.

## 3.7 Maintainability and compatibility

Exact Flink, Fluss, Java, Python, broker protocol/SDK, and Arrow REST versions are mandatory release inputs. Upgrade requires compatibility tests, schema/serializer analysis, savepoint/restore proof, rollback plan, and updated evidence.

Every requirement, DDL, contract, test, metric, and runbook SHALL use the same identity/state vocabulary. Documentation link/schema consistency is validated in CI.

## 3.8 Observability SLO evidence

The platform SHALL expose enough metrics, logs, correlation IDs, immutable audit, alerts, and health states to prove every acceptance gate. Missing observability for an invariant makes that invariant unproven and blocks live-money release.

## 3.9 Latency event boundaries

The platform SHALL record distinct timestamps for `trigger_consumed_at`, `decision_created_at`, `sink_submit_at`, `sink_ack_at`, `instruction_visible_at`, `checkpoint_commit_at` when applicable, and `executor_received_at`. The decision-latency SLO SHALL use the explicitly defined trigger-to-visibility pair. Checkpoint durability and Executor receipt SHALL be reported as separate boundaries.

## 3.10 RPO and failure clocks

RPO SHALL be defined per durable boundary for raw accepted packets, immutable instructions, postback audit, Executor attempts/audit, projections, and EOD data. Each failure test SHALL record fault injection time, detection time, gate-block time, recovery-complete time, and data/effect loss. “Report RPO/RTO” without an acceptance threshold is insufficient.

## 3.11 State and capacity budgets

Every managed or durable state item SHALL have a cardinality bound or evidence-gated measurement plan, serialized-size estimate, cleanup trigger, checkpoint contribution, restore size/time, and skew behavior. Capacity acceptance SHALL include post-one-workload-VM-loss resources, Flink catch-up rate, checkpoint bandwidth, Fluss quorum/re-replication, maximum backlog, and sustained 50,000 ticks/s (3,000 instruments) operation.

### 3.11.1 Co-located resource monitoring

Flink and Fluss sharing workload VMs is a deliberate capacity risk. The platform SHALL monitor co-located resource consumption as a named risk, not an invisible default:

- Checkpoint completion/failure, checkpoint duration, checkpoint size, restore count, state size, and state cleanup rate
- Processing delay, backpressure, operator busy/idle time
- CPU utilization, heap/non-heap memory use, total container memory against the 85% alert threshold
- SSD space consumption, disk I/O saturation, and network bandwidth use
- Fluss append/fetch latency, replication lag, tablet health

Numerical resource alert thresholds (beyond the existing documented limits) SHALL be set only after `PERF-PROD-60000-001` and `PERF-STATE-CHECKPOINT-60000-001` establish production-rate baselines. Until then, thresholds are placeholders, not promises.

Required response to checkpoint or resource threshold breach:

1. Alert immediately.
2. Suppress new decisions or halt orders only when an existing safety invariant is breached (checkpoint failure, state corruption, safe-halt condition).
3. Do not invent a new automatic shutdown rule beyond the existing safety invariants.

| Evidence ID | Purpose | Status |
| --- | --- | --- |
| `PERF-STATE-CHECKPOINT-60000-001` | State growth, checkpoint stability, and co-located resource use at 50,000 ticks/s (3,000 instruments) | `EVIDENCE-BLOCKED`; live-money blocking |
