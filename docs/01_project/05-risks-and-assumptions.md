# Risks and Assumptions Register

Risks and assumptions are actionable records, not passive prose. Every open item needs an owner, validation evidence, and a decision or mitigation before the affected production capability is accepted.

---

## ID namespacing

| Namespace | Scope | Defined in |
| --- | --- | --- |
| `ASM-001`–`ASM-999` | Project-level assumption | This register only |
| `RISK-001`–`RISK-999` | Project-level risk | This register only |
| `ASM-ING-*`, `ASM-STOR-*`, `ASM-CMP-*`, `ASM-BL-*`, `ASM-RNK-*` | Domain-scoped assumptions | Individual functional requirement files |
| `ASM-NFR-*`, `ASM-DATA-*`, `ASM-IF-*`, `ASM-OPS-*`, `ASM-EXE-*`, `ASM-PF-*` | Cross-cutting assumptions | Non-functional, data, interface, ops, executor, platform requirement files |

Domain-scoped and cross-cutting assumption IDs are defined in their owning requirement files and SHALL NOT be redefined here. Project-level IDs (`ASM-*`, `RISK-*`) are defined only in this register. Other documents SHALL reference project-level IDs by name without redefining them.

---

## Evidence levels

Every validation claim SHALL use exactly one of these evidence levels:

| Level | Name | Meaning | May close a risk? |
| --- | --- | --- | --- |
| 1 | **Documented by vendor** | Official spec, API doc, or vendor communication describes the behavior | No — insufficient alone for runtime risks |
| 2 | **Observed in capture** | Behavior observed in one or more real sessions or packet captures | No — may close documentation-level assumptions only |
| 3 | **Tested in sandbox** | Intentional test in a controlled non-production environment exercises the behavior | Yes — when test covers all required conditions |
| 4 | **Validated in production-like environment** | Reproduced under production workload, topology, versions, and failure modes | Yes — definitive |

A vendor document (Level 1) SHALL NOT alone close a runtime-behavior risk. A risk marked as mitigated with only Level 1 or Level 2 evidence SHALL remain open for a production-validation gate unless an explicit risk acceptance is recorded.

---

## Open risks

| ID | Severity | Risk | Owner | Trigger / evidence required | Mitigation or acceptance condition | Current status | Status date | Previous status | Evidence reference |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RISK-001 | Critical | Arrow exposes no stable tick sequence/event ID; fingerprint dedup can collapse identical legitimate ticks or miss semantically duplicate packets. | Ingestion | Captured packet corpus and broker protocol confirmation (Level 1+2 complete; Level 3 pending) | Quantify false-positive/false-negative rate; version the fingerprint and TTL; accept bounded best-effort semantics | Open | 2026-07-23 | — | Arrow binary protocol confirmed (Level 1); HFT LTPC/Full capture analysis pending (Level 2) |
| RISK-002 | Critical | Broker accepts an order but Executor crashes before recording acknowledgement. | Execution | Fault injection between REST response and durable state update (Level 3) | Durable attempt ledger, broker reconciliation before retry, broker idempotency key if available | Open | 2026-07-23 | — | — |
| RISK-003 | Critical | The order path does not halt consistently during uncertain state. | Platform + Execution | Safe-halt chaos test (Level 3) | Durable gate enforced before every broker call; halt within 5 seconds; explicit resume checks | Open | 2026-07-23 | — | — |
| RISK-004 | Critical | `client_order_ref` too long for Arrow `remarks` field (max 16 chars). | Execution + Action Capture | Arrow API documentation | Risk retired — Arrow REST docs confirm `remarks` is max 16 chars and echoed in REST responses and WS order updates | Closed | 2026-07-23 | Open | Arrow REST API spec (Level 1); DEC-012 |
| RISK-005 | High | EOD offload failure races short Fluss retention. | Storage | Failed/offline S3 test spanning EOD (Level 3) | Multi-day raw retention buffer, verified commit manifest, retries and pre-expiry alert | Open | 2026-07-23 | — | — |
| RISK-006 | High | Shared checkpoint storage is a production single point of failure. | Platform | Loss of checkpoint host/share (Level 3) | Redundant durable checkpoint storage or explicit reduced-HA acceptance | Open | 2026-07-23 | — | — |
| RISK-007 | Medium (deferred) | Variable broker arrivals can exceed the 50,000 ticks/s average baseline; unmeasured cluster capacity could under-size the cluster or falsely indicate readiness. (The 90,000 ticks/s capacity-peak campaign is retired, DEC-036; the 60,000 ticks/s gate is current, DEC-045.) | Performance | `PERF-PROD-60000-001` is deferred to the 3,000-instrument phase (`PERF-PROD-90000-001` retired, DEC-036). The current testing phase validates the 1,024-instrument / single-connection configuration. | Reassess before the premium-tier 3,000-instrument phase | Open (deferred) | 2026-07-31 | — | — |
| RISK-008 | High | Fluss connector semantics may not provide atomic visibility across all LOG and KV outputs. | Compute + Storage | Version-specific connector test (Level 3) | Document per-table visibility and design consumers for partial visibility if cross-table atomicity is unavailable | Open | 2026-07-23 | — | — |
| RISK-009 | Medium | Swarm overlay latency may violate the decision-path p99 target. | Platform | Production-like multi-VM benchmark (Level 3+) | Placement optimization and measured p99 under load | Open | 2026-07-23 | — | — |
| RISK-010 | Medium | Three Fluss/Flink nodes may not tolerate one VM loss as configured. | Platform | `FAIL-VM-LOSS-60000-001` per-node chaos matrix (Level 3) | Set replication/quorum/placement explicitly and prove one-node tolerance | Open | 2026-07-23 | — | — |
| RISK-011 | Critical | Current physical DDLs encode superseded identities, sequence assumptions, ownership, and retention and cannot be safely applied. Needs regeneration against pinned Fluss 0.9 + Flink 2.2. | Platform + Storage | Pinned-version DDL apply and parity suite (Level 3) | Generate clean-break DDLs from reconciled logical schemas. Fluss 0.9 features (BYTES, partial_update, changelog images) are confirmed (Level 2). | Mitigated | 2026-07-23 | Open | Fluss 0.9.0 feature confirmation (Level 2); DDL generation unblocked. Full DDL parity suite pending (Level 3). |
| RISK-012 | High | Single-operator (Saurabh, DEC-044) gate resume and Executor fencing may be bypassed by incomplete identity/access-control implementation. | Platform + Execution | Authorization, concurrency, and adversarial control tests (Level 3) | Distinct authenticated approvals bound to epoch/evidence hash; immutable audit; one fenced active Executor per account/order partition | Open | 2026-07-23 | — | — |
| RISK-013 | High | The approved policy-controlled money-moving audit retention may not satisfy final legal/regulatory policy or deletion obligations. | Platform + Compliance | Legal/policy review (Level 1) | Encrypt, restrict and audit access; approve the one-year minimum or longer retention before live-money release | Open | 2026-07-23 | — | — |
| RISK-014 | High | Seven-day SSD capacity for 3,000 instruments at the 50,000 ticks/s average baseline may exceed the 250 GB per-VM allocation (allocation reduced from 500 GB on 2026-09-21, CHG-280 — this raises, not lowers, the chance this risk materialises; the 90,000 ticks/s peak model is retired, DEC-036; 60,000 ticks/s gate, DEC-045), including replication, checkpoints, and the selected slow-Fluss buffer policy. | Storage + Platform | Baseline capacity model (`STOR-7DAY-90000-001` retired with the peak campaign, DEC-036; Level 3) | Measure projected data volume at the baseline profile; add warning, critical, and stop thresholds; do not guess byte values; ensure retention ceiling does not trigger unsafe expiry | Open | 2026-07-29 | — | — |
| RISK-015 | Low (resolved) | The slow-Fluss policy may not operate as designed under production conditions. | Ingestion + Platform | Resolved by capacity: Fluss ingests up to 1-2M ticks/s vs the platform’s theoretical ceiling (90K tps; sustained gate 50K, DEC-036). Bounded pending-append limits remain as defensive backpressure. | No buffer or pause mechanism required; monitor pending-append limits as the capacity-fault bound | Closed | 2026-07-31 | — | — |
| RISK-016 | Medium | The per-instrument freshness threshold may be misconfigured — too sensitive (false STALE during normal quiet periods) or too loose (trading on stale data). | Platform + Data Quality | `EVIDENCE-GATE-FRESHNESS-001` broker-message freshness, trade-tick freshness, quote freshness, and exchange/session behaviour evidence (Level 3) | Define signal type and threshold from evidence; version the freshness state; scope halts to affected instruments only; do not guess a numeric threshold | Open | 2026-07-23 | — | — |
| RISK-017 | Medium | Conditional Executor auto-resume may either fail to auto-resume when safe (operational delay) or auto-resume when unsafe (duplicate order risk). | Execution | `REC-EXEC-SAFE-AUTO-001` restart scenarios with complete and incomplete proof sets (Level 3) | Prove all seven conditions are necessary; prove auto-resume is blocked when any condition is missing; audit every auto-resume event | Open | 2026-07-23 | — | — |
| RISK-018 | Medium | Stop-loss orders cannot pass through the execution bridge: `SL-LMT`/`SL-MKT` are refused because the command envelope and fingerprint carry no stop trigger, so a position cannot be protected by a stop-loss placed through this bridge. | Execution | A caller needing stop-loss orders through the bridge, or the live-money release review of exposure management | Fail-closed refusal with an explicit error (an SL order is never submitted with its trigger silently dropped) until stop-trigger carriage lands end to end — envelope, fingerprint, Rust protocol, mapping — as a coordinated deploy | Open (deferred) | 2026-09-13 | — | `06_execution_bridge/go-bridge/models.go` refusal (P3-043, `8216307`); `arrow.OrderRequest.TriggerPrice` exists in the pinned SDK (`third_party/go-arrow/arrow/orders.go:27`) |

---

## Assumptions requiring validation

| ID | Assumption | Owner | Validation method | Current status | Status date | Previous status | Evidence level | Evidence reference |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ASM-001 | TCP preserves order within each Arrow WebSocket connection, but the feed has no usable sequence number. | Ingestion | Protocol review and capture analysis | Validated | 2026-07-23 | Partially confirmed | Level 2 | Arrow binary protocol: HFT LTPC/Full modes confirmed. No packet/event ID in any mode. DEC-012 updated. |
| ASM-002 | Arrow postbacks expose `broker_order_id`, lifecycle status, and the submitted `remarks` value. | Action Capture | REST API docs + WebSocket order-updates spec review; sandbox order round-trip (Level 3 pending) | Partially validated | 2026-07-23 | To verify end-to-end | Level 1 | Order update WS objects include `exchangeOrderID`, `orderStatus`, `reportType`, `fillShares`, `averagePrice`, and echoed `remarks`. End-to-end round-trip evidence (Level 3) pending. |
| ASM-005 | The production Swarm workload VMs (v1: 3 Manager+Worker; v2: N≥3 Worker VMs, managers isolated) can sustain the variable 50,000 ticks/s average baseline and safely bound/recover when arrivals exceed it while one HA VM is unavailable. (The 90,000 ticks/s peak campaign is retired, DEC-036; 60,000 ticks/s gate, DEC-045.) | Platform | Baseline/peak production-like load plus node-loss tests (Level 4) | To verify | 2026-07-29 | — | — | — |
| ASM-006 | S3 `ap-south-1` can complete verified EOD offload within 30 minutes. | Storage | Full-volume offload test (Level 3+) | To verify | 2026-07-23 | — | — | — |
| ASM-008 | The selected Fluss version supports the required binary payload type, KV state tables, changelog behavior, replication, retention extension, and lake properties. | Storage | Pinned-version DDL and integration suite (v1.0.0; re-validation in progress) | Partially validated | 2026-09-22 | Validated | Level 2 | Fluss 0.9.1-incubating confirms: `BYTES` ✅, KV+partial_update ✅, FULL changelog ✅, `$changelog` virtual tables ✅, replication ✅, retention extension ✅, Iceberg lake tiering ✅, ARRAY/MAP/ROW ✅. 1.0.0 note 2026-09-22: gate suites green on 1.0.0 images; full Level-3 suite pending final certificate. |

---

## Status history

Historical statuses for IDs that have changed. These are not active records.

| ID | Previous status | Previous status date | New status | Status date | Reason |
| --- | --- | --- | --- | --- | --- |
| RISK-004 | Open | (prior) | Closed | 2026-07-23 | `remarks` field max length confirmed per Arrow API spec; DEC-012 |
| RISK-011 | Open | (prior) | Mitigated | 2026-07-23 | Fluss 0.9 + Flink 2.2 versions pinned; DDL generation unblocked |
| ASM-001 | Partially confirmed | (prior) | Validated | 2026-07-23 | 4-mode protocol analysis complete; no packet/event ID found |
| ASM-002 | To verify end-to-end | (prior) | Partially validated | 2026-07-23 | API docs + WS spec reviewed; sandbox round-trip pending |
| ASM-008 | To verify | (prior) | Validated | 2026-07-23 | Fluss 0.9.0 feature confirmation complete |

---

## Superseded decisions and assumptions

Items removed from active tables because the architecture or technology they reference is no longer part of the system. These are preserved for audit traceability only and SHALL NOT be treated as active constraints.

| ID | Description | Status | Superseded date | Reason |
| --- | --- | --- | --- | --- |
| ASM-007 | ~~OpenAlgo exposes deterministic order-submission responses needed for reconciliation.~~ | Obsolete | 2026-07-23 | OpenAlgo removed from architecture (DEC-006 revised). Arrow REST `POST /order/regular` returns `{status:"success", data:{orderNo, requestTime}}`. Reconciliation via `GET /user/orders`, `/user/trades`, `/user/positions` (DEC-023). |

---

## Review cadence

- Critical open risks block live-money production acceptance.
- High risks require mitigation evidence or explicit risk acceptance before production.
- Update this register when evidence changes; do not duplicate stale assumptions in the charter.
- Link test evidence or issue IDs in the evidence reference column when available.
- When a risk or assumption status changes: update the row in the active table, then append an entry to Status history. Do not create a duplicate row.
- Every evidence claim in the Status field SHALL use the four-tier evidence model. Level 1 (Documented by vendor) alone is insufficient to close a runtime risk.
