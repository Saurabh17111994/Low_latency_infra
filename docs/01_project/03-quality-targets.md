# Quality Targets and Delivery Guarantees

## Performance SLOs

Every SLO must report p50, p95, and p99, the workload profile, UTC clock source, sample duration, and whether failures/restarts are included.

| Stage | Target | Measurement boundary |
| --- | ---: | --- |
| Decode to raw append acknowledgement | < 50 ms target | Broker event received → `raw_table_1` acknowledgement (includes ≤ 20 ms transport linger) |
| ~~Trigger tick to winner commit~~ | ~~**p99 < 100 ms**~~ | ~~Signal-triggering tick consumed by Flink → `Trade_Decisions` commit at 50,000 ticks/s~~ — **REMOVED 2026-08-15 (CHG-005 — decision feed out of scope, not deferred)** |
| Winner commit to Executor receipt | Baseline required | Instruction commit → Executor changelog receipt; set release threshold from the pinned connector benchmark |
| Broker REST call | Baseline required | Arrow REST request start → verified broker response; separate from stream SLO and evidence-gated |
| Data-path recovery | < 30 s target | Failure detection → ingestion and Flink processing resumed |
| Safe-halt | < 5 s target | Uncertain-state detection → order gate blocks new submissions |
| EOD offload | < 30 min target | Market close → verified Iceberg commit and manifest |

The 15-second event-time candle wait is a business/windowing characteristic, not hidden latency. Report it separately as `window_end → candle publication` and `trigger_tick → winner_commit`.

Every latency report MUST include p50, p95, p99, UTC clock source, test duration, instrument count (3,000), the declared workload profile and observed total tick rate, failure/restart inclusion, exact software versions, and VM specification. Internal diagnostic timestamps (source receipt, raw visibility, Signal-job consumption, ~~candidate/ranking evaluation, winner commit~~ — REMOVED 2026-08-15 CHG-005) are recorded for diagnosis but are not independent release gates. (**The 100 ms p99 trigger-to-commit target is REMOVED 2026-08-15, CHG-005.**)

## Workload envelope

The active instrument manifest is fixed at **3,000 instruments** for a trading session (runtime manifest changes require a controlled restart). Market-data arrival is variable: an instrument may receive fewer than ≈16.7 ticks/s, and no instrument may exceed **20 ticks/s**. The expected baseline is an average of **≈16.7 ticks/s per instrument** over the declared measurement window, or **50,000 ticks/s** across the manifest. The acceptance gate is **60,000 ticks/s sustained** (3,000 × 20, DEC-045); the 90,000 ticks/s peak campaign is RETIRED (DEC-036). Arrival spacing is not fixed and a 50 ms per-instrument schedule is prohibited as a production claim.

Final machine sizing (CPU, RAM, disk I/O, network bandwidth) is evidence-gated by `PERF-PROD-60000-001` and the one-workload-VM-loss test. Current deployment allocations (250 GB SSD per VM) are a starting point, not a proven sizing result.

| Scenario | Tick rate | Duration | Purpose |
| --- | ---: | --- | --- |
| Variable baseline | 50,000 ticks/s average (3,000 instruments; ≈16.7 ticks/s/instrument average) | ~~30-minute production-manifest test~~ REMOVED (DEC-037, 2026-08-13); certified at the synthetic hot-path envelope | Required latency release gate |
| Capacity peak | ~~60,000 ticks/s~~ RETIRED (DEC-036); theoretical cap ceiling (3,000 × 20) for generator stress only | — | No peak-capacity acceptance evidence required |

Synthetic workloads must preserve variable per-instrument arrivals, maintain the declared average/cap, and record the generator seed/profile. The p99 <100 ms release target applies at the baseline; the gate proves safety and boundedness rather than inventing a second latency threshold (the peak campaign is retired, DEC-036). The 30-minute 3,000-instrument production-manifest run is REMOVED from acceptance (DEC-037, 2026-08-13 — user decision: not to be tested); the 50k gate is certified at the synthetic hot-path envelope.

The platform must meet the production latency SLO at the 50,000 ticks/s baseline and must verify bounded backlog, backpressure, checkpoint stability, recovery, and absence of acknowledged data loss at that gate (the 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045).

### Production-workload evidence record

| ID | Purpose | Status |
| --- | --- | --- |
| `PERF-PROD-60000-001` | Daily declared-duration run at 3,000 instruments and 50,000 ticks/s with complete resource/checkpoint/latency report | `DEFERRED`; not a live-money blocker for the current testing phase. The current phase validates the 1,024-instrument / single-connection configuration. |
| `PERF-PROD-90000-001` | RETIRED with the peak campaign (DEC-036, 2026-08-13) — was: declared-duration peak run at 3,000 instruments and 60,000 ticks/s | `RETIRED`; no peak-capacity evidence row remains |

## Delivery guarantees

| Boundary | Guarantee |
| --- | --- |
| Broker WebSocket → ingestion | At-least-once; broker-provided sequence is not assumed |
| Ingestion → Fluss LOG | At-least-once with client retry behavior |
| Tick deduplication | Bounded best-effort using an event fingerprint; not proof of exact identity when Arrow supplies no sequence/event ID |
| Flink managed state and Fluss sinks | Exactly-once only where the pinned Flink/Fluss integration tests prove the specific checkpointed boundary |
| Fluss command → Executor | At-least-once changelog delivery; Executor deduplicates by `instruction_id` / attempt identity |
| Executor → broker REST | At-least-once command submission unless broker idempotency/reconciliation proves effectively-once behavior |
| Fill capture | At-least-once postback processing with idempotent state updates and immutable audit events |

A broker REST call is never described as exactly-once merely because the producing Flink job is checkpointed.

### Broker connection recovery

Broker connection recovery MAY take up to **five minutes**. This objective does not override per-instrument freshness safety: an affected instrument is halted as soon as it becomes `STALE` or `UNKNOWN`, regardless of the reconnection objective. A disconnected broker does not permit trading on stale data — the affected instruments halt immediately when their freshness rule fails, and the remaining healthy instruments continue trading.

## Order-gate safe-halt

The Executor owns the final gate before every new-order or position-changing broker call.

### Broker REST response timeout

An Arrow REST request without a verified response after **15 seconds** SHALL become an `UNKNOWN` outcome. The Executor SHALL immediately: persist the attempt as `UNKNOWN`, write an immutable `Execution_Audit` event, halt all new orders globally, and initiate reconciliation before any retry or resume. The complete timeline is: request starts → 15-second timeout → detection → gate block (≤5 seconds from detection) → reconciliation.

### Gate state machine

```text
HALTED
  → RECONCILING(evidence_hash, epoch)
  → APPROVAL_PENDING(first_approver)
  → ENABLED(second_distinct_approver)
  → HALTED(reason, detected_at, next_epoch)
```

The gate enters `HALTED` for duplicate-order risk, unknown submission outcome, checkpoint failure affecting the order path, stale signal state, missing fill correlation, or failed changelog continuity. A restart with unverifiable gate state defaults to `HALTED`.

### Automatic restart recovery

Data-path services (Ingestion, Flink jobs) MAY recover automatically within **5 minutes**, subject to their own readiness checks.

Executor auto-resume after a restart is permitted **only** when durable evidence proves ALL of the following:

1. Known gate state and epoch (not corrupted or missing)
2. One valid fencing owner with durable epoch/token
3. No `UNKNOWN` attempts (all attempts are in a terminal resolved state)
4. No unresolved order correlations (every `client_order_ref` has a verified `broker_order_id` mapping)
5. Healthy changelog continuity (no gaps, valid consumer position)
6. Healthy Signal-job/checkpoint evidence (recent successful checkpoint, no state corruption)
7. Fresh mandatory health signals (Signal job, Action Capture, Ingestion all report healthy)

If ANY proof is missing, the Executor remains `HALTED`. Reconciliation and the existing approved-resume path apply. Executor auto-resume SHALL produce an immutable audit event and OpenObserve notification including the evidence hash and every check performed. Do not leave conflicting active text that "every restart requires single-operator (Saurabh, DEC-044) approval" alongside this conditional rule — auto-resume is permitted only with complete proof; without it, the single-operator (Saurabh, DEC-044) approval path applies.

Resumption requires:

1. Broker order reconciliation
2. Open position and fill reconciliation
3. Changelog offset / consumer-health verification
4. Confirmation that the signal job and checkpoints are healthy
5. Resolution of every unknown attempt (**and, pre-2026-08-15, reservation — REMOVED CHG-005**)
6. Single-operator (Saurabh, DEC-044) approval of the same gate epoch and evidence hash

Existing positions may be monitored during a halt, but no new order is submitted until the gate returns to `ENABLED`.

## Per-instrument freshness

A data-quality problem (stale data, broker disconnect, or unknown instrument state) halts decisions and orders for affected instruments only — it does not halt unrelated healthy instruments.

### Freshness states

Every instrument SHALL have a versioned `instrument_freshness` state with at minimum:

| State | Meaning | Effect on orders |
| --- | --- | --- |
| `FRESH` | Recent verified data received within the configured threshold | Eligible for decisions |
| `STALE` | No recent verified data within the configured threshold | Blocked for decisions and orders |
| `UNKNOWN` | Cannot determine freshness (source unavailable, unverifiable) | Blocked for decisions and orders |
| `MARKET_CLOSED` | Exchange session is closed | Blocked for decisions and orders |

A `STALE` or `UNKNOWN` instrument blocks new decisions and new orders for that instrument only. Unrelated healthy instruments remain eligible. The exact freshness signal type and threshold are evidence-gated (`EVIDENCE-GATE-FRESHNESS-001`) because some instruments may legitimately be quiet; no numeric threshold is assigned until proven by broker-message freshness, trade-tick freshness, quote freshness, and exchange/session behaviour evidence.

### Automatic recovery

An instrument transitions from `STALE` or `UNKNOWN` back to `FRESH` automatically only after fresh data remains stable for the configured evidence-backed recovery period. A new session begins each trading day. Recovery evidence SHALL be auditable.

### Scoped halt path

A scoped, auditable safety-halt request SHALL flow from the data-quality owner to the Executor for affected instruments. The Executor gate's global halt (for unknown broker outcomes, checkpoint failures, etc.) remains a separate, independently enforced safety layer.

## Source links

- Non-functional requirements: [`../02_requirements/03-non-functional.md`](../02_requirements/03-non-functional.md)
- Compute guarantees: [`../02_requirements/02-functional/03-compute.md`](../02_requirements/02-functional/03-compute.md)
- Executor contract: [`../02_requirements/02-functional/07-executor.md`](../02_requirements/02-functional/07-executor.md)
