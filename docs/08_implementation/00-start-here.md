# Implementation Guide — Start Here

<!-- markdownlint-disable MD013 -->

## Purpose

This directory converts the stable project requirements and build contracts into implementation-ready engineering dossiers. It defines **how an implementer must realize and prove the system**, while the upstream documents remain authoritative for **what the system must do**.

No document in this directory proves that code exists or that a runtime guarantee has passed. Runtime claims require the evidence records defined here.

## How to use this folder

Build the project in numeric order. Each phase file contains **what to build** and a **verification mapping**; [`11-testing-and-release.md`](./11-testing-and-release.md) contains the detailed test procedures, evidence requirements, and final release evidence. Finish the mapped tests for one phase before moving to the next. `01-foundation.md` contains shared rules needed before building.

## Authority and conflict rule

Use the repository authority order from [`../01_project/00-index.md`](../01_project/00-index.md):

1. Executable implementation and tests
2. Active architectural decisions
3. Validated authoritative DDLs
4. Build contracts
5. Detailed requirements
6. Implementation dossiers and summaries

An implementation dossier must not silently redefine an upstream requirement. If implementation detail exposes a conflict, record it in [`01-foundation.md`](./01-foundation.md) and keep the affected work blocked until the authoritative layer is reconciled.

The 2026-08-14 doc-consistency reconciliation is recorded as [`DEC-039`](../01_project/04-decisions.md). The dossiers in this directory implement its settled facts: HFT feed modes are canonical `ltpc` (40 B) + `full` (196 B); timestamps are canonical epoch milliseconds; `Postback_Projection_Ledger` is included in the MVP build; `Safety_Halt_Requests` is a KV control table; the acceptance matrix is fully mapped (132 requirements / 152 acceptance tests).

## Documentation-first workflow

```text
implementation task
  → authoritative requirement/decision identified
  → implementation dossier completed
  → unresolved external facts recorded as evidence gates
  → design review approved
  → code and DDL implementation
  → focused tests and operational evidence
  → implementation task verified and struck through
```

Documentation-complete is not code-complete. A checklist item may be marked documentation-complete only when its dossier, traceability, test cases, and unresolved evidence gates are explicit.

## Dossier map

| Dossier | Purpose | Primary plan phases |
| --- | --- | --- |
| [`01-foundation.md`](./01-foundation.md) | Plan, governance, software versions, storage rules, and shared safety rules | 0–2 |
| [`02-schema-storage.md`](./02-schema-storage.md) | Data tables/storage instructions and their test design | 3 |
| [`03-ingestion.md`](./03-ingestion.md) | Ingestion instructions and test design | 4 |
| [`04-signal-job.md`](./04-signal-job.md) | Signal-job instructions and test design | 5 |
| [`05-execution-core.md`](./05-execution-core.md) | Execution Core — integrated Action Capture + Babysitter + Executor (Nautilus → go-arrow bridge → Arrow) | 6–7 |
| [`08-local-compose.md`](./08-local-compose.md) | Local runtime instructions and test design | 8 |
| [`09-production-swarm.md`](./09-production-swarm.md) | Production runtime instructions and test design | 9 |
| [`10-observability.md`](./10-observability.md) | Monitoring/operations instructions and test design | 10 |
| [`11-testing-and-release.md`](./11-testing-and-release.md) | Master test list, traceability, and final release evidence | 11–12 |
| [`15-ingestion-test-hardening.md`](./15-ingestion-test-hardening.md) | Ingestion test-hardening backlog — additional robustness tests mapped to `03-ingestion.md` aspects (2026-08-15 audit) | 4 (ingestion) |
| [`../commands/COMMANDS.md`](../commands/COMMANDS.md) | Command reference — every project command entry point, categorized functionally (lifecycle / trading / testing / ops / prod); 4-command fast path for day-to-day use | — |
| **Roadmap** | Step-by-step plans live INSIDE their dossiers (2026-08-13 merge): current build plan → `04-signal-job.md` (appended section); P7 bench + completed gaps → `11-testing-and-release.md`; P10 rehearsal → `09-production-swarm.md`; executor/execution-core plan → `05-execution-core.md` | — |

## Document status vocabulary

Every dossier SHALL include a status banner with these four dimensions (defined in [`01-foundation.md`](./01-foundation.md)):

| Dimension | Options |
| --- | --- |
| **Design status** | `Draft`, `Design-ready`, `Evidence-blocked`, `Superseded` |
| **Implementation status** | `Not-implemented`, `Implementing`, `Implemented`, `Validated` |
| **Evidence status** | `Untested`, `Tested-in-sandbox`, `Production-validated` |
| **Live-money status** | `Blocked`, `Approved-for-testing`, `Live` |

A dossier may be Design-ready, Evidence-blocked, and Live-money blocked simultaneously: implementation can proceed behind an adapter or disabled gate, but live-money release cannot.

The previous single-axis status vocabulary (`Draft`, `Design-ready`, `Implementation-ready`, `Evidence-blocked`, `Validated`, `Superseded`) is superseded by the 4-dimension banner above. Existing dossiers using the old vocabulary remain valid until their next revision.

## Current readiness — reconciled 2026-08-21 (E4)

| Area | Design status | Implementation status | Evidence status | Live-money status |
| --- | --- | --- | --- | --- |
| Architecture and ownership | Design-ready | Implemented | Tested-in-sandbox | Blocked |
| Broker protocols | Design-ready | Implemented (error half VERIFIED: TOTP `execution-auth-001` len 238 + re-auth `reauth.go` + Arrow REST error 401/UNKNOWN/duplicate — `a1-*`/`a4-*`; success half `RCF-EQ ×1` live place proven 2026-08-25 `26082501010305` — sandbox margin shortfall ₹10500) | Tested-in-sandbox (error half) | Blocked |
| DDL/schema | Design-ready | Implemented (27 tables, `ddl_sha256` + `compatibility_class`, composite-PK matrix, 27/27 live on dev Fluss) | Tested-in-sandbox (`compat-fluss-*` + matrix verifier + live DDL drills) | Blocked |
| Ingestion | Design-ready | Implemented (285 ingestion + 489 common tests; losslessness + 1800 s soak proven) | Tested-in-sandbox (10,716 rows fake→Fluss, 49k tps synthetic envelope, `full-audit` C6 `489/285/419`) | Blocked |
| Signal job | Design-ready | Implemented (Slice 1 candles + Slice 2.1 signal LIVE smoke — 205k candles/1,074 instruments/48 ckpt; `SIG-FAIL-001` ckpt-failure + `feature_candles_15s` KV-only) | Tested-in-sandbox (envelope + `make gate`/`full-audit` green) | Blocked |
| Execution Core (Action Capture + Babysitter + Executor — Nautilus + go-arrow bridge, 2026-08-21) | Design-ready (re-scoped CHG-028) | Implemented (WP-0..8 DONE: `LiveNodeRuntime` 1800 s soak B1, crash fence B2, gate lifecycle B3, durable 4 clients B7, clock drift B8; multi-conn C1..C4 synthetic `48,660 tps`; T9 `RCF-EQ ×1` live order proven to Arrow 2026-08-25 — `MARGIN ERROR`, sandbox unfunded) | Tested-in-sandbox (196 Rust lib + 18.7 s/1.12 s Go + 247 Java; `make gate` 13/13 2026-08-25) | Blocked |
| Local runtime | Design-ready | Implemented (`t8` 12/12 + `execution_network_check` PASS on `--profile execution-t3`; Swarm duties on holder) | Tested-in-sandbox | Blocked |
| Production runtime | Design-ready | Not-implemented (needs prod VMs D1 — `BLOCKED: needs prod VMs`) | Untested (`PERF-PROD-60000`/`FAIL-VM-LOSS`/`DR-001..006`/`D7` await prod stack) | Blocked |
| Test/evidence program | Design-ready | Implemented (`RELEASE_EVIDENCE_2026-08-21.md` + 13-item package; every AC has a pointer — E5c; `full-audit`/`pin-check`/`cep-check` green) | Tested-in-sandbox (`make gate` 2026-08-21 `ALL GATES PASSED`; **`make gate` re-captured 2026-08-25 13/13 PASS** `logs/soak/monday-gates-20260825-203040/` — CHG-103) | Blocked |

> **E4 note (2026-08-21, CHG-078):** this table is the single `Current readiness` truth for the laptop-now cut. Live-money stays `Blocked` for every row until E5 single-operator (Saurabh, DEC-044) sign-off. `Production runtime` honest `Not-implemented/Untested` — requires the VM era (`D1→D7`). No row is claimed `Production-validated` on a laptop.

## Mandatory implementation order

1. Approve documentation governance and cross-cutting invariants.
2. Record version/protocol evidence gates.
3. Validate DDL capability and schema lifecycle.
4. Implement ingestion and test fixtures.
5. Implement the Signal job.
6. Implement the Execution Core (Action Capture + Babysitter + Executor) as the last functional work, with broker calls disabled until safety tests pass — see `05-execution-core.md`.
8. Implement local integration runtime.
9. Implement production Swarm and operational controls.
10. Produce the release evidence package.

Implementation may overlap where dependencies permit, but no downstream task may invent an upstream contract.

## Per-phase execution guide

Each phase below follows a **contracts-first** workflow:

```text
for each phase:
  1. read the contracts in docs/04_contracts/   ← WHAT (binding spec)
  2. read the dossier in docs/08_implementation/ ← HOW (engineering plan)
  3. write code and DDL
  4. run focused tests + record evidence
  5. mark phase complete before moving on
```

If contract and dossier disagree, the contract wins. Flag the conflict in
[`01-foundation.md`](./01-foundation.md) and block the affected work.

---

### Phase 1: Mock Broker ✅ COMPLETED

| Step | Artifact | Status |
| --- | --- | --- |
| Contract | [`../04_contracts/arrow_broker.md`](../04_contracts/arrow_broker.md) (MKT data section) | Read |
| Dossier | [`11-testing-and-release.md`](./11-testing-and-release.md#performance-benchmark-procedure) | Read |
| Code | `code/02_services/05_mock_arrow/src/.../MockArrowServer.java` | ✅ Implemented |
| Tests | `MOCK-UNIT-001`, `MOCK-UNIT-002`, `MOCK-UNIT-003`, `MOCK-PERF-001` | Suite present (`SyntheticWorkloadTest`); executed indirectly via ingestion E2E; standalone MOCK-* evidence report pending |

---

### Phase 2: Ingestion ✅ COMPLETED (2026-08-09)

Status: Phases 2a-2g done — 713 tests (247 ingestion + 466 common historical 2026-08-25; corrected from 192/304 after the 2026-08-14 Standard-feed test deletion; common recounted 2026-08-15 at 177 historical 2026-08-15 = 160 + 17 — COMPAT-FLUSS-006 live bucket-skew probe +1, full-manifest routing identity +1, `KvStaleWriteRejectionTest` +7 (COMPAT-FLUSS-004 rejected/quarantined/audited half), plus 8 tests already in the tree but absent from the prior figure — and grown since to 464 via the SCH-23/SCH-20/SCH-24/SCH-15 additions + 2026-08-24 bump (CHG-003/005/006/007/2026-08-24, CHG-100 2026-08-25; docs-audit C6 line 466/247/388 (compute 388 — the 2026-08-17 Design-B merge `34af190` −19 DEC-038-era tests, then −10 CHG-023 item-1 emitter→native-reporter swap, then −2 CHG-023 item-2 native-TTL expiry swap, then −11 CHG-023 item-4 StallGuardedSink removal 2026-08-17, then +1 SIG-FAIL-001 checkpoint-failure test `SignalJobCheckpointFailureIntegrationTest` 2026-08-17; compute count corrected 2026-08-16, was 268)); ingestion +5 instrument-manifest-writer tests 2026-08-15 — ING-SCHEMA-002 + ING-INT-004; +3 hardening 2026-08-15 — ING-DQ-011 fuzz corpus (CHG-008/009/010); +4 M2 hardening 2026-08-15 — ING-UNIT-018 config parity (CHG-012); +13 M3 failure-path E2E 2026-08-15 — ING-FAIL-008 crash-loop, ING-FAIL-009 auth-failure, ING-FAIL-010 shutdown-deadlock, ING-INT-005 readiness matrix (CHG-017); +7 M4 Go-bridge/data-quality 2026-08-15 — ING-UNIT-014/015/016/017, ING-RES-002/003/004, ING-TCP-003 (CHG-018); +3 M5 telemetry/ops 2026-08-15 — ING-UNIT-021/022 OTLP scrubbing + cardinality (G6), ING-FAIL-006 warning-window throttle, plus the Go ING-UNIT-020 FATAL-message cases and the ING-INT-006 entrypoint harness (CHG-019); +1 CHG-032 2026-08-18 — ING-UNIT-023 bridge SIGTERM-drain regression (CHG-015 follow-up); +1 CHG-033 2026-08-18 — ING-UNIT-024 real JVM shutdown-hook layer (main-thread join, exit 143)), 0 failures, 8 ingestion env-gated skips (common 464/0/1-skip historical 2026-08-24; the 11 live-Fluss common integration tests run only with `FLUSS_BOOTSTRAP` set); E2E fake-broker → Fluss green (10,716 rows persisted, 58,951 ticks/s baseline probe on the 1,024-instrument envelope). Open items: 50k perf gate certified 2026-08-13 at the synthetic hot-path envelope (socket 49,242 tps / 0 wire loss; append 49,578 tps / p99 &lt; 5 ms; the 90k peak campaign is retired, DEC-036); ING-RES-001 real-backoff soak PASS (100/100 cycles, 2852.7 s, no leak; hub res001-soak, 2026-08-13); the 3,000-instrument / 3-connection production-envelope run is removed from acceptance (DEC-037) — see [`03-ingestion.md`](./03-ingestion.md) Status.

Read these **in order** before writing any code:

| Step | Artifact | Purpose |
| --- | --- | --- |
| **Contract** | [`../04_contracts/01-ingestion.md`](../04_contracts/01-ingestion.md) | What ingestion must do — per-tick append, backpressure, batching rules |
| **Contract** | [`../04_contracts/02-storage.md`](../04_contracts/02-storage.md) | `raw_table_1` contract — routing key, retention, append semantics |
| **Contract** | [`../04_contracts/arrow_broker.md`](../04_contracts/arrow_broker.md) (market section) | Broker protocol — binary frames, zstd, auth, subscription |
| **Dossier** | [`03-ingestion.md`](./03-ingestion.md) | How to build — state machines, config, failure modes, test IDs |
| **DDL** | `code/01_platform/02_sql/ddl/02_raw_table_1.sql` | Physical table schema |

**What to build:**

1. Go `arrow-bridge` (Arrow Go SDK → stdout **proto frames, T6 primary**; NDJSON via `TRANSPORT=pipe` rollback) pipes into Java `IngestionService` (sniff → validate → fingerprint → Fluss `raw_table_1` writer)
2. Reconnect loop with exponential backoff (epoch bump on reconnect)
3. Per-tick append latency tracking
4. Backpressure: stop accepting at `MAX_PENDING_APPEND_RECORDS` (50k) / `MAX_PENDING_APPEND_BYTES` (64MB)
5. Readiness probe: returns not-ready when backpressured

**Acceptance tests:** `FAIL-PENDING-001`, `STATE-DEDUP-001`

---

### Phase 3: Signal Job 🔴 NEXT

- > Head start: Slice 1 (raw source → validation → dedup → 15 s candles → `feature_candles_15s`) is implemented with 25 green tests and live-smoke-verified 2026-08-09 (205,146 candles, 1,074 instruments, 48 checkpoints); see [`04-signal-job.md`](./04-signal-job.md) §Slice 1 evidence. The slot-scoped safety consumer shell (`SafetyHaltJob` + `SafetyStateTracker` + `SuppressionGate` in `common`) is also implemented and live-verified — SAFETY-INT-001 passed 2026-08-09. Remaining: forming-bar handoff + Business Logic (Slice 2). ~~Ranking/Reservations/Decisions (Slice 3)~~ — **REMOVED 2026-08-15 (CHG-005, not deferred).**

Read these **in order** before writing any code:

| Step | Artifact | Purpose |
| --- | --- | --- |
| **Contract** | [`../04_contracts/03-compute.md`](../04_contracts/03-compute.md) | Signal job contract — sources, sinks, checkpointing |
| **Contract** | [`../04_contracts/04-business-logic.md`](../04_contracts/04-business-logic.md) | Feature compute, candidate detection, filtering rules |
| **Contract** | [`../04_contracts/10-ranking.md`](../04_contracts/10-ranking.md) | **REMOVED 2026-08-15 (CHG-005 — in-operator ranking out of scope, not deferred); stub retained for cross-reference** |
| **Dossier** | [`04-signal-job.md`](./04-signal-job.md) | How to build — state layout, dedup, candle (**ranking/reservation/decisions REMOVED 2026-08-15, CHG-005**) |
| **DDL** | `code/01_platform/02_sql/ddl/03_feature_candles_15s.sql`, `05_signal_candidates.sql` (**`06_ranking_results.sql`, `07_trade_decisions.sql`, `15_portfolio_reservations.sql` REMOVED from scope 2026-08-15, CHG-005 — DDL files retained as reserved schema: still in `schema_manifest.json` and applied by the 2026-08-24 scratch run, but never written by any job**) | Physical schemas |

**What to build (in-order, inside one Flink job):**

1. Feature compute: `raw_table_1` source → per-instrument `Candle15s` + `FormingBar`
2. Candidate detection: feature state → `SignalCandidate` (max 1 active per instrument)
3. ~~Ranking: in-operator, deterministic tie-break, rejection codes~~ — **REMOVED 2026-08-15 (CHG-005)**
4. ~~Reservation: portfolio capacity check → `ReservationState`~~ — **REMOVED 2026-08-15 (CHG-005)**
5. Decision publish: write `TradeDecisions` to Fluss changelog

**Acceptance tests:** `STATE-CANDLE-001`, `STATE-DEDUP-001` (at Flink level), perf tests

---

### Phase 4+5: Execution Core (Action Capture + Babysitter + Executor) 🔴 NEXT AFTER SIGNAL

Read these **in order** before writing any code:

| Step | Artifact | Purpose |
| --- | --- | --- |
| **Contract** | [`../04_contracts/06-action-capture.md`](../04_contracts/06-action-capture.md) | Postback consumer, lifecycle, position projection |
| **Contract** | [`../04_contracts/05-babysitter.md`](../04_contracts/05-babysitter.md) | Babysitter contract (MVP no-op) |
| **Contract** | [`../04_contracts/07-executor.md`](../04_contracts/07-executor.md) | Executor contract — gates, attempts, audit, safe-halt |
| **Contract** | [`../04_contracts/arrow_broker.md`](../04_contracts/arrow_broker.md) (postback + REST sections) | Postback protocol — WS JSON, identity mapping; Arrow REST — `POST /order/regular`, auth, response |
| **Dossier** | [`05-execution-core.md`](./05-execution-core.md) | **Single integrated engineering plan** — Nautilus execution core + go-arrow bridge (sole Arrow-facing component) + custom gate/fencing/correlation glue |
| **DDL** | `09_order_lifecycle.sql`, `10_positions.sql`, `16_postback_quarantine.sql`, `17_postback_projection_ledger.sql`, `11_execution_gate.sql`, `12_execution_attempts.sql`, `13_order_correlation.sql`, `14_execution_audit.sql`, `18_safety_halt_requests.sql` | Physical schemas |

**What to build (one execution core, per `05-execution-core.md`):**

1. go-arrow bridge (localhost) — the ONLY component that talks to Arrow: auth, order REST, order-updates WS, positions
2. Nautilus execution/position core — OMS, position engine, risk, reconciliation, event-store audit, with a thin `ExecutionClient` adapter to the bridge
3. Fluss trade-row reader + projection sinks (Fills, Order_Lifecycle, Positions, Execution_* tables, quarantine, ledger)
4. Custom safety glue — single-operator (Saurabh, DEC-044) gate (`HALTED → ENABLED`), fencing, attempt/correlation mapping, unknown-outcome halt
5. Babysitter — MVP no-op observer on position events; `POSITION_ACTIONS_ENABLED` stays `false` and fails closed

**Starts `HALTED`.** Money calls disabled until all gate checks + acceptance tests pass; live money stays BLOCKED.

**Acceptance tests:** `AC-UNIT-001`–`AC-UNIT-005`, `AC-INT-001`, `BROKER-PB-001`, `AC-FAIL-001`–`AC-FAIL-004`, `AC-REC-001`, `BAB-UNIT-001/002` (implemented), `BAB-INT-001`, `BAB-FAIL-001/002`, `BAB-OPS-001`, `EXE-UNIT-001/003–005`, `EXE-INT-001`, `EXE-FAIL-001`–`EXE-FAIL-006`, `EXE-OPS-001`, `EXE-AUDIT-001`, `ARROW-REST-001/002`

---

### Phase 6: Integration & Evidence

| Step | Artifact | Purpose |
| --- | --- | --- |
| **Contract** | [`../04_contracts/openobserve.md`](../04_contracts/openobserve.md) | OpenObserve OTLP metrics/logs/traces, alert thresholds |
| **Contract** | [`../04_contracts/09-platform-runtime.md`](../04_contracts/09-platform-runtime.md) | Compose/Swarm topology, health checks |
| **Dossier** | [`08-local-compose.md`](./08-local-compose.md) | Local Docker Compose integration |
| **Dossier** | [`09-production-swarm.md`](./09-production-swarm.md) | Production Swarm: v1 4 VMs (M1-3 Manager+Worker) → v2 7 VMs (M1-3 Manager ONLY + W1-3) |
| **Dossier** | [`10-observability.md`](./10-observability.md) | Dashboards, alerts, runbooks |
| **Dossier** | [`11-testing-and-release.md`](./11-testing-and-release.md) | Master test list, traceability, release evidence |

1. Wire all services in Docker Compose (already scaffolded)
2. Run the 30-min variable-baseline perf test — current phase envelope: 1,024 instruments / 20,480 ticks/s average; the 3,000-instrument / 50k ticks/s average baseline is the deferred production target
3. Run the peak-capacity campaign — current phase: sustained 20,480 ticks/s with ≤20 ticks/s per instrument; the 90k ticks/s peak campaign is RETIRED (DEC-036); the 50k sustained gate is the production target
4. Capability evidence: Fluss LOG/KV/changelog, Flink checkpoint/savepoint/rescale, Arrow REST sandbox
5. Record evidence per `EvidenceRecord` format
6. Produce release evidence package (`11-testing-and-release.md`)

---

### Staleness note

If any contract changes during implementation, re-read the dossier and code before the downstream phase — the dossier may be stale. Record the discrepancy in [`01-foundation.md`](./01-foundation.md).
