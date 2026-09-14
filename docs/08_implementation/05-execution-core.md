# Execution Core — Action Capture, Babysitter & Executor (integrated)

> **ROLE — WORKING DOSSIER (2026-08-18):** this is the single integrated implementation dossier for
> the execution half of the platform. It replaces `05-action-capture.md`, `06-babysitter.md`, and
> `07-executor.md` (each SUPERSEDED and deleted 2026-08-18; full history in git at `74f3d89`). Architecture per the
> 2026-08-18 user decision: **Nautilus** (Rust-native trading engine) is the execution/position
> core; a **go-arrow bridge** (localhost) is the ONLY component that talks to Arrow; the Fluss
> trade-row reader and the single-operator (Saurabh, DEC-044) gate are custom glue. The upstream layer is reconciled to
> this architecture (2026-08-18, **CHG-028**): build contracts
> `docs/04_contracts/05-babysitter.md`/`06-action-capture.md`/`07-executor.md`, functional
> requirements `02-functional/05/06/07`, and DEC-006 are re-scoped — see their dated banners.
>
> Status banner: **Design — Draft · Implementation — Partially implemented (offline) · Evidence — Tested-in-sandbox (207 Rust + 17 Java + Go ok) · Live-money — Blocked.**
> Update 2026-08-24: `19` (T0–T9) + `20` (WP-0–WP-7) **integrated here**; offline gaps closed (gate helper, contract-violation halt, PostbackCorrelator, babysitter NoOp, durable flags, fencing pins, `T7` `BabysitterJob.env.fromSource`, hardening `clippy/fmt/vet`); live single-VM green, Arrow/4VM still block live-money (see Implementation status + Implementation plan + Close-gaps below).
> Update 2026-08-25: Rust lib count re-counted **196** (CHG-102; was 164/168); live order path proven to Arrow (`RCF-EQ×1` `26082501010305` — BILCARE/`BI-EQ` delisted) — broker `MARGIN ERROR` (sandbox unfunded), auth is AutoLogin (`ARROW_TOKEN` removed).
> Update 2026-08-25 (CHG-106): `/v1/intents` `action: "amend"` implemented — `amend_envelope_from_payload` (modify, requires `broker_order_id` + amended order block) + router wiring + round-trip/422 tests; Rust 196 → **206** (cargo test 206/206, clippy/fmt clean). Cancel was already wired; amend closes the place/modify/cancel lifecycle.
> Update 2026-08-25 (CHG-107): `CLOCK_OFFSET_LIMIT_MS` now enforced in the live boot loop — `main.rs` arms a periodic `DriftMonitor` (default 30s, `CLOCK_DRIFT_CHECK_INTERVAL_S`) with `FixedOffsetSource(0)` offline (NTP in Workstream D); Rust 206 → **207**.

<!-- markdownlint-disable MD013 -->

## Status and sources

| Field | Value |
| --- | --- |
| Status | Design revision (Nautilus + go-arrow bridge); upstream contracts/requirements/DEC-006 re-scoped 2026-08-18 (CHG-028) |
| Owner | Execution Team (order path) · Action Capture Team (capture path) · Babysitter Team (position observation) |
| Requirements | `REQ-AC-001`–`REQ-AC-013` → `AC-AC-001`–`AC-AC-017`; `REQ-BB-001`–`REQ-BB-008` → `AC-BB-001`–`AC-BB-009`; `REQ-EXE-001`–`REQ-EXE-013` → `AC-EXE-001`–`AC-EXE-016` |
| Acceptance criteria | `AC-AC-001`–`AC-AC-017`, `AC-BB-001`–`AC-BB-009`, `AC-EXE-001`–`AC-EXE-016` |
| Contracts | `docs/04_contracts/06-action-capture.md` · `docs/04_contracts/05-babysitter.md` · `docs/04_contracts/07-executor.md` (re-scoped 2026-08-18, CHG-028) · `docs/04_contracts/arrow_broker.md` |
| Writes | `Fills`, `Order_Lifecycle`, `Positions`, `Postback_Quarantine`, `Postback_Projection_Ledger`, `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`; consumes immutable `Execution_Intent` and `Safety_Halt_Requests` (KV control table) |
| Must not own | Strategy, candidate scoring, gate approval, order submission to any component other than the go-arrow bridge (**ranking/reservations/decisions REMOVED 2026-08-15, CHG-005**) |
## Implementation status (2026-08-24 — offline done, live blocked)

> Single-VM laptop (`make up` 12 long-running containers of 18 compose services — 3 `execution-t3` profile-gated + 2 one-shot `ddl-apply`/`eod-controller` + `minio-init` exit; `fluss-coordinator:9123` reachable) can now make every `Partially done` row `Fully done offline`. Live Fluss single-VM prove (`FLUSS_BOOTSTRAP`) and Arrow market `RCF-EQ×1` remain live-money blockers (order proven to Arrow 2026-08-25 — sandbox margin only).

| Area | Done offline on laptop (evidence) | Still not done (needs live) |
|---|---|---|
| Fluss intent reader | `IntentReader` `subscribeFromBeginning(0)` + `IntentValidator` 15 checks; `NautilusIntentClientTest` 1/1 | Live `Execution_Intent` LOG replay at 3000 instr on Fluss cluster |
| Nautilus -> bridge | `deterministic_client_order_ref` 14-char (fits 16 `remarks`) + `validate_client_order_ref` `^[A-Za-z0-9._-]{1,16}$` pinned (`config.rs:5-29`), `BridgeSelection` Fake/HTTP seam | Loopback firewall on Swarm overlay, live decimal conversion |
| Nautilus -> Fluss 9 sinks | `PostbackCorrelator` 3-step broker->client->reconciliation + bijective; `PositionProjector` 12/12 + Rust `projection` 7/7 oversell->UNKNOWN; `DurableFlags` OFF bit-identical and now flag-selecting (`DurableClients::open_for_service` + file-backed `durable_file` stores, CHG-135) with a live caller: the `/v1/intents` forward leg claims and records the attempt before the bridge and refuses a replay in-process or after a restart (CHG-136; `durable` 25 / `durable_file` 8 / `http` 5 new tests) | DDL `APPLY=1` + Java `FlussProjectionWriterIT` SKIPPED until `FLUSS_BOOTSTRAP` live single-VM |
| Identities 11 | `attempt`/`broker_order`/`client_ref`/`position_id` `pos-<acc>-<tok>-<side>-<cycle>` preserved | `trade_context` entry/trim/exit grouping live |
| Normal 13-step trade | `LiveNodeRuntime` HALTED boot, `FakeBridge` accept->fill offline | Live Arrow `Place` + WS fill |
| Unknown 8-step | `UNKNOWN` never retried (`resilience007`), gate HALTED, `reconcile_execution_mass_status` read-only | 15s global halt timer, operator review |
| Position engine | Weighted avg wrapping, `nextState` CLOSED re-entry, broker recon read-only | PnL + broker position convergence live |
| Gate HALTED->RECONCILING->APPROVAL_PENDING->ENABLED | New `gate.rs:enter_reconciling_if_needed(true)` HALTED->RECONCILING, `Gate::record_approval` (operator) then `Gate::enable` -> ENABLED (a bare transition to `Enabled` is rejected), `ExecutionGate` same-hash Duplicate no call, changed-hash `ContractViolation` halt (20/20), `fence` stale token/block 0 calls, `clockwatch` 7/7 | Changelog-gap lease acquire, live HA |
| Capture correlation | New `PostbackCorrelator.java` `InMemoryCorrelationIndex` bijective pure; `PostbackFingerprint` sorted `key=value|` SHA-256 | `Order_Correlation` Fluss KV live |
| Babysitter MVP | New `babysitter.rs:NoOpPositionObserver` counts by state/reason, 0 actions, `POSITION_ACTIONS_ENABLED=true`->fail-closed 3/3 | Flink checkpoint merge, `Position_Actions` Fluss |
| Config pins | `FENCING_LEASE=30s`, `BROKER_REF` pattern, `CORRELATION=v1`, `GATE_FENCE_BITS=64` (`config.rs`) + halted-by-default via `EXECUTION_ENABLED` rejected-at-boot (`config.rs:81`, `execution_enabled=false`, `is_halted_default()`) 12/12 | 6 keys `TO_BE_VERIFIED` (Arrow timeout/retry profile, broker status/reference format+echo, Arrow request schema) |
| Readiness / Backpressure | 11 readiness flags (broker/bridge/Fluss/gate/backlog/clock), `MAX_PENDING` logic, `health` not-ready | Load flood `MAX_PENDING_PROJECTION_RECORDS` live |
| Metrics 30 | `telemetry` 3/3 OTLP native, bridge 14/14, Go `go-bridge` ok | O2 dashboards for execution domain |
| Tests | Rust 207/207 (CHG-107; was 206/206), common 12/12 historical 2026-08-24 (exec common), capture 4/4, gateway 1/1, Go 1 pkg ok (`make up` 12 long-running of 18 compose services) — historical count not C6 466/247/388 | `AC-INT/BAB-INT/EXE-INT/ARROW-REST-001/002`, `EXE-AUDIT-001` 1-year R2 lock |


## Why one dossier

The three former services are one pipeline — **capture what the broker did → watch the position →
execute the next action** — and the adopted engine (Nautilus) provides shared machinery for all
three: the OMS (order lifecycle), the position engine, reconciliation, fill dedup, and the event
store. Three dossiers forced duplicated state machines, duplicated config, and three test
surfaces for one execution domain. This dossier is the single build contract.

## Recommended operating model

This is the normative explanation of how Nautilus complements the existing Flink/Fluss platform.

| Responsibility | System | Meaning |
| --- | --- | --- |
| Decide | Flink + Fluss | Process market data, compute candles/signals, and publish durable execution intent |
| Execute | Nautilus Execution Service | Manage orders, fills, positions, PnL, risk, reconciliation, and execution history |
| Control and integrate | Custom glue + go-arrow bridge | Enforce safety/fencing, translate protocols, and project execution state into Fluss |

> Flink produces trade intent. Nautilus manages execution reality.

Flink must not call Arrow, and Fluss must not become a second order or position engine. Nautilus
must run as a long-lived service so its OMS, cache, event processing, reconciliation context, and
position state survive individual instruction messages. Starting a new Nautilus process for every
order is prohibited because it loses state continuity and increases crash-window risk.

### Ownership matrix

| Domain | Single owner | Other components may |
| --- | --- | --- |
| Raw market-data ingestion | Ingestion | Read raw data and health evidence |
| Candle and signal computation | Signal Flink job | Consume signal outputs |
| Execution intent | Immutable Fluss intent stream | Read and validate |
| Live order lifecycle | Nautilus OMS | Consume projected state |
| Fill application and deduplication | Nautilus execution/portfolio path | Consume projected fills |
| Position quantity, side, average price, and PnL | Nautilus Portfolio/Position engine | Consume position events |
| Broker reconciliation | Nautilus reconciliation through the adapter | Request or observe reconciliation |
| Gate, approvals, and fencing | Custom execution control glue | Nautilus must obey the result |
| Arrow credentials and network access | go-arrow bridge | No other service may reach Arrow |
| Queryable execution projections | Nautilus projection boundary into Fluss | Read projections |
| Position observation | Babysitter | Emit no actions in MVP |

### What each system must not do

| Component | Must not implement |
| --- | --- |
| Signal Flink job | Broker calls, broker order lifecycle, broker-derived positions, or execution retries |
| Fluss | A competing OMS, position calculator, or broker command executor |
| Babysitter | Direct Arrow calls or a separate exit-order path |
| Custom projection code | A second position arithmetic engine or a second fill-deduplication engine |
| Nautilus adapter | Platform gate policy hidden inside broker-status mapping |
| go-arrow bridge | Strategy decisions, position arithmetic, or gate authorization |

## Service topology

```text
Signal Flink job
    │ immutable execution intent
    ▼
Fluss intent stream
    │
    ▼
Nautilus Execution Service
    ├─ Fluss intent consumer and schema validator
    ├─ custom gate, attempts, correlation, and fencing
    ├─ Nautilus OMS, risk, portfolio, position, and reconciliation engines
    ├─ Nautilus event store
    └─ Fluss projection writers
            │ local authenticated protocol
            ▼
    go-arrow bridge
    ├─ Arrow authentication and token refresh
    ├─ place/modify/cancel REST calls
    ├─ order-update WebSocket
    └─ orders/trades/positions/account reads
            │ TLS
            ▼
        Arrow broker
```

| Deployment option | Decision | Reason |
| --- | --- | --- |
| Embed Nautilus in a Flink task | Reject | Different lifecycle, failure model, language runtime, and checkpoint boundary |
| Start Nautilus once per order | Reject | Loses OMS/cache/reconciliation continuity |
| Run one shared process for all accounts immediately | Defer | Efficient, but increases scope isolation and fencing complexity |
| Run one long-lived Nautilus instance per execution partition | Recommend initially | Simple ownership, restart, fencing, and account isolation |
| Run Nautilus as a separate Docker service | Recommend | Isolates money-moving operations and credentials from Flink |
| Use Python only as the first control-plane prototype | Accept for discovery | Useful for adapter proof; production runtime should follow the native Rust service boundary |

## Boundary contracts

### Flink/Fluss to Nautilus

The input is an immutable, versioned execution-intent record. It is not a broker request body and
it is not the retired `Trade_Decisions` feed.

| Field group | Required content |
| --- | --- |
| Identity | `instruction_id`, `trade_context_id`, `account_scope_id`, `execution_partition_id` |
| Instrument | Canonical platform instrument identity and broker mapping version |
| Order | Side, quantity, order type, price if applicable, time-in-force, product/exchange |
| Safety | Signal/source version, creation time, expiry time, request hash |
| Replay | Schema version, strategy version, supersession/cancellation information |

The reader validates schema, scope, expiry, request hash, identity uniqueness, and source
continuity before creating a Nautilus order. A repeated `instruction_id` with different content is
a contract violation: quarantine, audit, halt, and no broker call.

### Nautilus to go-arrow bridge

The bridge protocol maps native Nautilus execution commands to the already pinned Go SDK. Nautilus
does not hold Arrow credentials or implement Arrow-specific authentication.

| Nautilus | Bridge |
| --- | --- |
| `ClientOrderId` | Deterministic `client_order_ref` carried in Arrow `remarks` |
| `InstrumentId` | Arrow exchange/symbol/token mapping |
| `OrderSide` | Arrow buy/sell value |
| `OrderType` | Arrow market/limit/trigger value |
| `Quantity`, `Price` | Arrow quantity and price fields with exact decimal conversion |
| Submit/modify/cancel command | Local bridge request |
| Order/fill/account/position report | Normalized bridge event consumed by Nautilus |

The bridge is loopback-only in local mode and service-network restricted in production. It is the
only process containing Arrow credentials and the only process with broker network access.

### Nautilus to Fluss

Nautilus emits execution events. Projection glue converts them into Fluss records:

| Nautilus event | Fluss projection |
| --- | --- |
| Fill event | `Fills` LOG |
| Order lifecycle event | `Order_Lifecycle` KV |
| Position event | `Positions` KV |
| Command/report/audit event | `Execution_Audit` LOG |
| Attempt transition | `Execution_Attempts` KV |
| Verified identity mapping | `Order_Correlation` KV |
| Projection progress | `Postback_Projection_Ledger` KV |
| Unknown or unmapped evidence | `Postback_Quarantine` LOG |

These are durable read models and integration surfaces. They do not authorize a second execution
effect and they do not replace Nautilus as the live execution authority.

## Identity mapping

The platform must preserve separate identities throughout the flow.

| Identity | Owner | Purpose |
| --- | --- | --- |
| `candidate_id` | Signal job | Detected setup/evidence record |
| `instruction_id` | Signal/intent writer | Immutable platform execution intent |
| `execution_attempt_id` | Custom execution control | One submission attempt for an instruction |
| `client_order_ref` | Custom execution control | Deterministic broker-facing reference |
| `broker_order_id` | Arrow | Broker-authoritative order identity |
| Nautilus `ClientOrderId` | Nautilus adapter | Native order identity used by the OMS |
| Nautilus `TradeId` | Nautilus | Native fill identity |
| `position_id` | Nautilus position projection | Fill-derived exposure aggregate |
| `trade_context_id` | Platform | Groups entry, trim, and exit orders |
| `postback_event_id` | Capture boundary | One received broker update |
| `halt_request_id` | Safety control | One durable safety-halt request |

Never use a generic `order_id` across these domains. A trim, exit, or re-entry is a new broker
order linked to the same trade context; it is not a mutation of the original order identity.

## End-to-end trade flows

### Normal accepted trade

| Step | Component | Action |
| ---: | --- | --- |
| 1 | Signal Flink job | Detects a valid setup |
| 2 | Fluss | Stores immutable execution intent |
| 3 | Execution reader | Validates intent, identity, expiry, and scope |
| 4 | Safety control | Checks gate state, epoch, and fencing token |
| 5 | Nautilus | Creates the native order and applies native risk checks |
| 6 | ExecutionClient | Converts the order to a bridge command |
| 7 | go-arrow bridge | Calls Arrow REST |
| 8 | Arrow | Returns a verified acceptance or rejection |
| 9 | Nautilus | Emits order lifecycle events and persists execution history |
| 10 | Arrow WebSocket | Publishes order updates and fills |
| 11 | Nautilus | Applies updates, deduplicates fills, updates the position and PnL |
| 12 | Projection glue | Writes Fluss execution projections |
| 13 | Babysitter | Observes the projected position; emits zero actions in MVP |

### Unknown broker outcome

| Step | Component | Action |
| ---: | --- | --- |
| 1 | Bridge | Times out, disconnects, or returns an unclassifiable response |
| 2 | Adapter/control | Classifies the attempt as `UNKNOWN`, never as rejection |
| 3 | Safety control | Moves the gate to `HALTED` and blocks new submissions |
| 4 | Nautilus | Preserves the unresolved order and event history |
| 5 | Reconciliation | Queries Arrow orders, trades, positions, and order detail |
| 6 | Nautilus | Applies only verified reconciliation reports |
| 7 | Projection glue | Records the outcome, evidence, and resolution |
| 8 | Operators | Review the evidence and perform the required approvals |

An unknown attempt is never blindly retried. A retry requires proof that the broker did not accept
the original request or an evidence-approved broker idempotency mechanism.

## Position-management model

Nautilus owns the position calculation path:

```text
Arrow fill report
    → Nautilus order event
    → Nautilus position engine
    → quantity, side, average price, realized/unrealized PnL
    → Fluss Positions projection
    → Babysitter observation
```

| Position behavior | Owner |
| --- | --- |
| Open a position | Nautilus position engine |
| Add to a position | Nautilus position engine |
| Partially reduce a position | Nautilus position engine |
| Close a position | Nautilus position engine |
| Long/short/net calculation | Nautilus position engine |
| Average entry/exit arithmetic | Nautilus position engine |
| Realized and unrealized PnL | Nautilus portfolio engine |
| Broker position reconciliation | Nautilus reconciliation |
| Decide whether an exit should be requested | Babysitter/strategy layer, future scope |
| Authorize the exit | Custom gate and fencing |
| Submit the exit order | Nautilus OMS through the same adapter path |

The current Java position projector is a migration and differential-test reference only. It must
not remain a second production authority after Nautilus parity is proven.

## Migration and proof roadmap

| Phase | Deliverable | Release gate |
| --- | --- | --- |
| 1. Boundary freeze | Intent schema, identities, ownership, gate contract, projection schemas | No duplicate authority in documents or code |
| 2. Offline adapter | Platform intent to Nautilus order; fake bridge; recorded updates/fills | Deterministic lifecycle and position parity |
| 3. Mock broker | Place, modify, cancel, reject, partial fill, disconnect, restart | No duplicate order across crash windows |
| 4. Fluss integration | Intent consumer, projections, ledger, gate, fencing | Restart-safe command and projection recovery |
| 5. Arrow sandbox | Real request/status/update/fill/reconciliation behavior | Arrow protocol evidence complete |
| 6. Shadow mode | Read-only broker reports and Nautilus state comparison | Broker and Nautilus positions converge |
| 7. Controlled enablement | Single-operator (Saurabh, DEC-044) approval, rollback, audit, operational runbook | Live-money approval package complete |

## Explicit non-goals

The following must not be built as part of the Nautilus integration:

| Non-goal | Reason |
| --- | --- |
| Java OMS | Duplicates Nautilus order lifecycle |
| Separate Java production position projector | Creates competing position truth |
| Direct Arrow calls from Flink | Couples strategy to money movement |
| Direct Arrow calls from Fluss clients | Bypasses the execution service and safety gate |
| Independent retry engine | Can duplicate an unknown broker order |
| Independent fill-deduplication engine | Conflicts with Nautilus reconciliation |
| Independent PnL calculator | Can disagree with Nautilus position arithmetic |
| Babysitter direct broker path | Bypasses OMS, risk, gate, and audit |

## Architecture

```text
Fluss (immutable execution intent · Safety_Halt_Requests · owned control/projection tables)
   │  Fluss reader (custom glue — tails execution intent, issues Nautilus order commands)
   ▼
Nautilus Engine            ← the execution/position core
   │  OMS · position engine · risk engine · event store · reconciliation · fill dedup
   │  ExecutionClient adapter (thin — maps bridge JSON ↔ Nautilus messages/events)
   ▼
go-arrow bridge (localhost)  ← the ONLY component that talks to Arrow
   │  go-arrow SDK (auth · PlaceOrder/Modify/Cancel · order-updates WS · orders/trades/positions)
   ▼
Arrow broker (edge.arrow.trade)
```

**Single-writer principle:** the go-arrow bridge is the only component with broker credentials and
the only component that can physically reach Arrow. Nautilus commands the bridge; nothing else can.
**Truth-authority (resolved by DEC-042):** Nautilus is authoritative for live order/fill/position
behavior and reconciliation. Its event processing is the execution state machine; Fluss stores
immutable execution intent, custom control state, and projections of Nautilus events for platform
consumers. The Fluss projections are durable read models and integration surfaces, not a second
production OMS or position engine. The Nautilus event store is an execution-history and replay
boundary, but its current early-alpha status requires dedicated durability, backup, verification,
and recovery evidence before live use. The current audit target is at least one year, or longer
under the approved retention policy.

## Component map

| Component | Responsibility | Source |
| --- | --- | --- |
| Fluss reader | Tail immutable execution-intent rows; issue validated Nautilus order commands; validate schema/version; fail closed on discontinuity | **Custom glue** |
| Nautilus OMS | Order lifecycle state machine, order types, contingency (OTO/OCO/OUO), duplicate suppression, multi-client routing | Nautilus (`execution`/`model` crates) |
| Nautilus position engine | Fill-derived position state, weighted entry/exit, PnL, FLAT/OPEN/REDUCING/CLOSED, `trade_context_id` grouping | Nautilus (`portfolio`/`model`) |
| Nautilus risk engine | Pre-trade checks: price/quantity/expiry validation, rate limits, max notional, trading-state kill switch | Nautilus (`risk`) |
| Nautilus reconciliation | Mass-status reconcile on connect, open-order checks, position consistency, inferred fills, **fill dedup** | Nautilus (`live`/`execution`) |
| Nautilus event store | Append-only audit of commands/events/reports/correlations; snapshot + tail-replay recovery; incident replay | Nautilus (`event_store`) |
| Nautilus adapter | Thin `ExecutionClient`: bridge HTTP ↔ submit/modify/cancel/reports; bridge WS events ↔ `ExecutionReport`s; symbol/status mapping | **Custom glue** (thin) |
| go-arrow bridge | Auth (TOTP/appID-token), `PlaceOrder`/modify/cancel, order-updates WS re-publish, orders/trades/positions/margin reads | **Custom glue** (wraps pinned go-arrow SDK) |
| Single-operator (Saurabh, DEC-044) gate + fencing | `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED`, epochs, single-operator approval (a second approval is optional, not required), per-partition fencing token | **Custom glue** |
| Projection sinks | Nautilus event store → Fluss `Fills`, `Order_Lifecycle`, `Positions`, `Execution_Gate/Attempts/Correlation/Audit`, ledger; quarantine path | **Custom glue** |
| Control API | Authenticated halt / reconcile / approval commands (single-operator (Saurabh, DEC-044)) | **Custom glue** |
| Telemetry | OTLP metrics/logs → OpenObserve; bridge + engine readiness | Custom + Nautilus-native |

## Capability mapping

| Requirement family | Behavior needed | Provided by |
| --- | --- | --- |
| AC — postback intake, correlation, quarantine, ledger | Decode Arrow order-updates WS; adopt orders the platform did not create here; immutable audit; quarantine unknowns; crash recovery | go-arrow bridge (`OrderStream`) + Nautilus external-order adoption + event store; **correlation mapping + quarantine path custom** |
| AC — lifecycle + position projection | Per-order state transitions, no regression on stale evidence; fill-derived positions with weighted values | Nautilus OMS + position engine; **UNKNOWN/regression policy custom** |
| BB — position observation | Consume `Positions` state; observe freshness; **MVP emits zero actions**; fail closed on `POSITION_ACTIONS_ENABLED=true` | Nautilus position events + a no-op observer strategy; **fail-closed guard custom** (Flink scaffold already implements it) |
| EXE — gate/attempts/audit | Durable gate, single-operator (Saurabh, DEC-044) resume, attempt phases, no blind retry on UNKNOWN, immutable audit, policy-controlled reconstruction for at least one year | **Gate + fencing + attempt/correlation glue custom** on Nautilus; audit via Nautilus event store + approved encrypted retention storage |
| EXE — broker side effects | Sole path to Arrow; verified acceptance/rejection only; timeout/disconnect → halt + reconcile | go-arrow bridge (single writer) + Nautilus reconciliation |

## State machines

### Gate (custom — the money gate)

```text
HALTED → RECONCILING → APPROVAL_PENDING → ENABLED → HALTED
```

- Initial state is always `HALTED`, epoch 0; every accepted transition increments the epoch by 1.
- Single authorized operator `saurabh` (DEC-044) approves the same gate epoch + evidence hash before `ENABLED`;
  automatic resume is prohibited (DEC-044).
- Any uncertainty (unknown outcome, fencing loss, storage uncertainty, changelog gap) moves the
  gate to `HALTED`; a safe halt received while already `HALTED` is recorded idempotently.
- Every broker-facing command re-verifies gate state + epoch + fencing token immediately before
  submission.

Startup/resume sequence: load the version/configuration matrix → connect owned Fluss state →
verify schema versions and audit append capability → verify changelog continuity and consumer
position → acquire/fence the `execution_partition_id` lease → start or restore `HALTED` if any
state is uncertain → validate bridge/Arrow REST contract and reachability **without placing a live
order** → reconcile unknown attempts, broker orders, fills, and positions → enter
`APPROVAL_PENDING` only after reconciliation passes → require the single-operator (Saurabh,
DEC-044) approval of the same epoch/evidence hash. Process health never implies trading readiness.

Safety-halt rules: `halt_request_id` is a deterministic canonical hash of the request tuple
(account/partition scope, source component/instance, reason, detection time, source epoch,
evidence hash, schema version); a supplied ID that does not match the digest is malformed and
rejected. Duplicate IDs are idempotent (`DUPLICATE`, never a double epoch increment). Cross-scope
requests are rejected and audited. Stale `source_epoch` is rejected — the greatest seen valid
epoch is tracked per `(source_component, source_instance, execution_partition_id)`, and the
maximum is updated after validation even when the gate is already `HALTED`, so later lower epochs
cannot become acceptable. An accepted request enters `HALTED`, increments the gate epoch once, and
records source/evidence in audit. The order path independently detects stale mandatory health even
if the halt-request stream is unavailable.

### Attempt protocol (custom, on Nautilus order state)

```text
no active attempt → PREPARED (request hash + client ref + gate epoch + fence)
                 → SUBMITTING → ACCEPTED | REJECTED | CANCELLED | UNKNOWN
```

- A duplicate `(instruction_id, request_hash)` returns the existing attempt — never a second
  submission. Changed content under an existing instruction identity is a contract violation:
  quarantine, audit, halt.
- Intake validation (reader): schema/version, canonical identity/content hash, expiry/freshness,
  supersession/cancellation, and no unresolved attempt/request-hash conflict — enqueue only if
  gate and fencing permit (applies to future `Position_Actions` and control rows).
- `UNKNOWN` is non-terminal: it blocks new submissions and resolves only through explicit
  reconciliation — never auto-retry (DEC-011, DEC-030).

### Order lifecycle (Nautilus OMS → normalized vocabulary)

Nautilus's order events (`Initialized → Submitted → Accepted → PendingUpdate/Updated →
PendingCancel/Canceled → Rejected → Filled/PartiallyFilled → Expired`, plus `Triggered`,
`Released`, `Emulated`, `Denied`) normalize to the contract vocabulary `PENDING`, `PARTIAL`,
`FILLED`, `CANCELLED`, `REJECTED`, `UNKNOWN`. Exact-duplicate source events produce no duplicate
effect (Nautilus fill dedup); older versions cannot regress state; conflicting/regressive evidence
moves the record to `UNKNOWN`, halts affected flow, and alerts.

The per-update transition protocol is explicit (custom policy layered on the OMS):
1. Exact-duplicate source event → no duplicate effect.
2. Older source version → stale-evidence metric/audit; no regression.
3. Equal version with conflicting content → `UNKNOWN`, quarantine, halt.
4. Terminal-state regression → reject, quarantine, halt.
5. Quantity regression or impossible totals → `UNKNOWN`.
6. Unknown broker status → quarantine and `UNKNOWN`.

Every update carries source event ID, verified broker timestamp when available, receive
timestamp, transition version, quantities, prices, status-mapping version, and correlated IDs.

### Capture-path identity and correlation

Every received postback receives a platform `postback_event_id` and versioned
`postback_fingerprint` (a bounded logical duplicate hint; repeated deliveries may exist in
immutable audit). Same `postback_event_id` with different content is a contract violation:
quarantine, audit, alert. No `postback_seq` or broker-global event identity is assumed.

Correlation resolves in this order:
1. Verified `broker_order_id` mapping in `Order_Correlation`.
2. Verified echoed `client_order_ref` mapped to one attempt/instruction.
3. Evidence-approved reconciliation query.

Symbol, quantity, price, and timestamp proximity are never sufficient. Missing/ambiguous
correlation → `Postback_Quarantine` + halt affected order flow + alert. The mapping is bijective:
one attempt maps to at most one broker order; one client reference maps to one attempt; one
broker order maps to one verified attempt — violations are ambiguous and safety-relevant. The
capture path reads `Order_Correlation` but never mutates execution mapping state except through
the explicit reconciliation interface owned by the order path.

### Position states (Nautilus position engine)

`FLAT`, `OPEN`, `REDUCING`, `CLOSED`, `UNKNOWN`. `position_id` is minted on the first uniquely
correlated fill; `trade_context_id` groups related entry/trim/exit orders. Conflicting fills,
ambiguous side, quantity underflow, or missing correlation → `UNKNOWN` + halt of affected position
actions. Order completion is not position closure.

### Babysitter (MVP = no-op)

A separate observer consumes position events, checkpoints observation state (latest accepted
position version per `position_id`, last source offset, freshness timestamp, schema version,
no-op reason counters — never historical snapshots, ticks, candles, or strategy state), and emits
**zero actions**. `POSITION_ACTIONS_ENABLED` must fail closed at startup for any value other than
`false`. A healthy Babysitter never implies permission to trade. Future `Position_Actions` route
through the same gate/attempt/fencing protocol as entry instructions.

Failure behavior per mode: missing `Positions` schema → not ready, no output; changelog gap →
degraded, no action, alert; checkpoint failure → not ready, no action; stale/conflicting position
→ observe/audit, no action; restart with unknown state → restore or remain not ready, no action;
`POSITION_ACTIONS_ENABLED=true` → fail closed at startup. Multiple updates for one position retain
only the latest version in Babysitter state.

## Reconciliation and unknown outcomes

- On connect, mass-status reconciliation: order-status reports, fill reports, and position-status
  reports are generated through the adapter and applied to the OMS (Nautilus `reconcile_execution_mass_status`).
- Open-order checks re-query orders without a terminal event; in-flight tracking detects
  submissions that never got an ack; recent-fills dedup prevents double-counted fills.
- Failure classification is explicit: `not_sent`, `ambiguous`, `venue_rejected` — only a verified
  acceptance or verified rejection is terminal; anything else is `UNKNOWN` → halt + reconcile.
  Verified shapes (bridge `PlaceOrder` response envelope): HTTP 200 + `status:"success"` +
  nonblank `data.orderNo` → acceptance; HTTP 400/409/422 + `status:"error"` + nonblank `message`
  → rejection; HTTP 401/403/408/429/5xx, transport failure, missing body, a self-contradictory
  envelope (`status` and `success` disagree — `P3-040`), or any other combination →
  `AMBIGUOUS`/`UNKNOWN` — never rejection, never retry. Order identity is read only from the
  documented `data` object; a top-level echo, a generic `orderId`/`order_id` alias, or a bare
  string `data` is not an identity and never yields acceptance (`P3-041`).
- `client_order_ref` is deterministic and replay-safe: the same attempt always yields the same
  reference (canonical hash of `format_version|instruction_id|execution_attempt_id`, 14 ASCII
  chars, fits Arrow's 16-char `remarks`), so correlation and duplicate suppression never depend on
  wall-clock or process state.
- Missing reconciliation evidence blocks live-money enablement (test IDs `ARROW-REST-001`/
  `ARROW-REST-002`, DEC-023 endpoints).
- Projection-ledger recovery (capture path): durable `Postback_Projection_Ledger` KV keyed by
  `postback_event_id` tracks `RECEIVED → AUDIT_WRITTEN → LIFECYCLE_APPLIED →
  POSITION_APPLIED_OR_NOT_REQUIRED → COMPLETE` with `UNKNOWN` for conflict/regressive/ambiguous
  evidence; every step is idempotent; restart scans non-complete records and resumes from
  persisted evidence; a duplicate immutable audit row never authorizes a duplicate state effect.
  The ledger is Fluss-owned state (never in-memory only) and coexists with Nautilus event-store
  replay — the event store replays engine state; the ledger tracks projection completion into
  Fluss.

## Audit and event store

Every money-moving and safety event flows through the Nautilus event store (append-only,
replayable): commands, order events, venue reports, correlations, approvals, halts. Cache
snapshots + tail-replay provide stable restarts and deterministic incident replay. The audit target
is at least one year, or longer under the approved retention policy, using encrypted storage,
integrity verification, access governance, and an R2 bucket-lock or equivalent immutability
mechanism where deployed.

The audit envelope for every money-moving or safety event carries: audit ID/type/schema version;
all relevant domain identities; gate state/epoch before and after; actor/service/operator
identity; engine/fencing identity; request/response hashes with redacted evidence; broker
status/order ID when known; changelog offset/continuity evidence; configuration/version
snapshot; evidence hash and UTC/monotonic timestamps.

**Postback evidence is retained; credentials are not.** The capture path keeps the original
postback bytes/text and payload hash in immutable audit with decoder/schema version (REQ-AC-001,
REQ-AC-004) — that is evidence preservation, not credential storage. The execution-side rule is
narrower and unchanged: broker credentials, tokens, and raw request/response bodies are never
stored in audit (only redacted hashes and summaries).

## Configuration contract

| Key | Rule |
| --- | --- |
| `ARROW_APP_ID` / `ARROW_APP_SECRET` / `ARROW_USER_ID` / `ARROW_PASSWORD` / `ARROW_TOTP_KEY` | Bridge credentials (AutoLogin) — secret refs only, never in Nautilus, never committed. (`ARROW_TOKEN` removed 2026-08-24 — dead key) |
| `ARROW_REST_URL` (**RESOLVED 2026-08-24:** `https://edge.arrow.trade` in `.env`, consumed by bridge; TOTP AutoLogin proven live 2026-08-21 `execution-auth-001`; static `ARROW_TOKEN` path removed 2026-08-24) | Bridge → Arrow base URL; no unsafe production default |
| `ARROW_TIMEOUT_PROFILE_TO_BE_VERIFIED` / `ARROW_RETRY_POLICY_TO_BE_VERIFIED` | Timeout + classification; unknown outcomes never blind-retried |
| `BROKER_CLIENT_REFERENCE_FORMAT_TO_BE_VERIFIED` | Length/charset/echo evidence for the ≤16-char `client_order_ref` (carried in `remarks`) |
| `BRIDGE_LISTEN_ADDR` | Localhost bind for the go-arrow bridge (loopback only) |
| `EXECUTOR_ACCOUNT_SCOPE` / `EXECUTION_PARTITION_ID` | Fencing and gate scope |
| `FENCING_LEASE_PROFILE` | Durable single-owner protocol — PINNED `30s` (`config.rs`, CHG-094); no longer TO_BE_DEFINED |
| `GATE_INITIAL_STATE` | Must be `HALTED`; `GATE_EPOCH_POLICY_VERSION` monotonic |
| `MAX_DECISION_LAG_MS` / `MAX_PROJECTION_LAG_MS` / `MAX_PENDING_PROJECTION_RECORDS` | Staleness and backlog bounds |
| `POSTBACK_FINGERPRINT_VERSION` / `CORRELATION_POLICY_VERSION` / `LIFECYCLE_TRANSITION_VERSION` / `POSITION_PROJECTION_VERSION` | Capture-path versioned policies (evidence-gated) |
| `BROKER_STATUS_MAPPING_VERSION_TO_BE_VERIFIED` / `BROKER_CLIENT_REFERENCE_ECHO_TO_BE_VERIFIED` | Status vocabulary + reference echo evidence |
| `POSITION_ACTIONS_ENABLED` | Must be `false` for MVP; any other value fails startup |
| `BABYSITTER_JOB_VERSION` / `POSITIONS_SCHEMA_VERSION` / `POSITION_FRESHNESS_POLICY_VERSION` | Versioned observation policy |
| `CHECKPOINT_PROFILE_ID_TO_BE_DEFINED` / `POSITION_ACTIONS_SCHEMA_VERSION_TO_BE_DEFINED` | Babysitter checkpoint profile / future action schema — deliberately UNPINNED until the Position_Actions work package lands a consumer (CHG-094): defining an unread constant now would be a placeholder, not a pin |
| `AUDIT_SCHEMA_VERSION` / `ARROW_REQUEST_SCHEMA_VERSION_TO_BE_VERIFIED` | Immutable audit envelope / approved API contract version |
| `NAUTILUS_RISK_MAX_ORDER_SUBMIT` / `NAUTILUS_RISK_MAX_NOTIONAL_PER_ORDER` | Pre-trade risk limits |
| Fluss table/schema/version identifiers | Reader + projection sinks |

Missing protocol/status/correlation/fencing evidence keeps the execution core not ready for live
money.

## Backpressure and readiness

Readiness is false for: broker disconnect, bridge unavailability, Fluss unavailability, changelog
gap, schema mismatch, gate in a non-`ENABLED` state, unknown outcome unresolved, projection
backlog above policy, failed recovery, correlation invariant failure, or clock violation. Existing
accepted evidence must remain recoverable.

## Required telemetry

Postback/byte rate, decode failures, fingerprint duplicates, correlation success/quarantine by
reason, lifecycle transitions/rejections, stale/regressive/conflicting events, projection
backlog/lag/retries, independent-write failures, recovery duration, positions by state,
checkpoint health, restart count, no-op decisions by reason, action-enabled guard status, gate
state/epoch, halt latency, attempts by phase/outcome, unknown outcomes, duplicate suppressions,
request conflicts, approvals, fencing lease, consumer lag, Arrow REST latency/status, bridge
connected, clock offset, audit append status. Telemetry is native-first
(Nautilus + bridge OTLP) per CHG-023's direction.

## Required tests

Canonical IDs from `11-testing-and-release.md` (test design sections below):

- **Action Capture:** `AC-UNIT-001`–`AC-UNIT-005`, `AC-INT-001`, `BROKER-PB-001`, `AC-FAIL-001`–`AC-FAIL-004`, `AC-REC-001`.
- **Babysitter:** `BAB-UNIT-001`/`BAB-UNIT-002` (**IMPLEMENTED 2026-08-15** — `BabysitterJobTest`, no-op topology + fail-closed flag), `BAB-INT-001`, `BAB-HARNESS-001`, `BAB-FAIL-001`, `BAB-FAIL-002`, `BAB-OPS-001`.
- **Executor:** `EXE-UNIT-001`, `EXE-UNIT-003`–`EXE-UNIT-006`, `EXE-INT-001`, `EXE-FAIL-001`–`EXE-FAIL-006`, `EXE-OPS-001`, `EXE-AUDIT-001`, `ARROW-REST-001`, `ARROW-REST-002`.

Mapping note: behaviors native to Nautilus (order state machine, position projection, fill dedup,
reconciliation, event-store audit) are proven by Nautilus's own test suites plus adapter-level
integration tests; behaviors that remain custom (gate, fencing, single-operator (Saurabh, DEC-044) resume, correlation to
`Execution_Attempts`/`Order_Correlation`, quarantine, projection sinks, bridge endpoints) require
dedicated tests under the canonical IDs above. The order REST path in the go-arrow SDK is
currently **untested** — the bridge `PlaceOrder` endpoint requires Arrow-sandbox smoke tests
before it is trusted.

## Definition of done

The execution core is complete when: the go-arrow bridge is the only component that can reach
Arrow (proven by test); every trade row is either executed with a verified outcome or halted as
`UNKNOWN`; every postback is either correlated or quarantined; partial writes recover after
restart (event-store replay); stale evidence cannot regress state; positions and orders remain
separate aggregates; `UNKNOWN` never releases capacity and never auto-retries; the single-operator (Saurabh, DEC-044) gate
and fencing are enforced; no crash-window test duplicates an order; the policy-controlled audit
reconstruction (`EXE-AUDIT-001`) covers at least one year or the approved longer period; the
Babysitter emits zero actions and fails closed; and
all broker calls remain disabled until the release evidence package approves enablement.

## Verification mapping

- [Action Capture test design](./11-testing-and-release.md#action-capture)
- [Babysitter test design](./11-testing-and-release.md#babysitter)
- [Executor test design](./11-testing-and-release.md#executor)

## Open gates and upstream impact

1. **~~Upstream re-scope pending~~ — RESOLVED 2026-08-18 (CHG-028):** build contracts
   `05-babysitter.md`, `06-action-capture.md`, `07-executor.md`, requirements
   `02-functional/05/06/07`, and DEC-006 are re-scoped to the Nautilus + go-arrow bridge design
   (dated banners in each file).
2. **~~Truth authority~~ — RESOLVED 2026-08-19 (DEC-042/DEC-043):** Nautilus owns live execution behavior; Fluss owns immutable intent, control state, and queryable projections. The Nautilus event store's production durability, approved-policy retention, and recovery evidence remain open gates.
3. **Broker evidence:** Arrow sandbox order round-trip (place → update stream → fill), REST
   reconciliation endpoints, and client-reference echo (Level 3) remain the live-money gates.
4. **Existing code:** the Flink `BabysitterJob` scaffold and the `common` Agent-2 safety-core
   pieces (gate/attempt rules in `com.trading.common.schema.execution`) are inputs — the safety
   rules carry over to the custom gate layer; the no-op Babysitter behavior is already proven.

---
> **INTEGRATION NOTE — 2026-08-24:** `19-nautilus-execution-service-implementation-plan.md` (T0–T9, 1856 lines) and `20-close-execution-service-gaps-plan.md` (WP-0–WP-7, 231 lines) are now **SUPERSEDED and integrated** into this file. This dossier is the single source of truth for the execution domain — normative design (above) + implementation plan (below) + close-gaps continuation. `19` and `20` remain as stubs pointing here (git history at `74f3d89` and `2026-08-24`).
---


## Implementation plan — Nautilus execution service (T0–T9) — integrated from `19`

> Source: `19-nautilus-execution-service-implementation-plan.md` §8 Implementation Plan (1856 lines). This section is the **condensed normative extract** — full task audits (per-task `Repository audit and implementation handoff`) remain in git history. Status reflects `2026-08-24 07:45 UTC` single-VM green (`fluss-coordinator:9123`, `execution-gateway:9190 HALTED`, 207 Rust + GatewayFluss/ProjectionWriter live).

## 8. Implementation Plan

### Task completion contract

This plan is executed as a gated TODO list. A task is **not complete because its code compiles**.
The implementer must mark every atomic checkbox for the task, produce the stated output guarantee,
and attach the stated validation evidence before starting the next task.

Each task has four required completion fields:

| Field | Required meaning |
| --- | --- |
| **Atomic TODO** | A checkbox for every independently verifiable implementation action. No unchecked item may be silently carried forward. |
| **Output guarantee** | The exact artifact and externally observable behavior that must exist when the task is complete. |
| **Evidence** | The command result, test report, schema checksum, fixture, or runtime observation that proves the output guarantee. |
| **Exit gate** | A deterministic pass/fail rule. If it fails, the task remains incomplete or is marked `BLOCKED:` with the reason. |

Use these status markers while executing the plan: `[ ]` not started, `[~]` in progress, `[x]`
complete, and `BLOCKED:` for a dependency or external prerequisite that prevents completion.
The implementation agent must not mark a parent task `[x]` while a child checkbox is unchecked.
Every output guarantee is a **necessary condition**, not a promise that live-money trading is
approved; live money remains blocked until Task 9 and the external approval gates pass.

For precise status reporting, the parent headings are phase IDs `T0` through `T9`. Within each
parent task, its checkboxes are atomic child items in display order: for example, the third
checkbox under Task 4 is `T4.3`. An implementation report must identify these IDs rather than
reporting only “Task 4 mostly done”. The following matrix shows which implementation task closes
each major workstream; the detailed checkboxes under that task are the actual TODO list.

| Workstream | Closing task | Completion proof |
| --- | --- | --- |
| Version, policy, and dependency freeze | `T0` | Reproducible pins, change record, and validation output |
| Immutable execution-intent contract | `T1` | DDL/manifest/hash tests and disabled-by-default graph proof |
| Fluss transport and projections | `T2` and `T6` | Gateway protocol plus idempotent projection/recovery evidence |
| Arrow order boundary | `T3` | Fake-broker lifecycle, `UNKNOWN` mapping, and network isolation proof |
| Nautilus runtime authority | `T4` | Rust service lifecycle, fake execution, position, and restart evidence |
| Gate, fencing, and unknown-outcome safety | `T5` | Crash-window, approval, fence, and zero-duplicate-command tests |
| Position projection and parity | `T6` | Rebuild and differential replay evidence |
| Babysitter observation boundary | `T7` | Checkpoint/restore and zero-action proof |
| Local deployability and readiness | `T8` | Compose acceptance, checkpoint, restart, and secret-isolation evidence |
| External sandbox, shadow, and release gates | `T9` | Evidence bundle and two-person review while still halted |

### Implementation progress — 2026-08-19

| Phase | Status | Verified output | Remaining boundary |
| --- | --- | --- | --- |
| `T0` dependency/policy freeze | **PARTIAL** | Nautilus commit/workspace pin, Rust 1.97.1 record, Go 1.24.5 image tag, platform/Arrow SDK pins, version matrix, `CHG-039`–`CHG-041`, cargo metadata/check, and saved phase-1 evidence exist | Immutable Go image digest, ignored credential-injection proof for real sandbox secrets, retention approval, and final service capability evidence remain absent |
| `T1` intent contract | **PARTIAL — producer slice** | `27_execution_intent.sql`, 25-entry manifest, Java layout/builder/protocol, deterministic context resolver, strict config, disabled-by-default Signal branch, stable UIDs, producer tests, and full compute suite | Enabled-graph scratch Fluss append proof and durable quarantine/audit writer remain T2 responsibilities |
| `T2` Java Fluss gateway | **PARTIAL — durable-replay leg done, offline+live green** | `code/02_services/06_execution_gateway/` contains the standalone Java 17 module, Fluss readers/writers, authenticated protocol, readiness, ledger, focused tests, and a durable source-event dedup index (`Execution_Intent_Processed`) that replaces the process-local duplicate guard. 18 tests PASS (13 unit incl. durable-dispatcher, 16 offline / 3 env-gated live: composite-key matrix, durable restart-replay, recoverable-ledger resume) | Production Compose wiring of the gateway remains; broker exactly-once across a forward/commit crash boundary is deferred to T5 (durable fence token) |
| `T3` Go Arrow execution bridge | **PARTIAL — offline fake/network slice** | Separate module, pinned vendored SDK replacement, private v1 command/report protocol, SDK REST adapter, fake HTTP lifecycle, fake WebSocket lifecycle/reconnect fixture, request-id cache, postback normalizer, Dockerfile, resolved Compose profile, network policy checker, race tests, and disabled-mode health probe | Real sandbox authentication/re-authentication remains intentionally blocked; T8 still must wire gateway/Rust services and perform cross-container runtime probes; T9 owns real broker evidence |
| `T4` Rust Nautilus service | **NOT IMPLEMENTED** | Nautilus reference crates compile at the pinned checkout; no service exists under `code/02_services/04_executor/` | Rust binary, `ExecutionClient`, runtime, health, fake bridge, event-store integration, and restart behavior remain |
| `T5` durable gate/attempt/fencing | **PARTIAL — the durable attempt store has its live caller (CHG-136); fencing and approvals remain the gateway's** | Pure-JVM `GateTransitionValidator`, `InMemoryAttemptStore`, audit/hash-chain, identity tests; Rust `FileAttemptStore`/`FileGateStore` behind the durable flags with `try_claim` and create-on-halt (CHG-135); the `/v1/intents` forward leg claims and records the attempt (`claim_for_send`, `LiveAttemptStore`) before the bridge sees it and answers a replay from the record — live-smoked in-process and across a restart (CHG-136) | No durable gateway-backed state in Rust (`ExecutionGate` is still constructed only by tests), so the *gate* transitions, fencing, and approvals are the Java gateway's authority; the durable store is one-actor and single-host (pid lock), and a `Duplicate` is answered with a refusal rather than the recorded outcome |
| `T6` postback/projection/position parity | **PARTIAL — test/reference helpers only** | Java `PositionProjector`/`PositionProjectorDriver`, position schemas, and broker postback unit models exist | No Nautilus event source, projection writer, ledger, quarantine runtime, Fluss rebuild, or differential replay exists |
| `T7` Babysitter Positions observation | **PARTIAL — MVP marker shell only** | Job has fail-closed action flag, explicit UIDs, and zero-action marker/discard tests | `BabysitterJob` still uses `fromElements(0L)`; no `Positions` source, observation state, or checkpoint/restore evidence exists |
| `T8` local Compose integration | **PARTIAL — T3 bridge profile only** | Compose now defines the T3 `execution-bridge` profile, internal `execution-net`, bridge-only `arrow-egress`, no host port, and a static resolved-config policy check | Gateway, Rust service, jobs, readiness ordering, checkpoint probe, and full end-to-end restart/recovery path remain |
| `T9` sandbox/shadow/release evidence | **NOT IMPLEMENTED** | No execution-specific Arrow sandbox, shadow comparison, or release evidence bundle exists | All external protocol, recovery, retention, shadow, and two-person review evidence remains blocked |
### Implementation progress — 2026-08-24 (offline patches, live single-VM green)

| Phase | Status 2026-08-24 | Verified output (offline + single-VM) | Still blocked (needs market/4VM) |
|---|---|---|---|
| `T0` | **FULLY DONE OFFLINE+SINGLE-VM** | `versions.pin:33` `golang:1.24.5-alpine@sha256:daae04eb...` digest pinned, `EXECUTION_BRIDGE_GO_TOOLCHAIN=1.24.5`, `cargo metadata --locked` + `go vet` pass, `git check-ignore` secrets ignored | — |
| `T1` | **FULLY DONE OFFLINE+SINGLE-VM** | `27_execution_intent.sql` + `ExecutionIntentBuilder` deterministic `trade_context_id` + `EXECUTION_INTENT_ENABLED=false` default, compute suite 319 run, DDL file exists | — |
| `T2` | **FULLY DONE OFFLINE+SINGLE-VM** | Java gateway `IntentReader` `subscribeFromBeginning(0)` + `NautilusIntentClient` fence re-check, `make up` 12 long-running/18 defined (`fluss-coordinator:9123` Started), live `GatewayFlussIntegrationTest` 1/1 3.5s `Skipped:0` + `FlussProjectionWriterIntegrationTest` 1/1 9.0s | — |
| `T3` | **FULLY DONE OFFLINE** | `go-bridge` separate module, fake HTTP+WS lifecycle, `broker_classification` 401/429->UNKNOWN, `EventHub` fan-out, `go test ./...` ok `0.236s`, network check | Live re-auth (AutoLogin) + `edge.arrow.trade` `Place` proven 2026-08-25 (`RCF-EQ×1` `26082501010305`); `order-updates` WS fill needs **funded market session** |
| `T4` | **FULLY DONE OFFLINE+SINGLE-VM** | `code/02_services/04_executor/` Rust binary `0.62.0` + `gate.rs:enter_reconciling_if_needed` + `executiongate` 20/20 `ContractViolation` + `gate` 9/9 + live `FlussGateStateStore` connect via gateway | — |
| `T5` | **FULLY DONE OFFLINE+SINGLE-VM** | `InMemoryAttemptStore` `by_instruction`/`has_instruction` 20/20, `Budget/Breaker` only `Transient`, `clockwatch` 7/7, fence stale 0 calls, `put(PREPARED)` before `bridge.call` + `CrashHooks after_prepared` | — |
| `T6` | **FULLY DONE OFFLINE+SINGLE-VM** | `PostbackCorrelator` 3-step bijective pure + Java `PositionProjector` 12/12 + Rust `projection` 7/7 i64 wrapping + live `FlussProjectionWriter` 9.0s | — |
| `T7` | **FULLY DONE OFFLINE+SINGLE-VM** | New `babysitter.rs:NoOpPositionObserver` 3/3 + `BabysitterPositionsSource.java` `positions` changelog source swap (`env.fromSource(flussPositionsSource)`), `fail-closed` when `POSITION_ACTIONS_ENABLED=true` | — |
| `T8` | **FULLY DONE OFFLINE+SINGLE-VM** | `docker-compose.yml` `execution-net`/`arrow-egress` + `execution-gateway` + `nautilus` `profiles:[execution-t3]` + `make up --profile execution-t3` → `execution-gateway Running` `execution-bridge Healthy` `nautilus Started` `gate HALTED health on 0.0.0.0:9190` | — |
| `T9` | **PARTIAL — MARKET-OPEN 2026-08-25** | Place proven to Arrow: `POST /order/regular` `26082501010305` `RCF-EQ×1 @₹105` (broker `MARGIN ERROR` — sandbox unfunded, not code). Auth AutoLogin live. | Full chain (funded place → WS fill → reconcile → cancel) + R2 1-year lock + 4VM HA `docker-stack.yml` needs **funded market session + 4VM** |

All T0-T8 as of 2026-08-24 `FULLY DONE` offline+single-VM green (`cargo 207/207`, `common 12/12` historical 2026-08-24 + `BabysitterPositionsSource`, `go ok`, `GatewayFluss 3.5s`, `ProjectionWriter 9.0s`, `make up --profile execution-t3` 18 Running) — not current C6 466/247/388. T3/T9 live market: place proven to Arrow (`MARGIN ERROR` — sandbox unfunded); fill still needs funded order.
### Live single-VM evidence — 2026-08-24 07:37/07:45 UTC (fluss-coordinator:9123)

```text
FLUSS_BOOTSTRAP=fluss-coordinator:9123 mvn test -Dtest=GatewayFlussIntegrationTest  :: PASS 1/1 3.5s Skipped:0
FLUSS_BOOTSTRAP=fluss-coordinator:9123 mvn test -Dtest=FlussProjectionWriterIntegrationTest :: PASS 1/1 9.0s Skipped:0
docker compose ps :: 12 long-running of 18 compose services (fluss-coordinator, fluss-tablet, flink-jobmanager, openobserve, otel-collector)
docker compose --profile execution-t3 ps :: execution-gateway Running, execution-bridge Healthy, nautilus Started, gate HALTED health 0.0.0.0:9190
cargo test --lib :: 207/207 pass (CHG-107: +1 clock-drift enforcement wiring test; CHG-106: +10 amend; CHG-102 re-count was 196: gate 13, executiongate 20, babysitter 3, projection 15, durable 11, resilience 8, clockwatch 7, config 12, gateway_protocol 13, http 23, bridge 14+, intent 14, engine 10, t9paper 12, etc.)
mvn -f common test -Dtest=PositionProjectorTest :: 12/12
go test ./... :: ok 0.236s
```

T0-T2,T4-T6,T8 now fully proven on single-VM Fluss (offline+sandbox), T7 observer fully offline. Remaining live gates are only multi-VM HA (`docker-stack.yml`) and the Arrow sandbox `RCF-EQ×1` funded fill (place proven 2026-08-25, `MARGIN ERROR`).

This progress table is evidence that the first slices are intentionally **not** reported as full
task completion. The system remains safe: the new bridge defaults to `disabled`, no broker
credentials are committed, no Rust execution service or Java gateway is connected, and no Signal
producer can submit an order.

### Repository-backed implementation audit — 2026-08-19

This audit was performed against the current files, not against task intent or documentation
claims. Status meanings are strict: **IMPLEMENTED** means the planned runtime behavior and its
tests exist; **PARTIAL** means only a contract, helper, scaffold, or offline slice exists;
**NOT IMPLEMENTED** means the planned component/path is absent. Existing prerequisite helpers do
not count as completion of the task that must integrate them.

| Task | Classification | Evidence inspected | Determining fact |
| --- | --- | --- | --- |
| `T0` | **PARTIAL** | `versions.pin:21-33`, `version_matrix.yaml:404-430`, `nautilus_trader` commit `74d57e7e...`, `CHG-039.md`–`CHG-041.md`, and `logs/nautilus-execution/phase1/` | Rust and Go target versions are recorded and Rust 1.97.1 is proven; the Go image digest, real credential-injection proof, policy approval, and final service capability evidence remain absent |
| `T1` | **PARTIAL** | `27_execution_intent.sql`, `ExecutionIntentBuilder.java`, `ExecutionIntentContextResolver.java`, `ExecutionIntentProducerFunction.java`, `SignalJob.java`, `SignalJobConfig.java`, and compute tests | Candidate rows still store null context, but the enabled producer resolves the approved deterministic entry context; only local enabled-graph Fluss evidence and T2 durable quarantine remain |
| `T2` | **PARTIAL** | `06_execution_gateway/` POM, `IntentReader`, `FlussControlStateStore`, `GatewayProtocol`, `FlussProjectionWriter`, `FlussProjectionLedgerStore`, `ProjectionApplier`, `GatewayHttpServer`, and 12 unit tests plus 1 env-gated integration test | Offline module and fail-closed runtime boundary exist; live Fluss scratch replay, real Nautilus endpoint evidence, and Compose wiring remain |
| `T3` | **PARTIAL** | `06_execution_bridge/go-bridge/main.go`, `server.go`, `broker.go`, `postback.go`, `fake_arrow_broker_test.go`, `go test -race ./...`, `execution_network_check.py`, resolved Compose `execution-t3` profile, Dockerfile | Offline fake place/modify/cancel/fill/partial-fill/reject/timeout/UNKNOWN/reconnect behavior and static network policy pass; live token refresh, cross-container route probes, and sandbox evidence remain |
| `T4` | **NOT IMPLEMENTED** | `code/02_services/04_executor/` contains only Python scaffold/Dockerfile/README/requirements; no `Cargo.toml` or Rust source | No Nautilus service or custom `ExecutionClient` exists |
| `T5` | **PARTIAL** | `code/common/.../GateTransitionValidator.java`, `InMemoryAttemptStore.java`, audit and identity tests | Rules and in-memory models exist, but no durable runtime invokes them before bridge calls |
| `T6` | **PARTIAL** | `code/common/.../PositionProjector.java`, `PositionProjectorDriver.java`, broker postback tests, projection DDLs | Reference arithmetic/schema helpers exist, but no Nautilus-to-Fluss projection path or rebuild/replay runtime exists |
| `T7` | **PARTIAL** | `BabysitterJob.java:65-97`, `BabysitterJobTest.java` | Strict no-op shell and fail-closed flag are tested, but source is still `fromElements(0L)` |
| `T8` | **PARTIAL** | T3 supplies `FakeBroker`; `docker-compose.yml:284-285,409-424` still leaves execution services commented | No end-to-end local path, private service network, checkpoint acceptance, or restart/recovery probe exists |
| `T9` | **NOT IMPLEMENTED** | no execution-specific sandbox/shadow evidence under `logs/`; Arrow VM rows remain `PINNED_AWAITING_EVIDENCE` | No real sandbox order, fill, reconciliation, shadow, or release review has occurred |

Verified commands for this audit:

```text
go vet ./... && go test -race ./...                         # execution bridge: PASS
mvn -o test                                                # compute: 319 run, 0 failures, 17 skipped
cargo metadata --locked --no-deps                           # pinned Nautilus workspace: PASS
cargo check --locked -p nautilus-execution -p nautilus-live -p nautilus-event-store  # PASS
```

These commands prove the implemented slices compile and test; they do not promote any partial
task to complete and do not authorize live orders.

### Task 0 — Freeze authority, versions, and change boundary — **PARTIAL**

**Objective:** Convert the design into pinned implementation inputs before writing runtime code.

**Files/components:**

- Modify `docs/08_implementation/05-execution-core.md` and this plan if evidence changes.
- Modify `code/01_platform/04_scripts/version_matrix.yaml` with the approved Nautilus commit,
  Rust toolchain, Go toolchain, and bridge SDK commit.
- Create the implementation change record using the next available `CHG-<N>` number when code/DDL
  work begins.


> **Offline vs live boundary (unchanged):** Every `T0–T8` row is `FULLY DONE offline+single-VM` on laptop (`make up` 12 long-running/18 compose services, `FLUSS_BOOTSTRAP=fluss-coordinator:9123` live single-VM green). Only `T9` (`RCF-EQ×1` live Arrow place proven to Arrow 2026-08-25 — `MARGIN ERROR`, sandbox unfunded; WS fill/reconciliation/shadow + single-operator enablement pending funded re-run) and the running-`LiveNode` event path (`WP-2` deferred) still need market/4VM.

---

## Close-gaps continuation — WP-0–WP-7 — integrated from `20`

> Source: `20-close-execution-service-gaps-plan.md` (231 lines). Each WP is ordered, owns disjoint file sets, and is deterministic/offline except `T9`/`WP-2` live pieces. Appendix below is verbatim from `20` §§1–8 (status now `2026-08-21/24` live-verified).

### Original 20 header — now integrated (see wrapper § Close-gaps continuation above) — verbatim excerpt follows
> **ROLE — WORKING PLAN (2026-08-21):** executable backlog for everything the
> `19-nautilus-execution-service-implementation-plan.md` audit (re-audited
> 2026-08-20) still lists as **PARTIAL** or **NOT IMPLEMENTED**. Each Work
> Package (WP) carries what-is-built -> what-to-do -> expected tests -> pass gate.
> It follows the dossier convention in `19-...plan.md` and `17-...plan.md`.
> The authoritative blueprint stays `19-...plan.md`; this file is the
> run-the-remaining-gaps continuation.
>
> **Scope boundary (user, 2026-08-21):** reuse the existing Go execution bridge
> (fake profile) and defer the full in-process Nautilus event path. Therefore
> WP-2's running-`LiveNode` event wiring is sequenced last / separately, NOT
> first. All WPs here are implementable with local Fluss + the fake/paper path
> except the explicitly-flagged IP-blocked items in §8 (live-Arrow evidence).

## Status

| Field | Value |
| --- | --- |
| Status | **WP-0, WP-1, WP-2, WP-3, WP-4, WP-5, WP-6, WP-7 DONE** (wired + live-verified 2026-08-21; Go digest pinned, T4 intent POST, LiveNode construction, runbooks); **T9 live evidence — IP gate ACCEPTED + TOTP auth PROVEN live 2026-08-21 (`execution-auth-001`, token len 238); pending only the DEC-044 release review + gate enable (not yet run)** |
| Owner | Execution and Platform teams |
| Affected | `04_executor` (Rust), `06_execution_gateway` (Java), `06_execution_bridge` (Go), common projection/attempt stores, Compose, `submit-jobs.sh`, `docs/08_implementation/19-...plan.md` |
| Baseline | Rust `cargo test --offline` **79 lib pass** (was 53; +`gateway_protocol`+`http`+`LiveNode` WP-2) + 1 live interop pass; common Maven 426; compute Maven 336+; Go `go test -race` green — **updated 2026-08-21 post-WP-7** |
| Change records | next CHG numbers assigned per WP (see §7) |

## 1. What was still NOT fully implemented (from the 19-plan audit) — **now resolved 2026-08-21 except T9**

> **Ledger note 2026-08-21:** this table was the open backlog at the 2026-08-20 re-audit. Every row except `T9` is now `DONE` and live-verified; the `Where it lands` column is kept for traceability.

| Task | Gap as audited 2026-08-20 | Where it landed | Status now | Evidence |
| --- | --- | --- | --- | --- |
| T0 | Go image digest; credential-injection proof; retention approval finalization | WP-7 | **DONE 2026-08-21** | `golang:1.24.5-alpine@sha256:daae04e…` pinned in `versions.pin` + `Dockerfile` (rebuild `9ae18fd9`), `t0-1787240279` (`SHA256SUMS`, `no_secrets:true`), `t0-arrow-scan.txt` only `01_ingestion`, retention **APPROVED 2026-08-20 by Saurabh** (CHG-055; WP-7 commits `4db6616`+`b093d97`) |
| T1 | Enabled-graph Fluss append proof; durable quarantine/audit writer | WP-4 (with gateway) | **DONE (live-verified)** | `FlussPostbackQuarantineStore` + env-gated live test (`LogScanner` reads `Postback_Quarantine` row) + differential parity `positions_oracle.json` `cargo test differentially` cross-language (CHG-052 `3f4594d`) |
| T2 | Live Compose wiring of the Java gateway; broker exactly-once across crash (-> T5) | WP-6 / WP-3 | **DONE (live wiring)** | `06_execution_gateway` on `[trading-net, execution-net]` with `--add-opens=java.base/java.nio=ALL-UNNAMED`, `GatewayHttpServer` `/healthz` 200, `FLUSS_BOOTSTRAP` DNS, `docker compose --profile execution-t3 up` live Fluss `9123` (WP-6 `e5f2a8c`+`53b9988`) — exactly-once crash gap deferred to T5 fence (correctly) |
| T3 | Cross-container runtime probe (gateway -> bridge -> Rust -> postback -> Fluss) | WP-6 | **DONE (probe)** | `bridge:8787/healthz` `UP disabled` + `gateway:9180/healthz` 200 on both nets + `nautilus:9190/healthz` `HALTED` + `POST /v1/intents` `503`/`401` private probes + `t8_sandbox_contract_check.py` 12/12 PASS (WP-6) |
| T4 | Executor Dockerfile; boot-HALTED binary; **running `LiveNode` event path (deferred by user)** | WP-1 / WP-2 | **DONE (WP-1) + DEFERRED (WP-2)** | WP-1: `src/bootstrap.rs`+`http.rs` (`/healthz`+`/readyz`, draining 503), `main.rs` `Runtime::init` HALTED, `Dockerfile` `rust:1.97.1` (WP-1); WP-2 `LiveNodeBuilder` now constructs via `FakeBridge`+`CacheView` (`780a643` `live_node_builds_with_bridge_client` proves `build` succeeds) — full `LiveNode::run` event loop stays **DEFERRED per user scope** |
| T5 | Fluss-backed `GateStateStore`/`AttemptStore` writers; env-gated gateway->Fluss integration | WP-3 | **DONE (writers glued + restart)** | `FlussGateStateStore`+`FlussAttemptStore` with `attemptRefreshOnRecovery`, `InMemory*` hydration, `ExecutionCommandGate` durable protocol, `cargo`+`mvn -o` green (CHG-051; WP-3) |
| T6 | Fluss-backed projection writers; Rust normalized Nautilus envelope emitter | WP-4 | **DONE (live-verified)** | `FlussProjectionWriter`+`Ledger` live-verified (`FlussProjectionWriterIntegrationTest` reads back `Positions`/`Order_Lifecycle`), `src/projection/mod.rs` i64 parity + `differential_parity.rs` vs Java oracle (CHG-052) |
| T7 | Env-gated MiniCluster + live Fluss restore run; `submit-jobs.sh` launcher | WP-5 | **DONE (live restore + launcher)** | `BabysitterJob` `RETAIN_ON_CANCELLATION` + `BABYSITTER_STATE_RECOVERY_PATH`, env-gated `COMPUTE_INT_TEST_T7` MiniCluster restore green, `submit-jobs.sh` waits `counts.completed>0` (CHG-053 `830d109`) |
| T8 | Fix executor compose credentials; wire gateway+executor; run topology; probes | WP-6 | **DONE (topology + re-scope)** | `ARROW_*` never on `executor:`/`nautilus` (T4 boundary, private `GATEWAY_SHARED_SECRET`), `execution-net internal:true`, `arrow-egress` bridge-only, zero host `ports:` for all 3, `12/12` sandbox + `execution_network_check` PASS (CHG-053/054) |
| T9 | **All live-Arrow evidence — not yet run (IP gate accepted 2026-08-21)** | §8 | awaiting Arrow login auth + DEC-044 release review | No real broker order/fill/reconciliation evidence yet; end of this plan's scope |

## 2. Work Package 0 — Production BridgeClient transport (DONE this session)

Built and verified (commit pending CHG): `src/bridge/transport.rs`
(`HttpBridgeClient`: `POST /v1/commands`, `GET /healthz`, Bearer auth, minimal
loopback HTTP client — no new deps), registered in `bridge/mod.rs`, plus the
**wire-contract fix** in `protocol.rs` (`rename_all = "snake_case"` on the three
wire types) so the Rust protocol matches the real Go bridge byte-for-byte.

- Tests: 5 offline unit tests + `tests/live_go_bridge.rs` interop against the real fake bridge.
- Evidence: `cargo test --offline` 53 pass; live interop `SUCCESS`/`fake-broker-order-1`.
- **On completion of this plan segment:** fold into CHG-049 (or a dedicated CHG), and
  update the `19-...plan.md` T4 row to note a production `BridgeClient` transport now exists.
- **Still open here (2026-08-21 UPDATE — now closed, CHG-079):** the WS `/v1/events` intake is **implemented** in `src/bridge/transport.rs` — `take_reports()` spawns `report_intake_loop` (a WebSocket transport with reconnect backoff, currently a loopback implementation rather than `tokio-tungstenite`), and the production `HttpBridgeClient` transport is now **selectable in `engine.rs` via `BridgeSelection` (WP-2 remainder, CHG-079)**: a configured `BRIDGE_ENDPOINT` selects `HttpBridgeClient`, no endpoint keeps the offline `FakeBridge` default; the service still boots the gate `HALTED`. Live interop vs the Go fake bridge proven (`cargo test --offline --test live_go_bridge` PASS, 2026-08-21).

## 3. Work Packages — implementable now (ordered)

### WP-1 — Rust: executor Dockerfile + boot-HALTED binary (T4 offline remainder) — **DONE 2026-08-21**

- **What's built (this pass):** real bootstrap `src/{bootstrap,http}.rs`; `main.rs` now parses
  `ServiceConfig::from_env()`, boots the gate `HALTED`, serves `GET /healthz` + `GET /readyz`
  (minimal dependency-free tokio HTTP server), and shuts down gracefully on Ctrl-C/SIGTERM
  (`/readyz` -> 503 while draining). `config.rs` relaxed to health-only boot: gateway/bridge
  endpoints optional (consumed only when connecting later), `EXECUTOR_LISTEN_ADDR` (default
  `127.0.0.1:8787`), `EXECUTION_ENABLED` still fails closed at boot, and it **never reads
  `ARROW_*`** (proven by a test). Multi-stage `executor/Dockerfile` already existed (T8.4, rust:1.97.1
  -> bookworm-slim, `ENV EXECUTION_ENABLED=false`) and is now fully sufficient for boot because
  endpoints are optional.
- **Tests / evidence:** `cargo test --offline` **63 pass** (added config 6, http 3, bootstrap 2),
  `clippy -D warnings` clean, `fmt` clean, LSP 0 findings. Live run: boots `HALTED`, `/healthz` 200
  `{"gate_state":"HALTED","enabled":false,"trading_ready":false}`, `/readyz` 200, 404 on unknown,
  SIGTERM -> draining -> clean exit rc=0. Docker image build is env-gated (needs network for the
  pinned Nautilus git deps + rust image; runs in WP-6 compose / CI).
- **CHG on completion:** CHG-050.

### WP-2 — Rust: running-`LiveNode` event path (T4 live boundary) — **DEFERRED (user choice)**

> **DONE 2026-08-21 (CHG-054, commit 780a643 — user-approved after WP-6):**
> `BridgeExecutionClientFactory::create` now builds a real `ExecutionClientCore`
> from the `LiveNode`-supplied `CacheView` (shared `Rc<RefCell<Cache>>`) and the
> deterministic `FakeBridge` (`Hedging`/`Cash`, `TRADER-001`/`ACCOUNT-001`/`SIM`).
> The client still boots `HALTED`; the "kernel cache private" boundary is resolved
> because `CacheView` is the intended sharing handle. `LiveNodeBuilder::from_config`
> → `add_exec_client` → `build` now succeeds and logs `Registered ExecutionClient-exec`
> (`cargo test --offline` 79 lib pass, new `live_node_builds_with_bridge_client`). The
> production `HttpBridgeClient` and `LiveNode::run` loop remain deferred, but the OMS/
> risk/portfolio/reconciliation surface is proven constructible offline.

### WP-3 — T5: Fluss-backed gate/attempt writers (Java) — **DONE** (completed 2026-08-21: glued + restart-refresh)

- **Writers (already shipped in commit 68e46f8, CHG-044/045 wiring):** `FlussGateStateStore`
  (fence/lease/ownership + single-operator (Saurabh, DEC-044) approval) and `FlussAttemptStore` (exactly-one PREPARED)
  in `code/common/.../schema/execution/`. They delegate the protocol to the InMemory
  implementations (the writes a raw KV upsert must never be trusted to provide) and persist each
  applied write to the v3 Fluss tables via UpsertWriter; `read()` falls back to Fluss lookup.
  In-memory stores retained for offline unit tests.
- **This pass:** added env-gated durable drill
  `FlussGateAttemptStoresIntegrationTest` (tag `integration`, gated on `FLUSS_BOOTSTRAP`, scratch
  tables created/dropped, schemas derived from the `Execution*Columns` ownership constants so they
  can never drift from the pinned v3 DDL). Proves on the durable store: gate HALTED-boot + epoch 0,
  monotonic fence token + owner persisted, single-operator (Saurabh, DEC-044) approval persisted, **PREPARED-before-bridge**
  (`prepare()` row found by raw Fluss lookup with gate_fence_token persisted), and **exactly-one
  command** (re-prepare same `(instruction_id, request_hash)` returns DUPLICATE, one row).
- **Tests / evidence:** `mvn -o -pl common test-compile` clean; full `mvn -o -pl common test`
  **426 run, 0 fail, 0 error, 1 skip** (unrelated env-gated test). New drill compiles and is correctly
  env-gated (contributes 0 without `FLUSS_BOOTSTRAP`). The drill itself is **env-gated and NOT run
  here** — no Fluss cluster is up on `localhost:9123` (runs in WP-6 compose / CI, same leg as the
  Docker image and Positions drill). Cross-restart zero-duplicate is enforced by the command gate's
  reconciliation over these durable rows (babysitter path, WP-5), not by the store alone — recorded
  in the test javadoc.
- **CHG on completion:** CHG-051.

**Completion pass (2026-08-21):** the earlier "DONE" was correctly challenged — the Fluss writers
existed but were **not glued** (zero production callers; engine still on in-memory) and had **no
restart-refresh** (a restarted process would re-mint). Both closed now:
  1. **Wired** — `ExecutionGatewayMain` now opens `FlussGateStateStore` + `FlussAttemptStore` from
     `GatewayConfig` (`EXECUTION_GATE_TABLE`/`EXECUTION_ATTEMPTS_TABLE`, shared Fluss client) in the
     try-with-resources, failing fast at boot if the v3 DDL tables are absent, and registers
     readiness. (Constructing `ExecutionCommandGate` against them is the executor bridge order path,
     WP-2 — no production `BridgeCaller` exists yet, so that is not seeded with a fake.)
  2. **Restart-refresh (hydration)** — `InMemoryGateStateStore.hydrate` (install row + seed the
     monotonic fence sequence), `InMemoryAttemptStore.hydrate` (rebuild identity + replay-key
     indexes); `FlussGateStateStore.read/init` and `FlussAttemptStore.prepare/transition/resolveUnknown`
     hydrate-if-absent by lookup so a restarted process re-derives prior fences/approvals/attempts and
     returns `DUPLICATE` instead of minting a second PREPARED.
- Tests: offline `InMemoryStoreHydrationTest` (fence monotonic across hydrate; duplicate-after-restart);
  env-gated `FlussGateAttemptStoresIntegrationTest` gained a **cross-restart** method (fresh instance B
  re-derives state + returns DUPLICATE); `mvn -pl common,02_services/06_execution_gateway -am test` ->
  **428 common / 18 gateway pass** (env-gated skips only). **Live-verified 2026-08-21**: the env-gated
  drill ran (not skipped) against a real local `apache/fluss:0.9.1-incubating` cluster from the repo's
  own compose — both methods passed (`Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`), so the
  prepared-before-bridge / exactly-one and cross-restart zero-duplicate claims now hold on the durable
  store against real Fluss, not just offline.

### WP-4 — T6: Fluss-backed projection writers + Rust emitter + T1 quarantine + differential parity (Java + Rust) — **DONE (live-verified + cross-language parity, CHG-052)**

- **What's built:** pure-JVM projection engine + ledger (426 tests green); gateway `FlussProjectionWriter`/`FlussProjectionLedgerStore`.
- **Status note (2026-08-21, reopened for steps 2-3 + parity, now complete):**
  - **Step 1 (live-verified):** `FlussProjectionWriterIntegrationTest` drives the *real* `FlussProjectionWriter` against a live Fluss cluster — postback -> normalized envelope -> projection rows land in `Positions`/`Order_Lifecycle`/`Order_Correlation` (read back via `Lookuper`), upsert idempotent.
  - **Step 2 (Rust emitter):** new `code/02_services/04_executor/src/projection/mod.rs` — faithful port of `PositionProjector`/`PositionProjectorDriver`/`KvStateUpdateProtocol`/`PositionLifecycle`; `ProjectionEmitter::emit_fill` maps `ReportEnvelope` -> Positions row; i64 arithmetic mirrors Java `long` for bit-identical parity.
  - **Step 3 (T1 quarantine/audit):** `FlussPostbackQuarantineStore` appends to the `Postback_Quarantine` LOG; env-gated live test reads the row back via `LogScanner` (passed live).
  - **Differential parity (cross-language):** `DifferentialParityFixtureTest` (Java oracle) output pinned as `tests/fixtures/positions_oracle.json`; `tests/differential_parity.rs` reproduces it field-for-field + the oversell -> VIOLATION negative. Rust == Java for `BUY10@1000, BUY5@1100, SELL8@1050, SELL7@1060` -> CLOSED open==closed==15, avgEntry 1033, avgExit 1054.
  - **Gateway-protocol HMAC envelope parity (Java ↔ Rust, NEW 2026-08-24):** `GatewayProtocolParityTest` (Java, 5/5) + 5 parity tests in `gateway_protocol.rs` (14/14 scoped incl. 8 pre-existing) — byte-identical canonicalization proven on a deliberately non-alphabetical payload (`{"zulu":"z","alpha":"a","qty":100}`): payload_json, sha256 payload_hash, HMAC auth, outer field order all equal; Rust `verify(Java token)` + Java `verify(Rust token)` both accepted; no canonicalization defect; preserve_order (Cargo.toml L39) retained.
- **What to do:** *(all done)*
  1. ✅ Wire projection writers to Fluss (env-gated int test): postback -> normalized envelope -> idempotent `Postback_Projection_Ledger` + fill/lifecycle/position projections; no arithmetic in JVM/Rust.
  2. ✅ Rust normalized Nautilus-envelope emitter from `bridge/client.rs` (maps `ReportEnvelope` -> projection rows) — the exact seam WP-2 will reuse.
  3. ✅ T1 durable quarantine/audit writer closes here.
- **Tests / gate:** common Maven green; env-gated gateway projection + T1 quarantine int tests green (live); `cargo test --offline` + `clippy -D warnings` + `fmt --check` clean; differential parity vs `PositionProjectorDriver` oracle holds cross-language.
- **CHG:** CHG-052 (reopened for steps 2-3 + parity, now records full WP-4 completion).

### WP-5 — T7: Babysitter live restore + launcher (Flink)

- **What's built:** `BabysitterJob` reads `Positions` changelog, checkpointed ValueState version gate, offline restore tests (319 compute green — docs-audit C6 2026-08-21, was 336 pre-CHG-005).
- **Status: DONE — live-verified (CHG-053).** Both items closed:
  1. ✅ Env-gated MiniCluster + live Fluss restore run (`COMPUTE_INT_TEST_T7`): start -> checkpoint -> restore -> duplicate is a no-op, **green against the real dev Fluss cluster** (phase 1 checkpoints & retains `chk-N`, phase 2 on a fresh MiniCluster restores from it and stays a no-op observer through replay/stale/conflict; no action/execution table created, `Positions` unchanged).
  2. ✅ `submit-jobs.sh` launcher wiring for Babysitter (submit + wait for readiness/checkpoint) — already present and now exercised: waits on `counts.completed > 0`, fails closed.
  - **Hardening folded in while making the gate green:** Babysitter now externalizes checkpoints (`RETAIN_ON_CANCELLATION`, mirroring SignalJob) so a deliberate cancel/restart retains observation state; added optional `BABYSITTER_STATE_RECOVERY_PATH` restore (same `StateRecoveryOptions.SAVEPOINT_PATH` key SignalJob uses). Two test-harness bugs fixed: the poll helper required the same latest checkpoint across two 5 s polls (impossible for a 2 s-interval job) and the computed recovery path was never wired into the job.
- **Tests / gate:** ✅ live-restore int test green (BUILD SUCCESS); offline config/job suite green; launcher waits for a completed checkpoint (`counts.completed > 0`).
- **CHG on completion:** CHG-053.

### WP-6 — T8/T3: local Compose topology, probes, credential re-scope

> **DONE 2026-08-21 (CHG-053 + CHG-054, commits e5f2a8c + 53b9988, live-verified):**
> 1. ✅ T8 contradiction fixed — `executor:` no longer carries `ARROW_*` (T4 boundary, still disabled, `EXECUTION_ENABLED=false`).
> 2. ✅ `execution-gateway` (`trading-net` + `execution-net`, `--add-opens`, `FLUSS_BOOTSTRAP` DNS) + `nautilus` (`execution-net`, `POST /v1/intents` with `GatewayProtocol` HMAC, 503 `HALTED` fail-closed) are profile-gated `execution-t3` with zero host ports; `nautilus` gets `GATEWAY_SHARED_SECRET` (private, never logged).
> 3. ✅ `docker compose --profile execution-t3 up` live on `01_docker` Fluss (9123, after freeing `p10` conflict) + cross-container probes: `nautilus:9190/healthz` `HALTED`, `bridge:8787/healthz` `UP disabled`, `gateway:9180/healthz` on both nets, `POST /v1/intents` valid envelope → 503 `gate HALTED` / bad auth → 401 / `GET` → 405.
> 4. ✅ `t8_sandbox_contract_check.py` 12/12 PASS + `execution_network_check.py` PASS + `docker compose config` zero ports + `arrow-egress` is bridge-only. No live order route (`EXECUTION_ENABLED` never true, gate `HALTED`).

### WP-7 — T0 cross-cutting + evidence/runbooks

> **DONE 2026-08-21 (CHG-055, commits 4db6616 + b093d97, T0 bundle t0-1787240279):**
> Go builder image digest pinned (`golang:1.24.5-alpine@sha256:daae04e…` in `versions.pin` + `Dockerfile`, rebuild verified); credential-injection proof captured (`t0-git-ignore.txt` for `.env`, `t0-arrow-scan.txt`/`t0-compose-arrow.txt` shows `ARROW_*` only in `01_ingestion` exception); execution runbooks added (`docs/06_operations/01-runbooks.md` § Execution service runbooks) and retention recorded as **APPROVED** below. Remaining `BLOCKED` is only T9 — and its broker static-IP gate was **accepted 2026-08-21** (new Arrow API `New_API_new_Static_IP`, App ID `2177ba96adc0`, Primary IP `152.58.33.134`); live TOTP auth was proven the same day (`logs/execution/execution-auth-001-20260821.md`); it now waits only on the single-operator (Saurabh, DEC-044) release review and the gate enable for a bounded order.
- **Retention — APPROVED 2026-08-21 by Saurabh (owner, this turn):** the one-year baseline `SAURABH-1Y-APPROVAL-2026-08-20` placeholder is now **approved** by Saurabh for the current scope. Evidence bundles `t0-…`/`t4-…` `retention_policy` remains `SAURABH-1Y-APPROVAL-2026-08-20` (commit `4db6616`/`b093d97`); under DEC-044 the evidence review requirement is satisfied by the single `saurabh` review (the legacy `two_person_review` field for the T0 bundle is satisfied by that review alone — a second reviewer is not required and not checked). Longer retention remains policy-driven per CHG-038/DEC-043.

## 4. Sequencing and dependencies

```text
WP-0 done ---> WP-1 (executor image+boot) ---> WP-6 (compose, needs WP-1 image)
                 |
                 +---> WP-3 (Fluss gate writers) ---> WP-4 (needs WP-3 stores + emitter)
                 |
                 +---> WP-5 (Babysitter, Flink -- parallel to 3/4)
WP-7 (cross-cutting, any time)   WP-2 (running LiveNode -- after WP-6, deferred per scope)
```

## 5. Verification commands (run after each WP)

```text
cargo test --offline && cargo clippy --all-targets -D warnings && cargo fmt --check   # WP-1/2/4 rust
mvn -o test                                                                          # common + compute + gateway (WP-3/4/5)
go test -race ./...                                                                  # bridge (WP-6)
COMPUTE_INT_TEST_T7=1 mvn test                                                       # WP-5 env-gated restore
EXECUTION_BRIDGE_URL=... EXECUTION_BRIDGE_AUTH_TOKEN=... cargo test --test live_go_bridge   # WP-0 regression
python t8_sandbox_contract_check.py                                                   # WP-6
```

## 6. Final-checklist mapping (from 19-plan §12)

| 19-plan unchecked item | Closed by |
| --- | --- |
| Custom `ExecutionClient` lifecycle + bridge protocol tests | WP-2 (deferred) |
| Rust service starts `HALTED` | WP-1 |
| Nautilus only production authority | WP-2 (deferred) |
| Fluss projections + ledger recover idempotently | WP-4 |
| Local Compose runs full sandbox topology, no live route | WP-6 |
| Restart/unknown/fencing/duplicate/quarantine/recovery evidence + runbooks | WP-7 |
| Arrow sandbox order evidence | §8 (IP gate accepted 2026-08-21; awaiting auth + release) |

## 7. Change-record assignments

| CHG | Work | Depends on |
| --- | --- | --- |
| CHG-049 | WP-0 transport + wire-contract fix (this session) | none |
| CHG-050 | WP-1 executor Dockerfile + boot-HALTED | — |
| CHG-051 | WP-3 Fluss gate/attempt writers | WP-0 |
| CHG-052 | WP-4 projection writers + Rust emitter | CHG-051 |
| CHG-053 | WP-5 Babysitter restore + launcher | — |
| CHG-054 | WP-6 compose topology + credential re-scope | CHG-050 |
| CHG-055 | WP-7 evidence + runbooks | all |

## 8. Explicitly NOT in this plan (IP gate accepted 2026-08-21 — awaiting auth + release review)

- **T9** live-Arrow order-path evidence (real `RCF-EQ x1`, broker id `26082501010305` + `remarks` — place proven 2026-08-25, `MARGIN ERROR` pending funded re-run).
- **T9** live fills/WebSocket transcript, live reconciliation snapshots, live shadow mode.
- **T3** real sandbox authentication / auto re-auth.
- **T3** stop-loss carriage (`SL-LMT`/`SL-MKT`) — refused fail-closed, not an IP-gate item: the command envelope and fingerprint carry no stop trigger, so an SL order is rejected outright rather than submitted with its trigger silently dropped (P3-043, `8216307`). Revisit when a caller needs SL orders through the bridge; the change moves the wire contract on both sides, so it needs a coordinated deploy (envelope + fingerprint + Rust protocol + mapping).
- Full-path **real** runtime evidence over live Arrow.

Each of these was gated on the broker's static-IP acceptance (gate **accepted 2026-08-21**) and the
single-operator (Saurabh, DEC-044) release review; they now await an Arrow login auth path and that review,
and until then the system remains `HALTED`/`disabled` by design.


---

## Supersession and source of truth

| Document | Status 2026-08-24 | Where its content now lives |
|---|---|---|
| `05-execution-core.md` (this file) | **SOLE SOURCE OF TRUTH** — normative design + `19` T0–T9 + `20` WP-0–WP-7 | Here |
| `05-action-capture.md` / `06-babysitter.md` / `07-executor.md` | SUPERSEDED & deleted `2026-08-18` `74f3d89` | `05` §§ Why one dossier / Ownership matrix |
| `19-nautilus-execution-service-implementation-plan.md` | SUPERSEDED `2026-08-24` — stub points here | `05` § Implementation plan — Nautilus execution service (T0–T9) |
| `20-close-execution-service-gaps-plan.md` | SUPERSEDED `2026-08-24` — stub points here | `05` § Close-gaps continuation — WP-0–WP-7 |

> **Change discipline:** All future execution-domain edits land in `05-execution-core.md`. `19`/`20` are frozen stubs — do not extend them. Git history preserves their full text (`19` at `1856` lines, `20` at `231` lines). `CHG-028`/`DEC-042`/`DEC-043`/`DEC-044` remain authoritative.

## Verification mapping (unchanged — plus T0–T8 hardening)

- [Action Capture test design](./11-testing-and-release.md#action-capture)
- [Babysitter test design](./11-testing-and-release.md#babysitter)
- [Executor test design](./11-testing-and-release.md#executor)
- **T0–T8 hardening now gated in `make docs-audit`:** `cargo clippy -D warnings` (207 Rust, 0 errors) + `cargo fmt --check` + `go vet ./...` (bridge), proven `2026-08-24` `make docs-audit` hardening `Finished` before `C14` pre-existing 19 fails. See `Makefile:295-303`.
- **Single-VM live prove:** `FLUSS_BOOTSTRAP=fluss-coordinator:9123 mvn test -Dtest=GatewayFlussIntegrationTest` `PASS 1/1 3.5s` + `FlussProjectionWriterIntegrationTest` `PASS 1/1 9.0s` (`execution-gateway` `Running`, `execution-bridge` `Healthy`, `nautilus` `HALTED 9190`).
