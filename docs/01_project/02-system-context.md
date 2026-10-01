# System Context and Ownership

## Current topology

```text
Arrow market-data WebSocket — live standard token stream (wss://ds.arrow.trade, `ARROW_FEED=token`) + selectable HFT (wss://socket.arrow.trade); the 2026-08-14 Standard-feed removal note describes the HFT-only period, ended 2026-09-24
  → Ingestion (binary decoder, epoch-sec → epoch-ms, paise → rupees)
  → Fluss raw_table_1 LOG
  → Signal Flink job
      ├─ bounded fingerprint deduplication
      ├─ event-time candle state
      ├─ forming-bar signal detection
      ├─ Signal_Candidates LOG
      ├─ Signal_Candidates_current KV
          → Fluss immutable execution intent
          → Nautilus Execution Service
              ├─ OMS, risk, portfolio/position engine, reconciliation
              ├─ custom gate/fencing and Fluss projection sinks
              └─ Nautilus ExecutionClient → localhost go-arrow bridge
                  → Arrow REST (POST /order/regular)
                  → Arrow broker

Arrow order-updates WebSocket (wss://order-updates.arrow.trade, JSON)
  → go-arrow bridge order-update intake
      → Nautilus Execution Service
      ├─ Fills LOG
      ├─ Order_Lifecycle KV
      ├─ Positions KV (fill-derived)
      └─ Postback_Quarantine LOG
          ↕
      Babysitter Flink job
          → position-management action KV (future)
          → Executor

Eligible LOG tables → EOD Iceberg/S3
All services → OpenObserve
```

## Deployable components

| Component | Owns | Does not own |
| --- | --- | --- |
| Ingestion | Decode, normalize, fingerprint, append raw ticks, report suspected discontinuities | Business enrichment, candle aggregation, order placement |
| Fluss storage | Table schemas, distribution, retention, changelog and lake tiering | Strategy rules or broker calls |
| Signal Flink job | Dedup, candles, forming-bar detection, candidate audit (**ranking/winner output REMOVED 2026-08-15, CHG-005**) | Broker REST side effects, fill lifecycle |
| Nautilus Execution Service | Authoritative order lifecycle, fills, positions, PnL, reconciliation, risk, event processing, and execution projections | Strategy computation, market-data ingestion, direct Arrow network access |
| go-arrow bridge | Arrow authentication, REST order calls, order-update WebSocket, broker reads | Strategy, gate decisions, position arithmetic |
| Execution control glue | Fluss instruction intake, durable gate, attempts, correlation, fencing, quarantine, and projection orchestration | Order arithmetic, broker protocol implementation |
| Action Capture | Historical name for the capture path now implemented inside the Nautilus Execution Service | Strategy, position decisions, independent order state machine |
| Babysitter Flink job | Post-entry position-management decisions | New entry signals, authoritative order lifecycle |
| Executor | Historical name for the order path now implemented by Nautilus plus custom execution control glue | Strategy, direct Arrow REST calls, independent OMS or position state |
| Arrow REST | Broker order entry and management (`https://edge.arrow.trade`) | Fluss consumption, strategy, fill capture, gate decisions |
| OpenObserve | Logs, metrics, traces, operational alerting | Trading decisions |

## Identity model

The old overloaded `order_id` term is replaced by three identities:

| Identity | Created by | Lifetime and purpose |
| --- | --- | --- |
| `instruction_id` | Business Logic | Stable platform decision; one winning instruction can produce multiple broker orders |
| `client_order_ref` | Executor | Short, deterministic reference sent through Arrow `remarks` (maximum 16 characters) |
| `broker_order_id` | Arrow broker | Broker's authoritative order identity returned by order submission and postbacks |

Persist the mapping between these identities before treating an order as safely reconciled. A trim, exit, or re-entry is a new broker order linked to the same position/trade context, not a mutation of the original broker order.

## State ownership

Order lifecycle and position lifecycle are different aggregates:

- **Order lifecycle:** Nautilus OMS state, normalized to `SUBMITTING`, `PENDING`, `PARTIAL`, `FILLED`, `CANCELLED`, `REJECTED`, `UNKNOWN`; keyed by `broker_order_id`.
- **Position lifecycle:** Nautilus position state, normalized to `FLAT`, `OPEN`, `REDUCING`, `CLOSED`, `UNKNOWN`; keyed by `position_id` and linked to `trade_context_id`.
- **Execution authority:** Nautilus is authoritative for live order/fill/position state; Fluss contains durable projections for platform consumers.
- **Order gate:** `ENABLED`, `HALTED`, `RECONCILING`, `APPROVAL_PENDING`; owned by custom execution control glue and enforced before Nautilus commands the bridge.

`partial_update` protects independent column groups from clobbering. It does not by itself solve stale writes, ordering within one column group, broker-side idempotency, or external REST atomicity.

## Runtime environments

| Environment | Orchestrator | Purpose |
| --- | --- | --- |
| Local/development | Docker Compose | Fast single-host development and integration testing; singleton platform services are acceptable |
| Production | Docker Swarm | Four-VM deployment: three HA workload VMs and one dedicated observability VM; placement, replicas, persistent storage, and quorum rules are mandatory |

The production topology is not proven merely by running three processes. Failure tolerance and recovery must be demonstrated by the test plan.

## Source links

- Data flow and interfaces: [`../02_requirements/05-interfaces.md`](../02_requirements/05-interfaces.md)
- Data ownership: [`../02_requirements/04-data.md`](../02_requirements/04-data.md)
- Runtime details: [`../02_requirements/02-functional/09-platform-runtime.md`](../02_requirements/02-functional/09-platform-runtime.md)
- Detailed Nautilus operating model (ownership matrix, service topology, boundary contracts,
  identity mapping, trade flows, position management, migration roadmap):
  [`../08_implementation/05-execution-core.md`](../08_implementation/05-execution-core.md#recommended-operating-model)
