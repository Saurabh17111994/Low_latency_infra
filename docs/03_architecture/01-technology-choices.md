# Technology Choices

## Status

Technology choices are architectural targets. The following versions are now **confirmed** (DEC-021, 2026-07-23):

- **Fluss:** 0.9.1-incubating
- **Flink:** 2.2.1
- **Java:** 17.0.19
- **Flink-Fluss connector:** `fluss-flink-2.2-0.9.1-incubating.jar`

Arrow protocol evidence is available from the Go SDK (`github.com/arrow-trade/go-arrow`) and REST API docs (`docs.arrow.trade`). Exact container image digests remain deferred until build.

## Apache Fluss

Fluss is the live streaming bus and operational storage layer.

- LOG tables hold immutable events and audit records.
- KV tables hold current materialized operational state.
- Table ownership, distribution, retention, changelog behavior, replication, and lake tiering are explicit contracts.
- `partial_update` is permitted only with tested column ownership, stale-update rejection, and merge semantics.
- No cross-table atomicity is assumed without a version-specific connector test.

Required logical tables include `raw_table_1`, `feature_candles_15s`, `forming_bar`, `Signal_Candidates`, `Signal_Candidates_current`, `Order_Lifecycle`, `Positions`, `Fills`, `Postback_Projection_Ledger`, `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`, `Safety_Halt_Requests`, `Postback_Quarantine`, `suspected_discontinuities`, `ingestion_quarantine`, and `instruments`. (`Ranking_Results`, `Trade_Decisions`, `Portfolio_Reservations` REMOVED 2026-08-15, CHG-005; `Position_Actions` is future phase only.)

**Confirmed version:** Fluss 0.9.1-incubating. Features: `BYTES` column type, KV tables with `partial_update` and FULL changelog images, `$changelog` virtual tables for CDC/audit, Aggregation Merge Engine, Auto-Increment columns for dictionary tables, ARRAY/MAP/ROW/nested complex types, ALTER TABLE schema evolution (zero-copy append), Snapshot Leases for consumer-safe snapshots, Cluster Rebalance, Compacted LogFormat, Iceberg/Parquet/Lance lake formats, Azure Blob + ADLS Gen2 support, POJO Java client API. See [Fluss 0.9 release blog](https://fluss.apache.org/blog/releases/0.9/).

The exact server/client release is now pinned rather than evidence-gated.

## Apache Flink

MVP has exactly two jobs:

1. **Signal job:** deduplication, event-time candle computation, forming-bar detection, Business Logic. **(In-operator ranking, portfolio reservations, and instruction publication REMOVED 2026-08-15, CHG-005.)**
2. **Babysitter job:** checkpointed position observation and strict no-op action behavior in MVP.

Flink managed state and Fluss sink guarantees are exactly-once only at the tested, version-pinned boundary. They do not make broker REST calls or independent projections exactly-once.

**Confirmed version:** Flink 2.2.1 with Java 17 support (Java 17.0.19 is the pinned runtime). New in 2.2: VECTOR_SEARCH, Materialized Tables, Delta Joins, Balanced Tasks Scheduling. ⚠️ Flink 2.x has a significantly different DataStream API surface vs 1.x — existing 1.x code patterns must be migrated.

Ingestion is one Java 17 service process from binary WebSocket decode through the supported Fluss 0.9.1-incubating Java client append path. The Arrow HFT market-data WebSocket (`wss://socket.arrow.trade`) uses a binary protocol with 2 modes (LTPC 40B, Full 196B), little-endian integers, zstd-compressed inbound, prices in **paise** (÷100 for rupees). No JSON on the market-data stream. Auth via `appID` + `token` query params, subscribe via JSON token IDs (≤1024 per connection, ≤512 per request). Heartbeat: client sends `PONG` text every 3s, stall timeout 15s. (The Standard feed `wss://ds.arrow.trade` was removed 2026-08-14.)

**No OpenAlgo.** DEC-006's direct-Executor wording is superseded by DEC-041/DEC-042: the Nautilus Execution Service commands a localhost-only go-arrow bridge, and only that bridge calls Arrow's native REST API.

## Nautilus Execution Service and Arrow REST

The integrated Execution Core is the only platform domain permitted to initiate money-moving calls. Nautilus runs as a separate long-lived Rust-native service and owns the live OMS, position/portfolio calculations, risk checks, fill handling, and reconciliation. A custom Nautilus `ExecutionClient` maps native commands and reports to a localhost-only go-arrow bridge. Only the bridge holds Arrow credentials and physically calls the REST/WebSocket APIs:

- `POST /order/regular` — place order. Request: `{exchange, symbol, quantity, transactionType: "B"/"S", order: "LMT"/"MKT", product: "I"/"C"/"M", price, validity: "DAY"/"IOC", remarks (max 16 chars), mpp (bool)}`. Response: `{status:"success", data:{orderNo, requestTime}}`.
- `GET /user/orders` — order book (reconciliation).
- `GET /user/trades` — trade book (fills).
- `GET /user/positions` — current positions.
- `GET /order/{id}` — single order detail with full lifecycle history.
- `PATCH /order/regular/{id}` — modify order.
- `DELETE /order/regular/{id}` — cancel order.
- Auth: `appID` + `token` headers (token obtained from `/auth/app/authenticate-token`, 24hr expiry).
- Rate limit: 10 req/sec per endpoint.
- MKT orders: disabled by default; use `mpp:true` for upper-limit routing.
- Order lifecycle: PENDING → OPEN → COMPLETE (filled) / CANCELLED / REJECTED. `reportType` gives finer detail.

Postbacks arrive through a separate WebSocket (`wss://order-updates.arrow.trade`) consumed by the bridge and normalized into Nautilus execution reports/events.

Custom execution control glue durably persists an attempt, deterministic `client_order_ref`, gate epoch, and fencing token before Nautilus commands the bridge. Timeout, disconnect, malformed response, crash window, or ambiguous response produces `UNKNOWN`, halts the gate, and requires Nautilus/Arrow reconciliation before retry.

Nautilus is authoritative for live order, fill, position, PnL, and reconciliation behavior. Fluss remains the durable integration plane for immutable intent, safety control state, execution attempts/correlation, and queryable projections. Flink and Fluss SHALL NOT implement a competing production OMS or position engine. The Nautilus event store is an additional execution-history and replay boundary; its early-alpha status requires dedicated durability, verification, backup, and recovery evidence before live use.

## EOD controller

The EOD controller is a named service or scheduled job that owns manifest creation, verification, retry/backoff, retention extension, expiry protection, storage-pressure alerts, and manual reconciliation. It is not the brokers, the Signal job, or the Executor.

The controller persists durable state with restart/resume behavior. A trading day's source data cannot expire while its manifest is unverified, retryable, or under reconciliation. The controller emits critical alerts on verification failure, insufficient expiry margin, S3 unavailability, and storage pressure. The exact scheduler/runtime implementation remains evidence-gated.

OpenObserve is the target backend for structured operational logs, metrics, traces where supported, health signals, and alerts. Correlation IDs and immutable local execution audit remain mandatory if telemetry delivery is unavailable.

Observability must prove throughput, latency, deduplication, checkpoint health, replication, EOD manifests, gate state, unknown outcomes, reconciliation, approvals, security events, and every live-money acceptance gate.

## Iceberg and S3

Eligible immutable events are offloaded at EOD to encrypted Iceberg/S3 storage. The offload produces a verification manifest containing table/schema version, trading date, source range, row/byte counts, hashes/checksums, commit status, and retry state.

Source retention is at least three complete trading days and extends while the relevant manifest is unverified, retryable, or under reconciliation. Execution, order, fill, gate, correlation, approval, reconciliation, and future action audit is immutable, encrypted, integrity-verifiable, access-controlled, and retained for at least one year according to approved policy.

## Container runtime

- Local development and integration use Docker Compose.
- Production uses a four-VM Docker Swarm: v1: 3× Manager+Worker + 1 O2 (4 VMs), v2: 3× Manager ONLY + N≥3 Workers + 1 O2 (7 VMs) — same stack, see 09 v1→v2. Each v1 workload VM: 250 GB.
- Production images use immutable digests; `latest` and version ranges are prohibited.
- Production secrets use Docker Swarm secrets and least-privilege service identities.
- Production network traffic uses mandatory encrypted overlay/TLS-protected transport for all sensitive paths (broker, Arrow REST, S3, operator control, secret delivery, and cross-host money-moving/state traffic). Exact mechanism remains evidence-gated but encryption is not optional.
- MVP requires four mandatory alert groups with owner, threshold, routing, and acknowledgement: order safety, streaming health, storage safety, and security. Critical alerts have defined escalation, remediation, and closure evidence.
- Every managed or durable state category must have a cardinality bound or evidence-gated measurement plan, serialized-size estimate, cleanup trigger, checkpoint contribution, and restore size/time for production readiness.
- Approved audit retention requires WORM-equivalent immutability, legal-hold capability, key rotation with historical decryptability, role-restricted access with access audit, retrieval evidence, event-to-manifest hash-chain integrity, and single-operator (Saurabh, DEC-044) authorized deletion where policy permits. Exact storage mechanisms remain evidence-gated. **2026-08-14: on the configured store (Cloudflare R2) the WORM mechanism is 'bucket locks' — prefix retention rules (duration / until-date / indefinite) via the Cloudflare dashboard/Wrangler/API; the S3 Object Lock API is not implemented on R2.**

Compose is deliberately simpler, but it cannot prove production HA or live-money safety.

## Technology decision references

- Active decisions: `../01_project/04-decisions.md`
- Non-functional requirements: `../02_requirements/03-non-functional.md`
- Platform requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Build contracts: `../04_contracts/01-ingestion.md`, `../04_contracts/02-storage.md`, `../04_contracts/03-compute.md`, `../04_contracts/07-executor.md`
