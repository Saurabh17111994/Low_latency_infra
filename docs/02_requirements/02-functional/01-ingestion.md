# 02.1 — Ingestion

## Purpose and readiness

Ingestion is the sole market-data entry point. It connects to the evidence-approved broker stream, preserves each original packet losslessly, maps verified fields into normalized typed columns, computes a versioned event fingerprint, and appends to Fluss. The production workload has a variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) with a hard 20 ticks/s/instrument maximum (DEC-045). The acceptance gate is 60,000 ticks/s sustained; the 90,000 ticks/s capacity-peak campaign is retired (DEC-036).

**Tier-scoped deployment (current testing phase):** Arrow basic tier provides one WebSocket connection; premium tier provides three. The current testing phase runs on the basic tier with exactly **one HFT connection** and the approved **1,024-instrument manifest** `Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv` (unique tokens, quoted CSV). The full 3,000-instrument / 3-connection coverage remains the approved future production target and is **deferred** to a later phase, which requires the 3-socket capability evidence before activation. The implementation MUST accept a configured connection count and manifest path so the deferred phase is a configuration change, not a rewrite.

Live-money readiness is blocked until Platform and Execution provide official protocol artifacts or captured packets and compatibility tests pass against the exact deployed decoder version.

## Constraints

- Ingestion SHALL NOT insert Kafka, ZeroMQ, Python, or any intermediate transport between the Go arrow-bridge (decode) and Fluss append. The process boundary is two colocated processes in one container (Go bridge + Java); stdin/stdout is the kernel pipe, not a transport.
- Ingestion SHALL NOT silently drop a validly received packet. Every accepted or rejected packet SHALL produce audit evidence.
- Ingestion SHALL NOT guess broker fields. Unknown packet versions are quarantined; unknown fields remain recoverable through `raw_payload`.
- Ingestion SHALL NOT use ingestion time as event time. If no verified broker event timestamp exists, the packet SHALL be quarantined or marked invalid.
- Ingestion SHALL NOT calculate exact missing sequence ranges unless future broker evidence proves a suitable sequence exists.
- Ingestion SHALL NOT perform logical deduplication. Bounded fingerprint deduplication is owned by the Signal Flink job.
- Ingestion SHALL NOT perform inline historical backfill in MVP.
- Ingestion SHALL NOT log original packet bytes, credentials, or secrets.
- Ingestion SHALL accept variable broker arrivals. It SHALL NOT require or infer a fixed 50 ms tick interval.
- Synthetic workload profiles use an ≈16.7 ticks/s/instrument baseline average at the 50,000 gate (the generator keeps a 20 ticks/s/instrument capability, MOCK-UNIT-002) and SHALL enforce a 20 ticks/s/instrument maximum; live broker arrivals below or above the baseline are valid.
- Ingestion SHALL NOT batch ticks. Each accepted tick SHALL be submitted immediately. `INGESTION_MAX_BATCH_RECORDS` SHALL validate within `1..1000` (default `1`) and `INGESTION_MAX_BATCH_WAIT_MS` SHALL validate within `0..100` (default `0`). Startup SHALL fail for out-of-range values.
- Application-level batching beyond a single tick is prohibited.
- Ingestion SHALL maintain pending append counters in both records (`MAX_PENDING_APPEND_RECORDS=50000`, validated 100..1000000) and bytes (`MAX_PENDING_APPEND_BYTES = min(67108864, floor(container_memory_limit_bytes × 0.10))`).
- Before accepting a tick, ingestion SHALL reject it when accepting would exceed either pending limit.
- At 80% of either pending limit (`PENDING_APPEND_WARNING_PERCENT=80`), ingestion SHALL set readiness false and emit a warning event containing current records, current bytes, and both limits.
- At 100% of either pending limit, ingestion SHALL stop broker reads/subscriptions, keep readiness false, emit a critical event, and preserve an acknowledged-loss/uncertainty record. Silently discarding data is prohibited.
- Pending counters SHALL decrease only after the append completes, whether successful or failed.
- Fluss append uncertainty SHALL NOT be described as lossless delivery. Every retry and timeout is counted and exposed.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-ING-001 | TCP preserves order within each Arrow WebSocket connection, but the feed has no usable broker sequence number. | ASM-001 |
| ASM-ING-002 | The official Arrow Go SDK decoder remains compatible with protocol evolution through its published release cycle. | ASM-003 |
| ASM-ING-003 | The instrument manifest loaded at startup is authoritative and complete for the trading session. Runtime instrument changes require a controlled restart. | REQ-ING-004 |
| ASM-ING-004 | The Fluss Java client's buffering, retry, and append-acknowledgement behavior matches the pinned client version. | REQ-ING-008, REQ-ING-012 |
| ASM-ING-005 | Production hosts can maintain UTC clock offset within 100 ms. | REQ-ING-006 |
| ASM-ING-006 | A single ingestion process instance can sustain the variable 50,000 ticks/s baseline and safely bound/recover when arrivals exceed it. (The 90,000 ticks/s peak campaign is retired, DEC-036; 60,000 ticks/s gate, DEC-045.) | REQ-ING-016, RISK-007 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **At-least-once delivery:** The WebSocket-to-Fluss boundary is at-least-once. Retransmitted or replayed packets may produce duplicate raw rows. Logical deduplication belongs to the Signal Flink job.
- **Best-effort fingerprinting:** The event fingerprint is not exact identity. Identical legitimate events may be collapsed, and some duplicates may pass. Metrics distinguish duplicate candidates, dedup hits, and estimated collision risk.
- **No broker sequence assumption:** Ingestion cannot calculate exact missing tick counts or sequence gaps. Discontinuity records are suspected, not proven.
- **Bounded memory with hard limit:** Under sustained backpressure, ingestion makes readiness false before memory exhaustion. Packets may be dropped only under an explicit acknowledged-loss policy with readiness impact.
- **Partial subscription is not READY:** Unless an explicitly approved degraded mode exists, all configured instruments must be subscribed for readiness.
- **Unknown outcomes are counted, not hidden:** Append timeouts, Fluss unavailability, and uncertain write outcomes increment uncertainty counters and affect readiness. They are never silently absorbed.
- **Slow-Fluss policy:** Resolved by capacity. Fluss ingests up to 1-2 million ticks/s, and the platform's theoretical cap ceiling is 60,000 ticks/s (3,000 instruments × 20 ticks/s; sustained gate 60,000 per DEC-045). The steady state and peak are within Fluss capacity with margin, so neither a durable local SSD buffer nor a controlled subscription pause is required. Bounded pending-append limits (50,000 records / `min(64MiB, 10% container memory)` bytes) remain as the defensive backpressure bound; reaching them indicates a platform-capacity fault, not a normal operating condition.

## Out of Scope

The following capabilities are explicitly NOT owned by Ingestion:

- **Logical deduplication:** Owned by the Signal Flink job via bounded fingerprint state.
- **Candle computation and OHLCV aggregation:** Owned by the Signal Flink job.
- **Signal detection, strategy evaluation:** Owned by the Signal Flink job. **(Ranking REMOVED 2026-08-15, CHG-005.)**
- **Broker order submission and execution:** Owned by the Executor.
- **Postback capture, fill lifecycle, and position projection:** Owned by Action Capture.
- **Babysitter position monitoring and action emission:** Owned by the Babysitter Flink job.
- **EOD offload to Iceberg/S3:** Owned by the EOD controller.
- **Real-time gap reconciliation and automatic historical backfill:** Deferred; not in MVP scope.
- **Multi-broker support:** Deferred; not in MVP scope.
- **Runtime instrument manifest changes:** Deferred; a controlled restart applies a new manifest.
- **BSE and currency derivatives:** Deferred; not in MVP scope.
- **Premium-tier 3-connection / 3,000-instrument full coverage:** Deferred to a later phase; requires the 3-socket capability evidence from Arrow before activation. The current testing phase runs on basic tier (1 connection, 1,024-instrument manifest).

## REQ-ING-001: Process boundary

Ingestion SHALL decode through the official Arrow Go SDK (arrow-bridge subprocess) and write through the supported Fluss Java client path. Both processes SHALL run in the same container, connected by stdin/stdout. It SHALL NOT introduce Kafka, ZeroMQ, Python, or another transport between decode and Fluss.

The decoder MAY use a verified first-party SDK. If no supported SDK exists, it SHALL implement only the documented/captured protocol and pass the golden packet corpus. Unknown packet versions SHALL be quarantined rather than guessed.

**Acceptance:** A compatibility suite decodes all golden packets byte-for-byte and rejects unknown versions without process corruption.

## REQ-ING-002: Evidence-gated broker connection

Endpoint, authentication fields, compression, subscription limits, packet schemas, reconnect behavior, and token refresh semantics SHALL come from approved broker evidence. Current prose values are hypotheses until verified.

Configuration SHALL include endpoint, credential secret references, connection limits, subscription limits, reconnect backoff, protocol version, and decoder version. Secrets SHALL come from local `.env` only in development and Docker Swarm secrets in production.

**Failure behavior:** An unverified or unsupported protocol version sets readiness false, emits a critical alert, and prevents affected packets from entering the typed stream.

## REQ-ING-003: Connection identity

Each logical connection slot SHALL have:

- `connection_id`: stable identity derived from credential principal and slot.
- `connection_epoch`: monotonically increasing local epoch for each reconnect/restart assignment.
- `instance_id`: unique process instance.

No identity is assumed to be broker-global. Reassignment of instruments to another slot creates a new connection scope.

## REQ-ING-004: Instrument manifest

Ingestion SHALL load an explicit, versioned active-instrument manifest from CSV at startup. The approved manifest path for the current testing phase is `Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv` (1,024 unique NSE cash tokens, quoted CSV). The full 3,000-instrument manifest is the deferred production target. The manifest location may be a configurable path; the CSV format SHALL be the Arrow `GET /all` schema. Readiness requires:

1. Manifest file reachable and parsable.
2. Manifest version recorded.
3. Configured minimum instrument count met.
4. Every row validated for required routing fields (token, exchange, symbol, trading_symbol).
5. Subscription acknowledgements received for all required instruments (via the HFT `wss://socket.arrow.trade` sub message — the Standard feed was removed 2026-08-14).

Market hours by exchange:

- **NSE/NFO/INDEX:** 9:15 AM - 3:30 PM IST
- **MCX:** 4:00 AM - 11:30 PM IST

EOD offload timing: NSE after 4 PM IST, MCX after 11:30 PM IST.

Runtime instrument changes are deferred; a controlled restart applies a new manifest.

## REQ-ING-005: Packet preservation and typed mapping

For every accepted packet ingestion SHALL persist:

| Field                 | Requirement                                                                               |
| --------------------- | ----------------------------------------------------------------------------------------- |
| `raw_payload`         | Original broker packet bytes, losslessly encoded as BYTES/VARBINARY by the reconciled DDL |
| `payload_hash`        | Cryptographic hash of original bytes for integrity checks; algorithm/version recorded     |
| Typed columns         | Verified 1:1 field mapping and approved unit/time normalization only                      |
| `decoder_version`     | Exact decoder/schema version                                                              |
| `event_fingerprint`   | Versioned bounded duplicate fingerprint                                                   |
| `fingerprint_version` | Algorithm/canonicalization version                                                        |

A canonical decoded JSON representation MAY be emitted as a separate diagnostic field or lake derivation, but SHALL NOT replace original packet bytes.

Unknown fields remain recoverable through `raw_payload`. Unknown packet versions are quarantined with original bytes and reason metadata.

## REQ-ING-006: Timestamp semantics

- `event_time` is the verified broker event timestamp converted to UTC epoch milliseconds.
- `ingest_ts` is the synchronized local time immediately before append submission.
- `append_ack_ts` is the synchronized local time when Fluss acknowledges the append, used for write latency.
- Original timestamp bytes remain in `raw_payload`.

If no verified event timestamp exists, the packet SHALL NOT silently use ingestion time as event time. It is quarantined or marked invalid according to the approved packet contract.

Production hosts SHALL maintain UTC clock offset within 100 ms and expose offset metrics.

## REQ-ING-007: Tick classification

Packet richness and market event type SHALL be modeled separately. A full-depth packet is not automatically a QUOTE event.

A tick contributes to candle aggregation only when the verified payload represents an eligible trade and has valid `event_time`, `instrument_token`, price, and quantity. Quote/depth data remains available for future context but does not contribute to MVP OHLCV.

## REQ-ING-008: Delivery semantics

The WebSocket-to-Fluss boundary is at-least-once. Ingestion SHALL not claim broker or Fluss retry duplicates are eliminated unless the pinned client integration test proves the specific case.

Ingestion SHALL append accepted packets even if their fingerprint has been seen; bounded logical deduplication is owned by Compute. This preserves raw audit fidelity.

## REQ-ING-009: Event fingerprint

Because no stable broker event/sequence ID is assumed, ingestion SHALL calculate a versioned fingerprint from stable normalized values and packet bytes approved by the fingerprint specification. At minimum the specification defines:

- Field order and encoding
- Numeric and timestamp canonicalization
- Hash algorithm
- Included connection scope
- Fingerprint version
- State TTL recommendation
- Collision/identical-legitimate-event limitation

The fingerprint is best-effort. Metrics SHALL distinguish duplicate candidates, dedup hits, and estimated collision risk. Documentation SHALL never call it exact identity.

## REQ-ING-010: Suspected discontinuities

Ingestion SHALL NOT calculate exact missing sequence ranges unless future broker evidence proves a suitable sequence. It MAY create a `suspected_discontinuities` record for:

- Connection interruption
- Heartbeat timeout
- Unsupported packet/version
- Exchange-time jump beyond configured policy
- Subscription loss or negative acknowledgement
- Decoder failure burst

Each record includes `discontinuity_id`, connection scope, affected manifest/segment, detection timestamps, reason code, evidence summary, and status. The live data path continues unless capacity, corruption, or authentication policy requires shutdown. Inline historical backfill is prohibited in MVP.

## REQ-ING-011: Invalid and unknown packets

| Condition                   | Required behavior                                                                 |
| --------------------------- | --------------------------------------------------------------------------------- |
| Unknown protocol version    | Preserve bytes in quarantine; readiness false for affected stream; critical alert |
| Decode failure              | Preserve bytes and reason; increment error counter; apply bounded error policy    |
| Missing instrument identity | Quarantine; do not append to keyed raw table                                      |
| Invalid trade values        | Append raw/typed audit row marked invalid; Compute excludes it                    |
| Authentication exhausted    | Stop affected connection; readiness false; critical alert                         |
| Fluss append uncertainty    | Retry only under pinned client policy; expose uncertainty/loss counters           |

No validly received packet may be silently dropped.

## REQ-ING-012: Backpressure and bounded memory

The exact Fluss client buffering behavior is evidence-gated. Regardless of implementation, ingestion SHALL:

- Bound process memory and pending bytes.
- Expose pending bytes, blocked append duration, timeout count, and dropped/quarantined packet count.
- Stop readiness and alert before uncontrolled memory exhaustion.
- Never describe TCP receive-buffer behavior as lossless flow control without a broker test.

Production acceptance SHALL sustain the defined workload envelope with bounded backlog and no acknowledged packet loss.

## REQ-ING-013: Readiness and health

Liveness proves the process and event loop are responsive. Readiness requires all mandatory broker connections/subscriptions, Fluss connectivity, decoder compatibility, valid instrument manifest, recent successful append acknowledgement, acceptable clock offset, and observability delivery.

Partial subscription is not READY unless an explicitly approved degraded mode identifies missing instruments and disables affected trading decisions.

## REQ-ING-014: Graceful shutdown

Shutdown SHALL stop new subscriptions/reads, drain pending accepted packets for a configured deadline, persist loss/uncertainty counts, close clients, and report whether the drain completed. Forced termination is an abnormal-loss event and SHALL create an operational alert.

## REQ-ING-015: Required metrics and logs

Metrics SHALL include packet/byte throughput, append acknowledgements, write latency percentiles, active/subscribed connections, manifest version, decode/quarantine counts by reason, fingerprint counts, suspected discontinuities, pending bytes, reconnects, clock offset, readiness, and acknowledged-loss count.

Logs SHALL be structured and include service, instance, connection scope, protocol/decoder version, manifest version, and correlation identifiers. Original packet bytes and credentials SHALL never be logged.

## REQ-ING-016: Acceptance gates

1. Golden packet compatibility and unknown-version quarantine tests pass.
2. Original bytes round-trip losslessly and payload hashes match.
3. Typed normalization matches approved fixtures.
4. Reconnect/re-subscription tests prove manifest completeness.
5. Duplicate/fingerprint tests document both false-positive and false-negative limits.
6. Backpressure tests stay within configured memory and backlog bounds.
7. Variable 50,000 ticks/s average-baseline tests with defined loss accounting and no per-instrument rate above 20 ticks/s. (The 90,000 ticks/s peak tests are retired, DEC-036; 60,000 ticks/s gate, DEC-045.)
8. Credential rotation, exhaustion, shutdown, clock-skew, and observability failure tests pass.

##
