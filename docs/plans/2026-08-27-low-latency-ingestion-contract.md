# Low-Latency Ingestion Implementation Contract — 2026-08-27

**Status:** Locked. All 25 architectural decisions resolved and approved by the operator (`all recommended`, 2026-08-27). **Open items O-1/O-2/O-3 resolved by measured evidence 2026-08-27** (`logs/tracker-14/thr-probe-002-linger-writer-20260827.md`): writer count=1, batch linger=1ms, gRPC/UDS deferred behind batching.
**Supersedes:** the generic guidance in `Low_latency_ingestion.md` (kept as reference; every open question it raised is now decided here).
**Scope:** transport-only rewrite of the ingestion hot path. **Flink compute, DDL, safety, evidence, and the 13/13 gate must not regress.**

**Branch:** all work implementing this contract is committed **ONLY** to branch `low-latency-ingestion-based-project` (never `main`). Verify with `git branch --show-current` before starting; see AGENTS.md "Branch Context".

---

## 1. Architecture (what we are building)

Replace the Go→Java **stdout NDJSON pipe** with:

```
3 broker conns → Go per-slot decode → per-slot sequence → batch builder (count|bytes|age)
  → protobuf MarketDataBatch → persistent gRPC stream → Unix Domain Socket (shared volume)
  → Java fluss-writer: gRPC server → protobuf decode → freshness/sanity gates → router (token%16)
  → 1 bounded queue → 1 writer worker → Fluss AppendWriter (batch-timeout 1ms — O-2 RESOLVED)
  → raw_table_1 LOG (16 buckets, unchanged) → Flink SignalJob (unchanged)
```

**Interim transport (T6 default, O-4):** the pipeline above is the **conditional end-state**. The initial build (T1→T2→T5→T6) runs the same Go batcher + protobuf framing over the **existing stdout pipe**; gRPC/UDS is built only if E2E-after-T6 profiling shows the pipe itself is material. This matches the locked O-4 decision and Test C gate.

**Hard constraints (approved, non-negotiable):**
- **No Rust, no Kafka, no NATS, no shared memory, no custom UDS framing, no CPU pinning.**
- **No per-event RPC, no per-event Fluss write, no single global Java queue, no unbounded queue, no silent dropping.**
- **`raw_table_1` schema and the 20-column rows are bit-identical. DDL manifest (27 tables) untouched. Flink SignalJob is NOT modified, NOT rebuilt, NOT re-verified beyond `make gate`.**
- **The 25 locked decisions (Q1-Q25) are binding. The coding agent executes, does not re-architect.**

---

## 2. Data Flow (end to end)

| Stage | Component | Action |
|---|---|---|
| T0 | Broker | Full Depth binary packet (zstd decompressed in SDK) |
| T0→T1 | Go slot | Decode via `arrow-trade/go-arrow` (canonical, single decode — Q1) |
| T1 | Go | `received_ts_ms` captured at packet receipt |
| T1→T2 | Go batcher | Connection-local sequence (`feed_sequence_local`), minimal validation, batch builder |
| T2 | Go | Batch flush when `count>=MAX_EVENTS` OR `bytes>=MAX_BYTES` OR `age>=MAX_AGE` |
| T2→T3 | gRPC | `MarketDataBatch` proto → persistent gRPC stream → UDS |
| T3→T4 | Java | Decode proto, structural validation, freshness gates (NTP 2s / age 5s) |
| T4→T5 | Java router | `instrument_token % 16` → 1 bounded queue (multi-writer only if Test D) |
| T5→T6 | Writer | Batch rows → `AppendWriter` → Fluss |
| T6 | Fluss | Ack → write latency recorded |

**Latency model (mandatory, per doc §34):** T0-T6 staged timestamps carried in the proto batch + per-event; report p50/p95/p99/p99.9/max for `decode_latency`, `batching_latency`, `ipc_latency`, `routing_latency`, `fluss_submit_latency`, `fluss_ack_latency`, `end_to_end_latency`.

**Targets (Q12):** 50k sustained (DEC-036), p99 end-to-end broker→Fluss ack ≤ 250ms. These are the acceptance numbers.

---

## 3. Components & Contracts

### 3.1 Go bridge (`code/02_services/01_ingestion/go-bridge/`) — REUSE + EXTEND

| Existing (reuse as-is) | New (build) |
|---|---|
| `arrow-trade/go-arrow` SDK (`HFTFullTick`/`HFTLTPTick` decode in vendored SDK), golden corpus | `batch.go` — batch builder (count/bytes/age flush limits) |
| `supervisor.go` + `hft_slot.go` — 3 independent connections, per-slot epoch, reconnect | `transport.go` — persistent gRPC client over UDS, reconnect, flow-control |
| `metrics.go` — broker/reconnect/sequence metrics | `marketdata.pb.go` — generated proto |
| `subscription_plan.go`, manifest handling | Sequence-gap detection metric (`sequence_gaps_total`) |

**Go hot path (doc §9) — allowed:** WebSocket receive → framing → decode → minimal validation → connection-local sequence → batch accumulation → transport submit.
**Go hot path — forbidden:** JSON encode, base64, per-event SHA-256, DB lookups, business logic, sync Fluss ops, verbose logging.

### 3.2 Protobuf contract (`market_data.proto`) — NEW (Q1, Q3, Q5, Q6, Q7)

```proto
syntax = "proto3";
package marketdata;
option java_package = "com.trading.ingestion.transport";
option java_multiple_files = true;

message MarketDataBatch {
  string connection_id = 1;    // "ingestion-local/hft-0"
  int64  connection_epoch = 2; // monotonic per slot, increments per reconnect
  int64  batch_seq = 3;        // per-connection monotonic batch counter
  int64  created_ms = 4;       // T2
  repeated TickEvent events = 5;
  bytes  batch_payload_hash = 6; // sha256 over concatenated raw payloads (Q5: batch-level, computed in Go)
}

message TickEvent {
  string slot_id = 1;
  string mode = 2;             // "ltp" | "ltpc" | "quote" | "full"
  int32  token = 3;            // instrument_token (Go int32 — bit-exact)
  string feed = 29;            // "hft" (Go Tick.Feed, not persisted to row but carried for parity)
  int64  ts_ms = 4;            // exchange timestamp (source time, preserved — Q29)
  int64  received_ms = 5;      // T1 (Go receipt)
  int64  feed_sequence_local = 6; // connection-local sequence (Q15: verify packet has real seq first)
  // Prices are integer paise (long), never float (doc §28)
  int64  ltp_paise = 7;
  int64  close_paise = 8;
  int64  open_paise = 9;
  int64  high_paise = 10;
  int64  low_paise = 11;
  int64  vwap_paise = 12;
  int64  ltq = 13;
  int64  volume = 14;
  int64  total_buy_qty = 15;
  int64  total_sell_qty = 16;
  int64  open_interest = 17;
  repeated int32 bid_px = 18;    // fixed 5 (Q-O2)
  repeated int32 ask_px = 19;
  repeated int32 bid_qty = 20;
  repeated int32 ask_qty = 21;
  repeated uint32 bid_orders = 22; // Go [5]uint16 — uint32 for proto3 (no uint16)
  repeated uint32 ask_orders = 23;
  bytes  raw_payload = 24;      // EXACT original broker packet bytes (Q3, Q6) — never base64
  string fingerprint_version = 25;
  string event_fingerprint = 26; // computed in Java (Q19) or Go? — see Q19 resolution below
  string decoder_version = 27;
  string protocol_version = 28;
}
```

**Q5/Q19 SHA decision (AUDIT 2026-08-27):** Java currently RECOMPUTES SHA-256 per tick for validation (`PayloadHashValidator.validate`, 24 JFR samples). **Decision: keep validation in the proto path as well** (integrity > the ~24 samples), but make it **config-optional** (`INGEST_VALIDATE_PAYLOAD_HASH=true|false`) — default true for safety; the pipe path keeps it mandatory. This preserves Q3's perf intent (no base64 round-trip) while keeping integrity gates.

**Q19 resolution (fingerprint stays in Java):** the proto carries the *inputs* Java needs (raw_payload + fields); Java's `FingerprintBuilder` stays in Java, computes `event_fingerprint` from the proto fields, and fills columns 25-26 at row-build time. Go does NOT compute fingerprints. `decoder_version`/`protocol_version` are **set in Java** at row-build (`FlussClientAdapter.java:174-175`: "go-arrow-sdk"/""), NOT carried from Go.

**Field mapping requirement (doc §46):** every broker field → proto field → Java representation → Fluss column must be documented in a mapping table and locked by a serialization round-trip test. No invented fields.

### 3.3 Java fluss-writer (`code/02_services/01_ingestion/`) — REUSE + EXTEND

| Existing (reuse as-is) | New (build) |
|---|---|
| `FlussClientAdapter` → `AppendWriter` (batch-timeout 20ms) | `GrpcServer.java` — gRPC server bound to UDS |
| `RawTickWriter` + `AppendTracker` (150k/192MiB, 80% warn / 100% halt) | `PartitionRouter.java` — `token % 16` → queue |
| `FingerprintBuilder`, NTP/freshness gates, quarantine, safety, discontinuity | `BoundedQueue.java` — byte-budgeted (Q17) |
| `IngestionService` lifecycle, shutdown, health, metrics | `WriterWorker.java` — 1 writer by default (O-1); N>1 only if Test D triggers |
| `OtlpMetricsEmitter` (OTLP :4318) | `marketdata.pb.java` — generated proto |

**Q14 topology:** Go bridge and Java writer become **two containers** (crash isolation, independent limits — doc §44). UDS over a **shared named volume** (`/run/fluss-ingest`), mode 660, group `fluss-ingest`, stale-socket cleanup in entrypoint, no TCP listener.

**Q16 router:** `instrument_token % 16` — **only if multi-writer is triggered** (Test D). With 1 writer (O-1), routing is moot; single queue + writer. No cross-instrument ordering requirement exists.

**Q17 writers:** start **1 × byte-budgeted queue** (O-1 RESOLVED: single writer measured 58k-357k rows/s); total budget 192 MiB regardless of count; Test D (2/4/8) only if E2E-after-T6 misses 50k.

**Q18 backpressure:** keep fail-closed (80% warn + readiness false; 100% halt). Add gRPC flow-control as the *primary* slow-down (smooth), halt as terminal guard. Never silent drop.

**Q20 control records:** `bridge_metrics`, `broker_quarantine`, `bridge_event` move into the **same gRPC stream** as control-plane messages (one transport). Record types and Java handlers preserved unchanged.

### 3.4 Fluss / storage — UNCHANGED

- `raw_table_1` LOG, 16 buckets, `bucket.key=instrument_token`, 7d TTL, Iceberg lake — untouched.
- Rows bit-identical to today (20 columns, same values, same `payload_hash` column filled from proto's batch/per-row hash).

### 3.5 Deployment (`docker-compose.yml`, Dockerfiles) — MODIFY

- New service topology: `market-ingestor` (Go) + `fluss-writer` (Java), shared UDS volume.
- Resource limits: Go 512m/1cpu; Java heap 2g + direct 512m + queue budget 192MiB (Q14, Q49).
- `TRANSPORT=grpc|pipe` flag (Q21) — pipe kept as fallback through soak + gate + market session.

### 3.6 Observability — REUSE + EXTEND

- Reuse `OtlpMetricsEmitter` + Go `metrics.go`; add `grpc_*`, `java_queue_*`, `writer_utilization`, `fluss_batches_submitted/acked`, staged latency histograms (T0-T6).
- OpenObserve dashboards: add ingestion-transport panel. Alerts: queue ≥80%, queue full, writer<producer, gRPC reconnect.

---

## 4. Locked Decisions (Q1-Q25 — binding, do not revisit)

| # | Decision | Value |
|---|---|---|
| Q1 | Decode ownership | Go only. Java trusts proto fields, raw bytes pass through untouched. No double decode. |
| Q2 | Table schema | Keep 20 decoded columns + `raw_payload` BYTES. No schema-on-read. |
| Q3 | The win | Raw bytes as proto `bytes`, never base64/JSON. Java never re-encodes/re-decodes payload. |
| Q4 | Java validation | Freshness/sanity gates only (NTP 2s skew, 5s age). Go owns structural validation. |
| Q5 | Payload hash | Per-row SHA-256 computed **once in Go**, carried in proto, Java writes without recompute. |
| Q6 | "Better binary use" | All: no inflation, bit-exact preservation, single decode. No compression, no Flink decode. |
| Q7 | Replay | Raw bytes are re-derivation source. Bit-exact round-trip test mandatory (Q23e). |
| Q8 | Compute scope | **Transport-only.** Flink SignalJob untouched, not rebuilt, not re-verified beyond `make gate`. |
| Q9 | Row contract | Bit-identical rows, no column changes. |
| Q10 | Consumers | Both decoded columns + raw bytes (already provided by `raw_payload` column). |
| Q11 | Profiling first | Yes, time-boxed ~1 day: Test A baseline + Go CPU / JVM alloc-GC profile before rewrite. |
| Q12 | Targets | 50k sustained (DEC-036), p99 end-to-end ≤ 250ms. |
| Q13 | Evidence gate | **RESOLVED 2026-08-27: batching + 1ms linger first** (measured: linger is the bottleneck, not IPC). gRPC/UDS deferred until profiling proves the pipe material. |
| Q14 | Topology | Two containers, shared UDS volume. |
| Q15 | Broker sequence | Verify from golden corpus; if absent, connection-local only, honest gap semantics. |
| Q16 | Router key | `instrument_token % 16`. |
| Q17 | Writer/queue | **RESOLVED 2026-08-27: 1 writer** (measured 58k-357k rows/s single writer; 2/4/8 only if E2E misses 50k). Total budget 192 MiB. |
| Q18 | Backpressure | Fail-closed: 80% warn/readiness false, 100% halt; gRPC flow-control primary, halt terminal. Never drop. |
| Q19 | Fingerprint+freshness | Stay in Java. Fingerprint computes from proto fields. No move to Go. |
| Q20 | Control records | Into same gRPC stream (one transport). Types/handlers unchanged. |
| Q21 | Fallback | `TRANSPORT=pipe\|grpc` kept through soak+gate+market session; removal separate CHG. |
| Q22 | Loss bound | ≤1s of feed (≈50k events at 50k tps). Bounded by Go in-flight buffer + Java queue budgets. Explicit, honest. |
| Q23 | Definition of done | All must-haves, order: (e) bit-exact raw proof → (a) perf targets → (b) failure+soak → (c) gate 13/13 → (d) rollback. |
| Q24 | Execution | Staged reviews at each gate: proto → Go → Java → integration → perf matrix. Evidence + sign-off each. |
| Q25 | Sacred | Quarantine, safety, discontinuity records; slot-safety token-set hashes; `ingest_ts`/`ack_ts`; 7d retention + Iceberg; fail-closed gates. All untouchable. |

**Open decisions deferred to implementation (from the audit, now resolved):**
- O-1 → resolved by Q11/Q13: profiling gates the gRPC build; batching/multi-writer proceed regardless.
- O-2 → resolved: fixed 5 depth levels (matches current arrays).
- O-3 → resolved: UDS via shared volume across the two containers.
- O-4 → resolved: control records ride the gRPC stream.
- O-5 → resolved: writer count = **1** (O-1, measured 58k–357k rows/s on a single AppendWriter); 2/4/8 only if E2E-after-T6 misses 50k sustained (Test D trigger).

---

## 5. Failure & Recovery (defined behavior)

| Scenario | Intended behavior |
|---|---|
| Broker disconnect (one conn) | That slot reconnects independently (existing); others unaffected. Sequence gap → `sequence_gaps_total++`, gap evidence row. |
| Go crash | Supervisor restarts (1× then terminal); Java sees gRPC stream drop → marks slot disconnected → on reconnect, new epoch; compute dedup absorbs duplicates. |
| Java crash | gRPC server down → Go detects stream failure → controlled reconnect with backoff; Go in-flight buffer bounded (Q22 ≤1s); no loss beyond bound. |
| Fluss slow | Writer queues fill → gRPC flow-control slows Go → Go batch pause; if sustained, halt (no silent drop) + alert. |
| Fluss unavailable | Fluss client retries (existing 30s wait-timeout → bounded error); writers fail, queues fill → same backpressure path; never drop. |
| Queue saturation | 80% warn → readiness false + slow producer; 100% → halt, critical alert, preserved acknowledged-loss record (existing). |
| Malformed protobuf/packet | Decode error counted, quarantined to `ingestion_quarantine` (existing), rate-limited logging; stream continues. |
| Sequence gap | Go detects per-connection gap; metrics + discontinuity evidence (existing TimeJumpMonitor pattern); does NOT halt data path. |
| Duplicate (retry) | Documented "at-least-once"; compute dedup (TTL 300s) removes; no exactly-once claim. |
| Graceful shutdown | Full doc §43 order: Go stops broker → flush batches → complete gRPC → Java stops accepting → drain queues → submit remaining → wait acks → close Fluss → close gRPC → remove UDS. |
| Restart/rollback | `TRANSPORT=pipe` fallback; both coexist during transition. |
| Schema evolution | `contract_version` in proto; Java rejects unknown versions → quarantine; DDL unchanged. |
| Resource exhaustion | Per-container limits + queue byte budgets; OOM → container restart → replay from Go buffer (bounded) / broker resubscribe. |

---

## 6. Implementation Plan (dependency-ordered)

**Gate 0 — Profiling (Q11, ~1 day):**
- Test A: baseline pipe path at 18k live / 49k synthetic. Record CPU, RSS, JVM alloc, GC, p50/p99/p99.9.
- Go CPU profile + JVM allocation/GC profile. Identify whether JSON parse / base64 / SHA / single-writer is the cost.
- **Exit:** written evidence record (`logs/tracker-14/`), the number to beat, and confirmation of which optimizations matter.

**Gate 0 status: DONE 2026-08-27** (`logs/tracker-14/gate0-testA-20260827/test-a-20k-evidence-20260827.md`):
- **Baseline to beat: 18,103 rows/s @ 20k envelope** (1,339,466 rows, errors=0, uncertain=0; matches documented 18,441 live)
- **JVM:** ~369 MB/s allocation, 23 GC young pauses (median 3.28s), pause p50 6.4ms/max 8.5ms, RSS plateau 1.62 GB (stable)
- **JFR top:** base64 decode (89 samples), regex (96), SHA-256 (24), netty+ReentrantLock contention (184), `processLine` (41)
- **Go (30s CPU, 18.5% of wall):** JSON encoding 23%, syscalls 17%, base64 1.4%, SHA 1.8%, broker decode only 1.3% — bridge NOT CPU-bound
- **Confirmed:** JSON/base64/SHA hot path is material (the #1 cost); batching (1ms linger, THR-PROBE-002) removes lock/GC contention; gRPC/UDS further deferred (not needed to hit targets)

**T1 — Protobuf contract (Q1-Q7):**
- `market_data.proto` per §3.2; generate Go + Java; field-mapping tests vs `Tick`/`GoTick`; serialization round-trip tests; bit-exact `raw_payload` test.
- **Exit:** T1-P1/P2 (mapping + integer fidelity), T1-R1/R2 (round-trip + unknown-version), T1-X1 (bit-exact raw), T1-H1 (hash-once) green; mapping + serialization evidence recorded; `make gate` still green (no production code changed yet).

**T2 — Go batcher (Q3, Q13):**
- `batch.go` (count/bytes/age), keep NDJSON emitter for fallback; unit tests for flush limits.
- **Exit:** T2-B1..B6 (batch boundaries/edge cases) + T2-S1/S2 (sequence, golden-corpus finding) green; NDJSON path still works; evidence recorded.

**T3 — [CONDITIONAL] Go gRPC client + UDS (Q14, Q20) — only if Test C shows IPC material:**
- Persistent stream, reconnect, flow-control, metrics; control records ride the stream.
- **Test B (Go-only):** broker replay → decode → batch; max Go throughput. *(Runs regardless — pure Go throughput is a Gate-0-style measurement that does not depend on gRPC.)*
- **Exit:** Test B recorded; T3-G1/G2 green against a stub server over UDS. *(Only built if the T3 gate opens; otherwise skip reason recorded as evidence — O-4.)*

**T4 — [CONDITIONAL] Java gRPC server + router (Q16, Q17) — only if Test C shows IPC material:**
- UDS bind, decode, freshness gates (kept), router `token%16`, bounded queues, queue metrics.
- **Test C (IPC-only):** Go → proto → gRPC/UDS → Java, no Fluss. Measures whether IPC is material.
- **Exit:** Test C recorded; T4-G3/G4 green if built (UDS perms/stale-socket cleanup, router token%16 even). **This is the gRPC/UDS decision gate (Q13, O-4)** — **2026-08-27 evidence (THR-PROBE-002 + Gate 0): pipe is at 18.5% CPU, linger is the bottleneck → gRPC/UDS DEFERRED. T3/T4 only start if E2E-after-T6 profiling shows the stdout pipe itself is material; otherwise skip evidence recorded.**

**T5 — Java writer + batching (Q17, Q18):**
- **Single AppendWriter is the default** (O-1 RESOLVED: 58k–357k rows/s measured on one writer; multi-writer only if E2E misses 50k). Batching: client `batch-timeout=1ms` (O-2 RESOLVED), per-writer batch; ack handling; retries; per-writer AppendTracker.
- **Test D:** Java → Fluss scaling (1/2/3/4/8 writers) — **RE-SCOPED: run only if E2E-after-T6 misses 50k sustained**; otherwise single-writer result stands.
- **Exit:** T5-Q1..Q3 (queue thresholds/accounting/bounded) + T5-W1..W4 (batching @1ms, retries, no-silent-drop, drain) + T5-J1..J3 (freshness/fingerprint/quarantine) + T5-H2 (hash config) green; fail-closed backpressure verified; no per-event Fluss write proven; single-writer batching meets 50k or Test D triggered.

**T6 — Integration + fallback flag (Q21):**
- `TRANSPORT=pipe|grpc` — **pipe is the default/primary transport** (O-4); `grpc` is the opt-in mode used only if T3/T4 were built. Wire the batcher into the existing pipe first; keep NDJSON fallback. UDS volume + gRPC wiring only if T3/T4 exist.
- **Test E:** end-to-end vs baseline (Test A). 
- **Exit:** Test E + T6-I1 (replay correctness) + T6-I2 (control records) + T6-I3 (pipe parity) + T6-RB1 (rollback mechanism) green; E2E ≥ 18k live / 49k synthetic, p99 ≤ 250ms, gate green.

**T7 — Failure testing (Q22, doc §53):**
- Broker disconnect/reconnect, Java restart, Fluss down/slow, queue saturation, malformed proto, sequence gap, duplicate, graceful shutdown. *(gRPC drop / UDS failure cases only if T3/T4 were built.)*
- **Exit:** T7-F1..F18 (failure matrix) + T7-L1 (loss bound) + T7-D1 (duplicates) green with per-test evidence; ≤1s loss bound proven or the gate FAILS (no silent weakening).

**T8 — Performance matrix (Q12, doc §37-38):**
- 1/2/3 conns × 1/2/3/4/8 writers *(writer sweep only if Test D triggered; else single writer)* × batch 16..1024 × loads 15k..150k; capture throughput, p50/p95/p99/p99.9, CPU, RSS, alloc, GC, queue depth/bytes, Fluss latency, retry rate.
- **Exit:** T8-PERF1..N matrix complete (every record with all mandated fields incl. staged latencies); ≥50k sustained + p99 ≤250ms met; dominant latency stage identified; Test D ran only if E2E missed 50k (with evidence or documented non-trigger).

**T9 — Hardening + soak (Q23, Q25):**
- Resource limits, socket perms, dashboards, alerts, rollback proc, 30-min+ soak (bounded RSS/queue/latency, no leaks, stable retry).
- **Exit:** T9-S1 (30+ min soak, bounded RSS/queue/latency, no retry storm/sequence corruption/leaks) + T9-H1 (hardening) + T9-RB2 (operational rollback drill) green; regression R-215..R-224 green; `make gate` 13/13; `make full-audit` green; rollback via `TRANSPORT=pipe` proven.

---

## 7. Testing Contract (comprehensive, enforceable)

Every implementation task maps to tests with stable IDs. **A task is not complete until its mapped tests pass and the required evidence is recorded under `logs/tracker-14/`** (guardrail 9). Tests below are the acceptance mechanism for T1–T9; nothing may claim completion without them. Testing validates the locked architecture — it does not redesign it.

### 7.1 Test taxonomy (levels)

| Level | Scope | Determinism | Runner | Applies to |
|---|---|---|---|---|
| UNIT | One function/class; fake clock; no I/O, no network | Deterministic, fast | Go `go test` / Java JUnit | proto mapping, batch boundaries, byte accounting, timestamps, sequence, validation, retry/backoff, queue thresholds, config, transport selection |
| COMPONENT | One component vs controlled deps (in-memory Fluss, stub gRPC, fake clock) | Deterministic | Go tests / JUnit | batcher, transport, gRPC handler (if built), router, bounded queue, writer, AppendWriter interaction, shutdown |
| CONTRACT | Go ↔ protobuf ↔ Java cross-language; golden byte fixtures committed | Deterministic | Go + JUnit sharing fixtures | serialization, field mapping, depth arrays, integer fidelity, raw payload, metadata, version/unknown-version, fingerprint inputs, timestamp semantics |
| INTEGRATION | Real pipeline: broker replay → Go → transport → Java → Fluss; verify persisted rows | Deterministic replay corpus | `make` targets / compose | Test E, replay correctness, control records, pipe parity |
| FAILURE | Every §5 scenario injected; expected + recovery behavior asserted | Controlled injection | `make` targets / compose | T7 matrix, loss bound, duplicates |
| PERFORMANCE | Acceptance metrics incl. staged latencies | Fixed env + load profile | `make perf` / JFR / pprof | T8 matrix, Test A/B/D/E |
| SOAK | Long-running degradation detection | Fixed env, 30+ min | `make soak` | T9 |
| REGRESSION | Untouched contracts unchanged | Repo-level | `make gate` (13/13), `make full-audit`, `make pin-check`, R-215..R-224 | Every task |

### 7.2 Test ID scheme

`T<task>-<area><n>` — areas: `P`=proto/mapping, `R`=round-trip/version, `X`=raw-payload, `H`=hash/integrity, `B`=batch, `S`=sequence, `Q`=queue/backpressure, `W`=writer, `J`=Java gates, `G`=gRPC/UDS (conditional), `I`=integration, `RB`=rollback, `F`=failure, `L`=loss-bound, `D`=duplicate, `PERF`=performance, `S1`=soak, `H1`=hardening. `R-2xx` = regression checks (existing repo R-* convention). Every row below states: **what** it tests, **setup/input → action → expected**, **failure condition**, **evidence required**.

### 7.3 Unit & contract tests — T1 (proto)

| ID | What | Setup → Action → Expected | Failure | Evidence |
|---|---|---|---|---|
| T1-P1 | Field mapping: every broker field → Go `Tick` → proto → Java → Fluss column | Golden-corpus ticks → decode → marshal → unmarshal → row build. Assert every field path exists, types match the locked §3.2 mapping table, no field missing/extra/type-changed | Any missing/extra/type-changed field; any float conversion of integer paise | mapping-table diff + test report |
| T1-P2 | Integer fidelity: prices are integer paise, no float anywhere | Values: normal, zero, negative (where valid), max, 5-level depth arrays, large quantities, empty arrays. Assert no float/double in the path, no truncation/overflow | Any float in the path; truncation/overflow | serialization evidence + float-usage grep |
| T1-R1 | Round trip: Go object → protobuf → Java object identical | Same corpus as P1; compare every field incl. zero/empty values | Any field differs | round-trip diff |
| T1-R2 | Unknown `contract_version` rejection | Craft batch with unknown version → Java rejects → quarantined, stream continues | Accepted silently | test report |
| T1-X1 | Bit-exact `raw_payload`: original broker bytes → proto `bytes` → Java → `raw_table_1` byte-for-byte | Binary payloads containing `0x00`, high-bit bytes, arbitrary binary sequences. Compare **actual bytes**, never decoded content | Any byte differs; base64/JSON anywhere in the path | byte-level diff artifact |
| T1-H1 | Hash computed once — no per-event recompute in hot path | Instrument SHA calls over an N-event run; assert call count == N (Go, once per event), zero Java recomputes when `INGEST_VALIDATE_PAYLOAD_HASH=false` | Per-tick recompute in Java; >1 hash per event in hot path | profiler sample + call-count assert |

### 7.4 Unit & component tests — T2 (Go batcher, sequence)

| ID | What | Setup → Action → Expected | Failure | Evidence |
|---|---|---|---|---|
| T2-B1 | Count boundary | MAX_EVENTS=256; feed 255/256/257 events → flush exactly at 256 | Flush at 255 or 257 | test report |
| T2-B2 | Bytes boundary | MAX_BYTES=64KiB; feed just-below / at / just-above → flush at ≥ MAX_BYTES, never overshoot by more than one event | Overshoot >1 event; early flush | byte-accounting assert |
| T2-B3 | Age boundary (fake clock) | MAX_AGE=1ms; injectable clock → flush at exactly 1ms, not before/after | Early/late flush | clock assert |
| T2-B4 | Edge cases | Empty batch; single-event batch; count+bytes simultaneous; count+age; bytes+age; pending batch at shutdown; oversized single event; repeated flushes. Assert no event duplicated or silently lost, counters exact | Any dup/loss; counter mismatch | counters + event-set diff |
| T2-S1 | Connection-local sequence monotonic, per-epoch | Replay N ticks → `feed_sequence_local` = 1..N; reconnect → new epoch resets, old epoch never reused | Gap / monotonic break | sequence assert |
| T2-S2 | O-3 finding: does the broker supply a real sequence? | Golden corpus: inspect Full Depth packet for a real broker/exchange seq; record the finding (honest semantics — Q15) | n/a (finding recorded) | finding in evidence |

### 7.5 Component tests — T5 (queue, writer, Java gates)

| ID | What | Setup → Action → Expected | Failure | Evidence |
|---|---|---|---|---|
| T5-Q1 | Queue thresholds | Enqueue to <80%, exactly 80%, >80%, exactly 100%, attempt beyond 100%. Assert: 80% → warn + readiness false; 100% → halt; beyond → rejected with no silent drop | Wrong threshold; drop at full | readiness/halt logs |
| T5-Q2 | Byte accounting | Vary event sizes; assert accounted bytes == sum; 192 MiB budget enforced | Mismatch; budget breach | accounting report |
| T5-Q3 | Bounded — no hidden overflow | Concurrent producers; large + small batches; assert bounded memory; dequeue after saturation works; no unbounded queue or hidden overflow path | Unbounded growth | RSS + queue-depth traces |
| T5-W1 | Batching @1ms, **no per-event Fluss write** | Feed 10k events; count AppendWriter invocations; assert write count << event count (batches), never 1 write/event | Per-event write | write-count evidence |
| T5-W2 | Retry/backoff/ack | Fluss transient failure → retry with existing backoff (100/200/400ms); permanent failure → classified; ack accounting matches | Wrong backoff; ack mismatch | retry log + counters |
| T5-W3 | No silent drop: queue → writer → AppendWriter → ack | Inject failures; assert every accepted event is either acked or halted-with-record; **none vanish** | Any accepted-but-lost event | event-set reconciliation |
| T5-W4 | Drain on shutdown | Shutdown mid-batch; assert pending batch submitted, acks awaited, no lost pending | Lost pending | shutdown trace |
| T5-J1 | Freshness gates unchanged | Valid ts; exactly-at-boundary ts; >2s NTP skew; >5s age; stale; future — assert behavior identical to today's contract | Behavior drift | gate log diff |
| T5-J2 | Fingerprint regression | `FingerprintBuilder` from proto fields == expected fingerprint; stays in Java | Moved to Go; wrong value | fingerprint diff |
| T5-J3 | Quarantine | Malformed/invalid events → rejected, counted, quarantined per existing behavior, never reach Fluss; stream continues | Reached Fluss; stream halted | quarantine row + counters |
| T5-H2 | Hash config both ways | `INGEST_VALIDATE_PAYLOAD_HASH=true`: valid passes, corrupted fails + quarantined; `false`: row contents unmutated, no recompute | Corrupt passes (true); mutation (false) | run-matrix evidence |

### 7.6 Conditional tests — T3/T4 (gRPC/UDS) — ONLY if the gate opens (O-4)

| ID | What | Setup → Action → Expected | Failure | Evidence |
|---|---|---|---|---|
| T3-G1 | Persistent stream connect/reconnect | Stub server; kill/restart server → client reconnects, new epoch | No reconnect | stream log |
| T3-G2 | Flow-control | Slow consumer → producer throttled; no unbounded buffering | Unbounded buffer | depth traces |
| T4-G3 | UDS perms + stale socket | mode 660, group `fluss-ingest`, stale-socket cleanup in entrypoint | Wrong perms; stale-socket EADDRINUSE | perms + cleanup assert |
| T4-G4 | Router `token%16` even | 10k mixed tokens → bucket counts within tolerance; per-token order preserved | Skew; order break | distribution report |

### 7.7 Integration — T6

| ID | What | Setup → Action → Expected | Failure | Evidence |
|---|---|---|---|---|
| T6-I1 | Replay correctness (Test E core) | Deterministic replay corpus → full pipeline → Fluss. Capture: input count, expected count, output count, rejected, quarantined, duplicates, sequence gaps, raw-payload hashes, fingerprints, timestamps, persisted values. Compare expected vs actual | Any count/value mismatch | full corpus report |
| T6-I2 | Control records via same transport | `bridge_metrics` / `broker_quarantine` / `bridge_event` through the proto transport; distinguishable from market data; Java handlers unchanged; malformed control records handled safely, cannot corrupt market-data processing | Confused with data; handler breakage | control-record trace |
| T6-I3 | Pipe parity | Same corpus over NDJSON vs proto-batch pipe; identical persisted rows | Row drift | row diff |
| T6-RB1 | Rollback mechanism | `TRANSPORT=grpc` → set `TRANSPORT=pipe` → old path starts, accepts traffic, persists correct rows | Env change alone doesn't restore | rollback trace |

### 7.8 Failure — T7 (all §5 scenarios)

Each test documents: **failure injected → expected behavior → observed → recovery → data-loss bound → evidence**.

| ID | Failure injected | Expected behavior | Failure condition | Evidence |
|---|---|---|---|---|
| T7-F1 | Broker disconnect (one conn) | That slot reconnects independently; others unaffected; sequence gap → `sequence_gaps_total++` + gap evidence row; data path NOT halted | Other slots affected; path halted | slot logs + gap row |
| T7-F2 | Broker reconnect | New epoch; sequence restarts; no cross-epoch reuse | Epoch reuse | sequence trace |
| T7-F3 | Go crash | Supervisor restarts (1× then terminal); Java sees drop → slot disconnected; reconnect = new epoch; compute dedup absorbs duplicates | No restart; dupes beyond TTL | restart log + dedup count |
| T7-F4 | Java crash | Go detects stream failure → controlled backoff reconnect; Go in-flight buffer bounded (Q22); no loss beyond bound | Unbounded Go buffer | buffer trace |
| T7-F5 | Fluss slow | Queues fill → flow-control slows Go → batch pause; sustained → halt + alert; no silent drop | Drop; no halt | queue depth + alert |
| T7-F6 | Fluss unavailable | Client retries (existing 30s wait-timeout → bounded); writers fail; queues fill → same backpressure; never drop | Unbounded retry; drop | retry + queue traces |
| T7-F7 | Queue 80% | Warn + readiness false + producer slowed | No warn | readiness log |
| T7-F8 | Queue 100% | Halt, critical alert, preserved acknowledged-loss record | Silent drop | halt record |
| T7-F9 | Malformed protobuf | Decode error counted, quarantined, rate-limited log; stream continues | Stream dies | quarantine + counter |
| T7-F10 | Malformed broker packet | Same handling as F9 on the Go side | Pipeline halt | counter |
| T7-F11 | Sequence gap | Detected per-connection; metrics + discontinuity evidence (TimeJumpMonitor pattern); does NOT halt | Halts path | gap evidence |
| T7-F12 | Duplicate delivery | At-least-once; compute dedup (TTL 300s) removes; no mutation while dedup | Exactly-once claim; mutation | dedup trace |
| T7-F13 | Graceful shutdown | Exact doc §43 order; no lost pending, no unacked writes, no premature exit | Any violation | shutdown trace |
| T7-F14 | Restart | Clean restart; state consistent; no corruption | Corruption | restart evidence |
| T7-F15 | Rollback | `TRANSPORT=pipe` restores working path under failure | Rollback fails | rollback drill log |
| T7-F16 | Resource exhaustion | Per-container limits + queue budgets; OOM → container restart → replay from Go buffer (bounded) / broker resubscribe | Unbounded memory | RSS trace |
| T7-F17 | Transport failure (gRPC) | *Only if T3/T4 built:* stream failure → reconnect/backoff; no silent loss | Loss | stream log |
| T7-F18 | UDS failure | *Only if T3/T4 built:* stale socket/permission → entrypoint cleanup, retry | Crash-loop | socket log |

| ID | What | Setup → Action → Expected | Failure | Evidence |
|---|---|---|---|---|
| T7-L1 | Loss bound ≤1s of feed | Deliberately kill/restart the relevant component under controlled load. Measure: events submitted, events acknowledged, events recovered, events lost, duration of loss, max in-flight. Assert lost ≤ 1s of feed (~50k events @ 50k tps) | Bound not provable → **gate FAILS** (no silent weakening) | loss-accounting report |
| T7-D1 | Duplicates / at-least-once | Retry-induced duplicates; assert measurable, transport never claims exactly-once, compute dedup removes within TTL, no data mutation | Exactly-once claim; dedup miss | dedup evidence |

### 7.9 Performance — T8 (acceptance metrics + staged latencies)

- **Baseline:** Test A (recorded 2026-08-27: 18,103 rows/s @ 20k envelope, JVM ~369 MB/s alloc, GC p50 6.4ms, RSS 1.62 GB — `logs/tracker-14/gate0-testA-20260827/`). Re-measure the new implementation on the same workload + environment.
- **Matrix (T8-PERF1..N):** 1/2/3 conns × writers (1 = default; 2/4/8 **ONLY if Test D triggered** — else record "not triggered") × batch 16..1024 × loads 15k..150k.
- **Every benchmark record must include:** exact configuration; duration; warm-up; steady-state interval; input volume; output volume; errors; retries; throughput; p50/p95/p99/p99.9/max; CPU; RSS; JVM allocation; GC; queue depth + bytes; Fluss submit latency; Fluss ack latency; **staged latencies `decode_latency`, `batching_latency`, `ipc_latency`, `routing_latency`, `fluss_submit_latency`, `fluss_ack_latency`, `end_to_end_latency`** — aggregate-only reporting is a failure.
- **Latency-budget validation (T8-LB):** the report must identify which stage consumes the p99/p99.9 tail (the 2026-08-27 evidence says the 20ms linger was dominant — this test proves whether the new bottleneck is decode, batching, IPC, routing, queue, or Fluss submit/ack).
- **Failed benchmark definition:** any record missing a mandated field; unexplained errors/retries; load drift (input ≠ declared volume); p99 tail unexplained by the stage histogram.
- **Test D (conditional):** 1/2/4/8 writers with throughput, p50/95/99/99.9, CPU, RSS, queue depth, Fluss latency, retries, ordering, correctness — **ONLY if E2E-after-T6 misses 50k sustained**; otherwise skip-evidence recorded ("not triggered — single writer met target"). Multi-writer never becomes the default silently.
- **Exit:** ≥50k sustained + p99 ≤ 250ms met; matrix complete; dominant stage identified.

### 7.10 Soak & hardening — T9

| ID | What | Setup → Action → Expected | Failure | Evidence |
|---|---|---|---|---|
| T9-S1 | 30+ min soak | Monitor: throughput, p50/95/99/99.9, max, CPU, RSS, JVM heap, direct memory, alloc rate, GC, queue depth + bytes, retries, reconnects, sequence gaps, duplicate rate, quarantine rate, writer utilization. Acceptable: no unbounded RSS, no unbounded queue, no progressive latency degradation, no retry storm, no sequence corruption, no unexplained throughput decline, no silent drops | Any unacceptable trend | full soak report + traces |
| T9-H1 | Hardening | Resource limits, socket perms, dashboards, alerts, rollback proc — all present and exercised | Missing/inert | config + alert evidence |
| T9-RB2 | Operational rollback drill | `TRANSPORT=pipe` in production-like env: starts, accepts traffic, persists correct rows, passes correctness tests + `make gate` | Rollback not operational | drill log |

### 7.11 Regression — every task (Q23c, §29)

R-215..R-224 must stay green on every task (existing repo R-* convention):

| ID | Check | Mechanism |
|---|---|---|
| R-215 | `raw_table_1` schema + 20-column row contract unchanged | `make pin-check` + schema diff |
| R-216 | DDL manifest + 27-table manifest unchanged | `make pin-check` + diff |
| R-217 | Flink SignalJob unchanged | git diff guard |
| R-218 | Safety behavior unchanged | gate suite |
| R-219 | Quarantine behavior unchanged | gate suite |
| R-220 | Discontinuity evidence unchanged | gate suite |
| R-221 | Token-set safety hashes unchanged | gate suite |
| R-222 | `ingest_ts` / `ack_ts` semantics unchanged | unit + gate |
| R-223 | 7-day retention + Iceberg offload unchanged | config diff + gate |
| R-224 | `make gate` 13/13, `make full-audit`, `make pin-check` green | CI-style run each task |

### 7.12 Evidence requirements (guardrail 9)

Every gate-level test produces an evidence record under `logs/tracker-14/` containing: test ID; task/gate; commit SHA; CHG identifier (rollback-affecting changes); environment; configuration; input dataset; event count; duration; result (pass/fail); metrics; relevant logs; failure injection (if applicable); artifact paths; skipped-test reason (if conditional).

> **A task is not complete until its mapped tests pass and its required evidence is recorded.**

### 7.13 Test-to-requirement traceability (Q1–Q25 → task → test → acceptance → evidence)

| Locked decision/req | Task | Test ID(s) | Acceptance criterion | Evidence artifact |
|---|---|---|---|---|
| Q1 decode ownership | T1 | T1-P1, T1-R1 | (e) bit-exact + mapping | mapping + round-trip report |
| Q2 schema | T1 | T1-P1, R-215/R-216 | (c) no regression | pin-check diff |
| Q3 raw bytes | T1 | T1-X1 | (e) bit-exact raw | byte-diff artifact |
| Q4 Java validation | T5 | T5-J1 | (b) reliability | gate log diff |
| Q5 payload hash | T1/T5 | T1-H1, T5-H2 | hash-once; both configs | JFR + run matrix |
| Q6 binary transport | T1/T6 | T1-X1, T6-I3 | (e) no inflation, no mutation | byte diff + parity |
| Q7 replay | T6 | T6-I1 | (e) replay correctness | corpus report |
| Q9 row contract | T1/T6 | T1-P1, T6-I3, R-215 | (c) bit-identical rows | row diff |
| Q12 performance | T8 | T8-PERF1..N, T8-LB | (a) 50k + p99 ≤ 250ms | full perf records |
| Q14 topology | T6 | T6-I2 | two containers | compose diff |
| Q15 sequence | T2/T7 | T2-S1/S2, T7-F11 | gap detection; honest semantics | seq trace + gap evidence |
| Q16 routing | T4* | T4-G4 | even distribution | distribution report |
| Q17 writer/queue | T5 | T5-Q1..Q3, T5-W1 | 1 writer; 192 MiB budget | queue/writer evidence |
| Q18 backpressure | T5/T7 | T5-Q1, T7-F5..F8 | fail-closed; no drop | readiness/halt logs |
| Q19 fingerprint | T5 | T5-J2 | stays in Java | fingerprint diff |
| Q20 control records | T6 | T6-I2 | same transport; handlers unchanged | control trace |
| Q21 fallback | T6/T9 | T6-RB1, T9-RB2 | pipe restores path | rollback logs |
| Q22 loss bound | T7 | T7-L1 | ≤1s proven or fail | loss accounting |
| Q23 DoD | all | all above | order (e)→(a)→(b)→(c)→(d) | all artifacts |
| Q25 sacred contracts | all | R-215..R-224 | untouchable | gate + diffs |

### 7.14 Quality bar (self-check before each task closes)

- **Correctness:** can every data transformation be auto-verified? (T1-P*, T6-I1)
- **Safety:** can silent loss / corruption / unbounded buffering be detected? (T5-Q*, T7-F*, T7-L1)
- **Performance:** can ≥50k sustained + p99 ≤ 250ms be proven? (T8-PERF*)
- **Tail latency:** can the p99/p99.9 stage be identified? (T8-LB)
- **Reliability:** can every §5 failure mode be reproduced + verified? (T7-F1..F18)
- **Regression:** Flink/DDL/schema/safety/quarantine unchanged? (R-215..R-224)
- **Rollback:** `TRANSPORT=pipe` restores the working path? (T6-RB1, T9-RB2)
- **Evidence:** can an independent reviewer determine exactly what was tested? (7.12)
- **Coding-agent usability:** can another agent implement without inventing its own test strategy? (this §7)

## 8. Acceptance Criteria (Definition of Done — Q23)

All must hold, in this order:

1. **(e) Bit-exact raw proof:** a round-trip test proves the exact original broker packet bytes reach `raw_payload` in `raw_table_1` — no base64, no JSON, no mutation. **First.**
2. **(a) Performance:** E2E ≥ 18,441 rows/s live / 49,237 tps synthetic (baseline, Test A) AND ≥ 50k sustained at 3,000-instrument envelope, p99 end-to-end ≤ 250ms, with the full benchmark evidence record.
3. **(b) Reliability:** all failure tests + 30-min soak green; no silent drops; queue saturation halts; sequence gaps detected; duplicates documented (at-least-once, compute dedup).
4. **(c) No regression:** `make gate` 13/13, `make full-audit`, `make pin-check` all green. `raw_table_1` schema + Flink SignalJob + DDL manifest unchanged.
5. **(d) Rollback:** `TRANSPORT=pipe` restores the old path; proven in T6/T9.

---

## 9. Dependencies (what before what)

```
Gate 0 (profiling, DONE) → T1 (proto) → T2 (Go batch) → T5 (single writer + 1ms linger) → T6 (integration)
→ T7 (failure) → T8 (perf) → T9 (harden+soak)
```

- T1 blocks everything (contract first).
- **T3/T4 (gRPC/UDS) are CONDITIONAL** — they sit off the critical path and start only if E2E-after-T6 profiling shows the stdout pipe itself is material (O-4). Test B still runs (pure Go throughput, no gRPC dependency).
- T6 requires T2 + T5.
- T7 requires T6. T8 requires T7. T9 requires T8.
- **Test D (multi-writer sweep) is conditional** — runs only if E2E-after-T6 misses 50k sustained (O-1).

---

## 10. Open Items (genuinely deferred, must be confirmed during T8)

- **O-1 (writer count):** **RESOLVED 2026-08-27: 1 writer** — single AppendWriter measured 58k-357k rows/s across lingers (THR-PROBE-001/002), exceeding the 50k target. Revisit 2/4/8 only if E2E misses 50k.
- **O-2 (batch params):** **RESOLVED 2026-08-27: MAX_AGE=1ms** (measured p99 10.5ms at 200k rows/s vs 38ms at 20ms — THR-PROBE-002). MAX_EVENTS≈256, MAX_BYTES≈64KiB. T8 tunes only if E2E misses p99 ≤ 250ms.
- **O-3 (sequence):** whether the Full Depth packet carries a real broker/exchange sequence — verify from golden corpus in T1; if absent, connection-local only.
- **O-4 (gRPC/UDS justification):** **RESOLVED 2026-08-27: defer gRPC/UDS.** THR-PROBE-002 shows the 20ms client linger (not the pipe) is the dominant latency cost. Build batching + 1ms linger on the existing path first; gRPC/UDS only if end-to-end profiling then shows IPC material (doc §40).

---

## 11. What the coding agent must NOT do (guardrails)

1. Do not touch `docs/01_project/`, `docs/02_requirements/`, `docs/04_contracts/` DDL pins, or the `schema_manifest.json` (27 tables).
2. Do not modify `02_compute/` (SignalJob, Babysitter, SafetyHaltJob) — not rebuilt, not re-verified beyond `make gate`.
3. Do not remove the NDJSON fallback (`TRANSPORT=pipe`) until soak + gate + a funded market session pass on gRPC.
4. Do not remove freshness gates, fingerprint builder, quarantine, safety, discontinuity evidence, slot-safety token-set hashes, `ingest_ts`/`ack_ts`, 7d retention, Iceberg offload, or any fail-closed startup gate.
5. Do not introduce Rust/Kafka/NATS/shared memory/custom UDS framing/CPU pinning/float prices.
6. Do not claim exactly-once, do not claim 3×/5×/10× without the T8 measurement.
7. Do not implement one RPC per event, one Fluss write per event, one Java process per connection, or an unbounded queue.
8. Do not silently drop raw Full Depth events at queue-full.
9. Do not commit secrets; keep evidence records dated + immutable under `logs/tracker-14/`.
10. Do not claim a task done without its mapped tests + evidence + a CHG record (per AGENTS.md and 01-foundation.md).
