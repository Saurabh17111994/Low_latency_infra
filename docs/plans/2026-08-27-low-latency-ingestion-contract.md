# Low-Latency Ingestion Implementation Contract — 2026-08-27

**Status:** Locked. All 25 architectural decisions resolved and approved by the operator (`all recommended`, 2026-08-27). **Open items O-1/O-2/O-3 resolved by measured evidence 2026-08-27** (`logs/tracker-14/thr-probe-002-linger-writer-20260827.md`): writer count=1, batch linger=1ms, gRPC/UDS deferred behind batching.
**Supersedes:** the generic guidance in `Low_latency_ingestion.md` (kept as reference; every open question it raised is now decided here).
**Scope:** transport-only rewrite of the ingestion hot path. **Flink compute, DDL, safety, evidence, and the 13/13 gate must not regress.**

---

## 1. Architecture (what we are building)

Replace the Go→Java **stdout NDJSON pipe** with:

```
3 broker conns → Go per-slot decode → per-slot sequence → batch builder (count|bytes|age)
  → protobuf MarketDataBatch → persistent gRPC stream → Unix Domain Socket (shared volume)
  → Java fluss-writer: gRPC server → protobuf decode → freshness/sanity gates → router (token%16)
  → 3 bounded queues → 3 writer workers → Fluss AppendWriter (batch-timeout 20ms)
  → raw_table_1 LOG (16 buckets, unchanged) → Flink SignalJob (unchanged)
```

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
| T4→T5 | Java router | `instrument_token % 16` → one of 3 bounded queues |
| T5→T6 | Writer | Batch rows → `AppendWriter` → Fluss |
| T6 | Fluss | Ack → write latency recorded |

**Latency model (mandatory, per doc §34):** T0-T6 staged timestamps carried in the proto batch + per-event; report p50/p95/p99/p99.9/max for `decode_latency`, `batching_latency`, `ipc_latency`, `routing_latency`, `fluss_submit_latency`, `fluss_ack_latency`, `end_to_end_latency`.

**Targets (Q12):** 50k sustained (DEC-036), p99 end-to-end broker→Fluss ack ≤ 250ms. These are the acceptance numbers.

---

## 3. Components & Contracts

### 3.1 Go bridge (`code/02_services/01_ingestion/go-bridge/`) — REUSE + EXTEND

| Existing (reuse as-is) | New (build) |
|---|---|
| `arrow-trade/go-arrow` SDK, `hft_stream.go` decode, golden corpus | `batch.go` — batch builder (count/bytes/age flush limits) |
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
  int64  token = 3;            // instrument_token
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
  repeated int32 bid_orders = 22;
  repeated int32 ask_orders = 23;
  bytes  raw_payload = 24;      // EXACT original broker packet bytes (Q3, Q6) — never base64
  string fingerprint_version = 25;
  string event_fingerprint = 26; // computed in Java (Q19) or Go? — see Q19 resolution below
  string decoder_version = 27;
  string protocol_version = 28;
}
```

**Q19 resolution (fingerprint stays in Java):** the proto carries the *inputs* Java needs (raw_payload + fields); Java's `FingerprintBuilder` stays in Java, computes `event_fingerprint` from the proto fields, and fills columns 25-26 at row-build time. Go does NOT compute fingerprints. `decoder_version`/`protocol_version` carried from Go.

**Field mapping requirement (doc §46):** every broker field → proto field → Java representation → Fluss column must be documented in a mapping table and locked by a serialization round-trip test. No invented fields.

### 3.3 Java fluss-writer (`code/02_services/01_ingestion/`) — REUSE + EXTEND

| Existing (reuse as-is) | New (build) |
|---|---|
| `FlussClientAdapter` → `AppendWriter` (batch-timeout 20ms) | `GrpcServer.java` — gRPC server bound to UDS |
| `RawTickWriter` + `AppendTracker` (150k/192MiB, 80% warn / 100% halt) | `PartitionRouter.java` — `token % 16` → queue |
| `FingerprintBuilder`, NTP/freshness gates, quarantine, safety, discontinuity | `BoundedQueue.java` — byte-budgeted (Q17) |
| `IngestionService` lifecycle, shutdown, health, metrics | `WriterWorker.java` — N=3 writers, each owns a queue |
| `OtlpMetricsEmitter` (OTLP :4318) | `marketdata.pb.java` — generated proto |

**Q14 topology:** Go bridge and Java writer become **two containers** (crash isolation, independent limits — doc §44). UDS over a **shared named volume** (`/run/fluss-ingest`), mode 660, group `fluss-ingest`, stale-socket cleanup in entrypoint, no TCP listener.

**Q16 router:** `instrument_token % 16`. Never connection/slot-based. No cross-instrument ordering requirement exists.

**Q17 writers:** start 3 × byte-budgeted queues; total budget 192 MiB regardless of count; Test D (2/4/8) decides the winner.

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
- O-5 → resolved: writer count 3 initially, benchmark decides.

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
- **Exit:** proto + mapping tests green; `make gate` still green (no production code changed yet).

**T2 — Go batcher (Q3, Q13):**
- `batch.go` (count/bytes/age), keep NDJSON emitter for fallback; unit tests for flush limits.
- **Exit:** Go tests green; NDJSON path still works.

**T3 — Go gRPC client + UDS (Q14, Q20):**
- Persistent stream, reconnect, flow-control, metrics; control records ride the stream.
- **Test B (Go-only):** broker replay → decode → batch; max Go throughput.
- **Exit:** Test B recorded; gRPC client connects to a stub server over UDS.

**T4 — Java gRPC server + router (Q16, Q17):**
- UDS bind, decode, freshness gates (kept), router `token%16`, bounded queues, queue metrics.
- **Test C (IPC-only):** Go → proto → gRPC/UDS → Java, no Fluss. Measures whether IPC is material.
- **Exit:** Test C recorded; **this is the gRPC/UDS decision gate (Q13)** — if IPC-only ≈ pipe, gRPC still proceeds (decided) but the perf evidence is documented.

**T5 — Java multi-writer (Q17, Q18):**
- N AppendWriters; per-writer batch; ack handling; retries; per-writer AppendTracker.
- **Test D:** Java → Fluss scaling (1/2/3/4/8 writers).
- **Exit:** writer-count winner chosen from evidence; fail-closed backpressure verified.

**T6 — Integration + fallback flag (Q21):**
- `TRANSPORT=grpc|pipe`; wire both containers + UDS volume into compose; keep NDJSON fallback.
- **Test E:** end-to-end vs baseline (Test A). 
- **Exit:** E2E ≥ 18k live / 49k synthetic, p99 ≤ 250ms, gate green.

**T7 — Failure testing (Q22, doc §53):**
- Broker disconnect/reconnect, Java restart, gRPC drop, UDS failure, Fluss down/slow, queue saturation, malformed proto, sequence gap, duplicate, graceful shutdown.
- **Exit:** full matrix green with evidence.

**T8 — Performance matrix (Q12, doc §37-38):**
- 1/2/3 conns × 1/2/3/4/8 writers × batch 16..1024 × loads 15k..150k; capture throughput, p50/p95/p99/p99.9, CPU, RSS, alloc, GC, queue depth/bytes, Fluss latency, retry rate.
- **Exit:** the 50k sustained + p99 250ms acceptance numbers met with evidence.

**T9 — Hardening + soak (Q23, Q25):**
- Resource limits, socket perms, dashboards, alerts, rollback proc, 30-min+ soak (bounded RSS/queue/latency, no leaks, stable retry).
- **Exit:** soak green; `make gate` 13/13; `make full-audit` green; rollback via `TRANSPORT=pipe` proven.

---

## 7. Testing Requirements (per task)

| Task | Required tests |
|---|---|
| T1 | Proto field-mapping vs `Tick`/`GoTick`; serialization round-trip; bit-exact `raw_payload`; unknown-version rejection |
| T2 | Batch flush at count/bytes/age boundaries; batch empty; single-event batch |
| T3 | Stream connect/reconnect; flow-control; UDS path/perms; control-record framing |
| T4 | Router distribution (token%16 even); queue byte accounting; freshness gates still reject stale/future |
| T5 | Writer retry/backoff; ack handling; per-writer halt at 100%; no silent drop |
| T6 | `TRANSPORT=pipe` fallback parity; E2E vs baseline; gate green |
| T7 | Full failure matrix (doc §53) |
| T8 | Full benchmark matrix (doc §37) with all metrics |
| T9 | Soak (doc §53): bounded RSS, stable latency/retry, no leaks, sequence integrity |

---

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
Gate 0 (profiling) → T1 (proto) → T2 (Go batch) + T3 (Go gRPC) → T4 (Java server+router) → T5 (writers)
→ T6 (integration) → T7 (failure) → T8 (perf) → T9 (harden+soak)
```

- T1 blocks everything (contract first).
- T3/T4 can proceed in parallel after T1 (Go client + Java server, tested against stubs).
- T6 requires T2-T5.
- T7 requires T6. T8 requires T7. T9 requires T8.
- **Do not** start T3+ until Test C's evidence is recorded (Q13).

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
