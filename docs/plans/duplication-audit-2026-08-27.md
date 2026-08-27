# Duplication / Overlap Audit — Low-Latency Ingestion (2026-08-27)

**Purpose:** Before implementing T1–T9, map what already exists vs. what the plan
adds, so we build only genuinely-new pieces and never run two parallel
implementations of the same concern.

**Scope:** Go bridge (`go-bridge/`) + Java ingestion (`src/main/java`), the only
two surfaces the plan touches. DDL/compute/safety contracts are untouched (guardrails).

---

## 1. Existing components (no duplication — keep)

| Component | File | What it does |
|---|---|---|
| Bridge NDJSON emitter | `go-bridge/ndjson.go` | `EmitTick` → JSON line; base64 `raw_payload`; SHA-256 `payload_hash` |
| Bridge event model | `ndjson.go` `BridgeEvent` | manifest/slot/control records |
| Java pipe parser | `bridge/BridgeEventParser.java` | parse NDJSON exactly-once (R-214), base64→bytes |
| Java row converter | `write/FlussRowConverter.java` | GoTick → 20-col GenericRow, `rawPayloadUnsafe()` for BYTES |
| Async writer | `write/RawTickWriter.java` | ACCEPTED immediate, OutcomeListener, retries 100/200/400ms |
| Backpressure | `write/AppendTracker.java` | 150k/192MiB, 80% warn / 100% halt |
| Fingerprint/freshness | `IngestionService` + `PayloadHashValidator` | Q19/Q20 gates (kept) |
| Telemetry | `telemetry/OtlpMetricsEmitter.java` | OTLP :4318 |

## 2. What the plan adds (new, no existing equivalent)

| Plan stage | New piece | Existing? | Verdict |
|---|---|---|---|
| T1 | `market_data.proto` + Go/Java codegen | **No `.proto` exists** | BUILD NEW |
| T2 | Go batcher (count/bytes/age) | No batching in Go today (emits per-tick) | BUILD NEW |
| T3 | Go gRPC client (conditional) | No gRPC anywhere | BUILD ONLY IF GATE OPENS |
| T4 | Java gRPC server + router `token%16` | No router class exists | BUILD ONLY IF GATE OPENS |
| T5 | Java batching @1ms + single writer | `FlussClientAdapter.java:59` hardcodes 20ms — **config change, not new class** | **CHANGE IN PLACE** |
| T6 | `TRANSPORT=pipe\|grpc` flag | No flag today | ADD SMALL FLAG |

## 3. Concrete changes (not new classes) — the "delete/modify" part

- **`FlussClientAdapter.java:59`** — change `"20ms"` → config-driven `"1ms"` (O-2).
  No new writer; single AppendWriter stays (O-1).
- **`ndjson.go`** — keep NDJSON emitter as fallback; add proto path *beside* it.
  The NDJSON emitter is the `TRANSPORT=pipe` fallback, not dead code.
- **`RawTickWriter`** — unchanged interface; only flush cadence changes via config.

## 4. Explicitly NOT to build (would duplicate)

- ❌ A second JSON parser in Java (BridgeEventParser already parses exactly-once)
- ❌ A second SHA-256 (Go computes once; Java validates only — Q3/Q5)
- ❌ Multi-writer machinery until Test D proves E2E < 50k (O-1)
- ❌ gRPC/UDS until E2E-after-T6 profiling proves the pipe is material (O-4)
- ❌ Any change to DDL (20 cols), `raw_payload` BYTES, freshness/quarantine/safety gates

## 5. NDJSON path lifecycle (decided 2026-08-27)

**Concern:** keeping both paths forever = stale dead code. Resolved with a
**scheduled removal**, not "keep both":

| Phase | NDJSON/pipe path | Proto path |
|---|---|---|
| Now → T6 | Only working path — keep (production) | Building (T1–T2) |
| T6–T9 (soak) | `TRANSPORT=pipe` fallback (Q21, rollback net) | Primary |
| **Post-soak** | **DELETE** emit path + `BridgeEventParser` + NDJSON-only tests | Sole path |

- **Anti-staleness during soak:** run ≥1 soak day per week in `TRANSPORT=pipe`
  mode so the fallback is exercised, not rotting. `make gate` keeps its tests green.
- **Deletion trigger (T10/decommission):** proto path stable ≥7 days in soak,
  `make gate` green without NDJSON code, rollback window closed.
- **After deletion:** `TRANSPORT` flag simplified to proto-only; JSON framing +
  parser gone; docs updated (Q21 fallback fulfilled by proto-over-pipe itself).

## 6. What gets deleted post-soak (concrete list)

- `go-bridge/ndjson.go` JSON emit path (`EmitTick` JSON + base64) — proto emit replaces it
- Java `bridge/BridgeEventParser.java` — proto decoder replaces it
- NDJSON-only Go tests (`ndjson_test.go` JSON cases) + Java parser tests
- Keep: `BridgeEvent` control-record model (manifest/slot records still ride the stream)

---

*Evidence: gate0-testA-20260827 JFR/pprof; thr-probe-002; contract Q1-Q25.*
