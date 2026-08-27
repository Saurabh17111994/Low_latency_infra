# Contract Audit — 2026-08-27 (Plan Mode, Read-Only)

**Audited:** `docs/plans/2026-08-27-low-latency-ingestion-contract.md` (334 lines)
**Method:** cross-checked every claim against actual code (`go-bridge/`, `src/main/java/`, DDL) + measured evidence (`logs/tracker-14/`).
**Verdict:** ✅ Architecture sound; ⚠️ 4 real inconsistencies + 4 refinements needed. No fundamental redesign.

---

## A. Real Inconsistencies (must fix)

### A1. Proto `TickEvent` field set ≠ actual Go `Tick` / Java `GoTick`
Contract §3.2 proto (lines 82-108) omits fields that EXIST today and are written to Fluss:
- **`atv`** (avg trade value) — in Go `Tick`, Java `GoTick`, but **NOT written to Fluss row** (vestigial in row path). Proto decision: either omit (matches row) or include (matches GoTick). Recommend: omit from proto since it's not persisted.
- **`btv`** (buy trade value) — same, NOT written to row. Recommend: omit.
- **`feed`** — in Go `Tick` ("hft"), Java `GoTick`, **NOT written to row**. Recommend: omit (or carry for future).
- **Type mismatches:**
  - Go `Token` is `int32` → proto says `int64` (bit-exactness demands `int32`/`sint32`)
  - Go `BidOrd/AskOrd` are `[5]uint16` → proto says `repeated int32` (should be `uint32`/`sint32`)
  - Go `BidPx/AskPx/BidSize/AskSize` are `[5]int32` → proto `repeated int32` OK
- **Missing control fields** that Java GoTick expects: `record_type`, `connection_id`, `connection_epoch`, `slot_id`, `received_ts_ms`, `feed_sequence_local` — proto has some (connection_id/epoch/seq) but not `record_type`/`slot_id`/`received_ms` consistently

**Impact:** T1's field-mapping test will FAIL against the real structs. Must add atv/btv/feed, fix types to int32/uint16.

### A2. "Java never recomputes SHA" (Q3/Q5) contradicted by current code
- `PayloadHashValidator.validate()` at `IngestionService.java:1782` **recomputes SHA-256 on every tick** (`sha256(packet)`) — measured in JFR as 24 SHA samples.
- Contract claims "Java writes without recompute" — but current code recomputes for VALIDATION.
- **Decision needed:** keep Java SHA validation (safety, but costs ~24 samples/tick) vs. trust proto's Go-computed hash (perf, but weaker integrity). Contract must state which.

### A3. "3 bounded queues / 3 writer workers" still in §1 + §3.3 — contradicts O-1 (1 writer)
- §1 architecture diagram (line 17): "3 bounded queues → 3 writer workers"
- §3.3 (line 126): `WriterWorker.java — N=3 writers`
- §3.3 (line 133): "start 3 × byte-budgeted queues; Test D decides"
- **But O-1 (line 176): RESOLVED: 1 writer.**
- **Impact:** executor will build 3 queues/3 writers that evidence says unnecessary. Must update to 1 writer default.

### A4. `hft_stream.go` referenced in §3.1 — file does not exist
- §3.1 (line 58): "`arrow-trade/go-arrow` SDK, `hft_stream.go` decode, golden corpus"
- Actual: no `hft_stream.go` in `go-bridge/` (files: main.go, hft_slot.go, ndjson.go, etc.)
- Decode likely lives in vendored SDK or `main.go`. Fix the reference.

---

## B. Refinements (optional but recommended)

### B1. `ingest_ts`/`ack_ts` provenance
Contract §5 + Q25 say these are sacred, but the proto doesn't carry them.
- `ingest_ts` = Java row-build time; `ack_ts` = Fluss ack time.
- **Recommendation:** keep them Java-side (implicit), document in mapping table, don't add to proto (would be redundant).

### B2. Router `token % 16` with 1 writer
If O-1 stands (1 writer), `token % 16` routing to 3 queues is moot. With 1 writer, either:
- Drop the router (single queue/writer), or
- Keep `token % 16` for future scaling but route to a single queue (harmless).
**Recommendation:** drop router for now; single queue + writer; re-add only if Test D (multi-writer) triggers.

### B3. `decoder_version`/`protocol_version` provenance — **set in JAVA, not Go**
- `FlussClientAdapter.java:174-175`: `decoder_version = "go-arrow-sdk"`, `protocol_version = ""` — **hardcoded in Java**.
- Contract §3.2 says "carried from Go" — **wrong**. Go has no such fields.
- **Recommendation:** proto should NOT carry them; Java keeps setting them at row-build (existing behavior). Remove from proto field list or mark Java-set.

### B4. Batch payload hash vs per-row hash (Q5)
Proto has both `batch_payload_hash` (bytes) and per-row `raw_payload` + `payload_hash` field. Currently Java validates per-row hash (`PayloadHashValidator`). With proto, per-row hash is still needed (row contract). **Recommendation:** keep per-row `payload_hash` in proto; `batch_payload_hash` optional extra (cheap integrity of batch framing).

---

## C. Correct in Contract (verified against code)

- ✅ DDL 20 columns match actual `02_raw_table_1.sql` (verified above) — event_fingerprint … schema_version
- ✅ `raw_payload` BYTES + `payload_hash` STRING match DDL
- ✅ Single writer (O-1) matches measured 58k-357k rows/s (THR-PROBE-001/002)
- ✅ 1ms linger (O-2) matches measured p99 10.5ms @ 200k
- ✅ gRPC/UDS deferred (O-4) matches Gate 0 (pipe at 18.5% CPU)
- ✅ `make gate` green status + no-regression scope (transport-only)
- ✅ Q25 sacred items (quarantine/safety/discontinuity/ingest_ts/ack_ts/7d+Iceberg) all present in code
- ✅ No `.proto` exists today → T1 genuinely new
- ✅ No router class exists → T4 genuinely new (conditional)

---

## D. Proposed Fixes (in order)

1. **Fix proto `TickEvent`** (§3.2): align with Go `Tick` + Java `GoTick` **and the actual Fluss row**:
   - Add fields the row persists: (verify against DDL's 20 cols — only those that map to row cols need proto)
   - Change `token`→`int32`, `bid_orders/ask_orders`→`uint32` (Go types)
   - `atv`/`btv`/`feed`: NOT persisted → omit from proto (or optional)
   - Lock with T1 mapping test vs GoTick.
2. **Resolve A2** (SHA in Java): decide keep-Java-validation (safety) vs. trust-Go (perf). Recommend: keep validation but make it config-optional, so the proto path can skip it (Q3 perf win) while the pipe path keeps it.
3. **Update §1 + §3.3** to 1 writer / 1 queue (O-1). Remove "3 writer workers", "N=3", "start 3 × byte-budgeted queues". Router `token%16` → conditional (only if multi-writer).
4. **Fix `hft_stream.go` reference** → actual file (decode in SDK or `main.go`).
5. **Document `ingest_ts`/`ack_ts`** as Java-side implicit, add to mapping table.
6. **`decoder_version`/`protocol_version`**: remove from proto; Java keeps hardcoding at row-build (`FlussClientAdapter.java:174-175`).

---

## E. Exit Criteria for this Audit — ALL COMPLETE 2026-08-27
- [x] Proto field set == Go `Tick` + Java `GoTick` (T1 mapping test green) — **contract §3.2 updated**: `token`→int32, `feed` added, `bid/ask_orders`→uint32, Q19 decoder/protocol Java-set
- [x] §1/§3.3 writer count consistent (1) — **contract updated**: arch diagram 1 queue/1 writer, `WriterWorker` N=1, Q17 1 queue, Q16 router conditional
- [x] SHA validation decision documented (A2) — **contract §3.2**: keep validation, config-optional `INGEST_VALIDATE_PAYLOAD_HASH`
- [x] All file references resolve to real files — **`hft_stream.go` → vendored SDK `HFTFullTick`/`HFTLTPTick`**
