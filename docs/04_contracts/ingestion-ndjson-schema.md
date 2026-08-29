# DEPRECATED — Tick NDJSON Schema (transport removed 2026-08-29)

> **This contract is historical.** The NDJSON pipe transport was removed
> 2026-08-29: the bridge emits length-prefixed protobuf `TransportFrame`
> only (`TRANSPORT=proto|grpc`; any other value is FATAL). This document is
> retained for the record — see the proto contract instead.

<!--
  Version 2.2 — 2026-08-08.
  Contract between go-bridge/main.go stdout and IngestionService.java stdin.
  This schema is the single source of truth for the pipe contract.
  Supersedes v1.0 (2026-07-30) — see Versioning.

  v2.2 is an ADDITIVE extension of contract version 2: one new record type
  (bridge_metrics) and two new required bridge_event fields. The integer
  contract_version stays 2 — consumers from v2.0/v2.1 that do not know the
  new record type ignore it (Java routes unknown record types through the
  quarantine parser, which falls through), and the new fields are additive
  JSON keys. A consumer that validates strictly (Java BridgeEvent) requires
  the new fields; older producers that predate v2.2 must not be paired with
  such a consumer.
-->

## Delivery

One record per line, newline-delimited JSON (NDJSON). No array wrapping. No pretty-printing. Empty lines are ignored. Malformed lines are counted as errors and quarantined.

There are **four record types**, discriminated by the required `record_type` field:

| `record_type` | Producer | Consumer |
| --- | --- | --- |
| `tick` | Go bridge (each decoded HFT tick — the Standard feed was removed 2026-08-14) | `IngestionService.processLine` → `raw_table_1` |
| `bridge_event` | Go bridge (lifecycle transitions) | `IngestionService.processBridgeEvent` → health/metrics/evidence |
| `bridge_metrics` | Go bridge (10s telemetry snapshot, v2.2+) | `IngestionService.processLine` → `parseMetrics` → Go-authoritative gauges |
| `broker_quarantine` | Go bridge (undecodable/unknown broker packets) | `IngestionService` → `ingestion_quarantine` |

Contract version is integer `2` (v2.2 is an additive document revision — see Versioning).

## Tick record

```json
{
  "record_type": "tick",
  "contract_version": 2,
  "connection_id": "ingestion-local/hft-0",
  "connection_epoch": 1,
  "slot_id": "hft-0",
  "received_ts_ms": 1785471200000,
  "feed_sequence_local": 1,
  "feed": "hft",
  "mode": "full",
  "token": 26000,
  "ltp_paise": 15150,
  "close_paise": 15000,
  "open_paise": 14900,
  "high_paise": 15200,
  "low_paise": 14800,
  "vwap_paise": 15050,
  "ltq": 100,
  "volume": 500000,
  "total_buy_qty": 250000,
  "total_sell_qty": 200000,
  "atv": 15000,
  "btv": 10000,
  "open_interest": 1234567,
  "ts_ms": 1751212800000,
  "bid_px": [150, 149, 148, 147, 146],
  "ask_px": [152, 153, 154, 155, 156],
  "bid_qty": [100, 200, 300, 400, 500],
  "ask_qty": [100, 200, 300, 400, 500],
  "bid_orders": [1, 2, 3, 4, 5],
  "ask_orders": [1, 2, 3, 4, 5],
  "raw_payload": "KAEAAAAAAA==",
  "payload_hash": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
}
```

### Tick field reference

| Field | Type | Unit | Description |
| --- | --- | --- | --- |
| `record_type` | `string` | — | `"tick"` (required) |
| `contract_version` | `int` | — | `2` (required) |
| `connection_id` | `string` | — | `${instanceId}/${slotId}` (required) |
| `connection_epoch` | `int` | — | monotonic per slot, ≥1, increments before each reconnect |
| `slot_id` | `string` | — | `"hft-0"` … (required) |
| `received_ts_ms` | `int64` | epoch ms | Java-side receive time (required) |
| `feed_sequence_local` | `int64` | — | Monotonic per-slot tick sequence starting at 1, reset per connection epoch (required). Diagnostic ordering evidence; not part of the dedup fingerprint. |
| `feed` | `string` | — | `"hft"` (the only feed — Standard removed 2026-08-14) |
| `mode` | `string` | — | `"ltpc"` or `"full"` (the only HFT modes — Standard feed carrying `ltp`/`quote` removed 2026-08-14) |
| `token` | `int32` | — | Arrow instrument token (bucket key for `raw_table_1`) |
| `ltp_paise` | `int32` | paise | Last traded price (₹1 = 100 paise) |
| `close_paise` | `int32` | paise | Previous close price |
| `open_paise` | `int32` | paise | Day open |
| `high_paise` | `int32` | paise | Day high |
| `low_paise` | `int32` | paise | Day low |
| `vwap_paise` | `int32` | paise | Volume-weighted average price |
| `ltq` | `int32` | shares | Last traded quantity |
| `volume` | `int64` | shares | Cumulative volume |
| `total_buy_qty` | `int64` | shares | Total buy quantity at best levels |
| `total_sell_qty` | `int64` | shares | Total sell quantity at best levels |
| `atv` | `uint32` | — | Average traded value |
| `btv` | `uint32` | — | Best traded value |
| `open_interest` | `int64` | — | Open interest (F&O) or 0 |
| `ts_ms` | `int64` | epoch ms | Broker event timestamp in UTC milliseconds |
| `bid_px` | `int32[5]` | paise | 5-level bid prices (index 0 = best bid) |
| `ask_px` | `int32[5]` | paise | 5-level ask prices |
| `bid_qty` | `int32[5]` | shares | 5-level bid quantities |
| `ask_qty` | `int32[5]` | shares | 5-level ask quantities |
| `bid_orders` | `uint16[5]` | count | 5-level bid order count |
| `ask_orders` | `uint16[5]` | count | 5-level ask order count |
| `raw_payload` | `string` | Base64 | **Exact decompressed broker packet bytes** that produced this tick (required). Decoded JSON must NOT replace these bytes. |
| `payload_hash` | `string` | hex | **SHA-256** hex digest of the `raw_payload` bytes (required). Java validates before append; mismatch → quarantine `HASH_MISMATCH`. |

## Bridge event record

Lifecycle records are `record_type="bridge_event"`. The `event` field names the transition; `state` is the resulting slot state.

```json
{
  "record_type": "bridge_event",
  "contract_version": 2,
  "event": "slot_state",
  "slot_id": "hft-0",
  "connection_id": "ingestion-local/hft-0",
  "connection_epoch": 1,
  "state": "CONNECTING",
  "assigned_tokens": 1024,
  "acknowledged_tokens": 0,
  "rejected_tokens": 0,
  "reason": "",
  "received_ts_ms": 1785471200000,
  "manifest_fingerprint": "8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c",
  "assigned_token_set_hash": "8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c"
}
```

### Lifecycle examples

`slot_state`:
```json
{"record_type":"bridge_event","contract_version":2,"event":"slot_state","slot_id":"hft-0","connection_id":"ingestion-local/hft-0","connection_epoch":1,"state":"CONNECTING","assigned_tokens":1024,"acknowledged_tokens":0,"rejected_tokens":0,"reason":"","received_ts_ms":1785471200000,"manifest_fingerprint":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c","assigned_token_set_hash":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c"}
```

`subscription_ack` (full success):
```json
{"record_type":"bridge_event","contract_version":2,"event":"subscription_ack","slot_id":"hft-0","connection_id":"ingestion-local/hft-0","connection_epoch":1,"state":"ACTIVE","assigned_tokens":1024,"acknowledged_tokens":1024,"rejected_tokens":0,"reason":"","received_ts_ms":1785471200100,"manifest_fingerprint":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c","assigned_token_set_hash":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c"}
```

`heartbeat_failed`:
```json
{"record_type":"bridge_event","contract_version":2,"event":"heartbeat_failed","slot_id":"hft-0","connection_id":"ingestion-local/hft-0","connection_epoch":1,"state":"BACKOFF","assigned_tokens":1024,"acknowledged_tokens":1024,"rejected_tokens":0,"reason":"[redacted]","received_ts_ms":1785471210000,"manifest_fingerprint":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c","assigned_token_set_hash":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c"}
```

`feed_stalled`:
```json
{"record_type":"bridge_event","contract_version":2,"event":"feed_stalled","slot_id":"hft-1","connection_id":"ingestion-local/hft-1","connection_epoch":2,"state":"STALLED","assigned_tokens":1024,"acknowledged_tokens":1024,"rejected_tokens":0,"reason":"no_tick_for_15s","received_ts_ms":1785471215000,"manifest_fingerprint":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c","assigned_token_set_hash":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c"}
```

`reconnect`:
```json
{"record_type":"bridge_event","contract_version":2,"event":"reconnect","slot_id":"hft-0","connection_id":"ingestion-local/hft-0","connection_epoch":2,"state":"BACKOFF","assigned_tokens":1024,"acknowledged_tokens":1024,"rejected_tokens":0,"reason":"retry_in_2s","received_ts_ms":1785471216000,"manifest_fingerprint":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c","assigned_token_set_hash":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c"}
```

`auth_failure`:
```json
{"record_type":"bridge_event","contract_version":2,"event":"auth_failure","slot_id":"hft-0","connection_id":"ingestion-local/hft-0","connection_epoch":3,"state":"AUTH_FAILED","assigned_tokens":1024,"acknowledged_tokens":0,"rejected_tokens":0,"reason":"authentication_refresh_exhausted","received_ts_ms":1785471219000,"manifest_fingerprint":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c","assigned_token_set_hash":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c"}
```

`bridge_shutdown`:
```json
{"record_type":"bridge_event","contract_version":2,"event":"bridge_shutdown","slot_id":"hft-0","connection_id":"ingestion-local/hft-0","connection_epoch":1,"state":"BACKOFF","assigned_tokens":1024,"acknowledged_tokens":1024,"rejected_tokens":0,"reason":"","received_ts_ms":1785471300000,"manifest_fingerprint":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c","assigned_token_set_hash":"8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c"}
```

### Bridge event field reference

| Field | Type | Description |
| --- | --- | --- |
| `record_type` | `string` | `"bridge_event"` |
| `contract_version` | `int` | `2` |
| `event` | `string` | `slot_state` \| `subscription_ack` \| `heartbeat_failed` \| `feed_stalled` \| `disconnect` \| `reconnect` \| `auth_failure` \| `bridge_shutdown` |
| `slot_id` | `string` | slot identifier |
| `connection_id` | `string` | `${instanceId}/${slotId}` |
| `connection_epoch` | `int` | ≥1 |
| `state` | `string` | `AUTHENTICATING` \| `CONNECTING` \| `SUBSCRIBING` \| `ACTIVE` \| `STALLED` \| `BACKOFF` \| `PARTIAL` \| `AUTH_FAILED` \| `TERMINAL` |
| `assigned_tokens` | `int` | tokens assigned to the slot |
| `acknowledged_tokens` | `int` | tokens acknowledged by the broker |
| `rejected_tokens` | `int` | tokens rejected by the broker |
| `reason` | `string` | bounded ≤512 chars, redacted; never credentials |
| `received_ts_ms` | `int64` | epoch ms |
| `manifest_fingerprint` | `string` | SHA-256 hex (64 lowercase) of the whole manifest token set — sorted tokens, each as 8 big-endian bytes (v2.2+, required by strict consumers) |
| `assigned_token_set_hash` | `string` | SHA-256 hex (64 lowercase) of this slot's assigned token set, same encoding as the fingerprint (v2.2+, required by strict consumers) |

The two identity hashes use the same encoding as `SafetyHaltWriter.computeAssignedTokenHash` / `InstrumentManifestLoader.computeFingerprint`: SHA-256 over the **sorted** token list, each token as **8 big-endian bytes**. The Java consumer treats a fingerprint/token-hash mismatch as a **warn-only cross-check** (increments `FINGERPRINT_MISMATCH` / `TOKEN_HASH_MISMATCH` decode-error counters) — events are never rejected, because Go and Java token sets can legitimately differ in dev synthetic mode. Both fields are always present in the real bridge's stdout (identity configured at startup); Go omits them only when identity was never configured.

`reason` is always redacted by the Go bridge before emission (secrets, tokens, app secrets, and raw payload values scrubbed). Java independently redacts bridge stderr forwarding and parser exceptions (defense in depth — never rely on one layer).

## Bridge metrics record

`record_type="bridge_metrics"` is a periodic (10s) telemetry snapshot produced by the bridge supervisor's ticker (v2.2+). The Java consumer routes it **before** the GoTick fall-through (it must never misparse as a tick with `feed=""` → `INVALID_SCHEMA` quarantine). Values are Go-authoritative and overwrite the Java lifecycle-derived gauges; those lifecycle values are only the pre-first-metrics fallback.

```json
{
  "record_type": "bridge_metrics",
  "contract_version": 2,
  "ts_ms": 1785471200000,
  "reconnect_consecutive": 3,
  "active_sockets": 1,
  "go_goroutines": 42
}
```

| Field | Type | Description |
| --- | --- | --- |
| `record_type` | `string` | `"bridge_metrics"` |
| `contract_version` | `int` | `2` |
| `ts_ms` | `int64` | snapshot wall-clock time, > 0 |
| `reconnect_consecutive` | `int` | highest per-slot consecutive-reconnect streak at snapshot time (resets when a slot reaches ACTIVE) |
| `active_sockets` | `int` | currently open broker sockets (open/close counted by the bridge, so orphaned sockets are visible) |
| `go_goroutines` | `int` | `runtime.NumGoroutine()` — leak detection evidence |

Java maps these onto `bridge.reconnect_consecutive`, `bridge.active_sockets`, `bridge.go_goroutines` gauges. A `bridge_metrics` line with an unknown `contract_version` is a protocol error (thrown, like unknown event versions). Missing/zero `ts_ms` is rejected. Any other `record_type` yields `Optional.empty()` — never a rejection.

## Broker quarantine record

Undecodable or unknown broker packets with recoverable packet boundaries become `record_type="broker_quarantine"`:
```json
{
  "record_type": "broker_quarantine",
  "contract_version": 2,
  "slot_id": "hft-0",
  "connection_id": "ingestion-local/hft-0",
  "connection_epoch": 1,
  "token": 26000,
  "reason": "MALFORMED_JSON",
  "raw_payload": "KAEAAAAAAA==",
  "payload_hash": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "detected_ts_ms": 1785471210000
}
```

Java hash-validates `raw_payload` against `payload_hash` and persists to `ingestion_quarantine`.

## Rules

1. **All prices in paise.** Never rupees. Code must not divide-by-100 or assume decimal.
2. **All timestamps in epoch milliseconds UTC.** Never seconds, never local time.
3. **Depth arrays are 5-element.** `full` mode always provides 5 bids + 5 asks. `ltpc` mode may have zero-length or null arrays.
4. **Missing/unknown fields are omitted.** JSON `omitempty` — do NOT send `0` or `null` for absent fields. Java side defaults to zero.
5. **feed+mode disambiguate the data.** `feed=hft, mode=full` has more fields populated than `feed=hft, mode=ltpc` (HFT is the only feed — Standard removed 2026-08-14).
6. **No duplicate ticks assumed to be identical.** Two ticks with the same `token`+`ltp_paise`+`ts_ms` but different `ltq` or `bid_px` are different events.
7. **raw_payload is the exact decompressed broker packet bytes** (Base64), never the JSON line. `payload_hash` is their SHA-256.
8. **One record per line, atomic writes.** The emitter serializes complete lines; three slot goroutines may emit concurrently.
9. **Java errors:** malformed JSON → quarantine + `decode.errors`; unknown lifecycle version/event → fatal protocol failure; valid tick with bad business values → quarantine; valid lifecycle event → state/evidence/metrics update.
10. **Records are additive within a contract version.** Unknown `record_type` values fall through to the next parser (quarantine, then metrics, then tick bind) and are never silently dropped or rejected; `bridge_metrics` is routed before the GoTick bind. New fields on known records are additive JSON keys — strict consumers require them, lenient consumers (Go `omitempty`) may omit.
11. **`bridge_metrics` lines must never be quarantined as `INVALID_SCHEMA`** — a metrics line bound as a GoTick has `feed=""`, so routing order is part of the contract.

## Versioning

| Version | Date | Changes |
| --- | --- | --- |
| 1.0 | 2026-07-30 | Initial schema. Matches `go-bridge/main.go` `Tick` struct and `IngestionService.java` `GoTick` class. |
| 2.0 | 2026-08-01 | Added `record_type`, `contract_version=2`, `connection_id`, `connection_epoch`, `slot_id`, `received_ts_ms`, `raw_payload`, `payload_hash`; added `bridge_event` and `broker_quarantine` record types; documented lifecycle event examples and Java handling. |
| 2.1 | 2026-08-01 | Added `feed_sequence_local` (monotonic per-slot tick sequence, not part of dedup fingerprint). |
| 2.2 | 2026-08-08 | **Additive extension, contract version stays 2.** Added `bridge_metrics` record type (10s telemetry: `reconnect_consecutive`, `active_sockets`, `go_goroutines`); added `manifest_fingerprint` + `assigned_token_set_hash` (SHA-256 over sorted tokens, 8-byte big-endian each) to `bridge_event` — required by the strict Java consumer, warn-only cross-check on mismatch. Slot safety/capacity gauges (`bridge.slot.safety_state`, `bridge.slot.unsafe_duration_ms`, `bridge.slot.capacity_remaining`) are Java-side exports of the same evidence. |

## Implementation References

- **Go side:** `code/02_services/01_ingestion/go-bridge/main.go` — `Tick` struct, `runHFTEpoch`/`emit()`, identity wiring (`SetManifestFingerprint`/`SetSlotTokenHash`); `code/02_services/01_ingestion/go-bridge/ndjson.go` — `EmitTick`, `EmitEvent`, `EmitMetrics`, redaction; `code/02_services/01_ingestion/go-bridge/metrics.go` — reconnect-streak/active-socket state, 10s metrics ticker
- **Java side:** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java` — `GoTick` inner class, `processLine()`, `processBridgeEvent()`, `parseMetrics` routing, `decodeAndValidatePayload()`; `bridge/BridgeEventParser.java` (events + metrics), `bridge/BridgeMetrics.java`, `bridge/BrokerQuarantine.java`, `bridge/PayloadHashValidator.java`
