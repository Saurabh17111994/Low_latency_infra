# Segment Build Contract — Ingestion

## Boundary

Two colocated processes in the same container consume the evidence-approved broker stream: a Go arrow-bridge (Arrow Go SDK for auth, WebSocket, binary decode, zstd decompression) pipes NDJSON to Java IngestionService (validate, fingerprint, Fluss append). Original packet bytes are preserved as raw_payload, approved fields are mapped into typed columns, a versioned bounded fingerprint is calculated, and each accepted tick is appended to `raw_table_1` individually through the supported Fluss Java client.

## Inputs

- Versioned instrument manifest (loaded from an operator-approved Arrow `GET /all` or `GET /nse` CSV; static for the trading session — a change applies via controlled restart, ASM-ING-003)
- Arrow market-data WebSocket: `wss://socket.arrow.trade?appID=X&token=Y&zstd=1` (HFT feed — the Standard feed `wss://ds.arrow.trade` was removed 2026-08-14)
- Binary protocol: HFT modes — LTPC (40 bytes), Full (196 bytes), little-endian, zstd-compressed
- Prices in **paise** (int32, ÷100 for rupees); raw frame timestamps in **nanoseconds (unix)**, converted by the bridge to UTC **epoch milliseconds** (`ts_ms` — the platform's canonical unit; never seconds)
- Subscribe via JSON: `{"code":"sub","mode":"full","full":[tokens]}`
- Heartbeat: client sends `PONG` text every 3s; read timeout 5s
- Auth: token from `/auth/app/authenticate-token` (24hr TTL, refreshable)
- Swarm secret references in production
- Exact go-arrow SDK version and Fluss 0.9.1-incubating Java client version
- NDJSON tick schema (versioned contract between Go bridge stdout and Java stdin)

## Outputs

- `raw_table_1`: original bytes, payload hash, decoder/protocol version, typed fields, timestamps, fingerprint/version, validity state
- `suspected_discontinuities`: connection/subscription/heartbeat/decoder evidence; never fabricated sequence ranges
- Quarantine for unknown versions, decode failures, and missing instrument identity
- `instruments` (operator manifest loader): one row per `(instrument_token, manifest_version)` upserted through the raw client with the composite PK — the first production composite-PK raw-client writer (kv.format-version=2 + single-field subset bucket key; ING-INT-004 live proof 2026-08-15). Re-loading a manifest version is idempotent; prior versions are retained (R-090). The source CSV is operator-approved and static (no automatic daily refresh; ASM-ING-003).

## Guarantees

- Broker-to-Fluss is at-least-once.
- Raw events are not deduplicated at ingestion.
- Arrow provides no broker sequence/event ID — confirmed by Go SDK + REST docs. Fingerprint dedup (DEC-012) is correct.
- Original binary payload bytes are never replaced by canonical JSON.
- Memory/backlog are bounded; exact client internals remain version-gated.

## Failure behavior

Unsupported protocol, incomplete subscription, auth exhaustion, append uncertainty beyond policy, or quarantine bursts set readiness false and alert. Live processing never performs inline history backfill.

## Acceptance

Golden packet decoding, byte round-trip/hash, typed normalization, reconnect/subscription completeness, fingerprint limitations, bounded backpressure, credential rotation, the variable 50,000 ticks/s average baseline workload tests must pass. (The 90,000 ticks/s peak-capacity campaign is retired, DEC-036; 60,000 ticks/s gate, DEC-045.)

## Requirement traceability

- Functional: `REQ-ING-001` through `REQ-ING-016`
- Cross-cutting: `03-non-functional.md` §§3.1–3.3, 3.6–3.8; `04-data.md` §§4.1–4.6; `05-interfaces.md` §§5.1 and 5.11; `06-operational.md` §§6.2–6.3, 6.5, 6.8–6.10

See `../02_requirements/02-functional/01-ingestion.md`.
