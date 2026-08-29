# Ingestion

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status and sources

| Field | Value |
| --- | --- |
| Status | Implementation active; Phases 2a-2g complete; 716 tests (247 ingestion + 466 common — 2026-08-25 CHG-102 re-verified; raw surefire 469 minus 3 gated `FlussBundleReader*` reports; corrected from 192/304 after the 2026-08-14 Standard-feed test deletion; common recounted 2026-08-15 at 177 historical 2026-08-15 = 160 + 17 — COMPAT-FLUSS-006 live bucket-skew probe +1, full-manifest routing identity +1, `KvStaleWriteRejectionTest` +7 (COMPAT-FLUSS-004 rejected/quarantined/audited half), plus 8 tests already in the tree but absent from the prior figure — and grown since to 466 via the SCH-23/SCH-20/SCH-24/SCH-15 additions (CHG-003/005/006/007, CHG-100 2026-08-25, CHG-102 2026-08-25; docs-audit C6 line 466/247/388 (compute 388 — the 2026-08-17 Design-B merge `34af190` −19 DEC-038-era tests, then −10 CHG-023 item-1 emitter→native-reporter swap, then −2 CHG-023 item-2 native-TTL expiry swap, then −11 CHG-023 item-4 StallGuardedSink removal 2026-08-17, then +1 SIG-FAIL-001 checkpoint-failure test `SignalJobCheckpointFailureIntegrationTest` 2026-08-17; compute count corrected 2026-08-16, was 268)); ingestion +5 instrument-manifest-writer tests 2026-08-15 — ING-SCHEMA-002 unit + ING-INT-004 live; +3 hardening 2026-08-15 — ING-DQ-011 fuzz corpus (CHG-008/009/010); +4 M2 hardening 2026-08-15 — ING-UNIT-018 config parity (CHG-012); +13 M3 failure-path E2E 2026-08-15 — ING-FAIL-008 crash-loop, ING-FAIL-009 auth-failure, ING-FAIL-010 shutdown-deadlock, ING-INT-005 readiness matrix (CHG-017); +7 M4 Go-bridge/data-quality 2026-08-15 — ING-UNIT-014/015/016/017, ING-RES-002/003/004, ING-TCP-003 (CHG-018); +3 M5 telemetry/ops 2026-08-15 — ING-UNIT-021/022 OTLP scrubbing + cardinality (G6 RESOLVED), ING-FAIL-006 warning-window throttle, plus the Go ING-UNIT-020 FATAL-message cases and the ING-INT-006 entrypoint harness (CHG-019); +1 CHG-032 2026-08-18 — ING-UNIT-023 bridge SIGTERM-drain regression (CHG-015 follow-up); +1 CHG-033 2026-08-18 — ING-UNIT-024 real JVM shutdown-hook layer (main-thread join, exit 143)), 0 failures, 8 env-gated skips (ingestion; common 464/0/1-skip historical 2026-08-24; the 11 live-Fluss common integration tests run only with `FLUSS_BOOTSTRAP` set). **2026-08-14: the Standard feed is REMOVED — the platform consumes only the HFT feed (`wss://socket.arrow.trade`); `runStandard`/`fromStandardTick`/`ARROW_USE_STANDARD` and the standard-mode tests are deleted (user decision).** Open items: ING-PERF-001 50k gate certified 2026-08-13 at the synthetic hot-path envelope (socket 49,242 tps / 0 wire loss; append hot path 49,578 tps / 0 failures / p99 &lt; 5 ms — floors 47.5k/48k PASS); the 30-min 3,000-instrument production-envelope run is REMOVED from acceptance (DEC-037, 2026-08-13 — user decision: not to be tested); ING-RES-001 100-cycle real-backoff soak PASS 2026-08-13 (100/100 cycles, 2852.7 s, goroutines 3 &lt; baseline 4, fds 7 = baseline — no leak; evidence logs/res001-real-backoff/res001-real-backoff-20260813.out); BROKER-MD-001 live evidence captured 2026-08-13 (real wire on the HFT feed: 40/196 B zstd; AutoLogin/TOTP verified programmatically, no device token; VM-BROKER-MKT-008 COMPATIBLE; evidence logs/broker-md-001/); 90k peak campaign retired (DEC-036); ING-TCP-001 count-based losslessness: `ARROW_TICK_COUNTS` per-token emitted-tick counters (env-gated, stderr report, chunked) + `TokenCountReconcile` probe + `reconcile-compare.py` — validation PASS 2026-08-13 (HFT-1024 × 3 epochs: 3,072 sink rows = 3 × 1,024 bridge ticks; 0 lost / 0 extra / 0 vanished; evidence `logs/tracker-14/losslessness-validation-20260813.md`); market-hours 15-min proof PASS 2026-08-14 (epoch 08:24:06–08:39:06Z, HFT-1024: bridge 646,102 emitted = sink 646,102 rows; per-token --exact reconcile 1024/1024 match, 0 lost; evidence `logs/tracker-14/losslessness-markethours-20260814.md`); the bridge's final tick-count report is now file-persisted (`ARROW_TICK_COUNTS_FILE`, default /tmp/arrow-tick-counts.txt) because the parent JVM closes child pipe streams at shutdown — a stderr-only final report died with SIGPIPE (exit 141); tooling committed at `code/01_platform/04_scripts/ing-tcp001/`; HFT mode handles 1,024 tokens; the HFT config-key mismatches (response-timeout key name/unit + the dead-but-validated pins) were FIXED 2026-08-14 — the bridge now reads and enforces every documented key (pins FATAL on wrong values; `STALL_TIMEOUT_SECONDS` and `SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS` are honored at runtime); REMAINING OPEN — the deferred item below is an ACCEPTED DEFERRAL (user decision, 2026-08-14): it does not block current 1,024-instrument / single-connection operation, and it will be implemented in a future phase only when the user decides it is needed — no action required now. The 3,000-instrument / 3-connection production envelope is NOT implemented (previously deferred by DEC-036/037) — requires the premium broker tier + multi-connection fan-out before it can run. (The instrument manifest is operator-approved and static for the trading session — a change applies via controlled restart, per ASM-ING-003; the approved-manifest persistence path `InstrumentManifestWriter` remains implemented as proven by ING-INT-004.) |
| Owner | Ingestion Team |
| Requirements | `REQ-ING-001`–`REQ-ING-016` → `AC-ING-001`–`AC-ING-015` |
| Build contract | `docs/04_contracts/01-ingestion.md` |
| Writes | `raw_table_1`, `suspected_discontinuities`, quarantine evidence, `instruments` (operator manifest loader — `InstrumentManifestWriter`, the first production composite-PK raw-client writer; ING-INT-004 live proof 2026-08-15) |
| Must not own | Candle computation, strategy, broker orders, fill lifecycle (**ranking REMOVED 2026-08-15, CHG-005**) |

### Process boundary

Two colocated processes in the same container form the ingestion boundary:

**Go arrow-bridge** — uses the official Arrow Go SDK (`go-arrow`) for: authentication (AutoLogin or static token), WebSocket connection (`wss://socket.arrow.trade`), binary frame decode (HFT LTPC + FULL including 5-level bid/ask depth; the Standard feed that carried LTP/Quote modes was removed 2026-08-14 — see [`DEC-039`](../01_project/04-decisions.md) §1 and `DEC-012`), zstd decompression (HFT), subscription management, keepalive, and reconnection. Outputs **proto frames (T6 — `TRANSPORT=proto`, the only transport)**: ticks batched (1ms/256/64KiB) into length-prefixed protobuf `TransportFrame` on stdout. The NDJSON pipe transport was removed 2026-08-29; any non-proto `TRANSPORT` value is FATAL in the bridge.

**Java IngestionService** — sniffs the bridge stdout (T6): proto frames → batched `MarketDataBatch` → `processTickEvent`. The single path validates, resolves instruments, computes a versioned canonical fingerprint, and appends each tick to `raw_table_1` through the Fluss Java client.

The pipe is the kernel's stdin/stdout — not a message queue, not a network hop. Both processes share one container lifecycle. Do not insert Python, Kafka, ZeroMQ, or another transport between the Go bridge and the Java Fluss append.

### Internal modules

| Module | Responsibility |
| --- | --- |
| `bridge` | Go arrow-bridge process lifecycle, stdin/stdout contract (proto `TransportFrame`), reconnect backoff |
| `model` + `bridge` | Verified units, timestamps, instrument mapping, validity classification (lives in the `model`/`bridge` packages) |
| `fingerprint` | Canonical versioned best-effort event fingerprint |
| `writer` | Bounded Fluss append, acknowledgements, retry classification |
| `discontinuity` | Connection/subscription/heartbeat/time-jump evidence |
| `quarantine` | Preserve unsupported/malformed packet evidence |
| `health` | Liveness, readiness, subscription completeness, clock and append health |
| `telemetry` | Structured logs and bounded-cardinality metrics |
| `config` | Startup validation of every configuration key |
| `safety` | Safety-halt evidence — records when/why safety gates halted trading |
| `shutdown` | Uncertainty-journal persistence on shutdown |

### Configuration contract

| Key | Required | Semantics |
| --- | ---: | --- |
| Broker synthetic profile — fixed inside code (`PlatformConfig`), not an env key | — | `20` average ticks/instrument/s for the synthetic baseline profile; not a live-feed interval or per-instrument requirement |
| Broker hard maximum — fixed inside code (`PlatformConfig` → `FixedScope`), not an env key | — | `30` ticks/instrument/s hard maximum enforced by the synthetic workload profile |
| `ARROW_APP_ID` | Yes | Arrow application ID for the Go bridge |
| `ARROW_APP_SECRET` | Yes | Arrow application secret for AutoLogin |
| `ARROW_TOKEN` | No | **REMOVED 2026-08-24** — code rejects it at startup (`IngestionConfig.java`, `go-bridge/main.go`); AutoLogin creds (`ARROW_USER_ID`+`ARROW_PASSWORD`+`ARROW_TOTP_KEY`) are the only auth path |
| `ARROW_USER_ID` | No | User ID for AutoLogin (with password+TOTP) |
| `ARROW_PASSWORD` | No | Password for AutoLogin |
| `ARROW_TOTP_KEY` | No | TOTP secret for 2FA during AutoLogin |
| `ARROW_HFT_LATENCY_MS` | No | HFT tick interval ms (default 50, range 50-60000) |
| `ARROW_INSTRUMENT_TOKENS` | No | Comma-separated instrument tokens; empty = synthetic 50-instrument dev set |
| `ARROW_TICK_COUNTS` | No | Per-token emitted-tick counters for count-based losslessness evidence (ING-TCP-001); value = stderr report interval seconds (default 60); unset = off |
| `ARROW_MAX_EVENT_AGE_MS` | Yes | Max age of a broker tick relative to receive time before it is quarantined as STALE (ms); positive long, no default — must be set |
| `ARROW_MAX_FUTURE_EVENT_SKEW_MS` | Yes | Max future skew of a broker tick relative to receive time before it is quarantined as FUTURE (ms); positive long, no default — must be set |
| `ARROW_HFT_CONNECTIONS` | No | HFT socket count — if set, must equal `1` (pinned) |
| `ARROW_HFT_MAX_TOKENS_PER_CONNECTION` | No | Max instruments per connection — if set, must equal `1024` (pinned) |
| `ARROW_HFT_MAX_TOKENS_PER_REQUEST` | No | Max instruments per subscription request — if set, must equal `512` (pinned) |
| `ARROW_HFT_HEARTBEAT_SECONDS` | No | Heartbeat interval — if set, must equal `3` (pinned) |
| `ARROW_HFT_STALL_TIMEOUT_SECONDS` | No | Broker stall timeout (default 15, range 5-60) |
| `ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS` | No | Subscription response timeout (default 10, range 1-60) |
| `ARROW_HFT_RECONNECT_BASE_SECONDS` | No | Reconnect base backoff — if set, must equal `1` (pinned) |
| `ARROW_HFT_RECONNECT_MAX_SECONDS` | No | Reconnect max backoff — if set, must equal `30` (pinned) |
| `ARROW_HFT_AUTH_REFRESH_ATTEMPTS` | No | Auth refresh retries — if set, must equal `3` (pinned) |
| `ARROW_HFT_MIN_ACTIVE_SLOTS` | No | Minimum active slots before not-ready — if set, must equal `1` (pinned) |
| `ARROW_HFT_MULTI_CONNECTION_APPROVED` | No | Multi-socket approval flag (default false); rejected in `prod` |
| `INGESTION_ALLOW_DEGRADED` | No | Degraded-mode approval flag (default false); rejected in `prod` |
| `GO_ARROW_SDK_VERSION` | No | Pinned go-arrow SDK version tag `v0.0.0-20260622-7cce1630`; if unset the pinned version is used (warning logged) |
| `FLUSS_BOOTSTRAP` | Yes | Pinned environment endpoint (e.g. fluss-coordinator:9123) |
| `RAW_TABLE_NAME` | Yes | Must equal reconciled schema manifest |
| `INSTRUMENT_MANIFEST_PATH` | Yes | Path to the approved instrument-manifest CSV; the manifest version is a loader parameter (default 1) |
| `INGESTION_MAX_BATCH_RECORDS` | Yes | Validated `1..1000` (default `1`); append each accepted tick immediately; startup fails outside range |
| `INGESTION_MAX_BATCH_WAIT_MS` | Yes | Validated `0..100` (default `0`); do not wait for a batch; startup fails outside range |
| `MAX_PENDING_APPEND_RECORDS` | Yes | Validated `100..1000000` (default `150000` — T2 3k-tune, was `50000` pre-streaming-3000); stop accepting new broker data and set readiness false at the limit |
| `MAX_PENDING_APPEND_BYTES` | Yes | Default 201326592 (192 MiB; minimum 1 MiB) — T2 3k-tune, was 67108864/64 MiB pre-streaming-3000; stop accepting new broker data and set readiness false at the limit |
| `PENDING_APPEND_WARNING_PERCENT` | Yes | Default 0.80 (range 0.10-0.99); emit warning alert and set readiness false at that fraction of either pending limit |
| `APPEND_TIMEOUT_SECONDS` | Yes | Pinned client classification (default 5 s, range 1-30) |
| `DRAIN_DEADLINE_SECONDS` | Yes | Shutdown drain deadline for pending appends (default 30 s, range 1-300); added 2026-08-15 (ING-FAIL-010) — the deadline was previously hardcoded 30 s, and a never-acking Fluss must not hang shutdown |
| Fingerprint version — fixed inside code (`FingerprintBuilder.FINGERPRINT_VERSION`), not an env key | — | Canonical algorithm version (currently 1) |
| `CLOCK_OFFSET_LIMIT_MS` | Yes | 2000 ms (2s) code default (T10 NTP 2s gate); configurable via env; drift >2s → not-ready, future ticks >2s → quarantine per slot |
| `DEPLOY_ENV` | No | Deployment environment (default `dev`); `prod` rejects `INGESTION_ALLOW_DEGRADED=true` and `ARROW_HFT_MULTI_CONNECTION_APPROVED=true` |
| `ALLOW_RUNTIME_DDL` | No | `true` = DdlBootstrap may create missing tables at startup (local dev); default `false` = verify-only |
| `CLOCK_CHECK_REQUIRED` | No | `true` = clock offset outside `CLOCK_OFFSET_LIMIT_MS` is FATAL at startup (exit 1); default `false` |
| `UNCERTAINTY_JOURNAL_PATH` | No | Writable journal path (default `~/.local/state/trading-platform/ingestion/uncertainty-journal.jsonl`; container default `/data/ingestion/uncertainty-journal.jsonl`) |
| `ACCOUNT_SCOPE_ID` | No | Account scope stamped on safety-halt evidence rows (default `QP3796`) |
| `OTEL_COLLECTOR_HOST` | No | OTLP metrics endpoint (default `otel-collector:4318`) |
| `READINESS_FILE_PATH` | No | Where the readiness marker is written; unset = no marker file |
| `ARROW_BRIDGE_BIN` | No | Go arrow-bridge binary path (default `/app/arrow-bridge`) |
| `NTP_SERVER` | No | Comma-separated NTP servers for the clock check (default `ntp.ubuntu.com,time.google.com,in.pool.ntp.org`) |

Missing required configuration makes readiness false. Production never falls back to demo credentials or a guessed endpoint.

**Config notes (2026-08-14, fixed):** every `ARROW_HFT_*` key in the table is now read AND enforced by the Go bridge (in addition to the Java startup validation). Pinned keys (`ARROW_HFT_CONNECTIONS`, `ARROW_HFT_MAX_TOKENS_PER_CONNECTION`, `ARROW_HFT_MAX_TOKENS_PER_REQUEST`, `ARROW_HFT_HEARTBEAT_SECONDS`, `ARROW_HFT_RECONNECT_BASE_SECONDS`, `ARROW_HFT_RECONNECT_MAX_SECONDS`, `ARROW_HFT_AUTH_REFRESH_ATTEMPTS`, `ARROW_HFT_MIN_ACTIVE_SLOTS`) fail bridge startup with a clear FATAL message if set to any value other than the pin. Tunable keys are honored at runtime: `ARROW_HFT_STALL_TIMEOUT_SECONDS` (5-60 s) drives the feed-stall watchdog, `ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS` (1-60 s) drives the subscription-response wait, `ARROW_HFT_LATENCY_MS` drives the tick interval. (The bridge previously read an undocumented `ARROW_HFT_RESPONSE_TIMEOUT_MS`; that key is removed — the documented seconds key is the only one.)

### Startup sequence

1. Validate configuration and exact versions.
2. Initialize telemetry without logging secrets.
3. Connect to Fluss and validate required table/schema version.
4. Load exactly one approved instrument manifest snapshot.
5. Validate every active row and routing field.
6. Validate the Go arrow-bridge binary exists and is runnable; a missing or non-runnable binary is a FATAL startup error (clear message, non-zero exit).
7. Start arrow-bridge as subprocess with configured auth env vars.
8. Java sniffs bridge's stdout (proto frames only).
9. Enter READY only after recent successful Fluss append acknowledgement and acceptable clock offset.

### Packet processing algorithm

```text
receive proto frame from stdin
→ parse JSON; reject malformed lines
→ validate token, price, and timestamp semantics
→ resolve instrument manifest row; quarantine missing tokens
→ classify validity (VALID_TRADE / VALID_NON_TRADE / INVALID_VALUES / MISSING_INSTRUMENT)
→ hash raw JSON bytes for payload integrity
→ calculate versioned event fingerprint
→ create raw row containing original bytes + typed fields + provenance
→ submit single tick immediately through bounded writer (no application batching; INGESTION_MAX_BATCH_RECORDS=1, INGESTION_MAX_BATCH_WAIT_MS=0; transport rows coalesced ≤ 20 ms linger)
→ record acknowledgement timestamp or uncertainty
```

Ingestion appends an accepted raw packet even if its fingerprint was seen before. Compute performs bounded logical deduplication (the durable dedup set is Fluss-authoritative under DEC-038). No time-based or record-count-based application batching is permitted: each accepted tick is submitted individually. The Fluss client may coalesce rows into transport batches, bounded at 20 ms linger (`client.writer.batch-timeout`).

> **Broker burst cadence (2026-08-26, Arrow HFT protocol docs):** `latency` is the per-symbol tick interval (project `ARROW_HFT_LATENCY_MS=50` → 1 tick per stock per 50 ms); broker frames are zstd-compressed and multiple instrument ticks are **concatenated in a single message**. So 1,024/3,000 instruments arrive as one burst block every 50 ms, not spread across the window. The `client.writer.batch-timeout=20 ms` linger therefore splits a 50 ms burst into ~2–3 partial sends. This is a documented tuning-mismatch candidate (align to 50 ms), not a proven bottleneck — measurement (below) does not show the single reader+writer choking at the target rate.

### Future: 3-connection / 3000-instrument scaling plan (deferred — NOT to be implemented now)

**Status: diagnosis only (2026-08-26, `flink-performance` BLOCK: diagnosis only — no lever without measured bottleneck proof).** The measured intake path is NOT the bottleneck: parse+fingerprint run at ~373k ticks/s and the single Fluss writer appends at ~60.8k ticks/s, at/above the 60k target. No rewrite is required to reach 3 sockets. This section records the *minimal* change plan so it is ready when multi-connection is approved — nothing here is a current defect.

**Why 3 sockets, not 1:** the Arrow HFT protocol encodes at most `MaxHFTTokensPerConnection = 1024` instruments per socket. To subscribe 3,000 instruments you need `ceil(3000 / 1024) = 3` sockets. Everything needed to *run* 3 sockets already exists in code:

| Need | Already in code | Location |
| --- | --- | --- |
| 3-connection cap | `MaxHFTConnections = 3` | `go-bridge/subscription_plan.go` |
| Configurable count | `ARROW_HFT_CONNECTIONS` range 1..3 | `main.go` policy block |
| Token split across sockets | `SubscriptionPlan.Slots` (`hft-0/1/2`) | `subscription_plan.go` |
| Multi-slot supervisor | `runHFTSupervisor` (multi-slot) | `supervisor.go` |
| One raw table | All slots → one proto stream (T6) → one `RawTickWriter` → one `raw_table_1` | Java `IngestionService` |
| Per-slot fidelity | `slot_id`/`connection_id`/`connection_epoch` on every event | bridge events |

**The single deliberate guard (the only change required to enable 3 sockets):**

- `go-bridge/main.go` `runHFT`: `if len(plan.Slots) != 1` → `FATAL: approved for exactly one Arrow HFT socket`. This is a **policy gate**, not a design gap. Enabling 3 sockets means relaxing it to allow `len(plan.Slots) <= MaxHFTConnections` (and honoring the existing `ARROW_HFT_MULTI_CONNECTION_APPROVED` flag).
- **Do NOT change**: the Java reader, `RawTickWriter`, `FlussClientAdapter`, table contract, or fingerprints. All sockets funnel to the same writer and same `raw_table_1`; dedup stays as-is (fingerprint already includes connection scope).

**Wiring checklist when implementing (governed):**

1. Build a 3-slot `SubscriptionPlan` (token slice of the full approved manifest across `Slots`); verify the plan fingerprint matches the manifest router.
2. Set `ARROW_HFT_CONNECTIONS=3` and `ARROW_HFT_MULTI_CONNECTION_APPROVED=true`; confirm `DEPLOY_ENV`/policy accepts it (currently rejected in `prod`).
3. Relax the single-socket gate only after approval; keep the FATAL behavior for `len(Slots) > MaxHFTConnections` as a hard safety bound.
4. Confirm per-slot epoch/slot_id routing so duplicates across sockets are still deduped correctly (they are — fingerprint + scope).
5. Re-run the throughput/memory probes at the 3000-instrument, 3-socket envelope before any live go-live.

**Known non-blocking candidate (do NOT pre-emptively tune):** the `client.writer.batch-timeout=20 ms` linger splits the 50 ms broker burst into ~2–3 partial sends. At 3 sockets this reappears per socket. It is not a proven bottleneck at 60k; align to 50 ms (if ever) as a separate, evidence-gated change — not part of enabling 3 sockets.


### Fingerprint contract

The versioned canonical specification must define:

- Hash algorithm and output encoding.
- Connection scope fields.
- Field order.
- Null representation.
- Integer/decimal/price canonicalization.
- Timestamp unit and timezone.
- Trade/quote/depth fields included.
- Raw-byte contribution, if approved.
- Collision and identical-legitimate-event limitation.

Fingerprint identity is best-effort, not broker-global identity ([`DEC-012`](../01_project/04-decisions.md)).

### Backpressure and memory

- The pending append queue is bounded by `MAX_PENDING_APPEND_RECORDS` (default 150000 records) and `MAX_PENDING_APPEND_BYTES` (default 201326592 bytes / 192 MiB).
- Before accepting a tick, ingestion SHALL reject it when accepting would exceed either pending limit.
- At 80% of either pending limit: set readiness false, emit a warning event containing current records, current bytes, and both limits.
- At 100% of either pending limit: stop broker reads/subscriptions, keep readiness false, emit a critical event, and preserve an acknowledged-loss/uncertainty record. Silently discarding data is prohibited.
- Pending counters SHALL decrease only after the append completes, whether successful or failed.
- Record receive time, append-start time, append-acknowledgement time, append outcome, record size, and error class for every append outcome.
- Retryable append failures retry with exponential backoff (100, 200, 400 ms) up to 3 attempts; fatal failures do not retry. A timeout outcome is `UNCERTAIN` (the row may already be persisted) and is never retried. Classification uses the pinned `RetryClassifier`.
- No unbounded custom queue is permitted.
- Arrow payloads SHALL NOT be compressed in the ingestion-to-Fluss path.
- TCP flow control is not described as lossless without a broker test.

### Slow-Fluss ingestion policy (`EVIDENCE-GATE-ING-BUFFER-001`) — RESOLVED BY CAPACITY

When Fluss latency, retry count, pending records, or pending bytes cross a configured threshold, the platform alerts immediately and automatic system checks determine readiness.

**Resolution:** Measured real single-writer Fluss capacity (2026-08-26, `FlussThroughputProbeTest` against the live dev Fluss, one shared `AppendWriter`): **60,805 rows/s**, avg 19.5 ms, p50 18.8 ms, **p99 45.5 ms**, 20,480 acked. The platform's sustained gate is 50,000 ticks/s per DEC-036, and the 3,000-instrument / 3-connection target is ~60,000 t/s. The single shared writer meets the 60k target but with ~1.0× margin (thin, not "with margin"); the earlier "1-2 million ticks/s" figure is not supported by this measurement and is superseded. Neither a durable local SSD buffer nor a controlled subscription pause is required today, but because the single-writer headroom at 60k is thin, ingestion capacity is a watch item (not a proven blocker).

**Remaining defensive bound:** Bounded pending-append limits (150,000 records / 192 MiB bytes default) remain. Reaching a limit is a platform-capacity fault, not a normal operating condition; the existing readiness-halt behavior applies, and indefinite in-memory buffering or silent data loss remains prohibited.

### Failure matrix

| Failure | Required behavior |
| --- | --- |
| Go bridge crash | Java detects the crash (stdin EOF / non-zero exit code), logs `BRIDGE_CRASH`, records a `DROP` discontinuity, restarts the bridge once, then stops cleanly (exit 0); not-ready is signalled by clearing the readiness marker |
| Go bridge auth failure | Exit immediately with error to stderr; Java exits on pipe close |
| Missing instrument | Quarantine; do not append keyed raw row |
| Invalid trade values | Append evidence marked invalid; Compute excludes |
| Broker disconnect | Go bridge SDK handles reconnect internally; Java pauses processing during gaps |
| Subscription incomplete | Go bridge reports via stderr; Java detects startup subscription failure |
| Fluss unavailable | Bound pending data; retry only under tested policy; not ready |
| Append timeout/unknown | Record uncertainty; do not claim lossless delivery |
| Clock offset violation | Not ready; alert |
| Forced shutdown | Record abnormal loss/uncertainty evidence |

### Shutdown

Stop new reads/subscriptions, drain accepted pending writes for a configured deadline, persist uncertainty counters, close clients, and report drain result. Restart increments the assigned connection epoch and revalidates the manifest/subscriptions.

### Telemetry

Metrics include packet/byte rate, append acknowledgement latency, pending bytes, reconnects, active/subscribed connections, manifest version, decode/quarantine reasons, fingerprint count, discontinuities, clock offset, readiness, and acknowledged loss.

Logs include service, instance, connection scope, decoder/protocol version, manifest version, and event/correlation IDs. Raw packets and credentials are never logged.

### Required tests

- `ING-UNIT-001` golden packet decode.
- `ING-UNIT-002` unknown version quarantine.
- `ING-UNIT-003` byte/hash round trip.
- `ING-UNIT-004` normalization and validity classification.
- `ING-UNIT-005` fingerprint canonicalization and fixtures.
- `ING-INT-001` manifest load/subscription completeness.
- `ING-INT-002` Fluss append and acknowledgement timestamps.
- `ING-INT-003` no application batching: one append per accepted tick; no batch >1 record.
- `ING-FAIL-001` reconnect/resubscribe/epoch.
- `ING-FAIL-002` bounded append backpressure (80% warning, 100% critical halt, no unrecorded drop).
- `ING-FAIL-003` forced shutdown and uncertainty accounting.
- `ING-TCP-001` count-based broker TCP losslessness: 15-min market-hours run on the single production connection; per-token bridge emitted-tick counts (`ARROW_TICK_COUNTS`, stderr reports + final report persisted to `ARROW_TICK_COUNTS_FILE`) reconciled against per-token Fluss rows (`TokenCountReconcile` probe: raw_table_1 LogScanner + ingestion_quarantine BatchScanner; `reconcile-compare.py --exact --sink total`) — exact equality per token proves no loss. **Market-hours proof PASS 2026-08-14** (epoch 08:24:06–08:39:06Z: bridge 646,102 = sink 646,102; 1024/1024 tokens exact; evidence `logs/tracker-14/losslessness-markethours-20260814.md`). Instrumentation validated live 2026-08-13 post-close (3 HFT-1024 epochs: 3,072 emitted = 3,072 stored; evidence `logs/tracker-14/losslessness-validation-20260813.md`).
- `ING-PERF-001` variable 50,000 ticks/s average-baseline full session; append accept→ack p99 <5 ms (code budget `P99_BUDGET_NS`, `PerfBaselineTest#appendHotPathP99`).
- `ING-PERF-002` ~~60,000 ticks/s peak~~ RETIRED with the peak campaign (DEC-036, 2026-08-13); superseded by the 60,000 ticks/s gate (`ING-PERF-001`).

Current golden-corpus coverage (Step 2 of the ingestion audit):

- `go-bridge/testdata/golden/` — committed wire frames + golden tick records (full-tick 196B, ltp-tick 40B, response 540B wire-only fixture, unknown-packet 64B), generated reproducibly by `go-bridge/cmd/gen-corpus`.
- Go (`go-bridge/golden_corpus_test.go`): the real bridge path (SDK connect + `runHFT` against a fake broker) emits golden ticks matching the golden records byte-for-byte, including the full depth ladder; the unknown packet is rejected at SDK decode and never emitted; `raw_payload`/`payload_hash` preserve the exact frame bytes + SHA-256.
- Java (`GoldenCorpusPayloadHashTest`): `PayloadHashValidator` accepts every golden packet and decodes it to the exact frame bytes; a tampered frame is rejected with `HASH_MISMATCH`.
- Java service: unknown `feed` values are quarantined (`UNKNOWN_VERSION`) before trade classification (`IngestionService` step 3, `AC-ING-002` defense-in-depth; the SDK already rejects unknown packet types at decode).
- Mode-coverage (HFT feed): the bridge emits HFT LTPC + FULL ticks through the vendored SDK (`hft_stream.go` `parseHFTLTP`/`parseHFTFull`, 40 B / 196 B zstd); the golden corpus (`go-bridge/testdata/golden/`) pins FULL and LTP frames byte-for-byte, and `hft_stream_test.go` pins the SDK parse layouts. Standard-stream decode coverage (13/17/93/249 B `ParseMarketTick` layouts, CAS trailer, `fromStandardTick`) was REMOVED 2026-08-14 with the Standard feed — the vendored SDK still carries those parsers (pinned third-party code) but the bridge never invokes them. The AutoLogin auth fix (validate-2fa host + appID field) remains in the vendored SDK.

ING-RES-001 resilience coverage (Step 3 of the ingestion audit, `go-bridge/resilience_100_test.go`):

- `TestINGRES001OneHundredForcedDisconnectReconnectCycles` — drives `runReconnectLoop` with the real SDK connect path (`streamFactoryFor` → `ConnectHFTDataStreamURL`) through 100 forced disconnect/reconnect cycles against a wire drop broker (subscription response + one tick, then abrupt TCP close). Asserts ≥ 100 cycles complete, final goroutine count ≤ baseline + 2, final open-FD count ≤ baseline + 2, and a connection high-water mark ≤ 1 (no orphan sockets). Backoff is suppressed for wall-clock speed (~33s); backoff timing itself is unit-tested separately (`TestReconnectLoopEpochAndBackoffAfterForcedDisconnect`).
- `TestINGRES001HealthySlotNotInterruptedByPeerReconnect` — real supervisor (`runHFTSupervisorWithFactory`) with slot `hft-0` on the real SDK + drop broker and slot `hft-1` on a healthy fake stream: the healthy slot stays `ACTIVE` with ticks flowing while the peer slot disconnects/reconnects through the supervisor's real 1s→2s backoff.
- Java live-thread clause (within baseline + 2 after reconnects) is exercised by the JVM-level crash-restart harness `code/01_platform/04_scripts/soak-reconnect-loop.sh` (restart budget 1, FD/thread baseline assertions after restart); the full wall-clock 100-cycle supervisor soak with real backoff completed 2026-08-13 (hub `res001-soak`, `ING_RES001_REAL_BACKOFF=1`): 100/100 forced-disconnect cycles in 2852.7 s, final goroutines 3 vs baseline 4, fds 7 vs baseline 7 — no leak; `--- PASS`; evidence `logs/res001-real-backoff/res001-real-backoff-20260813.out` (3-cycle smoke earlier: 3/3 cycles, goroutines 3 vs 4, fds 39 vs 39 — no leak).

### Implementation checklist (from [`01_plan.md`](./01-foundation.md) Task 2)

Before code is accepted, verify each item:

1. Parse and validate every ingestion configuration key in the constants table above before connecting to Arrow or Fluss.
2. Decode and validate one broker tick, then submit that single tick immediately to the Fluss writer.
3. No time-based or record-count-based application batch exists.
4. Pending append counters exist in both records and bytes.
5. Before accepting a tick, reject it when accepting would exceed either pending limit.
6. At 80% of either pending limit: set readiness false, emit warning event containing current records, current bytes, and both limits.
7. At 100% of either pending limit: stop broker reads/subscriptions, keep readiness false, emit critical event, preserve acknowledged-loss/uncertainty record. Do not silently discard data.
8. Decrease pending counters only after append completes, whether successful or failed.
9. Record receive time, append-start time, append-acknowledgement time, append outcome, record size, and error class for every append outcome.
10. Do not compress Arrow payloads in the ingestion-to-Fluss path.

#### Acceptance checks

- At the variable 50,000 ticks/s average baseline (3,000 instruments; every instrument ≤20 ticks/s; the 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045), one append submission per accepted input tick; no application batch contains more than one record. The gate is certified at the synthetic hot-path envelope; the 3,000-instrument / 3-connection production-envelope run is removed from acceptance (DEC-037, 2026-08-13).
- 2026-08-13 certification under the DEC-036 50k gate (`PerfBaselineTest`, `INGESTION_INT_TEST_PERF=true`, in-process — no live Fluss): mock-socket wire run 492,419 emitted = 492,419 received (49,242 tps, 0 wire loss; socket floor 47.5k PASS); append hot path 148,733 writes (49,578 tps, 0 failures, p99 accept→ack 0.000 ms; floor 48k, budget &lt; 5 ms PASS). 2/2 green. The 30-minute 3,000-instrument production-manifest run is removed from acceptance (DEC-037, 2026-08-13 — not to be tested).
- **2026-08-26 re-measurement (live dev Fluss, no code change):** hot-path Java append (`PerfBaselineTest#appendHotPathP99`, stub converter, in-process) **49,502 tps / p99 0.001 ms / 0 fail**; real single-writer Fluss capacity (`FlussThroughputProbeTest`, one shared `AppendWriter` against live `raw_table_1`) **60,805 rows/s / avg 19.5 ms / p50 18.8 ms / p99 45.5 ms, 20,480 acked / 0 fail**; JSON-parse + SHA-256 fingerprint cost (throwaway probe on the real `GoTick` NDJSON shape) **~373k ticks/s / 2.7 µs per tick ≈ 6.2× the 60k target**. Verdict: the single-reader/single-writer intake is **not** the bottleneck at the 60k target — the write path is the tightest stage (~61k, thin margin) and is a watch item; the earlier singleton-reader "wall" hypothesis is not confirmed. These figures are diagnosis evidence, not new acceptance gates.
- Simulated slow Fluss writer reaches 80% warning condition before exceeding a pending limit.
- Simulated unavailable Fluss writer reaches 100% condition without exceeding either limit and without an unrecorded drop.

### Definition of done

Implementation is complete only when the broker corpus and versions are pinned, the go-arrow SDK version is pinned in `go.mod`, all required fields match the validated DDL, all configuration constants in section 2 of [`01_plan.md`](./01-foundation.md) are validated at startup and reject incorrect values, tests pass, memory remains bounded with tiered 80%/100% response, no application batching exists, readiness reflects partial subscriptions/append uncertainty, the NDJSON schema is versioned and documented, the Go bridge binary builds from pinned go-arrow dependency, and no active documentation refers to Kite, `seq_no`, or exact missing sequence ranges without approved evidence.

## Verification mapping

The required behavior above is verified by the canonical [Ingestion test design](./11-testing-and-release.md#ingestion): `ING-UNIT-001` to `ING-UNIT-005`, `ING-INT-001` to `ING-INT-003`, `BROKER-MD-001`, `ING-FAIL-001` to `ING-FAIL-003`, `ING-TCP-001`, and `ING-PERF-001` to `ING-PERF-002`.

## Integrated test-hardening backlog (15-plan, 2026-08-15 → 2026-08-23) — absorbed

> **INTEGRATED 2026-08-23:** `15-ingestion-test-hardening.md` was the 2026-08-15 hardening plan for 8 audit gaps (G1–G8) and 26 hardening IDs across M1–M5 plus CHG-032/CHG-033 follow-ups. Its **all 8 gaps are RESOLVED, all 26 IDs are landed, suite 236/0/8-skips** (was 193 at audit — now 236 via +43). The substance — gap resolutions, per-ID implementing classes, and milestone gates — is absorbed here. The standalone 15 file is retained as a historical marker with an INTEGRATED banner; consult this section for current truth. Out-of-scope explicitly deferred (3,000-instrument / 3-connection envelope via DEC-036/037, 90k peak retired) is **correctly not implemented** and does not block current 1,024 / 1 operation (see §Status). Go path note: docs say `go-bridge/` but code lives at `code/02_services/01_ingestion/go-bridge/` — functionally identical, no missing behavior. Changes tracked via `CHG-015`..`CHG-019` / `CHG-032` / `CHG-033`.

| Gap | Severity | Resolution 2026-08-15 | Tests / evidence |
| --- | --- | --- | --- |
| G1 `TIME_JUMP` never emitted | High (code gap) | **RESOLVED M1** — `TimeJumpMonitor` + periodic clock re-measurement in `IngestionService` (`CLOCK_OFFSET_LIMIT_MS`) emits one `TIME_JUMP` row per episode | `discontinuity/TimeJumpMonitor.java` `TimeJumpMonitorTest` (8) + `DiscontinuityReasonMappingTest` `ING-FAIL-007` |
| G2 halt latch never auto-resets | Medium | **RESOLVED M1** — pinned `halted until restart` (fail-closed, needs operator restart) | `write/AppendTracker.java` `AppendTrackerTest.haltLatchPersistsAfterDrain` `ING-FAIL-005` |
| G3 `reconcile-compare.py` zero tests | High | **RESOLVED M2** — comparator suite 20 `unittest` cases, fail-closed on empty/trunc, chunked, `-1` sentinel | `code/01_platform/04_scripts/tests/test_reconcile_compare.py` `ING-TCP-002` |
| G4 no fuzz on parse path | Medium | **RESOLVED M4** (landed early) — seeded `FuzzIngestionTest` 3 seeds × ~810 lines, no external lib | `FuzzIngestionTest.java` `ING-DQ-011` |
| G5 Go bridge tests minimal | Medium | **RESOLVED M2+M5** — 13 exec-self FATAL (wrong pins `CONNECTIONS=2`, non-int, OOR) + missing `ARROW_APP_ID` + FATAL message text, env stripped | `code/02_services/01_ingestion/go-bridge/hft_policy_test.go` `ING-UNIT-018` + `ING-UNIT-020` (14 tests) |
| G6 metrics secret scrubbing untested | Medium | **RESOLVED M5** — `OtlpMetricsEmitter` now scrubs `ARROW_TOKEN=`/`Bearer`/`raw_payload` from reason labels (found real leak) | `telemetry/OtlpMetricsEmitter.java` `OtlpMetricsEmitterTest.exportBodyScrubsSecrets` `ING-UNIT-021` |
| G7 Java↔Go parity by convention | Medium | **RESOLVED M2** — 11-key table mirrored Java `ConfigParityTest` + Go `hft_policy_test.go` (`hftPin`/`hftRange` for `ARROW_HFT_LATENCY_MS`/`CONNECTIONS`) | `config/ConfigParityTest.java` + `hft_policy_test.go` `ING-UNIT-018` |
| G8 READY gating no exhaustive test | Low | **RESOLVED M3** — `ReadinessGatingMatrixTest` walks 8 dimensions (liveness/Fluss/tracker/broker/sub/data/frame/clock) | `health/ReadinessGatingMatrixTest.java` `ING-INT-005` |

| Milestone | IDs | Evidence / implementing classes | Suite after |
| --- | --- | --- | --- |
| **M1 Safety core** | `ING-FAIL-004` `005` `007` `ING-DQ-010` | `AppendTrackerTest.concurrentAcceptReleaseKeepsInvariants` (16×100k), `haltLatchPersists…`, `TimeJumpMonitor` + `IngestionNoSilentDropTest` (no-op-sink seam, `accepted+quarantined+rejected==lines`) | `204/0/8` |
| **M2 Losslessness+config** | `ING-TCP-002` `ING-UNIT-018` `019` | comparator 20 + parity 11-key + `docs_audit.py` **C16** `c16_env_key_drift` `PASS` | `211/0/8` |
| **M3 Failure-path E2E** | `ING-FAIL-008` `009` `010` `ING-INT-005` | `BridgeCrashLoopE2ETest` (scripted `2× BRIDGE_CRASH` `one restart`) `BridgeAuthFailureE2ETest` (exit 2 `auth_failure`) `ShutdownDeadlockTest` (`DRAIN_DEADLINE_SECONDS` 30 `UncertaintyJournal.Entry` R-260) `ReadinessGatingMatrixTest` | `224/0/8` |
| **M4 Go bridge+data quality** | `ING-UNIT-014` `015` `016` `017` `ING-RES-002` `003` `004` `ING-TCP-003` | `StaleDataTradeGuardTest.exactBoundaryMatrix` (7-case) `PayloadHashValidatorTest` (R-186 `sha256("")`) `FingerprintBuilderTest.goldenHashFrozen` `aec03d3d…` + `dedupSemanticsPinned`, `subscription_plan_test.go` `reconnect_test.go` (`Backoff` `1,2,4,8,16,30…`) `ndjson_test.go` (`contract_version=2` + chunked 52×`<603B` `file==stderr`) | `231/0/8` |
| **M5 Telemetry+ops** | `ING-UNIT-021` `022` `ING-INT-006` `ING-FAIL-006` `ING-UNIT-020` | `OtlpMetricsEmitterTest` (scrub+`labelCardinalityBounded` `{service.name…}` `hft-\d+`) `tests/test_docker_entrypoint.sh` (3 FATAL codes/messages, `shellcheck` clean, `run-monday-gates.sh` wired) `AppendTrackerTest.warningThrottledToOncePer30sWindow` (reflection, no sleep) | `234/0/8` |
| **Follow-up CHG-032** | `ING-UNIT-023` | `BridgeShutdownRegressionTest.java` (16 threads? in-process `a980a33` drain `bridge loop ended` `single journal`, ING-DQ-010 seam, no Fluss/Go) | `235/0/8` |
| **Follow-up CHG-033** | `ING-UNIT-024` | `BridgeShutdownHookTest.java` + `BridgeShutdownDriver.java` (spawned JVM `SIGTERM` exit `143` `main-thread join`, real hook layer) | **`236/0/8`** |

**Per-gap distillation (what 03 now carries, not just where 15 pointed):**

- **G1/G2 (code gaps):** `TIME_JUMP` is now a wired discontinuity (not an enum gap) and the halt latch semantic is pinned fail-closed — both have explicit `ING-FAIL-007/005` tests, not just docs.
- **G3/G4 (tooling / fuzz):** `reconcile-compare.py` fails closed and `FuzzIngestionTest` covers malformed/oversized/NUL/1 MB lines — both default-run via no-op-sink seam (no Fluss needed).
- **G5/G7 (Go):** Java `exactInt`/`intRange` ↔ Go `hftPin`/`hftRange` are tested by the same 11-key table; the `go-bridge/` vs `code/02_services/01_ingestion/go-bridge/` path difference is cosmetic (49 `*.go` files present, `91` grep hits).
- **G6 (telemetry):** the OTLP scrub found and fixed a real `ARROW_TOKEN=` leak in the export body — `C15` group-writable still `PASS`.
- **G8 (readiness):** `isReady()` false when any single dimension false — `10` cases incl. fail-closed clock.

**Ground-rule carry-forward (from 15 §2):** pure JDK / Go stdlib / Python `unittest` only (no new deps), every `ING-*` ID now in `11-testing-and-release.md` §Ingestion and `docs_audit.py` C6 line `341/236/294` (prior `341/235…` etc — see 03 §Status); 3k envelope stays `ACCEPTED DEFERRAL` per §5.
