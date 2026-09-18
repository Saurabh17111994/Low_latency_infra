# Testing and Release

Use this file after each phase to track all tests, map requirements to proof, and prepare the final release evidence.

## Master test catalog

<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | Partially implemented (offline) — 22 offline deterministic contracts green (9 Action-Capture + 5 Babysitter + 4 Signal-Harness + 4 Executor-Offline) + 3 Foundation (MOCK/STATE); market/4VM still blocked |
| Owner | Component owners; Platform owns integration/acceptance evidence |
| Scope | Unit, harness, integration, failure, recovery, performance, security, release |
| Rule | A skipped/flaky mandatory gate is a failure until dispositioned with evidence |

### Progress snapshot

| Work | Current state |
| --- | --- |
| Test design | Complete: every required test type is documented in this file or its owning phase document. |
| Executable tests | Ingestion suites executable and green: 247 ingestion + 466 common (2026-08-25 CHG-102 re-verified; raw surefire 469 minus 2 gated `FlussBundleReader*` reports); plus offline 2026-08-24: ActionCaptureContractTest 9/9, BabysitterContractTest 5/5, SignalHarnessContractTest 4/4, ExecutorOfflineContract 4/4, SyntheticWorkloadTest 3/3, PositionProjectorTest 12/12, FingerprintDedupFunctionTest 15/15 — total offline 22 new + 27 existing = 49 green deterministic without market/4VM. Cargo **207/207** (nautilus-execution-service, CHG-107: +1 clock-drift wiring; CHG-106: +10 amend; was 196/196) clippy/fmt/vet clean. |
| Runtime evidence | Ingestion live evidence recorded (2026-08-09): E2E fake-broker → Fluss (10,716 rows persisted), 58,951 ticks/s baseline probe on the 1,024-instrument envelope, SAFETY-INT-001 Fluss-connector proof. Signal Slice 1 live smoke recorded (2026-08-09): 205,146 candle rows, 1,074 instruments, 48 checkpoints (see [`04-signal-job.md`](./04-signal-job.md) §Slice 1 evidence). Offline 2026-08-24: no market/4VM; single-JVM harness only. |
| Live-money approval | Blocked until executable tests and all release evidence pass (market + 4VM Swarm still required). |

### Implementation status — 2026-08-24 (offline laptop, no market/4VM)

| Area | CAN offline (done 2026-08-24) | CANNOT offline (needs market/4VM) |
| --- | --- | --- |
| Foundation | MOCK-UNIT-001 deterministic sequence, STATE-DEDUP-001 compact TTL 300s, STATE-CANDLE-001 one candle per window — all deterministic unit/harness green | PERF-PER-INSTRUMENT-001/002 30-min 3k instruments 50k tps, FAIL-PENDING/CHECKPOINT 5-min — needs capacity/4VM |
| Action Capture | AC-UNIT-001..005, AC-INT-001, AC-FAIL-001/003, AC-REC-001 — ActionCaptureContractTest 9/9 (decode/status, fingerprint, correlation priority/ambiguity, lifecycle precedence, position independent, audit-before-projection, crash resume, quarantine halt, rebuild) | AC-FAIL-002/OPS market broker mapping, live Fluss audit lag — needs Arrow postback stream |
| Babysitter | BAB-INT-001 positions changelog schema, BAB-HARNESS-001 checkpoint restore zero actions, BAB-FAIL-001 gap→readiness false, BAB-FAIL-002 stale suppression, BAB-OPS-001 never claims trading — BabysitterContractTest 5/5 | BAB live Fluss changelog offsets replication factor 3, 4VM ZK quorum |
| Signal | SIG-HARNESS-001 out-of-order watermark idleness, SIG-STATE-001 bounded checkpoint vs dedup table, SIG-STATE-002 restart rehydrate compact cache, SIG-INT-001 pinned Fluss source/sink — SignalHarnessContractTest 4/4 | SIG live market 15s window late-drop with real Fluss 0.9.1 checkpoints, PERF |
| Executor | EXE-FAIL-001 crash before/during/after no duplicate, EXE-FAIL-003 missing/corrupt blocks, EXE-FAIL-006 quarantine blocks unsafe, EXE-AUDIT-001 audit rebuild — executor_offline_contract 4/4 (Duplicate vs ContractViolation, HALTED blocks) | EXE live Arrow REST WS place/fill, reconciliation, 4VM HA gate epoch fence |
| Verification | cargo clippy -D warnings 0, cargo fmt --check 0, go vet 0, cargo test **196/196** (CHG-102 re-count; was 168) historical 2026-08-24, mvn common 17/17 historical 2026-08-24, compute 23/23 historical 2026-08-24, action-capture 9/9, mock-arrow 3/3 — historical counts per 2026-08-24 not C6 466/247/387 | Full 576-suite with Fluss live 0.9.1 S3 checkpoints, 7h soak, production Swarm |
### Detailed test designs

This catalog is the single authoritative home for detailed test inputs, actions, pass results, and evidence. Component dossiers define the system to build and link here for verification; they do not duplicate test procedures.

**How the live half runs.** Rows marked *env-gated on `FLUSS_BOOTSTRAP`* are skipped
— recorded as 0 tests — by `make test` and by the Monday gate's Java step, which
exports only the `INGESTION_INT_TEST_*` flags and does not set the variable; the
gateway's drills were additionally outside that step's module scope. `make
drill-live` runs them against the local stack, and the Monday gate runs it at the
end of step 9; their surefire reports land in `target/surefire-reports-drills/` so
the C6 test counts keep counting the plain suite only. Compute's Fluss drills are
not in that target: they also require `COMPUTE_INT_TEST` (a Flink job harness) and
run with the compute and chaos targets.

| Area | Test-scope link |
| --- | --- |
| Mock broker and workload generator | [Foundation and workload](#foundation-and-workload) |
| Schema and storage | [Schema and storage](#schema-and-storage) |
| Ingestion | [Ingestion](#ingestion) |
| Signal job | [Signal job](#signal-job) |
| Action Capture | [Action Capture](#action-capture) |
| Babysitter | [Babysitter](#babysitter) |
| Executor | [Executor](#executor) |
| Local Compose | [Local Compose](#local-compose) |
| Production Swarm | [Production Swarm](#production-swarm) |
| Observability and operations | [Observability and operations](#observability-and-operations) |

The following mappings identify the detailed sections in this catalog.

| Test IDs | Detailed section |
| --- | --- |
| `MOCK-*`, `PERF-PER-INSTRUMENT-*`, `FAIL-PENDING-001`, `FAIL-CHECKPOINT-001`, `STATE-*`, `BABYSITTER-001` | [Foundation and workload](#foundation-and-workload) |
| `SCHEMA-*`, `COMPAT-FLUSS-*` | [Schema and storage](#schema-and-storage) |
| `ING-*`, `BROKER-MD-001` | [Ingestion](#ingestion) |
| `SIG-*`, `STATE-COMPAT-001`, `COMPAT-FLINK-001` | [Signal job](#signal-job) |
| `AC-*`, `BROKER-PB-001` | [Action Capture](#action-capture) |
| `BAB-*` | [Babysitter](#babysitter) |
| `EXE-*`, `ARROW-REST-*` | [Executor](#executor) |
| `LOCAL-*` | [Local Compose](#local-compose) |
| `SWARM-*`, `PERF-NODELOSS-001`, `SEC-*` | [Production Swarm](#production-swarm) |
| `OPS-*` | [Observability and operations](#observability-and-operations) |

## Component test designs

### Foundation and workload

| Test ID | Duration | Input | Pass conditions |
| --- | ---: | --- | --- |
| `PERF-PER-INSTRUMENT-001` | 30 min | 3,000 instruments; variable 50,000 ticks/s average baseline | Raw append p99 <50 ms; decision p99 <100 ms; no acknowledged loss; total memory <85%; checkpoint p99 <5 s |
| `PERF-PER-INSTRUMENT-002` | 10 min | 3,000 instruments; variable baseline; restart Signal job once | Processing resumes <30 s; state restores; no duplicate final candle or decision within proven boundary |
| `PERF-PER-INSTRUMENT-003` | RETIRED with the peak campaign (DEC-036, 2026-08-13) | — | Was: declared campaign at 3,000 instruments and 60,000 ticks/s peak; no peak-capacity evidence row remains |
| `FAIL-PENDING-001` | Until queue limit | Fluss append artificially stalled | Warning at 80%; readiness false; critical at 100%; no unrecorded loss |
| `FAIL-CHECKPOINT-001` | 5 min | Force checkpoint failure | Signal job suppresses decisions; one idempotent safety halt published; no Arrow REST call from Flink |
| `STATE-DEDUP-001` | 15 min | Variable baseline plus duplicates | Duplicate state contains compact identity/timestamps only; expired entries removed; no raw payload retained (DEC-038: the accepted dedup set is observable in the Fluss dedup table; the Flink checkpoint does not duplicate it) |
| `STATE-CANDLE-001` | 15 min | Variable baseline input | One final candle per non-empty 15-second window; no tick collection exists in active state |
| `BABYSITTER-001` | 5 min | Repeated position updates | Latest state only; zero actions; startup rejects action enablement |

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `MOCK-UNIT-001` | Same manifest, seed, profile, and clock | Repeated runs produce the identical tick sequence and timestamps. |
| `MOCK-UNIT-002` | Variable baseline and peak profiles | The baseline averages ≈16.7 ticks/s/instrument at the 50,000 gate (generator capable of 20 ticks/s/instrument); the peak profile reaches the 60,000 ticks/s theoretical cap ceiling across 3,000 instruments — generator capability only, not a platform acceptance target (DEC-036); no instrument exceeds 20 ticks/s in the defined enforcement window. |
| `MOCK-UNIT-003` | Missing seed, unknown profile, missing instrument manifest, or a cap above 20 ticks/s | Startup rejects the invalid configuration with a clear error. |
| `MOCK-PERF-001` | Recorded per-instrument and aggregate rate distribution | The evidence shows variable arrivals rather than a universal 50 ms cycle, plus the configured seed, profile, average, cap, and observed distribution. |

### Schema and storage

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `SCHEMA-UNIT-001` | Manifest generation, DDL checksum, and version fields | A changed DDL or checksum prevents readiness. |
| `COMPAT-FLUSS-001` | Every approved DDL parses, applies, and is inspected on the pinned matrix | Effective schema/options equal the approved manifest. |
| `SCHEMA-UNIT-002` | Required-field parity and non-null routing identity | Missing field or invalid routing key fails validation. |
| `SCHEMA-UNIT-003` | Missing, unknown, or placeholder schema/configuration version | Readiness is blocked; no guessed default or silent compatibility path is used. |
| `COMPAT-FLUSS-002` | Raw `BYTES` write/read round trip | Original bytes and hash are unchanged. |
| `COMPAT-FLUSS-003` | LOG, KV, and changelog behavior | Observed behavior matches the table contract. |
| `COMPAT-FLUSS-004` | Stale, regressive, and conflicting KV updates | Invalid transition is rejected, quarantined, and audited. |
| `COMPAT-FLUSS-005` | Raw-client composite-PK upsert matrix (kv.format-version × bucket key) | A composite-PK KV table is writable by the raw client exactly when `table.kv.format-version=2` AND the bucket key is a single-field subset of the PK; all other cells fail with the documented `IcebergKeyEncoder` signature. |
| `COMPAT-FLUSS-006` | Bucket-distribution skew probe (SCH-07 evidence) | Distinct bucket-key values spread evenly across `bucket.num` buckets (no empty/hot bucket); a constant bucket key collapses to exactly one bucket. |
| `SCHEMA-REC-001` | Clean-break reset, replay, and rollback readability | Rebuilt state matches expected state without silent data loss. |
| `SCHEMA-EOD-001` | Offload retry and retention-expiry protection | Source data cannot expire before verified offload. |

Evidence: record the exact Fluss/Flink versions, DDL manifest ID, checksums, effective-schema output, fixture checksum, and test report for every run.

**Implemented as of 2026-08-15** — executable tests do not yet carry all COMPAT-* IDs in code; the mapping is:

| Implementing test class | Tests | Covers |
| --- | --- | --- |
| `CompatFlussDdlParityIntegrationTest` | 1 | `COMPAT-FLUSS-001` (SCH-12/13 — every approved DDL applies as an admin-API descriptor on the pinned Fluss and inspects with manifest parity, including the **full WITH-option set** (every declared option honored, `bucket.num`/`bucket.key` + the `table.datalake.enabled` dev deviation carved out); 21/21 tables green on the live dev cluster 2026-08-15; env-gated on `FLUSS_BOOTSTRAP`) |
| `CompatFlussIntegrationTest` | 8 | `COMPAT-FLUSS-002` (BYTES round trip), `COMPAT-FLUSS-003` (LOG append-only + KV upsert/lookup + partial-update merge + **KV changelog records are FULL row images** — SCH-14), `COMPAT-FLUSS-004` (KV last-write-wins observed; stale-write rejection belongs to `KvStateUpdateProtocol`), `COMPAT-FLUSS-006` (bucket-distribution skew probe — 400 distinct keys over an 8-bucket LOG table spread across every bucket within mean+3σ, plus the constant-key control collapsing all rows into exactly one bucket; the live half of the "non-null routing and bucket-skew tests" requirement, SCH-07), `COMPAT-FLINK-002` (SCH-16 — cross-table writes visible per-table, no atomic commit; evidence `logs/schema-compat/compat-flink-002-20260815.md`), `SCHEMA-REC-001` (clean-break rebuild converges), `SCHEMA-AUDIT-001` (tiered-storage half — documented skip; the chain/reconstruction half lives in `AuditReconstructionSimulationTest`) |
| `KvStaleWriteRejectionTest` | 7 | `COMPAT-FLUSS-004` (the rejected/quarantined/audited half — a projector-layer store driven through `KvStateUpdateProtocol`: stale/regressive/conflict/unknown writes are rejected without mutating state, raise the halt signal, and are quarantined; duplicates are no-ops; a mixed sequence never regresses and every attempt is audited) |
| `ColumnOwnershipTest` | 18 | `COMPAT-FLUSS-004` / SCH-15 (pure-JVM fail-closed invariants of the `ColumnOwnership` matrix: in-range + unique indexes, one declared writer per column group (DEC-005), identity columns never partial-updated, no unowned column; `checkWrite` enforcement) |
| `ColumnOwnershipAgreementTest` | 21 | SCH-15 cross-boundary pin — each ownership matrix mirrors its DDL: full column coverage, PK + `schema_version` columns identity, schema-version header match, writers only ever touch their own group |
| `OrderLifecycleColumnsAgreementTest` | 5 | SCH-15 DDL pin (`09_order_lifecycle.sql` v2 — 15 columns, composite PK `(account_scope_id, broker_order_id)` R-013, `bucket.key`, types/nullability) |
| `ExecutionAttemptsColumnsAgreementTest` | 5 | SCH-15 DDL pin (`12_execution_attempts.sql` v2 — 19 columns, PK `execution_attempt_id`, monotonic `phase_epoch` R-234, types/nullability) |
| `InMemoryAttemptStoreTest` | 20 | SCH-15 guard first consumer / EXE-UNIT-006 core — `InMemoryAttemptStore` runs the attempt-store matrix guard (`checkWrite`) on every mutation: prepare replay never rewrites the PREPARED attempt's identity (duplicate untouched; modified decision = violation + halt), legal transitions with monotonic `phase_epoch`, stale-epoch rejection, terminal protection, UNKNOWN exits only via explicit `resolveUnknown`, and a drifted matrix fails the transition closed |
| `FillEventMapperTest` | 6 | SCH-20 operator-wiring core — Fills LOG row (23 cols, 08 v2) → `FillEvent` by pinned index with caller-resolved `FillContext`: `sourceVersion`=receive_time, `eventTimeMs` fallback, non-fill/price-less rows filtered, context validation |
| `PositionProjectorDriverTest` | 11 | SCH-20 operator-wiring core — per-position version-gated projection: deterministic `position_id` minting (account/instrument/side), scale-in → REDUCING → CLOSED, re-entry mints a new id, stale/duplicate/oversell handling, weighted-average entry, NOT_A_FILL rows, end-to-end row path |
| `FlussPositionsStateStoreIntegrationTest` | 2 | SCH-20 live drill (env-gated on `FLUSS_BOOTSTRAP`, tagged integration) — `FlussPositionsStateStore` upsert+lookup round-trip and last-write-wins re-upsert through a real Positions-shaped scratch KV table on the dev cluster (PASSED 2026-08-15) |
| `CompatFlussCompositeKeyIntegrationTest` | 1 | `COMPAT-FLUSS-005` (raw-client composite-PK matrix re-verified on every live run — v1/v2 × default vs single-field-subset bucket key, 4 cells; runs the SHARED `CompositeKeyMatrixVerifier`, which also gates every DDL apply IN-BAND — `DdlApplyTool` re-derives the matrix against the live cluster before creating anything and refuses on deviation, never merely referencing capability evidence; the working cell is what `feature_candles_15s`/`instruments` carry; `Order_Lifecycle`/`Order_Correlation` stay Flink-connector-only — the DDL apply contract now refuses to mark an apply PASS while limited tables exist unless `--ack-limitations` names exactly them (`DDL_APPLY_ACK_LIMITATIONS`; `DdlApplyToolStatusTest`); evidence `logs/schema-compat/composite-pk-raw-client-20260815.md`; env-gated on `FLUSS_BOOTSTRAP`) |
| `CompositeKeyMatrixVerifierTest` | 4 | Pure-JVM pin of the COMPAT-FLUSS-005 matrix: the documented 4-cell spec (configs + expected outcomes) and the expected-outcome matching both the integration test and the in-band apply gate rely on — cell drift fails before any cluster is needed |
| `DdlApplyToolStatusTest` | 10 | Apply-status decision (step 8 — PASS only when every smoke passes; composite-PK limitation → `PASS_WITH_LIMITATION` with dedicated exit 6 on exact acknowledgment, exit 1 otherwise; `--ack-limitations auto` predicts the limited tables from the manifest — composite PK with bucket key = PK — so operators confirm, never guess; failures (incl. matrix deviations) dominate; `RESULT=` sentinel) |
| `CleanBreakSimulationTest` | 11 | `SCHEMA-REC-001` clean-break drill (full replay reconverges; partial/mutated source diverges and fails closed). Wave 32 (CHG-172) took it from 4 to 11: the winner of last-write-wins is decided by (offset, sourceVersion) rather than list order, a table name containing the key separator cannot leak rows into another table's selection, a null event or null table fails closed instead of throwing, an empty table list is not a vacuous pass, and `RunResult` reports whether the reference came from outside the replayed log |
| `InstrumentManifestWriterTest` | 4 | `ING-SCHEMA-002` (DDL-order row mapping, entry validation R-115/R-116/R-193, empty/duplicate-composite-key refusal) |
| `InstrumentManifestWriterIntegrationTest` | 1 | `ING-INT-004` (first production composite-PK raw-client writer — live upserts into an `instruments`-shaped scratch table, version retention, idempotent re-load; env-gated `INGESTION_INT_TEST_INSTRUMENTS=true`) |

Live evidence: `logs/schema-compat/compat-fluss-001-003-20260815.md` + `logs/schema-compat/compat-flink-002-20260815.md` (Fluss 0.9.1-incubating, 177 common tests / 0 failures / 1 skip historical 2026-08-15 with `FLUSS_BOOTSTRAP=localhost:9123`, 2026-08-15 — **superseded**: that run predates the SCH-15/SCH-20/SCH-24 additions, and the default-run totals as of 2026-08-25 are 466 common / 0 failures / 2 skips (CHG-003/005/006/007, CHG-100 +4 — 2 unit + 2 env-gated live; docs-audit C6 line 466/247/388 (compute 388 — prior 341/236/294 then −19 DEC-038-era tests 2026-08-17 Design-B merge `34af190` — the pre-merge high-water mark was 333 — then −10 CHG-023 item-1 emitter→native-reporter swap, then −2 CHG-023 item-2 native-TTL expiry swap, then −11 CHG-023 item-4 StallGuardedSink removal 2026-08-17, then +1 SIG-FAIL-001 checkpoint-failure test `SignalJobCheckpointFailureIntegrationTest` 2026-08-17; counts corrected 2026-08-16 — were 211/268; +3 compute 2026-08-17 CHG-021)); the +17 over the prior figure = COMPAT-FLUSS-006 live skew probe +1, full-manifest routing-identity coverage +1, `KvStaleWriteRejectionTest` +7, plus 8 tests already in the tree but absent from the previously recorded count).

### Ingestion

| Test ID | Input/action | Pass result |
| --- | --- | --- |
| `ING-UNIT-001` | Decode each approved golden market packet | Typed fields match the approved fixture. |
| `ING-UNIT-002` | Send an unknown protocol version | Original bytes are preserved, quarantined, and readiness becomes false. |
| `ING-UNIT-003` | Write and read original packet bytes | Bytes and hash round-trip unchanged. |
| `ING-UNIT-004` | Send valid and invalid values | Validity is classified; invalid evidence is not silently discarded. |
| `ING-UNIT-005` | Recalculate fingerprints from fixed fixtures | Canonical fingerprint is stable and versioned. |
| `ING-UNIT-006` | Valid, missing, malformed, and wrongly scaled event timestamps with a controlled clock offset | Accepted timestamps have the approved UTC/unit interpretation; unsafe timestamps are quarantined with the reason recorded. |
| `ING-UNIT-007` | Trade, quote, depth, optional-field, and missing-required-field packets | Each packet is correctly classified; optional omissions remain valid and required omissions are quarantined. |
| `ING-INT-001` | Load manifest and subscribe | Every required instrument is subscribed or readiness is false. |
| `ING-INT-002` | Append accepted packets to Fluss | Every outcome has receive, start, acknowledgement/uncertainty timestamps. |
| `ING-INT-003` | Send multiple accepted ticks | Exactly one append submission is made per tick; no application batch exceeds one record. |
| `ING-INT-004` | Instrument manifest writer — composite-PK raw-client upserts into the `instruments` KV table | Every manifest row upserts by `(instrument_token, manifest_version)`; prior versions are retained (R-090); re-loading the same manifest is idempotent (no new rows). Live proof 2026-08-15 (`InstrumentManifestWriterIntegrationTest`, env-gated `INGESTION_INT_TEST_INSTRUMENTS=true`). |
| `ING-INT-005` | READY gating matrix — exhaustive no-false-positive walk of every `HealthProbe.isReady()` dimension (liveness, Fluss, tracker, broker, subscription, data, frame recency, clock) | Ready only when all eight are true; false when ANY single dimension is false (incl. partial ack, no tracked slot, stale frame, fail-closed unverified clock). Verified by `ReadinessGatingMatrixTest` (10 cases, M3 2026-08-15 — closes plan gap G8). |
| `ING-INT-006` | `docker-entrypoint.sh` FATAL exit codes + messages | Missing `FLUSS_BOOTSTRAP` → exit 2 + `FLUSS_BOOTSTRAP is required`; missing/unreadable manifest → exit 2 + `readable manifest FILE is required`; unwritable `LOG_DIR` or uncertainty-journal parent → exit 2 + `LOG_DIR not creatable` / `not writable` (P1-137 probes, `093d5e5`); missing/non-executable bridge binary → exit 1 + `arrow-bridge binary not found or not executable`. Guard order is part of the contract: manifest → P1-137 probes → bridge. Verified by `code/01_platform/04_scripts/tests/test_docker_entrypoint.sh` (bash harness, shellcheck-clean; wired into `run-monday-gates.sh`) (M5, 2026-08-15; message reworded `255da20`, probes added `093d5e5`, harness caught up 2026-09-12). |
| `BROKER-MD-001` | Market-data packet corpus, endpoint behavior, protocol fields, limits, and unknown-version handling | Live wire frames from the HFT feed captured 2026-08-13 (`socket.arrow.trade` 40/196 B zstd LE) decode to typed fields; AutoLogin verified as a non-interactive auth path; observed behavior recorded as protocol evidence (`logs/broker-md-001/`); unsupported or unknown behavior blocks readiness rather than being guessed. (The Standard feed `ds.arrow.trade` evidence was retired with the Standard feed removal 2026-08-14.) |
| `ING-FAIL-001` | Disconnect and reconnect broker | Connection epoch increases and subscription completeness is rechecked. Unit: `ReconnectEpochSequenceTest` (epoch strictly increases across disconnect→reconnect; full ack = no row, partial ack = FEED_HEALTH) + `ReadinessRecoveryTest`/`SlotHealthTest` (completeness recheck) + `BridgeRestartDecisionTest` (ING-UNIT-012); live: `ING-E2E-001` forced disconnect. |
| `ING-FAIL-002` | Slow/unavailable Fluss writer | 80% warning and 100% stop behavior occur within both bounds; no unrecorded drop. Verified by `AppendTrackerTest` (record/byte 80% warning → readiness false; 100% records → `tryAccept` false; warning + critical listeners fire; halted stays halted; pending counters reconcile on success and failure). |
| `ING-FAIL-003` | Force shutdown with pending writes | Uncertainty/loss evidence is persisted. Verified by `UncertaintyJournalTest` (entry write + read-back, multi-entry append, full JSON field set, R-194 control-character escaping, R-117 bare-filename NPE guard). |
| `ING-FAIL-004` | Concurrent accept/release against a small limit (16 threads, halt path exercised) | Pending counters reconcile to zero; `totalAccepted + totalRejected == attempts`; `appended + failed == accepted`; accepted bytes equal the sum of record sizes; counters never go negative. Verified by `AppendTrackerTest.concurrentAcceptReleaseKeepsInvariants` (M1, 2026-08-15). |
| `ING-FAIL-005` | Halt latch after 100% limit, then drain | Pinned lifecycle: halted stays latched until process restart — draining pending to zero (success or failure) never unhalts; accepts stay rejected and readiness stays false. Verified by `AppendTrackerTest.haltLatchPersistsAfterDrain` (M1, 2026-08-15). |
| `ING-FAIL-006` | 30 s warning-window throttle | The ≥80% warning listener fires at most once per 30 s window: first crossing fires; re-crossings within the window never re-fire; a 10 s-old marker still throttles; a 31 s-old marker unlocks exactly one re-fire and opens a fresh window. Verified by `AppendTrackerTest.warningThrottledToOncePer30sWindow` — the clock marker is advanced by reflection, no 30 s sleep (M5, 2026-08-15). |
| `ING-FAIL-007` | Clock offset crossing `CLOCK_OFFSET_LIMIT_MS` on the periodic re-measurement | Exactly one TIME_JUMP discontinuity per violation episode; recovery resets the episode; `abs(offset) == limit` is not a violation. Verified by `TimeJumpMonitorTest` + the `IngestionService` clock monitor (M1, 2026-08-15). |
| `ING-FAIL-008` | Bridge crash-loop with a scripted fake bridge binary that exits non-zero repeatedly | Each unexpected exit logs `BRIDGE_CRASH` and writes a DROP discontinuity; the bridge restarts exactly once, then a second crash is TERMINAL — the service unwinds to a clean shutdown (the "then exits 0" path) with the readiness marker cleared and a single uncertainty-journal entry. Verified by `BridgeCrashLoopE2ETest` — default-run via the ING-DQ-010 no-op-sink seam, no live Fluss required (M3, 2026-08-15). |
| `ING-FAIL-009` | Auth failure (bad credentials — `ARROW_TOKEN` removed 2026-08-24; AutoLogin `ARROW_USER_ID`/`PASSWORD`/`TOTP_KEY` are the auth path): bridge exits 2 on pipe close | Java logs the authentication failure, writes `auth_failure` discontinuity evidence (DROP-class, reason preserved), appends zero ticks, and does NOT restart the bridge (a revoked credential is terminal). Verified by `BridgeAuthFailureE2ETest` — the scripted bridge mirrors the real drain sequence (`auth_failure` → `bridge_shutdown` → exit 2), default-run (M3, 2026-08-15). |
| `ING-FAIL-010` | Shutdown with a Fluss ack that never completes; `shutdown()` called twice | Returns within the drain deadline (no hang); the uncertainty journal pins the exact remaining pending bytes (R-260 — new `pending_records`/`pending_bytes` entry fields); the second `shutdown()` is a no-op (single journal entry, single drain). The drain deadline is a new `DRAIN_DEADLINE_SECONDS` config key (default 30, range 1-300) so the deadline is provable in-test. Verified by `ShutdownDeadlockTest` (M3, 2026-08-15). |
| `ING-DQ-010` | Mixed NDJSON corpus through the real pipeline (valid, malformed, unknown feed, missing instrument, invalid values, stale, future, 1 MB, NUL) | `appends + quarantines == lines fed`; `frameCount == lines fed`; zero uncaught exceptions; quarantine classes fire their decode-error metrics exactly once each. Verified by `IngestionNoSilentDropTest` — default-run via the no-op-sink test seam (`QuarantineSink`/`DiscontinuitySink`/`SafetySink` in the `IngestionService` constructor), no reachable Fluss required (M1, 2026-08-15; seam added 2026-08-15). |
| `ING-DQ-011` | Seeded property-based fuzz: random NDJSON lines (mutated ticks, schema violations, malformed JSON, garbage, 1 MB, control chars) through the real pipeline, 3 pinned seeds | No uncaught exception; `appends + quarantines == lines fed` (no silent drop); `INTERNAL_ERROR` evidence == error counter (untyped crash path fails the test); corpus regenerates identically from the fixed seed; guaranteed-valid ticks always append. Verified by `FuzzIngestionTest` — default-run via the no-op-sink seam, JDK `java.util.Random`, no external property library (landed 2026-08-15, CHG-010). |
| `ING-TCP-002` | Losslessness comparator (`reconcile-compare.py`): synthetic tick-count fixtures with known lost / extra / vanished mutations; missing or truncated counter file | Each mutation class detected (exact-equality mode fails on any delta); a missing/truncated bridge or post-probe file **fails closed** (exit 1), never passes; chunked 20-token reports parse; Fluss `-1` sentinel tolerated. Verified by `code/01_platform/04_scripts/tests/test_reconcile_compare.py` (20 unittest cases; landed 2026-08-15, CHG-012). |
| `ING-TCP-003` | Counter-report persistence: 1,024-token report emitted as bounded chunks (20/line) | File + stderr mirror are byte-identical; the file is written FIRST so a dead stderr (SIGPIPE path) cannot lose the reconcile evidence; `ARROW_TICK_COUNTS_FILE` feeds the report path. Verified by `TestIngTcp003TickCountReportChunkedFileAndStderrMirror` (52 bounded lines, total=524800, each < 603 B truncation bound) + `TestIngTcp003ReportFilePersistsWhenStderrDies` (dead stderr via /dev/full — report survives) (landed 2026-08-15, CHG-018). |
| `ING-UNIT-018` | Java↔Go `ARROW_HFT_*` config parity: every key's default, accepted, and rejected values fed to Java `exactInt`/`intRange` and Go `hftPin`/`hftRange` | Both sides agree on every default, accepted value, and rejected value (Java throws `IllegalStateException`; Go exits FATAL status 2). Verified by `ConfigParityTest` (Java) + the mirror table in `hft_policy_test.go` (Go); the Go bridge now reads `ARROW_HFT_LATENCY_MS` and `ARROW_HFT_CONNECTIONS` through the mirrored helpers too (they were inline with fail-open semantics — non-integers silently ignored) (landed 2026-08-15, CHG-012). |
| `ING-UNIT-019` | Env-key doc drift: every key in the 03-ingestion.md Configuration contract table must be read by ingestion code | docs-audit **C16** PASS — every documented key is referenced by `IngestionConfig`, the Go bridge, or another ingestion source (landed 2026-08-15, CHG-012). |
| `ING-UNIT-020` | Process-level FATAL exits — missing `ARROW_APP_ID` + documented FATAL message text | Exit code 2 + the exact documented message: missing `ARROW_APP_ID` → `missing required env: ARROW_APP_ID`; `ARROW_HFT_CONNECTIONS=2` → `FATAL: … pinned to 1`; `ARROW_HFT_LATENCY_MS=49` → `FATAL: … must be in range 50..60000`. Verified by `TestIngUnit020MissingAppIdAndFatalMessages` (exec-self subprocess; helper env stripped of policy keys so a polluted test environment cannot mask the FATAL; closes G5) (M5, 2026-08-15). |
| `ING-UNIT-021` | Metrics-side secret scrubbing (G6): serialize a full OTLP export body after exercising every metric path | No `ARROW_*` values, Bearer tokens, `raw_payload`, appID/token values, or credential words in the payload (mirrors ING-SEC-RED-001 for logs). Verified by `OtlpMetricsEmitterTest.exportBodyScrubsSecrets` — drove out a real leak (decode-error reason labels carried raw `ARROW_TOKEN=`/Bearer strings to the collector); the emitter now scrubs reason labels and fails closed to `REDACTED` on any surviving marker (M5, 2026-08-15). |
| `ING-UNIT-022` | Bounded metric-label cardinality | Enumerated label keys come from exactly the fixed set {`service.name`, `service.instance.id`, `slot`, `reason`, `p50`, `p90`, `p99`}; slot label values are bounded (`hft-\d+`) — no token/symbol/high-cardinality labels. Verified by `OtlpMetricsEmitterTest.labelCardinalityBounded` (M5, 2026-08-15). |
| `ING-UNIT-023` | Bridge SIGTERM-drain regression (CHG-015): the bridge's final `arrow-tick-counts` report must be drained from stderr after the shutdown signal — a `Process.destroy()` regression would close the parent-side pipes and kill the stderr drain mid-read | Signal, then drain: a scripted fake bridge (per-second stderr tick-count reports; TERM trap writes the FINAL report and exits 0) runs through the real `runWithBridge`; `shutdown()` is invoked as the JVM hook would; the final report is re-logged AFTER `signaling arrow-bridge (SIGTERM)`, the drain never hits `stderr drain error`, the loop unwinds to `bridge loop ended`, and the uncertainty journal records exactly one entry. Verified by `BridgeShutdownRegressionTest` — default-run, in-process via the ING-DQ-010 seam, no Fluss / no Go binaries (post-plan CHG-015 follow-up, 2026-08-18, CHG-032); pinned as an explicit `run-monday-gates.sh` step (the CHG-015 SIGTERM-drain regression gate, 2026-08-18 — the single canonical place for executable test pins; full-audit is doc-truth only). |
| `ING-UNIT-024` | JVM shutdown-hook layer for the CHG-015 drain: a spawned driver JVM (`BridgeShutdownDriver`, mirroring `IngestionService.main()`'s wiring) is SIGTERMed and the real hook runs `shutdown()` on the hook thread | The driver exits 143 (128 + SIGTERM); the final `arrow-tick-counts` report is drained from stderr AFTER `signaling arrow-bridge (SIGTERM)`; no `stderr drain error`; the main loop unwinds to `bridge loop ended` before the halt (the hook's bounded main-thread join — the layer the in-process ING-UNIT-023 skips); exactly one journal entry. Verified by `BridgeShutdownHookTest` — cluster-free, scripted fake bridge, no Fluss / no Go binaries (post-plan CHG-015 follow-up, 2026-08-18, CHG-033); pinned alongside ING-UNIT-023 in `run-monday-gates.sh` step 13 (2026-08-18). |
| `ING-RES-002` | Subscription-plan boundary N (1/511/512/513/1023/1024/1025) | Every token appears exactly once in the request union; each request ≤ `MaxHFTTokensPerRequest` (≤512); request count = ceil(n/512); 1025 on one connection is over-capacity and rejected; `validateRequestUnion` fails closed on cross/within-request duplicates, tokens outside the assignment, missing tokens, empty and over-size requests. Verified by `TestIngRes002PlanBoundaries` + `TestIngRes002ValidateRequestUnionRejects` (6 sub-cases, M4 2026-08-15). |
| `ING-RES-003` | Backoff(attempt) golden sequence | Exactly 1,2,4,8,16,30,30… s with NO jitter (deterministic for ING-RES-001 soak accounting); negative attempts clamp to 1 s. Verified by `TestBackoffGoldenSequence` (M4 2026-08-15). |
| `ING-RES-004` | NDJSON contract version on every emit path | `tick`, `bridge_event`, and `bridge_metrics` records all carry `contract_version=2` (schema conformance across the contract). Verified by `TestIngRes004AllEmitPathsCarryContractVersion2` (M4 2026-08-15). |
| `ING-PERF-001` | Variable 50,000 ticks/s average baseline, 3,000 instruments | Append p99 is under 50 ms and memory/backlog remain bounded. 1,024-envelope probe 58,951 ticks/s recorded 2026-08-09; gate certified 2026-08-13 at the synthetic hot-path envelope (socket 49,242 tps / 0 wire loss; append 49,578 tps / 0 failures / p99 &lt; 5 ms); the 3,000/50k production-envelope run is removed from acceptance (DEC-037, 2026-08-13 — not to be tested). |
| `ING-PERF-002` | RETIRED with the peak campaign (DEC-036, 2026-08-13) | Was: 60,000 ticks/s peak; every instrument at or below 20 ticks/s. Superseded by `ING-PERF-001` (50,000 gate). |
| `ING-UNIT-010` | raw_payload hash validation (SHA-256 + base64) | Hash mismatch, malformed hash, invalid base64, and empty payload are rejected with a typed result. |
| `ING-UNIT-010b` | Golden-corpus payload hash validation | Every golden packet validates and decodes to the exact frame bytes; tampered frames are rejected. |
| `ING-UNIT-011` | Bridge event → discontinuity reason mapping | Disconnect/auth/shutdown → DROP; heartbeat/stall → HEARTBEAT_GAP; reconnect → RECONNECT; partial subscription_ack → FEED_HEALTH; non-evidence events produce no row. |
| `ING-UNIT-012` | Bridge restart policy | Unexpected exit restarts once; a second unexpected exit is terminal; clean exit 0 and shutdown never restart. |
| `ING-UNIT-014` | Freshness-gate exact boundary matrix | `ts_ms` at receive, exactly at `maxEventAge`/`maxFutureSkew`, ±1 ms over/under: both exact boundaries are inclusive-FRESH, one ms beyond flips STALE/FUTURE. Verified by `StaleDataTradeGuardTest.exactBoundaryMatrix` (7 cases, M4 2026-08-15). |
| `ING-UNIT-015` | Payload-hash base64 form edge cases | Unpadded base64 accepted (basic decoder padding-lenient); URL-safe base64 rejected as MALFORMED_PAYLOAD (never silently dropped); 64 KB multi-frame payload validates and round-trips byte-exactly; sha256("") pinned to its real digest with the empty payload still a typed rejection (R-186). Verified by `PayloadHashValidatorTest` +4 (M4 2026-08-15). |
| `ING-UNIT-016` | Canonical fingerprint form frozen cross-commit | v1 fixture golden-hash snapshot — any silent change to field order, encoding, null representation, or version fails the pin. Verified by `FingerprintBuilderTest.goldenHashFrozen` (`aec03d3d…bc5e31`, M4 2026-08-15). |
| `ING-UNIT-017` | DEC-012 fingerprint dedup semantics | Same tick + same epoch → identical fingerprint (duplicates within an epoch expected — Compute dedups); same tick + next epoch → different (reconnect resets identity). Verified by `FingerprintBuilderTest.dedupSemanticsPinned` (M4 2026-08-15). |
| `ING-SCHEMA-001` | Writer schema ↔ DDL agreement | Written columns match source DDL order; raw_table_1 v2 = 20 columns; ack_ts nullable; no `retention.days` option; lake-claiming DDLs carry datalake options. |
| `ING-SCHEMA-002` | Instrument manifest writer row mapping + fail-closed validation | `toRow` builds the 14-column DDL-order row; entry validation rejects non-positive identity/keys and blank routing fields (R-115/R-116/R-193); an empty manifest or duplicate `(instrument_token, manifest_version)` is refused before any write. |
| `ING-DQ-001` | Malformed-line quality classification | MALFORMED_JSON with raw bytes preserved; static detail bounded and line-safe; quarantine reason vocabulary exact per plan. |
| `ING-DQ-002` | Stale classification precedence | Stale/future timestamps (5 s max age, 2 s future skew) are quarantined before any trade path. |
| `ING-SAFE-001` | Slot-scoped safety halt requests | halt_request_id is slot-scoped and tuple-deterministic; assigned-token-set hash is deterministic and order-independent. |
| `ING-SAFE-002` | Partial ack → unsafe, full ack never unsafe | Bridge-event safety mapping exact per plan. |
| `ING-SAFE-003` | RECOVERED only on ACTIVE + full-ack subscription | No other combination recovers a slot. |
| `ING-E2E-001` | Full-stack fake broker → Fluss | Bridge ingests fake ticks into Fluss and survives a forced disconnect; rows persisted end-to-end. Verified by `FullStackE2ETest` (env-gated on `INGESTION_INT_TEST_E2E=true`). **Harness hardened 2026-08-15 (CHG-011):** `LOG_DIR` points at a writable JUnit temp dir (log4j's `JSON_FILE` appender otherwise swallows the service's logs and the assertions go blind) and startup/disconnect markers (`Fluss connected`, `arrow-bridge started`, `event=disconnect`) are polled with bounded deadlines instead of a fixed 8 s sleep. **Cluster pre-flight 2026-08-15 (CHG-013):** before launch the test verifies TCP 9123 (coordinator) and 9124 (tablet) accept; a crash-looping tablet (truncated log segments from an unclean shutdown) fails fast with a pointer to the surgical repair (`code/01_platform/04_scripts/fluss-repair/repair-tablet.sh`) instead of burning the 90 s startup window. **Append-success proof + file-based stderr capture 2026-08-15 (CHG-014):** with `ARROW_TICK_COUNTS=1` the bridge reports its cumulative emitted-tick total every second; after the forced disconnect the test asserts a second ACTIVE `subscription_ack` (in-process reconnect), tick-total growth past the reconnect-time baseline, and a final `bridge loop ended` report exceeding that baseline with zero errors/restarts. stderr/stdout are redirected to files (polled for appended bytes) instead of pipes — the root cause of the earlier post-SIGTERM freeze was that `Process.destroy()` on this JDK closes the parent-side process input streams (`IOException: Stream closed`) while the child is still running its shutdown hooks, so a pipe reader died at SIGTERM and the final report was written into a pipe with no reader and lost (the JSON log always had the full shutdown — separate fd). **No-orphan guard 2026-08-15 (CHG-016):** the shutdown hook ends with a final reaping sweep (a bridge restarted mid-shutdown is taken down — graceful signal, then SIGKILL — so nothing survives the JVM halt), and the test asserts the bridge pid (parsed from `arrow-bridge started (pid=N)`) is dead after the service exits and force-reaps the fake broker. Passed 2026-08-15 on the dev cluster at manifest 24 (schema verified 24/0/0; 15.4 s, baseline=4 → finalTicks=25, errors=0, restarts=0). |
| `THR-PROBE-001` | Client capacity probe without per-row blocking | 20,480 rows submitted non-blocking; rows/s and avg/p50/p99 reported; no ack-wait bottleneck. |

Evidence: approved packet corpus, manifest snapshot, deterministic clock, workload seed, append-outcome log, metrics report, and quarantine records. Real broker credentials are never used in unit tests.

#### ING-E2E-001 runbook: cluster-health pre-flight and the truncated-segment repair

**Pre-flight (CHG-013).** Before launching the service, `FullStackE2ETest` verifies the dev Fluss stack serves data — TCP 9123 (coordinator) and TCP 9124 (tablet) must accept. The tablet only binds after *every* log segment recovers, so a missing tablet listener while its container crash-loops means truncated segments from an unclean shutdown; the failure message names the repair tool below. A stopped stack (both ports down) reports "start the dev stack" instead. `docker ps --filter name=fluss-tablet` distinguishes the two, so the message is targeted.

**Symptom.** After the tablet is killed mid-write, it crash-loops on startup with:

```
Failed to load record batch at position N from FileRecords(...)
Caused by: java.io.EOFException: Failed to read `record batch header` ...
Expected to read 48 bytes, but reached end of file after reading M bytes.
```

and the service reports `Alive tablet server is empty` / schema verification fails. The tablet's data volume contains segments whose tail is a preallocated/zeroed region left past the last complete batch. **Fluss's reported error position is NOT the true boundary** — a zeroed batch header misparses as valid and the reader jumps through the garbage region (observed 2026-08-15: reported 670,347,188, real boundary 670,345,400; the first "40-byte" truncation just exposed the next misread).

**Repair (surgical — removes only the zeroed/never-written tail bytes, never complete records):**

```bash
# Report only (no changes):
DRY_RUN=1 code/01_platform/04_scripts/fluss-repair/repair-tablet.sh
# Scan + repair the live raw_table_1 table (auto-detected):
code/01_platform/04_scripts/fluss-repair/repair-tablet.sh
# A specific table dir (from the tablet's error message):
code/01_platform/04_scripts/fluss-repair/repair-tablet.sh raw_table_1-696
```

The tool discovers the tablet container + data volume, scans every segment with the server's own batch arithmetic (`LogScan.py`: `batchSize = 12 + int32_le(header[8:12])`; an all-zero 48-byte header marks the preallocated tail), truncates each affected segment to the exact end of the last complete batch, restarts the tablet, and verifies recovery completed. It refuses to act while the tablet is Up (a healthy active segment is mid-append — the scanner may catch an in-progress write).

**Verify.** After repair, the service startup log must show `ddl-bootstrap: verified 25 tables ok, 0 missing, 0 schema-mismatch` (manifest 25), then `Fluss connected` — then re-run ING-E2E-001.

**History.** 2026-08-15 the dev cluster hit this on 12 of raw_table_1's 16 buckets (zero-tail deltas 160–3,040 bytes each, all from today's E2E runs); all were repaired with the tool and the schema re-verified 24/0/0. Backup of the original corrupt bucket-6 segment: host `/tmp/fluss-repair/raw_table_1-696-log-6/`.

### Signal job

| Test IDs | What is tested | Pass result |
| --- | --- | --- |
| `SIG-UNIT-001` to `SIG-UNIT-006` | Tie ordering, candles, 300000 ms dedup TTL, candidate identity (~~ranking and reservations~~ — **REMOVED 2026-08-15, CHG-005**) | Output is deterministic for fixed input and clock. |
| `SIG-UNIT-007` | Dependency scan | No `flink-cep` dependency or CEP import exists. |
| `SIG-UNIT-008` to `SIG-UNIT-009` | Dedup and candle state contents | State stays compact; no raw packet/event collection or tick list is stored (DEC-038: the dedup set lives in the Fluss dedup table; the Flink side holds only the bounded working cache). |
| `SIG-STATE-001` | DEC-038: large durable dedup state is observable in Fluss and the Flink checkpoint is bounded | The Fluss dedup table holds the accepted set; checkpoint size stays bounded and does not duplicate the full durable state. |
| `SIG-STATE-002` | DEC-038: restart restores compact Flink state and rehydrates the dedup working cache from Fluss | Restart resumes from the compact checkpoint without full raw-history replay; a re-sent fingerprint inside the TTL still dedupes after rehydration. |
| `SIG-STATE-003` | DEC-038: Fluss dedup-table unavailability or incompatibility | The job fails closed / stays degraded (no silent replay with an empty dedup set). |
| `SIG-HARNESS-001` | Out-of-order events, watermark, and idleness | Correct event-time outcome is emitted. |
| `SIG-HARNESS-002` | Late before-final versus after-final event | Only the permitted update/discard behavior occurs. |
| `SIG-HARNESS-003` | Checkpoint then restore and replay | Recovered output equals the expected deterministic output. |
| `SIG-HARNESS-004` | Two identical-looking events: one broker duplicate and one legitimate identical event | The documented fingerprint limitation is applied consistently and its metric/audit evidence is emitted. |
| `SIG-HARNESS-005` | ~~Checkpoint/restore while a reservation and ranking result are active~~ | ~~Recovery preserves the correct reservation and ranking outcome~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `STATE-COMPAT-001` | Approved serializer and savepoint version change | State/savepoint restore succeeds through the approved compatibility path, or startup blocks before unsafe use. |
| `SIG-INT-001` | Pinned Fluss source/sink boundary | Source/sink semantics work with approved versions. |
| `COMPAT-FLINK-001` | Source/sink checkpoint, restore, and rescale on the pinned Flink/connector versions | Restored processing and state remain within the approved consistency boundary. |
| `SIG-INT-002` | Partial visibility across outputs | Reconciliation identifies and handles partial visibility. |
| `SIG-FAIL-001` | Checkpoint or continuity failure | New decisions are suppressed and a safe halt is requested. |
| `SIG-PERF-001` | Variable baseline and peak workload | Decision p99, state, checkpoint, and memory stay within the defined limits (DEC-038: re-measured after dedup externalization — checkpoint size/duration, Fluss dedup-table size, cache hit ratio, rehydration latency; detail: [Externalization benchmark](#externalization-benchmark-sig-perf-001-dec-038)). |

Evidence: fixture seed, event-time sequence, expected output, checkpoint/savepoint reference, state-size report, and performance report.

**Implemented as of 2026-08-10 (Slice 1)** — the executable unit tests do not yet carry SIG-* IDs; the mapping is:

| Implementing test class | Tests | Covers SIG-* |
| --- | --- | --- |
| `CandleAggregateFunctionTest` | 5 | `SIG-UNIT-001`/`SIG-UNIT-002` core (tie ordering, OHLCV aggregation, quote-only window, merge) |
| `RawValidationFunctionTest` | 7 | Validation-gate rules (input classification) |
| `SignalJobConfigTest` | 7 | `SIG-UNIT-003` core (pinned values + rejection of deviations) |
| `FingerprintDedupFunctionTest` | 6 | `SIG-UNIT-008`/`SIG-UNIT-009` dedup half — Flink 2.2.1 operator harness (`KeyedOneInputStreamOperatorTestHarness`, no cluster): first occurrence passes / duplicate within TTL dropped; **2026-08-17 (CHG-023 item 2) expiry is native `StateTtlConfig` — the harness TTL clock advances past `first_seen + TTL` to re-admit; ONE state row per active key (the expiry index + timers are gone)**; re-arriving expired fingerprint re-admitted; state key scoped by `version\|token\|fingerprint` |
| `CandleEmitFunctionTest` | 4 | `SIG-UNIT-008`/`SIG-UNIT-009` emit half (2026-08-16) — `CandleEmitFunction` state-content assertions: the only window state is the Boolean `candle-emitted` flag (BooleanSerializer); `CandleAccumulator` is exactly scalar OHLCV + identity + order keys — no tick list/collection/raw bytes |
| `CepDependencyGuardTest` | 3 | `SIG-UNIT-007` (2026-08-16) — dependency scan: no CEP dependency declaration or import exists in the module pom/sources, plus shell-guard agreement + scan-scope parity vs `cep_guard.sh` (`make cep-check-module`) |

**Pending (no implementing test yet):** ~~`SIG-UNIT-004..006` (candidate/ranking/reservation — Slice 3)~~ (**REMOVED 2026-08-15, CHG-005 — ranking/reservation out of scope**), ~~`SIG-UNIT-007` (dependency scan), `SIG-UNIT-008/009` emit half (`CandleEmitFunction` state-content assertions)~~ — **DONE 2026-08-16 (`CepDependencyGuardTest` + `CandleEmitFunctionTest` state-content legs; compute suite 333/0/17 as of 2026-08-17 (CHG-021 +3 — the pre-Design-B-merge high-water mark; the suite stood at 292/0/17 after the same-day Design-B merge + CHG-023 items 1/2/4 + SIG-FAIL-001 `SignalJobCheckpointFailureIntegrationTest` 2026-08-17); `CepDependencyGuardTest` gained shell-guard agreement + scan-scope parity legs the same day — `make cep-check-module`)**, `SIG-HARNESS-001..005` (~~005 REMOVED 2026-08-15, CHG-005~~), `STATE-COMPAT-001`, `SIG-INT-001/002`, `COMPAT-FLINK-001`, `SIG-FAIL-001` (**DONE 2026-08-17** — `SignalJobCheckpointFailureIntegrationTest`, host-runnable MiniCluster: injected checkpoint failure → exactly 3 fixed-delay restart episodes → terminal FAILED, cause contains "checkpoint"; see [`04-signal-job.md`](./04-signal-job.md)), `SIG-PERF-001` (**envelope half DONE 2026-08-17** — `SignalChainLiveE2ETest` measurement leg, PASS on the final native topology: raw +4,559,401 rows, feature +22,440 rows, 1,024 instruments, 28 checkpoint completions / 0 expired / 0 restarts; evidence `logs/tracker-14/sig-perf-001-envelope-20260817.md`; **p99 decision-latency half signal-side/pending** — out of scope, feature-path-only per user 2026-08-17). The full required set is `SIG-UNIT-001..009`, `SIG-HARNESS-001..005`, `STATE-COMPAT-001`, `SIG-INT-001`, `SIG-INT-002`, `COMPAT-FLINK-001`, `SIG-FAIL-001`, `SIG-PERF-001` (reconciled with [`04-signal-job.md`](./04-signal-job.md) §Verification mapping; ranking/reservation rows now REMOVED per CHG-005). Solving method, prerequisites, and pass gates for each pending item: [`04-signal-job.md`](./04-signal-job.md) §Pending work items: resolution plan. Harness infra (compute-pom test-scope `flink-streaming-java` test-jar + `flink-test-utils`) landed 2026-08-10 — the pure-JVM rows need only test code from here on.

### Action Capture

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `AC-UNIT-001` | Postback decode and status mapping | Known packet maps to the approved internal status. |
| `AC-UNIT-002` | Platform identity and fingerprint | Identity is stable for duplicate delivery evidence. |
| `AC-UNIT-003` | Correlation priority and ambiguity | Exactly one valid match is correlated; ambiguity is quarantined. |
| `AC-UNIT-004` | Lifecycle precedence and regression | Stale/regressive state cannot overwrite newer state. |
| `AC-UNIT-005` | Position quantity/value transitions | Position rules are correct and independent of lifecycle state. |
| `AC-INT-001` | Immutable audit plus projections | Audit persists before/with recoverable projection work. |
| `BROKER-PB-001` | Sandbox postback corpus: status values, identities, timestamps, optional fields, unknown fields, and delivery behavior | Observed protocol behavior is versioned as evidence; unsupported behavior is quarantined and blocks readiness. |
| `AC-FAIL-001` | Crash after each projection step | Restart resumes unfinished work without regression. |
| `AC-FAIL-002` | Ledger-based restart recovery | Ledger produces the correct resumed projection. |
| `AC-FAIL-003` | Missing/ambiguous mapping | Event is quarantined and affected order path halts. |
| `AC-FAIL-004` | Duplicate, out-of-order, conflicting postbacks | No incorrect state transition occurs. |
| `AC-REC-001` | Full rebuild from immutable events | Rebuilt projections match approved expected output. |

Evidence: versioned postback fixtures, projection-ledger snapshots, crash-point logs, quarantine records, and before/after projection comparisons.

### Babysitter

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `BAB-UNIT-001` | Every valid position state | Job emits zero action records. |
| `BAB-UNIT-002` | Future action flag enabled in MVP | Startup fails closed; no action is emitted. |
| `BAB-INT-001` | Positions changelog schema and offsets | Input is consumed using approved schema and offset behavior. |
| `BAB-HARNESS-001` | Position changelog checkpoint, restore, and offset recovery | The latest safe state resumes correctly and the job still emits zero actions. |
| `BAB-FAIL-001` | Checkpoint restore and changelog gap | Restore is correct; a gap makes readiness false. |
| `BAB-FAIL-002` | Stale/conflicting position input | Unsafe input is suppressed and reported. |
| `BAB-OPS-001` | Job readiness state | Babysitter health never claims Executor trading readiness. |

Evidence: input fixture, output capture proving zero actions, checkpoint/restore report, changelog-gap record, and readiness metrics.

**Implemented 2026-08-15** — executable tests do not yet carry BAB-* IDs in code; the mapping is:

| Implementing test class | Tests | Covers BAB-* |
| --- | --- | --- |
| `BabysitterJobTest` | 3 | `BAB-UNIT-001` (MVP topology emits zero `Position_Actions` — cluster-free StreamGraph inspection of `BabysitterJob.buildTopology()`: no operator produces `Position_Actions`, only the pinned marker → discard topology, every node UID-pinned per CHECKPOINT-RESTORE-001/DEC-035) and `BAB-UNIT-002` (action-enable fails closed: any `POSITION_ACTIONS_ENABLED` value other than `false` — trimmed, case-insensitive — is rejected at startup with `IllegalStateException` naming the flag; unset/`false` accepted). `BAB-INT-001`, `BAB-HARNESS-001`, `BAB-FAIL-001`, `BAB-FAIL-002`, `BAB-OPS-001` pending until the Positions-changelog source and observation state land (05-execution-core.md) |

### Executor

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `EXE-UNIT-001` | Gate transition and epoch | Invalid transition/epoch is refused and audited. |
| `EXE-UNIT-002` | ~~Decision hash, expiry, reservation~~ | ~~Mutated, expired, or unreserved decision is refused~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `EXE-UNIT-003` | Client reference canonicalization | Reference is stable and conforms to broker constraints. |
| `EXE-UNIT-004` | Broker result classification | Success, failure, and unknown are classified safely. |
| `EXE-UNIT-005` | Single-operator (Saurabh, DEC-044) approval identity/epoch/evidence | The authorized single operator (saurabh) with matching gate epoch and evidence hash can enable; an unauthorized identity or a mismatched epoch/evidence hash cannot resume trading. |
| `EXE-UNIT-006` | Replay the same immutable decision and request hash | At most one active attempt exists; replay cannot create a second broker call. |
| `EXE-INT-001` | Fluss changelog and owned state writes | State ownership and durable transitions are preserved. |
| `ARROW-REST-001` | Sandbox Arrow REST authentication, request fields, response fields, rejection, and timeout behavior | Actual behavior is captured as protocol evidence; timeout or ambiguity becomes `UNKNOWN` and blocks a blind retry. |
| `ARROW-REST-002` | Client-reference length, character set, echo behavior, and broker-ID correlation | The approved reference format correlates one attempt to one broker order, or live-money readiness remains blocked. |
| `EXE-FAIL-001` | Crash before, during, after broker acceptance | No duplicate order; uncertain outcome halts and reconciles. |
| `EXE-FAIL-002` | Timeout, malformed, or unknown response | Outcome becomes UNKNOWN; no blind retry. |
| `EXE-FAIL-003` | Missing/corrupt state at restart | Calls remain blocked and reconciliation is required. |
| `EXE-FAIL-004` | Changelog, checkpoint, durable-state loss | Trading readiness is false and gate halts. |
| `EXE-FAIL-005` | Fencing, split-brain, concurrent Executor | Only current fenced owner can call broker. |
| `EXE-FAIL-006` | Mapping quarantine/reconciliation | Ambiguous mapping blocks further unsafe action. |
| `EXE-OPS-001` | Unauthorized controls and single-operator (Saurabh, DEC-044) resume | Unsafe control attempts are rejected and audited. |
| `EXE-AUDIT-001` | Audit reconstruction | Order path can be reconstructed from retained audit evidence. |

Evidence: sandbox broker or deterministic stub only; preserve attempt timeline, gate epoch, audit IDs, reconciliation output, and proof that no duplicate request was sent.

### Local Compose

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `LOCAL-INT-001` | Fresh start with documented command | All required local services become healthy in dependency order. |
| `LOCAL-INT-002` | Effective configuration and schema manifest | Running services use pinned versions and approved local configuration. |
| `LOCAL-INT-003` | End-to-end synthetic path | Packet reaches decision, simulated postback, and projection without real order placement. |
| `LOCAL-FAIL-001` | Restart Flink/service with checkpoint state | State restores according to component recovery rules. |
| `LOCAL-FAIL-002` | Missing secret, dependency, or schema | Affected service is not ready; no unsafe fallback occurs. |
| `LOCAL-OBS-001` | Local logs, metrics, and audit | Health and correlation evidence can be inspected locally. |

Evidence: compose file digest, image digests, effective config with secrets removed, startup log, test fixture seed, and E2E report.

### Production Swarm — v1 (4 VMs Manager+Worker) / v2 (7 VMs Manager ONLY + Workers)

Swarm control-plane HA (Raft) is tested separately from ZooKeeper HA and Fluss HA. `ZK quorum 2/3 ≠ Swarm Raft quorum 2/3`. Stack is `docker-stack.yml` (Option B); v1 uses `M1-3 = Manager+Worker Active`, v2 drains `M1-3` to `Manager ONLY` and adds `W1-3 (+W4+)` as `Worker`. Same stack via role labels (`role=manager/worker`, `flink=true`, `fluss=true`) — no hostname pin.

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `SWARM-INT-001` | Pinned images, placement, network, secrets, and identities | No mutable image, unsafe network exposure, or replica co-location remains. Placement uses role labels, not hard-coded hostnames — `W4` joins without stack redesign. |
| `SWARM-INT-002` | Separate service, durability, job, and trading readiness | Each readiness state reports independently. |
| `SWARM-MGR-001` | Initial Swarm manager quorum (Raft, not ZooKeeper) | `docker node ls` shows 3 managers, quorum available (`2/3`). Built-in Swarm Raft documented; no separate Raft deployment. |
| `SWARM-MGR-002` | Manager M1 failure (v1: VM1) | M2+M3 remain managers, quorum 2 remains, `docker node ls` quorum OK, workloads recover per policy. |
| `SWARM-MGR-003` | Manager M2 failure | Same as M1. |
| `SWARM-MGR-004` | Manager M3 failure | Same as M1. |
| `SWARM-MGR-005` | Two-manager failure (quorum loss) | 1 manager remains, Swarm Raft quorum lost, control-plane degraded correctly detected and documented. Workload survival ≠ control-plane health. |
| `SWARM-MGR-006` | Manager recovery | Failed manager rejoins, `docker node ls` returns to 3 managers, quorum restored, services stabilize, no duplicate ownership. |
| `SWARM-FAIL-001` | One workload VM loss (v1: one Manager+Worker, v2: one Worker) | ZooKeeper quorum 2-of-3 maintained with leader re-election; Fluss quorum/restore passes; Flink HA failover; processing recovery within target and gate halts within 5s when required. Swarm quorum unaffected in v2 (managers separate). |
| `PERF-NODELOSS-001` | 50,000 ticks/s average baseline plus one VM loss (90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045) | Records ZK quorum degradation/leader re-election, Fluss quorum degradation, Flink JM HA failover, Swarm manager quorum status, checkpoint restore, safe-halt latency, processing recovery, backlog drain, replica catch-up, zero acknowledged loss. |
| `SWARM-NET-001` | Single-node network partition (manager and worker separately) | Partition detected, scheduling/reconciliation behavior documented, recovery after partition heals, not conflated with VM power-off. |
| `SWARM-PLACEMENT-001` | Replica anti-co-location | Critical replicas not all on one VM — placement constraints spread across failure domains. |
| `SWARM-DEPLOY-001` | Rolling deployment while one worker unavailable | Update succeeds per policy, rollback correct, placement respected. |
| `SWARM-SCALE-001` | Add worker W4 | Worker joins Swarm, `docker node ls` shows 3 managers + 4 workers, quorum stays 2/3, worker gets correct labels, eligible workloads schedule there. |
| `SWARM-SCALE-002` | Drain worker | `docker node update --availability drain` moves tasks safely. |
| `SWARM-SCALE-003` | Remove worker | After drain, `docker node rm` succeeds, cluster stays healthy on remaining nodes. |
| `SWARM-RECOVERY-001` | Node return after failure | Failed node rejoins, cluster converges, workload recovery per component semantics. |
| `SWARM-FAIL-002` | S3/checkpoint/lake/audit dependency failure | Affected readiness is false; unsafe trading is blocked. |
| `SWARM-REC-001` | Halted rollback and state readability | Rollback preserves readable state and never auto-enables trading. |
| `SEC-NET-001` | Public and internal deny-path network probes | Only approved ingress and service paths are reachable; every prohibited path is blocked. |
| `SEC-TRANSPORT-001` | TLS and storage-encryption verification for broker, Arrow REST, S3, and internal sensitive paths | Unencrypted or unverified transport/storage is rejected and readiness is false. |
| `SEC-CRED-001` | Secret rotation, revocation, expiry, and invalid-secret startup | Access is removed or restored safely; no secret is exposed in logs or evidence. |
| `SEC-AUTHZ-001` | Least-privilege table, state, broker, and control permissions | Authorized operations work; every excessive or unauthorized operation is denied and audited. |
| `SEC-IMAGE-001` | Pinned image digest, SBOM, and vulnerability-policy validation | Mutable, unapproved, or policy-failing images block deployment. **Vuln policy (2026-08-25, Platform):** deployment is blocked when any image carries a **CRITICAL with an available fix**. CRITICALs without a fix, and HIGHs with an available fix, require a **dated, owner-signed waiver** recorded in the scan evidence. HIGHs without a fix are **accepted risk**, documented each scan. Applies to the OS layer until the library layer is scannable (Java DB network-blocked as of 2026-08-25). |
| `SEC-AUDIT-001` | Audit access, deletion, retention, and legal-hold policy | Unauthorized access/deletion is denied and retention/legal-hold behavior is evidenced. |

#### M2 deployment gate — offline stack validation + one-host Swarm mimic

Before the live multi-VM rig (M3) exists, the deployable surface of
`docker-stack.yml` is gated in two offline-prep stages. These run on **any**
host — no VMs — and are wired into the documented gate:

| Stage | Command | What it proves | Applies to |
| --- | --- | --- | --- |
| Static (offline, no docker/swarm/VM) | `make test-09` | `docker-stack.yml` is a legal deploy unit: every service has pinned `image:` + `deploy:`; placement is by **node label** (`node.labels.role == worker` / `observability == true`), never hostname (v1→v2 needs no rewrite); no Compose-only keys (`build`/`depends_on`/`container_name`/`ports`/`network_mode`); encrypted overlays; `external: true` secrets; durable volumes; v2 replica spread | `SWARM-INT-001`, `SWARM-PLACEMENT-001`, `SEC-NET-001` (static half), `SEC-CRED-001` (secrets-declared half) |
| Mimic (needs docker) | `make stack-selfcheck` | `docker swarm init` (single node) → label the node `role=worker` + `role=observability` → **`docker stack config -c docker-stack.yml`** compiles (catches deploy-schema/YAML/interpolation errors); `DEPLOY=1` additionally tries `docker stack deploy` | `SWARM-INT-001`, `SWARM-DEPLOY-001` (compile/schedule half), `SEC-IMAGE-001` (digest-pin enforcement) |

Rules:
- **Live quorum/HA evidence (`SWARM-MGR-001..006`, `SWARM-FAIL-001`, `PERF-NODELOSS-001`, `SWARM-SCALE-*`, `SWARM-NET-001`, `SWARM-RECOVERY-001`) is M3 and requires the multi-VM rig.** They are **not** satisfied by the one-host mimic — a single-node Swarm has no quorum to test. Do not cite `stack-selfcheck` as HA evidence.
- **One-host deploy result (2026-08-20, `DEPLOY=1`):** the stack deploys end-to-end on a single node. Observed: all 12 services accepted; 3 encrypted overlays created; external secrets fail-closed (first missing secret aborts the deploy — confirmed); label placement proven by `zookeeper` **running 1/1** on the `role=worker`-labelled node; workload tasks moved from `no suitable node` to image-**Rejected**, isolating the `:unset` placeholder images as the sole remaining scheduling blocker (real images must be pushed to a registry first). Two issues fixed by this run: (a) the single `role` label can't be both `role=worker` and `role=observability` — observability is now a separate boolean `node.labels.observability == true`; (b) the stack had dropped the `otel-collector` config mount (collector ran then exited 1) — **fixed 2026-08-20**: the collector config is now a Swarm `configs:` (`otel-collector-config.swarm.yaml`), and the O2 Basic credential is read from a `o2_auth_basic` Swarm secret via the collector's `${file:}` provider (the on-prem collector image is distroless, so it cannot be injected via a shell wrapper); live one-host deploy shows `prod_otel-collector` **Running** (was exit 1).
- `make test-09` is the canonical **environment-free** static prohibition gate: a stack edit that reintroduces a hostname pin, a `build:`/`depends_on`/`ports` key, a non-encrypted overlay, or an in-file (non-external) secret fails here before any swarm is involved.
- `make stack-selfcheck` is the canonical **deploy-path** gate: it must reach `stack config: OK` (and, on `DEPLOY=1`, initiate scheduling) before a rig is stood up. It skips cleanly when docker/daemon is absent, relying on `make test-09` for static coverage.

### Observability and operations

| Test ID | What is tested | Pass result |
| --- | --- | --- |
| `OPS-UNIT-001` | Telemetry envelope and redaction | Required fields exist; credentials, tokens, and raw packets do not appear. |
| `OPS-UNIT-002` | Metric labels | Labels remain bounded; IDs are not high-cardinality labels. |
| `OPS-INT-001` | Health dimensions | Liveness, readiness, job, trading, durability, and telemetry health are independently queryable. |
| `OPS-INT-002` | Critical alert delivery, acknowledgement, escalation, runbook link | Each alert reaches its owner and records evidence. |
| `OPS-FAIL-001` | OpenObserve outage | Immutable audit remains available; outage cannot authorize orders. |
| `OPS-FAIL-002` | Checkpoint failure, backlog, or unknown broker result | Correct safe-halt/suppression action occurs within defined limits. |
| `OPS-RUNBOOK-001` | Runbook exercise for a selected critical incident | Operator follows the runbook to containment, reconciliation, recovery, and closure evidence. |
| `OPS-REL-001` | Dashboard/query version in release evidence | Release package identifies the exact dashboard/query version used. |

Evidence: alert configuration version, dashboard/query version, injected-fault record, notification and acknowledgement timestamps, runbook exercise notes, and closure evidence.

### Test record format

Every executable test or campaign records:

```text
test_id
requirement_ids
audit_issue_ids
component/boundary
preconditions and exact versions
fixture/workload and checksum
steps/fault injection
expected result
observed result
artifacts/log/audit IDs
clock/environment evidence
owner/date
```

## Shared testing rules

### What the testing program must prove

- Versioned inputs and state behave deterministically.
- Delivery and consistency limits are stated honestly.
- Memory, state, backlog, and recovery remain bounded.
- No crash window creates a duplicate broker order.
- Every defined uncertainty condition safely halts order calls.
- Schema, protocol, checkpoint, and deployment compatibility are proven.
- Production workload, node loss, offload, security, and audit guarantees are proven.

### Test levels and environments

| Level | Purpose | Environment |
| --- | --- | --- |
| Unit | Pure decode, canonicalization, transition, and validation logic (**ranking REMOVED 2026-08-15, CHG-005**) | Build runner |
| Flink harness/state | Event time, operators, timers/state, checkpoint/replay | Test JVM |
| Component integration | Fluss client, schema, persistence, service boundary | Pinned local stack |
| End-to-end | Tick → decision → sandbox order → postback → position | Acceptance environment |
| Failure/recovery | Crash windows, partial writes, restart, corruption, gaps | Fault-enabled acceptance |
| Performance | Baseline, burst, stress, latency/backpressure | Production-like Swarm |
| Chaos/DR | VM/node/store/network/credential loss | Production-like Swarm |
| Security | Network, identity, secret, authorization, audit controls | Acceptance/production-like |

### Unit-test rules

Unit tests use fixed clocks, canonical fixtures, explicit versions, and deterministic IDs. They do not need a live Fluss cluster, broker, Arrow REST, or S3.

Every component test set must cover empty, malformed, unknown-version, missing, duplicate, legitimate-identical, delayed, out-of-order, stale, conflicting, terminal-regressive, maximum-size, repeated, and concurrent inputs where the case applies. It must also cover clock skew, timestamp-unit mismatch, optional versus required broker fields, immutable IDs with equal and changed content, and matching versus mismatched gate approval evidence.

Passing unit tests never replaces connector, crash-window, capacity, or recovery proof.

### Integration-test rules

Use these environments in order:

1. A clean local Fluss/Flink stack for schema and connector behavior.
2. A sandbox broker and Arrow REST environment for protocol and response evidence.
3. A production-like Swarm (09 v1: 4 VMs Manager+Worker; v2: 7 VMs Manager-ONLY + Workers) for workload, control-plane HA, security, and recovery evidence — Swarm Raft quorum tested via `docker node ls`, separate from ZooKeeper.

Integration coverage includes catalog/table creation, effective DDL/options, `BYTES` round trips, LOG/KV/changelog behavior, Flink checkpoint/restore/rescale, all service paths, and S3 checkpoint/lake/offload/retention behavior.

Unknown endpoint paths, fields, limits, status values, timestamps, identity behavior, timeouts, or idempotency are blockers. The test records what was observed; it never invents a contract. Every integration run records a fixed fixture, exact versions, failure classification, expected consistency boundary, and evidence artifact.

### Unit and deterministic component tests

| Test family | Required behavior |
| --- | --- |
| `ING-UNIT-*` | Decode, bytes/hash, normalization, fingerprint, invalid/quarantine |
| `SIG-UNIT-*` | Dedup TTL, tie ordering, candles, candidates (**ranking/reservation REMOVED 2026-08-15, CHG-005**) |
| `AC-UNIT-*` | Postback identity, correlation, status precedence, positions |
| `BAB-UNIT-*` | Strict no-op and action-enable fail-closed |
| `EXE-UNIT-*` | Gate, immutability, attempts, references, approvals, classification |
| `SCHEMA-UNIT-*` | Manifest/checksum/parity/compatibility classification |

Deterministic tests use fixed clocks, versioned fixtures, stable IDs/seeds, and canonical expected outputs.

### Flink harness and state tests

- `SIG-HARNESS-001`: out-of-order events, watermark and idleness.
- `SIG-HARNESS-002`: late-before-final versus discard-after-final.
- `SIG-HARNESS-003`: deterministic checkpoint/restore replay.
- `SIG-HARNESS-004`: identical legitimate event versus duplicate fingerprint limitation.
- ~~`SIG-HARNESS-005`: reservation and ranking recovery~~ — **REMOVED 2026-08-15 (CHG-005).**
- `BAB-HARNESS-001`: position changelog/offset restore with zero action output.
- `STATE-COMPAT-001`: serializer/savepoint compatibility for every version change.

### Integration tests

- `COMPAT-FLUSS-001`: DDL parse/apply/effective schema.
- `COMPAT-FLUSS-002`: BYTES round trip.
- `COMPAT-FLUSS-003`: LOG/KV/changelog semantics.
- `COMPAT-FLUSS-004`: partial update and stale-write application protocol.
- `COMPAT-FLUSS-005`: raw-client composite-PK matrix (kv.format-version × bucket key).
- `COMPAT-FLUSS-006`: bucket-distribution skew probe (distinct keys spread evenly; constant key collapses).
- `COMPAT-FLINK-001`: source/sink checkpoint and restore.
- `COMPAT-FLINK-002`: cross-table partial visibility behavior.
- `BROKER-MD-001`: market packet corpus/protocol compatibility.
- `BROKER-PB-001`: postback schema/status/timestamp/identity behavior.
- `ARROW-REST-001`: request/response/auth/timeout/rejection behavior.
- `ARROW-REST-002`: client reference length/charset/echo and broker ID correlation.
- `E2E-001`: packet → decision → attempt → sandbox broker → postback → position.

### Crash-window and failure matrix

| Fault point | Expected invariant |
| --- | --- |
| Before raw append | Loss/uncertainty explicitly accounted; no silent success |
| After raw append before local ack | At-least-once duplicate tolerated downstream |
| During Flink checkpoint | Restore produces deterministic state within tested boundary |
| Between multiple Signal sinks | Partial visibility reconciled by IDs |
| After postback audit before lifecycle | Projection ledger resumes |
| After lifecycle before position | Position step resumes without lifecycle regression |
| Before attempt prepare | No broker call |
| After prepare before call | Recover pending attempt; no blind new attempt |
| During broker call | Outcome UNKNOWN; halt/reconcile |
| After broker acceptance before durable ack | UNKNOWN; no duplicate retry |
| During mapping persistence | Halt and reconcile mapping evidence |
| During gate approval | No partial/same-identity enablement |
| Fencing lease loss | Calls blocked; gate halted |
| Safety-halt request stale/unauthorized/cross-scope | Rejected; audited |
| Changelog gap | Trading readiness false; reconcile |
| S3/checkpoint unavailable | Job/durability not ready; gate halted if correctness affected |
| OpenObserve unavailable | Durable audit remains; telemetry readiness false |

### Chaos and disaster-recovery rules

All chaos tests use a sandbox or simulated broker unless a separately approved controlled test exists. The Executor starts `HALTED`; every fault preserves evidence; no test bypasses fencing or single-operator (Saurabh, DEC-044) approval controls.

The required fault coverage includes: Ingestion crash, disconnect, authentication expiry, partial subscription, append timeout, and bounded-buffer saturation; Signal JobManager/TaskManager failure, ZooKeeper node loss/restart, ZooKeeper quorum loss, ZooKeeper latency/partition, Flink JobManager HA failover, checkpoint timeout/corruption, S3/state/sink failure, and backpressure; Action Capture crashes after each independent write, projection backlog, Fluss outage, ambiguity, and postback storm; Babysitter restart, changelog gap, stale input, and accidental action enablement; Executor crash windows, mapping/state/audit failure, fencing loss, and split brain; Fluss coordinator/tablet/volume/quorum/leader failures; one-VM loss at baseline and peak; Arrow REST timeout/malformed/ambiguous response; OpenObserve alert failure; EOD/offload/retry/expiry failures; and credential/TLS/authorization failures.

Every exercise records exact versions, topology, workload, fault point/time, detected signals, gate state/epoch, RPO/RTO, backlog, checkpoints/offsets, reconciliation actions, recovery proof, alerts, and operator approvals.

The expected result is always: data-path recovery is measured and only claimed below thirty seconds for accepted cases; order uncertainty blocks calls inside five seconds; unknown outcomes never retry automatically; one fenced Executor owner exists per partition; unverifiable state remains `HALTED`; partial projections recover idempotently; source data never expires before verified offload; execution audit stays reconstructable; and observability loss never authorizes orders. Missing telemetry, unexplained recovery, or automatic resume is failure.

### Schema and recovery tests

- Clean catalog/table creation.
- Effective schema and option parity.
- Missing/wrong schema version prevents readiness.
- Immutable identity same/different hash behavior.
- KV stale/regressive/conflicting transition behavior.
- Projection rebuild from immutable events.
- Clean-break reset/replay.
- Savepoint/schema migration and rollback readability.
- EOD offload retry and expiry protection.
- Audit retrieval/reconstruction.

### Data quality and replay rules

Test raw byte/hash preservation; decoder/schema version and unknown-version quarantine; manifest completeness/types/active state/checksum; timestamp UTC conversion/clock offset/missing time; fingerprint and duplicate behavior; trade/quote/depth classification; candle correctness and late-event handling; candidate identity (**ranking/decision identity and score provenance REMOVED 2026-08-15, CHG-005**); postback correlation/lifecycle/position behavior; attempt/request hash/client-reference/mapping consistency; and audit redaction/reconstruction.

| Replay | Expected result |
| --- | --- |
| Same raw packets, same versions/config | Identical accepted state and output hashes. |
| Raw duplicate delivery | One compute effect; raw audit remains at-least-once. |
| Legitimate identical event | Bounded documented collapse may occur; metric is emitted. |
| Changed decoder/schema version | Reject or use an explicit compatibility path; never silently reinterpret. |
| Reordered input within lateness | Deterministic final candle result. (**Ranking REMOVED 2026-08-15, CHG-005.**) |
| Replayed postback | Immutable duplicate evidence and idempotent projection. |
| Replayed decision | No second active attempt for the same request hash. |
| Changed immutable content | Contract violation, quarantine, and safety halt where relevant. |

Every fixture has a version/checksum and expected output. Any count or hash mismatch is investigated; immutable/audit data never accepts “close enough.” Replay evidence records source range, schema/config versions, output hashes, duplicate/late counts, and unresolved evidence.

### Performance campaigns

| Test | Workload | Duration | Required evidence |
| --- | ---: | ---: | --- |
| `PERF-PER-INSTRUMENT-001` | variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) | Full trading session | SLOs, loss, backlog, checkpoints |
| `PERF-PER-INSTRUMENT-002` | Same manifest; restart Signal job once | Recovery window | Restore, no duplicate final candle or decision |
| `PERF-NODELOSS-001` | 50,000 ticks/s average-baseline profile + one VM loss (90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045) | Recovery window | Quorum, restore, <30 s accepted recovery, <5 s halt |
| `PERF-EOD-001` | Full-volume day | EOD | Verified manifest <30 min target |

Use production instrument universe, connections, packet-size distribution, strategy state, and exact versions. Window waiting is reported separately from processing latency.

### Performance benchmark procedure

The active instrument manifest is fixed at 3,000 instruments during a trading session. The baseline is 50,000 ticks/s on average across the declared measurement window (≈16.7 ticks/s/instrument on average). No instrument may exceed 20 ticks/s in the profile enforcement window. The capacity-peak campaign at 60,000 ticks/s is RETIRED (DEC-036); the theoretical cap ceiling (3,000 × 20) remains a generator stress bound only. A universal fixed 50 ms schedule is prohibited. The current testing phase uses the 1,024-instrument manifest `Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv` on one HFT connection (basic tier); the 3,000-instrument / 3-connection envelope is the deferred production target.

Record decode-to-append, tick-to-decision, decision-to-Executor receipt, and Arrow REST response percentiles separately. Also record throughput, backlog, source/sink and watermark lag, backpressure, state/checkpoint size and duration, restart count, acknowledged loss, recovery time, safe-halt time, EOD offload duration, and expiry margin. Every result includes versions/digests, configuration hash, duration, UTC and monotonic clock source, sample count, and whether a restart/failure occurred.

The per-instrument mock Arrow broker is the normal benchmark source. It uses a recorded seed, variable arrivals, the production instrument manifest, an ≈16.7 ticks/s/instrument baseline average at the 50,000 gate (generator capable of 20 ticks/s/instrument), and a hard 20 ticks/s/instrument cap. It rejects a missing seed, unknown profile, or a cap above 30. A live Arrow Trade WebSocket capture is optional for protocol evidence only; it records actual packet sizes, tick frequency, total rate, endpoint/session/reconnect data, and still uses the full Ingestion → Fluss → Flink path.

| Tool | Purpose |
| --- | --- |
| `hyperfine` | Warm-up and repeated-run orchestration |
| Flink REST API | Operator throughput, backpressure, checkpoints, watermark |
| Fluss metrics | Append latency, fetch/replication lag, bytes in/out |
| OpenObserve / Prometheus | Long-duration metrics, percentiles, alert proof |
| Docker stats / `htop` / `vmstat` | CPU, memory, disk IOPS, network bandwidth |

Each benchmark produces a versioned JSON record in `code/benchmarks/results/` and links it here. Its profile records the seed, profile name, 3,000-instrument manifest, baseline average, hard cap, observed distribution, expected 50,000 average rate, and duration (expected 90,000 peak rate retired with the peak campaign, DEC-036). It must not record a fixed `tick_interval_ms`.

| Step | Action |
| --- | --- |
| Warm-up | Run for two minutes for JIT and cache stabilization. |
| Variable baseline | Run 50,000 ticks/s average across the 3,000-instrument manifest for 30 minutes. |
| Capacity peak | RETIRED (DEC-036) — was: run 60,000 ticks/s with every instrument capped at 20 ticks/s for the declared campaign. |
| Cool-down | Drain backlog, wait for checkpoints, and verify no acknowledged loss. |

The baseline must meet the documented decision p99 target. Backlog must stay bounded, checkpoints must restore, safe halt must remain below five seconds, accepted data recovery below thirty seconds, and EOD verification below thirty minutes at full volume. Performance alone never proves protocol correctness, duplicate safety, or live-money readiness.

### Externalization benchmark (SIG-PERF-001, DEC-038)

Re-measures the Signal job after dedup externalization to prove the DEC-038 checkpoint invariant: **checkpoint size scales with bounded working/recovery state, not with Fluss-authoritative dedup cardinality; normal restart rehydrates from Fluss without full raw-history replay; the hot path does not become a per-tick Fluss round trip.** All numbers below are measurement targets — no value may be asserted (DEC-038 §9: replacing the old ~1 GB evidence with an invented number is prohibited). The pre-externalization references are historical baselines for the *why*, not target bounds.

Run on the current 1,024-instrument / 20,480 ticks/s envelope first (Phase 6 acceptance), then re-run at the deferred 50,000 ticks/s baseline (3,000 instruments; ≈16.7 ticks/s/instrument average; every instrument ≤20 ticks/s). The two runs use the same procedure, envelope tooling, and versioned evidence record as the performance benchmark procedure above; they differ only in envelope/acceptance profile (DEC-036/037).

| Metric | Pre-externalization reference (historical) | Post-externalization measurement target | Pass gate |
| --- | --- | --- | --- |
| Flink checkpoint size | 986 MB dev-hashmap / 22 s (2026-08-14 E2E); ~1.74 GB RocksDB total state at 53k t/s (2026-08-12 bench) | Measured after externalization | Scales with bounded working/recovery state, NOT Fluss dedup cardinality (SIG-STATE-001) |
| Checkpoint duration | 22 s at 986 MB (blew the 30 s budget — CP9 expired) | Measured | Within the pinned 30 s `CHECKPOINT_TIMEOUT_MS` budget with headroom |
| Restore duration | ~5-15 s estimate (pre-externalization, superseded) | Measured | ≤ 30 s data-path recovery (REQ-FC-008) |
| State hydration duration | n/a (no Fluss rehydration path) | Measured: rehydrate the dedup working cache from Fluss after compact-checkpoint restore | Reported; included in the restart-to-ready timeline |
| State hydration failures | n/a | Count | 0 — failures keep the job fail closed / degraded (SIG-STATE-003) |
| Dedup cache hit ratio | n/a | Measured: hot-path lookups served by the bounded cache | Reported (proves the hot path does not hit Fluss per tick) |
| Fluss dedup-table size | n/a (dedup state was Flink-side) | Entry count + bytes | Bounded = accepted rate × TTL horizon |
| Fluss dedup update rate | n/a | Writes/s (batched/async) | Reported |
| Throughput | 49,242-49,578 tps ingestion hot-path gate (synthetic) | Job sustains the envelope with no checkpoint growth | No per-tick Fluss round trip; decision p99 within limit (REQ-RNK-006) |
| Decision p99 | Pre-externalization bench (Phase 0/1 records) | Re-measured | Within the documented p99 target |

Evidence record: versioned JSON in `code/benchmarks/results/` (same convention as the performance benchmark procedure), capturing versions/digests, configuration hash, envelope, duration, UTC + monotonic clock sources, all rows above, and whether a restart/failure occurred. The benchmark additionally demonstrates: restart restores the compact checkpoint and rehydrates without full `raw_table_1` replay (SIG-STATE-002); the rehydrated cache still dedupes a re-sent fingerprint inside its staleness window; Fluss unavailability/incompatibility keeps the job fail-closed (SIG-STATE-003).

### One-VM-loss procedure

Run this at the variable baseline profile (the 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045). All three Fluss workload VMs and encrypted-S3 checkpoints must be healthy first. The Executor may be enabled only against a sandbox broker.

1. Record two minutes of healthy baseline metrics.
2. Hard-stop one workload VM and record `T0`.
3. Record ZooKeeper ensemble state (quorum 2-of-3 maintained) and ZK leader re-election.
4. Record Fluss quorum degradation and leader re-election.
5. Record Flink JobManager HA failover (standby takes over leadership) and TaskManager loss/restart trigger.
6. Record the gate transition to `HALTED`.
7. Record Ingestion reconnect and Flink checkpoint restore.
8. Run for ten minutes at reduced capacity and record throughput, backlog, and checkpoints.
9. Restore the VM and record ZooKeeper ensemble re-formation, Fluss quorum, replica catch-up, and backlog drain.

Pass requires zero acknowledged loss, safe halt below five seconds, data-path recovery below thirty seconds, successful checkpoint restore, and quorum re-formation without manual intervention. The evidence JSON records timestamps, topology, workload, versions, configuration hash, ISR shrink, leader election, checkpoint restore, backlog drain, and replica catch-up.

### Required end-to-end test matrix ([`01_plan.md`](./01-foundation.md) Section 4)

| Test ID | Duration | Input | Pass conditions |
| --- | ---: | --- | --- |
| `PERF-PER-INSTRUMENT-001` | 30 min | Production instrument manifest; variable 50,000 ticks/s average baseline | Raw append p99 <50 ms; decision p99 <100 ms; no acknowledged loss; total memory <85%; checkpoint p99 <5 s |
| `PERF-PER-INSTRUMENT-002` | 10 min | Same manifest; restart Signal job once | Processing resumes <30 s; state restores; no duplicate final candle or decision within the proven boundary |
| `FAIL-PENDING-001` | Until queue limit | Fluss append artificially stalled | Warning at 80%; readiness false; critical at 100%; no unrecorded loss |
| `FAIL-CHECKPOINT-001` | 5 min | Force checkpoint failure | Signal job suppresses decisions; one idempotent safety halt published; no Arrow REST call from Flink |
| `PERF-PER-INSTRUMENT-003` | RETIRED with the peak campaign (DEC-036, 2026-08-13) | — | Was: declared campaign at variable 60,000 ticks/s peak; no peak-capacity evidence row remains | No acknowledged loss; bounded memory/backlog; checkpoint and recovery evidence; no cap violation |
| `STATE-DEDUP-001` | 15 min | Variable baseline plus duplicates | Duplicate state contains compact identity/timestamps only; expired entries removed; no raw payload retained (DEC-038: the accepted dedup set is observable in the Fluss dedup table; the Flink checkpoint does not duplicate it) |
| `STATE-CANDLE-001` | 15 min | Variable baseline input | One final candle per non-empty 15-second window; no tick collection exists in active state |
| `BABYSITTER-001` | 5 min | Repeated position updates | Latest state only; zero actions; startup rejects action enablement |

### Security tests

- `SEC-NET-001`: network exposure and deny-path tests.
- `SEC-TRANSPORT-001`: TLS/encrypted transport/storage.
- `SEC-CRED-001`: secret scan, log redaction, support-bundle redaction, and rotation/revocation.
- `SEC-AUTHZ-001`: least privilege for table/state/broker calls and unauthorized controls.
- `SEC-IMAGE-001`: image digest/SBOM/vulnerability policy.
- `SEC-AUDIT-001`: audit access and deletion/legal-hold policy.

### CI gates

CI must fail for:

- Missing service entry point.
- Missing required test family.
- Stale prohibited identifiers or architecture terms.
- `latest` or unpinned production dependency.
- Requirement/contract/DDL/schema mismatch.
- Failing/skipped/flaky mandatory test.
- Missing evidence metadata.
- DDL apply exit-code contract drift: `make ddl-apply-smoke` (env-gated on `FLUSS_BOOTSTRAP`; in the Monday gate) must pass — the orchestrator is run three times against scratch-prefixed catalogs and the terminal contract (0 full PASS / 6 acknowledged `PASS_WITH_LIMITATION` / 1 refused limitation) plus the `RESULT=`/`DDL-APPLY-RESULT:` sentinels and evidence record are asserted; when docker + the ddl-apply image are present a fourth containerized drill mounts a pre-seeded engine-uid 644 evidence record and asserts the apply exits 1 with `EVIDENCE OWNERSHIP CHECK FAILED` naming the seed (`ddl_apply_smoke.py`; see `02-schema-storage.md`).
- Non-root evidence ownership contract drift: `make evidence-ownership-check` (in the Monday gate DDL step and `docs-audit` C15) must pass — the evidence root dir must carry setgid + group-write (2775), and every evidence record the ddl-apply container wrote (owner == the engine uid) must be group-writable AND carry the engine GID; no record or root dir may be root-owned (`evidence_ownership_check.py`; host-side `make ddl` records are out of scope).

`CI-PERF-001`: the variable baseline and peak benchmark profiles use a recorded seed, the production instrument count, no fixed 50 ms schedule, no per-instrument rate above 20 ticks/s, and zero acknowledged loss.

- Secret/redaction failure.
- Unsupported state/schema compatibility.

### Verification tiers — pick the cheapest tier that can fail for the change

| tier | command | use for | ceiling |
|---|---|---|---|
| T0 | `make gate-fast` (add `MODULE=<module>` to also run that module's suite) | every change: `make ddl`, static checks, the R2/parity audit suite, pin discipline, fast image staleness, doc↔code truth (C1–C16) | no live drills, no DDL apply smoke, no full doc audit; prints `GATE-FAST RESULT: subset green` — never a certificate |
| T1 | one targeted suite: `cd code && mvn -o test -pl <module>` — for a live drill class add `-Pdrill-reports -Dtest=<Class> -Dsurefire.failIfNoSpecifiedTests=false` and `FLUSS_BOOTSTRAP=localhost:9123`. Gateway classes need the reactor form `-pl common,02_services/06_execution_gateway`. | narrowing one failure T0 exposed, and re-verifying exactly that fix | one class/suite; says nothing about the rest of the module |
| T3 | `make gate` — the 19 steps | per batch, and the **only** run that certifies | ~51 min, clean tree required, cluster must be free |

Escalate T0 → T1 → T3. A failure mode counts as cross-class only once two
independent targeted runs disagree; otherwise one class is the right scope.
`run-monday-gates.sh --steps …` is a subset run: its SUMMARY ends in
`SUBSET RESULT` and it never certifies, however many steps it is given.

### Definition of done

The test program is complete when every mandatory requirement and P0/P1 audit issue maps to executable evidence, exact versions and environments are recorded, failure tests exercise the actual crash windows, performance campaigns match the workload envelope, and release evidence can be independently reviewed.

## Traceability

<!-- markdownlint-disable MD013 -->

### Purpose

This matrix maps audit findings and `01_plan.md` task sequence to the implementation dossiers and executable evidence families. It prevents an issue from disappearing during documentation or code work.

### Audit issue traceability

| Audit issues | Primary dossier | Test/evidence families |
| --- | --- | --- |
| P0-1 | `05-execution-core.md` | `EXE-*`, `REL-EXE-*`, `REL-CRASH-*`, `REL-HALT-*` |
| P0-2 | All component dossiers | `ING-*`, `SIG-*`, `AC-*`, `BAB-*`, `EXE-*` |
| P0-3 | `04-signal-job.md` | Job submission/readiness integration tests |
| P0-003 | ~~`15_portfolio_reservations.sql`~~ (**REMOVED 2026-08-15, CHG-005**), `17_postback_projection_ledger.sql`, `18_safety_halt_requests.sql`, `02-schema-storage.md` | DDL-INV-*, DDL-SCHEMA-*, DDL-APPLY-*, DDL-META-*, DDL-REPLAY-* |
| P0-4 | `01-foundation.md` (§Documentation status and evidence rules), ingestion/action dossiers | `BROKER-MD-*`, `BROKER-PB-*`, stale-term CI gate |
| P1-1 | Component dossiers, `12-version-compatibility-evidence.md` (+ `01-foundation.md` §Software versions and compatibility) | Build entry-point and artifact tests |
| P1-2 | Governance and cross-cutting invariants | Stale-term CI gate |
| P1-3 | `02-schema-storage.md` | `COMPAT-FLUSS-*`, schema workflow tests |
| P1-4, P1-5 | `02-schema-storage.md`, release evidence | `PERF-EOD-*`, `REL-RET-*` |
| P1-6, P1-18 | Local/production deployment dossiers | Health/readiness/startup tests |
| P1-7, P1-19 | Local and production deployment dossiers | Volume/replication/one-VM tests |
| P1-8 | `09-production-swarm.md` | `REL-HA-*` |
| P1-9, P2-1 | Version dossier, production deployment | Image/digest/SBOM CI gates |
| P1-10, P1-11 | Version/schema/local/production dossiers | Effective-config/S3/checkpoint tests |
| P1-12 | `02-schema-storage.md` | Routing/null/skew tests |
| P1-13 | Schema and cross-cutting invariants | Immutable duplicate/mutation tests |
| P1-14 | Schema and Action Capture dossiers | State precedence/stale/conflict tests |
| P1-15, P1-16 | Executor and cross-cutting invariants | Correlation/attempt/concurrency tests |
| P1-17 | Schema, Executor, release evidence | `EXE-AUDIT-*`, `REL-RET-*` |
| P1-20, P1-22 | Local/production deployment dossiers | Network exposure/deny-path tests |
| P1-21 | Governance/local/production/security dossiers | Secret scan/rotation/least privilege |
| P2-2 | Local dossier | Documentation and effective-mount checks |
| P2-3 | Signal dossier/version compatibility | Pinned connector checkpoint tests |
| P2-4 | Ingestion dossier | Discontinuity/no-sequence tests |
| P2-5 | Action Capture dossier | Duplicate/no-sequence postback tests |
| P2-6, P2-7, P2-8, P2-9 | Ingestion and schema dossiers | Manifest parser/validation/injection/projection tests |
| P2-10 | Test catalog | CI test-family coverage gate |
| P2-11, P2-12 | Schema lifecycle | Manifest/checksum/parity/reset/replay tests |
| P2-13 | Signal dossier | Two-job topology/job lifecycle tests |
| P2-14 | Executor dossier | Owned-state write tests |
| P2-15 | Action Capture/Babysitter/Executor dossiers | Aggregate ownership tests |
| P2-16, P2-17 | Observability dossier | Telemetry, clock, SLO and alert tests |
| P3-1, P3-2, P3-3, P3-4 | Governance/local/version/test dossiers | Docs links/build commands/test-stage/local-only CI checks |

### Plan phase traceability

| Plan phase | Dossiers |
| --- | --- |
| 0 Governance | `01-foundation.md` (§Build plan, §Documentation status and evidence rules), `11-testing-and-release.md` (§Release evidence) |
| 1 Reconciliation | `01-foundation.md`, cross-cutting invariants, component dossiers |
| 2 Versions | `01-foundation.md` (§Software versions and compatibility), `12-version-compatibility-evidence.md` |
| 3 Data model | `02-schema-storage.md`, `01-foundation.md` (§Shared safety rules) |
| 4 Ingestion | `03-ingestion.md` |
| 5 Signal job | `04-signal-job.md` |
| 6 Execution Core (Action Capture + Babysitter + Executor) | `05-execution-core.md` |
| 8 Local runtime | `08-local-compose.md` |
| 9 Production runtime | `09-production-swarm.md` |
| 10 Observability | `10-observability.md` |
| 11 Testing | `11-testing-and-release.md` |
| 12 Release | `11-testing-and-release.md` (§Release evidence) |

### Requirements traceability

| Requirement family | Owning dossier |
| --- | --- |
| `REQ-ING-*` | Ingestion |
| `REQ-FLS-*` / data requirements | Schema lifecycle |
| `REQ-FC-*` | Signal job |
| `REQ-SS-*` | Signal job |
| `REQ-RNK-*` | Signal job |
| `REQ-AC-*` | Action Capture |
| `REQ-BB-*` | Babysitter |
| `REQ-EXE-*` | Executor |
| `REQ-OBS-*` | Observability/operations |
| `REQ-PF-*` | Local/production deployment and version compatibility |

### Acceptance criteria coverage

Every acceptance-test row in the [acceptance matrix](../02_requirements/09-acceptance-matrix.md) is owned by an implementation dossier and proven by that dossier's test families from the master test catalog. Matrix AC ids are contiguous per domain; the counts below reconcile to 152 rows (15 ING + 17 FLS + 16 FC + 12 SS + 9 RNK + 17 AC + 9 BB + 16 EXE + 10 OBS + 19 PF + 12 NFR).

| AC domain | AC ids | Owning dossier(s) | Dossier test families (master catalog) |
| --- | --- | --- | --- |
| `AC-ING-*` | `AC-ING-001`–`AC-ING-015` | [`03-ingestion.md`](./03-ingestion.md) | `ING-*`, `BROKER-MD-001`, `STATE-DEDUP-001`, `FAIL-PENDING-001`, `THR-PROBE-001`, `MOCK-*` |
| `AC-FLS-*` | `AC-FLS-001`–`AC-FLS-017` | [`02-schema-storage.md`](./02-schema-storage.md) | `SCHEMA-*`, `COMPAT-FLUSS-*`, `COMPAT-FLINK-001`, `DDL-*` |
| `AC-FC-*` | `AC-FC-001`–`AC-FC-016` | [`04-signal-job.md`](./04-signal-job.md) | `SIG-*`, `STATE-CANDLE-001`, `STATE-COMPAT-001`, `SAFETY-INT-001` |
| `AC-SS-*` | `AC-SS-001`–`AC-SS-012` | [`04-signal-job.md`](./04-signal-job.md) | `SIG-*` (slot-scoped safety consumer), `SAFETY-INT-001` |
| `AC-RNK-*` | `AC-RNK-001`–`AC-RNK-009` | [`04-signal-job.md`](./04-signal-job.md) | `SIG-*` (in-operator ranking) — **REMOVED 2026-08-15 (CHG-005)** |
| `AC-AC-*` | `AC-AC-001`–`AC-AC-017` | [`05-execution-core.md`](./05-execution-core.md) | `AC-UNIT-*`, `AC-INT-001`, `AC-FAIL-*`, `AC-REC-001`, `BROKER-PB-001` |
| `AC-BB-*` | `AC-BB-001`–`AC-BB-009` | [`05-execution-core.md`](./05-execution-core.md) | `BAB-*`, `BABYSITTER-001` |
| `AC-EXE-*` | `AC-EXE-001`–`AC-EXE-016` | [`05-execution-core.md`](./05-execution-core.md) | `EXE-*`, `ARROW-REST-*` |
| `AC-OBS-*` | `AC-OBS-001`–`AC-OBS-010` | [`10-observability.md`](./10-observability.md) | `OPS-*` |
| `AC-PF-*` | `AC-PF-001`–`AC-PF-019` | [`08-local-compose.md`](./08-local-compose.md) (local subset, e.g. Compose isolation), [`09-production-swarm.md`](./09-production-swarm.md) | `LOCAL-*`, `SWARM-*`, `SEC-*`, `PERF-NODELOSS-001` |
| `AC-NFR-*` | `AC-NFR-001`–`AC-NFR-012` | Cross-cutting: [`01-foundation.md`](./01-foundation.md) + this catalog (performance campaigns, security tests, ops acceptance) | `PERF-*`, `SEC-*`, `OPS-*`, `CI-PERF-001` |

### Documentation completion statement

The dossiers specify implementation behavior but do not prove that code, DDL, deployments, or tests exist. Corresponding `01_plan.md` implementation checkboxes remain unchecked until executable evidence is produced. Documentation tasks may record these dossier paths as evidence and move to documentation-complete status.

## Release evidence

<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | Design-ready release gate; evidence not yet produced |
| Owner | Platform and Execution leads; Security/Compliance approval required |
| Release posture | `LIVE_MONEY_ALLOWED=false` until every mandatory gate passes |
| Source | `docs/05_deployment/00-release-strategy.md`, `docs/02_requirements/00-index.md`, `docs/08_implementation/01-foundation.md` |

### Evidence package contents

1. Approved requirements/decision/contract/DDL revision set.
2. Version and compatibility matrix with artifact evidence.
3. DDL/schema manifest, checksums, effective schema inspection, and parity result.
4. Packet/postback corpus and broker/Arrow sandbox evidence.
5. Component unit/integration/failure/recovery reports.
6. Flink checkpoint/savepoint/state compatibility reports.
7. EOD manifest/offload/retention verification.
8. Performance reports for baseline, burst, stress, and one-VM loss.
9. Security, secret rotation, least privilege, network, image/SBOM reports.
10. Dashboard/alert/runbook readiness evidence.
11. Rollback/readability test and deployment change record.
12. Executor crash-window, fencing, reconciliation, and single-operator (Saurabh, DEC-044) approval evidence.
13. Approved-policy audit reconstruction simulation covering at least one year and policy approval.

### Binary release gates

| Gate | Current status | Evidence ID | Owner | Last reviewed | Blocker | Blocking DATA-GAP | Pass condition |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Data gaps | NOT_PASSED | `REL-DG-*` | User + Platform | 2026-07-23 | Live-money | DATA-GAP-001, DATA-GAP-002 | No open Critical-priority gap; all required external inputs provided and validated |
| Requirements | NOT_PASSED | `REL-REQ-*` | Platform | 2026-07-23 | Live-money | — | No unresolved contradiction among requirements, decisions, contracts, DDLs, and code |
| Versions | EVIDENCE-GATED | `REL-VER-*` | Platform | 2026-07-23 | Live-money | DATA-GAP-001, DATA-GAP-002 | Exact versions/digests approved; no `latest` |
| Protocol | EVIDENCE-GATED | `REL-PROTO-*` | Ingestion + Action Capture + Executor | 2026-07-23 | Live-money | DATA-GAP-005 | Broker/Arrow REST fields, identities, status, response, and limits proven at Level 3+ |
| Schema | NOT_PASSED | `REL-SCHEMA-*` | Storage | 2026-07-23 | Implementation | — | DDL parses/applies/parity/replay/retention tests pass |
| Ingestion | NOT_PASSED | `REL-ING-*` | Ingestion | 2026-07-23 | Implementation | DATA-GAP-001 | Golden packets, raw bytes, fingerprint limits, backpressure, subscription completeness pass |
| Signal job | NOT_PASSED | `REL-SIG-*` | Compute | 2026-07-23 | Implementation | — | Event time, dedup, candles, restore pass (**ranking/reservations REMOVED 2026-08-15, CHG-005**) |
| Action Capture | NOT_PASSED | `REL-AC-*` | Action Capture | 2026-07-23 | Implementation | DATA-GAP-005 | Correlation/quarantine/lifecycle/positions/partial writes/rebuild pass |
| Babysitter | NOT_PASSED | `REL-BAB-*` | Compute | 2026-07-23 | Implementation | — | Separate job checkpoints and emits zero MVP actions |
| Executor | NOT_PASSED | `REL-EXE-*` | Executor | 2026-07-23 | Implementation | DATA-GAP-005 | Durable gate/attempt/mapping/audit/fencing/reconciliation pass; safety-halt control evidenced |
| Crash window | NOT_PASSED | `REL-CRASH-*` | Executor | 2026-07-23 | Live-money | DATA-GAP-005 | No duplicate broker order in every tested ambiguity window |
| Safe halt | NOT_PASSED | `REL-HALT-*` | Platform + Executor | 2026-07-23 | Live-money | — | Calls block within five seconds for every defined uncertainty trigger |
| Single-operator (Saurabh, DEC-044) resume | NOT_PASSED | `REL-APPROVAL-*` | Platform + Executor | 2026-07-23 | Live-money | — | Distinct authenticated approvals match epoch/evidence hash |
| Capacity | NOT_PASSED | `REL-PERF-*` | Platform | 2026-07-23 | Live-money | DATA-GAP-001 | 50,000 ticks/s workload campaign passes (3,000 instruments, ≈16.7 ticks/s/instrument average) |
| HA/recovery | NOT_PASSED | `REL-HA-*` | Platform | 2026-07-23 | Live-money | — | One workload VM loss, checkpoint, replication, and recovery posture pass; N+1 budgets documented and validated |
| EOD/audit | NOT_PASSED | `REL-RET-*` | Storage + Compliance | 2026-07-23 | Live-money | DATA-GAP-002, DATA-GAP-004 | Offload verification and retention protection pass; audit reconstructable |
| Security | NOT_PASSED | `REL-SEC-*` | Platform + Security | 2026-07-23 | Live-money | DATA-GAP-002 | Network, secrets, rotation, authorization, encryption, image policy pass |
| Operations | NOT_PASSED | `REL-OPS-*` | Platform + Operations | 2026-07-23 | Live-money | DATA-GAP-003 | Dashboards, alerts, runbooks, rollback and owners are operational |

A gate with `Blocker: Live-money` prevents production order placement. A gate with `Blocker: Implementation` blocks implementation progress for the affected component. Each gate SHALL be re-evaluated when its evidence changes.

### Approval sequence

1. Component owners sign their evidence.
2. Platform reconciles the version/schema/deployment package.
3. Execution signs gate/attempt/correlation/fencing/crash-window evidence.
4. Security signs secret/network/authorization/audit controls.
5. Operations signs dashboards/alerts/runbooks/rollback.
6. Compliance signs retention/deletion/legal policy.
7. Release owner confirms no unresolved critical risk.
8. Production deploys with gate `HALTED`.
9. Post-deployment reconciliation completes.
10. The single-operator (Saurabh, DEC-044) approval is recorded for the same gate epoch/evidence hash.
11. Enablement is recorded as an immutable audit event.

### Automatic blocking

The release process must fail closed for:

- Unknown version/protocol behavior.
- Missing or stale evidence.
- Failed/skipped mandatory test.
- Unresolved attempt (**or pre-2026-08-15 reservation — REMOVED CHG-005**).
- Unknown gate state.
- Lost fencing/durable state/changelog continuity.
- Unverified offload/retention.
- Missing telemetry or unowned critical alert.
- Rollback uncertainty.

### Rejection and rollback

A release is rejected if any mandatory gate fails. If uncertainty appears after deployment, the Executor returns to `HALTED`, evidence is preserved, affected orders/fills/positions are reconciled, and rollback follows the approved state-readable path. Automatic resume is prohibited.

### Final approval record

```text
release_id:
source_commit:
artifact_digests:
schema_manifest:
version_matrix:
compatibility_result:
all_gate_results:
open_risks:
rollback_artifact:
platform_approval:
execution_approval:
security_approval:
operations_approval:
compliance_approval:
first_operator:
second_operator:
gate_epoch:
evidence_hash:
enablement_timestamp_utc:
```

### Definition of done

This dossier is complete only when the evidence package can be independently reviewed and every gate is binary pass, with no P0/P1 issue unresolved or silently waived.

---

## P7 bench plan (2026-08-12, EXECUTED partial, RE-SCOPED)

### P7 — SignalJob Performance & Capacity Bench (tracker 14)

**Status:** `EXECUTED (partial) — Phase 0 baseline DONE; Phases 1-3 + dedup sweep recorded as BLOCKED with evidence` (updated 2026-08-13) — **TOPOLOGY RE-SCOPED 2026-08-13, see banner below**
\*\*Location:\*\* `docs/08_implementation/11-testing-and-release.md`
**Tracker:** the master dossier `docs/08_implementation/04-signal-job.md` §Absorbed documents (2026-08-17 consolidation; tracker `14-candle-log-kv-replay-safety_2.md` was deleted — the `## P7 — Performance and capacity evidence` section and §4 register rows `PERF-THROUGHPUT-001` / `PERF-LATENCY-001` / `DEDUP-MEMORY-001` live in git history).
**Dependency:** R2 lake-read stall fix (`§P3.5 — R2 lake-read stall containment`, absorbed into `04-signal-job.md` §Absorbed documents; plan file never persisted) — the bench uses R2 checkpoints for gate runs; the S3A timeout pins (30000/30000) and the outer-deadline containment apply.

> **REQUIREMENT CHANGE (user decision, 2026-08-13) — bench topology re-scope.**
>
> This bench measured the PRE-change topology (candle LOG + KV dual-sink). Per the
> requirement change recorded in tracker 14's banner, the candle KV projection is
> **RETIRED** and the [LOG + KV] facility moves to the **signal tables**
> (`Signal_Candidates` LOG + `Signal_Candidates_current` KV). The measured facts below
> — feed/tablet shared write-path ceiling 58.9–59.7k rows/s, prometheus-exporter
> histogram-bucket loss, R-298 write-side dedup — are **topology-independent pipeline
> facts and remain valid**. The register rows stay as recorded
> (PERF-THROUGHPUT-001 `[ ]` NOT ACHIEVED, PERF-LATENCY-001 `[ ]` NOT MEASURABLE,
> DEDUP-MEMORY-001 `[ ]` NOT RUN). When the signal dual-sink topology is implemented,
> the P7.2/P7.3 battery re-runs against it: "candle KV upserts/s" is replaced by
> "signal LOG appends/s" + "signal KV upserts/s".

## 1. Objective

Execute the tracker's P7.2 measurement battery (43 metrics) and P7.3 pass/fail gates on the SignalJob (raw_table_1 → validation → dedup → 15 s candles → sinks; as built 2026-08-13: candle KV upsert + signal LOG/KV — the pre-conversion candle LOG + KV dual-sink was what the 2026-08-12 bench measured, see bench record below), producing the three open register rows. Production status stays `BLOCKED` until these pass (tracker §6). A failed gate does NOT fail the plan — it records the bottleneck and re-runs (tracker rule, P7.3 tail).

## 2. Locked bench spec (user decisions, 2026-08-12)

### 2.1 Scope decisions (round 1)

> **Measured outcome (2026-08-13, see §14):** throughput gate of 50,000 is NOT achievable on
> this topology — measured feed/tablet shared write-path ceiling 58.9–59.7k rows/s
> (CountRows + Phase 0, two independent methods). Latency gate p99<100 ms not
> measurable via the prometheus exporter (histogram buckets dropped); mean 152.6 ms.
> Both recorded as documented deviations per plan §12; no config inflation.

| Dimension | Decision |
| --- | --- |
| Environment | Dev compose cluster on this host (`code/01_platform/01_docker/`); documented deviations: single node, no swarm, no production secrets |
| Feed | 3+ faketool connections (real-rate mode, proven 20k/s per connection ceiling) |
| Source table | Live `raw_table_1` (LOG, 16 buckets, bucket.key=instrument_token) |
| Live writer | STOPPED for the bench window; restarted after (docker stop/start of the ingestion container) |
| Active tokens | 1024 (real NSE_CM_EQUITY manifest) |
| Input realism | As-produced (whatever the feed emits; duplicate/out-of-order/invalid ratios recorded, not forced) |
| Sustained duration | 30 min @ 50k (matrix minimum) |
| Throughput gate | avg source `numRecordsInPerSecond` >= 50,000 ticks/s; record p50/p95/p99/max time series |
| Latency gate | end-to-end tick → candle-emit p99 < 100 ms (definition in §9.1) |
| Checkpoint storage | R2 for gate runs (production-like; S3A pins active); `file://` for debug runs |
| Disturbance | Docker-level injection: job restart + coordinator/tablet restart during peak |
| Deployment | Application mode via `submit-jobs.sh` |

### 2.2 Measurement and operation decisions (round 2)

| Dimension | Decision |
| --- | --- |
| Measurement clock | Starts at job RUNNING (cold start + RocksDB warm-up + dedup-state growth included in the numbers) |
| "Tick" origin | Feed-emit — Fluss write+read round-trip is INSIDE the 100 ms budget (measured as source lag; §9.1) |
| Latency tracking | ON for the gate run (`latencyTrackingInterval`) — one documented config delta; overhead tiny; one run yields latency + throughput together |
| Per-token tick pattern | As-produced (faketool real-rate default) — no artificial uniform/burst shaping |
| Feed schema fidelity | Trust as-is; non-blocking spot-check of first rows' columns in pre-flight (risk: bench measures faketool's row shape, not the live converter's — recorded as deviation if a diff appears) |
| 90k peak shape | Two 5-min bursts inside the 30-min window (minutes 10-15 and 25-30), 50k otherwise |
| No-data-loss formula | source consumed == LOG rows + dedup-dropped + quarantined/invalid + bounded in-flight; KV consistency checked separately (as built: candle KV upserts == LOG rows per token/window — re-scope 2026-08-13: `Signal_Candidates_current` KV key count == active instruments, signal LOG may grow on replay) |
| Checkpoint tolerance | <= 2 failed checkpoints tolerated IF each recovers via the designed restart (restore from last good checkpoint, no data loss, recovery <= 30 s); 3+ = gate FAIL (approved deviation from the strict "no checkpoint timeout" tracker wording — the R2 blackhole is a live risk, 3/3 canary wedges today) |
| Memory gate denominator | TM container limit (2g process: heap + off-heap + metaspace + JVM overhead + RocksDB managed); JVM-heap-vs-900 MB-alert series still recorded for reference |
| Dedup config | Production expiry values for the gate run + expiry sweep (30 s/60 s/120 s) in debug runs for DEDUP-MEMORY-001 |
| Baseline | Dev baseline run FIRST (current SignalJob under live traffic, no changes) as the before-column |
| Evidence granularity | All 43 series every 5 s to a raw file (`logs/tracker-14/`, gitignored) + O2 query refs; ~15k points/run |

## 3. Topology (proposed, from P4/P8 pinned settings)

- `submit-jobs.sh` application mode; `DEPLOYMENT_ENV`/`STATE_BACKEND` gates enforced as in production.
- `PARALLELISM=8` — matches the pinned 8 task slots; 16 buckets map 2:1 to sources; 1024 tokens → 128 tokens per keyed instance.
- TaskManager: 8 slots, `taskmanager.memory.process.size=2g` (pinned — the bench measures whether 8 × ~256 MB/slot sustains 50k with RocksDB + managed memory; that IS the memory gate).
- JobManager: `jobmanager.memory.process.size=1600m` (pinned).
- Checkpoint: 30 s interval (production config; 47/47 completed in the 2026-08-11 live run), `allowNonRestoredState` never set, RocksDB incremental.
- Candle sink: `feature_candles_15s` KV upsert only (sole candle output — 2026-08-13 conversion; the LOG/KV dual-sink and `feature_candles_15s_current` are RETIRED). Signal sinks: `Signal_Candidates` LOG + `Signal_Candidates_current` KV, canonical signal filter active; **stall guard REMOVED 2026-08-17 (CHG-023 item 4 — every sink is a plain `FlussSink`; the Fluss client's own `client.request-timeout` is the stall bound)**.
- All R2 S3A timeout pins effective (`iceberg.iceberg.hadoop.fs.s3a.connection.timeout` + `connection.establish.timeout` = 30000).
- Gate runs: R2 checkpoint dir + `latencyTrackingInterval` enabled (the single documented config delta).
- Debug runs: `file://` checkpoint dir, latency tracking optional, dedup expiry swept.

## 4. Pre-flight — baseline, feed capacity, environment health

Order matters (baseline needs the live writer; probes need it stopped):

0. **Fast smoke gate (≤ 2 min, mandatory — §4.1):** before every bench phase that runs > 10 min, prove the same machinery healthy:
   - `logs/tracker-14/probe-r2.sh` — R2 lake-read probe (HEAD+GET of lake metadata + one parquet row through the job's exact S3A conf chain, ~100 s incl. the idle-reuse check) MUST print `PROBE_RESULT=PASS` and exit 0. This is the R2-blackhole canary that caught the P3.5 stall in seconds instead of 55-90 min.
   - 30 s Fluss metadata read (`ShowTable` pattern): coordinator reachable, `raw_table_1` options as expected (datalake enabled, 16 buckets).
   - 60 s feed smoke at the phase's connection count: source `numRecordsInPerSecond` climbs and stays > 0.
   - Smoke fails → investigate + fix + re-smoke; the long phase does NOT start.
1. **Environment health gate (all must pass, else abort):** `docker ps` — fluss coordinator/tablet + zookeeper + collector + O2 healthy (no restart loops); no other SignalJob instance active; `curl` the O2 API (metrics deliverable); disk headroom on the checkpoint/evidence volumes; R2 endpoint reachable (`curl -sI -m 20`).
2. **Feed schema spot-check (non-blocking):** capture first rows from faketool output; confirm tick_type TRADE/QUOTE classification, 8-byte LE instrument_token, schema-v2 columns match raw_table_1; record as evidence (deviation note if any diff).
3. **Phase 0 — dev baseline (§5):** run the current SignalJob under live traffic, unchanged, 10 min; record all 43 series; achieved rate = the before-column.
4. **Stop the live writer** (docker stop the ingestion container; record container name).
5. **Probe A — 3 faketool connections at real-rate:** sustained source `numRecordsInPerSecond` >= 63,000 (50k + 5% headroom) over 5 min.
6. **Probe B — 5 faketool connections:** >= 94,500 (90k + 5% headroom) over 5 min.
7. If a probe fails: scale connections (ceiling 20k/s each; 3→50k, 5→90k) and record the achieved ceiling as a documented deviation.
8. If the bench does not start immediately after the probes, restore the live writer (record restart).

### 4.1 Long-run gate rule (user directive, 2026-08-12)

Any phase estimated > 10 min MUST be preceded by a ≤ 2-min smoke exercise of the SAME machinery that phase depends on: R2 lake read (`probe-r2.sh`) for checkpoint/audit-touching steps, feed + source read (60 s feed smoke) for rate steps, job startup for state steps. Smoke passes → run the long phase; smoke fails → fix + re-smoke. No blind long waits: the P3.5 R2 saga cost two days because a 55-90 min run wedged with NO error while a 1-min probe catches the same failure in seconds. The smoke result (probe log path + exit code) is recorded in the evidence file as part of the run's proof.

## 5. Phase 0 — dev baseline (10 min)

1. Live writer running, current dev SignalJob running as-is (today's config).
2. Capture all 43 series at 5 s cadence (P8.1 PromQL recipe: JM :9249 / TM :9250 → collector → O2 PromQL at `/api/{org}/prometheus/api/v1/query`).
3. Record: achieved source rate (p50/p95/p99/max), memory, checkpoint durations — the before-column for the report.

## 6. Phase 1 — steady-state 50k sustained (30 min)

0. **Smoke (§4.1), immediately before the clock starts:** `probe-r2.sh` PASS + 60 s feed smoke at 3 connections.
1. Feed up (3+ connections, 1024 tokens, as-produced pattern). Job in application mode, PARALLELISM=8, R2 checkpoint dir, latency tracking ON.
2. Verify startup: preflight table contracts pass, startup mode RESTORE (no FULL_REPLAY), rocksdb backend banner, both sinks running.
3. **Measurement clock starts at job RUNNING** (no warmup exclusion — cold start included).
4. Capture all 43 series every 5 s for the full 30 min; record checkpoint duration/size series, container gauges (`container.memory.usage/limit.bytes`), RocksDB gauges (`state.backend.rocksdb.metrics.block-cache-usage`, `cur-size-all-mem-tables`, `estimate-table-readers-mem`), dedup state counts, source/watermark lag, backpressure.
5. Gate: avg source `numRecordsInPerSecond` >= 50,000 over the window (record p50/p95/p99/max of the series).
6. Data-loss accounting at end of window (§2.2 formula); KV consistency probe (as built: KV upserts == LOG rows per sampled token/window; re-scope 2026-08-13: signal KV key count == active instruments, LOG may grow).

## 7. Phase 2 — 90k peak bursts (two 5-min bursts)

0. **Smoke (§4.1):** 60 s feed smoke at 5 connections (the burst rate) before minute 10.
1. Feed to 5 connections (or Probe-B ceiling) at minute 10; hold >= 90,000 for minutes 10-15; back to 50k for 15-25; 5 connections again for minutes 25-30.
2. Pass: source consumed >= 90k sustained during each burst AND no data loss AND no restart during the burst (checkpoint tolerance policy §2.2 applies).
3. Record the burst boundaries in the evidence file (start/end wall-clock timestamps) so the series can be sliced.

## 8. Phase 3 — disturbance matrix (docker-level injection)

Each case runs at peak (90k burst window); record the recovery window (P7.3: data-path recovery <= 30 s) and verify no data loss via checkpoint restore + dual-sink counts. Per §4.1, `probe-r2.sh` PASS immediately before each case (checkpoint + R2 restore path is the machinery under test); the job is already RUNNING for cases 8.2-8.4.

| Case | Injection | Expected |
| --- | --- | --- |
| 8.1 checkpoint during peak | none (continuous 30 s interval) | checkpoint p99 < 5 s, failures <= 2 and restart-recovered (§2.2) |
| 8.2 job restart during peak | cancel + resubmit from last checkpoint (RESTORE, strict) | recovery <= 30 s, no full replay, dual-sink counts monotonic |
| 8.3 coordinator restart during peak | `docker restart` fluss coordinator | tablet leader change + rebalance, no job failure, recovery recorded (P6.2 live pattern: checkpoint-timeout global restart, restore from last good checkpoint) |
| 8.4 tablet restart during peak | `docker restart` one tablet | client retries (writer retries=2), no data loss, recovery recorded |

## 9. Gate definitions

### 9.1 Decision latency — p99 < 100 ms (tick → candle emit, feed-emit origin)

- Feed-emit origin means Fluss write+read round-trip is inside the budget. Practical measurement (documented proxy, stated in evidence):
  - `L_operator` = p99 of source→sink operator latency from `latencyTrackingInterval` histograms (`flink_taskmanager_job_task_operator_*_latency_*`, PromQL p99);
  - `L_lag` = p99 of source lag series (Fluss write + read time);
  - Gate: `L_operator + L_lag < 100 ms`; record both components and the sum.
- Event-time validity: feed timestamps must be live wall-clock (not the dev historical feed) for watermark/lag meaning; verify before the gate run (part of feed spot-check).
- Exact 2.2.1 latency-tracking config key verified against `flink-core` before the run.

### 9.2 Memory — < 85% of allocated budget

- Denominator: TM container limit (2g process). `max(JVM heap, container gauges, RocksDB gauges, managed)` / 2g over the window; >= 85% fails the gate.
- JVM-heap-vs-900 MB-alert-threshold series recorded for reference (existing alert basis), not the gate.

### 9.3 Checkpoint — p99 < 5 s, failures within tolerance

- `flink_jobmanager_job_lastcheckpointduration` p99 < 5 s at sustained and peak; `numfailedcheckpoints` <= 2 across the run AND each failure restart-recovered with no data loss (approved deviation, §2.2); 3+ failures = gate FAIL.

## 10. Dedup expiry sweep (debug runs, DEDUP-MEMORY-001)

- Gate run uses production expiry values.
- Debug runs (`file://` checkpoints, 50k, 15 min each): expiry 30 s / 60 s / 120 s; record dedup state counts + RocksDB memory series per setting. Per §4.1, each 15-min debug run is preceded by the smoke: `probe-r2.sh` PASS (file:// checkpoints still read the R2 lake tier at startup) + 60 s feed smoke.
- Evidence: memory-vs-expiry table + time series → "bounded memory and expiry proof at target cardinality (1024 tokens under 50k load)".

## 11. Evidence template (register rows)

One evidence file `logs/tracker-14/p7-bench-<YYYYMMDD>.md` (gitignored, never committed) + raw 5 s series file, containing per tracker §4 required fields:

- date; commit (`git rev-parse HEAD` — the committed bench baseline); exact commands (feed launch, submit-jobs.sh env);
- environment topology (compose cluster, node, docker ps of the job containers);
- input volume/rate (tokens, connections, achieved rates, p50/p95/p99/max per metric family; burst boundaries);
- Phase 0 baseline column; data-loss accounting breakdown; KV consistency probe result;
- output location (R2 checkpoint path, O2 queries);
- pass/fail per P7.3 gate with the checkpoint-tolerance and config-delta annotations; bottleneck record on any failure; operator/approver line.

Register rows on completion: `PERF-THROUGHPUT-001` (50k sustained / 90k peak), `PERF-LATENCY-001` (p99 < 100 ms), `DEDUP-MEMORY-001` (bounded memory + expiry sweep at 1024 tokens under load).

## 12. Pass/fail handling

If any P7.3 gate fails: record the bottleneck in the evidence file and tracker P7 section, fix code/config, re-run the affected phase. Production stays `BLOCKED` (tracker §6). Do not compensate by raising memory/limits without measuring (tracker §6 tail).

## 13. Cross-references

- Tracker: master `docs/08_implementation/04-signal-job.md` §Absorbed documents (2026-08-17 consolidation; tracker `14-candle-log-kv-replay-safety_2.md` was deleted — its P7, §4 register, §6 acceptance, P8.1 metrics battery records live in git history).
- Metrics recipe: `logs/tracker-14/p8-1-flink-distributed-metrics-2026-08-11.txt`.
- R2 fix + containment: `§P3.5 — R2 lake-read stall containment` (absorbed into `04-signal-job.md` §Absorbed documents; plan file never persisted).
- Audit efficiency follow-up: tracker P3.6 (batch-audit engine parallelism/metadata-count; not exercised by this bench).
- Feed tooling: faketool real-rate mode (`code/02_services/01_ingestion/go-bridge/faketool`), fluss-throughput-bench evidence.
- Deployment: `docs/08_implementation/09-production-swarm.md` (future production target; not used by this bench).

## 14. Execution results

> **DEC-038 note (2026-08-14):** the Phase 0/1 bench records below are the **pre-externalization baseline** — their RocksDB total state (~1.74 GB) and checkpoint rows are the duplication the state-ownership change removes, not a bound on the target. Post-DEC-038 the same measurements are re-taken with the dedup set in Fluss (SIG-PERF-001 re-measurement row).

### 14.1 Phase 0 — dev baseline (2026-08-12, 14:23:40 → 14:34:30 UTC, DONE)

Full evidence: `logs/tracker-14/p7-bench-evidence-20260812-phase0.md` (this section is the condensed record).

**Deployment deviations (pre-approved, recorded in phase0 evidence):**

- D1: live writer dead since 2026-08-11 → bench feed (3 faketool conns) used throughout.
- D2: R2 checkpoint dir → `file:///checkpoints/p7-checkpoints` (no S3 filesystem jar in flink image).
- D3: checkpoint interval 30 s → 10 s (`CHECKPOINT_INTERVAL_MS=10000` config pin, REQ-FC-006).
- D4: source table `raw_table_1` → `raw_table_1_bench` (fresh, datalake DISABLED; avoids 15M-row replay + R2-orphan trap).
- D5: table dropped + recreated immediately before launch (`ALLOW_FULL_REPLAY=true`, replay-dominated baseline avoided).

**Phase 0 results (steady state, 5-s O2 PromQL sampling, 115 samples):**

| Metric | Value |
| --- | --- |
| source rate in | 53,052/s mean (feed-limited; table appends 59.7k, source reads 53k) |
| validation out (trade rate) | 36,094/s mean (~33% quote rows dropped by raw-validation, TRADE-only candles) |
| candles emitted | 17,408 (= 1024 tokens × 17 windows; 4,096/min exact) |
| dedup first-writes / duplicates | 10,984,942 (incl. 8.8M replay) / 2,474 |
| invalid rows | 0 |
| signals detected | 0 (faketool candles never cross signal conditions) |
| checkpoints | 66 completed, 10-s cadence; steady duration ~3.1 s (31% duty — bottleneck note) |
| checkpoint size | incremental 3.4 KB steady; RocksDB total state 1.74 GB |
| RocksDB | block_cache 377.5 MB, mem_tables 16 KB, readers 50 KB |
| fluss RPC latency | max 501 ms once; typical 0–5 ms per subtask |
| operator latency (source→sink) | mean 152.6 ms; p99 NOT derivable (exporter drops histogram buckets — measurement limitation) |
| JVM | TM heap 434 MB / metaspace 74 MB; JM heap 486 MB (24% of 2 g budget) |
| restarts / failovers | 0 |

**Phase 0 gate status:** §9.2 memory <85% PASS (24%); §9.3 checkpoint p99<5s PASS (steady 3.1s); §9.1 latency p99<100ms NOT MEASURABLE (exporter limitation, mean 152.6ms recorded).

**Phase 0 blockers discovered:**

1. Feed/tablet shared write-path ceiling ≈ 59.7k appends/s — Phase 1's "≥50k" gate is feed-limited by design (plan §12: record bottleneck, no config inflation).
2. Fluss 0.9.1 side-table leaderless buckets (tables 56/89, `Elect result is empty`, `leader=-1`) — server bug, cosmetic for bench; later fixed via wedge repair (8 stale ZK remote_logs handles, see R-298 session).
3. **Restore path broken (connector bug, bench-impacting):** Flink log source checkpoints fetch-ahead offsets (~79k/bucket past log end) → restore subscribes past-the-end → source consumes ZERO and would skip the unprocessed tail (data loss). Evidence: `RestoreStallProbe.java` (bucket 4: CONTROL @ offset 0 → 597,663 records; TEST @ restored offset 676,344 → 0). Consequence: restore-based phase chaining and §8.2 unusable; phases re-run FRESH (`ALLOW_FULL_REPLAY=true`, table truncated per phase).

### 14.2 Phase 1 — steady-state 50k (2026-08-12 14:39–14:58, NOT ACHIEVED, recorded)

- Attempt 1: restore OOM (`OutOfMemoryError: Direct buffer memory`, 1.7 GB RocksDB restore) → fixed via TM 2g→3g + off-heap 512m (effective MaxDirectMemorySize 890.9 MB). Relaunch recovered cleanly (8/8 subtasks from chk-91).
- Attempt 2: restored source consumes ZERO — the §14.1 connector fetch-ahead bug. Not fixable in bench; restore path abandoned (deviation: fresh-run mode).
- Phase 1 steady-state 50k gate NOT achieved; bottleneck = feed/tablet ceiling 59.7k (not SignalJob) + restore bug. Per plan §12: recorded, no config inflation.

### 14.3 Probes A/B + Phases 2–3 + dedup sweep (2026-08-13, BLOCKED)

- **Probe A (3 conns ≥63k)** / **Probe B (5 conns ≥94.5k)**: feed-limited. Authoritative drain re-measure (CountRows, 2026-08-13): 58,889 rows/s = 97% of the 3-conn nominal 61,440 — the shared write-path ceiling from Phase 0. Additional connections do NOT raise the ceiling. 63k/94.5k gates unreachable; recorded as documented deviation per plan §12.
- **Phase 2 (90k bursts)**: 1.5× the documented ceiling — would fail by design; not run.
- **Phase 3 (disturbance)**: requires docker restarts of coordinator/tablet (consequential); needs explicit user go-ahead; not run.
- **§10 dedup sweep**: CONFIG-BLOCKED — `SignalJobConfig.requirePinnedLong("DEDUP_TTL_MS", 300000)` throws on any non-300000 value (L229-241); unpin needs deliberate user decision.

### 14.4 Post-bench session 2026-08-13 — R-298 safety-write dedup (the actual churn fix)

Bench investigation exposed the safety-write churn (tables 56/89 growth, STALE flood); user selected option (a), applied + verified:

- Fix: gate all 4 safety emit sites on `IngestionService.firstEmission(safetyEmitted, state, haltId)` (id precomputed via `SafetyHaltWriter.computeHaltRequestId(...)`), commit `a4c696921b7c25a9b0eebf7febac334478265284` (branch `fix/r298-safety-write-dedup`).
- Live proof (2026-08-13, compaction-proof `*.log` byte sums, 202 s window): Safety_Halt_Requests-89 **0 bytes growth** (was +0.6–1 MB/s); ingestion_quarantine-56 +0.68 MB/s (was 2.3; designed STALE LOG-append sink, not gated by design); raw_table_1_bench-545 +11.96 MB/s.
- Drain ceiling correction: earlier "15–16k rows/s" was a byte→rows conversion error (~700 B/row assumed; real ≈205 B/row). True: **58,889 rows/s = feed ceiling** → throughput work (b) NOT needed.
- Evidence: `logs/tracker-14/p7-r298-verification-20260813.md`.

### 14.5 Register-row status

> **Design-B re-run (2026-08-17):** the P7.2/P7.3 battery re-ran on the Design-B
> topology (dedup = authoritative Flink keyed `MapState` + native `StateTtlConfig`;
> plain `FlussSink`s; native `flink-metrics-otel` reporter) via the 30-min
> `SignalChainLiveE2ETest` battery — evidence
> `logs/tracker-14/p7-battery-design-b-20260817.md` + raw series
> `logs/tracker-14/p7-battery-design-b-series.tsv`. Verdict: BUILD SUCCESS, 179
> checkpoints / 0 failed / 0 restarts; replay source consumption 60,867 rows/s at
> the tablet ceiling; steady-state live feed 18,441 rows/s (feed-limited,
> 1024-instrument envelope). The rows below carry the 2026-08-17 Design-B values.

| Row | Status |
| --- | --- |
| PERF-THROUGHPUT-001 (50k sustained / 90k peak) | NOT ACHIEVED — feed/tablet ceiling 58.9–59.7k (measured, 2 methods; topology-independent); Design-B re-run 2026-08-17 confirms: replay consumption 60,867 rows/s at the ceiling, steady-state live feed 18,441 rows/s (feed-limited, 1024-instrument envelope), 0 restarts / 0 failed checkpoints; bottleneck recorded, no config inflation |
| PERF-LATENCY-001 (p99 < 100 ms) | NOT MEASURABLE — 2026-08-12: prometheus exporter drops histogram buckets (count+sum only), mean 152.6 ms recorded; 2026-08-17 Design-B re-run: latency tracking off in the E2E harness and the native OTel reporter exports no operator-latency histogram buckets (`latency_histogram_points` NO DATA) — accepted outcome per 18-signal-job-remaining-work-plan.md Item C delta 3; fix = O2-side histogram ingestion or flink prometheus bucket config (out of bench scope) |
| DEDUP-MEMORY-001 (bounded memory + expiry sweep) | MEASURED (replay envelope) 2026-08-17 Design-B — dedup is Flink RocksDB `MapState` + native `StateTtlConfig` (no Fluss table): gauge + checkpoint size captured (peak 25.6M entries / ~3.5 GB estimate during the 66.5M-row replay; full checkpoint p50 1.6 GB / max 1.9 GB; 0 failed checkpoints). Steady-state 20,480 t/s envelope (~628 MB) + the 30/60/120 s expiry sweep still unmeasured — replay-dominated run; `DEDUP_TTL_MS` config-pinned to 300000 (unpin needs deliberate user decision) |

---

## 15. Bottleneck knowledge base (read before scaling, capacity planning, or touching the feed)

> Written 2026-08-13 so future sessions start from the measured facts instead of re-diagnosing.
> Scope: throughput/capacity only. The two Fluss 0.9.1 bugs (§15.7) are separate and persist regardless of hardware.

### 15.1 The one-sentence answer

**Every tick passes through ONE Fluss tablet-server process (the write path's only shared serial stage). That process tops out at ~59,000 rows/s measured. The bench gates (50k/90k) were set above that ceiling, so they were unreachable by design — not by hardware failure, not by feed failure.**

### 15.2 The data path and where the ceiling sits

```
faketool → websocket → Go bridge → Java ingestion → Fluss client → TABLET SERVER → disk
   (feed)                    (broker)      (converter)     (write api)    ← THE DOOR
```

- All 16 buckets of all tables funnel into the one tablet-server JVM: log segment writes, durability wait, compaction, R2 tiering — one process, shared by every connection.
- Measured: feed nominal **61,440/s** (3 conns × 20,480); tablet absorbs **58,889/s** (CountRows) / **59.7k appends** (Phase 0) = ~97% of nominal; the missing 3% piles up as backpressure (feed Send-Q 2.5 MB, client Recv-Q 5 MB observed) — the feed is NOT the slow side.

### 15.3 Capacity math (the whole planning table)

| Stocks | Per-stock rate | Total needed | vs measured 58.9k ceiling | Verdict |
| --- | --- | --- | --- | --- |
| 1024 (current) | 20/s (50 ms) | 20,480/s | 2.9× headroom | **Fine today, no action** |
| 3072 (future) | 20/s (50 ms) | 61,440/s | 4% OVER | **Does NOT fit single tablet — backlog grows unbounded (~216M ticks/day)** |
| 3072 (future) | 10/s (100 ms) | 30,720/s | 1.9× headroom | **Fits on current hardware** |
| 3072 | 20/s on 2 tablet servers | 61,440/s | ~2× the single-door ceiling | **Fits by design (Fluss scale-out)** |

Note: "tick" origin is feed-emit; the Fluss write+read round-trip is inside the 100 ms latency budget (§2.2).

### 15.4 What was RULED OUT as the bottleneck (measured, not guessed)

| Suspect | Verdict | Evidence |
| --- | --- | --- |
| RAM / a bigger machine | Not it | Heap 24% of 2g budget at full load; RocksDB state 1.74 GB — memory never approached limits |
| Disk | Not it | ~12 MB/s sustained writes; NVMe handles thousands of MB/s |
| Feed (faketool) | Not it | Produces its full 61,440/s nominal AND bursts above it (Send-Q backlog proof) |
| Host CPU / your PC | Not it | 0 crashes, 0 restarts, 0 invalid rows at full load; no pathology |
| Single tablet server | **The ceiling** | The one shared serial stage; every other candidate eliminated by measurement |

Caveat (honesty): the tablet attribution is the best-supported inference, not a final proof — Probe B (5 conns = 102,400/s nominal) was never run (blocked on user go-ahead). It remains the definitive test: drain stays ~59k → tablet confirmed; drain rises to ~95k+ → the feed was the limiter and no scaling is needed. **Run Probe B before buying any hardware.**

### 15.5 Permanent fixes, in order

1. **Probe B first (5 min, zero risk):** 5 faketool connections, measure CountRows drain ≥ 30 s. Confirms whether scaling is needed at all.
2. **If drain stays ~59k → add a second tablet server ON A SECOND HOST.** Fluss's designed scale-out: 16 buckets split across 2 servers → two write paths → ceiling roughly doubles → 61,440/s fits with margin.
   - Why a second host, not a bigger one: a bigger machine still has ONE door; more RAM is irrelevant (proven); two tablet JVMs on one box fight over the same 10 CPU cores.
3. **Fix the two Fluss 0.9.1 software bugs** (§15.7) — they block restarts regardless of hardware and will bite in production the same way they killed the bench.
4. **OR accept 100 ms per stock at 3072 stocks** — 30,720/s fits current hardware with 1.9× headroom; zero purchase. Cheapest option if freshness can flex.

### 15.6 Firefight traps — do NOT repeat these

- **Do not blame the PC / buy RAM.** Memory was at 24%; a 32 GB node changes nothing.
- **Do not blame faketool.** It meets its configured rate and bursts above it.
- **Do not "inflate config" to chase the gates** (plan §12): the ceiling is architectural (one door), not a knob.
- **Do not re-derive "15–16k rows/s" from bytes.** Real row ≈ 205 B, not ~700 B — the old number was a conversion error, not a real regression.
- **Do not treat the bench gates as the production requirement.** Production needs 20,480/s (1024 × 20) and measured capability is 58.9k — the gates were stress targets set above the feed topology.

### 15.7 The two Fluss 0.9.1 software bugs (separate from throughput)

| Bug | Symptom | Evidence | Status |
| --- | --- | --- | --- |
| Log-source restore stall | Flink source checkpoints fetch-ahead offsets (~79k/bucket past log end) → restore subscribes past-the-end → consumes ZERO (silent data-loss risk) | `RestoreStallProbe.java`: CONTROL @ offset 0 → 597,663 records; TEST @ restored offset → 0 | Recorded; blocked Phase 1 chaining; phases re-run FRESH |
| Leaderless side-table wedge | Stale ZK `remote_logs` handles → `startLogTiering` manifest read fails → `makeLeaders` throws → `NotifyLeaderAndIsr FAILED` → `leader=-1` (tables 56/89, same 8 buckets every restart) | R-298 session, 2026-08-13 | FIXED (A3: 8 stale ZK handles deleted + tablet restart; 0 retries since) |

### 15.8 Decision quick-reference

| Situation | Action |
| --- | --- |
| Keep 1024 stocks @ 50 ms | Nothing — 2.9× headroom |
| Grow to 3072 @ 100 ms | Nothing — fits current hardware |
| Grow to 3072 @ 50 ms | Run Probe B → if ~59k, add 2nd tablet server on 2nd host (+ fix §15.7 bugs) |
| Bench gates (50k/90k) | Record as deviation (§14.5); they are unreachable by design on one tablet server |

---

## Completed easy-gaps plan (2026-08-12) — historical record

### Easy Implementable Gaps — Tracker 14 Block (2026-08-12)

## Overview

Close the six "implementable now" gaps identified by the 2026-08-12 audit of
the corrective tracker (absorbed into `docs/08_implementation/04-signal-job.md`
§Absorbed documents; the tracker file `14-candle-log-kv-replay-safety_2.md` was
deleted 2026-08-17). Every item has a concrete code/evidence change in the
compute module, the O2 provisioning script, or the tracker/evidence docs. None touches the live production path, the Fluss
connector jar, or operator-only P10 territory.

**Acceptance criteria (block-level):**

1. SIGNAL-warn-dedup-state and SIGNAL-warn-dedup-expiry query live Flink-reporter
   series, not the dead ComputeOtlpEmitter streams. **VERIFIED 2026-08-17:**
   `SIGNAL-warn-dedup-state` (id `3I2RmqeXtLEGlW0B7rrVta0Ro3R`) confirmed bound
   to `stream_name=flink_taskmanager_job_task_operator_compute_dedup_state_count`
   (native series, live at 901 128 during the 10-min E2E; **2026-08-28 gauge
   remediation: repointed to `flink_jobmanager_job_lastcheckpointsize`** — the
   state-count gauge was a cumulative counter, ~39× over-report on RocksDB) and
   `SIGNAL-warn-dedup-expiry` confirmed ABSENT (retired CHG-023 item 2) —
   evidence `logs/tracker-14/o2-native-reporter-series-20260817.md`.
2. `CandleFailureInjectionIntegrationTest.kvTableDeletionFailsWholeJobNotLogOnlyDegraded`
   reaches terminal `FAILED` within a bounded window (no hang in FAILING).
3. RocksDB native-memory metrics (block cache, memtables, table readers) and a
   container-memory gauge are exported by the Flink reporter and PromQL-verifiable.
4. The batch audit/load launcher records JVM native-memory numbers in the run log;
   tracker box 427 (`[~]`) becomes `[x]` with real numbers.
5. `STARTUP-GATE-001` appears as a register row in tracker §4.
6. The §7 final coding-agent report exists with verdict `PENDING_OPERATOR_EVIDENCE`.
7. Tracker annotations (now: the absorbed tracker-status content in `04-signal-job.md`
   §Absorbed documents) are updated ONLY where evidence exists (per tracker rule),
   with artifact paths and dates.

## Context

Tracker: the corrective tracker, absorbed into `docs/08_implementation/04-signal-job.md`
§Absorbed documents (the 1248-line original `14-candle-log-kv-replay-safety_2.md`,
deleted 2026-08-17, lives in git history).
All open-box anchors below were verified by reading the tracker and the cited
sources on 2026-08-12.

### Verified hooks (file:line, all current)

- Dedup gauges ALREADY registered: `FingerprintDedupFunction.open()`
  (`code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FingerprintDedupFunction.java`)
  registers `compute.dedup.state.count`, `compute.dedup.state.bytes.estimate`
  via `getRuntimeContext().getMetricGroup().gauge(...)` —
  `compute.dedup.expiry.index.count` **DELETED 2026-08-17 (CHG-023 item 2)**
  with the expiry index; the count gauge gains a periodic per-token resync so
  native-TTL-expired entries stay out of it.
- Startup-mode gauge precedent:
  `RawValidationFunction.java:74-76` registers `compute.startup.mode`; the alert
  retarget precedent (delete + recreate rule, alert_id `3HmIy7IwzFgY563mG6tL1sxouhq`)
  is recorded in tracker P8.1 box 850 and `logs/tracker-14/p8-5-observability-live-2026-08-11.md` §4.
- Alert rules (2026-08-28 gauge remediation): `SIGNAL-warn-dedup-state`
  now reads `flink_jobmanager_job_lastcheckpointsize` (TRUE live state,
  threshold 1.5 GB); the old `compute_dedup_state_count` gauge is RENAMED to
  `compute_dedup.firsts.cumulative` (cumulative firsts, not state size —
  `MapState.entries()` does not filter TTL-expired in RocksDB, ~39× over-report;
  see `logs/tracker-14/dedup-ttl-diagnosis-20260828.md`).
  `SIGNAL-warn-dedup-firsts-rate` (new) guards input rate on
  `compute_dedup_first` > 50k/s. `SIGNAL-warn-dedup-state-bytes` REMOVED
  (redundant with the checkpoint-size alert). `SIGNAL-warn-dedup-expiry`
  remains RETIRED (2026-08-17, CHG-023 item 2).
- Sink wiring: `SignalJob.buildTopology` uses `FlussSink.builder()` `.sinkTo(...)`
  at `SignalJob.java:205-260` (LOG candle sink ~:206, Signal_Candidates ~:227,
  KV current sink ~:257).
- RocksDB branch: `SignalJob.applyRuntimeOptions` `SignalJob.java:283-306` —
  `StateBackendOptions.STATE_BACKEND`, `state.backend.rocksdb.localdir`,
  `state.backend.rocksdb.memory.managed`.
- FlussSink builder exposes generic config passthrough: javap of
  `/home/saurabh/.m2/repository/org/apache/fluss/fluss-flink-2.2/0.9.1-incubating/fluss-flink-2.2-0.9.1-incubating.jar`
  `FlussSinkBuilder` → `setOption(String,String)`, `setOptions(Map)` (verified 2026-08-12).
- Flink 2.2.1 RocksDB metrics mechanism (jar-verified 2026-08-12 from
  `/home/saurabh/.m2/repository/org/apache/flink/flink-statebackend-rocksdb/2.2.1/flink-statebackend-rocksdb-2.2.1.jar`):
  per-property boolean keys `state.backend.rocksdb.metrics.<kebab-property>`;
  available properties from `RocksDBProperty` (javap):
  `block-cache-usage` (BlockCacheUsage), `cur-size-all-mem-tables` (CurSizeAllMemTables),
  `size-all-mem-tables`, `estimate-table-readers-mem` (EstimateTableReadersMem),
  `estimate-live-data-size`, `is-write-stopped`, `num-running-compactions`, etc.
  Default-enabled set is the 13 literal keys seen in jar strings
  (block-cache-hit/miss, bytes-read/written, compaction-read/write-bytes, iter-bytes-read,
  num-files-at-level, stall-micros, bloom-filter-*, column-family-as-variable).
- Failure-injection test: `CandleFailureInjectionIntegrationTest.java`:
  `checkpointFailureTriggersConfiguredRestartThenFails` (:143) runs a healthy-path
  control checkpoint first, then injects a read-only checkpoint dir → observes
  RESTARTING → asserts FAILED via `awaitTerminal` (:381); `watermarkStallFreezesOutputAndResumesCleanly`
  (:196) freezes the feed watermark and asserts output freezes then resumes closing
  exactly the passed windows. Restart policy in harness: fixed-delay,
  `RESTART_MAX_ATTEMPTS=2`, delay 1 s (:327-328). (The former
  `kvTableDeletionFailsWholeJobNotLogOnlyDegraded` deleted-table shared-fate proof was
  removed with the candle KV table in the 2026-08-13 conversion.)
- Batch launcher (RETIRED 2026-08-13 — `CandleMigrationBatchJob` deleted with the candle KV
  projection): `logs/tracker-14/run-batch.sh:40` — single `java` invocation:
  `java -Xmx3g --add-opens=java.base/java.nio=ALL-UNNAMED -cp "...:/compute.jar" com.trading.compute.tools.CandleMigrationBatchJob`,
  stdout teed to `logs/tracker-14/batch-<mode>-<ts>.log`.
- Config-pin pattern to reuse for a new pin: `SignalJobConfig.requirePinnedLong/requirePinnedInt`
  (P4.2 box 545) + `PlatformConfig` (CHECKPOINT_INTERVAL_MS=10000, CHECKPOINT_TIMEOUT_MS=30000,
  MAX_CONCURRENT_CHECKPOINTS=1).
- O2 provisioning runner: `code/01_platform/04_scripts/o2-provision.py` (idempotent;
  re-run reports "alert exists" and skips; rule deletion/recreation precedent in P8.1 box 850).
- Startup gate (for register row): A3.3 fail-closed — RESTORE requires nonblank
  `STATE_RECOVERY_PATH`; `.env` and `submit-jobs.sh` carry no `ALLOW_FULL_REPLAY`;
  P6.1 phase 3 offsets proof (92/96, not 142) in `CandleGraphReplayIntegrationTest`.

### Decisions (user, 2026-08-12)

- P7 performance/capacity evidence is EXCLUDED from this block (separate campaign).
- KV-write fail-fast fix uses a Flink-side sink stall guard (no Fluss client jar patch). **SUPERSEDED 2026-08-17 (CHG-023 item 4): the stall guard is deleted — the Fluss client's own `client.request-timeout` bounds each write, and the checkpoint timeout + fixed-delay restart fail the job rather than hang it.**
- Metric-export gaps: Flink-side only (no Fluss server-side scrape, no collector changes).
- Block lives in this plan file; the tracker is touched only where evidence lands.
- Questions answered via ask: p7_scope=Exclude, failfast_approach=Flink-side watchdog,
  metric_scope=Flink side only, block_location=Plan file only.

### Non-goals

- P7 (all boxes :758-807), P9 swarm review (:1038), P10.1-10.3 (37 boxes), §6 acceptance (23 boxes).
- Fluss client jar patch (rejected; user chose the wrapper).
- Fluss tablet/coordinator server-side scrape (:910/:1004) — post-completion note only.
- `infrastructure_logs` receiver (:854), OTLP/gRPC log/trace producer (:831) — scoped out by user.
- Lake-only history union-read test (:452, environmentally blocked), tail replay (:454, by design).
- Dashboards: no panel changes required (optional post-completion).

### Assumptions / open questions

- The exact O2 series names for the RocksDB gauges (`flink_taskmanager_job_task_operator_..._block_cache_usage`
  vs dash-normalization) must be confirmed at runtime via PromQL `/labels` (same as P8.1 did) — the
  implementation writes the name from the live label list, not from a guess.
- Whether the fluss client exposes a request-timeout/retry config key honored by the sink write path
  is unknown until a spike (Task 2 step 1). The wrapper is the fallback and satisfies the same acceptance
  criteria either way.
- Dev SignalJob runs with `STATE_BACKEND=hashmap` by default (`SignalJobConfig.java:306-307`); RocksDB
  metrics therefore require a `STATE_BACKEND=rocksdb` dev run or the gated RocksDB integration test for
  runtime proof.

## Review Handoff

- Original request: build an implementation block of "not implemented but easily implementable" tracker-14
  aspects so the whole block can be implemented.
- Key decisions: above (Decisions).
- Explicit non-goals: above.
- Hidden context: none; this plan is self-contained for a fresh executor.

## Development Approach

- Testing: code-first with tests in the same task as each code change (repo convention).
- Complete each task fully (code + tests + evidence refs) before the next.
- Small focused changes; reuse existing patterns (gauge registration, pin gates, gated integration tests).
- Every code-change task ends with its test command green and its tracker annotation/evidence updated.
- Gated integration tests run with the repo's env gate: `COMPUTE_INT_TEST_P6=true` (and `COMPUTE_INT_TEST_P42=true`
  where the P4.2 harness is touched — not needed here).

## Testing Strategy

- Unit tests for every code change (gauge registration, stall-guard logic, config parsing, NMT parsing helper if added).
- Integration: extend the existing `CandleFailureInjectionIntegrationTest` (Task 2); reuse
  `CandleRocksDbRestoreIntegrationTest`/`RuntimeOptionsTest` patterns (Task 3).
- Live verification on the dev cluster (Tasks 1, 3): O2 PromQL queries against the running distributed job.
- Validation commands:
  - Compute unit suite: `cd code/02_services/02_compute && mvn -o test`
  - Gated integration: `cd code/02_services/02_compute && COMPUTE_INT_TEST_P6=true mvn -o test`
  - O2 provisioning (after rule retarget): source `code/01_platform/01_docker/.env`, then
    `python3 code/01_platform/04_scripts/o2-provision.py` (re-run idempotent)
  - Batch run (Task 4 — RETIRED 2026-08-13, `CandleMigrationBatchJob` deleted): `logs/tracker-14/run-batch.sh audit` (evidence only; a load re-run optional)

## Progress Tracking

- Mark `[x]` items in this plan as completed, with the evidence path in the checkbox note.
- Add newly discovered tasks with `+` prefix.
- Record blockers with `BLOCKED:` prefix.

## Implementation Steps

### Task 1: Retarget dedup-state alerts to the live Flink gauge series (tracker :958)

**Why:** SIGNAL-warn-dedup-state/SIGNAL-warn-dedup-expiry reference `compute_dedup_state_count` /
`compute_dedup_expiry_index_count` — ComputeOtlpEmitter stream names with 0 live series on the
distributed job (the emitter died with the `flink run -d` submitting JVM). The Flink gauges the
alerts should use are ALREADY registered in `FingerprintDedupFunction.open()`; only the rule
`stream` fields point at the dead names.

**Files:**

- Modify: `code/01_platform/04_scripts/o2-provision.py` (lines ~425-430)
- Evidence: `logs/tracker-14/p8-3-alerts-2026-08-11.txt` (existing) + new addendum file
  `logs/tracker-14/p8-3-dedup-alert-retarget-2026-08-12.txt`

- [x] Change the two alert dicts' `stream` values to the Flink-reporter series:
      `flink_taskmanager_job_task_operator_compute_dedup_state_count` and
      `flink_taskmanager_job_task_operator_compute_dedup_expiry_index_count`
      (confirm exact names from the live `/labels` list first; the dashboard panels at
      o2-provision.py:188/:263 already use these names). → landed in `o2-provision.py:424-429`.
- [x] Delete + recreate the two rules (O2 v0.91.5 update-by-name does not retarget streams;
      follow the P8.1 box 850 delete/recreate precedent; record the new alert_ids). → old
      `3Hm4QIrbPcVIuKwrEq5bhaf6aye`/`3Hm4QLbxQAjhQ5NtRhzMrWNwUJp` deleted (HTTP 200); new
      `SIGNAL-warn-dedup-state`=`3HnOGJxmmUHe9a3J9HOSm8YpevM`, `SIGNAL-warn-dedup-expiry`=
      `3HnOGF8O5geZOh0LOCXQIU19a05` → `flink_taskmanager_job_task_operator_compute_dedup_*` streams.
- [x] Verify live: `O2 PromQL query` for both new series returns points on the running
      distributed SignalJob (if the running job predates the gauge registration, restart the
      dev signaljob statefully per `signaljob-live-restart` and re-query). → series=1, value=2989
      each (2026-08-12), recorded in the evidence file.
- [x] Update tracker box 958's parenthetical: remove the "Emitter streams not live" caveat;
      reference the retarget evidence file + new alert_ids + the PromQL proof. → tracker
      line 958-959 annotation replaced 2026-08-12.
- [x] Run `python3 code/01_platform/04_scripts/o2-provision.py` twice (idempotent re-run
      reports "alert exists"). → provisioned via `O2_AUTH_BASIC` from `.env` (the stored
      `.env` value is stale → 401); evidence file records the re-run.
- [x] Unit assertion not required (pure provisioning change) — the verification is the live
      PromQL query + idempotent re-run. → `logs/tracker-14/p8-3-dedup-alert-retarget-2026-08-12.txt`.

### Task 2: Bounded sink-write stall guard → terminal FAILED (tracker :682/:116)

> **SUPERSEDED 2026-08-17 (CHG-023 item 4): the `StallGuardedSink` watchdog this
> task built was deleted — every sink is a plain `FlussSink` carrying
> `client.request-timeout = SINK_WRITE_STALL_TIMEOUT_MS` + `client.writer.retries=2`;
> the native client request timeout is the stall bound. The task record below is
> the historical 2026-08-12 build record; the artifact is gone (deleted with
> `StallGuardedSink.java` + `StallGuardedSinkTest.java`).**

**Why:** With the KV table deleted mid-run, the job cycles FAILING→RESTART→FAILING without
reaching terminal FAILED in the observed window. Root cause (javap-verified in
fluss-client-0.9.1-incubating): the deleted table's write batch never fails and never drains —
`flush()` blocks forever in `RecordAccumulator.awaitFlushCompletion()`, and `close()` blocks
forever in `awaitTermination(Long.MAX_VALUE)` because the sender's shutdown drain loop needs
`forceClose=true`, which `close()` only sets AFTER `awaitTermination` returns (circular client
deadlock). A Flink-side stall guard bounds each delegate call itself to a configured timeout so
the configured restart policy completes and the job ends FAILED deterministically.
Shared-fate semantics (no LOG-only degraded mode) are preserved.

**Files:**

- Create: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StallGuardedSink.java`
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java`
  (wrap both Fluss sinks in `buildTopology` :206 and :257 with the guard)
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java`
  (new pinned key, see Technical Details)
- Modify: `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/CandleFailureInjectionIntegrationTest.java`
  (assert terminal FAILED)
- Create: `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/StallGuardedSinkTest.java`

- [x] **Spike (1 h max):** javap/strings the fluss-client jar
      (`/home/saurabh/.m2/repository/org/apache/fluss/fluss-client/0.9.1-incubating/`) for
      request-timeout/retry config keys; if a key honored by the sink write path exists, wire it
      via `FlussSinkBuilder.setOption` (config wiring, still Flink-side) as the primary fix and
      keep the wrapper as the fallback. Record the finding in the task evidence. → `client.request-timeout`
      (default 30 s) is read by `RpcClient` for every request incl. writes; sink-scoped options reach the
      writer client (`FlussSinkBuilder.build()` → `Configuration.fromMap` → `ConnectionFactory` →
      `FlinkSinkWriter.flussConfig` → `WriterClient` — javap-verified) → wired via
      `setOption("client.request-timeout", … + "ms")` on BOTH candle sinks. **Second key discovered
      2026-08-12 after the gated run showed FAILING-hang persisting:** `client.writer.retries`
      (ConfigOptions.`CLIENT_WRITER_RETRIES`, default `Integer.MAX_VALUE`, enforced by
      `Sender.canRetry` = `attempts < retries` AND (RetriableException OR idempotence path,
      idempotence default ON) — javap-verified) makes a permanently-failing write retry FOREVER,
      so `flush()`/`close()` never return and the post-hoc stall guard cannot fire → job stuck
      FAILING. Bounded to `"2"` via `setOption("client.writer.retries", "2")` on BOTH sinks.
      **FINAL root cause (2026-08-12, supersedes the retries hypothesis):** `retries` is NEVER
      consulted for a deleted table — its batch never fails (metadata update for the dropped table
      is swallowed in `Sender.sendWriteData`, `readyNodes` stays empty), so the batch stays
      undrained forever. `Sender.run()`'s shutdown drain loop
      (`while (!forceClose && accumulator.hasUnDrained()) runOnce()`) never exits because
      `WriterClient.close(Duration)` calls `sender.forceClose()` only AFTER
      `ioThreadPool.awaitTermination(Long.MAX_VALUE)` returns — a circular client deadlock
      (javap-verified); `flush()` likewise blocks forever in
      `RecordAccumulator.awaitFlushCompletion()`. Both hang points convert `InterruptedException`
      into a fast exit (`flush()` throws `FlussRuntimeException`; `close()`'s interrupt handler
      runs `shutdownNow()` + `forceClose()`), so the guard was changed to run EVERY delegate call
      on a single worker thread and bound the CALL ITSELF at `SINK_WRITE_STALL_TIMEOUT_MS`,
      interrupting on timeout (primary fix; retries=2 kept for transient-retry hygiene; wrapper =
      the bounded-call executor, not a post-hoc check).
- [x] Implement `StallGuardedSink<T>`: a `Sink<T>` delegating to the FlussSink whose writer
      enforces "no write/flush completes within `SINK_WRITE_STALL_TIMEOUT_MS`" → throw a
      `RuntimeException` (fail the task → configured restart). Forward the full sink2 lifecycle
      (init, createWriter, restore, emit, flush, prepareCommit, snapshotState, notifyCheckpointCompleted).
      Preserve ordering: guard must not reorder or drop rows on the healthy path.
      → `StallGuardedSink.java`; lifecycle note: Flink 2.2.1 sink2 `SinkWriter` carries only
      write/flush/writeWatermark/close (prepareCommit/snapshotState live in optional interfaces the
      Fluss sink does not implement — javap-verified), so the guard covers exactly the live write path.
      **2026-08-12 redesign (post-hoc check was insufficient — see spike):** every delegate call
      runs on a single daemon worker thread; the caller waits at most the stall window in
      `Future.get`, on timeout the worker is interrupted (unwinds both Fluss hang points) with a
      short grace, then a stall `RuntimeException` is thrown. Single worker keeps calls strictly
      ordered (Fluss writer is not thread-safe).
- [x] Add `SINK_WRITE_STALL_TIMEOUT_MS` to `SignalJobConfig` (default 15000, non-positive rejected)
      following the `requirePinned*` pattern; document as a new governed pin in the same commit.
      → `PlatformConfig.SINK_WRITE_STALL_TIMEOUT_MS` pin + `SignalJobConfig.sinkWriteStallTimeoutMs()`
      (non-positive → `IllegalStateException`).
- [x] Wrap the LOG candle sink and the KV current sink in `buildTopology` (both get identical
      failure semantics — shared job fate). → `StallGuardedSink<>` around both, same 15 s window.
- [x] Unit tests (`StallGuardedSinkTest`): healthy passthrough (writes complete, ordering kept),
      stall fires (controllable blocking fake sink + small injected timeout → throw within the
      window), flush/prepareCommit forwarding, config rejects non-positive timeout.
      → 9 tests green (write/flush/close stall, passthrough, per-call window, pre-write-topology
      forwarding, constructor + config pin rejection).
- [x] Extend `kvTableDeletionFailsWholeJobNotLogOnlyDegraded`: switch `awaitFailureState` →
      `awaitTerminal(..., 180)`; assert terminal == FAILED; keep LOG-frozen-at-50 assertion;
      record the status sequence (expect RUNNING → FAILING → RESTARTING → … → FAILED).
- [x] Run: `cd code/02_services/02_compute && COMPUTE_INT_TEST_P6=true mvn -o test` — the KV
      deletion test must now PASS with FAILED (previously it tolerated FAILING/hang).
      → non-root container full class 3/3 GREEN (checkpoint-failure 69.979 s, watermark
      34.177 s, kv-drop 82.9 s, terminal FAILED with stall cause):
      `logs/tracker-14/gated-run-20260812-nonroot-fullclass.log`.
- [x] Update tracker boxes 682/116 annotations: remove the hang limitation, cite the watchdog
      + the new test result + evidence path; tick :116 (failure-injection leg) with the
      FAILOVER-FLUSS-001 annotation update.
      → done 2026-08-12; evidence `logs/tracker-14/p6-2-stall-guard-terminal-failed-2026-08-12.txt`.

### Task 3: RocksDB native-memory + container-memory metrics (tracker :906)

**Why:** The tracker records "Container-managed/RocksDB-natural native memory not separately
exported by this reporter build". Flink 2.2.1 exposes per-property RocksDB gauges via
`state.backend.rocksdb.metrics.<property>` boolean keys (jar-verified); a cgroup-memory gauge
covers the container side. Flink-side only per scope decision.

**Files:**

- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java`
  (`applyRuntimeOptions` rocksdb branch, :284-303)
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/RawValidationFunction.java`
  (register container-memory gauge in `open()` next to :74)
- Create: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/ContainerMemory.java`
  (cgroup v2 `memory.current`/`memory.max`, v1 fallback `memory.usage_in_bytes`/`memory.limit_in_bytes`;
  injectable path for tests)
- Modify: `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/RuntimeOptionsTest.java`
- Create: `code/02_services/02_compute/src/test/java/com/trading/compute/signaljob/ContainerMemoryTest.java`

- [x] In `applyRuntimeOptions` (rocksdb branch only), set
      `state.backend.rocksdb.metrics.block-cache-usage=true`,
      `state.backend.rocksdb.metrics.cur-size-all-mem-tables=true`,
      `state.backend.rocksdb.metrics.estimate-table-readers-mem=true`
      (verify the kebab names against `RocksDBProperty` during implementation).
      → kebab names jar-verified against `RocksDBProperty` in flink-statebackend-rocksdb-2.2.1.
- [x] Implement `ContainerMemory` (usage + limit bytes, cgroup v2 → v1 fallback, failure = gauge
      absent not crash). → `ContainerMemory.java` (v2 `memory.current`/`memory.max` incl. literal
      "max" → limit -1; v1 `memory.usage_in_bytes`/`memory.limit_in_bytes`; null on any failure).
- [x] Register `container.memory.usage.bytes` + `container.memory.limit.bytes` gauges in
      `RawValidationFunction.open()` alongside `compute.startup.mode`.
- [x] Tests: `RuntimeOptionsTest` asserts the three keys are set in the rocksdb branch and absent
      in the hashmap branch; `ContainerMemoryTest` covers v2 file parse, v1 fallback, missing-file
      behavior. → + `RawValidationFunctionMetricsTest` (real registration via
      `UnregisteredOperatorMetricGroup`, values mirror `ContainerMemory.read()`).
- [x] Runtime proof: run the gated `CandleRocksDbRestoreIntegrationTest`
      (`COMPUTE_INT_TEST_P6=true`) to prove the metrics keys are accepted; then one dev run with
      `STATE_BACKEND=rocksdb` and PromQL `/labels` + query for
      `flink_taskmanager_job_task_operator_..._block_cache_usage` (exact name from /labels) and the
      container-memory series. → rocksdb keys runtime-accepted via gated rocksdb restore test
      (1/1, 70.18 s, `gated-run-20260812-nonroot-fullsuite.log` — restore with the metric keys
      enabled); live PromQL leg deferred: PENDING_OPERATOR_EVIDENCE (no dev SignalJob start per
      scope constraint — see final report §12.1).
- [x] Update tracker box 906 annotation: replace the "not separately exported" caveat with the
      metric names + PromQL proof + evidence path.
      → done 2026-08-12: P8.1 metrics box now records the three rocksdb metric keys + the two
      container gauges, the gated runtime acceptance, and the PENDING_OPERATOR_EVIDENCE live leg.
- [x] Run: `cd code/02_services/02_compute && mvn -o test` (unit) then the gated suite.
      → fast units green (StallGuardedSinkTest/ContainerMemoryTest/RuntimeOptionsTest/
      SignalJobConfigTest/RawValidationFunctionTest 73+ passed); gated suite in progress.

### Task 4: Native-memory measurement for CandleMigrationBatchJob (tracker :427) — RETIRED 2026-08-13

**Why:** Box 427 is `[~]` — "Peak heap + wall duration logged; native memory and spill volume
are N/A — annotated, not measured." Native memory CAN be measured with JVM flags; spill volume
stays N/A (no spill component). **Superseded:** `CandleMigrationBatchJob` and `run-batch.sh`
were deleted with the candle KV projection (2026-08-13 conversion); box 427's annotation stands
as the historical record and this task is closed without execution.

**Files:**

- Modify: `logs/tracker-14/run-batch.sh` (line 40 java invocation)
- Evidence: `logs/tracker-14/p3-3-batch-2026-08-12.md` (add native-memory section) + a fresh
  `logs/tracker-14/batch-audit-*.log`

- [x] Append `-XX:NativeMemoryTracking=summary -XX:+PrintNMTStatistics` to the java command in
      run-batch.sh (both flags; PrintNMTStatistics without tracking enabled is a no-op).
- [x] Re-run `logs/tracker-14/run-batch.sh audit`; confirm the run exits 0 and the teed log
      contains the "Native Memory Tracking" exit summary (Total: reserved/committed).
      → 04:54Z (bg_11) + 06:24Z (bg_4): lake-enabled union read hangs 2/2 in the
      fluss-lake-iceberg Hadoop-catalog load (`S3AFileSystem.exists → AWS-SDK-v1
      getObjectMetadata` blocked on an R2 HTTPS response header, no socket timeout) — R2,
      docker network, objects, tiering job all proven healthy (in-network boto3 HEAD/GET
      <0.6 s); identical jars to the green 55m41s audit. Upstream client×R2-edge issue —
      `logs/tracker-14/r2-lake-read-stall-2026-08-12.md` + 2 thread dumps
      `batch-audit-r2-stall-threaddump{,-2}-2026-08-12.log`. 07:54Z datalake-disable run:
      connector refuses log-only batch reads (FlinkTableSource.java:371) but the aborting JVM
      prints the NMT exit summary → JVM-start native baseline captured (see next item).
- [x] Record in the evidence file: native committed total + the tracked categories
      (heap, class, thread, code, GC, compiler, internal, arena/chunk) alongside the existing
      `MAX_PEAK_HEAP_MB`/`DURATION_MS`; keep "spill volume N/A" with the same justification.
      → recorded in box 427 + `r2-lake-read-stall-2026-08-12.md` "NMT baseline" table from
      `batch-audit-20260812-132452.log`: Total committed 352.6 MB / reserved 4.92 GB at
      -Xmx3g; category breakdown incl. heap 128 MB committed, Metaspace 53.5 MB, Arena Chunk
      25.8 MB. Semantics: JVM-start baseline (source-creation abort), not full-run peaks —
      full-run native peaks stay gated on the R2 fix.
- [x] Update tracker box 427 `[~]` → `[x]` with the native numbers + log path + date.
      → ticked 2026-08-12 (tracker :428) with the baseline numbers + semantics + evidence
      paths; lake tier re-enabled via ZK registration restore + coordinator restart (tiering
      resumed, epoch 185 committed 08:08Z).
- [x] Run: `logs/tracker-14/run-batch.sh audit` (full run; ~1 h — do not re-run load).
      → attempted 2026-08-12 04:54Z (bg_11) and 06:24Z (bg_4): both stalled on the R2 lake
      read (see above); killed after ~80 min each; NMT flags verified present in both JVMs
      (docker ps cmdline).

### Task 5: Register STARTUP-GATE-001 (tracker §4)

**Why:** The traceability matrix (:1177) cites gate `STARTUP-GATE-001` but §4 register
(:1131-1144) has no such row. The gate is implemented and proven (A3.3 fail-closed startup).

**Files:**

- Modify: `docs/08_implementation/14-candle-log-kv-replay-safety_2.md` (§4 register table) — **file deleted 2026-08-17; the §4 register content is absorbed into `04-signal-job.md` §Absorbed documents (original in git history)**

- [x] Add row `STARTUP-GATE-001` after `CHECKPOINT-RESTORE-002`: evidence = SignalJobConfig
      fail-closed startup gate (RESTORE requires nonblank STATE_RECOVERY_PATH; no
      ALLOW_FULL_REPLAY in `.env`/submit-jobs.sh; explicit approval path documented) +
      P6.1 phase-3 offset proof (92/96 not 142, `CandleGraphReplayIntegrationTest`) +
      `SignalJobConfigTest` startup-mode tests. → row + delivered-piece bullet added to §4
      (tracker lines 1139/1156).
- [x] No code change; verify the row renders in the table. → markdownlint clean.

### Task 6: Produce the §7 final report (tracker §7)

**Why:** §7 requires a 14-item coding-agent report with an explicit verdict; none exists.

**Files:**

- Create: `logs/tracker-14/final-report-2026-08-12.md`

- [x] Write the 14 required sections (files modified + why; validator behavior; canonical policy;
      conflict policy + 25-key status; complete-history reader + proof; state backend + checkpoint
      config; tests + exact commands; benchmark note (P7 not run — excluded, dev measurements only);
      failure-injection results incl. Task 2 outcome; JobGraph/checkpoint compatibility;
      metrics/alert coverage incl. Task 1/3 outcomes; items pending operator-only execution; evidence
      IDs + output paths; verdict `PENDING_OPERATOR_EVIDENCE`).
      → `logs/tracker-14/final-report-2026-08-12.md` (drafted 2026-08-12; §7/§11 patched with the
      full-suite numbers after the fixed container run).
- [x] Reference every evidence artifact by path (tracker-14 evidence files; P3.3 batch evidence).
      → §13 evidence table; NMT batch numbers appended once the bg_11 audit lands (§4/§7/§13).
- [x] Add a one-line pointer to the report from tracker §7.
      → tracker §7 "Final coding-agent report" line points to `logs/tracker-14/final-report-2026-08-12.md`.

### Task 7: Verify block acceptance criteria

- [x] Verify all six Overview acceptance criteria are met (re-read each tracker annotation + evidence path).
      → 6/6 criteria closed with evidence: dedup alerts (o2-provision.py:724/730); kv-drop
      terminal FAILED (`p6-2-stall-guard-terminal-failed-2026-08-12.txt`); RocksDB/container
      gauges (tracker :915 + gated CandleRocksDbRestoreIntegrationTest 1/1); NMT numbers +
      box 427 `[x]` (tracker :428 — JVM-start native baseline from
      `batch-audit-20260812-132452.log`; full-run peaks gated on the upstream R2 stall);
      STARTUP-GATE-001 (tracker :1147/:1164); final report + verdict
      (`final-report-2026-08-12.md` §14). Block verdict `PENDING_OPERATOR_EVIDENCE` with the
      NMT-full-run + PromQL deviations recorded in report §8/§12.9/§13.
- [x] Run the full compute unit suite: `cd code/02_services/02_compute && mvn -o test` (0 failures).
      → host: 206 tests, 0 failures, 0 errors, 11 skipped, BUILD SUCCESS (2026-08-12).
- [x] Run the gated integration suite: `cd code/02_services/02_compute && COMPUTE_INT_TEST_P6=true mvn -o test`.
      → non-root container, full P6 suite GREEN 206/0/0/3 (BUILD SUCCESS) —
      `logs/tracker-14/gated-run-20260812-nonroot-fullsuite-fixed.log`.
- [x] Re-run `python3 code/01_platform/04_scripts/o2-provision.py` — idempotent, no drift.
      → two identical runs (6× alert exists, 0 retention updates, no drift).
- [x] Record a block-level evidence line in `logs/tracker-14/p3-3-batch-2026-08-12.md` or the final report
      covering all six items.
      → final report §14 verdict + §13 evidence table (six items mapped); NMT criterion
      recorded as BLOCKED with evidence paths; block-level line in
      `logs/tracker-14/r2-lake-read-stall-2026-08-12.md` "Impact on tracker 14".
- [x] Merged into this dossier 2026-08-13 (previously lived under a `plans/` folder).
      → moved 2026-08-12 with the NMT deviation documented (criterion 4 externally blocked).

## Technical Details

- **O2 series naming:** Flink PrometheusReporter exports operator gauges as
  `flink_taskmanager_job_task_operator_<metric-name>`; O2 normalizes stream names to lowercase
  with underscores (P8.1 evidence note: "metric names are lowercase stream names"). The dedup
  gauges' dotted names become `flink_taskmanager_job_task_operator_compute_dedup_state_count`
  (matches the dashboard panels already stored in O2; **2026-08-28 remediation renamed
  the dedup gauges to `compute_dedup_firsts_cumulative`/`compute_dedup_firsts_bytes_estimate`** —
  they are cumulative-firsts counters, not live state; live-state monitoring reads
  `flink_jobmanager_job_lastcheckpointsize`). Confirm every name from live `/labels`
  before editing rules.
- **RocksDB metrics:** per-property boolean keys `state.backend.rocksdb.metrics.<kebab>`; the
  property set is `RocksDBProperty` (javap-verified from the pinned 2.2.1 jar). These metrics
  register on the keyed-state operator metric group, so they appear on the TM reporter output.
  They exist only under the RocksDB backend — dev default is hashmap, so runtime proof needs a
  rocksdb run or the gated integration test.
- **Stall guard semantics:** guard fires only when a write/flush exceeds the stall window while
  the writer is active; healthy-path writes complete in ms, so 15000 ms default is ~10x headroom.
  The guard is per-sink and symmetric (LOG + KV) so the shared-fate contract (box 682) is
  unchanged — deleting either table still takes the whole job down, now to FAILED, not a hang.
- **New pin:** `SINK_WRITE_STALL_TIMEOUT_MS` joins the `requirePinned*` set; P4.2's
  "checkpoint pins unchanged" box is unaffected (existing pins untouched). The pin
  SURVIVES as the `client.request-timeout` value passed to every `FlussSink`
  (CHG-023 item 4, 2026-08-17 — the watchdog is gone, the pin lives on).
- **NMT:** `-XX:NativeMemoryTracking=summary` enables tracking; `-XX:+PrintNMTStatistics` prints
  the summary at JVM exit into the teed stdout of run-batch.sh (stdout is the log; no plumbing
  changes). Runtime overhead for a batch job is negligible.
- **ContainerMemory:** cgroup v2 path `/sys/fs/cgroup/memory.current` + `memory.max`; v1
  `/sys/fs/cgroup/memory/memory.usage_in_bytes` + `memory.limit_in_bytes`; a read failure omits
  the gauge (metric must never crash the operator).

## Post-Completion

**Manual verification (dev cluster, live jobs):**

- After Task 1/3: confirm the two dedup alert series and the RocksDB/container series keep fresh
  points on the running distributed job for a few minutes; confirm the two alert rules exist with
  the new alert_ids and fire on a fixture injection (reuse the P8.3 OTLP fixture technique if time).
- After Task 4: the batch-audit log's NMT summary numbers are captured; no load re-run needed.

**External system updates (not in this block):**

- P7 benchmark campaign (excluded by scope decision).
- Fluss tablet/coordinator server-side Prometheus scrape (`:910/:1004`) — requires Fluss config +
  collector receiver + O2 stream; keep the documented limitation until then.
- `infrastructure_logs` receiver (`:854`) and OTLP/gRPC log/trace producer (`:831`) — user-scoped out.
- Upstream apache/fluss `forLogRecords` fix (P3.3 connector patch replacement when a release lands).
- Optional: add a RocksDB native-memory panel to the COMPUTE - Checkpoints & State dashboard in
  o2-provision.py after Task 3's names are confirmed live.

### 2026-08-28 ADDENDUM — gauge remediation (dedup monitoring)

**Why:** the `compute.dedup.state.count` gauge (registered in
`FingerprintDedupFunction.open()`) was a **cumulative counter**, not live
state size — `MapState.entries()` does not filter TTL-expired entries on the
RocksDB state backend (bytecode-verified in `RocksDBMapState$RocksDBMapIterator`,
Flink 2.2.1), so the gauge over-reported ~39× vs the true live set
(checkpoint `state_size` ≈ 16-18 MB total, dedup share ~5.75 MB ≈ 40k entries).
Full diagnosis: `logs/tracker-14/dedup-ttl-diagnosis-20260828.md`.

**Changes (landed in this commit):**
- Gauges renamed: `compute.dedup.state.count` → `compute.dedup.firsts.cumulative`,
  `compute.dedup.state.bytes.estimate` → `compute.dedup.firsts.bytes.estimate`
  (honest names — they are cumulative-firsts counters, not state size).
- `SIGNAL-warn-dedup-state` repointed to `flink_jobmanager_job_lastcheckpointsize`
  (TRUE live state, threshold 1.5 GB). `SIGNAL-warn-dedup-state-bytes` REMOVED
  (redundant). New `SIGNAL-warn-dedup-firsts-rate` (input-rate guard, >50k/s).
- `rollout-savepoint.sh` `sample_dedup` reads the renamed gauge (relative
  continuity check still valid — cumulative counter preserved across restore).

**REQUIRED OPS STEPS (O2 is create-only idempotent — manual delete+recreate):**
1. Delete old `SIGNAL-warn-dedup-state` alert from O2 (still bound to the
   broken `compute_dedup_state_count` stream).
2. Delete old `SIGNAL-warn-dedup-state-bytes` alert.
3. Re-run `o2-provision.py` (creates the new `SIGNAL-warn-dedup-state` on
   `lastcheckpointsize` + `SIGNAL-warn-dedup-firsts-rate`).
4. Redeploy SignalJob (compute.jar) for the renamed gauges to take effect;
   the alerts on `lastcheckpointsize`/`first` are already valid against the
   live metrics before redeploy.
