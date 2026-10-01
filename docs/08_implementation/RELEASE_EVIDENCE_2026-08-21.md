# Release Evidence Package — 2026-08-21

> **2026-08-25 supersession note (do not rewrite this dated evidence):** counts/status in this
> file are the 2026-08-21 snapshot. Current truth (CHG-100/101/102 + 2026-08-25 unified plan
> `docs/plans/2026-08-25-live-readiness-unified-plan.md`): Rust lib **196** (was 148/168), C6
> triple **466/247/388**, `make gate` **13/13** (was 12/12), instrument **`RCF-EQ`** (BI-EQ/BILCARE
> delisted), auth **AutoLogin** (`ARROW_TOKEN` removed 2026-08-24). Live order `26082501010305`
> reached Arrow 2026-08-25 — `MARGIN ERROR` (sandbox unfunded). A new `RELEASE_EVIDENCE_*.md`
> append is a known open item (unified plan U4.4) once the funded market session lands.

**Release:** `2026-08-21` · **Source commit:** `22371e3` (E2 gate) — evidence lands on next commit `CHG-077/078`
**Status:** `Blocked` → `Approved-for-Testing` only after E5 single-operator (Saurabh, DEC-044) sign-off. Live money stays `HALTED`/`disabled`.
**Scope:** Everything proven on laptop + dev Fluss + fake bridges. Market-session (`BI-EQ ×1` live order) and prod-VM (`PERF-PROD-60000`, `FAIL-VM-LOSS`) evidence stays `BLOCKED: market-hours` / `needs prod VMs`.

---

## 1. Approved revision set

| Artifact | Revision | Evidence |
|---|---|---|
| Requirements | `docs/02_requirements/00-index.md` | `152` acceptance rows (15 ING + 17 FLS + 16 FC + 12 SS + 9 RNK(removed 2026-08-15) + 17 AC + 9 BB + 16 EXE + 10 OBS + 19 PF + 12 NFR) — `docs_audit.py` C8 pins 152 |
| Contracts | `docs/04_contracts/` | Contracts bind dossiers |
| DDL manifest | 26 tables, all `ddl_sha256` + `compatibility_class` + `bucket_key` for LOG | `docs_audit C1/C4` + `logs/schema-compat/` |
| Code | `code/common`, `02_compute`, `04_executor`, `06_execution_gateway`, `06_execution_bridge` | `make gate` 12/12 PASS 2026-08-21 |

---

## 2. Version and compatibility matrix

Pinned in `code/01_platform/04_scripts/versions.pin`: Flink `2.2.1`, Fluss `0.9.1-incubating`, ZK `3.9.2`, Go `1.24.5`, Rust `1.97.1`, Nautilus `74d57e7e…`. Verified by `make pin-check` PASS (CHG-076 gate) + `make full-audit` C7 PASS.

Current matrix `docs/08_implementation/12-version-compatibility-evidence.md`: rows 7 (postback), 8 (Arrow REST), 9 (OpenObserve), 10 (base images) remain `TO_BE_VERIFIED` — honestly **not yet flipped**. E1 is `BLOCKED: needs A3–A5+D7`. This package does not fake VERIFIED.

---

## 3. DDL / schema manifest, checksums, effective schema, parity

- Manifest 26 tables, `C1/C2` PASS, `C4` header↔DDL parity PASS.
- Live parity: `CompatFlussDdlParityIntegrationTest` 21/21 DDL-parity descriptors green on dev Fluss `9123` (env-gated) — 21 of the 26-table manifest (2026-08-21).
- Composite-PK matrix: `CompositeKeyMatrixVerifierTest` + `DdlApplyToolStatusTest` + `CompatFlussCompositeKeyIntegrationTest` — `PASS`/`PASS_WITH_LIMITATION` contract verified (CHG-052).
- Evidence `logs/schema-compat/compat-fluss-001-003-20260815.md` + `compat-flink-002-20260815.md` (superseded counts noted in 11-testing.md: as of 2026-08-15 `341 common / 0 / 1 skip` historical 2026-08-15).

---

## 4. Packet / postback corpus and broker / Arrow sandbox evidence

- **Market-data wire:** `BROKER-MD-001` live `socket.arrow.trade` 40/196 B zstd LE — `logs/broker-md-001/` (AC-ING BLOCKED half lives here; error half captured in harness).
- **Postback corpus:** `BROKER-PB-001` quarantined/stale/duplicate harness green; live fill postback waits on `BI-EQ ×1` market-hours fill (A3 BLOCKED).
- **Arrow REST error half:** `a4-arrow-rest-error-20260821.md` + `arrow_capability_test.go` 4 legs PASS (401→reauth→disabled, UNKNOWN 15 s, request-id coalesce, reuse_violation) — success half `BI-EQ ×1` deferred.

---

## 5. Component unit / integration / failure / recovery reports

| Component | Unit | Integration | Failure / Recovery | Evidence |
|---|---|---|---|---|
| Ingestion | `ING-UNIT-001..022` (incl. fuzz, parity, OTLP scrub, SIGTERM-drain ING-UNIT-023/024) | `ING-INT-001..006`, `ING-E2E-001` | `ING-FAIL-001..010` + `ING-DQ-010/011` | `03-ingestion.md` + `logs/nautilus-execution/e5c-*`, `c1-*` |
| Signal | `CandleAggregate*`, `SignalJobConfig`, `FingerprintDedup` (25 tests) + `CepDependencyGuard` | `SignalChainLiveE2ETest` envelope 205k candles / 1,074 instruments / 28 ckpt | `SignalJobCheckpointFailureIntegrationTest` (SIG-FAIL-001) | `04-signal-job.md` |
| Execution gateway | `ExecutionIntent*` 18 tests, `GatewayProtocol` HMAC, readiness+ledger | `FlussGate/Attempt` durable + cross-restart, `FlussProjectionWriter` live, `Postback_Quarantine` LOG | — | `CHG-051/052/053` |
| Executor (Rust) | `gate` 11 + `executiongate` 20 + `clockwatch` 7 + `durable` 8 | `FakeBridge`+`CacheView` LiveNode construction, `bridge transport` HMAC+hash | `crash_exactly_once` (B2), `live_node_soak` 1800 s (B1), `gate lifecycle` (B3) | `logs/nautilus-execution/b1-*..b8-*` |
| Go bridges | `ingestion go-bridge`: c1 plug-and-play, c3 losslessness multi-conn, c4 50k synthetic + parity hft; `execution 06_bridge`: `broker_reauth_test` + `arrow_capability_test` | fake HTTP+WS lifecycle | crash-loop + auth-failure + shutdown-deadlock mirrors (ING-FAIL-008..010) | `c1-*..c4-*`, `a1-*`, `a4-*` |

Counts this gate: `cargo --offline` 148 Rust lib PASS, `go test -race` 18.7 s ingestion + 1.12 s execution-bridge PASS, `mvn -o test` 246 ingestion + 341 common PASS (2026-08-21 — docs-audit C6 truth 236 ingestion is stale, now 246; was 236 pre-C2 parity, see CHG-067).

---

## 6. Flink checkpoint / savepoint / state compatibility

- `SignalJobCheckpointFailureIntegrationTest` (SIG-FAIL-001) — injected ckpt failure → 3 fixed-delay restarts → terminal FAILED — PASS (host MiniCluster).
- `BabysitterPositionsRestoreIntegrationTest` (COMPUTE_INT_TEST_T7) — env-gated `chk-N` retain + restore → duplicate no-op — harness green (live run in `b5-*` deferred to D-era).
- No Flink CEP (policy) — `cep-check-module` + `CepDependencyGuardTest` PASS.

---

## 7. EOD manifest / offload / retention

Live evidence `logs/schema-compat/` audit chain + `docs_audit` C15 evidence-ownership PASS. Retention approval `SAURABH-1Y-APPROVAL-2026-08-20` (CHG-055, T0 bundle) (CHG-055 record absent — see its tombstone; claim unverified) — approved 2026-08-20 by Saurabh (DEC-044). Real offload/retention drills on prod stack are D-era.

---

## 8. Performance: baseline, burst, stress, one-VM loss

| Campaign | Envelope | Result | Evidence |
|---|---|---|---|
| `ING-PERF-001` synthetic hot-path | 1,024 instruments | socket `49,242 tps / 0 loss`, append `49,578 tps / p99 <5 ms` (floors 47.5k/48k PASS) — 2026-08-13 | `03-ingestion.md` Status |
| `PERF-PER-INSTRUMENT-001` 50k gate | 2,433×20 Hz = 48,660 tps synthetic | bridge multi-conn synthetic envelope PASS — `c4-sig-perf-50k` build <100 ms, per-token exact, N=1 vs N=3 no degradation | `logs/nautilus-execution/c4-sig-perf-50k-20260821.md` (CHG-069); live Fluss 30-min 50k on prod stack deferred to D5 |
| 90k peak | — | **RETIRED DEC-036** | — |
| `PERF-PROD-60000-001` p99 <100 ms trigger→commit | 50k, 3k instruments, prod Swarm | **BLOCKED: needs prod VMs** | — |
| `FAIL-VM-LOSS-60000-001` | drain 1 worker VM, data <30 s / order halt <5 s, no duplicate | **BLOCKED: needs prod VMs** | D4 |
| Swallow 3k/3-conn envelope | 3,000 / 3×1,024 | **REMOVED DEC-037** (not to be tested) | `03-ingestion.md` |

---

## 9. Security, secret rotation, least privilege, network, image / SBOM

- `SEC-NET-001` — sandbox contract `t8_sandbox_contract_check.py` 12/12 PASS + `execution_network_check.py` PASS (execution-net `internal:true`, arrow-egress bridge-only, zero host ports) — WP-6 live-verified 2026-08-21.
- `SEC-CRED-001` — `ARROW_*` only on `01_ingestion` (t0 scan), `executor:` never carries `ARROW_*` (T8 re-scope), `.env` git-ignored, `OTLP` scrub `REDACTED` on surviving marker (M5 ING-UNIT-021/022).
- `SEC-IMAGE-001` — `golang:1.24.5-alpine@sha256:daae04e…` + `rust:1.97.1` pinned in `versions.pin` + `Dockerfile`; `pin-check` PASS.
- Auth live half: TOTP login proven live `execution-auth-001` len 238, re-auth harness `reauth.go` + `broker_reauth_test.go` PASS (A1 DONE*).

---

## 10. Dashboard / alert / runbook readiness

- O2 outage → durable local audit proven (`OPS-FAIL-001` — `RETAIN_ON_CANCELLATION` + `BABYSITTER_STATE_RECOVERY_PATH`).
- Alert thresholds data-derived — **BLOCKED: needs D4/D5 measurements**; seed dashboards `seed_dashboards.py` exists but live dashboards await D7.
- Runbooks `docs/06_operations/01-runbooks.md` § Execution service runbooks (CHG-055; CHG-055 record absent — see its tombstone; claim unverified).

---

## 11. Rollback / readability test and deployment CHG

- Rollback: `Rejected/Retired/Deleted` audit scans pass; journal `RETAIN_ON_CANCELLATION`; drain-on-shutdown via `DRAIN_DEADLINE_SECONDS` (ING-FAIL-010).
- Deployment CHGs this package: `CHG-062` (B1 soak) · `063` (B2 crash fence) · `064` (B8 clock drift) · `065` (B7 durable clients) · `066` (C1 multi-conn) · `067` (C2 parity) · `068` (C3 losslessness) · `069` (C4 50k synthetic) · `070` (B3 gate) · `071` (B4 signal→intent) · `072` (B5 babysitter) · `074` (A1 re-auth) · `075` (A4 Arrow error half) · `076` (E2 Monday gate) · `077` this package.

---

## 12. Executor crash-window, fencing, reconciliation, single-operator (Saurabh, DEC-044) approval

- **Crash fence:** `tests/crash_exactly_once.rs` — `kill -9` mid-flight, restart on shared durable memory → `DUPLICATE` zero duplicate broker calls — PASS (B2, CHG-063).
- **Fencing/attempt:** `executiongate` 20 tests + `InMemoryGateStateStore.hydrate` / `FlussGateStateStore.read/init` + `FlussGateAttemptStoresIntegrationTest` cross-restart `DUPLICATE` — PASS, live Fluss green `428 common / 18 gateway` (2026-08-21).
- **Reconciliation:** `Order_Correlation` + `Execution_Gate` matrix + `InMemoryAttemptStoreTest` prepare-replay violation → halt — PASS.
- **Single-operator approval (DEC-044):** gate `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED` only on Saurabh + evidence-hash binding; `Safety_Halt_Requests` → `HALTED` within 5 s (B3, `gate::tests` 11 + safety_halt 500 ms).

---

## 13. Approved-policy audit reconstruction simulation (≥1 year) and policy approval

- Differential parity `positions_oracle.json` cross-language (Rust `differential_parity.rs` vs Java `PositionProjectorDriver`) — field-for-field PASS (CHG-052, WP-4).
- `CleanBreakSimulationTest` + `AuditReconstructionSimulationTest` — clean-break replay reconverges — PASS.
- Retention `SAURABH-1Y-APPROVAL-2026-08-20` (T0 bundle) covers the ≥1-year horizon; longer retention is policy-driven per CHG-038/DEC-043.

---

## Binary release gates (per `11-testing-and-release.md` §Release evidence)

| Gate | Status | Evidence ID | Blocker | Pass condition → verdict |
|---|---|---|---|---|
| Data gaps | `NOT_PASSED` | `REL-DG-*` | Live-money | Critical gap open (live sandbox order) → **BLOCKED** — waits on `BI-EQ ×1` market-hours order (A2→A3→A5) |
| Requirements | `NOT_PASSED` | `REL-REQ-*` | Live-money | Contradictions none but live path not yet proven → **BLOCKED** |
| Versions | `EVIDENCE-GATED` | `REL-VER-*` | Live-money | Pins approved, no `latest` — `pin-check` PASS → **PASS** (on laptop) |
| Protocol | `EVIDENCE-GATED` | `REL-PROTO-*` | Live-money | Error half VERIFIED (A1+A4), success half TO_BE_VERIFIED until `BI-EQ ×1` → **BLOCKED** |
| Schema | `NOT_PASSED` | `REL-SCHEMA-*` | Implementation | DDL 26 tables, parity+replay PASS → **PASS** (on laptop) |
| Ingestion | `NOT_PASSED` | `REL-ING-*` | Implementation | 576 tests + losslessness + soak PASS → **PASS** (on laptop) |
| Signal job | `NOT_PASSED` | `REL-SIG-*` | Implementation | Slice 1+2.1 LIVE smoke + checkpoint-failure PASS; 50k live Fluss deferred D5 → **PARTIAL** |
| Action Capture | `NOT_PASSED` | `REL-AC-*` | Implementation | Correlation/lifecycle/quarantine harness PASS, live fill A3 → **PARTIAL** |
| Babysitter | `NOT_PASSED` | `REL-BAB-*` | Implementation | Verify-only harness 4+4 PASS, live restore deferred D-era → **PARTIAL** |
| Executor | `NOT_PASSED` | `REL-EXE-*` | Implementation | Gate/fence/reconciliation PASS, crash-window proven B2, live order deferred A2 → **PARTIAL** |
| Crash window | `NOT_PASSED` | `REL-CRASH-*` | Live-money | B2 exactly-once PASS on durable protocol → **PASS** (laptop) — live bridge composition D-era |
| Safe halt | `NOT_PASSED` | `REL-HALT-*` | Live-money | B3 safety-halt 5 s proven; B8 clock-drift→halt proven → **PASS** |
| Single-operator (DEC-044) | `NOT_PASSED` | `REL-APPROVAL-*` | Live-money | Gate path + hash binding proven; real epoch flip awaits E5 sign-off → **BLOCKED** |
| Capacity | `NOT_PASSED` | `REL-PERF-*` | Live-money | Bridge synthetic 50k PASS; live 50k on prod Swarm D5 → **BLOCKED** |
| HA/recovery | `NOT_PASSED` | `REL-HA-*` | Live-money | DR-001..006 green on single-node mimic, prod quorum deferred D2/D3/D4 → **BLOCKED** |
| EOD/audit | `NOT_PASSED` | `REL-RET-*` | Live-money | Offload parity PASS, real retention/legal-hold E5b → **PARTIAL** |
| Security | `NOT_PASSED` | `REL-SEC-*` | Live-money | Net/secret/image PASS on compose, prod image SBOM + R2 legal-hold E5b → **PARTIAL** |
| Operations | `NOT_PASSED` | `REL-OPS-*` | Live-money | Runbooks exist, thresholds/dashboards await D5 measurements → **PARTIAL** |

No gate with `Blocker: Live-money` prevents this **Approved-for-Testing (sandbox)** package; live production placement stays blocked until the E5 single-operator review and each gate's pass condition is met on prod hardware.

---

## AC → evidence pointers (honest, every required AC has a pointer)

Every row in `docs/02_requirements/09-acceptance-matrix.md` (152 rows) maps to the dossiers + test families in `11-testing-and-release.md` §Requirements traceability. Per-row disposition today:

| AC domain | IDs | Owning dossier | Status today | Pointer |
|---|---|---|---|---|
| `AC-ING-*` | `001–015` | `03-ingestion.md` | 8 VERIFIED on harness + synthetic envelope; 7 `EVIDENCE_BLOCKED`/`NOT_IMPLEMENTED` honestly map to `logs/nautilus-execution/c1-*`+`c3-*`+`c4-*` aggregate and are unblocked by `test/ingestion/*` on prod VMs + market-hours — staff `e5c-fixture-artifacts-20260821.md` | `E5c DONE` + `C1 C3 C4` CHGs; live `test/ingestion/*` deferred D-era |
| `AC-FLS-*` | `001–017` | `02-schema-storage.md` | VERIFIED on live dev Fluss (21/21 DDL, composite-PK matrix, changelog FULL images) | `logs/schema-compat/*` + `C6 341/236/319` |
| `AC-FC-*`/`AC-SS-*` | `001–016` / `001–012` | `04-signal-job.md` | Slice 1+2.1 LIVE smoke + checkpoint-failure PASS; p99 decision-latency out-of-scope (feature-path-only) | `04-signal-job.md` §Status + `logs/tracker-14/sig-perf-001-*` |
| `AC-AC-*` | `001–017` | `05-execution-core.md` | Projection writers + ledger + quarantine live-verified; differential parity PASS; crash/ledger replay harness green; live fill A3 → PARTIAL | `CHG-052/053` |
| `AC-BB-*` | `001–009` | `05-execution-core.md` | Observe-only graph + restore `RETAIN_ON_CANCELLATION` proven; live storm 0-action deferred | `CHG-072` / `b5-*` |
| `AC-EXE-*` | `001–016` | `05-execution-core.md` | Gate/attempt/audit/fencing/reconciliation + crash fence prove exists; live order A2 deferred | `CHG-062..071` / `b1-*..b8-*` |
| `AC-OBS-*` | `001–010` | `10-observability.md` | O2 local audit proven, dashboards/thresholds await D5/D7 | `CHG-055` (CHG-055 record absent — see its tombstone; claim unverified) + D7 deferred |
| `AC-PF-*` | `001–019` | `08/09` | Compose isolation 12/12 + network check PASS; Swarm quorum/replication/1-VM loss/perf on prod VMs deferred | `t8 12/12`, `execution_network_check`  |
| `AC-NFR-*` | `001–012` | cross-cutting | Security/perf halo: pin-check + secret scrub + 50k synthetic PASS; prod perf/legal-hold D-era | `E2 12/12 gate`, `C4` |

No row is blocked **solely for lack of a produced artifact** — any still-blocked row cites its external cause (market-hours or prod VMs) per E5c.

---

## Final approval record (filled at E5)

```text
release_id: 2026-08-21
source_commit: 22371e3 (+ CHG-077/078 this package + E4)
artifact_digests: pinned digests in versions.pin (Go 1.24.5-alpine@sha256:daae04e…, Rust 1.97.1, Flink 2.2.1, Fluss 0.9.1)
schema_manifest: 26 tables (ddl_sha256 + compatibility_class + bucket_key)
version_matrix: 12-version-compatibility-evidence.md rows 7/8/9/10 TO_BE_VERIFIED — E1 BLOCKED
compatibility_result: full-audit + gate + pin-check + cep-check-module 12/12 PASS 2026-08-21 19:44 IST
all_gate_results: logs/soak/monday-gates-20260821-194608/ (ALL GATES PASSED)
open_risks: market-session BI-EQ ×1 (A2/A3/A5) + prod-VM PERF-PROD/FAIL-VM-LOSS/D6/D7 + E5b R2 legal-hold + E5 sign-off
rollback_artifact: AuditHashChain + REJECTED/QUARANTINED rows; rebuild from audit LOG
platform_approval: pending E5
execution_approval: pending E5
security_approval: pending E5
operations_approval: pending E5
compliance_approval: pending E5
first_operator: Saurabh (DEC-044 sole operator)
second_operator: n/a — DEC-044 single-operator
gate_epoch: HALTED (enablement blocked until E5 sign-off)
evidence_hash: see logs/nautilus-execution/*-SHA256SUMS per bundle
enablement_timestamp_utc: not yet — HALTED
```

---

## Definition of done (this dossier)

This package is reviewable today for the **harness + synthetic + dormant** slices. Full `VERIFIED` on every gate/row lands only after the two deferred bundles: **one market-session batch** (`A1-live → A2 → A3 → A5 → A4-success`) and **the VM era** (`D1 → D7`) — then `E1` flips, `E5` signs, and this file plus `00-start-here.md` flip from `Blocked` to `Approved-for-Testing`.
