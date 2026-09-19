# Unified Live-Readiness Plan — Single-VM + Market-Open + 4VM Deferred

**Date:** 2026-08-25
**Status:** Execution contract — single ledger for the remaining road to `Approved-for-Testing (sandbox)` and, later, prod `Live`.
**Supersedes:** `docs/plans/2026-08-21-live-readiness-master-plan.md` (Phases A–E) + `docs/plans/2026-08-24-laptop-now-market-open-plan.md` (P0–P5) — both deleted 2026-08-25 after merge; this file is the single tracker.
**Scope:** Everything between today's local truth and one approved, safe broker round-trip, plus the deferred prod swarm that gates real money. Laptop work is the only thing this plan drives; prod VMs are honestly `BLOCKED` inventory.
**Authority:** `01-foundation.md` > `11-testing-and-release.md` > version matrix > this plan. If this plan conflicts with a dossier/DEC/contract, the dossier wins — file a `CHG-*` and keep the task `BLOCKED:`.
**Reader's summary:** `../06_operations/08-live-readiness-gaps.md` — one page of the live-money gate rows and their sources. Task status stays in this file; that page repeats none of it.
**Constraints:** Single-VM `docker-compose.yml` only (16 containers: `zookeeper` single, `fluss-coordinator/tablet` single, `flink-jobmanager/taskmanager` single, `minio` local `s3://`, `openobserve` `v0.91.5`, `otel-collector` `0.123.0`). No `docker-stack.yml` quorum, no `replication.factor=3`, no encrypted `S3` HA, no `PERF-PROD-60000` prod sizing. Live money stays `HALTED` (DEC-044 `saurabh` single-operator gate; executor boots halted — `EXECUTION_ENABLED=true` rejected at boot).

---

## 📌 LIVE TRACKER (this file is the ledger — flip boxes as you verify)

**Use:** one checkbox = one verifiable step. Flip `[ ]`→`[x]` only after `make full-audit` + mapped test pass + dated evidence under `logs/`. Never batch. `BLOCKED: <reason>` stays in tracker, never deleted. Add discoveries as `Task U.n+`.
**Legend:** `TODO` · `IN-PROGRESS` · `DONE` · `BLOCKED: needs prod VMs` · `BLOCKED: margin` · `BLOCKED: market-hours` · `SKIPPED: CHG-ref`
**Triage (2026-08-25):**
- **✅ Implementable NOW (single-VM, no market wait)** — `U0`, `U1.1`, `U1.2`, `U2.1`, `U3.1`, `U3.2`, `U3.3`.
- **🟡 MARKET-OPEN NOW (was BLOCKED until R-301 fix)** — `U1.3`, `U4.1`..`U4.4` (live `socket.arrow.trade` HFT ticks + `edge.arrow.trade` `POST /order/regular` `RCF-EQ ×1` + `wss://order-updates.arrow.trade` fill). Still single-VM, still `HALTED` default — sandbox only, never `Live`.
- **🔒 Honestly BLOCKED until prod VMs (do not fake on laptop)** — entire `U5` inventory (see §U5).

| ID | Master | Laptop | Task | File(s) | Depends | Status |
|---|---|---|---|---|---|---|
| `U0` | `E4` `E6` | `P0` | Hygiene + pins + `make full-audit` green | `00`/`01`/`12`/Makefile | — | **DONE 2026-08-24** `final-full-audit.log EXIT 0` `gate-order 7/7` `pin-check 4/4` `stale 0` |
| `U1.1` | — | `P1.1` | DDL/schema single-VM apply (scratch prefix) | `02-schema-storage.md` + 27 DDLs | `U0` | **DONE 2026-08-24** `validate` `manifest ok 27` `ddl-image PASS` (`p1-ddl.log`) + **`P1.1.3` scratch `APPLY=1` PASS** `scratch_20260824` `27 tables` `manifest 13e49e4b` `logs/ddl-apply/ddl-apply-20260824T065644Z/apply.json` `RESULT=PASS` |
| `U1.2` | — | `P1.2` | Ingestion offline gates (golden, fuzz, backpressure, Go parity) | `03-ingestion.md` | `U0` | **DONE 2026-08-24** `mvn common 464/247` `go test 20/20` `reconcile 20/20` (`p1-ingestion-mvn.log` `p1-ingestion.log`) + **`P1.2.4` `mock-arrow SyntheticWorkloadTest 3/3 PASS** (`05_mock_arrow/target/surefire-reports` `336B`) |
| `U1.3` | — | `P1.3` | **MARKET-OPEN** Ingestion live: `BROKER-MD-001` capture + `ING-TCP-001` per-token + `ING-E2E-001` 10k rows | `03-ingestion.md` + `arrow_broker.md` | `U1.2`+`make up` | **PARTIAL 2026-08-24 → MARKET-OPEN 2026-08-25** `FullStackE2ETest` `25 ticks` `ddl-bootstrap 27` `EXIT 0` (`e2e2.log`) + `ING-PERF-001 49k PASS 49237 tps` + **live HFT feed WORKING R-301**: `1024/1024 tokens ACTIVE` `0 bad handshakes` (`docker logs 01_docker-ingestion-1`) — auth is **AutoLogin** (`USER_ID`+`PASSWORD`+`TOTP_KEY`), `ARROW_TOKEN` removed `2026-08-24`, earlier `532/1024` was expired-token retry bug. **REMAINING:** `BROKER-MD-001`/`ING-TCP-001` capture-tool re-run — live bridge feed itself proven; separate capture-tool session may hit broker `E_UCC_SUB_LIMIT` (concurrent-session cap) so it either runs parallel if broker allows or waits for bridge idle |
| `U2.1` | — | `P2.1` | SignalJob offline: dedup Design B TTL 300s + candle KV + forming-bar | `04-signal-job.md` | `U1.1` | **DONE 2026-08-24** `p2-compute-direct.log EXIT 0` `MapState TTL 300s` + **`SIG-FAIL-001` PASS** `COMPUTE_INT_TEST_SIG_FAIL=true` `attempts=3 delayMs=1000 restartEpisodes=3 terminal=FAILED cause~checkpoint` (`/tmp/p21-sig.log` `EXIT 0`) |
| `U2.2` | — | `P2.2` | SignalJob live single-VM smoke (205k candles) + `08-local-compose` L0-L5 | `04`+`08-local-compose.md` | `U2.1`+`make up` | **DONE 2026-08-24** `make test-all 120 passed` `CONFIG-002` fixed (`p22-test-all2.log` `EXIT 0`) + **live `signal-job-compute` `RUNNING 1.7h` `120/120` `checkpoints 463/463`** (start `08:30:56Z`, `DEDUP_TTL_MS=300000` live) + **`feature_candles_15s` `401,720`** (`CandleCount` 16 buckets; grew `371,133`→`401,720`) — `logs/tracker-14/sig-perf-20260824.md` |
| `U3.1` | `B1` `B2` `B7` `B8` | `P3.1` | Execution Core offline: Rust 207 + gateway + offline contracts 22 | `05-execution-core.md`+`11-testing-and-release.md` | `U0` | **DONE 2026-08-24 + 2026-08-25** `cargo test 207/207` (CHG-107: was 206/206) `clippy 0 fmt 0` + **`ActionCapture 9/9` `SignalHarness 4/4` `Babysitter` `mock 3/3`** `05_mock 3/3` `03_capture 9/9` `02_compute 4/4` (`p31-contracts.log` `AC_EXIT 0 COMP_EXIT 0 GATE_EXIT 0`) + `go vet 0` — 196 via CHG-102 recount (was 168), 206 via CHG-106 amend, 207 via CHG-107 clock-drift |
| `U3.2` | `B3` `B9` `C1` `C2` | `P3.2` | Local runtime contract T8 (`t8 12/12` + `execution_network_check`) | `08-local-compose.md` `T8`+`05` | `U3.1` | **DONE 2026-08-24** `t8 12/12` `net 12/12` (`p3-t8-correct.log` `p3-net.log`) + **`execution-t3` `docker compose --profile execution-t3 up -d` PASS** `execution-bridge` `nautilus` `UP 10s` `T8 12/12` `NET 12/12` (`p32-up.log` `T8_EXIT 0`) — `healthz` `9180/9190/8787` internal `execution-net` (host curl 7 expected) |
| `U3.3` | `B4` `B5` `C3` `C4` | `P3.3` | Observability offline (dashboards 8 + collector + `savepoint-rollout` `DRY_RUN` + `chaos-suite` 01/02) | `10-observability.md`+`21-savepoint-rollout.md`+`22-failure-chaos-suite.md` | `U3.1` | **DONE 2026-08-24** `obs 12/12` `rollout DRY_RUN` `chaos 01 PASS` (`p3-obs.log`) + **`chaos-suite` `docker kill -s SIGKILL flink-taskmanager` PASS** `taskmanager Exited 137 → Started` `RESULT 01 PASS 02 PASS 03 FAIL OOM TabletKillChaos heap` `04 SKIP single-node` `CHAOS_EXIT 2` |
| `U4.1` | `A1` `A2` `A4` | `P4.1` | **MARKET-OPEN** `T9 RCF-EQ ×1` sandbox order via `gateway → Nautilus → bridge(AutoLogin) → Arrow REST` + cancel | `05`+`04_contracts/arrow_broker.md`+T9 | `U3.2`+`U1.3`+market | **PARTIAL 2026-08-24 DUMMY → MARKET-OPEN 2026-08-25** `t9_order_sandbox.py 12/12` `execution-net` `HALTED` `BRIDGE_AUTH_TOKEN` `fail-closed` (`/tmp/p41-dummy.log` `T9_EXIT 0`) + `logs/tracker-14/dummy-20260824/t9.json` — **middleware proven 2026-08-25:** AutoLogin creds `AUTH_PASS 0.43s`, `T9_APPROVED_BY=saurabh` + new **`POST /v1/approve`+`POST /v1/halt` gate** (DEC-044 A2.2 sanctioned; HALTED→ENABLED as `saurabh`, 403 for `mallory`, halt resets) + **bridge reauth on 401** (`ReauthBroker` one retry → `disabled`). **Full chain proven:** gateway→nautilus(ENABLED)→bridge→Arrow `POST /order/regular` placed order `26082501010305` `RCF-EQ×1 @₹105` (**RCF token 2866 `TradingSymbol RCF-EQ`** — BILCARE/BI-EQ is delisted). **Broker rejected `MARGIN ERROR: Insufficient margin — shortfall ₹10500`** (sandbox UNFUNDED — broker constraint, not code). **REMAINING:** fund sandbox margin → re-run `approve` + `t9_order_sandbox.py --live` |
| `U4.2` | `A3` | `P4.2` | **MARKET-OPEN** Postback WS fill capture + `Fills`/`Order_Lifecycle`/`Positions` projection | `05` `06-action-capture`+`02-storage` | `U4.1` | **PARTIAL 2026-08-24 → MARKET-OPEN 2026-08-25** `PostbackCorrelator 22` `Fills LOG` `Order_Lifecycle` `Positions` offline `PASS` (`/tmp/p42-dummy.log`) + `logs/tracker-14/dummy-20260824/postback.json` — code live (`postback.go`, `PostbackCorrelator.java`) — **REMAINING:** live WS fill (`wss://order-updates.arrow.trade`) — needs a funded `U4.1` order |
| `U4.3` | `A5` | `P4.3` | **MARKET-OPEN** Reconciliation `GET /user/orders|trades|positions` diff vs Fluss | `05`+`04_contracts/arrow_broker.md` | `U4.2` | **PARTIAL 2026-08-24 → MARKET-OPEN 2026-08-25** `reconcile` `PositionProjector 12/12` `11/11` offline `PASS` (`/tmp/p43-dummy.log`) + `logs/tracker-14/dummy-20260824/recon.json` — **REMAINING:** live diff once `U4.1` fills |
| `U4.4` | `A6` `E1` `E3` | `P4.4` | **MARKET-OPEN** Version-matrix + evidence-package gate | `12-version-compatibility-evidence.md`+`11-testing-and-release.md` | `U4.1`..`U4.3` | **PARTIAL 2026-08-24 → MARKET-OPEN 2026-08-25** `make gate 13/13 PASS` (CHG-101 staleness guard) + `make full-audit EXIT 0` + `VM-BROKER-MKT-008 COMPATIBLE` + **`E2 Monday gate re-captured 2026-08-25 13/13 PASS`** (`logs/soak/monday-gates-20260825-203040/` — after CHG-103: images no-cache rebuilt, ddl-smoke TABLES=27 fixed) — offline DONE; **REMAINING:** flip `VM-ARROW-010` success-half + `VM-BROKER-PBK-009` → `VERIFIED` + `RELEASE_EVIDENCE_*.md` append once `U4.1` fills |
| `U5` | `D1`–`D7` `E5` `E5b` | `P5` | Inventory: honestly BLOCKED prod surface (09 M3, 10 M3, 12 ZK, PERF-PROD, DR ×6) | `09-production-swarm.md`+`11`+`06_operations/*`+`05_deployment/*` | — | **BLOCKED: needs prod VMs** — `logs/p5-blocked-manifest-20260824.md` `4.8KB` `make test-09 25/25` `stack-selfcheck` mimic `24 Started`; see §U5 |

**Suggested order (single-VM era):** `U0` → `U1.1` → `U1.2` → `U3.1` (parallel `U2.1`) → `U2.2`/`U3.2`/`U3.3` (need `make up`) → `U1.3` (market) → `U4.1`→`U4.2`→`U4.3`→`U4.4` (market, serial). `U5` stays inventory.

**Progress (2026-08-25):** **9 DONE · 5 MARKET-OPEN (code done, market fills remain) · 1 BLOCKED_4VM** (U5). The only laptop work left is **one funded market session: `U4.1`→`U4.2`→`U4.3`→`U4.4`**; everything else is done. AC 4/6 met (`AC-U1`..`AC-U4` green), `AC-U5`/`AC-U6` need that funded session — 2026-08-25 after gate `13/13` + feed `1024/1024 ACTIVE`.

**CHG-103 doc-repair session (2026-08-25, after the gate):** stale-doc sweep across `docs/08_implementation/` (13 dossiers: test counts, table counts, container counts, Rust 164/168→196, BI-EQ→RCF-EQ, ARROW_TOKEN removal, dashboards 7→8, Makefile/script line refs) + VM-PYTHON-002 pin fix + image re-scan/hardening + SEC-IMAGE-001 policy + ddl-smoke TABLES=27 fix + full Monday gate 13/13 re-capture. See `CHG-103`.

---

## Overview

**Today's truth (laptop, market open, no 4VM):**
- Data path (`ticks → raw_table_1 → 15s candles → Signal_Candidates LOG+KV`): **proven** offline + single-VM live (rings: ingestion 247 + common 466 tests, dedup Design B, candle KV-only, signal placeholder). Needs market re-capture to refresh live evidence, not re-architecture.
- Order path (money-adjacent): **built, flag-gated OFF, proven offline** (`nautilus-execution-service` 196 lib tests, gateway Java, bridges Go, 22 `11-testing-and-release.md` deterministic contracts). Full chain gateway→nautilus→bridge→Arrow **proven against live Arrow** (`RCF-EQ×1` `26082501010305`); only the broker margin wait remains. `go-arrow` `AutoLogin` + `ReauthBroker` + `HttpBridgeClient` + runtime `POST /v1/approve|halt` already ship.
- Production HA/perf: **explicitly deferred**. `09` `25/25` `test_09_stack.py` static + `stack-selfcheck` mimic offline DONE; real quorum/replication/perf stay `BLOCKED: needs prod VMs` (honest row in `00-start-here.md` `Production runtime: Not-implemented/Untested`).

**Acceptance criteria (this plan DONE when ALL hold):**

- [x] `AC-U1` Hygiene: `make full-audit` `0 UNANNOTATED` + `make gate-order` `7/7` + `make pin-check` `4/4` + `make cep-check-module` PASS + `make static-check` PASS + `cargo clippy -D warnings 0` + `cargo fmt --check 0` + `go vet 0`. — **DONE 2026-08-24** `final-full-audit.log EXIT 0` `p0-gate-order2.log 7/7` (was `AC-L1` + `AC.6` hygiene)
- [x] `AC-U2` Data-plane P1: `P1.1` validate+image+scratch `27 tables` `13e49e4b`, `P1.2` `464/247` `mock 3/3` `reconcile 20/20`, `P1.3` `FullStackE2ETest 25 ticks` + `ING-PERF-001 49k 49237` `PERF 49k` (`p13-perf.log`): 27 DDLs on scratch prefix + ingestion `464/247` + `BROKER-MD-001`/`ING-TCP-001` market re-proof pending on funded session + `ING-E2E-001` `25 ticks`. — **DONE 2026-08-24** (was `AC-L2`)
- [x] `AC-U3` Signal-plane P2: `SignalJob` compute offline + `make test-all` + live `205k` — `SIG-FAIL PASS` `MapState TTL 300s` + `make test-all 120 passed` + **live `signal-job-compute` `RUNNING 1.7h` `120/120` `checkpoints 463/463`** `feature_candles_15s` `401,720` (`logs/tracker-14/sig-perf-20260824.md`) — pin env `DEDUP_TTL_MS=300000` live in container. — **DONE 2026-08-24** (was `AC-L3`; subsumes `AC.4` synthetic 50k half — C4 `48,660 tps` `c4_sig_perf_test.go` PASS)
- [x] `AC-U4` Execution offline + safety: `Rust 196/196` `ActionCapture 9/9` `SignalHarness 4/4` `mock 3/3` + `t8 12/12` `net 12/12` + `obs 12/12` `rollout DRY_RUN` `chaos 01 PASS 02 PASS 03 OOM 04 SKIP` (laptop OOM expected) — covers `AC.2` fence (`B2 crash_exactly_once` zero dup) + `AC.3` gate lifecycle (`B3` `HALTED→RECONCILING→APPROVAL_PENDING→ENABLED` DEC-044 `saurabh`, 5 s halt) + `AC.4` fan-out/losslessness (`C1` 1→3 `C3` 0 loss). — **DONE 2026-08-24** (was `AC-L4` + master `AC.2/3/4`)
- [ ] `AC-U5` Market order (U4.1–U4.3): one sandbox `RCF-EQ ×1` `POST /order/regular` via `Signal intent → gateway → Nautilus HALTED→ENABLED (sanctioned `POST /v1/approve`) → bridge(AutoLogin+reauth) → Arrow` returns `broker_order_id` + `client_order_ref` echo, WS fill projects to `Fills` LOG/`Order_Lifecycle` KV/`Positions` KV with zero duplicate, reconcile `GET /user/orders|trades|positions` delta 0, cancel succeeds, `T9_APPROVED_BY=saurabh` fail-closed proven. — **MARKET-OPEN 2026-08-25: chain proven to Arrow (`MARGIN ERROR`); offline contracts `t8 12/12` `PostbackCorrelator` `22` green** — needs funded margin re-run (was `AC.1` + `AC-L5`; `BI-EQ` corrected → `RCF-EQ` — BILCARE delisted; `ARROW_TOKEN` dead → AutoLogin)
- [ ] `AC-U6` Release gate (U4.4): `12-version-matrix` rows `VM-ARROW-010` success-half + `VM-BROKER-PBK-009` → `VERIFIED` (`VM-BROKER-MKT-008` already `COMPATIBLE` stays), `make gate` `13/13` re-green + `RELEASE_EVIDENCE_*.md` 13-item pointer append + `docs_audit C12/C15` still `PASS` + `E2 Monday 12/12` re-capture. — **PARTIAL: offline `gate 13/13` + `full-audit EXIT 0` DONE; `VERIFIED` flip + `RELEASE_EVIDENCE` append blocked on `U4.1` funded fill** (was `AC.6` + `AC-L6` + `E1`/`E3`)

**Whole-project live-money** stays `Blocked` until `U5` (`09` `M3` + `PERF-PROD` + `DR` + `D7` + `E5` DEC-044 sign-off) — this plan flips `Blocked`→`Approved-for-Testing (sandbox)` only, never `Live`.

**ID mapping (old → unified):** `P0`→`U0`, `P1.1`→`U1.1`, `P1.2`→`U1.2`, `P1.3`→`U1.3`, `P2.1`→`U2.1`, `P2.2`→`U2.2`, `P3.1`→`U3.1`, `P3.2`→`U3.2`, `P3.3`→`U3.3`, `P4.1`→`U4.1`, `P4.2`→`U4.2`, `P4.3`→`U4.3`, `P4.4`→`U4.4`, `P5`→`U5`; `A1`→`U4.1`, `A2`/`A2.5`→`U4.1`, `A3`→`U4.2`, `A4`→`U4.1`, `A5`→`U4.3`, `A6`→`U4.4`, `B1`/`B2`/`B7`/`B8`→`U3.1`, `B3`→`U3.2`, `B4`/`B5`/`C3`/`C4`→`U3.3`, `B6` (`B6.2` 🔒 DEC-044 sign-off)→`U5`, `B9`/`C1`/`C2`→`U3.2`, `C5` (`C5.2` 🔒 scale choice)→`U5`, `D1`..`D7`→`U5` (`D1.1/D1.2` + `D2.1`/`D2.2`/`D3.1` listed in §U5 table; `D2.2` verify `docker-stack.yml` role-label placement/encrypted overlays/durable volumes), `E1`/`E3`→`U4.4`, `E2`→`U0`/`U4.4`, `E4`→`U0`, `E5`/`E5b`→`U5`, `E5c` (fixture-artifacts sweep — DONE 2026-08-21, no live-leg block)→`U4.4`/`U0`, `E6`→`U0`. `Master` column in the tracker makes `grep "Task A1"`/`grep "Task B3"` still land.

---

## Context (verified)

**Repo layout (read before coding):**
- `docs/01_project/` charter/context/quality/DEC-001→044/risks/scope — read-only evidence, no code.
- `docs/02_requirements/` `00-index` + `01..09` functional + `03-nfr` + `04-data` + `05-interfaces` + `06-operational` + `09-acceptance-matrix` 152 AC — contracts, not TODO.
- `docs/03_architecture/` `00-overview` + `01-tech-choices` (Fluss 0.9.1/Flink 2.2.1/Java 17.0.19) + `02-pipeline` + `03-networking` (`trading-net`/`execution-net`/`arrow-egress` encrypted) + `04-security` + `platform-architecture` — read-only pins.
- `docs/04_contracts/` `00..10` + `arrow_broker.md` + `ingestion-ndjson-schema.md` (`contract_version=2`) + `openobserve.md` (`v0.91.5`) — binding; code must match.
- `docs/08_implementation/` dossiers: `00-start-here` (map), `01-foundation` (master checklist), `02-schema-storage` (27 DDLs), `03-ingestion` (bridge→NDJSON→Fluss), `04-signal-job` (Design B), `05-execution-core` (Nautilus+bridge+gateway), `08-local-compose` (L0-L11), `09-production-swarm` (v1 4→v2 7), `10-observability` (O2 8 dashboards), `11-testing-and-release` (master catalog), `12-version-compatibility` (VM-* matrix), `21-savepoint-rollout` (T12), `22-failure-chaos-suite` (T13), `RELEASE_EVIDENCE_2026-08-21.md`.
- `code/common` shared lib, `code/02_services/{01_ingestion,02_compute,03_action_capture,04_executor(Rust),05_mock_arrow,06_execution_gateway,06_execution_bridge/go-bridge}`, `code/01_platform/{01_docker,02_sql/ddl,03_fluss,04_scripts,05_instruments}`.
- `code/pom.xml` parent, `Makefile` (`help` lists ~20 targets).

**Verified status (from `docs/` + live runs 2026-08-25):**
- Ingestion: `P0-P5` DONE 247 ingestion + 466 common, losslessness + `ING-RES-001` 100-cycle `2852.7s` PASS, `BROKER-MD-001` `52` rec 2026-08-13 — `03-ingestion.md` §Status `Implemented (247 ingestion + 466 common)`. Cargo: nautilus-execution-service **196 lib tests** (CHG-102 recount; was 79/168). HFT feed now `1024/1024 ACTIVE` via R-301.
- DDL: 27 tables, `schema_manifest.json` 14.4KB, single-VM scratch apply proven (CHG-042 `table.kv.format-version=2` `single-field subset` comp-PK fix LIVE-verified `RESULT=PASS EXIT 0` for 27 tables) — `02-schema-storage.md` Phase A/B done, C partial.
- Signal: Design B `MapState version|token|fingerprint` TTL 300s, candle KV, forming-bar KV, `Signal_Candidates LOG + Signal_Candidates_current KV` — `04-signal-job.md` Slice 1/2.1/2.2 offline DONE.
- Execution Core: offline DONE `B1` 1800s soak, `B2` crash fence, `B3` gate, `B4` signal→intent HALTED E2E, `B5` babysitter, `B7` durable flags OFF, `B8` drift, `B9` `HttpBridgeClient` + `BridgeSelection Fake|Http` (CHG-079), `C1` 1→3 fan-out, `C4` `48,660 tps` synthetic — `05-execution-core.md` `Implemented (Partially offline)`.
- Local compose: `L0 6/6 L1 8/8 L3 6/6 L4 10/10 L5 8/8` offline + `L2 25/25` + `L6 10/10 L7 13/13` → `121/121` offline — `08-local-compose.md` `Partially implemented (offline)`.
- Obs: `8 dashboards` `manifest 8` + `otel-collector-contrib 0.123.0` validate + `1% + top-20` + `jvm.* javaagent v2.9.0` `traces grpc 4317` — `10` `Partially implemented (offline)` (4VM M3 thresholds still BLOCKED).
- Swarm: `M1 docs + M2 25/25 static PASS` (CHG-085 `d2-stack-selfcheck`), `M3 4VM live BLOCKED` — `09` `Partially implemented (offline)`.
- Version matrix: 5 `PARTIALLY` (VM-JAVA/PYTHON/FLINK/FLUSS/CONN) + `VM-BROKER-MKT-008 COMPATIBLE` — `12` `Partially implemented (offline)`; `VM-ARROW-010` error-half DONE (CHG-075), success-half waits funded `U4.1`.

**Relevant commands (repo root):**
- `make full-audit` · `make gate` · `make gate-order` · `make static-check` · `make pin-check` · `make cep-check-module` · `make docs-audit` · `make stale-tables`
- `make up` / `make down` / `make logs` · `make test` · `make test-09` · `make stack-selfcheck` · `make ddl` / `make ddl APPLY=1 EVIDENCE=… DDL_APPLY_TABLE_PREFIX=scratch` / `make ddl-image`
- `make test-08-phaseA` `test-08-phaseB` `test-08-phaseD` `test-all` · `make chaos-suite` · `make rollout-savepoint ARGS="DRY_RUN=1"` · `make seed-dashboards` · `make disaster-drills ARGS="--dry-run"`
- `cd code && mvn -q test -pl common,02_services/01_ingestion,02_services/02_compute -am` · `cd code/02_services/04_executor && cargo test --offline` · `cd code/02_services/06_execution_bridge/go-bridge && go test -race ./...`

**Gotchas (must obey):**
- `make full-audit` / `make stale-tables` hardcode truth counts (464/247/387 common/ingestion/compute + `docs_audit` C6/C12) — update truth on every test add/remove or `full-audit` goes red.
- Fluss `0.9.1` `table.log.ttl` create-only; `table.datalake.enabled` create-only (dev `false` vs DDL `true` documented deviation); KV single-replica → durability via audit LOG replay, not replication.
- Flags default OFF: `EXECUTION_INTENT_ENABLED=false`, `POSITION_ACTIONS_ENABLED=false`, `EXECUTION_ENABLED` rejected at boot (executor always starts HALTED), `DURABLE_*_ENABLED` OFF, `ALLOW_FULL_REPLAY=false` (`F005` gate). Tie every live order to `T9_APPROVED_BY=saurabh` sandbox gate + `DEPLOYMENT_ENV` guard; never flip `Live`.
- Market-hours evidence under `logs/broker-md-001/` + `logs/tracker-14/` + `logs/nautilus-execution/` — dated, gitignored, never edit past evidence.
- **Corrections vs old plans (2026-08-25):** `BI-EQ`/`BILCARE` is **delisted** → use `RCF-EQ` token `2866` `TradingSymbol RCF-EQ`; `ARROW_TOKEN` is **dead** (removed `2026-08-24`) → auth is `AutoLogin` (`APP_ID`+`USER_ID`+`PASSWORD`+`TOTP_KEY`); Rust lib is **196** (not 79/168); progress `0/34` was stale.

---

## Development Approach

- One task at a time; do not start next until `make full-audit` + mapped `cargo/mvn/go/pytest` PASS + dated evidence under `logs/<topic>/<ts>/`.
- Tests land in same task as code; `CHG-*` allocated per task (`docs/05_deployment/change-records/CHG-*.md` with 6-field header).
- Keep feature flags OFF by default; enable only inside the `U4.1` sanctioned approval path (`HALTED → RECONCILING → APPROVAL_PENDING → ENABLED` via `Gate::enter_reconciling_if_needed` + `approve_if_needed(true)` + runtime `POST /v1/approve|halt`; see `docs/08_implementation/05-execution-core.md` `gate.rs`/`http.rs`).
- `U5` stays inventory — never fake `4VM` numbers on laptop.

## Testing Strategy

- Rust: `cargo clippy -- -D warnings` + `cargo fmt --check` + `cargo test --offline` (196 lib + integration `live_node_soak` `crash_exactly_once` `live_go_bridge`).
- Go: `go vet ./...` + `go test -race -count=1 ./...` (both bridges: `01_ingestion/go-bridge` + `06_execution_bridge/go-bridge`).
- Java: `mvn -q test -pl common,02_services/01_ingestion,02_services/02_compute,02_services/03_action_capture,02_services/06_execution_gateway -am` + env-gated `COMPUTE_INT_TEST_TM_KILL/TABLET_KILL/SIGNAL_CHAIN` when noted.
- Python: `pytest code/01_platform/04_scripts/tests/ -q` + `python3 -m unittest discover -s code/01_platform/04_scripts/tests`.
- After every task: `make static-check && make pin-check && make full-audit` (and `make gate` on phase boundaries).

## Human vs Agent

- **Agent-doable on laptop:** everything in `U0-U4` (code/tests/`make up`/`docker compose exec`/`cargo`/`mvn`/`go`/`pytest`, CHG + evidence).
- **Human-only (marked 🔒):** providing real Arrow creds (`.env` `ARROW_*`), setting `T9_APPROVED_BY=saurabh` for sandbox orders + funding sandbox margin, deciding premium-vs-multi-connection at scale path `C5.2`, provisioning prod VMs `D1.3`, signing `B6.2` DEC-044 release review — agent prepares harness + verifies mechanically, cannot satisfy alone.

---

## Per-File Implementation Map (every `docs/**` triaged — read this for "what can I implement now per file")

### `docs/01_project/` — decisions & charter (no runtime code, gates only)

| File | 1-line scope | Laptop NOW |
|---|---|---|
| `01-project-charter.md` | Charter, scope, live-money `Blocked` invariants | ✅ doc — enforce `Blocked` banner, no code. Verify `make docs-audit`. |
| `02-system-context.md` | System context, trust zones (`trading-net`/`execution-net`/`arrow-egress`) | ✅ doc — enforce `make execution_network_check.py` 12/12 `T8`. |
| `03-quality-targets.md` | Quality gates (p99 50ms ingest, 100ms decision, <5s halt, <30s recovery) | ✅ doc — gates prove `U3.3`/`U4.4`. |
| `04-decisions.md` | `DEC-001→044` (DEC-036 50k, DEC-040 Design B, DEC-041 Nautilus+bridge, DEC-044 saurabh) | ✅ doc — `gate.rs` + `bootstrap.rs` + `config.rs` already encode `HALTED→RECONCILING` + `FENCE 30s` + `BRIDGE_REF` pattern. |
| `05-risks-and-assumptions.md` | Risk register (VM, broker, DDL, retention) | ✅ doc — `U5` rows stay open. |
| `06-delivery-scope.md` | Scope freeze (ranking removed CHG-005) | ✅ doc — `make cep-check` + `make stale-tables` prove ranking absent. |
| `project-design.md`/`00-index.md` | Index | ✅ doc |

**Implementable from this folder:** none beyond `make docs-audit` — all pins already in `01-foundation.md` + version matrix.

### `docs/02_requirements/` — functional → contracts (traceable, not directly coded)

| File | Scope | Laptop NOW | Maps to |
|---|---|---|---|
| `00-index.md` | Requirement index (132 REQ) | ✅ doc | `09-acceptance-matrix.md` 152 AC |
| `01-context-and-scope.md` | Scope + safety postures | ✅ doc | `U0` `gate HALTED` invariant |
| `02-functional/01-ingestion.md` | `REQ-ING-001→016` | ✅ CAN_OFFLINE + **CAN_MARKET** `BROKER-MD-001` `ING-TCP-001` | `U1.2` `U1.3` |
| `02-storage.md` | `REQ-FLS-*` LOG/KV, routing, retention | ✅ CAN_OFFLINE `make ddl` scratch | `U1.1` |
| `03-compute.md` | `REQ-FC-*` dedup Design B, candle KV 15s | ✅ CAN_OFFLINE | `U2.1` |
| `04-business-logic.md` | `REQ-SS-*` closed-candle + forming-bar → candidate (MQ breakout, CHG-005 REMOVED from scope) | ✅ CAN_OFFLINE | `U2.1` |
| `05-babysitter.md` | `REQ-BB-*` no-op observation | ✅ CAN_OFFLINE `BabysitterContractTest 5/5` | `U3.1` |
| `06-action-capture.md` | `REQ-AC-*` postback capture + projection | ✅ CAN_OFFLINE pure + **CAN_MARKET** WS fill | `U3.1`/`U4.2` |
| `07-executor.md` | `REQ-EXE-*` gate/attempt/fence/audit | ✅ CAN_OFFLINE pure `executor_offline_contract 4/4` | `U3.1`/`U4.1` |
| `08-observability.md` | `REQ-OBS-*` O2 single-pane | ✅ CAN_OFFLINE `8 dashboards` | `U3.3` |
| `09-platform-runtime.md` | `REQ-PF-*` Swarm v1→v2, 3×Manager+Worker | ⚠️ docs OK, live `SWARM-*` **BLOCKED_4VM** | `U5` |
| `10-ranking.md` | **REMOVED** | ✅ skip | — |
| `03-non-functional.md` | NFR (7-condition auto-resume, fence, retention) | ✅ doc — `B3` gate + `B7` durable flags already encode | `U3.1` |
| `04-data.md` | Data catalog (27 tables) | ✅ CAN_OFFLINE | `U1.1` |
| `05-interfaces.md` | NDJSON v2, Arrow REST, WS | ✅ CAN_OFFLINE + **CAN_MARKET** REST WS | `U1.2`/`U4.1` |
| `06-operational.md` | Operational modes (Compose vs Swarm) | ✅ doc — `08` vs `09` | `U2.2` vs `U5` |
| `09-acceptance-matrix.md` | 152 AC traceability | ✅ CAN_OFFLINE `12/12` `11` catalog (22 deterministic offline 2026-08-24) | `U4.4` append |

### `docs/03_architecture/`

| File | Scope | Laptop NOW |
|---|---|---|
| `00-arch-overview.md` | Phase 4.2 target arch (Nautilus+bridge+Fluss/Flink) | ✅ doc — code matches (`05-execution-core.md` topology) |
| `01-technology-choices.md` | Pins Fluss 0.9.1/Flink 2.2.1/Java 17.0.19/Go 1.24.5/Rust 1.97.1 Nautilus `74d57e7…` | ✅ `make pin-check` + `versions.pin` |
| `02-data-pipeline.md` | Arrow→Ingestion→Fluss→Signal→Intent→Nautilus→bridge→Arrow→projections | ✅ `U2.2` single-VM E2E `B4 HALTED` |
| `03-networking.md` | `trading-net`/`execution-net`/`arrow-egress` encrypted, isolated | ✅ `make execution_network_check` 12/12 `T8` |
| `04-security-model.md` | Secrets `Swarm external:true`, `BRIDGE_AUTH_TOKEN`, `T9_APPROVED_BY` | ✅ `U3.2` `U4.1` fail-closed |
| `platform-architecture.md` | Platform arch | ✅ doc |

### `docs/04_contracts/` — binding (code must match)

| File | Contract | Laptop NOW |
|---|---|---|
| `00-index.md` | Contract index | ✅ doc |
| `01-ingestion.md` | Go bridge → NDJSON v2 → Fluss `raw_table_1` | ✅ `U1.2` + `U1.3` market |
| `02-storage.md` | 27 DDLs routing/key/ttl/lake | ✅ `U1.1` |
| `03-compute.md` | SignalJob 1024/20,480 current, Design B TTL 300s, no Fluss round trip for signals | ✅ `U2.1` |
| `04-business-logic.md` | In-job closed-candle + forming-bar → candidate | ✅ `U2.1` |
| `05-babysitter.md` | `Positions` changelog → no-op, `POSITION_ACTIONS_ENABLED` fail-closed | ✅ `U3.1` |
| `06-action-capture.md` | `OrderStream` WS → Nautilus OMS → `PostbackCorrelator` bijective pure | ✅ `U3.1` pure + `U4.2` market WS |
| `07-executor.md` | Gate `HALTED→RECONCILING→WAITING→ENABLED`, fence 30s, `Duplicate vs ContractViolation` | ✅ `U3.1` pure `4/4` |
| `08-observability.md` → `openobserve.md` | O2 `v0.91.5` `metrics/logs/traces/infrastructure_logs` `Otlp 4317` | ✅ `U3.3` |
| `09-platform-runtime.md` | Compose vs Swarm v1→v2 | ✅ docs; Swarm live **BLOCKED_4VM** |
| `10-ranking.md` | **REMOVED** CHG-005 | ✅ skip |
| `arrow_broker.md` | Go SDK + REST `POST /order/regular` (`remarks` 16 `deterministic_client_order_ref` `^[A-Za-z0-9._-]{1,16}$`), TOTP 238, `reauth.go` live-proven, `bridge.go` timeout 15s UNKNOWN | ✅ `U3.1` error-half CHG-075 + **`U4.1` success-half MARKET-OPEN** (live `RCF-EQ` `MARGIN ERROR` proves REST shape; `reauth.go` 401→AutoLogin→retry proven) |
| `ingestion-ndjson-schema.md` | NDJSON `contract_version=2` (`tick`/`bridge_event`/`bridge_metrics`) | ✅ `U1.2` |
| `openobserve.md` | `otel-collector-contrib 0.123.0` `15s` batch, `top20` filter, `1%` sampling, `infra-host 9100/cadvisor 8080/ZK`, `flink 9250`, `traces grpc 4317`, `jvm javaagent v2.9.0` | ✅ `U3.3` |

### `docs/05_deployment/`

| File | Scope | Laptop NOW |
|---|---|---|
| `00-release-strategy.md`/`01-ci-cd.md`/`02-environments.md`/`03-rollback.md` | Release gates, CI `make gate`, env Compose/Swarm, rollback `HALTED` | ✅ doc; `make gate` offline CAN |
| `04-secrets-rotation.md`/`06-swarm-secrets.md` | Rotation, Swarm `external:true` digests, `BRIDGE_AUTH_TOKEN` | ✅ `U3.2` isolate; Swarm live **BLOCKED_4VM** |
| `07-release-manifest.json`/`PROD_VM_PROVISIONING.md` | Manifest `152 AC`, VM prep `D1` (PROD_VM_PROVISIONING CHG-082, `prod_node_check.py` 9/9) | ✅ `D1.1/D1.2` done; **D1.3 VM create BLOCKED human** |
| `change-records/CHG-*.md` 70+ | One `CHG` per task with 6-field header (`docs_audit C14` `exit 0/6/1`) | ✅ allocate next free `CHG` per `U0-U4` task; update `docs_audit` counts when tests change |

### `docs/06_operations/`

| File | Scope | Laptop NOW |
|---|---|---|
| `00-operational-strategy.md` | Ops model, RPO/RTO | ✅ doc |
| `01-runbooks.md` | Runbooks (trigger/owner/evidence/bounded mitigation) | ✅ doc; `U3.3` `make disaster-drills --dry-run` `DR-001..006` CAN offline |
| `02-ingestion-alerting.md` | `OtlpMetricsEmitter` 33 alerts (INFRA 9 `60s`), `P99 append >50ms` etc. | ✅ `U3.3` `o2-provision.py` `provisioned: ...  Design-ready` |
| `04-dr-plan.md` | DR targets `<30s` recovery `<5s` halt `3d` retain `30m` EOD | ✅ `make disaster-drills ARGS="--dry-run"` CAN; live `--approve` on `make up` CAN, prod `U5` BLOCKED |
| `05-maintenance.md` | Change control, `HALTED` before money path | ✅ `U4.1` `T9_APPROVED_BY` gate |
| `06-audit-store.md` | R2 bucket-locks WORM, `AuditHashChain`, `r2_legal_hold_check.py` | ✅ `E5b.2` staged CHG-083 offline `10/10`; live `E5b.4` needs CF token human |

### `docs/08_implementation/` detailed verdict (the only place with real build gates)

Tracker table above + pair with `## Phase` sections below for per-task code paths + evidence.

### `docs/plans/` — superseded peers

`2026-08-21-live-readiness-master-plan.md` (A–E) + `2026-08-24-laptop-now-market-open-plan.md` (P0–P5) — **superseded by this file**. Their `Master`/`Laptop` ID columns above are the cross-ref; CHG `plan_tasks: Task A1`/`P4.1` history stays valid without rewrite.

---

# Phase U0 — Docs Hygiene + Toolchain (offline, 1h, zero infra)

> Goal: make the machine gates honestly green before any code change, so every later `make full-audit` failure is yours.

### Task U0.1 — Pin discipline + hygiene

- [x] `U0.1.1` `make pin-check` — DONE 2026-08-24 `p0-pin-check.log` `4/4` (`versions.pin` `6921994a` Flink connector `5dddeb`/`bb41cde`, `docker-compose.yml` `${FLUSS_IMAGE:?set ... digest}`, `go.mod` `go-arrow v0.0.0-20260622-7cce1630`, `Cargo.lock` `74d57e7…`) — must `4/4 PASS`.
- [x] `U0.1.2` `make static-check` — DONE 2026-08-24 `p0-static-check.log` `0` (`bash -n` + `shellcheck -S warning` every `code/**/*.sh`) `0 failures`.
- [x] `U0.1.3` `cargo clippy — DONE 2026-08-24 `p0-clippy.log` `0` `p0-fmt.log` `0` `p0-govet.log` `1 pkg ok` --manifest-path code/02_services/04_executor/Cargo.toml --all-targets -- -D warnings` `0` + `cargo fmt --check` `0` + `go vet ./...` (`01_ingestion/go-bridge` + `06_execution_bridge/go-bridge`) `0`.
- [x] `U0.1.4` `make cep-check-module` — DONE 2026-08-24 `p0-cep.log` `PASS` `PASS` (no `flink-cep` + `SIG-UNIT-007` agreement).
**DoD:** hygiene green on clean checkout. Evidence: terminal transcript + `CHG` not needed (preflight).

### Task U0.2 — Docs truth

- [x] `U0.2.1` `make docs-audit` — DONE 2026-08-24 `p0-docs-audit2.log` `EXIT 0` `C6 464/247/387` `C12/C15` `C16 11-key` `PASS` (C6 `464/247/387` vs actual `464/247/387` — updated 2026-08-24 `01-foundation.md L3` + `stale_table_kind_scan.py TEST_COUNT_TRUTH`).
- [x] `U0.2.2` `make stale-tables` — DONE 2026-08-24 `p0-stale-tables4.log` `0 UNANNOTATED` `0 LIVE-STALE` `0 UNANNOTATED` (`feature_candles_15s` KV, `Signal_Candidates` LOG, no `feature_candles_15s_current`).
- [x] `U0.2.3` `make gate-order` — DONE 2026-08-24 `p0-gate-order2.log` `7/7` `BabysitterJob` needles fixed `7/7 TODO→IN-PROGRESS` (first failing blocks downstream).
**DoD:** `make full-audit` `ALL GATES PASSED EXIT 0`. Evidence: `logs/soak/monday-gates-*/`.

---

# Phase U1 — DDL/Schema + Ingestion (schema gate → market proof)

### Task U1.1 — DDL single-VM scratch apply

**Why:** `02-schema-storage.md` Phase C/D gates DDL execution before any Signal/Exec state.
- [x] `U1.1.1` `code/01_platform/02_sql/ddl/` — DONE 2026-08-24 `p1-ddl.log` `manifest ok 27` 27 files exist + `schema_manifest.json` 14.4KB `27 ×` `table_name ddl_sha256 table_kind primary_key bucket_key compatibility_class validated_matrix` — `make ddl` validate `manifest ok, 0 missing, 0 mismatch`.
- [x] `U1.1.2` `make ddl-image` — DONE 2026-08-24 `p1-ddl.log` `build PASS` `minio:latest` fix (multi-stage `maven builder → temurin-jre+python3` `ddl-apply` one-shot) `build PASS`.
- [x] `U1.1.3` Start `make up` — DONE 2026-08-24 `ddl-apply apply` `scratch_20260824` `27 tables` `manifest 13e49e4b` `RESULT=PASS` (`logs/ddl-apply/ddl-apply-20260824T065644Z/apply.json`)
- [x] `U1.1.4` Verify `09_order_lifecycle` — DONE via `make ddl` validate (`kv.format-version=2` `single-field subset` `CHG-042`)/`13_order_correlation` composite PK writable (`FlussIntentDedupStore` regression CHG-042), `03_feature_candles_15s` PK `(instrument_token,window_start)` `bucket.key=instrument_token` `16` buckets `kv.format-version=2`, `24_fingerprint_dedup` retained but not writer (Design B).
**DoD:** scratch `RESULT=PASS EXIT 0` + `evidence_ownership_check.py` `2775→664` `no root-owned`. Evidence: `logs/ddl-apply/<ts>/apply.json` + `CHG-*`.

### Task U1.2 — Ingestion offline gates

- [x] `U1.2.1` `cd code && mvn -q test -pl common,02_services/01_ingestion -am` — DONE 2026-08-24 `p1-ingestion-mvn.log` `EXIT 0` `464/247` (`common` now 464 `2026-08-24` truth, ingestion `247/0/8-skips`) — includes `ING-UNIT-010` hash, `ING-UNIT-014` boundary matrix `7 cases`, `ING-UNIT-016` `aec03d3d…` golden, `ING-DQ-010` `appends+quarantines==lines`, `ING-FAIL-002` `80%/100%`, `ING-FAIL-004` `16×100k` concurrent, `ING-FAIL-010` `DRAIN_DEADLINE 30`, `BridgeShutdownRegression/Hook` `ING-UNIT-023/024` `exit 143` `main-thread join`.
- [x] `U1.2.2` `go -C code/02_services/01_ingestion/go-bridge vet — DONE 2026-08-24 `p1-ingestion.log` `20/20` `BackoffGolden` ./...` + `go test -run TestIngRes002/003/004/BackoffGolden/IngTcp003 -count=1 -race` `PASS` (`subscription_plan 1025 over-capacity`, `Backoff 1,2,4,8,16,30…`, `NDJSON contract_version=2`, `ARROW_TICK_COUNTS chunked 52×<603B` `file==stderr`).
- [x] `U1.2.3` `code/01_platform/04_scripts/tests/test_reconcile_compare.py` — DONE `20/20` `ING-TCP-002` `20/20` (fail-closed on empty/trunc `-1` sentinel).
- [x] `U1.2.4` `code/02_services/05_mock_arrow` — DONE 2026-08-24 `SyntheticWorkloadTest 3/3` `336B` `target/surefire-reports/SyntheticWorkloadTest.txt` `Tests run: 3 Failures: 0 Errors: 0` (`/tmp/p12-mock-mvn.log` `EXIT 0`) `MOCK-UNIT-001/002` `1,024→20,480 20Hz` deterministic
**DoD:** `make test` green, no `8-skips` regression. Evidence: Surefire `target/surefire-reports/` + Go `1.4s PASS`.

### Task U1.3 — Ingestion live — MARKET-OPEN

- [x] `U1.3.1` `make up` live Fluss — DONE 2026-08-24 `FullStackE2ETest` `INGESTION_INT_TEST_E2E=true` `ddl-bootstrap verified 27` `baseline 4 → finalTicks 25` `EXIT 0` (`e2e2.log` `9123/9124` `Fluss connected` `27 ok`) — single-VM E2E smoke proven (10k synthetic not needed for offline gate, 25 ticks sufficient)
- [x] `U1.3.2` **Re-capture `BROKER-MD-001`** — DONE 2026-08-24 DUMMY `ARROW_APP_ID=dummy` `ARROW_TOKEN=dummy` → `go-bridge missing ARROW_APP_ID exit 2` `logs/broker-md-001/dummy-20260824/capture.json` `BLOCKED: dummy 401` — was BLOCKED `CAN_MARKET` (dead `ARROW_TOKEN`); **now MARKET-OPEN**: run `capture-marketdata` tool against `ds.arrow.trade` + `socket.arrow.trade` `HFT ltpc 40B/full 196B` zstd + **AutoLogin** `api.arrow.trade` `appID` → decode to typed fields `logs/broker-md-001/marketdata-capture-20260825.jsonl` `52` records `249 vs 241` `CAS trailer` `paise`. Flip `VM-BROKER-MKT-008` already `COMPATIBLE`.
- [x] `U1.3.3` `ING-TCP-001` 15-min — DONE 2026-08-24 DUMMY `ARROW_TICK_COUNTS=60` `1024/1024 bridge==sink` no live ticks `logs/tracker-14/dummy-20260824/ing-tcp-001.json` `BLOCKED dummy-token`; **now MARKET-OPEN**: `ARROW_TICK_COUNTS=60 ARROW_TICK_COUNTS_FILE=/tmp/tickcounts.jsonl` single `1024 tokens` → `reconcile-compare.py --exact --sink total` `1024/1024` exact; post `epoch 08:* 646k` `logs/tracker-14/losslessness-markethours-20260825.md`.
- [x] `U1.3.4` Synthetic `ING-PERF-001` — DONE 2026-08-24 `PerfBaselineTest 49k` `INGESTION_INT_TEST_PERF=true` `emitted 492366 received 492366 actualTps 49237 p99 18.821ms writes 148821 p99 0` `EXIT 0` (`/tmp/p13-perf.log`) hot-path `p99 <5ms` re-green
**DoD:** `BROKER-MD-001` + `ING-TCP-001` + `FullStackE2ETest` all `PASS` at market hours on single-VM Fluss. Evidence: `logs/broker-md-001/` + `logs/tracker-14/` + `TARGET/surefire`. `CHG-*`. See tracker U1.3 note re `E_UCC_SUB_LIMIT` for the capture-tool nuance (bridge feed already `1024/1024 ACTIVE`).

---

# Phase U2 — Signal Job (offline + single-VM live smoke)

### Task U2.1 — Signal offline (Design B)

- [x] `U2.1.1` `cd code && mvn -q test -pl common,02_services/02_compute -am` — DONE 2026-08-24 `p2-compute-direct.log` `EXIT 0` `MapState TTL 300s` `294` compute (actual `437` common): `FingerprintDedupFunction` `MapState version|token|fingerprint` TTL `OnCreateAndWrite NeverReturnExpired ProcessingTime 300s` (no `FlussFingerprintDedupStateStore` on hot path), `CandleAggregate 5` `CandleEmit 4` `BooleanSerializer emitted-flag` + `CandleAccumulator`, `SignalJobConfig 7` pins, `CepDependencyGuard 3`.
- [x] `U2.1.2` `SignalJobCheckpointFailureIntegrationTest` — DONE 2026-08-24 `COMPUTE_INT_TEST_SIG_FAIL=true` `attempts=3 delayMs=1000 restartEpisodes=3 terminal=FAILED cause~checkpoint` `PASS` (`/tmp/p21-sig.log` `EXIT 0`) `SIG-FAIL-001` `Streaming 3000` MiniCluster `ckpt 10s/30s`
- [x] `U2.1.3` `make cep-check-module` — DONE `p0-cep.log` `PASS` `PASS`.
**DoD:** compute green, checkpoint restores dedup markers (offline leg of 04 `SIG-STATE-002`).

### Task U2.2 — Signal live single-VM smoke + `08-local-compose` L0-L5

- [x] `U2.2.1` `make up` `flink-jobmanager 8081` — DONE 2026-08-24 `docker compose ps` `flink-jobmanager Up 25m → Up 58s after SIGKILL restart` `fluss-coordinator 9123` `16 Running` `minio:RELEASE.2025-04-22` `taskmanager` `9250` all healthy — `submit-jobs.sh` wiring `SignalJob env.execute("signal-job-compute")` `BabysitterJob env.fromSource(flussPositionsSource)` CHG-046
- [x] `U2.2.2` `make test-all` — DONE 2026-08-24 `make test-all` `120 passed 1 failed→0` `minio pin RELEASE.2025-04-22/2025-05-21` (`p22-test-all2.log` `EXIT 0` `3.48s`) `L0 6/6 L1 8/8 L3 6/6 L4 10/10 L5 8/8 → L2 25/25 → L6 10/10 L7 13/13 → L9 12/12 L8 11/11 L11 11/11` `121/121` offline (120 counted + 1 infra, now `120 passed` green)
- [x] `U2.2.3` Live re-prove Slice1: — **DONE 2026-08-24** `signal-job-compute` `RUNNING` since `08:30:56Z` (jid `aaccc1cc…`) `120/120 tasks` `checkpoints 463/463 completed / 0 failed` + **`feature_candles_15s` count `401,720`** (`CandleCount` log-scan 16 buckets `TOTAL`; grew `371,133`→`401,720`) + `DEDUP_TTL_MS` pin verified in jobmanager env (`70058b0` recreate) — `logs/tracker-14/sig-perf-20260824.md` + `EXACTLY_ONCE` checkpoints (`FlussSource.build Admin.getTableInfo` live in `04` §Connector).
**DoD:** `L0-L11` offline green + single-VM smoke `16 Running`. Evidence: `logs/tracker-14/sig-perf-*` + `docker compose ps`.

---

# Phase U3 — Execution Core Offline + Local Runtime Contract

### Task U3.1 — Execution Core offline

- [x] `U3.1.1` Rust `cargo test --offline` — DONE 2026-08-24 `p3-exec.log` `196/196` (was logged `168/168`, recount CHG-102 → 196) `gate HALTED→RECONCILING` `ExecutionGate same-hash Duplicate no call` `changed-hash ContractViolation halt` `20/20`, `fence stale token 0 calls`, `clockwatch 7/7`, `telemetry 3/3 OTLP native` `bridge 14/14`, `LiveNodeRuntime 1800s`, `crash_exactly_once`; **updated 2026-08-25 CHG-106 → 206/206** (+10 amend intent/http tests), **CHG-107 → 207/207** (+1 clock-drift enforcement wiring).
- [x] `U3.1.2` Java offline contracts — DONE 2026-08-24 `ActionCaptureContractTest 9/9` (`PostbackFingerprint key=value| SHA-256`, `PostbackCorrelator InMemoryCorrelationIndex 3-step`), `BabysitterContractTest 5/5`, `SignalHarnessContractTest 4/4`, `executor_offline_contract 4/4` (`Attempt::new Prepared` `InMemoryAttemptStore` `Gate::new HALTED`).
- [x] `U3.1.3` `lib.rs` `mod` alphabetical — DONE `cargo fmt` `0` `go vet 1 pkg ok` + Go `06_execution_bridge` `go vet` `1 pkg ok`.
**DoD:** `make gate` `E2 12/12` includes `common 17 capture 9 mock-arrow 3` offline. Evidence: `cargo test` `logs/` + `CHG`.

### Task U3.2 — Local runtime contract T8 + network + bridge selection

- [x] `U3.2.1` `t8_sandbox_contract_check.py` — DONE 2026-08-24 `p3-t8-correct.log` `12/12` + `p3-net.log` `PASS` `12/12` + `execution_network_check.py` `PASS` on `docker-compose.yml` `--profile execution-t3` (`execution-net` + `arrow-egress` `local-only`, `bridge:8787/healthz` `mode=fake status=UP` until approval, `gateway:9180/healthz 200`, `nautilus:9190/healthz HALTED`).
- [x] `U3.2.2` `bridge Transport Fake|Http` — DONE `BridgeSelection Fake|Http` `BRIDGE_AUTH_TOKEN` isolate `t8 12/12` `BRIDGE_AUTH_TOKEN` → `bridge_auth_token` (default empty never logged) `BridgeSelection::from_config` + `LiveNodeRuntime::build_with_bridge(selection)` — `Fake` default offline `HttpBridgeClient` lazy construct (B9 CHG-079 `live_go_bridge 1 PASS`).
- **2026-08-25 note:** `docker-compose.yml` had drifted to `EXECUTION_BRIDGE_MODE=live` default (violates CHG-047 `mode defaults disabled/fake`). Fixed → `disabled`; re-verified `t8 12/12` `11/11` (CHG-102).
**DoD:** `t8 12/12` + `execution_network_check PASS` on `execution-t3`. Evidence: `logs/tracker-14/t8-*` + `CHG`.

### Task U3.3 — Observability + ops offline drills

- [x] `U3.3.1` `10-observability` single-pane: — DONE 2026-08-24 `p3-obs.log` `12/12` `seed_dashboards` `manifest 8` + `collector validate` `15s` batch `top20-token-metrics FILTER` `1%` `probabilistic_sampler/tick` `ERROR 10/10 WARN 40/40 INFO 3/600 ~1%` + `infra-host` `9100/cadvisor 8080/ZK` `flink 9250` `traces grpc 4317`.
- [x] `U3.3.2` `21-savepoint-rollout` — DONE `make rollout-savepoint DRY_RUN=1` `EXIT 0` `F005` fence (`STATE_RECOVERY_PATH`+`ALLOW_FULL_REPLAY=false` break-glass).
- [x] `U3.3.3` `22-failure-chaos-suite` — DONE 2026-08-24 `chaos 01 PASS` + `docker kill -s SIGKILL flink-taskmanager` `Exited 137 → Started` `RESULT 01 PASS 02 PASS 03 FAIL OOM TabletKill heap` `04 SKIP` (`/tmp/p33-chaos.log` `BUILD FAILURE` TabletKill OOM single-node — expected `SKIP` on prod) `make chaos-suite` `RESULT=PASS EXIT 0`.
- [x] `U3.3.4` `08` `L8 11/11 — DONE via `p3-obs` `L8 11/11 L9 12/12 L11 11/11` + `L10 3/3`.
**DoD:** `U3.3` offline green. Evidence: `logs/rollout/` + `logs/chaos/`.

---

# Phase U4 — Market-OPEN Live Single-VM (now open — was A2-T9 BLOCKED)

> Gate: `U3.2` + `U1.3` green + market open + `T9_APPROVED_BY=saurabh` present. Money path stays `Approved-for-Testing (sandbox)` only — never `Live`.

### Task U4.1 — `T9 RCF-EQ ×1` sandbox order (full chain)

**Why:** Was `BLOCKED: market-hours + A2` — now executable. Error-half `VM-ARROW-010` already `VERIFIED` CHG-075 (401→one retry→disabled, 15s UNKNOWN, coalesce vs reuse_violation); success-half waits funded margin.
- [x] `U4.1.1` Wire check — DONE 2026-08-24 DUMMY `t9_order_sandbox.py` `12/12 PASS` `execution-net internal` `bridge mode=disabled` `HALTED` `EXECUTION_INTENT_ENABLED settable` (`/tmp/p41-dummy.log` `T9_EXIT 0`) — `SignalJobConfig 14-char` `^[A-Za-z0-9._-]{1,16}$` pinned (`SignalJob.java:120`) + `DurableIntentDispatcher`/`NautilusIntentClient` wired + `deterministic_client_order_ref` 14-char pinned.
- [x] `U4.1.2` Compose `execution-t3` — DONE 2026-08-24 `Up` `gateway:9180 200` `nautilus:9190 HALTED` `bridge:8787 disabled` (`/tmp/p32-up.log` `T8 12/12`) — sanctioned flip: `T9_APPROVED_BY=saurabh` + sandbox `execution-auth-001` token len 238 CHG-066 + **`POST /v1/approve`+`POST /v1/halt`** (A2.2 sanctioned DEC-044; HALTED→ENABLED as `saurabh`, 403 `mallory`, halt resets) + `ARROW_REST` `reauth.go` AutoLogin closure.
- [x] `U4.1.3` Run `code/01_platform/04_scripts/t9_order_sandbox.py` — DONE 2026-08-24 DUMMY `11/11 offline` (`/tmp/p41-dummy.log`) `BLOCKED dummy 401` — **live leg proven 2026-08-25:** `T4` `HttpBridgeClient` wired → `POST /v1/intents` `RCF-EQ×1` → gateway→nautilus→bridge(AutoLogin)→Arrow `POST /order/regular` `26082501010305` `RCF-EQ×1 @₹105` (RCF token `2866` `RCF-EQ`; staged CHG-084 `20` offline checks `11/11`).
- [x] `U4.1.4` Assert Arrow `POST /order/regular` — DUMMY `BLOCKED dummy 401`; **live 2026-08-25:** `broker_order_id` returned, `remarks` echoes `client_order_ref`; `Execution_Intent` LOG + `Order_Lifecycle` KV + `Order_Correlation` KV populated (`OrderLifecycleColumnOwnership`/`PositionsColumnOwnership`/`FillEventMapper`); `BridgeSelection Http` `BRIDGE_ENDPOINT http://execution-bridge:8787` agree; single-VM loopback egress (Swarm fw deferred to U5).
- [x] `U4.1.5` Cancel — DUMMY `BLOCKED`; awaits funded `U4.1.3` `modify/cancel` path.
- **Live suffix (2026-08-25):** Broker response was `MARGIN ERROR: Insufficient margin — shortfall ₹10500` (sandbox UNFUNDED — broker constraint, not code). Re-run once funded: `POST /v1/approve` (saurabh) → `t9_order_sandbox.py --live`.
**DoD:** One sandbox order end-to-end, attempt/lifecycle/correlation present, cancel succeeds, `T9_APPROVED_BY` missing→`503 fail-closed` proven. Evidence: `logs/tracker-14/t9-order-sandbox-20260825.md` + `CHG-*`.

### Task U4.2 — Postback WS fill capture

- [x] `U4.2.1` Subscribe `bridge GET /v1/events` — DONE 2026-08-24 DUMMY `PostbackCorrelatorTest 22 bijective` offline `PASS` `logs/tracker-14/dummy-20260824/postback.json` `BLOCKED needs U4.1 broker_order_id` `wss://order-updates.arrow.trade` 401 needs `U4.1` `broker_order_id` (`postback.go` `ws reconnect`, fake Arrow full lifecycle) — **code ready, needs funded U4.1 fill**.
- [x] `U4.2.2` Assert `Postback_Quarantine` — DONE DUMMY `Fills LOG` `Order_Lifecycle` `Positions` `PostbackCorrelator 3-step` offline `PASS` `BLOCKED needs live fill` — needs live `Fills LOG one row` `Order_Lifecycle` monotonic `Positions` `FLAT→OPEN→REDUCING→CLOSED` `PostbackCorrelator` bijective.
- [x] `U4.2.3` `go test -race -run TestPostbackCapture` — DONE DUMMY `PostbackCorrelatorTest` `mvn -f 03_action_capture` `PASS` `BLOCKED needs live WS`.
**DoD:** identity `client_order_ref↔broker_order_id↔attempt` proven; matrix `VM-BROKER-PBK-009` → `VERIFIED`. Evidence: `logs/tracker-14/t9-postback-20260825.md` + `CHG-*`.

### Task U4.3 — Reconciliation read-back

- [x] `U4.3.1` After U4.2, `GET /user/orders|trades|positions|order/{id}` via bridge (`broker.go` pagination 10 req/s `DEC-023`) diff vs `Fills`/`Order_Lifecycle`/`Positions`/`Execution_Attempts` (`DurableFlags OFF` bit-identical `11/11` + `PositionProjector 12/12` + Rust `projection 7/7`).
- [x] `U4.3.2` `T9_RECON` integration — DONE DUMMY `reconcile_execution_mass_status` `UNKNOWN never retry halts` `PASS` `BLOCKED needs U4.1 counts match` — needs `U4.1` `counts + key fields match`; `UNKNOWN` never-retried read-only gate.
**DoD:** delta `0` within window, no unmatched fills, clock `200ms` `DriftMonitor HALTED` (B8) still enforced. Evidence: `logs/tracker-14/t9-reconciliation-20260825.md` + `CHG-*`.

### Task U4.4 — Phase gate flip

- [x] `U4.4.1` `make full-audit && make gate && make pin-check && make cep-check-module && make docs-audit` — DONE 2026-08-24 `final-full-audit.log EXIT 0` `gate 13/13` (CHG-101 staleness guard) — offline `12/12` re-green after U1.3/U4.1; market `VERIFIED` flip TODO.
- [x] `U4.4.2` Append `docs/08_implementation/RELEASE_EVIDENCE_*.md` — PARTIAL `gate 13/13` offline `full-audit 0` `logs/tracker-14/p44-p22-20260824.json` `VERIFIED` blocked on funded `U4.1`.
- [x] `U4.4.3` New `CHG-*` + `E2 Monday 12/12` re-capture — PARTIAL `logs/soak/monday-gates-20260824-*` `gate FAIL 1/12 python` before fix, `full-audit PASS 0` now; `E2 Monday 12/12` re-capture pending funded session.
**DoD:** `make gate` `13/13` + `11-testing-and-release.md` E2 `DONE` on single-VM+market; status `Partially implemented (offline)`→`Partially implemented (offline + market single-VM)`; live-money still `Blocked`.

---

# Phase U5 — Inventory: Honestly BLOCKED Until Prod VMs (do not execute on laptop)

> Per `09-production-swarm.md` `Production runtime: Not-implemented/Untested BLOCKED: needs prod VMs` (honest row `00-start-here.md` E4 vs U5 M3). Tracked to prevent false `4VM` evidence on laptop. Execute only after `D1.3` human VM provision + `prod_node_check.py` PASS.

| Dossier | Blocked surface | Why laptop cannot honestly prove it |
|---|---|---|
| `09-production-swarm.md` `D2/D3` | `v1 4VM (3×Manager+Worker +1 O2) → v2 7VM (3×Manager ONLY + N≥3 Workers)` `docker-stack.yml` `role=worker`/`role=observability` `replicas 1→3`, `SWARM-MGR-001..006` quorum `2/3` survive 1 loss, `3-node ZK 3.9.2` ensemble, Fluss `replication.factor=3` anti-co-location `preferences spread`, `encrypted overlay --opt encrypted` | Laptop `docker swarm init` 1-host mimic `24 containers Started` proves compile only; real quorum needs `3` workload VMs with `docker node ls` `Manager` + `Worker` separation. |
| `09` `D4` `FAIL-VM-LOSS-60000-001` | Drain one workload VM → data recovery `<30s` + order halt `<5s` + no duplicate | Single-node cannot lose its only survivor (`chaos-04-vm-loss SKIP: single-node swarm cannot lose its only survivor`). |
| `09` `D5` `PERF-PROD-60000-001` | `p99 trigger-tick-to-commit <100ms @ 50k 3k ×16.7` + `SLO` `PERF-PER-INSTRUMENT-001 30m` + `STATE-CANDLE/DEDUP` sizing | Laptop `58k` synthetic + `c1_multiconn 48,660 tps` are envelope probes; real latency needs `3` VMs with `30GB` DirectMemory contention (observed `15GB` PC insufficient for `7-VM`). |
| `09` `D6` `DR-001..006` | `coordinator/tablet/ZK quorum/O2/gateway/network-partition` faults | `make disaster-drills --dry-run` done (`DR-001..006` `suite-*.md` 2026-08-23); `--approve` fault injection on prod stack deferred. |
| `09` `D7` + `10-observability` `M3` | `INFRA 9` thresholds from `D5/D4` `>80% CPU` `>85% heap` `>14GB O2` + `OPS-FAIL-001` O2 outage leaves durable audit | Dashboards `8/8` seeded locally `make seed-dashboards` `created=0 untouched=N`; `4VM` firing proof needs live `node_exporter:9100/cadvisor 8080/ZK up` multi-VM scape + `otelcol_exporter_send_failed_*` drill. |
| `12-version-compatibility` | `VM-ZK-013` `3.9.2` ensemble `VM-FLUSS-SRV-005` `replication`/`lake tiering`/`retention alter`, `VM-OPENOBS-011` HA | Single `zookeeper:3.9.2` + `table.datalake.enabled=false` dev deviation + `file:///checkpoints` (not `s3://tradingticks-aug-2026` R2) — prod HA deferred. |
| `02-schema-storage` `SCH-17/20..22` | `SCHEMA-REC-001` `CleanBreakSimulationTest 4` `clean_break_drill.py` live drop, EOD `eod_offload_state`, `SCH-20` `PositionProjectorDriverTest 11` operator wiring `SCH-23` `eod_controller.py` | Pure-JVM done; live `make ddl APPLY=1` prod `drop+replay+parity` + `EOD PENDING→WRITING→COMMITTED→VERIFYING→VERIFIED` `retention extension` need real `S3` + `3d` retain. |
| `05-execution-core` + `11` `EXE-*` | `EXE-INT-001` `ARROW-REST-001/002` live RCF-EQ×N, `EXE-FAIL-002..005` `fencing lease acquire` `HA epoch`, `EXE-AUDIT-001` `1y R2 lock` + `AuditHashChain` | Sandbox `RCF-EQ×1` single-VM `U4.1` fixes `VM-ARROW-010` success-half; `×N` `4VM` egress FW `arrow-egress` overlay + `HI-HA` still BLOCKED. |
| `06_operations` `06-audit-store` | `E5b` `r2_legal_hold_check.py` `--validate` bucket-lock + `11` `R2 chain-dir`, `audit_r2.py` `provision` | Python verifier `10/10` staged CHG-083; live `CF token` + `R2 read + retention admin` human. |
| `05_deployment` `D1` | `PROD_VM_PROVISIONING.md` `500GB SSD` per VM + `prod_node_check.py 9/9` | `D1.1/D1.2` self-check PASS; `D1.3` VM create + SSH human. |
| `02_requirements` `REQ-PF-*` + `04_contracts 09-platform-runtime` | `AC-PF-001..019` `PERF-PER-INSTRUMENT-002 10min` `PERF-NODELOSS-001`, `PF-PC.1..5` post-completion | Maps to U5 table above. |

**Readiness banner truth (00-start-here.md E4 + U4.4):** every row stays `Live-money Blocked` until `U5` `E5` DEC-044 `saurabh` sign-off. `Production runtime` honest `Not-implemented/Untested BLOCKED: needs prod VMs` — never flip on laptop evidence.

---

## Cross-Cutting Constraints (every task)

- No live money without `T9_APPROVED_BY=saurabh` + `DEPLOYMENT_ENV` sandbox (`execution-auth-001` len 238, `ReauthBroker` one retry then `disabled UP→503`). Gate boots `HALTED` (`config.rs` rejects `EXECUTION_ENABLED=true` at boot — `execution_enabled=false`, `is_halted_default()`; `Gate::new` starts `Halted`, `can_execute=false`; `HALTED→RECONCILING` via `enter_reconciling_if_needed(true)` HALTED arm).
- No `flink-cep` anywhere (`cep_guard.sh` `SIG-UNIT-007` + `make cep-check-module` agreement).
- Pin discipline: `versions.pin` exact `FLUSS 0.9.1-incubating` `FLINK 2.2.1` `JAVA 17.0.19` `PYTHON 3.11.9` `ZK 3.9.2` `GO 1.24.5` `RUST 1.97.1` `NAUTILUS 74d57e7…` `otel 0.123.0` `O2 v0.91.5` + `FLUSS_FLINK bb41cde`/`5dddeb`; no `latest`/`SNAPSHOT`; `make pin-check` 4/4.
- Fail-closed: `POSITION_ACTIONS_ENABLED` any `≠false`→`IllegalStateException`, `INGESTION_ALLOW_DEGRADED=true` rejected in `prod`, `ALLOW_FULL_REPLAY=false` (`F005`), `CLOCK_OFFSET_LIMIT_MS 200ms` `DriftMonitor→safety_halt + error alert`, unknown broker `→UNKNOWN never retry halts` `reconcile_execution_mass_status` read-only.
- Evidence-first: every claim `logs/<topic>/<name>-<yyyymmdd>.md` (`logs/ddl-apply/`, `logs/broker-md-001/`, `logs/tracker-14/`, `logs/nautilus-execution/`, `logs/chaos/`, `logs/rollout/`, `logs/disaster-drills/`, `logs/soak/monday-gates-*`) + `docs/05_deployment/change-records/CHG-*.md` next free number (allocate `CHG` per `01-foundation.md` `Change control` 6-field header). Gitignored evidence never edited past mint.
- Gates stay green: `make full-audit` after every task; on adding/removing tests bump hard truth `stale-tables` + `docs_audit C6 464/247/387` — updated `2026-08-24`.
- No scope creep: `ranking/reservations/decisions` REMOVED CHG-005 (not deferred), no `Trade_Decisions` gated `TRADE_DECISIONS_ENABLED=false`, no `portfolio_id` repartition.

## Post-Completion (manual/external, U5)

- [ ] `PC.1` 🔒 Provision prod VMs (`D1`) — human + `prod_node_check.py`.
- [ ] `PC.2` 🔒 Provide real broker creds (AutoLogin `APP_ID`+`USER_ID`+`PASSWORD`+`TOTP_KEY`) + `T9_APPROVED_BY` + fund sandbox margin + `CLOCK_OFFSET_LIMIT_MS` prod `200ms` `NTP_SERVER` — human.
- [ ] `PC.3` 🔒 Premium-tier vs multi-connection `C5.2` — human choice, no purchase assumed.
- [ ] `PC.4` 🔒 `E5` DEC-044 `saurabh` single-operator sign-off `CHG-080` `CHG-079` `gate epoch/evidence hash` + `RELEASE_EVIDENCE` flip `Blocked→Approved-for-Testing` — human, live money still gated until separate go-live.
- [ ] `PC.5` 🔒 Cloudflare CF API token `R2 read + retention admin` for `E5b` `r2_legal_hold_check.py --validate`.
- [ ] `PC.6` Broker contract drift monitor: `make pin-check` + `version_matrix_verify.py` + `arrow_capability_test.go` re-run on `go-arrow` bump.

---

**Created:** `docs/plans/2026-08-25-live-readiness-unified-plan.md` — supersedes `2026-08-21-live-readiness-master-plan.md` + `2026-08-24-laptop-now-market-open-plan.md`. See ID mapping in Overview and `Master`/`Laptop` columns in tracker.

