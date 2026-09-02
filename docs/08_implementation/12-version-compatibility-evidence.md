<!-- markdownlint-disable MD013 -->

# Version compatibility evidence plan

This document is the **capability-evidence plan** for the pinned version matrix.
Its job is to turn each matrix row from `UNKNOWN` (pinned, not proven) into
`COMPATIBLE` (proven by a capability test). It maps every boundary in
`code/01_platform/04_scripts/version_matrix.yaml` to the concrete test IDs already
defined in `11-testing-and-release.md`, states the pass condition, says where the
result is recorded, and defines the gate that lets `make ddl` actually create the
tables.

## Status

| Field | Value |
| --- | --- |
| Status | Partially implemented (offline) — 6 boundaries COMPATIBLE/COMPATIBLE_WITH_LIMITATION with live evidence (VM-JAVA-001, VM-FLINK-SRV-003/004, VM-FLUSS-CLI-006, VM-FLUSS-CONN-007, VM-BROKER-MKT-008 COMPATIBLE 2026-08-13, VM-OPENOBS-011, VM-FLUSS-SRV-005) + VM-PERF-001 loopback 59k; 6 still UNKNOWN/TO_BE_VERIFIED needing market/4VM (VM-PBK-009, VM-ARROW-010, VM-IMAGES-012, VM-ZK-013, VM-PERF-001, VM-NAUTILUS-014) |
| Owner | Platform |
| Scope | 15 boundaries (VM-JAVA-001 … VM-NAUTILUS-014) |
| Rule | Matrix stays UNKNOWN until capability tests flip to COMPATIBLE; DDL gate requires VM-JAVA-001, VM-PYTHON-002, VM-FLINK-SRV-003/004, VM-FLUSS-SRV-005, VM-FLUSS-CONN-007 |

### Implementation status — 2026-08-24 (offline laptop, no market/4VM)

| Boundary | Status 2026-08-24 | Evidence offline (laptop) | Needs market/4VM |
| --- | --- | --- | --- |
| VM-JAVA-001 Java 17.0.19 | PARTIALLY (pin verified) | `java -version 17.0.19` + `mvn -v` Java 17 + `code/pom.xml <flink.version>2.2.1</flink.version> <fluss.version>0.9.1-incubating` compiles (`common 17/17` historical 2026-08-24, `compute 23/23` historical 2026-08-24 — not C6 466/247/388) — version_matrix_verify.py OK pin discipline, pin-check.sh 4/4 PASS | Live build smoke on Flink 2.2 image `LOCAL-INT-002` full stack |
| VM-PYTHON-002 Python 3.11.9 | COMPATIBLE (2026-08-25) | `ddl-apply version` in rebuilt image: `python3 Python 3.11.9` == pin `PYTHON_VERSION=3.11.9` (image rebuilt on `python:3.11.9-slim` + `openjdk-17-jre-headless`); self-test `DDL-APPLY-RESULT: PASS exit=0`; `pin-check.sh` 4/4 PASS | None — pin now enforced at build + runtime (`ddl-apply version`); host tooling 3.11.15 in-family |
| VM-FLINK-SRV-003 / VM-FLINK-API-004 Flink 2.2.1 | PARTIALLY (pin verified) | `code/pom.xml` flink 2.2.1 managed deps compile green; `docker compose config` parses `flink-jobmanager:2.2.1` via fluss-flink-2.2 connector `5dddeb...` SHA, `flink-streaming-java` `bb41cde...` | Live `COMPAT-FLINK-001` checkpoint/restore/rescale + `SIG-HARNESS-003/005` on Docker Flink 2.2.1 |
| VM-FLUSS-SRV-005 Fluss 0.9.1-incubating | COMPATIBLE_WITH_LIMITATION (2026-08-24) | 27 DDL files exist, `pin-check` PASS, `version_matrix.yaml` result COMPATIBLE_WITH_LIMITATION (2026-08-24: changelog FULL SCH-14 + bucket-skew COMPAT-FLUSS-006 closed; retention/lake create-only limits recorded) | Replication/failover (multi-node) remains UNKNOWN; lake re-enable create-only limitation |
| VM-FLUSS-CLI-006 Fluss client 0.9.1 | COMPATIBLE (2026-08-24) | `versions.pin` `FLUSS_CLIENT_JAR_SHA256=6921994a2067...` official client jar, `CompatFlussIntegrationTest` compile vs 0.9.1-incubating 100/1000 appends 0 loss | Routing/bucket-key stress + connector checkpoint closed 2026-08-24; replication = server/topology boundary |
| VM-FLUSS-CONN-007 fluss-flink-2.2:0.9.1 | COMPATIBLE (2026-08-24) | `FLUSS_FLINK_CONNECTOR_JAR_SHA256=5dddeb...` `COMPATIBLE` source consume 15,219,441 rows → 205k candles 48 EXACTLY_ONCE checkpoints (`04-signal-job.md` §Connector, `logs/safety-int-001/`) | None — closed 2026-08-13/15/24 (COMPAT-FLINK-001 rescale + STATE-COMPAT-001 + SIG-INT-001/002) |
| VM-ZK-013 ZooKeeper 3.9.2 | NOT FULLY | `docker-compose.yml` single `zookeeper:3.9.2` parses, Fluss registers via `zookeeper:2181` single-node | 3-node ensemble quorum 2/3, Flink HA leader election — needs 4VM Swarm stack |
| VM-BROKER-MKT-008 Broker market feed | COMPATIBLE (EVIDENCE_RECORDED_LIVE 2026-08-13) | `logs/broker-md-001/marketdata-capture-20260813.jsonl` 52 records both feeds, standard full 249 B (depth 109, CAS trailer) HFT 40/196 zstd, `pin-check` golden 6/6 OK, broker_md corpus pinned | None — done (DEC-037 live reconnect removed, ING-RES-001 soak PASS) |
| VM-BROKER-PBK-009 Broker postback | NOT FULLY | `go-arrow streams.go OrderStream` shape pinned, no corpus | Build postback corpus `BROKER-PB-001` — needs live order-updates WS market hours |
| VM-ARROW-010 Arrow REST | PARTIALLY (2026-08-24) | `ARROW_REST_URL=https://edge.arrow.trade` pinned + TOTP auth PROVEN live 2026-08-21 (`execution-auth-001`, token len 238) | Order round-trip `ARROW-REST-001/002` (market hours + approval); timeout/retry pins open |
| VM-OPENOBS-011 OpenObserve v0.91.5 | COMPATIBLE_WITH_LIMITATION (2026-08-24)  | `otel-collector-config.yaml` `0.123.0` validate OK, `o2-provision.py` 43 alerts (INFRA 9 @60s) `docker compose config` O2 `v0.91.5-amd64`, dashboards 8/8, `OPS-INT-001` telemetry redaction (offline) | Multi-host M3 firing (4VM) + PERF-PROD-60000 remain; OPS-INT-001 + OPS-FAIL-001 evidence recorded 2026-08-24 (DR-004) |
| VM-IMAGES-012 Base images | UNKNOWN (2026-08-24 partial — digest+SBOM verified; vuln-policy unverified) | `versions.pin` digests: `FLUSS_CLIENT 6921994a`, `FLUSS_FLINK 5dddeb`, `FLINK_STREAMING bb41cde`, `docker-compose.yml` `${FLUSS_IMAGE:?set ...}` digest-required, `pin-check.sh` no SNAPSHOT | 12/12 digests verified + SBOM attestations (4/5 images) + OS-layer trivy counts disclosed (`logs/version-matrix/images-012-20260824T171559Z.md`); library-layer scan-unavailable, SEC-IMAGE-001 threshold TO_BE_VERIFIED |
| VM-PERF-001 Mock-broker 50k | PARTIALLY | `PerfBaselineTest` loopback 59,221 tps 592k/10s 0 loss `PARTIAL_EVIDENCE_LOOPBACK_50K` (ingestion 247 tests green) | Fluss ingestion capacity 50k + 90k peak RETIRED DEC-036 — needs multi-node E2E perf |
| VM-NAUTILUS-014 Nautilus 0.62.0 | PARTIALLY | `NAUTILUS_COMMIT=74d57e7…` `NAUTILUS_RUST_TOOLCHAIN 1.97.1` `cargo 1.97.1` `cargo test 207/207` (CHG-107; was 206/206) `EXECUTION_BRIDGE_GO 1.24.5` `f622f8a9…` SHA, `nautilus-execution-service` 0.62.0 | Locked Rust service lifecycle + event-store replay + restart crash-window — needs live execution bridge evidence |

## Authority and scope

- Authority order: executable tests > this plan > the matrix file > prose.
- This is a **plan only** (documentation). It executes nothing and needs a
  runnable environment (Docker) to run.
- Pinned versions live in `code/01_platform/04_scripts/versions.pin`; the matrix
  and verification script live next to it (`version_matrix.yaml`,
  `version_matrix_verify.py`).
- The DDL application contract (`ddl_apply.py`) refuses to apply tables until the
  gate below is met.

## Prerequisites

1. `versions.pin` contains the pinned platform versions (done: Flink 2.2.1,
   Fluss 0.9.1-incubating, ZooKeeper 3.9.2, Java 17.0.19, Python 3.11.9,
   pinned images).
2. A runnable local stack (Docker + Compose) per `08-local-compose.md` using the
   pinned images.
3. `make ddl` already emits `schema_manifest.json` (21 tables as of 2026-08-14, pre-CHG-003 — now 27) — confirmed; it
   still refuses application by design.
4. For external rows, a sandbox broker and Arrow REST stub (no real credentials).

## Boundary → evidence mapping

For each boundary: run the listed test(s) against the running stack, record the
output into the matrix columns (`result`, `observed_behavior`, `limitations`,
`date`, `fixture`, `scenario`), then flip `compatibility_class` to `COMPATIBLE`
on pass. External boundaries that need real broker/Arrow contracts stay
`TO_BE_VERIFIED` (blocked by DATA-GAP-001 / DATA-GAP-005).

| # | Boundary | Matrix id | Evidence test IDs | Pass condition |
| --- | --- | --- | --- | --- |
| 1 | Java 17.0.19 | VM-JAVA-001 | `LOCAL-INT-002`; build smoke on Java 17 | `common` + services compile/run on the pinned JVM; effective config reports `17.0.19`; no module fails to start |
| 2 | Python 3.11.9 | VM-PYTHON-002 | run `ddl_apply.py` + `version_matrix_verify.py` on 3.11 | Both scripts execute; matrix parses; verifier passes |
| 3 | Flink 2.2.1 | VM-FLINK-SRV-003, VM-FLINK-API-004 | `COMPAT-FLINK-001`; `SIG-HARNESS-003`, `SIG-HARNESS-005`; `STATE-COMPAT-001` | Source/sink checkpoint, restore, rescale correct on 2.2.1; savepoint restores through the approved compatibility path |
| 4 | Fluss 0.9.1-incubating | VM-FLUSS-SRV-005 | `COMPAT-FLUSS-001`..`006`; `SCHEMA-UNIT-001`/`002`/`003` | All 27 DDLs parse/apply; effective schema == manifest; LOG/KV/changelog behavior matches; stale/conflict KV rejected and audited; distinct bucket keys spread evenly across buckets (constant key collapses) |
| 5 | Fluss connector (fluss-flink-2.2:0.9.1) | VM-FLUSS-CONN-007 | `COMPAT-FLINK-001`; `SIG-INT-001` | Pinned connector checkpoint/restore on the 2.2.1 boundary works with the Fluss source/sink |
| 5c | Fluss client 0.9.1 | VM-FLUSS-CLI-006 | `COMPAT-FLUSS-006` | `CompatFlussIntegrationTest` compile + append/consume vs 0.9.1-incubating; 100/1000 appends 0 loss; routing/bucket-key stress — `COMPATIBLE` (closed 2026-08-24) |
| 5a | ZooKeeper ensemble (3.9.2) | VM-ZK-013 | `SWARM-INT-002`; `SWARM-FAIL-001`; `PERF-NODELOSS-001` | 3-node ensemble starts; quorum 2-of-3 survives one node loss; Fluss coordinator/tablet register via `zookeeper.address`; Flink JobManager HA leader election + failover works |
| 5b | Fluss state-table schema + rehydration (DEC-038, 2026-08-14) | VM-FLUSS-SRV-005 | `SIG-STATE-001` to `SIG-STATE-003`; `STATE-COMPAT-001` (Fluss-state-table half); rehydration integration test | Dedup state-table schema/serialization reads and writes match on the pinned connector; restart rehydrates the Flink working cache from the Fluss table; schema change is additive or blocks before unsafe use; Fluss unavailability/incompatibility fails closed |

**Partial evidence recorded (2026-08-09, SAFETY-INT-001):** `fluss-flink-2.2:0.9.1-incubating` resolves from Maven Central; `FlussSource.build()` performs a live `Admin.getTableInfo` (fail-fast); live KV upsert → primary-key lookup → `RowDataDeserializationSchema` read worked against Fluss 0.9.1-incubating. This covers the connector boundary partially (VM-FLUSS-CONN-007 was `UNKNOWN` at the time — **CLOSED 2026-08-24:** `COMPAT-FLINK-001` rescale + `SIG-INT-001/002` + `STATE-COMPAT-001` green; row flipped `COMPATIBLE`). Evidence: `docs/08_implementation/04-signal-job.md` §Connector and compile evidence, `logs/safety-int-001/`.
| 6 | Broker market feed | VM-BROKER-MKT-008 | `BROKER-MD-001` | Live corpus (2026-08-13) decodes to typed fields on both feeds; full-mode 249 B confirmed; unknown protocol version quarantined; behavior recorded as evidence — `COMPATIBLE` (`EVIDENCE_RECORDED_LIVE`) |
| 7 | Broker postback | VM-BROKER-PBK-009 | `BROKER-PB-001` | Postback status/identity/timestamp/optional-field behavior recorded; unsupported behavior quarantined — `TO_BE_VERIFIED` |
| 8 | Arrow REST API | VM-ARROW-010 | `ARROW-REST-001`, `ARROW-REST-002` | Request/response/auth/timeout captured; client-reference correlates one attempt to one broker order — `TO_BE_VERIFIED` |
| 9 | OpenObserve | VM-OPENOBS-011 | `OPS-INT-001`, `OPS-FAIL-001` | Telemetry envelope/redaction correct; OpenObserve outage leaves durable audit available — `TO_BE_VERIFIED` |
| 10 | Base images | VM-IMAGES-012 | `LOCAL-INT-002`; `SWARM-INT-001`; `SEC-IMAGE-001` | Images referenced by digest; no mutable tag; digest+SBOM PARTIAL evidence (2026-08-24); vulnerability-policy PASS TO_BE_VERIFIED |
| 11 | Mock-broker 50k | VM-PERF-001 | `PERF-PROD-60000`; `PerfBaselineTest` | Loopback 59,221 tps 592k/10s 0 loss recorded (`PARTIAL_EVIDENCE_LOOPBACK_50K`); multi-node E2E 50k capacity needs 4VM — `PARTIALLY` |

**Matrix evidence recorded (2026-08-24, laptop-only):** `version_matrix.yaml` rows 1/3/4
flipped `COMPATIBLE` (`EVIDENCE_RECORDED_LIVE` / `EVIDENCE_RECORDED`):

| Row | Status | Evidence (captured 2026-08-24) |
|---|---|---|
| VM-JAVA-001 | `COMPATIBLE` (`17.0.19` exact) | `docker run --rm flink:2.2.1-scala_2.12-java17 java -version` → Temurin 17.0.19+10; host JVM 17.0.19 identical; full module suites green on it (common 489 / ingestion 285 / compute 424) |
| VM-FLINK-SRV-003 | `COMPATIBLE` | `FLINK_IMAGE=flink:2.2.1-scala_2.12-java17` + pin-check 4/4 PASS (`FLINK_VERSION pinned`); live signal job on 2.2.1 — 463/463 EXACTLY_ONCE checkpoints, `STATE_RECOVERY_PATH` restore; `SignalHarnessContractTest` 4/4 (2026-08-24); 2026-08-09 48-checkpoint exactly-once evidence (`logs/safety-int-001/`); **COMPAT-FLINK-001 rescale GREEN 2026-08-24** — `CompatFlinkCheckpointRescaleIntegrationTest` 1/1 (checkpoint → restore at 2× parallelism, serializer-change blocked as asserted; `COMPUTE_INT_TEST_COMPAT_FLINK=true`, host-runnable MiniCluster) |
| VM-FLINK-API-004 | `COMPATIBLE` | pom `flink.version` 2.2.1 (streaming-java/clients/table-*/connector-base/test-utils); compute module green (387 tests) against the 2.2.1 API on JVM 17.0.19 |
| VM-PYTHON-002 | `COMPATIBLE` (2026-08-25, DRIFT RESOLVED) | ddl-apply image rebuilt on `python:3.11-slim` — image `python3 --version` == `3.11.16` (3.11-series, matches pin `3.11.9`); `ddl-apply version` subcommand (new) asserts series vs `PYTHON_VERSION` in `versions.pin` (pin added 2026-08-25); self-test PASS. 2026-08-24 record (historical): image shipped 3.14.4 (`DRIFT_RECORDED_PIN_UNSATISFIED`) |
| VM-IMAGES-012 | `UNKNOWN` (partial evidence) | 2026-08-25 re-scan: 12 image RepoDigests verified matching versions.pin/.env; buildx SBOM attestations for fluss/flink/zookeeper/golang (openobserve attestation absent but **SPDX SBOM captured** `openobserve-20260825.spdx.json`); trivy OS-layer per image (ddl-apply 14/3, python:3.11-slim 14/3, golang 17/2, zookeeper 13/0, cadvisor 7/1, fluss 2/0, flink 0/0); **SEC-IMAGE-001 numeric threshold PINNED 2026-08-25** (CRITICAL-with-fix blocks; HIGH-with-fix + CRITICAL-no-fix need dated waiver; HIGH-no-fix accepted risk) — see `11-testing-and-release.md`; library-layer scan-unavailable (Java DB network block) (`logs/version-matrix/images-012-20260825T141500Z.md`, `PARTIAL_EVIDENCE_DIGEST_SBOM_OS_ONLY`) |
| VM-FLUSS-CONN-007 | `COMPATIBLE` | 2026-08-24 flip: COMPAT-FLINK-001 rescale 1/1 (2026-08-24) + SIG-INT-001/002 3/3 (2026-08-13) + STATE-COMPAT-001 serializer half (2026-08-15) — connector checkpoint/restore/rescale + sink partial-visibility proven on the 2.2.1 boundary |
| VM-FLUSS-SRV-005 | `COMPATIBLE_WITH_LIMITATION` | 2026-08-24: changelog FULL closed (SCH-14 `compatFluss003ChangelogFullImage`); bucket-skew closed (COMPAT-FLUSS-006); limitation = replication/failover (multi-node) + retention/lake create-only (`LIMITATION_RECORDED_REPLICATION_PENDING`) |
| VM-FLUSS-CLI-006 | `COMPATIBLE` | 2026-08-24: client compile + runtime + routing/bucket-key stress (COMPAT-FLUSS-006) on official 0.9.1-incubating jar; replication is a server/topology boundary |
| VM-PYTHON-002 | `COMPATIBLE` (2026-08-25, DRIFT RESOLVED) | ddl-apply image rebuilt on `python:3.11.9-slim` — image `python3 --version` == `3.11.9` == pin; `ddl-apply version` subcommand (new) asserts series vs `PYTHON_VERSION` in `versions.pin` (pin added 2026-08-25); self-test PASS. 2026-08-24 record (historical): image shipped 3.14.4 (`DRIFT_RECORDED_PIN_UNSATISFIED`) |
| VM-ARROW-010 | `UNKNOWN` (partial evidence) | URL `https://edge.arrow.trade` pinned + TOTP AutoLogin proven live 2026-08-21 (`execution-auth-001`); order round-trip + timeout/retry pins remain (market hours + approval) |
| VM-OPENOBS-011 | `COMPATIBLE_WITH_LIMITATION` | OPS-INT-001 (redaction) + live OTLP / dashboards 8/8 / 43 alerts + OPS-FAIL-001 DR-004 outage drill 2026-08-24; limitation = multi-host M3 + PERF-PROD-60000 |

Python drift was a real finding from the 2026-08-24 pass: proposed `3.11.9` matched no
runtime — host was 3.11.15 (3.11-series, acceptable for tooling) and the execution
image ran 3.14.4. **RESOLVED 2026-08-25:** the ddl-apply runtime stage was rebuilt on
the exact pinned base `python:3.11.9-slim` (official image = the pin, no distro
drift possible) + `openjdk-17-jre-headless` for the precompiled engine;
`PYTHON_VERSION=3.11.9` was added to `versions.pin`; a `ddl-apply version` subcommand
now proves the series at runtime. VM-PYTHON-002 flipped `COMPATIBLE` 2026-08-25.

**Live evidence recorded (2026-08-13, BROKER-MD-001):** real wire frames from the HFT
Arrow feed (socket.arrow.trade HFT 40/196 B zstd LE) captured raw and decoded to
typed fields; paise scaling verified; AutoLogin (non-interactive, no device token)
verified after the vendored-SDK fix (validate-2fa host api.arrow.trade + `appID` body
field). VM-BROKER-MKT-008 flipped `COMPATIBLE` (`EVIDENCE_RECORDED_LIVE`).
Live reconnect/replay/echo is not an acceptance item (DEC-037, 2026-08-13);
bridge-level ING-RES-001 soak PASS (2026-08-13) is the reconnect evidence.
Evidence: `logs/broker-md-001/`. (The Standard feed `ds.arrow.trade` evidence
13/17/93/249 B was retired with the Standard feed removal 2026-08-14.)

## Environment tiers

Run the evidence in this order (matches the mandatory build order):

1. **Clean local Flink/Fluss stack** — for `COMPAT-FLUSS-*` and `COMPAT-FLINK-*`
   (boundaries 1, 3, 4, 5). This is what unblocks DDL application.
2. **Sandbox broker + Arrow stub** — for `BROKER-MD-001`, `BROKER-PB-001`,
   `ARROW-REST-*` (boundaries 6, 7, 8). These are live-money gates, not DDL gates.
3. **Observability stack** — for `OPS-*` / OpenObserve (boundary 9).

## Execution order

1. Stand up the local stack with pinned images (`08-local-compose.md`).
2. `make ddl` — confirms the version gate passes and emits the manifest.
3. Run `COMPAT-FLUSS-001`..`004` and `COMPAT-FLINK-001` against the running stack.
4. Record each result into `version_matrix.yaml` (flip `compatibility_class` to
   `COMPATIBLE` on pass).
5. Run `make ddl APPLY=1 EVIDENCE=<file>` (capability evidence file, e.g. the
   COMPAT-FLUSS-* / COMPAT-FLINK-* record) — this executes the full 9-step
   application contract (empty-catalog precondition, deterministic apply,
   effective-schema parity, write/read smoke, evidence record) and reports
   PASS or the failing step explicitly.

## The gate (when DDL application unblocks)

`make ddl` applies the tables only when **all** hold:

- `VM-JAVA-001`, `VM-PYTHON-002`, `VM-FLINK-SRV-003`, `VM-FLINK-API-004`,
  `VM-FLUSS-SRV-005`, `VM-FLUSS-CONN-007` are `COMPATIBLE`.
  (2026-08-24: JAVA + FLINK-SRV/API + FLUSS-CONN-007 done (CONN flipped
  COMPATIBLE 2026-08-24); PYTHON-002 flipped `COMPATIBLE` 2026-08-25
  (image rebuilt on `python:3.11.9-slim` — drift from 3.14.4 resolved);
  FLUSS-SRV-005 is
  `COMPATIBLE_WITH_LIMITATION` since 2026-08-24 — replication/failover and
  retention/lake create-only limits remain (multi-node evidence pending) —
  so the gate's `COMPATIBLE` requirement is still not met there.)
- `COMPAT-FLUSS-001`..`004` and `COMPAT-FLINK-001` pass (passed to `make ddl`
  as the `--matrix-evidence` capability evidence).
- `make ddl APPLY=1 EVIDENCE=<file>` completes the 9-step contract with exit 0.
  (The historical reconciliation blocker, superseded 2026-08-10, was removed
  2026-08-15 — its exit criteria are now enforced by the 9-step contract
  itself: pinned versions, parse/apply on the pinned Fluss, schema parity,
  clean create, smoke, and evidence.)

External rows (`VM-BROKER-MKT-008`, `VM-BROKER-PBK-009`, `VM-ARROW-010`,
`VM-OPENOBS-011`) may remain `TO_BE_VERIFIED`. They gate the **release**
`REL-PROTO` (live-money) gate, not DDL application — the schema does not depend on
the broker contract.

## Status tracking

- Each run fills the matrix columns `result`, `observed_behavior`, `limitations`,
  `date`, `fixture`, `scenario`.
- `compatibility_class` flips `UNKNOWN → COMPATIBLE` on pass (or stays
  `TO_BE_VERIFIED` for external rows).
- The `01-foundation.md` "Version matrix" checklist item already records
  `Implementation: Implemented`; flip its `Evidence` axis to `Tested` once this
  plan's boundaries 1–5 are `COMPATIBLE`.
- Every run records exact versions/digests, fixture checksum, and the
  `schema_manifest` ID, per `11-testing-and-release.md` test-record format.

## Honest limitation

This plan **documents** how to prove the versions. It does not run the tests.
Until the Docker environment exists and boundaries 1–5 are executed and pass, the
matrix stays `UNKNOWN` and `make ddl` continues to refuse table application. That
is the intended fail-closed posture.
