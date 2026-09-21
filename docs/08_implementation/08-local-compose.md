# Local Compose

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | **Partially implemented (offline) — 121/121 offline + `local_int_004` PASS; Swarm/HA + live-money blocked** |
| Owner | Platform Team |
| Runtime | Docker Compose, single host |
| Prohibited use | Production HA evidence or live-money enablement |
| Acceptance criteria | `AC-PF-016` (Compose network-isolation subset); the full `REQ-PF-001`–`REQ-PF-012` / `AC-PF-001`–`AC-PF-019` family is owned by [`09-production-swarm.md`](./09-production-swarm.md) |
| Offline evidence `2026-08-24` | `L0 6/6` `L1 8/8 + L3 6/6 + L4 10/10 + L5 8/8` `L2 25/25` `L6 10/10 + L7 13/13` `L10 3/3 + local_int_004 offline 10` `L9 12/12 + L8 11/11 + L11 11/11` — `COMPOSE minio/mc:RELEASE.2024-11-07T00-52-20Z` pinned `CONFIG-002` fixed |
| Still needs live/4VM | `L8` tablet/JM `HA` `S3 s3://...` `replication.factor=3` `ZK 3.9.2` + `09-production-swarm.md` + `ARROW-REST-001/002` |
### Local topology

Compose may run one development instance of:

```text
ZooKeeper (single node — dev simplification; production = 3-node ensemble)
Fluss coordinator/tablet
Flink JobManager/TaskManager
Signal and Babysitter job submitter
Ingestion
Nautilus Execution Service
go-arrow bridge / Arrow REST sandbox adapter
OpenObserve
```

Local volumes and one-node services are intentional development simplifications. They cannot prove replication, one-VM tolerance, encrypted S3 recovery, or production capacity.

### Runtime contracts

- All images and dependencies use explicit development versions; no `latest` default.
- All required tables are created from the validated schema manifest or the environment is clearly marked schema-unready.
- Nautilus Execution Service starts `HALTED`.
- The go-arrow bridge is the only service with Arrow credentials or Arrow network access.
- The T3 `execution-t3` profile attaches the bridge only to internal `execution-net` plus the
  bridge-only `arrow-egress` network and publishes no host port. The profile defaults to
  credential-free `disabled` mode; `fake` mode is also offline-only. T8 must attach the gateway
  and Nautilus service to `execution-net` and run the container route probes.
- Nautilus owns the live OMS/position state; Fluss execution tables are written only by the projection boundary.
- Broker calls point only to sandbox/simulation unless an explicit non-default test profile is selected.
- Production credentials, audit buckets, checkpoints, and endpoints are rejected.
- Services expose liveness and readiness separately.
- Job submitter installs exactly Signal and Babysitter jobs and verifies both are running/checkpointing.

The full Nautilus operating model behind these runtime contracts — service topology, boundary
contracts, identity mapping, trade flows, and the migration roadmap — is documented in
[`05-execution-core.md`](./05-execution-core.md#recommended-operating-model).

### Configuration application

The runtime must explicitly apply or mount:

- Fluss server configuration.
- Flink job/checkpoint configuration.
- Schema manifest and DDL version.
- Local test object-store/lake configuration if used.
- Service protocol/configuration versions.
- Secret references from ignored local files only.

A repository file that is not mounted or passed to a process is not effective configuration.

### Health checks

| Service | Liveness | Readiness |
| --- | --- | --- |
| ZooKeeper | Client port responds | Quorum semantics available for local profile (single-node is acceptable locally) |
| Fluss coordinator | Process/RPC responds | Metadata and quorum semantics available for local profile |
| Fluss tablet | Process/RPC responds | Required test tables readable/writable |
| Flink JobManager | REST/RPC responds | Job submission accepted |
| Flink TaskManager | Task slot responds | Required jobs have resources |
| Ingestion | Event loop responds | Manifest/subscriptions/append/clock/telemetry pass |
| Nautilus Execution Service | Event loop responds | Native engine, Fluss intent/projection path, gate, and event-store readiness pass |
| go-arrow bridge | Process responds | Sandbox connectivity and order-update stream contract pass; no live order |
| Compute submitter | Process responds | Both jobs running/checkpointing |
| Babysitter job | Job running | Input schema/offset/checkpoint pass; no-op guard active |
| Execution control | Process responds | Durable state known, gate known, never implies ENABLED |
| Arrow REST | Sandbox API responds | Contract probe passes; no live order |
| OpenObserve | API responds | Telemetry delivery or approved local degradation |

### Network and ports

Document each exposed port as one of:

```text
LOCAL_OPERATOR_ONLY
LOCAL_SERVICE_ONLY
SANDBOX_CALLBACK
```

Bind operator interfaces to localhost by default. Do not expose Fluss internal RPC, ZooKeeper client/peer ports (2181/2888/3888 — `LOCAL_SERVICE_ONLY`), Flink administrative APIs, or Arrow REST order APIs beyond the local test boundary.

### Local secret rules

- `.env` is ignored and contains only sandbox/test credentials.
- No production credentials may be accepted by the local profile.
- Secrets are not printed in startup logs or diagnostics.
- Credential absence causes readiness failure for the dependent service.
- Local secret usage does not satisfy production Swarm-secret requirements.

### Startup procedure

1. Validate local profile and reject production markers.
2. Start infrastructure.
3. Wait for health checks, not only container start.
4. Validate schema manifest and required tables.
5. Start jobs and verify actual job IDs/status/checkpoints.
6. Start data services and validate subscriptions/projections.
7. Start Nautilus Execution Service in `HALTED` and start the go-arrow bridge with sandbox-only credentials.
8. Run simulation/sandbox reconciliation before any controlled test enablement.

The T3-only bridge policy can be checked without starting the platform:

```bash
python3 code/01_platform/04_scripts/execution_network_check.py \
  --compose code/01_platform/01_docker/docker-compose.yml
```

This validates the resolved `execution-t3` Compose profile. It is not a substitute for T8's
runtime connectivity probes or T9's real sandbox authentication evidence.

### JVM and memory configuration

Every Java container SHALL enforce the standard formula from the Signal job spec unless overridden for Flink TaskManagers (RocksDB workload, direct-memory-heavy):

| Config key | Required value | Enforcement |
| --- | --- | --- |
| `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT` | `65` | Java max heap equals 65% of the container memory limit |
| `NON_HEAP_MEMORY_RESERVE_PERCENT` | `35` | Container limit minus Java max heap must be at least 35% |
| `CONTAINER_MEMORY_ALERT_PERCENT` | `85` | Emit warning at or above 85% total container memory |

Local Compose uses minimal resource limits (no production sizing). The concrete numbers below are dev defaults, not production guarantees.

### Shutdown procedure

Stop new simulated money-moving calls, record gate state, drain/reconcile test attempts, checkpoint jobs where applicable, stop services, and preserve diagnostic evidence. `make clean` is destructive and must never target production storage.

### Local acceptance

- [x] Clean startup fails safely when schema/configuration is missing. (2026-08-24: `test_CONFIG_006` `test_START_005` `test_SCHEMA_006/007` PASS — missing `fluss.conf`/schema → `not ready`, `docker compose config` fails, `Pin` still green)
- [x] Both required Flink jobs are visible and healthy. (2026-08-24: `test_JOB_001` exactly 2 jobs + `002 Signal running` + `003 Babysitter running` + `004 both checkpointing` `counts.completed>0` PASS — `make up` `flink-jobmanager:8081/jobs/overview` green)
- [x] Executor cannot place a live order under the local profile. (2026-08-21: `t8_sandbox_contract_check.py` 12/12 — `EXECUTION_ENABLED=false` never true, gate boots HALTED, `POST /v1/intents` 503 fail-closed; `test_PROD_010` gate-monotonic green after DEC-044 assertion fix)
- [x] Health dimensions distinguish process health, readiness, job health, and trading readiness. (2026-08-24: `test_HEALTH_002` liveness UP / readiness DOWN + `008 Nautilus trading readiness not implied` PASS)
- [x] Service-to-service network access matches the documented allowlist. (2026-08-21: `execution_network_check.py` PASS — bridge is the only order-path Arrow egress; `execution-net` internal, zero host ports)
- [x] Local secrets are ignored and redacted — **amended 2026-09-21: "sandbox-only" was wrong.**
  `~/.env.arrow` holds the live Arrow app (dated exception in `05_deployment/04-secrets-rotation.md`), so
  that file's contents are production material. What the check below proves is structural — where
  credentials live and that `.env.example` carries placeholders — never that a file's contents are sandbox.
  (2026-08-21: `t8_sandbox_contract_check.py` PASS — `.env.example` blank placeholders, ARROW creds only in ignored `.env`/`~/.env.arrow`, compose PROD suite SEC-010 green)
- [x] 10-instrument fake-broker smoke: 10 random instruments → fake bridge (mimicking live broker: `PlaceOrder`/`Modify`/`Cancel` + `UNKNOWN`/`REJECT` + fill stream) → Nautilus order lifecycle + position → Fluss projections (`Order_Lifecycle`/`Positions`/`Order_Correlation`) → Babysitter observes (zero actions) — passes on local compose with **no live Arrow credentials** (`execution-t3` `disabled`/`fake`). (2026-08-24: `local_int_004_smoke.py --offline --instruments 10` PASS + `test_LOCAL_INT_004_offline_contract` PASS + `test_STREAM_003` 10 instruments PASS + `test_EXEC_012` projection consistency PASS)

### Implementation status — 2026-08-24 (offline CAN closed, Swarm/live blocked)

> Single-VM laptop (`make up` 12 long-running containers of 18 compose services — 3 `execution-t3` profile-gated, 2 one-shot, `minio-init` exit; `fluss-coordinator:9123` reachable) closes every `Partially` row offline. `L8` `PERF-NODELOSS` `S3` `replication.factor=3` remain `4VM` blocked; `ARROW-REST` remains market blocked.

| Layer | Tests | Done offline `2026-08-24` (evidence) | Still needs live/4VM |
|---|---|---|---|
| `L0 CONFIG-001..006` `219` | `6` | `docker compose config` + `pin-check` `golang:1.24.5-alpine@sha256:daae04e` `rust:1.97.1` `minio/mc:RELEASE.2024-11-07T00-52-20Z` fixed `CONFIG-002` + `ENVIRONMENT=production` reject + `O2_PASSWORD` cannot default + secret leakage + `fluss.conf` not mounted → `not ready` `6/6 PASS` | none |
| `L1 HEALTH-001..008` `259` + `L3 START-001..006` `388` + `L4 SCHEMA-001..010` `416` + `L5 JOB-001..008` `442` | `8+6+10+8=32` | `HEALTH-001 healthy` `002 liveness UP / readiness DOWN` `003 Fluss 9123 R/W` `004 tablet` `005 JM REST` `006 TM slots` `007 O2 5080` `008 Nautilus HALTED` + `START Clean` `Fluss/TM/O2/Schema unavailable` + `SCHEMA manifest 27 ddl_sha256 bucket_key 16 missing→DOWN` + `JOB exactly 2 Signal+Babysitter checkpointing` `58/58 PASS` `test_l1_l3 + test_l4` | `JobManager HA` `ZK 3.9.2` quorum |
| `L2 NETWORK-001..010 + SEC-001..015` `297` | `25` | `execution_network_check.py PASS` `bridge sole arrow-egress` `execution-net internal:true 0 host ports` `gateway→Nautilus→bridge` `Flink no Arrow` `25/25 PASS` `test_l2` | `Swarm` `encrypted overlays` `production secrets` |
| `L6 STREAM-001..010` `474` + `L7 EXEC-001..013` `520` | `19` | `STREAM single/burst 1k/10 instruments/ts/out-of-order/duplicate/Gap103/Gap does not halt/backpressure` + `EXEC HALTED boots/blocks/Place/Modify/Cancel/REJECT/UNKNOWN/partial 40/100/full/correlation` `FakeBridge extendable 8598b421f` `19/19 PASS` `test_l6_l7` (EXEC-003/004/005 and EXEC-006/007 are collapsed into single tests) | `live tick→Fluss` `arrow-egress` firewall prod |
| `L10 LOCAL-INT-004` `572` | `3` | `10 random fake Place→HALTED→fill→OMS→Fluss Projections→Babysitter zero actions` `local_int_004 offline 10 PASS` `3/3 PASS` `test_l10` | `LIVE Fluss` `GatewayFluss 3.5s` already green, market `Place` still blocked |
| `L9 OBS-001..012` `637` | `12` | `OTLP otel-collector:4318→O2` `health identity` `no secrets` `telemetry failure not unsafe` `12/12 PASS` `test_l9` | `O2 dashboards` live scrape |
| `L8 FAIL-001..010 + SAFETY-001` `587` | `11` | `Restart ingestion/JM/TM/tablet/Nautilus/bridge` `partition` `UNKNOWN` `double delivery` `Fail closed on ambiguity` `11/11 PASS` `test_l8` **offline fake** | `HA` `tablet kill→recovery` `S3` `DR-001..006` `chaos-suite` `4VM` |
| `L11 RES-001..006 + PERF-001..005` `667` | `11` | `memory limit` `JVM 65%` `35% reserve` `85% alert` `CPU/disk degrades` `1k/5k/10k/10k sustained/burst` `11/11 PASS` `test_l11` **dev limits** | `PERF-NODELOSS-001` `3000 instr` `50k tps` prod sizing |
## Verification mapping

The required behavior above is verified by the canonical [Local Compose test design](./11-testing-and-release.md#local-compose): `LOCAL-INT-001` to `LOCAL-INT-003`, `LOCAL-FAIL-001`, `LOCAL-FAIL-002`, and `LOCAL-OBS-001`.

> **Added 2026-08-21:** the 10-instrument fake-broker smoke above is verified as `LOCAL-INT-004` (10 random instruments, `execution-t3` `fake` bridge mimicking live Arrow lifecycle, no `ARROW_*` live secrets): intent → gateway → bridge → Nautilus `ReportEnvelope` → `src/projection/mod.rs` → Fluss `Order_Lifecycle`/`Positions`/`Order_Correlation` → Babysitter `Positions` observation (zero actions). Runs on the local single-host Compose exactly as it will run on the Swarm — same images, same `execution-net` isolation (v1: 4 VMs Manager+Worker, v2: 7 VMs Manager ONLY + Workers per 09 DECISION 2026-08-20).

## Layered test suite — local compose (L0–L11, 118 pytest tests / 121 evidence count)

> **Purpose 2026-08-21:** the six canonical tests in `11-testing-and-release.md#local-compose`
> (`LOCAL-INT-001..003`, `LOCAL-FAIL-001/002`, `LOCAL-OBS-001`) plus the new `LOCAL-INT-004`
> prove acceptance, but not isolation and integration depth. This layered suite defines the
> **test contract before implementation** so local Compose naturally satisfies it and the same
> behavioral tests can later run against the Swarm (v1 4 VMs Manager+Worker, v2 7 VMs Manager ONLY per 09) — same images, different placement. Do not implement all 116 at once —
> implement per the priority in §18 below.

### Test architecture

| Layer | Purpose | Tests |
| ----- | ------- | -----:|
| L0 | Static / config validation | ~15 |
| L1 | Container / process health | ~10 |
| L2 | Network isolation / security | ~15 |
| L3 | Dependency / startup / readiness | ~10 |
| L4 | Schema / config correctness | ~10 |
| L5 | Flink job lifecycle | ~8 |
| L6 | Streaming data path | ~12 |
| L7 | Execution / Nautilus lifecycle | ~15 |
| L8 | Failure / restart / recovery | ~15 |
| L9 | Observability | ~10 |
| L10 | End-to-end business-flow simulation | ~8 |
| L11 | Resource / memory behavior | ~8 |
| **Total** | | **118** |

Harness shape (same Compose stack, contract-based — no container-name/sleep coupling):

```text
tests/
├── config/          # L0 CONFIG-001..006
├── health/          # L1 HEALTH-001..008
├── network/         # L2 NETWORK-001..010 + SEC-*
├── startup/         # L3 START-001..006
├── schema/          # L4 SCHEMA-001..010
├── flink/           # L5 JOB-001..008
├── streaming/       # L6 STREAM-001..010
├── execution/       # L7 EXEC-001..013 + LOCAL-INT-004 (strict)
├── failure/         # L8 FAIL-001..010 + SAFETY-001
├── observability/   # L9 OBS-001..012
├── performance/     # L11 PERF-001..005 + RES-001..006
└── e2e/             # L10 10-instrument smoke (LOCAL-INT-004)
```

Runner:

```bash
make test-local          # L0–L4 fast offline + health
make test-network        # L2 isolation
make test-execution      # L7 + L10
make test-failure        # L8
make test-performance    # L11
make test-all            # all L0–L11
```

---

### L0 — Static configuration tests (no containers required)

#### CONFIG-001 — Compose syntax valid

`docker compose config` must succeed.

*Assert:* YAML parses, variable interpolation succeeds, no unresolved required variable.

#### CONFIG-002 — No `latest`

Scan every image.

*Assert:* no image uses `:latest`; every upstream image has explicit version/tag or digest (e.g. `golang:1.24.5-alpine@sha256:daae04e…`, `rust:1.97.1`).

#### CONFIG-003 — Production marker rejection

Put `ENVIRONMENT=production` (and separately production endpoint / S3 bucket / credential var / DB URI) into local `.env`.

*Assert:* compose validation fails safely; no service starts.

#### CONFIG-004 — Required secrets cannot silently default

Remove `O2_PASSWORD` / `ARROW_TOKEN` / `OPENALGO_API_KEY` (each separately).

*Assert:* dependent service becomes **not ready**; no fake/default credential substituted.

#### CONFIG-005 — Secret leakage scan

Inspect `docker compose logs` / `docker inspect` / `docker compose config`.

*Assert:* secret values never appear.

#### CONFIG-006 — Effective configuration

For each `fluss.conf`, Flink config, schema manifest, DDL, object-store config — remove its mount temporarily.

*Assert:* service detects missing config; does not silently use an unintended default. (Spec says: a repo file not mounted/passed is not effective configuration.)

---

### L1 — Health tests

#### HEALTH-001 — All containers eventually healthy

`running != healthy` is not sufficient. Every required service must reach its readiness state.

#### HEALTH-002 — Liveness vs readiness

Kill a dependency while leaving the process alive.

*Assert:* liveness stays `UP`, readiness becomes `DOWN`. (Core to the gate design.)

#### HEALTH-003 — Fluss coordinator health

`process alive` + `RPC available` + `metadata available` + required tables accessible.

#### HEALTH-004 — Fluss tablet health

`RPC works` + required table read works + required table write works.

#### HEALTH-005 — Flink JobManager

`REST reachable` + job submission accepted.

#### HEALTH-006 — Flink TaskManager

`slot available` + required task can schedule.

#### HEALTH-007 — OpenObserve

`API reachable` + telemetry can be ingested.

#### HEALTH-008 — Nautilus

`liveness = UP`, `readiness = READY`, `trading readiness != ENABLED`. A healthy Nautilus must **not imply** trading readiness.

---

### L2 — Network isolation / security (strongest suite)

#### NETWORK-001 — Bridge is the only Arrow-connected service

Try Arrow connectivity from `ingestion`, `flink`, `compute`, `babysitter`, `nautilus`, `openobserve` → denied. `go-arrow bridge → Arrow sandbox` → allowed.

#### NETWORK-002 — Bridge profile disabled

Without `execution-t3` → bridge does not run, no Arrow network, no credentials required.

#### NETWORK-003 — Fake mode is offline

`execution-t3=fake` → fake mode cannot resolve/connect to Arrow; fake broker stays fully local. (Fake must not become a different real-broker path.)

#### NETWORK-004 — No host port on bridge

`docker ps` / `docker inspect` → bridge has no published host port.

#### NETWORK-005 — `execution-net` isolation

From another service `curl http://bridge:...` → only explicitly allowed services can reach bridge.

#### NETWORK-006 — Nautilus can reach bridge

`Nautilus → execution-net → bridge` works.

#### NETWORK-007 — Gateway can reach Nautilus

`gateway → execution-net → Nautilus` works. (Current gateway is on `[trading-net, execution-net]`.)

#### NETWORK-008 — Arrow credentials absent outside bridge

Env/config of all containers → `ARROW_APP_ID`/`ARROW_TOKEN`/`ARROW_WS_URL`/`ARROW_ORDER_URL` only where allowed (ingestion exception, bridge; never on `nautilus`/`flask`/`compute`).

#### NETWORK-009 — Flink cannot reach Arrow

Explicit regression.

#### NETWORK-010 — Host cannot bypass gateway

Try host → internal Nautilus/bridge ports → denied unless an intentional `LOCAL_OPERATOR_ONLY` port.

Additional security regressions (also L2):

#### SEC-001 — No production credentials
No prod credential file or env var is accepted by the local profile.

#### SEC-002 — No production endpoints
No prod broker / S3 / DB endpoint is accepted; compose fails safely.

#### SEC-003 — Bridge is sole Arrow network client
Only `go-arrow bridge` has Arrow network access (NETWORK-001).

#### SEC-004 — No Arrow credentials outside bridge
`ARROW_*` only on bridge (and ingestion exception) — NETWORK-008.

#### SEC-005 — Bridge has no host port
NETWORK-004 — `docker ps` shows no published port.

#### SEC-006 — No Fluss internal port exposure
Fluss RPC/tablet ports are `LOCAL_SERVICE_ONLY`, not published to host.

#### SEC-007 — No ZooKeeper peer port exposure
`2181/2888/3888` not published beyond local service boundary.

#### SEC-008 — No Flink admin port exposure
Flink REST/admin not exposed beyond local operator.

#### SEC-009 — No Arrow REST external exposure
Order REST not exposed beyond bridge.

#### SEC-010 — No secrets in logs
`docker logs` / `inspect` / `config` never show secret values — CONFIG-005.

#### SEC-011 — Fake mode cannot reach internet
`execution-t3=fake` cannot resolve/connect to Arrow — NETWORK-003.

#### SEC-012 — HALTED blocks order execution
`EXEC-002` — HALTED gate blocks any money-moving command.

#### SEC-013 — Telemetry cannot enable trading
Losing OpenObserve does not make Nautilus ready to trade.

#### SEC-014 — Restart cannot enable trading
Restarting any service does not silently flip gate to `ENABLED`.

#### SEC-015 — Malformed execution request cannot bypass gate
Bad intent / missing correlation / bad signature → rejected, no order.

---

### L3 — Startup / dependency tests

#### START-001 — Clean startup

Empty volumes `docker compose up` → proper dependency order, no crash-loop cycles.

#### START-002 — Fluss unavailable

Stop coordinator before startup → Flink not healthy, ingestion not falsely ready.

#### START-003 — TaskManager delayed

JobManager first → eventually healthy, jobs pending until TaskManager resources appear, submitter does not fail permanently.

#### START-004 — OpenObserve unavailable

Core trading simulation stays safe; service reports telemetry degradation, not false success.

#### START-005 — Schema unavailable

Startup fails safely. (Maps to acceptance criterion.)

#### START-006 — Job submitter starts too early

Delay Flink → submitter retries/waits, no duplicate job storm.

---

### L4 — Schema tests

#### SCHEMA-001 — Manifest exists

#### SCHEMA-002 — DDL matches manifest

#### SCHEMA-003 — Required tables exist

At least `Order_Lifecycle`, `Positions`, `Order_Correlation` plus signal/babysitter inputs per schema.

#### SCHEMA-004 — Column types match

#### SCHEMA-005 — Distribution keys match

#### SCHEMA-006 — Missing table causes readiness failure

#### SCHEMA-007 — Wrong schema version rejected

#### SCHEMA-008 — Extra incompatible column rejected

#### SCHEMA-009 — Fresh volume bootstraps schema

#### SCHEMA-010 — Existing schema not destructively recreated

---

### L5 — Flink job lifecycle

#### JOB-001 — Exactly two jobs

Submitter produces `Signal` + `Babysitter` and nothing else.

#### JOB-002 — Signal job running

#### JOB-003 — Babysitter job running

#### JOB-004 — Both checkpointing

Check actual checkpoint progress, not just `RUNNING`.

#### JOB-005 — Duplicate submission prevention

Restart submitter → no duplicate jobs.

#### JOB-006 — Job failure detected

Kill Signal → submitter notices.

#### JOB-007 — Job restart

Restore Signal → correct resume per policy.

#### JOB-008 — Wrong job detected

Manually create unexpected Flink job → runtime reports policy violation.

---

### L6 — Streaming data-path

```text
fake/source → ingestion → Fluss → Flink → Signal → Babysitter
```

#### STREAM-001 — Single tick

One instrument tick → ingestion → Fluss → Flink consumed.

#### STREAM-002 — Burst (1,000 ticks)

No loss, ordering, timestamps, throughput.

#### STREAM-003 — Multiple instruments (10)

Instrument isolation.

#### STREAM-004 — Timestamp correctness

`event_time` / `ingest_ts` semantics.

#### STREAM-005 — Out-of-order tick

Late event → ingestion accepts, Flink event-time handles.

#### STREAM-006 — Duplicate tick

Downstream dedup per contract.

#### STREAM-007 — Gap (100,101,103)

Gap `102` → ingestion continues.

#### STREAM-008 — Gap does not halt stream (104,105 continue)

#### STREAM-009 — Backpressure

Slow downstream → backpressure observable, ingestion stays within safety boundary.

#### STREAM-010 — Tick latency

`event_time → ingest_ts`, `ingest_ts → Flink` — SLA metrics.

---

### L7 — Execution / Nautilus lifecycle (strict)

#### EXEC-001 — Nautilus starts HALTED

`gate = HALTED` on first boot.

#### EXEC-002 — HALTED blocks order

Valid intent → no broker call, lifecycle records blocked.

#### EXEC-003 — Fake mode PlaceOrder

`PlaceOrder` → fake gateway receives it.

#### EXEC-004 — Modify (Place → Modify)

#### EXEC-005 — Cancel (Place → Cancel)

#### EXEC-006 — REJECT

Fake `REJECT` → Nautilus transitions, Fluss projection matches.

#### EXEC-007 — UNKNOWN

Fake `UNKNOWN` → not marked successful, stays reconcilable.

#### EXEC-008 — Partial fill (100 requested, 40 filled)

Order lifecycle + position correct.

#### EXEC-009 — Full fill

Position update.

#### EXEC-010 — Fill stream ordering

Controlled update sequence → ordering rules hold.

#### EXEC-011 — Correlation

`signal → instruction → gateway order → broker order → fill` can be correlated.

#### EXEC-012 — Projection consistency

Nautilus authoritative → Fluss equals projected view.

#### EXEC-013 — Projection cannot mutate Nautilus state

Direct write to projection does not become authoritative.

---

### L10 — End-to-end 10-instrument smoke (LOCAL-INT-004 — the most important local test)

For each of **10 random instruments** (fresh run `execution-t3=fake`, no `ARROW_*` live secrets):

1. Generate signal → instruction.
2. Place fake order → produce `ACK`, optional `MODIFY`/`CANCEL`, `REJECT`, `UNKNOWN`, partial + full fills.
3. Produce order updates in realistic async order.
4. Verify Nautilus state, Fluss projection, correlation.
5. Verify Babysitter sees resulting `Positions` and generates **zero actions**.
6. Verify `Arrow credentials = absent`, `real broker calls = 0`, `bridge host port = absent`, `live gateway = inaccessible`.

This one test replaces a basic "place fake order" check — same images and `execution-net` isolation will later run unchanged on the Swarm (v1 4 VMs Manager+Worker, v2 7 VMs).

---

### L8 — Failure / restart / recovery

#### FAIL-001 — Restart ingestion

Kill → reconnect, no uncontrolled duplicate.

#### FAIL-002 — Restart Flink TaskManager

Job recovers via checkpoint.

#### FAIL-003 — Restart JobManager

Jobs recover/reconcile.

#### FAIL-004 — Restart Fluss tablet

Data remains as documented.

#### FAIL-005 — Restart Nautilus (critical)

Durable state reconstructed, gate does **not** silently become `ENABLED`.

#### FAIL-006 — Restart bridge

No accidental orders during reconnect.

#### FAIL-007 — Network partition bridge ↔ Nautilus

No duplicate orders, execution controlled.

#### FAIL-008 — Fake broker timeout

Order does not incorrectly transition to `FILLED`.

#### FAIL-009 — Fake broker UNKNOWN after timeout

State stays reconcilable.

#### FAIL-010 — Double delivery

Identical execution event twice → projection idempotency.

#### SAFETY-001 — Fail closed on ambiguity

Ambiguous: unknown broker response / missing correlation / missing position / missing schema / missing credential / stale gateway / unknown order status / Nautilus restart mid-lifecycle.

*Expected:* `DO NOT` place a new money-moving order → `HALTED`/`UNKNOWN`/`REQUIRES_RECONCILIATION`.

---

### L9 — Observability

#### OBS-001 — Every required service emits telemetry

#### OBS-002 — Health telemetry contains service identity

#### OBS-003 — Flink checkpoint failure observable

#### OBS-004 — Restart count observable

#### OBS-005 — WS reconnect observable

#### OBS-006 — Gap count observable

#### OBS-007 — Tick throughput observable

#### OBS-008 — Tick→ingest latency observable

#### OBS-009 — Instruction→order latency observable

#### OBS-010 — Fill→lifecycle latency observable

#### OBS-011 — No secrets in logs

#### OBS-012 — Telemetry failure doesn't cause unsafe behavior

(Observability must be non-critical to trading correctness.)

---

### L11 — Resource / memory + performance

#### RES-001 — Memory limit enforced

#### RES-002 — JVM heap = 65%

Verify actual flags, not just Compose vars.

#### RES-003 — 35% non-heap reserve

#### RES-004 — 85% alert threshold

#### RES-005 — CPU saturation

Sustained load → observe latency/backpressure/dropped messages.

#### RES-006 — Disk pressure

Fill volume near limit → clear readiness degradation, no silent corruption.

#### PERF-001 — 1k ticks/s

#### PERF-002 — 5k ticks/s

#### PERF-003 — 10k ticks/s

Spec target: ~10k ticks/s sustained with no drops. For each, record `input rate`, `Fluss write`, `Flink consumption`, `p50/p95/p99`, `CPU/RAM`, `backpressure`, `checkpoint duration`.

#### PERF-004 — 10k sustained (long, not 10s)

#### PERF-005 — Burst + recovery (2k → 10k → 20k → 2k)

---

### Restart preservation (part of L8/L11)

| State | Restart expected |
| ----- | ---------------- |
| Fluss raw data | preserved |
| Flink checkpoint | preserved |
| Nautilus OMS state | reconstructed / preserved |
| Position state | preserved / reconstructed |
| Fake broker state | explicitly documented |
| OpenObserve data | preserved |
| Temporary runtime state | may reset |

Test both `docker compose down` → `up` and `docker compose down -v` → `up` (latter must be a known clean dev state, not mysterious partial state).

---

### Implementation priority (do this order)

#### Phase A — Must exist before usable

`CONFIG-001..006`, `HEALTH-001..008`, `NETWORK-001..010` (+`SEC-001..015`), `START-001..006`, `SCHEMA-001..006`, `JOB-001..005`

#### Phase B — Core correctness

`STREAM-001..010`, `EXEC-001..013`, `LOCAL-INT-001..004`

#### Phase C — Failure safety

`FAIL-001..010`, `SAFETY-001`

#### Phase D — Production-confidence locally

`OBS-001..012`, `RES-001..006`, `PERF-001..005`

*The same behavioral tests (especially `STREAM-*`, `EXEC-*`, `LOCAL-INT-004`) later point at the Swarm (v1 4 VMs, v2 7 VMs) with only config change — see 09 v1→v2.*

