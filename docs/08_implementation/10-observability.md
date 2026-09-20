# Observability

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | **Partially implemented (offline) — 8/8 dashboards + collector `0.123.0` validate + 650-tick 1% + top-20 + alerts `43` + JVM `ingestion/gateway` + traces `4317` green on `1-host` + OPS-INT-001/OPS-FAIL-001 evidence 2026-08-24 (DR-004); live `4VM` `M3` firing + `PERF-PROD-60000` still blocked** |
| Owner | Platform/Operations; component owners emit telemetry (infra/JVM/host owners added 2026-08-22) |
| Backend | **OpenObserve is the ONLY live observability backend for all trading + project infrastructure (metrics/logs/traces/alerts); immutable execution audit remains a separate S3/object-store system per `DEC-043` (not O2 retention) — see `docs/04_contracts/openobserve.md#K`.** |
| Sources | `REQ-OBS-*`, `docs/01_project/03-quality-targets.md`, `docs/04_contracts/openobserve.md` |
| Acceptance criteria | `AC-OBS-001`–`AC-OBS-010` (proving families: `OPS-UNIT-*`, `OPS-INT-*`, `OPS-FAIL-*`, `OPS-RUNBOOK-001`, `OPS-REL-001`) |
| Offline evidence `2026-08-24` | `dashboards 8` `manifest 8` `305 tests 5/5` `collector validate` `infra-host 9100/cadvisor 8080/ZK` `filelog/infrastructure → infrastructure_logs` `traces grpc 4317` `jvm.* javaagent v2.9.0` `INFRA 9 alerts 60s` `cargo clippy -D 0 fmt 0 vet 0` |
| Still needs `4VM live` | `ZK quorum 2/3` `Fluss LOG≥2` `Flink HA S3` `50k ticks/s` `Safe-halt <5s Recovery <30s` `OPS-FAIL/REL M3` `09` |
> **2026-08-22 re-scope (user decision):** OpenObserve is re-affirmed as the **single pane for everything in this project — not only trading (ingestion/signal/executor) but also project infrastructure: JVM (all services), hosts, containers, Docker/Swarm, ZooKeeper, Flink HA, Fluss, EOD, and security.** This patch makes that scope explicit in the dossier before any collector/dashboard code changes (`00-start-here.md` doc-first rule). Historical wording retained; new scope annotated with date.

### Common telemetry envelope

Every log/event/trace record contains where applicable:

```text
service_name
service_version
instance_id
environment
schema_version
protocol_version
configuration_hash
correlation_id
audit_id
event_time_utc
receive_time_utc
processing_time_utc
monotonic_duration
clock_offset_ms
severity
failure_class
bounded_scope
```

Do not include credentials, tokens, raw packets, or unnecessary account/person identifiers.

### Metric design rules

- Metric labels are bounded; instrument/order IDs belong in structured logs or audit, not labels.
- Every latency defines start/end boundary, unit, clock source, workload, and percentile.
- Every counter defines retry/duplicate semantics.
- Every health metric identifies the failing dependency.
- Missing telemetry is an observable failure, not a healthy zero.

### Metric cardinality and sampling (T11, streaming-3000 hardening — implemented 2026-08-22)

Cardinality rule: **global and slot metrics are always exported; per-token
series exist only through the top-20 sampler.** At the 3,000-token envelope,
naive per-token emission would create ~30,000 series (3,000 tokens × 10
metrics) and break both OpenObserve and the dashboards. The collector
(`code/01_platform/01_docker/otel-collector-config.yaml`,
`otel-collector-config.swarm.yaml`) enforces the bound; no service bypasses it.

| Bound | Mechanism | Config |
| --- | --- | --- |
| Metrics aggregation 15s | Prometheus scrape `scrape_interval: 15s` per job (flink, infra-host, infra-containers); OTLP metric pipelines use `batch/metrics` (`timeout: 15s`); log batches stay 5s | `processors.batch/metrics`, `service.pipelines.metrics{/prometheus}` |
| Global + slot metrics ALWAYS | No token label → the guard never matches → series pass | N/A (default pass) |
| Per-token series ≤ top-20 | `filter/top20-token-metrics` on BOTH metric pipelines drops every datapoint carrying a per-token label (`token`) whose value is not in the allowlist regex; global/slot series (no token label) always pass | `processors.filter/top20-token-metrics`; env **`METRICS_TOP20_TOKENS_REGEX`** = anchored alternation of ≤20 tokens, e.g. `^(AAA|BBB|...|TTT)$` |
| Tick logs 1% (errors/gaps 100%) | `transform/tick-sampling` sets `sampling.priority=100` on `severity_number >= WARN` or `body.level` ERROR/WARN (errors + gap/drop events: feed_stalled, heartbeat_failed, disconnect, quarantine, backpressure); `probabilistic_sampler/tick` then samples the remaining records at 1% using a per-record seed derived from the log4j2 `instant` (uniform per record, not per token) | `processors.transform/tick-sampling`, `processors.probabilistic_sampler/tick`, `service.pipelines.logs` |

**Default posture is fail-safe:** `METRICS_TOP20_TOKENS_REGEX` defaults to a
never-matching pattern (`a^`), so per-token series are **dropped entirely**
until operations sets the env to the real top-20 set (the emitter side — the
compute service — selects which tokens are "top-20" and the collector hard-
guards the bound; an allowlist misconfiguration can only lose per-token
visibility, never exceed 20 per-token series per metric family).

**Verified 2026-08-22 on the pinned `otel/opentelemetry-collector-contrib:0.123.0`
binary** (build-only; the 3k fake-ticks test runs separately at mock-3k):

- `otelcol validate` passes for both configs (swarm variant validated with a
  resolvable secret path — the only difference is the Swarm secret mount).
- Tick-log sampling runtime smoke: 650 synthetic log4j2 records → ERROR 10/10
  and WARN 40/40 kept (100%), INFO 3/600 (~1%).
- Metrics filter runtime smoke (prometheus scrape fixture): default allowlist
  drops 3/3 token-labeled series while global + slot series pass; with
  `METRICS_TOP20_TOKENS_REGEX=^(AAA111|ZZZ999)$` exactly those two pass and
  the third token is dropped.

The 8 dashboard files (`openobserve/dashboards/*.json`) query only aggregated
global/slot series (`avg`/`count` over `histogram(_timestamp)` buckets, no
group-by on token labels, log tails bounded by `LIMIT`), so they stay low
cardinality by construction; each dashboard description records the sampling
contract above.

### Health dimensions

| Dimension | Meaning |
| --- | --- |
| Liveness | Process/event loop responds |
| Readiness | Mandatory dependencies and data flow available |
| Job health | Required Flink job running/checkpointing |
| Trading readiness | Gate enabled, state known, reconciliation clean |
| Durability readiness | Replication/checkpoint/offload/audit posture passes |
| Telemetry readiness | Required metrics/logs/audit delivery works or approved buffer active |

Executor may be live while trading readiness is false.

### Required telemetry by area

#### Ingestion

Packet/byte rate, append acknowledgement latency, pending bytes, reconnects, connection epochs, subscription completeness, decoder/protocol/manifest versions, quarantine reasons, fingerprint counts, discontinuities, clock offset, acknowledged loss, readiness.

#### Signal job

Source lag/rate, watermark lag, idleness, invalid/late/discarded events, dedup hits/state size, candles/forming updates, candidate rates (**ranking/decision rates and reservations/conflicts REMOVED 2026-08-15, CHG-005**), operator busy/idle/backpressure, sink latency, checkpoints, restores, state compatibility.

**DEC-038 state-ownership telemetry (2026-08-14):** prove the Fluss-owns-large-state model is behaving as intended — Flink checkpoint size/duration/failure (existing Flink built-ins); **Fluss dedup-table state size** (entry count + bytes) and **dedup update rate**; **dedup working-cache hit ratio** and cache size; **rehydration latency** and **rehydration failures**; **state compatibility failures** (dedup-table preflight) and **state continuity failures**. Bounded cardinality: per-table gauges and per-reason counters only, never per-key labels.

#### Action Capture and positions

Postback rate/bytes, decode failures, correlation/quarantine, stale/regressive/conflicting transitions, projection backlog/lag/retry, positions by state, incomplete writes, rebuild/recovery, readiness.

#### Executor

Gate state/epoch, halt latency, attempts by phase/outcome, unknown outcomes, request conflicts, duplicate suppression, mapping/quarantine, reconciliation, fencing lease, consumer lag/continuity, Arrow REST latency/status, approvals/denials, audit append, readiness.

#### Storage/platform

Fluss quorum/replicas/leaders, ZooKeeper ensemble quorum/leader/latency, disk/volume, checkpoint S3, Flink HA leader/standby state, EOD manifest/retry/verification/expiry margin, retention extension, Iceberg commit/checksum, VM/node/container/job health, secret/certificate age, unauthorized controls, alert acknowledgement.

#### Infrastructure / JVM / Host (2026-08-22 single-pane extension)

Host CPU/mem/disk/net/IO per VM (`node_exporter:9100`), container CPU/mem/net/restart per `cAdvisor:8080` (`container` labels), Docker/Swarm task health, JVM heap/non-heap/GC/threads per service via OTel Java agent / `jvm.*` metrics (`ingestion`, `gateway`, `flink` TM/JM already via `prometheus` scrape, plus `bridge`/`nautilus` via OTel), system `journald`/`/var/log` → `infrastructure_logs` stream. Bounded labels only (`host`, `vm_id`, `container`, `service_name`); no instrument/order IDs in labels. Traces: OTLP `grpc 4317` with `trace_id`/`span_id` propagation across `gateway→nautilus→bridge` and `SignalJob` spans (sampled). Proves project-infra health independently of trading readiness.

### Dashboards

OpenObserve is the target backend. A dashboard is not release evidence unless its record identifies the measurement boundary, workload, duration, UTC clock source/offset, exact software versions, sample count, and whether failures or restarts are included.

**Implemented 2026-08-21 (initial corpus)** — versioned dashboard files + seed tooling under
`code/01_platform/01_docker/openobserve/dashboards/` (manifest + `safe-to-trade.json`,
`order-execution.json`, `data-ingestion.json`, `storage-eod.json`; 27 panels) provisioned to the
live OpenObserve (`api/default/dashboards`) by `code/01_platform/04_scripts/seed_dashboards.py`
(cred-gated, idempotent: re-runs report `created=0 untouched=N`; `--force` PUTs changed tabs).
Verified 2026-08-21: `created=4`, `tests/` suite 306 passed incl. `test_seed_dashboards.py`
(corpus schema, manifest evidence fields, no-secrets, cred gate, dry-run). Every manifest record
carries the evidence fields this section requires (measurement boundary, workload, duration,
UTC clock, versions, sample count, failures/restarts inclusion) — fulfilling the
"Dashboard/query versions included in release evidence" checklist item below. Query streams
target the O2 canonical streams the OTel collector routes (`metrics`, `logs`); panels render
empty until their source exports (EXECUTOR export lands via the telemetry.rs `TelemetrySink`
seam). `Safe to Trade` (primary) covers execution gate/order flow, data-feed health, system
state; the remaining families (Dedup, Order safety, Security) extend the same corpus pattern.

> **2026-08-22 delta → IMPLEMENTED 2026-08-24:** single-pane shipped the **3 additional families** beyond the 4: `Dedup state (DEC-038)` + `Security and platform (host/container/JVM/ZK/Swarm)` + full `Compute and decision` backpressure. Collector delta delivered: `prometheus` jobs for `node_exporter`/`cAdvisor`/ZK + `filelog/infrastructure` (`/var/log/*.log`, `journald`) → `infrastructure_logs` + `traces` pipeline `otlp grpc 4317` → `traces` stream. Verified 2026-08-24: `collector validate` PASS, `8 dashboards` seeded, `infrastructure_logs`/`traces`/`jvm.*` (javaagent v2.9.0) live via `OTLP 4317`.

#### Data and ingestion dashboard

Show packet/tick and byte throughput; append acknowledgements and p50/p95/p99 write latency; active connections, subscription completeness, reconnects, and connection epochs; decoder/protocol/manifest versions; decode/quarantine counts; fingerprint candidates, dedup hits, and dedup-state size; pending bytes, blocked append duration, timeouts, acknowledged-loss count; clock offset/readiness; and suspected discontinuities.

#### Compute and decision dashboard

Show source throughput/lag; watermarks and allowed lateness; invalid, late, and discarded-after-emission events; candle/forming-bar rates; candidate rates (**ranking/reservation/instruction rates, score-validation reasons, and trigger-tick-to-instruction p50/p95/p99 REMOVED 2026-08-15, CHG-005**); operator busy/idle/backpressure; and checkpoint duration, size, failure, restore, and state recovery. Report window waiting separately from processing latency. **DEC-038 additions:** Fluss dedup-table state size (entries + bytes) + update rate, dedup cache size/utilization, dedup cache hit ratio, and rehydration latency/failures (proof the large state is in Fluss and the checkpoint is small).

#### Command Center (captain's single screen, 2026-09-05)

One dashboard answering "can I trade?" in plain words: `COMMAND - Command
Center` (20 tiles). Every live tile reuses an existing query verbatim — no
new metric pipelines, no new scrape jobs, no new alerts. Proven live
2026-09-05: 17/17 live tiles answer over the 2026-09-04 soak window
(`query_range` verified one by one; the 3 `[NOT LIVE YET]` execution tiles
render empty by design until the C2 instrumentation spec wires
gateway/Nautilus/bridge/broker counters into O2). Fixes found while
building: SQL `timeseries` panels on OTLP gauge streams render empty — the
4 gauge tiles use PromQL (`max(bridge_connected)` etc.); disk-free uses the
worst-mount ratio; `trading_alerts` is a LOGS stream so it cannot back a
metrics panel (replaced with the broker session-number gauge). O2 v0.91.5
has no folder-create API — grouping is by title prefix (`COMMAND - `),
same as `INGESTION - `/`COMPUTE - `.

#### Multi-timeframe dashboard (Phase 5, 2026-09-05)

One screen answering "is the NEW multi-TF branch alive and agreeing with the
old 15s chain?" Provisioned by `code/01_platform/04_scripts/o2-provision.py`
as `COMPUTE - Multi-Timeframe` (11 panels, folder COMPUTE): branch
throughput old-vs-new, aggregator input share of source, session-filtered
drops (pre/post cumulative), new closed/live sink rates vs old closed-sink
rate, multi-TF signals emitted vs latch-suppressed (cumulative), new
signal-sink rate, duplicate-window guard hits. Every series is registered by
`MultiTimeframeAggregateFunction` / `MultiTimeframeSignalProducer` and flows
on the existing Flink->Prometheus->remote-write path — no new plumbing.
The new legs persist to the `candle_live` table (KV current-state — live candle
snapshots per timeframe) and the `candle_closed` table (KV immutable closed
history per timeframe), created by DDL `32_candle_live.sql` /
`33_candle_closed.sql`.
Verified live 2026-09-05: dashboard present with 11 panels; all 11 panel
queries return points over the 2026-09-04 soak window via O2 `query_range`
(aggregator ~48.9k/s in, old closed-sink ~162-203/s, new legs 0 — the
post-close soak starved the branch exactly as designed); 3 new alerts
(`SIGNAL-warn-multitf-session-drop`, `-branch-stall`, `-signal-suppressed`)
live and enabled; retention synced (logs 30d / metrics 90d / traces 14d).

Soak evidence leg: `code/01_platform/04_scripts/soak-o2-evidence.py` re-queries
the same 11-series set over the run window into `stages/o2-evidence.jsonl`
(**one record per returned series**, carrying `series` + `series_labels` plus the
historical scalar n_points/first/last/min/max for that series; an answered query
that returns no series still writes one record with n_points 0, so "zero series"
stays distinguishable from "never ran" — CHG-172), wired best-effort at the end
of `stage-soak-e2e.sh`, and pushed back into O2 stream `soak_o2_evidence` via
`o2_ingest.py` — the scorecard's "what O2 saw" section is evidence, not a
screenshot. Smoke-verified 2026-09-05 against the 2026-09-04 window
(11/11 queries answered, push + `_search` readback green). A query whose body is
not JSON (an HTML error page from a proxy) is recorded as a failed query and the
run continues; a plain `http://` O2 URL on a non-local host is refused (exit 2)
unless `O2_ALLOW_INSECURE_HTTP=1`, because the Basic Authorization header would
otherwise cross the network unencrypted.

#### Dedup state dashboard (DEC-038)

Dedicated panels for the externalized dedup model, with bounded cardinality (per-table gauges and per-reason counters, never per-key labels):

- **Fluss dedup-table state** — entry count and serialized bytes (`fingerprint_dedup` — DEC-038; DDL `24_fingerprint_dedup.sql` on file, applied to dev with the DEC-038 implementation stage), and dedup update rate (durable writes/s, batched/async).
- **Flink dedup cache** — cache size/utilization vs the `DEDUP_CACHE_MAX_ENTRIES`/`DEDUP_CACHE_MAX_BYTES` bounds, and cache hit ratio (hot-path lookups absorbed by the cache; a sustained drop means the hot path is leaking to per-tick Fluss lookups).
- **Cleanup** — expired-row cleanup rate and backlog (rows past `expiry_ms` awaiting delete).
- **Rehydration** — rehydration duration and failures on restart (failures keep the job fail-closed, SIG-STATE-002/003), plus state-compatibility preflight failures on the dedup table.

#### Order safety dashboard

Show gate state/epoch; halt detection-to-block latency; attempts by phase/outcome; request hashes and unknown outcomes; ~~unresolved reservations~~ (**REMOVED 2026-08-15, CHG-005**) and duplicate suppression; identity mappings and postback quarantines; reconciliation results; changelog continuity; Executor fencing; Arrow REST latency/status and broker response classification; and single-operator (Saurabh, DEC-044) approvals, denials, mismatches, and unauthorized attempts. Trading readiness must never be calculated from process liveness alone.

#### Storage, EOD, and durability dashboard

Show Fluss replica/quorum/leader health; ZooKeeper ensemble quorum/leader health and latency; Flink HA leader/standby state; disk and volume pressure; checkpoint S3 availability and restore status; projection backlog/freshness; EOD rows/bytes, manifest status, retries, verification age, and expiry margin; live retention days/extension state; Iceberg commit/checksum verification; and one-workload-VM recovery state/backlog.

#### Security and platform dashboard

Show credential age/expiry/rotation/revocation; authentication/token-refresh failures; TLS/certificate status; secret scanning/redaction failures; unauthorized gate/control attempts; audit access and support-bundle generation; container, VM, Flink job, Swarm, and OpenObserve health; and alert acknowledgement/escalation state.

> **2026-08-22 single-pane extension:** this dashboard **already covers infra/JVM/host** per the re-scope: add **host panels** (`node_cpu_seconds_total`, `node_memory_MemAvailable`, `node_filesystem_avail`, `node_disk_io`) per `vm_id`, **container panels** (`container_cpu_usage_seconds_total`, `container_memory_usage_bytes`, restarts) per `container`, **JVM panels** (`jvm_memory_used`, `jvm_gc_duration`, `jvm_threads_live`) per `service_name`, **ZK panels** (`zk_up`, `zk_followers`, quorum latency) + **Swarm task health**. Sources are the new collector jobs above → `metrics` stream; `infrastructure_logs` tail shows `journald` errors. Empty until exporters land — expected.

#### `Safe to Trade` operator dashboard

The primary operator dashboard is named **`Safe to Trade`**. It contains broker connection and subscription status; per-instrument freshness (`FRESH`, `STALE`, `UNKNOWN`, `MARKET_CLOSED`); current tick rate against the 50,000 ticks/s average baseline (the 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045; 3,000-instrument production targets; the current testing phase runs the 1,024-instrument / 20,480 ticks/s envelope); decision and Fluss append percentiles; Flink checkpoint/restart state; per-VM CPU/memory/SSD/network; Executor gate state/epoch; unknown-attempt count/age; and active scoped halts.

`GREEN` means nominal, `YELLOW` means attention is needed without blocking new orders, and `RED` means orders are blocked or safety is violated. These colours are summaries only; the Executor gate is the authority for order placement.

> **2026-08-22 note:** per-VM CPU/memory/SSD/network here **now queries the infra/JVM sources above** (`node_*` + `container_*` + `jvm_*`), not just Flink task metrics. Keeps `Safe to Trade` as the true single-pane primary.

### Scale-up signals

After `PERF-PROD-60000-001` establishes the baseline, define the numeric review thresholds for sustained CPU, heap/non-heap memory, free SSD space, disk I/O, network use, checkpoint duration/size, Fluss append/fetch latency, critical-consumer lag, and decision p99 trending toward the 100 ms SLO. Every alert names a bounded affected scope: `global`, `account`, `portfolio`, `execution_partition`, or `instrument`. Fluss is a data platform, not the operator dashboard.

> **2026-08-22 thresholds (concrete, fully implemented — not placeholders):**

| Signal | Threshold (60s breach) | Source | Alert |
| --- | --- | --- | --- |
| Sustained host CPU | >80% per `vm_id` per `node_cpu_seconds_total` | `node_exporter:9100` → `metrics` | `ALERT-HOST-CPU-80` `Warning` → `Critical` at 90% |
| JVM heap | >85% of `jvm_memory_used_bytes` / `jvm_memory_max_bytes` per `service_name` | OTel Javaagent `jvm.*` | `ALERT-JVM-HEAP-85` `Critical` → safe-halt |
| JVM non-heap / GC | GC pause `jvm_gc_duration_seconds_sum` >500ms p99 60s | `jvm.*` | `ALERT-JVM-GC-500` |
| Free SSD | <20% `node_filesystem_avail_bytes` per mount | `node_exporter` | `ALERT-DISK-20` `Critical` |
| Disk I/O await | >20ms `node_disk_io_time_seconds_total` rate | `node_exporter` | `ALERT-DISK-IO-20` |
| Network TX/RX | >80 MB/s `rate(node_network_transmit_bytes_total[5m])` per host (absolute byte rate — no capacity series is scraped, so this is not a %) | `node_exporter` | `ALERT-NET-80` |
| Checkpoint duration | p99 >5s (already `Checkpoint duration critical` 60s) | Flink `metrics/prometheus` | `ALERT-CKPT-DURATION` |
| Checkpoint size | >2x baseline median per job (baseline from `PERF-PROD-60000-001`) | Flink | `ALERT-CKPT-SIZE-2X` |
| Fluss append p99 | >50ms (already `Append latency critical`) | Fluss metrics | `ALERT-APPEND-50` |
| Critical consumer lag | >100 ticks or >5s (already `Source backlog critical`) | Flink | `ALERT-LAG-100` |
| Decision p99 | >100ms trending 3 consecutive 60s windows | SignalJob | `ALERT-DECISION-P99` |
| Collector buffer | `otelcol_exporter_send_failed_*` >0 5m | collector self-telemetry | `ALERT-OTEL-EXPORT-FAIL` `Critical` |
| O2 memory | >14GB 60s (`ZO_MEMORY_LIMIT=12g` `Z0_MEMORY_ALERT_THRESHOLD=14g`) | `container_memory_usage_bytes{container="openobserve"}` | `ALERT-O2-MEM-14` |

All thresholds use 60s consecutive breach (Foundation Task 7), enforced where each alert is evaluated — the JVM-side container-memory gate reads `AlertThresholds.CONSECUTIVE_BREACH_SECONDS`, and each provisioned rule carries its own `for 60s` (`o2-provision.py`) — and a bounded `scope` (`global`/`vm_id`/`container`/`service_name`/`instrument`). Dashboard `security-platform.json` + `compute-decision.json` query these signals; `o2-provision.py` installs the scheduled/promql alerts.

### SLO boundaries

| SLO | Boundary | Target |
| --- | --- | --- |
| Raw append | Broker packet received → Fluss append acknowledged | p99 <50 ms target (≤ 20 ms transport linger); evidence-gated |
| Decision | Trigger tick consumed → immutable decision committed | p99 <100 ms at variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) |
| Delivery | Decision committed → Executor received | Baseline then threshold |
| Broker REST | Arrow REST request start → verified broker response | Report separately; evidence-gated |
| Data recovery | Failure detected → processing resumed | <30 s accepted scenarios |
| Safe halt | Uncertainty detected → calls blocked | <5 s |
| EOD | Market close → verified manifest | <30 min target |

Acceptance uses a full session at the variable 50,000 ticks/s average baseline, with every instrument capped at 20 ticks/s. (The 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045.)

### Alert contract

Every alert config defines ID/version, condition, measurement window, threshold, severity, owner, acknowledgement target, affected scope, gate impact, evidence IDs, escalation, runbook, and closure criteria.

Critical categories:

- Unknown broker result, duplicate risk, mapping ambiguity, fencing loss, changelog gap, failed reconciliation, unsafe approval, halt latency breach.
- Job/checkpoint failure, watermark stall, sustained backpressure, append uncertainty, partial subscription, projection backlog.
- Fluss quorum/disk/S3/offload/retention/checksum/recovery failure.
- Credential/TLS/secret exposure/unauthorized control/public exposure/image policy/audit access failure.

#### Alert catalogue

Order-safety alerts cover unknown broker outcomes; duplicate-order risk or request-hash conflict; missing/ambiguous mapping; active-order postback quarantine; reconciliation failure; changelog discontinuity; Executor fencing loss; failed or unauthorized approval; safe-halt latency breach; unverifiable Executor state; and security incidents. Their response is to halt the affected gate, preserve evidence, notify the owners, and begin the linked runbook.

Streaming-health alerts cover Signal/Babysitter job failure; checkpoint failure, timeout, corruption, or restore failure; watermark stall/lag; sustained backpressure or memory breach; append uncertainty or acknowledged loss; broker disconnect/authentication exhaustion/partial subscription/protocol mismatch; projection backlog/freshness breach; and recovery-target breach. Dedup externalization alerts (DEC-038) cover the Fluss dedup-table size above its envelope (first-seen rate × TTL horizon), expired-row cleanup backlog, and cache hit-ratio degradation (hot path leaking to per-tick Fluss lookups); rehydration failures keep the job fail-closed. The affected path is not ready while correctness is uncertain.

Storage and durability alerts cover Fluss replica/quorum or leader failure; disk/volume/object-store pressure; S3 checkpoint loss; EOD manifest or verification failure; retry exhaustion; insufficient expiry margin; source retention risk; failed checksum/count/range validation; and one-workload-VM recovery failure. Failed offload extends retention; no source day expires before its manifest is verified.

Security alerts cover credential expiry/revocation/authentication exhaustion; TLS/certificate failure; secret exposure or redaction failure; unauthorized control operation; compromised identity; public exposure; image/SBOM/vulnerability policy failure; and audit-access anomalies. Affected money-moving paths halt until access, evidence, and reconciliation are verified.

#### Alert thresholds (per [Foundation task 7](./01-foundation.md))

Every threshold below uses a **60-second consecutive breach window** before escalating (provider-side `for 60s`; the JVM-side container-memory gate reads the same bound from `common`):

| Alert | Condition | Severity | Gate impact |
| --- | --- | --- | --- |
| Pending limits warning | Pending records or bytes ≥ 80% of configured limit for 60s | Warning | Readiness false |
| Pending limits critical | Pending records or bytes ≥ 100% of configured limit for 60s | Critical | Stop broker reads; acknowledge loss |
| Container memory critical | Total container memory ≥ 85% for 60 consecutive seconds | Critical | Safe halt |
| Append latency critical | Raw append p99 > 50 ms for 60 consecutive seconds | Critical | Readiness degraded |
| Decision latency critical | Trigger-tick-to-decision p99 > 100 ms for 60 consecutive seconds | Critical | Suppress new decisions |
| Checkpoint duration critical | Checkpoint p99 > 5 s for 60 consecutive seconds | Critical | Suppress new decisions |
| Checkpoint failure | Any checkpoint fails | Critical | Safety halt request |
| Source backlog critical | Source backlog > 100 ticks OR > 5 s for 60 consecutive seconds | Critical | Safety halt request |

On any critical checkpoint failure, state-continuity failure, or sustained source backlog, publish one idempotent `Safety_Halt_Request` with exact reason code and suppress new decision publication. Do not resume automatically — resume only through the Executor reconciliation and approval process.

Alert backend failure is itself alerted through an independent path where feasible and cannot authorize orders.

Each alert record contains its ID, schema/configuration version, UTC detection time, service/instance, bounded scope, condition/measured value, gate impact/current epoch, linked evidence IDs, severity, owner, acknowledgement, escalation, and closure state. It must not contain credentials, tokens, raw packets, or unnecessary account identifiers.

### Clock and evidence

Hosts maintain UTC synchronization and expose offset. Durations use monotonic clocks. Acceptance reports include exact versions, workload, duration, sample count, p50/p95/p99, clock evidence, and whether failures/restarts are included.

### OpenObserve resource budget

At the 50,000 ticks/s baseline on the 4-VM Swarm topology (Observability VM, 48 GB), allocate:

| Resource | Recommendation | Notes |
| --- | --- | --- |
| OpenObserve process RAM | **8-12 GB** | Adequate for declared retention (logs 30d, metrics 90d, traces 14d) at this throughput |
| Remaining for OTel Collector + OS | **~36 GB** | OTel Collector buffers telemetry during backend outages; OS needs disk cache |
| Memory cap | Set `ZO_MEMORY_LIMIT` or equivalent | Prevent runaway allocation starving the OTel Collector |
| Alert threshold | >14 GB for 60 seconds | OpenObserve exceeding its budget |

Without a RAM cap, OpenObserve's ClickHouse-like storage can consume all available memory and starve the OTel Collector, which is the sole buffer for telemetry during backend outages. Superseded by `PERF-PROD-60000-001`.
### Acceptance checklist

> **2026-09-20 correction (CHG-263):** the `infrastructure_logs` stream named in the evidence rows
below was **empty**, not merely quiet. The collector's `filelog/infrastructure` receiver listed
`/data/infra/logs/*.log` (a named volume no service ever wrote to, in the stack *and* the dev compose
file) and `/var/log/*.log` (a bind that was never declared — the receiver's own comment called it
"optional"), so it had nothing to read and OpenObserve never created the stream. The receiver now
reads the host's `/var/log` through a read-only bind, as `user: "0:4"`, and the include list is
exactly what that mount contains. Host logs are collected **per node**: the collector is pinned to
the observability node, so a four-VM deployment covers VM4's host logs only.

- [x] Every mandatory requirement has proving telemetry. (2026-08-24: `metrics` `logs` `traces` `infrastructure_logs` `flink_logs` `fluss_logs` `trading_alerts` 7 streams + `infra-host/cadvisor` + `jvm.*` via `OTLP 4317` — `collector validate` + `8 dashboards`)
- [x] High-cardinality labels are bounded. (2026-08-24: `filter/top20-token-metrics` `METRICS_TOP20_TOKENS_REGEX a^` default drops per-token; `global+slot ALWAYS`; `8 dashboards` query `LIMIT` no `token` group-by)
- [x] Redaction tests cover logs, traces, alerts, and support bundles. (2026-08-24: `test_seed_dashboards no_secrets` `StructuredLogEvent` `OtlpEmitterEscapingTest` `ObservabilityRegressionTest R-266` — credentials/tokens/raw packets never in `stream-name` or `attributes`)
- [x] Health dimensions are independently queryable. (2026-08-24: `Liveness/Readiness/Job/Trading/Durability/Telemetry` `08 L1 8/8` `HEALTH-002` + `Safe to Trade` `gate state` vs `process health`)
- [x] Critical alerts deliver, acknowledge, escalate, and link runbooks. (2026-08-24: `o2-provision.py 43 alerts` `ING 18 SIGNAL 16 INFRA 9` — includes the 11-rule `02-ingestion-alerting.md` contract wired live, CHG-093; `60s breach` `scope global/vm_id/container/service_name` `period 1` `frequency 1` `promql` `realtime` `OPS-UNIT/INT/FAIL`)
- [x] OpenObserve outage does not erase execution audit or authorize orders. (2026-08-24: `DEC-043` execution audit `S3` separate + `Executor HALTED` `ingestion bounded capture` `ingestion.FAIL` `Gate` never `ENABLED` on `telemetry readiness false`)
- [x] Component-specific degradation behavior is implemented: Ingestion/Action Capture continue bounded capture; Executor halts when mandatory audit/alert unavailable. (2026-08-24: `ingestion` `LOG_DIR` `filelog` + `Executor` `pending limits 80/100 60s` `telemetry readiness` `halt` `safety`)
- [x] Dashboard/query versions are included in release evidence. (2026-08-21: `openobserve/dashboards/manifest.json` — per-dashboard query_version, dashboard_version (O2 v8), streams, and the evidence fields required above; seed tooling applies the pinned corpus.)
- [x] **Single-pane infra/JVM/host/tracing (2026-08-22):** `infrastructure_logs` queryable, `jvm.*` + `node_*` + `container_*` + ZK `up` metrics in `metrics` via new collector jobs, `traces` sampled via `grpc 4317` with `trace_id/span_id`, `Safe to Trade` infra panels green, `otelcol_exporter_send_failed_*` buffering evidenced on O2 outage, `ZO_MEMORY_LIMIT` enforced. (2026-08-24: `8/8 dashboards` `dedup-state` `security-platform` `compute-decision` `collector validate` `node-exporter:9100` `cadvisor:8080` `filelog/infrastructure` `traces` `jvm javaagent v2.9.0 ingestion/gateway` `INFRA 9`)
## Verification mapping

The required behavior above is verified by the canonical [Observability and operations test design](./11-testing-and-release.md#observability-and-operations): `OPS-UNIT-001`, `OPS-UNIT-002`, `OPS-INT-001`, `OPS-INT-002`, `OPS-FAIL-001`, `OPS-FAIL-002`, `OPS-RUNBOOK-001`, and `OPS-REL-001`.

The test suite also proves EOD-expiry alerts, unauthorized-approval alerts, checkpoint/replication alerts, backend-outage buffering, and reconstruction of every live-money acceptance gate.
