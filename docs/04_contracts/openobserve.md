# OpenObserve Integration Contract

> Source of truth for the platform observability backend. Derived from the
> OpenObserve Integration Specification (self-hosted, provide-only-OpenObserve
> rule). In authority order this is a *contract* layer; executable code and
> tests outrank it on conflict.

## A. Deployment

- **Type:** self-hosted.
- **Image (pin):** `public.ecr.aws/zinclabs/openobserve:v0.91.5-amd64`.
- **Topology:** OpenObserve + OTel Collector + (optional) Prometheus/Alertmanager on VM4. Flink/Fluss run on VM1–VM3.

## C. Ingestion protocol

- **Primary:** OpenTelemetry (**OTLP**) through the OTel Collector.
- **Transport:** OTLP/gRPC is the canonical transport for logs and traces; OTLP/HTTP is permitted for metrics emitters that cannot use the gRPC client.
- **Fallback (only for non-OTLP components):** HTTP JSON ingest (`openobserve:5080/api/<org>/logs/_json`). The Java `common` services emit OTLP-shaped records via the collector; they do not call HTTP directly.

## D. Endpoints + auth

- **Collector logs/traces target:** `http://otel-collector:4317` (OTLP gRPC).
- **Collector metrics target:** `http://otel-collector:4318/v1/metrics` (OTLP/HTTP).
- **Per-host file collection (CHG-273):** a second collector, `otel-collector-logs`, runs on every
  node (`mode: global`) and owns every `filelog` receiver — the host's `/var/log` and the node-local
  log volumes. It deliberately exposes **no** OTLP receiver: emitters keep targeting
  `otel-collector`, and shipping logs to a per-node service would scatter one service's records
  across nodes.
- **OpenObserve UI/API:** `http://openobserve:5080`.
- **Auth:** Basic Authentication, **read from environment variables** — never hardcoded.

Application emitters must not send credentials directly to OpenObserve. The
collector owns the OpenObserve exporter and authentication configuration.
Collector failure must not block ingestion, SignalJob processing, or order-path
safety behavior.

## E. Stream / signal mapping (relevant to `common`)

| Stream | Our source |
| --- | --- |
| `platform_logs` | Executor, scheduler, trading/risk engine, REST APIs (incl. `common` StructuredLogger) |
| `flink_logs` | JobManager/TaskManager/operator/checkpoint events |
| `fluss_logs` | Coordinator/TabletServer/storage |
| `infrastructure_logs` | Docker/Linux/system |
| `trading_alerts` | **`AlertThresholds`** (8 categories), risk violations |
| `metrics` | Flink/Fluss/JVM/Python/infra metrics |
| `traces` | distributed requests, pipelines, API requests |

## F. Structured logging contract (every record)

**Required:** `timestamp, level, service, component, subsystem, host, vm_id, environment, correlation_id, trace_id, span_id, message`.
**Optional:** `symbol, timeframe, strategy, job_name, task_name, exception, stacktrace`.
Logs **must be structured JSON**; plain text is prohibited. Our `StructuredLogger`
already emits all required fields plus optional `symbol/strategy/exception`.

## G/H/I. Metrics / tracing / alerts

- **Metrics:** collect Flink (job health, checkpoints, backpressure, throughput, latency), Fluss (r/w throughput, tablet health, storage), Python (request/latency/queue/error), infra (CPU/mem/disk/net/Docker/VM).
- **Tracing:** OTel standard; every request propagates `trace_id` + `span_id`; traced logs include them.
- **Alerts:** sources = trading/risk/scheduler/executor/Flink/Fluss/infra; destination = `trading_alerts`; categories = Critical/Error/Warning/Information.
- SignalJob-specific metrics include source throughput/lag, validation rejection reasons, deduplication state, watermarks, candle KV writes (sole candle output — 2026-08-13), signal LOG/KV sink counts, sink failures, checkpoints, backpressure, memory, restarts, and full-replay startup mode.
- Distributed TaskManager metrics must use a distributed-safe Flink metrics/reporter path. A single-JVM static counter is diagnostic-only and is not production evidence.

### Metric naming and label conventions (verified live 2026-08-11)

- **Naming:** O2 stores metric names as lowercase stream names (PromQL queries the stream name directly); emitter normalization is dot→underscore (`compute.invalid.byReason.schema-version` → `compute_invalid_byreason_schema_version`). Flink reporter names are the Flink metric registry names verbatim (`flink_taskmanager_job_task_operator_*`).
- **Label sets are bounded** (P8.5 audit, all 53 label names across the store enumerated):
  - Flink reporter families: `{host, instance, job, job_id, job_name, operator_id, operator_name, task_id, task_name, task_attempt_num, task_attempt_id, subtask_index, tm_id}` — fixed by cluster topology; `bucket` (Fluss reader) is bounded by the table's bucket count (16).
  - Collector/OTel families: `{service_name, service_instance_id, service_version, instrumentation_library_*, aggregation_temporality, is_monotonic, exporter/receiver/transport, start_time, flag}` — fixed SDK labels.
  - Scrape targets: `{instance, job}`.
- **Forbidden:** no payloads, stack traces, order IDs, symbol/ticker/instrument tokens, or unbounded instrument IDs in metric labels — none exist in the live store (label-name audit). `task_attempt_id` is per-attempt and grows only with restarts; attempts age out with the 90d metric retention.
- **Units/temporality:** checkpoint duration ms, checkpoint size bytes, record counts as counters, latency histograms ms; emitter counters are DELTA temporality (one temporality per stream).

## K. Retention

- Logs 30d, Metrics 90d, Traces 14d, Alerts 180d.
- **Note:** this does **not** satisfy the project's separate policy-controlled audit retention
  requirement. Audit retention is a separate S3-backed store (deferred item);
  OpenObserve is the *live* observability backend only.

## L. AI-agent rules (relevant to code)

- Never bypass the OTel Collector.
- Never write observability data to local files except temporary buffering.
- Never introduce additional logging systems.
- Emit structured JSON logs only; use OTel semantic conventions.
- Treat OpenObserve as the **only** observability backend.

## M. Implementation status and evidence boundary

- The current compute implementation emits selected metrics, including
  schema-version rejections and startup mode, through OTLP/HTTP to the
  collector. This is partial coverage, not complete SignalJob observability.
- The collector's `filelog` receiver is enabled (2026-08-11) for the approved
  ingestion-JSON-log scope (`/data/ingestion/logs/*.json`), verified queryable in
  the `platform_logs` stream (P8.2 evidence). Structured SignalJob/Flink logs
  remain operator-gated (live SignalJob restart required).
- Distributed Flink metrics, end-to-end traces, dashboard queries, alert firing
  and recovery, retention, and collector/OpenObserve outage recovery require
  runtime evidence; source code or a configuration file alone is insufficient.
- OpenObserve availability and delivery are live-observability concerns. The
  audit-retention requirement remains a separate S3/object-store
  control and must not be marked complete from OpenObserve evidence.
- The corrective implementation/evidence tracker for the candle replay and
  production-readiness work is the master dossier
  `docs/08_implementation/04-signal-job.md` §Absorbed documents (2026-08-17
  consolidation — tracker `14-candle-log-kv-replay-safety_2.md` was deleted); it operationalizes this contract without
  replacing it.

## N. Platform observability requirements

### Boundary

OpenObserve receives operational logs, metrics, and supported traces. Correlation
IDs and immutable execution audit remain mandatory even when distributed trace
propagation is unavailable.

### Required proof

- Data and compute metrics prove throughput, latency percentiles, fingerprint
  behavior, invalid/late data, watermarks, backpressure, and checkpoints.
- Order metrics prove gate epoch/state, halt latency, attempt outcomes,
  unknowns, mappings/quarantine, reconciliation, approvals, changelog
  continuity, and Arrow REST responses.
- Storage and runtime metrics prove replication/quorum, node health, checkpoint
  storage, EOD manifest/retry/expiry margin, storage pressure, secrets, and
  security events.

### MVP alerts and health

Order-safety, streaming-health, storage-safety, and security alert groups must
be configured, routed to `trading_alerts`, and tested. Thresholds are versioned
configuration. Health must separate liveness, readiness, job health, trading
readiness, and durability readiness.

### Security and audit

Credentials, original payloads, and unnecessary account identifiers must be
redacted. Audit access must be role-restricted and audited. Execution, gate,
order, fill, correlation, approval, reconciliation, and future position-action
audit must be immutable, encrypted, and retained for at least one year or longer under approved policy in a separate
S3/object-store-backed audit system.

### Acceptance

Metric and alert emission, redaction, cardinality, backend-outage behavior,
health transitions, safe-halt alerts, offload-expiry alerts, unauthorized
resume alerts, and release-gate reconstruction tests must pass. OpenObserve
outage must not erase durable execution audit or authorize orders. Components
must expose their degraded reason and bounded buffering state.

### Component-specific degradation

Ingestion and Action Capture may continue bounded evidence capture only when
durable source/audit writes, local buffering, and readiness policy remain
healthy. The Executor must halt new money-moving calls when mandatory execution
audit, safety-control acknowledgement, or alert visibility is unavailable.
Telemetry delivery must never block SignalJob processing, ingestion, or order
path safety behavior; telemetry loss must be visible through a delivery-health
signal.

### Requirement traceability

- Functional: `REQ-OBS-001` through `REQ-OBS-008`.
- Cross-cutting: `03-non-functional.md` §§3.1–3.8; `04-data.md` §§4.1,
  4.3–4.7; `05-interfaces.md` §§5.9–5.11; `06-operational.md` §§6.3,
  6.5–6.10.
- Implementation and evidence tracker:
  `docs/08_implementation/04-signal-job.md` §Absorbed documents (2026-08-17
  consolidation; `14-candle-log-kv-replay-safety_2.md` was deleted).

## Matrix status

`openobserve` row in `version_matrix.yaml`: `proposed_version: v0.91.5-amd64`,
`evidence_source: docs/04_contracts/openobserve.md`,
`evidence_method: manual_spec_review`, `compatibility_class: UNKNOWN`
(pending capability-evidence run against a live collector).
