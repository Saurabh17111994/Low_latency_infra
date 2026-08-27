# Segment Build Contract — Platform and Runtime

## Environments

Local development/integration uses Docker Compose. Production uses Docker Swarm — v1: 4 VMs (3× Manager+Worker + 1 O2), v2: 7 VMs (3× Manager ONLY + N≥3 Workers + 1 O2, same stack via drain). Compose is not production HA evidence. See 09 DECISION 2026-08-20 v1→v2.

## Production requirements

- Exact image digests and Java/Python/Flink/Fluss/Arrow REST/protocol versions; no `latest`.
- Three-node Fluss replication/quorum with anti-co-location.
- Flink checkpoints/savepoints in encrypted S3 (G4 Durability T9):
  prod requires `CHECKPOINT_DIR=s3://` or `s3a://` (bucket encryption AES256, R2 default or S3 SSE-S3 via `fs.s3a.server-side-encryption-algorithm=AES256` in `SignalJob`/`BabysitterJob`) — validated by `SignalJobConfig` which fails fast at startup with a clear, actionable message if `CHECKPOINT_DIR` is missing or not `s3://` when `DEPLOYMENT_ENV=production` or `PROFILE=prod` (case-insensitive, `PROFILE=prod` alias honored); `SAVEPOINT_DIR` likewise requires encrypted S3 in prod when set; Swarm `docker-stack.yml` enforces `CHECKPOINT_DIR=${CHECKPOINT_DIR:?encrypted s3:// mandatory}}` and `state.checkpoints.dir` + `state.savepoints.dir` + `fs.s3a.server-side-encryption-algorithm=AES256`, secrets via Swarm (`aws_access_key_id`/`aws_secret_access_key` at `/run/secrets/...` with `_FILE` fallback in the job); dev keeps a local durable volume (`flink-checkpoints:/checkpoints` via `docker-compose.yml` `CHECKPOINT_DIR=file:///checkpoints` default, `DEPLOYMENT_ENV=dev`) — no S3 required in dev, and `SignalJobConfig` deliberately allows `file://`/`/tmp` only in dev.
- Mandatory encrypted overlay/TLS-protected transport for all sensitive paths (broker, Arrow REST, S3, operator control, secret delivery, and cross-host money-moving/state traffic). Exact mechanism remains evidence-gated but encryption is not optional.
- Docker Swarm secrets and least-privilege service identities.
- Durable volumes and encrypted policy-controlled audit/lake storage with a one-year minimum target.
- Executor fencing: one active owner per `execution_partition_id`.
- EOD controller service or scheduled job owning manifest lifecycle, retention extension, and storage-pressure alerts.
- N+1 resource budget: per-VM CPU, memory, network, disk, Flink slots, Fluss capacity, checkpoint bandwidth, and catch-up rate documented; post-loss validation at the variable 50,000 ticks/s average baseline. (The 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045.)

## Readiness

Fluss quorum/schemas, Flink jobs/checkpoints, broker subscriptions/schema, Executor durable state/changelog/Arrow REST, observability, and gate state are checked independently. Startup never auto-enables order placement.

## Capacity and failure gates

Pass variable 50,000 ticks/s average baseline full session (≈16.7 ticks/s/instrument average; 90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045), one workload VM loss at per-instrument production rate, data recovery <30 seconds, safe-halt <5 seconds, decision p99 <100 ms, and EOD manifest <30 minutes target.

## Security and rollout

Money-moving deployments begin halted and require reconciliation/single-operator (Saurabh, DEC-044) enablement. Rollback defaults halted when uncertain. Secret rotation, network exposure, TLS, redaction, least privilege, audit access, vulnerability policy, and compromised-credential recovery are tested.

## Requirement traceability

- Functional: `REQ-PF-001` through `REQ-PF-012`
- Cross-cutting: `03-non-functional.md` §§3.1–3.8; `04-data.md` §§4.1, 4.3, 4.6–4.7; `05-interfaces.md` §§5.1–5.11; `06-operational.md` §§6.1–6.10

See `../02_requirements/02-functional/09-platform-runtime.md` and `../02_requirements/06-operational.md`.
