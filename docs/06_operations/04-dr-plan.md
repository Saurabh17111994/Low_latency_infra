# Disaster Recovery Plan

## Recovery objectives and limits

The platform reports RPO/RTO for each tested scenario. It does not claim general HA or exactly-once behavior beyond the evidence.

Targets for accepted scenarios:

- Data processing recovery under 30 seconds
- Safe-halt under five seconds after uncertainty detection
- At least three complete trading days retained live
- EOD manifest verification under 30 minutes at full-volume baseline
- No duplicate broker orders in crash-window tests

The order path does not resume automatically after any uncertainty.

## Protected assets

- Fluss LOG events and KV operational state
- Flink checkpoints/savepoints in encrypted, versioned S3
- `Execution_Audit`, postback/fill audit, gate transitions, attempts, mappings, approvals, and reconciliation evidence
- EOD Iceberg manifests, source ranges, counts/bytes, hashes/checksums, and commit state
- Immutable release/configuration/version records
- Quarantine and suspected-discontinuity evidence

Operational projections must be rebuildable from immutable events/audit or have a tested backup/restore contract.

## Failure scenarios

### Process or container failure

Restart from the approved immutable artifact and tested checkpoint/savepoint. Verify state, offsets, projections, changelog continuity, readiness dimensions, and telemetry. For the Signal job, verify Fluss authoritative state after the compact checkpoint restore and rehydrate the dedup working cache (DEC-038). Executor state uncertainty starts `HALTED`.

### Flink JobManager/TaskManager or checkpoint-store failure

Keep the order gate halted if signal or changelog continuity is uncertain. Restore the compact Flink checkpoint from S3, verify Fluss authoritative-state availability and compatibility (dedup state table, candle/signal tables), rehydrate the dedup working cache from Fluss, verify dedup/window/forming-bar state and sink visibility, then reconcile before resume. (**Ranking/instruction continuity REMOVED 2026-08-15, CHG-005.**) (DEC-038, 2026-08-14: the Flink checkpoint is not a second complete copy of the durable Signal business state.)

### Fluss tablet, quorum, or workload VM loss

Verify three-node replication/quorum, anti-co-location, remaining capacity, backlog, acknowledged-loss counters, and recovery point. Restore Flink from S3 as needed. Run the one-workload-VM acceptance scenario at the variable 50,000 ticks/s average baseline (90,000 ticks/s peak retired, DEC-036; 60,000 ticks/s gate, DEC-045) and record RPO/RTO.

### Broker or Arrow REST outage

Stop affected calls according to the Executor contract. Unknown responses become `UNKNOWN`; do not blindly retry. Reconcile broker orders, fills, positions, and mappings after service recovery.

### S3 outage

Checkpoint/offload readiness fails. Do not expire source data. Keep the gate halted when checkpoint recovery or EOD verification is affected. Retry with approved backoff and verify manifests/checksums before marking recovered.

### Observability outage

Preserve local structured logs and immutable execution audit. If required safety evidence or alerting is unavailable, trading readiness fails and the order gate halts according to policy. Recover telemetry and verify alert delivery.

### Security/credential compromise

Halt affected order flow, preserve evidence, revoke/rotate credentials, verify least privilege and redaction, scan for unauthorized access, reconcile all affected execution state, and require single-operator (Saurabh, DEC-044) approval.

## Recovery sequence

1. Declare incident, scope, severity, owner, and gate impact.
2. Halt new money-moving calls.
3. Preserve logs, audit, checkpoints, offsets, manifests, configurations, and version IDs.
4. Stabilize quorum, storage, network, credentials, and deployment ownership.
5. Restore jobs/services from approved artifacts and compatible state.
6. Verify schemas, replication, checkpointing, changelog continuity, projections, and telemetry.
7. Reconcile broker orders, fills, positions, attempts, and mappings. (**Reservations REMOVED 2026-08-15, CHG-005.**)
8. Verify EOD retention/offload safety.
9. Produce evidence hash for the current gate epoch.
10. Require the single-operator (Saurabh, DEC-044) authenticated approval before enabling the gate.
11. Record RPO/RTO, data loss/lateness accounting, root cause, and follow-up actions.

## DR exercises

At minimum test process restart, tablet/worker loss, one workload VM loss, checkpoint corruption/unavailability, S3 interruption, Fluss quorum degradation, observability loss, broker disconnect, credential revocation, Executor crash windows, postback projection partial failure, and EOD offload retry.

Exercises use production-like instruments, rates, packet distributions, connection counts, exact versions, and realistic state volume. Simulated trading does not waive live-money release gates.

## DR evidence

Retain timeline, failure injection, commands/procedures, checkpoint IDs, offsets, replica state, backlog, audit IDs, broker evidence, gate transitions, approvals, RPO/RTO, and verification results. Do not delete durable state or source data during an emergency without approved destructive-change authorization.

## References

- Rollback: `../05_deployment/03-rollback.md`
- Operational requirements: `../02_requirements/06-operational.md` §§6.5–6.7
- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Storage contract: `../04_contracts/02-storage.md`
