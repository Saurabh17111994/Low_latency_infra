# Maintenance and Change Operations

## Maintenance principles

Maintenance is performed against an explicit environment, release/configuration identity, state/schema compatibility classification, owner, rollback plan, and verification checklist. Production money-moving changes begin with the Executor gate `HALTED`.

Local Compose maintenance is not production procedure or HA evidence.

## Planned maintenance procedure

1. Create a change record with scope, affected components, requirements/contracts/DDL/interfaces, versions, state impact, risk, rollback, and acceptance checks. Records are filed in [`../05_deployment/change-records/`](../05_deployment/change-records/_template.md) as `CHG-<N>.md` using the template, and must name the six fields validated by docs-audit C14.
2. Schedule outside critical workload windows where possible and verify EOD retention margin.
3. Confirm backup/checkpoint/savepoint and audit availability.
4. Halt new money-moving calls and reconcile attempts, orders, fills, positions, mappings, and projections. (**Reservations REMOVED 2026-08-15, CHG-005.**)
5. Verify the target artifact/configuration is immutable and compatibility-tested.
6. Apply the change with controlled Swarm update, restart, DDL, secret, network, or capacity procedure.
7. Monitor the required health, dashboard, and alert views in [`../08_implementation/10-observability.md`](../08_implementation/10-observability.md).
8. Run targeted functional, recovery, security, and SLO checks.
9. Keep gate halted until reconciliation and post-change verification pass.
10. Require the single-operator (Saurabh, DEC-044) authenticated approval for the same current epoch/evidence hash before enablement.
11. Record outcome, evidence, deviations, and follow-up work.

## Change classes

### Application and dependency update

Requires immutable image digest, exact dependency/version matrix, unit/integration/compatibility tests, state/savepoint analysis, and rollback artifact. Rolling/canary deployment is allowed only where compatibility is proven.

### Flink job update

Verify serializer/state compatibility, checkpoint/savepoint restore, source offsets, multi-TF window state, and sink behavior. (**Ranking state and instruction-duplication risk REMOVED 2026-08-15, CHG-005.**) **(SUPERSEDED 2026-09-03, DEC-054: dedup is an operator-local per-token count window, intentionally not checkpointed; there is no Fluss dedup table and no rehydration path — the window rebuilds from live ticks.)** Any uncertainty keeps the affected path not ready and the gate halted.

### Fluss schema or DDL change

Requires versioned DDL, ownership/identity review, additive/removed/renamed field analysis, replay/lake tests, consumer compatibility, deployment order, rollback/reset/replay plan, and migration evidence. Clean-break changes are pre-live only unless separately approved.

### Configuration/capacity change

Record workload basis, resource impact, bucket/partition/replication impact, checkpoint/backpressure implications, SLO test, and rollback. Do not use lower-than-production workload as readiness evidence.

### Retention or EOD policy change

Verify manifest fields, counts, ranges, hashes/checksums, commit state, retry behavior, expiry margin, three-day live buffer, approved audit-retention policy, and legal/compliance approval where applicable.

### Secret/network/security change

Use the secrets runbook and security tests. Verify least privilege, rotation/revocation, TLS/transport, redaction, public exposure, audit access, and compromised-credential recovery.

## Routine checks

At the approved cadence, review the required dashboard and alert views in [`../08_implementation/10-observability.md`](../08_implementation/10-observability.md), then follow the relevant runbook or maintenance procedure for any failed condition. Record the review outcome and evidence.

## Emergency maintenance

For imminent safety or durability risk:

1. Halt the affected gate immediately.
2. Preserve evidence and announce scope/owner.
3. Apply the smallest reversible mitigation.
4. Do not delete volumes, checkpoints, audit, or source data without authorization.
5. Reconcile all affected state and execute rollback/DR procedures as needed.
6. Restore service readiness only through the single-operator (Saurabh, DEC-044) approval process.

## Closure criteria

A maintenance event is closed only when the change is verified, alerts are clear or dispositioned, state and offsets are consistent, storage/retention safety is intact, audit evidence is stored, rollback remains understood, and the gate epoch has either remained halted or been explicitly re-enabled by the authorized single operator (Saurabh, DEC-044).

## References

- Deployment strategy: `../05_deployment/00-release-strategy.md`
- CI/CD: `../05_deployment/01-ci-cd.md`
- Rollback: `../05_deployment/03-rollback.md`
- Secrets rotation: `../05_deployment/04-secrets-rotation.md`
- Operational requirements: `../02_requirements/06-operational.md`
