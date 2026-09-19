# Live-Readiness Gaps

## Purpose and authority

One page answering: what stands between the current platform and a live-money
release. This document states nothing new. Every row restates an existing gate
evaluation and names its source, so a reader can check it. Where this document and
the release gate documents disagree, those documents win.

Sources:

- `docs/05_deployment/00-release-strategy.md` — release stages and release gates
- `docs/08_implementation/RELEASE_EVIDENCE_2026-08-21.md` — binary release gates and the approval record
- `docs/08_implementation/09-production-swarm.md` — milestone status M1–M3
- `docs/08_implementation/08-local-compose.md` — local profile status

See also: `docs/plans/2026-08-25-live-readiness-unified-plan.md` is the single
execution ledger — the ordered phases, the per-task checkboxes, and every
`BLOCKED:` reason live there. This page deliberately repeats no task status, so the
two cannot drift: rows below come from the release gate documents, and task-level
progress belongs to the ledger.

## Current position

- The release strategy states the platform is **blocked for live-money release**
  until the evidence-gated protocol, exact-version, schema, safety, capacity,
  recovery, security, and observability gates pass. Paper or simulated trading does
  not waive those gates.
- Stage 1 (local contract validation) is done offline: 121/121 offline suites plus
  `local_int_004` pass on the single-host Compose profile.
- The evidence package records six live-money gate rows as `NOT_PASSED` or
  `EVIDENCE-GATED`, the gate epoch as `HALTED`, and all five approvals (platform,
  execution, security, operations, compliance) as `pending E5`.
- Stage 2 (production-like acceptance) is where progress stops. Every remaining item
  is missing real-world evidence, not missing code.

## What is missing

| # | Missing evidence | Why it is open | Gates it holds |
| --- | --- | --- | --- |
| 1 | A live order has never been placed through the platform (`BI-EQ ×1`, sandbox) | Needs a market-hours session | data gaps `REL-DG`, protocol success half `REL-PROTO`, requirements `REL-REQ`, postback fill corpus |
| 2 | A production capacity number: 50,000 ticks/s session average against the 60,000 ticks/s gate (DEC-045) | `PERF-PROD-60000` needs production VMs | capacity `REL-PERF` |
| 3 | One-workload-VM-loss posture: data under 30 s, order halt under 5 s, no duplicate order | `FAIL-VM-LOSS` needs production VMs | HA/recovery `REL-HA` |
| 4 | Real HA: 3-node ZooKeeper quorum, `replication.factor=3`, encrypted S3 checkpoint/savepoint recovery | `DR-001..006` are green only on a single-node mimic | HA/recovery `REL-HA` |
| 5 | Version matrix rows 7–10 (postback wire, Arrow REST, OpenObserve, base images) | Still `TO_BE_VERIFIED`; E1 waits on A3–A5 + D7 | exact-version matrix |
| 6 | Alert thresholds and live dashboards | Need measurements from rows 2–3 (D4/D5/D7) | observability |
| 7 | Ingestion acceptance rows still lacking artefacts (7 of 15) | `EVIDENCE_BLOCKED`/`NOT_IMPLEMENTED`, unblocked by `test/ingestion/*` on production VMs plus market hours | requirements `REL-REQ` |
| 8 | Single-operator approval (DEC-044) | Gate path and hash binding are proven; the real epoch flip waits for E5 sign-off after rows 1–7 | approval `REL-APPROVAL`; automatic enablement and automatic resume are prohibited |
| 9 | Current-tree caveats recorded 2026-09-19 | Last certification run `f2faf565`; no gate run since, by decision; no live load-test run exists, so no throughput figure exists | none — recorded so recent offline work is not mistaken for certification |

## Bucket view

What actually unblocks the list above:

- **One market-hours sandbox evening** — clears row 1 and most of row 5. The largest
  single win, because row 1 alone holds four gates.
- **Four VMs**, physical or rented — clears rows 2, 3, 4 and part of 7.
- **Measurements taken during those runs** — clears row 6.
- **The single-operator approval** — row 8, last, after the rest pass.

Nothing in this list is source code. Additional code work does not move it.

## What a single VM can and cannot prove

The single-host Compose profile closes Stage 1 and supports learning, simulation, and
fake-broker smoke runs. It cannot produce evidence for:

- replication, quorum, or one-VM-loss tolerance (`replication.factor=1` locally)
- encrypted S3 checkpoint recovery (local checkpoints use a local path)
- production capacity (`~10k ticks/s` is the local target; the production gate is
  50,000 ticks/s average, 60,000 ticks/s peak per DEC-045)
- any live order, including the sandbox order in row 1

The local profile rejects production credentials and keeps the executor halted by
default (`EXECUTION_ENABLED=false`; a fresh or unverifiable start begins `HALTED`), so
running it cannot place a real order.

## What this document does not do

It does not replace the release gates, and it does not change any gate status.
Approving a live-money release requires the gate evidence itself, evaluated per
`docs/05_deployment/00-release-strategy.md`.
