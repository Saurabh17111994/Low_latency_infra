# Production VM Guide — provisioning, deployment, and operation (v1 4 → v2 7 VMs)

- **Plan task:** `D1.1` — VM provisioning + agent-verifiable checklist
  (`docs/plans/2026-08-25-live-readiness-unified-plan.md`, Phase U5)
- **Authoritative references this doc must stay consistent with:**
  `docs/08_implementation/09-production-swarm.md`, `docs/05_deployment/02-environments.md`,
  `code/01_platform/01_docker/docker-stack.yml`, `docs/06_operations/04-dr-plan.md`,
  `docs/05_deployment/06-swarm-secrets.md`.
- **Status at issue (2026-08-21):** D1.1/D1.2 authored; **D1.3 (human provisions the VMs)
  is OPEN** — this is the checklist the operator follows, and `prod_node_check.py` (D1.2)
  is the verification gate that must pass before D2.
- **Merged 2026-09-19:** this is the single production document. §1–§4 define the target
  machines and their verification gate; §5–§6 the prerequisites and the gaps; §7–§14 the
  stage-by-stage path from workstation to operating cluster. The former
  `08-production-deployment-guide.md` was merged here so the deployment path has one home;
  the path was also kept because `prod_node_check.py`, the live-readiness ledger and `CHG-082`
  reference it.

**Status:** Operational guide. It records *procedure*, never *readiness*.
**Authority when documents disagree:** executable implementation and tests → active decisions (`../01_project/04-decisions.md`) → DDLs → build contracts (`../04_contracts/`) → detailed requirements (`../02_requirements/`) → this guide.
**Status authority:** task/status truth lives in the live-readiness ledger (`../plans/2026-08-25-live-readiness-unified-plan.md`) and the deployment dossier (`../08_implementation/09-production-swarm.md`). This guide repeats no status and must not be used as evidence that a stage passed.

> **Read this first.** Stages `S5`–`S10` describe a path this project has **never executed**
> (`09-production-swarm.md` M3 = `NOT FULLY`; M1 docs and M2 static checks only). Treat them as a
> first-run procedure with a debugging budget, not as routine operation. Five mechanics they need do
> not exist yet — they are listed in §6.1 and marked `[NOT BUILT]` at each use. §6.2 lists the exact
> values the deployment is still missing.

**Environment label.** Every stage below carries exactly one: `[WORKSTATION]` · `[LOCAL]` · `[ACCEPTANCE]` · `[PRODUCTION]`.
Local Compose commands are never production procedures (`../02_requirements/06-operational.md` §6).

## 1. Target topology (v2 — 7 VMs, the production target)

| VM | Role | Swarm role | Node labels required | Notes |
|---|---|---|---|---|
| `M1` | Swarm manager #1 | `manager` (Active) | `role=manager` | Raft quorum 2/3; v2 → `drain` |
| `M2` | Swarm manager #2 | `manager` (Active) | `role=manager` | v2 → `drain` |
| `M3` | Swarm manager #3 | `manager` (Active) | `role=manager` | v2 → `drain` |
| `W1` | Worker | `worker` | `role=worker` | Workloads run here |
| `W2` | Worker | `worker` | `role=worker` | |
| `W3` | Worker | `worker` | `role=worker` | |
| `W4+` | Worker (scale, optional) | `worker` | `role=worker` | joins with NO stack redesign |
| `O1` | Observability | outside Swarm | `observability=true` | OpenObserve + telemetry; must NOT be inside the manager quorum |

**Disk:** 500 GB SSD per VM (workload VMs and the observability VM). Managers in v2 are
small-footprint (≈10 GB disk, 2 CPU / 2 GB RAM per `09-production-swarm.md` §v2) — treat
500 GB as the workload/observability floor, not a manager requirement.

**v1 baseline (ship-now, 4 VMs)** — same stack, different labels: `M1 M2 M3` are
Manager+Worker (labels `role=worker` AND `role=manager`, `docker node update
--availability drain` never applied) and `O1` outside Swarm. Per the DECISION 2026-08-20
in `docker-stack.yml`, adopt v2 only on a trigger: `N>6` workers, sustained CPU >80%, or
Raft election flaps.

## 2. Hard rules (fail = provisioning defect)

1. **No hostname pinning anywhere.** The stack places by `node.labels.role == worker` and
   `node.labels.observability == true` only. Never edit `docker-stack.yml` to name a host;
   W4+ joins by labeling, not by stack rewrite (`test_09_stack.py` enforces this).
2. **Manager quorum is 3** (tolerant of 1 loss). O1 (observability) is outside the swarm;
   its loss must never authorize orders or erase the durable audit.
3. **Anti-co-location of critical replicas:** Fluss replicas, ZooKeeper ensemble members
   (3-node, quorum 2-of-3) and Flink HA (JobManager leader) must land across SEPARATE
   workload VMs (`02-environments.md` §Workload VMs).
4. **Encrypted overlays** for `trading-net`/`execution-net`; Swarm secrets `external: true`
   (`06-swarm-secrets.md`); per-node durable volumes declared, never hostname-bound.
5. **No stray host ports** for the execution/gateway/bridge trio (private topology, T8).

## 3. Verification gate (D1.2 — `prod_node_check.py`)

`code/01_platform/04_scripts/prod_node_check.py` verifies per-VM **disk / label / role**
from an inventory file (SSH or a cloud-API access profile), and **exits non-zero on drift**.

- Inventory: `--inventory <JSON>` (schema documented in the script header and in a bundled
  example `prod_vms.example.json`).
- Run now offline: `--self-check` proves the checker logic without VMs (no provisioning
  needed — the only runnable mode until D1.3).
- GA preflight (after D1.3): `python3 prod_node_check.py --inventory prod_vms.json --out
  logs/nautilus-execution/` — **must exit 0 before D2** (swarm bootstrap).

## 4. Honest sizing note

The final service-to-node placement, CPU/RAM, SSD IOPS/throughput and network bandwidth
are **`EVIDENCE-BLOCKED`** until `PERF-PROD-60000-001` and `FAIL-VM-LOSS-60000-001` pass
on the real stack (D5/D4). 500 GB SSD per VM is a starting allocation, not a proven sizing
result (`09-production-swarm.md`).

## 5. Prerequisites

### 5.1 Decisions to make before touching a VM

| Decision | Notes | Default |
| --- | --- | --- |
| Container registry | All nodes must pull the same digests | one private registry; record its hostname in the image lock |
| VM specs | Workload/observability floor is **500 GB disk**; a *learning* rig may be smaller, but perf evidence then remains unprovable | see §6.1 sizing caveat |
| Checkpoint/savepoint target | Production **requires** `s3://` + encryption; `CHECKPOINT_DIR` must not be `file://` in prod | encrypted S3 bucket + prefix |
| Swarm ports | 2377/tcp, 7946/tcp+udp, 4789/udp open between VM1–VM3 | required |
| Time sync | Platform halts itself beyond the configured clock-offset limit | NTP enabled on every VM |
| Observability retention | Logs/metrics/traces retention drives the O2 VM's RAM | per `../08_implementation/10-observability.md` |

### 5.2 Artifacts that must exist

- Secret values for the 9 names the stack demands (S6 in §9).
- The node inventory JSON (`code/01_platform/04_scripts/prod_vms.example.json` is the template).
- The deploy values listed in §6.2 — five are still missing.
- SSH access to all four VMs.

## 6. What does not exist yet

### 6.1 `[NOT BUILT]` mechanics this guide depends on

| Gap | What is missing | Blocks |
| --- | --- | --- |
| Image publication | `runtime.lock` **exists** and is validated (`pin-check.sh` step 5/6 rejects bare tags; `digest-pin.sh` resolves a tag to its manifest digest) — but nothing wires the lock into the deploy environment (`.env` carries bare tags), and the four locally-built images were never pushed, so no registry digest exists to pin (CHG-218) | S1, and therefore S7 |
| Secret bootstrap from one file | The stack requires 9 `external: true` secrets; the creation doc lists 5 differently-named examples. The stack file is authoritative for **names**; there is no single values file → creation step | S6 |
| Host bootstrap + clock check | No script installs Docker/enables NTP; `prod_node_check.py` verifies reachability, disk and role/labels — **not** clock offset | S4 |
| EOD trigger | `eod_controller.py` is a one-shot; nothing schedules it. The requirement names a scheduled owner (`../02_requirements/06-operational.md` §6.8) | S11 |
| Multi-node pre-deploy validation | `stack_selfcheck.sh` refuses to run when the swarm has more than one node ("refusing to label — single-host mimic, not a cluster"), so **nothing validates a real cluster's stack before `docker stack deploy`** | S7 |

**Sizing caveat:** the final service-to-node CPU/RAM/IOPS/bandwidth allocation is `EVIDENCE-BLOCKED` until the production performance and one-VM-loss scenarios pass (§4 above). The 500 GB per-node disk figure is a starting allocation, not a proven sizing result.

### 6.2 The values a deploy must have — and what is missing today

`stack_selfcheck.sh` refuses a deploy that carries placeholders: it demands 13 non-empty values
(`required_vars`). Reproduced read-only on this repository, **5 are missing**:

| Missing value | Why it is missing |
| --- | --- |
| `INGESTION_IMAGE`, `EXECUTION_BRIDGE_IMAGE`, `EXECUTION_GATEWAY_IMAGE`, `NAUTILUS_IMAGE` | built locally and never pushed, so no registry manifest digest exists to pin (`runtime.lock` records them as retired pins, CHG-218) |
| `CHECKPOINT_DIR` | development runs `file:///checkpoints`; production needs an encrypted `s3://` prefix |

Present but **not yet immutable**: `FLUSS_IMAGE`, `FLINK_IMAGE` and `OPENOBSERVE_IMAGE` are bare tags
in `.env` while `runtime.lock` holds their digests. The lock is the source of the digest; the deploy
environment is what the nodes actually pull. Until those two agree, a deploy is tag-based, not
digest-based.

So S1 is not "build the images". It is **publish the four local images and make every reference the
deploy environment carries an immutable digest.**

## 7. The workflow at a glance

| Stage | Env | Who | Entry | Exit |
| --- | --- | --- | --- | --- |
| S0 Pre-flight | `[WORKSTATION]` | operator | clean tree, secrets file present | version pin, docs audit, change control, tests, images build all green |
| S1 Build, publish, pin images | `[WORKSTATION]` + registry | operator / CI | S0 green | every image reference is registry-qualified and digest-pinned **in the deploy environment** |
| S2 Create the VMs | `[PRODUCTION]` | **human only** | provider account, specs decided | 4 reachable VMs, SSH keys held, inventory JSON filled |
| S3 Verify nodes | `[ACCEPTANCE]` | operator | S2 done | `prod_node_check.py --inventory …` exits 0 for every node |
| S4 Bootstrap hosts | `[PRODUCTION]` | operator | S3 done | Docker + NTP + sysctls + ports ready on all 4 |
| S5 Cluster init + labels | `[PRODUCTION]` | operator | S4 done | `docker node ls` = 3 managers, quorum 2/3, labels applied |
| S6 Create the 9 secrets | `[PRODUCTION]` | operator | S5 done | `docker secret ls` shows 9/9, names identical to the stack file |
| S7 Deploy the stack | `[PRODUCTION]` | operator | S6 done | every service converges; placement matches labels; 2 Flink jobs running |
| S8 Readiness verification | `[PRODUCTION]` | operator | S7 done | five readiness dimensions assessed **separately** and recorded |
| S9 Data-loop smoke | `[PRODUCTION]` | operator | S8 done | ticks → raw table → candles → candidates proven on this deployment |
| S10 Failure drills | `[PRODUCTION]` | operator | S9 done | one-VM loss + quorum + placement drills pass, RPO/RTO recorded |
| S11 Operate | `[PRODUCTION]` | operator | S10 done | EOD verified, backups verified, maintenance/rollback/upgrade procedures usable |

**Money path rule for every stage:** the executor gate stays `HALTED` until an authenticated single-operator approval (DEC-044) covers the *same* gate epoch and evidence hash. No stage here may enable order placement; automatic enablement and automatic resume are prohibited (`../02_requirements/06-operational.md` §6.1–6.2).

## 8. Mental model (five lines)

1. **One artifact set, three environments.** Images are built once, pushed by immutable digest, and pulled — never rebuilt per environment (`01-ci-cd.md` §Promotion model).
2. **Environment differences live in configuration, not in code.** Dev uses Compose + local volumes + `file:///checkpoints`; production uses Swarm + encrypted S3 (`../04_contracts/09-platform-runtime.md`).
3. **Readiness is dependency-driven, never port-driven.** Liveness, readiness, job health, trading readiness and durability readiness are five separate questions (`02-environments.md` §Readiness dimensions).
4. **Startup is not enablement.** Dependencies and health checks never authorize orders; a process can be live and not ready.
5. **Uncertainty halts, it never retries blindly.** Unknown execution outcome → halt, reconcile, human approval.

**What runs where** (`02-environments.md` §Production placement): VM1–VM3 are Swarm `Manager+Worker` and each hosts one ZooKeeper ensemble member (3-node, quorum 2-of-3), Fluss capacity with three-node LOG replication, Flink JobManager/TaskManager, and the assigned services. VM4 hosts OpenObserve and telemetry **outside the Swarm** — its loss must never authorize orders or erase durable audit.

## 9. Stage runbooks

Stages S2–S7 are the operator steps of `D1.3` (create the machines, verify them, install
Docker, form the cluster, create the secrets, deploy).

### S0 — Pre-flight `[WORKSTATION]`
**Do** (from the repository root, tree clean, `secrets.env` present for local work):
```bash
make pin-check                     # exact-version discipline
python3 code/01_platform/04_scripts/docs_audit.py
python3 code/01_platform/04_scripts/change_control_check.py --dir docs/05_deployment/change-records
python3 code/01_platform/04_scripts/t8_sandbox_contract_check.py          # executor cannot reach the live path
python3 code/01_platform/04_scripts/execution_network_check.py --compose code/01_platform/01_docker/docker-compose.yml
make stack-config                  # compose/stack definitions parse
make test-all                      # component tests
```
**Expect:** every command exits 0; `stack-config` prints its own compile-only caveat (a green stack config is not deploy evidence).
**Exit:** all green, and the change you are deploying has a change record.
**Stop if:** the auditor, the contract checker or the tests fail — a failed mandatory stage blocks promotion.

### S1 — Build, publish, pin `[WORKSTATION]` + registry `[NOT BUILT]`
**Entry:** S0 green.
**Do:** build with the existing stamped builder, publish each app image, resolve its manifest digest, and write that digest into the deploy environment (§6.2).
```bash
make images                                                    # stamped build
bash code/01_platform/04_scripts/digest-pin.sh <image:tag>      # tag -> tag@sha256:<manifest digest>
make pin-check                                                  # step 5/6 rejects bare tags in runtime.lock
# [NOT BUILT] publish step: push the four locally-built images, then write the digests into the deploy env
```
**Expect:** the four registry images keep the digests `runtime.lock` already records; the four locally-built images gain a registry-qualified digest that actually resolves.
**Exit:** every reference the stack pulls is `registry/name@sha256:…` in the **deploy environment**, not only in the lock.
**Stop if:** any image is referenced by a mutable tag — production prohibits `latest`, floating tags and version ranges — or the lock carries a digest while `.env` still carries the bare tag.

### S2 — Create the VMs `[PRODUCTION]` — human
**Do:** create four VMs (cloud provider of choice) matching §5.1; record IPs, SSH user and key. Fill the inventory JSON.
**Exit:** four reachable machines and a complete inventory file.
**Note:** this step is human-only; it is `D1.3` in the provisioning doc, and it is the last thing that must happen before any cluster work.

### S3 — Verify nodes `[ACCEPTANCE]`
```bash
python3 code/01_platform/04_scripts/prod_node_check.py --self-check                  # offline proof of the checker (no VMs)
python3 code/01_platform/04_scripts/prod_node_check.py --inventory prod_vms.json --out logs/nautilus-execution/
```
**Expect:** per node — `reachability`, `disk`, `swarm` (role/availability/labels) all PASS.
**Exit:** exit code 0. Drift is a provisioning defect: fix the node, not the checker.
**Note:** role/labels are read by the node's own Swarm NodeID, so this check must be re-run after S5.

### S4 — Bootstrap hosts `[PRODUCTION]` `[NOT BUILT]`
**Do (target shape):** on all four VMs — install Docker Engine, enable NTP, apply the required sysctls, open the Swarm ports, ensure log output lands in `/var/log/` (the collector reads `/var/log/*.log`; there is **no** journald receiver configured).
**Exit:** `docker version` works on each node without `sudo`; clock offset within limit; ports reachable.
**Stop if:** clocks drift — the platform halts itself on offset violations, and today's node checker will not catch it.

### S5 — Cluster init, labels, quorum `[PRODUCTION]`
```bash
# on VM1
docker swarm init --advertise-addr <vm1-ip>          # capture the join commands it prints
# on VM2 and VM3 (manager token)
docker swarm join --token <manager-token> <vm1-ip>:2377
# VM4: do NOT join — observability stays outside the quorum
# on VM1: labels (never hostnames)
docker node update --label-add role=manager <node>   # x3
docker node update --label-add role=worker  <node>   # x3   (v1 baseline: managers also run workloads)
docker node ls                                        # expect 3 managers, quorum 2/3
# v2 only (dedicated managers): docker node update --availability drain m1 m2 m3
```
**Exit:** three managers `Reachable`, one Leader, and `prod_node_check.py` re-run passes the label checks.
**Stop if:** fewer than three managers are visible — quorum is the whole point of the third VM.

### S6 — Create the nine secrets `[PRODUCTION]` `[NOT BUILT]` (bootstrap step)
The stack declares exactly these `external: true` secrets — names are authoritative in `code/01_platform/01_docker/docker-stack.yml`:
```text
aws_access_key_id      aws_secret_access_key   o2_password        o2_auth_basic
arrow_app_secret       arrow_password          arrow_totp_key
execution_bridge_auth_token                   gateway_shared_secret
```
**Do:** create all nine from a single values file, never from a command line visible to `ps` (`06-swarm-secrets.md` §1 shows the `< (printf …)` idiom for the ingestion-era names; the current stack needs the nine above).
**Exit:** `docker secret ls` shows 9/9 with exact name matches.
**Stop if:** any name differs — `docker stack deploy` fails with "secret not found", and a partial create leaves the stack half-deployable.

### S7 — Deploy the stack `[PRODUCTION]`
On a real cluster this is manual: `stack_selfcheck.sh` refuses to run with more than one node (§6.1), and
`docker stack deploy` has **no** `--env-file` — values come from the shell environment.
```bash
# from VM1 only, in a shell that carries the real values
docker stack config -c code/01_platform/01_docker/docker-stack.yml >/dev/null   # compiles the manifest
docker stack deploy -c code/01_platform/01_docker/docker-stack.yml --with-registry-auth "$STACK_NAME"
docker stack services "$STACK_NAME"
docker service ps <service> --no-trunc        # placement must match labels, not hostnames
```
**Notes:** `--with-registry-auth` is required when the registry is private (GHCR packages are) or the
nodes cannot pull. Use **one** stack name everywhere — the checked script defaults to `prod`, the
deployment docs show `trading`; pick one and never mix.
**Expect:** replicas converge; nothing stuck at `0/N`; encrypted overlays and internal-only execution networks are the ones declared by the stack.
**Exit:** all services converge, and Flink shows exactly the expected jobs running.
**Stop if:** a service reports "no suitable node" — that is a labelling problem (S5), not a scheduling problem.

### S8 — Readiness verification `[PRODUCTION]`
Assess the five dimensions **separately** and record each (`02-environments.md` §Readiness dimensions):
| Dimension | Question | Where it is answered |
| --- | --- | --- |
| Liveness | does the process/loop respond? | service health |
| Readiness | are mandatory dependencies and the data flow available? | ZK quorum, Fluss quorum/schema, S3 access |
| Job health | are the required Flink jobs running and checkpointing? | Flink REST/UI |
| Trading readiness | is the executor gate state known and reconciliation clean? | gate state — expected `HALTED` |
| Durability readiness | replication, checkpoints, offload, audit retention, recovery posture | checkpoints in encrypted S3, EOD manifest |
**Exit:** each dimension recorded as its own line of evidence. A healthy container is not sufficient for any higher dimension.
**Stop if:** `CHECKPOINT_DIR` is not `s3://` in production — the job is designed to fail fast rather than run without durable checkpoints.

### S9 — Data-loop smoke `[PRODUCTION]`
```bash
make test-25-smoke                     # existing smoke target
```
**Expect:** ticks land in the raw table, candles form, signal candidates appear; ingestion telemetry visible in OpenObserve.
**Exit:** the loop is proven on *this* deployment — not on the laptop's.
**Reference:** the local equivalent and its L0–L11 ladder is `../08_implementation/08-local-compose.md`; commands there are `[LOCAL]` and are not production procedures.

### S10 — Failure drills `[PRODUCTION]`
```bash
make chaos-suite                       # includes chaos-04 VM-loss (self-skips when nodes are insufficient)
make disaster-drills                   # DR-001..006; --dry-run first, --approve to inject
```
**Expect:** one workload VM loss tolerated by ZooKeeper quorum (2/3); Flink restores from encrypted S3; backlog and recovery duration measured; no duplicate broker orders.
**Record:** RPO/RTO **per scenario** — broader claims than the tested scenario are prohibited (`../02_requirements/06-operational.md` §6.5).
**Stop if:** recovery is unmeasurable — then the scenario is not evidence and stays open.

### S11 — Operate `[PRODUCTION]`
| Activity | Procedure |
| --- | --- |
| EOD | `eod_controller.py` must be run by a scheduled owner `[NOT BUILT]`; verify manifest counts/ranges/hashes then retention (`../06_operations/07-lake-archive-ops.md`) |
| Routine checks | dashboard/alert review per `../06_operations/05-maintenance.md` §Routine checks |
| Backup/restore | lake day restore via `r2-restore.sh`; platform-state backup/restore is **not yet evidenced** |
| Maintenance window | `05-maintenance.md` §Planned maintenance (11 steps, gate halted first) |
| Upgrade | pin bump → images → rolling Swarm update → verify → keep gate halted until reconciliation |
| Rollback | `03-rollback.md`: application-only / schema-or-state / infrastructure, each with its own preconditions |
| Secret rotation | `04-secrets-rotation.md` §Rotation procedure (9 steps, gate halted for order-path credentials) |

## 10. Evidence per stage

Nothing in this guide is evidence by itself. Capture per stage so gate rows can be flipped honestly:

| Stage | Capture | Feeds |
| --- | --- | --- |
| S0 | auditor + test output, change record id | promotion preconditions |
| S1 | registry digests, `pin-check` output, deploy-env values | artifact provenance |
| S3 | `prod_node_check.py` output per node | provisioning evidence (`D1.2`) |
| S5–S7 | `docker node ls`, `docker service ls/ps`, deploy log | deployment evidence (`SWARM-DEPLOY-*`) |
| S8 | five readiness dimensions, checkpoint location evidence | readiness evidence |
| S9 | smoke output, tick/candle/candidate counts | loop proof on this deployment |
| S10 | drill logs, measured RPO/RTO per scenario | `FAIL-VM-LOSS-60000`, `DR-001..006`, `SWARM-MGR-*` |
| S11 | EOD manifest verification, restore drill output | retention/DR gates |

## 11. Failure playbook (first move, not a full fix)

| Symptom | Most likely cause | First move |
| --- | --- | --- |
| `docker stack deploy` → "secret not found" | secret names drifted from the stack file | compare `docker secret ls` against the 9 names in S6 (§9) |
| Service stuck `0/N`, "no suitable node" | missing/typo'd node labels | re-label (`S5`), confirm with `prod_node_check.py` |
| Image pull fails on one node | image not published / registry not reachable from that node | publish in S1, verify pull on the failing node |
| Flink job restarts, checkpoints failing | `CHECKPOINT_DIR` not `s3://`, or S3 credentials/prefix wrong | fix checkpoint config — never "fix" it by going back to `file://` |
| Gate halts itself, service logs clock offset | VM clock drift beyond the configured limit | enable NTP, re-sync, then reconcile before resuming |
| ZooKeeper/Fluss quorum degraded | one node down or partitioned | verify quorum state and anti-co-location, preserve backlog accounting, restore from S3 |
| O2 unreachable | observability VM down | restore telemetry; trading readiness fails if required safety evidence is unavailable |
| EOD did not run | no scheduled owner exists yet | run the controller, verify manifest, extend retention |
| Container killed (exit 137) | memory limit vs actual need | read limits against the documented budget; do not widen limits without recording the change |

**Firefighting avoidance is structural, not personal:** every stage above has an exit criterion you can observe, and every failure above maps to a documented runbook in `../06_operations/01-runbooks.md` (gate halt, reconciliation and resume, executor restart, execution-gate recreate, unknown outcome, broker disconnect, Flink/checkpoint failure, Fluss/quorum/VM loss, host power cut, postback projection failure, EOD offload failure, observability outage, security incident).

## 12. Change control

Every deployment-affecting change records all seven items (`00-release-strategy.md` §Change-control rule): requirements/contracts/DDL/interfaces affected · version and schema compatibility classification · state and savepoint impact · gate/halt requirement · prechecks and acceptance tests · rollback criteria and recovery path · post-deployment verification. Records are filed as `change-records/CHG-<N>.md` with the six fields validated by `docs_audit` C14 and checked by:
```bash
python3 code/01_platform/04_scripts/change_control_check.py --dir docs/05_deployment/change-records
```
Prohibited: automatic gate enablement, automatic gate resume, deleting volumes/checkpoints/audit during emergency rollback, presenting local Compose results as production evidence, and any broader RPO/RTO or exactly-once claim than the tested scenario.

## 13. What this document does not cover

| Topic | Where it belongs |
| --- | --- |
| Perf/capacity numbers and sizing proof | §4 above, `09-production-swarm.md` (all sized rows are `EVIDENCE-BLOCKED`) |
| Live-money authorization | `04-decisions.md` DEC-044, `RELEASE_EVIDENCE_2026-08-21.md` |
| Topology rationale (v1 → v2) | `09-production-swarm.md` §Swarm control-plane architecture |
| Data-path and contract detail | `../04_contracts/*`, `../03_architecture/02-data-pipeline.md` |
| Alert thresholds and dashboards | `../08_implementation/10-observability.md` |
| Local development ladder (L0–L11) | `../08_implementation/08-local-compose.md` |

## 14. References

- `00-release-strategy.md` — stages, release gates, change-control rule
- `01-ci-cd.md` — artifact identity, promotion model, deployment procedure, failure policy
- `02-environments.md` — environment matrix, placement, startup order, acceptance
- `03-rollback.md` — rollback classes and recovery procedures
- `04-secrets-rotation.md` · `06-swarm-secrets.md` — secret storage, rotation, Swarm specifics
- §1–§4 of this document — target topology, hard rules, the `prod_node_check.py` gate, the sizing note
- `../08_implementation/09-production-swarm.md` — v1/v2 topology, milestones, current status
- `../06_operations/00-operational-strategy.md` · `01-runbooks.md` · `04-dr-plan.md` · `05-maintenance.md`
- `../04_contracts/09-platform-runtime.md` — runtime contract, checkpoint/encryption rules
