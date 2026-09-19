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
| `O1` | Observability | `worker` (joined, no vote) | `observability=true` | OpenObserve + telemetry; joined as a **worker**, so it never joins the manager quorum |

**Disk:** 500 GB SSD per VM (workload VMs and the observability VM). Managers in v2 are
small-footprint (≈10 GB disk, 2 CPU / 2 GB RAM per `09-production-swarm.md` §v2) — treat
500 GB as the workload/observability floor, not a manager requirement.
**VM1 also hosts the image registry** (≈3–4 GB of layers on top of its own images): in v2 that
does not fit a ≈10 GB manager disk, so either give the registry node more disk or move the
registry to a worker.

**v1 baseline (ship-now, 4 VMs)** — same stack, different labels: `M1 M2 M3` are
Manager+Worker (label `role=worker`; manager is a Swarm *role*, not a label, and a node holds
exactly one value per label key) and `O1` joins as a **worker** carrying `observability=true`
— joined so the observability services can be scheduled at all, but never a manager, so the
quorum stays 3. Per the DECISION 2026-08-20 in `docker-stack.yml`, adopt v2 only on a
trigger: `N>6` workers, sustained CPU >80%, or Raft election flaps.

## 2. Hard rules (fail = provisioning defect)

1. **No hostname pinning anywhere.** The stack places by `node.labels.role == worker` and
   `node.labels.observability == true` only. Never edit `docker-stack.yml` to name a host;
   W4+ joins by labeling, not by stack rewrite (`test_09_stack.py` enforces this).
2. **Manager quorum is 3** (tolerant of 1 loss). O1 (observability) is a worker, outside the
   manager quorum; its loss must never authorize orders or erase the durable audit.
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
| Container registry | All nodes must pull the same digests | **`registry:2` on VM1** (`<vm1-ip>:5000`), brought up in S4: the other three nodes pull over the private network, the workstation pushes only through an SSH tunnel, and no registry credential exists on any VM. Record the resolved digests in the image lock |
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
| Image publication | `runtime.lock` **exists** and is validated (`pin-check.sh` step 5/6 rejects bare tags; `digest-pin.sh` resolves a tag to its manifest digest) — but nothing wires the lock into the deploy environment (`.env` carries bare tags), and the four locally-built images were never pushed, so no registry digest exists to pin (CHG-218). The build plan for this item is owned by `../08_implementation/09-production-swarm.md` §Pre-deploy implementation items | S1 (build) and S4 (publish), and therefore S7 |
| Secret bootstrap from one file | The stack requires 9 `external: true` secrets; the creation doc lists 5 differently-named examples. The stack file is authoritative for **names**; there is no single values file → creation step | S6 |
| Host bootstrap + clock check | No script installs Docker/enables NTP; `prod_node_check.py` verifies reachability, disk and role/labels — **not** clock offset. The sysctl list S4 needs is itself undefined in this repository — decide and record it before the first boot | S4 |
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

So S1 is not "build the images". It is **build them, publish them to the registry on VM1 (S4), and
make every reference the deploy environment carries an immutable digest.**

## 7. The workflow at a glance

| Stage | Env | Who | Entry | Exit |
| --- | --- | --- | --- | --- |
| S0 Pre-flight | `[WORKSTATION]` | operator | clean tree, secrets file present | version pin, docs audit, change control, tests, images build all green |
| S1 Build images | `[WORKSTATION]` | operator / CI | S0 green | eight images present locally; build green |
| S2 Create the VMs | `[PRODUCTION]` | **human only** | provider account, specs decided | 4 reachable VMs, SSH keys held, inventory JSON filled |
| S3 Verify nodes | `[ACCEPTANCE]` | operator | S2 done | `prod_node_check.py --inventory …` exits 0 for every node |
| S4 Bootstrap hosts + registry + publish | `[PRODUCTION]` + `[WORKSTATION]` | operator | S3 done | Docker + NTP + sysctls + ports on all 4; registry serving on VM1; every image reference digest-pinned **in the deploy environment** |
| S5 Cluster init + labels | `[PRODUCTION]` | operator | S4 done | `docker node ls` = 3 managers + 1 worker, quorum 2/3, labels applied, swarm locked |
| S6 Create the 9 secrets | `[PRODUCTION]` | operator | S5 done | `docker secret ls` shows 9/9, names identical to the stack file |
| S7 Deploy the stack | `[PRODUCTION]` | operator | S6 done | every service converges; placement matches labels; 2 Flink jobs running |
| S8 Readiness verification | `[PRODUCTION]` | operator | S7 done | five readiness dimensions assessed **separately** and recorded |
| S9 Data-loop smoke | `[PRODUCTION]` | operator | S8 done | ticks → raw table → candles → candidates proven on this deployment |
| S10 Failure drills | `[PRODUCTION]` | operator | S9 done | one-VM loss + quorum + placement drills pass, RPO/RTO recorded |
| S11 Operate | `[PRODUCTION]` | operator | S10 done | EOD verified, backups verified, maintenance/rollback/upgrade procedures usable |

**Read it as a sequence:** the table order *is* the run order — `S0 → S11`, nothing started before
its Entry is green. Two stages invert intuition: **S1 only builds** (publishing waits for S4, because
the registry lives on VM1 and VM1 does not exist until S2), and **S5 sets the swarm lock**, because
`--autolock` is a `swarm init` flag and cannot be added afterwards without re-initialising.

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

### S1 — Build the images `[WORKSTATION]`
**Entry:** S0 green.
**Do:** build with the existing stamped builder. **Publishing deliberately waits for S4** — the
registry lives on VM1, which does not exist until S2/S4, so there is nothing to push to yet.
```bash
make images                                                    # stamped build
make pin-check                                                  # step 5/6 rejects bare tags in runtime.lock
docker images | grep -E "trading-|01_docker-"                   # the images the stack pulls
```
**Expect:** the six project-built images (Flink runtime, Fluss runtime, and the four app images)
plus the two third-party images (OpenObserve, ZooKeeper) are present locally.
**Exit:** the build is green and every image the stack names exists locally.
**Stop if:** any image is referenced by a mutable tag — production prohibits `latest`, floating tags and version ranges.

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

### S4 — Bootstrap hosts, registry, publish `[PRODUCTION]` + `[WORKSTATION]` `[NOT BUILT]`
**Entry:** S3 green. Run 1–4 **on all four VMs**; 5 on VM1 only; 6 from the workstation.

**1. Docker Engine** (Docker's official Ubuntu repository steps — follow the upstream page if a
line has changed):
```bash
sudo apt-get update && sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update && sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin
sudo usermod -aG docker "$USER"        # then re-login (or `newgrp docker`) for it to take effect
```

**2. Clock.** TOTP is clock-based and the platform halts itself beyond the offset limit:
```bash
sudo apt-get install -y chrony && sudo systemctl enable --now chrony
timedatectl                            # expect "System clock synchronized: yes"
chronyc tracking                       # expect a small System time offset
```

**3. Sysctls — not yet defined.** This repository does not record which sysctls production needs
(§6.1). Decide the list, write it down, then apply it; Flink's own host requirements
(swappiness, socket backlog, overcommit, file descriptors) are the starting point. Do not paste a
list nobody recorded.

**4. Logs and ports.** Application output must land in `/var/log/*.log` (the collector reads that
path; there is **no** journald receiver configured). Open the Swarm ports between the four VM IPs
only: `2377/tcp`, `7946/tcp+udp`, `4789/udp` — plus `22/tcp` from the workstation, and nothing else
(§9 Security baseline).

**5. Registry on VM1** — a plain container, **not** a Swarm service: a rolling service update must
never take the registry down while the other nodes are pulling from it.
```bash
# on VM1
docker run -d --name registry --restart=always -p 5000:5000 \
  -v /var/lib/registry:/var/lib/registry registry:2

# on ALL FOUR VMs — Docker refuses to pull from a plain-HTTP registry unless it is declared.
# MERGE into /etc/docker/daemon.json if it already exists; do not overwrite it.
#   { "insecure-registries": ["<vm1-ip>:5000"] }
sudo systemctl restart docker
```

**6. Publish and pin** — from the workstation, through a tunnel, so port 5000 never opens to the
internet:
```bash
ssh -N -L 5000:localhost:5000 <ssh-user>@<vm1-ip> &      # keep running for the pushes
docker tag  <project-built-image> localhost:5000/<project-built-image>
docker push localhost:5000/<project-built-image>          # prints "digest: sha256:…" = the manifest digest
bash code/01_platform/04_scripts/digest-pin.sh localhost:5000/<project-built-image>
```
Push the six project-built images (Flink runtime, Fluss runtime, and the four app images). The two
third-party images (OpenObserve, ZooKeeper) come from their public registries at the digests
`runtime.lock` already records — if either digest no longer resolves, mirror that image here too.
Then write the digests into the deploy environment (§6.2) using **`<vm1-ip>:5000/…@sha256:…`**, not
`localhost` — every node resolves `localhost` as itself, and only VM1 hosts the registry. The digest
itself is identical whichever hostname the push went through.

**Note:** if `digest-pin.sh` refuses a plain-HTTP registry (`buildx imagetools` may), use the digest
`docker push` printed, or `docker image inspect --format '{{index .RepoDigests 0}}'` on the machine
that pushed — both are the same manifest digest and need no registry query.

**Exit:** `docker version` works on each node without `sudo`; `timedatectl` shows a synchronized
clock; `curl -s http://<vm1-ip>:5000/v2/_catalog` lists the pushed repositories; every image
reference in the deploy environment is `<vm1-ip>:5000/name@sha256:…`.
**Stop if:** clocks drift (the platform halts itself on offset violations and today's node checker
will not catch it), or an image reference is still a bare tag.

### S5 — Cluster init, labels, quorum `[PRODUCTION]`
```bash
# on VM1 — --autolock is set HERE and only here: it is a swarm-init flag
docker swarm init --autolock --advertise-addr <vm1-ip>   # capture the join commands it prints
# on VM2 and VM3 (MANAGER token)
docker swarm join --token <manager-token> <vm1-ip>:2377
# on VM4 (WORKER token) — it must join, or the three observability services have no node to land
# on and sit at 0/1 forever. A worker does not vote: the quorum stays 3 and VM4's loss cannot
# elect a leader.
docker swarm join --token <worker-token> <vm1-ip>:2377
# on VM1: labels (never hostnames). A node holds ONE value per label key — a second --label-add
# on the same key overwrites the first, so role=manager and role=worker cannot coexist on one node.
docker node update --label-add role=worker <node>         # x3: VM1, VM2, VM3 (v1: managers also run workloads)
docker node update --label-add observability=true <vm4>   # x1: VM4 ONLY
docker node ls                                            # expect 3 managers + 1 worker
# v2 only (dedicated managers): docker node update --availability drain m1 m2 m3
```
**Why VM4 carries `observability` and not `role`:** the 13 workload services require
`node.labels.role == worker` and the 3 observability services require
`node.labels.observability == true`. Leaving `role` off VM4 keeps the trading stack from being
scheduled onto the observability VM; leaving `observability` off VM1–VM3 keeps OpenObserve off the
workload nodes. Both directions matter.

**Exit:** three managers `Reachable`, one Leader, one worker carrying the `observability` label, and
`prod_node_check.py` re-run passes the label checks.
**Stop if:** fewer than three managers are visible — quorum is the whole point of the third VM.
**Note:** `--autolock` means a manager that reboots stays locked until you run `docker swarm unlock`.
That is the point — without it the Raft log and mTLS keys sit unencrypted on disk. Keep the unlock
key where you keep the other secrets.

### S6 — Create the nine secrets `[PRODUCTION]` `[NOT BUILT]` (bootstrap step)
The stack declares exactly these `external: true` secrets — names are authoritative in `code/01_platform/01_docker/docker-stack.yml`:
```text
aws_access_key_id      aws_secret_access_key   o2_password        o2_auth_basic
arrow_app_secret       arrow_password          arrow_totp_key
execution_bridge_auth_token                   gateway_shared_secret
```
**Do — five you hold, piped over SSH.** The value goes in on stdin, so it never reaches the VM's
disk, either shell's history, or a process list on either machine. Never copy `.env` or
`secrets.env` to a VM.
```bash
printf '%s' "$VALUE" | ssh <ssh-user>@<vm1-ip> 'docker secret create arrow_password -'
# the same for the other four: arrow_app_secret, arrow_totp_key,
#                              aws_access_key_id, aws_secret_access_key
```
**Do — four generated on VM1 and never transported:**
```bash
O2PW="$(openssl rand -base64 24)"
printf '%s' "$O2PW" | docker secret create o2_password -
printf 'Basic %s' "$(printf 'admin:%s' "$O2PW" | base64 -w0)" | docker secret create o2_auth_basic -
openssl rand -hex 32 | docker secret create execution_bridge_auth_token -
openssl rand -hex 32 | docker secret create gateway_shared_secret -
```
`o2_auth_basic` is the **complete header value** `Basic <base64(user:password)>`, not bare base64 —
that is the format the readers in `code/01_platform/04_scripts/` expect. `06-swarm-secrets.md` §1
shows the older `< (printf …)` idiom and still lists ingestion-era names; the list above is the
authoritative one.

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
**Notes:** with the VM1 registry (S4) every node pulls over the private network and **no registry
credential exists on any VM**; `--with-registry-auth` stays harmless. Use **one** stack name
everywhere — the checked script defaults to `prod`, the deployment docs show `trading`; pick one and
never mix.
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

### Security baseline — three set-once items

A rented VM's hypervisor can read a guest's memory, so the goal is **not** to prevent that — it is to
shrink what is exposed, cap what a stolen credential can do, and leave a tripwire. These three are
the whole hardening story, and all three are platform features: no extra software, no daemons,
nothing to update. Do them once.

| # | Item | Where | Done when |
| --- | --- | --- | --- |
| 1 | **Broker-side limits**: no fund withdrawal on the trading login, per-day order cap, maximum order size, login IP whitelist, and a separate market-data `app_id` for ingestion with no order rights | the broker's own settings | the trading login cannot withdraw and cannot place an oversized order; ingestion's `app_id` is rejected for order entry |
| 2 | **CloudPe security group**: inbound `22/tcp` from the workstation IP only, the Swarm ports (`2377/tcp`, `7946/tcp+udp`, `4789/udp`) between the four VM IPs only, everything else denied; SSH keys only with password authentication off; 2FA on the CloudPe panel | CloudPe panel | a scan from outside shows nothing but SSH; `docker node ls` still shows all four nodes |
| 3 | **`--autolock` at `swarm init`** (S5) + Swarm secrets only + R2 temporary credentials | Docker / Cloudflare | the manager's Raft log and mTLS keys are unusable without `docker swarm unlock`; no static S3 key exists on any VM |

**Notes that save a bad day:**

- CloudPe's **default security group allows all traffic on all ports and cannot be deleted** — create
  a custom group and attach it to all four VMs. A newly created group denies all inbound by default.
- **Security-group rules cover IPv4 only** — keep IPv6 off the VMs, or that traffic is unfiltered.
- **Locking yourself out is recoverable**: the CloudPe panel console is the break-glass path.
- **`--autolock` costs one command after a manager reboot** (`docker swarm unlock`). Managers reboot
  rarely; without the lock, the Raft log and mTLS keys are readable at rest.
- **Deliberately not done, with reasons:** LUKS full-disk encryption (needs a human at every boot —
  an availability dependency bought for disk-only protection, while the design already keeps
  passwords out of files), `fail2ban` (pointless once password authentication is off), authenticated
  NTP (the platform already halts on clock offset). None of them stops a hypervisor memory read.

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
