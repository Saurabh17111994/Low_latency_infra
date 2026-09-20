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
   `node.labels.observability == true` only. Never edit `docker-stack.yml` to name a host.
   **Exception (CHG-265): the per-host agents carry no constraint at all.** `node-exporter`
   and `cadvisor` are `mode: global`, so they run on every node including VM4; any label
   here would exclude the nodes whose metrics must exist. Everything else keeps label
   placement, and a hostname pin stays forbidden everywhere. They also take
   `hostname: "{{.Node.Hostname}}"`, because otherwise every node's series reports the
   container id as its own name;
   W4+ joins by labeling, not by stack rewrite (`test_09_stack.py` enforces this).
2. **Manager quorum is 3** (tolerant of 1 loss). O1 (observability) is a worker, outside the
   manager quorum; its loss must never authorize orders or erase the durable audit.
3. **Anti-co-location of critical replicas:** Fluss replicas, ZooKeeper ensemble members
   (3-node, quorum 2-of-3) and Flink HA (JobManager leader) must land across SEPARATE
   workload VMs (`02-environments.md` §Workload VMs).
4. **Encrypted overlays** for `trading-net`/`execution-net`; Swarm secrets `external: true`
   (`06-swarm-secrets.md`); per-node durable volumes declared, never hostname-bound.
5. **No stray host ports** for the execution/gateway/bridge trio (private topology, T8).
6. **No healthcheck may require another service or cluster formation.** Swarm only publishes a DNS
   record for tasks whose healthcheck has PASSED, so a probe that waits for peers can never pass:
   the peers it waits for cannot resolve it either (DEC-047). Liveness only — reach your own port,
   read your own file. Quorum and dependency state belong in metrics, logs and alerts. A
   `test_09_stack.py` guard fails the build on `zkServer.sh status`-style probes.
7. **No service binds its own name in order to start.** The same bootstrap circle as rule 6:
   Swarm publishes a DNS name only for tasks that are Running, so a listener bound to that name
   (Fluss `bind.listeners` was `...://fluss-coordinator:9123`) fails its first bind and can
   never reach Running (DEC-051). Bind a literal — `0.0.0.0` where peers/clients must reach
   the service, `127.0.0.1` where it must be loopback-only — and advertise the resolvable
   service name. A `test_09_stack.py` guard fails the build on either half drifting.

8. **No comment text inside `FLINK_PROPERTIES` / `FLUSS_PROPERTIES`.** Those are YAML block
   scalars, so the text reaches the service verbatim and YAML comment semantics never apply.
   Measured twice: a prose line was loaded as a configuration property (Flink, run8c), and a
   rehearsal note appended to a path value survived into the value —
   `high-availability.storageDir` became `file:/checkpoints/flink-ha#REHEARSAL-ONLY(...)`, a URI
   with a fragment, and the jobmanager died in `FileSystemJobResultStore` looking like a storage
   outage (run8d). Keep prose above the key; put rehearsal markers on their own comment line
   above the block. `test_09_stack.py` fails the build on any `#` inside the six blocks.

9. **A `file://` HA or checkpoint path needs its directory prepared, because Flink will not tell
   you when it cannot create it.** `FileSystemJobResultStore.createBasePathIfNeeded()` ignores
   `mkdirs`'s return value: it logs `Created highly available job result storage directory at …`
   and then fails `listStatus` with `The base directory of the JobResultStore isn't accessible`,
   exiting 239 — a message that reads like a storage outage but means "permission denied"
   (javap-verified on Flink 2.2.1; reproduced in the rehearsal, where a fresh named volume is
   `root:root 755` and the Flink image runs as uid 9999). Production is not exposed by
   construction — `high-availability.storageDir`, `state.checkpoints.dir` and Fluss
   `remote.data.dir` are all `s3://` there, where `mkdirs` is a no-op and `listStatus` returns an
   empty listing. Two consequences for the VMs: (a) if anyone ever falls back to local paths,
   create and `chown` those directories first; (b) object stores make the same silence possible
   on a **rejected** write, so after the first boot confirm the HA markers actually appear under
   the bucket path before trusting leadership handover.

10. **Flink's advertised RPC port and its bind port are two settings — pin both.** Flink 2.x
    splits them: `jobmanager.rpc.port` / `taskmanager.rpc.port` (6123, also the image default)
    is the port peers dial, while `*.rpc.bind-port` decides what is actually bound. Measured on
    the deployed image: a standalone jobmanager binds `jobmanager.rpc.port`, but **with
    `high-availability.type: zookeeper` it binds an ephemeral port** unless
    `jobmanager.rpc.bind-port` pins it (measured `…:35821` and `…:42955`), and a taskmanager
    binds ephemeral unless `taskmanager.rpc.bind-port` pins it (measured 33255/33801/46237) —
    consistent with HA meaning "the address is discovered through ZooKeeper rather than
    dialled". Production is the HA jobmanager plus a statically addressed taskmanager, i.e. the
    one combination that cannot work: in the rehearsal the jobmanager was Running and healthy
    with leadership granted while Pekko bound `…:42353`, so the taskmanager's dial to the
    advertised `flink-jobmanager:6123` was refused until it killed itself with
    `RegistrationTimeoutException ... within the specified maximum registration duration
    PT5M`. The cluster had **no** TaskManagers and no failing component: every service showed
    Running. The trap is symmetric — the ResourceManager dials a registered TaskManager's
    *advertised* port, so a TaskManager that binds an ephemeral port is equally unreachable.
    `docker-stack.yml` pins both services to 6123 (CHG-253) and `test_09_stack.py` fails the
    build when a Flink service pins one port without the other.

    **Both roles also need the same `high-availability.*` keys (CHG-255).** Pinning the ports
    made the taskmanager's registration message reach the ResourceManager, and the
    ResourceManager threw it away:
    `FencingTokenException: Fencing token mismatch: Ignoring message RemoteFencedMessage(00000000000000000000000000000000, RemoteRpcInvocation(ResourceManagerGateway.registerTaskExecutor(…))) because the fencing token 0…0 did not match the expected fencing token 9f3175f098076291f8095be45eee44b3`
    — the taskmanager registered with token `0`, because the fencing token travels with the
    leader information the HA service hands out, and a taskmanager without
    `high-availability.*` falls back to the static address instead of reading ZooKeeper. The
    cluster then reports `{"taskmanagers":[]}` while the jobmanager looks healthy. The
    taskmanager declares the same five keys as the jobmanager — `high-availability.type`,
    `.zookeeper.quorum`, `.storageDir`, `.zookeeper.path.root` and `.cluster-id` — and the
    keys must **agree**: a different ensemble, cluster id or root path is a different election
    to read. `test_09_stack.py` fails the build when the two role blocks disagree on any of
    them.

## 3. Verification gate (D1.2 — `prod_node_check.py`)

`code/01_platform/04_scripts/prod_node_check.py` verifies per-VM **disk / clock / label / role**
from an inventory file (SSH or a cloud-API access profile), and **exits non-zero on drift**. The
clock is **measured**, not labelled: `timedatectl` must report a synchronized clock and
`chronyc tracking` an offset within `CLOCK_OFFSET_LIMIT_MS` (default 200 ms — the limit the executor
itself halts at, tightened or loosened with `--max-offset-ms`).

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
| Operator-access ports | `5000/tcp` on VM1 (registry) and `5080/tcp` on the OpenObserve node, each limited to the workstation's address | required by S4 step 4 |
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
| Image publication | `image-publish.sh` (CHG-247, seven images since CHG-256) pushes the project-built images and writes digest-pinned deploy values, and `digest-pin.sh` resolves digests against a plain-HTTP registry — rehearsed end-to-end against a **local** `registry:2` only. VM1's registry does not exist yet, so `.env` still carries bare tags and no digest-pinned production environment has been produced | S4 (publish), and therefore S7, S7b |
| Executor's live clock source | `ChronycOffsetSource` (CHG-272) reads the host's `chronyc tracking` behind the existing `OffsetSource` trait and fails closed when it cannot — but the executor runs in a **container**, so production needs `chrony` in that image **and** the host's chrony socket reachable from it (and a decision about the socket's ownership, since it is root-owned). Neither can be proved without a VM, so `CLOCK_OFFSET_SOURCE` stays unset (fixed source) and the host clock is gated by `prod_node_check.py` at S3/S7 meanwhile | S7, before the first live order |
| EOD lake offload | The trigger exists now — the `eod-scheduler` stack service (CHG-269) runs `eod_controller.py` daily — but the lake path itself still needs the R2 bucket and keys, so the service ships with `EOD_OFFLOAD=none` and the manifest lifecycle is proven without offload | S11 |

**Sizing caveat:** the final service-to-node CPU/RAM/IOPS/bandwidth allocation is `EVIDENCE-BLOCKED` until the production performance and one-VM-loss scenarios pass (§4 above). The 500 GB per-node disk figure is a starting allocation, not a proven sizing result.

### 6.2 The values a deploy must have — and what is missing today

`stack_selfcheck.sh` refuses a deploy that carries placeholders: it demands 16 non-empty values
(`required_vars`). Reproduced read-only on this repository, **8 are missing**:

| Missing value | Why it is missing |
| --- | --- |
| `INGESTION_IMAGE`, `EXECUTION_BRIDGE_IMAGE`, `EXECUTION_GATEWAY_IMAGE`, `NAUTILUS_IMAGE` | built locally and never pushed, and the registry that would hold them exists only on VM1 (S4) — `image-publish.sh` produces digest-pinned values but has **not** run against a real VM1 (`runtime.lock` records the four as retired pins, CHG-218) |
| `CHECKPOINT_DIR` | development runs `file:///checkpoints`; production needs an encrypted `s3://` prefix |
| `DDL_APPLY_IMAGE` | built locally and never pushed, like the four above; CHG-269 made the EOD scheduler its first consumer, so a deploy that leaves it empty now stops at interpolation instead of starting a service |
| `EOD_TABLES` | an operator decision with no default: which tables the EOD manifest covers. The repository's own EOD test uses `candle_closed` (7-day TTL, durable) |
| `O2_PASSWORD` | OpenObserve's root password, interpolated into its environment (not a Swarm secret). `.env.example` ships it empty and `.env` carries no value either — CHG-271 added it to `required_vars`, where it was missing from every list, so the deploy stops at interpolation instead of starting an observability stack nobody can log into |

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
| S1 Build images | `[WORKSTATION]` | operator / CI | S0 green | nine images present locally (seven project-built + two third-party); build green |
| S2 Create the VMs | `[PRODUCTION]` | **human only** | provider account, specs decided | 4 reachable VMs, SSH keys held, inventory JSON filled |
| S3 Verify nodes | `[ACCEPTANCE]` | operator | S2 done | `prod_node_check.py --inventory …` exits 0 for every node |
| S4 Bootstrap hosts + registry + publish | `[PRODUCTION]` + `[WORKSTATION]` | operator | S3 done | Docker + NTP + sysctls + ports on all 4; registry serving on VM1; every image reference digest-pinned **in the deploy environment** |
| S5 Cluster init + labels | `[PRODUCTION]` | operator | S4 done | `docker node ls` = 3 managers + 1 worker, quorum 2/3, labels applied, swarm locked |
| S6 Create the 9 secrets | `[PRODUCTION]` | operator | S5 done | `secrets-bootstrap.sh --check` prints `[PASS] all 9 secrets exist` |
| S7 Deploy the stack | `[PRODUCTION]` | operator | S6 done | `stack_selfcheck.sh CLUSTER=1` green, then every service converges; placement matches labels; 2 Flink jobs running |
| S7b Apply the DDL catalog (first boot only) | `[PRODUCTION]` | operator | S7 green, Fluss catalog still EMPTY | 27 manifest tables exist and `DDL-APPLY-RESULT: PASS` is recorded |
| S8 Readiness verification | `[PRODUCTION]` | operator | S7b done | five readiness dimensions assessed **separately** and recorded |
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
make images                                                    # stamped build (six stack images)
make ddl-image                                                 # the DDL-apply tool image (not a stack service)
make pin-check                                                  # step 5/6 rejects bare tags in runtime.lock
docker images | grep -E "trading-|01_docker-"                   # the images the stack pulls
```
**Expect:** the seven project-built images (Flink runtime, Fluss runtime, the four app images, and the
DDL-apply tool image that S7b runs)
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
**Entry:** S3 green. Run 0–4 **on all four VMs**; 5 on VM1 only; 6 from the workstation.

**0. Repository — the guide runs from the clone, on every node.** Nothing in this guide copies a file
to a node by hand, and every later step executes from this clone.
```bash
sudo apt-get install -y git
git clone https://github.com/Saurabh17111994/Low_latency_infra.git ~/arrow-infra
cd ~/arrow-infra && git log -1 --format='%H %ci'   # record this commit in the change record
```
Cloned into the login user's home on purpose: no `sudo`, no ownership to repair, and the same user
that runs Docker runs the scripts. Every node runs `main` from the public repository, so **the VMs
see only what is pushed** — commit and push before bootstrapping, or a node runs older code than
the workstation. A node without the clone fails at S6 (`secrets-bootstrap.sh: not found`) rather
than silently running a stale copy.

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

**3. Sysctls — the recorded production list (CHG-272).** Four rules, each naming the failure it
prevents. They are *comparisons*, not equalities: a host already tuned beyond a floor is ready, and
writing an exact value would **lower** a host that is already better (this laptop reports
`vm.max_map_count = 1048576`).

| Knob | Rule | Why |
| --- | --- | --- |
| `vm.swappiness` | ≤ 1 | JVM heaps and RocksDB block caches are large and latency-critical; paging them out turns a p99 spike into a stall |
| `net.core.somaxconn` | ≥ 32768 | Kafka-protocol clients, task managers and health probes open many sockets; the accept queue overflows at the 4096 default |
| `net.ipv4.tcp_max_syn_backlog` | ≥ 16384 | the queue before `accept()`, paired with `somaxconn` |
| `vm.max_map_count` | ≥ 262144 | Flink's RocksDB state backend mmaps many regions per instance (`STATE_BACKEND=rocksdb`); the 65530 default fails state open |

Deliberately **not** set, with the measurement that justifies skipping each: `fs.file-max` (already
effectively unlimited on a modern kernel), the Docker daemon's `LimitNOFILE` (the package default is
already 524287, so no systemd drop-in is needed), and `vm.overcommit_memory` (it masks real memory
exhaustion, and the container limits bound memory here). Anything else is a measurement, not a guess.

`vm-bootstrap.sh --apply` writes these to `/etc/sysctl.d/99-arrow-infra.conf` — idempotently, the file
is rewritten only when its content changes — and applies them with `sysctl --system`; `--check`
verifies the four rules and fails on a **wrong value**, not on a missing decision.

**4. Logs and ports.** Application output must land in `/var/log/*.log` (the collector reads that
path; there is **no** journald receiver configured, so `/var/log/syslog` — not the journal — is
what carries system messages). The collector mounts the host's `/var/log` **read-only** and reads
`/var/log/*.log` plus `/var/log/syslog` (CHG-263: `syslog` has no `.log` suffix, so the glob alone
would miss it). Open the Swarm ports between the four VM IPs
only: `2377/tcp`, `7946/tcp+udp`, `4789/udp` — plus `22/tcp` from the workstation. Two service ports
are deliberate exceptions, and each is limited to **one source address** instead of being open to the
private network:

| Port | Binds on | Allowed source | Why | Where it is set |
|---|---|---|---|---|
| `5000/tcp` | VM1 | the workstation only | the four nodes pull images through it; the workstation pushes through an SSH tunnel | registry container in S4 step 5 + firewall |
| `5080/tcp` | the node running `openobserve` (VM4 by label) | the operator's workstation only | the OpenObserve UI **is** the single pane of truth (CHG-260) | `ports:` in the stack (so it survives redeploys) + firewall |

Everything else stays closed (§9 Security baseline). The OpenObserve port binds `0.0.0.0` on its node
because a stack file cannot express a loopback binding — `host_ip` is rejected at deploy time with
`Additional property host_ip is not allowed` (measured 2026-09-20) — so the firewall rule, not the
stack file, is what keeps it private.

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
internet. One command tags, pushes and digest-resolves the seven project-built images and rewrites the
seven image lines in the deploy environment:
```bash
ssh -N -L 5000:localhost:5000 <ssh-user>@<vm1-ip> &      # keep running for the pushes
bash code/01_platform/04_scripts/image-publish.sh \
     --registry localhost:5000 --env-registry <vm1-ip>:5000 --tag prod \
     --write-env code/01_platform/01_docker/.env
```
`--env-registry` exists because the two addresses differ: the push goes through the tunnel as
`localhost:5000`, while the deploy environment must carry `<vm1-ip>:5000` — a node resolving
`localhost` reaches itself, not VM1. The digest is content-addressed, so it is the same either way.
`--print-map` shows what it will push, `--self-check` verifies the set offline, and it refuses to run
at all if the registry does not answer. The manual equivalent, image by image:
```bash
ssh -N -L 5000:localhost:5000 <ssh-user>@<vm1-ip> &      # keep running for the pushes
docker tag  <project-built-image> localhost:5000/<project-built-image>
docker push localhost:5000/<project-built-image>          # prints "digest: sha256:…" = the manifest digest
bash code/01_platform/04_scripts/digest-pin.sh localhost:5000/<project-built-image>
```
Push the seven project-built images (Flink runtime, Fluss runtime, the four app images, and the DDL-apply
tool image S7b runs on VM1 — without it the catalog step has no image to run). The two
third-party images (OpenObserve, ZooKeeper) come from their public registries at the digests
`runtime.lock` already records — if either digest no longer resolves, mirror that image here too.
Then write the digests into the deploy environment (§6.2) using **`<vm1-ip>:5000/…@sha256:…`**, not
`localhost` — every node resolves `localhost` as itself, and only VM1 hosts the registry. The digest
itself is identical whichever hostname the push went through.

**Note (measured 2026-09-19 against a local `registry:2` over plain HTTP):** `digest-pin.sh` resolved
every pushed image with no extra flags, and its value matched the digest `docker push` printed;
`docker pull <repo>@sha256:<digest>` then succeeded, while a wrong digest failed with "manifest
unknown". A *remote* plain-HTTP registry still needs the `insecure-registries` entry from step 5 —
that is a daemon setting, not a resolver limitation. If a resolution ever does fail, use the
`digest: sha256:…` line `docker push` prints; note that `docker image inspect --format
'{{json .RepoDigests}}'` records `repo@sha256:…` **without** the tag, so it is unambiguous only while
that repository holds a single tag.

**What is scripted, and what is not** (CHG-266). Steps 0–2 are one idempotent command, run on each
of the four VMs, whenever it is re-run:
```bash
ssh <ssh-user>@<vm-ip> 'cd ~/arrow-infra && bash code/01_platform/04_scripts/vm-bootstrap.sh --apply'
bash code/01_platform/04_scripts/vm-bootstrap.sh --check \
     --registry <vm1-ip>:5000 --extra-free-ports 5000   # VM1; VM4 instead: --extra-free-ports 5080
```
It installs git and the clone, Docker Engine from Docker's repository, and chrony; the clock is
asserted rather than assumed — `timedatectl` must report a synchronised clock **and** `chronyc
tracking` an offset within 1 s (`--max-offset` tightens or loosens it). `--check` is read-only, exit
code = number of FAILs, and also verifies that `docker` works without `sudo`, that `/var/log/syslog`
belongs to group `adm` (CHG-263), that the Swarm ports are free, and that `daemon.json` declares the
plain-HTTP registry.

**Step 3 applies the recorded list, and nothing else.** `--apply` writes the four rules of §6.1 to
`/etc/sysctl.d/99-arrow-infra.conf` (rewriting only when the content changes) and applies them with
`sysctl --system`; `--check` verifies each rule, so a wrong value is a `[FAIL]` naming the rule and a
host already tuned beyond a floor passes. The script contains no `sysctl -w` and no knob that is not in
that table.

**Before deploying — after step 6 has just rewritten the deploy environment.** Compose interpolates
that file silently, so a missing key becomes an empty string and a tag becomes "whatever the node
happened to pull". The preflight runs first and only passes on a coherent environment (CHG-267):
```bash
python3 code/01_platform/04_scripts/deploy_preflight.py \
     --env-file code/01_platform/01_docker/.env --expect production --check-lake --secrets-check
```
Exit code = number of FAILs. It checks the six values the platform cannot invent (the same six
`stack_selfcheck.sh` requires after the fact), that `R2_ENDPOINT` is `https://` and `CHECKPOINT_DIR`
is `s3://<bucket>/…` — a local path dies with the node that holds it — that every `${VAR}` the stack
interpolates resolves to a non-empty value, and that every image a node would pull is pinned, judging
the *effective* image so a stack-side default is checked too. `--check-lake` performs one signed LIST
through `r2-list.sh` (missing or wrong R2 credentials become one line of output instead of a tiering
mystery), `--secrets-check` runs `secrets-bootstrap.sh --check`. `--expect dev` demotes the
production-only rules to `[INFO]`.

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
# on VM4 (WORKER token) — it must join, or the five observability services have no node to land
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
`node.labels.role == worker` and the 3 observability services (collector, OpenObserve,
alert-consumer, node-exporter, cAdvisor) require
`node.labels.observability == true`. Leaving `role` off VM4 keeps the trading stack from being
scheduled onto the observability VM; leaving `observability` off VM1–VM3 keeps OpenObserve off the
workload nodes. Both directions matter.

**Exit:** three managers `Reachable`, one Leader, one worker carrying the `observability` label, and
`prod_node_check.py` re-run passes the label checks.
**Stop if:** fewer than three managers are visible — quorum is the whole point of the third VM.
**Note:** `--autolock` means a manager that reboots stays locked until you run `docker swarm unlock`.
That is the point — without it the Raft log and mTLS keys sit unencrypted on disk. Keep the unlock
key where you keep the other secrets.

### S6 — Create the nine secrets `[PRODUCTION]` (bootstrap step)
The stack declares exactly these `external: true` secrets — names are authoritative in
`code/01_platform/01_docker/docker-stack.yml`, and `secrets-bootstrap.sh --check` fails if its own
list ever diverges from it:
```text
aws_access_key_id      aws_secret_access_key   o2_password        o2_auth_basic
arrow_app_secret       arrow_password          arrow_totp_key
execution_bridge_auth_token                   gateway_shared_secret
```
**Do — one command, values piped over SSH.** The values file never lands on a VM's disk: the script
reads it from stdin, and each value reaches `docker secret create` on stdin too, so it touches
neither filesystem, neither shell history, nor either process list.
```bash
ssh <ssh-user>@<vm1-ip> 'cd ~/arrow-infra && code/01_platform/04_scripts/secrets-bootstrap.sh \
  --values-file /dev/stdin --o2-user admin@example.com' < ~/vm-secrets.env
```
That file holds the six values only you have — one `KEY=VALUE` per line, any letter case:
`arrow_app_secret`, `arrow_password`, `arrow_totp_key`, `aws_access_key_id`,
`aws_secret_access_key` and `o2_password`. The remaining three are generated on VM1:
`execution_bridge_auth_token`, `gateway_shared_secret`, and `o2_auth_basic` derived from the O2 user
and that password. A value you do supply for either generated token wins over generation, so a hand
rotation is never fought. Keep the file outside the repository and delete it when the secrets exist.

**`o2_auth_basic` is bare base64** — `base64("<o2-user>:<password>")`, with **no** `Basic ` prefix.
The collector config writes the scheme itself (`Authorization: "Basic ${file:/run/secrets/o2_auth_basic}"`
in `otel-collector-config.swarm.yaml`), so a prefixed value authenticates as `Basic Basic …` and
OpenObserve answers 401 with nothing useful in the log. An earlier revision of this step said the
opposite; the script derives the correct form and `--self-check` pins it.

**Entrypoint paths are unchanged:** Swarm mounts secrets at `/run/secrets/<name>`, and every consumer
reads them from there through a `*_FILE` variable — `AWS_ACCESS_KEY_ID_FILE`/`AWS_SECRET_ACCESS_KEY_FILE`
for the six S3 services, `EXECUTION_BRIDGE_AUTH_TOKEN_FILE` for the bridge, `GATEWAY_SHARED_SECRET_FILE`
for the execution gateway and the executor, and `ARROW_APP_SECRET_FILE`/`ARROW_PASSWORD_FILE`/
`ARROW_TOTP_KEY_FILE` for ingestion's Arrow bridge. `otel-collector` needs no variable: it expands the
file inside its own config (`${file:/run/secrets/o2_auth_basic}`). `ARROW_APP_ID` and `ARROW_USER_ID`
are **plain deploy values, not secrets** — the stack takes them as `${…:?}` environment entries, so no
secret of those names belongs here.

**`o2_password` is created but never mounted.** OpenObserve v0.91.5 has no `_FILE` support: it panics
at startup when the root password variable is missing — with the secret file sitting right next to it —
so the stack passes `ZO_ROOT_USER_PASSWORD` from the deploy environment instead. Give the same value to
both paths: `secrets-bootstrap.sh` takes it from the values file, and the deploy shell needs
`O2_PASSWORD` exported from that same file (S7), so the two cannot drift. **Choose a value that passes
OpenObserve's own policy** — 8 to 128 characters with at least one lowercase letter, one uppercase
letter, one digit and one special character — otherwise the container panics with
`ZO_ROOT_USER_PASSWORD is too weak` and `backend job init failed: channel closed` and never becomes
healthy (measured in the rehearsal with a password that had no digit, uppercase letter or symbol).
`secrets-bootstrap.sh` makes that mistake impossible to deploy: it refuses to run when the values file
omits `o2_password` (it is the one value that cannot be generated on the VM, because the deploy shell
has to export the same string), and it refuses a value that fails the policy, naming the policy in the
error.

**Exit:** `secrets-bootstrap.sh --check` prints `[PASS] all 9 secrets exist`.
**Stop if:** any name differs, or any already exists. Swarm secret values are immutable, so the
script refuses rather than skipping: `docker stack deploy` fails with "secret not found", and a
partial create leaves the stack half-deployable. To rotate: `docker secret rm <name>`, re-create,
then `docker service update --force <service>`.

### S7 — Deploy the stack `[PRODUCTION]`
Validate before deploying (CHG-270). `CLUSTER=1` never runs `swarm init` and never writes a node label —
a validator that fixes what it measures cannot report a real misconfiguration — and it fails on two things a
green `docker stack config` cannot see: a node that is not Ready+Active, and a `node.labels.… == …`
constraint no node satisfies. `docker stack deploy` has **no** `--env-file` — values come from the shell
environment.
```bash
# from VM1 only, in a shell that carries the real values
bash code/01_platform/04_scripts/stack_selfcheck.sh CLUSTER=1   # nodes, live manager, placement constraints
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

**Traps observed on a real deploy** (each one cost a full debugging cycle; none is caught by
`docker stack config`, which compiles all of them with rc=0):

- **Never split the stack across several `-c` files.** With two or more, list-valued fields
  (`healthcheck.test`, `configs`, `secrets`, `ports`) are **APPENDED**, not replaced — an override
  produced a four-element `healthcheck.test` in which the original probe still ran. Deploy the single
  committed file.
- **A failed deploy leaves orphans.** `docker stack rm` frees networks *and* configs asynchronously;
  re-deploying immediately can fail on "network not found" or "config … already exists". Wait for them
  to disappear (`docker network ls`, `docker config ls`), or remove the `prod_*` objects explicitly.
- **`update_config.failure_action: rollback` hides a broken fix.** Updating a service whose tasks keep
  dying reverts the spec silently — `docker service inspect` shows `rollback_paused` and the new
  configuration was never really live. Validate a config fix with a FRESH create after `docker stack rm`,
  exactly as a first production deploy does.
- **Swarm configs are immutable.** Replacing the instrument manifest needs a new config name, or the
  deploy fails with "only updates to Labels are allowed".
- **A scrape target with no service fails silently.** The collector scraped `node-exporter:9100` and
  `cadvisor:8080` while the stack declared neither: every scrape failed with a warning in the collector
  log, no deploy ever failed, and the infrastructure panels stayed empty. A missing scrape target is not
  a deploy error — the stack test asserts that every name the collector scrapes is a service, and the
  collector's own log is where a drifted target shows up (`Failed to scrape Prometheus endpoint`).
- **`docker stack deploy` silently DROPS compose keys it cannot express.** Measured 2026-09-20:
  `privileged: true` and `pid: host` compile, deploy with `rc=0`, and never reach the task — the running
  container reports `privileged=false` and an empty pid mode, while every `volumes:` mount survives.
  Anything security-relevant must therefore be verified on the running task (`docker inspect <task>`),
  never inferred from the stack file. The stack omits both keys and says why.
- **A `start-first` update needs a free slot, and without one it stalls silently.** The replicated-1
  services pin `max_replicas_per_node: 1`; on a cluster where the only eligible node already runs the
  replica (one-worker rehearsal, or production with two of three workers down) the new task stays
  `Pending — no suitable node (max replicas per node)`, the service reports `update in progress`, and
  the **old container keeps serving with the old configuration**. The deploy command exits 0 and the
  change never lands; the log shows nothing. Remedies in order: give the update a free node (a healthy
  cluster does this by itself), or roll that service with
  `docker service update --update-order stop-first <service>` — safe for a 3-member quorum, which keeps
  two members up — and watch `docker service ps <service>` for `Pending` after any deploy whose effect
  you expect to see. On 2026-09-20 this hid a ZooKeeper configuration change for a full round.
- **`configs:` paths are relative to the stack file, not to your shell.** The stack pulls in
  `./otel-collector-config.swarm.yaml`, `./alert-consumer.py` and `./fluss-r2-secrets-from-file.sh`,
  so deploying *a copy* of `docker-stack.yml` from another directory uses **that directory's** copies.
  Measured 2026-09-20: the stack was rendered into `~/.p6v/reh/` and deployed from there while the
  collector config was refreshed only in the repo — the deploy reported
  `Updating service prod_otel-collector`, created **no new config version**, and the collector kept
  running the previous file for hours. Deploy from `code/01_platform/01_docker/`, or copy all three
  referenced files next to the stack file, and confirm the content landed with
  the decoder in the next bullet (`base64 -d` is the wrong tool: this engine prints a byte array).
- **A stack-managed `configs:` object is immutable, and a redeploy does NOT refresh it.** After the
  paragraph above was fixed, the redeploy *still* changed nothing: `docker config ls` showed the same
  two-hour-old object and no versioned copy, the collector task was never replaced, and decoding the
  live config showed `job_name: flink | infra-host | infra-containers` — no `infra-zookeeper`. The
  deploy compares the config **name**, not its bytes, so a new file silently has no effect. To apply a
  config change either recreate the stack (`docker stack rm` then deploy — what S7b's first boot and
  the rehearsals do), or change the config's **name** in the stack file and in the services that
  reference it, so the deploy has something new to create. `docker config inspect` prints the payload
  as a byte array on this engine; decode it before grepping:
  `docker config inspect <name> --format '{{.Spec.Data}}' | python3 -c "import sys,re;print(bytes(int(n) for n in re.findall(r'\d+', sys.stdin.read())).decode())"`

### S7b — Apply the DDL catalog (first boot only) `[PRODUCTION]`
**Entry:** S7 green on a cluster whose Fluss catalog is still empty.
**Why this step exists:** the stack deploys no DDL step of its own. `ingestion` starts with
`allowRuntimeDdl=false` and exits fail-closed (`Fluss schema verification failed`) while the 27 tables of
`02_sql/ddl/schema_manifest.json` are missing, so on a fresh cluster it never reaches `1/1` and the data
loop cannot start. Nothing else in this sequence creates them.
**Precondition — check it, do not assume it:** the catalog is EMPTY. The tool refuses a populated catalog
(exit 3), so this is a once-per-cluster step.
**Do (on VM1):**
```bash
REPO=<path to the cloned repo>
DDL_APPLY_IMAGE="$(sed -n 's/^DDL_APPLY_IMAGE=//p' "$REPO/code/01_platform/01_docker/.env")"
COORD="$(docker ps -q -f name=prod_fluss-coordinator | head -1)"
mkdir -p /var/lib/trading/ddl-apply-evidence
docker run --rm --network "container:$COORD" \
  -e FLUSS_BOOTSTRAP=fluss-coordinator:9123 \
  -e DDL_APPLY_EVIDENCE_DIR=/app/logs/ddl-apply/prod-first-boot \
  -e DDL_APPLY_MATRIX_EVIDENCE=/app/code/01_platform/02_sql/ddl/schema_manifest.json \
  -v /var/lib/trading/ddl-apply-evidence:/app/logs/ddl-apply \
  "$DDL_APPLY_IMAGE" apply
```
**Expect:** `DDL-APPLY-RESULT: PASS exit=0`, then `evidence-ownership-check: PASS`, and exit code 0. The
record (`apply.json`, `tables_applied: 27`) lands in the mounted directory.
**Exit:** the catalog holds the 27 manifest tables. Then restart ingestion — its earlier task already
exited and consumed Swarm's restart budget:
```bash
docker service update --force prod_ingestion
docker service ls --filter name=prod_ingestion            # 1/1 needs the vendor feed too, see below
```
**Three measured traps:**
- `--network container:$COORD` is not a preference: the stack's overlay networks are deliberately not
  `attachable`, so a standalone container cannot join them; sharing the coordinator container's network
  namespace is what makes `fluss-coordinator:9123` resolve.
- Never pass `--apply-verified` or `--matrix-evidence` as flags: the dispatcher refuses them as duplicated
  state (exit 2). Both travel in the environment, as above; the matrix path points at the manifest baked
  into the image, not at a host file.
- `ingestion` reaching `1/1` also needs the Arrow vendor feed. With placeholder credentials the bridge is
  rejected (`Login request failed … user not found`), the service drains with 0 ticks and stays `0/1`. This
  step removes the schema failure; it is not a substitute for vendor credentials.
**Rehearsed:** 2026-09-20 on the single-node rehearsal cluster (`run9`). This exact command returned
`DDL-APPLY-RESULT: PASS exit=0` for 27 tables, and the following ingestion attempt reached the vendor
(which answered `user not found` for the placeholder user) with Fluss writes accepted, including a
`DROP/arrow-bridge crashed` discontinuity record.

### S8 — Readiness verification `[PRODUCTION]`
Assess the five dimensions **separately** and record each (`02-environments.md` §Readiness dimensions):
| Dimension | Question | Where it is answered |
| --- | --- | --- |
| Liveness | does the process/loop respond? | service health |
| Readiness | are mandatory dependencies and the data flow available? | ZK quorum, Fluss quorum/schema, S3 access |
| Job health | are the required Flink jobs running and checkpointing? | Flink REST/UI |
| Trading readiness | is the executor gate state known and reconciliation clean? | gate state — expected `HALTED` |
| Durability readiness | replication, checkpoints, offload, audit retention, recovery posture | checkpoints in encrypted S3, EOD manifest |
**OpenObserve is the one destination for every log and metric**, so it is part of readiness, not an
extra. Measured on the rehearsal cluster (2026-09-20): the collector's four log streams
(`flink_logs`, `fluss_logs`, `platform_logs`, `trading_alerts`) and the ingestion metric streams are
present, and the operator login (`ZO_ROOT_USER_EMAIL` + the deploy-environment `O2_PASSWORD`) answers
`200` on `/api/default/streams`. An OpenObserve that answers `401`, or one that holds no log stream,
fails this dimension.
Host and container metrics come from the two per-node agents, `node-exporter` (`:9100`, host
filesystems/CPU/memory through the `/host/*` mounts and `--path.rootfs`) and `cAdvisor` (`:8080`,
container cgroups). Both are `mode: global` and carry no constraint (CHG-265), so they cover every node — and each probes
itself every 30 s — a probe that depends on another service is not a liveness check.
ZooKeeper answers Prometheus on `:7000/metrics` per replica (`metricsProvider.className` +
`metricsProvider.httpPort` in `ZOO_CFG_EXTRA`; the provider class ships in the pinned image, so this
is configuration, not an extra component). That is what makes a quorum or latency problem visible:
without it, the only signal from ZooKeeper is a TCP port that either accepts or does not.
Host log files reach the pane through the collector's read-only `/var/log` mount, into the
`infrastructure_logs` stream: `syslog`, `auth.log`, `kern.log` and the rest of `*.log` from the node
the collector runs on. Two things about it are worth knowing before trusting a quiet stream:
the collector runs as `user: "0:4"` (root:adm) for this — `auth.log` and `kern.log` are
`640 syslog:adm` and a stack file cannot add a supplementary group (`group_add` is rejected), so the
alternative was collecting nothing from those files; and **the mount is per node** — the collector
is `mode: global` but pinned to `observability == true`, which in this topology is VM4 alone, so
VM1–VM3 host logs are not in the pane. The services' own log volumes (`flink_logs`, `fluss_logs`,
`platform_logs`) are named volumes, which Swarm keeps on the node that writes them, so the same
boundary applies to them.

**Opening the dashboard.** OpenObserve is the only service in the stack with a published port:
`mode: host`, `published: 5080`, on the node that runs the task — VM4 by the S5 label. The S4 firewall
table limits `5080` to the operator's address, which is the control; the stack file cannot bind
loopback (`host_ip` is rejected, see S4 step 4). Two ways in, both fine:

```bash
# either tunnel (works even when the firewall rule has not been applied yet)
ssh -N -L 5080:127.0.0.1:5080 <vm4-ip>      # leave running, open http://127.0.0.1:5080
# or directly, if your workstation is the allowed source
# http://<vm4-ip>:5080
```

The publish follows the node that currently runs the task, and is worth one command when nothing
answers: `docker service ps prod_openobserve --format '{{.Node}} {{.CurrentState}}'`. Because the port
lives in the stack file it survives redeploys; an ad-hoc `docker service update --publish-add` would
not. The development compose stack publishes the same port, so run one at a time: on a workstation
that runs both, stop the compose OpenObserve first (`make down`), or the Swarm task cannot bind `5080`
and stays `Pending`.
**Exit:** each dimension recorded as its own line of evidence. A healthy container is not sufficient for any higher dimension.
**Stop if:** `CHECKPOINT_DIR` is not `s3://` in production — the job is designed to fail fast rather than run without durable checkpoints.

### S8b — Validate the deployed cluster `[PRODUCTION]`
S3 verifies the machines and the stack file's tests verify the file; neither sees what the cluster
did with it. That gap is where every failure of 2026-09-20 lived — a task `Pending` on
`no suitable node` while `docker stack deploy` reported success, a bind mount whose source was
missing on the node the task landed on, a global agent covering one node out of four.

```bash
python3 code/01_platform/04_scripts/cluster_check.py \
    --expect code/01_platform/04_scripts/prod_vms.json --out ~/readiness
```

**Redeploying after a `configs:` change.** Swarm configs are immutable: a stack redeploy that
would change the collector config's contents fails with `only updates to Labels are
allowed` (`failed to update config prod_otel-collector-config`, measured 2026-09-20).
Apply such a change with `docker stack rm prod` followed by `docker stack deploy` — the
order the rehearsal uses — or give the config a new name.

Read-only, no SSH, exit code = number of FAILs. It checks node readiness and node labels, replica
counts, tasks waiting on a placement or a mount, `max_replicas_per_node: 1` spread, global-agent
coverage against the node labels each service selects, which service publishes a port, and — with
`--expect` — whether the cluster is the size the inventory describes.
Two verdicts, deliberately different: **FAIL** means the cluster could satisfy the check and does
not (something is broken); **WARN** means the nodes present cannot satisfy it (three replicas, one
eligible node) — expected on a rehearsal cluster, a defect on the four VMs. Measured on the 1-node
rehearsal cluster (2026-09-20) it returned exactly two FAILs, both real, and nothing else:
`prod_ingestion 0/1` with `task: non-zero exit (1)` — the missing `ARROW_*` credentials. The two
Flink services the single node cannot fit were WARNs, not FAILs.

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
| EOD | The `eod-scheduler` service fires `eod_controller.py` daily at `EOD_AT`/`EOD_ZONE` (defaults 23:30 Asia/Kolkata) with `EOD_TABLES`; it restarts forever, and its healthcheck reads its own heartbeat file (`--check-heartbeat --max-age 2700`) — no other service is involved. Set `EOD_OFFLOAD=lake` only once the R2 bucket and keys exist. Verify manifest counts/ranges/hashes then retention (`../06_operations/07-lake-archive-ops.md`) |
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
| 2 | **CloudPe security group**: inbound `22/tcp` from the workstation IP only, the Swarm ports (`2377/tcp`, `7946/tcp+udp`, `4789/udp`) between the four VM IPs only, `5000/tcp` on VM1 and `5080/tcp` on the OpenObserve node **from the workstation IP only**, everything else denied; SSH keys only with password authentication off; 2FA on the CloudPe panel | CloudPe panel | a scan from outside shows nothing but SSH and the dashboard, which is exactly the two exceptions recorded in S4 step 4 |
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
| S7b | `DDL-APPLY-RESULT` sentinel, `apply.json` path, table count | catalog-creation evidence (first boot only) |
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
