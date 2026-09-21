# Production Swarm

Build this phase, then implement the tests in the second section before moving on.

## What to build

<!-- markdownlint-disable MD013 -->

### Status

| Field | Value |
| --- | --- |
| Status | Partially implemented (offline) — M1 docs + M2 deployment static 31/31 PASS on laptop (single-VM `docker-compose.yml` + `docker-stack.yml` `test_09_stack.py`), M3 4VM live (`SWARM-MGR` quorum 2/3, `replication.factor=3`, `S3` HA, `PERF-NODELOSS`) still `TO_BE_VERIFIED` |
| Owner | Platform Team |
| Topology | v1: 4 VMs (3× Manager+Worker + 1 O2) → v2: 7 VMs (3× Manager-ONLY + N≥3 Workers + 1 O2), same stack, Option B |
| EOD controller | Named service or scheduled job owning manifest lifecycle |
| Live-money | Disabled until Phase 12 evidence passes |
| Acceptance criteria | `REQ-PF-001`–`REQ-PF-012` → `AC-PF-001`–`AC-PF-019` (proving families: `SWARM-*`, `SEC-*`, `PERF-NODELOSS-001`; local subset in `08-local-compose.md`) |

### Implementation status — 2026-08-24 (offline laptop, no 4VM; single-VM `docker-compose.yml` + `docker-stack.yml` static only)

| Milestone | Status 2026-08-24 | Evidence offline (laptop) | Needs 4VM Swarm |
| --- | --- | --- | --- |
| M1 Architecture (docs) | DONE | v1 4VM / v2 7VM Option B role labels cross-check `docs_audit` + `docker-stack.yml 724L` doc tables match; `09` `M1` docs parity `test_09_stack.py StackShape 5 Placement 3` | Live docs review on provisioned Swarm (labels visible `docker node ls`) |
| M2 Deployment (stack + 1-host mimic) | DONE | `docker-stack.yml` immutable digests `zookeeper@sha256:43d3…` `golang:1.24.5-alpine@sha256:daae04eb…`, `5 x-healthcheck` exceptions documented, `x-networks` encrypted `overlay` `attachable:false`, `secrets external:true`, `replicas 1→3` scale `31/31 PASS` `make test-09` + `docker compose config` parses; `stack_selfcheck.sh` `1-host swarm mimic` compile-only | `docker stack deploy` 7VM, `s3://tradingticks-aug-2026` `high-availability.type:zookeeper` `replication.factor=3` 8 `[ ]` placements, `SWARM-MGR-001..006` quorum 2/3 survive 1 loss |
| M3 Production HA (4VM live) | NOT FULLY | `make up` `12 Running/Started` single-VM `replication.factor=1` HA disabled `file:///checkpoints` (dev) — proves dev path | `3-node ZK 3.9.2` `HA/recovery` `PERF-NODELOSS 50k tps 3k instr` `DR-001..006` `chaos-suite` encrypted S3 recovery, capacity `500GB SSD` proof — cannot on 1 VM (`08:34` `cannot prove replication/one-VM tolerance/encrypted S3 recovery/production capacity`) |

### Placement model

| Node class | Required workload | Disk |
| --- | --- | --- |
| Workload VM 1 | Fluss replica/quorum, ZooKeeper ensemble member (1 of 3), Flink capacity (JobManager/TaskManager), assigned services | 500 GB SSD |
| Workload VM 2 | Fluss replica/quorum, ZooKeeper ensemble member (2 of 3), Flink capacity (JobManager/TaskManager), assigned services | 500 GB SSD |
| Workload VM 3 | Fluss replica/quorum, ZooKeeper ensemble member (3 of 3), Flink capacity (JobManager/TaskManager), assigned services | 500 GB SSD |
| Observability VM | OpenObserve and telemetry storage/collection | 500 GB SSD |

Fluss replicas cannot co-locate, and ZooKeeper ensemble members cannot co-locate: exactly one ZooKeeper node per workload VM. All three replicas of any critical Fluss/Flink role SHALL be placed across separate workload VMs via anti-co-location constraints. The 3-node ZooKeeper ensemble (quorum 2-of-3) and 3-node Fluss LOG-table replication survive loss of any single workload VM. OpenObserve loss must not authorize orders or erase local durable audit.

The final service-to-node placement, CPU, RAM, SSD IOPS/throughput, and network bandwidth are `EVIDENCE-BLOCKED` until `PERF-PROD-60000-001` and `FAIL-VM-LOSS-60000-001` pass. Current allocations (500 GB SSD per VM) are a starting point, not a proven sizing result.

### Stack requirements

The production stack must define:

- Immutable image digests for every image.
- Exact Java/Python/Flink/Fluss/connector/protocol versions.
- 3-node ZooKeeper ensemble (`zookeeper:3.9.2`), one member per workload VM, quorum 2-of-3, durable per-node data volume, internal-only ports (2181 client, 2888/3888 peer/leader).
- Placement constraints and anti-co-location.
- Resource reservations/limits.
- Health checks and readiness dependencies.
- Restart, update, rollback, and shutdown policies.
- Encrypted overlay/TLS-protected transport mandatory for all sensitive paths (broker, Arrow REST, S3, operator control, secret delivery, money-moving/state traffic).
- N+1 resource budget: per-VM CPU, memory, network, disk, Flink slots, Fluss capacity, checkpoint bandwidth, and catch-up rate documented; post-loss validation at 50,000 ticks/s variable average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average).
- Internal-only Fluss/tablet/checkpoint ports.
- Swarm secrets and least-privilege identities.
- Durable per-node Fluss volumes.
- Encrypted/versioned S3 checkpoint, Iceberg, and audit storage.
- Executor fencing and one active owner per `execution_partition_id`.

### Readiness sequence

1. Verify image digests, Swarm secrets, encrypted networks, volumes, and S3.
2. Verify ZooKeeper ensemble quorum (2-of-3), then Fluss quorum, replication, tablets, placement, and schema manifest.
3. Verify Flink control/workers (JobManager HA leader elected via ZooKeeper), checkpoint storage, and HA metadata storage.
4. Deploy Signal and Babysitter artifacts; verify running/checkpointing.
5. Verify Ingestion manifest/subscriptions and Action Capture protocol readiness.
6. Start Executor `HALTED`; verify state, mappings, continuity, Arrow REST contract, fencing, and telemetry.
7. Complete broker/order/fill/position/attempt reconciliation. (**Reservations REMOVED 2026-08-15, CHG-005.**)
8. Require the single-operator (Saurabh, DEC-044) approval for the same gate epoch/evidence hash.
9. Enable only the approved gate epoch.

A container or service becoming healthy never enables order placement.

### Bootstrap and scaling (config-driven)

The production stack is config-driven: the same stack files, environment variables, secrets, and artifacts run at any node count. Bootstrap on one production-like VM first, then scale to three by configuration and node addition, not by rewriting the stack.

| Stage | Nodes | ZooKeeper | Fluss replication | Flink HA | Status |
| --- | --- | --- | --- | --- | --- |
| Bootstrap | 1 workload VM + observability VM | Single node (no quorum) | Replication factor 1 (LOG and KV) | JobManager HA disabled | Validates config/DDL/connectors/jobs only; NOT HA evidence |
| Target | 3 workload VMs + observability VM | 3-node ensemble, quorum 2-of-3 | LOG replication ≥2, anti-co-located | `high-availability.type: zookeeper`, standby JobManagers | Production HA topology |

Scale-out steps (1 → 3 VMs): add the two workload nodes and labels, convert ZooKeeper single-node to the 3-member ensemble (update `server.X` entries and quorum config), raise Fluss LOG-table replication factor, enable Flink ZK HA with the ensemble quorum, and re-verify the readiness sequence. The single-VM bootstrap stage SHALL NOT be cited as quorum, replication, or HA evidence.

### Swarm control-plane architecture — DECISION 2026-08-20 (v1 Manager+Worker → v2 Manager ONLY)

**Principle:** Docker Swarm provides Raft consensus built-in. The project does not implement Raft. It only configures and validates the 3-manager topology.

**v1 — Baseline (4 VMs, ship now):** `VM1, VM2, VM3 = Swarm Manager + Worker (Active)`, `VM4 = Observability (worker, joined — label observability=true, no vote)`. VM4 must join the swarm or the observability services have no node to land on; a worker does not vote, so the quorum stays `2/3`. This is the authoritative production topology for the initial `N=3` worker baseline. It is cost-efficient (4 VMs) and HA-correct (Raft quorum `2/3`, tolerates 1 manager loss).

```text
                    PRODUCTION v1 — 4 VMs (NOW)
        VM1                     VM2                     VM3
 Docker Engine           Docker Engine           Docker Engine
 Swarm Manager+Worker    Swarm Manager+Worker    Swarm Manager+Worker
 Active                  Active                  Active
 ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
 │ ZK 1/3          │     │ ZK 2/3          │     │ ZK 3/3          │
 │ Fluss replica   │     │ Fluss replica   │     │ Fluss replica   │
 │ Flink JM / TM   │     │ Flink JM / TM   │     │ Flink JM / TM   │
 │ Ingestion etc   │     │ Workloads       │     │ Workloads       │
 └─────────────────┘     └─────────────────┘     └─────────────────┘
         \                       |                       /
          └──────────┬───────────┴──────────┬──────────┘
                     │   Swarm Raft        │
                     │   3 managers        │
                     │   quorum = 2 / 3    │
                     │   1 failure = OK    │
                     │   2 failures = lost │
                     └─────────────────────┘
                              +
                        VM4 = O1 Observability (worker, joined, no vote)
```

**v2 — Evolution (7 VMs, when scaling or contention observed):** `M1, M2, M3 = Swarm Manager ONLY (Drained)`, `W1, W2, W3 (+ W4...) = Worker`, `O1 = Observability (worker, joined, no vote)`. Trigger: `N>6` workers, sustained CPU >80%, or observed Raft election flaps. The same `docker-stack.yml` works unchanged — only `docker node update --availability drain M1 M2 M3` and adding `W1-3` changes.

```text
                    PRODUCTION v2 — 7 VMs (EVOLUTION)
     CONTROL PLANE — 3 managers (drained, no workloads)
        M1              M2              M3
     Manager ONLY    Manager ONLY    Manager ONLY
     Drained         Drained         Drained
         \               |               /
          └───────┬──────┴──────┬───────┘
                  │ Raft quorum │
                  │   2 / 3     │
                  └──────┬──────┘
                         |
     WORKLOAD PLANE — N workers (N >= 3)
        W1              W2              W3              W4  W5  W6 ...
     Worker          Worker          Worker          Worker (new)
  ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐
  │Fluss tablet│  │Fluss tablet│  │Fluss tablet│  │Flink TM   │
  │Flink TM    │  │Flink TM    │  │Flink TM    │  │+ capacity │
  └───────────┘   └───────────┘   └───────────┘   └───────────┘
                              +
                         O1 = Observability (worker, joined, no vote)
```

**Why v1 then v2:** v1 is correct for the initial baseline — Swarm docs state managers may be workers; dedicating 3 VMs to only Raft (2GB RAM) at `N=3` wastes 75% VM cost and exceeds the local PC (`15GB`) for `7-VM` validation. v2 provides strict control-plane isolation when worker pressure (Flink 30GB DirectMemory, Fluss compaction) risks Raft heartbeat latency (~10ms). The stack is forward-compatible: placement uses `role` labels (`role=manager`, `role=worker`, `flink=true`, `fluss=true`, `storage=nvme`), not hard-coded hostnames, so `W4` joins without stack redesign.

**Stack file mapping (Option B):**

| File | Topology | Swarm mode |
| :--- | :--- | :--- |
| `code/01_platform/01_docker/docker-compose.yml` | 08 local — 1 host | Compose (single Engine) |
| `code/01_platform/01_docker/docker-stack.yml` | 09 v1 — 4 VMs (`M1-3` as Manager+Worker) | `swarm init` on VM1, VM2/VM3 `join --manager`, O1 outside |
| same `docker-stack.yml` | 09 v2 — 7 VMs (`M1-3` drained) | `M1-3` `drain`, `W1-3` `join --worker`, `W4+` scale |

**Resource isolation for v1 (Manager+Worker co-location):**

Swarm Raft is light (~1 CPU, 2GB RAM, 5GB disk) but worker (Flink TM + Fluss) is heavy. v1 is safe only with explicit Swarm resource reservations/limits (infrastructure — not ranking reservation; ranking removed CHG-005):

| Swarm resource | Value | Enforcement |
| :--- | :--- | :--- |
| Manager CPU/RAM/disk | 2 CPU, 2GB RAM, 10GB disk per VM1-3 | Swarm resource allocation + alert if unavailable |
| Worker limit | 46GB RAM max per VM (85% alert) | `CONTAINER_MEMORY_ALERT_PERCENT=85` |
| Flink TM direct memory | 30GB max | `-XX:MaxDirectMemorySize` |

Without limits, Flink GC 20ms or Fluss compaction can delay Raft heartbeats → false election. v2 removes this risk by draining managers.

### Local development / HA validation environment

The local PC must reproduce production separation on lightweight VMs. `4-VM` local fits the dev PC (`15GB`); `7-VM` local is the v2 validation and requires stopping v1 VMs first or a larger host.

```text
              LOCAL PC — v1 validation (NOW)
                 |
     ┌───────────┼───────────┐
     |           |           |
    VM1         VM2         VM3         VM4
 Manager+Worker Manager+Worker Manager+Worker  Observability
  2CPU/3GB      2CPU/3GB      2CPU/3GB     1CPU/2GB
     |           |           |              |
     └───────────┴───────────┴──────────────┘
              (swarm init on VM1, VM2/VM3 join --manager, VM4 outside)

              LOCAL PC — v2 validation (FUTURE, optional W4)
                 |
     M1(1CPU/2GB) M2(1CPU/2GB) M3(1CPU/2GB)
     Manager ONLY Manager ONLY Manager ONLY
                 |
     W1(2CPU/3GB) W2(2CPU/3GB) W3(2CPU/3GB) [W4 2CPU/2GB]  O1(1CPU/2GB)
```

Purpose (both): validate topology, Swarm `docker node ls` quorum, placement, anti-co-location, manager/worker/VM failure, network partition, recovery, worker add/drain/remove, rolling deploy under degraded node. Not for `50k ticks/s`, latency, or storage perf — those require prod-sized infra.

### Phased implementation plan (v1 → v2)

| Milestone | Scope | Phases | Entry gate | Exit evidence |
| :--- | :--- | :--- | :--- | :--- |
| **M1 — Architecture** | Docs only, no infra | Inspect repo + cross-check (cross-check record superseded 2026-08-25 by `docs/plans/2026-08-25-live-readiness-unified-plan.md` §U5), update `09` with v1/v2 diagrams, label model, isolation, security | Cross-check complete | `09` updated, no contradictions, `M1-3 = Manager+Worker` pinned as v1 |
| **M2 — Deployment** | `docker-stack.yml` + Swarm config | Author `docker-stack.yml` with `replicas`, `placement: constraints [node.labels.role==worker || role==manager]` (not hostname), `preferences spread`, `healthcheck`, `restart_policy`, `update_config`, `rollback`, `Swarm resource reservations/limits` (infrastructure — not ranking; ranking removed CHG-005), `encrypted overlay` (`--opt encrypted` for `trading-net`, `execution-net`), `secrets` (Swarm secrets for S3/ARROW/O2), `volumes` (per-node durable) | `09` v1 diagram approved | `docker stack deploy` succeeds on `1-host swarm` mimic; `docker node ls` shows 3 managers quorum 2 |
| **M3 — Validation** | Local multi-VM HA | Deploy local 4-VM rig (multipass/Vagrant), run `SWARM-MGR-001..006`, `SWARM-NET-001`, `SWARM-PLACEMENT-001`, `SWARM-DEPLOY-001`, `SWARM-SCALE-001..003`, `RECOVERY-001`, `Flink SCALE` (TM slots), `Fluss SCALE` (native), `O1 FAIL` | M2 stack deploys | Evidence per `11-testing-and-release.md § Production Swarm` (SWARM-*, SEC-*, PERF-NODELOSS) |

Detailed test mapping is in `docs/plans/2026-08-25-live-readiness-unified-plan.md` §U5 (the earlier `docs/plans/production_swarm_plan_revision.md` Phases 13–17 and `docs/plans/2026-08-20-swarm-reference-cross-check.md` gap table were superseded/deleted with the 08-21/08-24 plan merge). v2 evolution (W4+ scale) is a label/drain operation with the same stack — no redesign.

### Repository and branching model

### Repository and branching model — DECISION 2026-08-20 (Option B)

**Decision:** Single branch, different files. The repository keeps one branch (`main`) for all code. Local and production runtimes are not separate branches. They share the same service code and images. Only the run files are different.

| File | Purpose | When used |
| :--- | :--- | :--- |
| `code/01_platform/01_docker/docker-compose.yml` | Local run — 1 computer (08) | Daily development, `make test-all` |
| `code/01_platform/01_docker/docker-stack.yml` | Production run — 4 computers (09) | Real 4-VM Swarm, or local mimic with `docker swarm init` on 1 computer |
| `code/01_platform/01_docker/.env` + `runtime.lock` | Secrets and fixed image versions | Both — local uses test values, production uses real secrets and `@sha256` digests |

**Why Option B:** Service code (`code/02_services/*`) does not change between local and production. Only settings change (`ZOOKEEPER count`, `FLUSS replication`, `S3 path`, `placement constraints`). Keeping one branch avoids copying the same code to 3 branches and keeps all fixes in one place.

Rejected: Option A (separate branches for local / mimic / prod) — rejected because it would duplicate the same service code across branches and make fixes harder to keep in sync.

**How to run:**

* Local (08): `docker compose -f code/01_platform/01_docker/docker-compose.yml up` or `make up`
* Local mimic of production (09 on 1 computer): `docker swarm init` then `docker stack deploy -c code/01_platform/01_docker/docker-stack.yml prod`
* Real production (09 on 4 computers): same `docker-stack.yml` on the 4 VMs after `swarm init` / `swarm join` and node labels

### Registry and storage — DECISION 2026-09-21 (public GHCR; one R2 bucket)

Supersedes the `registry:2`-on-VM1 choice the VM guide carried. The two inputs the
guide tracked as human-only are now decided. Both are recorded as **revisions**, not
silent edits: the guide's registry rows, S4 procedure, firewall table and acceptance
check were rewritten in CHG-274, and item 1 below was retargeted to GHCR with its
`registry:2` origin kept in the text.

| Input | Decision | Why | What it costs |
| --- | --- | --- | --- |
| Container registry | **Public GHCR packages** — `ghcr.io/<owner>/<image>:<tag>@sha256:…` | Every node pulls **anonymously**: no registry credential exists on any VM, and no node needs a `daemon.json` entry — the property the `registry:2` design also had, without a service to own, back up or size (that one wanted ≈3–4 GB of layers on VM1's manager disk and has no garbage collection). Public packages are free in storage and transfer, and publishing these images reveals nothing: the repository is public and no image bakes a secret (measured: 9 Dockerfiles, zero secret-bearing `ARG`/`ENV`/`COPY`). A *private* package moves the cost onto the workload — a PAT on every node, with a rotation you must not forget, where an expired token is a failed deploy | one classic PAT (`write:packages`/`read:packages`) on the workstation only, used per release |
| Checkpoint / lake storage | **Cloudflare R2** — one bucket, prefixes `checkpoints/` and `warehouse/` | S3-compatible with no egress fee and no capacity to groom, and the stack already takes `R2_ENDPOINT`/`R2_BUCKET`/`S3_WAREHOUSE_PATH` while `SignalJobConfig` already fails fast on a non-`s3://` value in production. Rejected: `file:` (dies with the node — the guide stops on it), MinIO on a VM (a pet with a disk), AWS S3 (egress cost, another account) | the bucket name + a scoped token pair; until they exist production stays on `file:` and `EOD_OFFLOAD=none` |

**Not verified:** a real push to GHCR (needs the PAT) and any four-node pull.

**Correction (2026-09-21, measured).** The push is verified and **no PAT was ever needed**:
GitHub Actions publishes the seven images with its own `GITHUB_TOKEN`
(`.github/workflows/publish-images.yml`, decision D1), and run `35567002862` on `e3ab030c`
completed all 13 steps, then committed the digest fragment as `24a061ad`. With no registry
credentials on the workstation, all seven `:prod` tags resolve to the digests recorded in
`code/01_platform/01_docker/images.published.env`, and anonymous `docker manifest inspect`
succeeds for each — so the four-node pull is the one part still unverified, and it is unverified
only because there are no four nodes yet.

### Pre-deploy implementation items (the five mechanics the VM guide marks `[NOT BUILT]`)

`docs/05_deployment/PROD_VM_PROVISIONING.md` §6.1 records five mechanics the
deployment path needs that no phase builds today. They are listed here so the
build plan owns them: all five are code rather than documentation, each needs its
own change record, and none needs the production VMs — they can be built and
tested on the workstation.

| # | Item | What to build | Acceptance test | Unblocks |
| --- | --- | --- | --- | --- |
| 1 | Publish every image and wire the lock into the deploy environment | Push the seven project-built images (Flink runtime, Fluss runtime, the four app images `INGESTION_IMAGE`, `NAUTILUS_IMAGE`, `EXECUTION_BRIDGE_IMAGE`, `EXECUTION_GATEWAY_IMAGE`, plus the DDL-apply **tool** image `DDL_APPLY_IMAGE` that the first-boot catalog step on VM1 runs — CHG-256) **to public GHCR packages** (Decision 2026-09-21; CHG-247 originally pushed to a `registry:2` container on VM1) — retired as pins in CHG-218 because an image that was never pushed has no registry manifest digest — then make every image reference the deploy environment carries an immutable digest (`ghcr.io/<owner>/name:tag@sha256:…`; a `localhost` address is wrong there, because every node resolves it as itself). `runtime.lock` already holds digests for `FLUSS_IMAGE`, `FLINK_IMAGE`, `OPENOBSERVE_IMAGE` and `ZOOKEEPER_IMAGE`; the deploy environment carries bare tags for three of them, so a deploy is currently tag-based. **Built (CHG-247): `code/01_platform/04_scripts/image-publish.sh`** — tags, pushes and digest-resolves those seven images in one pass and rewrites the seven deploy-environment lines, with `--env-registry` for the case where the push address differs from the pull address and `--self-check` for an offline pre-flight. Rehearsed end-to-end against a local `registry:2`: the pinned ref resolved by `docker pull`, a wrong digest failed, and `digest-pin.sh` needed no special handling over plain HTTP — and under a GHCR-shaped owner path all seven refs came out as `ghcr.io/<owner>/…:prod@sha256:…` (CHG-274, seven pushes). No image has been pushed to real GHCR yet. | `make pin-check` step `[5/6]` already rejects bare tags in `runtime.lock`; `tests/test_14_image_publish.py` asserts the push map covers every `${X_IMAGE:?}` the stack demands, that it never names a stock upstream image, and that the env rewrite preserves every unrelated line | VM guide `S1` (build) and `S4` (publish), therefore `S7` |
| 2 | Secrets bootstrap from one values file | One script creating the nine `external: true` secrets the stack declares, from a single git-ignored values file and never from a command line visible to `ps`. The stack file is authoritative for names; `docs/05_deployment/06-swarm-secrets.md` still creates five ingestion-era names, two of which (`arrow_app_id`, `arrow_user_id`) are not stack secrets at all — it must be rewritten in the same change. **Built (CHG-248): `code/01_platform/04_scripts/secrets-bootstrap.sh`** — creates all nine in one pass, generating the four internal/derivable ones and deriving `o2_auth_basic` as bare base64 (the collector config writes the `Basic ` scheme itself), with `--check` for presence and `--values-file /dev/stdin` so the values file never lands on a VM's disk. `tests/test_15_secrets_bootstrap.py` asserts parity with the stack's `secrets:` block, that all nine values travel on stdin and never in an argument, and that a refused run creates nothing | VM guide `S6` |
| 3 | Host bootstrap and a real clock source | A script installing Docker Engine and enabling NTP/chrony on a fresh VM, plus an offset check in `prod_node_check.py` (it probes reachability, disk, role and labels, and records `clock: UTC` as a label only — it measures no offset). Enforcement itself already exists: CHG-107 arms a periodic `DriftMonitor` in the live boot loop, but the offline path uses `FixedOffsetSource(0)`, so the real NTP/chrony `OffsetSource` is unbuilt. **Built (CHG-272):** the sysctl list is recorded as four rules, each with the failure it prevents, and `vm-bootstrap.sh --apply` writes and applies exactly those (a host already tuned beyond a floor passes — the rules are comparisons); `prod_node_check.py` **measures** each node's clock (`timedatectl` + `chronyc tracking`, the field S4 reads) and fails beyond the executor's own limit; `ChronycOffsetSource` is the real NTP-backed source, selected by `CLOCK_OFFSET_SOURCE=chronyc` and failing closed when unreadable. **Not verified:** the executor runs in a container, so production also needs `chrony` in that image and the host's chrony socket reachable from it — unprovable without a VM, so the default stays the fixed source and the host clock is gated at S3/S7 instead. | `prod_node_check.py --self-check` green on a fresh VM; a drift test proving `\|offset\| > CLOCK_OFFSET_LIMIT_MS` still fails closed to `safety_halt()` | VM guide `S4` |
| 4 | Multi-node pre-deploy validation | `stack_selfcheck.sh` refuses to run when the swarm has more than one node ("single-host mimic, not a cluster"), so nothing validates a real cluster's stack before `docker stack deploy`. Either parameterise that script for a real cluster or add a cluster validator covering the 16 `required_vars`, `docker stack config`, placement labels and node roles. **Built (CHG-270): `stack_selfcheck.sh CLUSTER=1`** — validation-only against an existing swarm: it never runs `swarm init` (a node meant to join a cluster must not become a one-node swarm of its own) and never writes a label, and it checks what a green `docker stack config` hides — every node `Ready`+`Active`, a live manager, and every `node.labels.… == …` constraint of the **rendered** stack satisfied by some node's labels (rendered, not source, so anchors and per-service overrides are included; the real render carries two: `role == worker` ×16, `observability == true` ×3). `CLUSTER=1 DEPLOY=1` is refused: `DOWN=1` defaults on, so deploying from a validator would remove the production stack as a side effect of a check. | Validator green against a multi-node swarm, with its failure modes asserted offline | VM guide `S7` |
| 5 | EOD scheduling | The stack requirement is "a named service or scheduled job owning manifest lifecycle", and `eod_controller.py` exists (SCH-23; subcommands `status`, `run`, `extend`, `reconcile`, `reset`), but no cron, systemd timer or stack service anywhere invokes it, so the EOD manifest lifecycle never starts in production. **Built (CHG-269): `code/01_platform/04_scripts/eod_schedule.py`** — a trigger that owns timing only (the controller keeps durability: its state table and lease), fired daily at `EOD_AT`/`EOD_ZONE` (default 23:30 `Asia/Kolkata`) from a one-replica `eod-scheduler` stack service that reuses the `ddl-apply` image, which already shipped the controller. `EOD_TABLES` is fail-closed with no default (an operator decision), `EOD_OFFLOAD` stays `none` until the lake bucket exists, and the container's healthcheck reads its own heartbeat file, never another service. `tests/test_15_eod_schedule.py` covers the clock (DST, strictly-after, recompute after every fire) and the process-level retry; `tests/test_09_stack.py` pins the service contract. | A scheduler entry or stack service that invokes the run, plus a test asserting the manifest reaches `VERIFIED` | VM guide `S11` |

**Ordering:** items 2 and 1 are the hard prerequisites for any deploy; 3 and 4 are
prerequisites for *validating* a deploy; 5 is independent. The two inputs that stayed
human-only are decided (Decision 2026-09-21 above): the registry is public GHCR, and
the `CHECKPOINT_DIR` (`s3://`) value comes from one R2 bucket. What remains human-only
is the bucket name and its token pair, tracked in the live-readiness ledger.

### The Flink runtime image (CHG-179)

The production stack cannot `build:` an image — Swarm ignores the directive — so
the Flink image the two Flink services run is built on a build host, pushed to a
registry, and referenced by digest through `FLINK_IMAGE`. It is **not** the stock
`flink` image: the stock image cannot run this job at all.

Build it, then push it, then pin it:

```bash
make flink-image                       # fetch + SHA256-verify + build + re-verify
docker push <repo>:<tag>
bash code/01_platform/04_scripts/digest-pin.sh <repo>:<tag>
# put the printed ref on FLINK_IMAGE in code/01_platform/01_docker/runtime.lock
```

**Push before pinning.** A locally built image that was never pushed has no
registry manifest digest, so `repo@sha256:<image-id>` does not resolve — the
pin would name an image no node can pull. `FLINK_IMAGE` stays on the stock
digest until that push happens.

The build lives in `code/01_platform/01_docker/flink-runtime/`; its README
carries the pin table, the reproducible-derivation rules, and the residual risk.
What it adds to the stock image, and why each item is load-bearing (all verified
in-container 2026-09-16, CHG-179):

| Addition | Without it |
| --- | --- |
| Fluss connector + filesystem plugins in `/opt/flink/lib` | The job cannot load the connector — `compute.jar` ships zero `org/apache/fluss` classes (they are `provided` scope, supplied by the cluster). |
| `/etc/hadoop/conf/core-site.xml` | The object-store **endpoint** is lost. Credentials are still found by the default chain, so `s3://` silently resolves against Amazon's real S3 and fails `403 InvalidAccessKeyId` — a confusing failure, not an obvious one. |
| `ENABLE_BUILT_IN_PLUGINS` | `flink-s3-fs-hadoop` ships in `/opt/flink/opt` but is not linked into `plugins/`, so `state.checkpoints.dir: s3://…` has no filesystem. |
| The secret-bridge entrypoint | Swarm delivers the credentials as **files**; `core-site.xml` can only expand environment variables. |

Two stack-side defects are fixed in `docker-stack.yml` rather than the image,
and both must stay explicit: each Flink service declares its `command`
(`jobmanager` / `taskmanager`) and its `AWS_REGION`. With no `command:` the
image's default is the usage/help path, so the container prints usage and
**exits 0** — Swarm then reports the task as started while nothing runs. With no
region, S3A fails `400 Bad Request`.

### The Fluss runtime image (CHG-183)

The same "cannot `build:`" rule applies to the Fluss servers. Their stock image
cannot load a lake plugin, because Fluss loads `/opt/fluss/plugins/<name>/` as
**separate classloaders**: the stock image ships `fluss-fs-s3` and
`fluss-fs-hdfs` in `plugins/s3` and `plugins/hdfs`, where the `iceberg` loader
cannot see them, and Iceberg's `HadoopCatalog` needs
`org.apache.hadoop.conf.Configurable` from exactly those jars.

`make fluss-image` builds the derived image from
`code/01_platform/01_docker/fluss-runtime/`; push it and pin `FLUSS_IMAGE` the
same way as `FLINK_IMAGE`. Measured 2026-09-16 with the identical production
`datalake.iceberg.*` config, same network and same credentials:

| Image | Result |
| --- | --- |
| Stock `apache/fluss` | container **exits** — `NoClassDefFoundError: org/apache/hadoop/conf/Configurable` |
| Derived | container **runs** — Iceberg `HadoopCatalog` constructed, `Successfully start Netty server` |

The dev compose gets the same effect from bind mounts out of
`fluss-plugins/`, which is a gitignored tree — `git ls-files` on it returns one
file, the README — so those jars exist only where someone downloaded them by
hand, and a Swarm stack cannot express a per-node bind mount portably.

Two traps, both recorded in CHG-183:

- **`remote.data.dir: s3://…` does NOT exercise this path.** That is log/KV
  tiering, which loads `plugins/s3` and already worked (FACT-011). Lake tiering
  needs `datalake.*` in the config before the iceberg classloader is involved at
  all.
- **`datalake.iceberg.iceberg.hadoop.fs.s3.impl` (and the `s3a` twin) are
  required.** Without them the classloader is fixed but startup fails
  `No FileSystem for scheme "s3"` — which reads like the same bug and is not.

### The Fluss startup fix (CHG-182 fixes CHG-181 — Tasks 1-4)

Same defect class as the Flink `command:` gap above, found while planning the
R2 tiering move. Four production Fluss services shipped with no `command:`, a
probe that cannot run, no R2 credentials in the process, and a tier directory
that lives on one machine.

| # | What was wrong | What changed |
| --- | --- | --- |
| 1 | No `command:` — image default is `help`, prints usage and exits 0, so Swarm reports started while no server runs | `fluss-coordinator` declares `command: ["coordinatorServer"]`; `fluss-tablet-1/2/3` declare `command: ["tabletServer"]` |
| 2 | Health probe used `CMD-SHELL` with `/dev/tcp` — `/bin/sh` in this image is dash, which has no `/dev/tcp`, so a healthy server reported unhealthy | Probe runs under `bash` against `$HOSTNAME` first, loopback fallback |
| 3 | R2 keys arrived as Swarm file secrets nothing read | New `fluss-r2-secrets-from-file.sh` (Swarm `configs:`) reads `*_FILE`, fails closed on unreadable/empty, exports into the process, then `exec`s the stock entrypoint |
| 4 | `remote.data.dir` was `/tmp/fluss/remote-data` on a volume with no driver and no placement pin — tiered bytes land on one machine and reads from another machine find nothing | All four services set `remote.data.dir: s3://${R2_BUCKET}/remote-data` plus the `s3.*` key set; `fluss-remote-data` volume deleted; `R2_BUCKET` joins the `stack_selfcheck.sh` required list |

Evidence (all run 2026-09-16):

- `docker run --rm apache/fluss:0.9.1-incubating` prints
  `Usage: docker-entrypoint.sh (coordinatorServer|tabletServer)` and exits 0.
- `docker stack config` rendered `command=None` for all four Fluss services
  before the fix; after, it renders `coordinatorServer` once and
  `tabletServer` three times.
- Live probe: `bash` against the open port exits 0, against a closed port
  exits non-zero; the same probe under `sh` (dash) fails even on the open port.
  Fluss binds the container **hostname**, not loopback, so the probe tries
  `$HOSTNAME` first and falls back.
- Live bridge, four runs against the real image: with both `*_FILE` readable
  both variables reach the server process and the newline is stripped; a
  missing or empty secret file aborts `FATAL` with rc=1; with no `*_FILE` set
  a directly-passed value is tolerated, so dev is unaffected.
- Live tiering **write**: a two-container trial carrying the production
  `FLUSS_PROPERTIES` rolled its segments and copied them to R2 —
  `Copied ... to remote storage as remote log segment: <uuid>` — and
  **committed the manifest**: ZooKeeper then held
  `remote_log_manifest_path: s3://<bucket>/remote-data-trial3/log/.../<uuid>.manifest`
  with `remote_log_end_offset: 9923850`. An independent bucket listing taken
  after the trial was stopped agreed: **833 objects / 66 MB, 208 of them `.log`
  segments** plus their index, timeindex and writer-snapshot companions. A first
  attempt showed 0 keys and looked like
  a failure; tiering copies only *closed* segments, and three small rows never
  rolled one. *A 0-key listing after a small write says nothing about tiering.*
- Live read of a table whose data is intact: `SELECT COUNT(*)` over
  10,000,000 rows returned `10000000`, so the trial cluster serves reads
  normally.
- Live counter-example worth keeping: a tablet started against a **fresh, empty**
  local directory did **not** re-read its log from R2. ZooKeeper still held the
  manifest pointer and offset, but the tiering task logged
  `Local end offset should be greater than remote end offset ... local: 0 and
  remote: 9923850. Reset remote end offset to local end offset.` and the same
  `SELECT COUNT(*)` returned **0**. In this version R2 is where tiered segments
  are kept for cost and for the lakehouse, not a restore path for a tablet that
  has lost its local log. This is a fact about a tablet **losing** its log; the
  client-side read path proven above is a different path and is unaffected.

Credential path: Swarm files at `/run/secrets/...` → bridge exports
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` → `FLUSS_PROPERTIES` keeps the
`${env.AWS_ACCESS_KEY_ID}` placeholders (the entrypoint leaves them; Hadoop
expands them at read time).

Not verified — the honest boundary of this evidence:

- **No real `docker stack deploy`.** This host is a Swarm worker, so
  `make stack-selfcheck` stops before reaching the stack and the compile check
  used instead is the manual `docker stack config` above.
- **A read served from R2 — now proven** (closed 2026-09-16, after this
  section was first written). A trial that let local cleanup proceed
  (`table.log.tiered.local-segments: 1`; the same key set to `0` is rejected by
  `LogTablet`, so cleanup never runs and every row stays local) tiered 17 closed
  segments and then **deleted its local copies**, leaving one local segment with
  base offset 981794 while the manifest covered offsets 0–981794. A default
  Flink SQL `SELECT` then returned a row whose bytes live at an offset below
  that base — a write3 row, so somewhere in 20005..520000, remote under every
  candidate position — and the task manager logged the client downloading all 17
  remote segments
  (`Successfully downloaded remote log segment file ...`). This is a
  **client-to-R2** read: the task manager carries `core-site.xml` and the R2
  environment, and no startup option is needed to reach the bucket.
- **What Swarm does with an unhealthy task** still needs a manager node. The
  startup fix rests on the false-signal argument alone.

And one thing this change deliberately does not claim: R2 is a tiering target,
**not** a recovery source for a tablet that has lost its local log (see the
counter-example above). Production gives each tablet its own data volume, so an
ordinary restart or reschedule keeps its local log; a lost volume is lost
either way. Lake tiering (`datalake.*`) is a separate matter, fixed in CHG-183:
the Fluss server image is now built from
`code/01_platform/01_docker/fluss-runtime/`, which bakes `fluss-fs-s3` +
`fluss-fs-hdfs` **into** `/opt/fluss/plugins/iceberg/` — what dev was getting
from its bind mounts. Fluss loads each plugin directory as its own classloader,
so the stock image's copies in `plugins/s3` and `plugins/hdfs` are invisible to
the iceberg one: with the production `datalake.iceberg.*` config the stock image
exits `NoClassDefFoundError: org/apache/hadoop/conf/Configurable`, while the
derived image starts with the Iceberg catalog loaded. `FLUSS_IMAGE` still names
the stock digest, so a deploy today still gets the stock behaviour — build, push
and pin before expecting the lake path to work (FACT-014). Full detail:
`docs/05_deployment/change-records/CHG-183.md`, CHG-182 for the log/KV path, and
the plan's *Post-Completion*,
`docs/plans/2026-09-16-production-fluss-startup-and-tiering.md`.

### Storage and recovery

- **Job artifact model (CHG-110 native split):** the compute image is the
  **platform only** (Flink + launcher) — the job jar is a **separate
  artifact**. In production the jar SHALL be published to the object store
  (R2/S3, alongside checkpoints) and referenced at submit time
  (`flink run s3://…/compute.jar` via `submit-jobs.sh` / rollout), keeping
  the image static + digest-pinned. A release = upload a new jar version +
  savepoint-restart; **no image rebuild per code change**. The image itself
  is the CHG-179 runtime image above — rebuild it only when the Flink/Fluss
  versions or the object-store wiring change.
- Fluss data: hot segments stay on durable per-node volumes (`fluss-data`, `fluss-tablet-data-1/2/3`) with tested replication (LOG tables; KV tables are single-replica in Fluss 0.9.1 — durability via Fluss remote storage + rebuild from audit (Flink checkpoints hold only small working/recovery state — DEC-038)); tiered segments live in R2 at `s3://${R2_BUCKET}/remote-data`, readable from any node holding the credentials.
- ZooKeeper ensemble members use durable per-node volumes; loss of one member is tolerated while quorum (2-of-3) holds.
- Flink checkpoints/savepoints use encrypted versioned S3; Flink JobManager HA metadata (`high-availability.storageDir`) uses the same encrypted S3 store, with leadership in ZooKeeper.
- Iceberg/audit storage uses encryption, versioning, and approved retention/lifecycle policy.
- Operational projections are rebuildable from immutable events/audit or a tested backup.
- Loss of any one workload VM is tested at 50,000 ticks/s variable average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average).
- RPO/RTO is recorded per failure scenario; no untested global claim is made.
- Flink JobManager HA: `high-availability.type: zookeeper` with `high-availability.zookeeper.quorum` = the 3-node ensemble, `high-availability.storageDir` = encrypted S3, `high-availability.zookeeper.path.root: /flink`, per-cluster `high-availability.cluster-id`. Multiple standby JobManagers run across workload VMs; ZooKeeper elects the leader, and a standby takes over JobManager failure without a full job re-submission.
- Checkpoint restart strategy: max 3 retries at 30s pause between attempts. After 3 consecutive checkpoint failures, the job fails. Swarm restarts it from the last successful checkpoint. If no valid checkpoint exists, the job stays down → critical alert → manual savepoint restore. Deployment SHALL reject unbounded retry. [Source: `REQ-FC-008`. The pre-DEC-038 headroom note ("estimated checkpoint size ~600 MB – 1 GB; 30s timeout provides 2-5× headroom over estimated write time") is **superseded 2026-08-14** — under DEC-038 the dedup set moves to Fluss and the checkpoint is small; the headroom statement is re-derived from measured post-externalization checkpoint size, not asserted.]

### Security and networking

Define separate logical networks for:

```text
data ingress
storage/control
compute
execution
observability
operator control
```

Only required ingress endpoints are exposed. Service identities are least-privilege. Broker/Arrow REST/S3/OpenObserve/TLS/operator credentials rotate through Swarm secrets or approved workload identity.

### Deployment and rollback

Every change record includes:

- Artifact digests and version matrix.
- DDL/schema/state compatibility.
- Savepoint/checkpoint impact.
- Gate halt and reconciliation requirement.
- Prechecks and acceptance evidence.
- Rollback artifact and state-readability path.
- Post-deployment verification.

Any uncertain rollback returns the Executor gate to `HALTED`. Schema-breaking clean break is permitted only before live-money release with reset/replay evidence.

The stage-by-stage procedure for executing this section on the production VMs —
build/publish/pin, cluster formation, secrets, deploy, readiness, drills — is
`docs/05_deployment/PROD_VM_PROVISIONING.md` §9 (`S0`–`S11`). That document is a
procedure only: it records no status, and no stage in it may enable order placement.

### M2 completion — Tier-2 stack hardening (2026-08-21)

Closes the remaining offline-gateable acceptance gaps in `docker-stack.yml`
(machine-checked by `test_09_stack.py::TestTier2Hardening`):

| Contract | Rule | Test |
| :--- | :--- | :--- |
| **No mutable image tags** | every service image is a literal `@sha256` digest **or** a `${...:?set RAM_IMAGE to an immutable digest}` form (operator must supply a digest at deploy). Bare tags (`foo`, `foo:1.2`, `foo:latest`) are rejected. | `test_no_mutable_image_tag` |
| **Health & readiness** | every service has a `healthcheck`, OR a documented `# x-healthcheck:` exception marker naming one of the allowed exceptions and why. | `test_every_service_healthcheck_or_documented_exception` |
| **Rollback defaults to halted** | every service with durable per-node volumes, plus the executor (`nautilus`), sets `update_config.failure_action: rollback` so a bad update returns to the last good state (gate back to `HALTED`). | `test_rollback_on_every_stateful_and_executor` |
| **Encrypted overlays** | `trading-net`, `execution-net`, `arrow-egress` are overlay + `encrypted: true` (SN/w08). | `test_overlay_networks_encrypted_all` |

Real digests pinned this pass: `zookeeper@sha256:43d3…`, `otel/…contrib@sha256:e393…`,
`python@sha256:9c90…`. Local/docker images (`ingestion`, `execution-*`, `nautilus`,
`fluss`, `flink`, `openobserve`) are pinned as `${…:?set … to an immutable digest}`
— no mutable tag may exist anywhere in the stack.

Healthcheck coverage: full `CMD`/`CMD-SHELL` probes on zookeeper-1/2/3 (`zkServer.sh
status`), fluss-coordinator (9123), fluss-tablet (9124), flink-jobmanager (8081),
openobserve (5080), ingestion + execution-bridge (kept). Five services intentionally
carry **no** swarm healthcheck, each with an in-place `# x-healthcheck:` reason:

| Service | Why no swarm healthcheck |
| :--- | :--- |
| `otel-collector` | distroless image — no shell, cannot run a CMD probe |
| `flink-taskmanager` | no fixed external listener; liveness = JM 8081 probe + slot allocation |
| `execution-gateway` | `GATEWAY_BIND_PORT` env-driven; readiness = app `GatewayReadiness` gate |
| `webhook-receiver` | stateless ingress; port in-app |
| `nautilus` | `EXECUTOR_LISTEN_ADDR` env-driven; liveness/ownership = app fencing |

Their *liveness* is the application-level readiness/fencing gates (not a TCP probe),
and each still honors Swarm `restart_policy` / `update_config.rollback`.
Runtime firing of the shell probes is validated on the M3/Swarm-mimic stack (M2 is
offline-gateable only — `docker stack config` rc=0 and the 25 `test_09` cases are
green; live quorum/HA is M3 evidence, per `## Verification mapping`).

### Capacity acceptance


The production-like topology must prove:

- variable 50,000 ticks/s average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) for a full session with decision p99 under 100 ms.
- One workload VM loss at the per-instrument production rate.
- Data recovery under 30 seconds for accepted scenarios.
- Safe-halt under five seconds.
- Full-volume EOD verification under 30 minutes.
- No duplicate broker order in crash-window tests.

### JVM and memory configuration

Every Java container (Ingestion, Flink TaskManager, Flink JobManager) SHALL enforce:

| Config key | Required value | Enforcement |
| --- | --- | --- |
| `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT` | `65` | Java max heap equals 65% of the container memory limit |
| `NON_HEAP_MEMORY_RESERVE_PERCENT` | `35` | Container limit minus Java max heap must be at least 35% |
| `CONTAINER_MEMORY_ALERT_PERCENT` | `85` | Emit critical alert at or above 85% total container memory |

- Set an explicit container memory limit for every Java container.
- Verify at startup that the container memory limit minus maximum heap is at least 35% of the container memory limit.
- Refuse production readiness when total container memory reaches or exceeds 85% for 60 consecutive seconds.

### Concrete sizing (48 GB VM) — pre-DEC-038, superseded

Derived for a Flink TaskManager on a 48 GB VM. All numbers are starting points — superseded by `PERF-PROD-60000-001` **and by the DEC-038 state-ownership change (2026-08-14)**. This table sized RocksDB around the ~1 GB dedup state budget; under DEC-038 the dedup set moves to Fluss and the Flink-side state is small (windows, timers, bounded working cache), so the RocksDB/direct-memory dominance and the split between the generic 65%/35% formula and a RocksDB-heavy split must be **re-derived from measured post-externalization memory**, not asserted. The rows below are the pre-change baseline.

| Resource | Value | Notes |
| --- | --- | --- |
| Container memory limit | 48 GB | Explicit Swarm limit |
| Java max heap (`-Xmx`) | **8 GB** | Modest — working state lives in RocksDB (direct memory); pre-DEC-038 rationale — re-derive after externalization |
| Direct memory (`-XX:MaxDirectMemorySize`) | **30 GB** | RocksDB block cache + Flink network buffers; pre-DEC-038 rationale — re-derive after externalization |
| OS reserve | **~10 GB** | OS page cache, off-heap, Fluss client |
| GC | `-XX:+UseG1GC -XX:MaxGCPauseMillis=20` | Protect p99 <100 ms decision SLO |
| Container memory alert at 85% | ~40.8 GB | Critical alert when hit for 60 consecutive seconds |

For non-Flink containers (Ingestion, Action Capture, Executor), use the generic 65%/35% formula above. The Flink TaskManager split is different because RocksDB uses direct memory for its block cache and SST buffers. Source (pre-DEC-038): dedup state budget ~1 GB, window + candidate state <10 MB (**ranking state REMOVED 2026-08-15, CHG-005**), leaving substantial headroom for RocksDB block cache, write buffers, and network memory — the dedup term moves to Fluss under DEC-038 and this rationale is re-derived.

### Acceptance checklist

- [ ] Production stack is separate from Compose.
- [ ] No mutable image tags or unpinned dependencies remain.
- [ ] Placement prevents Fluss replica and ZooKeeper ensemble-member co-location.
- [ ] ZooKeeper ensemble quorum 2-of-3 verified; one ZK node loss tolerated.
- [ ] Secret/identity/network tests pass.
- [ ] S3 checkpoint/lake/audit recovery passes.
- [ ] One-VM loss passes with documented RPO/RTO.
- [ ] Job, service, durability, and trading readiness are separate.
- [ ] Rollback defaults to halted and preserves state readability.

## Verification mapping

The required behavior above is verified by the canonical [Production Swarm test design](./11-testing-and-release.md#production-swarm-v1-4-vms-managerworker-v2-7-vms-manager-only-workers): `SWARM-INT-001`, `SWARM-INT-002`, `SWARM-FAIL-001`, `SWARM-FAIL-002`, `SWARM-REC-001`, `PERF-NODELOSS-001`, and `SEC-NET-001` to `SEC-AUDIT-001`.

---

## P10 rehearsal and cutover plan (RE-SCOPED 2026-08-13)

### P10 — Signal LOG/KV Dual-Sink Rehearsal & Cutover Runbook (tracker 14)

**Status:** `EXECUTED 2026-08-17 on the Design-B topology (all 10 re-scoped P10.1 boxes PASS — evidence logs/tracker-14/p10-rehearsal-design-b-20260817.md; P10.2/P10.3 delivered as the ready-runbook in §13; production execution stays deferred — no production exists). RE-SCOPED 2026-08-13 — previously PLANNED (not yet executed) as a candle KV migration rehearsal; target changed to the SIGNAL dual-sink per requirement change. Phase 0 isolation groundwork (overlay compose, empty rehearsal trio) remains valid; no data copy was ever performed.`
\*\*Location:\*\* `docs/08_implementation/09-production-swarm.md`
**Tracker:** the master dossier `docs/08_implementation/04-signal-job.md` §Absorbed documents (2026-08-17 consolidation; the tracker substance moved there — `14-candle-log-kv-replay-safety_2.md` was deleted; the `## P10 — Operator-only migration and cutover` (RE-SCOPED), `## P10.1 Isolated rehearsal`, `## P10.2 Production blue-green cutover`, `## P10.3 Rollback` records live in git history).
**Sequencing gate:** the tracker says "Do not execute until P1–P9 code/evidence gates are complete" — this plan starts strictly AFTER (a) the signal dual-sink implementation (candle **KV-only** sink + `Signal_Candidates` LOG + `Signal_Candidates_current` KV) and (b) the P7.2/P7.3 battery re-run on the new topology (`docs/08_implementation/11-testing-and-release.md`).
**Recipe source:** `skill://candle-kv-rollback-rehearsal` (B8.7 rollback/re-cutover procedure pattern) + `candle-failure-injection-tests` (state-restore verification patterns) + `§P3.5 — R2 lake-read stall containment` (absorbed into `04-signal-job.md` §Absorbed documents; plan file never persisted) (R2 containment).

> **REQUIREMENT CHANGE (user decision, 2026-08-13):** the candle [LOG + KV] facility is
> RETIRED (no per-stock candle audit). The [LOG + KV] facility moves to the trade-signal
> tables: `Signal_Candidates` → LOG (append one row per found signal, never updated) and
> `Signal_Candidates_current` → KV current-state (PK `instrument_token`, latest/active
> candidate per instrument, supersession overwrites). This rehearsal rehearses **that**
> dual-sink: cutover, bounded-replay idempotency (LOG grows / KV keys frozen), rollback,
> and re-cutover. There is **no candle history audit, no conflict reconciliation, no
> migration load**. The earlier Phase 0 record (`logs/tracker-14/p10-rehearsal-20260813.md`,
> divergences D1–D6) stays valid for the isolation mechanics (byte-copy, warehouse
> prefix, remapped ports); the candle-specific rehearsal steps below were removed.

## 1. Objective

Execute the full P10.1 isolated rehearsal on the dev host (all 10 re-scoped boxes), deliver P10.2/P10.3 as a ready-to-run production runbook (execution deferred — no production exists), and leave the register/evidence trail so production cutover is a command sequence, not a discovery exercise. Dev cluster is the qualification target (user decision); "production objects" in P10.1 wording = the live dev stack's objects.

## 2. Locked spec (user decisions, 2026-08-12 + 2026-08-13)

### 2.1 Scope decisions

| Dimension | Decision |
| --- | --- |
| Production definition | Dev cluster = qualification target; P10.2/10.3 delivered as a READY runbook, executed only when a real production deployment exists |
| Isolated env | Second compose project on this host (the `p10` overlay built 2026-08-13): separate project name → separate network, container names, remapped ports (12181/19123/19124/18081/19249/19250); live stack untouched |
| Rehearsal data | Full dev data + checkpoints (Fluss tablet segments + ZK metadata + archived known-good checkpoint); R2 lake objects shared read-only where the source reads the lake tier (§2.2) |
| Table provisioning | `feature_candles_15s` (KV, PK `(instrument_token, window_start)` — sole candle output, converted 2026-08-13), `Signal_Candidates` (LOG), `Signal_Candidates_current` (KV, PK `instrument_token`) via the identical gated DDL path; preflight validator re-targeted (SIGNAL-SCHEMA-001) |
| Signal LOG contract | Append one row per fired signal; never updated; replay appends are retained as evidence (never silently deleted) |
| Signal KV contract | Exactly one latest/active candidate per instrument; supersession replaces the row; KV key count == active instruments after replay |
| Bounded replay | Bounded replay run TWICE from the same source offsets — idempotency proof: signal LOG may grow, `Signal_Candidates_current` key count frozen |
| Checkpoint source | Archived known-good checkpoint (archived before live dev runs rotate it); restore reads the archive copy, never the live checkpoint dir |
| Rollback rehearsal | B8.7-style FULL: rollback (dual-sink → single-LOG artifact, pre-cutover checkpoint, signal KV frozen / LOG grows) AND re-cutover (dual-sink from its own checkpoint) |
| Operator | Autonomous execution by the agent; user reviews evidence + approves register rows |
| Sequencing | Strictly after signal dual-sink implementation + P7 re-run (no parallel override) |
| Consumers | P10.2 "point current-state consumers to `Signal_Candidates_current`" marked VACUOUS today (no consumers exist) — delivered as a runbook step for when the downstream pipeline lands |
| DDL path | Identical gated path: `ddl_apply.py` + version-matrix gate, against the isolated env's coordinator |
| Exposure record | Signal LOG + KV envelope: timestamp range + row counts + sampled keys written during the dual-sink window |

### 2.2 Isolation mechanism (design, verified against Fluss lake layout at execution)

- Fluss lake objects live at `s3://<bucket>/lake/<database>/<table>/...` (verified path layout: `lake/default/candle_scale_log/metadata/`). The isolated env creates its tables in a dedicated database (e.g. `rehearsal`) → its lake writes land at `lake/rehearsal/<table>/` — no R2 object collision with live `lake/default/...`.
- The rehearsal SignalJob reads the copied `raw_table_1` history (log segments + attached lake tier, read-only) and writes new candles/signals to the rehearsal env's own tables (db `rehearsal`, distinct lake path + separate tablet data dirs).
- Checkpoint restore reads the ARCHIVED COPY (`s3a://.../p10-rehearsal/<run>/chk-N` + `shared/`), never the live checkpoint dir.

## 3. Prerequisites (checked at Phase 0 entry)

- [ ] Signal dual-sink implemented: candle **KV upsert** sink (PK `(instrument_token, window_start)`), `Signal_Candidates` LOG append sink, `Signal_Candidates_current` KV sink; `CandleGraphReplayIntegrationTest` re-scoped and green.
- [ ] P7 battery re-run on the new topology with evidence registered (`PERF-*` + `DEDUP-MEMORY-001` rows annotated).
- [ ] Archived known-good checkpoint copied to a stable archive prefix (`s3a://…/p10-rehearsal/archive/`) BEFORE further live runs rotate it.
- [ ] Live stack healthy; no other rehearsal/bench in flight.
- [ ] R2 endpoint reachable; S3A timeout pins + outer-deadline containment available for every checkpoint step.
- [ ] Fast smoke gate probes available on the rehearsal network (§4.1): `probe-r2.sh` + bounded-read probe + KV-count probe.

## 4. Phase 0 — prepare isolated environment

1. **Archive the checkpoint:** copy the known-good R2 checkpoint tree (chk-N + `shared/` incremental SSTs) to the archive prefix; verify with `_metadata` read + a restore probe on a throwaway MiniCluster (CHECKPOINT-DURABILITY-001 recipe).
2. **Confirm the second compose project:** `docker compose -p p10 -f docker-compose.p10.yml` (overlay built 2026-08-13, remapped ports 12181/19123/19124/18081/19249/19250); same image digests; dev secrets. Trio health: `p10-zookeeper-1`, `p10-fluss-coordinator-1`, `p10-fluss-tablet-1` Up.
3. **Copy Fluss data:** copy the live tablet data dirs + ZK metadata into the rehearsal volumes while the live writer is stopped (brief ingestion stop; restart after copy — record start/stop timestamps). Verify segment integrity (torn-tail truncation procedure if needed — `fluss-tablet-crash-loop-repair` skill pattern).
4. **Database/table setup in the rehearsal env:** create database `rehearsal`; provision `raw_table_1`, `feature_candles_15s`, `Signal_Candidates`, `Signal_Candidates_current` via the gated DDL path (identical to production: `ddl_apply.py` + version-matrix gate — no bootstrap at service startup; DdlBootstrap owns only registry tables).
5. **Load the copied history:** copy log segments into the rehearsal `raw_table_1`; lake tier attaches to the existing R2 objects (shared read-only, §2.2).
6. **Environment health gate:** coordinator/tablet healthy in the rehearsal network; O2/collector reachable (or a rehearsal-scoped metric sink); rehearsal `raw_table_1` visible with the full copied history; `Signal_Candidates` empty LOG + `Signal_Candidates_current` empty KV visible.
7. **Fast smoke gate (≤ 2 min, §4.1):** `probe-r2.sh` against the rehearsal R2 path (`lake/rehearsal/...`) PASS + 30 s bounded log read on rehearsal `raw_table_1` returning the copied count + preflight validator smoke (expected PASS on the rehearsal tables). Smoke fails → fix + re-smoke before Phase 1.

### 4.1 Long-run gate rule (user directive, 2026-08-12)

Any phase estimated > 10 min MUST be preceded by a ≤ 2-min smoke exercise of the SAME machinery that phase depends on: R2 lake read (`probe-r2.sh`) for checkpoint steps, bounded log/batch reads for scan steps, checkpoint-restore probe for state steps. Smoke passes → run the long phase; smoke fails → fix + re-smoke. No blind long waits: the P3.5 R2 saga proved a 55-90 min audit can wedge with NO error while a 1-min probe catches the same failure in seconds. The smoke result (probe log path + exit code) is recorded in the evidence file as part of the run's proof.

## 5. Phase 1 — table preflight + dual-sink from copied checkpoint (P10.1 boxes 1-4, 6-7)

0. **Smoke (§4.1):** `probe-r2.sh` PASS + the archived-checkpoint restore probe (Phase 0 step 1) re-run immediately before this phase — the checkpoint-restore machinery is what this phase exercises.
1. Run the re-targeted preflight validator against the rehearsal tables: `feature_candles_15s` KV PK exactly `(instrument_token, window_start)` (sole candle output — 2026-08-13 conversion), `Signal_Candidates` LOG (no PK), `Signal_Candidates_current` KV PK exactly `[instrument_token]`, 22 columns/type/nullability, bucket.key `instrument_token` + 16 buckets. Negative legs: wrong-kind and wrong-schema tables fail before environment creation (SIGNAL-SCHEMA-001).
2. Submit the SignalJob (application mode, rehearsal env, PARALLELISM from P7) in RESTORE mode from the ARCHIVED checkpoint copy; `allowNonRestoredState=false` (never set — STARTUP-GATE-001 contract).
3. Verify: table preflight passes (incl. the Fluss dedup state table under DEC-038); startup mode = RESTORE (no FULL_REPLAY); source/window/detection state restored (CHECKPOINT-RESTORE-002 recipe: offsets, window state) and the dedup working cache rehydrated from the Fluss dedup table (DEC-038 — no dedup restore from checkpoint); signal LOG sink appends and `Signal_Candidates_current` sink starts cleanly (first upserts from the restored detection state).
4. Verify first checkpoint meets target (30 s interval; duration recorded; R2 pins active).

## 6. Phase 2 — bounded replay twice (P10.1 boxes 8-9)

0. **Smoke (§4.1):** `probe-r2.sh` PASS + rehearsal env health before the replay runs.
1. Run a bounded source replay twice (same offsets): 2nd run must leave `Signal_Candidates_current` **byte-identical in key count** (== active instrument count) while the `Signal_Candidates` LOG gains the replayed append rows (retained as evidence — duplicates after deliberate replay are never silently deleted).
2. Verify LOG may grow and KV keys do not: run the dual-sink job forward; confirm signal LOG row count grows while `Signal_Candidates_current` key count stays frozen.

## 7. Phase 3 — rollback + re-cutover rehearsal (P10.1 box 10, B8.7-style full)

0. **Smoke (§4.1):** `probe-r2.sh` PASS + restore probe against BOTH checkpoints (pre-cutover single-LOG and dual-sink era) before the rollback direction starts.
1. **Rollback direction:** stop the dual-sink job (approved operator procedure); preserve the `Signal_Candidates` LOG and `Signal_Candidates_current` KV tables; reconstruct the single-LOG artifact (signal KV sink stripped, restore wiring kept); restore from the pre-cutover checkpoint; verify signal KV key count frozen and LOG grows; checkpoints complete <= 30 s.
2. **Re-cutover direction:** stop the single-LOG job; restore the dual-sink job from the dual-sink era's own checkpoint; verify full graph restore + `Signal_Candidates_current` sink resumes.
3. **Exposure record (P10.3 box):** capture the signal LOG + KV envelope of the dual-sink window — timestamp range, LOG rows appended, KV upserts, sampled keys (the data that a production rollback would expose as duplicates).
4. No `allowNonRestoredState=true` shortcut; no automatic full replay at any point (STARTUP-GATE-001 / P10.3 contract).

## 8. Phase 4 — evidence + runbook delivery

1. Check all re-scoped P10.1 boxes in the tracker with evidence annotations (date + artifact path, register-format fields: commit, commands, topology, volume, output, pass/fail, operator/approver line).
2. Deliver `docs/08_implementation/15-signal-log-kv-production-cutover.md` (or appendix in the tracker): the P10.2 (10 boxes) + P10.3 (8 boxes) ready-runbook — exact commands, stop/RESTORE sequence, consumer-repoint step (marked for when the downstream pipeline exists), rollback triggers, exposure-record format. Execution deferred until a real production deployment exists.
3. Register rows updated where rehearsal evidence contributes (`CHECKPOINT-RESTORE-002` gains a signal dual-sink rehearsal-env variant annotation; `SIGNAL-DUAL-SINK-001`, `SIGNAL-SCHEMA-001` where exercised).

## 9. Evidence template

Per tracker §4 fields: date; commit/image IDs; exact command or test; environment topology (isolated project, ports, db name); input volume/rate (copied row counts, offsets); output location (archive checkpoint path, rehearsal O2 queries, evidence file `logs/tracker-14/p10-rehearsal-<date>.md`); pass/fail per box; operator/approver line (user review).

## 10. Pass/fail handling

Any box failing mid-rehearsal: record the failure + root cause in the evidence file, fix (env/config/artifact), re-run the affected phase. The rehearsal env is throwaway — a failed phase never touches live data (isolation is the safety net). Production stays `BLOCKED` (tracker §6).

## 11. Cross-references

- Tracker: master `docs/08_implementation/04-signal-job.md` §Absorbed documents (2026-08-17 consolidation); tracker `14-candle-log-kv-replay-safety_2.md` (deleted 2026-08-17) P10 (RE-SCOPED), §4 register (`SIGNAL-DUAL-SINK-001`, `SIGNAL-SCHEMA-001`), §6 acceptance records live in git history.
- Requirement change + retired candle machinery: `04-signal-job.md` §Absorbed documents — retired candle era (`CANDLE [LOG + KV] RETIRED` banner) + §5.1 REQ13 traceability; tracker 14 §4 register HISTORICAL rows (git history).
- Recipe skills: `candle-kv-rollback-rehearsal` (B8.7 rollback/re-cutover pattern), `candle-failure-injection-tests` (state-restore verification patterns), `fluss-tablet-crash-loop-repair` (segment integrity).
- P7 bench: `docs/08_implementation/11-testing-and-release.md` (topology re-scope banner; sequencing gate).
- R2 fix + containment: `§P3.5 — R2 lake-read stall containment` (absorbed into `04-signal-job.md` §Absorbed documents; plan file never persisted).
- Prior Phase 0 evidence: `logs/tracker-14/p10-rehearsal-20260813.md` (divergences D1–D6; overlay compose + empty rehearsal trio; data copy never performed).
- Production target (future): `docs/08_implementation/09-production-swarm.md`, `docs/05_deployment/06-swarm-secrets.md` (P9 open review).

## 12. Execution results

- **2026-08-13 Phase 0 (isolation groundwork):** `logs/tracker-14/p10-rehearsal-20260813.md` (divergences D1–D6; overlay compose + empty rehearsal trio; data copy never performed).
- **2026-08-17 Design-B re-run (P10.1 boxes 1–10):** `logs/tracker-14/p10-rehearsal-design-b-20260817.md` — Phase 0 rebuilt on live-data byte-copy (deviations DB1–DB6), boxes 4–7 PASS (archived chk-179 strict restore → RUNNING in 13.9 s; KV frozen at 1,025 keys; LOG +42 monotone; first checkpoint 6 s ≤ 30 s target; 19 steady-state completions), boxes 8–9 (bounded replay ×2) + box 10 (rollback/re-cutover per DB2 disposition) recorded there.

## 13. P10.2 + P10.3 ready-runbook (production blue-green cutover + rollback)

**Status:** READY-RUNBOOK — delivered 2026-08-17 per the re-scoped plan §8.2. Execution is DEFERRED until a real production deployment exists (dev cluster = qualification target; the rehearsal evidence above is the qualification). Command shapes use the future Swarm stack (`<stack>` = production stack name); the rehearsed dev equivalents are noted per step. Design-B annotations (DB) reference the deviations table in the 2026-08-17 evidence file.

### 13.1 P10.2 — Production blue-green cutover (10 boxes)

Precondition: the dual-sink artifact is the ONLY Design-B artifact (DB2) — there is no pre-dual-sink signal build; "cutover" here means cutting the running job over to a restored-from-checkpoint dual-sink deployment (e.g. new image, new cluster, or post-incident recovery), not introducing the KV sink for the first time.

1. **Stop SignalJob using the approved operator procedure.**
   Swarm: `docker service scale <stack>_signaljob=0` (graceful; wait for task exit: `docker service ps <stack>_signaljob --no-trunc`). Dev-rehearsed equivalent: cancel the in-process job (`client.cancel()` — RETAIN_ON_CANCELLATION keeps the checkpoints; rehearsed runs 1–4).
2. **Record the last successful durable checkpoint.**
   Identify the max `chk-N/_metadata` under the production checkpoint prefix (Swarm: `s3a://<bucket>/<checkpoint-prefix>/<jobId>/`; dev: `ls <CHECKPOINT_DIR>/<jobId>/chk-*/_metadata`). Record `chk-N`, its completion timestamp, and the checkpoint counter. Production s3a checkpoints are self-contained (object-store handles resolve by key at restore time — the file:// byte-copy non-relocatability finding DB5 does NOT apply to s3a).
3. **Create/verify `Signal_Candidates` + `Signal_Candidates_current`.**
   Verify via the job's own fail-closed gate — `SignalJob.preflightTableContracts` runs at startup (SIGNAL-SCHEMA-001 machinery: `Signal_Candidates` LOG no-PK, `Signal_Candidates_current` KV PK `instrument_token`, 16 buckets, `bucket.key=instrument_token`). If provisioning is needed, use the gated DDL path (`ddl_apply.py` + version-matrix gate) — never bootstrap-at-startup. Rehearsed: tables verified present via preflight (DB3).
4. **Start the dual-sink SignalJob in RESTORE mode.**
   Set `STATE_RECOVERY_PATH=<archive-or-production chk-N _metadata>` and DO NOT set `ALLOW_FULL_REPLAY` (startup-mode gate fails closed on neither/both/blank — STARTUP-GATE-001). Swarm: `docker service update --env-add STATE_RECOVERY_PATH=… <stack>_signaljob` (or the deploy manifest). Dev-rehearsed equivalent: the env map in `P10RehearsalRestore` (RESTORE mode, archived chk-179).
5. **Verify table preflight and startup mode.**
   Job log must show preflight PASS and RESTORE (not FULL_REPLAY); `allowNonRestoredState` is never set anywhere in the Design-B artifact (strict state matching — a state mismatch fails the restore loudly, which is the intended behavior).
6. **Verify checkpoints.**
   First checkpoint completes ≤ 30 s (the pinned `CHECKPOINT_TIMEOUT_MS`); the checkpoint counter CONTINUES from the restored id (evidence that coordinator state restored — rehearsed: 179 → 180 in 6 s). Monitor via O2 (`flink_job_last_checkpoint_duration` / completed-checkpoint count) or the checkpoint prefix.
7. **Point current-state consumers to `Signal_Candidates_current`.**
   **VACUOUS today — no consumers exist** (locked spec §2.1). Runbook step for when the downstream pipeline lands: consumers wanting latest/active-candidate-per-instrument read the KV table (PK lookup / changelog scan); never rebuild current-state by scanning the LOG.
8. **Keep LOG consumers only where append/history semantics are intended.**
   Also VACUOUS today; the rule: the `Signal_Candidates` LOG is the append-only evidence stream (one row per fired signal, never updated) — history/audit consumers read it, current-state consumers must not.
9. **Run the bounded replay acceptance test.**
   Restore a second instance from the SAME recorded checkpoint against an isolated copy of the production data (Phase 0 isolation mechanics: second compose project / byte-copy / warehouse prefix), run bounded, assert: signal LOG appends (replay rows retained as evidence — duplicates after deliberate replay are never silently deleted) while `Signal_Candidates_current` key count stays frozen == active-instrument count. Rehearsed twice on Design-B (runs 2–3, 2026-08-17).
10. **Record final evidence.**
    Per §9 template: date, commit/image IDs, exact commands, checkpoint ids, LOG/KV counts before/after, pass/fail per box, operator/approver line. File under `logs/tracker-14/` (or the production evidence location when it exists).

### 13.2 P10.3 — Rollback (8 boxes)

**Rollback triggers** (any one): dual-sink job repeatedly fails checkpoint/restore in production; `Signal_Candidates_current` sink wedges (StallGuardedSink terminal FAILED — FAILOVER-FLUSS-001 shared-fate) and current-state consumers degrade; a production incident where the restored dual-sink state is suspect and the last-known-good pre-incident checkpoint must be re-established.

**Design-B rollback reality (DB2):** the Design-B artifact has only ever existed as dual-sink, so "reconstruct the single-LOG artifact" is VACUOUS — a production rollback restores the pre-incident checkpoint WITH THE CURRENT dual-sink artifact (checkpoint-compatible by construction: same artifact wrote it). The rehearsed rollback = restore an earlier checkpoint with the current artifact and verify clean resume + KV frozen / LOG grows. Rolling back to a hypothetical pre-dual-sink artifact is impossible (it never ran Design-B state) and must never be attempted with `allowNonRestoredState=true`.

1. **Stop the dual-sink job.** Approved operator procedure (13.1 step 1).
2. **Preserve `Signal_Candidates` LOG and `Signal_Candidates_current` KV tables.** Never drop/truncate either table during rollback — the LOG is append-only evidence (replay appends retained), the KV is last-write-wins current state. No destructive operation on either is part of any rollback path.
3. **Restore the previous application artifact/checkpoint only if compatible.** Restore the last-known-good checkpoint with the CURRENT artifact (DB2). Compatibility = the checkpoint was written by the same dual-sink topology (strict state matching verifies this at restore — a mismatch fails loudly; never force it).
4. **Do not use `allowNonRestoredState=true` as an emergency shortcut.** The Design-B artifact never sets it; if a restore fails on unmatched state, that is a real incompatibility to investigate, not a gate to bypass (STARTUP-GATE-001 / P10.3 contract).
5. **Do not automatically full replay.** `ALLOW_FULL_REPLAY=true` is an explicit, separately-approved dev-only gate — never a rollback action in production (it would re-emit the entire raw history into the signal LOG).
6. **Repoint consumers if necessary.** VACUOUS today (no consumers); when they exist: current-state consumers stay on `Signal_Candidates_current` (the KV resumes from the restored checkpoint); no repoint is needed for a same-artifact rollback.
7. **Record the affected interval and duplicate exposure.** Exposure record format: (a) interval = [last-good checkpoint completion ts → rollback-restore ts]; (b) LOG rows appended during the interval (end-offset sum delta on `Signal_Candidates`); (c) KV upserts during the interval (keys touched); (d) sampled keys (≥10 instrument tokens with their LOG row offsets). This is the data a production rollback exposes as possible duplicates downstream.
8. **Define remediation before resuming production.** Downstream consumers of the LOG must be told the exposure record (duplicates in the interval are possible and retained); current-state consumers need no remediation (KV last-write-wins converges). Resume only after the restored job checkpoints cleanly (≤ 30 s) and the exposure record is filed.

### 13.3 Rehearsal qualification

Every machinery step above was rehearsed on the Design-B topology against the isolated p10 environment (2026-08-17): strict archived-checkpoint restore (boxes 4–7), bounded-replay idempotency twice (boxes 8–9: LOG re-appends retained, KV key count frozen at the active-instrument count), and rollback + re-cutover with the current artifact (box 10, DB2 disposition). Evidence: `logs/tracker-14/p10-rehearsal-design-b-20260817.md`. The two VACUOUS steps (consumer repoint, single-LOG artifact) are recorded as such in the locked spec §2.1 + DB2 and become live steps only when their preconditions exist.

## Extended reliability test suite — production Swarm (beyond the 7 canonical `SWARM-*`)

> **Purpose 2026-08-21:** the canonical 7 Swarm tests (`SWARM-INT-001/002`, `SWARM-FAIL-001/002`,
> `SWARM-REC-001`, `PERF-NODELOSS-001`, `SEC-NET-001..AUDIT-001`) prove the explicit
> `REQ-PF-001..012 → AC-PF-001..019` requirements, but not the hidden invariants (fencing,
> crash-window, partition, durability, overload). This layered suite defines the **system-
> design-oriented contract before Swarm implementation** so the same behavioral tests can run
> against the 4-VM production topology. Implement per the `P0→P4` release gate in §20 below.

### Test taxonomy (same harness can run on Compose and Swarm)

```text
tests/
├── config/ · deployment/ · topology/ · health/ · readiness/ · network/ · security/ · identity/
├── correctness/ · schema/ · streaming/ · flink/ · fluss/ · execution/ · fencing/
├── failure/ · network_partition/ · crash_window/ · durability/ · recovery/ · disaster_recovery/
├── scalability/ · overload/ · resource_exhaustion/ · retry_storm/ · capacity/
├── observability/ · time/ · eod/
├── upgrade/ · rollback/ · gate/ · chaos/
└── invariants/
```

---

### Offline unit-test coverage (04_executor crate — implemented 2026-08-21)

The extended suite below is deliberately system/VM-level, but several invariants are
already **provable offline in the nautilus executor's Rust unit tests** (no rig). These
are the FENCE/TIME/CORR cases closed at the unit level; the row-level/stack cases
(FENCE across processes, TIME with real NTP/clock drift) remain M3/rig evidence.

| Doc ID | Requirement | Where implemented | Mechanism |
| :-- | :-- | :-- | :-- |
| `INVARIANT-002` / §2 FENCE | at most one active owner per partition; a fenced/stale owner must not emit | `executiongate.rs` unit tests | a command whose `gate_epoch` or `fence_token` ≠ the durable `GateRow` returns `Blocked` with **zero bridge calls** |
| `CORR-008` | no phantom state — a disabled gate cannot produce a successful order | `disabled_gate_blocks_with_zero_calls` | durable row `Halted` + matching identity still ⇒ `Blocked`, zero calls |
| `CORR-009` / `CORR-010` | no impossible transition; lifecycle never moves backward | `gate.rs` unit tests (pre-existing) | only `HALTED→RECONCILING→APPROVAL_PENDING→ENABLED` is sanctioned; `safety_halt` is the single backward path |
| `CORR-001` / `CORR-003` | event uniqueness; idempotent replay / crash-window exactly-once | `executiongate.rs` crash tests (pre-existing) | same `(instruction_id, request_hash)` ⇒ `Duplicate`, zero calls; each crash window ⇒ exactly one call |
| `TIME-004` / `TIME-005` | clock skew boundary — latency never treated as negative | `gateway_protocol.rs` `deadline_boundary_at_now_is_valid_not_expired` | `deadline == now` is accepted (strict `<`); `now+1` expires. UTC epoch is the monotonic reference |
| `INVARIANT-003` / §12 `CONTROL-001/002/006` | no ENABLED gate without an authenticated single-operator (saurabh, DEC-044) approval; mismatched epoch/hash, unauthorized identity, stale approval, wrong identity, rollback all rejected | `gate.rs` (production change 2026-08-21b; reworked to DEC-044 single-operator 2026-08-21) | `transition(Enabled)` is removed from the sanctioned path — `Enabled` is reachable **only** via `Gate::enable`, which requires a declared epoch matching the caller's, plus one approval from the **authorised** operator (saurabh, DEC-044) bound to the **evidence** hash. A second approval is not required and not checked. `safety_halt` clears the approval + evidence (fail-closed re-approve) |

New tests added 2026-08-21b (offline batch 2): `stale_fence_token_never_issues_a_bridge_call`,
`stale_gate_epoch_never_issues_a_bridge_call`, `disabled_gate_blocks_with_zero_calls`,
`matching_fence_and_epoch_emits_exactly_one_call`, `deadline_boundary_at_now_is_valid_not_expired`,
`broker_unknown_halts_reconcile_and_never_retries`, `corr015_restart_after_durable_accepted_matches_uninterrupted`,
`fence009_epoch_monotonicity_new_supersedes_old_never`, `fence012_ownership_and_fence_token_survive_restart`,
and the projection validation matrix (`version_gate_conflict_…`, `fill_event_validate_rejects_negative_and_empty`,
`position_snapshot_validate_rejects_cross_and_negative_quantity`, `version_gate_evaluate_full_matrix`,
`lifecycle_derive_and_transition_matrix`, `projection_apply_reapply_is_idempotent_snapshot_unchanged`).
Covers additional doc IDs: `CORR-002/003/004/011/015`, `CRASH-ORDER-005/009`, `FENCE-009/012`,
`STATE-*` (full version-gate + lifecycle + quantity-rejection matrix), `INVARIANT-*`.

**Item 5 — single-operator + evidence-hash gate (`gate.rs`, production-code change 2026-08-21b; reworked to DEC-044 single-operator 2026-08-21):**
the final `APPROVAL_PENDING -> ENABLED` hop was removed from `Gate::transition`, making
`INVARIANT-003` **structural** (no bypass). `Enabled` is reachable only through the new
guarded `Gate::enable(epoch)`, enforced by `set_epoch` + `record_approval` + `add_authorized`;
`enable` returns `EnableError` (`EpochUnset`/`EpochMismatch`/`RequiresApproval`/`EvidenceMissing`)
and `record_approval` returns `ApprovalError` (`Unauthorized`/`NotApprovalPending`). DEC-044
(2026-08-21): a single approval by the authorized operator (saurabh) is sufficient and binds the
evidence hash; a second approval is not required and not checked. Tests: `invariant003_no_enabled_without_approval`,
`control001_unauthorized_operator_cannot_approve_or_enable`,
`dec044_second_approval_is_not_required_and_not_checked`,
`control002_enable_requires_declared_epoch`, `control002_mismatched_epoch_cannot_enable`,
`control006_safety_halt_invalidates_approvals_require_reapproval`,
`control002_operator_session_restart_requires_reapproval`, plus the rewritten sanctioned-path,
halt, and no-skip/regress tests. The two production-caller test helpers
(`execution/client.rs` roundtrip health test and `shutdown.rs` `enable()`) were migrated to the
single-operator (saurabh, DEC-044) flow.

Gate: `cargo test --lib` = **196 passed, 0 failed, 0 warnings (CHG-102 re-count; was 125; all binaries + `tests/differential_parity` + `tests/live_go_bridge` green on full `cargo test`)**.

**2026-08-21b — buckets A1 + A2 + B (implemented offline, no VMs):**

| Doc ID | Requirement | Where | Mechanism |
| :-- | :-- | :-- | :-- |
| `CRASH-ORDER-010` | reconciliation discovers already-accepted order, no duplicate | `executiongate.rs` `crash_order010_reconcile_discovers_accepted_no_new_call` | durable `Accepted` attempt resolves to `Accepted` with **zero** bridge calls |
| `FENCE-010` | broker call during ownership loss is fenced | `fence010_broker_call_during_ownership_loss_is_fenced` | ownership promoted to new epoch ⇒ stale-epoch call `Blocked`, zero calls |
| `FENCE-011` | late broker response cannot corrupt new owner | `projection/mod.rs` `fence011_late_duplicate_response_cannot_corrupt_state` | duplicate / stale re-delivery folds, never double-counts |
| `CORR-012` / `INVARIANT-010` | every money-moving action has a correlation id | `corr012_identical_request_hash_distinct_instructions_stay_correlated` | identical hash on two instructions = two correlated actions, not collapsed |
| `CORR-013` | Nautilus stays authoritative; projection can't silently become authority | `corr013_projection_is_pure_function_of_authority_events` | regression/duplicate never overwrite; size always derived (`open − closed`) |
| `CORR-014` | schema compatibility policy | `corr014_undefined_schema_version_is_rejected` (+ guard added to `PositionSnapshot.validate`) | undefined `schema_version` rejected, declared version accepted |
| `TIME-008` | ser/deser roundtrip fidelity | `gateway_protocol.rs` `time008_envelope_serialize_deserialize_roundtrips_exactly` | encode→verify restores every field + payload byte-identical |
| `OBS-*` | monotonic counters / timestamp correctness | `telemetry.rs` `obs_monotonic_counters_never_decrease` | exported snapshot never decreases per counter / in total |
| `DR-007` / `EOD-002` | replay twice / repeat offload ⇒ identical state | `projection/mod.rs` `dr007_replay_twice_produces_identical_state` | idempotent, no drift |
| `DR-008` / `EOD-003` | interrupted rebuild resumes to uninterrupted state | `dr008_interrupted_rebuild_resumes_to_same_state` | two-chunk rebuild == one-shot |
| `NET-PART` (offline semantics) | dependency outage ⇒ halt, once-only, no duplicate on recovery | `executiongate.rs` `netpart_dependency_outage_halts_and_never_duplicates` | outage→`UnknownHalted`; recovery never auto-retries — exactly one money-moving call |
| `RESILIENCE-002/003/004/005/007` | retry/backoff/circuit/idempotency | **`resilience.rs`** (`Backoff`, `RetryBudget`, `CircuitBreaker`, `IdempotencyGuard`, `RetryOrchestrator`) — **wired onto the live bridge transport** (`BridgeExecutionClient::execute_job` via `execute_async`) | exponential backoff capped; hard retry budget; breaker open→half-open probe→close; only `send_command` transport `Err` is retried (a broker envelope is terminal — UNKNOWN still fails closed, never auto-retried); retries observed via `bridge_transport_retries` |

---

### Bucket B — dependency-failure matrix coverage (2026-08-21b)

> **Wiring boundary (2026-08-21c):** the live binary's only outbound dependency call is the
> bridge (`send_command`), which is resilience-wired in the executor. Fluss, S3, the durable
> Broker-KV (`AttemptStore`) and the observability sink have **no in-process call site** in
> `04_executor` — they are separate cluster components exercised at the M3 rig. Resilience for
> them therefore applies at the cluster/rig boundary, not as executor code. Any future Rust
> client for these MUST be wrapped with `RetryOrchestrator::execute_async`/`execute` (the proven
> pattern), classifying only clearly-transient transport `Err`s as retryable and never
> auto-retrying an ambiguous/terminal outcome.

The §11 matrix rows split into **now-verifiable offline** (logic in the Rust authority /
resilience component) vs **rig evidence** (needs real cluster fault injection). The
`docker-stack.yml` digest-pinning / `failure_action: rollback` covers the deployment-dependency rows offline (`test_09`).

| Dependency failure | §11 behavior | Offline now? | Where it is proven |
| :-- | :-- | :-- | :-- |
| Executor failure | fenced takeover | ✅ | `FENCE-010/012`, `fence009_epoch_monotonicity…`, `fence012_ownership…` |
| Broker timeout | reconcile / UNKNOWN | ✅ | `broker_unknown_halts_reconcile_and_never_retries`, `CRASH-ORDER-005/009` |
| Broker UNKNOWN after accept | reconcile, never dup | ✅ | `crash_order010…`, `broker_unknown…` |
| Dependency outage (general) | halt, once-only, bounded retry | ✅ | `netpart…`, `resilience001_retry_storm_is_bounded…` |
| Dependency recovery | breaker closes, no thundering duplicate | ✅ (logic) | `resilience005_breaker_recovers…`, `resilience007_duplicate…` |
| Disk near full / resource exhaustion | stop unsafe growth | 🟡 harness now (acceptance = rig) | load/resource harness (below) |
| Fluss 1 replica / quorum | continue / degraded | ❌ rig | needs real Fluss |
| ZooKeeper 1/2 node loss | continue / control-plane halt | ❌ rig | needs real ZK quorum |
| S3 temporary / prolonged | retry / degrade | 🟡 partial (`resilience` logic) | rig for real S3 |
| Flink JM/TM failover | standby / reschedule | ❌ rig | needs Flink HA |

### Bucket B — load / scale harness (harness now, acceptance deferred to rig)

`tests/scale/run_scale_ladder.sh` (below) is the load ladder driver for §7 (`10k→…→100k/s`)
and §8/§9 (recovery-after-overload, resource-exhaustion). It drives the workload, samples
`throughput, p50/p95/p99, CPU, mem, disk, net, backpressure, checkpoint duration`.
**Credentialed acceptance** (the documented 50k/s, p99<100ms, one-VM-loss numbers) is **M3/rig
evidence** — the local single-node dev swarm can run a *sanity* ladder only, not production-
credentialed numbers. The harness is ready so the rig run is one command.


> `CLOCK_OFFSET_LIMIT_MS` is declared in the stack env but is **not yet enforced in the
> executor code** (verified: zero references in `04_executor/src`). No production logic
> exists for it yet, so no test was fabricated for it — enforcing skew beyond the
> envelope deadline is an open implementation item (not a test gap).

---

## Option A vs Option B — resilience & durability coverage status (2026-08-21d)

### Option A — implemented & verified (the executor's real outbound surface)

The live binary's only outbound dependency call is the **bridge** (`send_command`). It is now
resilience-wired: `BridgeExecutionClient::execute_job` runs every bridge round-trip through
`RetryOrchestrator::execute_async` (exponential backoff + hard retry budget + circuit breaker).
Only a clearly-transient transport `Err` is retried; a broker envelope (Success/Rejected/
**Unknown**) is terminal and fails closed — an `Unknown` outcome is **never** auto-retried
 (commits `beff8c1`, `94eb058`). Gate: **196 tests** (CHG-102 re-count 2026-08-25; earlier 126 was the 2026-08-21 measurement, before the
DEC-044 single-operator gate rework later the same day),
0 failed, 0 warnings, stable across parallel runs. Behavior proven: transient timeouts → retried then the order fills; persistent
outage → bounded exhaustion surfaces an error, never a phantom success.

### Remaining — and *why* (evidence-based, 2026-08-21d)

Fluss, S3, the durable Broker-KV and the observability sink have **no in-process call site** in
`04_executor` — they are separate cluster components (`03_fluss`, Postgres DDL, S3) exercised
at the M3 rig. Nothing below is blocked by a missing test; each is blocked by a real production
artifact that does not exist in this component yet.

| # | Remaining production gap | Why it is not done now | Blocker / gate |
| :-- | :-- | :-- | :-- |
| 1 | Fluss client + resilient append/reconcile | no Rust Fluss client; Fluss is a separate cluster the executor does not call in-process | real Fluss API + endpoint (**Option B** / rig) |
| 2 | S3 client + retry for DR/EOD offload | no `aws-sdk-s3`; offload is a projection in-memory artifact | real S3/MinIO + durable offload wiring (**Option B** / rig) |
| 3 | Durable Broker-KV `AttemptStore` impl + resilient writes | only `InMemoryAttemptStore` (test fixture); durable store is Postgres DDL (`25_trade_instruction_state`) not wired into the Rust gate | persistent `AttemptStore` impl + gate wiring (**Option B** / rig) |
| 4 | Observability export via a real sink | `TelemetrySink` default is the no-op `NullSink`; no OTLP/OpenObserve client | network sink + credentials (rig) |
| 5 | `CLOCK_OFFSET_LIMIT_MS` enforcement | `DriftMonitor` (clockwatch.rs) + `Runtime::enforce_clock_drift` existed and were unit-tested, but nothing in the live boot loop invoked them | **DONE 2026-08-25 (CHG-107):** `main.rs` now arms a periodic drift monitor (interval `CLOCK_DRIFT_CHECK_INTERVAL_S`, default 30s) with `FixedOffsetSource(0)` offline (real NTP/chrony source is a Workstream-D/prod concern behind the same `OffsetSource` trait); `|offset| > CLOCK_OFFSET_LIMIT_MS` or unmeasurable → fail-closed `safety_halt()`; recovery only via sanctioned reconcile→approval→enable. Rust 206→207 tests |
| 6 | Credentialed 50k/s, p99<100ms, one-VM-loss numbers | need a real multi-node cluster; local dev swarm is sanity-only | M3 rig |
| 7 | Real network partition, quorum loss, ZK/Fluss/Flink failover | need live multi-node cluster + genuine fault injection | M3 rig |

### Option B — build the durable outbound clients & wire them (planned, not started)

Option B is a **real architecture build** (new durable clients wired into the money-moving /
once-only path), not a small wiring item. It will be implemented only on explicit approval —
this section is the agreed plan.

**Scope:** add in-process Rust clients for the three durable outbound deps and the
observability sink, each wrapped with the proven `RetryOrchestrator` pattern, and wire them
into the gate + projection lifecycle. Guarded behind a Cargo feature so the offline slice stays
dependency-light.

**Dependencies to add (behind `prod-clients` feature):**
| Client | Dep | Reason |
| :-- | :-- | :-- |
| Durable Broker-KV (`AttemptStore`) | `tokio-postgres` (or `sqlx`) | once-only/duplicate records → `25_trade_instruction_state` |
| Fluss (journal / reconcile) | Fluss Rust/REST client | delta-log append + read for reconciliation |
| S3 (DR / EOD offload) | `aws-sdk-s3` or `rust-s3` (MinIO-compatible) | snapshot upload / replay, kept dev-swarm-testable via MinIO |
| Observability | `opentelemetry` + OTLP exporter | replace `NullSink` with a real sink |

**Wiring points:**
- **Gate / once-only:** swap `InMemoryAttemptStore` → persistent `PostgresAttemptStore` wrapped
  in a resilient decorator (`RetryOrchestrator::execute`); `put/get/has_duplicate` become
  bounded-retry + breaker-protected. Read/write classification: only transient transport/SQL
  errors are retried; a definitive duplicate/terminal is never re-issued.
- **Fluss reconcile:** new append client wired to the gate's reconcile path (resilient append;
  UNKNOWN append outcome fails closed, never auto-retried).
- **DR / EOD offload:** S3 store for snapshot upload + replay, resilient upload (temporary S3
  outage → retry; prolonged → degrade + alert, never corrupt).
- **Observability:** `ResilientSink` decorator over any `TelemetrySink`.

**Verified on the dev swarm vs deferred to rig:**
| Verify on dev swarm (local MinIO + Postgres + Fluss containers) | Deferred to M3 rig |
| :-- | :-- |
| Clients compile, connect & round-trip against local containers | Credentialed 50k/s, p99<100ms, one-VM-loss |
| Unit tests for the resilient decorators (retry/budget/breaker) offline | Real network partition / quorum loss |
| End-to-end gate→Broker-KV persistence & projection→S3 offload locally | S3 regional durability, Flink HA, ZK loss |
| Observability metrics exported end-to-end | Cluster-wide recovery-under-load |

**Staged tasks:** (1) add deps behind `prod-clients` feature + config; (2) `PostgresAttemptStore`
+ resilient decorator + tests; (3) Fluss append client + resilient reconcile + local integration;
(4) S3 offload/replay store + resilient upload + MinIO test; (5) OTLP sink + resilient export;
(6) wire behind feature flag in bootstrap; dev-swarm end-to-end; update this doc + matrix.

**Trade-off / when not to choose:** each dependency is added only because a real durable client
is genuinely required for the corresponding matrix row; if the executor is destined to stay an
offline/sandbox slice, pure Option A (bridge-only) is sufficient and Option B should not be
started.

### 1. Correctness and invariants (non-negotiable)


| ID | Test | What it proves |
| -- | ---- | -------------- |
| `CORR-001` | Event uniqueness | Same immutable event cannot be applied twice to authoritative state |
| `CORR-002` | Event ordering | Per-entity ordering rules are preserved |
| `CORR-003` | Idempotent replay | Replaying the same input produces the same authoritative state |
| `CORR-004` | Projection determinism | Rebuilding Fluss projections from audit/events produces identical state |
| `CORR-005` | LOG/KV convergence | LOG history and KV current state converge according to contract |
| `CORR-006` | Checkpoint determinism | Restore from a checkpoint produces equivalent state to uninterrupted execution |
| `CORR-007` | Recovery determinism | Repeated recovery from the same checkpoint produces equivalent state |
| `CORR-008` | No phantom state | A failed operation cannot create a successful order/position |
| `CORR-009` | No impossible transition | State machine rejects invalid lifecycle transitions |
| `CORR-010` | Monotonic lifecycle | Lifecycle never moves backward |
| `CORR-011` | Position conservation | Position changes reconcile exactly with accepted fills |
| `CORR-012` | Correlation completeness | Every money-moving action has a traceable correlation chain |
| `CORR-013` | Source-of-truth consistency | Nautilus remains authoritative; projections cannot silently become authority |
| `CORR-014` | Schema compatibility | Old state + new artifact behaves according to declared compatibility policy |

#### CORR-015 — Repeated restart equivalence

Run `load → process → checkpoint → restart` and compare against `load → process continuously` — authoritative result must be equivalent. Catches bugs ordinary restart tests miss.

---

### 2. Split-brain and fencing (`FENCE-*` — prove one active owner per `execution_partition_id`)

| ID | Test | Expected |
| -- | ---- | -------- |
| `FENCE-001` | Normal leader acquisition | One owner |
| `FENCE-002` | Graceful owner transfer | Old stops, new starts |
| `FENCE-003` | Abrupt owner death | New owner takes over |
| `FENCE-004` | Network partition | Exactly one fenced active owner |
| `FENCE-005` | Stale owner resumes | Old owner cannot place action |
| `FENCE-006` | Delayed old message | Stale epoch rejected |
| `FENCE-007` | Duplicate ownership request | One owner wins deterministically |
| `FENCE-008` | Concurrent takeover | No double ownership |
| `FENCE-009` | Epoch monotonicity | Old epoch can never supersede new epoch |
| `FENCE-010` | Broker call during ownership loss | Call is blocked/fenced |
| `FENCE-011` | Broker response arrives after fencing | Response cannot corrupt new owner's state |
| `FENCE-012` | Executor restart during active ownership | Ownership safely reconstructed |
| `FENCE-013` | ZK session expiration | Old owner becomes invalid |
| `FENCE-014` | Split network + reconnect | No duplicate execution after healing |

*Critical assertion (measured externally, not from logs):* `successful money-moving calls by active owners <= 1` during every fencing test.

---

### 3. Crash-window duplicate-order tests (`CRASH-ORDER-*`)

Failure mode:

```text
Executor → broker → broker accepted → Executor crashes → Executor doesn't know → retries
```

| ID | Failure point |
| -- | ------------- |
| `CRASH-ORDER-001` | Crash before request |
| `CRASH-ORDER-002` | Crash while request is in flight |
| `CRASH-ORDER-003` | Broker accepts, response lost |
| `CRASH-ORDER-004` | Response duplicated |
| `CRASH-ORDER-005` | Timeout followed by delayed ACK |
| `CRASH-ORDER-006` | Restart before lifecycle persistence |
| `CRASH-ORDER-007` | Restart after lifecycle persistence but before projection |
| `CRASH-ORDER-008` | Restart after projection but before acknowledgment |
| `CRASH-ORDER-009` | Broker returns UNKNOWN after acceptance |
| `CRASH-ORDER-010` | Reconciliation discovers already-accepted order |

*Hard assertion:* `one logical instruction → at most one broker order` unless the broker/API guarantees idempotency via an idempotency key.

---

### 4. Network partition tests (`NET-PART-*` — partial connectivity, not just kill)

| Test | Partition |
| ---- | --------- |
| `NET-PART-001` | Executor ↔ ZooKeeper |
| `NET-PART-002` | Executor ↔ Fluss |
| `NET-PART-003` | Executor ↔ Arrow |
| `NET-PART-004` | Executor ↔ observability |
| `NET-PART-005` | Flink ↔ Fluss |
| `NET-PART-006` | Flink ↔ ZooKeeper |
| `NET-PART-007` | Fluss ↔ S3 |
| `NET-PART-008` | JobManager ↔ TaskManager |
| `NET-PART-009` | Workload VM ↔ observability VM |
| `NET-PART-010` | One workload VM isolated from remaining cluster |

For each, define: `Can it continue? Should it degrade / halt? How quickly? What state is safe?` — makes failure semantics explicit.

---

### 5. Data-loss and durability tests (`DUR-*`)

| ID | Failure | Required proof |
| -- | ------- | -------------- |
| `DUR-001` | Fluss process crash | No committed data lost |
| `DUR-002` | Fluss tablet crash | Recovery from durable state |
| `DUR-003` | VM loss | Replica survives |
| `DUR-004` | Disk restart | Data readable |
| `DUR-005` | Corrupted tail segment | Repair/recovery behavior |
| `DUR-006` | Interrupted write | No phantom committed record |
| `DUR-007` | S3 transient failure | Retry bounded, no corruption |
| `DUR-008` | S3 prolonged failure | Safe degradation |
| `DUR-009` | S3 object missing | Explicit recovery failure, not silent success |
| `DUR-010` | Partial checkpoint upload | Incomplete checkpoint rejected |
| `DUR-011` | Checkpoint version mismatch | Restore rejected |
| `DUR-012` | Audit object corruption | Integrity detection |
| `DUR-013` | Restore from versioned object | Correct version recovered |
| `DUR-014` | Recovery after abrupt VM power loss | State remains consistent |

---

### 6. Byzantine-ish and stale-data tests (`STATE-*`)

Test stale timestamp/epoch/order-status, duplicate/out-of-order/future fills, impossible/negative quantities, unknown order/instrument, mismatched correlation/partition owner, malformed schema/missing field/invalid transition.

*Expected:* `reject` or `quarantine` or `mark UNKNOWN` — never silently interpret bad data as valid.

---

### 7. Backpressure and overload tests (`SCALE-*`)

Load ladder `10k → 20k → 30k → 40k → 50k → 60k → 75k → 100k ticks/s` — record `throughput, p50/p95/p99, CPU, memory, disk, network, Flink backpressure, checkpoint duration, Fluss write latency, queue depth, recovery time` at each step.

| ID | Test |
| -- | ---- |
| `SCALE-001` | 10k sustained |
| `SCALE-002` | 25k sustained |
| `SCALE-003` | 50k sustained |
| `SCALE-004` | 75k overload |
| `SCALE-005` | 100k overload |
| `SCALE-006` | Burst 2× baseline |
| `SCALE-007` | Burst 5× baseline |
| `SCALE-008` | Burst followed by normal rate |
| `SCALE-009` | Hot instrument distribution |
| `SCALE-010` | Highly skewed partition distribution |
| `SCALE-011` | Maximum instrument count |
| `SCALE-012` | Maximum concurrent signals |
| `SCALE-013` | Maximum execution-event rate |

*Also ask: when it cannot keep up, does it degrade predictably and recover without losing correctness?*

---

### 8. Recovery-after-overload (`REC-LOAD-001`)

```text
50k/s → temporary 100k/s → backpressure → return to 50k/s
```

Measure `recovery time`, `backlog drain rate`, `checkpoint behavior`, `p99`, `memory release`, `lag convergence`. Define `time_to_return_to_SLO`.

---

### 9. Resource-exhaustion tests

Gradually approach `70% → 80% → 85% → 90% → 95%` for memory (verify alerting/backpressure/refusal/safe shutdown), plus CPU saturation (decision latency), disk pressure (readiness degradation), network limit, file-descriptor / thread / connection-pool (Fluss/S3/broker) exhaustion. Fail with bounded explicit degradation, not cascading retries.

---

### 10. Retry-storm / cascading-failure (`RESILIENCE-*`)

`S3 failing → 100 clients retry → network saturates → more timeouts → more retries`

| ID | Test |
| -- | ---- |
| `RESILIENCE-001` | Retry storm containment |
| `RESILIENCE-002` | Exponential backoff correctness |
| `RESILIENCE-003` | Retry budget exhaustion |
| `RESILIENCE-004` | Circuit breaker opening |
| `RESILIENCE-005` | Recovery after dependency returns |
| `RESILIENCE-006` | Thundering herd after recovery |
| `RESILIENCE-007` | Duplicate retry prevention |

---

### 11. Dependency failure matrix (failure contract)

| Dependency | Failure | Expected system behavior |
| ---------- | ------- | ------------------------ |
| ZooKeeper | 1 node lost | continue |
| ZooKeeper | 2 nodes lost | control plane unavailable / safe halt |
| Fluss | 1 replica lost | continue |
| Fluss | quorum failure | defined degraded mode |
| S3 | temporary | retry |
| S3 | prolonged | checkpoint/lake degradation + alert |
| OpenObserve | down | trading remains safe; audit remains durable |
| Arrow | unavailable | execution unavailable |
| Broker | timeout | reconcile / UNKNOWN |
| Flink JM | failed | standby takeover |
| Flink TM | failed | reschedule |
| Executor | failed | fenced takeover |
| Network | partition | defined behavior |
| Disk | near full | stop unsafe growth |

---

### 12. Control-plane safety (`CONTROL-*`)

Unauthorized gate enable, approval by an unauthorized operator, mismatched epoch/hash, stale approval, wrong identity, evidence changed after approval without re-approval, deployment during active gate, rollback during active gate, operator session loss, control-plane restart.

*Hard rule:* `HALTED → ENABLED` only if `same gate epoch + same evidence hash + single-operator (saurabh, DEC-044) approval + authorized operator`.

---

### 13. Deployment / upgrade (`UPGRADE-*`)

Rolling update (one node at a time, two sequentially), image digest mismatch/incompatible, health-check failure, automatic/manual rollback, rollback after partial deployment with checkpoint from previous version, schema-compatible/incompatible, connector/Flink/Fluss upgrade. `UPGRADE-001`: deploy new version while processing `50k/s` — prove `no unsafe execution, no duplicate order, no p99 violation, state remains readable`.

---

### 14. Capacity scaling (`SCALE-TOPO-*`)

`SCALE-TOPO-001` 1 VM bootstrap, `SCALE-TOPO-002` Add VM2, `SCALE-TOPO-003` Add VM3, `SCALE-TOPO-004` ZK 1→3, `SCALE-TOPO-005` Fluss replication, `SCALE-TOPO-006` Flink ZK HA, `SCALE-TOPO-007` Rebalance, `SCALE-TOPO-008` Anti-co-location, `SCALE-TOPO-009` Capacity increase, `SCALE-TOPO-010` App behavior unchanged.

*Invariant:* scaling infra must **not require changing application semantics**.

---

### 15. Observability correctness (`OBS-CORR-*` / `OBS-VALIDATE-001`)

Beyond "telemetry exists" — prove `timestamp correctness, monotonic counters, duplicate/missing/delay/overload handling, trace correlation, clock skew, cardinality explosion`, plus `OBS-VALIDATE-001`: inject `tick → Fluss → Flink → signal → execution → fill` and verify one correlation ID reconstructs the whole path.

---

### 16. Clock and time (`TIME-*`)

`TIME-001` NTP offset, `TIME-002` 100ms skew, `TIME-003` 1s skew, `TIME-004` backward jump, `TIME-005` forward jump, `TIME-006` UTC/DST, `TIME-007` event-time vs processing-time, `TIME-008` ser/deser, `TIME-009` broker mismatch — verify latency never goes negative and UTC normalization holds.

---

### 17. Data replay and disaster recovery (`DR-001..010`)

`DR-001` Full recovery from checkpoints, `DR-002` previous checkpoint, `DR-003` S3 version, `DR-004` Rebuild projection from audit, `DR-005` Replay after Flink state loss, `DR-006` after Fluss projection loss, `DR-007` Replay twice, `DR-008` interrupted halfway, `DR-009` after schema upgrade, `DR-010` missing optional telemetry. Property: `rebuild(state) == expected_authoritative_state`.

---

### 18. EOD correctness (`EOD-001..003`)

`EOD-001` Full session → lake offload — assert partitions, row/count reconciliation, no missing/duplicate immutable events, manifest/object integrity/versioning/audit. `EOD-002` Repeat → idempotent. `EOD-003` Interrupt halfway → resume with no corruption/duplicate final objects.

---

### 19. Chaos test (`CHAOS-001`)

During `50k/s`, randomly inject one `container crash`, one `network delay`, one `S3 timeout`, one `TM failure`, one `Executor ownership transfer` (never simultaneously destroy quorum unless testing that). Measure `availability, p99, dropped/duplicate events/orders, recovery time, state divergence`.

---

### 20. Invariant checker — continuous during every major test (`INVARIANT-001..010`)

```text
INVARIANT-001 No duplicate broker order for same logical instruction
INVARIANT-002 At most one executor owner per partition
INVARIANT-003 No ENABLED gate without an authenticated single-operator (Saurabh, DEC-044) approval
INVARIANT-004 Nautilus authoritative state == expected state
INVARIANT-005 Projection state eventually converges
INVARIANT-006 No committed event disappears after recovery
INVARIANT-007 Checkpoint IDs never regress
INVARIANT-008 Lifecycle transitions are monotonic
INVARIANT-009 No stale executor can execute
INVARIANT-010 All money-moving actions have correlation IDs
```

Run **during** failure tests, not only after — major improvement over traditional suites.

---

### Release gate (proof, not test count)

| Class | Tests | Gate |
| ----- | ----- | ---- |
| P0 — Safety | `FENCE-*`, `CRASH-ORDER-*`, `CONTROL-*`, `SEC-*`, `INVARIANT-*` | No failures allowed |
| P1 — Correctness | `CORR-*`, `DUR-*`, `DR-*` | No unexplained divergence |
| P2 — HA | `SWARM-FAIL-*`, `FAIL-VM-LOSS-60000-001`, `NET-PART-*` | Must meet explicit RPO/RTO |
| P3 — Performance | `PERF-PROD-60000-001`, `PERF-NODELOSS-001`, `SCALE-*` | Must meet `50k/s, 3k instruments, p99 <100ms, one-VM loss` |
| P4 — Operational | `OBS-*`, `UPGRADE-*`, `EOD-*` | Must produce auditable evidence |

Acceptance is `Functional correctness + Safety invariants + Failure-budget/SLO evidence` — not just `SWARM-INT-001/002 pass`. The three failure classes (split-brain, crash-after-accept, stale-owner) matter more than 20 more happy-path tests.
