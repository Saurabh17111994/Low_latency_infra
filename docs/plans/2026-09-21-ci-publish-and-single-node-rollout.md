# CI image publishing and the single-node first deploy — plan (2026-09-21)

**Status:** proposed — awaiting approval. Nothing has been implemented for this plan yet;
every fact below was measured on this workstation on 2026-09-21.
**Baseline:** `ab3b1664`, tree clean, three commits unpushed (`c580f13c`, `987b8c0e`, `ab3b1664`).
**Next free change-record id:** CHG-276.
**Parent item:** `docs/08_implementation/09-production-swarm.md` build-plan item 1 — "Publish
every image and wire the lock into the deploy environment", retargeted to public GHCR by
CHG-274. This plan completes that item and reorders the rollout so the first real deploy is
**one VM**, not four.

## 1. Why this plan exists

Four problems, in the order they cost time:

1. **The publish step is a manual laptop ritual.** `image-publish.sh` is built (CHG-247) and
   rehearsed in GHCR shape (CHG-274: seven refs pushed to a local `registry:2` addressed as
   `ghcr.io/<owner>/<image>:prod@sha256:…`), but the only way to run it against real GHCR is a
   human on this laptop holding a classic PAT. That keeps the laptop in the production artifact
   path forever and adds a credential to create and rotate. Every image change becomes:
   rebuild → push → re-pin → verify, by hand, every time.
2. **The property the whole deploy rests on is unproven.** "No registry credential exists on
   any VM" (CHG-274's decision) depends on public packages pulling anonymously. Today that is a
   GitHub doc sentence, not a measurement on this system, and no image has ever been pushed to
   real GHCR.
3. **No single command builds all ten images from a clean checkout.** It is *possible* —
   `fetch-jars.sh` is tracked and hermetic (§2.1) — but the order is tribal: eight service
   images come from `make images`, the two runtime images need `make flink-image` /
   `make fluss-image` first. CI makes that order explicit, repeatable, and testable.
4. **The rollout assumed four machines before anything had run on one.** The full 19-service
   stack is functionally valid on a single node (§2.2). The first real deploy should therefore
   be one VM, watched over live sessions, and scaled only afterwards. That is a decision to
   record, not an improvisation.

## 2. Verified facts (the ground this plan stands on)

### 2.1 The build and publish path is already native and mostly complete

| Fact | Measured |
| --- | --- |
| Service images | `make images` builds **8** compose services — `compute ddl-apply eod-controller execution-bridge execution-gateway ingestion loadgen nautilus` — under content stamps, and refuses to build unstamped (`Makefile:267-271`) |
| Runtime images | Separate targets: `make flink-image` (`Makefile:422`), `make fluss-image` (`Makefile:444`), `make ddl-image` (`Makefile:408`) |
| Jar staging | `fetch-jars.sh` is **tracked in git** in both runtime dirs; downloads from `${FLUSS_MAVEN_BASE:-https://repo1.maven.org/maven2}`; every artifact carries a **SHA256 pin** verified before use and re-verified after the build (`--dest` / `--verify`) |
| Why jars are absent | `.gitignore:6` is `*.jar` — the jars are never committed, by design; the fetch is the mechanism |
| Publish | `image-publish.sh` (CHG-247) tags, pushes and digest-resolves the **seven** project images in one pass, with `--env-registry` for a push address that differs from the pull address, and `--self-check` for an offline pre-flight |
| Digests | `digest-pin.sh` resolves a manifest digest (`docker buildx imagetools inspect`, with skopeo/crane fallbacks) |
| Digest home | `runtime.lock` is tracked and already declares itself "the single source of truth for version compatibility"; it is read by `image-publish.sh`, `digest-pin.sh`, `pin-check.sh`, `stack_selfcheck.sh` and four test files |
| Existing enforcement | `make pin-check` step `[5/6]` already **rejects bare tags** in `runtime.lock`; `tests/test_14_image_publish.py` asserts the push map covers every `${X_IMAGE:?}` the stack demands, never names a stock upstream image, and that an env rewrite preserves every unrelated line |

### 2.2 The full production stack is valid on one node (measured today)

```
docker node ls        → 1 node, Leader, Ready/Active (saurabh-MS-7D90)
docker service ls     → 19 prod services, all 1/1 except:
                          prod_flink-jobmanager   1/2 (max 1 per node)
                          prod_flink-taskmanager  1/3 (max 1 per node)
                          prod_ingestion          0/1
docker node inspect   → labels {"observability":"true","role":"worker"}
flink overview        → taskmanagers 1, slots-total 8, slots-available 8
```

The two Flink shortfalls are redundancy, not capability: one taskmanager still offers **8 slots**.
`prod_ingestion 0/1` is the known vendor-credential gap (`ARROW_*`), not a topology limit.

### 2.3 The topology switch is configuration, not code

| Fact | Measured |
| --- | --- |
| The switch | `prod_vms.example.json` — top-level `$comment, access, disk_min_gb, nodes`; each node entry has `name, host, role, swarm, labels, expect_availability, disk_min_gb`. **The number of rows is the topology.** |
| Same stack either way | The identical `docker-stack.yml` runs on 1 node today and is the file the four-VM guide deploys |
| Expectations need no flag | `cluster_check.py`: exit code = number of FAILs; **FAIL** = "the cluster could satisfy this and does not. Something is broken."; **WARN** = "the current topology cannot satisfy it (three replicas, one eligible node). Not a deployment defect; a fact about how many nodes exist." |
| Labels (4-VM shape) | Runbook `PROD_VM_PROVISIONING.md:498-503`: `docker node update --label-add role=worker <node>` ×3 for VM1–VM3 ("v1: managers also run workloads"), `--label-add observability=true` ×1 for **VM4 only** |
| Trading-pinned (14) | Pin to `role == worker`: eod-scheduler, execution-bridge, execution-gateway, flink-jobmanager, flink-taskmanager, fluss-coordinator, fluss-tablet-1..3, ingestion, **nautilus**, zookeeper-1..3 |
| Observability-pinned (3) | Pin to `observability == true`: **alert-consumer, openobserve, otel-collector** |
| Per-node agents (3) | `mode: global` with no constraint — one task on **every** node: cadvisor, node-exporter, otel-collector-logs |
| Consequence for 1 VM | The single node must be a manager **and** carry **both** `role=worker` and `observability=true` — exactly what the working laptop node carries, and why every service schedules. The three global agents run there too |
| Known contradiction | `prod_vms.example.json` labels M1–M3 `{"role": "manager"}` while the runbook labels VM1–VM3 `role=worker` in v1. As written, the example leaves every workload-pinned service unschedulable |

**Correction (2026-09-21, before approval).** An earlier draft of this section listed six
services pinned to `observability == true`, including `nautilus`. That came from a grep that
matched the phrase inside **comments** (`docker-stack.yml:1018,1060,1107` each begin with `#`).
Parsing the file gives the map above: **fourteen** trading-pinned, **three** observability-pinned,
**three** global. `nautilus` pins to `role == worker` and already belongs on a trading node.

**Cross-check — "the entire trading facility on trading nodes, only observability on the
observability node" (2026-09-21).** Audited against the stack file, in both directions:

| Check | Result |
| --- | --- |
| Any trading service placed on `observability == true`? | **No.** The three observability-pinned services are `otel-collector`, `openobserve`, `alert-consumer` — none is trading |
| Any service free to float onto the observability node? | **No.** The only unconstrained services are the three `mode: global` per-host agents; every other service carries a label constraint |
| Any trading service reaching an observability endpoint? | **No.** No `role == worker` service names `openobserve`, `otel`, an OTLP port, `ZO_` or `alert-consumer` in its stack environment or healthcheck — so the observability node can stop without touching trading, which is what makes D9 visibility-only |
| …in a `command`, `entrypoint` or mounted `configs`? | **No.** Checked the same 14 services; the only config mounts among trading services are `fluss-r2-secrets-bridge` and `manifest-nse` |
| …in the image build contexts? | **No.** Nothing under `code/01_platform/01_docker/` outside the observability stack's own files references O2 or OTLP |
| Any trading service holding O2 credentials? | **No.** `O2_*` appears in exactly three services: `openobserve` (env), `otel-collector` and `otel-collector-logs` (secret `o2_auth_basic`). The trading facility does not hold the observability store's credentials |
| Can an observability service depend on trading? | **No.** None of the three has a healthcheck, so none can gate on another service (DEC-047 rule 6) |

Three tests already pin this, so it cannot regress silently: `test_workload_pinned_to_worker_label`
(`test_09_stack.py:103`) over `WORKLOAD` — the 14 names including `nautilus`;
`test_observability_pinned_to_observability_label` (`:114`) over `OBSERVABILITY` — exactly
`otel-collector, openobserve, alert-consumer`; and
`test_every_service_is_placed_by_label_except_per_host_agents` (`:123`), which requires a label
constraint on every service **except** the named `PER_HOST_AGENTS`, and requires those to be
`mode: global` with **no** constraint (`assert not cons`). No new test is needed for D7.

**One nuance, by design:** the three per-host agents (`node-exporter`, `cadvisor`,
`otel-collector-logs`) run on **every** node, trading nodes included — that is the only way the
trading nodes' own host, container and log telemetry exists. CHG-265/CHG-273 record the bug that
fixed: while those agents carried `observability == true`, on four VMs the trading nodes' metrics
came from nowhere.

**One consequence of that nuance:** because `otel-collector-logs` is per-host, its
`o2_auth_basic` secret is mounted on trading nodes too — the price of shipping each node's logs
to O2. Removing that would mean giving up per-node log shipping, so it stays.

**One caveat for the profiles:** the separation holds because the observability node carries
`observability=true` **only**. The workload constraint is a positive label match, not an exclusion,
so adding `role=worker` to that node would allow trading services to be scheduled onto it.

**Recorded decisions (2026-09-21).**

| # | Decision |
| --- | --- |
| D7 | `nautilus` runs on a trading node (`role == worker`) and never on the observability node. Verified already true — no stack change is needed; the requirement is recorded rather than left to inference |
| D8 | On four VMs the Flink shortfalls must become `jobmanager 2/2` and `taskmanager 3/3`. Once the cluster *could* satisfy them, a shortfall is a FAIL by design — that is Phase 4's acceptance |
| D9 | The observability VM is an accepted single point of failure **for visibility only**: if it dies, O2, the collectors and the alert consumer stop while trading keeps running. Accepted 2026-09-21; to be recorded in both the single-node and four-VM profiles |

### 2.4 Scaling 1 → 4 nodes needs no new tool

`vm-bootstrap.sh:11` — "no `docker swarm join` — that needs a token from VM1 and is step S5.
`--apply` prints it."; `vm-bootstrap.sh:173` — "re-run this script with `--check`, then do S5
(`docker swarm join`) with a token from VM1". No script anywhere assumes a node count: the only
quorum logic in the tree is ZooKeeper-*inside*-the-stack, and `disaster_drills.py` already carries
a single-node drill ("Single-node ZK is the whole dev quorum; production = 3-node ensemble").

### 2.5 GitHub facts this plan relies on (from GitHub's own documentation)

- Actions is free for public repositories using standard GitHub-hosted runners.
- GitHub's own image-publishing guide authenticates to GHCR with the workflow's **built-in
  `GITHUB_TOKEN`** plus `permissions: packages: write`; a classic PAT is needed only for
  *local/manual* pushes.
- In the Container registry, public packages can be pulled **anonymously**.
- New personal-account packages default to **private**, and the visibility change to public is
  **one-way**.
- GitHub Packages usage is free for public packages.

### 2.6 Not verified (honest gaps this plan must close or live with)

- No image has ever been pushed to **real** GHCR; the anonymous pull has never been performed.
- The runner's disk headroom for this build is unknown until the first run.
- Whether the repository needs a one-time "Workflow permissions: read and write" setting has not
  been checked (it can only be checked in the repository's settings UI).
- `otel-collector-logs` (`mode: global`) exists in the real stack file but is **absent from the
  generated rehearsal stack** (`~/.p6v/reh/docker-stack.rehearsal.yml`: 0 matches), so it has
  never run on this workstation; its first real run is on a VM. Confirm whether that omission is
  deliberate when writing the profiles.

## 3. Decisions (recommendations; the plan assumes these)

| # | Decision | Recommendation | Why |
| --- | --- | --- | --- |
| D1 | Who publishes | **GitHub Actions builds and publishes** with its own `GITHUB_TOKEN`; no PAT is ever created | Removes the laptop from the artifact path and removes a credential that would need rotation. The manual PAT push is retired, not kept as a parallel path |
| D2 | How digests reach the deploy | **The workflow updates `runtime.lock` and commits it; the deploy env is rendered from the lock** | `runtime.lock` is already the declared single source of truth, already read by five scripts, and `pin-check` already rejects bare tags in it. CI-built digests differ from laptop-built ones, so the lock — not a human — must carry them |
| D3 | Trigger | **Tag `v*` plus manual dispatch** — never every commit | A release is a tag; ad-hoc runs stay possible without a commit churn |
| D4 | First production deploy | **One VM**, watched over 2–3 live sessions; multi-VM is a later, configuration-only change | The stack is proven on one node (§2.2); the topology switch is the inventory (§2.3). Nothing is thrown away: adding nodes is the same stack plus `swarm join` |
| D5 | The label contradiction | **Fix `prod_vms.example.json` to match the runbook's v1 labeling** in this work | It is the exact trap that would present as "services stuck at 0/N" on provisioning day |
| D6 | The observability split | **Keep it available, not day-one work** | The labels and constraints already exist (§2.3); a second VM can be added whenever the watch shows the trading loop being disturbed. It is isolation, not HA — a different axis |

**Correction (2026-09-21, before approval).** Two mechanisms in D2/D3 changed once the workflow was
written against the scripts as they actually are. They are recorded here rather than edited into the
rows above, so the reasoning stays auditable.

- **D2's carrier is a new tracked fragment, not `runtime.lock`.** Measured: `image-publish.sh` writes
  digests only through `--write-env FILE` / `--merge-env FILE`, and it requires an **env-shaped file
  that already exists** (`FAIL: env file not found`, exit 3) whose `VAR=ref` lines it rewrites.
  `runtime.lock` has that shape, but its four local-build entries are deliberately commented out as
  retired pins (CHG-218), and `--write-env` is documented against the operator's git-ignored `.env`.
  The carrier is therefore `code/01_platform/01_docker/images.published.env`: tracked, secret-free,
  env-shaped, filled by the workflow after each publish. The deploy env is rendered from it with
  `image-publish.sh --merge-env <deploy env> < images.published.env`, **measured working before this
  approval** (rc=0; the image line lands in the copy, the tracked fragment is untouched).
  `runtime.lock` keeps its current job unchanged (base-image pins + `pin-check`), and no new
  lock-writing script is introduced. Supersedes the `runtime.lock` phrasing in this section, §4
  Phase 1 (files, step 6, acceptance), §6 verification item 1, and §9.1 stages 1–2.
- **D3's first commit is `workflow_dispatch` only; the `v*` tag trigger is a follow-up.** A
  tag-triggered run checks out a tag, so committing the fragment from there would have to push to the
  default branch — which either rewinds `main` or is rejected as a non-fast-forward — and a publish
  whose digests exist only in a run log is not durable. The tag trigger returns once the commit-back
  path has been proven by a dispatch run on the default branch.

## 4. Phases

### Phase 1 — CI builds and publishes the seven images (build-plan item 1)

**Files:** `.github/workflows/publish-images.yml` (new); `runtime.lock` (updated by the
workflow); a dated correction note in `docs/08_implementation/09-production-swarm.md` under the
registry decision table (its "Not verified" line says a real push "needs the PAT", which CI
retires); one change record (CHG-276).

**Workflow shape:**

1. `on: push: tags: ['v*']` plus `workflow_dispatch`; `permissions: contents: write, packages: write`.
2. Runner disk reclaim before building (the two runtime images plus eight service images will not
   fit the default free space).
3. `docker login ghcr.io` with `github.actor` + `secrets.GITHUB_TOKEN`.
4. Build in the order the tree requires: `make flink-image`, `make fluss-image`, `make images`.
5. `image-publish.sh --registry https://ghcr.io/<owner> --env-registry ghcr.io/<owner> --tag prod`
   (the exact flag combination to push **and** update the lock without touching any operator env
   file is confirmed by reading the script's usage before the workflow is written).
6. Commit the updated `runtime.lock`; print the seven digests into the run summary as the
   human-readable record.

**Acceptance:** the run publishes seven images; `runtime.lock` carries a `ghcr.io/...@sha256:…`
for each; an **anonymous** pull succeeds from a machine with no GitHub credentials
(`docker logout ghcr.io` then `docker manifest inspect`); `make pin-check` and
`tests/test_14_image_publish.py` stay green.

**One-time manual clicks (the only ones):** flip the seven packages to Public, and check the
repository's Actions workflow-permission setting.

### Phase 2 — the single-node profile

**Files:** `docs/05_deployment/PROD_VM_PROVISIONING.md` (new section);
`code/01_platform/04_scripts/prod_vms.example.json` (label fix + a documented one-row recipe);
one change record for the inventory change.

**Content, all measured:**

- The one VM is a manager carrying **both** `role=worker` and `observability=true` (the label
  rule that makes 19/19 schedule).
- Expected, non-defect WARNs: `flink-jobmanager 1/2`, `flink-taskmanager 1/3` ("max 1 per node"),
  and one taskmanager offering 8 slots.
- Which services land where: the 14 `role == worker` services and the 3 `observability == true`
  services all run on that one node, plus one task of each of the 3 global agents (cadvisor,
  node-exporter, otel-collector-logs).
- The labelling invariant the split depends on: the observability node carries
  `observability=true` **only**, never `role=worker` — otherwise trading services can be scheduled
  onto it (the workload constraint is a positive match, not an exclusion).
- Sizing: one larger box rather than four small ones — derived from the guide's per-node sizing
  and the measured footprint (this workstation runs all 19 services in 15.46 GB of RAM).
- What one VM **cannot** prove: quorum, node loss, failover, RPO/RTO, throughput at scale.
- **Exit criteria** for the 2–3 day watch, so the period is a test rather than a wait.
- The one-row inventory recipe (copy the example, delete the other rows) for `cluster_check.py --expect`.

**Acceptance:** `docker stack config` rc=0 with a single-node env; `cluster_check.py` against the
single-node swarm reports FAIL=0 with exactly the documented WARNs; every claim in the section is
copy-traceable to measured output.

**Correction (2026-09-21, during the stage).** The acceptance above promised FAIL=0 on the
single-node swarm. Run against a live single-node cluster it reports **7 PASS, 2 FAIL, 1 WARN**
(exit 2), and both FAILs are correct rather than defects: `replicas-complete` is
`prod_ingestion 0/1` — the known `ARROW_*` credential gap, which no single-node deck can close
without real credentials — and `no-stuck-tasks` repeats that task plus a `No such container`
history line on each Flink service, which the checker counts while its service is short of the
replicas the WARN explains. The acceptance this stage actually met is therefore: `docker stack
config` rc=0; no FAIL beyond those two; no WARN beyond the two Flink shortfalls; every claim
copy-traceable to measured output. Zero FAILs remains the criterion for a deck with real
credentials, which is VM-day, not this one. The guide's new §1b carries the measured block.

### Phase 3 — VM-1 day (needs your inputs, not code)

Provision one VM → `vm-bootstrap.sh --check` / `--apply` → `swarm init` → pull the seven images
anonymously → secrets bootstrap → `docker stack deploy` → the S-sections of the guide → watch the
loop over live sessions against Phase 2's exit criteria.

**Still blocked on you:** one VM; the R2 bucket name and its scoped token pair; real `ARROW_*`
vendor credentials.

### Phase 4 — multi-VM (deferred on purpose)

Add nodes with the same bootstrap and the native `docker swarm join` the script prints, label them
per §2.3, re-run the checker. Only then are quorum, node-loss, RPO/RTO and capacity claims
testable — and none of them may be claimed before that.

## 5. Deliberately out of scope

- Running the 51-minute gate on a runner; the gate stays on the workstation.
- Reproducible/byte-identical builds between CI and the workstation.
- Any self-hosted runner, any Kubernetes/Ansible/Terraform, any registry daemon on a VM.
- Replacing the local `registry:2` used by rehearsal — it keeps working unchanged.
- The observability VM as day-one work (D6).

## 6. Verification and evidence discipline

| Phase | Evidence |
| --- | --- |
| 1 | The workflow run itself; seven digests in `runtime.lock`; anonymous pull proof; `make pin-check` + `test_14_image_publish.py` green; `change-control` and `docs_audit` clean |
| 2 | `docker stack config` rc=0; `cluster_check.py` FAIL=0 against the single-node swarm with the one-row inventory, WARNs matching the section exactly |
| 3 | Per-service replica state, Flink job state, Fluss write/read proof, O2 receiving, EOD fire on the real clock, offload to R2 |
| 4 | Quorum, node-loss, RPO/RTO, capacity — each with its own measurement, no extrapolation from phase 3 |

## 7. Risks and honest limits

- **CI is not byte-identical to the gate-built images.** The production chain becomes "the same
  commit, built by GitHub". Closing that later means having the gate pull the published digests
  instead of building — a separate decision, not folded in here.
- **First runs will likely be red** while runner disk and caching are tuned. Free for public
  repositories, so the cost is time, not money.
- **Base-image pulls on shared runners** are subject to Docker Hub anonymous rate limits; if that
  bites, the mitigation is mirroring base images, not a credential on a VM.
- **Upload volume**: the two runtime images are large; a publish takes tens of minutes.
- **Fallback if CI fights us:** the manual one-time push against GHCR keeps VM-1 day unblocked.
  That fallback exists so a CI problem cannot become a schedule problem — it is not the target state.

## 8. What is needed from you

1. Approval of this plan (and of D1–D9 if you disagree with any).
2. A push, so the workflow file exists on the default branch (it rides with the three commits
   already waiting).
3. The two one-time GitHub clicks in Phase 1 (package visibility; workflow permissions).
4. For Phase 3 only: one VM, the R2 bucket + token pair, and real `ARROW_*` credentials.

## 9. Order of execution (including items outside this plan)

### 9.1 Critical path to a live single VM

| Stage | Work | Owner | Blocked by | Acceptance |
| --- | --- | --- | --- | --- |
| 0 | Land the waiting work: commit this plan (docs-only, no change record) and push the three commits already on the local tip | me; push on your word | your approval | tree clean, remote matches local |
| 1 | CI builds and publishes the seven images; flip package visibility; prove an unauthenticated pull (CHG-276) | me; your push + 2 clicks | stage 0 | seven `ghcr.io/…@sha256:` digests in `runtime.lock`; anonymous `docker manifest inspect` succeeds |
| 2 | Digest delivery: the deploy environment is rendered from `runtime.lock`, with no hand-copied digests | me | stage 1 | a fresh deploy env can be produced from the lock in one command |
| 3 | Single-node profile, inventory label fix, the labelling caveat, expected WARNs, exit criteria (CHG-277); confirm the `otel-collector-logs` rehearsal omission | me | nothing — docs/config only, runs independently of stages 1–2 | `docker stack config` rc=0 with a one-row env; `cluster_check.py` FAIL=0 against the single-node swarm with exactly the documented WARNs |
| 4 | VM-1 day: provision, bootstrap, anonymous pull, secrets, deploy, then watch 2–3 live sessions against stage 3's exit criteria | you provision; we run it together | one VM, the R2 bucket + token pair, real `ARROW_*` | every exit criterion measured and recorded |
| 5 | Multi-VM day: three more nodes, `swarm join`, labels, then quorum / node loss / RPO-RTO / capacity — each measured, none extrapolated | later | 2–3 live sessions on stage 4 | D8's `2/2` and `3/3`; no HA claim before its own measurement |

### 9.2 Other pending items, in the order I would take them

**Note (2026-09-21, during Stage 1).** The single-node profile listed in the stages above is
**CHG-278**, not CHG-277: the first CI run failed on a locale-dependent compatibility-jar pin, and
that fix took CHG-277 (`docs/05_deployment/change-records/CHG-277.md`). Only the plan's numbering
moved; its content did not.

None of these blocks the critical path.

| # | Item | Why here |
| --- | --- | --- |
| 1 | `style(executor): cargo fmt` | Cheap, and the pre-existing `cargo fmt` failure is what stops the `make docs-audit` target from passing |
| 2 | The test-count discrepancy (1797 vs 1783) | Settle which count is correct and record it, so the number stops drifting |
| 3 | The three pending `AGENTS.md` notes | Small, and they encode lessons that would otherwise be re-learned |
| 4 | Rehearsal registry persistence (storage lives under `/tmp`) | Rehearsal-only now: production moves to GHCR, so a reboot costs re-pushes and re-pins for rehearsal, not for production |
| 5 | Dev O2 vs rehearsal O2 `:5080` conflict | Small hygiene; it only bites when both stacks are up |
| 6 | CHG-273 per-node attribution of the file-collector metrics | Must be fixed before any four-VM attribution claim is made |
| 7 | VM-day verification riders — real `O2_PASSWORD`, in-container `chronyc`, the rebuilt ingestion image's first run | They need a real VM, so they ride stage 4 instead of blocking it |
