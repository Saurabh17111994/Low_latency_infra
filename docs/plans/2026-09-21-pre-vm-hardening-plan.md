# Pre-VM hardening plan — everything that can be done without a server

**Status:** proposed 2026-09-21. Nothing in this plan has been executed yet.

**Goal:** close every remaining unknown that does *not* need a rented server, so that VM day is
execution instead of discovery. Nothing here changes production behaviour; the production deploy
itself stays in `docs/05_deployment/PROD_VM_PROVISIONING.md`.

**Rules this plan inherits:** no gate or certification run — per-step measured evidence instead; a
commit needs your approval; code changes need a change record, docs-only changes do not; never quote
a subset run as certification.

---

## 0. The decision this plan encodes

*Your decision, 2026-09-21:* the first real deploy is **two machines** — one trading VM and one
observability VM — where the earlier plan assumed a single machine.

What follows from it, and what does not:

| Question | Answer | Why |
| --- | --- | --- |
| Swarm roles | trading = **manager**, observability = **worker** | With two managers, quorum is 2 of 2, so losing *either* machine halts the control plane — including on the trading node. One manager + one worker means losing the observability machine costs visibility only. |
| Labels | trading: `role=worker`; observability: `observability=true` only | 14 services pin `role == worker` and 3 pin `observability == true`. A node holds one value per label key, so the two live under different keys. |
| Global services | `cadvisor`, `node-exporter`, `otel-collector-logs` run on **both** machines | They are `mode: global` — three per machine by design, not a misplacement. |
| Unchanged | The whole trading facility lives on trading machines; only observability lives on the observability machine | The binding requirement from the earlier review. |
| Accepted cost | The observability machine is a single point of failure for *seeing* the system, never for trading | Recorded as a decision rather than a hope. |
| Still not used | No Kubernetes, no Ansible/Terraform, no self-hosted registry or runner, no service mesh | Your standing constraints. |

If you later prefer one machine for the first deploy, that profile already exists
(`PROD_VM_PROVISIONING.md` §1b) and phases 1–4 of this plan still apply — with both labels on the
single node instead. This plan is not a trap.

---

## 1. Order

| Phase | What | Needs | Blocks | Done when |
| --- | --- | --- | --- | --- |
| 0 | Topology decided and encoded: the two-row inventory, a placement-satisfiability check, the documents updated | nothing | phases 1–3 need a shape to aim at | with a two-row inventory, **no service is left without an eligible machine**, and both documents describe two machines |
| 1 | The published images, pulled and verified anonymously | nothing | phase 2 | 7/7 pulls succeed with no credentials and match the recorded digests |
| 2 | The production deck, run locally | phases 0–1, plus your OK to stop the local rehearsal cluster | phase 3 | the production deck runs here on the published images, and every failure is explained |
| 3 | Riders that never needed a VM | nothing hard | VM day | `chronyc` and the ingestion startup path are measured, not assumed |
| 4 | Code and hygiene from the standing list | nothing | — | each item committed with its own evidence |

Phase 0 before Phase 1 is deliberate: thirty minutes of decisions avoids buying two servers in the
wrong shape. Phases 1 and 2 are the only ones that can still uncover a **real defect**.

### Phase 0 — decide and encode the two-machine topology

*My work: one script/test change plus documentation. No server, no credentials.*

**0.1 Decide the swarm roles.** Write the recommendation from the table above into the runbook with
its reason. Acceptance: the runbook states manager/worker per machine and why, and
`prod_vms.example.json`'s `$comment` does not contradict it. Evidence: the diff and the quorum rule
quoted.

**0.2 Write the two-row inventory recipe.** Extend §1b's one-row recipe to two rows: trading =
manager carrying label `role=worker`; observability = worker carrying label `observability=true`
only. Acceptance: a copy-paste recipe that produces a valid file, sitting beside the one-row recipe,
with the warning that a node must never be labelled `role=manager`.

**0.3 Make "every service has a home" checkable — the important one.** Today only
`prod_node_check.py` and `cluster_check.py` exist, and both need a live swarm, so a mis-shaped
inventory is discovered *at deploy time* — exactly the "no suitable node" failure this project
already hit once. Add a static check that parses the stack file and the inventory (YAML and JSON
parsing, never grepping for constraints) and **names any service with no eligible node**. Read the
existing scripts first and extend the one that already owns the inventory; if a new small script is
cleaner, put it beside them and register it in the same test suite.

Acceptance: a hermetic test proves (a) the two-row inventory passes, (b) a trading node missing
`role=worker` fails with the unschedulable service named, (c) a node labelled `role=manager` fails.
Both runners must collect it (`unittest discover` and `pytest`). Evidence: output for the three
cases, plus a mutation that removes a label and is caught, then a byte-identical restore.

**0.4 Update the two documents that now describe the wrong shape.** In
`2026-09-21-ci-publish-and-single-node-rollout.md` the Stage 4 row says "one VM": add a dated note
that the first deploy is two machines and why, leaving the original row intact. In the runbook,
retitle §1b so the single-node profile reads as *the fallback* and the two-machine profile as the
first deploy. Acceptance: a reader arriving at either document reaches the same topology. Docs only
— no change record.

**0.5 `stack_selfcheck.sh` on a two-machine swarm — closed, and the premise was wrong.** This step
originally said the script refuses multi-node swarms because a cluster mode had been dropped.
Reading the script (2026-09-21) shows the opposite: `CLUSTER=1` exists (CHG-270) and is exactly the
safe mode — validation-only, never `swarm init`, never writes a label, and it verifies every
`node.labels.… == …` constraint of the rendered stack against the live nodes. What remained was
documentation rather than code, and the runbook now carries it: use `CLUSTER=1` on a cluster, and
know that the default mode's refusal is deliberate (its default mode is the single-node mimic, which
would try to turn a cluster into a one-node swarm). Evidence: the script's own header text, not a
run — this workstation is a single-node swarm, so a two-node refusal cannot be produced here without
inventing one.

### Phase 1 — the published images, proved end to end

*My work. Roughly twenty minutes plus a multi-gigabyte download.*

**1.1 Pull all seven anonymously.** There is no `~/.docker/config.json` on this workstation, so any
pull is anonymous by construction — which is precisely the VM's situation. Read the seven refs from
`code/01_platform/01_docker/images.published.env` (never retype them) and, for each, pull the full
`name:tag@sha256:…` reference. Acceptance: 7/7 succeed, and `docker buildx imagetools inspect
<name:tag>` prints the same digest the file records. Evidence: the seven `Digest:` lines beside the
seven file lines.

**1.2 Prove the images run, not merely arrive.** Start each image with its documented probe and
record what it does: a Flink or Fluss runtime image should report its version and exit; an
application image should reach its configuration check and refuse on missing credentials.
Acceptance: nothing fails for a *packaging* reason (missing jar, wrong entrypoint, wrong user) — the
only refusals are about configuration production supplies. Evidence: the command and its last ten
lines per image.

**1.3 Confirm the fragment is a deployable env fragment, not a document.** Run the documented merge
into a throwaway file: the seven `VAR=ref` lines must land with digests attached and the comment
header must survive — then run it a second time to prove idempotence. Acceptance: rc=0 twice, seven
rounded lines, header intact, no duplicated lines.

### Phase 2 — the production deck, run here

*My work. Needs your OK, because it stops the local rehearsal cluster (which is reproducible).*

**2.1 Render a real deploy environment.** From a copy of the env the runbook §6.2 defines, run the
merge from 1.3 with the real fragment, filling only what production supplies. Acceptance: the
project's pin check accepts it — no bare tags anywhere. The deck itself is the checklist for missing
variables: `docker stack config` names any that are absent.

**2.2 Resolve the deck before deploying it.** Acceptance: rc=0 and the service count matches the
deck (20 services for the production deck). Evidence: the resolved line count, as measured before.

**2.3 Prove the checkpoint guard fires.** Deploy once with a `file://` checkpoint directory and
record the refusal — the job is designed to fail fast rather than run without durable checkpoints.
Acceptance: a refusal naming the checkpoint configuration, not a silent start.

**2.4 Deploy the production deck on the single-node swarm.** Bring the rehearsal stack down first,
then deploy, watching for the first-ever start of `otel-collector-logs`. Acceptance: every service
reaches the state its configuration allows; each one that does not has its reason recorded
(credentials, R2, or a real defect). A real defect here is the entire payoff of this plan.

**2.5 Take the verdict table.** Run `prod_node_check.py` and `cluster_check.py --expect` against the
local swarm and record PASS/FAIL/WARN by name. Acceptance: the credential-bound failures are the
*only* failures; anything else is a finding.

**2.6 Record OpenObserve's disk usage after the run.** Acceptance: a measured number that replaces
the guess in the observability VM's sizing.

### Phase 3 — the riders

*My work, about half an hour.*

**3.1 `chronyc` inside the executor container.** Ask the running container's clock tool for its
status. Acceptance: the command's real output, or an explicit finding that the image has no such
tool — "unknown" is not an answer.

**3.2 The rebuilt ingestion image's startup path.** Start it and read its first log lines.
Acceptance: the configuration it reports is the configuration we set, and its refusal names the
missing feed login. **Nothing may be claimed about ingestion working** — that needs the real
credentials.

### Phase 4 — code and hygiene, in the standing order

| # | Item | Acceptance |
| --- | --- | --- |
| 4.1 | Per-node metric attribution (CHG-273) | metrics carry the node they came from; a test proves the label exists; mandatory before any four-VM attribution claim |
| 4.2 | `cargo fmt` | the pre-existing formatting failure is gone and the `make docs-audit` target can pass |
| 4.3 | The three pending `AGENTS.md` notes | each note is a lesson with its evidence, not a preference |
| 4.4 | Dev vs rehearsal OpenObserve `:5080` | both stacks can run at once, or the clash is documented with the chosen port |
| 4.5 | Rehearsal registry living in `/tmp` | decide: a durable path, or a documented statement that a reboot costs re-pushes and re-pins for rehearsal only |
| 4.6 | *Optional:* publish images by pushing a tag | a tag push publishes and records digests; needs its own change record; otherwise the manual Run-workflow click stands |

Every code item: a change record, a test that fails before and passes after, and both runners
collecting it.

---

## 2. What this plan cannot prove — and must never be quoted as proved

- ingestion actually receiving market data (needs the real feed credentials)
- durable recovery on R2 (needs the bucket and its token)
- losing a machine, quorum behaviour, RPO/RTO (two machines are not high availability)
- real capacity numbers under live load
- the runbook's nine exit criteria (they need the real machines)
- the real `O2_PASSWORD` (locally we choose our own; only production's is real)

---

## 3. Cost and disruption

- Phase 1 downloads several gigabytes; phase 2 a few more.
- Phase 2 stops the local rehearsal cluster for its duration. It is reproducible; nothing is lost.
- Nothing here touches the remote repository until you approve a commit, and nothing is pushed on
  its own.

---

## 4. Risks

| Risk | Mitigation |
| --- | --- |
| The local run drags in rehearsal assumptions (local paths, `file://` checkpoints) | One variable at a time, and every deviation from production written beside its evidence |
| A local-only failure is mistaken for a production defect | The acceptance lines separate credential refusals from real defects, and no local run is ever quoted as production evidence |
| Two machines get mistaken for high availability | Two machines are not HA; quorum and node-loss claims wait for three nodes, and this plan makes no HA claim |
| The two-row inventory passes statically while the real machines differ | The static check is a pre-deploy gate only; the live check still runs on VM day |

---

## 5. What is needed from you

1. Approval of this plan, including the two-machine decision it encodes.
2. For phase 2 only: your OK to stop the local rehearsal cluster while it runs.
3. Still standing from the earlier list: the two VMs, the R2 bucket and token, and the real
   `ARROW_*` credentials. This plan does not replace any of them.

---

## 6. What I would do first

Phase 0, steps 0.1–0.3: decide the roles, write the two-row recipe, and make "every service has a
home" checkable. It is documentation and test work, it needs nothing from you but approval, and it
is what stops VM day from being the first time anyone sees the topology.
