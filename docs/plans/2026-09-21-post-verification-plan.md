# Post-verification plan — from a verified two-machine deployment to the first live order

**Created:** 2026-09-21 · **Status:** proposed, not started · **Owner:** operator (workstation, CloudPe panel, broker portal, Cloudflare)
**Predecessor:** `../05_deployment/PROD_VM_PROVISIONING.md` — this plan starts where its stages stop.

## Overview

**Requested outcome.** Once the two-machine deployment passes every verification stage (S2–S11) in the
provisioning guide, make it safe enough to place a first live order: a read of a VM must not be able to
move money or cause unbounded loss, every credential must be rotatable in minutes, a suspected
compromise must be recoverable by rebuilding, and the operator must know what to do on the day it goes
wrong. Two further requirements are first-class here: the provider threat is met by containment rather
than prevention (see the threat model below), and the facility must reach an HA topology without a
redesign.

**Acceptance criteria** — all eight hold before the first live order:

1. Broker-side limits verified by *attempting* each forbidden action (withdrawal, oversized order, order
   entry from the market-data `app_id`, login from a non-whitelisted address) and seeing it refused.
2. Rotation executed once, dated, with the next date recorded.
3. The security alerts in the observability catalogue fire against a live source and land in `alert-store`.
4. A backup restored from R2 with a checksum match, and a rollback exercised once.
5. One-VM-loss drill run on the real quorum with measured RPO/RTO recorded.
6. The executor clock source is live (`CLOCK_OFFSET_SOURCE=chronyc`), or the first order is explicitly deferred.
7. An incident-response runbook exists and a fresh reader can execute it.
8. The HA migration is proven to be a row-count change: the four-row v1 inventory passes
   `placement_check.py` (rc=0, every service has a home, three eligible nodes for every `role == worker`
   service) — measured 2026-09-21, see "The path from 1+1 to HA" below.

**Non-goals.** A vault/secret-broker on the workstation; LUKS on the VMs; `fail2ban`; authenticated NTP;
Kubernetes/Ansible/Terraform; v2 (7 drained managers — trigger-gated, not planned here).

## Context

- Stages, evidence rows, exit criteria: `../05_deployment/PROD_VM_PROVISIONING.md` §9 and §10.
- Already-decided security baseline: the same guide §9 (broker limits, CloudPe security group,
  `--autolock`, and the "deliberately not done" list — including that none of it stops a hypervisor
  memory read).
- Rotation procedure and credential classes: `../05_deployment/04-secrets-rotation.md`.
- Alert contract and its security category: `../08_implementation/10-observability.md` §Alert contract,
  delivered durably by the `alert-consumer` service.
- Rollback: `../05_deployment/03-rollback.md`. Swarm secrets: `../05_deployment/06-swarm-secrets.md`.
- Deferred mechanics the guide tracks: §6.1 rows 2–3 (executor clock source, EOD lake offload).
- Two-machine inventory (placeholders, shape already proven offline 2026-09-21 with
  `placement_check.py` rc=0): `~/.p6v/p2/prod_vms.2row.json`.

## Review handoff

- **Approach:** keep every control that works *outside* the VM — broker, provider panel, object store,
  workstation — and treat the VM as untrusted by design. No new software: the plan arms facilities the
  repository already has.
- **Assumptions:** the provider's hypervisor can read the VM at any time; the broker enforces its limits
  server-side; the workstation is the most valuable machine in the system.
- **Unresolved decisions:** `EOD_TABLES` production list; R2 short-lived vs rotated static credentials;
  chrony socket ownership inside the container; durable rehearsal registry (keep or drop); a
  `.gitignore` line for `prod_vms.json`.
- **Authorization:** plan only — no gate or certification runs. Phase B/C/D/E are executed on an explicit
  go-ahead; code changes need a change record, docs-only changes do not.

## Security requirements and the threat model

**The security goal, in one sentence.** Even if the provider — or anyone else — can read a VM, they must
not be able to move money, exceed the operator's caps, reach the workstation, or make a rebuild
impossible; whatever they can read must be revocable, and every revocation must be discoverable by a
scheduled rotation.

### The provider threat (CloudPe), stated plainly

A VPS provider owns the hypervisor, so the provider can read a VM's disk, snapshot its RAM, watch the
panel console and modify its files. No guest-side measure changes that: LUKS protects disk copies only
and needs a human at every boot; `--autolock` protects the raft log until the node is running; and
confidential computing (AMD SEV-SNP / Intel TDX) — the only mechanism that blinds a hypervisor to guest
RAM — is not confirmed for this provider. **The design answer is containment, not prevention**, and the
containment lives outside the VM (broker, provider panel, object store, workstation) — exactly what
Phase B implements.

### Other cyber threats, and what closes each

| Threat | What stops it | Task | Residual risk |
| --- | --- | --- | --- |
| Provider reads disk or RAM | Nothing prevents it; caps, scoping, rotation and rebuildability contain it | B1, B3, B5, B7 | They see everything currently live |
| Provider insider abuses the VM (spam, mining, an attack source) | No public services, custom security group, abuse alerts | B2, B6 | Proving it needs their logs |
| Broker credential theft | Withdrawal off, order caps, IP whitelist, market-data `app_id` without order rights | B1, B5 | Capped trading until it is noticed |
| Account takeover (GitHub / CloudPe / Cloudflare / broker) | 2FA everywhere, no PAT anywhere by design, billing alerts | B2, B4 | Only what 2FA itself fails to cover |
| Workstation compromise or theft | Encryption, dedicated key, no sync folder, rotate on suspicion | B4, B5 | The PC holds plaintext by design |
| Network scanning or brute force | Security group (SSH from one address), keys only, password auth off | B2 | Your address is the only path in |
| Ransomware or deletion of backups | Bucket versioning + retention, offsite by construction | B3 | Cost and lifecycle mistakes |
| Image supply chain (mutable tag, poisoned image) | Digest-pinned everywhere; anonymous pull verified per release | guide §6.1 | Upstream compromise of a digest already pulled |
| Data in transit | TLS to broker/R2/O2, mTLS inside the swarm | guide §9 | None material |
| Operator error (wrong label, wrong value, accidental delete) | `placement_check`, `prod_node_check`, `stack_selfcheck` refusals; dry runs; rollback | B7, D2 | The human |
| Silent read (no detection possible) | Alerts plus rotation on a calendar — rotation is the only real answer | B5, B6, D3 | A patient reader stays invisible |

## The path from 1+1 to HA — a requirement, not a later idea

The two-machine shape is the v1 deck with two rows removed, and **the number of rows in `prod_vms.json`
IS the topology** (guide §1b) — so growth is a configuration change, never a redesign.

| Level | Machines | Quorum | What it buys |
| --- | --- | --- | --- |
| 1+1 (today) | trading (manager, `role=worker`) + observability (worker, `observability=true`) | 1 of 1 — losing trading stops the control plane | The split is proven; no HA |
| v1 (Phase E) | M1 M2 M3 (manager + `role=worker`) + O1 (`observability=true`) | 2 of 3 | Manager loss survivable; workloads spread over three nodes; `max_replicas_per_node: 1` becomes satisfiable |
| v2 | 7 VMs with drained managers | 2 of 3, dedicated control plane | Only on the recorded trigger: `N>6` workers, sustained CPU >80%, or Raft election flaps |

**Proven offline, 2026-09-21.** The four-row v1 inventory passes the same gate the two-row shape did:
`placement_check.py --stack code/01_platform/01_docker/docker-stack.yml --inventory ~/.p6v/p2/prod_vms.v1.json`
→ **rc=0, "every service has a home"**, and every `role == worker` service reports **3 eligible nodes
(M1, M2, M3)** — the exact condition the guide names for clearing the `max_replicas_per_node: 1` WARN —
while the observability services stay pinned to O1 alone.

**What must stay true so HA works without weakening security:**

1. **Secrets live on every manager.** Swarm keeps secrets in the raft log, so each added manager is
   another place a provider read can find them. The containment model does not change, but the rotation
   cadence matters more, not less.
2. **The broker IP whitelist must cover every node that can run the executor** — all workload VM
   addresses, or one stable egress address if the provider offers it. Growing the cluster without
   revisiting the whitelist silently breaks broker login.
3. **Observability stays a single point for visibility only** (accepted in the guide) until a second
   observability VM exists; losing it never touches trading.
4. **`--autolock` needs the unlock key at every manager reboot** — the operator must hold it, and losing
   it loses the raft log.
5. **Growth stays config-only**: rows + `docker swarm join` + labels; no stack redesign, no new images.

## Phase A — prerequisites (tracked elsewhere, not re-planned)

| Item | Tracked in | Gates |
| --- | --- | --- |
| Two VMs (trading = manager + `role=worker`; observability = worker + `observability=true`), 8 vCPU / 32 GB / 250 GB | todo #61, guide §1b/§5.1 | everything below |
| R2 bucket + scoped token pair | todo #62 | S7 (`CHECKPOINT_DIR`), task B3, C2, D1 |
| Real `ARROW_*` credentials (5 values) | todo #63 | S8 readiness, S9 smoke |
| VM day: guide stages S2–S11 with per-stage evidence | guide §9/§10 | the start of this plan |

## Phase B — safety that must be true before a first live order

### Task B1 — Broker-side limits, verified by attempting to break them
**Why:** this is the only control that caps the loss when a VM is read; everything else is convenience.
**Files:** none — broker portal settings, recorded in the guide §9 item 1.
**Depends on:** VM day complete (the trading VM's IP must exist for the whitelist).

- [ ] Disable withdrawal on the trading login; set the per-day order cap and maximum order size.
- [ ] Register a separate market-data `app_id` for ingestion with no order rights.
- [ ] Whitelist the login IP, naming the scope explicitly: **every node that can run the executor**
      (today one, after the HA migration all workload VMs) or one stable egress address. Record the scope
      so the growth path cannot silently break logins.
- [ ] Attempt each forbidden action and record the refusal: withdrawal, oversized order, order from the
      market-data `app_id`, login from another address.
- [ ] Evidence: four dated refusal notes (screenshot or broker message) stored with the VM-day evidence.

### Task B2 — Provider-side access, verified
**Why:** the panel and the security group are what keep the VMs off the open internet; both are one-time.
**Files:** none — CloudPe panel. Criteria are the guide §9 item 2.
**Depends on:** VMs created (Phase A).

- [ ] 2FA on the CloudPe panel; unique password; billing alert on any new VM.
- [ ] Custom security group attached to both VMs: SSH from the workstation IP only, Swarm ports between
      the VM IPs only, `5080/tcp` from the workstation IP only, everything else denied; IPv6 off.
- [ ] Evidence: an external port scan from outside shows only SSH and the dashboard.

### Task B3 — Object-store hygiene
**Why:** the checkpoint/lake bucket is the recovery path; a leaked key must not be able to erase history.
**Files:** Cloudflare R2 settings; the two Swarm secrets `aws_access_key_id` / `aws_secret_access_key`.
**Depends on:** R2 purchase (Phase A).

- [ ] Bucket-scoped token only (no account-wide token); note its expiry and scope.
- [ ] Object versioning + a retention rule on the bucket; verify by deleting a test object and restoring its version.
- [ ] Decide the open question: short-lived credentials, or a rotated static pair. Today the deck stores a
      static pair while the guide §9 item 3 says "temporary credentials" — one of the two must change.
- [ ] Record the read/write scope and the rotation date in the rotation log (task B5).

### Task B4 — Workstation hygiene (the machine every other control trusts)
**Why:** today the whole facility trusts this PC, which holds every plaintext secret.
**Files:** `~/.ssh/`, the git-ignored deploy env and `secrets.env` (no file contents committed).
**Depends on:** none.

- [ ] Dedicated SSH key for the VMs, passphrase-protected, `ForwardAgent no`; the personal key is never on a VM.
- [ ] Confirm whether the disk (or at least `$HOME`) is encrypted; if not, treat that as the highest-value fix.
- [ ] Confirm the secret files are outside every cloud-sync/backup folder, and stay `chmod 600`.
- [ ] 2FA (hardware key where possible) on GitHub, CloudPe, Cloudflare and the broker account.

### Task B5 — One rotation, executed and dated
**Why:** rotation is the only answer to a read you cannot detect; a procedure without a date never runs.
**Files:** `../05_deployment/04-secrets-rotation.md` (procedure); the deploy env on the workstation.
**Depends on:** B1–B4 in place.

- [ ] Rotate the five ARROW values + the R2 token following the documented procedure.
- [ ] Confirm every service is healthy afterwards (`docker service ls` all converged; readiness unchanged).
- [ ] Record the date and the next date (quarterly) in the rotation log.

### Task B6 — Arm the security alerts
**Why:** the alert catalogue already defines security alerts; nothing is watching until they are wired.
**Files:** `../08_implementation/10-observability.md` §Alert contract (source of definitions); O2 alert rules.
**Depends on:** VM day complete (O2 running).

- [ ] Wire the security-category alerts that have a live source (credential expiry/revocation, secret
      exposure/redaction, TLS failure, authentication exhaustion).
- [ ] Prove delivery: force one alert and show it in `alert-store`.
- [ ] Add the three infrastructure-abuse alerts: unexpected outbound, sustained CPU anomaly, container restart.

### Task B7 — Incident-response runbook
**Why:** the day something looks wrong is the wrong day to design the response.
**Files:** new `../05_deployment/08-incident-response.md` (docs-only, no change record needed).
**Depends on:** B1–B6 for the exact commands and dates.

- [ ] One page: *suspect a read → rotate broker + R2 + O2 → revoke panel sessions → review broker order
      history → rebuild the VMs from the clone* — with the rebuild sequence
      (`git clone` → `vm-bootstrap.sh` → `secrets-bootstrap.sh` → deploy) written out.
- [ ] State what is *not* recoverable without backups (volumes) and where they live (R2).
- [ ] Verification: a fresh reader executes the page against a non-production VM or dry-reads it and can name each command.

## Phase C — deferred engineering (each needs a change record)

### Task C1 — Executor clock source (unblocks the first live order)
**Why:** the guide's §6.1 row 2 records this as blocking the first live order; today it ships fail-closed.
**Files:** `code/02_services/04_executor/Dockerfile` (runtime stage), `code/01_platform/01_docker/docker-stack.yml`
(executor service: chrony socket bind-mount + `CLOCK_OFFSET_SOURCE`), the deploy env, executor tests.
**Depends on:** VM day complete (the host's socket mode/ownership is the fact that only exists there).

- [ ] Prove the mechanism locally first: a `chronyd` container plus the pinned executor image, with
      `chronyc tracking` parsed through the existing `ChronycOffsetSource`.
- [ ] Add the package + mount + env value; keep the fail-closed default for a missing socket.
- [ ] Change record + tests; republish through CI (`image-publish.sh --merge-env`) and update the digest.
- [ ] On the VM: `chronyc tracking` inside the container returns a non-empty offset, and the gate halts
      when the socket is removed (the negative case is the one that must be proven).

### Task C2 — EOD lake offload
**Why:** `eod_controller.py` has no offload target today — the service ships `EOD_OFFLOAD=none`, so the
manifest lifecycle is proven without offload.
**Files:** `code/01_platform/04_scripts/eod_controller.py`, stack env, tests, R2 credentials path.
**Depends on:** B3 (bucket hygiene), S11 evidence.

- [ ] Scope the writer against the existing manifest format before writing code (the trigger exists, the
      path does not — decide the smallest path that lands the manifest in the bucket).
- [ ] Prove it against a local S3-compatible endpoint first, then against R2.
- [ ] Flip `EOD_OFFLOAD` from `none`, with a change record.

## Phase D — prove recovery and operations

### Task D1 — Restore from a backup, once
**Why:** an unverified backup is a hope, not a control.
**Files:** none — guide S11 procedure; evidence into the VM-day evidence directory.
**Depends on:** B3, S11.

- [ ] Restore a checkpoint or EOD manifest from R2 and verify the checksum matches the source.
- [ ] Record time-to-restore as the first RTO number.

### Task D2 — Exercise a rollback and an upgrade
**Why:** the rollback document exists but has never been run on the deployed stack.
**Files:** `../05_deployment/03-rollback.md` (procedure).
**Depends on:** VM day complete.

- [ ] Roll one service back to its previous digest and forward again; both ends healthy, no data loss.
- [ ] Record the exact commands used, so the runbook and the guide stay true.

### Task D3 — Monitoring review and a trader's morning checklist
**Why:** the operator (not a daemon) is the last line of detection.
**Files:** O2 dashboards; a checklist section (docs-only) in `PROD_VM_PROVISIONING.md` or the IR runbook.
**Depends on:** B6.

- [ ] Five-line morning check: order-cap hits, service restarts, disk/volume pressure, alert-store deltas,
      unexpected logins.
- [ ] Confirm every panel the operator needs is reachable only from the workstation address.

## Phase E — grow to the v1 quorum

### Task E1 — Add two manager+worker VMs
**Why:** two machines cannot lose a manager; v1 (M1 M2 M3 + O1) gives quorum 2/3 and Flink redundancy.
**Files:** `prod_vms.json` (two added rows, `role: manager`, labels `{"role":"worker"}`), the guide §1 v1 shape.
**Depends on:** Phases B–D complete and a stable two-machine run (2–3 live sessions watched).

- [ ] Create two VMs to the same spec; join as manager+worker; label `role=worker`; never a `role=manager` label.
- [ ] `docker node ls` shows 3 managers + 1 worker, all Ready/Active; quorum 2/3.

### Task E2 — Re-verify after the join
**Why:** placement and labels are the failure mode this growth introduces.
**Files:** none — existing checkers.
**Depends on:** E1.

- [ ] `python3 code/01_platform/04_scripts/prod_node_check.py --inventory prod_vms.json` exits 0.
- [ ] `python3 code/01_platform/04_scripts/cluster_check.py --expect prod_vms.json --out ~/readiness` → `FAIL=0`.
- [ ] The `max_replicas_per_node: 1` WARN clears once three nodes are eligible (guide §1b).

### Task E3 — Real-quorum drills
**Why:** RPO/RTO measured on one manager are not the production numbers.
**Files:** none — guide S10 scenarios; results into evidence.
**Depends on:** E1, E2.

- [ ] Lose one manager and one worker in turn; trading continues; record RPO/RTO per scenario.
- [ ] v2 (7 VMs, drained managers) stays trigger-gated: `N>6` workers, sustained CPU >80%, or Raft election flaps.
- [ ] Confirm the two HA interactions before the drill: the nine secrets now exist on three managers, and
      the operator holds the `--autolock` unlock key for each of them.

## Phase F — the go-live gate

Before the first live order, every row must have its evidence artifact, not an intention:

| # | Must be true | Evidence |
| --- | --- | --- |
| 1 | Broker cannot move money or exceed caps | B1 refusal notes, dated |
| 2 | Market-data `app_id` cannot place orders | B1 refusal note |
| 3 | Security alerts armed and delivered | B6 forced-alert capture in `alert-store` |
| 4 | Rotation dated, next date set | rotation log |
| 5 | Backups restore with a checksum match | D1 record + measured RTO |
| 6 | Rollback exercised | D2 commands + healthy ends |
| 7 | Clock source live (`CLOCK_OFFSET_SOURCE=chronyc`) | C1 VM proof + negative case |
| 8 | Incident runbook executable | B7, dry-read by a fresh reader |
| 9 | One-VM-loss drill on the real quorum | E3 RPO/RTO |
| 10 | EOD verified on the deployment | guide S11 evidence |

## What this plan cannot prove

- That the provider is not reading the VM — no control here changes that; the plan only makes what is
  read capped, revocable and detectable.
- That the broker's limits are enforced for every order type (only the attempts in B1 are evidence).
- Production throughput/latency and the final capacity allocation — still `EVIDENCE-BLOCKED` per the
  guide §4 sizing caveat.
- That two machines are highly available — they are not; Phase E is what changes that.
- That a provider read can be detected — it cannot; task B5's rotation is the compensating control.
- That HA reduces exposure — it increases it: every added manager replicates the secrets. HA buys
  availability, and the containment model (caps, scoping, rotation) is what keeps that trade safe.

## Risks

| Risk | Why it matters | Mitigation |
| --- | --- | --- |
| C1 lands after the first deploy | forces a republish + redeploy | decide before the deploy, or accept it explicitly |
| R2 becomes the only copy of data while untested | silent loss | D1 before trusting it; keep the local rehearsal artefacts |
| Rotation without a calendar | drifts to never | the dated log and the quarterly next-date |
| The workstation is the weakest link | one theft or one browser compromise undoes Tier 1 | B4, and rotate on any suspicion |
| Alerts armed but unwatched | detection without response | D3's morning checklist |
| Growing to HA without revisiting the broker IP whitelist | broker login silently fails on the new nodes | B1 records the whitelist scope; E1 checks it before the drill |

## What is needed from you

1. The five ARROW values, the R2 bucket + scoped token, and the two VM IPs (Phase A).
2. Broker-portal and CloudPe-panel sessions for B1 and B2.
3. Four decisions: `EOD_TABLES` production list; R2 short-lived vs rotated static; the chrony socket
   ownership approach; the rehearsal registry (keep or drop).
4. The broker whitelist scope decision (all workload IPs vs one stable egress address) — it must be made
   before Phase E, and it is cheaper to decide it now.
5. A go-ahead per phase — nothing above runs on its own.

## What I would do first

While the purchases are in flight, the two PC-side artefacts are B7 (the incident-response page) and B6's
alert list — both docs-only, both usable on VM day, neither needs a gate run or a change record.
