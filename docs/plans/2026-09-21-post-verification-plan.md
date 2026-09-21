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

**Acceptance criteria** — all nine hold before the first live order:

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
9. The workstation holds no plaintext credential and no production value at rest: no token file, no
   passphrase-less key, and either an encrypted disk (B4.1) or the paste-and-delete interim (B4.2).

**Non-goals.** A vault/secret-broker on the workstation; LUKS on the *VMs* (the workstation is the
opposite case — B4.1 encrypts it); `fail2ban`; authenticated NTP; Kubernetes/Ansible/Terraform; v2
(7 drained managers — trigger-gated, not planned here).

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
- **Unresolved decisions:** `EOD_TABLES` production list; chrony socket ownership inside the container;
  durable rehearsal registry (keep or drop); a
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

## Test design — written before the implementation

Every task names its test **before** the code exists, so implementation has a target. Three rules apply
to all of them, and they are the rules this repository already uses:

- **Written first, same commit**: a code task lands with its test in the same commit; a docs-only task
  lands with its check (link, path, name, or set equality) run and recorded.
- **Mutation discipline**: each test must (a) fail when the mechanism is broken and (b) assert its own
  precondition — that the fixture was actually applied. A test that still passes with the mechanism
  removed is a defect, not a pass.
- **Hermetic**: no network, no real credentials, no wall-clock dependence. Canonical runners:
  `python3 -m pytest code/01_platform/04_scripts/tests -q -p no:cacheprovider -p no:unittest` and the
  crate's `cargo test`. Tests are **never counted** as certification.

### Workstation checks (task B4) — documented commands, not new code

If any of these grows into a script, it lands with a change record and its own test.

| ID | Test and pass criterion | Must-fail control | Evidence |
| --- | --- | --- | --- |
| T1 | Disk encryption (B4.1): `lsblk -o FSTYPE` shows `crypto_LUKS` on the root device, `/etc/crypttab` binds the TPM device, and the machine reboots without a passphrase prompt | the same checks must report FAIL before the reinstall — proving they discriminate — and a live-USB boot must see only ciphertext | dated output, before and after |
| T2 | No production value at rest (B4.2): a names-plus-patterns scan over `$HOME` returns zero hits for the production secret names while the container is unmounted | plant a decoy (`ARROW_APP_SECRET=dummy1234567890`) under the scan root; the scan must fail, then the decoy is removed | both runs, paired |
| T3 | GitHub over SSH (B4.3): `ssh -T git@github.com` prints the account name, `git ls-remote --exit-code` returns 0, `~/.git-credentials` is gone and `credential.helper` is empty | the old path must now be dead: `git ls-remote https://…` must fail **after** the token is revoked | four command outputs |
| T4 | Key retirement (B4.4/B4.5): `id_rsa*` absent, no `94.237.73.113` in `~/.ssh/config` or `known_hosts`, and a batch-mode SSH to that address fails | run the same assertions against a temp copy of the config with the block re-added; they must fail | grep output, both copies |
| T5 | VM key (B4.6): `ssh-keygen -y -P ""` **fails** (a passphrase exists), key mode 600, `.pub` mode 644, `IdentitiesOnly yes` present | generate a throwaway passphrase-less key in a temp directory; the same command must succeed, proving the check detects it | two command outputs |
| T6 | No credentials in new transcripts (B4.7): the value-shaped scan over sessions created after this date returns zero matches | append a dummy `aws_secret_access_key = "dummy1234567890abcdef"` to a copied transcript; the scan must flag it | both runs |
| T7 | 2FA armed (B4.6): a dated checklist with one row per account (GitHub, CloudPe, Cloudflare, broker) | not machine-checkable — recorded as a dated checklist, never quoted as a test | the checklist itself |

### Code tests

| ID | Test and pass criterion | Must-fail control | Evidence |
| --- | --- | --- | --- |
| T8 | Clock parser (C1, hermetic unit): a valid `chronyc tracking` fixture parses to the expected offset; malformed output and a missing program (`rc=127`) both produce the typed fail-closed error | mutate the parser to accept garbage or to default the offset; the test must fail; the test also asserts the fixture was consumed | `cargo test` output |
| T9 | Clock integration (C1, local): with the host socket mounted the executor reads a non-zero offset; with the socket removed the gate halts and reports the documented reason | the socket removal **is** the negative case; a halt without a stated reason fails | container logs + gate output |
| T10 | Image identity (C1 republish): the new executor digest differs from the previous one and the deploy env pins the new digest | repin the old digest in a temp copy; the pin check must fail | pin-check output |
| T11 | Seeder coverage (B6a): the seeded alert set equals the alert-contract set parsed from `10-observability.md`, Security family included; a second run duplicates nothing | drop one alert from the fixture; set equality must fail — the doc is read live, so a doc change breaks the test until the seeder is updated | test output + a local O2 query |
| T12 | Alert delivery (B6b, local stack): a synthetic alert reaches `alert-store` inside the documented window (`alert-routing-selftest.py` on the local compose) | stop `alert-store`; delivery must retry and surface the failure — a silent drop fails the test | selftest output, both runs |

### Doc-against-deck consistency checks

| ID | Test and pass criterion | Must-fail control | Evidence |
| --- | --- | --- | --- |
| T13 | Rotation coverage (B5): the union of secret names demanded by the deck (`secrets:` plus `${VAR:?}`) equals the set named in `04-secrets-rotation.md` and the bootstrap script | add a fake demanded variable to a fixture copy of the deck; the check must fail | check output |
| T14 | R2 wording (B3a, resolved 2026-09-21): the accepted choice — a scoped, rotatable static pair — is stated in `04-secrets-rotation.md`, and no document claims temporary credentials while the deck stores a static pair | reintroduce that claim in a fixture copy; the check must flag it | check output |

### Suites that can only run on VM day — specified now

| ID | Test and pass criterion | Must-fail control | Evidence |
| --- | --- | --- | --- |
| T15 | Broker refusals (B1): each forbidden action is refused with the expected message **and** a small allowed order succeeds as a positive control — without that control a dead API would look like a pass | if the positive control fails, the suite reports INCONCLUSIVE, never PASS | four refusal notes plus the control, dated |
| T16 | Provider exposure (B2): a scan from a non-whitelisted address finds no service, while `5080` answers only from the workstation address | the workstation scan must succeed on `5080` — that is the positive control | both scan outputs |

### Docs and rehearsals

| ID | Test and pass criterion | Must-fail control | Evidence |
| --- | --- | --- | --- |
| T17 | Runbook executability (B7): every fenced shell block passes `bash -n`, every referenced repository path exists, and every command is either resolvable here or explicitly marked VM-side; a fresh reader can name the first three actions | a fixture runbook with a bogus path and a bogus command must fail the check | checker output + the reader's note |
| T18 | Restore (D1): the restored artefact's checksum matches the source and the restore repeats into a clean scratch directory | flip one byte in a fixture copy; the checksum check must fail | checksums, both runs |
| T19 | Rollback (D2, local): the previous digest is healthy, then the forward digest is healthy again — asserted with an application-level probe, not just "container running" | one step deliberately uses a wrong digest; the drill must detect it and the documented recovery must work | drill log |
| T20 | Inventory shape (E, already run): `placement_check.py` exits 0 for both the two-row and the four-row inventories | delete a row or break a label in a fixture copy; the check must exit non-zero | rc values, both inventories |

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
- [x] **Decided 2026-09-21: a scoped, rotatable static pair is the accepted choice.** Temporary credentials
      need a credential-minting service — a new component to run, watch and itself rotate. Guide §9 row 3 and
      `04-secrets-rotation.md` now state the accepted pair; revisit only if R2's own short-lived tokens ever
      remove the need for that service.
- [ ] Record the read/write scope and the rotation date in the rotation log (task B5).

### Task B4 — Workstation hygiene (the machine every other control trusts)
**Why:** today the whole facility trusts this PC: it holds the deploy env, the SSH keys and every
rehearsal secret, so a stolen laptop hands all of it over at once.
**Files:** `~/.ssh/`, `~/.gitconfig`, `~/.p6v/`, `~/.pi/agent/sessions/` — no file contents are committed.
**Depends on:** none.

**Measured on this workstation, 2026-09-21** — four defects, five things already right:

| Defect | Evidence |
| --- | --- |
| Disk is not encrypted | `lsblk`: root is plain `ext4` on a 931 GB NVMe; `0` LUKS devices |
| The GitHub credential is plaintext | `credential.helper=store`; `~/.git-credentials` present (mode 600, value never read); the remote is `https://…` |
| `id_rsa` has no passphrase | `ssh-keygen -y -P ""` succeeded; only the legacy host block references it |
| Legacy provider access | `~/.ssh/config`: `Host 94.237.73.113`, `User root`, `id_rsa` + `id_ed25519` |

Already right, keep them: the screen locks after 5 idle minutes (`lock-delay 0`) · no cloud-sync folder in
`$HOME` · `~/.p6v/p2/prod.env` is mode 600 · `id_ed25519` is passphrase-protected · no secret is in the
repository.

**Chosen fixes — native, no new tool, nothing left to maintain afterwards:**

- [ ] **B4.1 Disk encryption: LUKS + TPM auto-unlock at the next OS reinstall.** The tools are already
      installed (`cryptsetup`, `systemd-cryptenroll`) and the machine has a TPM (`/dev/tpm0`), so the
      installer's "Encrypt the Ubuntu installation" plus `systemd-cryptenroll --tpm2-device=auto` gives
      an encrypted disk that still boots without a passphrase: no daily friction, and a removed or resold
      disk holds only ciphertext. **Schedule it before VM day**, so every production value is created on
      an encrypted disk. Back up first: `~/.ssh`, `~/.p6v`, the repository, and (optionally)
      `~/.pi/agent/sessions` — the whole set is a few hundred megabytes. Residual: a thief who also knows
      the login password.
- [ ] **B4.2 Until then, keep production values off this PC.** Nothing production-class exists yet (no
      real ARROW values, no R2 token), so the interim is free: rehearse with throwaway values, create the
      real ones at VM day, then delete the file once the swarm holds them. A `cryptsetup` file container
      is the native fallback if a file must exist sooner, but it adds a mount step and still leaves the
      session transcripts unencrypted — the reinstall is the endpoint, not the container.
- [ ] **B4.3 Replace the plaintext GitHub credential with SSH.** Measured precondition: **no key on this
      machine is registered with GitHub** (`ssh -T git@github.com` → `Permission denied (publickey)`).
      Order: generate a dedicated `id_ed25519_github` (passphrase-protected) → paste its `.pub` into
      GitHub → prove it (`ssh -T`) → `git remote set-url origin git@github.com:Saurabh17111994/Low_latency_infra.git`
      → prove a read (`git ls-remote`) → delete `~/.git-credentials` and unset `credential.helper` →
      revoke the stored token in GitHub. Maintenance afterwards: none.
- [ ] **B4.4 Retire the unencrypted `id_rsa`.** It is referenced only by the legacy host block, so
      destroy that server first (B4.5) and then delete `id_rsa` and `id_rsa.pub`. If the host must stay,
      add a passphrase in place (`ssh-keygen -p -f ~/.ssh/id_rsa`) — same public key, nothing to
      re-register.
- [ ] **B4.5 Retire the legacy provider footprint.** Confirm `94.237.73.113` is no longer needed, destroy
      it at the provider (which also stops it costing anything), secure that account with 2FA, then remove
      the host block from `~/.ssh/config`, delete the key and clear its `known_hosts` entries.
- [ ] **B4.6 One dedicated SSH key for the VMs** (passphrase-protected, `ForwardAgent no`), separate from
      the GitHub key; 2FA on GitHub, CloudPe, Cloudflare and the broker account.
- [ ] **B4.7 Stop putting real credentials in a conversation.** Nine session transcripts already hold 34
      value-shaped matches for ARROW/aws/O2 secret patterns (values were only matched, never printed).
      Nothing production-class exists yet, so nothing needs rotating today — from VM day on, real values
      are entered from the portal, not typed into a chat.

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
- That a local rehearsal is a production proof: T19 and T20 exercise the mechanism on this workstation;
  the production run stays part of the go-live gate (Phase F).
- That TPM unlock resists a thief who also knows the login password, or that T2/T6 find every secret —
  they match known names and shapes only.

## Risks

| Risk | Why it matters | Mitigation |
| --- | --- | --- |
| C1 lands after the first deploy | forces a republish + redeploy | decide before the deploy, or accept it explicitly |
| R2 becomes the only copy of data while untested | silent loss | D1 before trusting it; keep the local rehearsal artefacts |
| Rotation without a calendar | drifts to never | the dated log and the quarterly next-date |
| The workstation is the weakest link | one theft or one browser compromise undoes Tier 1 | B4, and rotate on any suspicion |
| Alerts armed but unwatched | detection without response | D3's morning checklist |
| Growing to HA without revisiting the broker IP whitelist | broker login silently fails on the new nodes | B1 records the whitelist scope; E1 checks it before the drill |
| The reinstall slips past VM day | the deploy env and the ARROW values would then sit in plaintext on this PC | B4.1 before VM day, or B4.2's paste-and-delete interim |

## What is needed from you

1. The five ARROW values, the R2 bucket + scoped token, and the two VM IPs (Phase A).
2. Broker-portal and CloudPe-panel sessions for B1 and B2.
3. Four decisions: `EOD_TABLES` production list; R2 short-lived vs rotated static; the chrony socket
   ownership approach; the rehearsal registry (keep or drop).
4. The broker whitelist scope decision (all workload IPs vs one stable egress address) — it must be made
   before Phase E, and it is cheaper to decide it now.
5. A go-ahead per phase — nothing above runs on its own.
6. Three workstation decisions: when to schedule the LUKS reinstall (before VM day), whether
   `94.237.73.113` can be destroyed, and the one-time step of adding a new public key to your GitHub
   account (B4.3).

## What I would do first

Order now, cheapest first: **B4** (three of its fixes take minutes; the reinstall is the only one that
needs scheduling), then **C1** (the only item with a hard before-deploy deadline), then **B7** and
**B6's** alert list while the purchases are in flight — docs-only, usable on VM day, no gate run and no
change record needed.
