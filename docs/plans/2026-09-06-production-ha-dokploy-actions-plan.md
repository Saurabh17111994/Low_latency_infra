# Production HA with Dokploy + GitHub Actions + 4 VMs — Context & Plan

**Date:** 2026-09-06 · **Status:** DECIDED (design), NOT STARTED (build)
**Decision:** Production = 4 rented VMs run by Dokploy, built/packed by GitHub
Actions, code kept on GitHub. Laptop stays development-only and is never part
of production.
**Authority note:** this plan is subordinate to the dossiers. If it conflicts
with `docs/08_implementation/09-production-swarm.md`, `docs/05_deployment/*`,
`docs/06_operations/*`, or a DEC/contract, the dossier wins — file a `CHG-*`.
Related tracker: `2026-08-25-live-readiness-unified-plan.md` (U5 row — this plan
is the *how* for that row's `09 M3 + 10 M3 + 12 ZK + PERF-PROD + DR x6`).

Written in plain language on purpose: future-me should get the whole idea back
in one read. Technical names are given once, in brackets, then plain words.

---

## 1. Goal (one line)

Code leaves my laptop → a robot tests and packs it → the packed boxes run on 4
machines that survive any single machine dying → my laptop can be switched off
the whole time.

## 2. Non-goals (what we are NOT doing)

- Laptop is never in the production loop. After `git push + tag`, the laptop
  can be closed, asleep, lost, or replaced — production never notices.
- No hand-editing of production machines — by humans or by AI agents. Fixes
  travel the road (locker → robot → boxes → machines), never shortcuts.
- No faked HA numbers on the laptop. Real survival is proven only by wounding
  real machines (Phase 7).
- This plan does not replace the funded-sandbox session (AC-U5/AC-U6) or the
  dossiers. It covers the machine side (U5), not the broker-money side.

## 3. The cast (every actor, plain words first)

| # | Plain name | Technical name | What it is | What it does |
|---|---|---|---|---|
| 1 | My laptop | dev machine | Where I write and test code today | Push code + release labels to the locker. Nothing else. Knows nothing about production. |
| 2 | The locker | GitHub repo | Shared cupboard on the internet holding code | Holds code + release labels (e.g. `v1.4.1` = "this exact set is approved"). Source of truth. |
| 3 | The robot | GitHub Actions | Free helper that sleeps until a label appears | Takes code from the locker → runs all tests → packs sealed boxes → shelves them → taps Dokploy → writes a diary. Free at our scale (2,000 min/month free; we need ~2 hrs). |
| 4 | The shelf | Container registry (default: GHCR) | Shelf of sealed, fingerprinted boxes | Machines collect boxes from here. Free at our scale. |
| 5 | Sealed boxes | Container images (pinned by digest) | Packaged code that can never be secretly changed | The only thing machines run. Built by the robot from Dockerfiles already in the repo. |
| 6 | The manager | Dokploy (free, self-hosted control panel) | Building manager with a web screen | Holds the cluster together, enforces the seating chart, accepts deliveries, health-checks, undoes bad deliveries, holds the secret locker, runs the reception desk (Traefik). Zero AI inside — fixed checklists only. |
| 7 | The building | 4 rented VMs in Docker Swarm | 3 workload machines (M1/M2/M3) + 1 watchman (O1) | Run the boxes. 3 copies of everything important (any 2 of 3 can decide, so 1 death changes nothing). Never phone the laptop. |
| 8 | The diary | CI logs + release evidence | Permanent record of every run | What code went in, what passed, which boxes came out, what machines reported, what was undone. This becomes release proof (DEC-044). |
| 9 | The secret locker | Dokploy/Swarm secrets | Locked compartment inside the machine group | Passwords/keys go in once, handed only to boxes that need them, never displayed, never in code/boxes/locker. |
| 10 | Off-site saves | S3 checkpoints + VM snapshots | Managed cloud storage | Flink's progress saves (resume-after-crash), disk snapshots. The "video-game checkpoint" of production. |

## 4. The flow (one picture)

```text
LAPTOP ──push + label──► LOCKER ──wakes──► ROBOT ──packs──► SHELF
 (me)                    (GitHub)           (Actions)         (GHCR boxes)
  │                         │                                     │
  │ closes here.            │ label = the only                    │ robot taps Dokploy
  │ Done for the day.       │ human action on                     ▼
  │                         │ release day.               DOKPLOY ──places boxes──► M1/M2/M3
  │                                                     (manager)    (run + vote 2-of-3)
  │                                                                        │
  │                                                                        ├──► O1 watchman
  │                                                                        ├──► S3 checkpoints
  │                                                                        └──► Arrow broker
```

Release-day walk-through (fix a bug in the gateway):

1. Morning, laptop: edit, test locally, push + label `v1.4.1`. Close laptop.
2. Robot wakes (the label woke it — nobody called it). Runs gateway tests; any
   failure stops everything here, VMs hear nothing, yesterday's boxes keep running.
3. Robot packs a new gateway box, fingerprints it, shelves it.
4. Robot taps Dokploy: "new gateway box, fingerprint …, collect it."
5. Machines pull the one box, restart only the gateway, health-check it. The
   other pieces are untouched.
6. Sick box? Old box stays, diary records the undo. Healthy? Release complete,
   diary proves what/when/approved-by-whom.

## 5. Dokploy's job description (and its boundaries)

Does: hold the cluster together · enforce the seating chart (which box on which
machine, how many copies, never-two-copies-together) · accept deliveries gradually
with health checks · undo bad deliveries automatically · hold the secret locker ·
run the reception desk (Traefik routing) · show all machines/boxes/logs on one
screen · scheduled backups of data it manages.

Does NOT: test code · pack boxes · design the HA rules (3-copy design, quorum,
kill-drill proof stay OUR decisions; Dokploy operates whatever is entered) ·
remove the VM bill · remove the need for the kill-a-VM proof.

Dokploy lives on one server (normally the first, doubling as Swarm manager). If
that server dies, tenants keep working but the manager's office is closed until
restored — so Dokploy's own data gets backups. It is the one pet in an
otherwise pet-free design.

## 6. The two gates (safety core — never invert)

- **Gate 1 — "Is it good?" (robot, before anything ships).** Full test suites,
  audits, contract checks on the robot's disposable machine. FAIL = boxes never
  made, VMs hear nothing. The lab test before medicine ships.
- **Gate 2 — "Did it start?" (machines, after delivery).** Each machine asks its
  new box only "are you breathing?" (process up, port answering). SICK = box
  discarded, yesterday's box keeps running. Swallowing the pill without choking.
- Rule: **the robot decides WHETHER it ships; the machines only confirm THAT it
  landed.** Testing on VMs is forbidden — unproven code must never sit inside
  production, even briefly.

## 7. The three test layers (only the last needs 4 machines)

| Layer | Checks | Runs on | Needs 4 VMs? |
|---|---|---|---|
| 1. Code correct? | Unit/integration/contract/audit tests | Robot | No |
| 2. Instructions correct? | Proofread the deploy definitions (copy counts, placements, secrets declared) + dolls-house rehearsal (pretend 3-node cluster inside one machine; walk join→label→deploy→kill-one→recover). Proves the PROCEDURE, not survival. | Robot | No (repo already has `test_09_stack.py` 25/25 + 1-host swarm mimic — robot re-runs them) |
| 3. Real building survives real fire? | Kill a real VM at real load; trading continues. `FAIL-VM-LOSS`, `PERF-PROD-60000-001`, `DR-001..006`. | The 4 production VMs, on schedule, forever (fire drills, not code tests — the code is proven by then; the drill tests the building). | Yes — by definition |

## 8. AI-agent policy (decided, applies from day one)

- Agents are supervised junior engineers: read anything, propose anything,
  touch production only through the front door (locker → robot → boxes →
  machines). No hand-edits on VMs, no exceptions.
- Eyes on production wide open (read logs/metrics/Dokploy screens, explain in
  plain words). Hands only run pre-written procedures (checklists, drills) —
  never improvise surgery.
- Interactive agenting from the laptop is fine (human present, nothing depends
  on it). Every AUTOMATIC agent lives on a cloud schedule (CI), never on the
  laptop — a laptop cron job would put the laptop back in the loop.
- Two bills, never mixed: robot muscle (minutes — free at our scale) vs agent
  thinking (brain calls — metered, cents per question). Cap monthly spend + cap
  steps per task; hitting the cap stops with a message, never silent spending.
- Every agent action is written to the diary (who/what/when).
- Dokploy contains zero AI and needs none. When an agent needs something done
  on the machines, Dokploy's buttons/API are the hands it borrows. Dokploy
  shows; the agent translates.

## 9. Money (monthly, at our scale)

| Piece | Cost |
|---|---|
| Locker (GitHub) | 0 |
| Robot (GitHub Actions — ~2 hrs of 2,000 free min) | 0 |
| Shelf (GHCR) | 0 |
| Dokploy software (free, open-source, self-hosted) | 0 |
| Dokploy's home (one of the 4 VMs — no extra machine) | 0 extra |
| Agent thinking (later, capped) | pocket change, capped |
| **4 rented VMs (500 GB SSD each per current sizing guess)** | **the only real bill (to be priced)** |
| Managed extras (optional: hosted monitoring instead of O1 self-host) | decision at pricing time |

Sizing note (from `09-production-swarm.md`): 500 GB/VM is a starting guess,
not proven — stays EVIDENCE-BLOCKED until `PERF-PROD-60000-001` passes. Start
on v1 (4 VMs, managers double as workers); v2 (dedicated managers) only on the
doc's triggers (workers > 6, sustained CPU > 80%, Raft election flaps).

## 10. Open questions (decided NOTHING here — verify before building)

1. **Stack-file mapping:** repo's prod design is one Swarm stack file with
   placement/anti-co-location rules, secret store, encrypted overlays. Verify
   how each is expressed inside Dokploy's app model (Dokploy accepts Compose
   files and runs a Swarm cluster — mapping needs a check, not an assumption).
2. **Dokploy home + backup:** confirm which server hosts it and how its data
   (settings, secret locker, history) is backed up and restored.
3. **Shelf choice:** default GHCR (same login as locker + robot).
4. **Robot → Dokploy handshake:** API/webhook trigger after packing (Dokploy
   documents both; pick at build time).
5. **Secret path:** broker keys/passwords must land in the cluster locker via
   Dokploy's secret handling — never in locker code, boxes, or CI logs.
6. **VM pricing + provider:** price 4x VMs meeting the floor; record choice.
7. **Registry/CI alternatives cross-check:** ChatGPT cross-check question saved
   in chat 2026-09-06 (judge any answer against the §11 checklist).

## 11. "Good CI" checklist (judge any setup against this)

1. It wakes itself (push/label starts it; no human starts it).
2. It follows the repo rulebook in order (`01-ci-cd.md` stages 1–5), no
   skipping. A pass means "proven," full stop.
3. It writes the diary (code-in, results, box fingerprints, machine reports,
   undos). The diary becomes DEC-044 release proof.

## 12. Build plan (phases, each with gate + evidence)

Repo rule: one checkbox = one verifiable step (mapped test + dated evidence
under `logs/`). Never batch. Phase 7 (live drills) runs only on real VMs.

### Phase 0 — Locker (code home)
- Code lives on GitHub; release-label convention (`vX.Y.Z`) agreed.
- Gate: push + tag visible remotely; branch protection on main if desired.
- Evidence: `logs/prod-ha/<date>-phase0-locker.md` (remote URL, tag list).

### Phase 1 — Shelf (GHCR default)
- Registry namespace created (private); retention/cleanup rule noted.
- Gate: push + pull one test box with fingerprint match.
- Evidence: `logs/prod-ha/<date>-phase1-registry.md` (namespace, digest).

### Phase 2 — Robot, Half 1: check + pack (NO VMs needed — start now)
- Workflows (`.github/workflows/`) implement `01-ci-cd.md` stages:
  (1) docs/contract checks, (2) static + supply-chain, (3) component tests,
  (4) integration/compat, (5) production-like acceptance (single-VM).
- Packing: build service boxes from repo Dockerfiles, push to shelf by
  immutable digest; diary written per run.
- Gate: a tagged dry-release goes fully green end-to-shelf with zero VMs.
- Evidence: workflow run URLs + `logs/prod-ha/<date>-phase2-ci-half1/` (run
  log, box digests, diary).

### Phase 3 — Land (VMs exist)
- Provision per `docs/05_deployment/PROD_VM_PROVISIONING.md` (D1.3 operator
  steps); record provider, sizes, monthly cost.
- Gate: `prod_node_check.py` (D1.2) passes before anything else.
- Evidence: `logs/prod-ha/<date>-phase3-provisioning/` (inventory, gate log).

### Phase 4 — Manager moves in (Dokploy)
- Dokploy installed on its home server; cluster joined (Swarm Nodes mode);
  Dokploy-data backup configured + restore tested once on day one.
- Gate: all nodes visible in Dokploy, backup/restore drill logged.
- Evidence: `logs/prod-ha/<date>-phase4-dokploy/` (topology log, backup proof).

### Phase 5 — Seating chart verified (open question §10.1 closed here)
- Placement/anti-co-location rules, secret locker entries, encrypted networks,
  per-node volumes expressed in Dokploy; `test_09_stack.py`-style static checks
  re-green against the Dokploy definitions.
- Gate: static checks pass; a canary box lands on the intended node ONLY.
- Evidence: `logs/prod-ha/<date>-phase5-placement/` (check logs, canary proof).

### Phase 6 — Robot, Half 2: deliver (wakes now)
- Deploy jobs: tap Dokploy with new digests → gradual collect → per-box
  health → auto-undo on sickness → diary. Secrets referenced by name only,
  never printed.
- Gate: one full release delivered + one forced-failure undo, both logged.
- Evidence: `logs/prod-ha/<date>-phase6-ci-half2/` (deploy log, undo proof).

### Phase 7 — Proof (the point of U5; real VMs, real wounds)
- `FAIL-VM-LOSS-60000-001` (kill one VM at load — trading continues),
  `PERF-PROD-60000-001` (sizing guess proven or corrected),
  `DR-001..006` live (container, checkpoint-store, tablet, broker, S3,
  credential-compromise; targets: recovery < 30 s, halt < 5 s).
- Gate: every drill PASS with dated evidence; sizing updated from measured
  numbers; DEC-044 sign-off.
- Evidence: `logs/prod-ha/<date>-phase7-proof/` per drill + sign-off record.
  On PASS, U5 sub-items (`09 M3`, `10 M3`, `12 ZK`, `PERF-PROD`, `DR x6`) flip
  in the live-readiness tracker via CHG record.

### Phase 8 — Watcher (eyes-only agent, later)
- Cloud-scheduled agent reads Dokploy screens/logs/metrics, reports
  plain-words health (or what's wrong). No write access. Monthly spend cap +
  per-task step cap set before first run.
- Gate: one week of reports + one simulated overspend-stop.
- Evidence: `logs/prod-ha/<date>-phase8-watcher/` (reports, cap proof).

## 13. Glossary (every term, plain words)

- **Swarm:** Docker's built-in team mode — several machines acting as one.
- **Sealed box / image:** packaged code + everything it needs, fingerprinted,
  unchangeable after sealing.
- **Digest/fingerprint:** a box's unique ID (`sha256:…`) — same ID means
  byte-identical box.
- **Registry/shelf:** internet shelf where boxes wait (GHCR = GitHub's shelf).
- **Quorum (2-of-3):** 3 voters where any 2 can decide — one death changes nothing.
- **Anti-co-location:** rule forbidding two copies of one thing on one machine.
- **Placement/seating chart:** which box runs on which machine, how many copies.
- **Health check:** machine asking a box "are you breathing?" (started? answering?).
- **Rollback/undo:** throwing away new boxes, keeping yesterday's running.
- **Overlay network:** private encrypted wiring between machines (automatic).
- **Secrets/secret locker:** passwords/keys stored locked, handed out sparingly.
- **Traefik/receptionist:** routes outside visitors to the right box; boxes hide behind it.
- **Checkpoint:** saved progress (video-game save) — crash, restart, resume.
- **Pipeline:** the robot's fixed checklist, run identically every time.
- **Diary:** permanent log of a robot run (in → results → boxes → reports → undos).
- **CI/CD:** CI = robot auto-tests+packs every change; CD = robot also delivers.
- **HA (high availability):** one machine can die and nobody notices.
- **DR drill:** scheduled practice of a disaster (kill X, prove recovery < target).
- **Pet vs cattle:** pet = unique machine you nurse; cattle = identical units you
  replace. Production must be cattle; Dokploy's home server is the one admitted pet.

## 14. Change log

- 2026-09-06: created from the Dokploy + GitHub Actions + 4 VMs decision
  (clarity sessions same day). Status DECIDED/DESIGN; build not started.
