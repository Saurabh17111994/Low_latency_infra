# Environment Facts Ledger

Verified truths about **where the work runs** - this machine, the prod VMs,
the images, the network. Not what the code does (dossiers), not what was
decided (change records): what is true about the ground under both.

## Why this file exists

Re-discovering machine truth costs a full investigation every time (tokens,
time, live probing). A fact recorded here with its proof is read in seconds.
A fact left in chat history is lost at the next compaction.

## Rules (read before adding a row)

1. **Proof or it is a rumor.** Every row names how it was verified: a command
   and its output, a doc section, an upstream URL. `hearsay` is never a proof.
2. **Expiry or it becomes a lie.** Every row names the event that kills it
   (`swarm re-init`, `image rebuild`, `VPC move`). A fact without a re-check
   condition misleads the next session.
3. **Append, never edit.** When a fact changes, add a new row with
   `Supersedes: FACT-nnn` and flip the old row to `DEAD`. History stays
   auditable - same discipline as `logs/tracker-14/`.
4. **One-line check where cheap.** If a fact can be re-verified in one
   command, the row carries it under `Check:`. Drift then announces itself
   instead of misleading silently.
5. **No secrets in this file.** Secret *names* and *mechanisms* are fine;
   secret *values* never land here.

## How to add a fact

Copy this skeleton. Keep the row small - one claim per FACT id.

```text
### FACT-nnn: <short claim>
Status: LIVE | DEAD (superseded by FACT-mmm)
Verified: <date> - <how: command + key output, doc ref, or URL>
Check: <one-line re-verification command, or "manual">
Recheck when: <the event that kills this fact>
<1-3 lines: the claim, its numbers, what breaks if you assume otherwise>
```

## Ledger

### FACT-001: this PC is a Swarm worker, not a manager
Status: LIVE
Verified: 2026-09-16 - `docker info` shows `ControlAvailable=false`,
`NodeID` empty, `Managers=0`; `docker service ls` and `docker node ls` both
fail with "This node is not a swarm manager".
Check: `docker info --format '{{.Swarm.ControlAvailable}}'` - `false` means worker-or-orphan
Recheck when: any `docker swarm init` / `join` / `leave` on this PC
This PC joined a swarm as a worker (the `ingress` overlay network proves it)
but its manager is gone - an orphaned worker. Plain containers and Compose
work fine; every `docker stack` / `docker service` / `docker node` command
fails. `stack_selfcheck.sh` would `swarm init` a throwaway single node -
safe on throwaway state, but cluster membership changes need the operator's
word first.

### FACT-002: production topology is v1 (Manager+Worker combined), v2 split is trigger-gated
Status: LIVE
Verified: 2026-09-16 - `docs/08_implementation/09-production-swarm.md`
DECISION 2026-08-20; `docs/05_deployment/PROD_VM_PROVISIONING.md` section 1.
Check: manual (doc decision)
Recheck when: v2 trigger fires (`N>6` workers, sustained CPU >80%, Raft election flaps)
v1 = 3 VMs as Manager+Worker + O1 observability outside Swarm (4 VMs total).
v2 = 3 manager-only (drained) + N workers + O1 (7 VMs). Same
`docker-stack.yml` serves both - v2 is labels plus drain, no rewrite.
Do NOT provision separate managers on day one.

### FACT-003: Swarm replaces a task whose container fails health checks
Status: LIVE
Verified: 2026-09-16 - upstream Docker docs ("How Swarm mode works"):
"If the container fails health checks or terminates, the task terminates ...
the orchestrator creates a new replica task that spawns a new container."
Check: manual (platform behavior)
Recheck when: Docker Engine major upgrade on prod VMs
Caveat from the tracker (moby#23962): during *updates* Swarm may reschedule
before the new task turns healthy - cosmetic on steady state, matters only
mid-deploy. The stack's own brake is `update_config` (`failure_action:
rollback`, `monitor: 30s`, `max_failure_ratio: 0.2`).

### FACT-004: prod job submit is `flink run -d` inside the jobmanager, not `stack deploy`
Status: LIVE
Verified: 2026-09-16 - `code/01_platform/04_scripts/rollout-savepoint.sh`
header + `docs/05_deployment/02-environments.md` startup order step 4.
Check: manual (repo procedure)
Recheck when: Flink deploy procedure changes
The stack starts Flink itself; Signal/Babysitter jobs go through the Flink
REST API: copy jar into `flink-jobmanager`, `flink run -d -c <Main> <jar>`
with explicit env. Upgrades are savepoint, stop, redeploy from the
savepoint so dedup state survives.

### FACT-005: host /etc/hosts maps fluss names to loopback (pre-existing leak)
Status: LIVE
Verified: 2026-09-16 - host `/etc/hosts` line 14 `127.0.0.1 fluss-tablet`,
line 17 `127.0.0.1 fluss-coordinator`. Operator decision: leave it.
Check: `grep -n "fluss-" /etc/hosts`
Recheck when: host file edited or dev machine replaced
Host-level change needs the operator's word, so the standing workaround is
`--add-host` on the commands that need real DNS. Never bake the workaround
into committed files.

### FACT-006: dev Fluss/Compose ports and tablet IP
Status: LIVE
Verified: 2026-09-16 - `docker-compose.yml` ports lines and live inspect:
coordinator 9123 (host-mapped), tablet 9124 (host-mapped), Flink REST 8081,
OpenObserve 5080, ZooKeeper 2181; tablet container IP 172.19.0.8 on
`01_docker_trading-net`.
Check: `docker ps` + `grep -nE '"(9123|9124|8081|5080|2181)' code/01_platform/01_docker/docker-compose.yml`
Recheck when: compose ports edited or network recreated
Overlay variants (p10 etc.) remap ports via `!override` - never assume base
ports hold on overlays (see AGENTS.md Hazards).

### FACT-007: Fluss and Flink images run dash as /bin/sh; openobserve has no shell at all
Status: LIVE
Verified: 2026-09-16 - live exec: `ls -l /bin/sh` ends in `dash` in both
the Fluss and Flink images; `sh`/`bash`/`ls`/`curl`/`wget`/`busybox` exec
all fail with "executable file not found in $PATH" on openobserve-1.
A CMD-SHELL probe on openobserve reported exit=-1 and flipped the status
to unhealthy.
Check: `docker exec <ctr> ls -l /bin/sh`
Recheck when: any of the three images is re-pinned or rebuilt
Consequences, all now enforced in `test_09_stack.py`: `/dev/tcp` probes
must run under `bash` (dash has no `/dev/tcp`); Fluss probes must target
`$HOSTNAME` (Fluss binds the container host, not loopback); shell-less
images (otel-collector, openobserve) must carry NO healthcheck probe.

### FACT-008: credentials split - .env for dev, Swarm secrets for prod
Status: LIVE
Verified: 2026-09-16 - `docker-stack.yml` `secrets:` block
(`aws_access_key_id`, `aws_secret_access_key`, `o2_password`,
`external: true`); AGENTS.md Hazards ("Secrets live in `.env`").
Check: manual (repo layout)
Recheck when: secret set changes
Dev reads `.env` (gitignored, from `.env.example`). Prod declares external
Swarm secrets created out-of-band - the stack file holds `${...}`
placeholders, never values. Never print secret values; load into shell
vars only.

### FACT-009: FLINK_IMAGE digest pin lives in runtime.lock, re-pin blocked on push
Status: LIVE
Verified: 2026-09-16 - `code/01_platform/01_docker/runtime.lock` line 19
names the digest-pinned Flink image; CHG-179 section 5 (push before pin).
Check: `grep -n FLINK_IMAGE code/01_platform/01_docker/runtime.lock`
Recheck when: `docker push` plus `digest-pin.sh` runs
The image is built locally and unpushed, so the pin cannot move yet.
Do not invent a digest.

### FACT-010: Swarm secrets must exist before `stack deploy`
Status: LIVE
Verified: 2026-09-16 - `docker-stack.yml` `secrets:` are all
`external: true`; `PROD_VM_PROVISIONING.md` step 5 gates deploy on D1.2.
Check: `docker secret ls` on a manager (unavailable from this worker PC)
Recheck when: secret set changes or prod cluster rebuilt
Creating secrets mutates cluster state - needs the operator's word, same
as `swarm init`, `stack deploy`, and `docker push`.
