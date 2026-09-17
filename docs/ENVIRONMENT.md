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

## Keeper (`env_facts.py`)

Hand-editing works, but the keeper is safer - it assigns ids, enforces
anchors, and writes fail-closed (backup, assert, atomic replace):

```text
python3 code/01_platform/04_scripts/env_facts.py add --title "..." \
  --verified "2026-09-16 - <command + key output>" --check "<rc=0 holds>" \
  --recheck "<event that kills it>" --body "<1-3 lines>"
python3 code/01_platform/04_scripts/env_facts.py retire FACT-003 --reason "..."
python3 code/01_platform/04_scripts/env_facts.py replace FACT-003 --title ... (same flags as add)
python3 code/01_platform/04_scripts/env_facts.py check    # re-run LIVE checks, flags drift
python3 code/01_platform/04_scripts/env_facts.py prune    # archive DEAD rows, keeps the ledger lean
```

Size rule: the ledger stays a quick read (~50 rows max). Past that, `prune`
moves DEAD rows to `docs/ENVIRONMENT.archive.md`; if LIVE rows still overflow,
split by area (`ENVIRONMENT.prod.md`) - never let one file become the burden
it was built to remove. Shape is gated by docs-audit C17.

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
Check: test "$(docker info --format '{{.Swarm.ControlAvailable}}')" = false
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
Check: grep -q "fluss-" /etc/hosts
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
Check: docker ps >/dev/null && grep -qE '"(9123|9124|8081|5080|2181)' code/01_platform/01_docker/docker-compose.yml
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
Check: docker exec 01_docker-fluss-tablet-1 ls -l /bin/sh | grep -q dash
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
Check: grep -q FLINK_IMAGE code/01_platform/01_docker/runtime.lock
Recheck when: `docker push` plus `digest-pin.sh` runs
The image is built locally and unpushed, so the pin cannot move yet.
Do not invent a digest.

### FACT-010: Swarm secrets must exist before `stack deploy`
Status: LIVE
Verified: 2026-09-16 - `docker-stack.yml` `secrets:` are all
`external: true`; `PROD_VM_PROVISIONING.md` step 5 gates deploy on D1.2.
Check: manual (run 'docker secret ls' on a prod manager - impossible from this worker PC)
Recheck when: secret set changes or prod cluster rebuilt
Creating secrets mutates cluster state - needs the operator's word, same
as `swarm init`, `stack deploy`, and `docker push`.

### FACT-011: Fluss 0.9.1 tiers closed segments to R2 and commits the manifest; clients read them back from R2, but R2 is not a tablet restore source
Status: LIVE
Verified: 2026-09-16 - two-container trial with production FLUSS_PROPERTIES: 142 'Copied ... to remote storage as remote log segment' lines, ZK .../remote_logs held remote_log_manifest_path on s3:// and remote_log_end_offset 9923850, bucket listing after stop 833 objects / 66 MB / 208 .log
Verified: 2026-09-16 - a second trial with table.log.tiered.local-segments 1 let cleanup delete the tiered local copies (local base offset 981794, manifest 0-981794), and a default-settings Flink SQL SELECT returned a write3 row at an offset below the local base (bounded 20005..520000, below 981794 under every candidate position) while the task manager logged 'Successfully downloaded remote log segment file' for all 17 remote segments: the client reads from R2
Check: manual (needs a live R2-tiered Fluss trial; see CHG-182 and the plan 2026-09-16 Post-Completion)
Recheck when: Fluss image bump, or a change to remote.data.dir
Tiering copies only *closed* log segments, so a small write proves nothing - a 0-key listing after 3 rows is normal (default log.segment.file-size is large).
A tablet started against a fresh empty local data directory does NOT re-read from R2: ZK keeps the manifest path but the LogTieringTask logs 'Reset remote end offset to local end offset' and COUNT(*) returns 0.
The client-side read path DOES reach R2 (fluss-flink's RemoteLogDownloader), so a reader whose local log no longer holds the offsets it needs is served from the bucket; the two facts are about different paths and neither contradicts the other.
table.log.tiered.local-segments must be > 0: LogTablet rejects 0, so a 0 never enables cleanup and the row you are trying to read below the local base stays local.
Production gives each tablet its own data volume (fluss-tablet-data-1/2/3:/tmp/fluss/data), so ordinary restarts and reschedules keep the local log; R2 is a tiering target and lakehouse feed, not node recovery.

### FACT-012: production Fluss tiering is proven in isolated trials only - no real stack deploy has ever run
Status: LIVE
Verified: 2026-09-16 - CHG-182 (docker stack config render + two-container trials); docker info on this host shows no manager (FACT-001)
Check: manual (needs a Swarm manager - this host is a worker, see FACT-001)
Recheck when: a real docker stack deploy runs on the 4-VM Swarm
PROVEN: Fluss servers start under the production FLUSS_PROPERTIES, tier closed segments to R2, commit the manifest, and a client reads them back from R2 (FACT-011). NOT PROVEN: docker stack deploy, per-service health on a real cluster, and the DEPLOY=1 gate on a manager are all unexercised. Do not re-derive the trial evidence or rerun it looking for a different answer - it is recorded in CHG-182 and docs/08_implementation/09-production-swarm.md. The 4-VM Swarm has never been exercised; FACT-002 is the topology decision, not evidence of a running cluster.

### FACT-013: production lake tiering (datalake.*) is unwired - the keys are set but no plugin jars are mounted
Status: DEAD (superseded by next row: lake-tiering plugins exist in a derived image, but FLUSS_IMAGE still names the stock one)
Verified: 2026-09-16 - docker-stack.yml carries datalake.iceberg.* on all four Fluss services but no plugin-jar mounts; dev compose bind-mounts fluss-fs-s3/fluss-fs-hdfs/fluss-fs-hadoop-shaded into plugins/iceberg/
Check: grep -c 'plugins/iceberg' code/01_platform/01_docker/docker-stack.yml | grep -q '^0$'
Recheck when: plugin jars are mounted in docker-stack.yml, or the datalake.* keys are removed
Log/KV tiering to R2 does NOT need these jars (FACT-011 proves it works without them). Lake tiering does. Whether it works in production as written is unverified and out of CHG-182 scope - do not assume it works, and do not 'fix' it by adding the keys again; they are already present and the gap is the missing jars.

### FACT-014: lake-tiering plugins exist in a derived image, but FLUSS_IMAGE still names the stock one
Status: LIVE
Verified: 2026-09-16 - CHG-183: built trading-fluss-runtime:0.1.0 and drove it against real R2; stock image exits NoClassDefFoundError Configurable, derived image starts with the Iceberg catalog loaded
Check: test -f code/01_platform/01_docker/fluss-runtime/Dockerfile && grep -q 'plugins/iceberg' code/01_platform/01_docker/fluss-runtime/Dockerfile
Recheck when: FLUSS_IMAGE in runtime.lock points at a pushed derived image, or fluss-runtime/ is removed
code/01_platform/01_docker/fluss-runtime/ bakes fluss-fs-s3 + fluss-fs-hdfs INTO /opt/fluss/plugins/iceberg/ (the stock image ships both in plugins/s3 and plugins/hdfs, which the iceberg classloader cannot see). This fixes the classloader gap only. runtime.lock's FLUSS_IMAGE is deliberately still the stock digest, because a locally built image has no registry manifest digest to pin. So a deploy today still gets the stock image, and the datalake.* keys would still fail with NoClassDefFoundError. Push the image and run digest-pin.sh before expecting lake tiering to work in production.

### FACT-015: recreating the Flink jobmanager/taskmanager triggers a Fluss bucket-registration settle
Status: DEAD (superseded by next row: purging raw_table_1 re-registers buckets and the tablet keeps deleting the old table's log segments afterwards)
Verified: 2026-09-17 - live: JM/TM recreated at 20:02:00/20:02:31; the coordinator logged 160 Batch Register LeaderAndIsr entries in 20:02-20:03 (48 still inside a 3-minute window) and the tablet churned LogTieringTask 'after becoming leader' lines until 20:04:52, then went quiet.
Check: docker logs 01_docker-fluss-coordinator-1 --since 2m 2>&1 | grep -c 'Batch Register LeaderAndIsr'
Recheck when: Fluss or Flink image re-pinned, or the JM/TM recreate path stops touching Fluss leadership
Recreating the Flink containers re-registers Fluss bucket leadership, so the coordinator and tablet are busy for roughly 2-3 minutes afterwards. An ingestion start inside that window can time out: run B (20:02:50) logged 'ddl-bootstrap: could not list databases: null' after hanging the full 30s, while run A (19:50:04) verified 27 tables in 0.58s on the same stack. Wait for the coordinator to go quiet before starting a measurement, and expect holistic-measure.sh to restart the TM itself at phase start.

### FACT-016: purging raw_table_1 re-registers buckets and the tablet keeps deleting the old table's log segments afterwards
Status: LIVE
Verified: 2026-09-17 - live: docker logs on 01_docker-fluss-coordinator-1 show one Batch Register LeaderAndIsr burst per harness run, each naming a NEW tableId (7041 at 19:49:59, 7042 at 20:02:46, 7043 at 20:08:12 UTC) that matches that run's 'raw table purged' line to the second; the tablet meanwhile logged 12 Remove fetcher/Deleting segments lines for the OLD tableId in 20:08:16-20:08:17.
Check: docker logs 01_docker-fluss-tablet-1 --since 2m 2>&1 | grep -c 'Deleting segments'
Recheck when: the purge stops dropping+recreating raw_table_1, or holistic-measure.sh gains a post-purge readiness wait
Each purge drops raw_table_1 and recreates it, so the table gets a new tableId and the coordinator re-registers its 16 buckets. The tablet then asynchronously tears down the PREVIOUS tableId's log segments for a second or more after the purge reports success. Purge and the first append can therefore overlap: in run 20260917-013752 ingestion started 1s after 'raw table purged' and its first append failed with FlussRuntimeException 'Failed to update metadata' at 20:08:19, while the tablet was still deleting the old table's segments. NOTE: an earlier version of this fact blamed the JM/TM recreate; that was wrong - the bursts track the purge, and a separate ~10-minute periodic task also re-registers.
