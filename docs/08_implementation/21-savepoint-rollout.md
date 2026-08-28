# 21 — Savepoint rollout (G5 Ops T12)

**Status:** Partially implemented (offline) + **LIVE VERIFIED 2026-08-24 + RE-VERIFIED 2026-08-25 (CHG-105)** — script + Makefile + SignalJobConfig F005 gate verified offline (`bash -n`/`shellcheck`/`DRY_RUN`); live savepoint/restore PASSED 2026-08-24: savepoint `file:/checkpoints/savepoints/savepoint-565320-eb7c86bee968` COMPLETED → graceful cancel → redeploy → new job `5869c40e7073768a7593fea2bc24b58b` RUNNING restored `is_savepoint=true` (id 1250), checkpoints continue (1255), 0 failed (evidence `logs/rollout/rollout-signal-job-compute-20260824-213501.log`). **RE-VERIFIED 2026-08-25 (CHG-105):** fixed the job auto-resolve regex (Flink's `/jobs/overview` omits `isStoppable` and field order is `jid,name,start-time,...` — the old fixed-order sed never matched, so auto-resolve was broken and 08-24 had to pass `JOB_ID=` explicitly). Fresh live rollout on the 22h-old job `7e9c1e49...` → savepoint `savepoint-7e9c1e-509f725c6b1d` COMPLETED → cancel → redeploy → new job `bdbc4d50418c88d97b28223b8dd43874` RUNNING 120/120 tasks, **all 128 key-group state handles restored from the savepoint** (TM `FullSnapshotRestoreOperation` logs), 10+ checkpoints completed post-restore (evidence `logs/rollout/rollout-signal-job-compute-20260825-211430.log`). Dedup continuity honestly degraded both runs — no live traffic in the post-restore window (feed idle; `state_count` 0→0). Prod `s3://` path still `TO_BE_VERIFIED` (prod VMs).


## Problem

A SignalJob update must not fall back to an offset-0 full replay: that
re-emits the whole backlog, balloons the fingerprint-dedup MapState past the
pinned checkpoint contract, and appends duplicate candle rows
(CANDLE-KV-REPLAY-001 A3.3). The fail-closed gate in `SignalJobConfig`
enforces this — a restart must set `STATE_RECOVERY_PATH` (restore) or
explicitly accept `ALLOW_FULL_REPLAY=true` (replay, break-glass only) — but
the operator needed a single rollout command that captures state, replaces
the artifact, and restores from the fresh savepoint.

## What the rollout does

`make rollout-savepoint` (backed by
`code/01_platform/04_scripts/rollout-savepoint.sh`) executes, in order:

1. **Preflight** — Flink REST (`JM_URL`, default `http://localhost:8081`)
   reachable; resolves the job by name (`JOB_NAME`, default
   `signal-job-compute` — matches `env.execute("signal-job-compute")` in
   SignalJob.java) and requires state `RUNNING`; `JOB_ID=<jid>` bypasses the
   lookup. In `DEPLOYMENT_ENV=production` the savepoint target must be
   `s3://*`/`s3a://*` (G4 T9 parity with submit-jobs.sh); dev defaults to
   `file:///checkpoints/savepoints` (the `flink-checkpoints` volume mounted
   in both JM and TM).
2. **Savepoint** — `POST /jobs/{jobid}/savepoints` (async request-id, polled
   until `COMPLETED`, `SAVEPOINT_TIMEOUT_S` default 600 s). The completed
   savepoint path becomes the restore path.
3. **Stop** — `PATCH /jobs/{jobid}?mode=cancel` (fallback
   `POST /jobs/{jobid}/cancel`), waits for `CANCELED`.
4. **Deploy** — `docker compose cp` the new jar
   (`JAR`, default `code/02_services/02_compute/target/compute.jar`) into
   `flink-jobmanager:$JAR_IN_CONTAINER`, then submits inside the JM container:

   ```bash
   docker compose exec -T \
     -e STATE_RECOVERY_PATH=<savepoint> -e ALLOW_FULL_REPLAY=false \
     [-e <job env forwarded from the caller>] \
     flink-jobmanager flink run -d \
     -c com.trading.compute.signaljob.SignalJob /opt/flink/jobs/compute.jar
   ```

   Why the `flink run` client and not the REST `/jars/run` path: the job
   config comes from `System.getenv()` in `main()` (SignalJobConfig via
   `fromEnv()`), and the JVM that runs `main()` is the submitting client.
   `submit-jobs.sh`'s `/jars/run` executes in the JobManager JVM, whose
   composed env carries `ALLOW_FULL_REPLAY=false` (restore-only default) and cannot take a
   per-release restore path. The `docker compose exec -e` pattern is the one
   already verified in `docs/06_operations/01-runbooks.md` (start/restart).
   The script forces `ALLOW_FULL_REPLAY=false`, so the F005 combination
   cannot occur, and forwards every job config var it inherited (see the
   `JOB_ENV_NAMES` list in the script). The pinned required vars —
   `DEDUP_TTL_MS`, `CANDLE_WINDOW_MS`, `CHECKPOINT_INTERVAL_MS`,
   `CHECKPOINT_TIMEOUT_MS`, `MAX_CONCURRENT_CHECKPOINTS` — have **no
   defaults** in SignalJobConfig and must be exported by the caller.
5. **Verify start** — the new job reaches `RUNNING`, its client log shows
   `startup mode = RESTORE (restore=true, fullReplay=false)` (a
   `FULL_REPLAY` or `F005` line fails the rollout), and it completes ≥ 1
   durable checkpoint.
6. **Dedup continuity (degradable)** — samples the TaskManager Prometheus
   metrics `compute_dedup_firsts_cumulative` (renamed 2026-08-28 from
   `compute_dedup_state_count` — cumulative counter, valid for the RELATIVE
   continuity check) / `compute_dedup_first` /
   `compute_dedup_duplicates` (`PROMETHEUS_URL`, default
   `http://localhost:9250/metrics` — the host-published TM port in dev)
   before the savepoint and after restore; fails if the restored state count
   drops below 50 % of the pre-rollout value (state was NOT preserved),
   warns and continues if the endpoint is unreachable, and reports the
   session dedup hit share as evidence. `VERIFY_DEDUP_STATE=0` disables.

The whole transcript (plus the savepoint path and job ids) lands in
`logs/rollout/rollout-<job>-<timestamp>.log` (gitignored evidence, repo
convention).

## Usage

```bash
# preview without touching anything
make rollout-savepoint ARGS="DRY_RUN=1"

# full rollout (env-driven; export the required pinned job config first)
export DEDUP_TTL_MS=60000 CANDLE_WINDOW_MS=15000 \
       CHECKPOINT_INTERVAL_MS=10000 CHECKPOINT_TIMEOUT_MS=30000 \
       MAX_CONCURRENT_CHECKPOINTS=1
make rollout-savepoint ARGS="JAR=/tmp/compute.jar"

# re-run an interrupted rollout from an already-captured path (deploy+verify only)
make rollout-savepoint ARGS="RECOVERY_PATH=file:/checkpoints/savepoints/savepoint-abc-123"

# direct invocation
code/01_platform/04_scripts/rollout-savepoint.sh --job-id <jid> --jar code/02_services/02_compute/target/compute.jar
```

Overlays (p10 rehearsal, ...): `COMPOSE_FILE` accepts the base file and
`COMPOSE_PROJECT` the project name (`-p p10`); `JM_URL` must point at the
overlay's published REST port and `PROMETHEUS_URL` at the overlay's TM port.
Production swarm: same script, with `JM_URL` = the stack's JobManager REST
endpoint, `SAVEPOINT_DIR=s3://...`, `DEPLOYMENT_ENV=production`, and the
deploy step targeting the JM service task container (the swarm equivalent of
`docker compose exec`).

## Scope and ownership

- Script: `code/01_platform/04_scripts/rollout-savepoint.sh` (T12, G5).
- Makefile target: `rollout-savepoint` (T12).
- No changes to `docker-compose.yml` / `docker-stack.yml` (T0/T9 own them) or
  `code/02_services/02_compute` (T3 owns it) — the script reads the job's
  env contract and overrides only `STATE_RECOVERY_PATH`/`ALLOW_FULL_REPLAY`
  at submit time; it does not modify the compose defaults.

## Rollback

If the restored job misbehaves, redeploy the previous jar through the same
script with `RECOVERY_PATH` set to the same savepoint (or the previous
checkpoint dir per the runbook) — the savepoint is retained (the stop is a
cancel, not a delete) and remains the restore anchor for both directions.

## Validation (this change)

- `bash -n` and `shellcheck -S warning` (repo static-check level) pass on the
  script.
- `make rollout-savepoint ARGS="DRY_RUN=1"` prints the full command plan
  without executing anything.
- Live acceptance (plan T12 at 1k): rolling update retains the dedup hit
  rate — measured externally by the existing E2E/bench gates; the script's
  step 6 supplies the state-continuity evidence.

### Implementation status — 2026-08-24 (offline laptop, no market/4VM; single-VM `docker-compose.yml` only)

| # | Doc claim | Code verification | Status 2026-08-24 | Evidence offline (laptop) | Needs live cluster |
| --- | --- | --- | --- | --- | --- |
| 1 | Problem: offset-0 replay blows dedup MapState past checkpoint contract | `SignalJobConfig.java:226-228` `StartupMode.FULL_REPLAY` + `CANDLE-KV-REPLAY-001 A3.3` gate `F005` `stateRecoveryPath` null → `FULL_REPLAY` else `RESTORE`; `SignalJob.java:69-80` logs `startup mode = RESTORE/FULL_REPLAY` | PARTIALLY | Gate code exists `SignalJobConfig.fromEnv()` `F005` blank/dual-set/missing checks `238-284`, `SignalJob.run` logs mode + `ComputeAlertLogs.emitAlertLog`; compile `mvn -q -Dtest=SignalJobConfigTest` green (offline) | Live fail-closed proof: restart without `STATE_RECOVERY_PATH` + `ALLOW_FULL_REPLAY=false` must fail with `[F005]` — needs Flink cluster |
| 2 | Rollout step 1 Preflight JM reachable + `RUNNING` `signal-job-compute` (`env.execute("signal-job-compute")`) | `rollout-savepoint.sh:224-245` `api_get /v1/config` `JM_URL` `JOB_NAME` default `signal-job-compute` matches `SignalJob.java:109` `env.execute("signal-job-compute")` ; `Makefile:230-231` target | PARTIALLY | `bash -n` 0 + `shellcheck -S warning` 0 + `make rollout-savepoint ARGS="DRY_RUN=1"` prints plan `JM_URL=http://localhost:8081` `JOB_NAME=signal-job-compute` without executing, handles unreachable JM (`WARN — JobManager REST not reachable — dry run continues`) | Live `RUNNING` state + `JOB_ID` lookup via `GET /jobs/overview` + cancel fallback — needs `docker compose up flink-jobmanager` |
| 3 | Step 2 Savepoint `POST /jobs/{jobid}/savepoints` async poll `SAVEPOINT_TIMEOUT_S 600` target `s3://` prod else `file:///checkpoints/savepoints` | `rollout-savepoint.sh:268-283` `DEPLOYMENT_ENV=production` rejects non-`s3://` (`G4 T9` parity `submit-jobs.sh`), `SAVEPOINT_DIR` default `file:///checkpoints/savepoints` (compose volume `flink-checkpoints`) | PARTIALLY | Script code verified `271-283` prod guard + dev default; `DRY_RUN` prints `POST …/savepoints {"target-directory":"file:///checkpoints/savepoints"}` without calling Flink | Live async request-id poll until `COMPLETED` → savepoint path becomes restore path — needs `RUNNING` job + Flink REST |
| 4 | Step 3 Stop `PATCH /jobs/{jobid}?mode=cancel` fallback `POST …/cancel` wait `CANCELED` | `rollout-savepoint.sh:298-342` `wait_state CANCELED` `JOB_STOP_TIMEOUT_S 120` | PARTIALLY | Code present, `DRY_RUN` prints `PATCH …?mode=cancel` fallback — no live call offline | Live cancel race handling — needs running job |
| 5 | Step 4 Deploy `docker compose cp JAR` + `exec -T -e STATE_RECOVERY_PATH=<savepoint> -e ALLOW_FULL_REPLAY=false … flink run -d -c com.trading.compute.signaljob.SignalJob` (not REST `/jars/run`) | `rollout-savepoint.sh:343-381` `compose cp` `exec_args` with `STATE_RECOVERY_PATH`+`ALLOW_FULL_REPLAY=false` `JOB_ENV_NAMES` forwarded, `SignalJobConfig` reads `System.getenv()` `fromEnv()`, `SignalJob.java:62-64` `SignalJobConfig.fromEnv()` → `fromElements` never `REST /jars/run` (JM env `ALLOW_FULL_REPLAY=true` cannot carry per-release path) — pattern verified `docs/06_operations/01-runbooks.md` `docker compose exec -e` | PARTIALLY | Script code verified `365-374` `flink run -d -c …`; `Makefile:222-231` documents env `JAR,JOB_ID,RECOVERY_PATH,JM_URL,SAVEPOINT_DIR,COMPOSE_FILE,DRY_RUN` + pinned vars `DEDUP_TTL_MS` etc. no defaults `must be exported`; `DRY_RUN` prints `docker compose cp …` + `exec -T -e STATE_RECOVERY_PATH=<fresh-savepoint> -e ALLOW_FULL_REPLAY=false` | Live `docker compose exec` submit + `F005`/`FULL_REPLAY` grep fails rollout if `ALLOW_FULL_REPLAY` true — needs JM container + jar |
| 6 | Step 5 Verify start `RUNNING` + log `startup mode = RESTORE (restore=true, fullReplay=false)` + ≥1 checkpoint | `rollout-savepoint.sh:417-447` `wait_state RUNNING` `START_TIMEOUT_S 180`, `grep FULL_REPLAY/F005` fails, `grep RESTORE`, poll `/jobs/{id}/checkpoints` until `completed>0` `CHECKPOINT_TIMEOUT_S 180` | PARTIALLY | Code present + `SignalJob.java:77-80` `LOG.info startup mode = RESTORE` + `ComputeAlertLogs.emitAlertLog` `startup-mode` — offline compile green; `DRY_RUN` skips live wait | Live Flink REST job state + log grep + checkpoint completed — needs restored job + checkpointing `EXACTLY_ONCE 10s/30s/1` |
| 7 | Step 6 Dedup continuity `compute_dedup_firsts_cumulative/first/duplicates` (renamed 2026-08-28; was `compute_dedup_state_count`) via `PROMETHEUS_URL http://localhost:9250/metrics` 50% floor degradable `VERIFY_DEDUP_STATE=0` | `rollout-savepoint.sh:208-224 sample_dedup`, `285-296` pre-sample, `426-432` T0, `449-497` post-restore `state_count >= 50%` before else `WARN` | PARTIALLY | Code verified best-effort, `DRY_RUN` prints `WARN — Prometheus dedup metrics unavailable — continuity check reduced` — offline degrade works | Live TM `:9250/metrics` + `1k` E2E `dedup hit rate` — needs Prometheus + traffic |
| 8 | Rollback same savepoint reusable `RECOVERY_PATH` = same path; evidence `logs/rollout/rollout-<job>-<ts>.log` gitignored | `rollout-savepoint.sh:246-266` `RECOVERY_PATH` skip savepoint/stop deploy only, `110-112` `EVIDENCE` `logs/rollout/…` | PARTIALLY | Branch `RECOVERY_PATH` code + `mkdir -p LOGDIR` `tee -a EVIDENCE` verified offline | Live redeploy previous jar via same path — needs cluster |
| 9 | Validation (this change) | `Makefile:288-295` `static-check` `bash -n`+`shellcheck` | PARTIALLY (offline done) | `bash -n` 0, `shellcheck -S warning` 0, `make rollout-savepoint ARGS="DRY_RUN=1"` `EXIT 0` transcript `logs/rollout/rollout-signal-job-compute-20260824-111021.log`, live at 1k pending | Live T12 at 1k rolling update retains dedup hit rate via E2E/bench gates |
