# Operational Runbooks

## Runbook standard

Every critical runbook records:

- Trigger, severity, and affected scope
- Owner role and acknowledgement target
- Evidence to collect before intervention
- Whether the Executor gate must halt
- Bounded mitigation steps
- Recovery and reconciliation procedure
- Verification and closure evidence
- Escalation path

Use this template:

```text
runbook_id/title
scope and severity
preconditions and safety posture
signals and evidence queries
immediate containment / gate action
diagnostic sequence
reconciliation steps
recovery procedure
validation and closure evidence
escalation and owner
rollback / abort criteria
```

Commands and API paths are version-specific implementation details. Validate them against the deployed release before execution.

## Gate halt

### Triggers

Halt for unknown broker outcome, duplicate-order risk, missing/ambiguous identity, ~~stale instruction/reservation state~~ (**REMOVED 2026-08-15, CHG-005**), changelog discontinuity, checkpoint failure affecting order correctness, missing fill, failed reconciliation, unauthorized action, security incident, or unverifiable Executor state.

### Procedure

1. Transition the affected gate scope to `HALTED` and increment/record the gate epoch. The halt is
   authorised and applied in one step:

   ```bash
   # read the epoch the envelope must name (the signer defaults it to 1, which only the first
   # transition of a fresh stack can use)
   docker run --rm --network 01_docker_execution-net curlimages/curl:latest \
       -sS http://nautilus:9190/healthz     # -> {..., "gate_state": "...", "gate_epoch": N}

   # mints a signed GATE_HALT envelope against the current epoch and posts it
   python3 code/01_platform/04_scripts/t9_order_sandbox.py --sign-control halt \
       --operator saurabh --evidence "<CHG id or ticket>" --reason "<why, recorded in the log>" \
       --gate-epoch <N> --post
   ```

   The reply is the executor's own answer: `{"halted":true,"gate_state":"HALTED","halted_by":…,"gate_epoch":N}`.
   Confirm with `GET /healthz` — `gate_state` must read `HALTED` and `gate_epoch` must have advanced.
   A `401` means the envelope was rejected (usually a stale `gate_epoch`: fetch it from `/healthz` and
   re-mint); a `403` means the signed `operator` is not the configured one. Without `--post` the signer
   only prints the envelope, which is what a client other than this script should reuse.
2. Record reason, detection timestamp, affected attempts, actor/service, and evidence reference. (**Reservations REMOVED 2026-08-15, CHG-005.**)
3. Confirm new money-moving calls stop within five seconds.
4. Preserve attempts, request hashes, mappings, responses, offsets, fills, and logs.
5. Alert the owner and begin the matching reconciliation runbook.

Existing positions may remain monitored, but no new money-moving call is permitted while halted.

## Gate reconciliation and resume

1. Reconcile broker orders and Arrow REST responses.
2. Reconcile fills, order lifecycle, and positions.
3. Verify `instruction_id` ↔ `execution_attempt_id` ↔ `client_order_ref` ↔ `broker_order_id` mappings.
4. Resolve every `UNKNOWN` attempt through evidence-backed disposition. (**Reservations REMOVED 2026-08-15, CHG-005.**)
5. Verify Executor fencing and durable state.
6. Verify Fluss changelog offsets/continuity.
7. Verify Signal job and checkpoint health.
8. Verify projection completion and quarantine disposition.
9. Produce an evidence hash bound to the current gate epoch.
10. Require the single-operator (Saurabh, DEC-044) authenticated authorized approval of that same hash/epoch:

    ```bash
    python3 code/01_platform/04_scripts/t9_order_sandbox.py --sign-control approve \
        --operator saurabh --evidence "<the evidence hash produced in step 9>" --gate-epoch <N> --post
    # -> {"approved":true,"gate_state":"ENABLED","approved_by":"saurabh","gate_epoch":N}
    ```

    Trading is live only when `GET /healthz` reports `gate_state: ENABLED` **and** `trading_ready: true`.
11. Transition to `ENABLED` only after that approval.

## Stopping or restarting the executor

The executor drains for ~15 s before it exits: the Nautilus kernel waits up to 10 s for residual
events, then the HTTP task gets a 5 s window (`HTTP_DRAIN_GRACE_SECS`). The `nautilus` service declares
`stop_grace_period: 30s` for exactly that reason — **Docker's 10 s default kills the process mid-drain**
(measured 2026-09-14: exit `137`, no `clean shutdown complete` line, queued attempts abandoned with no
accounting).

```bash
docker compose --env-file code/01_platform/01_docker/.env --env-file code/01_platform/01_docker/secrets.env \
  -f code/01_platform/01_docker/docker-compose.yml --profile execution-t3 stop nautilus
docker inspect --format 'ExitCode={{.State.ExitCode}}' 01_docker-nautilus-1   # expect 0
docker logs 01_docker-nautilus-1 2>&1 | grep -E "clean shutdown complete|no clean-shutdown report"
```

A clean stop logs `clean shutdown complete: queued bridge jobs abandoned as unresolved attempts
unresolved_attempts=N gate_state=Halted`. `no clean-shutdown report` (or a non-zero exit) means the
process did not reach the end of the sequence: treat any attempt that was in flight as `UNKNOWN` and
follow *Unknown execution outcome* before restarting.

Two things the stop path guarantees, and one it does not:

- The sequence halts the gate itself before draining, so a stop is restart-safe even from `ENABLED`
  (live-verified 2026-09-14: approval → `ENABLED` → plain `docker stop` → exit 0, `gate_state=Halted`).
- Attempts that never reached the broker are counted as unresolved, not dropped silently.
- It does **not** wait indefinitely: a stop that outruns the 30 s grace is a SIGKILL, so do not force
  a stop during a live incident without reading the log line first.

## Execution_Gate recreate (v4 merge engine, CHG-122)

**Trigger**: applying CHG-122 — adopting `table.merge-engine=versioned` on `fence_token`.
`DdlApplyTool` only ever CREATEs and refuses a non-empty catalog, and Fluss 0.9.1 has no
ALTER, so an options change requires drop + create.

**Scope and severity**: planned maintenance. Gate KV fence/lease state is destroyed; the
audit carrier is not (Iceberg lake + the store's immutable audit log). Live money must
already be `HALTED` (DEC-044) before starting.

**Preconditions and safety posture**:

- Gate is `HALTED` for every partition scope and no money-moving call is in flight.
- The DDL declares the merge engine — `GateTableAdmin show` must print
  `table.merge-engine = versioned`, `table.merge-engine.versioned.ver-column = fence_token`
  and `table.delete.behavior = ignore`.
- `schema_manifest.json` is regenerated (`ddl_apply.py --force`) so apply-parity agrees.

**Procedure**:

1. Archive the lake prefix aside first — a recreate against a stale iceberg dir throws
   `LakeTableAlreadyExistException`:
   `bash code/01_platform/04_scripts/r2-move-prefix.sh lake/default/Execution_Gate/ lake/_stale-<date>/Execution_Gate/`
2. Confirm the descriptor the tool will apply:
   `java -cp "code/common/target/classes:<cp.txt>" GateTableAdmin show`
3. Drop and recreate from the DDL descriptor:
   `java -cp "code/common/target/classes:<cp.txt>" GateTableAdmin recreate <bootstrap>`
   The tool refuses to touch the cluster if the DDL no longer declares
   `merge-engine=versioned` with `ver-column=fence_token`.
4. Re-apply through the offline contract so manifest and parity agree:
   `make ddl APPLY=1 EVIDENCE=<file>`

**Validation and closure evidence**:

- Table options match the DDL (apply-parity + `CompatFlussDdlParityIntegrationTest`).
- A stale-token write is dropped and surfaces as an error, never as silent success.
- A halt still lands under a concurrent stale token.
- No table or database leak; the reactor is clean.

**Rollback / abort criteria**: recreate with the pre-CHG-122 WITH block and regenerate the
manifest. The v3 row contract is unchanged, so existing rows stay readable. Rolling back
reinstates the stale-writer clobber window — it is not correctness-neutral, so abort only
on a failed parity check, never merely on fence state being gone.

**Escalation and owner**: Platform Team.

Automatic resume and approval reuse across epochs are prohibited.

## Unknown execution outcome

1. Mark the attempt `UNKNOWN`; do not create a replacement attempt.
2. Halt the affected gate scope.
3. Query the broker through the evidence-approved reconciliation method using the durable client/broker reference.
4. If acceptance is proven, persist the `broker_order_id` mapping and reconcile fills/positions.
5. If non-acceptance is proven, record terminal evidence before retry/release according to policy.
6. If neither is proven, require manual disposition. (**Reservation-capacity clause REMOVED 2026-08-15, CHG-005.**)
7. Record immutable execution audit and follow the resume runbook.

## Broker market-data disconnect

1. Capture connection identity/epoch, affected manifest, last verified event/receive time, subscription acknowledgements, clock offset, and reconnect attempts.
2. Mark affected Ingestion readiness false.
3. Reconnect using the approved backoff/authentication policy and revalidate all required subscriptions.
4. Create suspected-discontinuity evidence; do not fabricate exact sequence gaps.
5. Do not perform inline historical backfill in MVP.
6. Suppress affected trading decisions unless an explicitly approved degraded mode proves safe.
7. Verify append acknowledgements, bounded backlog, and telemetry before ready.

## Flink job or checkpoint failure

1. Identify the affected job, checkpoint, source offsets, watermark, state size, failure, and sink status.
2. Halt the order path if state continuity or instruction correctness is uncertain.
3. Restore the compact checkpoint from the tested encrypted S3 checkpoint/savepoint (DEC-038: the checkpoint holds source offsets, watermarks, timers, in-flight windows, and working-cache metadata — not the full dedup set).
4. Verify Fluss authoritative-state availability and compatibility (dedup state table, candle/signal tables); rehydrate the dedup working cache from Fluss.
5. Verify window, forming-bar, and source state consistency; verify the Fluss dedup table holds the accepted set (checkpoint did not duplicate it). (**Ranking state REMOVED 2026-08-15, CHG-005.**)
6. Verify no duplicate immutable instruction and no unaccounted partial sink visibility.
7. If restoration or Fluss-state verification cannot be proven, remain not ready and execute the approved reset/replay procedure.
8. Reconcile before any gate resume.

## Fluss quorum, tablet, or workload VM loss

1. Record failed VM/tablet, leader/replica state, quorum, disk, backlog, and acknowledged-loss counters.
2. Halt the order path if changelog, durable state, or mappings are uncertain.
3. Verify replica placement and remaining quorum before restart/reassignment.
4. Restore Flink from S3 checkpoints as required.
5. Measure backlog recovery and data correctness at the current workload.
6. Verify recovery under the accepted RTO and run reconciliation before resume.

## Host power cut (unclean shutdown) recovery

> 2026-09-02: two power cuts in one day. Symptoms seen and what they mean:
> torn (zeroed-tail) Fluss segments crash-loop the tablet on recovery — one
> table at a time (`Signal_Candidates_current`, then
> `feature_candles_15s_preview`, then `forming_bar` each surfaced on the NEXT
> restart until repaired); orphans (stray faketool, ingestion JVM) survive a
> killed runner but not a reboot; a capture started minutes after reboot is
> the likeliest to be lost. Guarded since: `repair-tablet.sh --all`,
> stray-process kills in `pipeline_preflight`, and a 10-min minimum-uptime
> gate on measurement runners (`MIN_UPTIME_S`).

1. Stabilize: wait until the host is past its first flap (measurement runs
   refuse to start until `MIN_UPTIME_S`, default 600s, of uptime).
2. Repair Fluss BEFORE any drill/measurement — one command:
   `code/01_platform/04_scripts/fluss-repair/repair-tablet.sh --all`
   (dry-run first with `DRY_RUN=1`). The sweep pins the container restart
   policy off, scans every table, truncates only zeroed tails (last complete
   batch boundary, zero record loss), restores the policy, starts the tablet,
   and fails the run if it is still crash-looping.
3. Bring the stack up (docker compose up the Fluss + Flink containers; the
   ingestion/compute services stay host-side per the drill recipe).
4. Verify: `pipeline_preflight` (its 180s Fluss-readiness wait fails closed
   if anything is still torn) before any run.
5. Resume the interrupted activity (re-run the runner — it purges stale
   state itself).

Never: drop/recreate tables to "fix" a crash-loop (drops the history the
drill evidence relies on), or run a measurement while the tablet shows
`Restarting` in `docker ps`.

## Postback correlation or projection failure

1. Preserve the immutable postback and payload hash.
2. Write or verify `Postback_Quarantine` for missing/ambiguous identity or invalid schema/status.
3. Halt affected new order/action flow.
4. Inspect durable projection pending/completion state.
5. Retry lifecycle and position projections idempotently using source event/version.
6. Reject regressive terminal-state updates; conflicting evidence becomes `UNKNOWN`.
7. Verify `Order_Lifecycle`, `Positions`, and mappings before closure. (**Reservation impact REMOVED 2026-08-15, CHG-005.**)

## EOD offload failure

1. Mark the manifest unverified/retryable and emit a critical storage alert.
2. Extend or preserve Fluss retention for affected source data.
3. Capture table/schema version, date, source range, rows/bytes, checksums, commit state, S3 errors, and expiry margin.
4. Retry with approved bounded backoff.
5. Verify the Iceberg commit and manifest before marking success.
6. Do not expire the source day; maintain at least three complete trading days live.

## Observability outage

1. Confirm whether telemetry ingestion, storage, query, or alert delivery failed.
2. Preserve local structured logs and immutable execution audit.
3. Verify order safety using durable gate/attempt state, not a green dashboard.
4. If required safety evidence or alerting is unavailable, mark trading readiness false and halt according to policy.
5. Recover telemetry, replay buffered data where supported, and verify alert delivery before closure.

## SignalJob (compute) operations

Environment: the distributed SignalJob runs on the compose Flink cluster as a
`flink run -d` job. Pinned envs are REQUIRED at submit
(`requirePinnedLong` fails the job when missing):

| Env | Pinned value | Meaning |
| --- | --- | --- |
| `DEDUP_TTL_MS` | `60000` | dedup expiry TTL |
| `CANDLE_WINDOW_MS` | `15000` | candle aggregation window |
| `CHECKPOINT_INTERVAL_MS` | `10000` | checkpoint cadence |
| `CHECKPOINT_TIMEOUT_MS` | `30000` | checkpoint timeout |
| `MAX_CONCURRENT_CHECKPOINTS` | `1` | no concurrent checkpoints |

Additional required envs: `RAW_TABLE`, `CANDLE_LIVE_TABLE`, `CANDLE_CLOSED_TABLE`,
`SIGNAL_CANDIDATES_TABLE`, `SIGNAL_CURRENT_TABLE`, `FLUSS_BOOTSTRAP_SERVERS` (coordinator port `9123` —
the tablet's client port is `9124` and `9123` on a tablet is nothing),
`CHECKPOINT_DIR` (MUST include a scheme: `file:///tmp/p8-checkpoints`; a bare
path throws `StringIndexOutOfBoundsException` at `SignalJob.java:310`).
**RETIRED (candle-era, 2026-09-05 multi-timeframe cutover) — early-signal / preview path envs.**
The job no longer reads any of these keys (verified by literal census of
`SignalJobConfig.java`: zero `EARLY_SIGNAL_*`/`PREVIEW_*`/`FORMING_BAR_TABLE` reads):
`PREVIEW_TABLE`, `PREVIEW_ENABLED`, `PREVIEW_INTERVAL_MS`, `PREVIEW_TTL_MS`,
`EARLY_SIGNAL_ENABLED`, `EARLY_SIGNAL_RULE`, `EARLY_SIGNAL_CONFIRM_AFTER_MS`,
`SIGNAL_TENTATIVE_MARKERS_TABLE`, `FORMING_BAR_TABLE`. Their tables
(`feature_candles_15s_preview`, `forming_bar`, `Signal_Tentative_Markers`) were deleted
with DDLs 30/04/31. Preview-cadence work now rides the multi-timeframe path as live
snapshots in `candle_live` (DDL 32); the authoritative env surface is the
`SignalJobConfig` literal set (`CANDLE_LIVE_TABLE`, `CANDLE_CLOSED_TABLE`,
`MULTITF_ENABLED`, `MULTITF_LIVE_SNAPSHOT_INTERVAL_MS`, `MULTITF_SESSION_BYPASS`,
`MULTITF_SIGNAL_CONTEXT_ENABLED`) — defaults are deliberately not restated here.
`STATE_RECOVERY_PATH`/`ALLOW_FULL_REPLAY` are runtime-only and absent from
`.env`; the fail-closed A3.3 gate governs both (no normal launch path supplies
`ALLOW_FULL_REPLAY=true`).

### Start (normal RESTORE path)

1. Build: `mvn -f code/02_services/02_compute/pom.xml -pl . package` (jar lands
   at the path the launcher expects; the distributed cluster uses
   `/opt/flink/jobs/compute.jar`).
2. Copy the jar into the jobmanager container if rebuilt.
3. Submit with the pinned env set (exact working dev command):
   `docker exec -e RAW_TABLE=… -e CANDLE_TABLE=… -e SIGNAL_CANDIDATES_TABLE=… -e SIGNAL_CURRENT_TABLE=… -e STATE_RECOVERY_PATH=… -e CHECKPOINT_DIR=file:///tmp/p8-checkpoints -e FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 -e DEDUP_TTL_MS=60000 -e CANDLE_WINDOW_MS=15000 -e CHECKPOINT_INTERVAL_MS=10000 -e CHECKPOINT_TIMEOUT_MS=30000 -e MAX_CONCURRENT_CHECKPOINTS=1 01_docker-flink-jobmanager-1 flink run -d -c com.trading.compute.signaljob.SignalJob /opt/flink/jobs/compute.jar`
   (This is the normal RESTORE-mode command: `STATE_RECOVERY_PATH` names the
   previous run's last checkpoint and `ALLOW_FULL_REPLAY` is absent; the log
   line `signal-job: startup mode = RESTORE (restore=…, fullReplay=false)` at
   INFO confirms restore. The FULL_REPLAY break-glass variant sets
   `ALLOW_FULL_REPLAY=true` and omits `STATE_RECOVERY_PATH`.)
4. Health: verify via `ps`, log evidence (`Restoring job`, `Completed
   checkpoint`, sink `RUNNING`), and checkpoint continuity — NOT the hub
   readiness report, which false-alarms "NOT ready" while the job is healthy.

### Stop

Graceful: cancel via the Flink UI/REST with a savepoint/checkpoint capture, or
`flink cancel -s <target> <jobid>`. Never SIGKILL the jobmanager.

### Restart without losing state

Set `STATE_RECOVERY_PATH` to the PREVIOUS RUN's last checkpoint directory
(`file:///tmp/p8-checkpoints/<previous-jobid>/chk-N`). Scope any log grep to
new log lines after restart. Verify `Restoring job` appears, then completed
checkpoints advance.

Under DEC-038 (2026-08-14) the checkpoint is compact: it restores source
offsets, watermarks, timers, in-flight windows, and dedup working-cache
metadata. After restore, verify the Fluss authoritative state (dedup state
table, candle/signal tables) is reachable and compatible, and that the dedup
working cache rehydrates from Fluss — the checkpoint is not a second complete
copy of the durable Signal business state.

**JM/TM container recreate destroys container-local `/tmp` checkpoints** (not on a
named volume; 2026-08-11 incident: a `docker cp` preserve raced live checkpointing,
captured a metadata-only checkpoint, and the job had to FULL_REPLAY). Before any
`docker compose up -d flink-jobmanager flink-taskmanager`:
1. Stop the job (`flink cancel` — a running job keeps subsuming checkpoints while
   you copy, so the preserved snapshot may be incomplete).
2. Preserve `docker cp 01_docker-flink-jobmanager-1:/tmp/p8-checkpoints/. <host>`.
3. After recreate, copy back and `docker exec -u root … chown -R flink:flink
   /tmp/p8-checkpoints` (docker cp restores as root; flink cannot create the
   checkpoint store otherwise).
4. Re-deploy the jar (`docker cp compute.jar …:/opt/flink/jobs/` — also
   container-local) and resubmit with `STATE_RECOVERY_PATH` pointing at the
   preserved `chk-N`. Restore-only submit must NOT set `ALLOW_FULL_REPLAY`
   (A3.3 gate rejects the combination).

## Replay incident (SignalJob)

Trigger: an approved FULL_REPLAY is started, or a restart is forced to replay
history (`ALLOW_FULL_REPLAY=true` accepted via A3.4).

1. Confirm the A3.4 WARN log line: `signal-job: startup mode = FULL_REPLAY
   (restore=false, fullReplay=true)` — a full replay without this line is a
   configuration error.
2. Record JobID, artifact hash, checkpoint directory, and the replay reason
   (approved operator step).
3. The storm test (2026-08-11, job `5f41f0c5`, 204,800 ticks) proves a replay
   produces zero alert storms: checkpoints complete (0 failed), and the
   schema-rejection rules do not fire on valid historical data.
4. Verify the replayed candle/sink counts match the deterministic expectation
   for the input snapshot; candidates = 0 on replay is deterministic (no
   detection during historical catch-up).
5. Closure: replay drained, checkpoints advancing, no unintended alert fires.

## Checkpoint failure (SignalJob)

Trigger: `SIGNAL-crit-checkpoint-failed` (failed checkpoints > 0) or
`SIGNAL-error-checkpoint-slow` (duration >= 24000 ms = 80% of the pinned
30000 ms timeout).

1. Capture JobID, failed checkpoint count, last completed checkpoint, source
   offsets, state size, and sink status.
2. Verify the job is still producing completed checkpoints; one transient
   failure restarts the task automatically (configured restart strategy).
3. If checkpoints stop completing: restore from the last good checkpoint
   (`STATE_RECOVERY_PATH` = `file:///tmp/p8-checkpoints/<jobid>/chk-N`), never
   from a checkpoint at/after the failure.
4. `allowNonRestoredState` is forbidden — the restore must be exact.
5. Verify dedup, window, forming-bar, and source state consistency, then resume
   normal operation. Closure: checkpoints advancing, 0 failed, alert recovered.

## Schema-preflight failure

Trigger: DDL/version preflight blocks startup (validation/contract gate).

1. Capture the failing table, expected vs actual schema, and version matrix
   state.
2. Check the table contract fields: PK, routing, bucket count, and the full column/type/nullability set (`TableContractValidator`, re-targeted by the 2026-08-13 re-scope to `feature_candles_15s` KV PK exactly `(instrument_token, window_start)` + `Signal_Candidates` LOG + `Signal_Candidates_current` KV PK `[instrument_token]` — SIGNAL-SCHEMA-001, implemented).
3. Do NOT bypass the gate; reconcile the DDL/schema with the manifest
   (`ddl_apply.py --force` regeneration must be byte-identical) and re-run.
4. Closure: preflight passes, job starts in the intended mode.

## Migration conflict (CandleMigrationTool) — RETIRED

> **RETIRED (2026-08-13):** `CandleMigrationTool` is decommissioned with the candle KV projection (requirement change — `feature_candles_15s` is now the KV-only sole candle output). The procedure below is retained as the historical record of the implemented candle-KV migration conflict handling.

Trigger: `CandleMigrationTool` audit/load exits 2 (unaccepted conflicts) or 1
(accept-list entries match no canonical key).

1. Read the stdout fields: `ACCEPT_KEYS_FILE`, `ACCEPTED_KEYS`,
   `UNACCEPTED_KEYS`, `ACCEPT_KEYS_NOT_FOUND`.
2. Re-run the read-only audit; every conflict must be on the operator-approved
   accept list (`CANDLE_MIGRATION_ACCEPT_KEYS_FILE`: `token,windowStart` per
   line, `#` comments allowed) or the load fails closed.
3. Accepted keys merge by `MAX(output_ts)` (last-write-wins — the same
   convergence the live KV sink applies); unknown conflicts abort.
4. Entries matching no canonical key abort exit 1 (typo/stale list).
5. Closure: `UNACCEPTED_KEYS=0`, `NOT_FOUND=0`, exit 0, DEST_ROWS_AFTER ==
   DISTINCT_KEYS.

## Fluss coordinator/tablet failure (compose)

Trigger: `SIGNAL-error-flink-jm-scrape-down` / `SIGNAL-error-flink-tm-scrape-down`
(collector cannot scrape), or `up == 0` for a Fluss service, or
`SIGNAL-crit-taskmanager-down`.

1. Record the failed service, leader/replica state, and quorum.
2. Compose: `docker compose up -d --force-recreate <svc>` (coordinator/tablet);
   Fluss data persists in named volumes.
3. Verify the coordinator (`fluss-coordinator:9123`) and each tablet are
   reachable from the jobmanager container before restarting the job.
4. If the SignalJob lost its Fluss connections, restart it from its last
   checkpoint (see SignalJob restart) — never from offset 0 without the
   approved replay path.
5. Closure: scrapes return to 1, checkpoints advance, alerts recover.

## Rollback (candle LOG→KV replay, CANDLE-KV-REPLAY-001) — RETIRED

> **RETIRED (2026-08-13):** the candle dual-sink this runbook rolls back is retired (`feature_candles_15s` is now the KV-only sole candle output). The registry and chk-1539 cutoff below are historical dev-rehearsal evidence. The re-scope target is the SIGNAL dual-sink rollback runbook (`Signal_Candidates` LOG + `Signal_Candidates_current` KV), delivered by tracker 14 P10 (`docs/08_implementation/09-production-swarm.md`).

Exact dev-rehearsed registry (2026-08-10; retired candle era, absorbed into the
master dossier `docs/08_implementation/04-signal-job.md` §Absorbed documents — the
13 file was deleted 2026-08-17):

| Job | Graph | Restore target | Last checkpoint |
| --- | --- | --- | --- |
| `0417068d` | pre-cutover single-LOG | — | chk-1538 (rollback restore target) |
| `87c48642` | dual-sink cutover | chk-1538 | chk-1732 (re-cutover restore target) |
| `4527918b` | rollback rehearsal (single-LOG, KV sink stripped, restore wiring + candle-contract preflight kept) | chk-1538 | chk-1572 |
| `92104dac` | current re-cutover | chk-1732 | chk-1827+ |

Tables: `feature_candles_15s` (LOG, never touched by rollback) and
`feature_candles_15s_current` (KV — writes freeze on rollback).
**Rollback cutoff = chk-1539** (the FIRST dual-sink checkpoint): restoring
any checkpoint at/after it into a single-LOG graph leaks KV state.

Rollback procedure:

1. Build the single-LOG artifact from CURRENT source with the KV-sink operator
   stripped (restore wiring + candle-contract preflight kept). Verify via
   `javap`: 0 kv-sink refs, restore + preflight present. The old pre-restore
   jars lack `STATE_RECOVERY_PATH` support and would offset-0 full-replay.
2. Submit with `STATE_RECOVERY_PATH=<pre-cutover job's last checkpoint>` =
   `file:///tmp/p8-checkpoints/0417068d…/chk-1538` (dev) — NEVER a checkpoint
   >= chk-1539.
3. Verify: KV frozen (key count constant), LOG continues to grow, checkpoints
   <= 30 s, `Restoring job` + completed checkpoints in the log.
4. Re-cutover (when safe): restore the dual-sink job from ITS OWN last
   checkpoint (`87c48642…/chk-1732`), not the rehearsal run's checkpoints
   (single-LOG graph → dual-sink restore fails).
5. Closure: KV count resumes growing, LOG/KV diverge only by design, alerts
   clear.

## Telemetry retention and data lifecycle

OpenObserve v0.91.5 retention contract (`docs/04_contracts/openobserve.md`):
logs 30 days, metrics 90 days, traces 14 days, alert definitions 180 days.

- Mechanism (source-verified, `src/service/compact/retention.rs`): per-stream
  `data_retention` (days) overrides the global
  `ZO_COMPACT_DATA_RETENTION_DAYS` default (3650 = 10 years). New metric
  streams inherit the global until the provisioning sync re-runs.
- Applied 2026-08-11 via `o2-provision.py` (idempotent `provision_retention`):
  all logs streams = 30, all 335 metric streams = 90. Re-run the provisioner
  after any new metric family appears.
- Alert rules are NOT streams: definitions + trigger history live in the O2
  meta store (`metadata.sqlite`), not subject to stream retention.
- The policy-controlled money-moving audit retention is a SEPARATE S3/object-store
  control (EOD/lake tier) and must NEVER be attributed to OpenObserve.
- When retention changes are needed: `PUT /api/{org}/streams/{stream}/settings`
  with `{"data_retention": N}` (partial update; other settings survive);
  minimum allowed is 3 days.

## Alert response catalogue

All rules route to the `dev-webhook` destination in dev (receiver logs:
`docker logs 01_docker-webhook-receiver-1 | grep 'POST /noop'`). O2 v0.91.5 v2
alerts have no first-class severity field — severity rides the rule-name prefix
(`SIGNAL-crit`/`error`/`warn`, `ING-crit`/`warn`). Realtime rules evaluate
continuously; while a condition holds, fires repeat on the ~30–75 s window
cadence. **24 rules provisioned (8 ING- ingestion + 16 SIGNAL- compute), verified live 2026-08-17** (the prior "26 = 9 + 17" count included the never-existing `SIGNAL-warn-dedup-cache-hit`; the dedup pair was re-provisioned the same day — see the banner below).

Compute/SignalJob rules:

> **DEDUP ALERTS RETARGETED (2026-08-17, CHG-022 / DEC-040):** the dedup set is
> authoritative Flink keyed state (Design B) — the `fingerprint_dedup` Fluss
> table and the bounded-cache gauges no longer exist. The two real rules
> (`SIGNAL-warn-dedup-state`, `SIGNAL-warn-dedup-expiry`) were re-provisioned the
> same day to watch the Flink `FingerprintDedupFunction` gauges
> (`compute_dedup_state_count` / `compute_dedup_expiry_index_count` — the series
> Design B keeps) with the threshold re-based from the DEC-038 250k cache cap to
> the **Design-B envelope ≈ 6.5M** (20 480 t/s × 300 s TTL ≈ 6.1M + headroom).
> **CHG-023 item 2 (2026-08-17): `SIGNAL-warn-dedup-expiry` is RETIRED — its
> series (`compute_dedup_expiry_index_count`) is deleted with the expiry index;
> expiry is native `StateTtlConfig` (no event-time timers to stall), and
> `SIGNAL-warn-dedup-state` covers the same envelope on the live-set count.**
> The old `SIGNAL-warn-dedup-cache-hit` row is DELETED — no such rule exists in
> O2 (verified live 2026-08-17) and the cache gauges it watched were removed by
> CHG-022. The retired Fluss table (`fingerprint_dedup`, DDL
> `24_fingerprint_dedup.sql`) is retained on file but unused; no rule watches it.

| Rule | Severity | Condition | Response | Recovery |
| --- | --- | --- | --- | --- |
| SIGNAL-crit-checkpoint-failed | Critical | failed checkpoints > 0 | Checkpoint failure runbook | checkpoint completes, count resets |
| SIGNAL-error-checkpoint-slow | Error | duration >= 24000 ms | Checkpoint failure runbook | duration back under 80% of timeout |
| SIGNAL-error-job-restarting | Error | restarts > 0 | Check flink_logs for the restart cause | restarts stop |
| SIGNAL-error-source-stalled | Error | source rate == 0 (2 min) | Check feed/bridge; **false-fires on quiesced dev feed** | feed resumes |
| SIGNAL-warn-kv-sink-zero | Warning | kv-sink rate == 0 (2 min) | Check KV sink task; **false-fires on quiesced dev feed** | sink writes resume |
| SIGNAL-warn-dedup-state | Warning | Flink dedup-state entries > 6.5M (`compute_dedup_state_count`; Design-B envelope ≈ 6.1M = 20 480 t/s × 300 s TTL + headroom) | Check accepted-rate × TTL math vs the envelope; state grows only when first-seen rate exceeds the envelope — no cache/cleanup pass to tune (Design B, CHG-022) | count drops back under envelope as expired entries' event-time timers fire |
| ~~SIGNAL-warn-dedup-expiry~~ | ~~Warning~~ | ~~Flink dedup expiry-index entries > 6.5M (`compute_dedup_expiry_index_count`)~~ — **RETIRED 2026-08-17 (CHG-023 item 2): the expiry index is deleted — expiry is native `StateTtlConfig`; `SIGNAL-warn-dedup-state` covers the envelope on the live-set count** | — | — |
| ~~SIGNAL-warn-dedup-cache-hit~~ | ~~Warning~~ | ~~dedup cache hit ratio below threshold (60 s)~~ — **DELETED 2026-08-17: no cache in Design B (CHG-022); rule never existed in O2** | — | — |
| SIGNAL-warn-schema-rejected-rate | Warning | rejects per flush > 10 | Check raw_table_1 schema vs validator | rejects stop |
| SIGNAL-crit-schema-version-rejected | Critical | any schema-version reject | Schema-preflight runbook | zero rejects |
| SIGNAL-crit-full-replay-started | Critical | startup mode == FULL_REPLAY | Replay incident runbook (fires only when the emitter ships in production) | replay drains, job runs |
| SIGNAL-error-flink-jm-scrape-down | Error | `up{instance=flink-jobmanager:9249} == 0` (2 min) | Check JM + collector scrape | scrape resumes |
| SIGNAL-error-flink-tm-scrape-down | Error | `up{instance=flink-taskmanager:9249} == 0` (2 min) | Check TM + collector scrape | scrape resumes |
| SIGNAL-warn-jvm-heap-high | Warning | JM heap >= 900 MB | GC/restart; check task slots | heap drops |
| SIGNAL-crit-taskmanager-down | Critical | registered TMs < 1 | Fluss/Flink cluster runbook | TM registers |
| SIGNAL-warn-scrape-slow | Warning | scrape duration >= 1 s | Reporter load check | duration drops |
| SIGNAL-warn-source-lag | Warning | event-time lag >= 600 s | Replay incident / source-stall check; **fires on stopped dev feed** (baseline ~244 s with the historical-timestamp dev feed) | feed resumes / replay drains |

Ingestion rules (existing): ING-crit-telemetry-delivery-failed (collector
export failures persist; degraded-delivery signal — absence-based conditions do
NOT fire, see the collector-outage limitation), ING-crit-fd-90,
ING-warn-fd-80, ING-crit-orphan-process, ING-warn-reconnect-streak-5,
ING-warn-capacity-80, and the slot/health rules defined in
`02-ingestion-alerting.md`.

Collector/O2 outage limitation (measured 2026-08-11): stopping the collector
2.5 minutes produces ZERO alert fires — O2 cannot alert on data it is not
receiving. Coverage for full outage = compose/supervisor restart policy +
`ING-crit-telemetry-delivery-failed` (degraded delivery while the collector is
up).

## Security or credential incident

1. Halt affected order flow.
2. Preserve access, gate, network, and execution evidence.
3. Revoke/rotate compromised credentials using `../05_deployment/04-secrets-rotation.md`.
4. Validate least privilege, transport, redaction, and unauthorized-control attempts.
5. Reconcile and require single-operator (Saurabh, DEC-044) resume.

## Execution service runbooks (gateway / nautilus / bridge) — WP-7

> **Topology:** `execution-gateway` (Java, `trading-net` + `execution-net` bridged) forwards
> private `GatewayProtocol` envelopes to `nautilus` (Rust, `execution-net` only, `POST /v1/intents`
> on `0.0.0.0:9190`). `execution-bridge` (Go, `execution-net` + `arrow-egress`, `0.0.0.0:8787`)
> is the only `arrow-egress` member (T3). All three have zero host `ports:` (T8 gate) and
> boot `HALTED`/`disabled` (`EXECUTION_ENABLED=false`). No `ARROW_*` reaches the Rust path
> (T4 boundary, `t8_sandbox_contract_check.py` + `execution_network_check.py`).

### Execution gateway (Java) — Fluss bootstrap / IntentReader halt

**Signals:** container `Up` then `Exited` with `IllegalConfigurationException: No resolvable bootstrap urls`
or `MemoryUtil` `InaccessibleObjectException` (`--add-opens` missing); later `invalid Execution_Intent at offset N: unsupported schema_version` in `execution-intent-reader` thread; `GET /healthz` 200 `{"healthy":true}` but `GET /readyz` 503 `{"flussReady":false}`.

**Evidence:** `docker logs 01_docker-execution-gateway-1`, `docker inspect` networks (`trading-net` + `execution-net`), `docker ps` (no ports), `FLUSS_BOOTSTRAP` env (`fluss-coordinator:9123` via `trading-net` DNS), `versions.pin` Go/Rust pins, `t8`/`execution_network_check` output.

**Gate action:** gateway never moves money while `flussReady`/`protocolReady`/`durableWriteReady` are false — `IntentReader.poll` marks `readiness.fail` and the reader thread halts; the HTTP server stays up for probe but `/v1/events` returns 503 `gateway not ready`. No execution intent is forwarded to nautilus while halted.

**Mitigation:**
1. `No resolvable bootstrap urls` → gateway is not on `trading-net` or Fluss is down. Verify `networks: [trading-net, execution-net]` in `docker-compose.yml` and that `fluss-coordinator:9123` resolves from the gateway net (`docker run --rm --network 01_docker_trading-net curlimages/curl -s http://execution-gateway:9180/healthz`). Restart Fluss first, then gateway (`restart: unless-stopped` will retry).
2. `MemoryUtil` / `java.base/java.nio=ALL-UNNAMED` → gateway was started without `--add-opens`. The image `ENTRYPOINT` must be `java --add-opens=java.base/java.nio=ALL-UNNAMED -jar /app/execution-gateway.jar` (matches `01_ingestion/docker-entrypoint.sh`). Rebuild with the pinned `Dockerfile`.
3. `unsupported schema_version` at offset 0 → `Execution_Intent` Fluss table contains a row with a schema version the gateway does not understand. Do not bypass the gate — reconcile the writer (common projection) and the table DDL/manifest, then re-run `ddl-apply` if the table was created with an old schema. The gateway stays halted until a valid envelope is seen.

**Recovery:** after Fluss and the `Execution_Intent` table are healthy, the gateway's `readiness` flips to `flussReady true` (poll `GET /readyz` until 200). No manual fund movement.

**Closure evidence:** `GET /healthz` 200, `GET /readyz` 200 `{"executionReady":true}`, `docker logs` shows no `execution intent halted`, `t8` 12/12 PASS.

### Nautilus execution service (Rust) — gate HALTED / private intents

**Signals:** `nautilus:9190/healthz` 200 `{"gate_state":"HALTED","trading_ready":false,"enabled":false}` (expected default); `readyz` 200 `{"ready":true,"gate_state":"HALTED"}` while running, 503 when `draining`; `POST /v1/intents` with a valid `GatewayProtocol` envelope returns 503 `{"accepted":false,"reason":"gate HALTED","gate_state":"HALTED"}` (fail-closed, correct); bad auth returns 401 `authentication failed`; `GET /v1/intents` returns 405; unsigned `POST /v1/approve` / `POST /v1/halt` return 401 — only a signed `GATE_APPROVE` / `GATE_HALT` envelope naming the current `/healthz` `gate_epoch` performs a control action (P3-020, recipe in `docs/commands/COMMANDS.md` §B).

**Evidence:** `docker logs 01_docker-nautilus-1` (`boot: gate HALTED, bridge mode fake|http, ...` — CHG-079: bridge mode reflects `BridgeSelection`; `http` means the production `HttpBridgeClient` is wired and **must** have `BRIDGE_AUTH_TOKEN` matching the bridge's `EXECUTION_BRIDGE_AUTH_TOKEN`, else the `/healthz` probe 401s), `docker inspect` env (`EXECUTION_ENABLED=false`, `GATEWAY_SHARED_SECRET` is set but never logged, `EXECUTOR_LISTEN_ADDR=0.0.0.0:9190`, `BRIDGE_ENDPOINT=http://execution-bridge:8787`, `BRIDGE_AUTH_TOKEN` present), `cargo test --offline` pass + `gateway_protocol` round-trip, `t8` 12/12, private probe via `docker run --rm --network 01_docker_execution-net curlimages/curl -s http://nautilus:9190/healthz` and the `POST /v1/intents` envelope probe (Rust `gateway_protocol::encode_envelope` helper).

**Gate action:** the Rust service **always** boots `HALTED` (since P3-020 the DEC-044 enable is only accepted as a signed `GATE_APPROVE` envelope naming the current `gate_epoch`, so an unauthenticated caller cannot enable or halt the gate); `/healthz` `trading_ready` is always false while halted. No broker command is emitted while halted, and a 503 on valid intents is the correct evidence that the private route is wired but not trading. Do not set `EXECUTION_ENABLED=true` — the config rejects it at boot (`must not be true at boot`).

**Mitigation:** if `healthz` reports `ENABLED` without a single-operator (Saurabh, DEC-044) approval, treat as incident and halt/safety-halt. If `POST` returns 404, the T4 `http.rs` `POST /v1/intents` route is not deployed — rebuild `nautilus` from the pinned `rust:1.97.1` image with the `gateway_protocol` + `http` changes. **Stale-image guard (CHG-101, content-addressed since CHG-124):** `make check-image-stale` fails when a compose `build:` image does not carry the content stamp of its inputs (the 08-20-vs-08-24 gateway/bridge case: the stale jar answered readyz 200 with fail-open semantics) — run it before any enable, and rebuild with `make images`, which stamps every image and re-verifies it. Images built before CHG-124 have no stamp and fall back to the timestamp comparison, which says so in its verdict.

**Recovery:** no recovery to `ENABLED` without the T5 `LiveNode` fence/approval path (deferred). For T8, the 503 on valid envelopes **is** the success condition.

**Closure evidence:** `healthz` HALTED, `readyz` 200, `POST` valid envelope 503 HALTED + `POST` bad auth 401, `GET` 405, no `ARROW_*` in `nautilus` env (`docker inspect`), image `01_docker-nautilus` built from `rust:1.97.1` digest-pinned toolchain and `EXECUTION_BRIDGE_GO_IMAGE` digest-pinned Go builder.

### Execution bridge (Go) — disabled / fake mode

**Signals:** `bridge:8787/healthz` 200 `{"status":"UP","mode":"disabled","credentials_in_process":false}`; `docker ps` shows `01_docker-execution-bridge-1` `healthy` with no host port; `execution-bridge` is the only `arrow-egress` member (`docker compose --profile execution-t3 config` shows `arrow-egress: null` only for the bridge).

**Evidence:** `docker logs 01_docker-execution-bridge-1` (`listening addr=0.0.0.0:8787 mode=disabled`), `docker inspect` networks (`execution-net`, `arrow-egress`), `go test -race ./...` in `06_execution_bridge/go-bridge`, `versions.pin` `EXECUTION_BRIDGE_GO_IMAGE=golang:1.24.5-alpine@sha256:daae04e…` digest.

**Gate action:** bridge in `disabled`/`fake` never reaches Arrow. `EXECUTION_BRIDGE_MODE` defaults `disabled` (never `live`). No host `ports:` and no `ARROW_*` env on the bridge except the market-data `ingestion` exception (verified by `execution_network_check.py`).

**Recovery:** no live order path while `mode=disabled`. For T8, the `UP disabled` health is success.

### Retention and audit — policy approval (WP-7 T0)

**Baseline:** audit and money-moving evidence (execution, order, fill, gate, correlation, approval, reconciliation, future position-action) is retained **at least one year** (CHG-038/DEC-043, `SAURABH-1Y-APPROVAL-2026-08-20`). Longer retention applies when the approved jurisdictional, contractual, legal-hold, or operational policy requires it. Storage is Cloudflare R2 with WORM-equivalent bucket lock (`audit_r2.py provision --set-lock`), not S3 Object Lock.

**Current posture — APPROVED 2026-08-20 by Saurabh (owner, this turn):** the one-year baseline `SAURABH-1Y-APPROVAL-2026-08-20` placeholder is now **approved** by Saurabh for the current scope. Evidence bundles `t0-…`/`t4-…` `retention_policy` remains `SAURABH-1Y-APPROVAL-2026-08-20` (commits `4db6616`/`b093d97`); under DEC-044 the evidence review requirement is satisfied by the single `saurabh` review (the legacy `two_person_review` field for the T0 bundle is satisfied by that review alone — a second reviewer is not required and not checked). Longer retention remains policy-driven per CHG-038/DEC-043.

**Runbook now:** keep the approved baseline (≥1y), preserve all evidence bundles sanitized (no secrets) with `t*-SHA256SUMS`, and keep the R2 bucket lock provisioning validated (`audit_r2.py validate` with `CLOUDFLARE_API_TOKEN`/`CLOUDFLARE_ACCOUNT_ID`). Do not expire source data while any EOD manifest is unverified/retryable/under reconciliation (EOD controller owns retention extension). If a future jurisdictional change requires longer retention, record a new approval ID in `versions.pin`/`evidence.json` and a superseding `CHG` with single-operator (Saurabh, DEC-044) review.

## Position State — stuck ACTIVE (indefinite block) — RB-POS-001

**Scope:** instrument_token (16 buckets, Fluss `Position_State` KV PK exactly `[instrument_token]`, `bucket.key=instrument_token`). **Severity:** Warning → Critical if trading blocked. **Owner:** Trading Ops + Execution Gateway owner (Nautilus). **Gate impact:** SignalJob suppresses next signal for that instrument forever until `Position_State.status=CLOSED` or `ADMIN_CLEAR` arrives (no TTL, `ValueState` survives restarts/checkpoints). Intentional stuck = liveness cost for correctness.

**Preconditions:** `ActiveSignalFeedbackFunction` is live (`SignalJob` CoProcess: Input1 signals, Input2 `Position_State` changelog `OffsetsInitializer.full()`). `TableContractValidator` passed (PK/bucket/schema). `flink-metrics-otel` -> `otel-collector:4318/v1/metrics` -> `metrics` + `ComputeAlertLogs` -> `trading_alerts` healthy.

**Signals and evidence queries:**
- Dashboard `Position State — Max-One-Active` (4 panels): gauge `compute.signal.active.count` stuck high; bar `dropped` rate high + `passed` zero for instrument; `cleared` zero for 30m+; table `trading_alerts` shows no `active-signal-cleared` for that token.
- SQL: `SELECT _timestamp, event, severity, body FROM "trading_alerts" WHERE event='active-signal-cleared' ORDER BY _timestamp DESC LIMIT 100` (normal CLOSED) and `WHERE event='active-signal-admin-clear'` (break-glass audit).
- SQL: `SELECT histogram(_timestamp) AS x, count(*) AS y FROM "metrics" WHERE metric_name LIKE '%compute_signal_dropped_active_exists%' GROUP BY x` and `'%compute_signal_active_count%'`.
- Alerts: `pos-state-high-active-count` (gauge >100 10m), `pos-state-high-dropped-rate` (>5/s 5m), `pos-state-admin-clear-break-glass` (realtime on `trading_alerts`).
- Lake truth (exact): `SELECT instrument_token, status, position_id, updated_ts, closed_ts, closed_reason FROM Position_State WHERE status='OPEN'` — count is authority (gauge is best-effort per-subtask, drifts after restore).

**Immediate containment:** no gate halt — this is per-instrument suppression by design. Do NOT flush Flink state or restart to clear (state is checkpointed and would restore). Confirm `SignalJob` checkpoints complete and no `Position_State` feedback lag (Fluss reader lag <5s).

**Diagnostic sequence:**
1. Confirm stuck token(s): correlate dropped log `active-signal-feedback: dropping … wait CLOSED` for `instrument_token` with gauge token detail (if top-20 sampler includes it, else lake query). Verify `Position_State` row for that token is `OPEN` or missing (no CLOSED yet) — if `CLOSED` exists but Flink still blocks, Fluss changelog subscription (`OffsetsInitializer.full()`) may be lagging; check Fluss TabletServer and Flink `numRecordsIn` for input2.
2. Determine why CLOSED never arrived: Execution Gateway (Nautilus) → `Position_State` write path failed, Nautilus crash, or position truly still open (correct block). Check `execution-gateway` + `nautilus` logs for `Position_State` append errors and `Signal_Candidates` → `Execution_Intent` → Nautilus → fill/close path.
3. If position is verifiably closed at broker but Nautilus failed to write `CLOSED`, treat as feedback-loss incident (reconcile `Positions`/`Order_Lifecycle` before any clear).
4. If position is still open at broker, block is correct — no action, monitor `closed_ts`.

**Reconciliation before clear:** verify `Positions` and broker order state for that `instrument_token` agree that no position is open; verify no duplicate close pending. Record ticket, approver (single-operator Saurabh, DEC-044), and evidence hash. **Never clear an actually-open position.**

**Recovery (ADMIN_CLEAR break-glass):**
1. Obtain approved ticket; this path requires explicit approval (audit trail in `trading_alerts`).
2. Write exactly one row to `Position_State` with the instrument's key:
   ```sql
   INSERT INTO Position_State (instrument_token, status, position_id, updated_ts, closed_ts, closed_reason, schema_version)
   VALUES (<instrument_token>, 'ADMIN_CLEAR', NULL, <now_ms>, <now_ms>, 'RB-POS-001 ticket=<id> reason=<text>', 'v1');
   ```
   Writer MUST be the Execution Gateway path (owner of the table); direct Fluss writes are break-glass only and must still be replicated to the lake (lake enabled, 5m Iceberg commit). `ADMIN_CLEAR` follows the same `STATUS_ADMIN_CLEAR` branch as `CLOSED` in the CoProcess and decrements the gauge.
3. Wait for changelog delivery: Flink log `active-signal-feedback: ADMIN_CLEAR … clearing active` and metric `compute.signal.cleared.admin` + `trading_alerts` `WARN active-signal-admin-clear` within 30s (depends on Fluss commit + changelog poll). If not seen in 2m, check Fluss coordinator/TabletServer and Flink input2 lag.
4. Verify next signal for that token can now pass: dashboard `passed` increments on next trigger; or force a test signal in dev.

**Validation and closure:** gauge drops by 1 for that bucket; `Position_State` lake shows `ADMIN_CLEAR` with `closed_reason` ticket; `trading_alerts` contains exactly one `active-signal-admin-clear` with token/clearedActive/activeCount; no duplicate `CLOSED`/`ADMIN_CLEAR` rows for same token. Close ticket with: token, `instrument_token`, previous active candidate id, previous `OPEN` `updated_ts`, new `ADMIN_CLEAR` `closed_ts/reason`, and Flink checkpoint id after clear. Escalate if clear did not propagate — do NOT retry blindly; check checkpoint/Savepoint consistency.

**Rollback/abort:** if post-clear verification shows the position was actually still open (broker reports open), immediately open a Critical incident, halt new signals for that scope if needed, and reconcile `Positions` — the clear cannot be undone except by waiting for the next natural `OPEN`→`CLOSED` cycle (the gauge will go ACTIVE again on next signal). `ADMIN_CLEAR` is idempotent per key (second `CLOSED`/`ADMIN_CLEAR` logs "no active to clear").

## References

- Operational requirements: `../02_requirements/06-operational.md`
- Executor contract: `../04_contracts/07-executor.md`
- Rollback and recovery: `../05_deployment/03-rollback.md`
- Alert definitions and thresholds: `../08_implementation/10-observability.md`

## Bench/load tools: faketool "dies" mid-run (PERF-AUDIT-001, 2026-08-26)

**Symptom:** `bench-throughput.sh` reaches `container health: healthy` + full
`subscription_ack acknowledged=1024`, then measurement windows produce 0 rows
and the journal shows `connection refused` on `127.0.0.1:8899` shortly after
the first ticks. The faketool log stops without `read end` or `send failed`.

**This is a checklist, not a single cause — three independent causes were
found on one day. Check them in order:**

1. **Fake-broker auth mode missing** — the bridge must not AutoLogin against
   faketool. If the journal shows `AutoLogin failed ... user not found`
   (HTTP 400) and the bridge exits 2, `ARROW_FAKE_BROKER=1` is not set in
   `docker-compose.soak.yml`. Add it (test-only mode: skip AutoLogin, fake
   token). See CHG-111.
2. **OTel endpoint on the gRPC port** — the javaagent speaks HTTP/protobuf, so
   `OTEL_EXPORTER_OTLP_ENDPOINT` MUST be `http://<collector>:4318`, never
   `:4317` (gRPC). Symptom: repeated
   `io.opentelemetry...HttpExporter - Failed to export logs ... EOFException:
   \n not found` in the ingestion logs, and 0 ticks appended even though the
   bridge subscribed. See CHG-111.
3. **Process-group kill by the launching harness** — if both the bench AND
   its faketool child die together (0.6s apart, no `teardown` line in the
   bench log), the bench was killed as a process group. `nohup ... &` from an
   interactive/agent shell does NOT protect children. Launch long load/bench
   jobs with `setsid` (or the harness's background-task launcher, e.g.
   `bg_run`), and verify with a monitor that the child survives across
   separate shells before trusting a 30-min run.

**Fast triage (2 minutes):**
1. `docker logs <ingestion-container> | grep -c EOFException` → 0 = OTel ok
   (cause 2 ruled out).
2. Journal contains `AutoLogin failed` / `user not found` → cause 1.
3. Both bench + faketool died at the same second, no teardown → cause 3.
4. If all three are clean, reproduce on the host (faketool + bridge, no
   container) — if it works, the difference is the container/launcher path.

**Proof the load path is healthy (before a long run):** run
`bench-throughput.sh` and require ALL of: healthy container, 1024 ack,
non-zero baseline `append_latency_ms_count`, 0 EOFException, AND the bench
finishing all 3 windows with `rows >= 0.90 x 20480 x elapsed_s`. This is the
smoke gate before any 30-min PERF-AUDIT run.

The row floor is derived, not fixed (P6-001): the bench generates 20,480
frames/s (1024 subscribed ids x 20 Hz) and a window passes at >= 90% of that
rate over the window length it actually measured (P6-032). The old flat
`rows >= 15000` was an effective 250 rows/s — about 2% of the offered load —
so a PASS said almost nothing. The rate, the share and the window length are
overridable for a smoke run: `BENCH_EXPECTED_RPS`, `BENCH_MIN_RATE`,
`BENCH_WINDOW_S`, `BENCH_WINDOWS`. A window whose latency cannot be read at
all now fails rather than reporting `p99=-1` and passing (P6-033).

## Bench journal: three ways it reads empty (2026-09-15)

The bench gates on evidence it reads from the ingestion journal — the
subscription ack before the windows, and the error/warn counts afterwards. Three
independent changes since the last recorded pass (`logs/soak/bench-20260831-212352`)
each emptied that read, so a healthy stream looked like a dead one. Check them in
order; all three are fixed in-tree, so a recurrence means one has regressed.

1. **Directory not writable by the container.** `bench-throughput.sh` creates the
   journal bind mount as the invoking user (mode 0775, typically uid 1000) and
   hands it to the ingestion container, which P1-137 dropped to uid 65532. The
   entrypoint probes `LOG_DIR` and exits 2, so the container never becomes
   healthy. The script now `chmod 0777`s the directory. Symptom:
   `container not healthy` plus `FATAL — LOG_DIR not writable` in
   `docker logs <container>`.
2. **The filename is per-host.** P1-132 names it
   `ingestion-${HOST}-${VM_ID}.json` (a volume shared with the collector must not
   interleave writers), but the scripts read a fixed `ingestion.json`. The bench
   globs `ingestion*.json`; the suite's `journal_stats` resolves the real file
   from the name it is given. Symptom: `journal subscription acks ... 0` and
   `no full subscription ack in journal` on a stream that is demonstrably running.
3. **The appender buffers, so a live reader sees nothing.** P1-278 made the
   JSON_FILE appender buffered (256KiB, `immediateFlush="false"`) to take a
   per-event syscall off the hot path; combined with `shutdownHook="disable"` the
   buffer was also dropped on exit. The hook is back on, and flush policy is an
   env seam (`LOG_IMMEDIATE_FLUSH`, `LOG_BUFFERED_IO`) that the bench overlay sets
   to flush per event — the soak path keeps buffering. Symptom: the journal file
   exists but stays 0 bytes for the whole run, and after a graceful stop too.

Verify with a real run, not by reading the script: the ack line must appear in
the journal while the run is live, and the result must show all three windows
PASSing at ~20,400 rows/s with zero decode errors.

### The bench reads the previous container's counters (2026-09-15, CHG-176)

With the journal readable, the next failure was the bench's own: window 1
reported `decode_errors delta=-4 != 0`, which reads as the feed sending junk.
The feed was healthy. OpenObserve keeps serving the newest sample in its query
window, and a container that has just started has not flushed its first OTLP
point yet — so the baseline and window 1's opening read were answered from the
**previous, stopped** container. Its counters are unrelated to the new one's, so
their difference is meaningless.

Symptom worth recognising: a baseline line that pairs
`append_latency_ms_count=0` with a non-zero `decode_errors`. No live process can
produce that — a process appending at 20k/s has a non-zero append count by its
first tick. Any of these three lines means the bench is reading two processes:

```text
baseline: append_latency_ms_count=0 decode_errors=7      <- the pair to look for
!! window 1: decode_errors delta=-4 != 0                 <- misreported as bad ticks
!! window N: decode_errors counter went backwards ...     <- the honest message
```

The fix polls the append counter until it is seen to **advance** before taking a
baseline (a stopped process cannot advance its own counter, and both counters
ship in one OTLP payload, so a fresh append sample proves the decode-error
sample is fresh too). A counter that never advances fails as
`baseline counter frozen`; a delta that still goes backwards is skipped as a
stale sample instead of being reported as bad ticks. `skip_window` still fails
the run, so an unmeasurable window never passes.


## C2 TM-kill-at-full-load drill (tracker-14 Block 1, 2026-09-02)

Script: `code/01_platform/04_scripts/tm-kill-full-load.sh` (change records
CHG-120/CHG-121; acceptance evidence `logs/tracker-14/tm-kill-full-load-*`).

Two modes:

- **Smoke** (default approval gate, ~5 min): `C2_MODE=smoke bash
  code/01_platform/04_scripts/tm-kill-full-load.sh` — compressed main drill
  (30s warm-up / 60s pre-kill / 120s post-kill, real kill included). Gates:
  recovery, post-kill checkpoints + progress, single-restart contract
  (G-TMK-10b), drain (G-TMK-10c).
- **Main** (full drill, ~25 min incl. analysis): `bash
  code/01_platform/04_scripts/tm-kill-full-load.sh` — 45s warm-up / 180s
  pre-kill / 240s post-kill at 10,240 ticks/s, TM kill, then G7c zero-loss
  parity + F4 settlement verdicts (event-horizon bounded — see CHG-121).

Operational notes:

- The drill owns its own stack usage (lock file serializes runs); it purges
  and recreates the raw/preview tables and runs ingestion host-side with
  the faketool feed. Cancel any running SignalJob first.
- Drill-only submit flag `tolerable-failed-checkpoints=3` (env
  `TOLERABLE_FAILED_CHECKPOINTS`): a transient catch-up checkpoint miss no
  longer escalates to a global failure restart; consecutive failures beyond
  the tolerance still fail the job.
- The drain phase stops the feed and waits for the source counter to
  stabilize before cancel/harvest — without it, F4/G7c read the kill
  backlog's lag tail as data loss (the 20260902-030521 "31 orphans" and
  20260902-034813 "1024 mismatches" were both this artifact).
