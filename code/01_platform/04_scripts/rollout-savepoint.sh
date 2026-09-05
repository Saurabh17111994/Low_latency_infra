#!/usr/bin/env bash
# rollout-savepoint.sh — G5 Ops T12 (streaming-3000 hardening): savepoint →
# stop → redeploy the SignalJob from the fresh savepoint, so the rolling
# update keeps the fingerprint-dedup state (MapState TTL 300s) instead of
# falling back to an offset-0 full replay.
#
# Control plane: Flink REST API v1 (the same contract submit-jobs.sh uses —
# /v1/config, /jobs/overview, /jobs/{jobid}, /jobs/{jobid}/savepoints,
# /jobs/{jobid}/checkpoints). The deploy step must run the SignalJob main()
# in a process whose environment it controls (SignalJobConfig reads
# System.getenv(); the REST /jars/run path executes main() in the JobManager
# JVM, where the composed env defaults ALLOW_FULL_REPLAY=true and cannot
# carry a per-release STATE_RECOVERY_PATH), so the jar is copied into the
# flink-jobmanager container and submitted with `flink run -d` under an
# explicit env — the pattern already verified in docs/06_operations/
# 01-runbooks.md (start/restart sections).
#
# Flow (default):
#   1. preflight: JobManager reachable; exactly one RUNNING job named
#      JOB_NAME (or JOB_ID given explicitly).
#   2. sample dedup evidence (state count + first/duplicate counters) from
#      the TaskManager Prometheus endpoint (best-effort; VERIFY_DEDUP_STATE).
#   3. trigger an async savepoint (target-directory = SAVEPOINT_DIR, or
#      file:///checkpoints/savepoints in dev) and poll until COMPLETED.
#   4. stop the job (PATCH /jobs/{jobid}?mode=cancel, cancel fallback) and
#      wait for state CANCELED.
#   5. deploy: docker compose cp <JAR> -> flink-jobmanager:<JAR_IN_CONTAINER>,
#      then `docker compose exec -T -e STATE_RECOVERY_PATH=<savepoint>
#      -e ALLOW_FULL_REPLAY=false ... flink run -d -c
#      com.trading.compute.signaljob.SignalJob <jar>`. Every job config var
#      set in this script's environment (see JOB_ENV_NAMES) is forwarded as
#      -e; the pinned required vars (DEDUP_TTL_MS, CANDLE_WINDOW_MS,
#      CHECKPOINT_INTERVAL_MS, CHECKPOINT_TIMEOUT_MS, MAX_CONCURRENT_CHECKPOINTS)
#      have no defaults in SignalJobConfig and MUST be exported by the caller.
#   6. verify: new job reaches RUNNING, its client log shows
#      'startup mode = RESTORE (restore=true, fullReplay=false)', and it
#      completes >= 1 durable checkpoint; then re-sample dedup evidence and
#      assert the restored state count is not lost (>= 50% of pre-rollout).
#
# Modes:
#   Full rollout (default): steps 3-6 with the fresh savepoint.
#   RECOVERY_PATH=<path> (or --recovery-path <path>): skip savepoint/stop —
#     the operator already captured state — deploy from <path> and verify.
#   DRY_RUN=1 (or --dry-run): print the exact commands without executing.
#
# Usage:
#   make rollout-savepoint                        # env-driven (JAR, JOB_ID, ...)
#   make rollout-savepoint ARGS="DRY_RUN=1"       # preview without touching
#   code/01_platform/04_scripts/rollout-savepoint.sh --job-id <jid> --jar <p>
#
# Env (all optional unless noted):
#   JM_URL                  Flink REST base, default http://localhost:8081
#   JOB_NAME                job to roll, default signal-job-compute
#   JOB_ID                  explicit job id (skips the overview lookup)
#   ENTRY_CLASS             default com.trading.compute.signaljob.SignalJob
#   SAVEPOINT_DIR           savepoint target dir; REQUIRED s3://* when
#                           DEPLOYMENT_ENV=production (else fail closed)
#   SAVEPOINT_TIMEOUT_S     default 600
#   JOB_STOP_TIMEOUT_S      default 120
#   START_TIMEOUT_S         default 180 (RUNNING wait)
#   CHECKPOINT_TIMEOUT_S    default 180 (>= 1 completed checkpoint wait)
#   COMPOSE_FILE            default code/01_platform/01_docker/docker-compose.yml
#   COMPOSE_PROJECT         optional (-p) for overlays such as p10
#   JAR                     host path to the new compute jar
#   JAR_IN_CONTAINER        default /opt/flink/jobs/compute.jar
#   VERIFY_DEDUP_STATE      1 (default, degrades to warning) | 0
#   PROMETHEUS_URL          TM Prometheus, default http://localhost:9250/metrics
#   HIT_SAMPLE_S            post-restore sampling window, default 30
#   LOGDIR                  evidence dir, default <repo>/logs/rollout

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# ---- defaults (env-driven) -------------------------------------------------

JM_URL="${JM_URL:-http://localhost:8081}"
JOB_NAME="${JOB_NAME:-signal-job-compute}"
JOB_ID="${JOB_ID:-}"
ENTRY_CLASS="${ENTRY_CLASS:-com.trading.compute.signaljob.SignalJob}"
RECOVERY_PATH="${RECOVERY_PATH:-}"
SAVEPOINT_DIR="${SAVEPOINT_DIR:-}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-dev}"
SAVEPOINT_TIMEOUT_S="${SAVEPOINT_TIMEOUT_S:-600}"
JOB_STOP_TIMEOUT_S="${JOB_STOP_TIMEOUT_S:-120}"
START_TIMEOUT_S="${START_TIMEOUT_S:-180}"
CHECKPOINT_TIMEOUT_S="${CHECKPOINT_TIMEOUT_S:-180}"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT/code/01_platform/01_docker/docker-compose.yml}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-}"
JAR="${JAR:-$ROOT/code/02_services/02_compute/target/compute.jar}"
JAR_IN_CONTAINER="${JAR_IN_CONTAINER:-/opt/flink/jobs/compute.jar}"
VERIFY_DEDUP_STATE="${VERIFY_DEDUP_STATE:-1}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9250/metrics}"
HIT_SAMPLE_S="${HIT_SAMPLE_S:-30}"
DRY_RUN="${DRY_RUN:-0}"
LOGDIR="${LOGDIR:-$ROOT/logs/rollout}"

# Job config vars forwarded as -e to the flink client JVM when set in the
# caller's environment. STATE_RECOVERY_PATH and ALLOW_FULL_REPLAY are owned
# by this script (restore mode always), so they are deliberately absent.
JOB_ENV_NAMES="RAW_TABLE CANDLE_TABLE SIGNAL_CANDIDATES_TABLE SIGNAL_CURRENT_TABLE \
DEDUP_TTL_MS CANDLE_WINDOW_MS WATERMARK_OUT_OF_ORDER_MS ALLOWED_LATENESS_MS \
SOURCE_IDLE_MS SOURCE_IDLE_ALERT_MS CHECKPOINT_INTERVAL_MS CHECKPOINT_TIMEOUT_MS \
MAX_CONCURRENT_CHECKPOINTS RESTART_MAX_ATTEMPTS RESTART_DELAY_MS CHECKPOINT_DIR \
SAVEPOINT_DIR DEPLOYMENT_ENV STATE_BACKEND PARALLELISM FLUSS_BOOTSTRAP_SERVERS \
FLUSS_DATABASE EXECUTION_INTENT_ENABLED CONFIGURATION_VERSION \
SIGNAL_STRATEGY_ID SIGNAL_STRATEGY_VERSION N7_RULE_ID \
OTEL_COLLECTOR_HOST INSTRUMENT_MANIFEST_PATH"

TS="$(date +%Y%m%d-%H%M%S)"
EVIDENCE="$LOGDIR/rollout-$JOB_NAME-$TS.log"

log()  { printf 'rollout: %s\n' "$*" | tee -a "$EVIDENCE"; }
warn() { printf 'rollout: WARN — %s\n' "$*" | tee -a "$EVIDENCE"; }
die()  { printf 'rollout: FATAL — %s\n' "$*" | tee -a "$EVIDENCE" >&2; exit 1; }

usage() {
	cat <<'EOF'
rollout-savepoint.sh — savepoint → stop → redeploy SignalJob from the savepoint (G5 T12)

Usage:
  rollout-savepoint.sh [--dry-run] [--jar PATH] [--job-id JID]
                       [--recovery-path PATH] [--job-name NAME]

Modes:
  default          savepoint current job, stop it, deploy JAR with
                   STATE_RECOVERY_PATH=<fresh savepoint>, verify restore.
  --recovery-path  deploy + verify only, restoring from the given path
                   (savepoint/stop already done by the operator).
  --dry-run        print the planned commands, execute nothing.
EOF
}

while [ "$#" -gt 0 ]; do
	case "$1" in
		--dry-run) DRY_RUN=1 ;;
		--jar) JAR="${2:?--jar needs a path}"; shift ;;
		--job-id) JOB_ID="${2:?--job-id needs a job id}"; shift ;;
		--job-name) JOB_NAME="${2:?--job-name needs a name}"; shift ;;
		--recovery-path) RECOVERY_PATH="${2:?--recovery-path needs a path}"; shift ;;
		--help|-h) usage; exit 0 ;;
		*=*)
			# KEY=VAL env override (make ARGS="DRY_RUN=1 JAR=/x" convention,
			# same shape as stack-config's DEPLOY=1). Overrides the defaults
			# computed above; validated so `export` cannot take junk.
			if [[ "$1" =~ ^[A-Za-z_][A-Za-z0-9_]*=.* ]]; then
				key="${1%%=*}"
				export "$key=${1#*=}"
			else
				die "malformed KEY=VAL argument '$1'"
			fi
			;;
		*) die "unknown argument '$1' (see --help)" ;;
	esac
	shift
done

mkdir -p "$LOGDIR"
printf 'rollout: evidence -> %s\n' "$EVIDENCE" | tee -a "$EVIDENCE"

# ---- helpers ---------------------------------------------------------------

api_get() { curl -fsS --max-time 15 "$JM_URL$1"; }

# savepoint/cancel requests may legitimately fail (job state races) — callers
# handle the exit code. Optional request body passed via stdin.
api_send() { # method path [body-file]
	local method="$1" path="$2" body="${3:-}"
	if [ -n "$body" ]; then
		curl -fsS --max-time 15 -X "$method" -H "Content-Type: application/json" \
			-d "$body" "$JM_URL$path"
	else
		curl -fsS --max-time 15 -X "$method" "$JM_URL$path"
	fi
}

job_state() { # jobid -> job state
	local jobid="$1"
	api_get "/jobs/$jobid" | sed -n 's/.*"state":"\([A-Z]*\)".*/\1/p'
}

wait_state() { # jobid expected-state timeout-s label
	local jobid="$1" expected="$2" timeout_s="$3" label="$4"
	local deadline state
	deadline=$(( $(date +%s) + timeout_s ))
	while [ "$(date +%s)" -lt "$deadline" ]; do
		state="$(job_state "$jobid")"
		if [ "$state" = "$expected" ]; then
			log "$label: job $jobid reached $expected"
			return 0
		fi
		if { [ "$state" = "FAILED" ] || [ "$state" = "CANCELED" ]; } && [ "$expected" != "$state" ]; then
			die "$label: job $jobid entered terminal state $state (wanted $expected)"
		fi
		sleep 3
	done
	die "$label: job $jobid did not reach $expected within ${timeout_s}s (last state: ${state:-unknown})"
}

compose() { # docker compose wrapper honoring file/project overrides
	# --env-file (.env + secrets.env, same pair every other compose call in
	# this project uses): without it, compose interpolates the tablet's
	# AWS_SECRET_ACCESS_KEY from the EMPTY environment and aborts before
	# running any command (observed 2026-08-30 recovery drill: deploy step
	# failed with "required variable AWS_SECRET_ACCESS_KEY is missing").
	local -a ef=(
		--env-file "$(dirname "$COMPOSE_FILE")/.env"
		--env-file "$(dirname "$COMPOSE_FILE")/secrets.env"
	)
	if [ -n "$COMPOSE_PROJECT" ]; then
		docker compose -f "$COMPOSE_FILE" "${ef[@]}" -p "$COMPOSE_PROJECT" "$@"
	else
		docker compose -f "$COMPOSE_FILE" "${ef[@]}" "$@"
	fi
}

# Prometheus sampling of the dedup evidence (best-effort). Prints a single
# line: firsts_cumulative first_total dup_total (empty when unavailable).
# 2026-08-28 gauge remediation: the gauge formerly named
# compute_dedup_state_count is RENAMED compute_dedup_firsts_cumulative (it is
# a cumulative-firsts counter, NOT live state size — MapState.entries() does
# not filter TTL-expired on RocksDB; see logs/tracker-14/dedup-ttl-diagnosis-
# 20260828.md). The rollout continuity check is RELATIVE (pre vs post restore),
# so the cumulative counter remains valid for it; live-state monitoring reads
# flink_jobmanager_job_lastCheckpointSize instead.
sample_dedup() {
	local body state first dup
	body="$(curl -fsS --max-time 10 "$PROMETHEUS_URL" 2>/dev/null)" || { echo ""; return 0; }
	# Transition-safe (2026-08-28 gauge remediation): accept the NEW name
	# (compute_dedup_firsts_cumulative) OR the OLD name
	# (compute_dedup_state_count) so a pre/post comparison works across a
	# redeploy that renames the gauge. Both are cumulative-firsts counters.
	state="$(printf '%s\n' "$body" \
		| grep -E '^flink_taskmanager_job_task_operator_compute_dedup_(firsts_cumulative|state_count)\{' \
		| awk '{ s += $NF } END { print s + 0 }')"
	first="$(printf '%s\n' "$body" | grep -E '^flink_taskmanager_job_task_operator_compute_dedup_first\{' \
		| awk '{ s += $NF } END { print s + 0 }')"
	dup="$(printf '%s\n' "$body" | grep -E '^flink_taskmanager_job_task_operator_compute_dedup_duplicates\{' \
		| awk '{ s += $NF } END { print s + 0 }')"
	if [ -z "$state" ] && [ -z "$first" ]; then
		echo ""
	else
		printf '%s %s %s\n' "$state" "$first" "$dup"
	fi
}

# ---- 1. preflight ----------------------------------------------------------

log "== rollout-savepoint: $JOB_NAME ($DEPLOYMENT_ENV) =="
[ "$DRY_RUN" = "1" ] && log "DRY RUN — commands are printed, nothing executes"

if ! command -v curl >/dev/null 2>&1; then
	die "curl is required"
fi
if ! api_get "/v1/config" >/dev/null 2>&1; then
	if [ "$DRY_RUN" = "1" ]; then
		warn "JobManager REST not reachable at $JM_URL — dry run continues with placeholder ids"
		JM_OK=0
	else
		die "JobManager REST not reachable at $JM_URL (set JM_URL for host/overlay/prod)"
	fi
else
	JM_OK=1
fi
[ "$JM_OK" = "1" ] && log "JobManager reachable at $JM_URL"

if [ -n "$RECOVERY_PATH" ]; then
	log "RECOVERY_PATH set ($RECOVERY_PATH) — skipping savepoint/stop, deploy+verify only"
	SAVEPOINT_PATH="$RECOVERY_PATH"
elif [ -n "$JOB_ID" ]; then
	log "explicit JOB_ID=$JOB_ID"
	if [ "$JM_OK" = "1" ]; then
		state="$(job_state "$JOB_ID")"
		[ "$state" = "RUNNING" ] || die "job $JOB_ID is not RUNNING (state=$state) — savepoint requires a running job"
	fi
else
	if [ "$JM_OK" = "1" ]; then
		overview="$(api_get "/jobs/overview")"
		# Field-order/omission tolerant: Flink's /jobs/overview does NOT include
		# isStoppable in this version, and field order is jid,name,start-time,
		# end-time,duration,state — so match jid+name+state in any order within
		# each job object rather than demanding a fixed field sequence
		# (2026-08-25: the old fixed-order regex never matched, breaking
		# auto-resolve).
		JOB_ID="$(printf '%s' "$overview" \
			| grep -oE '\{"jid":"[0-9a-f]{32}".*?"state":"(RUNNING|[A-Z_]+)"' \
			| grep -E "\"name\":\"$JOB_NAME\"" \
			| grep -E "\"state\":\"RUNNING\"" \
			| sed -n "s/.*\"jid\":\"\([0-9a-f]\{32\}\)\".*/\1/p" \
			| head -n 1)"
		[ -n "$JOB_ID" ] || die "no RUNNING job named '$JOB_NAME' in /jobs/overview (use JOB_ID=<jid> to target explicitly)"
		log "resolved $JOB_NAME -> job $JOB_ID (RUNNING)"
	else
		JOB_ID="<job-id>"
	fi
fi

# savepoint target directory: SAVEPOINT_DIR when set; production rejects
# non-S3 targets (G4 T9 parity with submit-jobs.sh); dev defaults to the
# flink-checkpoints volume mounted in both JM and TM.
if [ -z "${SAVEPOINT_PATH:-}" ]; then
	if [ -n "$SAVEPOINT_DIR" ]; then
		TARGET_DIR="$SAVEPOINT_DIR"
		if [ "$DEPLOYMENT_ENV" = "production" ] && [[ "$TARGET_DIR" != s3://* && "$TARGET_DIR" != s3a://* ]]; then
			die "DEPLOYMENT_ENV=production requires SAVEPOINT_DIR=s3://* (got '$TARGET_DIR')"
		fi
	else
		[ "$DEPLOYMENT_ENV" = "production" ] \
			&& die "DEPLOYMENT_ENV=production requires SAVEPOINT_DIR=s3://* — refusing file:// fallback"
		TARGET_DIR="file:///checkpoints/savepoints"
	fi
	log "savepoint target directory: $TARGET_DIR"
fi

# ---- evidence (pre) --------------------------------------------------------

STATE_BEFORE=""
if [ "$VERIFY_DEDUP_STATE" = "1" ]; then
	# 2026-08-28 gauge remediation: the pre baseline is the job's latest
	# completed checkpoint state_size (TRUE live state) — the cumulative
	# firsts gauge is not a state proxy and resets on restart.
	# RECOVERY_PATH mode has no old job (JOB_ID empty). Two failure modes
	# guarded (observed 2026-08-30 recovery drill, rc=22 then rc=1):
	# 1. curl -f on /jobs//checkpoints → HTTP 400 → exit 22
	# 2. EMPTY pipeline input + pipefail → sed -n 's/../../p' exits 1 when
	#    nothing is printed → assignment status 1 → set -e kills the script
	# The trailing `|| true` inside the substitution covers both;
	# STATE_BEFORE stays empty and the post-restore check degrades to a
	# sanity check (by design for this mode).
	STATE_BEFORE="$( { [ -n "$JOB_ID" ] && api_get "/jobs/$JOB_ID/checkpoints" \
		| sed -n 's/.*"state_size":\([0-9][0-9]*\).*/\1/p' | tail -1; } 2>/dev/null || true)"
	before="$(sample_dedup)"
	if [ -n "$STATE_BEFORE" ]; then
		log "dedup evidence (pre): checkpoint_state_size=$STATE_BEFORE counters=$before"
	else
		warn "checkpoint state_size unavailable for job $JOB_ID — continuity check reduced to a post-restore sanity check (counters=$before)"
	fi
fi

# ---- 2. savepoint + 3. stop --------------------------------------------------

if [ -z "${SAVEPOINT_PATH:-}" ]; then
	log "== triggering savepoint for job $JOB_ID =="
	if [ "$DRY_RUN" = "1" ]; then
		log "DRY: POST $JM_URL/jobs/$JOB_ID/savepoints {\"target-directory\":\"$TARGET_DIR\",\"cancel-job\":false}"
		SAVEPOINT_PATH="<fresh-savepoint>"
	else
		trigger="$(api_send POST "/jobs/$JOB_ID/savepoints" \
			"{\"target-directory\":\"$TARGET_DIR\",\"cancel-job\":false}")"
		REQ_ID="$(printf '%s' "$trigger" | sed -n 's/.*"request-id":"\([^"]*\)".*/\1/p')"
		[ -n "$REQ_ID" ] || die "savepoint trigger returned no request-id: $trigger"
		log "savepoint request $REQ_ID accepted"

		deadline=$(( $(date +%s) + SAVEPOINT_TIMEOUT_S ))
		status="PENDING"
		while [ "$(date +%s)" -lt "$deadline" ]; do
			response="$(api_get "/jobs/$JOB_ID/savepoints/$REQ_ID")"
			status="$(printf '%s' "$response" | sed -n 's/.*"status":{"id":"\([A-Z_]*\)".*/\1/p')"
			if [ "$status" = "COMPLETED" ]; then
				SAVEPOINT_PATH="$(printf '%s' "$response" \
					| sed -n 's/.*"location":"\([^"]*\)".*/\1/p')"
				break
			fi
			if [ "$status" = "FAILED" ]; then
				die "savepoint request $REQ_ID FAILED: $response"
			fi
			sleep 5
		done
		[ -n "$SAVEPOINT_PATH" ] || die "savepoint request $REQ_ID did not complete within ${SAVEPOINT_TIMEOUT_S}s (last status: ${status:-unknown})"
		log "savepoint COMPLETED: $SAVEPOINT_PATH"
	fi

	log "== stopping job $JOB_ID =="
	if [ "$DRY_RUN" = "1" ]; then
		log "DRY: PATCH $JM_URL/jobs/$JOB_ID?mode=cancel  (fallback: POST $JM_URL/jobs/$JOB_ID/cancel)"
	else
		if ! api_send PATCH "/jobs/$JOB_ID?mode=cancel" >/dev/null 2>&1; then
			warn "PATCH cancel unsupported — falling back to POST /jobs/$JOB_ID/cancel"
			api_send POST "/jobs/$JOB_ID/cancel" >/dev/null
		fi
		wait_state "$JOB_ID" "CANCELED" "$JOB_STOP_TIMEOUT_S" "stop"
	fi
fi

# ---- 4. deploy the new jar ---------------------------------------------------

[ -n "${SAVEPOINT_PATH:-}" ] || die "internal error: no restore path resolved"
if [ "$DRY_RUN" = "1" ]; then
	log "DRY: restore path STATE_RECOVERY_PATH=$SAVEPOINT_PATH"
else
	[ -r "$JAR" ] || die "jar not readable: $JAR (build it first, e.g. cd code && mvn -q package -pl 02_services/02_compute)"
	command -v docker >/dev/null 2>&1 || die "docker CLI is required for the deploy step"
fi

log "== deploying $JAR (restore path: $SAVEPOINT_PATH) =="
if [ "$DRY_RUN" = "1" ]; then
	log "DRY: docker compose -f $COMPOSE_FILE${COMPOSE_PROJECT:+ -p $COMPOSE_PROJECT} cp $JAR flink-jobmanager:$JAR_IN_CONTAINER"
	exec_args=(exec -T -e "STATE_RECOVERY_PATH=$SAVEPOINT_PATH" -e "ALLOW_FULL_REPLAY=false")
	for name in $JOB_ENV_NAMES; do
		[ -n "${!name:-}" ] && exec_args+=(-e "$name=${!name}")
	done
	log "DRY: docker compose -f $COMPOSE_FILE${COMPOSE_PROJECT:+ -p $COMPOSE_PROJECT} ${exec_args[*]} flink-jobmanager flink run -d -c $ENTRY_CLASS $JAR_IN_CONTAINER"
	log "== dry run complete — nothing executed =="
	exit 0
fi

compose cp "$JAR" "flink-jobmanager:$JAR_IN_CONTAINER"
log "jar copied to flink-jobmanager:$JAR_IN_CONTAINER"

exec_args=(exec -T -e "STATE_RECOVERY_PATH=$SAVEPOINT_PATH" -e "ALLOW_FULL_REPLAY=false")
for name in $JOB_ENV_NAMES; do
	[ -n "${!name:-}" ] && exec_args+=(-e "$name=${!name}")
done
log "submitting with STATE_RECOVERY_PATH=$SAVEPOINT_PATH and ALLOW_FULL_REPLAY=false (restore mode)"
submit_output="$(compose "${exec_args[@]}" flink-jobmanager flink run -d -c "$ENTRY_CLASS" "$JAR_IN_CONTAINER" 2>&1)" \
	|| die "flink run failed — see output above; restore path: $SAVEPOINT_PATH"

NEW_JOB_ID="$(printf '%s\n' "$submit_output" | sed -n 's/.*JobID \([0-9a-f]\{32\}\).*/\1/p' | head -n 1)"
if [ -z "$NEW_JOB_ID" ]; then
	printf '%s\n' "$submit_output" | tee -a "$EVIDENCE" >&2
	die "could not parse a JobID from the flink client output"
fi
log "submitted new job $NEW_JOB_ID"

if printf '%s\n' "$submit_output" | grep -q 'F005\|startup mode = FULL_REPLAY'; then
	printf '%s\n' "$submit_output" | tee -a "$EVIDENCE" >&2
	die "client log shows a config error or FULL_REPLAY — restore did NOT take; check STATE_RECOVERY_PATH/ALLOW_FULL_REPLAY"
fi
if printf '%s\n' "$submit_output" | grep -q 'startup mode = RESTORE'; then
	log "client log confirms: startup mode = RESTORE (restore=true, fullReplay=false)"
else
	# The SignalJob startup-mode INFO line does not reach the client stdout
	# or the client log file under this compose/exec deploy (log4j file
	# appenders + CLI stdout routing — observed 2026-08-22). The
	# authoritative restore proof is Flink's own TaskManager restore lines
	# ('Restoring state for N split(s)' + 'Starting to restore from state
	# handle ... <savepoint>'), observed ~30s after submit. NOTE: docker
	# compose logs --since rejects duration strings ("30s") and accepts
	# RFC3339 timestamps only (verified 2026-08-22), so compute the window.
	restore_proven=0
	for _ in $(seq 1 15); do
		since_ts="$(date -u -d '-30 seconds' +%Y-%m-%dT%H:%M:%SZ)"
		tm_restore="$(compose logs --since "$since_ts" --tail 800 flink-taskmanager 2>/dev/null \
			| grep -E 'Restoring state for [0-9]+ split|Starting to restore from state handle' || true)"
		if [ -n "$tm_restore" ]; then
			restore_proven=1
			break
		fi
		sleep 5
	done
	if [ "$restore_proven" = "1" ]; then
		log "restore confirmed via TaskManager restore lines (split restore + state handle from the savepoint)"
	else
		printf '%s\n' "$submit_output" | tee -a "$EVIDENCE" >&2
		die "no restore evidence: client log lacks 'startup mode = RESTORE' and TM logs show no restore lines — refusing to trust the restore"
	fi
fi

# ---- 5. verify start ---------------------------------------------------------

log "== verifying job $NEW_JOB_ID =="
wait_state "$NEW_JOB_ID" "RUNNING" "$START_TIMEOUT_S" "start"

# Dedup counters of the restored job at t0 — per-job metric instances restart
# at 0 on restore, so deltas vs this sample tell the continuity gate whether
# any row was actually processed in the verification window (traffic guard
# for the quiet-market case, observed 2026-08-22).
T0_DEDUP=""
if [ "$VERIFY_DEDUP_STATE" = "1" ]; then
	for _ in $(seq 1 10); do
		T0_DEDUP="$(sample_dedup)"
		[ -n "$T0_DEDUP" ] && break
		sleep 2
	done
fi

completed=""
deadline=$(( $(date +%s) + CHECKPOINT_TIMEOUT_S ))
while [ "$(date +%s)" -lt "$deadline" ]; do
	completed="$(api_get "/jobs/$NEW_JOB_ID/checkpoints" 2>/dev/null \
		| sed -n 's/.*"counts":{[^}]*"completed":\([0-9][0-9]*\).*/\1/p')"
	if [ -n "$completed" ] && [ "$completed" -gt 0 ]; then
		break
	fi
	sleep 5
done
{ [ -n "${completed:-}" ] && [ "$completed" -gt 0 ]; } \
	|| die "job $NEW_JOB_ID did not complete a checkpoint within ${CHECKPOINT_TIMEOUT_S}s (last completed=${completed:-0})"
log "job $NEW_JOB_ID completed checkpoint(s): $completed"

# ---- 6. dedup continuity evidence --------------------------------------------

if [ "$VERIFY_DEDUP_STATE" = "1" ]; then
	# 2026-08-28 gauge remediation: the dedup gauge (compute.dedup.firsts.
	# cumulative) is a CUMULATIVE counter that resets on restart — it is NOT a
	# live-state proxy, so it cannot compare pre/post restore. The TRUE
	# preserved state is the checkpoint state_size (REST API). The continuity
	# gate now compares latest-checkpoint state_size pre vs post restore.
	# Prometheus counters remain as traffic/hit-rate evidence only.
	log "== dedup continuity check (checkpoint state_size, ${HIT_SAMPLE_S}s sample) =="
	# Post-restore: sample the new job's latest completed checkpoint state_size.
	state_after=""
	deadline=$(( $(date +%s) + CHECKPOINT_TIMEOUT_S ))
	while [ "$(date +%s)" -lt "$deadline" ]; do
		state_after="$(api_get "/jobs/$NEW_JOB_ID/checkpoints" 2>/dev/null \
			| sed -n 's/.*"state_size":\([0-9][0-9]*\).*/\1/p' | tail -1)"
		[ -n "$state_after" ] && break
		sleep 5
	done
	# Post-restore traffic evidence (degradable): cumulative counters since
	# the restored start.
	after="$(sample_dedup)"
	first_after="$(printf '%s' "$after" | awk '{print $2}')"
	dup_after="$(printf '%s' "$after" | awk '{print $3}')"
	log "dedup evidence (post): checkpoint_state_size=$state_after counters=$after"
	if [ -n "$STATE_BEFORE" ] && [ -n "$state_after" ]; then
		if [ "$state_after" -lt $(( STATE_BEFORE / 2 )) ]; then
			# TTL semantics: state naturally shrinks as entries expire, but a
			# >50% drop in the FIRST post-restore checkpoint means the restore
			# did NOT carry the state.
			die "dedup checkpoint state_size after restore ($state_after) < 50% of pre-rollout ($STATE_BEFORE) — dedup state was NOT preserved; investigate before continuing"
		else
			log "dedup state preserved: checkpoint state_size $STATE_BEFORE -> $state_after (>= 50% gate passed)"
		fi
	elif [ -n "$state_after" ] && [ "$state_after" -gt 0 ]; then
		log "post-restore sanity: checkpoint state_size = $state_after (> 0)"
	else
		warn "checkpoint state_size unavailable after restore — continuity NOT asserted (restore itself already verified via TaskManager restore lines)"
	fi
	# Hit-rate evidence (traffic-dependent, reported not gated).
	if [ -n "$dup_after" ] && [ -n "$first_after" ] \
		&& [ "$dup_after" -gt 0 ] && [ "$first_after" -gt 0 ]; then
		pre_hits=$(( dup_after * 100 / (dup_after + first_after) ))
		log "session dedup hit share since restored start: ${pre_hits}% (dup=$dup_after first=$first_after)"
	fi
fi

log "== rollout complete: $JOB_NAME $JOB_ID -> $NEW_JOB_ID, restore from $SAVEPOINT_PATH =="
log "evidence: $EVIDENCE"
