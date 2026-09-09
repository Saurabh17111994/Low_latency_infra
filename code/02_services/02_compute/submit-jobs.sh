#!/usr/bin/env bash
# Submit the three Flink jobs (signal, babysitter, safety-halt) to the JobManager
# through its REST API. Feature computation is part of SignalJob. SafetyHaltJob
# is the slot-scoped Safety_Halt_Requests consumer (SAFETY-INT-001) and is
# submitted only when SAFETY_MANIFEST_TOKENS is configured (dev default: skip
# with a warning — it is the safety consumer, not a data-path job). Runs in the
# compute container.
set -euo pipefail

# Tracker 14 P4.1/P4.2 — deployment validation BEFORE anything is submitted:
# production must use the pinned RocksDB backend and durable S3 checkpoints;
# heap/HashMap state or local /tmp paths are never silently substituted.
case "${DEPLOYMENT_ENV:-dev}" in
	dev) ;;
	production)
		if [ "${STATE_BACKEND:-rocksdb}" = "hashmap" ]; then
			echo "compute: FATAL — STATE_BACKEND=hashmap is forbidden in DEPLOYMENT_ENV=production (tracker 14 P4.1); use rocksdb" >&2
			exit 1
		fi
		case "${CHECKPOINT_DIR:-}" in
			s3://* | s3a://*) ;;
			*)
				echo "compute: FATAL — CHECKPOINT_DIR must be an S3 object-store URI in DEPLOYMENT_ENV=production (tracker 14 P4.2)" >&2
				exit 1
				;;
		esac
		# Tracker 14 P4.2 — object-store checkpoint credentials come from secret
		# injection (env), never committed files; the job's own config fails the
		# same way (SignalJobConfig.s3Endpoint), this is the launcher-side gate
		# that runs before anything is submitted.
		if [ -z "${S3_ENDPOINT:-}" ] && [ -z "${R2_ENDPOINT:-}" ]; then
			echo "compute: FATAL — S3_ENDPOINT (or R2_ENDPOINT) is required for S3 checkpoints in DEPLOYMENT_ENV=production (tracker 14 P4.2)" >&2
			exit 1
		fi
		if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
			echo "compute: FATAL — AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required for S3 checkpoints in DEPLOYMENT_ENV=production (tracker 14 P4.2); inject via secrets, never commit files" >&2
			exit 1
		fi
		;;
	*)
		echo "compute: FATAL — DEPLOYMENT_ENV must be 'dev' or 'production', got '${DEPLOYMENT_ENV}' (tracker 14 P4.1)" >&2
		exit 1
		;;
esac

JM="${FLINK_JOBMANAGER:-flink-jobmanager}:8081"
JAR=/opt/flink-jobs/compute.jar

echo "compute: waiting for JobManager at ${JM}"
ready=0
# SC2034: loop counter is unused — the loop is a bounded retry-wait.
for _ in $(seq 1 30); do
	if curl -fsS "http://${JM}/v1/config" >/dev/null 2>&1; then
		ready=1
		break
	fi
	sleep 2
done

if [ "${ready}" -ne 1 ]; then
	echo "compute: FATAL — JobManager ${JM} did not become ready" >&2
	exit 1
fi

if [ ! -r "${JAR}" ]; then
	echo "compute: FATAL — compute jar is not readable: ${JAR}" >&2
	exit 1
fi

echo "compute: uploading ${JAR}"
upload_response=$(curl -fsS -X POST "http://${JM}/jars/upload" \
	-F "jarfile=@${JAR}")
uploaded_filename=$(printf '%s' "${upload_response}" \
	| sed -n 's/.*"filename":"\([^"]*\)".*/\1/p')
jar_id=${uploaded_filename##*/}

if [ -z "${jar_id}" ] || [ "${jar_id}" = "${uploaded_filename}" ]; then
	echo "compute: FATAL — JobManager upload response did not contain a jar filename: ${upload_response}" >&2
	exit 1
fi

wait_for_running() {
	local job_id="$1"
	local job_name="$2"
	local state=""

	for _ in $(seq 1 30); do
		state=$(curl -fsS "http://${JM}/jobs/${job_id}" \
			| sed -n 's/.*"state":"\([^"]*\)".*/\1/p')
		case "${state}" in
			RUNNING)
				echo "compute: ${job_name} is RUNNING (${job_id})"
				return 0
				;;
			FAILED|CANCELED|CANCELING|SUSPENDED|RECONCILING)
				echo "compute: FATAL — ${job_name} entered terminal/failed state ${state} (${job_id})" >&2
				return 1
				;;
		esac
		sleep 2
	done

	echo "compute: FATAL — ${job_name} did not reach RUNNING (last state: ${state:-unknown})" >&2
	return 1
}

# T8 (Phase 5): local readiness must reflect a completed checkpoint, not just a
# RUNNING job. Poll the Flink REST checkpoints endpoint for the durable
# checkpoint COUNTER (counts.completed) and fail closed if neither job reaches
# one completed checkpoint within the timeout — a job that runs but never
# checkpoints is not locally ready (checkpoint/restore evidence is a T8 exit
# gate). The compute container never receives Arrow credentials/env (see the
# compose service), so execution intent stays disabled for both jobs.
wait_for_checkpoint() {
	local job_id="$1"
	local job_name="$2"
	local completed=""

	for _ in $(seq 1 30); do
		completed=$(curl -fsS "http://${JM}/jobs/${job_id}/checkpoints" 2>/dev/null \
			| sed -n 's/.*"counts":{[^}]*"completed":\([0-9][0-9]*\).*/\1/p')
		if [ -n "${completed}" ] && [ "${completed}" -gt 0 ]; then
			echo "compute: ${job_name} completed ${completed} checkpoint(s) (${job_id})"
			return 0
		fi
		sleep 2
	done

	echo "compute: FATAL — ${job_name} did not complete a checkpoint within timeout (last completed=${completed:-0})" >&2
	return 1
}

# P2-086 idempotency probe: true when a job with the Flink-side name is
# already RUNNING. Field-order tolerant like rollout-savepoint.sh (this
# Flink's /jobs/overview omits isStoppable; see CHG-105).
job_already_running() {
	local expected="$1"
	local overview
	overview=$(curl -fsS "http://${JM}/jobs/overview" 2>/dev/null) || return 1
	printf '%s' "${overview}" \
		| grep -oE '\{"jid":"[0-9a-f]{32}".*?"state":"(RUNNING|[A-Z_]+)"' \
		| grep -F "\"name\":\"${expected}\"" \
		| grep -q '"state":"RUNNING"'
}

submit_job() {
	local job_name="$1"
	local entry_class="$2"
	local nonfatal="${3:-0}"
	local parallelism="${4:-}"
	local run_response
	local job_id

	# F005 fail-closed replay guard (CANDLE-KV-REPLAY-001 A3.3): a signal-job
	# restart must EITHER restore from a checkpoint (STATE_RECOVERY_PATH) OR
	# be an EXPLICIT operator-approved full replay. The compose flink-common
	# anchor now defaults ALLOW_FULL_REPLAY=false (restore-only, 2026-08-28
	# #3 hardening) — this guard is defense-in-depth for explicit overrides:
	# if the operator launches with ALLOW_FULL_REPLAY=true but NO
	# STATE_RECOVERY_PATH, refuse unless COMPUTE_ALLOW_REPLAY=1 (intentional
	# full-replay bootstrap, e.g. first-ever deploy). A silent offset-0
	# replay re-emits the whole backlog, balloons dedup MapState past the
	# pinned checkpoint contract, and appends duplicate candle rows to the
	# immutable Signal_Candidates LOG — observed 2026-08-10.
	if [ "${entry_class}" = "com.trading.compute.signaljob.SignalJob" ] \
		&& [ -z "${STATE_RECOVERY_PATH:-}" ] \
		&& [ "${ALLOW_FULL_REPLAY:-false}" = "true" ] \
		&& [ "${COMPUTE_ALLOW_REPLAY:-0}" != "1" ]; then
		echo "compute: FATAL — ${job_name} would start as a SILENT FULL REPLAY" >&2
		echo "compute:   (ALLOW_FULL_REPLAY=true from env, no STATE_RECOVERY_PATH)." >&2
		echo "compute:   Set STATE_RECOVERY_PATH=<savepoint|checkpoint> to resume, or" >&2
		echo "compute:   COMPUTE_ALLOW_REPLAY=1 to explicitly approve an offset-0 replay." >&2
		return 1
	fi

	# P2-086 idempotency: never submit a second instance while one is
	# RUNNING — restarts must reuse, not duplicate (observed 16-job slot
	# exhaustion). /jobs/overview carries the Flink job name
	# (env.execute), not the entry class, so map it here.
	local flink_name=""
	case "${entry_class}" in
		*signaljob.SignalJob*) flink_name="signal-job-compute" ;;
		*babysitter.BabysitterJob*) flink_name="Babysitter Positions observer" ;;
		*safetyhalt.SafetyHaltJob*) flink_name="Safety-halt consumer" ;;
	esac
	if [ -n "${flink_name}" ] && job_already_running "${flink_name}"; then
		echo "compute: SKIP ${job_name} — '${flink_name}' already RUNNING (idempotent restart, no duplicate submit)"
		return 0
	fi

	# P2-002: forward the restore path to Flink, not just check it — the
	# F005 guard above is hollow without this (the job still starts fresh /
	# offset-0 even when STATE_RECOVERY_PATH is set). Body fields are the
	# version-stable contract for /jars/:jarid/run; allowNonRestoredState
	# stays false (STARTUP-GATE-001: a state mismatch fails loud, never
	# silently drops state).
	local run_body="{\"entryClass\":\"${entry_class}\""
	if [ -n "${parallelism}" ]; then
		run_body="${run_body},\"parallelism\":${parallelism}"
	fi
	if [ "${entry_class}" = "com.trading.compute.signaljob.SignalJob" ] \
		&& [ -n "${STATE_RECOVERY_PATH:-}" ]; then
		local sp_esc
		sp_esc=$(printf '%s' "${STATE_RECOVERY_PATH}" | sed 's/\\/\\\\/g; s/"/\\"/g')
		run_body="${run_body},\"savepointPath\":\"${sp_esc}\",\"allowNonRestoredState\":false"
		echo "compute: restoring ${job_name} from STATE_RECOVERY_PATH=${STATE_RECOVERY_PATH}"
	fi
	run_body="${run_body}}"

	echo "compute: submitting ${job_name} (${entry_class})"
	run_response=$(curl -fsS -X POST "http://${JM}/jars/${jar_id}/run" \
		-H "Content-Type: application/json" \
		-d "${run_body}")
	job_id=$(printf '%s' "${run_response}" \
		| sed -n 's/.*"jobid":"\([^"]*\)".*/\1/p')

	if [ -z "${job_id}" ]; then
		echo "compute: FATAL — ${job_name} submission returned no job id: ${run_response}" >&2
		return 1
	fi

	wait_for_running "${job_id}" "${job_name}"
	if ! wait_for_checkpoint "${job_id}" "${job_name}"; then
		if [ "${nonfatal}" = "1" ]; then
			echo "compute: WARN — ${job_name} checkpoint gate failed; continuing (nonfatal job)" >&2
			return 0
		fi
		return 1
	fi
}

# SignalJob is managed by `make rollout-savepoint` (savepoint-based
# redeploy), NOT by the compute launcher — submitting it here would create a
# SECOND instance reading raw_table_1 alongside the rolled-out job
# (split-brain: independent dedup state, duplicate Signal_Candidates LOG
# rows). COMPUTE_SUBMIT_SIGNAL=1 opts the launcher in (fresh deploy with no
# existing job; F005 guard still applies — needs STATE_RECOVERY_PATH or
# COMPUTE_ALLOW_REPLAY=1).
# P2-087: degraded-launch tracking — a skipped/failed required job flips
# this; the launcher exits non-zero at the end so alerting fires. Safe
# only with the P2-086 idempotency above (else exit-1 + restart policy =
# resubmit loop).
failed=0
if [ "${COMPUTE_SUBMIT_SIGNAL:-0}" = "1" ]; then
	submit_job "signal-job-compute" "com.trading.compute.signaljob.SignalJob" || failed=1
else
	echo "compute: SKIP signal-job (COMPUTE_SUBMIT_SIGNAL != 1) — managed by make rollout-savepoint"
fi
# Babysitter is a no-op Positions observer (MVP marker shell) — its checkpoint
# gate failing (e.g. Fluss idle-table leader-less client quirk on an empty
# Positions table) must NOT block the SafetyHaltJob submission that follows.
# Parallelism pinned to 1: it is a slot-scoped observer; inheriting the
# cluster PARALLELISM=8 would require 8 slots (impossible alongside signal-job
# p=8 on the single-VM taskmanager).
# P2-086: a babysitter hiccup must not kill the launcher before
# SafetyHaltJob — nonfatal, but tracked for the final exit code (P2-087).
if ! submit_job "Babysitter MVP no-op job" "com.trading.compute.babysitter.BabysitterJob" 1 1; then
	echo "compute: WARN — babysitter submission/readiness failed; continuing to SafetyHaltJob (nonfatal job)" >&2
	failed=1
fi

# SafetyHaltJob — the slot-scoped safety-halt consumer (SAFETY-INT-001).
# Requires SAFETY_MANIFEST_TOKENS (comma-separated instrument tokens); without
# it the job fails at startup, so skip with a clear warning in dev rather than
# failing the whole compute container. Production MUST set it (fail-closed).
if [ -n "${SAFETY_MANIFEST_TOKENS:-}" ]; then
	# Parallelism pinned to 1 — same slot-scoped rationale as babysitter.
	if submit_job "SafetyHaltJob (slot-scoped safety consumer)" "com.trading.compute.safetyhalt.SafetyHaltJob" 0 1; then
		echo "compute: submitted jobs done (signal-job managed by rollout; babysitter no-op; safety-halt submitted)"
	else
		# CRITICAL — do NOT exit 1 here: `set -e` + `restart: unless-stopped`
		# would turn this into an infinite container restart loop, each
		# restart submitting a fresh duplicate job pair (observed: 16
		# duplicate safety-halt/babysitter jobs consuming all 10 TM slots
		# on 2026-08-26). The checkpoint gate is a T8 *readiness* signal,
		# not a crash: the job was submitted and reached RUNNING; a missing
		# checkpoint (e.g. Fluss idle-table quirk, slot contention) is a
		# warning the operator investigates, not a reason to resubmit.
		echo "compute: WARN — SafetyHaltJob checkpoint gate failed; container exits cleanly, NO resubmit" >&2
		echo "compute: WARN — inspect running SafetyHaltJob jobs before restarting this container" >&2
		failed=1
	fi
else
	# P2-003: production must fail closed without the safety consumer —
	# a prod deploy with no SafetyHaltJob is trading with no safety cover.
	if [ "${DEPLOYMENT_ENV:-dev}" = "production" ]; then
		echo "compute: FATAL — SAFETY_MANIFEST_TOKENS is required in DEPLOYMENT_ENV=production (safety consumer must not be skipped)" >&2
		exit 1
	fi
	echo "compute: WARN — SAFETY_MANIFEST_TOKENS unset; SafetyHaltJob NOT submitted (safety consumer skipped in dev; set it in production)"
	echo "compute: submitted jobs done (signal-job managed by rollout; babysitter no-op)"
fi

# P2-087: report degraded launches — compose/K8s finally sees them.
if [ "${failed}" -ne 0 ]; then
	echo "compute: launcher finished with degraded readiness (see WARN above)" >&2
	exit 1
fi
echo "compute: launcher finished — jobs submitted (readiness gate: see per-job WARN/FATAL above)"
exit 0
