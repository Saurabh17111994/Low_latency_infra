#!/usr/bin/env bash
# tiering-start.sh (2026-08-31) — start (or verify) the Fluss lake-tiering
# service as a Flink job on the running cluster.
#
# WHY THIS EXISTS: Fluss 0.9 lake tiering is NOT done by the Fluss servers —
# the coordinator only coordinates (LakeTableTieringManager heartbeats).
# The actual tiering runs in an external Flink job built from
# fluss-flink-tiering.jar (docs: maintenance/tiered-storage/
# lakehouse-storage.md "Start Datalake Tiering Service"). The 2026-08-31
# tiering smoke ran 10 minutes with NO tiering job running and every
# verification failed — this script + the smoke pre-flight guard close that
# failure mode.
#
# Usage:   tiering-start.sh            # start if not running, else verify
#          tiering-start.sh --status   # just check, exit 0 if RUNNING
set -euo pipefail

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
_DOCKER_DIR="$(cd "$_SCRIPT_DIR/../01_docker" && pwd)"
JM=01_docker-flink-jobmanager-1
TIERING_JAR=/opt/flink/lib/fluss-flink-tiering-1.0.0.jar
ENTRY=org.apache.fluss.flink.tiering.FlussLakeTieringEntrypoint

# --- config from the same files compose interpolates -------------------
# P6-247: the old two greps aborted under `set -e` with no message when .env was
# missing, and passed empty strings straight into --datalake.iceberg.warehouse /
# the s3a endpoint, so a config typo only surfaced minutes later as an S3A error
# inside the Flink job. Read tolerantly (optional `export `, quoted values, CRLF,
# surrounding blanks — forms compose's env_file accepts) and refuse to submit on
# an empty value. The LAST match wins, matching server.yaml's last-write-wins
# semantics that GUARD-C in the smoke depends on.
TIERING_ENV_FILE="${TIERING_ENV_FILE:-$_DOCKER_DIR/.env}"   # test seam
env_value() {
  sed -n "s/^[[:space:]]*\(export[[:space:]][[:space:]]*\)\?$1=//p" "$TIERING_ENV_FILE" 2>/dev/null \
    | tail -1 | tr -d '\r' \
    | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'$/\1/"
}
[ -r "$TIERING_ENV_FILE" ] || { echo "!! $_DOCKER_DIR/.env is missing or unreadable — the tiering job has no R2 endpoint or warehouse" >&2; exit 1; }
R2_ENDPOINT="$(env_value R2_ENDPOINT)"
WAREHOUSE="$(env_value S3_WAREHOUSE_PATH)"
[ -n "$R2_ENDPOINT" ] || { echo "!! R2_ENDPOINT is empty/missing in $TIERING_ENV_FILE — every s3a call in the tiering job would fail" >&2; exit 1; }
[ -n "$WAREHOUSE" ] || { echo "!! S3_WAREHOUSE_PATH is empty/missing in $TIERING_ENV_FILE — the tiering job would have no warehouse to write to" >&2; exit 1; }

log() { echo "[tiering-start] $*"; }

# --- status check via Flink REST (JobManager :8081 in-container) --------
# The tiering job registers pipeline.name "Fluss Lake Tiering" (set at
# submit below) — RUNNING means the service is live.
tiering_job_id() {
  docker exec "$JM" sh -c \
    'curl -fsS -m 5 http://localhost:8081/jobs/overview' 2>/dev/null \
    | python3 -c '
import json, sys
try:
    jobs = json.load(sys.stdin)["jobs"]
except Exception:
    sys.exit(1)
# P6-248: exact name, and any non-terminal state counts. Matching the substring
# "tiering" against RUNNING only made a CREATED/INITIALIZING/RESTARTING/FAILING
# job invisible, so a concurrent or restarting tiering job passed this check and
# the script submitted a SECOND tiering job.
TERMINAL = {"FINISHED", "CANCELED", "FAILED", "SUSPENDED"}
for j in jobs:
    if j.get("name") == "Fluss Lake Tiering" and j.get("state") not in TERMINAL:
        print(j["jid"]); break
' 2>/dev/null || true
}

# A running job is not necessarily a recoverable job.  The cluster default is
# currently NoRestartBackoffTimeStrategy, which turns a TaskManager loss into
# a permanent tiering outage (observed during the C2 drill on 2026-09-01).
# Verify the effective per-job strategy, not merely the REST RUNNING state.
tiering_has_restart_strategy() {
  local jid="$1" config
  # P6-249: $jid is interpolated into an in-container sh -c, so it is untrusted
  # input from the JobManager REST API. Restrict it to hex first — that removes
  # the shell metacharacters a compromised JM could inject (`x; rm -rf /; #`).
  case "$jid" in
    ''|*[!0-9a-fA-F]*) echo "!! refusing to query a non-hex job id: $jid" >&2; return 1 ;;
  esac
  config="$(docker exec "$JM" sh -c "curl -fsS -m 5 http://localhost:8081/jobs/$jid/config" \
    2>/dev/null || true)"
  [ -n "$config" ] || return 1
  # Parse the JSON and look only at the EFFECTIVE strategy field: the previous
  # `grep fixed[- ]delay` over the raw body matched any mention anywhere in the
  # config (a description or another key saying "fixed delay" was enough) while a
  # transient curl failure produced an empty body that was reported as "no
  # strategy". The REST API renders the token as "fixed-delay" or the label
  # "Restart with fixed delay (...)" depending on Flink version, and only those
  # forms — in the strategy field itself — may pass.
  printf '%s' "$config" | python3 -c '
import json, sys
try:
    cfg = (json.load(sys.stdin).get("execution-config") or {})
except Exception:
    sys.exit(1)
values = [cfg.get("restart-strategy"), cfg.get("restart-strategy.type")]
ok = any(v and ("fixed-delay" in str(v).lower() or "fixed delay" in str(v).lower())
         for v in values)
sys.exit(0 if ok else 1)
' 2>/dev/null
}

if [ "${1:-}" = "--status" ]; then
  JID="$(tiering_job_id)"
  if [ -n "$JID" ]; then
    if tiering_has_restart_strategy "$JID"; then
      log "tiering job RUNNING with fixed-delay restart (job_id=$JID)"
      exit 0
    fi
    # P6-632: distinct exit codes so the smoke's GUARD-A pre-flight can tell
    # "no job at all" (1) from "job running with the wrong strategy" (2).
    log "!! tiering job RUNNING without fixed-delay restart (job_id=$JID)"
    exit 2
  fi
  log "no RUNNING tiering job"
  exit 1
fi

# --- idempotent start ----------------------------------------------------
# P6-248: hold a lock across the check-then-submit pair — without it two
# concurrent starts both see "no job" and both submit, leaving two tiering jobs
# racing over the same tables.
command -v python3 >/dev/null 2>&1 || { echo "!! python3 is required on the host (Flink REST responses are parsed with it)" >&2; exit 1; }
LOCK_FILE="${TMPDIR:-/tmp}/tiering-start-$(id -u).lock"
exec 9>"$LOCK_FILE"
flock -n 9 || { echo "!! another tiering-start is in progress (lock $LOCK_FILE) — refusing to race it" >&2; exit 1; }

JID="$(tiering_job_id)"
if [ -n "$JID" ]; then
  if tiering_has_restart_strategy "$JID"; then
    log "tiering job already RUNNING with fixed-delay restart (job_id=$JID) — nothing to do"
    exit 0
  fi
  # P6-632: give the operator the exact command instead of leaving the outage in
  # place, and exit 2 so callers can distinguish it from a real failure.
  echo "!! tiering job $JID is RUNNING without fixed-delay restart — cancel it and resubmit:" >&2
  echo "     docker exec $JM flink cancel $JID && bash $0" >&2
  exit 2
fi

# GUARD (M-15/M-16, 2026-08-31): iceberg's shaded parquet write path lazily
# loads org.apache.hadoop.mapreduce.lib.input.FileInputFormat, which lives
# only in hadoop-mapreduce-client-core. Without it the job submits fine and
# tiers small rounds fine — then dies with NoClassDefFoundError under
# sustained write volume (observed 09:23Z and 15:28Z). The FIX is the minimal
# hadoop-mapreduce-compat jar (ONLY org/apache/hadoop/mapreduce/** — the full
# flink-shaded-hadoop-2-uber breaks S3A: NoSuchMethodError getTimeDuration,
# observed 16:23Z). Refuse to submit into a known-broken classpath; also
# refuse if the known-BAD uber jar is present.
MR_COMPAT=hadoop-mapreduce-compat-2.8.5.jar
HADOOP_UBER_BAD=flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
for svc in 01_docker-flink-jobmanager-1 01_docker-flink-taskmanager-1; do
  docker exec "$svc" sh -c "ls /opt/flink/lib/$MR_COMPAT >/dev/null 2>&1" \
    || { echo "!! GUARD-M15 FAILED: $MR_COMPAT not in $svc /opt/flink/lib/ —"
         echo "   add the mount to docker-compose.yml and recreate the flink containers"
         exit 1; }
  docker exec "$svc" sh -c "ls /opt/flink/lib/$HADOOP_UBER_BAD >/dev/null 2>&1" \
    && { echo "!! GUARD-M16 FAILED: $HADOOP_UBER_BAD IS in $svc — it breaks S3A (NoSuchMethodError)"
         exit 1; }
done
log "classpath OK: $MR_COMPAT present (and no hadoop-uber conflict) in both flink services"

log "submitting lake tiering job (iceberg -> $WAREHOUSE)"
# Arg names mirror the datalake.* keys the coordinator loads from
# server.yaml (datalake.format/type/warehouse + the iceberg.hadoop.fs.s3a.*
# block Fluss's HadoopUtils strips to plain hadoop keys). Credentials come
# from the container env (AWS_ACCESS_KEY_ID/SECRET — hadoop-aws reads them
# natively), NOT from the command line (would land in job logs).
# G4 (again, 2026-08-31): NO comments inside the continued command below —
# a '#' line comments out every remaining arg. That silently dropped
# fs.s3(.a).impl/region args here and the tiering job died at first table
# with "No FileSystem for scheme s3". Region MUST be an R2 region
# (auto/apac/eeur/enam); AWS names get InvalidRegionName -> HTTP 400
# (proven: auto=200, ap-south-1=400).
# P6-250: attempts=3 turned a TaskManager loss into a PERMANENT tiering outage
# 90s later — the same permanent outage this strategy was added to remove, just
# delayed. This job is a long-lived service, so the effective attempt count is
# unbounded (Integer.MAX_VALUE, Flink's accepted maximum) with a 30s fixed delay;
# `type=fixed-delay` is stated explicitly so a cluster default cannot change it.
if submit_out="$(docker exec "$JM" flink run -d \
  -c "$ENTRY" \
  -Dpipeline.name="Fluss Lake Tiering" \
  -Dparallelism.default=2 \
  '-Drestart-strategy.type=fixed-delay' \
  '-Drestart-strategy.fixed-delay.attempts=2147483647' \
  '-Drestart-strategy.fixed-delay.delay=30 s' \
  "$TIERING_JAR" \
  --fluss.bootstrap.servers fluss-coordinator:9123 \
  --datalake.format iceberg \
  --datalake.iceberg.type hadoop \
  --datalake.iceberg.warehouse "$WAREHOUSE" \
  --datalake.iceberg.iceberg.hadoop.fs.s3a.endpoint "$R2_ENDPOINT" \
  --datalake.iceberg.iceberg.hadoop.fs.s3a.endpoint.region auto \
  --datalake.iceberg.iceberg.hadoop.fs.s3a.path.style.access true \
  --datalake.iceberg.iceberg.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem \
  --datalake.iceberg.iceberg.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem \
  2>&1)"; then
  submit_rc=0
else
  submit_rc=$?
fi

# P6-633: require the full 32-hex JobID (the old [a-f0-9]+ matched a truncated
# or mangled id) and keep flink run's exit status visible — it used to be thrown
# away by `|| true`, so an auth/classpath/submit failure was only recognised by
# the absence of a JobID string.
# `|| true` is load-bearing: the pipeline exits 1 on a non-matching grep and
# pipefail would abort the shell here, before the failure message below.
NEW_JID="$(printf '%s\n' "$submit_out" | grep -oE 'JobID [0-9a-f]{32}' | awk '{print $2}' | head -1 || true)"
if [ -z "$NEW_JID" ]; then
  echo "!! submit failed (flink run exit status $submit_rc), output:" >&2
  printf '%s\n' "$submit_out" | tail -25 >&2
  exit 1
fi
[ "$submit_rc" -eq 0 ] || log "note: flink run exited $submit_rc but printed JobID $NEW_JID — verifying the job state below"

# FAIL-FAST: submit-success != job-running. A classpath break (missing
# s3a/hadoop jar) fails the job within seconds while `flink run` still
# prints a JobID. Poll the REST state for up to 30s.
# P6-634: terminal states FINISHED/SUSPENDED were unhandled (a finished job just
# spun to the timeout), FAILED jobs were left behind uncancelled, curl had no
# timeout so a hung JobManager could exceed the whole budget, and a missing host
# python3 or a dead JM produced an empty state that was reported as "not RUNNING".
for _ in $(seq 1 15); do
  STATE="$(docker exec "$JM" sh -c "curl -fsS -m 5 http://localhost:8081/jobs/$NEW_JID" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state","UNKNOWN"))' 2>/dev/null || echo UNKNOWN)"
  case "$STATE" in
    RUNNING)
      if tiering_has_restart_strategy "$NEW_JID"; then
        log "tiering job RUNNING with fixed-delay restart (job_id=$NEW_JID)"
        exit 0
      fi
      echo "!! tiering job RUNNING but effective restart strategy is not fixed-delay" >&2
      exit 1 ;;
    FAILED|CANCELED|FINISHED|SUSPENDED)
      echo "!! tiering job reached terminal state $STATE — fetching exception:" >&2
      docker exec "$JM" sh -c "curl -fsS -m 5 http://localhost:8081/jobs/$NEW_JID/exceptions" 2>/dev/null \
        | python3 -c 'import json,sys; d=json.load(sys.stdin); print((d.get("root-exception") or "")[:3000])' >&2 || true
      exit 1 ;;
  esac
  sleep 2
done
echo "!! tiering job state after 30s: ${STATE:-unknown} (not RUNNING)" >&2
exit 1
