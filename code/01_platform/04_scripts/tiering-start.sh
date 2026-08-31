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
TIERING_JAR=/opt/flink/lib/fluss-flink-tiering-0.9.1-incubating.jar
ENTRY=org.apache.fluss.flink.tiering.FlussLakeTieringEntrypoint

# --- config from the same files compose interpolates -------------------
R2_ENDPOINT="$(grep -E '^R2_ENDPOINT=' "$_DOCKER_DIR/.env" | cut -d= -f2-)"
WAREHOUSE="$(grep -E '^S3_WAREHOUSE_PATH=' "$_DOCKER_DIR/.env" | cut -d= -f2-)"

log() { echo "[tiering-start] $*"; }

# --- status check via Flink REST (JobManager :8081 in-container) --------
# The tiering job registers pipeline.name "Fluss Lake Tiering" (set at
# submit below) — RUNNING means the service is live.
tiering_job_id() {
  docker exec "$JM" sh -c \
    'curl -s http://localhost:8081/jobs/overview' 2>/dev/null \
    | python3 -c '
import json, sys
try:
    jobs = json.load(sys.stdin)["jobs"]
except Exception:
    sys.exit(1)
for j in jobs:
    if "tiering" in j["name"].lower() and j["state"] == "RUNNING":
        print(j["jid"]); break
' 2>/dev/null || true
}

if [ "${1:-}" = "--status" ]; then
  JID="$(tiering_job_id)"
  if [ -n "$JID" ]; then
    log "tiering job RUNNING (job_id=$JID)"
    exit 0
  fi
  log "no RUNNING tiering job"
  exit 1
fi

# --- idempotent start ----------------------------------------------------
JID="$(tiering_job_id)"
if [ -n "$JID" ]; then
  log "tiering job already RUNNING (job_id=$JID) — nothing to do"
  exit 0
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
submit_out="$(docker exec "$JM" flink run -d \
  -c "$ENTRY" \
  -Dpipeline.name="Fluss Lake Tiering" \
  -Dparallelism.default=2 \
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
  2>&1)" || true

NEW_JID="$(echo "$submit_out" | grep -oE 'JobID [a-f0-9]+' | awk '{print $2}' | head -1)"
if [ -z "$NEW_JID" ]; then
  echo "!! submit failed, output:" >&2
  echo "$submit_out" | tail -25 >&2
  exit 1
fi

# FAIL-FAST: submit-success != job-running. A classpath break (missing
# s3a/hadoop jar) fails the job within seconds while `flink run` still
# prints a JobID. Poll the REST state for up to 30s.
for _ in $(seq 1 15); do
  sleep 2
  STATE="$(docker exec "$JM" sh -c "curl -s http://localhost:8081/jobs/$NEW_JID" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null || true)"
  if [ "$STATE" = "RUNNING" ]; then
    log "tiering job RUNNING (job_id=$NEW_JID)"
    exit 0
  fi
  if [ "$STATE" = "FAILED" ] || [ "$STATE" = "CANCELED" ]; then
    echo "!! tiering job reached state $STATE — fetching exception:" >&2
    docker exec "$JM" sh -c "curl -s http://localhost:8081/jobs/$NEW_JID/exceptions" \
      | python3 -c 'import json,sys; d=json.load(sys.stdin); print((d.get("root-exception") or "")[:3000])' >&2
    exit 1
  fi
done
echo "!! tiering job state after 30s: ${STATE:-unknown} (not RUNNING)" >&2
exit 1
