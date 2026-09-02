#!/usr/bin/env bash
# =============================================================================
# stage-a2-baseline.sh — plan v2 Stage A2: fresh full-chain baseline capture.
#
# Purges raw+preview tables, starts faketool feed + host-side ingestion JVM +
# SignalJob, then runs stage-capture.sh for DURATION_S (default 720 = 12 min)
# on one aligned timeline, then cleans up (job cancelled, feed stopped —
# Fluss stack left up).
#
# Measurement-only composition of pipeline-lib primitives (same launch recipe
# as the C2 drill, no kill phase). Units the capture also ingests the
# ingestion JVM's java.out counters via INGESTION_JAVA_OUT (injection into
# stage-capture.sh, which is unchanged).
#
# Usage:
#   RATE_HZ=10 DURATION_S=720 bash stage-a2-baseline.sh
#   RATE_HZ=12 DURATION_S=720 bash stage-a2-baseline.sh     # future tier
#
# Rules enforced (plan v2): fresh tables (no backlog replay), fail-closed on
# any launch step, capture only while the job is RUNNING (stage-capture.sh
# aborts otherwise), one run dir per capture.
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROOT" || { echo "!! cannot cd to repo root $ROOT"; exit 1; }

RATE_HZ="${RATE_HZ:-10}"
DURATION_S="${DURATION_S:-720}"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
COMPOSE_DIR="$ROOT/code/01_platform/01_docker"

PHASE_NAME="stage-a2-baseline"
PHASE_OUT="$ROOT/logs/tracker-14/${PHASE_NAME}-$(date +%Y%m%d-%H%M%S)"
OUT="$PHASE_OUT/capture"
RUN_LOG="$PHASE_OUT/run.log"
mkdir -p "$OUT/j1"

exec > >(tee -a "$RUN_LOG") 2>&1

source "$SCRIPT_DIR/pipeline-lib.sh"
pipeline_install_cleanup_trap

JOB_ID=""
JVM_PID=""
FAKETOOL_PID=""

fatal() {
  echo "STAGE-A2: FAIL — $*" >&2
  printf 'FAIL\n%s\n' "$*" > "$PHASE_OUT/FAILURE.txt"
  exit 1
}

echo "STAGE-A2: rate=${RATE_HZ}Hz duration=${DURATION_S}s out=$PHASE_OUT"

# Same strictness as the drill: PURGE_STRICT removes stale history,
# ALLOW_FULL_REPLAY=false keeps the source in LATEST mode.
export PURGE_STRICT=true ALLOW_FULL_REPLAY=false

# --- Flink up? (power-cut left JM/TM down; drills assume them up) ---
if ! curl -fsS --max-time 5 "$FLINK_REST_URL/overview" >/dev/null 2>&1; then
  echo "STAGE-A2: Flink REST down — starting flink containers"
  ( cd "$COMPOSE_DIR" && docker compose --env-file .env --env-file secrets.env up -d flink-jobmanager flink-taskmanager ) \
    || fatal "cannot start flink containers"
  for i in $(seq 1 40); do
    curl -fsS --max-time 5 "$FLINK_REST_URL/overview" >/dev/null 2>&1 && break
    sleep 5
  done
  curl -fsS --max-time 5 "$FLINK_REST_URL/overview" >/dev/null 2>&1 \
    || fatal "Flink REST still down after 200s"
fi
echo "STAGE-A2: Flink up"

# --- Canonical preflight: sets CP + LIB_MANIFEST_SLICE, validates jars/
# bridge/manifest/ports, waits for Fluss, restarts the TM fresh (B3 guard),
# waits for TM registration (B5 guard). Required before purge/submit —
# without it CP and LIB_MANIFEST_SLICE are unbound and ingestion fails.
pipeline_preflight || fatal "pipeline preflight failed"

# --- Fresh tables (no backlog replay → measures steady state only) ---
pipeline_purge_raw_table || fatal "raw table purge failed"
pipeline_start_faketool || fatal "faketool start failed"
pipeline_start_ingestion || fatal "ingestion start failed"
pipeline_purge_preview_table || fatal "preview table purge failed"
pipeline_submit_job || fatal "SignalJob submission failed"

# --- Wait RUNNING (fail-closed; stage-capture.sh re-checks) ---
state=""
for i in $(seq 1 40); do
  state="$(curl -fsS --max-time 5 "$FLINK_REST_URL/jobs/$JOB_ID" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))' 2>/dev/null)"
  [ "$state" = "RUNNING" ] && break
  sleep 5
done
[ "$state" = "RUNNING" ] || fatal "job $JOB_ID never RUNNING (state=$state)"
echo "STAGE-A2: job $JOB_ID RUNNING — capturing ${DURATION_S}s"

# --- The capture (all stage metrics, one timeline) ---
JOB_ID="$JOB_ID" DURATION_S="$DURATION_S" \
  INGESTION_JAVA_OUT="$OUT/j1/java.out" \
  OUT_DIR="$PHASE_OUT/stages" \
  bash "$SCRIPT_DIR/stage-capture.sh" || fatal "stage capture failed"

echo "STAGE-A2: capture complete — evidence at $PHASE_OUT"
ls -la "$PHASE_OUT/stages"