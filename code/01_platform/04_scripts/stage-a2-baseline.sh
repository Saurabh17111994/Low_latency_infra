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
MIN_UPTIME_S="${MIN_UPTIME_S:-600}"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"

PHASE_NAME="stage-a2-baseline"
# P6-552: PID suffix — 1s timestamps collide across concurrent invocations
# (shared dir, interleaved tee output, clobbered FAILURE.txt/stages).
PHASE_OUT="$ROOT/logs/tracker-14/${PHASE_NAME}-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$PHASE_OUT" || { echo "STAGE-A2: FAIL — cannot create $PHASE_OUT" >&2; exit 1; }
OUT="$PHASE_OUT/capture"
RUN_LOG="$PHASE_OUT/run.log"
mkdir -p "$OUT/j1"

exec > >(tee -a "$RUN_LOG") 2>&1

source "$SCRIPT_DIR/pipeline-lib.sh"
pipeline_install_cleanup_trap

JOB_ID=""
# CHG-122: data path is containers now - no host PIDs to track

fatal() {
  echo "STAGE-A2: FAIL — $*" >&2
  printf 'FAIL\n%s\n' "$*" > "$PHASE_OUT/FAILURE.txt"
  exit 1
}

echo "STAGE-A2: rate=${RATE_HZ}Hz duration=${DURATION_S}s out=$PHASE_OUT"

# Host-stability gate (2026-09-02, after two power cuts in ~2h killed two
# captures): right after a reboot the machine is still settling and a second
# flap is most likely; also Fluss may be mid-recovery with torn segments that
# only surface minutes later. Refuse to start a measurement until the host has
# been up MIN_UPTIME_S (default 600s = 10min). If the power cut left torn
# Fluss segments, the preflight's 180s readiness wait will fail closed — run
# `fluss-repair/repair-tablet.sh --all` first.
# P6-553: validate both sides — a non-integer MIN_UPTIME_S made `[ -lt ]`
# error so the if-branch silently skipped the gate; a missing /proc/uptime
# produced a confusing post-reboot fatal instead of a clear one.
case "${MIN_UPTIME_S:-}" in ''|*[!0-9]*) fatal "MIN_UPTIME_S must be non-negative integer (got '${MIN_UPTIME_S:-}')";; esac
[ -r /proc/uptime ] || fatal "cannot read /proc/uptime — host-stability gate needs Linux /proc (uptime_s unknown)"
uptime_s="$(awk '{print int($1)}' /proc/uptime)"
case "$uptime_s" in ''|*[!0-9]*) fatal "cannot parse host uptime";; esac
if [ "$uptime_s" -lt "$MIN_UPTIME_S" ]; then
  fatal "host uptime ${uptime_s}s < MIN_UPTIME_S=${MIN_UPTIME_S}s — refusing a measurement start right after a reboot; re-run in $((MIN_UPTIME_S - uptime_s))s"
fi

# Same strictness as the drill: PURGE_STRICT removes stale history,
# ALLOW_FULL_REPLAY=false keeps the source in LATEST mode.
export PURGE_STRICT=true ALLOW_FULL_REPLAY=false

# --- Flink up? (power-cut left JM/TM down; drills assume them up) ---
if ! curl -fsS --max-time 5 "$FLINK_REST_URL/overview" >/dev/null 2>&1; then
  echo "STAGE-A2: Flink REST down — starting flink containers"
  # P6-554: reuse the lib's $COMPOSE (B1: both env files + explicit -f) instead
  # of a divergent cwd-dependent recipe. Array form "${COMPOSE[@]}" — scalar
  # $COMPOSE is only element 0 (bare `docker`) since P6-468 made it an array,
  # so the audit's literal `$COMPOSE up` would run `docker up` (no such command).
  "${COMPOSE[@]}" up -d flink-jobmanager flink-taskmanager \
    || fatal "cannot start flink containers"
  for _ in $(seq 1 40); do
    curl -fsS --max-time 5 "$FLINK_REST_URL/overview" >/dev/null 2>&1 && break
    sleep 5
  done
  curl -fsS --max-time 5 "$FLINK_REST_URL/overview" >/dev/null 2>&1 \
    || fatal "Flink REST still down after 200s"
fi
echo "STAGE-A2: Flink up"

# --- Canonical preflight: sets CP, validates jars/bridge/manifest/ports,
# waits for Fluss, restarts the TM fresh (B3 guard), waits for TM
# registration (B5 guard). Required before purge/submit — without it CP is
# unset and the ingestion launch fails.
pipeline_preflight || fatal "pipeline preflight failed"

# --- Fresh tables (no backlog replay → measures steady state only) ---
pipeline_purge_raw_table || fatal "raw table purge failed"
pipeline_start_faketool || fatal "faketool start failed"
pipeline_start_ingestion || fatal "ingestion start failed"
pipeline_submit_job || fatal "SignalJob submission failed"

# --- Wait RUNNING (fail-closed; stage-capture.sh re-checks) ---
state=""
for _ in $(seq 1 40); do
  state="$(curl -fsS --max-time 5 "$FLINK_REST_URL/jobs/$JOB_ID" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))' 2>/dev/null)"
  [ "$state" = "RUNNING" ] && break
  sleep 5
done
[ "$state" = "RUNNING" ] || fatal "job $JOB_ID never RUNNING (state=$state)"
echo "STAGE-A2: job $JOB_ID RUNNING — capturing ${DURATION_S}s"

# --- G24 (2026-09-02): RocksDB must land on the NAMED VOLUME -------------
# The CHG-120 degradation class: if state.backend.rocksdb.localdir is not in
# effect (dropped by the FLINK_PROPERTIES prefix collision or a bad env),
# RocksDB silently writes to the container overlay fs and per-op cost jumps
# ~100x (fingerprint-dedup ~2.3 ms busy/rec, pipeline capped ~1.7k/s) —
# invisible until throughput analysis. Fail the run INSTEAD of capturing a
# poisoned baseline: the job's RocksDB dirs are named job_<JOB_ID>_op_*;
# they must appear under /tmp/flink-rocksdb (the named volume mount).
if [ "${STATE_BACKEND:-rocksdb}" = "rocksdb" ]; then
  rocks_ok=""
  for _ in $(seq 1 12); do
    if $COMPOSE exec -T flink-taskmanager \
        sh -c "ls -d /tmp/flink-rocksdb/job_${JOB_ID}_op_* >/dev/null 2>&1"; then
      rocks_ok=1; break
    fi
    sleep 5
  done
  [ -n "$rocks_ok" ] || fatal "G24: no RocksDB dirs for job $JOB_ID under /tmp/flink-rocksdb after 60s — RocksDB is writing somewhere else (container overlay = the CHG-120 ~1.7k/s degradation). WHY: state.backend.rocksdb.localdir not in effect (FLINK_PROPERTIES prefix collision dropped it, or STATE_BACKEND_LOCAL_DIRS env wrong). Check: docker exec 01_docker-flink-taskmanager-1 grep -A4 localdir /opt/flink/conf/config.yaml — and guard G22 (check_flink_properties.py)"
  echo "STAGE-A2: G24 OK — RocksDB on the named volume (job_${JOB_ID}_op_* under /tmp/flink-rocksdb)"
fi

# --- The capture (all stage metrics, one timeline) ---
# B2 hooks (2026-09-02): FLUSS_PROBE_CP enables the two passive Fluss probes
# (read-lag.tsv log-end offsets + consumer-read.tsv KV lookups); $CP comes
# from pipeline_preflight above. INGESTION_JAVA_OUT enables ingestion.tsv
# (feed->ack OTLP payloads from the ingestion JVM's java.out).
JOB_ID="$JOB_ID" DURATION_S="$DURATION_S" \
  INGESTION_JAVA_OUT="$OUT/j1/java.out" \
  FLUSS_PROBE_CP="$CP" \
  OUT_DIR="$PHASE_OUT/stages" \
  bash "$SCRIPT_DIR/stage-capture.sh" || fatal "stage capture failed"

# --- G25 (2026-09-02): throughput-floor fail-fast --------------------------
# 2026-09-02 incident class: four successive 20Hz captures each exited PASS
# while the pipeline was silently degraded (source 19.8k -> 7.3k -> 5.4k ->
# 3.9k -> 3.4k/s against a 20,480/s feed; every keyed operator's per-record
# cost ~3x). The failure only surfaced in manual analysis AFTER burning the
# run. A capture whose source cannot sustain at least SOURCE_RATE_FLOOR_PCT
# (default 80%) of the feed rate is a POISONED BASELINE, not evidence -
# fail it, with the numbers and the likely classes to check.
floor_pct="${SOURCE_RATE_FLOOR_PCT:-80}"
expected_rate=$((RATE_HZ * 1024))
src_avg="$(python3 - "$PHASE_OUT/stages/stages.tsv" <<'PYCALC'
import csv, sys
rows = [r for r in csv.reader(open(sys.argv[1]), delimiter="\t")
        if len(r) > 4 and "raw-validation" in r[2] and r[4]]
rows.sort(key=lambda r: int(r[0]))
if len(rows) < 2:
    print(0); sys.exit(0)
dt = int(rows[-1][0]) - int(rows[0][0])
dout = int(rows[-1][4]) - int(rows[0][4])
print(dout // dt if dt > 0 else 0)
PYCALC
)"
floor_rate=$((expected_rate * floor_pct / 100))
if [ "${src_avg:-0}" -lt "$floor_rate" ]; then
  fatal "G25: source avg ${src_avg}/s is below ${floor_pct}% of the ${expected_rate}/s feed (floor ${floor_rate}/s) - the capture is a POISONED BASELINE, refusing to bless it. Likely classes: (1) RocksDB state silently misplaced (check G23/G24 lines in run.log above), (2) cumulative per-run degradation (2026-09-02: monotonic decline across runs 19.8k->3.4k, root cause then under investigation - see plan evolution log), (3) a new limiter (compare per-operator busy/per-record cost against the last good capture). Evidence: $PHASE_OUT/stages/stages.tsv"
fi
echo "STAGE-A2: G25 OK - source avg ${src_avg}/s >= ${floor_rate}/s (${floor_pct}% of feed)"

echo "STAGE-A2: capture complete — evidence at $PHASE_OUT"
ls -la "$PHASE_OUT/stages"
