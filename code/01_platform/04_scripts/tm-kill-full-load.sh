#!/usr/bin/env bash
# =============================================================================
# tm-kill-full-load.sh — C2 TaskManager kill-at-full-load drill.
#
# Proves the local single-TaskManager recovery contract:
#   load -> regular checkpoints -> SIGKILL the TM mid-burst -> TM re-registers
#   -> SignalJob restores from its last checkpoint -> a new checkpoint completes
#   -> the source catches up -> G7c raw-recount/final-candle parity is exact.
#
# This is a separate acceptance drill, not a modification of
# holistic-measure.sh. A chaos run must tolerate FAILING/RESTARTING states
# while a latency run must abort on them. It reuses pipeline-lib.sh for the
# canonical preflight, faketool, ingestion, submit, and cleanup paths.
#
# The raw feed is host-side and continues while the TM is down. The Fluss
# tablet is not killed. Therefore a missing post-recovery candle is real data
# loss, not an ambiguous source outage. G7 parity-only mode (CHG-120) runs the
# full raw recount even though this clean drill has no deliberate injection.
#
# Smoke mode (2026-09-02 rework) is the SAME drill, compressed: full load,
# the real SIGKILL, recovery, a new checkpoint, and a post-kill progress
# gate — only the windows are short and the G7c/F4 verdict (which needs a
# long window) is main-only. A smoke run fails fast: the 2026-09-02
# regression (job restarted at t+~140s during post-kill catch-up) is
# invisible to a no-kill smoke but trips the restart gate below in ~4 min.
#
# Usage:
#   C2_MODE=smoke bash tm-kill-full-load.sh
#   bash tm-kill-full-load.sh
#   RATE_HZ=10 PRE_KILL_S=180 POST_KILL_S=240 bash tm-kill-full-load.sh
#
# Evidence:
#   logs/tracker-14/tm-kill-full-load-<timestamp>/
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
# Pin the working directory to the repo root: children resolve repo-relative
# paths from their inherited cwd. Ingestion's ConfigGuard source-scans code/
# by walking UP from cwd (observed 2026-09-01: a launch from $HOME made the
# scan find nothing, every config key read-failed, and the drill aborted).
# ROOT is already absolute, so cd'ing also makes the drill immune to any
# launcher's working directory.
cd "$ROOT" || { echo "!! cannot cd to repo root $ROOT"; exit 1; }

RATE_HZ="${RATE_HZ:-10}"
POLL_S="${POLL_S:-5}"
C2_MODE="${C2_MODE:-main}"
WARMUP_S="${WARMUP_S:-45}"
# Accumulated vertex metrics only become visible after the first checkpoint
# completes. On a freshly restarted TaskManager the first checkpoint can lag
# the whole 45s warm-up (observed 2026-09-01 attempt 20260901-125337: zero
# checkpoints in 45s, every proof vertex unreported — while ingestion had
# appended 101k ticks and forming-bar-sink had already written 1558 rows
# through the complete chain). Poll for up to this many extra seconds; a
# live feed reports within one checkpoint cycle, a dead one still fails.
WARMUP_GRACE_S="${WARMUP_GRACE_S:-120}"
PRE_KILL_S="${PRE_KILL_S:-180}"
POST_KILL_S="${POST_KILL_S:-240}"
RECOVERY_TIMEOUT_S="${RECOVERY_TIMEOUT_S:-420}"
NEW_CP_TIMEOUT_S="${NEW_CP_TIMEOUT_S:-180}"
TIERING_TIMEOUT_S="${TIERING_TIMEOUT_S:-120}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-10000}"
CHECKPOINT_TIMEOUT_MS="${CHECKPOINT_TIMEOUT_MS:-30000}"
RESTART_MAX_ATTEMPTS="${RESTART_MAX_ATTEMPTS:-3}"
RESTART_DELAY_MS="${RESTART_DELAY_MS:-30000}"
TM_CONTAINER="${TM_CONTAINER:-01_docker-flink-taskmanager-1}"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
TM_METRICS_URL="${TM_METRICS_URL:-http://localhost:9250/metrics}"
MIN_PRE_KILL_CHECKPOINTS="${MIN_PRE_KILL_CHECKPOINTS:-2}"
JM_CONTAINER="${JM_CONTAINER:-01_docker-flink-jobmanager-1}"
# Smoke = compressed main drill. Wall-clock ≈ startup + 30s warm-up + 60s
# pre-kill + recovery (~50s) + 120s post-kill ≈ 5 min, against ~12 min for
# main. The fixed startup/preflight cost is shared by both modes.
SMOKE_WARMUP_S="${SMOKE_WARMUP_S:-30}"
SMOKE_PRE_KILL_S="${SMOKE_PRE_KILL_S:-60}"
SMOKE_POST_KILL_S="${SMOKE_POST_KILL_S:-120}"

PHASE_NAME="tm-kill-full-load"
[ "$C2_MODE" = "smoke" ] && PHASE_NAME="tm-kill-smoke"
PHASE_OUT="$ROOT/logs/tracker-14/${PHASE_NAME}-$(date +%Y%m%d-%H%M%S)"
OUT="$PHASE_OUT/$C2_MODE"
RUN_LOG="$PHASE_OUT/run.log"
mkdir -p "$OUT/j1" "$OUT/bin"
touch "$OUT/checkpoints.jsonl" "$OUT/checkpoints-detail.jsonl"
exec > >(tee -a "$RUN_LOG") 2>&1

# pipeline-lib.sh requires ROOT, OUT, and RATE_HZ before it is sourced.
# shellcheck source=pipeline-lib.sh
source "$SCRIPT_DIR/pipeline-lib.sh"
pipeline_install_cleanup_trap

JOB_ID=""
CP=""
TIERING_ID=""
PREEXISTING_TIERING_ID=""
TIERING_REQUIRED=0
DRILL_FAILED=0
KILL_EPOCH=0
PRE_KILL_MAX_CP=-1
RECOVERY_EPOCH=0
AUXILIARY_JOBS=0

fatal() {
  echo "TM-KILL-FULL-LOAD: FAIL — $*" >&2
  printf 'FAIL\n%s\n' "$*" > "$PHASE_OUT/FAILURE.txt"
  exit 1
}

mark_failure() {
  DRILL_FAILED=1
  echo "TM-KILL-FULL-LOAD: FINDING — $*" >&2
  printf '%s\n' "$*" >> "$PHASE_OUT/findings.txt"
}

# A full-load drill owns the single local TaskManager and both table names.
# Without a process lock, two launches can race through purge/submit, create
# duplicate tiering jobs, exhaust the ten local slots, and misdiagnose the
# resulting NoResourceAvailableException as a recovery failure (observed
# 2026-09-01).  Hold the descriptor for the whole run, including cleanup.
LOCK_FILE="$ROOT/logs/tracker-14/tm-kill-full-load.lock"
mkdir -p "$(dirname "$LOCK_FILE")"
command -v flock >/dev/null 2>&1 || fatal "flock is required for drill serialization"
exec 9>"$LOCK_FILE" || fatal "cannot open drill lock $LOCK_FILE"
flock -n 9 || fatal "another tm-kill-full-load drill is already running"
echo "TM-KILL-FULL-LOAD: lock acquired ($LOCK_FILE)"

require_positive_int() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || fatal "$name='$value' is not a non-negative integer"
  (( 10#$value > 0 )) || fatal "$name='$value' must be > 0"
}

for _arg in \
  "POLL_S=$POLL_S" "WARMUP_S=$WARMUP_S" "PRE_KILL_S=$PRE_KILL_S" \
  "SMOKE_WARMUP_S=$SMOKE_WARMUP_S" "SMOKE_PRE_KILL_S=$SMOKE_PRE_KILL_S" \
  "SMOKE_POST_KILL_S=$SMOKE_POST_KILL_S" \
  "POST_KILL_S=$POST_KILL_S" "RECOVERY_TIMEOUT_S=$RECOVERY_TIMEOUT_S" \
  "NEW_CP_TIMEOUT_S=$NEW_CP_TIMEOUT_S" "TIERING_TIMEOUT_S=$TIERING_TIMEOUT_S"; do
  require_positive_int "${_arg%%=*}" "${_arg#*=}"
done

case "$C2_MODE" in
  smoke|main) ;;
  *) fatal "C2_MODE='$C2_MODE' is invalid; use C2_MODE=smoke for the compressed drill or C2_MODE=main for the full TM-kill drill" ;;
esac

# Smoke window overrides must apply BEFORE the warm-up (2026-09-02 first
# attempt set them after it and the smoke still warmed up 45s).
if [ "$C2_MODE" = "smoke" ]; then
  WARMUP_S="$SMOKE_WARMUP_S"
  PRE_KILL_S="$SMOKE_PRE_KILL_S"
  POST_KILL_S="$SMOKE_POST_KILL_S"
  echo "TM-KILL-FULL-LOAD: smoke = compressed main drill (kill INCLUDED): warmup=${WARMUP_S}s pre-kill=${PRE_KILL_S}s post-kill=${POST_KILL_S}s"
fi

# Never sleep longer than 30s: a bounded wait remains interruptible and the
# harness cannot become an opaque multi-minute sleep after a failed guard.
sleep_bounded() {
  local remaining="$1" chunk
  while (( remaining > 0 )); do
    chunk=$(( remaining > 30 ? 30 : remaining ))
    sleep "$chunk"
    remaining=$(( remaining - chunk ))
  done
}

tm_running() {
  [ "$(docker inspect --format '{{.State.Running}}' "$TM_CONTAINER" 2>/dev/null || true)" = "true" ]
}

tm_registered() {
  curl -fsS --max-time 5 "$FLINK_REST_URL/taskmanagers" 2>/dev/null \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get("taskmanagers") else 1)' \
    >/dev/null 2>&1
}

job_state() {
  local state
  state="$(curl -fsS --max-time 5 "$FLINK_REST_URL/jobs/$JOB_ID" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state", ""))' 2>/dev/null || true)"
  [ -n "$state" ] && printf '%s\n' "$state" || printf '%s\n' "REST_UNAVAILABLE"
}

# Output: count<TAB>max-completed-id<TAB>max-completion-or-trigger-ms.
checkpoint_summary() {
  curl -fsS --max-time 10 "$FLINK_REST_URL/jobs/$JOB_ID/checkpoints" 2>/dev/null \
    | python3 -c '
import json, sys
try:
    history = json.load(sys.stdin).get("history", [])
except Exception:
    print("0\t-1\t0")
    raise SystemExit
done = [c for c in history if str(c.get("status", "")).upper() == "COMPLETED"]
def ident(c):
    try:
        return int(c.get("id", -1))
    except (TypeError, ValueError):
        return -1
def stamp(c):
    vals = []
    for k in ("latest_ack_timestamp", "trigger_timestamp"):
        try:
            vals.append(int(c.get(k, 0) or 0))
        except (TypeError, ValueError):
            pass
    return max(vals or [0])
print("%d\t%d\t%d" % (len(done), max([ident(c) for c in done] or [-1]),
                      max([stamp(c) for c in done] or [0])))
' 2>/dev/null || printf '0\t-1\t0\n'
}

# Find the current Fluss Lake Tiering job. Fluss may resubmit it with a new
# JobID after the same TM loss, so checking only the pre-kill ID is unsafe.
# Output: job_id<TAB>state, preferring a RUNNING instance.
find_tiering() {
  curl -fsS --max-time 10 "$FLINK_REST_URL/jobs/overview" 2>/dev/null \
    | python3 -c '
import json,sys
try:
    jobs=json.load(sys.stdin).get("jobs", [])
except Exception:
    raise SystemExit(0)
matches=[j for j in jobs if j.get("name") == "Fluss Lake Tiering"]
running=[j for j in matches if j.get("state") == "RUNNING"]
j=(running or matches)[-1] if (running or matches) else None
if j:
    print("%s\t%s" % (j.get("jid", ""), j.get("state", "")))
' 2>/dev/null || true
}

alert_count() {
  docker exec 01_docker-alert-consumer-1 sh -c \
    'if [ -f /data/alerts/alerts.jsonl ]; then grep -c "SIGNAL-crit-taskmanager-down" /data/alerts/alerts.jsonl || true; else echo 0; fi' \
    2>/dev/null || printf '0\n'
}

metric_totals() {
  # Return effective input-progress and all-write cumulative totals. The
  # source's live 0|0 snapshot is a known Flink/Fluss observation race; the
  # shared helper falls back only to bounded downstream vertices that prove
  # records are traversing this exact job. Missing metrics remain -1 so the
  # catch-up guard fails closed instead of treating an uninstrumented job as
  # healthy.
  local dump="$1"
  local source writes
  source="$(pipeline_metric_input_progress "$dump")"
  writes="$(printf '%s\n' "$dump" | awk -F'|' '
    $1 !~ /^STATE/ {v=$3; gsub(/^[ \t]+|[ \t]+$/, "", v); if (v ~ /^[0-9]+$/) s+=v}
    END {if (s == "") print -1; else print s}')"
  printf '%s\t%s\n' "${source:--1}" "${writes:--1}"
}

capture_sample() {
  local phase="$1" elapsed="$2" index="$3" epoch dump state totals
  epoch="$(date +%s)"
  dump="$(flink_metric_dump 2>/dev/null || true)"
  printf '%s\n' "$dump" >> "$OUT/metric-dumps.log"
  state="$(printf '%s\n' "$dump" | awk '$1 == "STATE" {print $2; exit}')"
  totals="$(metric_totals "$dump")"
  printf '%s\t%s\t%s\t%s\t%s\n' "$epoch" "$phase" "$elapsed" "${state:-UNKNOWN}" "$totals" \
    >> "$OUT/metric-progress.tsv"
  capture_checkpoint_history "$OUT/checkpoints.jsonl" || true

  # Prometheus is deliberately best-effort during the kill interval: the
  # endpoint is expected to disappear, but samples before and after the loss
  # are required by the post-run analyzer.
  if (( index % 3 == 0 )); then
    local prom
    prom="$(curl -s --max-time 5 "$TM_METRICS_URL" 2>/dev/null || true)"
    printf '%s\n' "$prom" | grep -E '^flink_.*compute_dedup_(first|duplicates)|^flink_.*compute_candles_late_dropped' \
      | sed "s/^/$epoch /" >> "$OUT/tm-prom-dedup-late.tsv" || true
    printf '%s\n' "$prom" | grep -iE '^flink_.*compute_invalid' \
      | sed "s/^/$epoch /" >> "$OUT/tm-prom-invalid.tsv" || true
    printf '%s\n' "$prom" | grep -E '^flink_.*latency' \
      | sed "s/^/$epoch /" >> "$OUT/tm-prom-latency.tsv" || true
    printf '%s\n' "$prom" | grep -E '^flink_taskmanager_job_task_checkpoint' \
      | sed "s/^/$epoch /" >> "$OUT/tm-prom-cp-phases.tsv" || true
    harvest_tm_gc_log "$OUT/tm-gc-live.log" || true
  fi
}

drill_drain_backlog() {
  # G-TMK-10c (2026-09-02): STOP THE FEED and wait for the pipeline to
  # drain before cancel/harvest. Root cause of the phantom-tail F4 orphans
  # (drills 20260902-022843: 4, 20260902-030521: 31 — always the last 1-2
  # windows, 34-64s before run end): post-CHG-120 the pipeline sustains
  # ~9,950/s against a 10,000/s feed — only ~0.5-3% drain margin. The
  # kill's ~500k-row replay backlog therefore NEVER drains while the feed
  # runs; the job gets cancelled while trailing the event clock by ~50s, so
  # tentatives (decided 1s into a window) are in the LOG but their finals
  # are still in the backlog — their settles are in flight, not dropped.
  # F4's 30s grace then misreads the lag tail as orphans. With the feed
  # stopped, the backlog drains at full processing speed (~10-60s) and the
  # F4 verdict becomes exact. The drain gate itself is the guard: a
  # pipeline that cannot drain within DRAIN_TIMEOUT_S fails the drill
  # instead of producing a bogus F4 verdict.
  local timeout_s="${DRAIN_TIMEOUT_S:-180}" stable_needed="${DRAIN_STABLE_POLLS:-3}"
  local poll_s=5 stable=0 last=-1 i=0 dump source
  echo "TM-KILL-FULL-LOAD: drain — stopping feed (faketool pid ${FAKETOOL_PID:-unknown})"
  [ -n "${FAKETOOL_PID:-}" ] && kill -9 "$FAKETOOL_PID" 2>/dev/null || true
  FAKETOOL_PID=""
  local deadline=$(( $(date +%s) + timeout_s ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    dump="$(flink_metric_dump 2>/dev/null || true)"
    source="$(metric_totals "$dump" | awk -F'\t' '{print $1}')"
    i=$((i+1))
    printf '%s\t%s\t%s\t%s\t%s\n' "$(date +%s)" "drain" "$i" \
            "$(printf '%s\n' "$dump" | awk '$1 == "STATE" {print $2; exit}')" \
            "${source:--1}\t-1" >> "$OUT/metric-progress.tsv" || true
    if [ "${source:--1}" -ge 0 ] 2>/dev/null && [ "$source" = "$last" ]; then
      stable=$((stable+1))
    else
      stable=0
    fi
    last="${source:--1}"
    if [ "$stable" -ge "$stable_needed" ]; then
      echo "TM-KILL-FULL-LOAD: drain complete (source stable at ${last} for ${stable_needed} polls)"
      printf 'drained=true\nstable_source=%s\npolls=%s\n' "$last" "$i" > "$OUT/drain.txt"
      return 0
    fi
    sleep_bounded "$poll_s"
  done
  printf 'drained=false\nstable_source=%s\npolls=%s\n' "$last" "$i" > "$OUT/drain.txt"
  mark_failure "pipeline did not drain within ${timeout_s}s of feed stop (source counter still advancing at ${last}) — F4 verdict would be bogus"
  return 1
}

run_load_phase() {
  local phase="$1" duration="$2" elapsed state index
  echo "TM-KILL-FULL-LOAD: phase=$phase duration=${duration}s"
  for ((elapsed=0; elapsed<duration; elapsed+=POLL_S)); do
    state="$(job_state)"
    [ "$state" = "RUNNING" ] \
      || fatal "$phase phase job state=$state at t+${elapsed}s"
    kill -0 "$FAKETOOL_PID" 2>/dev/null \
      || fatal "$phase phase faketool died at t+${elapsed}s"
    kill -0 "$JVM_PID" 2>/dev/null \
      || fatal "$phase phase ingestion JVM died at t+${elapsed}s"
    index=$(( elapsed / POLL_S ))
    capture_sample "$phase" "$elapsed" "$index"
    sleep_bounded "$POLL_S"
  done
}

wait_tm_registered() {
  local timeout_s="$1" i
  for ((i=0; i<timeout_s; i+=2)); do
    tm_running && tm_registered && { echo "TM registered after ${i}s"; return 0; }
    sleep_bounded 2
  done
  return 1
}

active_tiering_count() {
  curl -fsS --max-time 10 "$FLINK_REST_URL/jobs/overview" 2>/dev/null \
    | python3 -c '
import json,sys
active={"RUNNING","RESTARTING","FAILING","INITIALIZING","RECONCILING","CANCELING"}
try:
    jobs=json.load(sys.stdin).get("jobs", [])
except Exception:
    print(-1)
    raise SystemExit
print(sum(1 for j in jobs if j.get("name") == "Fluss Lake Tiering"
          and j.get("state") in active))
' 2>/dev/null || printf '%s\n' -1
}

cancel_preflight_tiering() {
  [ "$TIERING_REQUIRED" -eq 1 ] || return 0
  local state i
  state="$(curl -fsS --max-time 10 "$FLINK_REST_URL/jobs/$PREEXISTING_TIERING_ID" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state", ""))' \
    2>/dev/null || true)"
  case "$state" in
    RUNNING|RESTARTING|FAILING|INITIALIZING|RECONCILING|CANCELING)
      echo "TM-KILL-FULL-LOAD: stopping pre-existing tiering job $PREEXISTING_TIERING_ID before table purge"
      docker exec 01_docker-flink-jobmanager-1 flink cancel "$PREEXISTING_TIERING_ID" \
        >/dev/null 2>&1 \
        || fatal "cannot stop pre-existing tiering job $PREEXISTING_TIERING_ID before purge" ;;
    FAILED|CANCELED|FINISHED) return 0 ;;
    *) fatal "cannot determine pre-existing tiering job state before purge: $state" ;;
  esac
  for ((i=0; i<90; i+=2)); do
    [ "$(active_tiering_count)" = "0" ] && return 0
    sleep_bounded 2
  done
  fatal "pre-existing tiering job did not stop before table purge"
}

wait_job_recovery() {
  local timeout_s="$1" i state previous="" seen_transition=0
  : > "$OUT/recovery-timeline.tsv"
  printf 'epoch\telapsed_s\tstate\n' > "$OUT/recovery-timeline.tsv"
  for ((i=0; i<timeout_s; i+=2)); do
    state="$(job_state)"
    printf '%s\t%s\t%s\n' "$(date +%s)" "$i" "$state" >> "$OUT/recovery-timeline.tsv"
    if [ "$state" != "$previous" ]; then
      echo "TM-KILL-FULL-LOAD: recovery t+${i}s state=$state"
      previous="$state"
    fi
    case "$state" in
      FAILING|RESTARTING|CANCELING|INITIALIZING|RECONCILING)
        seen_transition=1 ;;
      FAILED|CANCELED|FINISHED|SUSPENDED)
        fatal "SignalJob entered terminal state=$state after TM kill" ;;
    esac
    if [ "$state" = "RUNNING" ] && [ "$seen_transition" -eq 1 ]; then
      RECOVERY_EPOCH="$(date +%s)"
      echo "TM-KILL-FULL-LOAD: SignalJob recovered after $((RECOVERY_EPOCH - KILL_EPOCH))s"
      return 0
    fi
    sleep_bounded 2
  done
  fatal "SignalJob did not complete a FAILED/RESTARTING -> RUNNING recovery within ${timeout_s}s"
}

wait_new_checkpoint() {
  local timeout_s="$1" kill_ms="$2" i count max_id max_ts
  for ((i=0; i<timeout_s; i+=5)); do
    capture_checkpoint_history "$OUT/checkpoints.jsonl" || true
    IFS=$'\t' read -r count max_id max_ts <<< "$(checkpoint_summary)"
    echo "TM-KILL-FULL-LOAD: post-kill checkpoints t+${i}s count=${count:-0} max_id=${max_id:--1} max_ts=${max_ts:-0}"
    if [ "${max_id:--1}" -gt "$PRE_KILL_MAX_CP" ] 2>/dev/null \
      && [ "${max_ts:-0}" -ge "$kill_ms" ] 2>/dev/null; then
      echo "${max_id}\t${max_ts}" > "$OUT/post-kill-checkpoint.txt"
      return 0
    fi
    sleep_bounded 5
  done
  fatal "no completed checkpoint newer than pre-kill max=${PRE_KILL_MAX_CP} and kill=${kill_ms}ms within ${timeout_s}s"
}

wait_tiering_recovery() {
  [ "$TIERING_REQUIRED" -eq 1 ] || {
    echo "tiering job was absent before drill — no tiering recovery assertion" > "$OUT/tiering-recovery.txt"
    return 0
  }
  local timeout_s="$1" i row jid state
  for ((i=0; i<timeout_s; i+=5)); do
    row="$(find_tiering)"
    jid="${row%%$'\t'*}"
    state="${row#*$'\t'}"
    if [ -n "$row" ] && [ "$state" = "RUNNING" ]; then
      TIERING_ID="$jid"
      printf 'PASS\t%s\t%s\n' "$jid" "$state" > "$OUT/tiering-recovery.txt"
      echo "TM-KILL-FULL-LOAD: tiering job RUNNING after TM recovery (job=$jid)"
      return 0
    fi
    case "$state" in
      FAILED|CANCELED|FINISHED)
        printf 'FAIL\t%s\t%s\n' "${jid:-unknown}" "$state" > "$OUT/tiering-recovery.txt"
        mark_failure "Fluss Lake Tiering did not recover after TM kill (job=${jid:-unknown}, state=$state)"
        return 0 ;;
    esac
    sleep_bounded 5
  done
  printf 'FAIL\t%s\t%s\n' "${TIERING_ID:-unknown}" "timeout" > "$OUT/tiering-recovery.txt"
  mark_failure "Fluss Lake Tiering was not RUNNING within ${timeout_s}s after TM recovery"
}

# ---------- preflight guards ------------------------------------------------
command -v docker >/dev/null 2>&1 || fatal "docker is required"
command -v curl >/dev/null 2>&1 || fatal "curl is required"
command -v python3 >/dev/null 2>&1 || fatal "python3 is required"

echo "TM-KILL-FULL-LOAD: start $(date -Iseconds)"
echo "TM-KILL-FULL-LOAD: mode=$C2_MODE evidence=$PHASE_OUT rate=${RATE_HZ}Hz warmup=${WARMUP_S}s pre=${PRE_KILL_S}s post=${POST_KILL_S}s"
echo "TM-KILL-FULL-LOAD: restart contract attempts=${RESTART_MAX_ATTEMPTS} delay_ms=${RESTART_DELAY_MS} checkpoint_ms=${CHECKPOINT_INTERVAL_MS}"

# G-TMK-0: validate host-side file mounts before probing the cluster. Short
# Compose bind syntax can turn a missing jar/script into a directory and the
# already-created container then fails with an opaque OCI exit 127. This check
# makes the drill's first failure local and actionable.
pipeline_validate_compose_bind_sources \
  || fatal "Compose file-bind preflight failed; restore the listed regular files before starting the stack"

# G-TMK-1: no competing compute job. A second SignalJob would read the same
# raw log and write the same candle keys, making a parity failure ambiguous.
# The Compose stack starts one read-only Babysitter observer by default; it
# reads Positions, not raw_table_1, so one instance is explicitly allowed.
# Any other active job, or duplicate Babysitter, remains a hard failure.
OVERVIEW="$(curl -fsS --max-time 10 "$FLINK_REST_URL/jobs/overview" 2>/dev/null)" \
  || fatal "Flink REST /jobs/overview unavailable"
JOB_LINES="$(printf '%s' "$OVERVIEW" | python3 -c '
import json,sys
for j in json.load(sys.stdin).get("jobs", []):
    print("%s\t%s\t%s" % (j.get("jid", ""), j.get("state", ""), j.get("name", "")))
' 2>/dev/null)" || fatal "cannot parse Flink /jobs/overview"
TIERING_JOBS=0
while IFS=$'\t' read -r _jid _state _name; do
  [ -n "${_jid:-}" ] || continue
  if [ "$_name" = "Fluss Lake Tiering" ]; then
    case "$_state" in
      RUNNING|RESTARTING|FAILING|INITIALIZING|RECONCILING|CANCELING)
        TIERING_JOBS=$((TIERING_JOBS + 1))
        TIERING_REQUIRED=1
        PREEXISTING_TIERING_ID="$_jid" ;;
    esac
    TIERING_ID="$_jid"
    if [ "$_state" = "RUNNING" ]; then
      : # It will be re-submitted after the mandatory fresh-TM preflight.
    elif [ "$_state" = "FAILED" ] || [ "$_state" = "CANCELED" ] || [ "$_state" = "FINISHED" ]; then
      echo "TM-KILL-FULL-LOAD: prior tiering job is $_state; it will be restarted with the guarded contract"
    else
      fatal "Fluss Lake Tiering is in transitional state before drill: job=$_jid state=$_state"
    fi
  elif [ "$_state" = "RUNNING" ] || [ "$_state" = "RESTARTING" ] || [ "$_state" = "FAILING" ] \
      || [ "$_state" = "INITIALIZING" ] || [ "$_state" = "RECONCILING" ] \
      || [ "$_state" = "CANCELING" ]; then
    if [ "$_name" = "Babysitter Positions observer" ]; then
      AUXILIARY_JOBS=$((AUXILIARY_JOBS + 1))
      echo "TM-KILL-FULL-LOAD: allowing one read-only auxiliary job: job=$_jid name=$_name state=$_state"
    else
      fatal "competing Flink job before drill: job=$_jid name=$_name state=$_state"
    fi
  fi
done <<< "$JOB_LINES"
[ "${TIERING_JOBS:-0}" -le 1 ] 2>/dev/null \
  || fatal "multiple Fluss Lake Tiering jobs detected before drill; refusing an ambiguous run"
[ "${AUXILIARY_JOBS:-0}" -le 1 ] 2>/dev/null \
  || fatal "duplicate Babysitter auxiliary jobs detected before drill; refusing slot/resource ambiguity"
printf 'job_id\tstate\tname\n%s\n' "$JOB_LINES" > "$OUT/preflight-jobs.tsv"
printf 'before\t%s\t%s\n' "${TIERING_ID:-none}" "$TIERING_REQUIRED" > "$OUT/tiering-recovery.txt"
ALERTS_BEFORE="$(alert_count)"

# G-TMK-2/G-TMK-3: canonical artifact, port, Fluss, and TM-registration
# preflight. PURGE_STRICT prevents stale raw/preview history invalidating G7.
export PURGE_STRICT=true ALLOW_FULL_REPLAY=false
export CHECKPOINT_INTERVAL_MS CHECKPOINT_TIMEOUT_MS
export RESTART_MAX_ATTEMPTS RESTART_DELAY_MS
pipeline_preflight || fatal "pipeline preflight failed"
cancel_preflight_tiering
pipeline_purge_raw_table || fatal "strict raw table purge failed"
if [ "$TIERING_REQUIRED" -eq 1 ]; then
  # pipeline_preflight deliberately restarts the TM to clear direct buffers;
  # that invalidates any co-hosted tiering execution.  Re-submit it only
  # after both table recreations, and require tiering-start's effective
  # fixed-delay strategy guard before the destructive kill.
  bash "$SCRIPT_DIR/tiering-start.sh" \
    || fatal "tiering restart failed after fresh-TM preflight"
fi
pipeline_start_faketool || fatal "faketool start failed"
pipeline_start_ingestion || fatal "ingestion start failed"
pipeline_purge_preview_table || fatal "strict preview table purge failed"
pipeline_ensure_tentative_markers_table || fatal "tentative-markers table ensure failed"
pipeline_submit_job || fatal "SignalJob submission failed"
[ -n "$JOB_ID" ] || fatal "SignalJob submission returned an empty job id"
flink_wait_state RUNNING 90 || fatal "SignalJob did not reach RUNNING after submission"

# G-TMK-4: warm-up must show live raw-path progress before any kill is
# attempted. The helper remains fail-closed when neither the raw source nor
# its bounded downstream proof vertices report progress.
echo "TM-KILL-FULL-LOAD: warming up ${WARMUP_S}s"
sleep_bounded "$WARMUP_S"
WARMUP_READ=-1
echo "TM-KILL-FULL-LOAD: warm-up metric gate (+${WARMUP_GRACE_S}s report grace)"
WARMUP_DEADLINE=$(( $(date +%s) + WARMUP_GRACE_S ))
while :; do
  WARMUP_DUMP="$(flink_metric_dump 2>/dev/null || true)"
  WARMUP_READ="$(metric_totals "$WARMUP_DUMP" | awk -F'\t' '{print $1}')"
  if [ "${WARMUP_READ:--1}" -gt 0 ] 2>/dev/null; then
    echo "TM-KILL-FULL-LOAD: warm-up gate passed (effective_input=${WARMUP_READ})"
    break
  fi
  if [ "$(date +%s)" -ge "$WARMUP_DEADLINE" ]; then break; fi
  sleep_bounded 10
done
printf '%s\n' "${WARMUP_DUMP:-}" > "$OUT/metrics-warmup.txt"
[ "${WARMUP_READ:--1}" -gt 0 ] 2>/dev/null \
  || fatal "raw-path progress metric is not advancing after warm-up + ${WARMUP_GRACE_S}s report grace (effective_input=${WARMUP_READ:--1}; raw source and bounded downstream proof vertices are all empty; last dump saved to metrics-warmup.txt)"

RUN_START="$(date +%s)"
printf '%s\n' "$RUN_START" > "$OUT/run-start-epoch"
printf 'epoch\tphase\telapsed_s\tstate\tsource_read\tall_writes\n' > "$OUT/metric-progress.tsv"

# G-TMK-5/G-TMK-6: prove the job is healthy and has recoverable state before
# the destructive action. The kill is at the end of this sustained phase.
run_load_phase pre-kill "$PRE_KILL_S"
IFS=$'\t' read -r PRE_CP_COUNT PRE_KILL_MAX_CP PRE_CP_MAX_TS <<< "$(checkpoint_summary)"
[ "${PRE_CP_COUNT:-0}" -ge "$MIN_PRE_KILL_CHECKPOINTS" ] 2>/dev/null \
  || fatal "only ${PRE_CP_COUNT:-0} completed checkpoints before kill; need ${MIN_PRE_KILL_CHECKPOINTS}"
[ "${PRE_KILL_MAX_CP:--1}" -ge 0 ] 2>/dev/null \
  || fatal "checkpoint history has no usable completed checkpoint before kill"
printf '%s\t%s\t%s\t%s\n' "$PRE_CP_COUNT" "$PRE_KILL_MAX_CP" "$PRE_CP_MAX_TS" "$JOB_ID" \
  > "$OUT/pre-kill-checkpoint.txt"

KILL_EPOCH="$(date +%s)"
KILL_MS=$(( KILL_EPOCH * 1000 ))
printf '%s\n' "$KILL_EPOCH" > "$OUT/tm-kill-epoch"
echo "TM-KILL-FULL-LOAD: KILLING $TM_CONTAINER at epoch=$KILL_EPOCH pre_kill_max_checkpoint=$PRE_KILL_MAX_CP"
tm_running || fatal "TaskManager is not running at the kill point"
TM_CONTAINER_ID="$(docker inspect --format '{{.Id}}' "$TM_CONTAINER" 2>/dev/null || true)"
[ -n "$TM_CONTAINER_ID" ] || fatal "cannot resolve TaskManager container id at kill point"
docker kill -s KILL "$TM_CONTAINER" >/dev/null \
  || fatal "docker SIGKILL failed for $TM_CONTAINER"

# G-TMK-7: Docker's local daemon has historically not applied
# restart:unless-stopped after docker kill -s KILL. Compensate explicitly;
# never call the TM recovered merely because the kill command returned 0.
TM_UP=0
for _i in 1 2 3 4 5; do
  if tm_running; then TM_UP=1; break; fi
  sleep_bounded 2
done
if [ "$TM_UP" -ne 1 ]; then
  echo "TM-KILL-FULL-LOAD: restart policy did not bring TM up — explicit docker start"
  docker start "$TM_CONTAINER" >/dev/null \
    || fatal "explicit docker start failed for $TM_CONTAINER_ID"
fi
wait_tm_registered 120 \
  || fatal "TaskManager did not re-register with JobManager within 120s"
TM_UP_EPOCH="$(date +%s)"
printf '%s\t%s\n' "$TM_UP_EPOCH" "$TM_CONTAINER_ID" > "$OUT/tm-recovered.txt"

# G-TMK-8: require an observed Flink recovery transition. A job that simply
# remains RUNNING after its only TM was killed is not evidence of restoration.
wait_job_recovery "$RECOVERY_TIMEOUT_S"

# G-TMK-9: regular checkpoint completion after the kill proves that state
# snapshotting resumed, not just that the REST endpoint says RUNNING.
wait_new_checkpoint "$NEW_CP_TIMEOUT_S" "$KILL_MS"

# Continue feeding after recovery so the kill-boundary windows close and the
# source has time to drain the replay/catch-up backlog. Live Flink counters
# may reset when recovered task attempts are installed, so do not compare one
# snapshot before the phase with one after it.
run_load_phase post-kill "$POST_KILL_S"
POST_PROGRESS="$(python3 "$SCRIPT_DIR/c2_progress.py" \
  "$OUT/metric-progress.tsv" post-kill 2>/dev/null)" \
  || fatal "post-recovery metric phase had no numeric samples"
# shellcheck disable=SC2034  # the summary TSV's trailing sample count: parsed to keep field
# positions aligned with c2_progress.py's as_tsv(), deliberately not asserted (the phase already
# fails when it has no numeric samples).
IFS=$'\t' read -r POST_START_READ POST_END_READ POST_READ_DELTA POST_READ_INCREASES \
  POST_START_WRITE POST_END_WRITE POST_WRITE_DELTA POST_WRITE_INCREASES \
  POST_READ_RESETS POST_WRITE_RESETS POST_SAMPLES <<< "$POST_PROGRESS"
printf '%s\n' "$POST_PROGRESS" > "$OUT/post-recovery-progress.txt"

# G-TMK-10: both source input and output must advance after recovery. Missing
# metrics are a hard failure: an uninstrumented/no-op job cannot pass. The
# reset-aware deltas handle task-attempt counter resets, while the increase
# count prevents a reset alone from being mistaken for recovered progress.
[ "${POST_READ_DELTA:--1}" -gt 0 ] && [ "${POST_READ_INCREASES:--1}" -gt 0 ] 2>/dev/null \
  || fatal "source did not advance after recovery: first=${POST_START_READ:--1} last=${POST_END_READ:--1} delta=${POST_READ_DELTA:--1} resets=${POST_READ_RESETS:--1}"
[ "${POST_WRITE_DELTA:--1}" -gt 0 ] && [ "${POST_WRITE_INCREASES:--1}" -gt 0 ] 2>/dev/null \
  || fatal "SignalJob output did not advance after recovery: first=${POST_START_WRITE:--1} last=${POST_END_WRITE:--1} delta=${POST_WRITE_DELTA:--1} resets=${POST_WRITE_RESETS:--1}"

# G-TMK-10b: the kill-induced restart is the ONLY restart this drill
# tolerates. A second RUNNING->RESTARTING/FAILED transition during post-kill
# catch-up (observed 2026-09-02: synchronous marker I/O stalled the task
# mailbox until checkpoint RPCs timed out and Flink escalated to a global
# failure) violates the recovery contract even if counters later resume.
RESTARTS_SINCE_KILL="$(docker logs --since "$((KILL_EPOCH - 5))" "$JM_CONTAINER" 2>&1 \
  | grep -F "$JOB_ID" | grep -c "switched from state RUNNING to RESTARTING" || true)"
FAILED_SINCE_KILL="$(docker logs --since "$((KILL_EPOCH - 5))" "$JM_CONTAINER" 2>&1 \
  | grep -F "$JOB_ID" | grep -c "switched from state RUNNING to FAILED" || true)"
printf 'restarts=%s\nfailed=%s\n' "${RESTARTS_SINCE_KILL:-0}" "${FAILED_SINCE_KILL:-0}" \
  > "$OUT/restarts-since-kill.txt"
[ "${RESTARTS_SINCE_KILL:-0}" -le 1 ] 2>/dev/null \
  || fatal "job restarted ${RESTARTS_SINCE_KILL} times after the TM kill (expected exactly 1 — the kill-induced recovery); recovery contract violated"
[ "${FAILED_SINCE_KILL:-0}" -eq 0 ] 2>/dev/null \
  || fatal "job transitioned to FAILED after the TM kill (${FAILED_SINCE_KILL} times)"

# G-TMK-10c: stop the feed and drain the replay backlog before cancel —
# in-flight settles must land before the F4 verdict (see drill_drain_backlog).
# Applies to BOTH modes (smoke exercises the drain plumbing too).
drill_drain_backlog || true

if [ "$C2_MODE" = "smoke" ]; then
  # Smoke verdict: kill + recovery + new checkpoint + post-kill progress +
  # the single-restart contract are the smoke gates. G7c/F4 parity needs a
  # full-length window and stays main-only. The RUNNING check MUST run
  # BEFORE pipeline_cleanup cancels the job (2026-09-02 first attempt
  # checked it after cleanup and failed itself).
  [ "$(job_state)" = "RUNNING" ] \
    || fatal "smoke: job is not RUNNING at verdict time ($(job_state))"
  flink_metric_dump > "$OUT/metrics-final.txt" || true
  capture_checkpoint_history "$OUT/checkpoints.jsonl" || true
  harvest_tm_gc_log "$OUT/tm-gc-final.log" || true
  RUN_END="$(date +%s)"
  printf '%s\n' "$RUN_END" > "$OUT/run-end-epoch"
  pipeline_cleanup
  sleep_bounded 5
  if [ "$DRILL_FAILED" -eq 0 ]; then
    cat > "$PHASE_OUT/RESULT.txt" <<EOF
PASS
Smoke (compressed main drill): TM kill, recovery, new checkpoint, post-kill progress, single-restart contract all green.
job_id=$JOB_ID
kill_epoch=$KILL_EPOCH
pre_kill_seconds=$PRE_KILL_S
post_kill_seconds=$POST_KILL_S
restarts_since_kill=${RESTARTS_SINCE_KILL:-0}
EOF
    echo "TM-KILL-FULL-LOAD: PASS — smoke drill green; full main drill (G7c+F4) is authorized"
    exit 0
  fi
  cat > "$PHASE_OUT/RESULT.txt" <<EOF
FAIL
Smoke drill failed before the G7c analysis stage.
job_id=$JOB_ID
kill_epoch=$KILL_EPOCH
restarts_since_kill=${RESTARTS_SINCE_KILL:-0}
EOF
  echo "TM-KILL-FULL-LOAD: FAIL — see $PHASE_OUT/findings.txt and run.log" >&2
  exit 1
fi

wait_tiering_recovery "$TIERING_TIMEOUT_S"
ALERTS_DURING="$(alert_count)"

# Harvest while the live job still exists; REST checkpoint history is trimmed
# after cancel. Then cancel only our SignalJob and leave the shared stack up.
flink_metric_dump > "$OUT/metrics-final.txt" || true
capture_checkpoint_history "$OUT/checkpoints.jsonl" || true
harvest_tm_gc_log "$OUT/tm-gc-final.log" || true
RUN_END="$(date +%s)"
printf '%s\n' "$RUN_END" > "$OUT/run-end-epoch"
pipeline_cleanup
sleep_bounded 5
ALERTS_AFTER="$(alert_count)"
printf 'before=%s\nduring=%s\nafter=%s\nrule=SIGNAL-crit-taskmanager-down\n' \
  "$ALERTS_BEFORE" "$ALERTS_DURING" "$ALERTS_AFTER" > "$OUT/alert-check.txt"

# G-TMK-11: G7c is the data-integrity verdict. Disable only latency guards:
# catch-up necessarily creates an expected latency burst. All data-quality,
# F6, F8, and settlement guards remain active.
echo "TM-KILL-FULL-LOAD: running G7 parity analysis (latency guard intentionally disabled)"
LATENCY_GUARD_OFF=1 python3 "$SCRIPT_DIR/holistic-analyze.py" \
  "$PHASE_OUT" "$CP" "$RUN_START" "$RUN_END" \
  | tee "$OUT/latency-analysis.txt"
ANALYSIS_RC=${PIPESTATUS[0]}
G7C_LINE="$(grep -E '^- G7c parity:' "$OUT/latency-analysis.txt" | tail -1 || true)"
G7C_COMPARED="$(printf '%s\n' "$G7C_LINE" | sed -n 's/^- G7c parity: \([0-9][0-9]*\) fully.*/\1/p')"
printf 'analysis_rc=%s\ng7c_line=%s\ng7c_compared=%s\n' \
  "$ANALYSIS_RC" "$G7C_LINE" "${G7C_COMPARED:-0}" > "$OUT/analysis-guard.txt"
[ "$ANALYSIS_RC" -eq 0 ] \
  || fatal "holistic analysis failed; see $OUT/latency-analysis.txt"
[ -n "$G7C_COMPARED" ] && [ "$G7C_COMPARED" -gt 0 ] 2>/dev/null \
  || fatal "G7c compared zero fully-closed windows — refusing a degenerate PASS"
grep -q "G7: data-quality guards passed" "$OUT/latency-analysis.txt" \
  || fatal "G7 data-quality pass marker missing"

if [ "$DRILL_FAILED" -eq 0 ]; then
  cat > "$PHASE_OUT/RESULT.txt" <<EOF
PASS
TM-kill-at-full-load recovery and G7 zero-loss parity passed.
job_id=$JOB_ID
kill_epoch=$KILL_EPOCH
recovery_epoch=$RECOVERY_EPOCH
pre_kill_completed_checkpoints=$PRE_CP_COUNT
pre_kill_max_checkpoint=$PRE_KILL_MAX_CP
g7c_compared=$G7C_COMPARED
taskmanager_alert_before=$ALERTS_BEFORE
taskmanager_alert_during=$ALERTS_DURING
taskmanager_alert_after=$ALERTS_AFTER
EOF
  echo "TM-KILL-FULL-LOAD: PASS — recovery + zero-loss parity proven"
  exit 0
fi

cat > "$PHASE_OUT/RESULT.txt" <<EOF
FAIL
TM recovery completed but one or more holistic acceptance guards failed.
job_id=$JOB_ID
kill_epoch=$KILL_EPOCH
recovery_epoch=$RECOVERY_EPOCH
EOF
echo "TM-KILL-FULL-LOAD: FAIL — see $PHASE_OUT/findings.txt and analysis evidence" >&2
exit 1
