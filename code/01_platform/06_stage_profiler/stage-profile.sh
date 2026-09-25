#!/usr/bin/env bash
# ═════════════════════════════════════════════════════════════════════════════
# stage-profile.sh — the single end-to-end stage profiler (greenfield).
#
# One command, one run, one report:
#
#   fake broker -> Go bridge -> Java ingestion (N containers) -> Fluss
#   raw_table_1 -> Flink -> Fluss feature tables (candles + signals)
#
# Per-step latency + throughput plus the end-to-end path, from the native
# meters the pipeline already ships. Nothing on the data path is modified:
# this file *runs* existing pieces (images, jars, probes, stage-capture.sh)
# and reads existing evidence. pipeline-lib.sh is sourced read-only for the
# proven bring-up helpers — no existing file is edited by this directory
# (see README.md).
#
# Flow:
#   phase smoke: preflight -> purge -> fleet -> job -> compile probes
#                -> warm-up (raw-log growth, bounded) -> capture -> raw sample
#                -> teardown -> PRESENCE GATE (fail-closed)
#   phase main:  same bring-up -> capture -> raw sample -> teardown
#   report:      profile.md + profile.tsv + presence.json at OUT_ROOT
#
# Env knobs (defaults): NSE_PATH (full 2433 universe), RATE_HZ=20,
# SMOKE_S=120, MAIN_S=900, PHASES=smoke,main (e.g. PHASES=smoke runs only the
# verified-smoke phase and reports it), INGESTION_CONTAINERS=3,
# PROBE_TOKEN_COUNT=12, RAW_SAMPLE_ROWS_PER_BUCKET=2000, WARMUP_S=45,
# OUT=logs/stage-profile-<ts>, CHECK_ONLY=0 (validate and exit).
#
# Session: the IST clock decides MULTITF_SESSION_BYPASS (default true when the
# 09:15-15:30 IST session is closed) so the candle path aggregates off-hours —
# the same rule holistic-measure.sh applies; production never bypasses.
# ═════════════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

NSE_PATH="${NSE_PATH:-$ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv}"
RATE_HZ="${RATE_HZ:-20}"
SMOKE_S="${SMOKE_S:-120}"
MAIN_S="${MAIN_S:-900}"
PHASES="${PHASES:-smoke,main}"
WARMUP_S="${WARMUP_S:-45}"
INGESTION_CONTAINERS="${INGESTION_CONTAINERS:-3}"
PROBE_TOKEN_COUNT="${PROBE_TOKEN_COUNT:-12}"
RAW_SAMPLE_ROWS_PER_BUCKET="${RAW_SAMPLE_ROWS_PER_BUCKET:-2000}"
MULTITF_ENABLED="${MULTITF_ENABLED:-true}"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
PROBE_BOOTSTRAP="${PROBE_BOOTSTRAP:-localhost:9123}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
OUT_ROOT="${OUT:-$ROOT/logs/stage-profile-$RUN_TS}"

SCRIPTS_DIR="$ROOT/code/01_platform/04_scripts"
STACK_DIR="$ROOT/code/01_platform/01_docker"
CAPTURE_SH="$SCRIPTS_DIR/stage-capture.sh"
PROFILE_PY="$SCRIPT_DIR/stage_profiler.py"
CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"

FAKETOOL_NAME="sp-faketool"
ING_PREFIX="sp-ingestion-"
LOG_PIDS=()
JOB_ID=""
CP=""
PROBE_BIN=""
PHASE_NAME=""
PHASE_DIR=""
TOKENS=""
UNIVERSE_ROWS=0

say() { printf 'stage-profile: %s\n' "$*"; }
fail() { printf 'stage-profile: FAIL — %s\n' "$*" >&2; exit 1; }

usage() {
  awk 'NR > 3 && /^set -euo pipefail/{exit} NR > 3 {sub(/^# ?/, ""); print}' \
    "${BASH_SOURCE[0]}"
  exit 0
}

case "${1:-}" in
  -h|--help) usage ;;
esac

# ── read-only bring-up library (never modified; sourced for its proven helpers)
# shellcheck source=/dev/null
source "$SCRIPTS_DIR/pipeline-lib.sh"

# ── teardown ────────────────────────────────────────────────────────────────

stop_fleet() {
  local i pid c
  if [ "${#LOG_PIDS[@]}" -gt 0 ]; then
    for pid in "${LOG_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    LOG_PIDS=()
  fi
  if [ -n "${FAKETOOL_LOG_PID:-}" ]; then
    kill "$FAKETOOL_LOG_PID" 2>/dev/null || true
    FAKETOOL_LOG_PID=""
  fi
  if [ -n "${PHASE_DIR:-}" ] && [ -d "$PHASE_DIR/capture" ]; then
    mkdir -p "$PHASE_DIR/capture/ticks"
    for i in $(seq 0 $((INGESTION_CONTAINERS - 1))); do
      c="$ING_PREFIX$i"
      if docker inspect "$c" >/dev/null 2>&1; then
        docker cp "$c:/tmp/arrow-tick-counts.txt" \
          "$PHASE_DIR/capture/ticks/$c.tick-counts.txt" 2>/dev/null || true
      fi
    done
  fi
  for i in $(seq 0 $((INGESTION_CONTAINERS - 1))); do
    docker rm -f "$ING_PREFIX$i" >/dev/null 2>&1 || true
  done
  docker rm -f "$FAKETOOL_NAME" >/dev/null 2>&1 || true
  if [ -n "${JOB_ID:-}" ]; then
    ( cd "$STACK_DIR" && docker compose --env-file .env --env-file secrets.env \
        exec -T flink-jobmanager flink cancel "$JOB_ID" ) >/dev/null 2>&1 || true
    JOB_ID=""
  fi
}

cleanup() {
  local rc=$?
  trap - EXIT
  stop_fleet >/dev/null 2>&1 || true
  exit "$rc"
}
trap cleanup EXIT

# ── preflight (mirrors the proven full-universe runs; no pipeline_preflight) ──

preflight() {
  say "preflight ($PHASE_NAME): Fluss ready, fresh taskmanager, image stamp"
  docker exec "$FLUSS_COORDINATOR_CONTAINER" sh -c 'exit 0' >/dev/null \
    || fail "Fluss coordinator '$FLUSS_COORDINATOR_CONTAINER' not reachable — is the dev stack up?"
  CP="$(cat "$CP_FILE")" || fail "missing Fluss classpath file: $CP_FILE"
  export CP
  OUT="$PHASE_DIR/capture"
  mkdir -p "$OUT"
  export OUT
  pipeline_compile_fluss_ready_probe
  pipeline_wait_for_fluss_ready
  ( cd "$STACK_DIR" && docker compose --env-file .env --env-file secrets.env \
      restart flink-taskmanager ) >"$PHASE_DIR/tm-restart.log" 2>&1 \
    || fail "taskmanager restart failed (see $PHASE_DIR/tm-restart.log)"
  sleep 12
  local i
  for i in $(seq 1 12); do
    if curl -fsS "$FLINK_REST_URL/taskmanagers" 2>/dev/null | grep -q '"id"'; then break; fi
    [ "$i" -eq 12 ] && fail "taskmanager did not register with the jobmanager"
    sleep 5
  done
  export PIPELINE_PREFLIGHT_OK=1
  local want got
  want="$(pipeline_loadgen_input_stamp)" || fail "cannot compute the loadgen build stamp"
  got="$(docker run --pull never --rm --network none "$LIB_LOADGEN_IMAGE" \
         cat /app/build-stamp 2>/dev/null | tr -d '[:space:]')"
  [ "$want" = "$got" ] \
    || fail "loadgen image is stale (image=$got sources=$want) — rebuild it before measuring"
}

purge_tables() {
  export PURGE_STRICT=true ALLOW_FULL_REPLAY=false
  pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/02_raw_table_1.sql" raw
  pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/05_signal_candidates.sql" signals
  if [ "$MULTITF_ENABLED" = "true" ]; then
    pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/32_candle_live.sql" candle_live
    pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/33_candle_closed.sql" candle_closed
    pipeline_ensure_candle_tables "$ROOT/code/01_platform/02_sql/ddl/32_candle_live.sql" candle_live
    pipeline_ensure_candle_tables "$ROOT/code/01_platform/02_sql/ddl/33_candle_closed.sql" candle_closed
  fi
}

# ── fleet, job, capture, sample ─────────────────────────────────────────────

start_fleet() {
  local out="$PHASE_DIR/capture"
  local rows per_slice i sfx t c
  rows="$(tail -n +2 "$NSE_PATH" | grep -c . || true)"
  [ "$rows" -gt 0 ] || fail "universe file has no rows: $NSE_PATH"
  [ $((rows % INGESTION_CONTAINERS)) -eq 0 ] \
    || fail "universe rows ($rows) not divisible by INGESTION_CONTAINERS ($INGESTION_CONTAINERS)"
  per_slice=$((rows / INGESTION_CONTAINERS))
  UNIVERSE_ROWS="$rows"
  say "fleet: $rows instruments -> $INGESTION_CONTAINERS x $per_slice (rate ${RATE_HZ}Hz)"
  head -1 "$NSE_PATH" > "$out/header.csv"
  tail -n +2 "$NSE_PATH" | split -l "$per_slice" -d -a2 - "$out/slice_"
  for i in $(seq 0 $((INGESTION_CONTAINERS - 1))); do
    sfx="$(printf '%02d' "$i")"
    cat "$out/header.csv" "$out/slice_$sfx" > "$out/manifest-$i.csv"
    mkdir -p "$out/j1-$i"
  done

  LIB_FAKETOOL_CONTAINER="$FAKETOOL_NAME" OUT="$out" pipeline_start_faketool

  for i in $(seq 0 $((INGESTION_CONTAINERS - 1))); do
    docker run -d --name "$ING_PREFIX$i" --network "$LIB_TRADING_NET" \
      -v "$out":/run -v "$out/j1-$i":/logs \
      -e LOG_DIR=/logs -e READINESS_FILE_PATH="/run/ingestion-$i.loadtest.ready" \
      -e "ARROW_HFT_URL=ws://$FAKETOOL_NAME:$FAKETOOL_PORT" -e ARROW_BRIDGE_BIN=/app/arrow-bridge \
      -e ARROW_FAKE_BROKER=1 -e TRANSPORT=proto -e SECRETS_VIA_ENV_FILE=1 \
      -e DEPLOYMENT_ENV=dev \
      --env-file "$LIB_SECRETS_FILE" \
      -e "ARROW_APP_ID=${ARROW_APP_ID:-testd}" -e "ARROW_USER_ID=${ARROW_USER_ID:-testd-user}" \
      -e "INSTRUMENT_MANIFEST_PATH=/run/manifest-$i.csv" \
      -e FLUSS_BOOTSTRAP=fluss-coordinator:9123 -e FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 \
      -e RAW_TABLE_NAME=raw_table_1 -e ARROW_HFT_CONNECTIONS=1 \
      -e ARROW_MAX_EVENT_AGE_MS=5000 -e ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000 \
      -e ARROW_HFT_LATENCY_MS=50 -e CLOCK_CHECK_REQUIRED=false \
      -e OTEL_COLLECTOR_HOST=otel-collector:4318 -e METRICS_LOCAL_LOG=1 \
      -e FLUSS_WRITER_MODE=generic -e FLUSS_WRITERS=1 -e FLUSS_WRITER_BATCH_SIZE_BYTES=0 \
      -e ARROW_TICK_COUNTS=30 "$LIB_LOADGEN_IMAGE" \
      java --add-opens=java.base/java.nio=ALL-UNNAMED -Xms512m -Xmx512m \
        -XX:MaxDirectMemorySize=512m \
        '-Xlog:gc*,safepoint:file=/logs/gc.log:time,uptime,level,tags' -Dlog.dir=/logs \
        -cp /app/ingestion.jar com.trading.ingestion.IngestionService >/dev/null
  done

  for i in $(seq 0 $((INGESTION_CONTAINERS - 1))); do
    docker logs -f "$ING_PREFIX$i" > "$out/j1-$i/java.out" 2>&1 &
    LOG_PIDS+=("$!")
  done

  for i in $(seq 0 $((INGESTION_CONTAINERS - 1))); do
    c="$ING_PREFIX$i"
    for t in $(seq 1 60); do
      [ -f "$out/ingestion-$i.loadtest.ready" ] && break
      if [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" != "true" ]; then
        fail "$c exited before readiness — last log lines: $(tail -3 "$out/j1-$i/java.out" | tr '\n' '|')"
      fi
      [ "$t" -eq 60 ] && fail "$c readiness file never appeared"
      sleep 2
    done
    for t in $(seq 1 90); do
      if grep -qF "HFT subscribed $per_slice" "$out/j1-$i/java.out" 2>/dev/null; then break; fi
      [ "$t" -eq 90 ] && fail "$c never confirmed subscription to $per_slice instruments"
      sleep 2
    done
  done
  say "fleet up: subscriptions confirmed"
}

submit_job() {
  JOB_ID=""
  export MULTITF_ENABLED
  OUT="$PHASE_DIR/capture" pipeline_submit_job
  [ -n "$JOB_ID" ] || fail "SignalJob submit produced no JOB_ID"
  say "job submitted: $JOB_ID"
  local i state
  for i in $(seq 1 60); do
    state="$(curl -fsS "$FLINK_REST_URL/jobs/$JOB_ID" 2>/dev/null \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))' 2>/dev/null || true)"
    case "$state" in
      RUNNING) return 0 ;;
      FAILED|CANCELED|FINISHED) fail "job entered $state" ;;
    esac
    sleep 2
  done
  fail "job did not reach RUNNING within 120s"
}

compile_probes() {
  PROBE_BIN="$PHASE_DIR/probes"
  mkdir -p "$PROBE_BIN"
  javac -cp "$CP" -d "$PROBE_BIN" \
    "$SCRIPTS_DIR/fluss-probes/FlussReadLagProbe.java" \
    "$SCRIPT_DIR/RawSampleReader.java" \
    >"$PHASE_DIR/probes.javac.log" 2>&1 \
    || fail "compiling the probes failed (see $PHASE_DIR/probes.javac.log)"
}

raw_log_end() {
  PROBE_READ_DEADLINE_MS=10000 timeout 30 java -Dlog.dir=/tmp/fluss-probe-logs \
    -cp "$PROBE_BIN:$CP" FlussReadLagProbe default raw_table_1 "$PROBE_BOOTSTRAP" 2>/dev/null \
    | tail -1 | cut -f5
}

# Bounded warm-up on a REAL completion signal: the raw log must advance twice.
warmup() {
  local deadline prev=0 now
  deadline=$(( $(date +%s) + WARMUP_S ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    now="$(raw_log_end || true)"
    case "$now" in
      ''|*[!0-9]*) ;;
      *) if [ "$prev" -gt 0 ] && [ "$now" -gt "$prev" ]; then
           say "warm-up: raw log advancing ($prev -> $now) — pipeline live"
           return 0
         fi
         prev="$now" ;;
    esac
    sleep 5
  done
  say "WARN: warm-up window ended without two growing raw samples; continuing (capture will show it)"
}

run_capture() {
  local secs="$1"
  say "capture ${secs}s -> $PHASE_DIR/stages"
  JOB_ID="$JOB_ID" DURATION_S="$secs" \
    INGESTION_JAVA_OUT="$PHASE_DIR/capture/j1-0/java.out" \
    FLUSS_PROBE_CP="$CP" OUT_DIR="$PHASE_DIR/stages" PROBE_TOKENS="$TOKENS" \
    FLINK_REST_URL="$FLINK_REST_URL" RATE_HZ="$RATE_HZ" \
    bash "$CAPTURE_SH" >"$PHASE_DIR/stage-capture.log" 2>&1 \
    || fail "stage-capture failed (see $PHASE_DIR/stage-capture.log)"
  say "capture done ($(wc -l < "$PHASE_DIR/stages/stages.tsv" 2>/dev/null || echo 0) Flink rows)"
}

sample_raw() {
  say "S1 raw sample: RawSampleReader raw_table_1 (${PROBE_TOKEN_COUNT} tokens x ${RAW_SAMPLE_ROWS_PER_BUCKET}/bucket)"
  if ! timeout 120 java \
      --add-opens=java.base/java.lang=ALL-UNNAMED \
      --add-opens=java.base/java.nio=ALL-UNNAMED \
      -Dlog.dir=/tmp/fluss-probe-logs -cp "$PROBE_BIN:$CP" RawSampleReader \
      default raw_table_1 "$TOKENS" "$RAW_SAMPLE_ROWS_PER_BUCKET" "$PROBE_BOOTSTRAP" \
      >"$PHASE_DIR/capture/raw-sample.jsonl" 2>"$PHASE_DIR/capture/raw-sample.err"; then
    say "WARN: raw sample failed or timed out — the presence gate refuses a missing S1"
  fi
}

run_phase() {
  PHASE_NAME="$1"
  local secs="$2"
  PHASE_DIR="$OUT_ROOT/$PHASE_NAME"
  mkdir -p "$PHASE_DIR/capture/ticks" "$PHASE_DIR/stages"
  say "=== phase '$PHASE_NAME' (${secs}s capture) ==="
  preflight
  purge_tables
  start_fleet
  submit_job
  compile_probes
  warmup
  run_capture "$secs"
  sample_raw
  stop_fleet
  say "phase '$PHASE_NAME' done"
}

check_only() {
  say "check-only: validating inputs (nothing is started)"
  [ -r "$NSE_PATH" ] || fail "unreadable universe: $NSE_PATH"
  local rows
  rows="$(tail -n +2 "$NSE_PATH" | grep -c . || true)"
  [ "$rows" -gt 0 ] || fail "universe has no data rows"
  [ $((rows % INGESTION_CONTAINERS)) -eq 0 ] \
    || fail "universe rows ($rows) not divisible by $INGESTION_CONTAINERS"
  [ -r "$CP_FILE" ] || fail "missing Fluss classpath file: $CP_FILE (build ingestion first)"
  [ -r "$CAPTURE_SH" ] || fail "missing $CAPTURE_SH"
  [ -r "$PROFILE_PY" ] || fail "missing $PROFILE_PY"
  command -v docker >/dev/null || fail "docker not on PATH"
  command -v javac >/dev/null || fail "javac not on PATH"
  command -v python3 >/dev/null || fail "python3 not on PATH"
  python3 "$PROFILE_PY" stages >/dev/null || fail "stage_profiler.py does not run"
  say "check-only OK: $rows instruments -> $INGESTION_CONTAINERS x $((rows / INGESTION_CONTAINERS)); smoke ${SMOKE_S}s, main ${MAIN_S}s"
}

# ── main ────────────────────────────────────────────────────────────────────

TOKENS="$(awk -F, -v n="$PROBE_TOKEN_COUNT" \
  'NR > 1 && c < n { print $4; c++ }' "$NSE_PATH" | paste -sd, -)"
[ -n "$TOKENS" ] || fail "could not read probe tokens from $NSE_PATH"

if [ "${CHECK_ONLY:-0}" = "1" ]; then
  check_only
  exit 0
fi

# Off-hours runs: mirror holistic-measure.sh — when the 09:15-15:30 IST session
# is closed, default MULTITF_SESSION_BYPASS=true so the candle path aggregates
# (production never bypasses; the flag exists for exactly this). An explicit
# env value always wins.
_now_ist="$(TZ=Asia/Kolkata date +%H%M)"
_ist_min=$((10#${_now_ist:0:2} * 60 + 10#${_now_ist:2:2}))
if [ "$_ist_min" -ge 555 ] && [ "$_ist_min" -lt 930 ]; then
  _session_open=true
  _bypass_default=false
else
  _session_open=false
  _bypass_default=true
fi
MULTITF_SESSION_BYPASS="${MULTITF_SESSION_BYPASS:-$_bypass_default}"
export MULTITF_SESSION_BYPASS
say "[session] open=$_session_open ist_minutes=$_ist_min MULTITF_SESSION_BYPASS=$MULTITF_SESSION_BYPASS"

mkdir -p "$OUT_ROOT"
printf 'MULTITF_SESSION_BYPASS=%s ist_minutes_now=%s session_open=%s\n' \
  "$MULTITF_SESSION_BYPASS" "$_ist_min" "$_session_open" > "$OUT_ROOT/session.txt"
say "universe: $NSE_PATH @ ${RATE_HZ}Hz (smoke ${SMOKE_S}s, main ${MAIN_S}s)"
say "evidence: $OUT_ROOT"

if [[ ",$PHASES," == *",smoke,"* ]]; then
  run_phase smoke "$SMOKE_S"
  if python3 "$PROFILE_PY" presence --phase "$OUT_ROOT/smoke" > "$OUT_ROOT/smoke-presence.json"; then
    say "smoke presence: PASS (every stage produced samples)"
  else
    say "SMOKE PRESENCE GATE FAILED — refusing the main phase:"
    cat "$OUT_ROOT/smoke-presence.json" >&2
    exit 1
  fi
  if [[ ",$PHASES," != *",main,"* ]]; then
    python3 "$PROFILE_PY" report --phase "$OUT_ROOT/smoke" --out "$OUT_ROOT" \
      --title "Stage profile (smoke) — ${UNIVERSE_ROWS} instruments @ ${RATE_HZ}Hz, ${SMOKE_S}s"
    say "REPORT: $OUT_ROOT/profile.md"
    exit 0
  fi
fi

if [[ ",$PHASES," == *",main,"* ]]; then
  run_phase main "$MAIN_S"
  python3 "$PROFILE_PY" report --phase "$OUT_ROOT/main" --out "$OUT_ROOT" \
    --title "Stage profile — ${UNIVERSE_ROWS} instruments @ ${RATE_HZ}Hz, ${MAIN_S}s"
  say "REPORT: $OUT_ROOT/profile.md"
fi
