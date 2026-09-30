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
# Env knobs (defaults): NSE_PATH (full 2433 universe), RATE_HZ=2,
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
RATE_HZ="${RATE_HZ:-2}"
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
# FEED=faketool (default): the 2 Hz fake broker (unchanged).
# FEED=real: one ingestion container against the real broker DataStream
#   (wss://ds.arrow.trade, ARROW_FEED=token, mode full); slots = ceil(rows/1024)
#   bridge connections inside that one process (2 433 -> 3 x 811). Market-hours
#   only for measurement; ALLOW_OFFHOURS_REAL=1 permits a bring-up-only dry run.
FEED="${FEED:-faketool}"
ALLOW_OFFHOURS_REAL="${ALLOW_OFFHOURS_REAL:-0}"
BRINGUP_ONLY="${BRINGUP_ONLY:-0}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
OUT_ROOT="${OUT:-$ROOT/logs/stage-profile-$RUN_TS}"

SCRIPTS_DIR="$ROOT/code/01_platform/04_scripts"
STACK_DIR="$ROOT/code/01_platform/01_docker"
CAPTURE_SH="$SCRIPTS_DIR/stage-capture.sh"
# CT-1 (docs/plans/2026-09-29-checkpoint-tail-remediation.md): the summary
# checkpoint endpoint's phase fields are null; only the details endpoint
# carries per-subtask phases, and only while the job is alive.
CP_PHASE_TOOL="$SCRIPTS_DIR/cp_phase_capture.py"
PROFILE_PY="$SCRIPT_DIR/stage_profiler.py"
CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"

FAKETOOL_NAME="sp-faketool"
ING_PREFIX="sp-ingestion-"
# W3-i (docs/plans/2026-09-30-w3-w4-design-note.md): JVM instrumentation for the
# residual-stall attribution. The profiler's java command already logs GC to
# /logs/gc.log (captured via the j1 mount); this adds the bounded JFR recording
# plus an independent /tmp GC copy, on every per-run ingestion writer. Both are
# pulled into the round evidence by stop_fleet(). No application behaviour
# change; an empty value disables it.
INGESTION_JAVA_TOOL_OPTIONS="${INGESTION_JAVA_TOOL_OPTIONS:--Xlog:gc*,safepoint:file=/tmp/gc.log:time,uptime,level,tags:filecount=1,filesize=50m -XX:StartFlightRecording=settings=profile,duration=1800s,filename=/tmp/ing-diag.jfr,maxsize=64m}"
LOG_PIDS=()
JOB_ID=""
CP=""
CP_PHASES_PID=""
STATS_PID=""
PROBE_BIN=""
PHASE_NAME=""
PHASE_DIR=""
TOKENS=""
UNIVERSE_ROWS=0

say() { printf 'stage-profile: %s\n' "$*"; }
fail() { printf 'stage-profile: FAIL — %s\n' "$*" >&2; exit 1; }

# Read one KEY=value from the stack .env without docker's env-file parsing:
# strips an inline ` # comment`, surrounding whitespace and quotes. Used for
# the two non-secret real-broker identifiers; secrets come from secrets.env.
stack_env_value() {
  local key="$1" line
  line="$(grep -E "^${key}=" "$STACK_DIR/.env" 2>/dev/null | head -1 || true)"
  [ -n "$line" ] || return 0
  line="${line#*=}"
  line="${line%%#*}"
  printf '%s' "$(printf '%s' "$line" | tr -d '[:space:]"'"'"'')"
}

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
  if [ -n "${CP_PHASES_PID:-}" ]; then
    kill "$CP_PHASES_PID" 2>/dev/null || true
    CP_PHASES_PID=""
  fi
  if [ -n "${STATS_PID:-}" ]; then
    kill "$STATS_PID" 2>/dev/null || true
    STATS_PID=""
  fi
  if [ -n "${PHASE_DIR:-}" ] && [ -d "$PHASE_DIR/capture" ]; then
    mkdir -p "$PHASE_DIR/capture/ticks"
    for i in $(seq 0 $((INGESTION_CONTAINERS - 1))); do
      c="$ING_PREFIX$i"
      if docker inspect "$c" >/dev/null 2>&1; then
        # W3-i: stop gracefully FIRST — the ingestion JFR is configured with a
        # filename and only materialises when the recording ends (duration
        # expiry or JVM exit); `docker rm -f` (SIGKILL) would leave 0 bytes.
        docker stop -t 10 "$c" >/dev/null 2>&1 || true
        docker cp "$c:/tmp/arrow-tick-counts.txt" \
          "$PHASE_DIR/capture/ticks/$c.tick-counts.txt" 2>/dev/null || true
        docker cp "$c:/tmp/gc.log" \
          "$PHASE_DIR/capture/ticks/$c.gc.log" 2>/dev/null || true
        docker cp "$c:/tmp/ing-diag.jfr" \
          "$PHASE_DIR/capture/ticks/$c.jfr" 2>/dev/null || true
      fi
    done
  fi
  # W3-i: the TM JVM's own GC log + JFR (fresh per phase — preflight restarts
  # the taskmanager); both are the primary evidence for spike attribution.
  if [ -n "${PHASE_DIR:-}" ] && [ -d "$PHASE_DIR/stages" ]; then
    docker cp "$FLINK_TM_CONTAINER:/opt/flink/log/gc.log" \
      "$PHASE_DIR/stages/tm-gc.log" 2>/dev/null || true
    docker cp "$FLINK_TM_CONTAINER:/opt/flink/log/tm-diag.jfr" \
      "$PHASE_DIR/stages/tm-diag.jfr" 2>/dev/null || true
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
  # CHG-458: fresh changelog storage per phase — the FS changelog base path is a
  # persistent shared volume; after each TM restart the in-memory file registry
  # no longer tracks earlier files, so leftovers produced the "state is not in
  # tracking" WARN floods (457-465 per window) and unbounded growth (1.9 GB over
  # a day). Profiler runs are independent measurements (no restore), so a fresh
  # base path is the correct precondition. Fail closed: a dirty changelog
  # invalidates the round's comparisons.
  docker exec "$FLINK_TM_CONTAINER" sh -c \
    'rm -rf /checkpoints/changelog && mkdir -p /checkpoints/changelog' \
    >"$PHASE_DIR/changelog-wipe.log" 2>&1 \
    || fail "changelog base-path wipe failed (see $PHASE_DIR/changelog-wipe.log)"
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

# FEED=real market-hours guard: DataStream carries live ticks only during the
# NSE session; outside it the broker re-sends stale snapshots, which Java's
# ARROW_MAX_EVENT_AGE_MS gate drops — a capture run would fail its presence
# gate by design. ALLOW_OFFHOURS_REAL=1 permits a wiring dry run (pair with
# BRINGUP_ONLY=1).
real_market_hours_guard() {
  [ "$ALLOW_OFFHOURS_REAL" = "1" ] && return 0
  local now_ist ist_min
  now_ist="$(TZ=Asia/Kolkata date +%H%M)"
  ist_min=$((10#${now_ist:0:2} * 60 + 10#${now_ist:2:2}))
  if [ "$ist_min" -ge 555 ] && [ "$ist_min" -le 930 ]; then
    return 0
  fi
  fail "FEED=real is outside the NSE session (09:15-15:30 IST) — live ticks only then; set ALLOW_OFFHOURS_REAL=1 (with BRINGUP_ONLY=1) for a wiring dry run"
}

start_fleet() {
  local out="$PHASE_DIR/capture"
  local rows per_slice i sfx t c
  rows="$(tail -n +2 "$NSE_PATH" | grep -c . || true)"
  [ "$rows" -gt 0 ] || fail "universe file has no rows: $NSE_PATH"
  [ $((rows % INGESTION_CONTAINERS)) -eq 0 ] \
    || fail "universe rows ($rows) not divisible by INGESTION_CONTAINERS ($INGESTION_CONTAINERS)"
  per_slice=$((rows / INGESTION_CONTAINERS))
  UNIVERSE_ROWS="$rows"

  # ── real broker branch (FEED=real) ──────────────────────────────────────
  # ONE container, `slots` bridge connections inside it (the bridge caps a
  # connection at 1024 tokens). The full universe file is used unfiltered.
  if [ "$FEED" = "real" ]; then
    real_market_hours_guard
    [ "$INGESTION_CONTAINERS" -eq 1 ] \
      || fail "FEED=real runs ONE ingestion container (got INGESTION_CONTAINERS=$INGESTION_CONTAINERS; set it to 1)"
    local slots=$(( (rows + 1023) / 1024 ))
    [ "$slots" -ge 1 ] && [ "$slots" -le 3 ] \
      || fail "FEED=real: $rows tokens exceed the 3 x 1024 slots capacity"
    # Non-secret identifiers from the stack .env, read explicitly: `docker run
    # --env-file .env` does NOT strip inline comments, and .env has one on
    # ARROW_INSTRUMENT_TOKENS. Secrets stay in secrets.env via --env-file.
    local real_app_id real_user_id
    real_app_id="$(stack_env_value ARROW_APP_ID)"
    real_user_id="$(stack_env_value ARROW_USER_ID)"
    [ -n "$real_app_id" ] || fail "FEED=real needs ARROW_APP_ID in $STACK_DIR/.env"
    [ -n "$real_user_id" ] || fail "FEED=real needs ARROW_USER_ID in $STACK_DIR/.env"
    say "fleet (real): $rows instruments -> 1 container x $slots slots (max 1024 tokens/slot, unfiltered universe)"
    cp "$NSE_PATH" "$out/manifest-00.csv"
    mkdir -p "$out/j1-0"
    docker run -d --name "${ING_PREFIX}0" --network "$LIB_TRADING_NET" \
      -v "$out":/run -v "$out/j1-0":/logs \
      -e LOG_DIR=/logs -e READINESS_FILE_PATH="/run/ingestion-0.loadtest.ready" \
      -e ARROW_BRIDGE_BIN=/app/arrow-bridge -e TRANSPORT=proto -e SECRETS_VIA_ENV_FILE=1 \
      -e DEPLOYMENT_ENV=dev \
      --env-file "$LIB_SECRETS_FILE" \
      -e "ARROW_APP_ID=$real_app_id" -e "ARROW_USER_ID=$real_user_id" \
      -e ARROW_FEED=token \
      -e "JAVA_TOOL_OPTIONS=$INGESTION_JAVA_TOOL_OPTIONS" \
      -e "ARROW_HFT_CONNECTIONS=$slots" -e ARROW_HFT_MULTI_CONNECTION_APPROVED=true \
      -e INSTRUMENT_MANIFEST_PATH=/run/manifest-00.csv \
      -e FLUSS_BOOTSTRAP=fluss-coordinator:9123 -e FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 \
      -e RAW_TABLE_NAME=raw_table_1 \
      -e ARROW_MAX_EVENT_AGE_MS=5000 -e ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000 \
      -e CLOCK_CHECK_REQUIRED=false \
      -e OTEL_COLLECTOR_HOST=otel-collector:4318 -e METRICS_LOCAL_LOG=1 \
      -e FLUSS_WRITER_MODE=generic -e FLUSS_WRITERS=1 -e FLUSS_WRITER_BATCH_SIZE_BYTES=0 \
      -e ARROW_TICK_COUNTS=30 "$LIB_LOADGEN_IMAGE" \
      java --add-opens=java.base/java.nio=ALL-UNNAMED -Xms512m -Xmx512m \
        -XX:MaxDirectMemorySize=512m \
        '-Xlog:gc*,safepoint:file=/logs/gc.log:time,uptime,level,tags' -Dlog.dir=/logs \
        -cp /app/ingestion.jar com.trading.ingestion.IngestionService >/dev/null
    docker logs -f "${ING_PREFIX}0" > "$out/j1-0/java.out" 2>&1 &
    LOG_PIDS+=("$!")
    for t in $(seq 1 60); do
      [ -f "$out/ingestion-0.loadtest.ready" ] && break
      if [ "$(docker inspect -f '{{.State.Running}}' "${ING_PREFIX}0" 2>/dev/null)" != "true" ]; then
        fail "${ING_PREFIX}0 exited before readiness — last log lines: $(tail -3 "$out/j1-0/java.out" | tr '\n' '|')"
      fi
      [ "$t" -eq 60 ] && fail "${ING_PREFIX}0 readiness file never appeared"
      sleep 2
    done
    # Token mode logs one "HFT subscribed <n> tokens" line per slot (811 x 3
    # for the 2 433 universe). Wait for every slot AND for the summed counts to
    # equal the universe, not for one exact per-container number.
    local got_slots got_tokens
    for t in $(seq 1 90); do
      got_slots="$(grep -c 'HFT subscribed' "$out/j1-0/java.out" 2>/dev/null || true)"
      got_tokens="$(grep -o 'HFT subscribed [0-9]* tokens' "$out/j1-0/java.out" 2>/dev/null \
        | awk '{s+=$3} END {print s+0}')"
      if [ "${got_slots:-0}" -ge "$slots" ] && [ "${got_tokens:-0}" -eq "$rows" ]; then break; fi
      [ "$t" -eq 90 ] && fail "${ING_PREFIX}0 never confirmed $slots slot subscriptions totalling $rows tokens (slots=${got_slots:-0} tokens=${got_tokens:-0})"
      sleep 2
    done
    say "fleet up (real): subscriptions confirmed (${got_slots} slots, ${got_tokens} tokens)"
    return 0
  fi
  # ── end real broker branch ──────────────────────────────────────────────

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
      -e "JAVA_TOOL_OPTIONS=$INGESTION_JAVA_TOOL_OPTIONS" \
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

# W3-i: per-container CPU/memory sampled at ~1 s during the phase — separates
# host/container scheduling stalls from JVM-internal stops when the next round's
# spike windows are attributed (W3/W4 design note). Killed by stop_fleet().
start_stats_sampler() {
  local f="$1"
  say "container stats sampler: docker stats -> $f"
  (
    while :; do
      printf 'ts=%s utc=%s\n' "$(date +%s.%N)" "$(date -u +%H:%M:%S)"
      docker stats --no-stream \
        --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' 2>/dev/null || true
      # Host pressure (PSI): a container preempted by the host shows up as
      # psi-cpu/io "some" rising while its own CPU% drops — the only host-level
      # discriminator between scheduling stalls and in-container stalls.
      for r in cpu io memory; do
        printf 'psi-%s ' "$r"
        head -1 "/proc/pressure/$r" 2>/dev/null || true
      done
      printf -- '--\n'
      sleep 1
    done
  ) >"$f" 2>&1 &
  STATS_PID=$!
}

run_capture() {
  local secs="$1"
  say "capture ${secs}s -> $PHASE_DIR/stages"
  JOB_ID="$JOB_ID" DURATION_S="$secs" \
    INGESTION_JAVA_OUT="$PHASE_DIR/capture/j1-0/java.out" \
    FLUSS_PROBE_CP="$CP" OUT_DIR="$PHASE_DIR/stages" PROBE_TOKENS="$TOKENS" \
    FLINK_REST_URL="$FLINK_REST_URL" RATE_HZ="$CAPTURE_RATE_HZ" \
    bash "$CAPTURE_SH" >"$PHASE_DIR/stage-capture.log" 2>&1 \
    || fail "stage-capture failed (see $PHASE_DIR/stage-capture.log)"
  say "capture done ($(wc -l < "$PHASE_DIR/stages/stages.tsv" 2>/dev/null || echo 0) Flink rows)"
}

# CT-1: per-subtask checkpoint phases, sampled while the job is alive (the
# checkpoint history is rolling; a cancel loses it). Started before the
# warm-up so the first checkpoints are covered; ended by a stop file so the
# wait costs at most one poll interval. Fails closed: a terminal checkpoint
# without a phase record is a failed capture, not a silent gap.
start_cp_phase_sampler() {
  local secs="$1"
  say "checkpoint phase sampler: /jobs/\$JOB_ID/checkpoints/details (capture window)"
  python3 "$CP_PHASE_TOOL" --rest-url "$FLINK_REST_URL" --job-id "$JOB_ID" \
    --out-dir "$PHASE_DIR/stages" --stop-file "$PHASE_DIR/.cp-phases-stop" \
    --duration "$((secs + WARMUP_S + 600))" \
    >"$PHASE_DIR/stages-cp-phases.log" 2>&1 &
  CP_PHASES_PID=$!
}

wait_cp_phase_sampler() {
  [ -n "${CP_PHASES_PID:-}" ] || return 0
  touch "$PHASE_DIR/.cp-phases-stop"
  local rc=0 n
  wait "$CP_PHASES_PID" || rc=$?
  CP_PHASES_PID=""
  [ "$rc" -eq 0 ] || fail "checkpoint phase capture failed (rc=$rc, see $PHASE_DIR/stages-cp-phases.log)"
  n="$(wc -l < "$PHASE_DIR/stages/cp-phases-detail.jsonl" 2>/dev/null || true)"
  [ "${n:-0}" -gt 0 ] \
    || fail "checkpoint phase capture wrote no records (see $PHASE_DIR/stages-cp-phases.log)"
  say "checkpoint phase capture: ${n} checkpoints"
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
  if [ "${BRINGUP_ONLY:-0}" = "1" ]; then
    say "BRINGUP_ONLY=1 — fleet and subscriptions confirmed, no capture; tearing down"
    stop_fleet
    say "bring-up-only: PASS"
    exit 0
  fi
  submit_job
  compile_probes
  start_cp_phase_sampler "$secs"
  start_stats_sampler "$PHASE_DIR/stages/docker-stats.log"
  warmup
  run_capture "$secs"
  wait_cp_phase_sampler
  sample_raw
  stop_fleet
  if [ -r "$SCRIPTS_DIR/stage_gc_summary.py" ]; then
    python3 "$SCRIPTS_DIR/stage_gc_summary.py" "$PHASE_DIR" \
      || say "WARN: gc summary failed (non-fatal)"
  fi
  say "phase '$PHASE_NAME' done"
}

check_only() {
  say "check-only: validating inputs (nothing is started)"
  [ -r "$NSE_PATH" ] || fail "unreadable universe: $NSE_PATH"
  local rows
  rows="$(tail -n +2 "$NSE_PATH" | grep -c . || true)"
  [ "$rows" -gt 0 ] || fail "universe has no data rows"
  case "$FEED" in
    faketool | real) ;;
    *) fail "FEED must be 'faketool' or 'real' (got '$FEED')" ;;
  esac
  local slots=0
  if [ "$FEED" = "real" ]; then
    [ "$INGESTION_CONTAINERS" -eq 1 ] \
      || fail "FEED=real runs ONE ingestion container (got INGESTION_CONTAINERS=$INGESTION_CONTAINERS; set it to 1)"
    slots=$(( (rows + 1023) / 1024 ))
    [ "$slots" -ge 1 ] && [ "$slots" -le 3 ] \
      || fail "FEED=real: $rows tokens exceed the 3 x 1024 slots capacity"
  else
    [ $((rows % INGESTION_CONTAINERS)) -eq 0 ] \
      || fail "universe rows ($rows) not divisible by $INGESTION_CONTAINERS"
  fi
  [ -r "$CP_FILE" ] || fail "missing Fluss classpath file: $CP_FILE (build ingestion first)"
  [ -r "$CAPTURE_SH" ] || fail "missing $CAPTURE_SH"
  [ -r "$CP_PHASE_TOOL" ] || fail "missing $CP_PHASE_TOOL"
  [ -r "$PROFILE_PY" ] || fail "missing $PROFILE_PY"
  command -v docker >/dev/null || fail "docker not on PATH"
  command -v javac >/dev/null || fail "javac not on PATH"
  command -v python3 >/dev/null || fail "python3 not on PATH"
  python3 "$PROFILE_PY" stages >/dev/null || fail "stage_profiler.py does not run"
  if [ "$FEED" = "real" ]; then
    say "check-only OK: $rows instruments -> FEED=real 1 container x $slots slots; smoke ${SMOKE_S}s, main ${MAIN_S}s"
  else
    say "check-only OK: $rows instruments -> $INGESTION_CONTAINERS x $((rows / INGESTION_CONTAINERS)); smoke ${SMOKE_S}s, main ${MAIN_S}s"
  fi
}

# ── main ────────────────────────────────────────────────────────────────────

TOKENS="$(awk -F, -v n="$PROBE_TOKEN_COUNT" \
  'NR > 1 && c < n { print $4; c++ }' "$NSE_PATH" | paste -sd, -)"
[ -n "$TOKENS" ] || fail "could not read probe tokens from $NSE_PATH"

if [ "${CHECK_ONLY:-0}" = "1" ]; then
  check_only
  exit 0
fi

# FEED validation runs in both paths (a typo must fail before anything starts).
case "$FEED" in
  faketool | real) ;;
  *) fail "FEED must be 'faketool' or 'real' (got '$FEED')" ;;
esac
FEED_LABEL="${RATE_HZ} Hz fake broker"
CAPTURE_RATE_HZ="$RATE_HZ"
if [ "$FEED" = "real" ]; then
  FEED_LABEL="real broker DataStream full"
  # Full-mode DataStream delivers ~1 Hz per token (measured 2026-09-24); the
  # value is a label for stage-capture's rate_hz field, not a pacing input.
  CAPTURE_RATE_HZ=1
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
say "universe: $NSE_PATH, ${FEED_LABEL} (smoke ${SMOKE_S}s, main ${MAIN_S}s)"
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
      --title "Stage profile (smoke) — ${UNIVERSE_ROWS} instruments, ${FEED_LABEL}, ${SMOKE_S}s"
    say "REPORT: $OUT_ROOT/profile.md"
    exit 0
  fi
fi

if [[ ",$PHASES," == *",main,"* ]]; then
  run_phase main "$MAIN_S"
  python3 "$PROFILE_PY" report --phase "$OUT_ROOT/main" --out "$OUT_ROOT" \
    --title "Stage profile — ${UNIVERSE_ROWS} instruments, ${FEED_LABEL}, ${MAIN_S}s"
  say "REPORT: $OUT_ROOT/profile.md"
fi
