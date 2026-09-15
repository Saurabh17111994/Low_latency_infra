#!/usr/bin/env bash
# stage-soak-e2e.sh — end-to-end soak at 2433 NSE stocks x RATE_HZ (no INJECT).
# NEW file for the 48k/s e2e (does NOT modify pipeline-lib.sh / stage scripts).
# Reuses pipeline-lib primitives; differs from stage-a2-baseline.sh in:
#   - the full NSE file, not pipeline-lib.sh's 1024-token slice
#   - 3 ingestion containers x 811 tokens (bridge single-socket policy)
#   - DURATION_S passed in (120 smoke / 900 main)
# Usage: DURATION_S=120 bash stage-soak-e2e.sh
# Knobs: NSE_PATH (universe CSV; defaults to pipeline-lib's LIB_MANIFEST),
#        TICK_STARTUP_BUDGET_S (max unexplained idle seconds between container
#        start and last tick; default 45, observed 3-34), TICK_TOLERANCE_PCT,
#        RATE_HZ, DURATION_S, FLINK_REST_URL.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROOT" || { echo "!! cannot cd to repo root $ROOT"; exit 1; }
RATE_HZ="${RATE_HZ:-20}"
DURATION_S="${DURATION_S:-120}"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
COMPOSE_DIR="$ROOT/code/01_platform/01_docker"
PHASE_OUT="$ROOT/logs/soak-e2e-$(date +%Y%m%d-%H%M%S)"
OUT="$PHASE_OUT/capture"
export OUT RATE_HZ MULTITF_SESSION_BYPASS MULTITF_SIGNAL_CONTEXT_ENABLED
# Candle-phase focus (2026-09-05): skip the per-tick signal-context snapshot
# build (signal-job work) unless the caller explicitly enables it.
: "${MULTITF_SIGNAL_CONTEXT_ENABLED:=false}"
export MULTITF_SIGNAL_CONTEXT_ENABLED
mkdir -p "$OUT/j1-0" "$OUT/j1-1" "$OUT/j1-2"
exec > >(tee -a "$PHASE_OUT/run.log") 2>&1
# shellcheck disable=SC1091
source "$SCRIPT_DIR/pipeline-lib.sh"
JOB_ID=""
# P6-568: `docker logs -f` mirrors are background children; without tracking
# their PIDs they outlive the containers and accumulate one orphan per run.
LOG_PIDS=""
# shellcheck disable=SC2034  # lib input: pipeline-lib.sh reads it when managing the faketool
LIB_FAKETOOL_CONTAINER="e2e-faketool"
# P6-566: derive the universe from the repo (pipeline-lib already resolves it
# relative to ROOT) instead of a developer-machine absolute path, so the drill
# runs on any host/CI checkout. NSE_PATH overrides for an out-of-tree universe.
NSE="${NSE_PATH:-$LIB_MANIFEST}"
fatal() { echo "SOAK-E2E: FAIL — $*" >&2; printf 'FAIL\n%s\n' "$*" > "$PHASE_OUT/FAILURE.txt"; exit 1; }
cleanup() {
  # P6-568: stop the log mirrors first — after `docker rm -f` they would spin
  # on a dead container, and a leaked tail keeps the run's fd/process alive.
  for p in $LOG_PIDS; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  # ING-TCP-001: drain the bridge's per-token emitted-tick counts BEFORE the
  # containers die (the file lives on the container fs; docker rm destroys
  # it). The bridge writes /tmp/arrow-tick-counts.txt at every ARROW_TICK_
  # COUNTS interval and at shutdown; per-container copies land in the
  # capture dir for the losslessness reconcile (broker bytes -> Fluss rows).
  for c in e2e-ingestion-0 e2e-ingestion-1 e2e-ingestion-2; do
    [ -d "$OUT" ] || continue
    if docker cp "$c:/tmp/arrow-tick-counts.txt" "$OUT/$c.tick-counts.txt" \
        >/dev/null 2>&1; then
      echo "cleanup: drained tick counts from $c"
    fi
  done
  for c in e2e-ingestion-0 e2e-ingestion-1 e2e-ingestion-2 e2e-faketool; do
    docker rm -f "$c" >/dev/null 2>&1 || true
  done
  if [ -n "$JOB_ID" ]; then
    echo "cleanup: cancelling SignalJob $JOB_ID"
    # P6-568: a swallowed cancel used to leave the job running with no signal
    # beyond a clean-looking teardown; say so and name the manual fix.
    if ! ( cd "$COMPOSE_DIR" && docker compose --env-file .env --env-file secrets.env exec -T flink-jobmanager flink cancel "$JOB_ID" ) >/dev/null 2>&1; then
      echo "!! WARN: cleanup could not cancel SignalJob $JOB_ID — it may still be running; cancel with: flink cancel $JOB_ID" >&2
    fi
  fi
  echo "cleanup: removed soak containers job=$JOB_ID"
}
trap cleanup EXIT
[ -r "$NSE" ] || fatal "NSE manifest not readable: $NSE"
# P6-217: the container count is fixed by the bridge policy (3 ingestions), but
# the PER-CONTAINER token count is arithmetic, not a literal: a hardcoded 811
# silently truncated the universe on growth (extra slices were split and then
# ignored) and failed confusingly on shrinkage. Derive both from the file.
UNIVERSE_ROWS="$(tail -n +2 "$NSE" | grep -c . || true)"
[ "$UNIVERSE_ROWS" -gt 0 ] || fatal "universe has no instrument rows: $NSE"
[ $((UNIVERSE_ROWS % 3)) -eq 0 ] \
  || fatal "universe has $UNIVERSE_ROWS instruments, not divisible by 3 ingestion containers — adjust the universe or the container count (the split would otherwise drop or duplicate rows)"
PER_SLICE=$((UNIVERSE_ROWS / 3))
echo "SOAK-E2E: rate=${RATE_HZ}Hz stocks=${UNIVERSE_ROWS} (3x${PER_SLICE}) duration=${DURATION_S}s out=$PHASE_OUT"
# --- TM fresh (B3) + registration wait (B5), Fluss ready ---
docker exec 01_docker-fluss-coordinator-1 sh -c 'exit 0' 2>/dev/null || fatal "fluss-coordinator not up"
CP="$(cat "$LIB_CP_FILE")"; [ -n "$CP" ] || fatal "empty CP"
export CP
pipeline_compile_fluss_ready_probe || fatal "probe compile failed"
pipeline_wait_for_fluss_ready || fatal "fluss not ready"
echo "SOAK-E2E: restarting flink-taskmanager (B3)..."
( cd "$COMPOSE_DIR" && docker compose --env-file .env --env-file secrets.env restart flink-taskmanager ) >/dev/null 2>&1 || fatal "TM restart failed"
sleep 12
# P6-567: both REST polls below keep curl/python stderr in a file instead of
# /dev/null (a malformed body was invisible), validate the reply as a number
# before comparing it (a non-numeric value tripped "integer expression
# expected" instead of a clear retry), and report the reason each round.
reg="" && for i in $(seq 1 12); do
  if ! reg="$(curl -fsS --max-time 5 "$FLINK_REST_URL/taskmanagers" 2>"$OUT/tm-poll.err" \
      | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get("taskmanagers",[])))' 2>>"$OUT/tm-poll.err")"; then
    reg=""
  fi
  [[ "$reg" =~ ^[0-9]+$ ]] && [ "$reg" -ge 1 ] && break
  echo "SOAK-E2E: TM registration poll $i/12: reg=${reg:-<no reply>} — $(head -1 "$OUT/tm-poll.err" 2>/dev/null)"
  reg=""
  sleep 5
done
[ "${reg:-0}" -ge 1 ] || fatal "TM never registered (B5)"
# shellcheck disable=SC2034  # lib state: read by pipeline-lib.sh's preflight guard
PIPELINE_PREFLIGHT_OK=1
# --- image-freshness gate (soak skips pipeline_preflight, so G27b never ran
# --- and a stale loadgen image measured OLD code on 2026-09-03/04). The
# --- image carries a content stamp baked at build time; mismatch = the
# --- sources changed since the image was built -> the run would measure
# --- stale code. Fail NOW with the reason instead of after 3 blind minutes.
LIB_LOADGEN_STAMP="$(pipeline_loadgen_input_stamp)" || fatal "cannot compute loadgen input stamp"
stamp_in_image="$(docker run --rm --network none "$LIB_LOADGEN_IMAGE" cat /app/build-stamp 2>/dev/null | tr -d '[:space:]')"
if [ -z "$stamp_in_image" ] || [ "$stamp_in_image" != "$LIB_LOADGEN_STAMP" ]; then
  fatal "loadgen image $LIB_LOADGEN_IMAGE is STALE (build stamp ${stamp_in_image:0:12} != current sources ${LIB_LOADGEN_STAMP:0:12}) — the run would measure OLD code. Rebuild: ( cd $COMPOSE_DIR && LOADGEN_BUILD_STAMP=$LIB_LOADGEN_STAMP docker compose build loadgen )"
fi
echo "SOAK-E2E: loadgen image fresh (stamp ${LIB_LOADGEN_STAMP:0:12})"
# --- fresh tables ---
pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/02_raw_table_1.sql" raw || fatal "raw purge failed"
pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/05_signal_candidates.sql" signals || fatal "signal purge failed"
pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/27_execution_intent.sql" intents || fatal "intent purge failed"
# Cutover (2026-09-05): the job preflights + writes candle_live/candle_closed.
# PURGE both (drop+recreate): they are LOG-append tables, so a run that only
# "ensures" accumulates every prior run's rows. The old-chain tables (03
# feature_candles_15s, 30 preview, 04 forming_bar, 31 tentative markers) are
# deleted — the job no longer reads or writes them, so no purge remains.
if [ "${MULTITF_ENABLED:-false}" = "true" ]; then
  pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/33_candle_closed.sql" candle_closed \
    || fatal "candle_closed purge failed"
  pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/32_candle_live.sql" candle_live \
    || fatal "candle_live purge failed"
  pipeline_ensure_candle_tables "$ROOT/code/01_platform/02_sql/ddl/32_candle_live.sql" "candle_live" \
    || fatal "candle_live ensure failed"
  pipeline_ensure_candle_tables "$ROOT/code/01_platform/02_sql/ddl/33_candle_closed.sql" "candle_closed" \
    || fatal "candle_closed ensure failed"
fi
# --- feed: 1 faketool + 3 ingestions x PER_SLICE ---
head -1 "$NSE" | grep -q "OptionType" || fatal "wrong universe file (missing OptionType column): $NSE"
head -1 "$NSE" > "$OUT/header.csv"
tail -n +2 "$NSE" | split -l "$PER_SLICE" -d -a2 - "$OUT/slice_"
# P6-217: enumerate what split actually produced. The old fixed loop named
# slice_aa/ab/ac whatever split did, so a 4th slice existed and was never
# loaded while the run still claimed full coverage.
SLICES=()
for s in "$OUT"/slice_*; do [ -e "$s" ] || fatal "split produced no slices for $NSE"; SLICES+=("$s"); done
[ "${#SLICES[@]}" -eq 3 ] || fatal "split produced ${#SLICES[@]} slices, expected 3 (universe ${UNIVERSE_ROWS} / ${PER_SLICE})"
i=0 && for s in "${SLICES[@]}"; do
  cat "$OUT/header.csv" "$s" > "$OUT/manifest-$i.csv"
  ntok="$(tail -n +2 "$OUT/manifest-$i.csv" | grep -c . || true)"
  [ "$ntok" -eq "$PER_SLICE" ] || fatal "manifest-$i has $ntok tokens, need exactly $PER_SLICE (universe must be 3 x $PER_SLICE = $((3 * PER_SLICE)))"
  i=$((i+1))
done
RATE_HZ="$RATE_HZ" pipeline_start_faketool || fatal "faketool start failed"
for i in 0 1 2; do
  docker run -d --name "e2e-ingestion-$i" --network "$LIB_TRADING_NET" \
    -v "$OUT":/run -v "$OUT/j1-$i":/logs \
    -e LOG_DIR=/logs -e READINESS_FILE_PATH=/run/ingestion-$i.loadtest.ready \
    -e ARROW_HFT_URL="ws://e2e-faketool:$FAKETOOL_PORT" -e ARROW_BRIDGE_BIN=/app/arrow-bridge \
    -e ARROW_FAKE_BROKER=1 -e TRANSPORT=proto -e SECRETS_VIA_ENV_FILE=1 \
    --env-file "$LIB_SECRETS_FILE" \
    -e "ARROW_APP_ID=${ARROW_APP_ID:-testd}" -e "ARROW_USER_ID=${ARROW_USER_ID:-testd-user}" \
    -e INSTRUMENT_MANIFEST_PATH=/run/manifest-$i.csv \
    -e FLUSS_BOOTSTRAP=fluss-coordinator:9123 -e FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 \
    -e RAW_TABLE_NAME=raw_table_1 -e ARROW_HFT_CONNECTIONS=1 \
    -e ARROW_MAX_EVENT_AGE_MS=5000 -e ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000 \
    -e ARROW_HFT_LATENCY_MS=50 -e CLOCK_CHECK_REQUIRED=false \
    -e OTEL_COLLECTOR_HOST=otel-collector:4318 \
    -e METRICS_LOCAL_LOG=1 \
    -e FLUSS_WRITER_MODE=generic -e FLUSS_WRITERS=1 -e FLUSS_WRITER_BATCH_SIZE_BYTES=0 \
    -e ARROW_TICK_COUNTS=30 "$LIB_LOADGEN_IMAGE" \
    java --add-opens=java.base/java.nio=ALL-UNNAMED -Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m \
    '-Xlog:gc*,safepoint:file=/logs/gc.log:time,uptime,level,tags' -Dlog.dir=/logs \
    -cp /app/ingestion.jar com.trading.ingestion.IngestionService >/dev/null \
    || fatal "ingestion-$i start failed"
  # P6-218: the secret-bearing variables (ARROW_APP_SECRET, ARROW_PASSWORD,
  # ARROW_TOTP_KEY) come from --env-file above, so they never appear in
  # `ps`/`docker inspect`/shell history. The ids above are not secrets.
  docker logs -f "e2e-ingestion-$i" > "$OUT/j1-$i/java.out" 2>&1 &
  LOG_PIDS="$LOG_PIDS $!"
done
for i in 0 1 2; do
  ok="" && for _ in $(seq 1 60); do
    [ -f "$OUT/ingestion-$i.loadtest.ready" ] && { ok=1; break; }
    # P6-569: a crashed container used to burn the whole 120s and then report
    # "not ready" with no cause. Stop as soon as it is not running.
    running="$(docker inspect --format '{{.State.Running}}' "e2e-ingestion-$i" 2>/dev/null || echo false)"
    [ "$running" = "true" ] || { echo "!! FAIL: ingestion-$i is not running while waiting for readiness (running=$running)"; break; }
    sleep 2
  done
  [ -n "$ok" ] || { tail -15 "$OUT/j1-$i/java.out" 2>/dev/null; fatal "ingestion-$i not ready (see $OUT/j1-$i/java.out)"; }
done
for i in 0 1 2; do
  # P6-569: the subscription gate is a deadline poll with a liveness check and
  # a log tail, not a single sleep-10 retry whose 2>/dev/null also hid a
  # missing mirror file.
  [ -f "$OUT/j1-$i/java.out" ] || fatal "ingestion-$i log mirror missing ($OUT/j1-$i/java.out) — the docker logs -f mirror never started"
  sub_ok="" && sub_end=$((SECONDS + 180))
  while [ "$SECONDS" -lt "$sub_end" ]; do
    grep -q "HFT subscribed $PER_SLICE" "$OUT/j1-$i/java.out" 2>/dev/null && { sub_ok=1; break; }
    running="$(docker inspect --format '{{.State.Running}}' "e2e-ingestion-$i" 2>/dev/null || echo false)"
    [ "$running" = "true" ] || { echo "!! FAIL: ingestion-$i exited before subscribing (running=$running)"; break; }
    sleep 2
  done
  [ -n "$sub_ok" ] || { tail -15 "$OUT/j1-$i/java.out" 2>/dev/null; fatal "ingestion-$i never subscribed $PER_SLICE tokens in 180s"; }
done
echo "SOAK-E2E: feed up (3x${PER_SLICE} = ${UNIVERSE_ROWS} tokens @ ${RATE_HZ}Hz)"
# --- job ---
export PURGE_STRICT=true ALLOW_FULL_REPLAY=false
pipeline_submit_job || fatal "SignalJob submission failed"
# P6-570: JOB_ID is set by a side effect; if it is empty the poll below would
# query /jobs/ (a LIST) and cleanup would skip the cancel, leaving the job
# running after the drill reported success.
[ -n "${JOB_ID:-}" ] || fatal "pipeline_submit_job returned success but JOB_ID is empty — the submit output was not parsed"
state="" && for i in $(seq 1 40); do
  if ! state="$(curl -fsS --max-time 5 "$FLINK_REST_URL/jobs/$JOB_ID" 2>"$OUT/job-poll.err" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))' 2>>"$OUT/job-poll.err")"; then
    state=""
  fi
  case "$state" in
    RUNNING) break ;;
    # P6-567: a job that already failed is not going to become RUNNING; fail
    # now with the state instead of after the full 200s wait. RESTARTING is
    # deliberately absent — it is transient and does become RUNNING.
    FAILED|FAILING|CANCELED|CANCELLED|SUSPENDED|FINISHED)
      tail -3 "$OUT/job-poll.err" 2>/dev/null
      fatal "job $JOB_ID reached state=$state after $((i * 5))s — see $FLINK_REST_URL/jobs/$JOB_ID/exceptions" ;;
  esac
  sleep 5
done
[ "$state" = "RUNNING" ] || fatal "job $JOB_ID never RUNNING (state=$state)"
echo "SOAK-E2E: job $JOB_ID RUNNING — capturing ${DURATION_S}s"
if [ "${STATE_BACKEND:-rocksdb}" = "rocksdb" ]; then
  rocks_ok="" && for i in $(seq 1 12); do
    if ( cd "$COMPOSE_DIR" && docker compose --env-file .env --env-file secrets.env exec -T flink-taskmanager sh -c "ls -d /tmp/flink-rocksdb/job_${JOB_ID}_op_* >/dev/null 2>&1" ); then rocks_ok=1; break; fi
    sleep 5
  done
  [ -n "$rocks_ok" ] || fatal "G24: no RocksDB dirs for job $JOB_ID under /tmp/flink-rocksdb"
  echo "SOAK-E2E: G24 OK"
fi
TOK12="$(tail -n +2 "$NSE" | head -2 | cut -d, -f4 | paste -sd, -)"
JOB_ID="$JOB_ID" DURATION_S="$DURATION_S" INGESTION_JAVA_OUT="$OUT/j1-0/java.out" \
  FLUSS_PROBE_CP="$CP" OUT_DIR="$PHASE_OUT/stages" PROBE_TOKENS="$TOK12" \
bash "$SCRIPT_DIR/stage-capture.sh" || fatal "stage capture failed"
echo "SOAK-E2E: capture complete — evidence at $PHASE_OUT"
# Cutover (2026-09-05): host-N7 signal gate. The old chain is deleted, so no
# side-by-side comparison remains. The run passes its signal leg when this
# run's rows include host N7 candidates in both the LOG and the current KV
# table. FlussRuleCounter prints "rule=<id> count=<n>" per rule id.
if [ "${MULTITF_ENABLED:-false}" = "true" ]; then
  echo "SOAK-E2E: MULTITF_ENABLED=true — running host-N7 signal gate"
  GATE_BIN="$PHASE_OUT/stages/probes"
  mkdir -p "$GATE_BIN"
  javac -cp "$CP" -d "$GATE_BIN" "$SCRIPT_DIR/fluss-probes/FlussRuleCounter.java" \
    > "$PHASE_OUT/stages/javac-FlussRuleCounter.log" 2>&1 \
    || fatal "host-N7 gate: FlussRuleCounter compile failed"
  for gate_table in Signal_Candidates Signal_Candidates_current; do
    # P6-571: keep the probe's stderr (it explains a failed census) and parse
    # the count with an anchored capture that tolerates trailing fields — the
    # old `cut -d= -f3` yielded garbage on format drift and `[ -ge ]` then
    # died with "integer expression expected" instead of a clear FAIL.
    gate_out="$(java --add-opens=java.base/java.nio=ALL-UNNAMED -Dlog.dir=/tmp/fluss-probe-logs \
      -cp "$GATE_BIN:$CP" FlussRuleCounter "$gate_table" "${PROBE_BOOTSTRAP:-localhost:9123}" 2>"$PHASE_OUT/stages/n7-gate-$gate_table.err")" \
      || { tail -5 "$PHASE_OUT/stages/n7-gate-$gate_table.err" 2>/dev/null; fatal "host-N7 gate: census of $gate_table failed (see stages/n7-gate-$gate_table.err)"; }
    printf '%s\n' "$gate_out" > "$PHASE_OUT/stages/n7-gate-$gate_table.txt"
    gate_n="$(printf '%s\n' "$gate_out" | sed -n 's/^rule=n7-range-breakout-v1 count=\([0-9][0-9]*\).*/\1/p' | head -1)"
    if ! [[ "$gate_n" =~ ^[0-9]+$ ]] || [ "$gate_n" -lt 1 ]; then
      grep -v '^$' "$PHASE_OUT/stages/n7-gate-$gate_table.txt" 2>/dev/null | head -10 >&2 || true
      fatal "host-N7 gate: no n7-range-breakout-v1 rows in $gate_table (census: stages/n7-gate-$gate_table.txt)"
    fi
    echo "SOAK-E2E: host-N7 gate $gate_table has $gate_n n7 rows"
  done
  echo "SOAK-E2E: host-N7 signal gate PASS"
fi
# Tick-count losslessness leg (2026-09-04): the per-container emitted-tick
# counts are THE authoritative feed-side throughput/loss evidence (Flink
# operator counters are empty on 2.2.1 via REST+Prom). The containers are
# still alive here — drain NOW (cleanup() at EXIT re-drains as a no-op-safe
# fallback), then verify the counts against the REAL run window.
# P6-220: what this leg asserts is (1) every container reports a count for
# EVERY token in its slice, (2) the file's own header total equals the sum of
# the per-token counts, (3) the tokens are uniform, and (4) the implied
# active window accounts for the container lifetime to within
# TICK_STARTUP_BUDGET_S. Property (4) is what makes this an ABSOLUTE rate
# gate: uniformity alone is satisfied by a uniform stall (every token at half
# rate), which property (4) rejects.
for c in e2e-ingestion-0 e2e-ingestion-1 e2e-ingestion-2; do
  docker cp "$c:/tmp/arrow-tick-counts.txt" "$OUT/$c.tick-counts.txt" >/dev/null 2>&1 \
    || echo "!! WARN: could not drain tick counts from $c (container already gone?)"
done
# Container life is measured at drain time; the container keeps ticking until
# `docker rm -f`, and the bridge rewrites the counts file on its own interval,
# so a correctly-ticking container still shows a few seconds of unexplained
# idle (archive of 19 runs, 2026-09-04/05: 3.2-33.2 s, median ~9 s = the JVM +
# bridge startup latency). TICK_STARTUP_BUDGET_S bounds that noise; an idle
# window far beyond it means the feed did not run for its lifetime — a
# uniform stall the uniformity check above cannot see (P6-219/P6-220).
DRAIN_EPOCH="$(date +%s)"
TICK_STARTUP_BUDGET_S="${TICK_STARTUP_BUDGET_S:-45}"
[[ "$TICK_STARTUP_BUDGET_S" =~ ^[0-9]+$ ]] || fatal "TICK_STARTUP_BUDGET_S must be a non-negative integer, got '$TICK_STARTUP_BUDGET_S'"
LIFETIMES=""
for c in e2e-ingestion-0 e2e-ingestion-1 e2e-ingestion-2; do
  c_start="$(docker inspect -f '{{.State.StartedAt}}' "$c" 2>/dev/null)"
  c_start_epoch="$(date -d "$c_start" +%s 2>/dev/null || echo "$DRAIN_EPOCH")"
  LIFETIME_S=$((DRAIN_EPOCH - c_start_epoch))
  [ "$LIFETIME_S" -gt 0 ] || LIFETIME_S="$DURATION_S"
  echo "SOAK-E2E: $c container lifetime ${LIFETIME_S}s (absolute rate gate: active window must account for it within ${TICK_STARTUP_BUDGET_S}s)"
  LIFETIMES="$LIFETIMES $LIFETIME_S"
done
TICK_TOLERANCE_PCT="${TICK_TOLERANCE_PCT:-2}"
LIFETIME_0="$(printf '%s' "$LIFETIMES" | awk '{print $1}')"
LIFETIME_1="$(printf '%s' "$LIFETIMES" | awk '{print $2}')"
LIFETIME_2="$(printf '%s' "$LIFETIMES" | awk '{print $3}')"
for c in e2e-ingestion-0 e2e-ingestion-1 e2e-ingestion-2; do
  tf="$OUT/$c.tick-counts.txt"
  if [ ! -s "$tf" ]; then
    echo "!! FAIL: tick-count file for $c missing/empty ($tf) — cleanup did not drain it; feed-side losslessness cannot be proven. Check cleanup() docker cp." >&2
    exit 2
  fi
  last_total="$(grep -oE 'total=[0-9]+' "$tf" | tail -1 | cut -d= -f2)"
  [ -n "$last_total" ] || { echo "!! FAIL: no total= line in $tf — malformed tick-count drain" >&2; exit 2; }
  case "$c" in
    *-0) LIFETIME_S="$LIFETIME_0" ;;
    *-1) LIFETIME_S="$LIFETIME_1" ;;
    *)   LIFETIME_S="$LIFETIME_2" ;;
  esac
  python3 - "$last_total" "$tf" "$c" "$RATE_HZ" "$TICK_TOLERANCE_PCT" \
           "$LIFETIME_S" "$TICK_STARTUP_BUDGET_S" "$PER_SLICE" <<'PYEOF' || exit 2
import sys, re
(total, tf, c, hz, tol, lifetime, budget, expect_tokens) = (
    int(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4]),
    float(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7]), int(sys.argv[8]))
token_counts = {}
for line in open(tf):
    for m in re.finditer(r"t=(\d+):n=(\d+)", line):
        token_counts[int(m.group(1))] = int(m.group(2))
ntokens = len(token_counts)
if ntokens == 0:
    print(f"!! FAIL: {c} tick file has no token rows — malformed drain", file=sys.stderr)
    sys.exit(1)
# (1) Coverage: a file holding only some of the slice used to pass silently,
#     so a container feeding 100 of its 811 tokens looked healthy.
if ntokens != expect_tokens:
    print(f"!! FAIL: {c} tick file covers {ntokens} tokens, expected {expect_tokens} "
          f"(its manifest slice) — the missing tokens emitted no ticks or were never "
          f"subscribed; feed-side coverage is not proven", file=sys.stderr)
    sys.exit(1)
# (2) The drained total must equal the sum of the per-token counts; a mismatch
#     means the file mixes two write generations (total from a later write than
#     the token rows, or vice versa) and neither number is trustworthy.
resummed = sum(token_counts.values())
if total != resummed:
    print(f"!! FAIL: {c} tick file total={total} but the per-token counts sum to "
          f"{resummed} (delta {resummed - total}) — mixed write generations in the "
          f"drain; re-drain and re-check", file=sys.stderr)
    sys.exit(1)
# (3) Uniformity: every token sees the same cadence (no per-token straggler).
counts = sorted(token_counts.values())
median = counts[len(counts) // 2]
lo, hi = median * (1 - tol/100), median * (1 + tol/100)
bad = [t for t, n in token_counts.items() if not (lo <= n <= hi)]
if bad:
    print(f"!! FAIL: {c} {len(bad)} tokens outside {tol}% of median {median}: "
          f"first {bad[:5]} counts {[token_counts[t] for t in bad[:5]]} — per-token feed loss or wrong rate",
          file=sys.stderr)
    sys.exit(1)
# (4) Absolute rate: the implied active window must account for the container's
#     whole life apart from the bounded startup/idle budget. This is the check
#     that fails a uniform stall, which (3) cannot: at half rate every token is
#     still perfectly uniform.
active_s = median / hz
idle_s = lifetime - active_s
if idle_s > budget:
    print(f"!! FAIL: {c} implied active window {active_s:.1f}s covers only {active_s / lifetime * 100:.0f}% "
          f"of the {lifetime}s container life — {idle_s:.1f}s unaccounted, budget {budget}s. "
          f"Every token is uniform at {median} ticks ({median / lifetime:.2f}/s vs {hz}Hz expected), "
          f"so this is a GLOBAL rate shortfall (feed stalled or ran slow), not per-token loss. "
          f"Raise TICK_STARTUP_BUDGET_S only with evidence that startup really took that long.",
          file=sys.stderr)
    sys.exit(1)
print(f"SOAK-E2E: {c} ticks OK — {ntokens} tokens x {median} ticks "
      f"({active_s:.1f}s active @ {hz}Hz of {lifetime}s life, {idle_s:.1f}s idle), "
      f"total {total} == sum, uniform within {tol}%")
PYEOF
done
ls "$PHASE_OUT/stages" | head -8
# O2 window-evidence leg (2026-09-05 single-pane policy): re-query OpenObserve
# for the fixed multi-TF query set over this run's capture window and store
# the raw per-query series summary as o2-evidence.jsonl. This is the QUERIED
# half of the single-pane contract (dashboards/alerts are the live half):
# the scorecard's O2 section is built from this file, so "what O2 saw" is
# evidence, not a screenshot. Best-effort BY DESIGN (the stages/ TSVs remain
# raw truth): a failed leg warns but never fails the run.
RUN_START_UTC="$(date -u -d "@$(stat -c %Y "$PHASE_OUT/stages/run-meta.txt" 2>/dev/null || date +%s)" +%Y-%m-%dT%H:%M:%SZ)"
RUN_END_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_ID="$(basename "$PHASE_OUT")"
if python3 "$SCRIPT_DIR/soak-o2-evidence.py" --start "$RUN_START_UTC" --end "$RUN_END_UTC" \
    --run "$RUN_ID" --job "${JOB_ID:-}" > "$PHASE_OUT/stages/o2-evidence.jsonl" 2>"$PHASE_OUT/stages/o2-evidence.err"; then
  echo "SOAK-E2E: O2 window evidence -> $PHASE_OUT/stages/o2-evidence.jsonl ($(wc -l < "$PHASE_OUT/stages/o2-evidence.jsonl") queries)"
  python3 "$SCRIPT_DIR/o2_ingest.py" soak_o2_evidence < "$PHASE_OUT/stages/o2-evidence.jsonl" \
    || echo "SOAK-E2E: WARN — O2 evidence push failed (raw truth: $PHASE_OUT/stages/o2-evidence.jsonl)"
else
  echo "SOAK-E2E: WARN — O2 window-evidence leg failed (see $PHASE_OUT/stages/o2-evidence.err); local TSVs remain raw truth"
fi
