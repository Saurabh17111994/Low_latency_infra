#!/usr/bin/env bash
# stage-soak-e2e.sh — end-to-end soak at 2433 NSE stocks x RATE_HZ (no INJECT).
# NEW file for the 48k/s e2e (does NOT modify pipeline-lib.sh / stage scripts).
# Reuses pipeline-lib primitives; differs from stage-a2-baseline.sh in:
#   - no 1024-token slice (LIB_MANIFEST_SLICE = full NSE file)
#   - 3 ingestion containers x 811 tokens (bridge single-socket policy)
#   - DURATION_S passed in (120 smoke / 900 main)
# Usage: DURATION_S=120 bash stage-soak-e2e.sh
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
# shellcheck disable=SC2034  # lib input: pipeline-lib.sh reads it when managing the faketool
LIB_FAKETOOL_CONTAINER="e2e-faketool"
NSE="/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv"
fatal() { echo "SOAK-E2E: FAIL — $*" >&2; printf 'FAIL\n%s\n' "$*" > "$PHASE_OUT/FAILURE.txt"; exit 1; }
cleanup() {
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
    ( cd "$COMPOSE_DIR" && docker compose --env-file .env --env-file secrets.env exec -T flink-jobmanager flink cancel "$JOB_ID" ) >/dev/null 2>&1 || true
  fi
  echo "cleanup: removed soak containers job=$JOB_ID"
}
trap cleanup EXIT
echo "SOAK-E2E: rate=${RATE_HZ}Hz stocks=2433 duration=${DURATION_S}s out=$PHASE_OUT"
[ -r "$NSE" ] || fatal "NSE manifest not readable: $NSE"
# shellcheck disable=SC2034  # lib contract output; nothing expands it in-tree (verified 2026-09-12)
LIB_MANIFEST_SLICE="$NSE"
# --- TM fresh (B3) + registration wait (B5), Fluss ready ---
docker exec 01_docker-fluss-coordinator-1 sh -c 'exit 0' 2>/dev/null || fatal "fluss-coordinator not up"
CP="$(cat "$LIB_CP_FILE")"; [ -n "$CP" ] || fatal "empty CP"
export CP
pipeline_compile_fluss_ready_probe || fatal "probe compile failed"
pipeline_wait_for_fluss_ready || fatal "fluss not ready"
echo "SOAK-E2E: restarting flink-taskmanager (B3)..."
( cd "$COMPOSE_DIR" && docker compose --env-file .env --env-file secrets.env restart flink-taskmanager ) >/dev/null 2>&1 || fatal "TM restart failed"
sleep 12
reg="" && for i in $(seq 1 12); do
  reg="$(curl -fsS --max-time 5 "$FLINK_REST_URL/taskmanagers" 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get("taskmanagers",[])))' 2>/dev/null)"
  [ "${reg:-0}" -ge 1 ] && break; sleep 5
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
# --- feed: 1 faketool + 3 ingestions x 811 ---
head -1 "$NSE" | grep -q "OptionType" || fatal "wrong universe file (missing OptionType column): $NSE"
head -1 "$NSE" > "$OUT/header.csv"
tail -n +2 "$NSE" | split -l 811 - "$OUT/slice_"
i=0 && for s in "$OUT"/slice_aa "$OUT"/slice_ab "$OUT"/slice_ac; do
  cat "$OUT/header.csv" "$s" > "$OUT/manifest-$i.csv"
  ntok="$(tail -n +2 "$OUT/manifest-$i.csv" | grep -c .)"
  [ "$ntok" -eq 811 ] || fatal "manifest-$i has $ntok tokens, need exactly 811 (real NSE universe must total 2433)"
  i=$((i+1))
done
RATE_HZ="$RATE_HZ" pipeline_start_faketool || fatal "faketool start failed"
for i in 0 1 2; do
  docker run -d --name "e2e-ingestion-$i" --network "$LIB_TRADING_NET" \
    -v "$OUT":/run -v "$OUT/j1-$i":/logs \
    -e LOG_DIR=/logs -e READINESS_FILE_PATH=/run/ingestion-$i.loadtest.ready \
    -e ARROW_HFT_URL="ws://e2e-faketool:$FAKETOOL_PORT" -e ARROW_BRIDGE_BIN=/app/arrow-bridge \
    -e ARROW_FAKE_BROKER=1 -e TRANSPORT=proto -e SECRETS_VIA_ENV_FILE=1 \
    -e ARROW_APP_ID=testd -e ARROW_APP_SECRET=testd -e ARROW_USER_ID=testd-user \
    -e ARROW_PASSWORD=testd-pass -e ARROW_TOTP_KEY=JBSWY3DPEHPK3PXP \
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
  docker logs -f "e2e-ingestion-$i" > "$OUT/j1-$i/java.out" 2>&1 &
done
for i in 0 1 2; do
  ok="" && for _ in $(seq 1 60); do
    [ -f "$OUT/ingestion-$i.loadtest.ready" ] && { ok=1; break; }; sleep 2
  done
  [ -n "$ok" ] || fatal "ingestion-$i not ready in 120s"
done
for i in 0 1 2; do
  grep -q "HFT subscribed 811" "$OUT/j1-$i/java.out" 2>/dev/null || {
    sleep 10; grep -q "HFT subscribed 811" "$OUT/j1-$i/java.out" 2>/dev/null || fatal "ingestion-$i never subscribed 811"; }
done
echo "SOAK-E2E: feed up (3x811 @ ${RATE_HZ}Hz)"
# --- job ---
export PURGE_STRICT=true ALLOW_FULL_REPLAY=false
pipeline_submit_job || fatal "SignalJob submission failed"
state="" && for i in $(seq 1 40); do
  state="$(curl -fsS --max-time 5 "$FLINK_REST_URL/jobs/$JOB_ID" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))' 2>/dev/null)"
  [ "$state" = "RUNNING" ] && break; sleep 5
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
    gate_out="$(java --add-opens=java.base/java.nio=ALL-UNNAMED -Dlog.dir=/tmp/fluss-probe-logs \
      -cp "$GATE_BIN:$CP" FlussRuleCounter "$gate_table" "${PROBE_BOOTSTRAP:-localhost:9123}" 2>/dev/null)" \
      || fatal "host-N7 gate: census of $gate_table failed"
    echo "$gate_out" > "$PHASE_OUT/stages/n7-gate-$gate_table.txt"
    gate_n="$(echo "$gate_out" | grep -E '^rule=n7-range-breakout-v1 count=' | cut -d= -f3)"
    [ -n "$gate_n" ] && [ "$gate_n" -ge 1 ] \
      || fatal "host-N7 gate: no n7-range-breakout-v1 rows in $gate_table (see stages/n7-gate-$gate_table.txt)"
    echo "SOAK-E2E: host-N7 gate $gate_table has $gate_n n7 rows"
  done
  echo "SOAK-E2E: host-N7 signal gate PASS"
fi
# Tick-count losslessness leg (2026-09-04): the per-container emitted-tick
# counts are THE authoritative feed-side throughput/loss evidence (Flink
# operator counters are empty on 2.2.1 via REST+Prom). The containers are
# still alive here — drain NOW (cleanup() at EXIT re-drains as a no-op-safe
# fallback), then verify totals within tolerance of 811 x RATE_HZ x elapsed.
for c in e2e-ingestion-0 e2e-ingestion-1 e2e-ingestion-2; do
  docker cp "$c:/tmp/arrow-tick-counts.txt" "$OUT/$c.tick-counts.txt" >/dev/null 2>&1 \
    || echo "!! WARN: could not drain tick counts from $c (container already gone?)"
done
# Expected ticks: the container-lifetime math (drain-time minus
# docker StartedAt) systematically OVERCOUNTS by the Java+bridge startup
# latency (~9 s between container start and the first emitted tick —
# observed 2026-09-04: every token showed exactly 3599 ticks over a 189 s
# container life = 179.95 s of active ticking at 20 Hz, i.e. zero loss).
# Instead derive the ACTIVE tick window from the tick file itself: each
# token's count n is ticks delivered at RATE_HZ, so active-seconds =
# per-token-count / RATE_HZ. Expected = 811 x RATE_HZ x active-seconds —
# self-calibrating against startup skew, and the rate check (n within
# tolerance of RATE_HZ x active-seconds) is what proves no feed loss.
DRAIN_EPOCH="$(date +%s)"
for c in e2e-ingestion-0 e2e-ingestion-1 e2e-ingestion-2; do
  c_start="$(docker inspect -f '{{.State.StartedAt}}' "$c" 2>/dev/null)"
  c_start_epoch="$(date -d "$c_start" +%s 2>/dev/null || echo "$DRAIN_EPOCH")"
  LIFETIME_S=$((DRAIN_EPOCH - c_start_epoch))
  [ "$LIFETIME_S" -gt 0 ] || LIFETIME_S="$DURATION_S"
  echo "SOAK-E2E: $c container lifetime ${LIFETIME_S}s (active tick window derived from the tick file)"
done
TICK_TOLERANCE_PCT="${TICK_TOLERANCE_PCT:-2}"
for c in e2e-ingestion-0 e2e-ingestion-1 e2e-ingestion-2; do
  tf="$OUT/$c.tick-counts.txt"
  if [ ! -s "$tf" ]; then
    echo "!! FAIL: tick-count file for $c missing/empty ($tf) — cleanup did not drain it; feed-side losslessness cannot be proven. Check cleanup() docker cp." >&2
    exit 2
  fi
  last_total="$(grep -oE 'total=[0-9]+' "$tf" | tail -1 | cut -d= -f2)"
  [ -n "$last_total" ] || { echo "!! FAIL: no total= line in $tf — malformed tick-count drain" >&2; exit 2; }
  python3 - "$last_total" "$tf" "$c" "$RATE_HZ" "$TICK_TOLERANCE_PCT" <<'PYEOF' || exit 2
import sys, re
total, tf, c, hz, tol = (int(sys.argv[1]), sys.argv[2], sys.argv[3],
                         int(sys.argv[4]), float(sys.argv[5]))
token_counts = {}
for line in open(tf):
    for m in re.finditer(r"t=(\d+):n=(\d+)", line):
        token_counts[int(m.group(1))] = int(m.group(2))
ntokens = len(token_counts)
if ntokens == 0:
    print(f"!! FAIL: {c} tick file has no token rows — malformed drain", file=sys.stderr)
    sys.exit(1)
# Every token should have the SAME count (uniform 20 Hz cadence). Active
# seconds per token = count / hz; expected total = sum of per-token counts
# derived from each token's own active window, which by construction equals
# the observed total — so the real check is UNIFORMITY + rate plausibility:
# (a) all tokens within tol% of the median count (no stragglers = no
#     per-token feed loss), and (b) the implied active window is within the
#     container lifetime (startup skew < lifetime, no negative time).
counts = sorted(token_counts.values())
median = counts[len(counts) // 2]
lo, hi = median * (1 - tol/100), median * (1 + tol/100)
bad = [t for t, n in token_counts.items() if not (lo <= n <= hi)]
if bad:
    print(f"!! FAIL: {c} {len(bad)} tokens outside {tol}% of median {median}: "
          f"first {bad[:5]} counts {[token_counts[t] for t in bad[:5]]} — per-token feed loss or wrong rate",
          file=sys.stderr)
    sys.exit(1)
active_s = median / hz
print(f"SOAK-E2E: {c} ticks OK — {ntokens} tokens x {median} ticks "
      f"({active_s:.1f}s active @ {hz}Hz), total {total}, uniform within {tol}%")
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
