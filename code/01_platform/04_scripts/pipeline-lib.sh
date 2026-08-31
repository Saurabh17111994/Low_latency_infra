#!/usr/bin/env bash
# =============================================================================
# pipeline-lib.sh — shared orchestration for the pipeline harness scripts.
#
# Sourced (never executed directly) by loadtest-preview.sh and
# holistic-measure.sh. Provides ONE battle-tested bring-up/teardown/metric
# path so gate scripts and measurement scripts cannot drift apart
# (decision 2026-08-30).
#
# Contract with the sourcing script:
#   - The source script MUST define, BEFORE sourcing this file:
#       ROOT                 project root (streaming_project_New/)
#       OUT                  evidence directory (created by lib preflight)
#       RATE_HZ              faketool rate (validated here)
#     and MAY override (defaults shown):
#       FAKETOOL_PORT=8899, ALLOW_FULL_REPLAY=false, WARMUP_S=45,
#       CHECKPOINT_TIMEOUT_MS=30000, DURATION_S=300
#   - Functions print diagnostics and `return 1` on failure — the caller
#     decides fail-fast (gate script) vs warn-and-continue (measurement).
#   - pipeline_install_cleanup_trap() installs the EXIT trap that kills
#     the faketool + ingestion JVM and cancels the job. State vars
#     (FAKETOOL_PID, JVM_PID, JOB_ID) live here.
#
# Bugs this lib exists to prevent (each observed live, 2026-08-29/30):
#   B1. `docker compose -f` without BOTH --env-file flags: required
#       interpolation vars (FLINK_IMAGE:? ...) hard-fail. The COMPOSE
#       variable here always carries the env files.
#   B2. Stale faketool holding :8899: preflight passed because the port
#       check had single quotes around $FAKETOOL_PORT (literal grep that
#       never matched). Double-quoted + anchored end here, PLUS a
#       liveness check after launch (a faketool that dies on bind leaves
#       a dead PID that must be caught immediately).
#   B3. TM direct-buffer accumulation across cancelled jobs → OOM at the
#       Fluss sinks. The TM is restarted before every run.
#   B4. Full-replay mode re-downloads tiered log segments (~80s each,
#       ~6 min cold) and stalls barriers past a 30s checkpoint timeout →
#       job FAILs. LATEST startup mode is the default here.
# =============================================================================

# Paths (derived from ROOT so callers only set ROOT + OUT)
LIB_JAR="$ROOT/code/02_services/02_compute/target/compute.jar"
LIB_ING_JAR="$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
LIB_BRIDGE_DIR="$ROOT/code/02_services/01_ingestion/go-bridge"
LIB_MANIFEST="$ROOT/../../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv"
LIB_FAKETOOL_SRC="$LIB_BRIDGE_DIR/faketool/main.go"
FAKETOOL_PORT="${FAKETOOL_PORT:-8899}"
LIB_COMPOSE_FILE="$ROOT/code/01_platform/01_docker/docker-compose.yml"
# B1 guard: always carry both env files.
COMPOSE="docker compose -f $LIB_COMPOSE_FILE --env-file $ROOT/code/01_platform/01_docker/.env --env-file $ROOT/code/01_platform/01_docker/secrets.env"
LIB_CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"

# Run state (owned by the lib; teardown reads these)
FAKETOOL_PID=""
JVM_PID=""
JOB_ID=""

pipeline_log() { echo "[pipeline $(date +%H:%M:%S)] $*"; }
pipeline_fail() { echo "[pipeline FATAL] $*" >&2; return 1; }

# ---------- rate validation (mirrors loadtest-run.sh B1) ----------
pipeline_validate_rate() {
  local hz="$1"
  case "$hz" in
    ''|*[!0-9]*) echo "RATE_HZ='$hz' is not a positive integer; valid rates: 1,2,4,5,8,10,20,25,40,50,100,125,200,250,500,1000" >&2; return 1;;
  esac
  [ "$hz" -gt 0 ] || { echo "RATE_HZ must be a positive integer, got '$hz'" >&2; return 1; }
  [ $(( 1000 % 10#$hz )) -eq 0 ] || { echo "RATE_HZ=$hz is invalid: faketool -real-rate-hz must divide 1000" >&2; return 1; }
}

# ---------- port availability (B2 guard) ----------
pipeline_port_free() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    # Double quotes so $port expands; anchor the port at end of the
    # address column (ss prints *:8899 or 0.0.0.0:8899).
    if ss -tln 2>/dev/null | awk 'NR>1 {print $4}' | grep -q ":$port"'$'; then return 1; fi
    return 0
  fi
  if (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null; then exec 3>&-; return 1; fi
  return 0
}

# ---------- preflight ----------
# Checks artifacts, kills stray faketools, verifies the port is free,
# restarts the TM (B3), resolves the 1024-token set, creates $OUT.
# On success sets: CP, TOKENS (caller-readable).
pipeline_preflight() {
  [ -f "$LIB_JAR" ] || { pipeline_fail "compute jar missing: $LIB_JAR (run: cd code/02_services/02_compute && mvn package)"; return 1; }
  [ -f "$LIB_ING_JAR" ] || { pipeline_fail "ingestion jar missing: $LIB_ING_JAR"; return 1; }
  [ -f "$LIB_CP_FILE" ] || { pipeline_fail "ingestion classpath file missing: $LIB_CP_FILE (run mvn package in 01_ingestion)"; return 1; }
  [ -f "$LIB_BRIDGE_DIR/arrow-bridge" ] || { pipeline_fail "arrow-bridge binary missing: $LIB_BRIDGE_DIR/arrow-bridge"; return 1; }
  [ -f "$LIB_FAKETOOL_SRC" ] || { pipeline_fail "faketool source missing: $LIB_FAKETOOL_SRC"; return 1; }
  [ -f "$LIB_MANIFEST" ] || { pipeline_fail "manifest CSV missing: $LIB_MANIFEST"; return 1; }
  pipeline_validate_rate "$RATE_HZ" || return 1
  pipeline_port_free "$FAKETOOL_PORT" || { pipeline_fail "port $FAKETOOL_PORT already in use — stale faketool/broker running (kill it or set FAKETOOL_PORT)"; return 1; }

  local stray
  stray=$(pgrep -x faketool || true)
  [ -z "$stray" ] || { pipeline_log "WARN: killing stray faketool(s): $stray"; for p in $stray; do kill -9 "$p" 2>/dev/null || true; done; sleep 1; }

  # Fluss must be reachable (tables applied)
  docker exec 01_docker-fluss-coordinator-1 sh -c 'exit 0' 2>/dev/null \
    || { pipeline_fail "fluss-coordinator container not up — run make up first"; return 1; }
  # B3 guard: fresh TM for every run.
  pipeline_log "restarting flink-taskmanager for direct-buffer hygiene..."
  $COMPOSE restart flink-taskmanager >/dev/null 2>&1 \
    || { pipeline_fail "flink-taskmanager restart failed"; return 1; }
  sleep 12
  # B5 guard (2026-08-30): wait until the TM is REGISTERED with the JM before
  # allowing job submit. A container "running" is not enough — observed job
  # f074d765 submitted while Registered TMs: 0 → NoResourceAvailableException
  # → job RESTARTING → phase aborted. Poll /taskmanagers for up to 60s.
  # NOTE: /taskmanagers returns {"taskmanagers":[...]} — assert on a non-empty
  # array (first pass of this guard grepped a nonexistent numRegisteredTMs
  # field and false-failed while the TM was registered).
  local tm_ok=0 tm_i
  for tm_i in $(seq 1 30); do
    if curl -s --max-time 3 http://localhost:8081/taskmanagers \
        | grep -q '"taskmanagers":\[{'; then
      tm_ok=1; break
    fi
    sleep 2
  done
  [ "$tm_ok" -eq 1 ] \
    || { pipeline_fail "TM not registered with JM after 60s — refusing to submit job"; return 1; }

  CP="$(cat "$LIB_CP_FILE")"
  [ -n "$CP" ] || { pipeline_fail "empty classpath from $LIB_CP_FILE"; return 1; }

  local mtok
  mtok=$(tail -n +2 "$LIB_MANIFEST" | wc -l)
  [ "$mtok" -ge 1024 ] || { pipeline_fail "manifest has only $mtok tokens; need >=1024"; return 1; }
  # G3 single source of truth (2026-08-31): the manifest SLICE handed to
  # Java (INSTRUMENT_MANIFEST_PATH) is the ONE token set for the run — the
  # bridge receives it from Java via the child-env handoff (startBridge
  # overwrites ARROW_INSTRUMENT_TOKENS with the loaded set), so the old
  # TOKENS env plumbing (and the 1,024-vs-2,431 skew that tripped the
  # fingerprint cross-check on every bridge event) is gone.
  local slice
  slice="$OUT/instruments-1024.csv"
  head -1025 "$LIB_MANIFEST" > "$slice"
  local ntok
  ntok=$(tail -n +2 "$slice" | grep -c .)
  [ "$ntok" -eq 1024 ] || { pipeline_fail "expected exactly 1024 tokens in slice, got $ntok"; return 1; }
  LIB_MANIFEST_SLICE="$slice"
  TOKENS=""   # deprecated: kept as empty for callers that still reference it

  mkdir -p "$OUT" "$OUT/bin" "$OUT/j1"
  pipeline_log "preflight OK (jars, bridge, manifest, port $FAKETOOL_PORT free, fluss up, TM fresh)"
}

# ---------- faketool ----------
# Builds + launches the faketool, waits for bind, asserts real-rate mode.
# B2 guard: after launching, verifies the process is STILL ALIVE — a
# bind failure (e.g. port raced) leaves a dead PID that must abort now,
# not at mid-run liveness.
pipeline_start_faketool() {
  pipeline_log "building faketool from $LIB_FAKETOOL_SRC"
  (cd "$LIB_BRIDGE_DIR" && go build -tags faketool -o "$OUT/bin/faketool" ./faketool) \
    || { pipeline_fail "faketool build failed"; return 1; }
  local inject_args=()
  # F2/F3 audit injection (2026-08-30): optional, from INJECT_* env vars.
  # See faketool main.go -inject-* flags for semantics.
  if [ "${INJECT_AFTER_MS:-0}" -gt 0 ]; then
    inject_args=(
      -inject-after-ms "${INJECT_AFTER_MS}ms"
      -inject-dups "${INJECT_DUPS:-0}"
      -inject-late "${INJECT_LATE:-0}"
      -inject-late-ms "${INJECT_LATE_MS:-90000}"
    )
    [ "${INJECT_EVERY_MS:-0}" -gt 0 ] \
      && inject_args+=(-inject-every-ms "${INJECT_EVERY_MS}ms")
    [ "${INJECT_MAX_ROUNDS:-0}" -gt 0 ] \
      && inject_args+=(-inject-max-rounds "${INJECT_MAX_ROUNDS}")
    pipeline_log "injection enabled: after=${INJECT_AFTER_MS}ms dups=${INJECT_DUPS:-0} late=${INJECT_LATE:-0} every=${INJECT_EVERY_MS:-0}ms"
  fi
  "$OUT/bin/faketool" -port "$FAKETOOL_PORT" -real-rate -real-rate-hz "$RATE_HZ" \
    "${inject_args[@]}" > "$OUT/faketool.log" 2>&1 &
  FAKETOOL_PID=$!

  local bound=0 i
  for i in $(seq 1 15); do
    if (exec 3<>/dev/tcp/127.0.0.1/$FAKETOOL_PORT) 2>/dev/null; then exec 3>&-; bound=1; break; fi
    sleep 1
  done
  if [ "$bound" != 1 ]; then
    echo "!! faketool did not bind :$FAKETOOL_PORT — log tail:" >&2
    tail -5 "$OUT/faketool.log" >&2
    return 1
  fi
  # B2 guard: alive after bind (catches instant-death after a successful bind too).
  kill -0 "$FAKETOOL_PID" 2>/dev/null || { pipeline_fail "faketool died right after bind — see $OUT/faketool.log"; return 1; }
  grep -q "real_rate=true" "$OUT/faketool.log" \
    || { echo "!! faketool log missing real_rate=true — log:" >&2; cat "$OUT/faketool.log" >&2; return 1; }
  pipeline_log "faketool on :$FAKETOOL_PORT (${RATE_HZ}Hz x 1024 = $((RATE_HZ * 1024))/s), pid $FAKETOOL_PID, real-rate confirmed"
}

# ---------- ingestion JVM ----------
# Canonical env block (same as loadtest-run.sh). Sets JVM_PID.
pipeline_start_ingestion() {
  # D6 right-size (2026-08-31): ingestion live-set measured 64-90MB over 17 runs;
  # 512m heap + 512m direct = 8x headroom (was 2g/1g). Verified by bench run G6/G7 guards.
  LOG_DIR="$OUT/j1" READINESS_FILE_PATH="/tmp/ingestion.loadtest.ready" \
  ARROW_HFT_URL="ws://127.0.0.1:$FAKETOOL_PORT" ARROW_BRIDGE_BIN="$LIB_BRIDGE_DIR/arrow-bridge" \
  ARROW_FAKE_BROKER="1" TRANSPORT="proto" \
  SECRETS_VIA_ENV_FILE="1" \
  ARROW_APP_ID="testd" ARROW_APP_SECRET="testd" \
  ARROW_USER_ID="testd-user" ARROW_PASSWORD="testd-pass" ARROW_TOTP_KEY="JBSWY3DPEHPK3PXP" \
  INSTRUMENT_MANIFEST_PATH="$LIB_MANIFEST_SLICE" \
  FLUSS_BOOTSTRAP="localhost:9123" FLUSS_BOOTSTRAP_SERVERS="localhost:9123" \
  RAW_TABLE_NAME="raw_table_1" ARROW_HFT_CONNECTIONS="1" \
  ARROW_MAX_EVENT_AGE_MS="${ARROW_MAX_EVENT_AGE_MS:-5000}" \
  ARROW_MAX_FUTURE_EVENT_SKEW_MS="2000" \
  ARROW_HFT_LATENCY_MS="50" CLOCK_CHECK_REQUIRED="false" \
  OTEL_COLLECTOR_HOST="localhost:4319" \
  FLUSS_WRITER_MODE="generic" FLUSS_WRITERS="1" FLUSS_WRITER_BATCH_SIZE_BYTES="0" \
  java --add-opens=java.base/java.nio=ALL-UNNAMED \
    -Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m \
    -Xlog:gc*,safepoint:file="$OUT/j1/gc.log:time,uptime,level,tags" \
    -Dlog.dir="$OUT/j1" \
    -cp "$LIB_ING_JAR" com.trading.ingestion.IngestionService > "$OUT/j1/java.out" 2>&1 &
  JVM_PID=$!
  pipeline_log "ingestion JVM pid $JVM_PID"

  local ready=0 i
  for i in $(seq 1 60); do [ -f "/tmp/ingestion.loadtest.ready" ] && { ready=1; break; }; sleep 2; done
  if [ "$ready" != 1 ]; then
    echo "!! JVM not ready — log tail:" >&2; tail -10 "$OUT/j1/java.out" >&2
    return 1
  fi
  for i in $(seq 1 30); do grep -q "HFT subscribed" "$OUT/j1/java.out" && break; sleep 1; done
  grep -q "HFT subscribed" "$OUT/j1/java.out" \
    || { echo "!! bridge never subscribed — log tail:" >&2; tail -10 "$OUT/j1/java.out" >&2; return 1; }
  pipeline_log "ingestion JVM ready + bridge subscribed (1024 tokens)"
}

# ---------- SignalJob submit ----------
# Deploys the jar and submits with the canonical env recipe. Sets JOB_ID.
# Knobs the caller may inject via the environment:
#   ALLOW_FULL_REPLAY (default false — B4 guard: LATEST startup mode),
#   CHECKPOINT_TIMEOUT_MS (default 30000).
# Purge the preview KV table (drop + recreate from its DDL) at RUN START.
# 2026-08-30 (tiering-off verify run): Fluss TTL is CALENDAR-DAY based, so
# the preview table's "60s TTL" never expires same-day data. Across many
# bench runs it accumulated 11.6M live keys → preview upserts slowed
# progressively (volume collapsed 722k → 63k → 15k → 0 across four runs)
# and the analyzer's from-earliest read (30s budget) couldn't reach the run
# window. Dropping + recreating before each run resets both. Preview data
# is transient diagnostics — nothing else consumes it between runs.
pipeline_purge_table() {
  # Generic drop+recreate from a DDL file. Used to bound what a fresh job
  # replays (the SignalJob source reads from EARLIEST — an unpurged table
  # means every phase replays all prior phases' rows) and to keep the
  # from-earliest evidence reads bounded (raw grows ~6M rows per 10-min
  # phase; reading all history would blow the analyzer's memory).
  local ddl_file="$1" label="$2"
  pipeline_log "purging $label table (drop + recreate)"
  cat > /tmp/TablePurge.java <<'JAVAEOF'
import com.trading.common.schema.ddl.DdlText;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class TablePurge {
    public static void main(String[] args) throws Exception {
        String ddl = Files.readString(Path.of(args[0]));
        DdlText.ParsedDdl parsed = DdlText.parse(ddl, args[0]);
        TablePath tp = TablePath.of("default", parsed.tableName());
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:9123");
        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin()) {
            try {
                admin.dropTable(tp, false).get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.out.println("drop skipped: " + e.getMessage());
            }
            admin.createTable(tp, DdlText.toDescriptor(parsed), false)
                    .get(60, TimeUnit.SECONDS);
            System.out.println("PURGED " + tp);
        }
    }
}
JAVAEOF
  local purge_out
  purge_out="$(cd /tmp && javac -cp "$CP" -d /tmp TablePurge.java 2>&1 \
      && java --add-opens=java.base/java.lang=ALL-UNNAMED \
       --add-opens=java.base/java.nio=ALL-UNNAMED \
       -cp "/tmp:$CP" TablePurge "$ddl_file" 2>&1)" || true
  rm -f /tmp/TablePurge.java /tmp/TablePurge.class
  if echo "$purge_out" | grep -q "PURGED"; then
    pipeline_log "$label table purged"
  else
    # Non-fatal: first run after a fresh DDL apply may legitimately have
    # nothing to drop; failure to CREATE would surface as the smoke gate
    # failing (preview metrics = 0). Log loudly for diagnosis.
    pipeline_log "WARN: $label purge output: $(echo "$purge_out" | tail -2)"
  fi
}

pipeline_purge_preview_table() {
  pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/30_feature_candles_15s_preview.sql" preview
}

pipeline_purge_raw_table() {
  # Raw must be purged BEFORE pipeline_start_ingestion (the ingestion JVM
  # writes it; preview is only written by the job, so it purges later).
  pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/02_raw_table_1.sql" raw
}

pipeline_submit_job() {
  pipeline_log "deploying SignalJob (previews 1s, early signals on, confirm-after 4s)"
  docker exec 01_docker-flink-jobmanager-1 mkdir -p /opt/flink/jobs 2>/dev/null \
    || { pipeline_fail "mkdir /opt/flink/jobs in flink-jobmanager failed"; return 1; }
  docker cp "$LIB_JAR" 01_docker-flink-jobmanager-1:/opt/flink/jobs/compute.jar \
    || { pipeline_fail "jar copy to flink-jobmanager failed"; return 1; }
  local submit_out
  # Single-timeline rule (2026-08-30): WATERMARK_OUT_OF_ORDER_MS is passed
  # explicitly so every run log shows the active value; default 500ms
  # matches the code default. NOTE: keep comments OUT of the continued
  # command — a '#' line between backslash continuations broke the whole
  # submit ("requires at least 2 args", observed 2026-08-30).
  submit_out="$($COMPOSE exec -T \
    -e ALLOW_FULL_REPLAY="${ALLOW_FULL_REPLAY:-false}" \
    -e DEPLOYMENT_ENV=dev \
    -e CONFIGURATION_VERSION=1.0.0 \
    -e ALGORITHM_VERSION=candle-15s-v1 \
    -e DEDUP_TTL_MS=60000 -e CANDLE_WINDOW_MS=15000 \
    -e WATERMARK_OUT_OF_ORDER_MS="${WATERMARK_OUT_OF_ORDER_MS:-500}" \
    -e CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-60000}" -e CHECKPOINT_TIMEOUT_MS="${CHECKPOINT_TIMEOUT_MS:-30000}" -e MAX_CONCURRENT_CHECKPOINTS=1 \
    -e PREVIEW_ENABLED=true -e PREVIEW_INTERVAL_MS="${PREVIEW_INTERVAL_MS:-500}" \
    -e EARLY_SIGNAL_ENABLED=true -e EARLY_SIGNAL_CONFIRM_AFTER_MS=4000 \
    -e SIGNAL_LOOKBACK_CANDLES=2 \
    -e FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 \
    -e SIGNAL_CANDIDATES_TABLE=Signal_Candidates \
    -e SIGNAL_CURRENT_TABLE=Signal_Candidates_current \
    flink-jobmanager flink run -d -Dmetrics.latency.interval="${LATENCY_TRACKING_MS:-2000}" \
      -c com.trading.compute.signaljob.SignalJob /opt/flink/jobs/compute.jar 2>&1)"
  JOB_ID="$(echo "$submit_out" | grep -oE 'JobID [a-f0-9]+' | awk '{print $2}' | head -1)"
  if [ -z "$JOB_ID" ]; then
    echo "!! SignalJob submit output:" >&2; echo "$submit_out" >&2
    return 1
  fi
  pipeline_log "SignalJob submitted: job_id=$JOB_ID"
}

# ---------- teardown ----------
pipeline_cleanup() {
  [ -n "$JVM_PID" ] && kill -9 "$JVM_PID" 2>/dev/null || true
  [ -n "$FAKETOOL_PID" ] && kill -9 "$FAKETOOL_PID" 2>/dev/null || true
  rm -f /tmp/ingestion.loadtest.ready
  if [ -n "$JOB_ID" ]; then
    echo "cleanup: cancelling SignalJob $JOB_ID"
    $COMPOSE exec -T flink-jobmanager flink cancel "$JOB_ID" >/dev/null 2>&1 || true
  fi
  echo "cleanup: killed jvm=$JVM_PID faketool=$FAKETOOL_PID job=$JOB_ID"
}
pipeline_install_cleanup_trap() { trap pipeline_cleanup EXIT; }

# ---------- checkpoint + GC telemetry (2026-08-30) ----------
# The REST /jobs/<id>/checkpoints history is TRIMMED after job cancel, so
# post-hoc queries only return the last few entries — live capture during
# the run is the only reliable record (observed: 10 of ~90 checkpoints
# survived after cancel).
capture_checkpoint_history() {
  # One snapshot of the full checkpoint history → JSONL appended per poll.
  local dest="${1:-$OUT/checkpoints.jsonl}"
  curl -s --max-time 10 "http://localhost:8081/jobs/$JOB_ID/checkpoints" \
    | python3 -c "
import json, sys
try:
    j = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for c in j.get('history', []):
    print(json.dumps({'id': c.get('id'), 'trigger_ts': c.get('trigger_timestamp'),
                      'duration_ms': c.get('end_to_end_duration'),
                      'size': c.get('state_size'), 'status': c.get('status')}))
" >> "$dest" 2>/dev/null || true
  # Dedupe on the checkpoint ID, not $1 — every JSONL line starts with
  # '{"id":' so $1 is identical for all lines and the old awk dropped
  # everything after the first entry (observed 2026-08-30: 600s run with a
  # 10s interval showed exactly ONE checkpoint — the checkpoint-burst
  # correlation was computed blind from a single sample).
  if [ -s "$dest" ]; then
    local tmp
    tmp="$(mktemp)"
    awk 'match($0, /"id": *[0-9]+/) { k=substr($0, RSTART, RLENGTH); if (!seen[k]++) print }' \
      "$dest" > "$tmp" && mv "$tmp" "$dest" || true
  fi
}

# Copy the TM's GC log (written by FLINK_ENV_JAVA_OPTS in docker-compose)
# into the evidence dir. Safe to call any time; empty if GC logging absent.
harvest_tm_gc_log() {
  local dest="${1:-$OUT/tm-gc.log}"
  docker exec 01_docker-flink-taskmanager-1 sh \
      -c 'cat /opt/flink/log/gc.log 2>/dev/null' > "$dest" 2>/dev/null || true
}

# ---------- Flink metrics ----------
# One REST call → lines of "name|read|write" per vertex (the format the
# gate scripts and the measurement script both consume).
flink_metric_dump() {
  local job="${1:-$JOB_ID}"
  curl -s --max-time 10 "http://localhost:8081/jobs/$job" | python3 -c "
import json,sys
try:
    j=json.load(sys.stdin)
except Exception:
    sys.exit(0)
print('STATE', j.get('state','?'))
for v in j.get('vertices',[]):
    m=v.get('metrics',{})
    print(v['name'],'|',m.get('read-records','-'),'|',m.get('write-records','-'))
" 2>/dev/null
}

# Wait until the job reaches the given state (default RUNNING) or timeout.
flink_wait_state() {
  local want="${1:-RUNNING}" timeout_s="${2:-60}" i state
  for i in $(seq 1 "$timeout_s"); do
    state="$(curl -s --max-time 5 "http://localhost:8081/jobs/$JOB_ID" | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('state',''))
except Exception: print('')" 2>/dev/null)"
    [ "$state" = "$want" ] && { pipeline_log "job state=$want after ${i}s"; return 0; }
    sleep 1
  done
  pipeline_fail "job did not reach state $want within ${timeout_s}s (last=$state)"
  return 1
}
