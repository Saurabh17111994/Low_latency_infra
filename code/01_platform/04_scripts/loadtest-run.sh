#!/usr/bin/env bash
# Load-test runner with ASSERTIVE preflight guards (audit #9 lessons, 2026-08-28).
#
# Failure class this guards against (each was hit during the 20,480/s compute test):
#   G1. faketool fails to start (bad flag/port) but script continues            -> port+alive assert
#   G2. faketool starts but idles (no -real-rate) => "live" but zero frames     -> log assert real_rate=true
#   G3. jar/bridge binary missing => java exits instantly, confusing output     -> file-exists preflight
#   G4. wrong env var names (INSTRUMENT_MANIFEST_PATH, ARROW_BRIDGE_BIN, ...)   -> single canonical env block (never re-typed)
#   G5. stray faketool from a previous run => "port already in use" -> the NEW
#       JVM silently talks to a STALE broker (wrong measurements!)              -> port-free preflight + pgrep -x cleanup
#   G6. token count exceeds the 1024 per-connection cap => bridge FATAL         -> count assert
#   G7. manifest CSV missing/deleted => synthetic fallback (50 instruments) or
#       empty set -> wrong subscription                                       -> file-exists + count assert
#   G8. collector reports p99 from a STALLED feed as if live (the 5.8s artifact)
#                                                                              -> feed-liveness tag in collect.sh
#   B1. faketool -real-rate-hz must DIVIDE 1000 (15Hz died "must divide 1000") -> RATE_HZ validity assert
#   B2. runs <190s are false negatives (stale storm needs >47s to appear)      -> min-duration guard in collect.sh
#   B3. faketool/JVM can die MID-RUN (session teardown) while the JVM keeps    -> mid-run liveness watcher
#       "measuring" nothing                                                    (t+60/t+180 checks, abort on death)
#   B4. /dev/tcp port probe once reported false-CLOSED while ss showed LISTEN  -> ss-based port check
#
# Usage: bash loadtest-run.sh [duration_s] [interval_s]  (env: RATE_HZ, default 20)
# Defaults: 240s duration, 30s interval. Output: logs/tracker-14/loadtest-<ts>/
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
JAR="$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
BRIDGE_DIR="$ROOT/code/02_services/01_ingestion/go-bridge"
# One level up, not two (see pipeline-lib.sh LIB_MANIFEST): `../../` lands in
# Jupyter_notebook/Arrow_broker, whose NSE_CM_EQUITY.csv has no LotSize column
# and is refused by the loader since d09e9795 (2026-09-07).
MANIFEST="$ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv"
FAKETOOL_SRC="$BRIDGE_DIR/faketool/main.go"
OUT="$ROOT/logs/tracker-14/loadtest-$(date +%Y%m%d-%H%M%S)"
DURATION_S="${1:-240}"
INTERVAL_S="${2:-30}"
RATE_HZ="${RATE_HZ:-20}"      # B1: faketool -real-rate-hz (must divide 1000)
JVM_PID=""; FAKETOOL_PID=""

fail() { echo "FATAL: $*" >&2; exit 1; }

# ---------- B1: rate validity ----------
# faketool's -real-rate-hz must DIVIDE 1000 (the ticker quantizes to whole
# milliseconds); 15Hz died in the audit with "must divide 1000". RATE_HZ comes
# from the env (default 20) and is passed straight through to faketool below.
validate_rate() { # validate_rate <hz>
  local hz="$1"
  case "$hz" in
    ''|*[!0-9]*) fail "RATE_HZ='$hz' is not a positive integer; valid rates: 1,2,4,5,8,10,20,25,40,50,100,125,200,250,500,1000";;
  esac
  [ "$hz" -gt 0 ] || fail "RATE_HZ must be a positive integer, got '$hz'"
  [ $(( 1000 % 10#$hz )) -eq 0 ] || fail "RATE_HZ=$hz is invalid: faketool -real-rate-hz must divide 1000 (1ms tick quantization). Valid rates: 1,2,4,5,8,10,20,25,40,50,100,125,200,250,500,1000"
}
validate_rate "$RATE_HZ"

# ---------- B4: port-free check ----------
# The /dev/tcp probe once reported false-CLOSED while `ss` showed LISTEN;
# prefer `ss -tln` when available, fall back to /dev/tcp only when ss is absent.
port_8899_free() {
  if command -v ss >/dev/null 2>&1; then
    if ss -tln 2>/dev/null | awk 'NR>1 {print $4}' | grep -q ':8899$'; then
      return 1   # a listener is bound to :8899 => busy
    fi
    return 0
  fi
  if (exec 3<>/dev/tcp/127.0.0.1/8899) 2>/dev/null; then exec 3>&-; return 1; fi
  return 0
}

cleanup() {
  [ -n "$JVM_PID" ] && kill -9 "$JVM_PID" 2>/dev/null || true
  [ -n "$FAKETOOL_PID" ] && kill -9 "$FAKETOOL_PID" 2>/dev/null || true
  rm -f /tmp/ingestion.loadtest.ready
  echo "cleanup: killed jvm=$JVM_PID faketool=$FAKETOOL_PID"
}

if [ "${1:-}" = "--check-only" ]; then
  # Preflight-only: verify the environment without launching anything.
  # Mirrors the G1/G3/G5/G6/G7 asserts below; used by `make check-loadtest-env`.
  [ -f "$JAR" ] || fail "ingestion jar missing: $JAR (run make build)"
  [ -f "$BRIDGE_DIR/arrow-bridge" ] || fail "arrow-bridge binary missing: $BRIDGE_DIR/arrow-bridge"
  [ -f "$FAKETOOL_SRC" ] || fail "faketool source missing: $FAKETOOL_SRC"
  [ -f "$MANIFEST" ] || fail "manifest CSV missing: $MANIFEST"
  MT=$(tail -n +2 "$MANIFEST" | wc -l)
  [ "$MT" -ge 1024 ] || fail "manifest has only $MT tokens; need >=1024"
  port_8899_free || fail "port 8899 busy — stale faketool?"
  echo "check-loadtest-env: OK (jar, bridge, faketool src, manifest >=1024 tokens, port 8899 free)"
  exit 0
fi

trap cleanup EXIT

echo "=== loadtest start $(date -Iseconds) duration=${DURATION_S}s interval=${INTERVAL_S}s ==="

# ---------- G1+G5+B4: port must be free before we start ----------
port_8899_free || fail "port 8899 already in use — a stale faketool/broker is running. Kill it first: pgrep -x faketool"

# ---------- G5: also kill any stray faketool by exact name (never -f; that matches our own shell) ----------
STRAY=$(pgrep -x faketool || true)
[ -z "$STRAY" ] || { echo "WARN: killing stray faketool(s): $STRAY"; for p in $STRAY; do kill -9 "$p" 2>/dev/null || true; done; sleep 1; }

# ---------- G3+G7: preflight files + counts ----------
[ -f "$JAR" ] || fail "ingestion jar missing: $JAR (run make build)"
[ -f "$BRIDGE_DIR/arrow-bridge" ] || fail "arrow-bridge binary missing: $BRIDGE_DIR/arrow-bridge"
[ -f "$FAKETOOL_SRC" ] || fail "faketool source missing: $FAKETOOL_SRC"
[ -f "$MANIFEST" ] || fail "manifest CSV missing: $MANIFEST (recreate from tokens: docs/...)"

MANIFEST_TOKENS=$(tail -n +2 "$MANIFEST" | wc -l)
[ "$MANIFEST_TOKENS" -ge 1024 ] || fail "manifest has only $MANIFEST_TOKENS tokens; need >=1024 for the 20k/s test"
echo "manifest OK: $MANIFEST_TOKENS tokens"

mkdir -p "$OUT" "$OUT/bin" "$OUT/j1"

# ---------- G6: exactly 1024 tokens (per-connection cap) ----------
# G3 (2026-08-31): single source of truth — slice the manifest to the
# instrument count of the run and hand ONLY the slice to Java; the bridge
# gets the same set from Java (child-env handoff in startBridge). The old
# TOKENS env var is no longer passed to the ingestion JVM.
MANIFEST_SLICE="$OUT/instruments-1024.csv"
head -1025 "$MANIFEST" > "$MANIFEST_SLICE"
N_TOKENS=$(tail -n +2 "$MANIFEST_SLICE" | grep -c .)
[ "$N_TOKENS" -eq 1024 ] || fail "expected exactly 1024 tokens in slice, got $N_TOKENS"
echo "tokens: $N_TOKENS (manifest slice: $MANIFEST_SLICE)"

# ---------- 1. faketool (build from source, like test-d) ----------
echo "building faketool from $FAKETOOL_SRC"
(cd "$BRIDGE_DIR" && go build -tags faketool -o "$OUT/bin/faketool" ./faketool) || fail "faketool build failed"
"$OUT/bin/faketool" -port 8899 -real-rate -real-rate-hz "$RATE_HZ" > "$OUT/faketool.log" 2>&1 &
FAKETOOL_PID=$!

# G1: wait for the port to actually bind
BOUND=0
for _ in $(seq 1 15); do
  if (exec 3<>/dev/tcp/127.0.0.1/8899) 2>/dev/null; then exec 3>&-; BOUND=1; break; fi
  sleep 1
done
[ "$BOUND" = 1 ] || { echo "!! faketool did not bind :8899 — log tail:"; tail -5 "$OUT/faketool.log"; fail "faketool failed to start"; }

# G2: assert real-rate mode is actually on (idle faketool = silently wrong measurements)
if ! grep -q "real_rate=true" "$OUT/faketool.log"; then
  echo "!! faketool log missing real_rate=true — log:"; cat "$OUT/faketool.log"
  fail "faketool is NOT in real-rate mode; aborting to avoid idle-feed measurements"
fi
echo "faketool on :8899 (${RATE_HZ}Hz x 1024 = $((RATE_HZ * 1024))/s), pid $FAKETOOL_PID, real-rate confirmed"

# ---------- 2. one ingestion JVM (single canonical env block — never re-typed) ----------
# G4: this block is the ONLY place these vars live; anything else sources it.
  # D6 right-size (2026-08-31): ingestion live-set measured 64-90MB over 17 runs;
  # 512m heap + 512m direct = 8x headroom (was 2g/1g). Verified by bench run G6/G7 guards.
LOG_DIR="$OUT/j1" READINESS_FILE_PATH="/tmp/ingestion.loadtest.ready" \
ARROW_HFT_URL="ws://127.0.0.1:8899" ARROW_BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge" \
ARROW_FAKE_BROKER="1" TRANSPORT="proto" \
SECRETS_VIA_ENV_FILE="1" \
ARROW_APP_ID="testd" ARROW_APP_SECRET="testd" \
ARROW_USER_ID="testd-user" ARROW_PASSWORD="testd-pass" ARROW_TOTP_KEY="JBSWY3DPEHPK3PXP" \
INSTRUMENT_MANIFEST_PATH="$MANIFEST_SLICE" \
FLUSS_BOOTSTRAP="localhost:9123" FLUSS_BOOTSTRAP_SERVERS="localhost:9123" \
RAW_TABLE_NAME="raw_table_1" ARROW_HFT_CONNECTIONS="1" \
ARROW_MAX_EVENT_AGE_MS="5000" ARROW_MAX_FUTURE_EVENT_SKEW_MS="2000" \
ARROW_HFT_LATENCY_MS="50" CLOCK_CHECK_REQUIRED="false" OTEL_COLLECTOR_HOST="localhost:4318" \
FLUSS_WRITER_MODE="generic" FLUSS_WRITERS="1" FLUSS_WRITER_BATCH_SIZE_BYTES="0" \
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m \
  -Dlog.dir="$OUT/j1" \
  -cp "$JAR" com.trading.ingestion.IngestionService > "$OUT/j1/java.out" 2>&1 &
JVM_PID=$!
echo "ingestion JVM pid $JVM_PID"

# ---------- readiness wait with log-tail on failure ----------
READY=0
for _ in $(seq 1 60); do [ -f "/tmp/ingestion.loadtest.ready" ] && { READY=1; break; }; sleep 2; done
[ "$READY" = 1 ] || { echo "!! JVM not ready — log tail:"; tail -10 "$OUT/j1/java.out"; fail "ingestion JVM failed to become ready"; }

# G4-extra: assert the bridge actually SUBSCRIBED (not just connected)
for _ in $(seq 1 30); do
  grep -q "HFT subscribed" "$OUT/j1/java.out" && break
  sleep 1
done
grep -q "HFT subscribed" "$OUT/j1/java.out" || { echo "!! bridge never subscribed — log tail:"; tail -10 "$OUT/j1/java.out"; fail "bridge subscription failed"; }
echo "ingestion JVM ready + bridge subscribed (1024 tokens)"

# ---------- B3: mid-run liveness ----------
# faketool/JVM can die MID-RUN (e.g. killed by session teardown) and leave the
# JVM consuming nothing while the collector keeps "measuring". Watch both PIDs
# at t+60 and t+180; on the first death print FATAL and abort the run early.
alive() { # alive <pid>: kill -0, refined against zombies (/proc state Z = dead)
  local pid="$1" st
  kill -0 "$pid" 2>/dev/null || return 1
  if [ -r "/proc/$pid/stat" ]; then
    st=$(awk -F')' '{split($2,a," "); print a[2]}' "/proc/$pid/stat" 2>/dev/null)
    [ "$st" != "Z" ] || return 1
  fi
  return 0
}
liveness_check() { # liveness_check <when> <pid> <name>: FATAL + rc 1 if dead
  local when="$1" pid="$2" name="${3:-proc}"
  if ! alive "$pid"; then
    echo "FATAL: mid-run liveness ($when): $name pid $pid is DEAD — measurements are garbage; aborting" >&2
    return 1
  fi
  return 0
}

# ---------- 3. collector with feed-liveness guard (G8) + mid-run liveness (B3) ----------
# Resolve the RUNNING signal-job-compute at collector start — NEVER hardcode:
# a rollout replaces the job ID and a stale default silently samples a
# CANCELED job (all metrics n/a, 0 VALID snapshots). Discovered 2026-08-29
# during the Finding #17 fix rollout (smoke sampled the dead e641dc3e).
if [ -z "${JOB_ID:-}" ]; then
  JOB_ID="$(curl -s --max-time 5 http://localhost:8081/jobs/overview 2>/dev/null \
    | python3 -c "
import json,sys
try:
    jobs=json.load(sys.stdin).get('jobs',[])
    running=[j['jid'] for j in jobs if j.get('state')=='RUNNING' and 'signal-job-compute' in j.get('name','')]
    print(running[0] if running else '')
except Exception:
    pass
" 2>/dev/null)"
  if [ -z "$JOB_ID" ]; then
    echo "FATAL: could not resolve a RUNNING signal-job-compute job id (rollout in progress?)" >&2
    exit 2
  fi
  echo "loadtest: resolved RUNNING signal-job-compute job id=$JOB_ID"
fi
bash "$(dirname "${BASH_SOURCE[0]}")/loadtest-collect.sh" "$OUT" "$DURATION_S" "$INTERVAL_S" "$JOB_ID" &
COLLECTOR_PID=$!
(
  sleep 60   # first liveness check at t+60
  if ! liveness_check t+60 "$FAKETOOL_PID" faketool || ! liveness_check t+60 "$JVM_PID" jvm; then
    kill -9 "$COLLECTOR_PID" 2>/dev/null || true
    exit 1
  fi
  sleep 120  # second liveness check at t+180
  if ! liveness_check t+180 "$FAKETOOL_PID" faketool || ! liveness_check t+180 "$JVM_PID" jvm; then
    kill -9 "$COLLECTOR_PID" 2>/dev/null || true
    exit 1
  fi
) &
LIVENESS_PID=$!

# wait on the collector (the main duration window) and on the liveness watcher
wait "$COLLECTOR_PID"; RC=$?
if [ "$RC" -ne 0 ]; then kill -9 "$LIVENESS_PID" 2>/dev/null || true; fi
wait "$LIVENESS_PID" || [ "$RC" -ne 0 ] || RC=1
[ "$RC" -eq 0 ] || fail "loadtest aborted (rc=$RC) — see FATAL above"

echo "=== loadtest done out=$OUT ==="
echo "--- post-run overlay check: ingestion container env still real feed? ---"
docker exec 01_docker-ingestion-1 sh -c 'echo "FAKE=$ARROW_FAKE_BROKER"' 2>/dev/null || echo "(container not up — skip)"
