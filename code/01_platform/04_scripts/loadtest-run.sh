#!/usr/bin/env bash
# Load-test runner with ASSERTIVE preflight guards (audit #9 lessons, 2026-08-28).
#
# Failure class this guards against (each was hit during the 20,480/s compute test):
#   G1. faketool fails to start (bad flag/port) but script continues            -> port+alive assert
#   G2. faketool starts but idles (no -real-rate) => "live" but zero frames     -> log assert real_rate=true
#   G3. jar/bridge binary missing => java exits instantly, confusing output     -> file-exists preflight
#   G4. wrong env var names (INSTRUMENT_MANIFEST_PATH, ARROW_BRIDGE_BIN, ...)   -> single canonical env block (never re-typed)
#   G5. stray faketool from a previous run => "port already in use" -> the NEW
#       JVM silently talks to a STALE broker (wrong measurements!)              -> reap strays, then port-free assert
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
#   B5. the collector/watcher outlive the run (up to 2min dead time after a      -> teardown kills BOTH; a final
#       short collector; both were leaked on abort)                                alive check replaces the wait
#   B6. a fixed /tmp readiness file false-passes after `kill -9` (the EXIT trap  -> per-run readiness path under
#       never ran, so a stale marker reads as "ready")                             $OUT, as pipeline-lib does
#   B7. inline credentials are readable in /proc/<pid>/environ (plan S3)         -> loaded from the 0600 secrets file
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
# P6-451/453: second-granularity timestamps collide when two runs start in the
# same second; the pid keeps each run's artifacts separate.
OUT="$ROOT/logs/tracker-14/loadtest-$(date +%Y%m%d-%H%M%S)-$$"
DURATION_S="${1:-240}"
INTERVAL_S="${2:-30}"
RATE_HZ="${RATE_HZ:-20}"      # B1: faketool -real-rate-hz (must divide 1000)
JVM_PID=""; FAKETOOL_PID=""; COLLECTOR_PID=""; LIVENESS_PID=""

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

# ---------- B5: numeric-argument validity (same shape as validate_rate) ----------
# DURATION_S/INTERVAL_S came straight from argv and were handed to the collector
# and to `sleep`; a typo ("240s", "", "-5") died later as a raw shell error inside
# collect.sh's `[ "$DURATION_S" -lt 190 ]` or silently skewed the sample cadence.
validate_pos_int() { # validate_pos_int <name> <value> <what>
  local name="$1" val="$2" what="${3:-}"
  case "$val" in
    ''|*[!0-9]*) fail "$name='$val' is not a positive integer${what:+ ($what)}";;
  esac
  [ "$val" -gt 0 ] || fail "$name must be a positive integer, got '$val'${what:+ ($what)}"
}

# ---------- G6: ONE token counter --------------------------------------------
# P6-130/132: the manifest preflight counted with `wc -l` while the slice assert
# counted with `grep -c .`, so a blank or CRLF line could pass one gate and fail
# the next. Count data rows only, CR stripped (R-081).
count_tokens() { # count_tokens <csv>
  tail -n +2 "$1" | tr -d '\r' | grep -c . || true
}

# ---------- S3/P6-447: credentials come from the 0600 secrets file ------------
# They were literals in the child environment, i.e. readable in
# /proc/<pid>/environ. Same git-ignored file the compose commands and
# run-ingestion-full.sh use, with the R-212/P6-682 hardening: refuse symlinks,
# accept owner-only modes (GNU or BSD stat), fail fast on a missing key.
SECRETS_FILE="${SECRETS_FILE:-$ROOT/code/01_platform/01_docker/secrets.env}"
resolve_arrow_credentials() {
  [ -f "$SECRETS_FILE" ] || fail "secrets file missing: $SECRETS_FILE (create it from the template, chmod 600)"
  [ -L "$SECRETS_FILE" ] && fail "$SECRETS_FILE is a symlink — refusing to source it (it could point anywhere)"
  local mode=""
  if stat -c '%a' "$SECRETS_FILE" >/dev/null 2>&1; then mode="$(stat -c '%a' "$SECRETS_FILE")"
  elif stat -f '%Lp' "$SECRETS_FILE" >/dev/null 2>&1; then mode="$(stat -f '%Lp' "$SECRETS_FILE")"; fi
  case "$mode" in
    600|400) ;;
    *) fail "$SECRETS_FILE is not owner-only (mode ${mode:-unknown}). Run: chmod 600 $SECRETS_FILE";;
  esac
  # shellcheck disable=SC1090
  source "$SECRETS_FILE"
  : "${ARROW_APP_SECRET:?ARROW_APP_SECRET must be set in $SECRETS_FILE}"
  : "${ARROW_PASSWORD:?ARROW_PASSWORD must be set in $SECRETS_FILE}"
  : "${ARROW_TOTP_KEY:?ARROW_TOTP_KEY must be set in $SECRETS_FILE}"
  echo "secrets: loaded from $SECRETS_FILE (mode $mode; values not printed)"
}

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

# ---------- B3: mid-run liveness (defined here: the startup waits use alive) ----------
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

cleanup() {
  [ -n "$JVM_PID" ] && kill -9 "$JVM_PID" 2>/dev/null || true
  [ -n "$FAKETOOL_PID" ] && kill -9 "$FAKETOOL_PID" 2>/dev/null || true
  # P6-131/134: the collector and the liveness watcher were never reaped, so an
  # aborted run leaked both (a live collector keeps sampling a run that is over).
  # Children first: once the watcher is gone its `sleep` is reparented and -P can
  # no longer reach it.
  if [ -n "$LIVENESS_PID" ]; then
    pkill -9 -P "$LIVENESS_PID" 2>/dev/null || true
    kill -9 "$LIVENESS_PID" 2>/dev/null || true
  fi
  [ -n "$COLLECTOR_PID" ] && kill -9 "$COLLECTOR_PID" 2>/dev/null || true
  [ -n "${READINESS_FILE:-}" ] && rm -f "$READINESS_FILE"
  echo "cleanup: killed jvm=$JVM_PID faketool=$FAKETOOL_PID collector=$COLLECTOR_PID liveness=$LIVENESS_PID"
}

if [ "${1:-}" = "--check-only" ]; then
  # Preflight-only: verify the environment without launching anything.
  # Mirrors the G1/G3/G5/G6/G7 asserts below; used by `make check-loadtest-env`.
  [ -f "$JAR" ] || fail "ingestion jar missing: $JAR (run make build)"
  [ -f "$BRIDGE_DIR/arrow-bridge" ] || fail "arrow-bridge binary missing: $BRIDGE_DIR/arrow-bridge"
  [ -f "$FAKETOOL_SRC" ] || fail "faketool source missing: $FAKETOOL_SRC"
  [ -f "$MANIFEST" ] || fail "manifest CSV missing: $MANIFEST"
  MT=$(count_tokens "$MANIFEST")
  [ "$MT" -ge 1024 ] || fail "manifest has only $MT tokens; need >=1024"
  port_8899_free || fail "port 8899 busy — stale faketool?"
  echo "check-loadtest-env: OK (jar, bridge, faketool src, manifest >=1024 tokens, port 8899 free)"
  exit 0
fi

# B5: only the real run path validates these (--check-only stays runnable with
# no positional arguments).
validate_pos_int DURATION_S "$DURATION_S" "run seconds; collect.sh gives no verdict below 190s"
validate_pos_int INTERVAL_S "$INTERVAL_S" "sample interval seconds"

trap cleanup EXIT

echo "=== loadtest start $(date -Iseconds) duration=${DURATION_S}s interval=${INTERVAL_S}s ==="

# ---------- G1+G5+B4: reap strays FIRST, then assert the port is free ----------
# P6-127/128: the port assert ran BEFORE the reap, so a stale faketool holding
# :8899 made the reap unreachable — the run failed instead of recovering. Reap by
# exact name (never -f; that matches our own shell), then re-assert: the port is
# fixed, so one run at a time, and a peer run's faketool is not a stray.
STRAY=$(pgrep -x faketool || true)
[ -z "$STRAY" ] || { echo "WARN: killing stray faketool(s): $STRAY"; for p in $STRAY; do kill -9 "$p" 2>/dev/null || true; done; sleep 1; }
port_8899_free || fail "port 8899 already in use — a stale faketool/broker is running. Kill it first: pgrep -x faketool"

# ---------- G3+G7: preflight files + counts ----------
[ -f "$JAR" ] || fail "ingestion jar missing: $JAR (run make build)"
[ -f "$BRIDGE_DIR/arrow-bridge" ] || fail "arrow-bridge binary missing: $BRIDGE_DIR/arrow-bridge"
[ -f "$FAKETOOL_SRC" ] || fail "faketool source missing: $FAKETOOL_SRC"
[ -f "$MANIFEST" ] || fail "manifest CSV missing: $MANIFEST (recreate from tokens: docs/...)"

MANIFEST_TOKENS=$(count_tokens "$MANIFEST")
[ "$MANIFEST_TOKENS" -ge 1024 ] || fail "manifest has only $MANIFEST_TOKENS tokens; need >=1024 for the 20k/s test"
echo "manifest OK: $MANIFEST_TOKENS tokens"

mkdir -p "$OUT" "$OUT/bin" "$OUT/j1"
# B6/P6-129/133: readiness marker per run, removed before the launch. The old
# fixed /tmp path survived a `kill -9` (the EXIT trap never ran), so the next run
# read the stale marker as "ready" and measured a JVM that was not up yet. Same
# per-run path pipeline-lib.sh uses for its container run.
READINESS_FILE="$OUT/ingestion.loadtest.ready"
rm -f "$READINESS_FILE"

# ---------- G6: exactly 1024 tokens (per-connection cap) ----------
# G3 (2026-08-31): single source of truth — slice the manifest to the
# instrument count of the run and hand ONLY the slice to Java; the bridge
# gets the same set from Java (child-env handoff in startBridge). The old
# TOKENS env var is no longer passed to the ingestion JVM.
MANIFEST_SLICE="$OUT/instruments-1024.csv"
# P6-130/132: `head -1025` trusted the manifest's shape (a blank/CRLF line or a
# reordered manifest produced a bad slice while the 1024-row count still passed).
# Validate the header, then build from real data rows only — the same shape as
# pipeline-lib.sh's P6-473 fix, so both harnesses slice identically.
HDR="$(head -1 "$MANIFEST" | tr -d '\r')"
case "$HDR" in
  *","*) ;;
  *) fail "manifest header has no comma separator: '$HDR'";;
esac
HDR_FIELDS="$(printf '%s\n' "$HDR" | awk -F, '{print NF}')"
[ "$HDR_FIELDS" -ge 2 ] || fail "manifest header has fewer than 2 fields: '$HDR'"
{ printf '%s\n' "$HDR"
  tail -n +2 "$MANIFEST" | tr -d '\r' | grep . | head -1024; } > "$MANIFEST_SLICE"
N_TOKENS=$(count_tokens "$MANIFEST_SLICE")
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
  port_8899_free || { BOUND=1; break; }        # B4: ss-based, not /dev/tcp
  alive "$FAKETOOL_PID" || break               # P6-448/454: do not sleep out a dead faketool
  sleep 1
done
if [ "$BOUND" != 1 ]; then
  echo "!! faketool did not bind :8899$(alive "$FAKETOOL_PID" || printf ' (faketool pid %s is DEAD)' "$FAKETOOL_PID") — log tail:"
  tail -5 "$OUT/faketool.log"
  fail "faketool failed to start"
fi

# G2: assert real-rate mode is actually on (idle faketool = silently wrong measurements)
# P6-450/455: the log write is buffered, so a single grep right after bind raced
# the flush (false fatal); and never `cat` a log that grows at 20k lines/s.
RATE_OK=0
for _ in $(seq 1 10); do
  grep -q "real_rate=true" "$OUT/faketool.log" && { RATE_OK=1; break; }
  alive "$FAKETOOL_PID" || break
  sleep 1
done
if [ "$RATE_OK" != 1 ]; then
  echo "!! faketool log missing real_rate=true — log tail:"; tail -20 "$OUT/faketool.log"
  fail "faketool is NOT in real-rate mode; aborting to avoid idle-feed measurements"
fi
echo "faketool on :8899 (${RATE_HZ}Hz x 1024 = $((RATE_HZ * 1024))/s), pid $FAKETOOL_PID, real-rate confirmed"

# ---------- 2. one ingestion JVM (single canonical env block — never re-typed) ----------
# G4: this block is the ONLY place these vars live; anything else sources it.
  # D6 right-size (2026-08-31): ingestion live-set measured 64-90MB over 17 runs;
  # 512m heap + 512m direct = 8x headroom (was 2g/1g). Verified by bench run G6/G7 guards.
# B7/S3: the credential keys are NOT re-typed here — resolve_arrow_credentials
# loads them from the 0600 secrets file and the JVM inherits them (inline values
# were readable in /proc/<pid>/environ). ARROW_APP_ID/USER_ID are identity, not
# credentials, so they stay in the block.
resolve_arrow_credentials
LOG_DIR="$OUT/j1" READINESS_FILE_PATH="$READINESS_FILE" \
ARROW_HFT_URL="ws://127.0.0.1:8899" ARROW_BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge" \
ARROW_FAKE_BROKER="1" TRANSPORT="proto" \
SECRETS_VIA_ENV_FILE="1" \
ARROW_APP_ID="testd" \
ARROW_USER_ID="testd-user" \
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
for _ in $(seq 1 60); do
  [ -f "$READINESS_FILE" ] && { READY=1; break; }
  alive "$JVM_PID" || break                    # P6-449/456: a dead JVM must not cost 120s
  sleep 2
done
if [ "$READY" != 1 ]; then
  echo "!! JVM not ready$(alive "$JVM_PID" || printf ' (jvm pid %s is DEAD)' "$JVM_PID") — log tail:"
  tail -10 "$OUT/j1/java.out"
  fail "ingestion JVM failed to become ready"
fi

# G4-extra: assert the bridge actually SUBSCRIBED (not just connected)
SUB_OK=0
for _ in $(seq 1 30); do
  grep -q "HFT subscribed" "$OUT/j1/java.out" && { SUB_OK=1; break; }
  alive "$JVM_PID" || break                    # P6-449/456: 30s of polling a dead JVM asserts nothing
  sleep 1
done
if [ "$SUB_OK" != 1 ]; then
  echo "!! bridge never subscribed$(alive "$JVM_PID" || printf ' (jvm pid %s is DEAD)' "$JVM_PID") — log tail:"
  tail -10 "$OUT/j1/java.out"
  fail "bridge subscription failed"
fi
echo "ingestion JVM ready + bridge subscribed (1024 tokens)"

# ---------- B3: mid-run liveness (watcher; helpers defined above) ----------

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

# Wait on the collector (the main duration window). P6-131/134: do NOT wait on the
# liveness watcher afterwards — it sleeps to t+180, so a collector that finishes
# sooner (any run <180s) bought up to 2min of dead time, and an early abort left it
# and the collector behind. Stop the watcher now and take the final liveness
# reading synchronously: that also covers a death after t+180.
wait "$COLLECTOR_PID"; RC=$?
if [ -n "$LIVENESS_PID" ]; then
  pkill -9 -P "$LIVENESS_PID" 2>/dev/null || true
  kill -9 "$LIVENESS_PID" 2>/dev/null || true
  wait "$LIVENESS_PID" 2>/dev/null || true
fi
if [ "$RC" -eq 0 ]; then
  liveness_check t+end "$FAKETOOL_PID" faketool || RC=1
  liveness_check t+end "$JVM_PID" jvm || RC=1
fi
[ "$RC" -eq 0 ] || fail "loadtest aborted (rc=$RC) — see FATAL above"

echo "=== loadtest done out=$OUT (rc=$RC) ==="
echo "--- post-run overlay check: ingestion container env still real feed? ---"
docker exec 01_docker-ingestion-1 sh -c 'echo "FAKE=$ARROW_FAKE_BROKER"' 2>/dev/null || echo "(container not up — skip)"
