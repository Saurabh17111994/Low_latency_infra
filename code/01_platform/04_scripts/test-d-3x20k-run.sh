#!/usr/bin/env bash
# Test D: 3-faketool / 3-JVM parallel ingestion bench — multi-writer to one table.
# Host-side ONLY — zero code changes, zero image changes.
# Topology: 3 faketools (ports 8899/8900/8901, real-rate 20 Hz, disjoint token
# thirds from the NSE manifest) -> 3 ingestion JVMs, each spawning its own
# arrow-bridge (ARROW_HFT_CONNECTIONS=1, TRANSPORT=proto, disjoint
# ARROW_INSTRUMENT_TOKENS) -> each JVM's own AppendWriter -> raw_table_1.
# Measure: aggregate appended rows/s vs the ~59k tablet ceiling + 0 loss.
# Evidence: $OUT/test-d-summary.txt, written and asserted by this script
# (APPENDED / ERRORS / UNCERTAIN / PENDING_REMAINING / AGG_ROWS_PER_S).
# Historical note: a 2026-08-28 5-min proto run reported 8,575,052 rows in 313s =
# 27,400/s, 0 errors (3-writer parallel proven); that log is NOT in the tree
# (logs/ is gitignored), which is exactly why the assertion above matters.
# Live 60k is emitter-blocked; synthetic PerfBaselineTest >=57.6k.
set -euo pipefail
# P6-578: derive the tree instead of hardcoding one developer machine, and make
# the out-of-tree manifest overridable. Everything the bench needs is then checked
# in preflight, so a missing file fails in a second rather than after the build.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${TEST_D_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
JAR="$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
BRIDGE_DIR="$ROOT/code/02_services/01_ingestion/go-bridge"
MANIFEST="${TEST_D_MANIFEST:-$ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv}"
preflight() {
  local missing=0 f c
  for f in "$JAR" "$MANIFEST"; do
    [ -e "$f" ] || { echo "!! missing: $f" >&2; missing=1; }
  done
  [ -d "$BRIDGE_DIR" ] || { echo "!! missing: $BRIDGE_DIR" >&2; missing=1; }
  for c in java go python3; do
    command -v "$c" >/dev/null 2>&1 || { echo "!! not on PATH: $c" >&2; missing=1; }
  done
  [ "$missing" = 0 ] || { echo "!! preflight failed (fix the lines above; TEST_D_ROOT/TEST_D_MANIFEST override the paths)"; exit 1; }
}
OUT="$ROOT/logs/tracker-14/test-d-$(date +%Y%m%d-%H%M%S)"
DURATION_S="${TEST_D_DURATION_S:-300}"   # 5 min default
export TRANSPORT="proto"   # T6 proto path (NOT NDJSON pipe) — the low-latency transport
# P6-579: no committed credentials. The gitignored secrets file the compose stack
# and run-ingestion-full.sh use is the source of truth (mode-checked the same way);
# a local fake-broker bench may fall back to explicit placeholders, and never to a
# real-looking TOTP seed.
resolve_arrow_credentials() {
  SECRETS_FILE="${SECRETS_FILE:-$ROOT/code/01_platform/01_docker/secrets.env}"
  export ARROW_APP_ID="${ARROW_APP_ID:-testd}"
  export ARROW_FAKE_BROKER="${ARROW_FAKE_BROKER:-1}"
  if [ -r "$SECRETS_FILE" ]; then
    # Same hardening as the canonical reader (run-ingestion-full.sh R-212/P6-682):
    # refuse a symlink before measuring, accept 400, and fall back to the BSD stat.
    if [ -L "$SECRETS_FILE" ]; then
      echo "!! $SECRETS_FILE is a symlink — refusing to source it" >&2; exit 1
    fi
    local mode=""
    if stat -c '%a' "$SECRETS_FILE" >/dev/null 2>&1; then
      mode="$(stat -c '%a' "$SECRETS_FILE")"
    elif stat -f '%Lp' "$SECRETS_FILE" >/dev/null 2>&1; then
      mode="$(stat -f '%Lp' "$SECRETS_FILE")"
    fi
    case "$mode" in
      600|400) ;;
      *) echo "!! $SECRETS_FILE has mode ${mode:-unknown} (want 600/400) — refusing to read it" >&2; exit 1 ;;
    esac
    # shellcheck disable=SC1090
    set -a; . "$SECRETS_FILE"; set +a
  fi
  if [ "$ARROW_FAKE_BROKER" = "1" ]; then
    export ARROW_USER_ID="${ARROW_USER_ID:-testd-user}" \
           ARROW_PASSWORD="${ARROW_PASSWORD:-fake-broker}" \
           ARROW_TOTP_KEY="${ARROW_TOTP_KEY:-fake-broker}"
  else
    : "${ARROW_APP_SECRET:?set it in $SECRETS_FILE, or run with ARROW_FAKE_BROKER=1}"
    : "${ARROW_TOTP_KEY:?set it in $SECRETS_FILE, or run with ARROW_FAKE_BROKER=1}"
    export ARROW_APP_SECRET ARROW_TOTP_KEY
  fi
}
export FLUSS_BOOTSTRAP="localhost:9123" FLUSS_BOOTSTRAP_SERVERS="localhost:9123"
export RAW_TABLE_NAME="raw_table_1"
export ARROW_MAX_EVENT_AGE_MS="${ARROW_MAX_EVENT_AGE_MS:-5000}" ARROW_MAX_FUTURE_EVENT_SKEW_MS="${ARROW_MAX_FUTURE_EVENT_SKEW_MS:-2000}"
export ARROW_HFT_LATENCY_MS="50"
export CLOCK_CHECK_REQUIRED="false" OTEL_COLLECTOR_HOST="localhost:4318"
export FLUSS_WRITER_MODE="generic" FLUSS_WRITERS="1" FLUSS_WRITER_BATCH_SIZE_BYTES="0"
# P6-222: `set -e` plus a bring-up failure (or a SIGINT during the feed) used to
# leave 3 faketools + 3 JVMs holding ports 8899-8901 and corrupt the next run.
cleanup() {
  local pid i
  for pid in "${JVMS[@]:-}"; do
    [ -n "$pid" ] || continue
    kill -TERM "$pid" 2>/dev/null || true
    for i in $(seq 1 120); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$pid" 2>/dev/null || true
  done
  for pid in "${FAKETOOL_PIDS[@]:-}"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
  done
  return 0
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# Build faketool
preflight
resolve_arrow_credentials
# P6-578: created only once preflight has passed, so a bad path fails with
# the explicit reason above rather than a mkdir error.
mkdir -p "$OUT" "$OUT/j1" "$OUT/j2" "$OUT/j3" "$OUT/bin"
echo "=== Test D start $(date -Iseconds) duration=${DURATION_S}s"
(cd "$BRIDGE_DIR" && go build -tags faketool -o "$OUT/bin/faketool" ./faketool)
# Start 3 faketools, one per connection (ports 8899/8900/8901), each serving its token third
FAKETOOL_PIDS=()
for port in 8899 8900 8901; do
  "$OUT/bin/faketool" -port $port -real-rate -real-rate-hz 20 > "$OUT/faketool-$port.log" 2>&1 &
  FAKETOOL_PIDS+=($!)
done
# P6-223: this loop used to discard its own result, so a faketool that never came
# up still produced a 5-min measurement that read as a throughput regression.
for port in 8899 8900 8901; do
  up=0
  for _ in $(seq 1 30); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then up=1; break; fi
    sleep 1
  done
  if [ "$up" != 1 ]; then
    echo "!! faketool on :$port never accepted a connection within 30s"
    tail -5 "$OUT/faketool-$port.log" 2>/dev/null || true
    exit 1
  fi
done
# Split the 1024-token manifest into 3 disjoint ranges (341/341/342)
python3 - "$MANIFEST" "$OUT" <<'PY'
import csv, sys, os
manifest, out = sys.argv[1], sys.argv[2]
toks = []
with open(manifest, newline='') as f:
    for row in csv.DictReader(f):
        try: toks.append(int(row['Token']))
        except (ValueError, KeyError): pass
toks = sorted(set(toks))
n = len(toks)
parts = [toks[:n//3], toks[n//3:2*(n//3)], toks[2*(n//3):]]
for i, p in enumerate(parts, 1):
    with open(f"{out}/tokens-{i}.txt", "w") as f:
        f.write(",".join(str(t) for t in p))
    print(f"part {i}: {len(p)} tokens")
PY
echo "=== faketool ready, launching 3 JVMs ==="
JVMS=()
for i in 1 2 3; do
  R="$OUT/j$i"
  # P6-225: the JVM reads exactly this path (IngestionService), so a leftover file
  # under /tmp could satisfy the wait instantly, and an inherited value could
  # collapse all three JVMs onto one file. Per-run, per-JVM, under the run dir.
  READY_FILE="$R/ingestion.ready"; rm -f "$READY_FILE"
  LOG_DIR="$R" READINESS_FILE_PATH="$READY_FILE" \
  ARROW_HFT_URL="ws://127.0.0.1:$((8898+i))" ARROW_BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge" \
  ARROW_INSTRUMENT_TOKENS="$(cat "$OUT/tokens-$i.txt")" \
  INSTRUMENT_MANIFEST_PATH="$MANIFEST" \
  ARROW_HFT_CONNECTIONS="1" \
  java --add-opens=java.base/java.nio=ALL-UNNAMED \
    -Xms2g -Xmx2g -XX:MaxDirectMemorySize=1g \
    -Dlog.dir="$R" \
    -cp "$JAR" com.trading.ingestion.IngestionService > "$R/java.out" 2>&1 &
  JVMS+=($!)
done
# Wait for all 3 ready
for i in 1 2 3; do
  READY=0
  for _ in $(seq 1 60); do [ -f "$OUT/j$i/ingestion.ready" ] && { READY=1; break; }; sleep 2; done
  [ "$READY" = 1 ] || { echo "!! JVM $i not ready"; tail -5 "$OUT/j$i/java.out"; exit 1; }
done
echo "=== all 3 ready at $(date -Iseconds), running ${DURATION_S}s"
START_EPOCH=$(date +%s)
while [ $(( $(date +%s) - START_EPOCH )) -lt "$DURATION_S" ]; do sleep 10; done
echo "=== duration reached, draining at $(date -Iseconds)"
cleanup
END_EPOCH=$(date +%s)
# P6-580: printing the close lines proved nothing (and under `set -o pipefail` a
# journal without the pattern even made the whole run exit non-zero after the
# fact). The aggregate is computed and asserted here: the journals are JSON-lines
# and each writer's close record carries the counters.
summary_of_journals() {   # <elapsed_s>
  python3 - "$OUT" "$1" <<'PYJ'
import re, sys
out, elapsed = sys.argv[1], int(sys.argv[2])
tot = {"appended": 0, "errors": 0, "uncertain": 0, "pending_remaining": 0}
problems = []
for i in (1, 2, 3):
    path = f"{out}/j{i}/journal/ingestion.json"
    close = None
    try:
        with open(path) as fh:
            for line in fh:
                if "raw-writer: closed" not in line:
                    continue
                m = re.search(r"raw-writer: closed \(([^)]*)\)", line)
                if m:
                    close = dict(kv.split("=", 1) for kv in m.group(1).split(", ") if "=" in kv)
    except OSError:
        problems.append(f"j{i}: journal missing ({path})")
        continue
    if close is None:
        problems.append(f"j{i}: no raw-writer close line")
        continue
    for k in tot:
        try:
            tot[k] += int(close.get(k, 0))
        except (TypeError, ValueError):
            problems.append(f"j{i}: unparsable {k}={close.get(k)!r}")
    print(f"  j{i}: appended={close.get('appended')} errors={close.get('errors')} "
          f"uncertain={close.get('uncertain')} pending_remaining={close.get('pending_remaining')}")
for p in problems:
    print("  " + p)
rate = tot["appended"] / elapsed if elapsed > 0 else 0
print(f"APPENDED={tot['appended']} ERRORS={tot['errors']} UNCERTAIN={tot['uncertain']} "
      f"PENDING_REMAINING={tot['pending_remaining']} ELAPSED_S={elapsed} AGG_ROWS_PER_S={rate:.0f}")
sys.exit(1 if problems or tot["errors"] or tot["uncertain"] or tot["pending_remaining"] else 0)
PYJ
}

echo "=== Test D done out=$OUT"
if ! summary_of_journals "$((END_EPOCH - START_EPOCH))" | tee "$OUT/test-d-summary.txt"; then
  echo "!! Test D evidence check FAILED: absent close line, or non-zero errors/uncertain/pending" >&2
  exit 1
fi
echo "=== Test D PASSED: aggregate rows/s asserted above (summary: $OUT/test-d-summary.txt)"
