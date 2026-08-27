#!/usr/bin/env bash
# Test D: 3-faketool / 3-JVM parallel ingestion bench — multi-writer to one table.
# Host-side ONLY — zero code changes, zero image changes.
# Topology: 3 faketools (ports 8899/8900/8901, real-rate 20 Hz, disjoint token
# thirds from the NSE manifest) -> 3 ingestion JVMs, each spawning its own
# arrow-bridge (ARROW_HFT_CONNECTIONS=1, TRANSPORT=proto, disjoint
# ARROW_INSTRUMENT_TOKENS) -> each JVM's own AppendWriter -> raw_table_1.
# Measure: aggregate appended rows/s vs the ~59k tablet ceiling + 0 loss.
# Evidence: logs/tracker-14/test-d-evidence-20260828.md (5-min proto run:
# 8,575,052 rows in 313s = 27,400/s, 0 errors — 3-writer parallel proven;
# live 60k is emitter-blocked, synthetic PerfBaselineTest >=57.6k).
set -euo pipefail
ROOT="/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project_New"
JAR="$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
BRIDGE_DIR="$ROOT/code/02_services/01_ingestion/go-bridge"
MANIFEST="/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv"
OUT="$ROOT/logs/tracker-14/test-d-$(date +%Y%m%d-%H%M%S)"
DURATION_S="${TEST_D_DURATION_S:-300}"   # 5 min default
INTERVAL_S="${TEST_D_INTERVAL_S:-10}"
mkdir -p "$OUT" "$OUT/j1" "$OUT/j2" "$OUT/j3" "$OUT/bin"
export ARROW_APP_ID="testd" ARROW_APP_SECRET="testd" ARROW_FAKE_BROKER="1"
export TRANSPORT="proto"   # T6 proto path (NOT NDJSON pipe) — the low-latency transport
export ARROW_USER_ID="testd-user" ARROW_PASSWORD="testd-pass" ARROW_TOTP_KEY="JBSWY3DPEHPK3PXP"
export FLUSS_BOOTSTRAP="localhost:9123" FLUSS_BOOTSTRAP_SERVERS="localhost:9123"
export RAW_TABLE_NAME="raw_table_1"
export ARROW_MAX_EVENT_AGE_MS="5000" ARROW_MAX_FUTURE_EVENT_SKEW_MS="2000"
export ARROW_HFT_LATENCY_MS="50"
export CLOCK_CHECK_REQUIRED="false" OTEL_COLLECTOR_HOST="localhost:4318"
export FLUSS_WRITER_MODE="generic" FLUSS_WRITERS="1" FLUSS_WRITER_BATCH_SIZE_BYTES="0"
# Build faketool
echo "=== Test D start $(date -Iseconds) duration=${DURATION_S}s"
(cd "$BRIDGE_DIR" && go build -tags faketool -o "$OUT/bin/faketool" ./faketool)
# Start 3 faketools, one per connection (ports 8899/8900/8901), each serving its token third
FAKETOOL_PIDS=()
for port in 8899 8900 8901; do
  "$OUT/bin/faketool" -port $port -real-rate -real-rate-hz 20 > "$OUT/faketool-$port.log" 2>&1 &
  FAKETOOL_PIDS+=($!)
done
for port in 8899 8900 8901; do
  for _ in $(seq 1 30); do (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null && { exec 3>&-; break; }; sleep 1; done
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
  LOG_DIR="$R" READINESS_FILE_PATH="/tmp/ingestion.ready$i" \
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
  for _ in $(seq 1 60); do [ -f "/tmp/ingestion.ready$i" ] && { READY=1; break; }; sleep 2; done
  [ "$READY" = 1 ] || { echo "!! JVM $i not ready"; tail -5 "$OUT/j$i/java.out"; exit 1; }
done
echo "=== all 3 ready at $(date -Iseconds), running ${DURATION_S}s"
START_EPOCH=$(date +%s)
while [ $(( $(date +%s) - START_EPOCH )) -lt "$DURATION_S" ]; do sleep 10; done
echo "=== duration reached, draining at $(date -Iseconds)"
for pid in "${JVMS[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
for pid in "${JVMS[@]}"; do
  for _ in $(seq 1 120); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
  kill -9 "$pid" 2>/dev/null || true
done
for fpid in "${FAKETOOL_PIDS[@]}"; do kill -9 "$fpid" 2>/dev/null || true; done
echo "=== Test D done out=$OUT"
echo "--- raw-writer close lines ---"
for i in 1 2 3; do grep -oE "raw-writer: closed[^\"]*" "$OUT/j$i/journal/ingestion.json" 2>/dev/null | tail -1; done
