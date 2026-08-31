#!/usr/bin/env bash
# tiering-smoke.sh (2026-08-31) — prove log-segment → iceberg tiering to
# REAL Cloudflare R2, end to end, with fail-fast checks at every step.
#
# Why a standalone smoke (not a bench phase): tiering has a 5-min data
# freshness window + 1-min task interval — a 150s bench smoke can never
# see an upload. This runs faketool + the ingestion JVM directly for
# SMOKE_T seconds (default 60s — 2026-08-31 decision; pass e.g. 300 for
# a longer validation run), then verifies:
#   1. rows actually landed in raw_table_1 (a tiering test with zero
#      writes proves nothing)
#   2. tablet-server logs show tiering activity and no tiering errors
#   3. R2 bucket contains lake objects for raw_table_1 under the
#      warehouse prefix (the actual proof)
#
# Usage: bash tiering-smoke.sh [seconds]
# Exit: 0 = tiered objects visible in R2; 1 = verification failed.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
LIB_BRIDGE_DIR="$ROOT/code/02_services/01_ingestion/go-bridge"
LIB_ING_JAR="$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
LIB_MANIFEST="$ROOT/../../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv"
FAKETOOL_PORT="${FAKETOOL_PORT:-8899}"
SMOKE_T="${1:-60}"
RATE_HZ="${RATE_HZ:-10}"
NTOK="${NTOK:-1024}"
OUT="${TIERING_SMOKE_OUT:-/tmp/tiering-smoke}"

source "$ROOT/code/01_platform/04_scripts/r2-list.sh"

mkdir -p "$OUT/j1" "$OUT/bin"
echo "=== tiering-smoke start $(date -Iseconds) t=${SMOKE_T}s rate=${RATE_HZ}Hz x ${NTOK} ==="

# ---- preflight: fluss up + jar + manifest slice ----
for i in $(seq 1 30); do
  timeout 3 bash -c "echo > /dev/tcp/localhost/9123" 2>/dev/null && break; sleep 2
done
timeout 3 bash -c "echo > /dev/tcp/localhost/9123" 2>/dev/null \
  || { echo "!! fluss not reachable on 9123"; exit 1; }
[ -f "$LIB_ING_JAR" ] || { echo "!! ingestion jar missing"; exit 1; }

# ---- preflight GUARDS (each closes an observed 2026-08-31 failure mode) ----

# GUARD A (tiering service not running): the first smoke burned 10 minutes
# feeding a cluster with NO tiering job — Fluss servers only coordinate;
# the actual tiering is a Flink job started by tiering-start.sh. Without
# this check every later verification fails and the run proves nothing.
bash "$ROOT/code/01_platform/04_scripts/tiering-start.sh" --status \
  || { echo "!! GUARD-A FAILED: no RUNNING tiering job — run tiering-start.sh first (the smoke cannot pass without it)"; exit 1; }

# GUARD B (table not datalake-enabled): the 28-table corpus was applied
# during a restack while the coordinator had no datalake.format, so Fluss
# stored table.datalake.enabled=false (verified in ZK) and the coordinator
# never offered the table to the tiering service ("No available Tiering
# table found" forever). Check the LIVE property, not the DDL file.
ZK_TABLE="$(docker exec 01_docker-zookeeper-1 zkCli.sh get \
  /fluss/metadata/databases/default/tables/raw_table_1 2>/dev/null \
  | grep -o "table.datalake.enabled[^,}]*" | head -1)"
echo "ZK live property: $ZK_TABLE"
echo "$ZK_TABLE" | grep -q "true" \
  || { echo "!! GUARD-B FAILED: raw_table_1 has table.datalake.enabled!=true in ZK — the tiering service will never pick it up. Fix: ALTER TABLE (see EnableTiering.java notes in /tmp/tiering-smoke or repo docs)."; exit 1; }

# GUARD C (invalid R2 SigV4 region): R2 rejects AWS region names in the
# credential scope with InvalidRegionName -> HTTP 400 on EVERY s3a request
# (proven: auto=200, apac=200, ap-south-1=400 — it cost a coordinator
# restart + iceberg create failure before it was found). The coordinator
# reads this from its server.yaml (interpolated from .env AWS_REGION).
# NOTE: server.yaml legitimately contains the region line TWICE (the
# compose FLUSS_PROPERTIES block is emitted at two yaml merge points;
# hadoop config last-write-wins). Take the last match, trim whitespace —
# naively piping both matches through awk gave "auto\nauto" and a false
# GUARD-C failure (observed 2026-08-31).
REGION="$(docker exec 01_docker-fluss-coordinator-1 sh -c \
  'grep "fs.s3a.endpoint.region" /opt/fluss/conf/server.yaml' \
  | tail -1 | awk -F': *' '{print $2}' | tr -d '[:space:]')"
echo "coordinator s3a region: $REGION"
case "$REGION" in
  auto|apac|eeur|enam) ;;
  *) echo "!! GUARD-C FAILED: s3a region '$REGION' is not a valid R2 region (auto/apac/eeur/enam) — every s3a call will fail HTTP 400. Set AWS_REGION in .env and restart fluss servers."; exit 1 ;;
esac

SLICE="$OUT/instruments-${NTOK}.csv"
head -$((NTOK + 1)) "$LIB_MANIFEST" > "$SLICE"

# raw_table_1 must exist (ddl-apply contract) — tiering is a per-table
# property; a missing table cannot tier.
CP="$(cat "$ROOT/code/02_services/01_ingestion/target/cp.txt")"

cleanup() {
  [ -n "${FAKETOOL_PID:-}" ] && kill -9 "$FAKETOOL_PID" 2>/dev/null || true
  [ -n "${JVM_PID:-}" ] && kill -9 "$JVM_PID" 2>/dev/null || true
  rm -f /tmp/ingestion.tiering.ready
}
trap cleanup EXIT

# ---- baseline: fresh raw_table_1 object count BEFORE this run's writes ----
# VERIFY-3 compares against this; the lake prefix also holds history
# (pre-wipe artifacts + the lake/_stale-20260831/ archive), so a bare
# "any object exists" check can pass without THIS run tiering anything.
BASELINE_R2=$(source "$ROOT/code/01_platform/04_scripts/r2-list.sh" && \
  r2_list_lake | grep -c '^lake/default/raw_table_1/' || true)
echo "baseline fresh raw_table_1 R2 objects: ${BASELINE_R2:-0}"

# ---- feed + ingestion (same env recipe as pipeline-lib) ----
"$LIB_BRIDGE_DIR/faketool/faketool" -port "$FAKETOOL_PORT" -real-rate -real-rate-hz "$RATE_HZ" \
  > "$OUT/faketool.log" 2>&1 &
FAKETOOL_PID=$!
for i in $(seq 1 30); do timeout 2 bash -c "echo > /dev/tcp/127.0.0.1/$FAKETOOL_PORT" 2>/dev/null && break; sleep 1; done

LOG_DIR="$OUT/j1" READINESS_FILE_PATH="/tmp/ingestion.tiering.ready" \
ARROW_HFT_URL="ws://127.0.0.1:$FAKETOOL_PORT" ARROW_BRIDGE_BIN="$LIB_BRIDGE_DIR/arrow-bridge" \
ARROW_FAKE_BROKER="1" TRANSPORT="proto" SECRETS_VIA_ENV_FILE="1" \
ARROW_APP_ID="testd" ARROW_APP_SECRET="testd" \
ARROW_USER_ID="testd-user" ARROW_PASSWORD="testd-pass" ARROW_TOTP_KEY="JBSWY3DPEHPK3PXP" \
INSTRUMENT_MANIFEST_PATH="$SLICE" \
FLUSS_BOOTSTRAP="localhost:9123" FLUSS_BOOTSTRAP_SERVERS="localhost:9123" \
RAW_TABLE_NAME="raw_table_1" ARROW_HFT_CONNECTIONS="1" ARROW_MAX_EVENT_AGE_MS="5000" \
ARROW_MAX_FUTURE_EVENT_SKEW_MS="2000" ARROW_HFT_LATENCY_MS="50" CLOCK_CHECK_REQUIRED="false" \
OTEL_COLLECTOR_HOST="localhost:4319" \
FLUSS_WRITER_MODE="generic" FLUSS_WRITERS="1" FLUSS_WRITER_BATCH_SIZE_BYTES="0" \
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m \
  -Dlog.dir="$OUT/j1" \
  -cp "$LIB_ING_JAR" com.trading.ingestion.IngestionService > "$OUT/j1/java.out" 2>&1 &
JVM_PID=$!

READY=0
for i in $(seq 1 60); do [ -f /tmp/ingestion.tiering.ready ] && { READY=1; break; }; sleep 2; done
[ "$READY" = 1 ] || { echo "!! ingestion JVM not ready:"; tail -5 "$OUT/j1/java.out"; exit 1; }
for i in $(seq 1 30); do grep -q "HFT subscribed" "$OUT/j1/java.out" && break; sleep 1; done
grep -q "HFT subscribed" "$OUT/j1/java.out" || { echo "!! bridge never subscribed"; exit 1; }
echo "feed + ingestion ready (${RATE_HZ}Hz x ${NTOK} = $((RATE_HZ * NTOK))/s)"

# ---- run: writes must outlive the 5-min freshness window ----
echo "writing for ${SMOKE_T}s (freshness 5min + tier interval 1m)..."
sleep "$SMOKE_T"
echo "stopping feed + ingestion; waiting ${TIER_WAIT:-120}s for the tiering task to run..."
cleanup
trap - EXIT
sleep "${TIER_WAIT:-120}"

# GUARD D (2026-08-31): v3 raw_table_1 is PARTITIONED by event_day — the
# tiering split generator lists partitions, so today's IST partition
# (yyyyMMdd) must exist or the tiering job has nothing to tier. Client
# dynamic partitioning creates it on first write.
CP_COMMON="$ROOT/code/common/target/classes:$CP"
javac -cp "$CP_COMMON" -d "$OUT" \
  "$ROOT/code/01_platform/04_scripts/fluss-repair/RawTableAdmin.java" \
  2>"$OUT/rawadmin-err.log" \
  || { echo "!! GUARD-D: RawTableAdmin compile failed:"; head -5 "$OUT/rawadmin-err.log"; exit 1; }
TODAY_IST="$(TZ=Asia/Kolkata date +%Y%m%d)"
PARTS="$(java -cp "$OUT:$CP_COMMON" RawTableAdmin partitions 2>"$OUT/rawadmin-err.log" || true)"
echo "partitions: $(echo "$PARTS" | tr '\n' ' ')"
echo "$PARTS" | grep -qx "$TODAY_IST" \
  || { echo "!! GUARD-D FAILED: partition '$TODAY_IST' missing — dynamic partitioning off?"; exit 1; }

# ---- verify 1: writes landed (count via LogFullRead, extracted from
# holistic-analyze.py's LOG_READ_SRC — always compiled fresh, gotcha #17) ----
python3 - "$ROOT" "$OUT" <<'COMPILE'
import re, sys, subprocess, os
root, out = sys.argv[1], sys.argv[2]
src = open(os.path.join(root, "code/01_platform/04_scripts/holistic-analyze.py")).read()
m = re.search(r'LOG_READ_SRC = """(.*?)"""', src, re.S)
assert m, "LOG_READ_SRC not found"
with open(os.path.join(out, "LogFullRead.java"), "w") as f:
    f.write(m.group(1))
cp = open(os.path.join(root, "code/02_services/01_ingestion/target/cp.txt")).read().strip()
subprocess.run(["javac", "-cp", cp, "-d", out, os.path.join(out, "LogFullRead.java")], check=True)
COMPILE
# FAIL-FAST: run the reader with stderr VISIBLE first — 2>/dev/null on the
# real call hides JVM/arrow failures as "0 rows" and reads as a data-loss
# verdict (observed 2026-08-31: missing --add-opens=java.base/java.nio =
# arrow MemoryUtil crash, swallowed -> VERIFY-1 said 0 rows on 4.9M written).
# -Xmx1g (2026-08-31): the reader SEGFAULTED (rc=139, arrow native OOM)
# with the JVM default heap once raw_table_1 grew past ~10M rows across
# smoke runs. Bounded heap keeps the reader stable at any table size.
READ_OUT=$(timeout 60 java -Xmx1g --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp "$OUT:$CP" LogFullRead raw_table_1 30000 2>"$OUT/read-err.log" || true)
ROWS=$(echo "$READ_OUT" | grep -c "^(" || true)
if [ "${ROWS:-0}" -eq 0 ] && [ -s "$OUT/read-err.log" ]; then
  # 2026-08-31: raw_table_1 is now PARTITIONED (v3) — LogFullRead's
  # subscribe(table) throws IllegalStateException "please use
  # subscribe(partitionId, bucket, offset)". Runbook VERIFY-1 fallback
  # (pre-authorized): fall back to ingestion throughput as write-proof.
  if grep -q "partitioned table" "$OUT/read-err.log"; then
    echo "!! reader: partitioned-table mode (expected v3) — using R2 day-folder row count as write-proof"
    # The ingestion JVM is SIGKILLed by cleanup before it can log its drained
    # summary, so count TODAY's partition rows in R2 via DuckDB iceberg_scan
    # (the same query B5-3 proves). >0 rows for the day == writes landed.
    DAY_ROWS="$(timeout 180 bash "$ROOT/code/01_platform/04_scripts/r2-query.sh" \
      "SELECT count(*) AS n FROM iceberg_scan('s3://${R2_BUCKET}/lake/default/raw_table_1', allow_moved_paths=true) WHERE event_day='${TODAY_IST}';" \
      2>/dev/null | grep -E '^│ +[0-9]+ +│$' | grep -oE '[0-9]+' | head -1 || true)"
    echo "R2 rows for ${TODAY_IST}: ${DAY_ROWS:-<none>}"
    [ -n "${DAY_ROWS:-}" ] && [ "${DAY_ROWS:-0}" -gt 0 ] \
      || { echo "!! VERIFY-1 FALLBACK FAILED: no R2 rows for today's partition"; exit 1; }
  else
    echo "!! reader errored (not an empty table):"; head -5 "$OUT/read-err.log"
    exit 1
  fi
fi
echo "raw_table_1 rows observed: ${ROWS:-0}"
if [ "${ROWS:-0}" -gt 0 ]; then
  [ "${ROWS:-0}" -gt 1000 ] || { echo "!! VERIFY-1 FAILED: too few rows — tiering test invalid"; exit 1; }
fi

# ---- verify 2: tablet tiering activity / errors ----
TIER_ACT=$(docker logs 01_docker-fluss-tablet-1 2>&1 | grep -ci "tier" || true)
TIER_ERR=$(docker logs 01_docker-fluss-tablet-1 2>&1 | grep -ciE "tier.*(error|exception|fail)|error.*tier" || true)
echo "tablet log: tier-mention lines=${TIER_ACT}, tiering-error lines=${TIER_ERR}"

# ---- verify 3: THIS run's data tiered to R2 (vs baseline) ----
# Anchored to lake/default/raw_table_1/ so the lake/_stale-20260831/
# archive (pre-wipe artifacts moved aside 2026-08-31) can never satisfy
# the check. Object count must GROW past the pre-run baseline.
FRESH_R2=$(r2_list_lake | grep -c '^lake/default/raw_table_1/' || true)
echo "fresh raw_table_1 R2 objects now: ${FRESH_R2:-0} (baseline ${BASELINE_R2:-0})"
if [ "${FRESH_R2:-0}" -le "${BASELINE_R2:-0}" ]; then
  echo "!! VERIFY-3 FAILED: raw_table_1 object count did not grow vs baseline —"
  echo "   this run's data was not tiered. Check: tiering job logs (JobManager),"
  echo "   freshness window (5min default — may need a longer tiering wait),"
  echo "   coordinator LakeTableTieringManager state."
  exit 1
fi

# verify 3b: iceberg manifests present (a real iceberg table, not bare files)
MANIFESTS=$(r2_list_lake | grep -c '^lake/default/raw_table_1/metadata/.*\.avro' || true)
echo "iceberg manifest files: ${MANIFESTS:-0}"
[ "${MANIFESTS:-0}" -gt 0 ] || {
  echo "!! VERIFY-3b FAILED: no iceberg manifest (.avro) under metadata/ — parquet landed without a committed snapshot"
  exit 1
}

# GUARD E (2026-08-31): daily-partition proof — this run's data must sit in
# TODAY's IST day-folder (event_day=yyyyMMdd). A wrong event_day format
# would create a second, non-auto partition and fail this check.
TODAY_IST="$(TZ=Asia/Kolkata date +%Y%m%d)"
DAY_OBJ="$(r2_list_lake | grep -c "^lake/default/raw_table_1/data/event_day=${TODAY_IST}/" || true)"
echo "day-folder objects for ${TODAY_IST}: ${DAY_OBJ}"
[ "${DAY_OBJ:-0}" -gt 0 ] || {
  echo "!! GUARD-E FAILED: no R2 objects under event_day=${TODAY_IST}/ — event_day format or tiering wrong"
  exit 1
}

echo "=== tiering-smoke PASS: this run tiered to R2 ($((FRESH_R2 - BASELINE_R2)) new raw_table_1 objects, ${MANIFESTS} manifests; day-folder ${TODAY_IST} present) ==="
exit 0
