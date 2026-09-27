#!/usr/bin/env bash
# feature-spike.sh -- MAP-vs-JSON storage spike for the stored feature layer (DEC-056).
#
# Writes the same feature payload three ways (MAP<STRING,DOUBLE>, MAP<INT,DOUBLE>,
# JSON string) into scratch KV tables, then measures the server-owned bytes per table
# from the tablet's data dir. The probe reports write/lookup/decode percentiles and
# round-trip correctness on stdout; this runner adds the byte lines and optional cleanup.
#
#   feature-spike.sh --windows 4 --features 64 --reads 1000 [--cleanup]
#
# Runs the probe inside the trading network (a Fluss client cannot reach the cluster from
# outside it) and reads the table data dir from the tablet container. Needs the classpath
# file the build produces (code/02_services/01_ingestion/target/cp.txt) and a JDK on the
# host: the ingestion image is JRE-only, so compilation happens here and only java runs there.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

IMAGE="${IMAGE:-01_docker-ingestion:latest}"
# Compose names it <project>_<network>: project = the compose directory name,
# network = trading-net as defined in code/01_platform/01_docker/docker-compose.yml.
NETWORK="${NETWORK:-01_docker_trading-net}"
TABLET="${TABLET:-01_docker-fluss-tablet-1}"
BOOTSTRAP="${BOOTSTRAP:-fluss-coordinator:9123}"
# Bytes-stability poll interval; the stub-docker test sets 0 to stay fast.
POLL_SECS="${POLL_SECS:-2}"
CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"
PROBE_NAME="FeatureSpikeProbe"
PROBE="$SCRIPT_DIR/fluss-probes/$PROBE_NAME.java"

# Spike defaults: 2433 instruments x 6 timeframes x 4 windows = 58 392 rows per variant.
WINDOWS=4
FEATURES=64
READS=1000
CLEANUP=0
# 6 timeframes per the Timeframe enum; instruments per docs/ENVIRONMENT.md (NSE cash universe).
TFS=6
INSTRUMENTS=2433

usage() {
  echo "usage: feature-spike.sh [--windows N] [--features N] [--reads N] [--cleanup]" >&2
  echo "                       [--bootstrap host:port] [--image REF] [--network NAME] [--tablet NAME]" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --windows)   [ $# -ge 2 ] || usage; WINDOWS="$2"; shift 2 ;;
    --features)  [ $# -ge 2 ] || usage; FEATURES="$2"; shift 2 ;;
    --reads)     [ $# -ge 2 ] || usage; READS="$2"; shift 2 ;;
    --cleanup)   CLEANUP=1; shift ;;
    --bootstrap) [ $# -ge 2 ] || usage; BOOTSTRAP="$2"; shift 2 ;;
    --image)     [ $# -ge 2 ] || usage; IMAGE="$2"; shift 2 ;;
    --network)   [ $# -ge 2 ] || usage; NETWORK="$2"; shift 2 ;;
    --tablet)    [ $# -ge 2 ] || usage; TABLET="$2"; shift 2 ;;
    *) usage ;;
  esac
done

for pair in "windows:$WINDOWS" "features:$FEATURES" "reads:$READS"; do
  name="${pair%%:*}"
  value="${pair#*:}"
  case "$value" in
    ''|*[!0-9]*) echo "feature-spike.sh: --$name must be a positive integer, got '$value'" >&2; usage ;;
  esac
  [ "$value" -gt 0 ] || { echo "feature-spike.sh: --$name must be positive" >&2; usage; }
done

[ -f "$CP_FILE" ] || { echo "feature-spike.sh: no $CP_FILE -- run 'make build' first" >&2; exit 3; }
command -v javac >/dev/null 2>&1 || { echo "feature-spike.sh: javac not on PATH (the image is JRE-only; compile on the host)" >&2; exit 3; }

BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/feature-spike-XXXXXX")"
mkdir -p "$BUILD_DIR"
# mktemp creates 0700, which the container's non-root user cannot traverse, and the
# container then fails with "ClassNotFoundException: FeatureSpikeProbe" - the same trap
# candle-verify.sh documents. The mount is read-only, so 0755 is enough.
chmod 755 "$BUILD_DIR"
[ -f "$PROBE" ] || { echo "feature-spike.sh: no such probe: $PROBE" >&2; exit 3; }
cp "$PROBE" "$BUILD_DIR/$PROBE_NAME.java"

javac -nowarn -cp "$(cat "$CP_FILE")" -d "$BUILD_DIR" "$BUILD_DIR/$PROBE_NAME.java"
if [ $? -ne 0 ]; then
  echo "feature-spike.sh: compile failed" >&2
  rm -rf "$BUILD_DIR"
  exit 4
fi

echo "SPIKE run windows=$WINDOWS features=$FEATURES reads=$READS rows_per_variant=$((WINDOWS * TFS * INSTRUMENTS))"
docker run --rm --network "$NETWORK" -v "$BUILD_DIR:/tmp/probe:ro" \
  --entrypoint java "$IMAGE" --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp /tmp/probe:/app/ingestion.jar "$PROBE_NAME" \
  --windows "$WINDOWS" --features "$FEATURES" --reads "$READS" --bootstrap "$BOOTSTRAP"
rc=$?

if [ "$rc" -eq 0 ]; then
  ROWS=$((WINDOWS * TFS * INSTRUMENTS))
  for variant in map_str map_int json array; do
    dir="$(docker exec "$TABLET" sh -c "ls -d /tmp/fluss/data/default/zz_feature_spike_${variant}-* 2>/dev/null | head -1")"
    if [ -z "$dir" ]; then
      echo "feature-spike.sh: no data dir found for $variant in $TABLET - bytes not measured" >&2
      rc=5
      continue
    fi
    # The writer acks before the segment flush lands, so read until the size stops
    # growing (two consecutive equal readings) instead of sleeping a fixed guess.
    prev=-1
    total_bytes=-1
    attempt=0
    while [ "$attempt" -lt 20 ]; do
      total_bytes="$(docker exec "$TABLET" du -sb "$dir" | cut -f1)"
      [ "$total_bytes" = "$prev" ] && break
      prev="$total_bytes"
      attempt=$((attempt + 1))
      sleep "$POLL_SECS"
    done
    # Per-bucket .index/.timeindex files are preallocated to a fixed 10 MiB each
    # (16 buckets => ~320 MiB per table), so the raw dir size says nothing about the
    # payload. The .log files carry the encoded rows; the kv-* dirs carry the state.
    log_bytes="$(docker exec "$TABLET" find "$dir" -name '*.log' -type f -printf '%s\n' | awk '{s+=$1} END {print s+0}')"
    index_bytes="$(docker exec "$TABLET" find "$dir" -type f \( -name '*.index' -o -name '*.timeindex' \) -printf '%s\n' | awk '{s+=$1} END {print s+0}')"
    kv_bytes="$(docker exec "$TABLET" sh -c "du -sb $dir/kv-* 2>/dev/null" | awk '{s+=$1} END {print s+0}')"
    log_bpr="$(awk -v b="$log_bytes" -v r="$ROWS" 'BEGIN { printf "%.0f", b / r }')"
    kv_bpr="$(awk -v b="$kv_bytes" -v r="$ROWS" 'BEGIN { printf "%.0f", b / r }')"
    echo "SPIKE bytes variant=$variant log_bytes=$log_bytes bytes_per_row=$log_bpr kv_bytes=$kv_bytes kv_bytes_per_row=$kv_bpr index_bytes=$index_bytes total_bytes=$total_bytes rows=$ROWS dir=$dir"
  done
fi

if [ "$CLEANUP" = 1 ] && [ "$rc" -eq 0 ]; then
  docker run --rm --network "$NETWORK" -v "$BUILD_DIR:/tmp/probe:ro" \
    --entrypoint java "$IMAGE" --add-opens=java.base/java.nio=ALL-UNNAMED \
    -cp /tmp/probe:/app/ingestion.jar "$PROBE_NAME" drop --bootstrap "$BOOTSTRAP"
fi

rm -rf "$BUILD_DIR"
exit "$rc"
