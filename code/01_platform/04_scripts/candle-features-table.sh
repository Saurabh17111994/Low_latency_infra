#!/usr/bin/env bash
# candle-features-table.sh -- dev scratch table for the merged candle+feature smoke
# (Wave B/DEC-059, CHG-474).
#
# Same operator-approved scratch route CHG-350 used for feature_values: this creates the
# table with the identical DDL 35 (proposal) shape instead of applying the DDL. DDL 35
# stays an unapplied proposal (see the DDL hazard in AGENTS.md). Modes:
#
#   candle-features-table.sh --mode create         create if absent + describe
#   candle-features-table.sh --mode describe       columns, PK, buckets, properties
#   candle-features-table.sh --mode tail --rows 20 scan + print the first rows
#   candle-features-table.sh --mode upsert-check   same-PK write twice, read back (last wins)
#
# Runs the probe inside the trading network (a Fluss client cannot reach the cluster from
# outside it). Needs the classpath file the build produces
# (code/02_services/01_ingestion/target/cp.txt) and a JDK on the host: the ingestion image
# is JRE-only, so compilation happens here and only java runs there.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

IMAGE="${IMAGE:-01_docker-ingestion:latest}"
NETWORK="${NETWORK:-01_docker_trading-net}"
BOOTSTRAP="${BOOTSTRAP:-fluss-coordinator:9123}"
MODE="${MODE:-describe}"
DATABASE="${DATABASE:-default}"
TABLE="${TABLE:-candle_features}"
ROWS="${ROWS:-20}"
CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"
PROBE_NAME="CandleFeaturesTableProbe"
PROBE="$SCRIPT_DIR/fluss-probes/$PROBE_NAME.java"

usage() {
  echo "usage: candle-features-table.sh [--mode create|describe|tail|upsert-check]" >&2
  echo "                              [--rows N] [--database DB] [--table T]" >&2
  echo "                              [--bootstrap host:port] [--image REF] [--network NAME]" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --mode)      [ $# -ge 2 ] || usage; MODE="$2"; shift 2 ;;
    --rows)      [ $# -ge 2 ] || usage; ROWS="$2"; shift 2 ;;
    --database)  [ $# -ge 2 ] || usage; DATABASE="$2"; shift 2 ;;
    --table)     [ $# -ge 2 ] || usage; TABLE="$2"; shift 2 ;;
    --bootstrap) [ $# -ge 2 ] || usage; BOOTSTRAP="$2"; shift 2 ;;
    --image)     [ $# -ge 2 ] || usage; IMAGE="$2"; shift 2 ;;
    --network)   [ $# -ge 2 ] || usage; NETWORK="$2"; shift 2 ;;
    *) usage ;;
  esac
done

case "$MODE" in
  create|describe|tail|upsert-check) ;;
  *) echo "candle-features-table.sh: unknown --mode '$MODE'" >&2; usage ;;
esac
case "$ROWS" in
  ''|*[!0-9]*) echo "candle-features-table.sh: --rows must be a positive integer, got '$ROWS'" >&2; usage ;;
esac

[ -f "$CP_FILE" ] || { echo "candle-features-table.sh: no $CP_FILE -- run 'make build' first" >&2; exit 3; }
command -v javac >/dev/null 2>&1 || { echo "candle-features-table.sh: javac not on PATH (the image is JRE-only; compile on the host)" >&2; exit 3; }

BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/candle-features-XXXXXX")"
mkdir -p "$BUILD_DIR"
# mktemp creates 0700, which the container's non-root user cannot traverse (see
# feature-spike.sh). The mount is read-only, so 0755 is enough.
chmod 755 "$BUILD_DIR"
[ -f "$PROBE" ] || { echo "candle-features-table.sh: no such probe: $PROBE" >&2; exit 3; }
cp "$PROBE" "$BUILD_DIR/$PROBE_NAME.java"

javac -nowarn -cp "$(cat "$CP_FILE")" -d "$BUILD_DIR" "$BUILD_DIR/$PROBE_NAME.java" || {
  echo "candle-features-table.sh: compile failed" >&2
  rm -rf "$BUILD_DIR"
  exit 4
}

echo "CANDLE-FEATURES-TABLE run mode=$MODE database=$DATABASE table=$TABLE"
docker run --rm --network "$NETWORK" -v "$BUILD_DIR:/tmp/probe:ro" \
  --entrypoint java "$IMAGE" --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp /tmp/probe:/app/ingestion.jar "$PROBE_NAME" \
  --mode "$MODE" --database "$DATABASE" --table "$TABLE" --rows "$ROWS" --bootstrap "$BOOTSTRAP"
rc=$?
rm -rf "$BUILD_DIR"
exit "$rc"
