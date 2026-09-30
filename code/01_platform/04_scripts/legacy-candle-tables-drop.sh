#!/usr/bin/env bash
# legacy-candle-tables-drop.sh -- dev drop for the three retired candle/feature tables
# after the Wave C cutover (CHG-481).
#
# W-C6 proved the tables receive zero rows while the merged writer runs; this script removes
# them from the dev cluster. The target set is FIXED in the probe (candle_live,
# candle_closed, feature_values) -- never parameterized here, so a mistyped name cannot drop
# a live table. Modes:
#
#   legacy-candle-tables-drop.sh --mode list              present/absent per target
#   legacy-candle-tables-drop.sh --mode drop --confirm    drop present targets + read back
#
# A drop without --confirm fails here (exit 3) before any container starts, and the probe
# refuses independently. Runs the probe inside the trading network (a Fluss client cannot
# reach the cluster from outside it). Needs the classpath file the build produces
# (code/02_services/01_ingestion/target/cp.txt) and a JDK on the host: the ingestion image
# is JRE-only, so compilation happens here and only java runs there.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

IMAGE="${IMAGE:-01_docker-ingestion:latest}"
NETWORK="${NETWORK:-01_docker_trading-net}"
BOOTSTRAP="${BOOTSTRAP:-fluss-coordinator:9123}"
MODE="${MODE:-list}"
DATABASE="${DATABASE:-default}"
CONFIRM=0
CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"
PROBE_NAME="LegacyTableDropProbe"
PROBE="$SCRIPT_DIR/fluss-probes/$PROBE_NAME.java"

usage() {
  echo "usage: legacy-candle-tables-drop.sh [--mode list|drop] [--confirm]" >&2
  echo "                                   [--database DB] [--bootstrap host:port]" >&2
  echo "                                   [--image REF] [--network NAME]" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --mode)      [ $# -ge 2 ] || usage; MODE="$2"; shift 2 ;;
    --confirm)   CONFIRM=1; shift ;;
    --database)  [ $# -ge 2 ] || usage; DATABASE="$2"; shift 2 ;;
    --bootstrap) [ $# -ge 2 ] || usage; BOOTSTRAP="$2"; shift 2 ;;
    --image)     [ $# -ge 2 ] || usage; IMAGE="$2"; shift 2 ;;
    --network)   [ $# -ge 2 ] || usage; NETWORK="$2"; shift 2 ;;
    *) usage ;;
  esac
done

case "$MODE" in
  list|drop) ;;
  *) echo "legacy-candle-tables-drop.sh: unknown --mode '$MODE'" >&2; usage ;;
esac
if [ "$MODE" = "drop" ] && [ "$CONFIRM" != "1" ]; then
  echo "DROP-REFUSED: --mode drop requires --confirm (removes candle_live, candle_closed, feature_values)" >&2
  exit 3
fi

[ -f "$CP_FILE" ] || { echo "legacy-candle-tables-drop.sh: no $CP_FILE -- run 'make build' first" >&2; exit 3; }
command -v javac >/dev/null 2>&1 || { echo "legacy-candle-tables-drop.sh: javac not on PATH (the image is JRE-only; compile on the host)" >&2; exit 3; }

BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/legacy-drop-XXXXXX")"
mkdir -p "$BUILD_DIR"
# mktemp creates 0700, which the container's non-root user cannot traverse (see
# feature-spike.sh). The mount is read-only, so 0755 is enough.
chmod 755 "$BUILD_DIR"
[ -f "$PROBE" ] || { echo "legacy-candle-tables-drop.sh: no such probe: $PROBE" >&2; exit 3; }
cp "$PROBE" "$BUILD_DIR/$PROBE_NAME.java"

javac -nowarn -cp "$(cat "$CP_FILE")" -d "$BUILD_DIR" "$BUILD_DIR/$PROBE_NAME.java" || {
  echo "legacy-candle-tables-drop.sh: compile failed" >&2
  rm -rf "$BUILD_DIR"
  exit 4
}

echo "LEGACY-CANDLE-TABLES-DROP run mode=$MODE database=$DATABASE confirm=$CONFIRM"
PROBE_ARGS=(--mode "$MODE" --database "$DATABASE" --bootstrap "$BOOTSTRAP")
[ "$CONFIRM" = "1" ] && PROBE_ARGS+=(--confirm)
docker run --rm --network "$NETWORK" -v "$BUILD_DIR:/tmp/probe:ro" \
  --entrypoint java "$IMAGE" --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp /tmp/probe:/app/ingestion.jar "$PROBE_NAME" "${PROBE_ARGS[@]}"
rc=$?
rm -rf "$BUILD_DIR"
exit "$rc"
