#!/usr/bin/env bash
# signal-candidates-table.sh -- operator view of the fired signal rows
# (the Signal_Candidates table the strategy host writes when a rule fires).
#
# Prints the newest --rows signals (newest detection time first) with
# symbol/rule/side/qty and the tf/trigger/range parsed out of the v2 audit
# JSON; --full prints the complete audit document, which carries the market
# snapshot the host handed the strategy at fire time.
#
#   signal-candidates-table.sh [--rows N] [--rule RULE_ID] [--full]
#
# Runs the probe inside the trading network (a Fluss client cannot reach the
# cluster from outside it). Needs the classpath file the build produces
# (code/02_services/01_ingestion/target/cp.txt) and a JDK on the host: the
# ingestion image is JRE-only, so compilation happens here and only java runs
# there. Read-only: the probe never writes to the table.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

IMAGE="${IMAGE:-01_docker-ingestion:latest}"
NETWORK="${NETWORK:-01_docker_trading-net}"
BOOTSTRAP="${BOOTSTRAP:-fluss-coordinator:9123}"
DATABASE="${DATABASE:-default}"
TABLE="${TABLE:-Signal_Candidates}"
ROWS="${ROWS:-20}"
RULE="${RULE:-}"
FULL=0
CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"
PROBE_NAME="SignalCandidatesViewer"
PROBE="$SCRIPT_DIR/fluss-probes/$PROBE_NAME.java"

usage() {
  echo "usage: signal-candidates-table.sh [--rows N] [--rule RULE_ID] [--full]" >&2
  echo "                                   [--database DB] [--table T] [--bootstrap host:port]" >&2
  echo "                                   [--image REF] [--network NAME]" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --rows)      [ $# -ge 2 ] || usage; ROWS="$2"; shift 2 ;;
    --rule)      [ $# -ge 2 ] || usage; RULE="$2"; shift 2 ;;
    --full)      FULL=1; shift ;;
    --database)  [ $# -ge 2 ] || usage; DATABASE="$2"; shift 2 ;;
    --table)     [ $# -ge 2 ] || usage; TABLE="$2"; shift 2 ;;
    --bootstrap) [ $# -ge 2 ] || usage; BOOTSTRAP="$2"; shift 2 ;;
    --image)     [ $# -ge 2 ] || usage; IMAGE="$2"; shift 2 ;;
    --network)   [ $# -ge 2 ] || usage; NETWORK="$2"; shift 2 ;;
    *) usage ;;
  esac
done

case "$ROWS" in
  ''|*[!0-9]*) echo "signal-candidates-table.sh: --rows must be a positive integer, got '$ROWS'" >&2; usage ;;
esac
[ "$ROWS" -ge 1 ] || { echo "signal-candidates-table.sh: --rows must be >= 1" >&2; usage; }

[ -f "$CP_FILE" ] || { echo "signal-candidates-table.sh: no $CP_FILE -- run 'make build' first" >&2; exit 3; }
command -v javac >/dev/null 2>&1 || { echo "signal-candidates-table.sh: javac not on PATH (the image is JRE-only; compile on the host)" >&2; exit 3; }

BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/signal-candidates-XXXXXX")"
mkdir -p "$BUILD_DIR"
# mktemp creates 0700, which the container's non-root user cannot traverse (see
# candle-features-table.sh). The mount is read-only, so 0755 is enough.
chmod 755 "$BUILD_DIR"
[ -f "$PROBE" ] || { echo "signal-candidates-table.sh: no such probe: $PROBE" >&2; exit 3; }
cp "$PROBE" "$BUILD_DIR/$PROBE_NAME.java"

javac -nowarn -cp "$(cat "$CP_FILE")" -d "$BUILD_DIR" "$BUILD_DIR/$PROBE_NAME.java" || {
  echo "signal-candidates-table.sh: compile failed" >&2
  rm -rf "$BUILD_DIR"
  exit 4
}

# Build argv without word-splitting: an optional --rule and --full slot in.
set -- --rows "$ROWS" --database "$DATABASE" --table "$TABLE" --bootstrap "$BOOTSTRAP"
if [ -n "$RULE" ]; then
  set -- "$@" --rule "$RULE"
fi
if [ "$FULL" -eq 1 ]; then
  set -- "$@" --full
fi

echo "SIGNAL-CANDIDATES-TABLE run rows=$ROWS database=$DATABASE table=$TABLE${RULE:+ rule=$RULE}"
docker run --rm --network "$NETWORK" -v "$BUILD_DIR:/tmp/probe:ro" \
  --entrypoint java "$IMAGE" --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp /tmp/probe:/app/ingestion.jar "$PROBE_NAME" "$@"
rc=$?
rm -rf "$BUILD_DIR"
exit "$rc"
