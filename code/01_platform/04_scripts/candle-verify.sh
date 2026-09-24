#!/usr/bin/env bash
# candle-verify.sh -- re-measure the candle late-tick loss on the live cluster.
#
# Independent of the signal job: it accumulates raw_table_1 per (token, window)
# itself and compares that against the candle rows the job wrote, so it does not
# trust the job's own counters. Reports matched / mismatched / absent windows, the
# OHLC bias counts, and how many absent windows had no trade row at all (which is
# the benign explanation for a missing candle).
#
#   candle-verify.sh --minutes 3 --timeframe ONE_M
#
# Runs the probe inside the trading network, because a Fluss client cannot reach
# the cluster from outside it. Needs the classpath file the build produces
# (code/02_services/01_ingestion/target/cp.txt) and a JDK on the host: the
# ingestion image is JRE-only, so compilation happens here and only java runs there.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

IMAGE="${IMAGE:-01_docker-ingestion:latest}"
# Compose names it <project>_<network>: project = the compose directory name,
# network = trading-net as defined in code/01_platform/01_docker/docker-compose.yml.
NETWORK="${NETWORK:-01_docker_trading-net}"
BOOTSTRAP="${BOOTSTRAP:-fluss-coordinator:9123}"
CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"
PROBE_NAME="CandleVerify"
PROBE="$SCRIPT_DIR/fluss-probes/$PROBE_NAME.java"
BUILD_DIR="${BUILD_DIR:-}"
MINUTES=""
TIMEFRAME="ONE_M"

usage() {
  echo "usage: candle-verify.sh --minutes <n> [--timeframe TF] [--bootstrap host:port] [--image REF] [--network NAME] [--probe CLASS]" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --minutes)   [ $# -ge 2 ] || usage; MINUTES="$2"; shift 2 ;;
    --timeframe) [ $# -ge 2 ] || usage; TIMEFRAME="$2"; shift 2 ;;
    --probe)     [ $# -ge 2 ] || usage; PROBE_NAME="$2"; PROBE="$SCRIPT_DIR/fluss-probes/$PROBE_NAME.java"; shift 2 ;;
    --bootstrap) [ $# -ge 2 ] || usage; BOOTSTRAP="$2"; shift 2 ;;
    --image)     [ $# -ge 2 ] || usage; IMAGE="$2"; shift 2 ;;
    --network)   [ $# -ge 2 ] || usage; NETWORK="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[ -n "$MINUTES" ] || usage
case "$MINUTES" in
  ''|*[!0-9]*) usage ;;
esac
[ -f "$CP_FILE" ] || { echo "candle-verify.sh: no $CP_FILE -- run 'make build' first" >&2; exit 3; }
command -v javac >/dev/null 2>&1 || { echo "candle-verify.sh: javac not on PATH (the image is JRE-only; compile on the host)" >&2; exit 3; }

own_build_dir=0
if [ -z "$BUILD_DIR" ]; then
  BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/candle-verify-XXXXXX")"
  own_build_dir=1
fi
mkdir -p "$BUILD_DIR"
# mktemp creates 0700, which the container's non-root user cannot traverse, and the
# container then fails with "ClassNotFoundException: CandleVerify" - a confusing
# symptom that costs a whole run. The mount is read-only, so 0755 is enough.
chmod 755 "$BUILD_DIR"
[ -f "$PROBE" ] || { echo "candle-verify.sh: no such probe: $PROBE" >&2; exit 3; }
cp "$PROBE" "$BUILD_DIR/$PROBE_NAME.java"

javac -nowarn -cp "$(cat "$CP_FILE")" -d "$BUILD_DIR" "$BUILD_DIR/$PROBE_NAME.java"
if [ $? -ne 0 ]; then
  echo "candle-verify.sh: compile failed" >&2
  [ "$own_build_dir" -eq 1 ] && rm -rf "$BUILD_DIR"
  exit 4
fi

# Each probe takes its own argv: CandleVerify wants a timeframe, EventDayProbe does
# not. Built with set --/"$@" rather than a joined string, so no argument ever depends
# on word-splitting.
if [ "$PROBE_NAME" = "CandleVerify" ]; then
  set -- "$MINUTES" "$TIMEFRAME"
else
  set -- "$MINUTES"
fi

# No exec here: the temp dir has to be cleaned up after docker returns.
docker run --rm --network "$NETWORK" -v "$BUILD_DIR:/tmp/probe:ro" \
  --entrypoint java "$IMAGE" --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp /tmp/probe:/app/ingestion.jar "$PROBE_NAME" "$@" --bootstrap "$BOOTSTRAP"
rc=$?
[ "$own_build_dir" -eq 1 ] && rm -rf "$BUILD_DIR"
exit "$rc"
