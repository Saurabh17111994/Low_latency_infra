#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
# P6-696: the bound is TickTableViewer.MAX_LIMIT — a value the viewer accepts is
# not refused here, and one it rejects is not forwarded just to fail in Java.
# tests/test_ticks_and_smoke_wave23.py asserts the two numbers stay equal.
MAX_LIMIT=10000
# P6-297: bound the probe so an unroutable host cannot hang for the TCP SYN
# timeout; overridable so a test does not have to wait out the default.
PROBE_TIMEOUT_SEC="${PROBE_TIMEOUT_SEC:-3}"

# P6-695: extra arguments are a caller typo, not something to ignore.
if [ "$#" -gt 1 ]; then
	printf 'Usage: %s [positive-row-limit <= %s]\n' "$0" "$MAX_LIMIT" >&2
	exit 2
fi
LIMIT="${1:-20}"

# P6-697: a missing JDK is not a cluster problem — say so before the probe, so
# the user never waits on the network to be told the wrong thing.
if ! command -v java >/dev/null 2>&1; then
	printf 'java not found in PATH (the viewer needs Java 17+ for --add-opens).\n' >&2
	exit 1
fi

if [ ! -f "$JAR" ]; then
	printf 'Missing ingestion JAR. Build it first with:\n  cd %s/code && mvn -pl 02_services/01_ingestion -am package\n' "$ROOT" >&2
	exit 1
fi

if [[ ! "$LIMIT" =~ ^[1-9][0-9]*$ ]] || [ "$LIMIT" -gt "$MAX_LIMIT" ]; then
	printf 'Usage: %s [positive-row-limit <= %s]\n' "$0" "$MAX_LIMIT" >&2
	exit 2
fi

# R-273 / P6-296: probe the same endpoint the viewer dials. The %:* / ##*:
# split turned "myhost" into HOST=myhost PORT=myhost, "[::1]:9123" into HOST="["
# and a comma list into a truncated host, so pre-flight could pass or fail on a
# different endpoint than Java got. Refuse the forms this probe cannot parse.
FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
case "$FLUSS_BOOTSTRAP" in
	*,*)
		printf 'Multiple bootstrap servers are not supported by the pre-flight probe: %s\n' "$FLUSS_BOOTSTRAP" >&2
		printf 'Pass the one endpoint the viewer should use, e.g. FLUSS_BOOTSTRAP=host:9123 %s\n' "$0" >&2
		exit 2
		;;
	*://*)
		printf 'Bootstrap must be host:port without a scheme: %s\n' "$FLUSS_BOOTSTRAP" >&2
		exit 2
		;;
esac
case "$FLUSS_BOOTSTRAP" in
	\[*\]:*) # IPv6 literal, e.g. [::1]:9123
		FLUSS_HOST="${FLUSS_BOOTSTRAP%]:*}"
		FLUSS_HOST="${FLUSS_HOST#[}"
		FLUSS_PORT="${FLUSS_BOOTSTRAP##*]:}"
		;;
	*:*)
		FLUSS_HOST="${FLUSS_BOOTSTRAP%:*}"
		FLUSS_PORT="${FLUSS_BOOTSTRAP##*:}"
		;;
	*)
		printf 'Bootstrap must be host:port — no port found in %s\n' "$FLUSS_BOOTSTRAP" >&2
		exit 2
		;;
esac
if [[ ! "$FLUSS_PORT" =~ ^[0-9]+$ ]] || [ "$FLUSS_PORT" -lt 1 ] || [ "$FLUSS_PORT" -gt 65535 ]; then
	printf 'Bootstrap port must be a number in 1-65535: %s\n' "$FLUSS_BOOTSTRAP" >&2
	exit 2
fi

# P6-297: this proves TCP reachability only. Metadata, table and auth failures
# still surface in Java, and Fluss can stop between this probe and `exec` — the
# probe shortens the path to a clear message, it does not remove the race.
# Keep the reason: the old form threw it away with 2>/dev/null.
if ! probe_err="$(timeout "$PROBE_TIMEOUT_SEC" bash -c 'exec 3<>/dev/tcp/"$0"/"$1"' "$FLUSS_HOST" "$FLUSS_PORT" 2>&1 >/dev/null)"; then
	printf 'TCP probe failed for %s: %s\n' "$FLUSS_BOOTSTRAP" "${probe_err:-no diagnostic}" >&2
	cat >&2 <<EOF
Local Fluss is not running on $FLUSS_BOOTSTRAP.
Start the local cluster first, then run this command again.
EOF
	exit 1
fi

exec java \
	--add-opens=java.base/java.nio=ALL-UNNAMED \
	-cp "$JAR" \
	com.trading.ingestion.TickTableViewer "$LIMIT"
