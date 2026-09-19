#!/bin/bash
# Run the full ingestion pipeline: Go arrow-bridge → Java → Fluss
# Pulls Arrow credentials from ~/.env.arrow (see run-ingestion.sh header).
set -euo pipefail

# R-165: derive paths from the script location; keep env overrides.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
CODE_DIR="${CODE_DIR:-$REPO_ROOT/code}"

# ---- Preflight: required tooling -------------------------------------------
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
if ! command -v "$JAVA_BIN" >/dev/null 2>&1; then
	JAVA_BIN="java"
fi
if ! command -v "$JAVA_BIN" >/dev/null 2>&1; then
	echo "ingestion: FATAL — java not found (set JAVA_HOME to a Java 17 JDK)" >&2
	exit 1
fi

JAVA_VERSION="$("$JAVA_BIN" -version 2>&1 | head -n 1 | sed -E 's/.*version "([^."]+).*/\1/')"
if [ "$JAVA_VERSION" != "17" ]; then
	echo "ingestion: FATAL — Java 17 required; found '$JAVA_VERSION' ($("$JAVA_BIN" -version 2>&1 | head -n 1)). Set JAVA_HOME to a Java 17 JDK." >&2
	exit 1
fi

# ---- Arrow credentials from ~/.env.arrow (never committed) ------------------
SECRETS_FILE="${SECRETS_FILE:-$HOME/.env.arrow}"
if [ ! -f "$SECRETS_FILE" ]; then
	echo "ingestion: FATAL — no secrets file at $SECRETS_FILE." >&2
	echo "Create it from the template in run-ingestion.sh (chmod 600)." >&2
	exit 1
fi
# R-212: the secrets file must be owner-only — permissive umasks leak creds.
# P6-682: the file is sourced below, so refuse symlinks (both -f and stat
# follow them, so an attacker-swapped target would be measured then executed),
# accept 400 as owner-only too, and use the BSD/macOS stat variant as fallback.
if [ -L "$SECRETS_FILE" ]; then
	echo "ingestion: FATAL — $SECRETS_FILE is a symlink. Refusing to source a symlinked secrets file (it could point anywhere)." >&2
	exit 1
fi
SECRETS_MODE=""
if stat -c '%a' "$SECRETS_FILE" >/dev/null 2>&1; then
	SECRETS_MODE="$(stat -c '%a' "$SECRETS_FILE")"
elif stat -f '%Lp' "$SECRETS_FILE" >/dev/null 2>&1; then
	SECRETS_MODE="$(stat -f '%Lp' "$SECRETS_FILE")"
fi
if [ "$SECRETS_MODE" != "600" ] && [ "$SECRETS_MODE" != "400" ]; then
	echo "ingestion: FATAL — $SECRETS_FILE is not owner-only (mode ${SECRETS_MODE:-unknown}). Run: chmod 600 \"$SECRETS_FILE\"" >&2
	exit 1
fi
# shellcheck disable=SC1090
source "$SECRETS_FILE"
: "${ARROW_APP_ID:?ARROW_APP_ID must be set in $SECRETS_FILE}"
: "${ARROW_APP_SECRET:?ARROW_APP_SECRET must be set in $SECRETS_FILE}"
# P6-027: TOTP-only since 2026-08-24 (commit 96c0b186 removed ARROW_TOKEN from
# this export list for that reason). The old `if [ -z "${ARROW_TOKEN:-}" ]`
# guard skipped all three checks whenever ARROW_TOKEN was set, so a token-only
# setup passed preflight and only failed inside the JVM — which rejects a
# non-blank ARROW_TOKEN outright. Fail here instead, with the same reason.
# Re-adding ARROW_TOKEN to the export list below would break the JVM: it is
# rejected on sight (IngestionConfig), so it must stay unexported.
if [ -n "${ARROW_TOKEN:-}" ]; then
	echo "ingestion: FATAL — ARROW_TOKEN removed 2026-08-24; use ARROW_USER_ID+ARROW_PASSWORD+ARROW_TOTP_KEY (TOTP AutoLogin) only." >&2
	exit 1
fi
: "${ARROW_USER_ID:?ARROW_USER_ID must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
: "${ARROW_PASSWORD:?ARROW_PASSWORD must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
: "${ARROW_TOTP_KEY:?ARROW_TOTP_KEY must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
# Export Arrow credentials to the Java child process.
export ARROW_APP_ID ARROW_APP_SECRET ARROW_USER_ID ARROW_PASSWORD ARROW_TOTP_KEY

# ---- Environment (local Fluss; keep localhost:9123) -------------------------
export FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
export FLUSS_BOOTSTRAP_SERVERS="${FLUSS_BOOTSTRAP_SERVERS:-localhost:9123}"
export RAW_TABLE_NAME="${RAW_TABLE_NAME:-raw_table_1}"
export ARROW_BRIDGE_BIN="$CODE_DIR/02_services/01_ingestion/go-bridge/arrow-bridge"
# Default NTP list: servers reachable from this network (pool.ntp.org is
# unreachable on this host). Tried in order until one answers.
export NTP_SERVER="${NTP_SERVER:-ntp.ubuntu.com,time.google.com,in.pool.ntp.org}"
# P6-683: the manifest lives in Arrow_broker/, a sibling of this checkout
# (outside the repo — it is not git-tracked), so default to a path derived from
# REPO_ROOT. The old default was an absolute /home/<user>/... path that only
# worked on the machine it was written on and leaked the username.
export ARROW_INSTRUMENT_MANIFEST="${ARROW_INSTRUMENT_MANIFEST:-$REPO_ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv}"
export INSTRUMENT_MANIFEST_PATH="$ARROW_INSTRUMENT_MANIFEST"
# Approved current-phase manifest: exactly 1,024 instruments (plan: 1-connection /
# 1,024-instrument phase). The token column is the 4th, unquoted numeric in both
# files, so cut works; the Go bridge also reads ARROW_INSTRUMENT_MANIFEST directly.
# R-081: validate the extracted count + strip CR (CRLF/BOM corruption must not
# silently produce a wrong subscription set).
# P6-289: the manifest must be checked BEFORE it is parsed. Previously the cut
# below ran first, so a missing path failed as a raw `cut` error even though a
# friendly FATAL existed further down (it was unreachable for this case).
if [ ! -f "$ARROW_INSTRUMENT_MANIFEST" ]; then
	echo "ingestion: FATAL — instrument manifest not found: $ARROW_INSTRUMENT_MANIFEST (set ARROW_INSTRUMENT_MANIFEST to override)" >&2
	exit 1
fi
TOKENS_CSV="$(cut -d',' -f4 "$ARROW_INSTRUMENT_MANIFEST" | tail -n +2 | sed 's/\r$//' | paste -sd, -)"
TOKEN_COUNT="$(printf '%s' "$TOKENS_CSV" | tr ',' '\n' | grep -c . || true)"
EXPECTED_TOKENS="${EXPECTED_TOKENS:-1024}"
if [ "$TOKEN_COUNT" != "$EXPECTED_TOKENS" ]; then
	echo "ingestion: FATAL — instrument token count $TOKEN_COUNT != expected $EXPECTED_TOKENS (manifest truncated or CRLF/BOM-corrupted): $ARROW_INSTRUMENT_MANIFEST" >&2
	exit 1
fi
export ARROW_INSTRUMENT_TOKENS="$TOKENS_CSV"

# ---- Timestamp-freshness evidence-gated values (plan B3) ---------------------
# User-approved production values (2026-08-01). Override via env only if feed
# latency measurements change them (ARROW_MAX_EVENT_AGE_MS / ARROW_MAX_FUTURE_EVENT_SKEW_MS
# are required at Java startup and have no code default).
export ARROW_MAX_EVENT_AGE_MS="${ARROW_MAX_EVENT_AGE_MS:-5000}"
export ARROW_MAX_FUTURE_EVENT_SKEW_MS="${ARROW_MAX_FUTURE_EVENT_SKEW_MS:-2000}"

# ---- Account scope (used to group subscriptions under one Arrow account) ------
# User's Arrow user id. Override via env if the production account differs.
export ACCOUNT_SCOPE_ID="${ACCOUNT_SCOPE_ID:-QP3796}"

# ---- Preflight: artifacts --------------------------------------------------
JAR="$CODE_DIR/02_services/01_ingestion/target/ingestion.jar"
if [ ! -f "$JAR" ]; then
	echo "ingestion: FATAL — ingestion.jar not found at $JAR. Build it first (make build)." >&2
	exit 1
fi
if [ ! -x "$ARROW_BRIDGE_BIN" ]; then
	echo "ingestion: FATAL — arrow-bridge binary not found or not executable: $ARROW_BRIDGE_BIN" >&2
	exit 1
fi
# ---- Preflight: local Fluss reachable --------------------------------------
# P6-684: the old `(exec 3<>/dev/tcp/$host/$port)` probe is bash-only, had no
# timeout (a filtered host hangs the launcher forever before the JVM starts),
# and its `%%:*`/`##*:` split produced a bogus port for IPv6 or a bare host.
# Delegate to a timed TCP connect instead; python3 is already a hard dependency
# of this platform's tooling.
FLUSS_HOST="${FLUSS_BOOTSTRAP%:*}"
FLUSS_PORT="${FLUSS_BOOTSTRAP##*:}"
if [ "$FLUSS_HOST" = "$FLUSS_BOOTSTRAP" ] || [ -z "$FLUSS_HOST" ] || [ -z "$FLUSS_PORT" ]; then
	echo "ingestion: FATAL — FLUSS_BOOTSTRAP must be host:port (got '$FLUSS_BOOTSTRAP')." >&2
	exit 1
fi
if ! python3 -c '
import socket, sys
host, port = sys.argv[1], int(sys.argv[2])
s = socket.socket()
s.settimeout(5)
try:
    s.connect((host, port))
finally:
    s.close()
' "$FLUSS_HOST" "$FLUSS_PORT" 2>/dev/null; then
	echo "ingestion: FATAL — local Fluss not reachable at $FLUSS_BOOTSTRAP. Start the stack first." >&2
	exit 1
fi
echo "ingestion: preflight OK (java=$JAVA_VERSION, bridge=$(basename "$ARROW_BRIDGE_BIN"), manifest=$ARROW_INSTRUMENT_MANIFEST)"

# ---- Uncertainty journal path (writable) ------------------------------------
JOURNAL_DIR="${UNCERTAINTY_JOURNAL_DIR:-$HOME/.local/state/trading-platform/ingestion}"
mkdir -p "$JOURNAL_DIR"
export UNCERTAINTY_JOURNAL_PATH="$JOURNAL_DIR/uncertainty-journal.jsonl"

# Live-mode hardening: strict clock check, no runtime DDL mutation.
export CLOCK_CHECK_REQUIRED="${CLOCK_CHECK_REQUIRED:-true}"
export ALLOW_RUNTIME_DDL="${ALLOW_RUNTIME_DDL:-false}"

cd "$CODE_DIR"

# Built-in durable logging: show live on stdout AND append to a dated file under
# the journal dir, so nothing depends on the caller piping to tee (avoids the
# /tmp permission issue and always leaves a copy for later inspection).
# P6-685: 1-second granularity meant two concurrent launchers (or a rapid
# restart) picked the same $RUN_LOG and interleaved through `tee -a`. Add $$ so
# the name is unique per process.
RUN_LOG="$JOURNAL_DIR/ingestion-$(date +%Y%m%d-%H%M%S)-$$.log"
echo "ingestion: log -> $RUN_LOG"

# ---- Preflight: duplicate-ingestion guard (R-080, P6-291) --------------------
# Refuse to start beside a live IngestionService JVM, which would double-write
# to Fluss. This check MUST come before the stale-bridge cleanup: the old order
# killed the running JVM's bridge first and only then refused to start, so a
# refused launch left the live ingestion bridgeless.
EXISTING_JAVA="$(pgrep -f 'com.trading.ingestion.IngestionService' 2>/dev/null || true)"
if [ -n "$EXISTING_JAVA" ]; then
	echo "ingestion: FATAL — IngestionService already running (pid(s): $EXISTING_JAVA). Refusing to start a duplicate (double-write risk). Stop it first." >&2
	exit 1
fi

# ---- Stale child bridge cleanup (plan §Launchers) ---------------------------
# The Go bridge (arrow-bridge) may be left running if a previous launcher was
# killed. Find and terminate stale instances of our bridge binary before start.
#
# P6-290: the old version matched with `pgrep -f "^$ARROW_BRIDGE_BIN"` (the
# path is a regex, so `.` matched any character) and then did an unconditional
# `kill; sleep 1; kill -9` on the snapshot. A PID reaped and reused between the
# snapshot and the signal would be SIGKILLed wrongly. Re-verify each PID is
# still the bridge we matched, and re-check right before the hard kill.
cleanup_stale_bridges() {
	local stale_pids pid
	stale_pids="$(pgrep -f "^$ARROW_BRIDGE_BIN" 2>/dev/null || true)"
	for pid in $stale_pids; do
		# Re-check identity at signal time; a vanished PID means it already exited.
		if ! pgrep -f "^$ARROW_BRIDGE_BIN" 2>/dev/null | grep -qx "$pid"; then
			continue
		fi
		echo "ingestion: killing stale arrow-bridge process(es): $pid"
		kill "$pid" 2>/dev/null || true
	done
	# Give interrupted bridges a moment to exit before escalating.
	[ -n "$stale_pids" ] && sleep 1
	for pid in $stale_pids; do
		if pgrep -f "^$ARROW_BRIDGE_BIN" 2>/dev/null | grep -qx "$pid"; then
			kill -9 "$pid" 2>/dev/null || true
		fi
	done
}
cleanup_stale_bridges

# PID is initialised here so a signal arriving during the preflight window
# cannot trip `set -u` with an unbound variable (P6-292). The process-
# substitution tee needs no variable: bash does not expose its PID, and the
# `wait` at the end of the script joins it (P6-686).
PID=""

# Forward termination signals so the bridge gets cleaned up by Java.
trap 'echo "ingestion: received SIGTERM, forwarding"; if [ -n "${PID:-}" ]; then kill -TERM "$PID" 2>/dev/null || true; fi' TERM INT

# Ensure the bridge is terminated on launcher exit (e.g. Ctrl+C, kill, crash).
cleanup_on_exit() {
	if [ -n "${PID:-}" ] && kill -0 "$PID" 2>/dev/null; then
		kill -TERM "$PID" 2>/dev/null || true
	fi
	cleanup_stale_bridges
}
trap cleanup_on_exit EXIT

# R-049: run the JVM through a process substitution so $! is the JVM's PID
# (not tee's). wait/traps then signal the real JVM and its child arrow-bridge.
# P6-686: `wait "$PID"` returning non-zero under `set -e` used to abort the
# script before `EXIT=$?` captured the value, so the JVM's exit code was
# replaced by errexit's and `exit "$EXIT"` never ran. Capture it explicitly,
# and also join the tee subshell so the log is complete before we exit.
"$JAVA_BIN" --add-opens=java.base/java.nio=ALL-UNNAMED \
	-Dorg.slf4j.simpleLogger.defaultLogLevel=info \
	-Dorg.slf4j.simpleLogger.showDateTime=true \
	-Dorg.slf4j.simpleLogger.logFile=System.err \
	-cp "$JAR" \
	com.trading.ingestion.IngestionService > >(tee -a "$RUN_LOG") 2>&1 &
PID=$!
wait "$PID" && EXIT=0 || EXIT=$?
# Wait for tee to drain and flush the log (P6-292: the writer was untracked).
wait || true
exit "$EXIT"
