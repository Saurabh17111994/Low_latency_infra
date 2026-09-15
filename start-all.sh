#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
#  start-all.sh — ONE COMMAND to start the whole ingestion pipeline:
#
#    Arrow broker  →  Go bridge  →  Java IngestionService  →  local Fluss
#
#  What this script does, in order:
#    1. Checks your Arrow credentials file exists (~/.env.arrow) — creates a
#       template you fill in if it's missing (this is the ONLY manual step).
#    2. Starts Fluss (zookeeper + coordinator + tablet) via docker compose
#       if it isn't already running.
#    3. Builds the Go bridge binary and the Java jar if they're out of date.
#    4. Creates the Fluss tables (DDL) if they don't exist yet (local dev only).
#    5. Runs the pipeline.  Press Ctrl+C to stop everything cleanly.
#
#  SECURITY: this file never contains or prints secrets. Credentials stay in
#  ~/.env.arrow (chmod 600, git-ignored).
# ═══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Config (edit these, or override via env when you run the script) ──────────
# R-132: derive PROJECT_ROOT from the script location (repo root) — portable
# to any checkout; keep the absolute path only as an env override.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$SCRIPT_DIR}"
CODE_DIR="${CODE_DIR:-$PROJECT_ROOT/code}"
COMPOSE_DIR="${COMPOSE_DIR:-$CODE_DIR/01_platform/01_docker}"
COMPOSE_FILE="${COMPOSE_FILE:-$COMPOSE_DIR/docker-compose.yml}"
SECRETS_FILE="${SECRETS_FILE:-$HOME/.env.arrow}"
MANIFEST="${ARROW_INSTRUMENT_MANIFEST:-$PROJECT_ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv}"
BRIDGE_DIR="${BRIDGE_DIR:-$CODE_DIR/02_services/01_ingestion/go-bridge}"
JAVA_DIR="${JAVA_DIR:-$CODE_DIR/02_services/01_ingestion}"
# P6-860: DDL_DIR used to be defined here and never read — DdlBootstrap (Java,
# step 4) owns the DDL, so a second copy in this script could only drift.
FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
ALLOW_RUNTIME_DDL="${ALLOW_RUNTIME_DDL:-true}" # local dev only; production must be false
GO_FLAGS="${GO_FLAGS:-}"                       # e.g. GO_FLAGS=-tags=netgo
# R-228: default to ONLINE maven (a fresh machine has an empty ~/.m2 and
# `-o` fails obscurely). Set MVN_FLAGS=-o when the local cache is warm.
MVN_FLAGS="${MVN_FLAGS:-}"
LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/logs}"

log() { printf '\033[1;36m[start-all]\033[0m %s\n' "$*"; }
die() {
	printf '\033[1;31m[start-all] FATAL:\033[0m %s\n' "$*" >&2
	exit 1
}

# R-052 / P6-298+300: a credentials file is DATA, never code. `source` executed
# whatever the file contained (a tampered or truncated file ran shell with the
# caller's privileges); this parses one `ARROW_*=` assignment per line and
# exports it through the single-assignment form, so metacharacters stay inert.
# P6-299+301: the old awk+%%/# parsing kept CR (Windows-edited files), quotes,
# whitespace and an `export ` prefix, rejected digits in the key, and dropped a
# final line with no trailing newline — all handled here.
parse_arrow_line() {
	local line="${1%$'\r'}"
	line="${line#"${line%%[![:space:]]*}"}" # trim leading space
	line="${line%"${line##*[![:space:]]}"}" # trim trailing space
	line="${line#export }"                  # optional `export ` prefix
	line="${line#"${line%%[![:space:]]*}"}"
	case "$line" in
	ARROW_[A-Z0-9_]*=*) ;;
	*) return 0 ;;
	esac
	local key="${line%%=*}" value="${line#*=}"
	key="${key//[[:space:]]/}"
	# `KEY = value` (space around `=`) is common in hand-edited files: trim both
	# ends of the value before deciding whether it is quoted.
	value="${value#"${value%%[![:space:]]*}"}"
	value="${value%"${value##*[![:space:]]}"}"
	case "$value" in
	\"*\") value="${value#\"}"; value="${value%\"}" ;;
	\'*\') value="${value#\'}"; value="${value%\'}" ;;
	*)
		# unquoted: drop a trailing whitespace-preceded inline comment
		case "$value" in *[[:space:]]#*) value="${value%%[[:space:]]#*}" ;; esac
		value="${value%"${value##*[![:space:]]}"}"
		;;
	esac
	[ -n "$key" ] || return 0
	export "$key=$value"
}

# P6-298: the file said `chmod 600` and nobody checked. Warn (not die: the
# values are inert now, but a world-readable credentials file is the finding).
warn_unless_private() {
	local file="$1" mode
	mode="$(stat -c %a "$file" 2>/dev/null || stat -f %Lp "$file" 2>/dev/null || echo '')"
	if [ -n "$mode" ] && [ "$mode" != "600" ]; then
		log "WARN: $file is mode $mode — expected 600 (run: chmod 600 $file)"
	fi
	if [ -n "$(find "$file" ! -user "$(id -u)" -print -quit 2>/dev/null)" ]; then
		log "WARN: $file is not owned by uid $(id -u)"
	fi
}

# R-228: tool preflight — a missing tool must fail with an actionable message.
for tool in java go mvn docker awk; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		die "required tool '$tool' not found on PATH"
	fi
done
[ -d "$CODE_DIR" ] || die "CODE_DIR not found: $CODE_DIR"

# R-132: validate the manifest explicitly (the default filename contains a space).
if [ ! -f "$MANIFEST" ]; then
	die "instrument manifest not found: $MANIFEST (set ARROW_INSTRUMENT_MANIFEST)"
fi

# ── 1. Arrow credentials ───────────────────────────────────────────────────────
# Preferred source: ~/.env.arrow (git-ignored, outside the repo).
# Fallback: the docker-compose .env (code/01_platform/01_docker/.env), which
# already holds the real Arrow creds for this machine. Only the ARROW_* keys
# are pulled — the .env is NOT sourced wholesale (it contains non-shell-safe
# values like O2_PASSWORD=<choose a password>). Values are never printed.
if [ -f "$SECRETS_FILE" ]; then
	warn_unless_private "$SECRETS_FILE"
	# `|| [ -n "$line" ]` keeps a final line that has no trailing newline.
	while IFS= read -r line || [ -n "$line" ]; do
		parse_arrow_line "$line"
	done <"$SECRETS_FILE"
	CRED_SOURCE="$SECRETS_FILE"
elif [ -f "$COMPOSE_DIR/.env" ]; then
	# R-052 (security): extract ARROW_* assignments WITHOUT eval — see
	# parse_arrow_line above; the value never reaches the shell parser.
	CRED_SOURCE="$COMPOSE_DIR/.env"
	while IFS= read -r line || [ -n "$line" ]; do
		parse_arrow_line "$line"
	done <"$COMPOSE_DIR/.env"
	# S2 (2026-08-29): secrets moved to secrets.env — pull ARROW_* from it too.
	# Parsed second, so secrets.env wins over .env; that precedence is now
	# spelled out in the source line instead of two overlapping log lines.
	if [ -f "$COMPOSE_DIR/secrets.env" ]; then
		CRED_SOURCE="$COMPOSE_DIR/.env + secrets.env"
		while IFS= read -r line || [ -n "$line" ]; do
			parse_arrow_line "$line"
		done <"$COMPOSE_DIR/secrets.env"
	else
		log "no $SECRETS_FILE — .env lacks ARROW_* and secrets.env is MISSING"
	fi
else
	# P6-861: umask 077 creates the file 600 from its first byte (the old form
	# was world-readable until the chmod), and a quoted delimiter stops the
	# heredoc from expanding anything a future edit adds.
	(
		umask 077
		cat >"$SECRETS_FILE" <<'EOF'
# Arrow broker credentials — fill these in, then re-run start-all.sh
ARROW_APP_ID=
ARROW_APP_SECRET=
ARROW_USER_ID=
ARROW_PASSWORD=
ARROW_TOTP_KEY=
# ARROW_TOKEN removed 2026-08-24 — use TOTP only
# (do not set ARROW_TOKEN)
EOF
	)
	chmod 600 "$SECRETS_FILE"
	die "created a template at $SECRETS_FILE — open it, fill in your Arrow credentials, then re-run."
fi
: "${ARROW_APP_ID:?ARROW_APP_ID must be set (credentials file / compose .env)}"
: "${ARROW_APP_SECRET:?ARROW_APP_SECRET must be set (credentials file / compose .env)}"
: "${ARROW_USER_ID:?ARROW_USER_ID must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
: "${ARROW_PASSWORD:?ARROW_PASSWORD must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
: "${ARROW_TOTP_KEY:?ARROW_TOTP_KEY must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
export ARROW_APP_ID ARROW_APP_SECRET ARROW_USER_ID ARROW_PASSWORD ARROW_TOTP_KEY
log "credentials OK (from ${CRED_SOURCE:-$SECRETS_FILE})"

# ── 2. Start Fluss core if not running ────────────────────────────────────────
# R-167: use bash's /dev/tcp instead of `nc` (not installed everywhere).
# P6-698/P6-701: the old parse (${VAR%:*} / ${VAR##*:}) broke on a multi-host
# bootstrap list and on an IPv6 literal; take the first endpoint and strip the
# brackets an IPv6 host must carry.
fluss_host_port() {
	local first="${FLUSS_BOOTSTRAP%%,*}"
	FLUSS_HOST="${first%%:*}"
	FLUSS_PORT="${first##*:}"
	FLUSS_HOST="${FLUSS_HOST#[}"
	FLUSS_HOST="${FLUSS_HOST%]}"
}
fluss_up() {
	fluss_host_port
	(exec 3<>"/dev/tcp/${FLUSS_HOST}/${FLUSS_PORT}") 2>/dev/null
}

# P6-698/P6-701: a TCP connect proves only that a socket is listening — the
# coordinator accepts connections while zookeeper/tablet are still initializing,
# so Java's DdlBootstrap used to race a half-ready cluster (and every later
# start paid a 60 s no-op wait for the same reason). Readiness also requires the
# tablet container to be running.
fluss_tablet_running() {
	(cd "$COMPOSE_DIR" && docker compose --env-file .env --env-file secrets.env \
		-f "$COMPOSE_FILE" ps -q --status running fluss-tablet 2>/dev/null) | grep -q .
}

fluss_ready() {
	fluss_up || return 1
	if fluss_tablet_running; then
		return 0
	fi
	# Degraded mode, announced rather than silent: a stack started outside
	# compose (docker run / k8s) has no container for `ps` to find, and dying
	# on that would break a working setup. TCP reachability stays the hard gate.
	if ! docker compose version >/dev/null 2>&1; then
		FLUSS_READY_TCP_ONLY=1
		return 0
	fi
	return 1
}

# P6-698: readiness is checked even when the port already answers. The race the
# finding names — a coordinator that accepts TCP while the tablet still
# initializes — is exactly the case where the port is open and the cluster is
# not ready, and the old shape skipped the check there.
FLUSS_READY_TCP_ONLY=0
# Test seam: the poll defaults to 30 x 2 s (60 s); a hermetic test drives the
# not-ready path without waiting a minute.
FLUSS_READY_TRIES="${FLUSS_READY_TRIES:-30}"
FLUSS_READY_SLEEP_SEC="${FLUSS_READY_SLEEP_SEC:-2}"
if ! fluss_ready; then
	if ! fluss_up; then
		log "Fluss not running — starting zookeeper + coordinator + tablet..."
		# The .env supplies FLUSS_IMAGE etc.; fail loudly if it's missing.
		[ -f "$COMPOSE_DIR/.env" ] || die "missing $COMPOSE_DIR/.env (copy from Arrow_broker/.env and fill in FLUSS_IMAGE)"
		[ -f "$COMPOSE_DIR/secrets.env" ] || die "missing $COMPOSE_DIR/secrets.env — the canonical compose form needs both env files"
		# P6-858/P6-859: COMPOSE_FILE was documented as overridable and never
		# read, so a custom stack file was silently ignored. It is named here
		# and in the readiness probe.
		(cd "$COMPOSE_DIR" && docker compose --env-file .env --env-file secrets.env \
			-f "$COMPOSE_FILE" up -d zookeeper fluss-coordinator fluss-tablet)
	fi
	log "waiting for Fluss to be ready on $FLUSS_BOOTSTRAP (coordinator + tablet)..."
	for i in $(seq 1 "$FLUSS_READY_TRIES"); do
		if fluss_ready; then break; fi
		sleep "$FLUSS_READY_SLEEP_SEC"
		[ "$i" = "$FLUSS_READY_TRIES" ] && die "Fluss did not become ready on $FLUSS_BOOTSTRAP after $((FLUSS_READY_TRIES * FLUSS_READY_SLEEP_SEC))s — check docker compose logs"
	done
else
	log "Fluss already running on $FLUSS_BOOTSTRAP — skipping docker compose"
fi
if [ "$FLUSS_READY_TCP_ONLY" = "1" ]; then
	log "WARN: docker compose unavailable — accepting TCP reachability on $FLUSS_BOOTSTRAP as readiness (tablet probe skipped)"
fi

# ── 3. Build bridge + jar if stale ────────────────────────────────────────────
mkdir -p "$LOG_DIR"
# P6-703: GO_FLAGS/MVN_FLAGS are word-split once into arrays, so a multi-word
# or quoted flag survives. `${ARR[@]+...}` keeps the empty-array case safe on
# bash < 4.4 under `set -u` (macOS ships 3.2).
GO_ARGS=()
if [ -n "$GO_FLAGS" ]; then read -r -a GO_ARGS <<<"$GO_FLAGS"; fi
MVN_ARGS=()
if [ -n "$MVN_FLAGS" ]; then read -r -a MVN_ARGS <<<"$MVN_FLAGS"; fi

BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge"
INGESTION_JAR="$JAVA_DIR/target/ingestion.jar"

# P6-702: the section comment promised "if stale" and the code rebuilt on every
# start. Rebuild only when an output is missing or older than a source file.
needs_rebuild() {
	local out="$1" srcdir="$2" pattern="$3"
	[ -f "$out" ] || return 0
	if [ -n "$(find "$srcdir" \( -name "$pattern" -o -name pom.xml \) -newer "$out" -print -quit 2>/dev/null)" ]; then
		return 0
	fi
	return 1
}

if needs_rebuild "$BRIDGE_BIN" "$BRIDGE_DIR" '*.go'; then
	log "building Go bridge..."
	(cd "$BRIDGE_DIR" && go build ${GO_ARGS[@]+"${GO_ARGS[@]}"} -o arrow-bridge .)
else
	log "Go bridge up to date — skipping build"
fi
# P6-703/P6-704: a silent `go build`/`mvn -q` failure used to surface only as an
# obscure `java -jar: file not found`; check the artifact now.
[ -x "$BRIDGE_BIN" ] || die "go build produced no executable: $BRIDGE_BIN"

if needs_rebuild "$INGESTION_JAR" "$JAVA_DIR/src" '*.java'; then
	# P6-704: the message said "offline" while R-228 made the default online.
	log "building Java jar..."
	(cd "$CODE_DIR" && mvn ${MVN_ARGS[@]+"${MVN_ARGS[@]}"} -q -pl 02_services/01_ingestion -am package -DskipTests)
else
	log "Java jar up to date — skipping build"
fi
[ -f "$INGESTION_JAR" ] || die "mvn package produced no jar: $INGESTION_JAR (re-run with MVN_FLAGS= to see the build log)"

# ── 4. Create tables if missing (local dev only) ──────────────────────────────
if [ "$ALLOW_RUNTIME_DDL" = "true" ]; then
	log "ensuring Fluss tables exist (ALLOW_RUNTIME_DDL=true, local dev)..."
	# The service calls DdlBootstrap.ensureTables when ALLOW_RUNTIME_DDL=true,
	# so tables are created on startup — nothing extra to do here.
else
	log "ALLOW_RUNTIME_DDL=false — tables must already exist; service verifies read-only"
fi

# ── 5. Run the pipeline ───────────────────────────────────────────────────────
log "starting ingestion pipeline (Ctrl+C to stop, logs → $LOG_DIR/ingestion.log)..."
export FLUSS_BOOTSTRAP
export FLUSS_BOOTSTRAP_SERVERS="$FLUSS_BOOTSTRAP"
export RAW_TABLE_NAME="${RAW_TABLE_NAME:-raw_table_1}"
export ARROW_BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge"
export ARROW_INSTRUMENT_MANIFEST="$MANIFEST"
# Java loader reads INSTRUMENT_MANIFEST_PATH (not ARROW_INSTRUMENT_MANIFEST);
# export both so the pipeline can start.
export INSTRUMENT_MANIFEST_PATH="$MANIFEST"
export ARROW_HFT_LATENCY_MS="${ARROW_HFT_LATENCY_MS:-50}"
export NTP_SERVER="${NTP_SERVER:-ntp.ubuntu.com,time.google.com,in.pool.ntp.org}"
# Timestamp-freshness evidence-gated values (plan B3; user-approved 2026-08-01:
# ARROW_MAX_EVENT_AGE_MS=5000 / ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000). Required by
# IngestionConfig at startup — no code default, so must be exported here.
export ARROW_MAX_EVENT_AGE_MS="${ARROW_MAX_EVENT_AGE_MS:-5000}"
export ARROW_MAX_FUTURE_EVENT_SKEW_MS="${ARROW_MAX_FUTURE_EVENT_SKEW_MS:-2000}"
export ALLOW_RUNTIME_DDL
mkdir -p "$LOG_DIR"

# P6-699/P6-706: the header promises "Press Ctrl+C to stop everything cleanly",
# and there was no trap to make that true — SIGINT/SIGTERM left the java process
# and the tee pipeline running. The Go bridge is a child of java (not of this
# script), so the JVM's own shutdown hook is what stops it; this trap's job is to
# deliver the signal to java and wait for that hook rather than exiting on top
# of it (BridgeShutdownHookTest / BridgeShutdownRegressionTest cover the hook).
# Decision (the finding asks for one): the Fluss containers are LEFT RUNNING —
# this script is also the "restart just the pipeline" path, and stopping the
# cluster on exit would silently undo step 2; `make down` stops them.
JAVA_PID=""
cleanup() {
	local rc=$?
	if [ -n "$JAVA_PID" ] && kill -0 "$JAVA_PID" 2>/dev/null; then
		log "stopping the pipeline (pid $JAVA_PID)..."
		kill -TERM "$JAVA_PID" 2>/dev/null || true
		local _i
		for _i in $(seq 1 50); do
			kill -0 "$JAVA_PID" 2>/dev/null || break
			sleep 0.1
		done
		kill -KILL "$JAVA_PID" 2>/dev/null || true
	fi
	exit "$rc"
}
trap cleanup INT TERM EXIT

# --add-opens required by the Fluss client's shaded Arrow (MemoryUtil touches
# java.nio internals on JDK 17+). Same flag the pom's surefire and
# run-ingestion-full.sh use.
# P6-699: process substitution, not `| tee` — with a pipeline `$!` is tee's pid
# and the trap above would signal the wrong process.
java --add-opens=java.base/java.nio=ALL-UNNAMED \
	-jar "$JAVA_DIR/target/ingestion.jar" > >(tee "$LOG_DIR/ingestion.log") 2>&1 &
JAVA_PID=$!

set +e
wait "$JAVA_PID"
rc=$?
set -e
# Clear before exiting: a reaped pid can be recycled, and the EXIT trap must not
# signal an unrelated process.
JAVA_PID=""
exit "$rc"
