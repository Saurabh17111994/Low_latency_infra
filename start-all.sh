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
DDL_DIR="${DDL_DIR:-$CODE_DIR/01_platform/02_sql/ddl}"
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
	# shellcheck disable=SC1090
	source "$SECRETS_FILE"
	log "credentials from $SECRETS_FILE"
elif [ -f "$COMPOSE_DIR/.env" ]; then
	# R-052 (security): extract ARROW_* assignments WITHOUT eval. `export
	# "$key=$value"` is a single quoted assignment argument — the shell never
	# re-evaluates the value, so shell metacharacters in secrets stay inert.
	while IFS= read -r line; do
		key="${line%%=*}"
		value="${line#*=}"
		[ -n "$key" ] || continue
		export "$key=$value"
	done < <(awk -F= '/^ARROW_[A-Z_]+=/ {print $1 "=" substr($0, index($0,"=")+1)}' "$COMPOSE_DIR/.env")
	log "no $SECRETS_FILE — using ARROW_* credentials from $COMPOSE_DIR/.env"
	# S2 (2026-08-29): secrets moved to secrets.env — pull ARROW_* from it too.
	if [ -f "$COMPOSE_DIR/secrets.env" ]; then
		while IFS= read -r line; do
			key="${line%%=*}"
			value="${line#*=}"
			[ -n "$key" ] || continue
			export "$key=$value"
		done < <(awk -F= '/^ARROW_[A-Z_]+=/ {print $1 "=" substr($0, index($0,"=")+1)}' "$COMPOSE_DIR/secrets.env")
		log "no $SECRETS_FILE — using ARROW_* credentials from $COMPOSE_DIR/.env + secrets.env"
	else
		log "no $SECRETS_FILE — .env lacks ARROW_* and secrets.env is MISSING"
	fi
else
	cat >"$SECRETS_FILE" <<EOF
# Arrow broker credentials — fill these in, then re-run start-all.sh
ARROW_APP_ID=
ARROW_APP_SECRET=
ARROW_USER_ID=
ARROW_PASSWORD=
ARROW_TOTP_KEY=
# ARROW_TOKEN removed 2026-08-24 — use TOTP only
# (do not set ARROW_TOKEN)
EOF
	chmod 600 "$SECRETS_FILE"
	die "created a template at $SECRETS_FILE — open it, fill in your Arrow credentials, then re-run."
fi
: "${ARROW_APP_ID:?ARROW_APP_ID must be set (credentials file / compose .env)}"
: "${ARROW_APP_SECRET:?ARROW_APP_SECRET must be set (credentials file / compose .env)}"
: "${ARROW_USER_ID:?ARROW_USER_ID must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
: "${ARROW_PASSWORD:?ARROW_PASSWORD must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
: "${ARROW_TOTP_KEY:?ARROW_TOTP_KEY must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
export ARROW_APP_ID ARROW_APP_SECRET ARROW_USER_ID ARROW_PASSWORD ARROW_TOTP_KEY
log "credentials OK (from $SECRETS_FILE)"

# ── 2. Start Fluss core if not running ────────────────────────────────────────
# R-167: use bash's /dev/tcp instead of `nc` (not installed everywhere).
fluss_up() {
	(exec 3<>"/dev/tcp/${FLUSS_BOOTSTRAP%:*}/${FLUSS_BOOTSTRAP##*:}") 2>/dev/null
}

if ! fluss_up; then
	log "Fluss not running — starting zookeeper + coordinator + tablet..."
	# The .env supplies FLUSS_IMAGE etc.; fail loudly if it's missing.
	[ -f "$COMPOSE_DIR/.env" ] || die "missing $COMPOSE_DIR/.env (copy from Arrow_broker/.env and fill in FLUSS_IMAGE)"
	[ -f "$COMPOSE_DIR/secrets.env" ] || die "missing $COMPOSE_DIR/secrets.env — the canonical compose form needs both env files"
	(cd "$COMPOSE_DIR" && docker compose --env-file .env --env-file secrets.env up -d zookeeper fluss-coordinator fluss-tablet)
	log "waiting for Fluss coordinator on $FLUSS_BOOTSTRAP..."
	for i in $(seq 1 30); do
		if fluss_up; then break; fi
		sleep 2
		[ "$i" = 30 ] && die "Fluss did not come up on $FLUSS_BOOTSTRAP after 60s — check docker compose logs"
	done
else
	log "Fluss already running on $FLUSS_BOOTSTRAP — skipping docker compose"
fi

# ── 3. Build bridge + jar if stale ────────────────────────────────────────────
mkdir -p "$LOG_DIR"
log "building Go bridge..."
(cd "$BRIDGE_DIR" && go build $GO_FLAGS -o arrow-bridge .)
log "building Java jar (offline)..."
(cd "$CODE_DIR" && mvn $MVN_FLAGS -q -pl 02_services/01_ingestion -am package -DskipTests)

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
# --add-opens required by the Fluss client's shaded Arrow (MemoryUtil touches
# java.nio internals on JDK 17+). Same flag the pom's surefire and
# run-ingestion-full.sh use.
java --add-opens=java.base/java.nio=ALL-UNNAMED \
	-jar "$JAVA_DIR/target/ingestion.jar" 2>&1 | tee "$LOG_DIR/ingestion.log"
