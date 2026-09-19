#!/bin/bash
# Ingestion smoke test — runs DDL bootstrap + 10 synthetic ticks against local Fluss.
# Writes to RAW_TABLE_NAME (default raw_table_1) on every run: point it at a
# scratch table on any cluster that is not yours.
set -euo pipefail

# R-166: derive the code dir from this script's location (portable).
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# P6-687/688: defaults, not overrides. FLUSS_BOOTSTRAP=remote:9123 ./smoke-test.sh
# now targets another cluster, and the throwaway credentials below are exported
# only when the caller has not supplied real ones (they are local-only values —
# never rely on them outside a local smoke run).
export FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
export ARROW_APP_ID="${ARROW_APP_ID:-smoke-test}"
export ARROW_APP_SECRET="${ARROW_APP_SECRET:-smoke-secret}" # local smoke-test only
# ARROW_TOKEN is rejected outright by IngestionConfig since 2026-08-24, so a fake
# token here made the live smoke path die in config validation. Export the TOTP
# trio instead — AutoLogin needs all three non-blank.
export ARROW_USER_ID="${ARROW_USER_ID:-smoke-user}"        # local smoke-test only
export ARROW_PASSWORD="${ARROW_PASSWORD:-smoke-password}"  # local smoke-test only
export ARROW_TOTP_KEY="${ARROW_TOTP_KEY:-smoke-totp-key}"  # local smoke-test only
# P6-689: the DDL bootstrap creates this table and 10 ticks land in it per run.
export RAW_TABLE_NAME="${RAW_TABLE_NAME:-raw_table_1}"
# R-050: IngestionConfig treats these as required with no code default —
# without them SmokeTest throws at the first config-validation step.
export ARROW_MAX_EVENT_AGE_MS="${ARROW_MAX_EVENT_AGE_MS:-5000}"
export ARROW_MAX_FUTURE_EVENT_SKEW_MS="${ARROW_MAX_FUTURE_EVENT_SKEW_MS:-2000}"
# SecretGuard refuses the SECRET_KEYS above when they sit in the env unless the
# process declares they arrived through an env file — the marker docker-compose.yml
# and the four host harnesses (loadtest-run, tiering-smoke, stage-soak-e2e,
# pipeline-lib) already set.
export SECRETS_VIA_ENV_FILE="${SECRETS_VIA_ENV_FILE:-1}"
# IngestionConfig requires a deployment env; compose defaults it to dev the same way.
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-dev}"

JAR="02_services/01_ingestion/target/ingestion.jar"
TEST_CLASSES="02_services/01_ingestion/target/test-classes"
PROBE_TIMEOUT_SEC="${PROBE_TIMEOUT_SEC:-3}"

# P6-293: fail fast with an actionable line instead of a Java
# ClassNotFoundException or a raw connection stacktrace.
if ! command -v java >/dev/null 2>&1; then
	printf 'java not found in PATH (the smoke test needs Java 17+ for --add-opens).\n' >&2
	exit 1
fi
if [ ! -f "$DIR/$JAR" ]; then
	printf 'Missing %s. Build it first with:\n  cd %s && mvn -pl 02_services/01_ingestion -am package\n' "$JAR" "$(dirname "$DIR")" >&2
	exit 1
fi
if [ ! -d "$DIR/$TEST_CLASSES" ]; then
	printf 'Missing %s. Build the test classes first with:\n  cd %s && mvn -pl 02_services/01_ingestion -am test-compile\n' "$TEST_CLASSES" "$(dirname "$DIR")" >&2
	exit 1
fi

# The service leg of the same preflight. TCP reachability is necessary, not
# sufficient — DDL bootstrap and auth still run in Java — and the reason is kept
# rather than discarded. A comma list is allowed here (the Java client dials the
# whole list), so the first endpoint is probed and named.
probe_endpoint="${FLUSS_BOOTSTRAP%%,*}"
case "$probe_endpoint" in
	*://*)
		printf 'Bootstrap must be host:port without a scheme: %s\n' "$FLUSS_BOOTSTRAP" >&2
		exit 2
		;;
esac
case "$probe_endpoint" in
	\[*\]:*) # IPv6 literal, e.g. [::1]:9123
		probe_host="${probe_endpoint%]:*}"
		probe_host="${probe_host#[}"
		probe_port="${probe_endpoint##*]:}"
		;;
	*:*)
		probe_host="${probe_endpoint%:*}"
		probe_port="${probe_endpoint##*:}"
		;;
	*)
		printf 'Bootstrap needs a port — no port found in %s\n' "$probe_endpoint" >&2
		exit 2
		;;
esac
if ! probe_err="$(timeout "$PROBE_TIMEOUT_SEC" bash -c 'exec 3<>/dev/tcp/"$0"/"$1"' "$probe_host" "$probe_port" 2>&1 >/dev/null)"; then
	printf 'TCP probe failed for %s: %s\n' "$probe_endpoint" "${probe_err:-no diagnostic}" >&2
	printf 'Start the cluster first (./start-all.sh), then run this again.\n' >&2
	exit 1
fi

cd "$DIR" || {
	printf 'cannot cd to script dir %s\n' "$DIR" >&2
	exit 1
}
# classpath separator below is ':' — Unix only (use ';' on Windows).
java --add-opens=java.base/java.nio=ALL-UNNAMED \
	-cp "$JAR:$TEST_CLASSES" \
	com.trading.ingestion.SmokeTest
