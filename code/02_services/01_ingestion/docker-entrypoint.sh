#!/usr/bin/env bash
# Ingestion entrypoint — Java IngestionService manages Go arrow-bridge lifecycle.
# Java launches bridge as subprocess (ProcessBuilder), reads proto frames (T6)
# from stdout (the NDJSON pipe transport was removed 2026-08-29; anything but
# TRANSPORT=proto|grpc is FATAL in the bridge), pipes bridge stderr into SLF4J
# logs. Bridge crash → discontinuity → shutdown.
set -euo pipefail

echo "ingestion: starting (FLUSS_BOOTSTRAP=${FLUSS_BOOTSTRAP:-fluss-coordinator:9123})"

# T6/CHG-115: proto is THE transport (NDJSON pipe removed 2026-08-29).
# An explicit TRANSPORT env (compose/stack) wins; entrypoint default is proto.
export TRANSPORT="${TRANSPORT:-proto}"
echo "ingestion: transport=${TRANSPORT}"
# P1-138: allowlist at the door — anything else is FATAL in the bridge,
# so reject typos here with the entrypoint FATAL pattern, not late in Go.
case "$TRANSPORT" in
	proto|grpc) ;;
	*) echo "ingestion: FATAL — TRANSPORT must be proto|grpc, got: $TRANSPORT" >&2; exit 2 ;;
esac
# P1-077: the service refuses startup without an explicit env (fail-closed) —
# default ad-hoc runs to dev; compose/stack set this explicitly (prod for stack).
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-dev}"
# Native phase 1 (P1-133): contract-named resource attributes, set ONCE here —
# never per log call, never on the tick path. service.name comes from the
# Dockerfile OTEL_SERVICE_NAME; host comes from the agent itself. An explicit
# operator value wins (override hatch).
export OTEL_RESOURCE_ATTRIBUTES="${OTEL_RESOURCE_ATTRIBUTES:-component=ticks-to-raw,subsystem=raw-append,environment=${DEPLOYMENT_ENV:-dev}}"

if [[ -z "${FLUSS_BOOTSTRAP:-}" ]]; then
	echo "ingestion: FATAL — FLUSS_BOOTSTRAP is required" >&2
	exit 2
fi

# P1-139: two names, one manifest — divergent values mean config drift,
# so refuse instead of silently letting the first name win.
if [[ -n "${ARROW_INSTRUMENT_MANIFEST:-}" && -n "${INSTRUMENT_MANIFEST_PATH:-}" && "${ARROW_INSTRUMENT_MANIFEST}" != "${INSTRUMENT_MANIFEST_PATH}" ]]; then
	echo "ingestion: FATAL — manifest vars diverge: ARROW_INSTRUMENT_MANIFEST=${ARROW_INSTRUMENT_MANIFEST} vs INSTRUMENT_MANIFEST_PATH=${INSTRUMENT_MANIFEST_PATH}" >&2
	exit 2
fi
MANIFEST_PATH="${ARROW_INSTRUMENT_MANIFEST:-${INSTRUMENT_MANIFEST_PATH:-}}"
# P1-139: -f (not just -r) — a directory is readable but is not a manifest.
if [[ -z "$MANIFEST_PATH" || ! -f "$MANIFEST_PATH" || ! -r "$MANIFEST_PATH" ]]; then
	echo "ingestion: FATAL — readable manifest FILE is required: ${MANIFEST_PATH:-<unset>}" >&2
	exit 2
fi
# Normalize: both names resolve to the same manifest so the Go bridge
# (ARROW_INSTRUMENT_MANIFEST) and the Java loader (INSTRUMENT_MANIFEST_PATH)
# can never load different snapshots.
export ARROW_INSTRUMENT_MANIFEST="$MANIFEST_PATH"
export INSTRUMENT_MANIFEST_PATH="$MANIFEST_PATH"

# Validate Go bridge binary (D6) — still checked for early failure
BRIDGE_BIN="${ARROW_BRIDGE_BIN:-/app/arrow-bridge}"
if [[ ! -x "$BRIDGE_BIN" ]]; then
	echo "ingestion: FATAL — arrow-bridge binary not found or not executable: $BRIDGE_BIN" >&2
	exit 1
fi
echo "ingestion: arrow-bridge binary OK ($BRIDGE_BIN)"

if [[ ! -r /app/ingestion.jar ]]; then
	echo "ingestion: FATAL — Java artifact not found: /app/ingestion.jar" >&2
	exit 2
fi

# P1-141: same FATAL pattern for the agent — a missing jar otherwise dies
# late with an obscure JVM Premain-Class error at exec time.
if [[ ! -r /app/opentelemetry-javaagent.jar ]]; then
	echo "ingestion: FATAL — OTEL javaagent not found: /app/opentelemetry-javaagent.jar" >&2
	exit 2
fi

# Export bridge path so Java ProcessBuilder can find it
export ARROW_BRIDGE_BIN="$BRIDGE_BIN"

MAIN_CLASS="${INGESTION_MAIN_CLASS:-com.trading.ingestion.IngestionService}"
# --add-opens is required by the Fluss client's shaded Arrow (MemoryUtil
# touches java.nio internals on JDK 17+) — must match the host launchers and
# surefire so container behaviour equals the verified host run path.
# 2026-08-22 single-pane: Javaagent auto-exports jvm.* + traces via env
# OTEL_* set in Dockerfile (agent at /app/opentelemetry-javaagent.jar). The
# -javaagent arg is injected here so it works even with custom MAIN_CLASS.
exec java -javaagent:/app/opentelemetry-javaagent.jar \
	--add-opens=java.base/java.nio=ALL-UNNAMED \
	-Dlog.dir="${LOG_DIR:-/data/ingestion/logs}" \
	-cp /app/ingestion.jar "${MAIN_CLASS}"
