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

if [[ -z "${FLUSS_BOOTSTRAP:-}" ]]; then
	echo "ingestion: FATAL — FLUSS_BOOTSTRAP is required" >&2
	exit 2
fi

MANIFEST_PATH="${ARROW_INSTRUMENT_MANIFEST:-${INSTRUMENT_MANIFEST_PATH:-}}"
if [[ -z "$MANIFEST_PATH" || ! -r "$MANIFEST_PATH" ]]; then
	echo "ingestion: FATAL — readable instrument manifest is required: ${MANIFEST_PATH:-<unset>}" >&2
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
