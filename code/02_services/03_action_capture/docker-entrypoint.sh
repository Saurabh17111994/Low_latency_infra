#!/usr/bin/env bash
# Action-capture entrypoint: subscribe to postbacks, write Fills + KV lifecycle.
set -euo pipefail
# Assign (not just display) the fallback so the JVM sees the same endpoint we log.
export FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-fluss-coordinator:9123}"
echo "action-capture: starting (FLUSS_BOOTSTRAP=${FLUSS_BOOTSTRAP})"
# JAVA_OPTS split is intentional; CMD/args are forwarded so ENTRYPOINT+CMD holds.
# shellcheck disable=SC2086
exec java ${JAVA_OPTS:-} -jar /app/action-capture.jar "$@"
