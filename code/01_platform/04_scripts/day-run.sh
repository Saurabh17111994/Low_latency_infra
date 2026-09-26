#!/usr/bin/env bash
# day-run.sh — thin entry for the daily single-command runner (CHG-324).
#
#   make day ARGS="start|status|stop"
#
# All logic lives in day_run.py (pure decisions + read-only probes; tests in
# tests/test_day_run.py). This wrapper only pins the interpreter.
set -euo pipefail
exec python3 "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/day_run.py" "$@"
