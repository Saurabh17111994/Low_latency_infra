#!/usr/bin/env bash
# =============================================================================
# perf-evidence-collector.sh — evidence capture for PERF-AUDIT-001 (the
# controlled measurement run). Runs ALONGSIDE bench-throughput.sh (load gen)
# and snapshots the audit's 7 evidence categories every INTERVAL seconds:
#   1. Flink per-subtask busy/backpressure/idle + records in/out (aggregated)
#   2. Checkpoint metrics (size, durations, counts)
#   3. Memory split (docker stats per container)
#   4. (GAP) Fluss client internals — documented gap, no clean source
#   5. (GAP) Fluss server internals — documented gap
#   6. Data distribution (per-vertex records-in)
#   7. Latency (O2 append_latency_ms count/sum; p99 fallback = mean)
#
# Usage: ./perf-evidence-collector.sh <out-dir> [interval-seconds]
#   out-dir: where snapshots land (logs/tracker-14/perf-audit-001-<ts>/)
#   interval: default 30
#
# Evidence: <out-dir>/snapshots/snap-<n>.json + <out-dir>/summary.tsv
#           <out-dir>/collector.log (stderr from the snapshot step)
#
# Env overrides:
#   FLINK_JM_CONTAINER  JobManager container    (default 01_docker-flink-jobmanager-1)
#   JOB_NAME_PATTERN    RUNNING job name match  (default "signal")
#   O2_BASE_URL         OpenObserve base URL    (default http://localhost:5080)
#   O2_ENV_FILE         credential file         (default 01_docker/secrets.env)
#   RUN_FOR_S           stop after N seconds    (default 0 = run until signalled)
#
# P6-011: this loop is deliberately NOT `set -e`. A poller that aborts the whole
# PERF-AUDIT-001 capture because one `docker exec` was slow turns a transient
# into total evidence loss — proved 2026-09-16, where a failing `docker` made
# iteration 1 the last one. Every failure is recorded per snapshot instead,
# counted, and reported at the end, so a capture that is broken throughout still
# says so loudly rather than quietly producing nothing.
# =============================================================================
set -uo pipefail

OUT_DIR="${1:?usage: perf-evidence-collector.sh <out-dir> [interval]}"
INTERVAL="${2:-30}"

# P6-463: 'abc'/'0'/'-5' used to reach `sleep` (fatal under set -e) or busy-loop.
case "$INTERVAL" in
'' | *[!0-9]*)
	echo "interval must be a positive integer, got '$INTERVAL'" >&2
	exit 2
	;;
esac
[ "$INTERVAL" -ge 1 ] || { echo "interval must be >= 1, got '$INTERVAL'" >&2; exit 2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PARSE_PY="$SCRIPT_DIR/perf_evidence_parse.py"
SNAP_DIR="$OUT_DIR/snapshots"

[ -f "$PARSE_PY" ] || { echo "missing companion $PARSE_PY" >&2; exit 2; }

# P6-135: a renamed compose project or a renamed job used to yield silent empty
# snapshots and exit 0. Both are configurable now.
JM_CONTAINER="${FLINK_JM_CONTAINER:-01_docker-flink-jobmanager-1}"
JOB_PATTERN="${JOB_NAME_PATTERN:-signal}"
O2_BASE="${O2_BASE_URL:-http://localhost:5080}"
O2_ENV_FILE="${O2_ENV_FILE:-$PROJECT_ROOT/code/01_platform/01_docker/secrets.env}"
RUN_FOR_S="${RUN_FOR_S:-0}"
case "$RUN_FOR_S" in
'' | *[!0-9]*)
	echo "RUN_FOR_S must be a non-negative integer, got '$RUN_FOR_S'" >&2
	exit 2
	;;
esac

# The MIB/column layout comes from the module, so header and rows cannot drift.
column_header() {
	python3 -c "
import sys
sys.path.insert(0, sys.argv[1])
from perf_evidence_parse import header_row
print(header_row())
" "$SCRIPT_DIR"
}

# P6-029: the credential travels in a 0600 curl config file, never on a command
# line where /proc/<pid>/cmdline exposes it. Removed by the trap and at exit.
O2_AUTH_FILE=""
read_o2_auth() {
	awk '/^O2_AUTH_BASIC=/{sub(/^[^=]*=/,""); gsub(/\r/,""); gsub(/^["'"'"']|["'"'"']$/,""); print; exit}' "$1" 2>/dev/null || true
}
if [ -f "$O2_ENV_FILE" ]; then
	AUTH="$(read_o2_auth "$O2_ENV_FILE")"
	if [ -n "$AUTH" ]; then
		O2_AUTH_FILE="$(umask 077; mktemp "${TMPDIR:-/tmp}/perf-o2auth.XXXXXX")"
		printf 'header = "Authorization: Basic %s"\n' "$AUTH" > "$O2_AUTH_FILE"
	fi
	unset AUTH
fi

cleanup() {
	rm -f "${O2_AUTH_FILE:-}"
}
trap cleanup INT TERM EXIT

if ! mkdir -p "$SNAP_DIR"; then
	echo "cannot create $SNAP_DIR (is $OUT_DIR writable?)" >&2
	exit 2
fi

if ! column_header > "$OUT_DIR/summary.tsv"; then
	echo "cannot write $OUT_DIR/summary.tsv" >&2
	exit 2
fi

# P6-463: a rerun used to restart at n=1 and overwrite the previous capture's
# snapshots. Continue after the highest existing index instead.
n="$(find "$SNAP_DIR" -maxdepth 1 -name 'snap-*.json' 2>/dev/null \
	| sed -n 's|.*/snap-\([0-9]\+\)\.json$|\1|p' | sort -n | tail -1)"
n="${n:-0}"

STOP=0
snapshots=0
errored=0
start_epoch="$SECONDS"

# P6-463: without this, Ctrl-C killed the collector mid-snapshot and the capture
# ended with no line saying how far it got. The handler only flips a flag; the
# loop notices after the in-flight snapshot finishes, so evidence is not truncated.
trap 'STOP=1' INT TERM

echo "perf-evidence: every ${INTERVAL}s -> $SNAP_DIR (jm=$JM_CONTAINER job~$JOB_PATTERN)"
echo "perf-evidence: O2 ${O2_BASE}$([ -n "$O2_AUTH_FILE" ] && echo " (credential loaded)" || echo " (no credential — latency will be unavailable)")"

while [ "$STOP" -eq 0 ]; do
	n=$((n + 1))
	# Stamped BEFORE the queries, so ts describes when the sample started rather
	# than when a slow JobManager finally answered (P6-466).
	STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

	payload="$(python3 -c "
import json, sys
print(json.dumps({
    'snapshot': int(sys.argv[1]),
    'out_dir': sys.argv[2],
    'started_at': sys.argv[3],
    'jm_container': sys.argv[4],
    'job_name_pattern': sys.argv[5],
    'o2_base': sys.argv[6],
    'o2_auth_file': sys.argv[7],
}))
" "$n" "$OUT_DIR" "$STARTED_AT" "$JM_CONTAINER" "$JOB_PATTERN" "$O2_BASE" "${O2_AUTH_FILE:-}")"

	if printf '%s' "$payload" | python3 "$PARSE_PY" >>"$OUT_DIR/summary.tsv" 2>>"$OUT_DIR/collector.log"; then
		snapshots=$((snapshots + 1))
		# A snapshot that recorded gaps is still evidence, but it is counted, so
		# the run cannot finish claiming a clean capture (P6-011).
		if python3 -c "
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(1)
sys.exit(0 if d.get('errors') else 1)
" "$SNAP_DIR/snap-$n.json"; then
			errored=$((errored + 1))
		fi
	else
		errored=$((errored + 1))
		printf '%s\n' "$(python3 -c "
import sys
sys.path.insert(0, sys.argv[1])
from perf_evidence_parse import error_row
print(error_row(int(sys.argv[2]), 'snapshot failed'))
" "$SCRIPT_DIR" "$n")" >>"$OUT_DIR/summary.tsv"
	fi

	[ "$STOP" -eq 1 ] && break
	if [ "$RUN_FOR_S" -gt 0 ] && [ "$((SECONDS - start_epoch))" -ge "$RUN_FOR_S" ]; then
		break
	fi
	sleep "$INTERVAL" &
	wait $! 2>/dev/null || true
done

cleanup
echo "perf-evidence: ${snapshots} snapshot(s) written, ${errored} with errors -> $OUT_DIR/summary.tsv"
if [ "$errored" -gt 0 ] && [ "$snapshots" -eq 0 ]; then
	echo "perf-evidence: NO snapshot succeeded — this capture is not usable evidence" >&2
	exit 1
fi
exit 0
