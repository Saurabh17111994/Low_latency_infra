#!/usr/bin/env bash
# soak-monitor.sh — sample live ingestion health during the Monday soak.
#
# Samples every INTERVAL_SECONDS:
#   - Java process open FDs (PROC_ROOT/<pid>/fd)
#   - Go bridge child open FDs (PROC_ROOT/<bridge_pid>/fd)
#   - Java JVM threads (PROC_ROOT/<pid>/status Threads:)
#   - Go bridge OS threads (PROC_ROOT/<pid>/status Threads:) — NOTE: a goroutine
#     leak does NOT necessarily raise the OS-thread count (the Go runtime
#     multiplexes goroutines over a small thread pool), so this column is a
#     weak proxy; the authoritative signal is the bridge's own
#     runtime.NumGoroutine() telemetry event (R-169, Phase 4).
#   - bridge lifecycle events from the JSON journal: reconnect /
#     heartbeat_failed / feed_stalled / subscription_ack. These are mirrored
#     into the journal by Java ("bridge lifecycle event=..."); the raw bridge
#     NDJSON tick lines are consumed in-process and never logged (R-020), so
#     per-tick rate is NOT journal-observable — it requires the bridge NDJSON
#     side-channel or the OTLP metrics endpoint (observability phase).
#
# Reads the journal INCREMENTALLY (one byte window per interval, from the last
# offset) and reports that window's counts, so a long soak never re-scans a
# 64 MB journal (R-137). Rotation and truncation are detected by inode + size
# and counted from the start of the new file, so a rotated journal loses no
# events (P6-199).
#
# G3 self-check: the monitor FAILS immediately if it cannot observe its subject
# (journal missing, or the Java *or* bridge process not found at startup) —
# recording bridge_fds=0 forever is not observation (P6-197).
#
# Seams (env, for tests and non-default layouts):
#   LOG_FILE     journal path            (default code/logs/ingestion.json)
#   OUT_DIR      evidence directory      (default logs/soak)
#   JAVA_MATCH   Java process pattern    (default com.trading.ingestion.IngestionService)
#   BRIDGE_MATCH bridge process pattern  (default arrow-bridge)
#   PROC_ROOT    /proc equivalent        (default /proc)
#
# Usage:  ./soak-monitor.sh [duration_seconds] [interval_seconds]
#   e.g.   ./soak-monitor.sh 3600 10     # 1 hour, every 10s
#          ./soak-monitor.sh             # forever until Ctrl+C
#
# Exit: 0 duration reached; 1 FATAL (bad arguments, or the subject/journal
#       cannot be observed).

set -euo pipefail

# ── Config (override via env; defaults derived from the script location) ─────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
LOG_FILE="${LOG_FILE:-$PROJECT_ROOT/code/logs/ingestion.json}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak}"
JAVA_MATCH="${JAVA_MATCH:-com.trading.ingestion.IngestionService}"
BRIDGE_MATCH="${BRIDGE_MATCH:-arrow-bridge}"
PROC_ROOT="${PROC_ROOT:-/proc}"

DURATION_SEC="${1:-0}" # 0 = run forever
INTERVAL_SEC="${2:-5}" # default 5s

fatal() {
	echo "FATAL: $*" >&2
	exit 1
}

is_uint() { case "$1" in '' | *[!0-9]*) return 1 ;; *) return 0 ;; esac; }

# Args are validated before anything is read: a non-numeric duration would abort
# on the `-gt` test under set -e, and INTERVAL_SEC=0 or garbage would make every
# `sleep` fail (P6-545).
validate_args() {
	is_uint "$DURATION_SEC" || fatal "duration '$DURATION_SEC' must be a non-negative integer (seconds)"
	is_uint "$INTERVAL_SEC" || fatal "interval '$INTERVAL_SEC' must be a positive integer (seconds)"
	[ "$INTERVAL_SEC" -ge 1 ] || fatal "interval must be >= 1 second"
}

# Newest matching PID (not numerically highest — R-057): etimes is elapsed time
# since start, so the SMALLEST etimes is the most recently started process.
# The pattern is matched against the FULL command line — for the JVM the class
# name lives in argv[4+], never in argv[3] ("java") (P6-018) — and it is passed
# through the environment so the awk process's own cmdline cannot self-match.
# Both PIDs are resolved in the same sample so a restart can never mix
# generations.
find_pid() {
	local rows
	# ps output is fully drained by awk (no early-exit SIGPIPE under pipefail,
	# which would make the substitution fail and abort under set -e).
	rows="$(ps -eo pid,etimes,args 2>/dev/null | PAT="$1" SELF="$$" awk '
			index($0, ENVIRON["PAT"]) && $1 != ENVIRON["SELF"] { print $1, $2 }')" || return 0
	[ -n "$rows" ] || return 0 # no match prints nothing at all (not a blank line)
	printf '%s\n' "$rows" | sort -k2 -n | awk 'NR==1 { print $1 }'
}

count_fds() {
	local pid="$1"
	[ -z "$pid" ] && {
		echo 0
		return
	}
	local n
	n=$(ls "$PROC_ROOT/$pid/fd" 2>/dev/null | wc -l) || n=0 # R-021: never abort on a dead PID
	echo "$n"
}
threads_of() {
	local pid="$1"
	[ -z "$pid" ] && {
		echo 0
		return
	}
	local n
	n=$(grep -s '^Threads:' "$PROC_ROOT/$pid/status" 2>/dev/null | awk '{print $2}') || n=0
	[ -n "$n" ] || n=0
	echo "$n"
}

# Journal identity/size — used to detect rotation (inode change) and
# truncation (size shrink) instead of silently reading past EOF (P6-199).
journal_size() { wc -c <"$LOG_FILE" 2>/dev/null | tr -d ' ' || true; }
journal_inode() { stat -c %i "$LOG_FILE" 2>/dev/null || true; }

# Count the four lifecycle patterns in ONE pass over the byte window
# [offset, size) (P6-546 — four separate `tail` reads of the same window
# quadrupled journal I/O and held the whole delta in a shell variable), and
# never let a failing `grep -c` contribute a second line to the count (P6-196).
# Prints "subs rec hb stall".
count_window() {
	local offset="$1" size="$2" window
	window=$((size - offset))
	[ "$window" -gt 0 ] || {
		printf '0 0 0 0'
		return 0
	}
	tail -c +"$((offset + 1))" "$LOG_FILE" 2>/dev/null |
		head -c "$window" |
		awk '
			/bridge lifecycle event=subscription_ack/ { subs++ }
			/bridge lifecycle event=reconnect/ { rec++ }
			/bridge lifecycle event=heartbeat_failed/ { hb++ }
			/bridge lifecycle event=feed_stalled/ { stall++ }
			END { printf "%d %d %d %d", subs + 0, rec + 0, hb + 0, stall + 0 }'
}

main() {
	validate_args
	[ -f "$LOG_FILE" ] || fatal "journal not found at $LOG_FILE — cannot observe the soak. Set LOG_FILE."

	# Startup observation check: BOTH subjects must exist or there is nothing to
	# soak. A missing bridge used to be recorded as bridge_fds=0 forever (P6-197).
	if [ -z "$(find_pid "$JAVA_MATCH")" ]; then
		fatal "no Java ingestion process matching '$JAVA_MATCH' — cannot monitor."
	fi
	if [ -z "$(find_pid "$BRIDGE_MATCH")" ]; then
		fatal "no bridge process matching '$BRIDGE_MATCH' — cannot monitor."
	fi

	mkdir -p "$OUT_DIR"
	TSV="$OUT_DIR/soak-summary-$(date +%Y%m%d-%H%M%S).tsv"
	echo "soak-monitor: log=$LOG_FILE"
	echo "soak-monitor: writing $TSV"
	echo "soak-monitor: interval=${INTERVAL_SEC}s duration=${DURATION_SEC}"
	printf 'ts\tjava_fds\tbridge_fds\tjava_threads\tbridge_os_threads\tsub_acks\treconnects\theartbeat_fails\tstalls\n' >"$TSV"

	start_epoch=$(date +%s)
	log_offset=$(journal_size)
	log_inode=$(journal_inode)
	[ -n "$log_offset" ] && [ -n "$log_inode" ] || fatal "journal not readable at $LOG_FILE"

	while true; do
		java_pid=$(find_pid "$JAVA_MATCH")
		bridge_pid=$(find_pid "$BRIDGE_MATCH")
		now=$(date '+%Y-%m-%d %H:%M:%S')

		jfds=$(count_fds "$java_pid")
		bfds=$(count_fds "$bridge_pid")
		jthr=$(threads_of "$java_pid")
		bthr=$(threads_of "$bridge_pid")

		# Rotation/truncation check BEFORE reading: a new inode or a shrunken
		# file means the bytes at the old offset belong to the old journal, so
		# the new file is counted from its start (P6-199).
		cur_size=$(journal_size)
		cur_inode=$(journal_inode)
		if [ -z "$cur_size" ] || [ -z "$cur_inode" ]; then
			fatal "journal disappeared at $LOG_FILE during the soak"
		fi
		if [ "$cur_inode" != "$log_inode" ] || [ "$cur_size" -lt "$log_offset" ]; then
			echo "soak-monitor: journal rotated or truncated — counting from 0"
			log_offset=0
			log_inode="$cur_inode"
		fi

		# The window counts ARE the per-interval deltas: subtracting a previous
		# cumulative total made every interval after the first negative (P6-198).
		counts="$(count_window "$log_offset" "$cur_size" || true)"
		read -r subs rec hb stall <<<"$counts"
		subs=${subs:-0}
		rec=${rec:-0}
		hb=${hb:-0}
		stall=${stall:-0}

		printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
			"$now" "$jfds" "$bfds" "$jthr" "$bthr" \
			"$subs" "$rec" "$hb" "$stall" >>"$TSV"

		printf '  %s java_fds=%s bridge_fds=%s java_threads=%s bridge_os_threads=%s sub_acks=%s rec=%s hb_fail=%s stalls=%s\n' \
			"$now" "$jfds" "$bfds" "$jthr" "$bthr" \
			"$subs" "$rec" "$hb" "$stall"

		log_offset=$cur_size

		# Run forever, or stop after duration
		if [ "$DURATION_SEC" -gt 0 ]; then
			elapsed=$(($(date +%s) - start_epoch))
			[ "$elapsed" -ge "$DURATION_SEC" ] && {
				echo "soak-monitor: done (${elapsed}s) → $TSV"
				return 0
			}
		fi
		sleep "$INTERVAL_SEC"
	done
}

main "$@"
