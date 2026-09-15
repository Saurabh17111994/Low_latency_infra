#!/usr/bin/env bash
# soak-reconnect-loop.sh — force bridge crash-restarts and verify NO leak.
#
# ALIGNED WITH THE SERVICE'S ACTUAL RESTART SEMANTICS (review R-001):
#   * The Go bridge installs a SIGTERM handler, so a plain `kill` makes it
#     exit cleanly (code 0). IngestionService treats exit code 0 as a
#     *requested* exit (NO_RESTART) and shuts the whole pipeline down. Only a
#     crash (non-zero exit) triggers the restart path.
#   * IngestionService.MAX_BRIDGE_RESTARTS = 1: the JVM restarts the bridge
#     exactly ONCE per process run; a second crash is TERMINAL (the JVM exits
#     cleanly with the restart budget exhausted — that is correct behavior,
#     not a failure).
#   * Therefore this script drives exactly ONE crash-restart cycle by default:
#     `kill -9` the bridge (a genuine crash), verify Java restarts it and the
#     process is healthy after the restart, and confirm the FD/thread counts
#     return to the pre-kill baseline.
#
# Leak metrics are compared against a HEALTHY PRE-KILL baseline sampled before
# the first kill (R-170, P6-205) and re-checked on every cycle, so the default
# single-cycle run can fail on a leak instead of only comparing after-vs-after.
# The Java PID is re-discovered every cycle (R-173), never reused from a stale
# capture.
#
# Recovery is POLLED up to SETTLE_SEC rather than slept for a fixed interval
# (P6-204): a JVM fork + resubscribe + journal flush can outlast any fixed
# sleep, and a fast restart should not have to wait one out.
#
# Proves plan §1377 for the service's bounded restart window: no socket,
# child-process, thread, or goroutine leak across the restart it actually
# performs. Non-destructive: never touches Fluss data, never places orders.
#
# Seams (env, for tests and non-default layouts):
#   LOG_FILE, OUT_DIR, JAVA_MATCH, BRIDGE_MATCH, INGESTION_SRC, PROC_ROOT
#   CONTAINER    ingestion container name; when set, discovery/SIGKILL/grep run
#                inside its PID namespace via `docker exec` (R-222)
#   CYCLES/SETTLE_SEC  also positional: ./soak-reconnect-loop.sh [cycles] [settle_seconds]
#
# Usage:  ./soak-reconnect-loop.sh [cycles] [settle_seconds]
#   e.g.   ./soak-reconnect-loop.sh 1 8    # one crash-restart, 8s settle budget
#          ./soak-reconnect-loop.sh        # default: the service's restart
#                                          # budget (1, from MAX_BRIDGE_RESTARTS)
#
# Cycles above the restart budget are refused (exit 2): killing the bridge
# beyond the JVM's restart budget is TERMINAL by design.
#
# Exit: 0 every cycle recovered with no leak signal; 1 a cycle failed (NOT
#       RECOVERED / LEAK / unverifiable kill target) or the subject is
#       unobservable; 2 usage error.

set -euo pipefail

# ── Config (override via env; defaults derived from the script location) ─────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
LOG_FILE="${LOG_FILE:-$PROJECT_ROOT/code/logs/ingestion.json}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak}"
JAVA_MATCH="${JAVA_MATCH:-com.trading.ingestion.IngestionService}"
BRIDGE_MATCH="${BRIDGE_MATCH:-arrow-bridge}"
INGESTION_SRC="${INGESTION_SRC:-$PROJECT_ROOT/code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java}"
PROC_ROOT="${PROC_ROOT:-/proc}"

# Container mode (R-222): Stage 3/4 run the java inside the ingestion
# container, so process discovery + SIGKILL + journal reads must target its PID
# namespace. Host mode (Stage 2 marathon) is the default: empty CONTAINER.
CONTAINER="${CONTAINER:-}"
if [ -n "$CONTAINER" ]; then
	docker inspect "$CONTAINER" >/dev/null 2>&1 || {
		echo "FATAL: container '$CONTAINER' not found — start the ingestion container first." >&2
		exit 1
	}
	NS() { docker exec "$CONTAINER" "$@"; }
else
	NS() { "$@"; }
fi

# The one progress signal Java actually writes to the journal for every bridge
# lifecycle event (subscription_ack ACTIVE after a restart = recovery). Tick
# NDJSON from the bridge is consumed in-process and never logged (R-004), so
# per-tick lines cannot be used.
PROGRESS_PATTERN="bridge lifecycle event="

# Service restart budget — read from source so it can never drift. A silent
# fallback hid a renamed constant from the operator (P6-548), so the fallback
# is announced.
resolve_restart_budget() {
	local n
	n="$({
		grep -oE 'MAX_BRIDGE_RESTARTS[[:space:]]*=[[:space:]]*[0-9]+' "$INGESTION_SRC" 2>/dev/null || true
	} | grep -oE '[0-9]+$' | head -1 || true)"
	if [ -z "$n" ]; then
		echo "reconnect-loop: WARN — MAX_BRIDGE_RESTARTS not found in $INGESTION_SRC; defaulting the restart budget to 1" >&2
		n=1
	fi
	echo "$n"
}
RESTART_BUDGET="${RESTART_BUDGET:-$(resolve_restart_budget)}"

CYCLES="${1:-$RESTART_BUDGET}"
SETTLE_SEC="${2:-8}"

# Leak thresholds vs the pre-kill healthy baseline (R-170).
LEAK_FD_MARGIN_PCT="${LEAK_FD_MARGIN_PCT:-50}" # allow 50% FD growth before failing
LEAK_THREAD_MARGIN="${LEAK_THREAD_MARGIN:-20}" # allow 20 threads growth before failing

fatal() {
	echo "FATAL: $*" >&2
	exit 1
}
usage_fail() {
	echo "FATAL: $*" >&2
	exit 2
}
is_uint() { case "$1" in '' | *[!0-9]*) return 1 ;; *) return 0 ;; esac; }

# Every numeric knob is validated before use: under set -e a non-numeric CYCLES
# aborted at the first `-gt` test with "integer expression expected", a
# non-numeric SETTLE_SEC failed inside `sleep`, and CYCLES=0 printed "all 0
# cycle(s) recovered cleanly" with exit 0 (P6-549).
validate_args() {
	local v
	for v in CYCLES RESTART_BUDGET SETTLE_SEC LEAK_FD_MARGIN_PCT LEAK_THREAD_MARGIN; do
		is_uint "${!v}" || usage_fail "$v='${!v}' is not a non-negative integer"
	done
	[ "$CYCLES" -ge 1 ] || usage_fail "CYCLES must be >= 1 — a zero-cycle run proves nothing"
	[ "$SETTLE_SEC" -ge 1 ] || usage_fail "SETTLE_SEC must be >= 1 second"
	if [ "$CYCLES" -gt "$RESTART_BUDGET" ]; then
		usage_fail "requested $CYCLES cycles but the service restarts the bridge at most $RESTART_BUDGET time(s) per run (MAX_BRIDGE_RESTARTS). A further kill is TERMINAL by design. Use CYCLES <= $RESTART_BUDGET."
	fi
}

# Newest matching PID (not numerically highest — R-173): etimes is elapsed time
# since start, so the SMALLEST etimes is the most recently started process —
# `tail -1` picked the OLDEST, so a stale leftover process was sampled and
# SIGKILLed (P6-201). Matches the FULL command line (the java class is not in
# argv[0]), and the pattern is passed via the environment so the awk process's
# own cmdline cannot self-match.
find_pid() {
	local rows
	# export (not VAR=... command): the awk is a pipeline sibling, so a
	# command-scoped assignment never reaches its ENVIRON. No early-exit
	# SIGPIPE under pipefail: the ps output is fully drained by awk first.
	export PAT="$1"
	rows="$(NS ps -eo pid,etimes,args 2>/dev/null | awk 'index($0, ENVIRON["PAT"]) { print $1, $2 }')" || return 0
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
	n=$(NS ls "$PROC_ROOT/$pid/fd" 2>/dev/null | wc -l) || n=0
	echo "$n"
}
threads_of() {
	local pid="$1"
	[ -z "$pid" ] && {
		echo 0
		return
	}
	local n
	n=$(NS grep -s '^Threads:' "$PROC_ROOT/$pid/status" 2>/dev/null | awk '{print $2}') || n=0
	[ -n "$n" ] || n=0
	echo "$n"
}
# Journal progress lines. In container mode the journal lives in the
# container's namespace, so the count has to be taken there too — grepping the
# host path read a stale/missing file and every cycle reported NOT RECOVERED
# (P6-202).
count_progress() {
	local n
	if [ -n "$CONTAINER" ]; then
		n=$(NS grep -Fc "$PROGRESS_PATTERN" "$LOG_FILE" 2>/dev/null) || n=0
	else
		n=$(grep -Fc "$PROGRESS_PATTERN" "$LOG_FILE" 2>/dev/null) || n=0
	fi
	echo "$n"
}

# A PID matched by substring is not proof of identity (P6-203): confirm the
# process still matches BRIDGE_MATCH and that its parent IS the discovered Java
# PID before SIGKILLing it. Combined with the oldest-PID bug, the old
# unchecked `kill -9` could destroy an unrelated `arrow-bridge`-named process.
bridge_pid_ok() {
	local pid="$1" java_pid="$2" line ppid
	[ -n "$pid" ] || return 1
	line="$(NS ps -o pid=,ppid=,args= -p "$pid" 2>/dev/null || true)"
	[ -n "$line" ] || return 1
	case "$line" in
	*"$BRIDGE_MATCH"*) ;;
	*) return 1 ;;
	esac
	ppid="$(printf '%s\n' "$line" | awk '{print $2}')"
	[ -n "$ppid" ] && [ "$ppid" = "$java_pid" ]
}

# Force the disconnect: SIGKILL only — a clean SIGTERM exit would be treated as
# a requested shutdown and stop the whole pipeline (R-001). The target is
# verified first (P6-203); refusing to kill is a cycle failure, because a cycle
# that never disconnected proves nothing.
kill_bridge() {
	local pid="$1" java_pid="$2"
	if [ -z "$pid" ]; then
		echo "  bridge PID not found — cannot force the disconnect" >&2
		return 1
	fi
	if ! bridge_pid_ok "$pid" "$java_pid"; then
		echo "  bridge PID $pid is not a '$BRIDGE_MATCH' child of Java $java_pid — refusing to SIGKILL" >&2
		return 1
	fi
	NS kill -9 "$pid" 2>/dev/null || true
	return 0
}

# Poll until recovery (Java alive AND a bridge child exists AND a fresh
# lifecycle event was logged) or the SETTLE_SEC deadline. Prints
# "<java_pid_after> <bridge_pid_after> <progress_now>" and lets the caller
# decide what that means.
wait_recovery() {
	local t0="$1" deadline java_pid_a bridge_pid_a progress_now
	deadline=$(($(date +%s) + SETTLE_SEC))
	while true; do
		java_pid_a="$(find_pid "$JAVA_MATCH")"
		bridge_pid_a="$(find_pid "$BRIDGE_MATCH")"
		progress_now="$(count_progress)"
		progress_now=${progress_now:-0}
		if [ -n "$java_pid_a" ] && [ -n "$bridge_pid_a" ] && [ "$progress_now" -gt "$t0" ]; then
			break
		fi
		[ "$(date +%s)" -ge "$deadline" ] && break
		sleep 1
	done
	printf '%s %s %s\n' "${java_pid_a:-}" "${bridge_pid_a:-}" "${progress_now:-0}"
}

main() {
	validate_args
	[ -f "$LOG_FILE" ] || fatal "journal not found at $LOG_FILE — cannot observe recovery. Is ingestion running?"

	mkdir -p "$OUT_DIR"
	RESULT="$OUT_DIR/reconnect-leak-$(date +%Y%m%d-%H%M%S).tsv"
	echo "reconnect-loop: $CYCLES cycle(s) (service restart budget), ${SETTLE_SEC}s settle budget"
	echo "reconnect-loop: budget=$RESTART_BUDGET  journal=$LOG_FILE"
	echo "reconnect-loop: result → $RESULT"
	echo 'cycle	java_fds_before	java_fds_after	bridge_fds_before	bridge_fds_after	java_threads_before	java_threads_after	progress_delta	recovered_ok	leak_ok' >"$RESULT"

	java_pid_before="$(find_pid "$JAVA_MATCH")"
	if [ -z "$java_pid_before" ]; then
		echo "FATAL: Java ingestion service not running (no process matching '$JAVA_MATCH')." >&2
		echo "Start it first: ./start-all.sh" >&2
		exit 1
	fi

	# Healthy PRE-KILL baseline (R-170, P6-205): sampled before the first kill
	# and compared on every cycle. Without it the first cycle seeded the
	# baseline from post-restart numbers, so a single-cycle run could not fail.
	bridge_pid_base="$(find_pid "$BRIDGE_MATCH")"
	baseline_java_fds="$(count_fds "$java_pid_before")"
	baseline_java_threads="$(threads_of "$java_pid_before")"
	baseline_bridge_fds="$(count_fds "$bridge_pid_base")"
	if [ "$baseline_java_fds" -lt 1 ] || [ "$baseline_java_threads" -lt 1 ]; then
		fatal "cannot read the Java baseline (fds=$baseline_java_fds threads=$baseline_java_threads) — the leak gate would be meaningless"
	fi
	echo "reconnect-loop: Java pid=$java_pid_before, baseline java_fds=$baseline_java_fds java_threads=$baseline_java_threads bridge_fds=$baseline_bridge_fds"

	# Baseline progress count (per-cycle deltas are computed against this).
	t0_progress=$(count_progress)
	echo "reconnect-loop: baseline progress lines=$t0_progress"

	fail=0
	for ((i = 1; i <= CYCLES; i++)); do
		if [ -z "$java_pid_before" ]; then
			echo "  cycle $i: Java PID lost before sampling — cannot continue" >&2
			fail=1
			break
		fi
		jfds_b=$(count_fds "$java_pid_before")
		jthr_b=$(threads_of "$java_pid_before")

		bridge_pid=$(find_pid "$BRIDGE_MATCH")
		bfds_b=$(count_fds "$bridge_pid")

		if ! kill_bridge "$bridge_pid" "$java_pid_before"; then
			echo "  cycle $i: disconnect not forced — the cycle proves nothing" >&2
			fail=1
			break
		fi

		read -r java_pid_after bridge_pid_a progress_now <<<"$(wait_recovery "$t0_progress")"
		jfds_a=$(count_fds "$java_pid_after")
		jthr_a=$(threads_of "$java_pid_after")
		bfds_a=$(count_fds "$bridge_pid_a")
		progress_delta=$((progress_now - t0_progress))

		# Recovered = Java alive AND a bridge child exists AND a fresh lifecycle
		# event (subscription_ack on resubscribe) was logged after the kill.
		recovered=1
		if [ -z "$java_pid_after" ] || [ -z "$bridge_pid_a" ]; then recovered=0; fi
		if [ "$progress_delta" -le 0 ]; then recovered=0; fi

		# Leak check (R-170): after-metrics must stay within margin of the
		# PRE-KILL baseline — on every cycle, first cycle included (P6-205).
		leak_ok=1
		max_fds=$((baseline_java_fds * (100 + LEAK_FD_MARGIN_PCT) / 100))
		max_thr=$((baseline_java_threads + LEAK_THREAD_MARGIN))
		max_bfds=$((baseline_bridge_fds * (100 + LEAK_FD_MARGIN_PCT) / 100))
		if [ "$jfds_a" -gt "$max_fds" ] || [ "$jthr_a" -gt "$max_thr" ] || [ "$bfds_a" -gt "$max_bfds" ]; then
			leak_ok=0
		fi

		printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
			"$i" "$jfds_b" "$jfds_a" "$bfds_b" "$bfds_a" \
			"$jthr_b" "$jthr_a" "$progress_delta" "$recovered" "$leak_ok" >>"$RESULT"

		if [ "$recovered" = "0" ] || [ "$leak_ok" = "0" ]; then
			echo "  cycle $i: $([ "$recovered" = 0 ] && echo NOT RECOVERED || echo LEAK DETECTED) (java_fds $jfds_b→$jfds_a, bridge_fds $bfds_b→$bfds_a, threads $jthr_b→$jthr_a, progress +$progress_delta)"
			fail=1
		else
			echo "  cycle $i: ok (java_fds $jfds_b→$jfds_a, bridge_fds $bfds_b→$bfds_a, threads $jthr_b→$jthr_a, progress +$progress_delta)"
		fi

		# Next cycle samples the (possibly new) Java process (R-173).
		java_pid_before="$java_pid_after"
		t0_progress="$progress_now"
	done

	echo "reconnect-loop: done. Result → $RESULT"
	if [ "$fail" = "1" ]; then
		echo "reconnect-loop: ⚠️ some cycles failed — inspect $RESULT" >&2
		exit 1
	fi
	echo "reconnect-loop: all $CYCLES cycle(s) recovered cleanly with no leak signal."
}

main "$@"
