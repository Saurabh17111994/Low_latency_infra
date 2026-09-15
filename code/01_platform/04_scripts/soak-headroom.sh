#!/usr/bin/env bash
# soak-headroom.sh — subscription headroom evidence for the whole session.
#
# Proves plan §1379: "subscription headroom is observable and alerted." It
# reads the live ingestion JSON journal and derives headroom from the slot
# lifecycle events mirrored there by Java ("bridge lifecycle event=..."):
#   capacity_used = acknowledged_tokens / assigned_tokens  (per subscription_ack)
#   headroom      = 1 - capacity_used  (0 = fully subscribed, no room)
#
# On the 1-slot / 1024-instrument envelope, a full ack is 1024/1024 = 100% of
# the assignment — and the LIMIT for one connection is MaxHFTTokensPerConnection
# (see subscription_plan.go). Headroom is reported BOTH against the assignment
# (acked/assigned) and against the per-connection cap (spare tokens), per slot
# and as a global worst case, so a multi-slot session cannot be read as one
# connection's number (P6-541).
#
# The script also reports any subscription PARTIAL/TERMINAL/REJECTED events
# (real evidence of tightness).
#
# If you later run OTel/Prometheus, this can read bridge.slot.capacity_used_percent
# directly instead — but the log-derived version works today with zero deps.
#
# The journal is read ONCE into a snapshot and every section is derived from
# that snapshot, so a rotation or a fresh ack landing mid-report cannot make
# the ack table, the tightness list and the statistics disagree (P6-193).
#
# Exit status is the machine-readable alert channel (P6-544):
#   0 pass · 1 FATAL (unusable arguments/journal) · 3 AT CAPACITY · 4 no ack rows.
#
# Usage:  ./soak-headroom.sh [log_file]
#   e.g.   ./soak-headroom.sh                 # default code/logs/ingestion.json
#          ./soak-headroom.sh logs/ingestion-2026-08-03.log
#
# Seams (env, for tests and non-default layouts):
#   LOG_FILE     journal path        (default code/logs/ingestion.json)
#   OUT_DIR      evidence directory  (default logs/soak)
#   CAP_TOKENS   per-connection cap  (default 1024, must be a positive integer)
#   SLOT         optional slot filter, e.g. SLOT=hft-1

set -euo pipefail

# ── Config (override via env; defaults derived from the script location) ─────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
# Precedence: CLI argument, then the LOG_FILE env seam (the header documents it,
# and the wave-28 monitor honours it the same way), then the repo default.
LOG_FILE="${1:-${LOG_FILE:-$PROJECT_ROOT/code/logs/ingestion.json}}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak}"
SLOT="${SLOT:-}"

# The journal message format (IngestionService.java:1476):
#   bridge lifecycle event=subscription_ack slot=... state=ACTIVE epoch=1
#   assigned=1024 acknowledged=1024 rejected=0 reason=...
# The regex tolerates the epoch= token between state= and assigned= (R-018) and
# keeps slot= in the tokens it extracts, so every row stays attributable to a
# connection (P6-541).
ACK_LINE_PATTERN='bridge lifecycle event=subscription_ack'
ACK_TOKENS_PATTERN='(slot=[^ ]+ )?state=[A-Z]+( epoch=[0-9]+)? assigned=[0-9]+ acknowledged=[0-9]+ rejected=[0-9]+'

fatal() {
	echo "FATAL: $*" >&2
	exit 1
}

# CAP_TOKENS flows into the cap-side arithmetic, so a zero or non-numeric value
# would divide by zero or print NaN (P6-540). Validated before anything is read;
# the validated value is printed for the caller to use.
resolve_cap() {
	local cap="${CAP_TOKENS:-1024}"
	case "$cap" in
	'' | *[!0-9]*) fatal "CAP_TOKENS must be a positive integer (got '${CAP_TOKENS:-}')" ;;
	esac
	[ "$cap" -gt 0 ] || fatal "CAP_TOKENS must be a positive integer (got '$cap')"
	printf '%s\n' "$cap"
}

# One grep of the journal into the snapshot every later section reads (P6-193).
# With SLOT set the snapshot is additionally narrowed to that connection, so a
# per-slot reading of a multi-slot session is possible (P6-541).
snapshot() {
	local snap="$1"
	if [ -n "$SLOT" ]; then
		grep -E "$ACK_LINE_PATTERN" "$LOG_FILE" | grep -F "slot=$SLOT " >"$snap" || true
	else
		grep -E "$ACK_LINE_PATTERN" "$LOG_FILE" >"$snap" || true
	fi
}

ack_table() {
	grep -oE "$ACK_TOKENS_PATTERN" "$1" | sort | uniq -c || true
}

# Tightness evidence (P6-194): an ack whose state is not ACTIVE, or that
# rejected tokens. The old exclusion expected `rejected=0` to follow `state=`
# directly, which no real line does, so every ack looked tight; and with no
# acks at all it printed the all-clean line. Both are fixed here: the state and
# rejected fields are parsed, and the empty snapshot is reported as empty.
tightness() {
	if [ ! -s "$1" ]; then
		echo "  (no subscription_ack rows — no tightness evidence either way)"
		return 0
	fi
	awk '
		{
			state = ""; rej = 0
			for (i = 1; i <= NF; i++) {
				if ($i ~ /^state=/)    { state = substr($i, 7) }
				if ($i ~ /^rejected=/) { rej = substr($i, 10) + 0 }
			}
			if (state != "ACTIVE" || rej != 0) { print; found = 1 }
		}
		END { if (!found) print "  (none — all acks full & clean)" }
	' "$1"
}

# Headroom statistics over the snapshot, printed as the third section. Returns
# 3 when the connection is at or over its capacity and 4 when the snapshot has
# no usable ack rows, so the caller can propagate the alert (P6-544).
#
# awk pass 1 resets assigned/acked per record and skips rows without a positive
# assignment, so a wrapped or drifted line cannot reuse the previous ack's
# numbers (P6-195); the first row seeds min/max explicitly instead of relying on
# empty-string coercion (P6-785); and nothing is sorted in awk — the values are
# emitted and sorted with `sort -n` outside, so the cost stays n log n on a
# day-long journal (P6-786).
stats() {
	local snap="$1" work="$2" cap="$3"
	local n="" min_u="" max_u="" avg="" max_cap="" idx="" p99="" rc=0

	awk -v vals="$work/used.vals" -v slots="$work/slot.vals" -v meta="$work/meta" -v cap="$cap" '
		{
			assigned = 0; acked = 0; slot = "unslotted"
			for (i = 1; i <= NF; i++) {
				if ($i ~ /^slot=/)         { slot = substr($i, 6); if (slot == "") slot = "unslotted" }
				if ($i ~ /^assigned=/)     { assigned = substr($i, 10) + 0 }
				if ($i ~ /^acknowledged=/) { acked = substr($i, 14) + 0 }
			}
			if (assigned <= 0) next
			used = (acked * 100) / assigned
			n++
			cap_used = (acked * 100) / cap
			if (n == 1) { min_u = used; max_u = used; max_cap = cap_used }
			else {
				if (used < min_u) min_u = used
				if (used > max_u) max_u = used
				if (cap_used > max_cap) max_cap = cap_used
			}
			sum += used
			print used > vals
			print slot "\t" used > slots
		}
		END {
			if (n == 0) { print "0 0 0 0 0" > meta; exit 0 }
			printf "%d %.6f %.6f %.6f %.6f\n", n, min_u, max_u, sum / n, max_cap > meta
		}
	' "$snap"

	[ -s "$work/meta" ] || {
		echo "  (statistics pass produced no output — see stderr)"
		return 5
	}
	read -r n min_u max_u avg max_cap <"$work/meta" || true
	n="${n:-0}"
	if [ "$n" -eq 0 ]; then
		echo "  (no subscription_ack rows found — nothing to measure)"
		return 4
	fi

	# Nearest-rank p99 is ceil(0.99 * n), not the rounded rank (P6-542): with
	# n = 68 the round formula picks rank 67 and understates the tail.
	idx="$(awk -v n="$n" 'BEGIN { c = n * 0.99; i = int(c); if (i < c) i++; if (i < 1) i = 1; if (i > n) i = n; print i }')"
	# The awk drains stdin before printing: an early `exit` would SIGPIPE `sort`
	# under pipefail and abort the whole report.
	p99="$(sort -n "$work/used.vals" | awk -v i="$idx" 'NR == i { p = $1 } END { printf "%.1f", p + 0 }')"
	p99="${p99:-0.0}"

	# Per-slot view (P6-541): a global max over mixed connections cannot be read
	# against one connection's cap. The aggregation is order-independent and the
	# output is sorted, so the section is byte-stable; no dedup, so samples= is a
	# row count and identical samples still count.
	if [ -s "$work/slot.vals" ]; then
		awk -F'\t' '
			{ c[$1]++; s[$1] += $2; if (c[$1] == 1 || $2 > mx[$1]) mx[$1] = $2 }
			END { for (k in c) printf "  slot=%s samples=%d max=%.1f%% avg=%.1f%%\n", k, c[k], mx[k], s[k] / c[k] }' \
			"$work/slot.vals" | sort
	fi

	awk -v n="$n" -v min="$min_u" -v max="$max_u" -v avg="$avg" -v p99="$p99" -v cap="$cap" -v maxcap="$max_cap" '
		BEGIN {
			printf "  samples=%d  min=%.1f%%  max=%.1f%%  p99=%.1f%%  avg=%.1f%%  (of assignment)\n", n, min, max, p99, avg
			# The per-connection cap can be exceeded even when the assignment
			# is not: assigned=2048 with acked=1500 is 73% of the assignment
			# but ~146% of the 1024-token cap, and used to report headroom
			# (P6-543). Alert on the cap side too.
			if (maxcap >= 100) printf "  ⚠️ AT CAPACITY (%.1f%% of the %d-token per-connection cap) — over MaxHFTTokensPerConnection; adding instruments requires a 2nd slot/connection (multi-connection approval, plan 816)\n", maxcap, cap
			spare = cap - (cap * maxcap) / 100
			if (spare < 0) printf "  vs per-connection cap: max used=%.1f%%  (OVER cap by %.0f tokens of %d)\n", maxcap, -spare, cap
			else printf "  vs per-connection cap: max used=%.1f%%  (%.0f spare tokens of %d)\n", maxcap, spare, cap
			headroom = 100 - max
			if (headroom <= 0) printf "  ⚠️ AT CAPACITY (%.1f%%) — 0 headroom on this connection; adding instruments requires a 2nd slot/connection (multi-connection approval, plan 816)\n", max
			else printf "  headroom=%.1f%%\n", headroom
			exit ((maxcap >= 100 || headroom <= 0) ? 3 : 0)
		}' || rc=$?
	return "$rc"
}

main() {
	local cap snap rc=0 stats_rc=0
	[ -f "$LOG_FILE" ] || fatal "no journal at $LOG_FILE — cannot report headroom. Set LOG_FILE."
	cap="$(resolve_cap)"
	mkdir -p "$OUT_DIR"

	# Working files (snapshot, emitted values, assembled report) live in one
	# temporary directory inside OUT_DIR and are removed on exit. The report is
	# assembled to a file rather than a pipeline, so the statistics exit status
	# survives to become this script's exit status (P6-544). The trap is
	# installed only after WORK is set, so no earlier `fatal` can trip it.
	WORK="$(mktemp -d "$OUT_DIR/.headroom-work.XXXXXX")"
	trap 'rm -rf "$WORK"' EXIT
	snap="$WORK/snapshot"
	snapshot "$snap"

	# Unique evidence name: two runs in the same second used to overwrite one
	# another's summary (P6-784).
	SUMMARY="$(mktemp "$OUT_DIR/headroom-summary-$(date +%Y%m%d-%H%M%S)-XXXXXX.txt")"

	{
		echo "headroom: scanning $LOG_FILE"
		if [ -n "$SLOT" ]; then
			echo "headroom: slot filter=$SLOT"
		fi
		echo "---"
		echo "Subscription acks (ACTIVE/PARTIAL/TERMINAL):"
		ack_table "$snap"
		echo "---"
		echo "Any partial/terminal/rejected events (tightness evidence):"
		tightness "$snap"
		echo "---"
		echo "Headroom (capacity_used = acknowledged/assigned; also vs the per-connection cap MaxHFTTokensPerConnection=$cap, see subscription_plan.go):"
		stats "$snap" "$WORK" "$cap" || stats_rc=$?
		echo "---"
		echo "Run this periodically during the day; pass = capacity stays below 100% of the assignment and the $cap-token per-connection cap."
	} >"$WORK/report"

	tee "$SUMMARY" <"$WORK/report"
	echo "headroom: summary → $SUMMARY"

	case "$stats_rc" in
	0) rc=0 ;;
	3)
		rc=3
		echo "headroom: ALERT — at capacity; adding instruments requires a 2nd slot/connection (see $SUMMARY)" >&2
		;;
	4)
		rc=4
		echo "headroom: NO DATA — no subscription_ack rows in $LOG_FILE (see $SUMMARY)" >&2
		;;
	*)
		rc="$stats_rc"
		echo "headroom: stats failed (rc=$stats_rc) — see $SUMMARY" >&2
		;;
	esac
	exit "$rc"
}

main "$@"
