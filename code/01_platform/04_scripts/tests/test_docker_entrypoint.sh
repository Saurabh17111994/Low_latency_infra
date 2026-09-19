#!/usr/bin/env bash
# ING-INT-006 — docker-entrypoint.sh FATAL-path contract (M5).
#
# Asserts the documented exit codes AND messages for the three pre-Java
# FATAL gates of code/02_services/01_ingestion/docker-entrypoint.sh:
#   missing FLUSS_BOOTSTRAP          → exit 2  "FLUSS_BOOTSTRAP is required"
#   missing/unreadable manifest      → exit 2  "readable manifest FILE is required"
#   diverging manifest vars          → exit 2  "manifest vars diverge"
#   legacy INSTRUMENT_MANIFEST_PATH   → satisfies the manifest gate (no ARROW_* var)
#   unwritable LOG_DIR               → exit 2  "LOG_DIR not creatable"        (P1-137)
#   missing/non-executable bridge    → exit 1  "arrow-bridge binary not found or not executable"
#
# Also asserts the FATAL stream contract: the message goes to stderr and stdout
# stays empty (P6-622), and that an empty-string FLUSS_BOOTSTRAP is rejected.
# Every invocation is bounded by timeout(1) — a FATAL gate that regressed into
# falling through would otherwise reach `exec java` and sit there (P6-239).
#
# The guard ORDER is part of the contract: the manifest check runs before the
# P1-137 writability probes, which run before the bridge check. Each case must
# satisfy every earlier guard, or it asserts a later guard's message — which is
# exactly how case 3 rotted when 093d5e5 added the probes (2026-09-08) and case
# 2 when 255da20 reworded the manifest message (2026-09-07).
#
# Pure bash + POSIX tools; no dependencies. Run directly:
#   bash test_docker_entrypoint.sh
# or via the Monday gate (run-monday-gates.sh step 3b), which also applies
# bash -n + shellcheck to this file.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# tests/ → 04_scripts/ → 01_platform/ → code/
# Overridable so the wave-52 guard module can run this suite against a deliberately
# broken fake entrypoint and prove the suite fails (P6-833 scope note).
ENTRYPOINT="${ENTRYPOINT:-$SCRIPT_DIR/../../../02_services/01_ingestion/docker-entrypoint.sh}"

FAILED=0

assert_eq() { # $1=got $2=want $3=label
	if [ "$1" != "$2" ]; then
		echo "FAIL: $3 — got exit=$1 want exit=$2"
		FAILED=1
		return 1
	fi
	echo "ok: $3 (exit=$1)"
}

assert_not_contains() { # $1=haystack $2=needle $3=label
	if [[ "$1" == *"$2"* ]]; then
		echo "FAIL: $3 — unexpected text found: $2"
		FAILED=1
		return 1
	fi
	echo "ok: $3"
}

assert_contains() { # $1=haystack $2=needle $3=label
	if [[ "$1" != *"$2"* ]]; then
		echo "FAIL: $3 — message not found: $2"
		echo "--- captured output ---"
		echo "$1"
		echo "----------------------"
		FAILED=1
		return 1
	fi
	echo "ok: $3"
}

if [ ! -r "$ENTRYPOINT" ]; then
	echo "FAIL: entrypoint not found: $ENTRYPOINT"
	exit 1
fi

# The entrypoint is always invoked explicitly as `bash "$ENTRYPOINT"`, so no exec
# bit is required. Do not chmod it here: that dirties the checkout and fails on a
# read-only mount (P6-831).

# P6-621: there is no `set -e` here, so an empty TMP would make the trap a
# `rm -rf ""` and MANIFEST a root-level `/manifest.csv`. Fail fast instead.
TMP="$(mktemp -d)" || { echo "FAIL: mktemp -d failed" >&2; exit 1; }
if [ -z "${TMP:-}" ] || [ ! -d "$TMP" ]; then
	echo "FAIL: mktemp returned an empty or non-directory path: '${TMP:-}'" >&2
	exit 1
fi
trap 'rm -rf "${TMP:?}"' EXIT
MANIFEST="$TMP/manifest.csv"
: > "$MANIFEST"
BOGUS_BIN="$TMP/no-such-bridge"

# P6-239: bound every invocation. 124 is timeout(1)'s own exit code, so a
# regressed FATAL gate that falls through to `exec java` fails here — named,
# in seconds — instead of burning the gate's 300s file-level timeout.
# Overridable for the guard module's hang mutant; the gate keeps the 10s default.
ENTRYPOINT_TIMEOUT="${ENTRYPOINT_TIMEOUT:-10s}"
TIMEOUT_BIN="$(command -v timeout || true)"

run_entrypoint() { # env-var list; echoes "<exit>|<output>", caller splits it
	local out rc
	# P6-622: `env -i` with no PATH is not the container environment — pass a
	# minimal deterministic one. P6-832: `${@+"$@"}` is the portable form — it
	# expands to zero words when called with no assignments (Case 1), where a bare
	# "$@" is unbound under `set -u` on bash < 4.4 and `${@-}` would pass one
	# EMPTY argument to env.
	if [ -n "$TIMEOUT_BIN" ]; then
		out="$("$TIMEOUT_BIN" "$ENTRYPOINT_TIMEOUT" env -i PATH=/usr/bin:/bin ${@+"$@"} bash "$ENTRYPOINT" 2>&1)"
	else
		out="$(env -i PATH=/usr/bin:/bin ${@+"$@"} bash "$ENTRYPOINT" 2>&1)"
	fi
	rc=$?
	if [ "$rc" = 124 ]; then
		out="$out
FAIL: entrypoint did not exit within $ENTRYPOINT_TIMEOUT — a FATAL gate fell through (P6-239)"
	fi
	printf '%s|%s' "$rc" "$out"
}

# split_run splits the "<exit>|<output>" capture into ENTRY_EXIT and ENTRY_OUT.
split_run() { # $1 = captured string
	ENTRY_EXIT="${1%%|*}"
	ENTRY_OUT="${1#*|}"
}

# ── Case 1: missing FLUSS_BOOTSTRAP → exit 2 ────────────────────────────────
echo "=== ING-INT-006 case 1: missing FLUSS_BOOTSTRAP ==="
split_run "$(run_entrypoint)"
assert_eq "$ENTRY_EXIT" 2 "missing FLUSS_BOOTSTRAP exits 2"
assert_contains "$ENTRY_OUT" "FLUSS_BOOTSTRAP is required" "missing-FLUSS_BOOTSTRAP message"

# ── Case 1b: the FATAL stream contract (P6-622) ──────────────────────────────
echo "=== ING-INT-006 case 1b: FATAL stream contract (stderr yes, stdout no) ==="
if [ -n "$TIMEOUT_BIN" ]; then
	"$TIMEOUT_BIN" "$ENTRYPOINT_TIMEOUT" env -i PATH=/usr/bin:/bin bash "$ENTRYPOINT" >"$TMP/c1b.out" 2>"$TMP/c1b.err"
else
	env -i PATH=/usr/bin:/bin bash "$ENTRYPOINT" >"$TMP/c1b.out" 2>"$TMP/c1b.err"
fi
ENTRY_EXIT=$?
assert_eq "$ENTRY_EXIT" 2 "missing FLUSS_BOOTSTRAP exits 2 (stream-split run)"
# The split capture must have really split: stdout carries the startup lines…
assert_contains "$(cat "$TMP/c1b.out")" "ingestion: starting" "startup line is on stdout"
assert_not_contains "$(cat "$TMP/c1b.out")" "FLUSS_BOOTSTRAP is required" "FATAL message is not on stdout"
# …and the FATAL message is stderr-only (the contract 2>&1 could never see).
assert_contains "$(cat "$TMP/c1b.err")" "FLUSS_BOOTSTRAP is required" "FATAL message is on stderr"

# ── Case 1c: empty-string FLUSS_BOOTSTRAP → exit 2 (P6-833 boundary) ───────────
echo "=== ING-INT-006 case 1c: empty FLUSS_BOOTSTRAP ==="
split_run "$(run_entrypoint FLUSS_BOOTSTRAP=)"
assert_eq "$ENTRY_EXIT" 2 "empty FLUSS_BOOTSTRAP exits 2"
assert_contains "$ENTRY_OUT" "FLUSS_BOOTSTRAP is required" "empty-FLUSS_BOOTSTRAP message"

# ── Case 2: missing manifest → exit 2 ────────────────────────────────────────
echo "=== ING-INT-006 case 2: missing manifest ==="
split_run "$(run_entrypoint FLUSS_BOOTSTRAP=localhost:9123)"
assert_eq "$ENTRY_EXIT" 2 "missing manifest exits 2"
assert_contains "$ENTRY_OUT" "readable manifest FILE is required" "missing-manifest message"

# ── Case 2b: unreadable manifest → exit 2 (P6-620) ───────────────────────────
echo "=== ING-INT-006 case 2b: unreadable manifest ==="
# `-r` is bypassed for root, so this case only means something as a normal user.
UNREADABLE="$TMP/unreadable-manifest.csv"
: > "$UNREADABLE"
chmod 000 "$UNREADABLE"
if [ "$(id -u)" -eq 0 ]; then
	echo "skip: case 2b needs a non-root user (root bypasses -r)"
elif [ -r "$UNREADABLE" ]; then
	echo "FAIL: case 2b precondition — chmod 000 left the file readable"
	FAILED=1
else
	split_run "$(run_entrypoint FLUSS_BOOTSTRAP=localhost:9123 ARROW_INSTRUMENT_MANIFEST="$UNREADABLE")"
	assert_eq "$ENTRY_EXIT" 2 "unreadable manifest exits 2"
	assert_contains "$ENTRY_OUT" "readable manifest FILE is required" "unreadable-manifest message"
fi

# ── Case 2c: diverging manifest vars → exit 2 (documented FATAL) ─────────────
echo "=== ING-INT-006 case 2c: ARROW_INSTRUMENT_MANIFEST vs INSTRUMENT_MANIFEST_PATH ==="
OTHER_MANIFEST="$TMP/other-manifest.csv"
: > "$OTHER_MANIFEST"
split_run "$(run_entrypoint \
	FLUSS_BOOTSTRAP=localhost:9123 \
	ARROW_INSTRUMENT_MANIFEST="$MANIFEST" \
	INSTRUMENT_MANIFEST_PATH="$OTHER_MANIFEST")"
assert_eq "$ENTRY_EXIT" 2 "diverging manifest vars exit 2"
assert_contains "$ENTRY_OUT" "manifest vars diverge" "diverging-manifest message"

# ── Case 3: missing bridge binary → exit 1 ───────────────────────────────────
echo "=== ING-INT-006 case 3: missing bridge binary ==="
# The P1-137 probes sit between the manifest and bridge checks, so this case
# must give them writable paths or the entrypoint stops there with exit 2.
split_run "$(run_entrypoint \
	FLUSS_BOOTSTRAP=localhost:9123 \
	ARROW_INSTRUMENT_MANIFEST="$MANIFEST" \
	LOG_DIR="$TMP/logs" \
	UNCERTAINTY_JOURNAL_PATH="$TMP/journal/uncertainty-journal.jsonl" \
	ARROW_BRIDGE_BIN="$BOGUS_BIN")"
assert_eq "$ENTRY_EXIT" 1 "missing bridge binary exits 1"
assert_contains "$ENTRY_OUT" "arrow-bridge binary not found or not executable" "missing-bridge message"

# ── Case 3b: non-executable bridge file → exit 1 (P6-620) ────────────────────
echo "=== ING-INT-006 case 3b: non-executable bridge ==="
NONEXEC="$TMP/bridge-not-executable"
: > "$NONEXEC"
chmod -x "$NONEXEC"
split_run "$(run_entrypoint \
	FLUSS_BOOTSTRAP=localhost:9123 \
	ARROW_INSTRUMENT_MANIFEST="$MANIFEST" \
	LOG_DIR="$TMP/logs" \
	UNCERTAINTY_JOURNAL_PATH="$TMP/journal/uncertainty-journal.jsonl" \
	ARROW_BRIDGE_BIN="$NONEXEC")"
assert_eq "$ENTRY_EXIT" 1 "non-executable bridge exits 1"
assert_contains "$ENTRY_OUT" "arrow-bridge binary not found or not executable" "non-executable-bridge message"

# ── Case 3c: legacy INSTRUMENT_MANIFEST_PATH satisfies the gate (P6-833) ──────
echo "=== ING-INT-006 case 3c: legacy INSTRUMENT_MANIFEST_PATH ==="
split_run "$(run_entrypoint \
	FLUSS_BOOTSTRAP=localhost:9123 \
	INSTRUMENT_MANIFEST_PATH="$MANIFEST" \
	LOG_DIR="$TMP/logs" \
	UNCERTAINTY_JOURNAL_PATH="$TMP/journal/uncertainty-journal.jsonl" \
	ARROW_BRIDGE_BIN="$BOGUS_BIN")"
assert_eq "$ENTRY_EXIT" 1 "legacy INSTRUMENT_MANIFEST_PATH passes the manifest gate (fails at the bridge)"
assert_contains "$ENTRY_OUT" "arrow-bridge binary not found or not executable" "legacy-manifest-bridge message"

# ── Case 4: unwritable LOG_DIR → exit 2 (P1-137 probe) ───────────────────────
echo "=== ING-INT-006 case 4: unwritable LOG_DIR ==="
# A LOG_DIR whose parent is a regular file: mkdir -p cannot succeed, and the
# failure needs no root or chmod assumptions.
split_run "$(run_entrypoint \
	FLUSS_BOOTSTRAP=localhost:9123 \
	ARROW_INSTRUMENT_MANIFEST="$MANIFEST" \
	LOG_DIR="$MANIFEST/logs" \
	UNCERTAINTY_JOURNAL_PATH="$TMP/journal/uncertainty-journal.jsonl")"
assert_eq "$ENTRY_EXIT" 2 "unwritable LOG_DIR exits 2"
assert_contains "$ENTRY_OUT" "LOG_DIR not creatable" "unwritable-LOG_DIR message"

# ── Verdict ───────────────────────────────────────────────────────────────────
if [ "$FAILED" = 1 ]; then
	echo "ING-INT-006: FAIL"
	exit 1
fi
echo "ING-INT-006: PASS (all entrypoint FATAL paths assert exit code + message)"
