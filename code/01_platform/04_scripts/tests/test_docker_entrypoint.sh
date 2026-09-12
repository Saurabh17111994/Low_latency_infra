#!/usr/bin/env bash
# ING-INT-006 — docker-entrypoint.sh FATAL-path contract (M5).
#
# Asserts the documented exit codes AND messages for the three pre-Java
# FATAL gates of code/02_services/01_ingestion/docker-entrypoint.sh:
#   missing FLUSS_BOOTSTRAP          → exit 2  "FLUSS_BOOTSTRAP is required"
#   missing/unreadable manifest      → exit 2  "readable manifest FILE is required"
#   unwritable LOG_DIR               → exit 2  "LOG_DIR not creatable"        (P1-137)
#   missing/non-executable bridge    → exit 1  "arrow-bridge binary not found or not executable"
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
ENTRYPOINT="$SCRIPT_DIR/../../../02_services/01_ingestion/docker-entrypoint.sh"

FAILED=0

assert_eq() { # $1=got $2=want $3=label
	if [ "$1" != "$2" ]; then
		echo "FAIL: $3 — got exit=$1 want exit=$2"
		FAILED=1
		return 1
	fi
	echo "ok: $3 (exit=$1)"
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

# The entrypoint may be exercised from a checkout whose working tree does not
# keep the executable bit; bash does not require it when invoked explicitly.
chmod +x "$ENTRYPOINT"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
MANIFEST="$TMP/manifest.csv"
: > "$MANIFEST"
BOGUS_BIN="$TMP/no-such-bridge"

run_entrypoint() { # env-var list; echoes "<exit>|<output>", caller splits it
	local out rc
	out="$(env -i "$@" bash "$ENTRYPOINT" 2>&1)"
	rc=$?
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

# ── Case 2: missing manifest → exit 2 ────────────────────────────────────────
echo "=== ING-INT-006 case 2: missing manifest ==="
split_run "$(run_entrypoint FLUSS_BOOTSTRAP=localhost:9123)"
assert_eq "$ENTRY_EXIT" 2 "missing manifest exits 2"
assert_contains "$ENTRY_OUT" "readable manifest FILE is required" "missing-manifest message"

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
