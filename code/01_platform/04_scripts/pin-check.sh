#!/usr/bin/env bash
# pin-check.sh — CI pin-discipline gate (foundation L548 "Exact versions/digests
# are recorded", L553 "Broker packet/postback corpus is versioned and
# reproducible", L554 "CI rejects mutable image tags and unpinned dependencies").
#
# Six checks:
#   1. version matrix shape (version_matrix_verify.py)
#   2. broker corpus integrity (corpus-pin.sh --verify)
#   3. external SNAPSHOT ban (pom-snapshot-scan.py)
#   4. platform version pins (versions.pin: no latest/TO_BE_PINNED)
#   5. runtime.lock image refs all digest-pinned (no bare tags)
#   6. Rust toolchain version agreement (rust_toolchain_pin_check.sh, P3-418)
# Exit 0 only when all six pass. Run as `make pin-check`.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$REPO_ROOT"

rc=0

echo "== [1/6] version matrix shape =="
python3 code/01_platform/04_scripts/version_matrix_verify.py \
	code/01_platform/04_scripts/version_matrix.yaml || rc=1

echo "== [2/6] broker corpus integrity =="
bash code/01_platform/04_scripts/corpus-pin.sh --verify || rc=1

echo "== [3/6] external SNAPSHOT ban =="
python3 code/01_platform/04_scripts/pom-snapshot-scan.py || rc=1

echo "== [4/6] platform version pins =="
# P6-138: the old `[^=]+$` let `*`, `^`, `~`, ranges, whitespace-only and even
# `latest` (as a suffix) pass as "pinned". Versions here are MAJOR.MINOR.PATCH
# with an optional SemVer prerelease qualifier (0.9.1-incubating) — nothing else.
PIN='code/01_platform/04_scripts/versions.pin'
if [ ! -f "$PIN" ]; then
	echo "FAIL: versions.pin missing ($PIN not found)"
	rc=1
elif grep -qE '^(FLINK|FLUSS)_VERSION=(latest|TO_BE_PINNED)' "$PIN" ||
	grep -qE '^(FLINK|FLUSS)_VERSION=.*(\*|\+|\^|~|\[|\(|,|SNAPSHOT)' "$PIN" ||
	grep -qE '^(FLINK|FLUSS)_VERSION=.*[[:space:]]' "$PIN"; then
	echo "FAIL: placeholder/range/snapshot platform version in versions.pin"
	rc=1
else
	grep -qE '^FLINK_VERSION=[0-9]+\.[0-9]+\.[0-9]+([-+.][0-9A-Za-z_-]+)*$' "$PIN" && echo "  FLINK_VERSION pinned" || {
		echo "FAIL: FLINK_VERSION missing or not a strict MAJOR.MINOR.PATCH pin"
		rc=1
	}
	grep -qE '^FLUSS_VERSION=[0-9]+\.[0-9]+\.[0-9]+([-+.][0-9A-Za-z_-]+)*$' "$PIN" && echo "  FLUSS_VERSION pinned" || {
		echo "FAIL: FLUSS_VERSION missing or not a strict MAJOR.MINOR.PATCH pin"
		rc=1
	}
fi

echo "== [5/6] runtime.lock image refs pinned =="
LOCK="$REPO_ROOT/code/01_platform/01_docker/runtime.lock"
if [ ! -f "$LOCK" ]; then
	echo "FAIL: runtime.lock missing (copy runtime.lock.example + pin digests)"
	rc=1
else
	# Every *_IMAGE= line in runtime.lock must carry @sha256:<digest>.
	# P6-139: the filter normalises each line first (leading whitespace,
	# `export`, spaces around `=`) — the old `^[A-Z0-9_]+_IMAGE=` silently
	# skipped those shapes, excluding them from BOTH counts. Case stays
	# UPPER (shell vars are case-sensitive; `lower_image` is a different
	# variable no consumer reads — counting it would bless a dead ref).
	# P6-140: exactly 64 hex. A 12-hex short image-ID is mutable/local-only;
	# the four local-build lines carry one and MUST fail until re-pinned.
	norm() { sed -E -e 's/^[[:space:]]+//' -e 's/^export([[:space:]]+|$)//' -e 's/[[:space:]]*=[[:space:]]*/=/'; }
	# P6-141: `grep -c` exits 1 on zero matches — without `|| true` the
	# script aborted under set -e instead of reporting FAIL, and `OK: 0`
	# would wrongly pass. Zero refs is a FAIL, not a vacuous pass.
	n=$(norm < "$LOCK" | grep -cE '^[A-Za-z0-9_]+_IMAGE=' || true)
	if [ "$n" -eq 0 ]; then
		echo "FAIL: no _IMAGE= refs found in runtime.lock"
		rc=1
	else
		bad=$(norm < "$LOCK" | grep -E '^[A-Za-z0-9_]+_IMAGE=' \
			| grep -vE '@sha256:[0-9a-f]{64}([[:space:]]+(#.*)?)?$' || true)
		if [ -n "$bad" ]; then
			echo "FAIL: bare/unpinned image refs in runtime.lock:"
			printf '%s\n' "$bad"
			rc=1
		else
			echo "  OK: $n image refs all digest-pinned"
		fi
	fi
fi

echo "== [6/6] Rust toolchain version agreement (P3-418) =="
bash code/01_platform/04_scripts/rust_toolchain_pin_check.sh || rc=1

if [ "$rc" -eq 0 ]; then
	echo "pin-check: PASS"
else
	echo "pin-check: FAILED"
fi
exit "$rc"
