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
if grep -qE '^FLINK_VERSION=(latest|TO_BE_PINNED)' code/01_platform/04_scripts/versions.pin ||
	grep -qE '^FLUSS_VERSION=(latest|TO_BE_PINNED)' code/01_platform/04_scripts/versions.pin; then
	echo "FAIL: placeholder platform version in versions.pin"
	rc=1
else
	grep -qE '^FLINK_VERSION=[^=]+$' code/01_platform/04_scripts/versions.pin && echo "  FLINK_VERSION pinned" || {
		echo "FAIL: FLINK_VERSION missing"
		rc=1
	}
	grep -qE '^FLUSS_VERSION=[^=]+$' code/01_platform/04_scripts/versions.pin && echo "  FLUSS_VERSION pinned" || {
		echo "FAIL: FLUSS_VERSION missing"
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
	# Registry digests are 64 hex; local build IDs are 12+ hex (image ID
	# pinning). Trailing comments (# ...) are allowed after the ref.
	bad=$(grep -E '^[A-Z0-9_]+_IMAGE=' "$LOCK" \
		| grep -vE '@sha256:[0-9a-f]{12,64}([[:space:]]+#.*)?$' || true)
	if [ -n "$bad" ]; then
		echo "FAIL: bare/unpinned image refs in runtime.lock:"
		printf '%s\n' "$bad"
		rc=1
	else
		n=$(grep -cE '^[A-Z0-9_]+_IMAGE=' "$LOCK")
		echo "  OK: $n image refs all digest-pinned"
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
