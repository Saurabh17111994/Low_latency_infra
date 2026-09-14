#!/usr/bin/env bash
# rust_toolchain_pin_check.sh — P3-418: the audited Rust toolchain version is
# declared in three places and only comments linked them:
#
#   1. versions.pin                       NAUTILUS_RUST_TOOLCHAIN=<v>
#   2. 04_executor/rust-toolchain.toml    channel = "<v>"
#   3. 04_executor/Cargo.toml             rust-version = "<v>"
#
# A drift between them is silent: cargo honours the nested toolchain file while
# the recorded/pinned version claims something else. This check fails on any
# disagreement, then verifies the toolchain rustup actually resolves inside the
# crate directory (the same resolution the Monday gate's executor step gets).
#
# Usage: rust_toolchain_pin_check.sh [REPO_ROOT]   (defaults to this repo)
# Exit 0 only when the three declarations agree and, when rustup is available,
# the resolved toolchain matches them.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${1:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"

PIN_FILE="$ROOT/code/01_platform/04_scripts/versions.pin"
TOOLCHAIN_FILE="$ROOT/code/02_services/04_executor/rust-toolchain.toml"
MANIFEST_FILE="$ROOT/code/02_services/04_executor/Cargo.toml"

rc=0

for f in "$PIN_FILE" "$TOOLCHAIN_FILE" "$MANIFEST_FILE"; do
	[ -f "$f" ] || {
		echo "FAIL: missing ${f#"$ROOT"/}"
		rc=1
	}
done

pin=""
channel=""
rust_version=""
if [ "$rc" -eq 0 ]; then
	pin=$(sed -n 's/^NAUTILUS_RUST_TOOLCHAIN=\(.*\)$/\1/p' "$PIN_FILE")
	channel=$(sed -n 's/^channel *= *"\(.*\)"$/\1/p' "$TOOLCHAIN_FILE")
	rust_version=$(sed -n 's/^rust-version *= *"\(.*\)"$/\1/p' "$MANIFEST_FILE")
fi

[ -n "$pin" ] || {
	echo "FAIL: NAUTILUS_RUST_TOOLCHAIN missing from ${PIN_FILE#"$ROOT"/}"
	rc=1
}
[ -n "$channel" ] || {
	echo "FAIL: channel missing from ${TOOLCHAIN_FILE#"$ROOT"/}"
	rc=1
}
[ -n "$rust_version" ] || {
	echo "FAIL: rust-version missing from ${MANIFEST_FILE#"$ROOT"/}"
	rc=1
}

if [ "$rc" -eq 0 ]; then
	if [ "$pin" != "$channel" ] || [ "$pin" != "$rust_version" ]; then
		echo "FAIL: Rust toolchain versions disagree: versions.pin=$pin rust-toolchain.toml=$channel Cargo.toml=$rust_version"
		rc=1
	else
		echo "  OK: NAUTILUS_RUST_TOOLCHAIN=$pin == rust-toolchain.toml channel == Cargo.toml rust-version"
	fi
fi

# Resolution check: only meaningful when the three declarations already agree.
# rustup is optional — a machine without it still gets the declarations checked.
if [ "$rc" -eq 0 ] && command -v rustup >/dev/null 2>&1; then
	active="$(cd "$ROOT/code/02_services/04_executor" &&
		rustup show active-toolchain 2>/dev/null | awk '{print $1}' | awk -F- '{print $1}')" || active=""
	if [ -z "$active" ]; then
		echo "  SKIP: rustup present but resolved no active toolchain here"
	elif [ "$active" != "$pin" ]; then
		echo "FAIL: rustup resolves '$active' inside 04_executor, pin says '$pin' (nested rust-toolchain.toml bypassed?)"
		rc=1
	else
		echo "  OK: rustup resolves $active for code/02_services/04_executor"
	fi
fi

exit "$rc"
