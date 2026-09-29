#!/usr/bin/env bash
# lint-enumeration.sh — the one list of shell scripts the static checks run over (L2-1).
#
# `make static-check` and gate step 1 both lint "every repo shell script". Before
# L2-1 each enumerated its own way from `code/`, so the three root entry scripts
# (start-all.sh, run-ingestion.sh, show-ticks.sh) were tracked but never checked
# by either, and the two lists could drift. This script is the single source:
# every tracked `*.sh` from the repo root, excluding vendored/build trees
# (target/third_party).
#
# Fails closed (exit 3) when the enumeration is empty or git cannot list the
# tree: a vacuous "0 scripts checked" is a green light over an unchecked tree,
# which is the bug this replaced.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
	echo "lint-enumeration: $REPO_ROOT is not a git work tree — cannot enumerate tracked scripts" >&2
	exit 3
fi
scripts="$(git ls-files '*.sh' | grep -v -E '(^|/)(target|third_party)/' || true)"
if [ -z "$scripts" ]; then
	echo "lint-enumeration: no tracked shell scripts found (empty enumeration fails closed)" >&2
	exit 3
fi
printf '%s\n' "$scripts"
