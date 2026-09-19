#!/usr/bin/env bash
# corpus-pin.sh — pin/verify the versioned broker packet corpus (foundation L553:
# "Broker packet/postback corpus is versioned and reproducible").
#
# The corpus is the golden Arrow frame set captured from the broker protocol:
#   code/02_services/01_ingestion/go-bridge/testdata/golden/
#   (full-tick, ltp-tick, response, unknown-packet + their .golden decoded forms)
# Every frame is sha256-pinned in code/01_platform/04_scripts/corpus.sha256 so a
# decoder change that alters a golden byte fails `make pin-check` instead of
# silently diverging from the versioned corpus.
#
# Usage:
#   corpus-pin.sh --verify       verify committed corpus against corpus.sha256 (CI)
#   corpus-pin.sh --regenerate   re-pin corpus.sha256 after an intentional corpus change

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
REL_CORPUS="code/02_services/01_ingestion/go-bridge/testdata/golden"
CORPUS_DIR="$REPO_ROOT/$REL_CORPUS"
MANIFEST="$SCRIPT_DIR/corpus.sha256"

if [ ! -d "$CORPUS_DIR" ]; then
	echo "ERROR: corpus dir missing: $CORPUS_DIR" >&2
	exit 2
fi

# P6-349 (wave 40): exactly one argument. `case "${1:-}"` matched a known verb
# and silently ignored whatever followed it, so `--verify anything` — and a
# typo'd verb that still matched — looked like a normal run.
if [ "$#" -ne 1 ]; then
	echo "Usage: $0 [--verify|--regenerate]" >&2
	exit 2
fi

case "$1" in
--verify)
	if [ ! -f "$MANIFEST" ]; then
		echo "ERROR: $MANIFEST missing — run 'corpus-pin.sh --regenerate' to pin the corpus" >&2
		exit 2
	fi
	# sha256sum -c expects paths relative to the invocation directory.
	# --strict (P6-060, wave 40): a malformed manifest line must FAIL the check
	# instead of only warning. GNU-only, like sha256sum itself — this is Linux
	# tooling (macOS ships shasum, not sha256sum), so --strict costs nothing.
	(cd "$REPO_ROOT" && sha256sum --strict -c "$MANIFEST")
	# P6-060 (wave 40): `-c` only checks the paths the manifest LISTS, so a new
	# file under the corpus dir left the check green while the corpus and its pin
	# diverged — the "versioned and reproducible" guarantee had a hole exactly
	# where a decoder change lands. Compare the two sets explicitly.
	pinned="$(sed -E 's/^[0-9a-fA-F]{64} [ *]//' "$MANIFEST" | LC_ALL=C sort)"
	actual="$(cd "$REPO_ROOT" && find "$REL_CORPUS" -type f | LC_ALL=C sort)"
	if [ "$pinned" != "$actual" ]; then
		echo "ERROR: $REL_CORPUS and $MANIFEST diverged — unpinned or renamed corpus file(s):" >&2
		diff <(printf '%s\n' "$actual") <(printf '%s\n' "$pinned") >&2 || true
		echo "       ('<' = on disk but not pinned; '>' = pinned but missing)" >&2
		echo "       run 'corpus-pin.sh --regenerate' after an intentional corpus change" >&2
		exit 1
	fi
	;;
--regenerate)
	# P6-061 (wave 40): the old form was
	#   (cd ... && sha256sum "$CORPUS_DIR"/* | sed "s|  $CORPUS_DIR/|  $REL|") >"$MANIFEST"
	# which TRUNCATES the committed pin file before the pipeline emits a byte (the
	# redirect is created first), so any mid-pipeline failure left a corrupted
	# manifest. The glob also skipped dotfiles, expanded to a literal `*` on an
	# empty dir, made sha256sum fail on a subdirectory, and relied on implicit
	# glob order. Hash with find+sort into a temp file, then rename atomically.
	if [ -z "$(cd "$REPO_ROOT" && find "$REL_CORPUS" -type f -print -quit)" ]; then
		echo "ERROR: $REL_CORPUS holds no files — refusing to write an empty pin file" >&2
		exit 2
	fi
	tmp="$(mktemp "$SCRIPT_DIR/.corpus.sha256.XXXXXX")"
	trap 'rm -f "$tmp"' EXIT
	(
		cd "$REPO_ROOT" &&
			find "$REL_CORPUS" -type f -print0 |
			LC_ALL=C sort -z |
			xargs -0 -r sha256sum --
	) >"$tmp"
	mv -- "$tmp" "$MANIFEST"
	trap - EXIT
	echo "Wrote $MANIFEST ($(wc -l <"$MANIFEST") files)."
	;;
*)
	echo "Usage: $0 [--verify|--regenerate]" >&2
	exit 2
	;;
esac
