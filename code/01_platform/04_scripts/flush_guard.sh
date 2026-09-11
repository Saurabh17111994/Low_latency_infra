#!/usr/bin/env bash
# Fail the build if a .flush() call sits inside a finally block in MAIN sources.
#
# Why this rule exists (2026-09-11): Fluss's TableWriter.flush() is not a
# per-writer flush. It delegates to WriterClient.flush(), which awaits every
# pending batch on the whole connection, and in 0.9.1-incubating that await is
# unbounded (WriteBatch.RequestFuture.await() is a bare latch.await(); there is
# no delivery timeout — RecordAccumulator carries a "TODO add deliveryTimeoutMs"
# and a batch whose leader is unknown is never completed). See FlussWriteProfiles
# for the full source chain.
#
# So a flush() in a finally block is actively harmful, not merely redundant:
#   - on the success path it is a no-op whenever the code already awaited the
#     write's own future (the ack it waits for happened already);
#   - on the failure path it does the exact opposite of cleanup — the futures
#     are still pending, so flush() blocks indefinitely and the original
#     timeout/failure it was supposed to sit behind never propagates. It masks
#     the bug it looks like it is guarding against.
# Because it is unbounded, it can also hang on an unrelated table sharing the
# connection, which no catch() can rescue.
#
# The correct shape is a single bounded get(): writer.upsert(row).get(timeoutMs,
# MILLISECONDS). That is what the guards protect, and it is what every store now
# does (FlussEodStateStore, FlussIntentDedupStore, FlussPositionsStateStore, ...).
#
# Scope: src/main/java only. Test sources legitimately flush in finally (they
# drive live clusters and want the write visible before assertions) and are not
# the trading request path, so they are intentionally out of scope.
set -euo pipefail

ROOT="${1:-.}"

# R-091 (same lesson as cep_guard.sh): a nonexistent/typo'd scan root silently
# matches nothing and the guard would "pass" without scanning anything.
if [ ! -d "$ROOT" ]; then
	echo "ERROR: scan root '$ROOT' is not a directory — refusing to run (the guard would scan nothing)." >&2
	exit 1
fi

SCANNED=0
HITS=""
while IFS= read -r file; do
	SCANNED=$((SCANNED + 1))
	HIT="$(awk '
	function reset() { inStr = 0; inChar = 0 }
	BEGIN { depth = 0; ftop = 0; pending = 0; inBlock = 0; inStr = 0; inChar = 0 }
	{
		n = length($0)
		i = 1
		while (i <= n) {
			c = substr($0, i, 1)
			two = substr($0, i, 2)
			if (inBlock) { if (two == "*/") { inBlock = 0; i += 2; continue } i++; continue }
			if (inStr)   { if (c == "\\") { i += 2; continue } if (c == "\"") { inStr = 0 } i++; continue }
			if (inChar)  { if (c == "\\") { i += 2; continue } if (c == "\x27") { inChar = 0 } i++; continue }
			if (two == "//") { break }
			if (two == "/*") { inBlock = 1; i += 2; continue }
			if (c == "\"") { inStr = 1; i++; continue }
			if (c == "\x27") { inChar = 1; i++; continue }
			if (substr($0, i, 7) == "finally") {
				before = (i == 1) ? "" : substr($0, i - 1, 1)
				after = substr($0, i + 7, 1)
				if ((before == "" || before !~ /[A-Za-z0-9_]/) && (after == "" || after !~ /[A-Za-z0-9_]/)) {
					pending = 1; i += 7; continue
				}
			}
			if (substr($0, i, 6) == ".flush") {
				if (ftop > 0 && depth >= fstack[ftop]) {
					printf "%s:%d: %s\n", FILENAME, FNR, $0
				}
				i += 6; continue
			}
			if (c == "{") {
				depth++
				if (pending) { ftop++; fstack[ftop] = depth; pending = 0 }
				i++; continue
			}
			if (c == "}") {
				if (ftop > 0 && depth == fstack[ftop]) { ftop-- }
				depth--
				i++; continue
			}
			i++
		}
	}
	' "$file")"
	if [ -n "$HIT" ]; then
		HITS="${HITS}${HIT}"$'\n'
	fi
done < <(find "$ROOT" -type d -name target -prune -o \
	-type f -path '*/src/main/java/*' -name '*.java' -print 2>/dev/null | sort)

if [ -n "$HITS" ]; then
	echo "ERROR: .flush() inside a finally block — this masks bounded-timeout failures"
	echo "(flush() is unbounded in Fluss 0.9.1; see the header of flush_guard.sh)."
	echo "Use a single bounded writer.<op>(row).get(timeout, MILLISECONDS) instead."
	printf '%s' "$HITS"
	exit 1
fi

echo "OK: no .flush() inside finally blocks in main sources (scanned $SCANNED files)."
