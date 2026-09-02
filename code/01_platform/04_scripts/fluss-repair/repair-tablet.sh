#!/bin/bash
# repair-tablet.sh — detect and surgically repair truncated Fluss log segments
# after an unclean tablet shutdown.
#
# Symptom this repairs: the tablet crash-loops on startup with
#     Failed to load record batch ... EOFException ... Expected to read 48
#     bytes, but reached end of file after reading N bytes
# caused by preallocated/zeroed tails left past the last complete batch when
# the tablet was killed mid-write. Fluss's reported error position is NOT the
# true boundary (a zeroed batch header misparses as valid and the reader jumps
# through the garbage region), so this tool scans each segment with the
# server's own batch arithmetic (LogScan.py) and truncates to the exact end of
# the last complete batch — only zeroed/never-written bytes are removed, never
# complete records.
#
# Usage:
#   ./repair-tablet.sh [TABLE]      scan + repair TABLE (default raw_table_1-696)
#   DRY_RUN=1 ./repair-tablet.sh    scan + report only, no changes
#
# Requires the docker CLI and the running dev stack (code/01_platform/01_docker).
# Scans run inside a throwaway python:3-alpine container with the tablet's data
# volume mounted; nothing on the host is modified except via docker.
#
# Full runbook: docs/08_implementation/11-testing-and-release.md (ING-E2E-001
# cluster-health runbook section).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# The table dir carries a server-assigned id suffix that changes whenever the
# table is recreated (raw_table_1-696 vs raw_table_1-387), so a bare
# "raw_table_1" resolves to the most recently modified matching dir.
TABLE_ARG="${1:-}"
DRY_RUN="${DRY_RUN:-0}"

# ── 1. Discover the tablet container + its data volume ───────────────────────
# Prefer an active (Up/Restarting) container over an Exited one from another
# compose project — several dev stacks can leave tablet containers around.
CONTAINER="$(docker ps --filter "name=fluss-tablet" --format '{{.Names}}\t{{.Status}}' \
    | grep -E 'Up|Restarting' | head -1 | cut -f1)"
if [ -z "$CONTAINER" ]; then
    CONTAINER="$(docker ps -a --filter "name=fluss-tablet" --format '{{.Names}}' | head -1)"
fi
if [ -z "$CONTAINER" ]; then
    echo "ERROR: no fluss-tablet container found — is the dev stack up?" >&2
    exit 2
fi
VOLUME="$(docker inspect "$CONTAINER" --format \
    '{{range .Mounts}}{{if eq .Destination "/tmp/fluss/data"}}{{if .Name}}{{.Name}}{{else}}{{.Source}}{{end}}{{end}}{{end}}')"
if [ -z "$VOLUME" ]; then
    echo "ERROR: could not find the tablet data volume (/tmp/fluss/data) on $CONTAINER" >&2
    exit 2
fi
echo "tablet container: $CONTAINER"
echo "data volume: $VOLUME"

# ── 2. Resolve the table dir (auto-detect when no arg given) ────────────────
if [ -n "$TABLE_ARG" ]; then
    TABLE_DIR="default/$TABLE_ARG"
    if ! docker run --rm -v "$VOLUME:/d" alpine:3.20 test -d "/d/$TABLE_DIR"; then
        echo "ERROR: table dir '$TABLE_DIR' not found in the volume. Existing tables:" >&2
        docker run --rm -v "$VOLUME:/d" alpine:3.20 sh -c 'ls -d /d/default/*/ 2>/dev/null | sed "s#/d/default/##; s#/##"' >&2
        exit 2
    fi
else
    # Auto-detect: the live raw_table_1 dir is the most recently modified match.
    RAW="$(docker run --rm -v "$VOLUME:/d" alpine:3.20 sh -c \
        'ls -dt /d/default/raw_table_1-*/ 2>/dev/null | head -1' | sed 's#/$##')"
    if [ -z "$RAW" ]; then
        echo "ERROR: could not auto-detect a raw_table_1 table dir. Pass one explicitly:" >&2
        docker run --rm -v "$VOLUME:/d" alpine:3.20 sh -c 'ls -d /d/default/raw_table_1-*/ 2>/dev/null' >&2
        exit 2
    fi
    TABLE_DIR="${RAW#/d/}"
    TABLE="${TABLE_DIR#default/}"
    echo "auto-detected table dir: $TABLE_DIR"
fi

# ── 3. Collect every log segment (all buckets, all rolled segments) ──────────
SEGMENTS="$(docker run --rm -v "$VOLUME:/d" alpine:3.20 \
    sh -c "find /d/$TABLE_DIR -name '*.log' | sort")"
COUNT="$(printf '%s\n' "$SEGMENTS" | sed '/^$/d' | wc -l)"
if [ "$COUNT" -eq 0 ]; then
    echo "ERROR: no *.log segments found under $TABLE_DIR" >&2
    exit 2
fi
echo "found $COUNT segment(s) under $TABLE_DIR"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
printf '%s\n' "$SEGMENTS" | sed '/^$/d' > "$TMP/segments.txt"

# ── 4. Scan every segment with LogScan.py (inside a throwaway container) ─────
# The find output is already container-absolute (/d/...), so the scanner and
# truncate receive those paths verbatim.
echo "=== scanning every batch (a 640 MiB segment takes ~1 min) ==="
SCAN_LOG="$TMP/scan.log"
docker run --rm -v "$VOLUME:/d" -v "$SCRIPT_DIR":/s -v "$TMP":/t \
    python:3.12-alpine sh -c \
    'while IFS= read -r seg; do python3 /s/LogScan.py "$seg" || true; done < /t/segments.txt' \
    > "$SCAN_LOG"
cat "$SCAN_LOG"

# ── 5. Decide: any truncations? ──────────────────────────────────────────────
# The scan prints, per truncated segment, a "<path>: size=... zero_tail=N bytes"
# line followed by a "TRUNCATE_TO=<end>" line. Extract (path, end) pairs.
# Keep both fields exact. The old parser used substr($0, 14), which dropped the
# first two digits of the offset (TRUNCATE_TO= is 12 bytes), and passed the
# entire diagnostic line as the filename. That made repair either fail or
# target the wrong byte boundary — a data-loss hazard in the repair tool.
if ! PAIRS="$(awk '
/^TRUNCATE_TO=/ {
    path = prev
    sub(/: size=.*/, "", path)
    if (path !~ /^\/d\/.*\.log$/ || $0 !~ /^TRUNCATE_TO=[1-9][0-9]*$/) {
        bad = 1
    } else {
        print path "\t" substr($0, index($0, "=") + 1)
    }
}
{ prev = $0 }
END { if (bad) exit 1 }
' "$SCAN_LOG")"; then
    echo "ERROR: malformed LogScan output — refusing to repair" >&2
    exit 1
fi
if [ -z "$PAIRS" ]; then
    echo
    echo "No truncated segments — nothing to repair."
    exit 0
fi

# Safety guard: while the tablet is Up, a "truncated tail" is most likely an
# in-progress append (the scanner caught the segment mid-write), not corruption.
# The repair flow is for a crash-looping/stopped tablet; refuse otherwise.
STATUS_NOW="$(docker ps -a --filter "name=$CONTAINER" --format '{{.Status}}')"
if printf '%s' "$STATUS_NOW" | grep -qE "^Up"; then
    echo
    echo "ERROR: found truncated-looking segments but the tablet is currently Up"
    echo "      ($STATUS_NOW). A healthy tablet's active segment is mid-append, so these"
    echo "      may be in-progress writes, not corruption. If the tablet is genuinely"
    echo "      crash-looping on a segment, stop it first and re-run:"
    echo "          docker stop $CONTAINER"
    echo "          $0 $TABLE"
    exit 1
fi
echo
echo "$(printf '%s\n' "$PAIRS" | wc -l) segment(s) have truncated tails (zeroed regions past the"
echo "last complete batch). The truncation removes ONLY those zeroed bytes."

if [ "$DRY_RUN" = "1" ]; then
    echo
    echo "DRY_RUN=1 — no changes made. Commands that WOULD run:"
    printf '%s\n' "$PAIRS" | while IFS=$'\t' read -r path end; do
        echo "  truncate -s $end $path"
    done
    exit 0
fi

# ── 6. Stop the tablet before touching its data files ────────────────────────
if docker ps --filter "name=$CONTAINER" --format '{{.Names}}' | grep -q .; then
    echo
    echo "stopping tablet container $CONTAINER ..."
    docker stop "$CONTAINER" >/dev/null
fi

# ── 7. Truncate each affected segment to the authoritative boundary ──────────
echo
echo "=== truncating to the exact last-complete-batch ends ==="
printf '%s\n' "$PAIRS" > "$TMP/pairs.tsv"
if ! docker run --rm -v "$VOLUME:/d" -v "$TMP:/t:ro" alpine:3.20 sh -c '
set -eu
while IFS="$(printf '\''\t'\'')" read -r path end; do
    case "$end" in
        '\'''\''|*[!0-9]*) echo "ERROR: non-numeric truncation offset: $end" >&2; exit 1 ;;
    esac
    size="$(stat -c '\''%s'\'' "$path")"
    if [ "$end" -le 0 ] || [ "$end" -ge "$size" ]; then
        echo "ERROR: unsafe truncation for $path: end=$end size=$size" >&2
        exit 1
    fi
    truncate -s "$end" "$path"
done < /t/pairs.tsv
'
then
    echo "ERROR: one or more truncations failed — refusing a partial repair result" >&2
    exit 1
fi
echo "truncated $(printf '%s\n' "$PAIRS" | wc -l) segment(s)"

# ── 8. Restart the tablet and verify it survives recovery ────────────────────
echo
echo "restarting tablet container $CONTAINER ..."
docker start "$CONTAINER" >/dev/null
sleep 12
STATUS="$(docker ps -a --filter "name=$CONTAINER" --format '{{.Status}}')"
echo "tablet status: $STATUS"
if printf '%s' "$STATUS" | grep -q "Restarting"; then
    echo
    echo "WARNING: the tablet is still crash-looping. The latest recovery error may"
    echo "point at a segment in a DIFFERENT table/bucket — re-run with that table, e.g."
    echo "    ./repair-tablet.sh <other-table-dir>"
    echo "Then verify the cluster serves the manifest (24/0/0) via the service's"
    echo "startup log: 'ddl-bootstrap: verified 25 tables ok, 0 missing, 0 schema-mismatch'."
    exit 1
fi
echo "tablet is up — recovery completed. Verify the schema with the service's"
echo "startup log ('ddl-bootstrap: verified 25 tables ok, 0 missing, 0 schema-mismatch')."
