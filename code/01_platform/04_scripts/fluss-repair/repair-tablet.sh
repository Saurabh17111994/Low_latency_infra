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
#   ./repair-tablet.sh --all        sweep EVERY table (power-cut recovery; pins
#                                   the container restart policy off for the
#                                   sweep, restores it, starts + verifies)
#   DRY_RUN=1 ./repair-tablet.sh    scan + report only, no changes
#
# Requires the docker CLI and the running dev stack (code/01_platform/01_docker).
# Scans run inside a throwaway python:3-alpine container with the tablet's data
# volume mounted; nothing on the host is modified except via docker.
#
# After an unclean shutdown (power cut), run `./repair-tablet.sh --all` BEFORE
# any drill/measurement: a torn segment in ANY table (not just raw) crash-loops
# the tablet on its next recovery, and the failure surfaces only later as a
# 180s preflight hang or a silent "tablet not ready" (observed 2026-09-02:
# torn tails in 6 of 26 tables after one power cut; each surfaced one at a
# time — Signal_Candidates_current, then feature_candles_15s_preview, then
# forming_bar — because recovery loads tables in order).
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
# 2026-09-02: `|| true` — under `set -euo pipefail` the grep's no-match exit
# (1) propagated through the command substitution and killed the script with
# zero output exactly when the tablet was STOPPED — the state the tool's own
# error message instructs (`docker stop ... && ./repair-tablet.sh ...`).
CONTAINER="$(docker ps --filter "name=fluss-tablet" --format '{{.Names}}\t{{.Status}}' \
    | grep -E 'Up|Restarting' | head -1 | cut -f1 || true)"
if [ -z "$CONTAINER" ]; then
    CONTAINER="$(docker ps -a --filter "name=fluss-tablet" --format '{{.Names}}' | head -1)"
fi
if [ -z "$CONTAINER" ]; then
    echo "ERROR: no fluss-tablet container found — is the dev stack up?" >&2
    exit 2
fi
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
VOLUME="$(docker inspect "$CONTAINER" --format \
    '{{range .Mounts}}{{if eq .Destination "/tmp/fluss/data"}}{{if .Name}}{{.Name}}{{else}}{{.Source}}{{end}}{{end}}{{end}}')"

# ─────────────────────────────────────────────────────────────────────────────
# repair_one_table <dir> <label> — scan + (unless DRY_RUN) truncate ONE table.
# Returns: 0 = repaired or nothing to do, 2 = clean (nothing torn), 1 = error.
# Prints LogScan lines + TRUNCATE_TO lines to stdout (parsed by callers).
# ─────────────────────────────────────────────────────────────────────────────
repair_one_table() {
  local TABLE_DIR="$1" TABLE="$2"
  # ── 3. Collect every log segment (all buckets, all rolled segments) ──────────
SEGMENTS="$(docker run --rm -v "$VOLUME:/d" alpine:3.20 \
    sh -c "find /d/$TABLE_DIR -name '*.log' | sort")"
COUNT="$(printf '%s\n' "$SEGMENTS" | sed '/^$/d' | wc -l)"
if [ "$COUNT" -eq 0 ]; then
    echo "ERROR: no *.log segments found under $TABLE_DIR" >&2
    return 3
fi
echo "found $COUNT segment(s) under $TABLE_DIR"

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
    return 1
fi
if [ -z "$PAIRS" ]; then
    echo
    echo "No truncated segments — nothing to repair."
    return 2
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
    return 1
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
    return 0
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
    return 1
fi
echo "truncated $(printf '%s\n' "$PAIRS" | wc -l) segment(s)"

  return 0
}


if [ -z "$VOLUME" ]; then
    echo "ERROR: could not find the tablet data volume (/tmp/fluss/data) on $CONTAINER" >&2
    exit 2
fi
echo "tablet container: $CONTAINER"
echo "data volume: $VOLUME"

# ── 2. --all sweep: every table dir, one stop/start cycle, policy pinned. ─────
# Power-cut recovery (2026-09-02). Torn segments surface ONE table at a time
# (recovery loads logs in order), so a per-table drill finds the next torn
# table only on the NEXT restart. Sweeping all tables while the tablet is
# stopped fixes the whole damage in one pass and refuses silently to leave
# anything behind (final status check fails the run if still crash-looping).
if [ "$TABLE_ARG" = "--all" ]; then
    echo "sweep mode: all tables, tablet stopped for the sweep"
    POLICY_BEFORE="$(docker inspect "$CONTAINER" --format '{{.HostConfig.RestartPolicy.Name}}')"
    [ -n "$POLICY_BEFORE" ] || POLICY_BEFORE="no"
    if [ "$DRY_RUN" != "1" ]; then
        docker update --restart=no "$CONTAINER" >/dev/null
        docker stop "$CONTAINER" >/dev/null 2>&1 || true
        sleep 2
    fi
    TABLES="$(docker run --rm -v "$VOLUME:/d" alpine:3.20 sh -c 'ls -d /d/default/*/ 2>/dev/null' | sed 's#/d/default/##; s#/##' | sed '/^$/d')"
    TORN_TOTAL=0
    for td in $TABLES; do
        rc=0
        out="$(repair_one_table "default/$td" "$td")" || rc=$?
        if [ "$rc" -eq 0 ]; then
            torn_in_table="$(printf '%s\n' "$out" | grep -c '^TRUNCATE_TO=' || true)"
            TORN_TOTAL=$((TORN_TOTAL + torn_in_table))
            [ "$torn_in_table" -gt 0 ] && echo "repaired: $td ($torn_in_table segment(s))"
        elif [ "$rc" -eq 2 ]; then
            echo "clean: $td"
        elif [ "$rc" -eq 3 ]; then
            # Metadata-only dir (table created but no log segments yet, or a
            # leftover empty dir from a table recreation). In --all sweep this
            # is normal — skipping is correct; aborting the sweep on it (the
            # pre-2026-09-04 behavior) left the tablet stopped with the
            # restart policy pinned off whenever any metadata-only dir sorted
            # first. Single-table mode still treats rc=3 as an error (below).
            echo "skip (no log segments): $td"
        else
            echo "ERROR repairing $td (rc=$rc):" >&2
            printf '%s
' "$out" >&2
            docker update --restart="$POLICY_BEFORE" "$CONTAINER" >/dev/null 2>&1 || true
            echo "tablet left stopped with restart policy $POLICY_BEFORE — start it manually if this abort was unexpected:" >&2
            echo "    docker update --restart=$POLICY_BEFORE $CONTAINER && docker start $CONTAINER" >&2
            exit 1
        fi
    done
    echo "sweep done: $TORN_TOTAL segment(s) truncated across all tables"
    [ "$DRY_RUN" = "1" ] && { echo "(DRY_RUN=1 — tablet not stopped/started, nothing changed)"; exit 0; }
    docker update --restart="$POLICY_BEFORE" "$CONTAINER" >/dev/null
    docker start "$CONTAINER" >/dev/null
    sleep 12
    STATUS_NOW="$(docker ps -a --filter "name=$CONTAINER" --format '{{.Status}}')"
    echo "tablet status: $STATUS_NOW"
    if printf '%s' "$STATUS_NOW" | grep -q "Restarting"; then
        echo "ERROR: tablet STILL crash-looping after the sweep — inspect:" >&2
        echo "    docker logs $CONTAINER 2>&1 | grep -E 'file=/tmp/fluss' | sort -u" >&2
        exit 1
    fi
    echo "tablet is up — recovery completed. Verify with the metadata probe:"
    echo "    pipeline_fluss_metadata_ready  (via pipeline-lib.sh)"
    exit 0
fi

# ── 3. Resolve the table dir (auto-detect when no arg given) ────────────────
if [ -n "$TABLE_ARG" ]; then
    TABLE_DIR="default/$TABLE_ARG"
    TABLE="$TABLE_ARG"
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

# ── 4. Single-table call ─────────────────────────────────────────────────────
rc=0
repair_one_table "$TABLE_DIR" "$TABLE" || rc=$?
[ "$rc" -eq 2 ] && exit 0          # clean, nothing torn
[ "$rc" -eq 3 ] && exit 2          # no segments found (error, distinct from clean)
[ "$rc" -ne 0 ] && exit 1          # refusal or failed truncation already printed
# fall through: rc=0, truncations applied

# ── 5. Restart the tablet and verify it survives recovery (single-table) ────
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
