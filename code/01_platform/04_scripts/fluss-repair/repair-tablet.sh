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
#   DRY_RUN=1 ./repair-tablet.sh    scan + report only: nothing is stopped,
#                                   started, truncated or re-policied
#
# Requires the docker CLI and the running dev stack (code/01_platform/01_docker).
# The tablet is stopped (with its restart policy pinned to `no`) BEFORE the scan, so
# an offset can never be measured against a segment that is still being appended to,
# and the restart policy is restored by an EXIT trap on every path out — including
# Ctrl-C and `set -e` aborts.
#
# Scans and verification run inside throwaway python:3.12-alpine containers. The scan
# mounts the tablet volume and this script directory READ-ONLY; only the final
# truncation step gets a writable volume mount. Nothing on the host is modified
# except via docker.
#
# After an unclean shutdown (power cut), run `./repair-tablet.sh --all` BEFORE
# any drill/measurement: a torn segment in ANY table (not just raw) crash-loops
# the tablet on its next recovery, and the failure surfaces only later as a
# 180s preflight hang or a silent "tablet not ready" (observed 2026-09-02:
# torn tails in 6 of 26 tables after one power cut; each surfaced one at a
# time — Signal_Candidates_current, then feature_candles_15s_preview, then
# forming_bar (both retired 2026-09-05) — because recovery loads tables in order).
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
# Test seams (unset in production): how long to wait for the tablet to leave
# `running` after `docker stop`, and to reach `running` after `docker start`.
STOP_WAIT_SECS="${STOP_WAIT_SECS:-60}"
START_WAIT_SECS="${START_WAIT_SECS:-60}"

TMP="$(mktemp -d)"
POLICY_BEFORE=""
POLICY_PINNED=0

# ── 0. Cleanup: restore the restart policy, drop the scratch dir ─────────────
# P6-092: a `set -e` failure, a signal, or any `exit` between `docker update
# --restart=no` and the restore used to leave the tablet stopped with
# auto-restart disabled. The EXIT trap now restores the policy on every path
# (INT/TERM are re-raised through `exit`, which runs it too).
# P6-093: bash also runs EXIT traps in $(...) subshells, and --all calls
# repair_one_table in a command substitution — the first iteration's
# `rm -rf "$TMP"` deleted segments.txt/scan.log/pairs.tsv that every later
# iteration still needed. Only the parent shell may clean up.
cleanup() {
    [ "${BASH_SUBSHELL:-0}" = "0" ] || return 0
    if [ "$POLICY_PINNED" = "1" ]; then
        docker update --restart="$POLICY_BEFORE" "$CONTAINER" >/dev/null 2>&1 || true
        echo "restart policy restored: $POLICY_BEFORE" >&2
    fi
    rm -rf "$TMP"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

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

# P6-387: `docker ps --filter name=X` is a SUBSTRING match, so a status read
# could aggregate several containers (fluss-tablet + fluss-tablet-helper) and
# misread health. Once the exact name is known only `docker inspect` is used:
# it can only ever describe that one container.
state() { docker inspect -f '{{.State.Status}}' "$CONTAINER" 2>/dev/null || echo unknown; }

VOLUME="$(docker inspect "$CONTAINER" --format \
    '{{range .Mounts}}{{if eq .Destination "/tmp/fluss/data"}}{{if .Name}}{{.Name}}{{else}}{{.Source}}{{end}}{{end}}{{end}}')"
if [ -z "$VOLUME" ]; then
    echo "ERROR: could not find the tablet data volume (/tmp/fluss/data) on $CONTAINER" >&2
    exit 2
fi
echo "tablet container: $CONTAINER"
echo "data volume: $VOLUME"

# ── 2. Stop / start helpers (restart policy pinned across the stop) ──────────
# P6-097: single-table mode used to `docker stop` without pinning the policy, so
# with --restart=always the daemon could restart the tablet mid-truncate and
# re-open the segments being cut. Both modes now pin `no` before stopping.
# P6-100: `docker stop` returns after SIGTERM handling, not once the process is
# gone; truncating then risks cutting a live append. Poll until it left `running`.
stop_tablet() {
    [ "$DRY_RUN" = "1" ] && return 0
    if [ "$POLICY_PINNED" = "0" ]; then
        POLICY_BEFORE="$(docker inspect "$CONTAINER" --format '{{.HostConfig.RestartPolicy.Name}}')"
        [ -n "$POLICY_BEFORE" ] || POLICY_BEFORE="no"
        docker update --restart=no "$CONTAINER" >/dev/null
        POLICY_PINNED=1
        echo "pinned restart policy: no (was $POLICY_BEFORE)"
    fi
    if [ "$(state)" != "running" ]; then
        return 0
    fi
    echo "stopping tablet container $CONTAINER ..."
    docker stop -t 30 "$CONTAINER" >/dev/null 2>&1 || true
    WAITED=0
    while [ "$(state)" = "running" ]; do
        WAITED=$((WAITED + 1))
        if [ "$WAITED" -ge "$STOP_WAIT_SECS" ]; then
            echo "ERROR: $CONTAINER is still running after ${STOP_WAIT_SECS}s — refusing to touch its data files" >&2
            return 1
        fi
        sleep 1
    done
    return 0
}

# P6-390/P6-391: a fixed `sleep 12` plus a single status read made verification
# flaky (slow recovery read as a crash loop, a fast flake read as success) and
# only `Restarting` counted as failure — `Exited`/`Dead`/`Created` reported
# success on a dead tablet. Poll until `running`; anything else fails the run.
start_tablet_and_verify() {
    [ "$DRY_RUN" = "1" ] && return 0
    if [ "$POLICY_PINNED" = "1" ]; then
        docker update --restart="$POLICY_BEFORE" "$CONTAINER" >/dev/null
        POLICY_PINNED=0
    fi
    echo
    echo "restarting tablet container $CONTAINER ..."
    docker start "$CONTAINER" >/dev/null
    WAITED=0
    STATUS="unknown"
    while :; do
        STATUS="$(state)"
        if [ "$STATUS" = "running" ]; then
            echo "tablet status: running (after ${WAITED}s)"
            echo "tablet is up — recovery completed. Verify the schema with the service's"
            echo "startup log ('ddl-bootstrap: verified 25 tables ok, 0 missing, 0 schema-mismatch')."
            return 0
        fi
        case "$STATUS" in
            exited|dead) break ;;
        esac
        WAITED=$((WAITED + 1))
        [ "$WAITED" -lt "$START_WAIT_SECS" ] || break
        sleep 1
    done
    echo "ERROR: tablet is '$STATUS' after the repair, not running — recovery did NOT complete" >&2
    echo "       the restart policy is back to '$POLICY_BEFORE'; the container is left as it is." >&2
    echo "       start it again once the cause is fixed: docker start $CONTAINER" >&2
    echo "       latest recovery error:" >&2
    docker logs "$CONTAINER" 2>&1 | grep -E 'file=/tmp/fluss' | sort -u | tail -5 >&2 || true
    echo "       a torn segment in a DIFFERENT table/bucket surfaces one at a time (recovery" >&2
    echo "       loads tables in order) — re-run: ./repair-tablet.sh --all" >&2
    return 1
}

# ─────────────────────────────────────────────────────────────────────────────
# repair_one_table <dir> <label> — scan + verify + truncate ONE table.
# The tablet MUST already be stopped (or DRY_RUN=1).
# Returns: 0 = repaired or nothing to do, 2 = clean (nothing torn), 3 = no log
# segments, 1 = error. Prints LogScan lines + TRUNCATE_TO lines to stdout
# (parsed by callers).
# ─────────────────────────────────────────────────────────────────────────────
repair_one_table() {
    local TABLE_DIR="$1" TABLE="$2"

    # ── 3. Collect every log segment (all buckets, all rolled segments) ───────
    # P6-094: $TABLE_DIR derives from $1, so it is passed as an ARGUMENT to
    # `sh -c`, never interpolated into the script text: a table arg with spaces,
    # `*`, `;`, `$()` or backticks used to run arbitrary commands inside the
    # throwaway container.
    SEGMENTS="$(docker run --rm -v "$VOLUME:/d:ro" alpine:3.20 \
        sh -c 'find "$1" -name "*.log" | sort' _ "/d/$TABLE_DIR")"
    COUNT="$(printf '%s\n' "$SEGMENTS" | sed '/^$/d' | wc -l)"
    if [ "$COUNT" -eq 0 ]; then
        echo "ERROR: no *.log segments found under $TABLE_DIR" >&2
        return 3
    fi
    echo "found $COUNT segment(s) under $TABLE_DIR"

    printf '%s\n' "$SEGMENTS" | sed '/^$/d' > "$TMP/segments.txt"

    # ── 4. Scan every segment with LogScan.py (inside a throwaway container) ──
    # The find output is already container-absolute (/d/...), so the scanner and
    # the truncation receive those paths verbatim.
    # P6-386: a scan needs no write access — /d and /s are mounted read-only and
    # the scan log is captured on the host, so a compromised or mistyped
    # LogScan.py cannot rewrite the tablet volume or the host script directory.
    # P6-095: `python3 LogScan.py "$seg" || true` masked a missing LogScan.py, a
    # python crash and an empty scan as "No truncated segments — nothing to
    # repair" (rc=2). The per-segment status is propagated and every segment must
    # report back, so a scanner that dies halfway cannot look like a clean scan.
    echo "=== scanning every batch (a 640 MiB segment takes ~1 min) ==="
    SCAN_LOG="$TMP/scan.log"
    SCAN_ERR="$TMP/scan.err"
    if ! docker run --rm -i -v "$VOLUME:/d:ro" -v "$SCRIPT_DIR:/s:ro" \
        python:3.12-alpine sh -c '
            rc=0
            while IFS= read -r seg; do
                [ -n "$seg" ] || continue
                echo "SCANNED $seg" >&2
                python3 /s/LogScan.py "$seg" || rc=1
            done
            exit $rc' < "$TMP/segments.txt" > "$SCAN_LOG" 2> "$SCAN_ERR"; then
        echo "ERROR: LogScan.py failed on one or more segments — refusing to repair:" >&2
        tail -20 "$SCAN_ERR" >&2 || true
        return 1
    fi
    SCANNED="$(grep -c '^SCANNED ' "$SCAN_ERR" || true)"
    if [ "$SCANNED" != "$COUNT" ]; then
        echo "ERROR: the scanner was handed $COUNT segment(s) but reported $SCANNED — refusing to repair" >&2
        tail -20 "$SCAN_ERR" >&2 || true
        return 1
    fi
    cat "$SCAN_LOG"

    # ── 5. Decide: any truncations? ──────────────────────────────────────────
    # The scan prints, per truncated segment, a "<path>: size=... zero_tail=N
    # bytes" line followed by a "TRUNCATE_TO=<end>" line. Extract (path, end)
    # pairs, both fields exact. The old parser used substr($0, 14), which dropped
    # the first two digits of the offset (TRUNCATE_TO= is 12 bytes), and passed
    # the entire diagnostic line as the filename. That made repair either fail or
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

    echo
    echo "$(printf '%s\n' "$PAIRS" | wc -l) segment(s) have truncated tails (zeroed regions past the"
    echo "last complete batch). The truncation removes ONLY those zeroed bytes."

    if [ "$DRY_RUN" = "1" ]; then
        # P6-388/P6-393: a dry run writes nothing AND changes nothing — no stop,
        # no policy update, no start. The old code returned 0 here and then
        # restarted the tablet anyway, and refused outright when it was Up.
        echo
        echo "DRY_RUN=1 — no changes made. Commands that WOULD run:"
        while IFS=$'\t' read -r path end; do
            echo "  truncate -s $end $path"
        done <<< "$PAIRS"
        return 0
    fi

    # P6-096: never measure an offset against a segment that is still being
    # written to. The caller stops the tablet before calling this function and
    # this assertion keeps a future reordering from silently reintroducing the
    # scan-to-truncate race.
    if [ "$(state)" = "running" ]; then
        echo "ERROR: $CONTAINER is running — refusing to truncate segments that may be mid-append" >&2
        return 1
    fi

    # ── 6. Verify EVERY pair, then truncate ──────────────────────────────────
    # P6-098: `0 < end < size` is not a safety check — a scanner bug or a stale
    # offset would still discard complete records. The bytes [end, size) must
    # really be zeroed before anything is cut.
    # P6-099: doing that inside the truncate loop cut the earlier segments before
    # refusing on a later one — a partial repair the caller reported as a
    # refusal. verify-and-truncate.py checks all pairs first, cuts only if every
    # one passes.
    printf '%s\n' "$PAIRS" > "$TMP/pairs.tsv"
    echo
    echo "=== verifying zeroed tails, then truncating to the exact ends ==="
    if ! docker run --rm -v "$VOLUME:/d" -v "$SCRIPT_DIR:/s:ro" -v "$TMP:/t:ro" \
        python:3.12-alpine python3 /s/verify-and-truncate.py /t/pairs.tsv; then
        echo "ERROR: verification failed — NO segment was truncated (a partial repair was refused)" >&2
        return 1
    fi

    return 0
}

# ── 7. --all sweep: every table dir, one stop/start cycle, policy pinned ─────
# Power-cut recovery (2026-09-02). Torn segments surface ONE table at a time
# (recovery loads logs in order), so a per-table drill finds the next torn table
# only on the NEXT restart. Sweeping every table while the tablet is stopped
# fixes the whole damage in one pass.
if [ "$TABLE_ARG" = "--all" ]; then
    echo "sweep mode: all tables, tablet stopped for the sweep"
    stop_tablet || exit 1
    # P6-389: NUL-delimited so a table dir containing whitespace survives, and an
    # empty result is an ERROR — a silent 0-table sweep used to "succeed" and
    # restart the tablet, masking a volume layout change as a clean bill of health.
    docker run --rm -v "$VOLUME:/d:ro" alpine:3.20 \
        find /d/default -mindepth 1 -maxdepth 1 -type d -print0 > "$TMP/tables.nul"
    if [ ! -s "$TMP/tables.nul" ]; then
        echo "ERROR: no table directories under /d/default in volume $VOLUME — nothing swept" >&2
        exit 2
    fi
    TORN_TOTAL=0
    TABLE_COUNT=0
    while IFS= read -r -d '' td; do
        TABLE_COUNT=$((TABLE_COUNT + 1))
        td="${td#/d/default/}"
        rc=0
        out="$(repair_one_table "default/$td" "$td")" || rc=$?
        if [ "$rc" -eq 0 ]; then
            torn_in_table="$(printf '%s\n' "$out" | grep -c '^TRUNCATE_TO=' || true)"
            TORN_TOTAL=$((TORN_TOTAL + torn_in_table))
            if [ "$torn_in_table" -gt 0 ]; then
                echo "repaired: $td ($torn_in_table segment(s))"
            fi
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
            printf '%s\n' "$out" >&2
            echo "the restart policy is restored to '$POLICY_BEFORE'; the tablet is left stopped." >&2
            echo "start it once the cause is fixed: docker start $CONTAINER" >&2
            exit 1
        fi
    done < "$TMP/tables.nul"
    echo "sweep done: $TORN_TOTAL segment(s) truncated across $TABLE_COUNT table(s)"
    if [ "$DRY_RUN" = "1" ]; then
        echo "(DRY_RUN=1 — tablet not stopped/started, nothing changed)"
        exit 0
    fi
    start_tablet_and_verify || exit 1
    echo "verify with the metadata probe: pipeline_fluss_metadata_ready (via pipeline-lib.sh)"
    exit 0
fi

# ── 8. Resolve the table dir (auto-detect when no arg given) ─────────────────
if [ -n "$TABLE_ARG" ]; then
    # P6-392: TABLE_ARG becomes a path under /d/default. Unvalidated, `../../etc`
    # or an absolute/glob segment escaped the table dir and made find and the
    # truncation operate outside it.
    case "$TABLE_ARG" in
        *[!A-Za-z0-9._-]*|.|..)
            echo "ERROR: invalid table argument '$TABLE_ARG' — expected a table directory name" >&2
            echo "       (letters, digits, dot, underscore, dash — no path separators)" >&2
            exit 2
            ;;
    esac
    TABLE_DIR="default/$TABLE_ARG"
    TABLE="$TABLE_ARG"
    if ! docker run --rm -v "$VOLUME:/d:ro" alpine:3.20 test -d "/d/$TABLE_DIR"; then
        echo "ERROR: table dir '$TABLE_DIR' not found in the volume. Existing tables:" >&2
        docker run --rm -v "$VOLUME:/d:ro" alpine:3.20 sh -c 'ls -d /d/default/*/ 2>/dev/null | sed "s#/d/default/##; s#/##"' >&2
        exit 2
    fi
else
    # Auto-detect: the live raw_table_1 dir is the most recently modified match.
    RAW="$(docker run --rm -v "$VOLUME:/d:ro" alpine:3.20 sh -c \
        'ls -dt /d/default/raw_table_1-*/ 2>/dev/null | head -1' | sed 's#/$##')"
    if [ -z "$RAW" ]; then
        echo "ERROR: could not auto-detect a raw_table_1 table dir. Pass one explicitly:" >&2
        docker run --rm -v "$VOLUME:/d:ro" alpine:3.20 sh -c 'ls -d /d/default/raw_table_1-*/ 2>/dev/null' >&2
        exit 2
    fi
    TABLE_DIR="${RAW#/d/}"
    TABLE="${TABLE_DIR#default/}"
    echo "auto-detected table dir: $TABLE_DIR"
fi

# ── 9. Single table: stop first (P6-096), then scan/verify/truncate ──────────
if [ "$DRY_RUN" = "1" ]; then
    STATUS_NOW="$(state)"
    if [ "$STATUS_NOW" = "running" ]; then
        # P6-388: a dry run reports instead of refusing — but say why the report
        # is advisory: a healthy tablet's active segment is mid-append by nature.
        echo "note: the tablet is running — torn-looking tails may be in-progress appends,"
        echo "      not corruption. DRY_RUN only reports; nothing is stopped or cut."
    fi
else
    stop_tablet || exit 1
fi

rc=0
repair_one_table "$TABLE_DIR" "$TABLE" || rc=$?
if [ "$rc" -eq 3 ]; then
    exit 2                          # no segments found (error, distinct from clean)
fi
if [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ]; then
    exit 1                          # refusal or failed verification already printed
fi
if [ "$DRY_RUN" = "1" ]; then
    exit 0                          # nothing was stopped, so nothing may be started
fi
if [ "$rc" -eq 2 ]; then
    # The tablet was stopped before the scan and turned out clean: put it back
    # instead of leaving the stack down for nothing.
    echo "nothing to repair — restarting the untouched tablet"
fi
start_tablet_and_verify || exit 1
exit 0
