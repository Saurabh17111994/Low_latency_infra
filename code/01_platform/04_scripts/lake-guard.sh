#!/usr/bin/env bash
# lake-guard.sh (2026-08-31, Item 6; hardened 2026-09-14, wave 12) — daily R2 lake
# health check. Cron-able; fail-fast with a greppable FAIL line.
#
# Checks (Asia/Kolkata):
#   1. yesterday's day-folder holds >= 1 real data object
#   2. the table's Iceberg metadata holds >= LAKE_GUARD_MIN_MANIFESTS non-empty
#      manifests and its newest manifest is younger than
#      LAKE_GUARD_MAX_MANIFEST_AGE_H hours
#   3. after LAKE_GUARD_AFTER (default 18:30) IST, today's folder holds >= 1 real
#      data object
#
# "Real data object" = a non-empty .parquet under data/event_day=<day>/ (P6-121,
# P6-122): a _SUCCESS marker, a .crc sidecar or a zero-byte directory marker used
# to satisfy the old ">= 1 key under the prefix" check on a table holding no data.
#
# Every check lists ONLY its own prefix (P6-437) at the moment it runs (P6-440):
# the old single whole-bucket snapshot was slow, cost R2 LIST calls and could not
# see an object written while the guard was running. A listing failure is a FAIL
# with a reason, never a bare non-zero exit (P6-438).
#
# Env knobs (all optional):
#   LAKE_GUARD_CHECK_DAY=20991231     # test hook: force the "yesterday" day
#   LAKE_GUARD_TABLE_PREFIX=<prefix>  # default ${R2_PREFIX}/default/raw_table_1/
#   LAKE_GUARD_MIN_MANIFESTS=2        # non-empty manifest floor
#   LAKE_GUARD_MAX_MANIFEST_AGE_H=48  # newest-manifest age ceiling
#   LAKE_GUARD_AFTER=1830             # IST hhmm at which the today-check applies
# Exit codes: 0 pass; 1 FAIL; 2 unusable input.
set -euo pipefail

_D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./r2-list.sh
source "$_D/r2-list.sh"

fail() { printf '!! lake-guard FAIL: %s\n' "$*" >&2; exit 1; }
bad()  { printf '!! lake-guard INPUT: %s\n' "$*" >&2; exit 2; }
_ist() { TZ=Asia/Kolkata date "$@"; }

# Test hook (like LAKE_GUARD_CHECK_DAY): the after-deadline check compares
# against the real IST day, so a run that straddles midnight flakes. Tests pin it.
TODAY="${LAKE_GUARD_TODAY:-$(_ist +%Y%m%d)}"
YEST="${LAKE_GUARD_CHECK_DAY:-$(_ist -d 'yesterday' +%Y%m%d)}"
MIN_MANIFESTS="${LAKE_GUARD_MIN_MANIFESTS:-2}"
MAX_AGE_H="${LAKE_GUARD_MAX_MANIFEST_AGE_H:-48}"
AFTER="${LAKE_GUARD_AFTER:-1830}"

# P6-436: every knob ends up inside a listing prefix or an arithmetic test, so an
# unchecked value is a rubber stamp (LAKE_GUARD_CHECK_DAY='.*' used to match every
# row and force a PASS).
_need_int() { case "$2" in ''|*[!0-9]*) bad "$1 must be a whole number, got '$2'" ;; esac; }
case "$YEST" in
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;;
    *) bad "LAKE_GUARD_CHECK_DAY must be yyyyMMdd, got '$YEST'" ;;
esac
case "$TODAY" in
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;;
    *) bad "LAKE_GUARD_TODAY must be yyyyMMdd, got '$TODAY'" ;;
esac
_need_int LAKE_GUARD_MIN_MANIFESTS "$MIN_MANIFESTS"
_need_int LAKE_GUARD_MAX_MANIFEST_AGE_H "$MAX_AGE_H"
_need_int LAKE_GUARD_AFTER "$AFTER"
[ "${#AFTER}" -eq 4 ] || bad "LAKE_GUARD_AFTER must be a 4-digit IST hhmm, got '$AFTER'"

r2_load || fail "cannot read the R2 config — see docs/06_operations/07-lake-archive-ops.md"

# P6-756: one derived prefix instead of three hard-coded copies of the table path.
# The default is the table the tiering job writes; renaming the warehouse or the
# table is an env change, not a silent mis-target.
TABLE_PREFIX="${LAKE_GUARD_TABLE_PREFIX:-${R2_PREFIX}/default/raw_table_1/}"
case "$TABLE_PREFIX" in
    */) ;;
    *) TABLE_PREFIX="${TABLE_PREFIX}/" ;;
esac
DATA_PREFIX="${TABLE_PREFIX}data/event_day="
META_PREFIX="${TABLE_PREFIX}metadata/"

list_prefix() {
    # $1 = label for messages, $2 = object prefix.
    local out
    if ! out="$(r2_list_lake "$2" 2>&1)"; then
        fail "R2 listing failed for $1 ($2): ${out:-no output from r2_list_lake}"
    fi
    printf '%s\n' "$out"
}

count_data() {
    # $1 = prefix. r2_list_lake emits TSV key<TAB>size<TAB>LastModified; count only
    # non-empty .parquet objects under the prefix (P6-121/122).
    awk -F'\t' -v p="$1" '
        $2 + 0 > 0 && index($1, p) == 1 && $1 ~ /\.parquet$/ { c++ }
        END { print c + 0 }'
}

printf 'lake-guard: today=%s yesterday=%s table=%s\n' "$TODAY" "$YEST" "$TABLE_PREFIX"

# ── 1. Yesterday ─────────────────────────────────────────────────────────────
Y_KEYS="$(list_prefix "yesterday ${YEST}" "${DATA_PREFIX}${YEST}/")"
Y="$(printf '%s\n' "$Y_KEYS" | count_data "${DATA_PREFIX}${YEST}/")"
printf 'yesterday data objects: %s\n' "$Y"
[ "$Y" -gt 0 ] || fail "no data for ${YEST} under ${DATA_PREFIX}${YEST}/ (empty, or only markers/sidecars)"

# ── 2. Iceberg metadata ──────────────────────────────────────────────────────
M_KEYS="$(list_prefix metadata "$META_PREFIX")"
M="$(printf '%s\n' "$M_KEYS" | awk -F'\t' -v p="$META_PREFIX" '
    $2 + 0 > 0 && index($1, p) == 1 && $1 ~ /\.avro$/ { c++ }
    END { print c + 0 }')"
printf 'iceberg manifests: %s (floor %s)\n' "$M" "$MIN_MANIFESTS"
[ "$M" -ge "$MIN_MANIFESTS" ] || fail "only ${M} non-empty manifest(s) under ${META_PREFIX} (floor ${MIN_MANIFESTS})"

# P6-439: a manifest count alone passes on arbitrarily old files, so the newest
# one must be recent — an old newest-manifest means the latest commit never landed.
NEWEST="$(printf '%s\n' "$M_KEYS" | awk -F'\t' -v p="$META_PREFIX" '
    index($1, p) == 1 && $1 ~ /\.avro$/ && $3 != "" { print $3 }' | sort | tail -1)"
[ -n "$NEWEST" ] || fail "no manifest under ${META_PREFIX} carries a LastModified timestamp — cannot tell a live table from orphaned files; refusing to pass"
AGE_H="$(python3 - "$NEWEST" <<'PYEOF'
import datetime
import sys

raw = sys.argv[1].strip()
try:
    ts = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
except ValueError:
    sys.exit(f"unparseable manifest timestamp: {raw!r}")
if ts.tzinfo is None:                      # a listing without a zone is UTC
    ts = ts.replace(tzinfo=datetime.timezone.utc)
now = datetime.datetime.now(datetime.timezone.utc)
print(int((now - ts.astimezone(datetime.timezone.utc)).total_seconds() // 3600))
PYEOF
)" || fail "cannot age-check the newest manifest (${NEWEST})"
printf 'newest manifest: %s (%sh old, ceiling %sh)\n' "$NEWEST" "$AGE_H" "$MAX_AGE_H"
[ "$AGE_H" -le "$MAX_AGE_H" ] || fail "newest manifest is ${AGE_H}h old (ceiling ${MAX_AGE_H}h): ${NEWEST} — the latest commit never landed"

# ── 3. Today, after the deadline ─────────────────────────────────────────────
HOUR="$(_ist +%H%M)"
if [ "$HOUR" -ge "$AFTER" ]; then
    T_KEYS="$(list_prefix "today ${TODAY}" "${DATA_PREFIX}${TODAY}/")"
    T="$(printf '%s\n' "$T_KEYS" | count_data "${DATA_PREFIX}${TODAY}/")"
    printf 'today data objects: %s\n' "$T"
    [ "$T" -gt 0 ] || fail "no data for ${TODAY} after ${AFTER:0:2}:${AFTER:2:2} IST under ${DATA_PREFIX}${TODAY}/"
fi
echo 'lake-guard PASS'
