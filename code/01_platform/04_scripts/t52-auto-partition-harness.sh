#!/usr/bin/env bash
# t52-auto-partition-harness.sh (2026-09-23) — T5.2 local half, native-first per DEC-053/A3.
#
# CLAIM BEING TESTED: a late write INSIDE the partition retention lands and is readable; a write
# OUTSIDE retention fails VISIBLY (never silently), which is P4-170's documented behaviour for
# event_time outside the retention window.
#
# Run this ONLY against the scratch cluster (`COMPOSE_PROJECT_NAME=fluss10scratch`): it creates
# tables, and the certified dev catalog must stay at 27/27. Never run it on the dev project.
#
# DDL shape is taken from Fluss 1.0.0's own FlinkTableSinkITCase: `PARTITIONED BY (col)` plus
# 'table.auto-partition.enabled'/'time-unit'; the partition column is a STRING holding the
# formatted time, and `auto-partition.key` is needed only when the partition value is DERIVED
# from a different column. So the written value itself decides whether the server auto-creates
# the partition (inside retention) or refuses (outside).
#
# Timing model (from the 1.0.0 source): num-precreate partitions ahead are created at DDL time;
# num-retention partitions behind are kept. We write at offsets inside and outside that window.
set -u
_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
cd "$(cd "$_SCRIPT_DIR/../../.." && pwd)" || exit 1   # repo root
JM="${T52_JM:-01_docker-flink-jobmanager-1}"   # window runs pass the scratch JM name
PREFIX="${1:-t52}"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="logs/soak/t52-auto-partition-$TS"; mkdir -p "$OUT/sql"
TZONE="${T52_TZ:-Asia/Kolkata}"
RETENTION=3; PRECREATE=2
TABLE="${PREFIX}_autopart_$(date -u +%s)"

echo "=== T5.2 auto-partition harness $(date -Iseconds)"
echo "  table=$TABLE  zone=$TZONE  num-retention=$RETENTION  num-precreate=$PRECREATE  out=$OUT"

# partition values: today, inside retention (today-1), outside retention (today-10)
# Partition values MUST use the table's own time-format. For time-unit=DAY the default is `yyyyMMdd`,
# and the partition names prove it (they read event_day=20260923, not event_day=2026-09-23). Writing
# ISO dates made every row miss its partition and the read-back legitimately returned 0 - so the values
# are emitted in the table's format and then cross-checked against the partitions below.
mapfile -t DAYS < <(python3 - "$TZONE" <<'PY'
import sys, datetime
from zoneinfo import ZoneInfo
tz = ZoneInfo(sys.argv[1]); today = datetime.datetime.now(tz).date()
for off in (0, -1, -10):
    print((today + datetime.timedelta(days=off)).strftime("%Y%m%d"))
PY
)
TODAY="${DAYS[0]}"; INSIDE="${DAYS[1]}"; OUTSIDE="${DAYS[2]}"   # yyyyMMdd
echo "  day offsets: today=$TODAY  inside=$INSIDE  outside=$OUTSIDE"

run_sql() { # run_sql <file> <tag> ; echoes rc, returns rc
  local f="$1" tag="$2"
  docker cp "$f" "$JM:/tmp/$(basename "$f")" >/dev/null 2>&1
  timeout 300 docker exec "$JM" /opt/flink/bin/sql-client.sh -f "/tmp/$(basename "$f")" > "$OUT/$tag.log" 2>&1
  local rc=$?
  echo "  $tag: rc=$rc"
  return $rc
}
hdr() { printf "%s\n" "SET 'sql-client.execution.result-mode' = 'tableau';" "SET 'execution.runtime-mode' = 'batch';"; }

# ---- A. create the auto-partitioned table -------------------------------------------
{ hdr
  echo "CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator:9123');"
  echo "USE CATALOG fluss_catalog;"
  echo "CREATE TABLE $TABLE (event_day STRING, id INT, payload STRING) PARTITIONED BY (event_day) WITH ("
  echo "  'bucket.num' = '1',"
  echo "  'table.auto-partition.enabled' = 'true',"
  echo "  'table.auto-partition.time-unit' = 'day',"
  echo "  'table.auto-partition.time-zone' = '$TZONE',"
  echo "  'table.auto-partition.num-precreate' = '$PRECREATE',"
  echo "  'table.auto-partition.num-retention' = '$RETENTION');"
} > "$OUT/sql/A-create.sql"
run_sql "$OUT/sql/A-create.sql" A-create || { echo "T52-RESULT: FAIL (create)"; exit 1; }

# ---- B. partitions that exist right after DDL (precreate window) --------------------
{ hdr
  echo "CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator:9123');"
  echo "USE CATALOG fluss_catalog;"
  echo "SHOW PARTITIONS $TABLE;"
} > "$OUT/sql/B-partitions.sql"
run_sql "$OUT/sql/B-partitions.sql" B-partitions || echo "  (B rc!=0 — record, not fatal)"
echo "  partitions now:"; grep -aE "^\s*\+|event_day=" "$OUT/B-partitions.log" | head -12 | cut -c1-110 | sed 's/^/    /'

# Cross-check the format we assumed against the partitions the server actually created. Without this, a
# format mismatch shows up only as a mysterious "0 rows", which is exactly what happened once already.
FMT_OK=0
grep -aq "event_day=$TODAY" "$OUT/B-partitions.log" && FMT_OK=1
if [ "$FMT_OK" = 1 ]; then
  echo "  format check: assumed $TODAY matches an existing partition (OK)"
else
  echo "  format check: MISMATCH - no partition named event_day=$TODAY; server created:"
  grep -aoE "event_day=[0-9A-Za-z_-]+" "$OUT/B-partitions.log" | head -4 | sed 's/^/    /'
fi

# ---- C/D. writes inside retention: today and today-1 --------------------------------
for pair in "C-today:$TODAY:100" "D-inside:$INSIDE:200"; do
  tag="${pair%%:*}"; rest="${pair#*:}"; day="${rest%%:*}"; id="${rest##*:}"
  { hdr
    echo "CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator:9123');"
    echo "USE CATALOG fluss_catalog;"
    for k in 1 2 3; do echo "INSERT INTO $TABLE VALUES ('$day', $((id+k)), 'inside-retention row $k');"; done
    # Alias must not contain dashes: an unquoted alias like rows_2026-09-23 is a ParseException
    echo "SELECT COUNT(*) AS rows_ok FROM $TABLE WHERE event_day = '$day';"
  } > "$OUT/sql/$tag.sql"
  run_sql "$OUT/sql/$tag.sql" "$tag" || echo "  !! $tag wrote or read badly — inspect $OUT/$tag.log"
  grep -aE "rows_|ERROR|Exception" "$OUT/$tag.log" | head -4 | cut -c1-120 | sed 's/^/    /'
done

# ---- E. write OUTSIDE retention: must fail VISIBLY ----------------------------------
{ hdr
  echo "CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator:9123');"
  echo "USE CATALOG fluss_catalog;"
  echo "INSERT INTO $TABLE VALUES ('$OUTSIDE', 999, 'outside-retention row');"
} > "$OUT/sql/E-outside.sql"
run_sql "$OUT/sql/E-outside.sql" E-outside; E_RC=$?
E_ERR="$(grep -acE "ERROR|Exception|not.*partition|partition.*not" "$OUT/E-outside.log" || true)"
# The batch sql-client prints "successfully submitted" + a Job ID and exits 0 even when the sink fails, so
# the failure has to be read where it actually appears: the job/JobManager log. What Fluss raises is a
# specific, actionable error naming the earliest retained partition - that is the visible failure.
E_JOB_ERR="$(docker logs --since 6m "$JM" 2>&1 | grep -ac "InvalidPartitionException.*$OUTSIDE" || true)"
echo "  outside-retention write: client rc=$E_RC, client error lines=$E_ERR, job-level InvalidPartitionException=$E_JOB_ERR"
docker logs --since 6m "$JM" 2>&1 | grep -a "InvalidPartitionException" | tail -1 | cut -c1-200 | sed 's/^/    /' 

# ---- F. verdict ---------------------------------------------------------------------
# PASS = inside writes read back, outside write visibly rejected (nonzero rc AND an error line).
# Inside-retention evidence: every statement succeeded AND the read-back row shows exactly 3.
INS_OK=0
INS_SUCC="$(grep -ac "Execute statement succeeded" "$OUT/C-today.log" 2>/dev/null || echo 0)"
INS_ROWS="$(grep -acE '^\| *3 *\|' "$OUT/C-today.log" 2>/dev/null || echo 0)"
echo "  inside-retention: $INS_SUCC statements succeeded, $INS_ROWS read-back row(s) showing 3"
[ "$INS_SUCC" -ge 4 ] && [ "$INS_ROWS" -ge 1 ] && INS_OK=1
D_OK=0
D_ROWS="$(grep -acE '^\| *3 *\|' "$OUT/D-inside.log" 2>/dev/null || echo 0)"
[ "$D_ROWS" -ge 1 ] && D_OK=1
echo "  late write inside retention (today-1): $D_ROWS read-back row(s) showing 3"
if [ "$INS_OK" = 1 ] && [ "$D_OK" = 1 ] && { [ "$E_RC" -ne 0 ] || [ "$E_JOB_ERR" -gt 0 ]; }; then
  echo "T52-RESULT: PASS — today lands and reads back; the LATE write inside retention lands in its own on-demand partition; outside retention is rejected with InvalidPartitionException (job-level=$E_JOB_ERR, client rc=$E_RC)"
else
  echo "T52-RESULT: FAIL — inside_ok=$INS_OK outside_rc=$E_RC outside_errors=$E_ERR (inspect $OUT)"
fi
echo "  (table $TABLE left in catalog for evidence; drop it with the scratch project)"
echo "=== harness end $(date -Iseconds) ==="
