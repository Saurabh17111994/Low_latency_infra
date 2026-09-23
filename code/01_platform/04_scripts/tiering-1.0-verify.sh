#!/usr/bin/env bash
# tiering-1.0-verify.sh (2026-09-23) — the scheduled verification that lake tiering
# works on Fluss 1.0.0 (native-adoption plan, entry condition; closes FACT-011's 1.0.0
# re-verification, the held W4 jar, and T7.1-T7.4).
#
# DESIGN (operator choice 2026-09-23): fresh table + fresh R2 prefix, NO archiving of
# raw_table_1's stale lake objects. Nothing here touches raw_table_1 or its R2 path.
#
# WHY A RECREATE IS NEEDED (verified in 1.0.0 source, not inferred):
#   - remote.log.task-interval-duration is read at RemoteLogManager construction and is
#     NOT in DynamicServerConfig's allowlist (fixed key set + the "datalake." prefix only)
#   - log.segment.file-size is server-scoped: table.log.* has 6 options and no segment size
# Tiering only copies ROLLED segments, so the window needs a small segment size to roll one
# from a few thousand rows. Both overrides travel in the SAME tablet recreate, and the SAME
# revert restores both. The certificate still describes the 0s/1GB configuration.
#
# SAFETY: a trap always restores .env and the tablet. NOTE (fixed 2026-09-23): the recreate
# MUST carry --no-deps — without it compose recreates the dependency chain (zookeeper +
# coordinator) and a mid-removal name collision renames the coordinator container to
# <hash>_01_docker-fluss-coordinator-1., whatever else fails below.
set -uo pipefail

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
ROOT="$(cd "$_SCRIPT_DIR/../../.." && pwd)"   # repo root: 04_scripts -> 01_platform -> code -> repo
cd "$ROOT" || exit 1
DOCKER_DIR=code/01_platform/01_docker
DC="docker compose --env-file $DOCKER_DIR/.env --env-file $DOCKER_DIR/secrets.env -f $DOCKER_DIR/docker-compose.yml"
TABLET=01_docker-fluss-tablet-1
JM=01_docker-flink-jobmanager-1
TS="$(date -u +%Y%m%dT%H%M%SZ)"
TABLE="tier_probe_$(date -u +%s)"
OUT="logs/soak/tiering-1.0-verify-$TS"
mkdir -p "$OUT"
RESULT() { echo "$@" | tee -a "$OUT/RESULT.txt"; }

echo "=== tiering-1.0-verify start $(date -Iseconds)  table=$TABLE  out=$OUT"

# ---------------- phase 0: snapshot the pre-state ----------------
cp "$DOCKER_DIR/.env" "$OUT/env.before"
grep -n "FLUSS_REMOTE_LOG_TASK_INTERVAL\|log.segment" "$DOCKER_DIR/.env" > "$OUT/env-relevant.before" 2>&1 || true
docker inspect "$TABLET" > "$OUT/tablet-inspect.before.json" 2>&1 || true
docker exec "$TABLET" sh -c 'grep -E "task-interval|segment.file-size" /opt/fluss/conf/server.yaml' > "$OUT/tablet-config.before" 2>&1 || true
bash code/01_platform/04_scripts/tiering-start.sh --status > "$OUT/tiering-job.before" 2>&1
echo "  job before: $(head -1 "$OUT/tiering-job.before")"
# R2 baseline for this name: must be zero objects under a prefix that does not exist yet
bash -c 'source code/01_platform/04_scripts/r2-list.sh >/dev/null 2>&1; r2_list_lake "$1" 2>/dev/null | wc -l' _ "$TABLE" > "$OUT/r2-baseline.count" 2>&1 || true
echo "  R2 objects under probe name before: $(cat "$OUT/r2-baseline.count" 2>/dev/null)"
CONTAINERS_BEFORE="$(docker ps --format '{{.Names}}' | sort | wc -l)"
echo "  containers before: $CONTAINERS_BEFORE"

# ---------------- revert (always) ----------------
reverted=0
revert() {
  [ "$reverted" = 1 ] && return 0
  reverted=1
  echo; echo "=== REVERT (4 steps): .env -> certified, tablet recreated, fixture dropped, job cancelled ==="
  cp "$OUT/env.before" "$DOCKER_DIR/.env" && echo "  [1/4] .env restored from snapshot"
  bash code/01_platform/04_scripts/stack-lock.sh $DC up -d --no-deps --force-recreate fluss-tablet > "$OUT/revert-tablet.log" 2>&1 \
    && echo "  [2/4] tablet recreated (certified config)" || echo "  [2/4] !! tablet recreate FAILED - see $OUT/revert-tablet.log"
  docker exec "$TABLET" sh -c 'grep -E "task-interval|segment.file-size" /opt/fluss/conf/server.yaml' > "$OUT/tablet-config.after" 2>&1 || true
  echo "  [2/4] tablet config now: $(tr '\n' ' ' < "$OUT/tablet-config.after")"
  if [ -n "${JID:-}" ]; then
    docker exec "$JM" flink cancel "$JID" > "$OUT/revert-cancel.log" 2>&1 \
      && echo "  [4/4] tiering job $JID cancelled" || echo "  [4/4] !! cancel failed - see $OUT/revert-cancel.log"
  else
    echo "  [4/4] no job id recorded"
  fi
  # fixture drop is done by the explicit step below when reachable; do it again defensively
  { echo "SET 'sql-client.execution.result-mode' = 'tableau';"
    echo "SET 'execution.runtime-mode' = 'batch';"
    echo "CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator:9123');"
    echo "USE CATALOG fluss_catalog;"
    echo "DROP TABLE $TABLE;"; } > /tmp/.tiering-drop.sql
  docker cp /tmp/.tiering-drop.sql "$JM:/tmp/.tiering-drop.sql" >/dev/null 2>&1 && timeout 120 docker exec "$JM" /opt/flink/bin/sql-client.sh -f /tmp/.tiering-drop.sql > "$OUT/revert-drop.log" 2>&1 || true
}
trap revert EXIT

# ---------------- phase 1: tablet overrides (one recreate) ----------------
echo; echo "=== phase 1: tablet overrides — task interval 1m, segment size 1mb ==="
python3 - <<'PY'
import pathlib
p = pathlib.Path("code/01_platform/01_docker/.env")
s = p.read_text()
old = "FLUSS_REMOTE_LOG_TASK_INTERVAL=0s\n"
assert s.count(old) == 1, f"interval line not found exactly once: {s.count(old)}"
s = s.replace(old, "FLUSS_REMOTE_LOG_TASK_INTERVAL=1m\n")
if "FLUSS_LOG_SEGMENT_FILE_SIZE" not in s:
    s = s.rstrip("\n") + "\n\n# tiering-1.0-verify.sh (2026-09-23): window-only override so a few thousand\n# rows roll a segment (tiering copies rolled segments only). Reverted by the script.\nFLUSS_LOG_SEGMENT_FILE_SIZE=1mb\n"
p.write_text(s)
print("  .env: interval 0s->1m, segment file size 1mb added")
PY
# the segment size needs a property line in the tablet service; add it next to the interval
python3 - <<'PY'
import pathlib, re
p = pathlib.Path("code/01_platform/01_docker/docker-compose.yml")
s = p.read_text()
pat = re.compile(r"^([ \t]*)remote\.log\.task-interval-duration: \$\{FLUSS_REMOTE_LOG_TASK_INTERVAL:-1m\}[ \t]*$", re.MULTILINE)
m = pat.search(s)
assert m, "interval property line not found in compose"
ind = m.group(1)
if "log.segment.file-size" in s:
    print("  compose: log.segment.file-size already parameterized (idempotent, nothing added)")
else:
    add = f"\n{ind}# tiering-1.0-verify.sh (2026-09-23): window-only companion to the interval override above;\n{ind}# restored with it. Remove when the verification is recorded.\n{ind}log.segment.file-size: ${{FLUSS_LOG_SEGMENT_FILE_SIZE:-1024m}}"
    s = s[:m.end()] + add + s[m.end():]
    p.write_text(s)
    print("  compose: log.segment.file-size parameterized (default 1024m = stock)")
PY
bash code/01_platform/04_scripts/stack-lock.sh $DC up -d --no-deps --force-recreate fluss-tablet > "$OUT/tablet-recreate.log" 2>&1 \
  && echo "  tablet recreated" || { echo "!! tablet recreate failed"; tail -5 "$OUT/tablet-recreate.log"; exit 1; }
sleep 12
docker exec "$TABLET" sh -c 'grep -E "task-interval|segment.file-size" /opt/fluss/conf/server.yaml' > "$OUT/tablet-config.during" 2>&1 || true
echo "  tablet config during window: $(tr '\n' ' ' < "$OUT/tablet-config.during")"
grep -q "1m" "$OUT/tablet-config.during" || { echo "!! interval override did not take effect"; exit 1; }

# ---------------- phase 2: tiering job ----------------
echo; echo "=== phase 2: start the tiering job (GUARD-A of the smoke) ==="
bash code/01_platform/04_scripts/tiering-start.sh > "$OUT/tiering-job.start" 2>&1 \
  && echo "  tiering-start.sh ok" || { echo "!! tiering-start.sh failed"; tail -8 "$OUT/tiering-job.start"; exit 1; }
bash code/01_platform/04_scripts/tiering-start.sh --status > "$OUT/tiering-job.status" 2>&1 \
  && echo "  job RUNNING: $(grep -oE '[0-9a-f]{32}' "$OUT/tiering-job.status" | head -1)" || { echo "!! no RUNNING job"; cat "$OUT/tiering-job.status"; exit 1; }
JID="$(grep -oE '[0-9a-f]{32}' "$OUT/tiering-job.status" | head -1)"
echo "$JID" > "$OUT/job-id"

# ---------------- phase 3: fixture table + rows ----------------
echo; echo "=== phase 3: fresh datalake-enabled table $TABLE + rows ==="
SQLDIR="$OUT/sql"; mkdir -p "$SQLDIR"
{
  echo "SET 'sql-client.execution.result-mode' = 'tableau';"
  echo "SET 'execution.runtime-mode' = 'batch';"
  echo "CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator:9123');"
  echo "USE CATALOG fluss_catalog;"
  echo "CREATE TABLE $TABLE (event_day STRING, id INT, note STRING, PRIMARY KEY (event_day, id) NOT ENFORCED) PARTITIONED BY (event_day) WITH ("
  echo "  'bucket.num' = '1',"
  echo "  'table.datalake.enabled' = 'true',"
  echo "  'table.datalake.format' = 'iceberg',"
  echo "  'table.datalake.freshness' = '5min',"
  echo "  'table.datalake.auto-compaction' = 'true');"
} > "$SQLDIR/01-create.sql"
python3 - "$SQLDIR" <<'PY'
import sys, pathlib, datetime
d = pathlib.Path(sys.argv[1]); day = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
for n in range(6):                      # 6 x 5000 rows, well past a 1mb segment
    rows = ",".join(f"('{day}',{n*5000+i},'tiering probe row {n*5000+i} padding padding padding')" for i in range(5000))
    (d / f"02-insert-{n}.sql").write_text(
        "SET 'sql-client.execution.result-mode' = 'tableau';\nSET 'execution.runtime-mode' = 'batch';\n"
        "CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator:9123');\n"
        "USE CATALOG fluss_catalog;\n"
        f"INSERT INTO {sys.argv[2] if len(sys.argv) > 2 else '@TABLE@'} VALUES {rows};\n")
print("  6 insert scripts written (30,000 rows)")
PY
# the TABLE name is interpolated by the shell, not passed twice
for f in "$SQLDIR"/02-insert-*.sql; do sed -i "s/@TABLE@/$TABLE/" "$f"; done
run_sql() { # run_sql <file> <tag>
  local f="$1" tag="$2"
  docker cp "$f" "$JM:/tmp/$(basename "$f")" >/dev/null 2>&1
  timeout 300 docker exec "$JM" /opt/flink/bin/sql-client.sh -f "/tmp/$(basename "$f")" > "$OUT/$tag.log" 2>&1
  local rc=$?
  echo "  $tag rc=$rc $(grep -icE 'error|exception' "$OUT/$tag.log" || true) error lines"
  return $rc
}
run_sql "$SQLDIR/01-create.sql" create || { echo "!! CREATE failed"; tail -12 "$OUT/create.log"; exit 1; }
for f in "$SQLDIR"/02-insert-*.sql; do run_sql "$f" "insert-$(basename "$f" .sql)" || echo "  !! insert $(basename "$f") failed"; done
echo "  rows expected: 30000"

# ---------------- phase 4: wait out freshness + interval ----------------
echo; echo "=== phase 4: waiting 420s (5min freshness + 1min task interval) ==="
for i in $(seq 1 7); do
  sleep 60
  echo "  +${i}min  tablet tiering mentions: $(docker logs --since 90s "$TABLET" 2>&1 | grep -icE 'tiering|remote.log|Tiering')  errors: $(docker logs --since 90s "$TABLET" 2>&1 | grep -icE 'ERROR.*tier|Tiering.*ERROR')"
done
docker logs --since 25m "$TABLET" > "$OUT/tablet.log.during" 2>&1 || true

# ---------------- phase 5: the proof — R2 objects ----------------
echo; echo "=== phase 5: R2 objects for $TABLE (the actual proof) ==="
bash -c 'source code/01_platform/04_scripts/r2-list.sh >/dev/null 2>&1; r2_list_all' > "$OUT/r2-all.after" 2>&1 || true
grep -i "$TABLE" "$OUT/r2-all.after" > "$OUT/r2-probe-objects" 2>&1 || true
COUNT="$(wc -l < "$OUT/r2-probe-objects")"
BYTES="$(awk -F'\t' '{s+=$2} END {print s+0}' "$OUT/r2-probe-objects")"
echo "  objects: $COUNT   total bytes: $BYTES"
head -5 "$OUT/r2-probe-objects" | cut -c1-120 | sed 's/^/    /'
TIER_ERR="$(grep -icE 'Tiering.*(ERROR|Exception)|Exception.*[Tt]iering' "$OUT/tablet.log.during" || true)"
echo "  tiering errors in tablet log: $TIER_ERR"
if [ "$COUNT" -gt 0 ] && [ "$BYTES" -gt 0 ]; then
  RESULT "TIERING-1.0-VERIFY: PASS — $COUNT lake objects, $BYTES bytes under $TABLE (fresh prefix, no archiving); tablet tiering errors: $TIER_ERR"
else
  RESULT "TIERING-1.0-VERIFY: FAIL — no lake objects under $TABLE (see $OUT); tablet tiering errors: $TIER_ERR"
fi

# ---------------- phase 6: revert (explicit; the trap is the backstop) ----------------
echo; echo "=== phase 6: revert ==="
bash -c "source '$ROOT/code/01_platform/04_scripts/r2-env.sh' 2>/dev/null" || true
{ echo "SET 'sql-client.execution.result-mode' = 'tableau';"; echo "SET 'execution.runtime-mode' = 'batch';";
  echo "CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator:9123');";
  echo "USE CATALOG fluss_catalog;"; echo "DROP TABLE $TABLE;"; } > "$SQLDIR/99-drop.sql"
docker cp "$SQLDIR/99-drop.sql" "$JM:/tmp/99-drop.sql" >/dev/null 2>&1
timeout 300 docker exec "$JM" /opt/flink/bin/sql-client.sh -f /tmp/99-drop.sql > "$OUT/drop-table.log" 2>&1 \
  && echo "  [3/4] fixture $TABLE dropped" || echo "  [3/4] !! drop failed - see $OUT/drop-table.log"
revert
echo; echo "  containers after: $(docker ps --format '{{.Names}}' | wc -l) (before: $CONTAINERS_BEFORE)"
echo "=== tiering-1.0-verify end $(date -Iseconds) ==="
