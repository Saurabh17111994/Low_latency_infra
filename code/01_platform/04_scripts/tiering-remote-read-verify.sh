#!/usr/bin/env bash
# tiering-remote-read-verify.sh (2026-09-23, CHG-306 follow-up)
#
# WHY THIS EXISTS: tiering-1.0-verify.sh proves the WRITE side (segments reach R2).
# Nothing had ever proven the READ side -- that a Fluss client can fetch a tiered
# (remote-only) segment back through RemoteFileDownloader. That is the untested
# consequence recorded in CHG-306 for the security-token ERROR
# ("Region is not set" from S3DelegationTokenProvider, raised server-side).
#
# HOW THE REMOTE READ IS FORCED (verified in Fluss 1.0.0 source, not inferred):
#   - the probe table sets table.log.tiered.local-segments=1 and table.log.local-ttl=1s,
#     so once a segment is tiered no local copy is kept and a read of those offsets
#     MUST come from R2 through the client's RemoteFileDownloader;
#   - the client-side signal is the token manager: SecurityTokenReceiverRepository
#     logs "New security tokens arrived" on success; DefaultSecurityTokenManager logs
#     "Failed to obtain security token" (+ "Region is not set") on failure. A table with
#     no remote segments produces neither (observed 2026-09-23 on raw_table_1).
#
# SAFETY: a trap always restores .env + the compose tablet override and recreates the
# tablet with --no-deps (without it compose recreates zookeeper/coordinator and a
# mid-removal name collision renames containers -- learned 2026-09-23).
set -euo pipefail
cd "$(dirname "$0")/../../.."
TS=$(date -u +%Y%m%dT%H%M%SZ)
TABLE="rr_probe_$(date -u +%s)"
ROWS=60000
OUT="logs/soak/tiering-remote-read-$TS"
mkdir -p "$OUT"
JM=01_docker-flink-jobmanager-1
TABLET=01_docker-fluss-tablet-1
DC="docker compose --env-file code/01_platform/01_docker/.env --env-file code/01_platform/01_docker/secrets.env -f code/01_platform/01_docker/docker-compose.yml"
cp code/01_platform/01_docker/.env "$OUT/env.before"
cp code/01_platform/01_docker/docker-compose.yml "$OUT/compose.before"
echo "=== tiering-remote-read-verify start $(date -Iseconds)  table=$TABLE  rows=$ROWS  out=$OUT"

sql() { # sql <file>  -> runs it in the JM sql-client
  docker cp "$1" "$JM:/tmp/.rr.sql" >/dev/null 2>&1
  timeout 600 docker exec "$JM" /opt/flink/bin/sql-client.sh -f /tmp/.rr.sql 2>&1
}

revert() {
  echo "  [revert] restoring .env + compose, recreating tablet, dropping $TABLE"
  cp "$OUT/env.before" code/01_platform/01_docker/.env
  cp "$OUT/compose.before" code/01_platform/01_docker/docker-compose.yml
  printf 'SET '"'"'sql-client.execution.result-mode'"'"'='"'"'tableau'"'"';\nCREATE CATALOG fluss_catalog WITH ('"'"'type'"'"'='"'"'fluss'"'"','"'"'bootstrap.servers'"'"'='"'"'fluss-coordinator:9123'"'"');\nUSE CATALOG fluss_catalog;\nDROP TABLE IF EXISTS `default`.%s;\n' "$TABLE" > /tmp/.rr-drop.sql
  ( sql /tmp/.rr-drop.sql > "$OUT/drop.log" 2>&1 || true )
  bash code/01_platform/04_scripts/stack-lock.sh $DC up -d --no-deps --force-recreate fluss-tablet > "$OUT/tablet-revert.log" 2>&1 || true
  if grep -q '^FLUSS_REMOTE_LOG_TASK_INTERVAL=0s' code/01_platform/01_docker/.env; then
    revert_ok=yes
  else
    revert_ok=no
  fi
  echo "  [revert] done (see $OUT/tablet-revert.log); dev tiering restored: $revert_ok"
}
trap revert EXIT

echo; echo "=== phase 1: tablet overrides (tiering on, 1mb segments) ==="
python3 - <<'PY'
import pathlib
p = pathlib.Path("code/01_platform/01_docker/.env"); s = p.read_text()
old = "FLUSS_REMOTE_LOG_TASK_INTERVAL=0s\n"
assert s.count(old) == 1, f"interval line not found exactly once: {s.count(old)}"
s = s.replace(old, "FLUSS_REMOTE_LOG_TASK_INTERVAL=1m\n")
if "FLUSS_LOG_SEGMENT_FILE_SIZE" not in s:
    s = s.rstrip("\n") + "\n\n# tiering-remote-read-verify.sh (2026-09-23): window-only override so a few\n# thousand rows roll a segment (tiering copies rolled segments only). Reverted by the trap.\nFLUSS_LOG_SEGMENT_FILE_SIZE=1mb\n"
p.write_text(s); print("  .env: interval 0s->1m, segment file size 1mb")
PY
python3 - <<'PY'
import pathlib, re
p = pathlib.Path("code/01_platform/01_docker/docker-compose.yml"); s = p.read_text()
pat = re.compile(r"^([ \t]*)remote\.log\.task-interval-duration: \$\{FLUSS_REMOTE_LOG_TASK_INTERVAL:-1m\}[ \t]*$", re.MULTILINE)
m = pat.search(s); assert m, "interval property line not found in compose"
if "log.segment.file-size" in s:
    print("  compose: segment size already parameterized")
else:
    ind = m.group(1)
    add = f"\n{ind}# tiering-remote-read-verify.sh (2026-09-23): window-only companion to the interval\n{ind}# override above; restored with it by the trap.\n{ind}log.segment.file-size: ${{FLUSS_LOG_SEGMENT_FILE_SIZE:-1024m}}"
    s = s[:m.end()] + add + s[m.end():]; p.write_text(s); print("  compose: segment size parameterized (default 1024m = stock)")
PY
bash code/01_platform/04_scripts/stack-lock.sh $DC up -d --no-deps --force-recreate fluss-tablet > "$OUT/tablet-recreate.log" 2>&1
echo "  tablet recreated; config during window:"
docker exec "$TABLET" sh -c 'grep -E "task-interval|segment.file-size" /opt/fluss/conf/server.yaml' | sed 's/^/    /'

echo; echo "=== phase 1.5: wait for leader election to settle (measured up to ~8 min after a pair recreate) ==="
SETTLE="rr_settle_$(date -u +%s)"
ready=no
for i in $(seq 1 24); do
  {
    echo "SET 'sql-client.execution.result-mode'='tableau';"
    echo "CREATE CATALOG fluss_catalog WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');"
    echo "USE CATALOG fluss_catalog;"
    echo "CREATE TABLE \`default\`.$SETTLE (id BIGINT) WITH ('bucket.num' = '1');"
    echo "INSERT INTO \`default\`.$SETTLE VALUES (1);"
    echo "SELECT COUNT(*) AS n FROM \`default\`.$SETTLE;"
    echo "DROP TABLE \`default\`.$SETTLE;"
  } > /tmp/.rr-settle.sql
  sql /tmp/.rr-settle.sql > "$OUT/settle-$i.log" 2>&1 || true
  if ! grep -qiE 'leader not found|exception|error|timeout' "$OUT/settle-$i.log"; then
    ready=yes; echo "  pair writable after $((i*15))s"; break
  fi
  echo "  attempt $i: $(grep -oiE 'Leader not found[^,]*|UnknownHostException|Exception|timeout' "$OUT/settle-$i.log" | head -1)"; sleep 15
done
[ "$ready" = yes ] || { echo "!! the Fluss pair never became writable; aborting (trap reverts)"; exit 1; }

echo; echo "=== phase 2: probe table forced to keep no local tiered segments ==="
{
  echo "SET 'sql-client.execution.result-mode'='tableau';"
  echo "CREATE CATALOG fluss_catalog WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');"
  echo "USE CATALOG fluss_catalog;"
  echo "CREATE TABLE \`default\`.$TABLE (id BIGINT, v STRING) WITH ("
  echo "  'bucket.num' = '1',"
  echo "  'table.log.tiered.local-segments' = '1',"
  echo "  'table.log.local-ttl' = '1s'"
  echo ");"
  python3 -c "
rows=$ROWS
for start in range(0, rows, 500):
    vals = ','.join('(%d,\'v%d\')' % (i, i) for i in range(start, min(start+500, rows)))
    print('INSERT INTO \`default\`.$TABLE VALUES %s;' % vals)
"
} > /tmp/.rr-create.sql
sql /tmp/.rr-create.sql > "$OUT/create.log" 2>&1
echo "  create+insert done; errors: $(grep -ciE 'error|exception' "$OUT/create.log" || true)"
grep -iE 'error|exception' "$OUT/create.log" | head -3 | cut -c1-150 | sed 's/^/    /' || true

echo; echo "=== phase 3: wait for the segment to reach R2 ==="
source code/01_platform/04_scripts/r2-list.sh >/dev/null 2>&1 || true
objs=0
for i in $(seq 1 40); do
  objs=$(r2_list_lake "$TABLE" 2>/dev/null | wc -l)
  if [ "${objs:-0}" -gt 0 ]; then echo "  R2 objects for $TABLE: $objs (after $((i*15))s)"; break; fi
  sleep 15
done
r2_list_lake "$TABLE" 2>/dev/null > "$OUT/r2-objects" || true
echo "  tablet tiering errors: $(docker logs $TABLET 2>&1 | grep -ciE 'tiering.*(error|fail)' || true)"
if [ "${objs:-0}" -eq 0 ]; then echo "!! no R2 objects -> tiering did not run; aborting (trap reverts)"; exit 1; fi

echo; echo "=== phase 4: read the table back (must fetch remote-only segments) ==="
pre_jm=$(docker logs $JM 2>&1 | grep -c 'security token' || true)
{
  echo "SET 'sql-client.execution.result-mode'='tableau';"
  echo "SET 'execution.runtime-mode'='batch';"
  echo "CREATE CATALOG fluss_catalog WITH ('type'='fluss','bootstrap.servers'='fluss-coordinator:9123');"
  echo "USE CATALOG fluss_catalog;"
  echo "SELECT COUNT(*) AS rows_read FROM \`default\`.$TABLE;"
} > /tmp/.rr-read.sql
sql /tmp/.rr-read.sql > "$OUT/read.log" 2>&1
read_rows=$(grep -oE '\|[[:space:]]*[0-9]+[[:space:]]*\|' "$OUT/read.log" | head -1 | tr -dc '0-9' || true)
if [ -z "$read_rows" ]; then read_rows=$(grep -oE '^ *[0-9]+ *\|' "$OUT/read.log" | head -1 | tr -dc '0-9' || true); fi
if [ -z "$read_rows" ]; then echo "  !! could not parse the row count; last 12 lines of read.log:"; tail -12 "$OUT/read.log" | sed 's/^/    /'; fi
echo "  rows read: ${read_rows:-<none>}  (written: $ROWS)"
echo "  read errors: $(grep -ciE 'exception|Region is not set|Failed to obtain|UnknownHost' "$OUT/read.log" || true)"
grep -iE 'exception|Region is not set|Failed to obtain|UnknownHost' "$OUT/read.log" | head -3 | cut -c1-150 | sed 's/^/    /' || true
echo "  client-side token lines in the JM log:"
docker logs $JM 2>&1 | grep -iE 'security token|New security tokens|Region is not set|Failed to obtain' | tail -5 | cut -c1-160 | sed 's/^/    /'
echo "  (token lines before this read: $pre_jm)"
echo "  server-side token/mint lines:"
docker logs 01_docker-fluss-coordinator-1 2>&1 | grep -iE 'security token|session credential|Region is not set|Failed to get file access' | tail -4 | cut -c1-160 | sed 's/^/    /' || true

echo; echo "=== phase 5: revert ==="
echo "  rows_read=$read_rows written=$ROWS r2_objects=$objs"
if [ "${read_rows:-0}" = "$ROWS" ]; then
  RESULT="TIERING-REMOTE-READ: PASS -- $ROWS rows written, $ROWS read back through a table with table.log.tiered.local-segments=1 + local-ttl=1s (remote-only after expiry); check the token lines above for whether the R2 mint succeeded"
else
  RESULT="TIERING-REMOTE-READ: FAIL -- read $read_rows of $ROWS rows; see $OUT/read.log"
fi
echo "$RESULT" | tee "$OUT/RESULT.txt"
