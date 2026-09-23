#!/usr/bin/env bash
# stack-restore-verify.sh (2026-09-23) — bring the dev stack up in the CERTIFIED config and
# verify it by Fluss REACHABILITY, not container counts (T4.0's lesson: an empty networks list
# is not proof of health). Body derived from T4.0's t41h-up-verify.sh; durable here so the
# scratch window's restore can reuse it. PROMOTED 2026-09-23 to tracked 04_scripts/: a durable
# restore-and-verify is exactly what belongs here rather than in an untracked evidence directory.
#
# Context: the tiering verification's revert recreated dependencies along with the tablet
# (revert-tablet.log shows the coordinator being recreated), leaving coordinator/tablet/Flink
# down. .env is already restored to FLUSS_REMOTE_LOG_TASK_INTERVAL=0s, so `make up` brings the
# stack back certified. Nothing is recreated by hand and no data moves.
set -u
cd "$(dirname "${BASH_SOURCE[0]}")/../../.." || exit 1   # repo root: this lives in code/01_platform/04_scripts/
OUT="logs/soak/stack-restore-$(date -u +%Y%m%dT%H%M%SZ)"; mkdir -p "$OUT"
echo "=== stack-restore-verify $(date -Iseconds) out=$OUT"

echo; echo "### 0. the two window property lines must render as stock (1m server default, 1024m) ###"
docker compose --env-file code/01_platform/01_docker/.env --env-file code/01_platform/01_docker/secrets.env \
  -f code/01_platform/01_docker/docker-compose.yml config 2>/dev/null \
  | grep -E "log.segment.file-size|remote.log.task-interval-duration|kv.snapshot.interval" | sed 's/^/  /'
echo "  (interval column must read 0s: .env line 129 restored to the certified override)"

echo; echo "### 1. make up ###"
make up > "$OUT/make-up.log" 2>&1; MKRC=$?
echo "  make up rc=$MKRC"
if [ "$MKRC" != 0 ]; then
  echo "  !! make up FAILED — stopping here: every check below would be measuring an absent stack,"
  echo "  !! and this script used to report success anyway. See $OUT/make-up.log"
  tail -6 "$OUT/make-up.log" | sed 's/^/    /'
  exit 2
fi
tail -4 "$OUT/make-up.log" | sed 's/^/    /'
sleep 25

echo; echo "### 2. container -> network attachment ###"
bad=0
for c in $(docker ps --format '{{.Names}}' | grep '^01_docker-' | sort); do
  net=$(docker inspect "$c" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null)
  [ -z "$net" ] && bad=$((bad+1))
  printf "  %-40s net=[%s]\n" "$c" "$net"
done
echo "  no-network containers: $bad  (alert-consumer is net=[] by design: network_mode service:openobserve)"

echo; echo "### 3. zookeeper attachment / documented remedy ###"
zknet=$(docker inspect 01_docker-zookeeper-1 --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null)
case "$zknet" in
  *01_docker_trading-net*) echo "  zookeeper on [${zknet}] ok";;
  *) echo "  zookeeper net=[${zknet}] -> re-attaching with the service alias"
     docker network connect --alias zookeeper 01_docker_trading-net 01_docker-zookeeper-1 2>&1 | sed 's/^/    /'
     docker restart 01_docker-fluss-coordinator-1 01_docker-fluss-tablet-1 01_docker-flink-jobmanager-1 01_docker-flink-taskmanager-1 > /dev/null 2>&1
     echo "    restart rc=$?"; sleep 35;;
esac

echo; echo "### 4. Fluss reachability ###"
echo "  coordinator resolves zookeeper: $(docker exec 01_docker-fluss-coordinator-1 sh -c 'getent hosts zookeeper 2>/dev/null || echo UNRESOLVED')"
echo "  JM -> fluss-coordinator:9123:   $(docker exec 01_docker-flink-jobmanager-1 sh -c 'timeout 4 bash -c "echo > /dev/tcp/fluss-coordinator/9123" >/dev/null 2>&1 && echo OPEN || echo CLOSED' 2>/dev/null)"
echo "  host -> localhost:9123:         $(timeout 4 bash -c 'echo > /dev/tcp/127.0.0.1/9123' >/dev/null 2>&1 && echo OPEN || echo CLOSED)"
echo "  tablet config (must be certified): $(docker exec 01_docker-fluss-tablet-1 sh -c 'grep -E "task-interval|segment.file-size" /opt/fluss/conf/server.yaml' 2>&1 | tr '\n' ' ')"

echo; echo "### 5. catalog, flink, jobs ###"
bash code/01_platform/04_scripts/catalog-guard.sh > "$OUT/catalog-guard.log" 2>&1
grep -aE "probe:|healthy|MORE|MISSING" "$OUT/catalog-guard.log" | head -4 | sed 's/^/  /'
curl -s --max-time 12 http://localhost:8081/overview | python3 -c "import json,sys; d=json.load(sys.stdin); print('  Flink', d.get('flink-version'), 'TMs', d.get('taskmanagers'), 'slots', d.get('slots-available'))" 2>/dev/null
curl -s --max-time 12 http://localhost:8081/jobs > "$OUT/flink-jobs.json" 2>&1
python3 - "$OUT/flink-jobs.json" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    jobs = d.get("jobs", [])
    if not jobs:
        print("  Flink jobs: NONE — a JM recreate dropped them; the observer job needs resubmission")
    for j in jobs:
        print(f"  job {j.get('name')}: {j.get('status')}  ({j.get('jid')})")
except Exception as e:
    print(f"  !! could not read Flink jobs: {e}")
PY
echo "  project containers up: $(docker ps --format '{{.Names}}' | grep -c '^01_docker-')"

echo; echo "### 6. VERDICT (the line that used to say DONE no matter what) ###"
upcount="$(docker ps --format '{{.Names}}' | grep -c '^01_docker-')"
catalog_ok=0; grep -aq "27/27" "$OUT/catalog-guard.log" 2>/dev/null && catalog_ok=1
jm_open=0
docker exec 01_docker-flink-jobmanager-1 sh -c 'timeout 4 bash -c "echo > /dev/tcp/fluss-coordinator/9123"' >/dev/null 2>&1 && jm_open=1
jobs_up=0
python3 -c "import json,sys; d=json.load(open('$OUT/flink-jobs.json')); sys.exit(0 if any(j.get('status')=='RUNNING' for j in d.get('jobs',[])) else 1)" 2>/dev/null && jobs_up=1
echo "  make up rc=${MKRC:-?} | containers=$upcount | catalog-guard 27/27=$catalog_ok | JM->coordinator=$jm_open | job RUNNING=$jobs_up"
if [ "${MKRC:-1}" = 0 ] && [ "$upcount" -gt 0 ] && [ "$catalog_ok" = 1 ] && [ "$jm_open" = 1 ]; then
  echo "  RESTORE VERDICT: OK — the dev stack is reachable and the catalog guard is green"
else
  echo "  RESTORE VERDICT: FAILED — the dev stack is NOT certified; do not trust anything downstream"
  exit 3
fi
echo DONE
