#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
COMPUTE_DIR="${REPO_ROOT}/code/02_services/02_compute"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
TM_METRICS_URL="${TM_METRICS_URL:-http://localhost:9250/metrics}"
echo "TM-KILL-CHAOS-02: start"
# Leg A always offline via MiniCluster
echo "TM-KILL-CHAOS-02: [leg A] running SignalJobTaskManagerKillIntegrationTest"
if ! command -v mvn >/dev/null 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg A] FAIL — mvn not found" >&2
  exit 1
fi
if ! mvn -f "${COMPUTE_DIR}/pom.xml" -o test-compile -q 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg A] compile check failed, retry without -o" >&2
  if ! mvn -f "${COMPUTE_DIR}/pom.xml" test-compile -q 2>&1; then
    echo "TM-KILL-CHAOS-02: [leg A] FAIL — compile" >&2
    exit 1
  fi
fi
if ! COMPUTE_INT_TEST_TM_KILL=true mvn -f "${COMPUTE_DIR}/pom.xml" -Dtest=SignalJobTaskManagerKillIntegrationTest -DfailIfNoTests=false test 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg A] FAIL — restore from checkpoint, no duplicate fingerprint" >&2
  exit 1
fi
echo "TM-KILL-CHAOS-02: [leg A] PASS — restore from checkpoint, no duplicate fingerprint"
# Leg B optional live stack
if ! command -v docker >/dev/null 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg B] SKIP — docker not found" >&2
  exit 0
fi
if ! docker ps --filter "name=flink-taskmanager" --format "{{.Names}}" 2>/dev/null | grep -q "flink-taskmanager"; then
  echo "TM-KILL-CHAOS-02: [leg B] SKIP — no flink-taskmanager container (stack not up)" >&2
  exit 0
fi
echo "TM-KILL-CHAOS-02: [leg B] live stack detected — SIGKILL taskmanager"
TM_CONTAINER="$(docker ps --filter "name=flink-taskmanager" --format "{{.ID}}" 2>/dev/null | head -n 1)"
if [[ -z "${TM_CONTAINER}" ]]; then
  echo "TM-KILL-CHAOS-02: [leg B] SKIP — no container id" >&2
  exit 0
fi
# capture pre-kill checkpoint
echo "TM-KILL-CHAOS-02: [leg B] killing ${TM_CONTAINER}"
if ! docker kill -s KILL "${TM_CONTAINER}" 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg B] FAIL — docker kill" >&2
  exit 1
fi
# wait for restart — explicit `docker start` after a SIGKILL: on this docker
# daemon `docker kill -s KILL` does not trigger the restart policy
# (RestartCount stays 0 even with restart: unless-stopped; observed
# 2026-08-22 for the tablet and again 2026-08-24 for the TM — the
# TabletKillChaosIntegrationTest documents and compensates for the same
# quirk). Fall back to docker start, then poll until the container is up.
if ! docker ps --filter "name=flink-taskmanager" --format "{{.Names}}" 2>/dev/null | grep -q "flink-taskmanager"; then
  echo "TM-KILL-CHAOS-02: [leg B] docker kill did not trigger the restart policy — explicit docker start"
  if ! docker start "${TM_CONTAINER}" 2>&1; then
    echo "TM-KILL-CHAOS-02: [leg B] FAIL — docker start" >&2
    exit 1
  fi
fi
TM_UP=0
for _ in $(seq 1 30); do
  if docker ps --filter "name=flink-taskmanager" --format "{{.Names}}" 2>/dev/null | grep -q "flink-taskmanager"; then
    TM_UP=1
    break
  fi
  sleep 2
done
if [[ "${TM_UP}" -ne 1 ]]; then
  echo "TM-KILL-CHAOS-02: [leg B] FAIL — taskmanager not restarted within 60s" >&2
  exit 1
fi
echo "TM-KILL-CHAOS-02: [leg B] taskmanager container is up again"
# The quick probe is not the full C2 acceptance run. It must still fail closed
# when Flink has no running job; the old WARN-and-continue path could report a
# PASS for a TM container with no recovered workload. Checkpoint freshness is
# asserted by tm-kill-full-load.sh, which runs the load and retains the
# checkpoint ID across the kill.
if ! command -v curl >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg B] FAIL — curl and python3 are required for the recovery probe" >&2
  exit 1
fi
if ! curl -fsS --max-time 10 "${FLINK_REST_URL}/jobs/overview" 2>/dev/null \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if any(j.get("state") == "RUNNING" for j in d.get("jobs", [])) else 1)' \
    >/dev/null 2>&1; then
  echo "TM-KILL-CHAOS-02: [leg B] FAIL — no RUNNING Flink job after kill" >&2
  exit 1
fi
if ! curl -fsS --max-time 10 "${TM_METRICS_URL}" 2>/dev/null | grep -q "compute_candles"; then
  echo "TM-KILL-CHAOS-02: [leg B] WARN — metrics not available"
fi
echo "TM-KILL-CHAOS-02: [leg B] PASS — container restart and job RUNNING (checkpoint freshness not asserted by quick probe)"
echo "TM-KILL-CHAOS-02: PASS — offline leg A PASS, leg B PASS|SKIP"
exit 0
