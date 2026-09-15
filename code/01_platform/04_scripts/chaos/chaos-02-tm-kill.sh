#!/usr/bin/env bash
# chaos-02-tm-kill.sh — TaskManager kill drill.
#
# Leg A (offline, always runs): SignalJobTaskManagerKillIntegrationTest on a
#   MiniCluster — restore from the last checkpoint with no duplicate fingerprint.
# Leg B (live, optional): SIGKILL the TaskManager container and prove the SAME
#   container comes back and the SAME job is RUNNING again. Checkpoint *freshness*
#   is asserted by tm-kill-full-load.sh, which keeps the checkpoint id across the
#   kill; this quick probe only proves the containment/restore round trip.
#
# Knobs (documented seams — all overridable from the environment):
#   FLINK_REST_URL              JobManager REST base     (default http://localhost:8081)
#   TM_METRICS_URL              TaskManager metrics URL (default http://localhost:9250/metrics)
#   CHAOS_TM_CONTAINER          pin the victim container (id or name). Unset on a
#                               scaled-out stack means "lowest id", logged, never
#                               an arbitrary pick
#   CHAOS_TM_RECOVERY_TIMEOUT_S budget for restart/re-register/restore (default 120)
#   CHAOS_TM_POLL_S             poll interval                            (default 5)
#   CHAOS_TM_METRICS_REQUIRED   1 = missing TM metrics is a FAIL, 0 = warn (default 1)
#   CHAOS_LOGDIR               evidence directory (default logs/chaos/chaos-<utc>/...)
#
# Exit: 0 PASS, or leg B SKIP (stack down — the SKIP is named, never silent)
#       1 FAIL (including an unreachable docker daemon, which is NOT a SKIP)
#       2 usage/config error
#
# Fail-closed notes (P6-043/044/045/047/048/331/332/333): a leg that executed no
# tests is not a PASS; the kill happens only after a RUNNING job is recorded; the
# container that must come back is identified by id, not by a name substring; and
# the post-kill probes poll until their deadline instead of racing one shot.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
COMPUTE_DIR="${REPO_ROOT}/code/02_services/02_compute"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
TM_METRICS_URL="${TM_METRICS_URL:-http://localhost:9250/metrics}"
CHAOS_TM_CONTAINER="${CHAOS_TM_CONTAINER:-}"
CHAOS_TM_RECOVERY_TIMEOUT_S="${CHAOS_TM_RECOVERY_TIMEOUT_S:-120}"
CHAOS_TM_POLL_S="${CHAOS_TM_POLL_S:-5}"
CHAOS_TM_METRICS_REQUIRED="${CHAOS_TM_METRICS_REQUIRED:-1}"
CHAOS_LOGDIR="${CHAOS_LOGDIR:-}"
TM_KILL_TEST_CLASS="SignalJobTaskManagerKillIntegrationTest"

LOGDIR=""

info() { echo "TM-KILL-CHAOS-02: $*"; }
warn() { echo "TM-KILL-CHAOS-02: WARN — $*" >&2; }
fail() { echo "TM-KILL-CHAOS-02: FAIL — $*" >&2; }

usage_fail() {
  echo "TM-KILL-CHAOS-02: FAIL — $*" >&2
  exit 2
}

have() { command -v "$1" >/dev/null 2>&1; }

require_positive_int() {
  local name="$1" value="$2"
  if [[ ! "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" -lt 1 ]]; then
    usage_fail "${name}=${value} is not a positive integer"
  fi
}

# --------------------------------------------------------------------------
# Leg A — offline kill/restore test
# --------------------------------------------------------------------------

# A Surefire run that executed nothing (wrong -Dtest target, an Assume/skip
# filter, a profile that drops the class) still exits 0 because the run passes
# -DfailIfNoTests=false. Reporting PASS for it is a silent no-op (P6-043).
leg_a_assert_ran() {
  local reports="${COMPUTE_DIR}/target/surefire-reports" parsed files tests skipped
  if [[ ! -d "${reports}" ]]; then
    fail "[leg A] no surefire reports under ${reports} — the test did not run"
    return 1
  fi
  parsed="$(python3 - "${reports}" "${TM_KILL_TEST_CLASS}" <<'PY'
import glob
import sys
import xml.etree.ElementTree as ET

reports, klass = sys.argv[1], sys.argv[2]
files = glob.glob(f"{reports}/TEST-*{klass}*.xml")
tests = skipped = 0
for path in files:
    root = ET.parse(path).getroot()
    tests += int(root.get("tests", "0"))
    skipped += int(root.get("skipped", "0"))
print(len(files), tests, skipped)
PY
)" || {
    fail "[leg A] could not read the surefire reports"
    return 1
  }
  read -r files tests skipped <<< "${parsed}"
  if [[ "${files:-0}" -lt 1 || "${tests:-0}" -lt 1 ]]; then
    fail "[leg A] 0 tests executed (classes=${files:-0} tests=${tests:-0}) — a no-op run is not a PASS"
    return 1
  fi
  if [[ "${skipped:-0}" -ge "${tests:-0}" ]]; then
    fail "[leg A] all ${tests} test(s) skipped — the kill path was not exercised"
    return 1
  fi
  info "[leg A] executed ${tests} test(s), ${skipped} skipped, across ${files} class(es)"
  return 0
}

run_leg_a() {
  if ! have mvn; then
    fail "[leg A] mvn not found"
    return 1
  fi
  if ! mvn -f "${COMPUTE_DIR}/pom.xml" -o test-compile -q 2>&1; then
    warn "[leg A] offline compile check failed, retrying with the local repository"
    if ! mvn -f "${COMPUTE_DIR}/pom.xml" test-compile -q 2>&1; then
      fail "[leg A] compile"
      return 1
    fi
  fi
  if ! COMPUTE_INT_TEST_TM_KILL=true mvn -f "${COMPUTE_DIR}/pom.xml" \
    -Dtest="${TM_KILL_TEST_CLASS}" -DfailIfNoTests=false test 2>&1; then
    fail "[leg A] restore from checkpoint, no duplicate fingerprint"
    return 1
  fi
  leg_a_assert_ran || return 1
  info "[leg A] PASS — restore from checkpoint, no duplicate fingerprint"
  return 0
}

# --------------------------------------------------------------------------
# Leg B — live stack
# --------------------------------------------------------------------------

# A permission or connection failure must fail loudly: masking it as "stack not
# up" is how a broken drill environment reads as a green drill (P6-331).
docker_daemon_ok() {
  local err
  if err="$(docker info --format '{{.ServerVersion}}' 2>&1 >/dev/null)"; then
    return 0
  fi
  printf '%s\n' "${err}" >&2
  return 1
}

# "<id> <name>" per candidate container (P6-332).
list_tms() {
  docker ps --filter "name=flink-taskmanager" --format '{{.ID}} {{.Names}}' 2>/dev/null || true
}

# Running state of ONE container id — with several TMs a name filter matches the
# survivors and hides the killed one (P6-046).
container_running() {
  local state
  state="$(docker inspect --format '{{.State.Running}}' "$1" 2>/dev/null || true)"
  [[ "${state}" == "true" ]]
}

wait_same_container() {
  local id="$1" deadline
  deadline=$(( $(date +%s) + CHAOS_TM_RECOVERY_TIMEOUT_S ))
  while (( $(date +%s) <= deadline )); do
    container_running "${id}" && return 0
    sleep "${CHAOS_TM_POLL_S}"
  done
  return 1
}

# TaskManager re-registered with the JobManager. A running container is not a
# re-registered TM: the JVM can still be starting, or registration can have
# failed, and the job probe would then run against a cluster with no slots
# (P6-047). Prints the TM count.
wait_tm_registered() {
  local deadline body count
  deadline=$(( $(date +%s) + CHAOS_TM_RECOVERY_TIMEOUT_S ))
  while (( $(date +%s) <= deadline )); do
    body="$(curl -fsS --max-time 10 "${FLINK_REST_URL}/taskmanagers" 2>/dev/null || true)"
    count="$(python3 -c 'import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    print(0)
    raise SystemExit(0)
print(len(data.get("taskmanagers", [])))' <<< "${body}" 2>/dev/null || echo 0)"
    if [[ "${count:-0}" -ge 1 ]]; then
      printf '%s' "${count}"
      return 0
    fi
    sleep "${CHAOS_TM_POLL_S}"
  done
  return 1
}

# Job ids that are RUNNING right now, one per line. Returns 1 when the endpoint
# is unreachable — "cannot tell" is not "no job" (P6-045).
jobs_running_ids() {
  local body
  body="$(curl -fsS --max-time 10 "${FLINK_REST_URL}/jobs/overview" 2>/dev/null || true)"
  [[ -n "${body}" ]] || return 1
  printf '%s' "${body}" | python3 -c 'import json, sys
data = json.load(sys.stdin)
for job in data.get("jobs", []):
    if job.get("state") == "RUNNING":
        print(job.get("jid") or job.get("id") or "")'
}

# Poll until a job that was RUNNING before the kill is RUNNING again. A freshly
# submitted different job is not a restore (P6-044), and the JobManager is
# normally reconciling for a while after the container is back, so one shot
# fails closed on a healthy recovery (P6-048). Prints the job id.
wait_job_restored() {
  local pre_file="$1" deadline ids jid
  deadline=$(( $(date +%s) + CHAOS_TM_RECOVERY_TIMEOUT_S ))
  while (( $(date +%s) <= deadline )); do
    ids="$(jobs_running_ids || true)"
    while IFS= read -r jid; do
      [[ -n "${jid}" ]] || continue
      if grep -qxF "${jid}" "${pre_file}"; then
        printf '%s' "${jid}"
        return 0
      fi
    done <<< "${ids}"
    sleep "${CHAOS_TM_POLL_S}"
  done
  return 1
}

# The metrics endpoint is typically not scrapable while the TM is still coming
# up, so it is polled; and a TM that rejoined without the workload restored has
# no metrics, which must not reach a PASS (P6-333).
wait_metrics() {
  local deadline body
  deadline=$(( $(date +%s) + CHAOS_TM_RECOVERY_TIMEOUT_S ))
  while (( $(date +%s) <= deadline )); do
    body="$(curl -fsS --max-time 10 "${TM_METRICS_URL}" 2>/dev/null || true)"
    if [[ "${body}" == *compute_candles* ]]; then
      return 0
    fi
    sleep "${CHAOS_TM_POLL_S}"
  done
  return 1
}

run_leg_b() {
  if ! have docker; then
    info "[leg B] SKIP — docker not found"
    return 0
  fi
  # Pre-existing guard (kept verbatim): the recovery probes are curl+python3.
  if ! have curl || ! have python3; then
    fail "[leg B] curl and python3 are required for the recovery probe"
    return 1
  fi
  if ! docker_daemon_ok; then
    fail "[leg B] docker daemon unreachable or permission denied (stderr above) — that is a broken drill host, not a stack that is down"
    return 1
  fi

  local rows
  rows="$(list_tms)"
  if [[ -z "${rows}" ]]; then
    info "[leg B] SKIP — no flink-taskmanager container (stack not up)"
    return 0
  fi
  info "[leg B] taskmanager candidates:"
  printf '%s\n' "${rows}" | sed 's/^/TM-KILL-CHAOS-02:   /'

  local tm_id=""
  if [[ -n "${CHAOS_TM_CONTAINER}" ]]; then
    tm_id="$(printf '%s\n' "${rows}" \
      | awk -v want="${CHAOS_TM_CONTAINER}" '$1 == want || $2 == want { print $1; exit }')"
    if [[ -z "${tm_id}" ]]; then
      usage_fail "CHAOS_TM_CONTAINER=${CHAOS_TM_CONTAINER} is not one of the flink-taskmanager containers listed above"
    fi
    info "[leg B] victim pinned by CHAOS_TM_CONTAINER: ${tm_id}"
  else
    tm_id="$(printf '%s\n' "${rows}" | sort | awk 'NR == 1 { print $1 }')"
    if [[ -z "${tm_id}" ]]; then
      info "[leg B] SKIP — no container id"
      return 0
    fi
    info "[leg B] no CHAOS_TM_CONTAINER set — victim pinned to the lowest id (${tm_id}) out of $(printf '%s\n' "${rows}" | wc -l | tr -d ' ')"
  fi

  # Baseline BEFORE the kill (P6-045): SIGKILLing a TaskManager removes real
  # capacity, so the drill only starts when there is a job to restore.
  local pre_ids
  if ! pre_ids="$(jobs_running_ids)"; then
    fail "[leg B] ${FLINK_REST_URL}/jobs/overview unreachable — no pre-kill baseline, refusing to kill a TaskManager blind"
    return 1
  fi
  if [[ -z "${pre_ids}" ]]; then
    info "[leg B] SKIP — no RUNNING Flink job: killing a TaskManager would destroy healthy capacity with nothing to restore"
    return 0
  fi
  printf '%s\n' "${pre_ids}" > "${LOGDIR}/jobs-before.txt"
  info "[leg B] pre-kill RUNNING job(s): $(printf '%s' "${pre_ids}" | tr '\n' ' ')"

  info "[leg B] SIGKILL ${tm_id}"
  if ! docker kill -s KILL "${tm_id}" 2>&1; then
    fail "[leg B] docker kill"
    return 1
  fi
  # On this daemon a SIGKILL does not trigger the restart policy (observed
  # 2026-08-22 for the tablet and again 2026-08-24 here; TabletKillChaosIntegrationTest
  # compensates the same way), so fall back to an explicit start — then require
  # the SAME container id to be Running again.
  if ! container_running "${tm_id}"; then
    info "[leg B] docker kill did not trigger the restart policy — explicit docker start"
    if ! docker start "${tm_id}" 2>&1; then
      fail "[leg B] docker start"
      return 1
    fi
  fi
  if ! wait_same_container "${tm_id}"; then
    fail "[leg B] container ${tm_id} is not Running again within ${CHAOS_TM_RECOVERY_TIMEOUT_S}s"
    return 1
  fi
  info "[leg B] container ${tm_id} is up again"

  local reg_count
  if ! reg_count="$(wait_tm_registered)"; then
    fail "[leg B] no TaskManager registered in ${FLINK_REST_URL}/taskmanagers within ${CHAOS_TM_RECOVERY_TIMEOUT_S}s"
    return 1
  fi
  info "[leg B] ${reg_count} taskmanager(s) registered with the JobManager"

  local restored_jid
  if ! restored_jid="$(wait_job_restored "${LOGDIR}/jobs-before.txt")"; then
    fail "[leg B] none of the pre-kill job(s) is RUNNING again within ${CHAOS_TM_RECOVERY_TIMEOUT_S}s"
    return 1
  fi
  info "[leg B] job ${restored_jid} is RUNNING again (same id as before the kill)"
  printf '%s\n' "${restored_jid}" > "${LOGDIR}/job-restored.txt"
  jobs_running_ids > "${LOGDIR}/jobs-after.txt" || true

  if wait_metrics; then
    info "[leg B] ${TM_METRICS_URL} serves compute_candles"
  elif [[ "${CHAOS_TM_METRICS_REQUIRED}" == "1" ]]; then
    fail "[leg B] ${TM_METRICS_URL} served no compute_candles within ${CHAOS_TM_RECOVERY_TIMEOUT_S}s (set CHAOS_TM_METRICS_REQUIRED=0 to record this as a warning instead)"
    return 1
  else
    warn "[leg B] metrics not available (CHAOS_TM_METRICS_REQUIRED=0 — recorded, not asserted)"
  fi

  info "[leg B] PASS — same container restarted, TaskManager re-registered, job ${restored_jid} RUNNING (checkpoint freshness is asserted by tm-kill-full-load.sh)"
  return 0
}

main() {
  require_positive_int CHAOS_TM_RECOVERY_TIMEOUT_S "${CHAOS_TM_RECOVERY_TIMEOUT_S}"
  require_positive_int CHAOS_TM_POLL_S "${CHAOS_TM_POLL_S}"
  case "${CHAOS_TM_METRICS_REQUIRED}" in
    0 | 1) ;;
    *) usage_fail "CHAOS_TM_METRICS_REQUIRED=${CHAOS_TM_METRICS_REQUIRED} must be 0 or 1" ;;
  esac

  info "start"
  LOGDIR="${CHAOS_LOGDIR:-${REPO_ROOT}/logs/chaos/chaos-$(date -u +%Y%m%d-%H%M%S)/chaos-02-tm-kill}"
  mkdir -p "${LOGDIR}"

  run_leg_a || exit 1
  local leg_b="PASS"
  if ! run_leg_b; then
    exit 1
  fi
  info "PASS — offline leg A PASS, leg B ${leg_b}"
  exit 0
}

main "$@"
