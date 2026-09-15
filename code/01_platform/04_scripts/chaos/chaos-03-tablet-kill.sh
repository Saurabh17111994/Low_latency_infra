#!/usr/bin/env bash
# chaos-03-tablet-kill.sh — tablet SIGKILL drill (DUR-TABLETKILL-001).
#
# Hard-kills the Fluss tablet container (`docker kill -s KILL`, the same
# unclean-death path a VM loss takes, minus the host loss itself — that is
# chaos-04) and lets TabletKillChaosIntegrationTest assert the invariants: the
# table is readable again after restart and the immutable LOG row count never
# shrank. Reported, not asserted, when the table is RF1: an unclean kill loses
# the just-acked-but-unfsynced tail on a single tablet, which is the dev
# configuration; set CHAOS_REPLICATION_REQUIRED=true on prod-like RF3 for the
# no-loss contract.
#
# Knobs (all overridable from the environment):
#   FLUSS_BOOTSTRAP             coordinator bootstrap      (default fluss-coordinator:9123)
#   TABLET_CONTAINER            pin the victim (name or id). Unset on a stack
#                               with exactly one fluss-tablet means "that one";
#                               with several it is an error, never an arbitrary pick
#   TABLET_KILL_ROWS            acked rows written before the kill        (default 25)
#   CHAOS_REPLICATION_REQUIRED  true = assert no acked-row loss (RF>=MIN)   (default false)
#   CHAOS_REPLICATION_MIN       minimum replication factor when required          (default 3)
#   CHAOS_SCAN_LIMIT_ROWS       tail window for the invariant scan, -1 = full (default 200000)
#   CHAOS_COMPILE_TIMEOUT_S     bound on the one online compile retry             (default 300)
#
# Exit contract (the same 0/1/3 the sibling drills and chaos-run.sh map):
#   0 PASS — the IT ran to its sentinel and the invariants held
#   3 SKIP — environment or stack absent (no docker, no running tablet)
#   2 FAIL — usage/config error (bad knob, pinned container is not a running tablet)
#   1 FAIL — the drill ran and an invariant broke
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
COMPUTE_DIR="${REPO_ROOT}/code/02_services/02_compute"
TEST_CLASS="TabletKillChaosIntegrationTest"
PREFIX="TABLET-KILL-CHAOS-03"

FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-fluss-coordinator:9123}"
TABLET_CONTAINER="${TABLET_CONTAINER:-}"
TABLET_KILL_ROWS="${TABLET_KILL_ROWS:-25}"
CHAOS_REPLICATION_REQUIRED="${CHAOS_REPLICATION_REQUIRED:-false}"
CHAOS_REPLICATION_MIN="${CHAOS_REPLICATION_MIN:-3}"
CHAOS_SCAN_LIMIT_ROWS="${CHAOS_SCAN_LIMIT_ROWS:-200000}"
CHAOS_COMPILE_TIMEOUT_S="${CHAOS_COMPILE_TIMEOUT_S:-300}"
TABLET_PINNED=0
# NOT `[[ -n ... ]] && TABLET_PINNED=1`: a standalone && whose test is false
# returns 1, and `set -e` would kill the script in the common unpinned case.
if [[ -n "${TABLET_CONTAINER}" ]]; then
  TABLET_PINNED=1
fi

info() { echo "${PREFIX}: $*"; }
warn() { echo "${PREFIX}: WARN — $*" >&2; }
fail() { echo "${PREFIX}: FAIL — $*" >&2; }
skip() { echo "${PREFIX}: SKIP — $*" >&2; exit 3; }
usage_fail() {
  echo "${PREFIX}: FAIL — $*" >&2
  exit 2
}

have() { command -v "$1" >/dev/null 2>&1; }

require_positive_int() {
  local name="$1" value="$2"
  if [[ ! "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" -lt 1 ]]; then
    usage_fail "${name}=${value} is not a positive integer"
  fi
}

# Fail fast on knobs the Java side would otherwise only choke on mid-drill, and
# on the vacuous-run case: TABLET_KILL_ROWS=0 acks nothing, so the acked-rows
# assertion would hold without any chaos having been injected (P6-336/P6-337).
validate_knobs() {
  local lower
  require_positive_int TABLET_KILL_ROWS "${TABLET_KILL_ROWS}"
  require_positive_int CHAOS_REPLICATION_MIN "${CHAOS_REPLICATION_MIN}"
  if [[ "${CHAOS_SCAN_LIMIT_ROWS}" != "-1" ]]; then
    require_positive_int CHAOS_SCAN_LIMIT_ROWS "${CHAOS_SCAN_LIMIT_ROWS}"
    if [[ "${CHAOS_SCAN_LIMIT_ROWS}" -lt "${TABLET_KILL_ROWS}" ]]; then
      usage_fail "CHAOS_SCAN_LIMIT_ROWS=${CHAOS_SCAN_LIMIT_ROWS} < TABLET_KILL_ROWS=${TABLET_KILL_ROWS} — the tail window could not hold every acked fingerprint"
    fi
  fi
  lower="$(printf '%s' "${CHAOS_REPLICATION_REQUIRED}" | tr '[:upper:]' '[:lower:]')"
  case "${lower}" in
    true | 1) CHAOS_REPLICATION_REQUIRED="true" ;;
    false | 0) CHAOS_REPLICATION_REQUIRED="false" ;;
    *) usage_fail "CHAOS_REPLICATION_REQUIRED=${CHAOS_REPLICATION_REQUIRED} must be true or false" ;;
  esac
}

# A permission or connection failure must fail loudly: reading it as "stack not
# up" is how a broken drill host turns into a green drill (P6-331, mirrored).
docker_daemon_ok() {
  local err
  if err="$(docker info --format '{{.ServerVersion}}' 2>&1 >/dev/null)"; then
    return 0
  fi
  printf '%s\n' "${err}" >&2
  fail "docker daemon unreachable or permission denied (stderr above) — that is a broken drill host, not a stack that is down"
  return 1
}

# Candidate tablets, sorted: with several replicas an auto-pick is not
# reproducible, so the runner demands a pin instead (P6-334/P6-338).
list_tablets() {
  docker ps --filter "name=fluss-tablet" --format '{{.Names}}' 2>/dev/null | sort || true
}

# A pinned override that does not resolve is a config error (2); an
# auto-discovered tablet that is gone or stopped means the stack is down (3)
# (P6-050/P6-051).
reject_tablet() {
  if [[ "${TABLET_PINNED}" == "1" ]]; then
    usage_fail "TABLET_CONTAINER: $*"
  fi
  skip "$* — start the stack, or pin a container that is up"
}

resolve_tablet() {
  local -a candidates=()
  if [[ "${TABLET_PINNED}" == "1" ]]; then
    info "victim pinned by TABLET_CONTAINER: ${TABLET_CONTAINER}"
    return 0
  fi
  mapfile -t candidates < <(list_tablets)
  info "fluss-tablet candidates: ${candidates[*]:-<none>}"
  case "${#candidates[@]}" in
    0) skip "no fluss-tablet container (start the stack; TABLET_CONTAINER overrides)" ;;
    1) TABLET_CONTAINER="${candidates[0]}" ;;
    *) usage_fail "${#candidates[@]} fluss-tablet containers are running — set TABLET_CONTAINER to the one to kill" ;;
  esac
}

# Exact-field check, not a substring grep: the value is passed to `docker kill`
# inside the IT, so a typo (or a regex) must not reach it (P6-050/P6-051).
verify_tablet() {
  local line name running
  line="$(docker inspect --format '{{.Name}} {{.State.Running}}' -- "${TABLET_CONTAINER}" 2>/dev/null || true)"
  if [[ -z "${line}" ]]; then
    reject_tablet "container '${TABLET_CONTAINER}' does not exist"
  fi
  name="${line%% *}"
  running="${line##* }"
  if [[ "${name}" != *fluss-tablet* ]]; then
    reject_tablet "container '${name}' is not a fluss-tablet container"
  fi
  if [[ "${running}" != "true" ]]; then
    reject_tablet "tablet '${name}' is not running (State.Running=${running})"
  fi
  TABLET_CONTAINER="${name#/}"
  info "tablet under test: ${TABLET_CONTAINER}"
}

# Offline test-compile probe. The online retry is announced and bounded so a
# missing local artifact fails fast instead of hanging the chaos run (P6-335/P6-339).
compile_tests() {
  if ! have mvn; then
    fail "mvn not found"
    return 1
  fi
  if mvn -f "${COMPUTE_DIR}/pom.xml" -o test-compile -q 2>&1; then
    return 0
  fi
  warn "offline test-compile failed (the local repository is incomplete) — retrying online once, bounded by CHAOS_COMPILE_TIMEOUT_S=${CHAOS_COMPILE_TIMEOUT_S}s"
  if have timeout; then
    if ! timeout "${CHAOS_COMPILE_TIMEOUT_S}" mvn -f "${COMPUTE_DIR}/pom.xml" test-compile; then
      fail "compile ${TEST_CLASS}"
      return 1
    fi
    return 0
  fi
  warn "timeout not found — the online retry is unbounded"
  if ! mvn -f "${COMPUTE_DIR}/pom.xml" test-compile; then
    fail "compile ${TEST_CLASS}"
    return 1
  fi
}

# Maven exiting 0 is not a PASS: the IT is env-gated and assumes its stack, so
# it can be skipped while the build stays green. The drill's own sentinel is the
# only proof it ran, and an unmet assumption is a named SKIP rather than a
# silent green (P6-049/P6-052).
run_live_test() {
  local mvn_out="${WORK}/mvn-${TEST_CLASS}.log" reason
  local -a ps=()
  local mvn_rc=0 tee_rc=0
  if ! COMPUTE_INT_TEST_TABLET_KILL=true \
    FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP}" \
    TABLET_CONTAINER="${TABLET_CONTAINER}" \
    TABLET_KILL_ROWS="${TABLET_KILL_ROWS}" \
    CHAOS_REPLICATION_REQUIRED="${CHAOS_REPLICATION_REQUIRED}" \
    CHAOS_REPLICATION_MIN="${CHAOS_REPLICATION_MIN}" \
    CHAOS_SCAN_LIMIT_ROWS="${CHAOS_SCAN_LIMIT_ROWS}" \
    mvn -f "${COMPUTE_DIR}/pom.xml" -Dtest="${TEST_CLASS}" -DfailIfNoTests=false test 2>&1 | tee "${mvn_out}"; then
    # PIPESTATUS must be captured as an array in one expansion: a second `local`
    # assignment on its own line has already overwritten it.
    ps=("${PIPESTATUS[@]}")
    mvn_rc="${ps[0]}"
    tee_rc="${ps[1]}"
    if [[ "${tee_rc}" -ne 0 ]]; then
      fail "the test log ${mvn_out} could not be written (tee exited ${tee_rc}) — no evidence, no verdict"
      return 1
    fi
    fail "${TEST_CLASS} exited ${mvn_rc}"
    return 1
  fi
  if grep -qF "${PREFIX}: RESULT=PASS" "${mvn_out}"; then
    return 0
  fi
  # Only these two mean "maven chose not to run the drill". `Tests run: 0` with
  # nothing skipped is a different story — no test was even selected — and is
  # left to the FAIL below instead of being excused as a skip.
  reason="$(grep -m1 -E 'Assumption failed|Skipped: [1-9]' "${mvn_out}" | tr -s ' ' || true)"
  if [[ -n "${reason}" ]]; then
    skip "the IT did not run: ${reason} (raise FLUSS_BOOTSTRAP, start the stack, or pin the right tablet)"
  fi
  fail "${TEST_CLASS} exited 0 but produced neither its RESULT=PASS sentinel nor a skip — no invariant was proven (P6-049)"
  return 1
}

main() {
  # Knobs first (a config error must stay loud on any host), then the docker
  # precondition BEFORE mktemp: with docker absent this script must reach its
  # named SKIP even on a host where PATH is broken enough to hide mktemp.
  validate_knobs
  info "start TABLET_KILL_ROWS=${TABLET_KILL_ROWS} replication_required=${CHAOS_REPLICATION_REQUIRED} scan_limit=${CHAOS_SCAN_LIMIT_ROWS}"
  if ! have docker; then
    skip "docker not found"
  fi
  WORK="$(mktemp -d)"
  trap 'rm -rf "${WORK}"' EXIT
  docker_daemon_ok || exit 1
  resolve_tablet
  verify_tablet
  compile_tests || exit 1
  info "running ${TEST_CLASS} FLUSS_BOOTSTRAP=${FLUSS_BOOTSTRAP} CHAOS_SCAN_LIMIT_ROWS=${CHAOS_SCAN_LIMIT_ROWS}"
  run_live_test || exit 1
  info "PASS — acked rows readable after the SIGKILL and the LOG row count never shrank (RF1 dev tail loss of the just-acked rows is reported, not failed)"
}

main "$@"
