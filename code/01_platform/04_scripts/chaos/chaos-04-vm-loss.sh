#!/usr/bin/env bash
# chaos-04-vm-loss.sh — VM-loss drill: take one Swarm worker out of service and
# prove (a) the order path halts inside its SLO and (b) the workload is
# rescheduled onto a surviving node inside its SLO.
#
# Modes:
#   drain     node availability -> drain, restored to active afterwards
#   poweroff  run CHAOS_VM_OFF_CMD (argv, no shell) and CHAOS_VM_ON_CMD to rejoin
#
# Knobs (documented seams — every one is overridable from the environment):
#   CHAOS_WORKLOAD_NODE      target worker: node ID or hostname (default: first
#                            Active+Ready worker; a manager is never eligible)
#   CHAOS_SERVICE            replicated service with a task on that worker
#                            (default: first eligible service)
#   CHAOS_VM_OFF_MODE        drain | poweroff                        (default drain)
#   CHAOS_VM_OFF_CMD         argv string run for poweroff — executed WITHOUT a
#                            shell, so no quoting/expansion/injection
#   CHAOS_VM_ON_CMD          argv string that rejoins the node — REQUIRED in
#                            poweroff mode (a drill that cannot undo itself is
#                            not run)
#   CHAOS_ORDER_PROBE_TCP    host:port of the order front door (optional; when
#                            unset the halt leg is not probed — never claimed)
#   CHAOS_NODE_READY_MIN     Ready+Active nodes required to attempt   (default 2)
#   CHAOS_ORDER_HALT_SLO_S   order-halt budget in seconds             (default 5)
#   CHAOS_RECOVERY_SLO_S     reschedule budget in seconds            (default 30)
#   CHAOS_NODE_READY_TIMEOUT_S  node-rejoin budget (poweroff)       (default 120)
#   CHAOS_VM_CMD_TIMEOUT_S   per-command timeout                     (default 60)
#   CHAOS_LOGDIR             evidence directory (default logs/chaos/chaos-<utc>/...)
#
# Exit: 0 drill ran, both verdicts passed
#       1 a verdict failed (order still up, no reschedule, node not restored)
#       2 usage/config error (bad knob, unknown mode, explicit target refused)
#       3 SKIP — precondition absent (no docker/timeout, not a swarm, too few
#         Ready nodes, no eligible worker or service)
#
# Safety invariants (P6-002): the drill never targets a manager — node discovery
# requires an empty ManagerStatus AND `node inspect .Spec.Role == worker`, and an
# explicit CHAOS_WORKLOAD_NODE that names a manager is refused, not honoured.
# The drain/the power-off is a controlled cluster perturbation; draining the
# leader would take the control plane down, which is not what this drill tests.
#
# Restore invariant (P6-343): the EXIT trap is armed BEFORE the node is touched
# and the restore is verified with `node inspect .Spec.Availability == active`.
# A node that does not come back is a FAIL with a non-zero exit, never a PASS.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

CHAOS_WORKLOAD_NODE="${CHAOS_WORKLOAD_NODE:-}"
CHAOS_SERVICE="${CHAOS_SERVICE:-}"
CHAOS_VM_OFF_MODE="${CHAOS_VM_OFF_MODE:-drain}"
CHAOS_VM_OFF_CMD="${CHAOS_VM_OFF_CMD:-}"
CHAOS_VM_ON_CMD="${CHAOS_VM_ON_CMD:-}"
CHAOS_ORDER_PROBE_TCP="${CHAOS_ORDER_PROBE_TCP:-}"
CHAOS_NODE_READY_MIN="${CHAOS_NODE_READY_MIN:-2}"
CHAOS_ORDER_HALT_SLO_S="${CHAOS_ORDER_HALT_SLO_S:-5}"
CHAOS_RECOVERY_SLO_S="${CHAOS_RECOVERY_SLO_S:-30}"
CHAOS_NODE_READY_TIMEOUT_S="${CHAOS_NODE_READY_TIMEOUT_S:-120}"
CHAOS_VM_CMD_TIMEOUT_S="${CHAOS_VM_CMD_TIMEOUT_S:-60}"
CHAOS_LOGDIR="${CHAOS_LOGDIR:-}"

LOGDIR=""
TARGET_NODE_ID=""
TARGET_NODE_HOST=""
RESTORE_NEEDED=false
RESTORE_FAILED=0
ON_ARGV=()

info() { echo "VM-LOSS-CHAOS-04: $*"; }
warn() { echo "VM-LOSS-CHAOS-04: WARN — $*" >&2; }
fail() { echo "VM-LOSS-CHAOS-04: FAIL — $*" >&2; }

# Preconditions that mean "this host cannot run the drill" are SKIPs (exit 3),
# never a silent pass: the verdict line names what was not verified.
skip() {
  echo "VM-LOSS-CHAOS-04: SKIP — $*" >&2
  exit 3
}

# A config error is not a skip: the operator asked for something impossible.
usage_fail() {
  echo "VM-LOSS-CHAOS-04: FAIL — $*" >&2
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
# Cluster facts
# --------------------------------------------------------------------------

# Locale-independent swarm check (P6-340). The old `docker info | grep "Swarm:
# active"` depended on docker's human-readable output.
swarm_active() {
  local state
  state="$(docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null || true)"
  [[ "${state}" == "active" ]]
}

# Nodes that are both Active and Ready — the only ones that can receive the
# rescheduled workload (P6-340, P6-341).
count_ready_active() {
  docker node ls --format '{{.Availability}} {{.Status}}' 2>/dev/null \
    | awk '$1 == "Active" && $2 == "Ready"' | wc -l | tr -d ' ' || true
}

# <id> <hostname> <availability> <status> <managerstatus>, one node per line.
node_rows() {
  docker node ls --format '{{.ID}} {{.Hostname}} {{.Availability}} {{.Status}} {{.ManagerStatus}}' 2>/dev/null || true
}

# First Active+Ready worker (P6-002). Managers are excluded twice over: an empty
# ManagerStatus and an explicit role check.
select_worker() {
  local id host avail status mgr role
  while read -r id host avail status mgr; do
    [[ -n "${id}" && -n "${host}" ]] || continue
    [[ "${avail}" == "Active" && "${status}" == "Ready" ]] || continue
    [[ -z "${mgr}" ]] || continue
    role="$(docker node inspect --format '{{.Spec.Role}}' "${id}" 2>/dev/null || true)"
    [[ "${role}" == "worker" ]] || continue
    printf '%s %s\n' "${id}" "${host}"
    return 0
  done < <(node_rows)
  return 1
}

# Explicit CHAOS_WORKLOAD_NODE, validated (P6-342): must exist, be Active+Ready,
# and be a worker. A refused target is a config error, not a skip.
resolve_node() {
  local want="$1" id host avail status mgr role
  while read -r id host avail status mgr; do
    [[ "${id}" == "${want}" || "${host}" == "${want}" ]] || continue
    if [[ "${avail}" != "Active" || "${status}" != "Ready" ]]; then
      usage_fail "CHAOS_WORKLOAD_NODE=${want} is ${avail}/${status}, not Active/Ready"
    fi
    if [[ -n "${mgr}" ]]; then
      usage_fail "CHAOS_WORKLOAD_NODE=${want} is a swarm manager (${mgr}) — refusing to take the control plane down"
    fi
    role="$(docker node inspect --format '{{.Spec.Role}}' "${id}" 2>/dev/null || true)"
    if [[ "${role}" != "worker" ]]; then
      usage_fail "CHAOS_WORKLOAD_NODE=${want} has role '${role:-unknown}' — refusing to target a non-worker"
    fi
    printf '%s %s\n' "${id}" "${host}"
    return 0
  done < <(node_rows)
  usage_fail "CHAOS_WORKLOAD_NODE=${want} is not a node in this swarm"
}

mode_is_replicated() {
  local mode
  mode="$(docker service inspect --format '{{if .Spec.Mode.Replicated}}replicated{{else}}global{{end}}' "$1" 2>/dev/null || true)"
  [[ "${mode}" == "replicated" ]]
}

service_replicas() {
  # "<current>/<desired>" as reported by service ls, or empty.
  docker service ls --format '{{.Name}} {{.Replicas}}' 2>/dev/null \
    | awk -v s="$1" '$1 == s {print $2}' || true
}

desired_replicas() {
  docker service inspect --format '{{.Spec.Mode.Replicated.Replicas}}' "$1" 2>/dev/null || true
}

service_running_on_node() {
  local svc="$1" host="$2" n
  n="$(docker service ps "${svc}" --format '{{.Node}} {{.CurrentState}}' 2>/dev/null \
    | awk -v h="${host}" '$1 == h && $2 == "Running" {n++} END {print n + 0}' || true)"
  [[ "${n:-0}" -ge 1 ]]
}

# First replicated service with a Running task on the target worker (P6-053).
# Assumes a bare name is unambiguous; docker service ls prints the name only.
select_service_on_node() {
  local host="$1" name
  while read -r name; do
    [[ -n "${name}" ]] || continue
    mode_is_replicated "${name}" || continue
    if service_running_on_node "${name}" "${host}"; then
      printf '%s\n' "${name}"
      return 0
    fi
  done < <(docker service ls --format '{{.Name}}' 2>/dev/null || true)
  return 1
}

# "<task id> <node hostname> <current state>" for every task of the service.
task_rows() {
  docker service ps "$1" --format '{{.ID}} {{.Node}} {{.CurrentState}}' 2>/dev/null || true
}

# A task that is Running on a node other than the victim and whose ID is not in
# the pre-drain baseline (P6-057). Swarm never reuses a task ID, so a new ID on a
# survivor is exactly the reschedule this drill claims — "some task is Running"
# would also be true while the old task is still winding down on the drained
# node.
find_rescheduled_task() {
  local svc="$1" victim_host="$2" before_file="$3" id node state
  while read -r id node state; do
    [[ -n "${id}" && -n "${node}" ]] || continue
    [[ "${node}" == "${victim_host}" ]] && continue
    [[ "${state}" == Running* ]] || continue
    if [[ -f "${before_file}" ]] && grep -q "^${id} " "${before_file}"; then
      continue
    fi
    printf '%s %s\n' "${id}" "${node}"
    return 0
  done < <(task_rows "${svc}")
  return 1
}

# --------------------------------------------------------------------------
# Probes
# --------------------------------------------------------------------------

# Order front door: 0 halted inside the SLO, 1 still accepting, 2 malformed
# CHAOS_ORDER_PROBE_TCP, 3 cannot bound the probe (no GNU timeout).
# The old loop broke on "halted" and otherwise fell through to PASS after
# ~10s; it also injected the address into `bash -c`. Here the address is
# validated, passed as argv, and a stuck port is a FAIL (P6-056). The budget is
# checked between probes, so a probe may overrun it by up to its own 0.5s
# timeout — the SLO is a budget for the port to stop answering, not a promise
# that the loop returns exactly on the deadline.
probe_order_halt() {
  local host port deadline
  host="${CHAOS_ORDER_PROBE_TCP%:*}"
  port="${CHAOS_ORDER_PROBE_TCP#*:}"
  [[ "${CHAOS_ORDER_PROBE_TCP}" == *:* ]] || return 2
  [[ "${host}" =~ ^[A-Za-z0-9._-]+$ ]] || return 2
  [[ "${port}" =~ ^[0-9]{1,5}$ ]] || return 2
  have timeout || return 3
  deadline=$(( $(date +%s) + CHAOS_ORDER_HALT_SLO_S ))
  while (( $(date +%s) <= deadline )); do
    # `exec 3<>` fails when the port is closed; the address travels as argv so
    # no operator-supplied text is parsed as shell syntax.
    if ! timeout 0.5 bash -c 'exec 3<>/dev/tcp/"$1"/"$2"' _ "${host}" "${port}" 2>/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

# Prints the runner of an argv command; the command string is split on
# whitespace and executed WITHOUT a shell (P6-055): an injected `;`, `$(...)` or
# `|` stays a literal argument instead of becoming code. A caller that needs a
# shell pipeline must wrap it in a script of its own.
run_argv() {
  local label="$1"
  shift
  if (($# == 0)); then
    fail "${label}: empty command"
    return 1
  fi
  timeout "${CHAOS_VM_CMD_TIMEOUT_S}" "$@"
}

# --------------------------------------------------------------------------
# Restore (armed before the node is touched)
# --------------------------------------------------------------------------

wait_node_ready() {
  local id="$1" deadline state
  deadline=$(( $(date +%s) + CHAOS_NODE_READY_TIMEOUT_S ))
  while (( $(date +%s) <= deadline )); do
    state="$(docker node inspect --format '{{.Availability}} {{.Status}}' "${id}" 2>/dev/null || true)"
    if [[ "${state}" == "active ready" ]]; then
      return 0
    fi
    sleep 2
  done
  return 1
}

# Idempotent: RESTORE_NEEDED is cleared first, so an explicit call in the happy
# path and a later EXIT trap cannot restore twice.
restore_node() {
  [[ "${RESTORE_NEEDED}" == "true" ]] || return 0
  RESTORE_NEEDED=false
  if [[ "${CHAOS_VM_OFF_MODE}" == "drain" ]]; then
    info "restoring node ${TARGET_NODE_ID} to availability=active"
    docker node update --availability active "${TARGET_NODE_ID}" >/dev/null 2>&1 || true
  else
    info "rejoining node ${TARGET_NODE_ID} via CHAOS_VM_ON_CMD"
    run_argv "rejoin" "${ON_ARGV[@]}" >/dev/null 2>&1 || true
  fi
  if wait_node_ready "${TARGET_NODE_ID}"; then
    info "node ${TARGET_NODE_ID} is Active+Ready again"
    return 0
  fi
  fail "node ${TARGET_NODE_ID} did NOT return to Active+Ready after restore — it may still be drained or powered off"
  RESTORE_FAILED=1
  return 1
}

on_exit() {
  local rc=$?
  if [[ "${RESTORE_NEEDED}" == "true" ]]; then
    restore_node || RESTORE_FAILED=1
  fi
  if [[ "${RESTORE_FAILED}" -eq 1 && "${rc}" -eq 0 ]]; then
    exit 1
  fi
  exit "${rc}"
}

# --------------------------------------------------------------------------
# Drill
# --------------------------------------------------------------------------

main() {
  require_positive_int CHAOS_NODE_READY_MIN "${CHAOS_NODE_READY_MIN}"
  require_positive_int CHAOS_ORDER_HALT_SLO_S "${CHAOS_ORDER_HALT_SLO_S}"
  require_positive_int CHAOS_RECOVERY_SLO_S "${CHAOS_RECOVERY_SLO_S}"
  require_positive_int CHAOS_NODE_READY_TIMEOUT_S "${CHAOS_NODE_READY_TIMEOUT_S}"
  require_positive_int CHAOS_VM_CMD_TIMEOUT_S "${CHAOS_VM_CMD_TIMEOUT_S}"
  case "${CHAOS_VM_OFF_MODE}" in
    drain | poweroff) ;;
    *) usage_fail "unknown CHAOS_VM_OFF_MODE=${CHAOS_VM_OFF_MODE} (drain|poweroff)" ;;
  esac

  info "start mode=${CHAOS_VM_OFF_MODE} node=${CHAOS_WORKLOAD_NODE:-auto} service=${CHAOS_SERVICE:-auto}"

  have docker || skip "docker not found"
  if ! have timeout; then
    skip "GNU timeout not found — the drain/power-off commands could not be bounded"
  fi
  if [[ "${CHAOS_VM_OFF_MODE}" == "poweroff" ]]; then
    [[ -n "${CHAOS_VM_OFF_CMD}" ]] || usage_fail "poweroff mode needs CHAOS_VM_OFF_CMD (argv string, e.g. 'ssh host -- poweroff')"
    if [[ -z "${CHAOS_VM_ON_CMD}" ]]; then
      usage_fail "poweroff mode needs CHAOS_VM_ON_CMD — a drill that cannot rejoin the node is not run (P6-054)"
    fi
    read -r -a ON_ARGV <<< "${CHAOS_VM_ON_CMD}"
    ((${#ON_ARGV[@]} > 0)) || usage_fail "CHAOS_VM_ON_CMD is empty after splitting"
  fi

  swarm_active || skip "this docker daemon is not in an active swarm (local compose has no swarm; single-node cannot lose)"
  local ready
  ready="$(count_ready_active)"
  if [[ "${ready:-0}" -lt "${CHAOS_NODE_READY_MIN}" ]]; then
    skip "only ${ready:-0} Active+Ready node(s) — a loss drill needs >= ${CHAOS_NODE_READY_MIN} (Drain/Down nodes cannot take over)"
  fi

  # --- target node ---------------------------------------------------------
  local node_line
  if [[ -n "${CHAOS_WORKLOAD_NODE}" ]]; then
    # resolve_node() reports a refused target with usage_fail(); inside a command
    # substitution that exit only reaches us as a status, so it is re-raised here.
    node_line="$(resolve_node "${CHAOS_WORKLOAD_NODE}")" || exit 2
  else
    node_line="$(select_worker)" || skip "no Active+Ready worker found (CHAOS_WORKLOAD_NODE overrides)"
  fi
  TARGET_NODE_ID="${node_line%% *}"
  TARGET_NODE_HOST="${node_line##* }"
  [[ -n "${TARGET_NODE_ID}" && -n "${TARGET_NODE_HOST}" ]] || skip "could not resolve a workload node"

  # --- target service ------------------------------------------------------
  local svc
  if [[ -n "${CHAOS_SERVICE}" ]]; then
    docker service inspect "${CHAOS_SERVICE}" >/dev/null 2>&1 \
      || usage_fail "CHAOS_SERVICE=${CHAOS_SERVICE} does not exist in this swarm"
    mode_is_replicated "${CHAOS_SERVICE}" \
      || usage_fail "CHAOS_SERVICE=${CHAOS_SERVICE} is not replicated — a global service cannot be rescheduled by a node drain"
    service_running_on_node "${CHAOS_SERVICE}" "${TARGET_NODE_HOST}" \
      || usage_fail "CHAOS_SERVICE=${CHAOS_SERVICE} has no Running task on node ${TARGET_NODE_HOST} — draining that node would prove nothing"
    svc="${CHAOS_SERVICE}"
  else
    svc="$(select_service_on_node "${TARGET_NODE_HOST}")" \
      || skip "no replicated service with a Running task on ${TARGET_NODE_HOST} (CHAOS_SERVICE overrides)"
  fi

  # --- evidence ------------------------------------------------------------
  LOGDIR="${CHAOS_LOGDIR:-${REPO_ROOT}/logs/chaos/chaos-$(date -u +%Y%m%d-%H%M%S)/chaos-04-vm-loss}"
  mkdir -p "${LOGDIR}"
  docker service ps "${svc}" 2>/dev/null | tee "${LOGDIR}/service-ps-before.txt" || true
  docker node ls 2>/dev/null | tee "${LOGDIR}/node-ls-before.txt" || true
  task_rows "${svc}" > "${LOGDIR}/task-baseline.txt"
  info "target node=${TARGET_NODE_ID} (${TARGET_NODE_HOST}) service=${svc} logs=${LOGDIR}"

  # --- arm the restore BEFORE the perturbation (P6-343) --------------------
  RESTORE_NEEDED=true
  trap on_exit EXIT

  if [[ "${CHAOS_VM_OFF_MODE}" == "drain" ]]; then
    info "draining node ${TARGET_NODE_ID} (${TARGET_NODE_HOST})"
    if ! docker node update --availability drain "${TARGET_NODE_ID}" 2>&1 | tee "${LOGDIR}/drain.log"; then
      fail "drain failed — node ${TARGET_NODE_ID} untouched by us"
      exit 1
    fi
  else
    local -a OFF_ARGV=()
    read -r -a OFF_ARGV <<< "${CHAOS_VM_OFF_CMD}"
    info "powering off node ${TARGET_NODE_ID} via CHAOS_VM_OFF_CMD (argv, no shell)"
    if ! run_argv "poweroff" "${OFF_ARGV[@]}" 2>&1 | tee "${LOGDIR}/poweroff.log"; then
      fail "CHAOS_VM_OFF_CMD failed"
      exit 1
    fi
  fi

  # --- order halt <SLO -----------------------------------------------------
  if [[ -n "${CHAOS_ORDER_PROBE_TCP}" ]]; then
    info "probing order halt within ${CHAOS_ORDER_HALT_SLO_S}s at ${CHAOS_ORDER_PROBE_TCP}"
    local probe_rc=0
    probe_order_halt || probe_rc=$?
    case "${probe_rc}" in
      0) info "order probe: connections refused within ${CHAOS_ORDER_HALT_SLO_S}s" ;;
      1) fail "order path still accepting connections at ${CHAOS_ORDER_PROBE_TCP} after ${CHAOS_ORDER_HALT_SLO_S}s — not halted"; exit 1 ;;
      2) usage_fail "CHAOS_ORDER_PROBE_TCP=${CHAOS_ORDER_PROBE_TCP} is not host:port" ;;
      3) fail "cannot bound the order probe: GNU timeout missing"; exit 1 ;;
      *) fail "order probe failed (rc=${probe_rc})"; exit 1 ;;
    esac
    echo "probe_rc=${probe_rc} slo_s=${CHAOS_ORDER_HALT_SLO_S}" > "${LOGDIR}/order-halt.txt"
  else
    warn "CHAOS_ORDER_PROBE_TCP unset — the order-halt leg was NOT probed and is not claimed"
    echo "not_probed=CHAOS_ORDER_PROBE_TCP unset" > "${LOGDIR}/order-halt.txt"
  fi

  # --- recovery <SLO: a NEW task Running on a surviving node (P6-057) ------
  info "waiting for ${svc} to be rescheduled off ${TARGET_NODE_HOST} within ${CHAOS_RECOVERY_SLO_S}s"
  local deadline=$(( $(date +%s) + CHAOS_RECOVERY_SLO_S ))
  local rescheduled="" replicas="" desired=""
  desired="$(desired_replicas "${svc}")"
  while (( $(date +%s) <= deadline )); do
    rescheduled="$(find_rescheduled_task "${svc}" "${TARGET_NODE_HOST}" "${LOGDIR}/task-baseline.txt")" || rescheduled=""
    replicas="$(service_replicas "${svc}")" || replicas=""
    if [[ -n "${rescheduled}" && "${replicas}" == "${desired}/${desired}" ]]; then
      break
    fi
    sleep 1
  done
  task_rows "${svc}" > "${LOGDIR}/task-after.txt"
  docker service ps "${svc}" 2>/dev/null | tee "${LOGDIR}/service-ps-after.txt" || true
  docker node ls 2>/dev/null | tee "${LOGDIR}/node-ls-after.txt" || true
  if [[ -z "${rescheduled}" ]]; then
    fail "service ${svc} has no NEW task Running on a surviving node within ${CHAOS_RECOVERY_SLO_S}s (a task that was already Running elsewhere is not a reschedule)"
    exit 1
  fi
  if [[ "${replicas}" != "${desired}/${desired}" ]]; then
    fail "service ${svc} reports ${replicas:-?} replicas, expected ${desired}/${desired}"
    exit 1
  fi
  echo "rescheduled_task=${rescheduled} replicas=${replicas} slo_s=${CHAOS_RECOVERY_SLO_S}" > "${LOGDIR}/recovery.txt"

  # --- restore (explicit, so the verdict order reads top-down) -------------
  info "recovery: new task ${rescheduled} Running on a surviving node, replicas ${replicas}"
  trap - EXIT
  if ! restore_node; then
    fail "restore failed — see node-ls-after.txt"
    trap - EXIT
    exit 1
  fi
  docker node ls 2>/dev/null | tee "${LOGDIR}/node-ls-restored.txt" || true
  info "PASS — VM loss: halt <${CHAOS_ORDER_HALT_SLO_S}s (if probed), recovery <${CHAOS_RECOVERY_SLO_S}s on a surviving node, node restored"
  exit 0
}

main "$@"
