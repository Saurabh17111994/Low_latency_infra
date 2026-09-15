#!/usr/bin/env bash
# chaos-01-slot-kill.sh — slot-kill chaos gate (drill 1 of the chaos suite).
#
# OFFLINE, deterministic, no cluster/broker. It runs the six Go gates that
# implement the 1-of-3 slot kill and requires each of them to actually run to
# its own `--- PASS`. It injects NO live fault: no process is killed, no
# signal is sent, no broker or slot is partitioned. A green run therefore
# proves the sharding / isolation / reconnect LOGIC — the blast radius of a
# real slot loss is proven by the live drills (02–04), not by this one. The
# PASS line says exactly that, so a green drill cannot be read as a live kill.
#
# Knob: CHAOS_GO_TEST_TIMEOUT  go test -timeout per gate (default 5m; an empty
#       value counts as unset) — an explicit bound per gate, so a hung
#       reconnect test fails with Go's timeout diagnostic instead of stalling
#       the whole chaos suite.
#
# Exit contract (chaos-run.sh maps 0 = PASS, 3 = SKIP, anything else = FAIL):
#   0 PASS — every gate ran to its own --- PASS (a vet warning is advisory)
#   3 SKIP — a gate skipped itself, or go is absent; no coverage was proven
#   2 FAIL — usage/config: the bridge directory is missing, or a knob is unusable
#   1 FAIL — a gate failed, or exited 0 without running its test
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
BRIDGE_DIR="${REPO_ROOT}/code/02_services/01_ingestion/go-bridge"
PREFIX="SLOT-KILL-CHAOS-01"
CHAOS_GO_TEST_TIMEOUT="${CHAOS_GO_TEST_TIMEOUT:-5m}"

# ONE inventory, name | fail label. The run order, the per-gate messages and
# the PASS-line count all derive from it, so adding or renaming a gate is a
# one-line edit (P6-717).
GATES=(
  "TestSubscriptionPlanShards3000Tokens|sharding"
  "TestSupervisorAuthTerminalIsolatedPerSlot|terminal isolated"
  "TestINGRES001HealthySlotNotInterruptedByPeerReconnect|healthy slot not interrupted"
  "TestReconnectLoopRecoversAfterFailures|reconnect recovers"
  "TestReconnectLoopEpochAndBackoffAfterForcedDisconnect|epoch and backoff"
  "TestINGRES001OneHundredForcedDisconnectReconnectCycles|100 cycles"
)

info() { echo "${PREFIX}: $*"; }
warn() { echo "${PREFIX}: WARN — $*" >&2; }
fail() { echo "${PREFIX}: FAIL — $*" >&2; }
skip() { echo "${PREFIX}: SKIP — $*" >&2; exit 3; }
usage_fail() {
  echo "${PREFIX}: FAIL — $*" >&2
  exit 2
}

have() { command -v "$1" >/dev/null 2>&1; }

# The skip reason Go prints is the line above `--- SKIP:`. Quoting it beats
# re-deriving it: the drill must report why an invariant went unproven.
skip_reason() {
  local log="$1" name="$2" reason
  reason="$(grep -B1 -m1 -E "^--- SKIP: ${name}([[:space:]]|$)" "${log}" 2>/dev/null | head -n 1 | sed 's/^[[:space:]]*//')" || true
  printf '%s' "${reason:-no reason given}"
}

# go test exits 0 even when it ran nothing, so exit status is not a verdict:
# the anchored -run regex plus the test's own `=== RUN` and `--- PASS` lines
# are the only evidence a gate ran. A renamed or moved test used to be
# reported as PASS (P6-041); it is now a failure that says so.
run_gate() {
  local name="$1" label="$2"
  # Two statements on purpose: a single `local` does not let a later assignment
  # see an earlier one, so ${name} would expand empty (SC2318).
  local log="${WORK}/${name}.log"
  info "running ${name} (-timeout ${CHAOS_GO_TEST_TIMEOUT}, ./...)"
  if ! go -C "${BRIDGE_DIR}" test -timeout "${CHAOS_GO_TEST_TIMEOUT}" -run "^${name}\$" -count 1 -v ./... >"${log}" 2>&1; then
    command cat "${log}" >&2 || true
    fail "${label}: go test ${name} failed (full output above)"
    return 1
  fi
  if grep -qE "^--- SKIP: ${name}([[:space:]]|$)" "${log}"; then
    skip "${label}: ${name} skipped itself — $(skip_reason "${log}" "${name}")"
  fi
  if ! grep -qE "^=== RUN[[:space:]]+${name}$" "${log}" || ! grep -qE "^--- PASS: ${name}([[:space:]]|$)" "${log}"; then
    command cat "${log}" >&2 || true
    fail "${label}: ${name} exited 0 without running — renamed, moved out of ./..., or never selected by the regex; no invariant was proven"
    return 1
  fi
  info "${label}: $(grep -m1 -E "^--- PASS: ${name}" "${log}")"
  return 0
}

# vet is ADVISORY here: a warning in a package this drill never touches must
# not be reported as a failed slot kill (P6-330). Its result is named in the
# final line so it is visible rather than silently dropped; a hard static gate
# belongs in its own target, not in a resilience drill.
run_vet() {
  local log="${WORK}/vet.log"
  VET="clean"
  if go -C "${BRIDGE_DIR}" vet ./... >"${log}" 2>&1; then
    info "go vet ./... clean (advisory)"
    return 0
  fi
  VET="DIRTY (advisory, not a resilience failure)"
  warn "go vet ./... reported issues — advisory only, the drill continues:"
  command cat "${log}" >&2 || true
  return 0
}

main() {
  # Config first: a missing bridge directory is this script's own
  # misconfiguration (a moved layout or a vendored copy) and must stay loud on
  # any host, instead of surfacing as six misleading test failures (P6-327).
  if [[ ! -d "${BRIDGE_DIR}" ]]; then
    usage_fail "bridge dir not found: ${BRIDGE_DIR} — the repo layout moved, or this script was copied out of it"
  fi
  if ! have go; then
    skip "go not found"
  fi
  if ! WORK="$(mktemp -d)"; then
    fail "cannot create a temp dir for the per-gate logs"
    exit 1
  fi
  trap 'rm -rf "${WORK}"' EXIT
  info "start offline resilience gate (no live slot kill) timeout=${CHAOS_GO_TEST_TIMEOUT} bridge=${BRIDGE_DIR}"
  local entry name label gate=0
  for entry in "${GATES[@]}"; do
    IFS='|' read -r name label <<<"${entry}"
    run_gate "${name}" "${label}" || exit 1
    gate=$((gate + 1))
  done
  run_vet
  info "PASS — offline resilience gate only: all ${gate} Go gates ran to their own --- PASS (no live slot kill, no signal, no partition was performed); go vet ${VET}"
}

main "$@"
