#!/usr/bin/env bash
#
# run-b4-halted-e2e.sh — B4.2 HALTED-path signal→intent E2E (non-market half).
#
# Runs the two live Java E2E tests that prove the B4.2 chain up to the
# fail-closed wall, WITHOUT a broker or market hours:
#
#   1. compute   B4SignalIntentE2ETest        signal→candidates→intents
#        - EXECUTION_INTENT_ENABLED=true: real SignalJob topology turns a
#          crafted rising raw series into Signal_Candidates + immutable
#          Execution_Intent LOG rows (rule-v1 breakout, lookback=2).
#        - flag absent (default): the same signal flow writes ZERO intents.
#   2. gateway   B4HaltedIntentConsumeDeferE2ETest   intent→gateway→DEFERRED
#        - a canonical Execution_Intent row reaches the real IntentReader +
#          DurableIntentDispatcher + NautilusIntentClient; with no ENABLED
#          Execution_Gate row and no durable fence token the handoff is
#          DEFERRED: zero Execution_Attempts / Order_Lifecycle rows, intent
#          stays replayable, readiness is fail-closed.
#
# Prereqs (D-era, no market):
#   * docker compose Fluss cluster up with the platform DDL applied to the
#     `default` catalog (26 tables): make ddl validates; the sanctioned apply
#     runs in the ddl-apply container (see Dockerfile README), or the cluster
#     may already carry the tables (the apply then REFUSES on the
#     empty-catalog precondition — that refusal itself proves the tables are
#     present). The tests fail with explicit messages if tables are missing.
#   * Maven offline deps in ~/.m2 (the repo gates already build these modules).
#
# Usage:
#   ./run-b4-halted-e2e.sh                 # runs both E2E tests (default)
#   FLUSS_BOOTSTRAP=host:9123 ./run-b4-halted-e2e.sh
#   ./run-b4-halted-e2e.sh --gateway-only | --compute-only
#
# P6-509: no `set -e` — each leg runs with `|| leg_status=$?` so a gateway
# failure still runs compute (triage in one invocation), and the footer only
# passes when every REQUESTED leg passed.
set -uo pipefail

CODE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJ_ROOT="$(cd "$CODE_ROOT/.." && pwd)"
export CODE_ROOT PROJ_ROOT
# P6-773: extra positional args used to be silently ignored (a typo ran the
# default instead of failing), and the usage text rejected the valid `both`.
if [ "$#" -gt 1 ]; then echo "usage: $0 [both|--gateway-only|--compute-only]" >&2; exit 2; fi
mode="${1:-both}"
case "$mode" in
  --gateway-only) run_gw=1; run_cp=0 ;;
  --compute-only) run_gw=0; run_cp=1 ;;
  both)           run_gw=1; run_cp=1 ;;
  *) echo "usage: $0 [both|--gateway-only|--compute-only]" >&2; exit 2 ;;
esac

: "${FLUSS_BOOTSTRAP:=localhost:9123}"
# P6-772: a malformed bootstrap (missing :port, empty host, non-numeric
# port) used to die deep inside the Java client after full Maven startup.
# POSIX case, not [[ =~ ]] (this script is read by /bin/sh tooling too).
case "$FLUSS_BOOTSTRAP" in *:*) port="${FLUSS_BOOTSTRAP##*:}";; *) port="";; esac
case "$port" in ''|*[!0-9]*) echo "FLUSS_BOOTSTRAP must be host:port, got '$FLUSS_BOOTSTRAP'" >&2; exit 2;; esac
case "$FLUSS_BOOTSTRAP" in :*) echo "FLUSS_BOOTSTRAP must be host:port, got '$FLUSS_BOOTSTRAP'" >&2; exit 2;; esac
export FLUSS_BOOTSTRAP
fluss_host="${FLUSS_BOOTSTRAP%:*}"; fluss_port="${FLUSS_BOOTSTRAP##*:}"

echo "== b4-halted-e2e: FLUSS_BOOTSTRAP=$FLUSS_BOOTSTRAP =="

# P6-510: cheap fail-fast on the documented DDL prereq before paying Maven
# startup (same /dev/tcp probe idiom as tiering-smoke.sh; a missing/broken
# table set still fails inside the Java tests with explicit messages, so the
# 26-table count stays advisory here — reachability is the hard gate).
if ! timeout 5 bash -c "cat < /dev/null > /dev/tcp/$fluss_host/$fluss_port" 2>/dev/null; then
  echo "fluss unreachable at $FLUSS_BOOTSTRAP (is the compose cluster up?)" >&2
  echo "hint: bring the cluster up, then confirm the 26-table DDL on the default catalog (make ddl)" >&2
  exit 1
fi

# P6-511: labels derive from what was actually requested — a single-leg run
# used to print [1/2]/[2/2]. P6-509: `|| leg_status=$?` keeps the second leg
# running after a first-leg failure (no set -e); the footer gates on both.
total=$((run_gw + run_cp)); step=0; gw_status=0; cp_status=0

if [ "$run_gw" = "1" ]; then
  step=$((step+1))
  echo "== [$step/$total] gateway: halted intent consume+defer =="
  # P6-175: failIfNoSpecifiedTests + failIfNoTests stop a renamed/missing/
  # skipped test class from false-PASSing this fail-closed gate; no -q so
  # skipped/empty runs stay visible in CI logs.
  (cd "$CODE_ROOT/02_services/06_execution_gateway" && \
   mvn -o test -Dtest=B4HaltedIntentConsumeDeferE2ETest -Dsurefire.failIfNoSpecifiedTests=true -DfailIfNoTests=true) \
    || gw_status=$?
  if [ "$gw_status" = "0" ]; then echo "   gateway HALTED E2E: PASS"; else echo "   gateway HALTED E2E: FAIL" >&2; fi
fi

if [ "$run_cp" = "1" ]; then
  step=$((step+1))
  echo "== [$step/$total] compute: signal->candidate->intent (enabled + disabled) =="
  # P6-176: same hardening as the gateway leg — both the enabled (intents
  # written) and disabled (zero intents) cases must actually run.
  (cd "$CODE_ROOT/02_services/02_compute" && \
   mvn -o test -Dtest=B4SignalIntentE2ETest -Dsurefire.failIfNoSpecifiedTests=true -DfailIfNoTests=true) \
    || cp_status=$?
  if [ "$cp_status" = "0" ]; then echo "   signal->intent E2E: PASS"; else echo "   signal->intent E2E: FAIL" >&2; fi
fi

# P6-512 (+P6-509): only the requested legs gate the footer — a single-leg
# run used to print the same ALL PASS as a full run.
if [ "$gw_status" = "0" ] && [ "$cp_status" = "0" ]; then
  if [ "$mode" = "both" ]; then
    echo "== b4-halted-e2e: ALL PASS (non-market half of B4.2; full fill leg "
    echo "   remains gated on A2.3/A2.4 sandbox config + T4 bridge wiring) =="
  else
    echo "== b4-halted-e2e: $mode PASS (single leg only; not full B4.2 coverage) =="
  fi
else
  echo "== b4-halted-e2e: FAIL (gateway=$gw_status compute=$cp_status mode=$mode) ==" >&2
  exit 1
fi
