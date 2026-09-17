#!/usr/bin/env bash
# CI-style module check for the no-CEP project rule (01-foundation.md): run the
# shell guard scoped to the compute module and confirm it agrees with the
# SIG-UNIT-007 JUnit dependency-scan test.
#
#   cep_guard.sh          -> shell gate on the compute module (same scope the
#                            SIG-UNIT-007 test scans)
#   CepDependencyGuardTest -> the in-JVM scan; its shell-guard-agreement and
#                            scan-scope-parity legs fail if the two ever
#                            disagree about the verdict or the file set
#
# Exit 0 only when both pass. Run from the repo root: `make cep-check-module`
# (honors MVN_FLAGS, e.g. MVN_FLAGS=-o for a warm local cache — R-143).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
GUARD="$ROOT/code/01_platform/04_scripts/cep_guard.sh"
MODULE="$ROOT/code/02_services/02_compute"
POM="$MODULE/pom.xml"

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

[ -f "$GUARD" ] || fail "cep_guard.sh not found at $GUARD"
[ -f "$POM" ] || fail "compute module pom not found at $POM"

echo "=== [1/2] shell guard on the compute module ==="
# P6-323: capture the guard output and branch on its exit code — exit 2 is an
# INFRA failure (triages differently), exit 1 is a CEP hit. The `||` shield is
# load-bearing: a bare failing `VAR=$(...)` would trip set -e before we read $?.
GUARD_RC=0
GUARD_OUT="$(bash "$GUARD" "$MODULE" 2>&1)" || GUARD_RC=$?
if [ "$GUARD_RC" -eq 2 ]; then
	echo "$GUARD_OUT" >&2
	fail "cep_guard.sh INFRA failure on the compute module (exit 2) — triage output above"
elif [ "$GUARD_RC" -ne 0 ]; then
	echo "$GUARD_OUT" >&2
	fail "cep_guard.sh found CEP references in the compute module"
fi
echo "$GUARD_OUT"

echo "=== [2/2] SIG-UNIT-007 JUnit test (CepDependencyGuardTest) ==="
# P6-324: standalone `-f $POM` is deliberate, not an oversight — R-272 keeps
# compute OUT of the root reactor, so `-pl :compute -am` cannot resolve it.
# Equivalence note: the module's only intra-repo dep is com.trading:common,
# resolved from ~/.m2; refresh it with `mvn -f code/pom.xml -pl common install`
# after touching common, or this check tests a stale common jar.
# shellcheck disable=SC2086  # MVN_FLAGS is a word-split flags variable by design
# P6-039: fail LOUD if the test class ever goes missing (no silent no-test pass).
# P6-040: `set -f` (noglob) scoped around the call — keeps the intentional
# flags-splitting, kills glob expansion of unquoted ${MVN_FLAGS:-}.
set -f
if ! mvn ${MVN_FLAGS:-} -q -f "$POM" test -Dtest=CepDependencyGuardTest -DfailIfNoTests=true -Dsurefire.failIfNoSpecifiedTests=true; then
	set +f
	fail "CepDependencyGuardTest failed (in-JVM scan / shell-guard agreement / scope parity)"
fi
set +f

echo "PASS: cep_guard.sh (compute module) + CepDependencyGuardTest agree — no CEP in the module"
