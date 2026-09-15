#!/usr/bin/env bash
# chaos-run.sh — the T13 failure-chaos runner: static self-check, then every
# drill in order, then one verdict.
#
# Contract with the children (chaos-0*.sh): exit 0 = PASS, exit 3 = SKIP
# (prerequisite absent — no coverage, not a failure), anything else = FAIL.
# Two exceptions are handled explicitly rather than silently:
#   * a drill that exits 0 but names a skipped part of itself (marker below) is
#     reported as PARTIAL — PASS for what ran, with the skipped part in the log;
#     chaos-02's leg B has no exit code of its own, its documented contract is
#     "PASS, or a named leg-B SKIP".
#   * a drill whose output could not be written to the evidence log counts as a
#     failure: a chaos suite whose evidence is missing has proven nothing.
#
# Exit: 0 all green (a partial drill is still green), 1 any FAIL / self-check /
#       evidence-write failure, 3 nothing was actually proven (every drill
#       skipped). A child's exit 2 (its own usage/config error) is a FAIL here,
#       not a skip: a misconfigured drill is not an absent prerequisite.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

# ONE inventory: name | script | in-band skip marker. The self-check, the run
# order and the "[N/total]" counters all derive from it, so adding a drill is a
# one-line edit (P6-346).
TESTS=(
  "01-slot-kill|chaos-01-slot-kill.sh|"
  "02-tm-kill|chaos-02-tm-kill.sh|TM-KILL-CHAOS-02: [leg B] SKIP"
  "03-tablet-kill|chaos-03-tablet-kill.sh|"
  "04-vm-loss|chaos-04-vm-loss.sh|"
)
TOTAL="${#TESTS[@]}"

TS="$(date -u +%Y%m%d-%H%M%S)"
LOGDIR="${REPO_ROOT}/logs/chaos/chaos-${TS}-$$"
if ! mkdir -p "${LOGDIR}"; then
  echo "chaos-run.sh: FAIL — cannot create the evidence directory ${LOGDIR}" >&2
  exit 1
fi
SUMMARY="${LOGDIR}/SUMMARY.txt"
echo "CHAOS-SUITE: start ${TS} repo ${REPO_ROOT} tests=${TOTAL}" | tee "${SUMMARY}"

SELF_FAIL=0
for entry in "${TESTS[@]}"; do
  IFS='|' read -r name file _ <<<"${entry}"
  script="${SCRIPT_DIR}/${file}"
  if ! bash -n "${script}" 2>>"${SUMMARY}"; then
    echo "SELF-CHECK FAIL: ${script} bash -n" | tee -a "${SUMMARY}"
    SELF_FAIL=1
  fi
  # stdout is captured too: shellcheck writes diagnostics there, and 2>> used to
  # drop them from the audit trail (P6-345).
  if command -v shellcheck >/dev/null 2>&1; then
    if ! shellcheck -S warning "${script}" >>"${SUMMARY}" 2>&1; then
      echo "SELF-CHECK FAIL: ${script} shellcheck" | tee -a "${SUMMARY}"
      SELF_FAIL=1
    fi
  else
    echo "SELF-CHECK WARN: shellcheck not installed — static check skipped for ${file}" | tee -a "${SUMMARY}"
  fi
done
if [[ "${SELF_FAIL}" -ne 0 ]]; then
  echo "CHAOS-SUITE: RESULT=FAIL EXIT=1 (self-check)" | tee -a "${SUMMARY}"
  exit 1
fi

PASS_COUNT=0
SKIP_COUNT=0
FAIL_COUNT=0
PARTIAL_COUNT=0
EVIDENCE_FAIL=0

run_one() {
  local idx="$1" name="$2" script="$3" marker="$4"
  local log="${LOGDIR}/${name}.log"
  echo "=== [${idx}/${TOTAL}] ${name} ===" | tee -a "${SUMMARY}"
  set +e
  bash "${script}" 2>&1 | tee "${log}"
  local -a ps=("${PIPESTATUS[@]}")
  local rc="${ps[0]}" tee_rc="${ps[1]}"
  set -e
  local label
  if [[ "${rc}" -eq 0 ]]; then
    if [[ -n "${marker}" ]] && [[ -f "${log}" ]] && grep -qF -- "${marker}" "${log}"; then
      label="PARTIAL"
      PARTIAL_COUNT=$((PARTIAL_COUNT + 1))
      echo "${name}: PARTIAL — ${marker} (what ran passed; the skipped part did not)" | tee -a "${SUMMARY}"
    else
      label="PASS"
      PASS_COUNT=$((PASS_COUNT + 1))
    fi
  elif [[ "${rc}" -eq 3 ]]; then
    label="SKIP"
    SKIP_COUNT=$((SKIP_COUNT + 1))
  else
    label="FAIL"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
  echo "RESULT [${idx}]: ${label} (exit ${rc})" | tee -a "${SUMMARY}"
  echo "${name}: ${label}" >>"${SUMMARY}"
  if [[ "${tee_rc}" -ne 0 ]]; then
    echo "EVIDENCE FAIL: ${log} is incomplete (tee exited ${tee_rc})" | tee -a "${SUMMARY}"
    EVIDENCE_FAIL=1
  fi
}

idx=0
for entry in "${TESTS[@]}"; do
  IFS='|' read -r name file marker <<<"${entry}"
  idx=$((idx + 1))
  run_one "${idx}" "${name}" "${SCRIPT_DIR}/${file}" "${marker}"
done

echo "CHAOS-SUITE: passed=${PASS_COUNT} skipped=${SKIP_COUNT} failed=${FAIL_COUNT} partial=${PARTIAL_COUNT} of ${TOTAL}" | tee -a "${SUMMARY}"
if [[ "${FAIL_COUNT}" -ne 0 || "${EVIDENCE_FAIL}" -ne 0 ]]; then
  echo "CHAOS-SUITE: RESULT=FAIL EXIT=1" | tee -a "${SUMMARY}"
  exit 1
fi
# An all-SKIP run proves nothing about resilience, so it must not read as PASS
# (P6-059): PASS_COUNT == 0 means no drill executed its assertions.
if [[ "$((PASS_COUNT + PARTIAL_COUNT))" -eq 0 ]]; then
  echo "CHAOS-SUITE: RESULT=SKIP EXIT=3 (0 passed, ${SKIP_COUNT} skipped — no coverage proven)" | tee -a "${SUMMARY}"
  exit 3
fi
echo "CHAOS-SUITE: RESULT=PASS EXIT=0" | tee -a "${SUMMARY}"
exit 0
