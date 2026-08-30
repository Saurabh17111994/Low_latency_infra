#!/usr/bin/env bash
# =============================================================================
# test-pipeline-lib.sh — mechanical guards for pipeline-lib.sh's submit command.
#
# WHY THIS EXISTS (2026-08-30): an inline '#' comment inserted between the
# backslash-continued lines of the SignalJob submit command silently broke the
# whole submission ("requires at least 2 arg(s)" / "-e: command not found").
# bash -n does NOT catch this — every line is individually valid shell. The
# existing runtime guard (JobID-extraction assert) caught it in 20s, but only
# at run time. These tests catch it BEFORE any pipeline run, mechanically.
#
# Run standalone:  bash test-pipeline-lib.sh
# Exits 0 only if every guard passes.
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="$SCRIPT_DIR/pipeline-lib.sh"

pass=0; fail=0
ok()   { echo "PASS: $*"; pass=$((pass+1)); }
bad()  { echo "FAIL: $*" >&2; fail=$((fail+1)); }

# ---- G1: the lib parses ----
bash -n "$LIB" && ok "G1 lib syntax valid" || { bad "G1 lib syntax invalid"; exit 1; }

# ---- G2: sourcing defines every public function the harness scripts call ----
# (a sourcing failure or a renamed function would break callers at run time)
ROOT="$SCRIPT_DIR/../../.." ; ROOT="$(cd "$ROOT" && pwd)"
OUT="$(mktemp -d)"
FAKETOOL_PORT=8899 RATE_HZ=10
# shellcheck source=pipeline-lib.sh
source "$LIB"
for fn in pipeline_preflight pipeline_start_faketool pipeline_start_ingestion \
          pipeline_submit_job pipeline_cleanup pipeline_install_cleanup_trap \
          pipeline_validate_rate pipeline_port_free flink_metric_dump flink_wait_state; do
    declare -f "$fn" >/dev/null 2>&1 && ok "G2 function $fn defined" \
        || bad "G2 function $fn MISSING from $LIB"
done

# ---- G3: the submit command is ONE complete command containing the
# required env vars and the jar — the exact failure class of 2026-08-30.
# A '#' comment inside the continuation splits it into fragments; grep'ing
# for the FINAL token (compute.jar) on the SAME logical line as the first
# (-e ALLOW_FULL_REPLAY) proves the continuation chain is intact.
submit_block="$(sed -n '/submit_out="\$(\$COMPOSE exec -T \\/,/"\$SUBMIT/p;/submit_out="\$(\$COMPOSE exec -T \\/,/compute.jar/p' "$LIB" 2>/dev/null)"
# Simpler + robust: extract the function body and join continuations.
body="$(declare -f pipeline_submit_job | sed 's/\\$//' | tr -d '\n')"
for needle in '-e ALLOW_FULL_REPLAY' '-e WATERMARK_OUT_OF_ORDER_MS' \
              '-e CHECKPOINT_TIMEOUT_MS' '-e PREVIEW_ENABLED=true' \
              'flink run -d' 'compute.jar'; do
    case "$body" in
        *"$needle"*) ok "G3 submit contains: $needle" ;;
        *)           bad "G3 submit command MISSING: $needle (broken continuation?)" ;;
    esac
done
# The FINAL jar token (the flink run target) must come AFTER the first -e
# (ordering sanity: one command). NOTE: compute.jar also appears earlier in
# the docker cp line — use the LAST occurrence, not the first.
first_pos="${body%%-e ALLOW_FULL_REPLAY*}"
first_pos=$(( ${#first_pos} + 1 ))
jar_pos="${body##*compute.jar}"
jar_pos=$(( ${#body} - ${#jar_pos} ))
[ "$first_pos" -gt 0 ] && [ "$jar_pos" -gt "$first_pos" ] \
    && ok "G3 submit is one ordered command (env vars → flink run → jar)" \
    || bad "G3 submit token order broken (env vars must precede compute.jar)"

# ---- G4: no '#' comment or blank lines inside any backslash-continued
# command — the EXACT 2026-08-30 bug class. State machine: after a line
# ending in '\\', the next line is INSIDE the continued command; a
# comment/blank there breaks the command silently (bash -n won't catch it).
in_cont=0
lineno=0
while IFS= read -r line; do
    lineno=$((lineno+1))
    trimmed="${line#"${line%%[![:space:]]*}"}"   # lstrip
    if [ "$in_cont" -eq 1 ]; then
        if [ -z "$trimmed" ] || [[ "$trimmed" == \#* ]]; then
            bad "G4 line $lineno: comment/blank INSIDE a backslash-continued command (the 2026-08-30 bug class): $trimmed"
            in_cont=0
            continue
        fi
    fi
    # G4b (2026-08-30, A3-run failure): a trailing comment on ANY line
    # inside a backslash-continued command kills the continuation — bash
    # treats "VAR=x # note" as the command ending at the '#', silently
    # dropping every following env var. This exact bug broke the A3 run
    # (OTEL_COLLECTOR_HOST + all subsequent vars lost). Rule: inside a
    # continuation, no '#' at all (space-hash caught; leading-# is G4).
    if [ "$in_cont" -eq 1 ] && [[ "$trimmed" == *" #"* ]]; then
        bad "G4b line $lineno: trailing comment inside a continued command kills the backslash chain: $trimmed"
        continue
    fi
    case "$line" in
        *\\) in_cont=1 ;;
        *)    in_cont=0 ;;
    esac
done < "$LIB"
[ "$fail" -eq 0 ] && ok "G4 no comments/blanks inside continued commands"

rm -rf "$OUT"
echo "---"
echo "guards: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
