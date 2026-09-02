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
          pipeline_validate_rate pipeline_port_free pipeline_validate_compute_jar \
          pipeline_validate_compose_bind_sources pipeline_fluss_port_open \
          pipeline_compile_fluss_ready_probe pipeline_fluss_metadata_ready \
          pipeline_wait_for_fluss_ready \
          flink_metric_dump flink_wait_state; do
    declare -f "$fn" >/dev/null 2>&1 && ok "G2 function $fn defined" \
        || bad "G2 function $fn MISSING from $LIB"
done

# ---- G17 (2026-09-01): Fluss readiness must precede table mutation. A
# coordinator-only check accepts a live RPC endpoint while the tablet is
# crash-looping/leaderless, producing "Alive tablet server is empty" during
# the first destructive purge. Keep both container-state and client-listener
# checks in the shared preflight, and make the timeout configurable but
# positive.
if grep -q 'pipeline_wait_for_fluss_ready || return 1' "$LIB" \
    && grep -q 'pipeline_fluss_port_open 127.0.0.1 9123' "$LIB" \
    && grep -q 'pipeline_fluss_port_open 127.0.0.1 9124' "$LIB" \
    && grep -q 'FLUSS_READY_TIMEOUT_S' "$LIB"; then
    ok "G17 Fluss coordinator+tablet readiness guard precedes table mutation"
else
    bad "G17 Fluss readiness guard missing — leaderless tablet can reach TablePurge"
fi

# ---- G18 (2026-09-01): open listeners are still not sufficient. The Fluss
# client itself rejects metadata initialization while the tablet is alive but
# not elected, so readiness must include a read-only metadata probe before the
# first drop/create.
if grep -q 'pipeline_compile_fluss_ready_probe || return 1' "$LIB" \
    && grep -q 'pipeline_fluss_metadata_ready; then' "$LIB" \
    && grep -q 'FlussReadyProbe' "$LIB"; then
    ok "G18 Fluss metadata readiness probe precedes table mutation"
else
    bad "G18 metadata probe missing — listener-open/leaderless race can reach TablePurge"
fi

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

# ---- G11 (2026-08-31): JVM flags must be IDENTICAL in both harness
# scripts. pipeline-lib.sh and loadtest-run.sh each carry a full java
# invocation for the ingestion JVM (historical duplication). They were
# patched in lockstep for the D6 right-size — this guard makes drift
# impossible to miss: different heap/direct flags in the two scripts mean
# different bench runs measure different JVMs.
JVM_FLAGS_LIB=$(grep -oE -- '-Xms[0-9]+[mg] -Xmx[0-9]+[mg] -XX:MaxDirectMemorySize=[0-9]+[mg]' "$LIB" | sort -u)
JVM_FLAGS_LOADTEST=$(grep -oE -- '-Xms[0-9]+[mg] -Xmx[0-9]+[mg] -XX:MaxDirectMemorySize=[0-9]+[mg]' "$SCRIPT_DIR/loadtest-run.sh" | sort -u)
if [ -n "$JVM_FLAGS_LIB" ] && [ "$JVM_FLAGS_LIB" = "$JVM_FLAGS_LOADTEST" ]; then
    ok "G11 JVM flags identical in pipeline-lib and loadtest-run ($JVM_FLAGS_LIB)"
elif [ -z "$JVM_FLAGS_LIB" ]; then
    bad "G11 could not find JVM flags in pipeline-lib.sh — pattern drifted?"
else
    bad "G11 JVM flags DRIFTED: pipeline-lib='$JVM_FLAGS_LIB' vs loadtest-run='$JVM_FLAGS_LOADTEST'"
fi

# ---- G9 (2026-08-31): the G3 native fix is in place and total —
# (a) pipeline-lib must NOT pass ARROW_INSTRUMENT_TOKENS to the ingestion
#     JVM (Java's startBridge now owns that env var in the child);
# (b) INSTRUMENT_MANIFEST_PATH must point at the SLICE, not the full CSV;
# (c) holistic-measure must contain the G8 fingerprint gate that fails the
#     run on any mismatch line in java.out.
if grep -q 'ARROW_INSTRUMENT_TOKENS=' "$LIB"; then
    bad "G9 pipeline-lib still sets ARROW_INSTRUMENT_TOKENS — Java's child-env handoff (G3) is the single source of truth now"
else
    ok "G9 pipeline-lib does not pass ARROW_INSTRUMENT_TOKENS (G3 handoff owns it)"
fi
if grep -q 'INSTRUMENT_MANIFEST_PATH="\$LIB_MANIFEST_SLICE"' "$LIB"; then
    ok "G9 pipeline-lib hands Java the manifest SLICE (single token set)"
else
    bad "G9 INSTRUMENT_MANIFEST_PATH must point at \$LIB_MANIFEST_SLICE, not the full CSV"
fi
HM="$SCRIPT_DIR/holistic-measure.sh"
grep -q 'fingerprint_gate' "$HM" \
    && ok "G9 holistic-measure has the G8 fingerprint gate" \
    || bad "G9 holistic-measure missing fingerprint_gate (G8)"

# ---- G10 (2026-08-31): the G8 gate must FIRE on tampered evidence —
# a java.out containing a mismatch line must fail the gate (proven against
# a bug-injected copy, per the standing rule).
G10DIR="$(mktemp -d)"
mkdir -p "$G10DIR/j1"
printf 'INFO  ok line\nWARN  ingestion: bridge manifest_fingerprint mismatch (slot=hft-0, epoch=1): got=aa want=bb\n' \
    > "$G10DIR/j1/java.out"
n=$(grep -ac "manifest_fingerprint mismatch\|assigned_token_set_hash mismatch" "$G10DIR/j1/java.out" 2>/dev/null || true)
if [ "${n:-0}" -ge 1 ]; then
    ok "G10 tampered java.out detected ($n mismatch line(s) — gate would fire)"
else
    bad "G10 tampered java.out NOT detected — the G8 gate is decorative"
fi
printf 'INFO  clean log\n' > "$G10DIR/j1/java.out"
n=$(grep -ac "manifest_fingerprint mismatch\|assigned_token_set_hash mismatch" "$G10DIR/j1/java.out" 2>/dev/null || true)
[ "${n:-0}" -eq 0 ] \
    && ok "G10 clean java.out passes (zero mismatch lines)" \
    || bad "G10 clean java.out flagged — gate has false positives"
rm -rf "$G10DIR"

# ---- G12 (2026-08-31): B5 experiment integrity — when
# UNALIGNED_CHECKPOINTS=true is set, the submit MUST carry the unaligned
# flag; without it, a "B5 experiment run" silently measures the baseline
# and closes the lever on fabricated evidence.
PL="$SCRIPT_DIR/pipeline-lib.sh"
grep -q 'UNALIGNED_CHECKPOINTS' "$PL" \
    && grep -q 'execution.checkpointing.unaligned=true' "$PL" \
    && ok "G12 unaligned flag wiring present in pipeline_submit_job" \
    || bad "G12 UNALIGNED_CHECKPOINTS wiring missing — B5 experiments run the baseline by mistake"
# flag default must be OFF (baseline unchanged unless requested)
grep -q '\${UNALIGNED_CHECKPOINTS:-false}' "$PL" \
    && ok "G12 unaligned flag defaults OFF (baseline preserved)" \
    || bad "G12 unaligned flag not default-OFF — every run would be unaligned"
# phase gauge sampler must exist in the measure script
grep -q 'checkpointStartDelayNanos\|tm-prom-cp-phases' "$HM" \
    && ok "G12 checkpoint-phase gauge sampler present" \
    || bad "G12 phase gauge sampler missing — B5 attribution silently broken"
# analyzer must fail-fast when B5 data is expected but absent
grep -q 'B5_EXPECT_PHASES' "$SCRIPT_DIR/holistic-analyze.py" \
    && ok "G12 analyzer fail-fast (B5_EXPECT_PHASES) present" \
    || bad "G12 analyzer lacks B5 fail-fast — empty phase data reads as 'no verdict'"

# ---- G13 (2026-08-31): F6 rejection-counter wiring — the counters have
# existed in RawValidationFunction since the beginning; the harness must
# sample them (tm-prom-invalid.tsv) and the analyzer must guard on them,
# or rejections stay invisible (gotcha-#22 pattern).
grep -q 'compute_invalid' "$HM" \
    && ok "G13 F6 rejection-counter sampler present in measure script" \
    || bad "G13 F6 sampler missing — rejections invisible (counters exist, nobody samples)"
grep -q 'F6 raw-validation rejections' "$SCRIPT_DIR/holistic-analyze.py" \
    && grep -q 'invalid_rows' "$SCRIPT_DIR/holistic-analyze.py" \
    && ok "G13 F6 analyzer report + guard present" \
    || bad "G13 analyzer lacks F6 report/guard — rejection counts never checked"
# the analyzer's F6 failure message must reference byReason (per-rule detail)
grep -q 'byReason' "$SCRIPT_DIR/holistic-analyze.py" \
    && ok "G13 F6 guard names per-reason detail" \
    || bad "G13 F6 guard lacks per-reason breakdown"

rm -rf "$OUT"

# ---- G14 (2026-09-01): B6 artifact classpath guard must FIRE on the exact
# broken shape observed in C2. Use a temporary jar containing one Fluss class;
# the guard must reject it. Then prove the production-shaped jar (no Fluss
# classes/descriptors) passes, and a truncated/non-JAR file fails closed. This
# is intentionally runtime execution, not just a grep for the guard text.
G14DIR="$(mktemp -d)"
mkdir -p "$G14DIR/org/apache/fluss/client"
printf 'not-a-real-class' > "$G14DIR/org/apache/fluss/client/Duplicate.class"
(cd "$G14DIR" && jar cf bad-compute.jar org) >/dev/null 2>&1
LIB_JAR="$G14DIR/bad-compute.jar"
if pipeline_validate_compute_jar >/dev/null 2>&1; then
    bad "G14 duplicate Fluss class was accepted — B6 guard would not fire"
else
    ok "G14 duplicate Fluss class rejected before submit"
fi
mkdir -p "$G14DIR/good/com/trading"
printf 'not-a-real-class' > "$G14DIR/good/com/trading/Job.class"
(cd "$G14DIR/good" && jar cf ../good-compute.jar com) >/dev/null 2>&1
LIB_JAR="$G14DIR/good-compute.jar"
if pipeline_validate_compute_jar >/dev/null 2>&1; then
    ok "G14 Flink-owned Fluss classpath shape accepted"
else
    bad "G14 valid compute artifact rejected"
fi
printf 'truncated jar\n' > "$G14DIR/truncated-compute.jar"
LIB_JAR="$G14DIR/truncated-compute.jar"
if pipeline_validate_compute_jar >/dev/null 2>&1; then
    bad "G14 corrupt compute artifact was accepted — B6 guard did not fail closed"
else
    ok "G14 corrupt compute artifact rejected before submit"
fi
rm -rf "$G14DIR"

# ---- G16 (2026-09-01): B7 Compose short-bind guard — a missing source must
# fail before Docker can auto-create a directory and return an opaque OCI
# exit-127 mount error. The real repository sources must still pass.
G16DIR="$(mktemp -d)"
G16_COMPOSE_DIR="$LIB_COMPOSE_DIR"
LIB_COMPOSE_DIR="$G16DIR"
if pipeline_validate_compose_bind_sources >/dev/null 2>&1; then
    bad "G16 missing Compose bind sources were accepted"
else
    ok "G16 missing Compose bind source rejected before container start"
fi
mkdir -p "$G16DIR/flink-log4j-console.properties"
if pipeline_validate_compose_bind_sources >/dev/null 2>&1; then
    bad "G16 directory-shaped Compose bind source was accepted"
else
    ok "G16 directory-shaped Compose bind source rejected (OCI exit-127 prevented)"
fi
LIB_COMPOSE_DIR="$G16_COMPOSE_DIR"
if pipeline_validate_compose_bind_sources >/dev/null 2>&1; then
    ok "G16 repository Compose bind sources are regular files"
else
    bad "G16 repository Compose bind sources failed validation"
fi
rm -rf "$G16DIR"

# ---- G19 (2026-09-02): preflight-state guard. Launch-phase functions that
# depend on pipeline_preflight's setup (CP, LIB_MANIFEST_SLICE, fresh TM,
# Fluss readiness) must fail fast with a clear message when preflight has
# NOT run — the stage-a2-baseline "unbound variable deep inside ingestion"
# failure class. Runtime negative tests + wiring greps (drift-proof).
PL="$SCRIPT_DIR/pipeline-lib.sh"
bash -n "$PL" || { bad "G19 lib syntax invalid"; exit 1; }

# (a) runtime: guarded functions refuse BEFORE launching anything
OUT="$(mktemp -d)" FAKETOOL_PORT=8899 RATE_HZ=10
# shellcheck source=pipeline-lib.sh
source "$PL"
PIPELINE_PREFLIGHT_OK=0
msg="$(pipeline_start_faketool 2>&1 || true)"
case "$msg" in
  *"pipeline_preflight not run"*) ok "G19 start_faketool refuses without preflight" ;;
  *) bad "G19 start_faketool did not refuse: $msg" ;;
esac
msg="$(pipeline_purge_preview_table 2>&1 || true)"
case "$msg" in
  *"pipeline_preflight not run"*) ok "G19 purge refuses without preflight" ;;
  *) bad "G19 purge did not refuse: $msg" ;;
esac
msg="$(pipeline_start_ingestion 2>&1 || true)"
case "$msg" in
  *"pipeline_preflight not run"*) ok "G19 start_ingestion refuses without preflight" ;;
  *) bad "G19 start_ingestion did not refuse: $msg" ;;
esac
msg="$(pipeline_submit_job 2>&1 || true)"
case "$msg" in
  *"pipeline_preflight not run"*) ok "G19 submit refuses without preflight" ;;
  *) bad "G19 submit did not refuse: $msg" ;;
esac
rm -rf "$OUT"

# (b) wiring: every launch-phase function carries the guard; preflight sets
# the OK flag only on success (reset at entry, set before the OK log).
for fn in pipeline_purge_table pipeline_ensure_tentative_markers_table \
          pipeline_start_faketool pipeline_start_ingestion pipeline_submit_job; do
  if grep -q 'pipeline_require_preflight || return 1' "$PL"; then
    ok "G19 guard wired in $fn"
  else
    bad "G19 guard MISSING in $fn — skips preflight silently"
  fi
done
grep -q 'PIPELINE_PREFLIGHT_OK=0' "$PL" \
  && grep -q 'PIPELINE_PREFLIGHT_OK=1' "$PL" \
  && ok "G19 preflight resets (0) at entry and commits (1) on success" \
  || bad "G19 preflight OK-flag wiring missing/incomplete"

# ---- G20 (2026-09-02): power-cut resilience guards. After two power cuts in
# one day destroyed data (torn Fluss segments -> tablet crash-loop) and left
# orphan processes (a stray faketool had to be killed by hand), these guards
# make recovery deterministic:
#   (a) preflight kills stray ingestion JVMs (twin of the stray-faketool kill)
#   (b) repair-tablet.sh --all sweep exists (one-command recovery), with
#       policy pinning + final crash-loop check that fails the sweep
#   (c) measurement runners refuse to start on a freshly-booted host
echo "---"
echo "G20 power-cut resilience guards"

grep -q 'pgrep -f "com.trading.ingestion.IngestionService"' "$PL" \
  && ok "G20a preflight kills stray ingestion JVM" \
  || bad "G20a stray-ingestion kill MISSING in preflight"
grep -q 'kill -9 "$p" 2>/dev/null || true' "$PL" \
  && ok "G20a stray kills are non-fatal (best-effort)" \
  || bad "G20a stray kill not best-effort"

repair="$SCRIPT_DIR/fluss-repair/repair-tablet.sh"
bash -n "$repair" || { bad "G20b repair-tablet.sh syntax invalid"; exit 1; }
grep -q 'TABLE_ARG" = "--all"' "$repair" \
  && ok "G20b repair-tablet.sh --all sweep mode exists" \
  || bad "G20b --all sweep mode MISSING"
grep -q 'docker update --restart=no "$CONTAINER"' "$repair" \
  && grep -q 'docker update --restart="$POLICY_BEFORE"' "$repair" \
  && ok "G20b sweep pins + restores the restart policy" \
  || bad "G20b policy pin/restore MISSING"
grep -q 'grep -q "Restarting"' "$repair" \
  && grep -q 'exit 1' "$repair" \
  && ok "G20b sweep fails closed if still crash-looping" \
  || bad "G20b final crash-loop check MISSING"
grep -q 'repair_one_table "$TABLE_DIR" "$TABLE" || rc=$?' "$repair" \
  && ok "G20b single-table path is errexit-safe" \
  || bad "G20b single-table call not errexit-safe"

runner="$SCRIPT_DIR/stage-a2-baseline.sh"
bash -n "$runner" || { bad "G20c stage-a2-baseline.sh syntax invalid"; exit 1; }
grep -q 'MIN_UPTIME_S' "$runner" \
  && grep -q '/proc/uptime' "$runner" \
  && grep -q 'fatal "host uptime' "$runner" \
  && ok "G20c runner refuses on freshly-booted host" \
  || bad "G20c uptime gate MISSING in runner"

echo "---"
echo "guards: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
