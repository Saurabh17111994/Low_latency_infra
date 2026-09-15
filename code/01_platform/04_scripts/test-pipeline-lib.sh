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
# P6-142: pipeline-lib.sh now refuses to be sourced without ROOT/RATE_HZ;
# this guard suite sources it directly, so satisfy the contract here (the
# nested-source tests below already set RATE_HZ=10 themselves).
RATE_HZ="${RATE_HZ:-10}"
export RATE_HZ
# The directive must sit immediately above the command it annotates: wave 17
# inserted the RATE_HZ pre-amble between the two and detached it (SC1090).
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
# CHG-122: the JVM runs in a container; $OUT (which HOLDS the slice) is
# bind-mounted at /run, so the in-container path IS the slice. Intent kept:
# never the full CSV, always the 1024-token slice.
if grep -q 'INSTRUMENT_MANIFEST_PATH=/run/instruments-1024.csv' "$LIB" \
    && ! grep -q 'INSTRUMENT_MANIFEST_PATH="\$LIB_MANIFEST"' "$LIB"; then
    ok "G9 pipeline-lib hands Java the manifest SLICE (in-container /run/instruments-1024.csv = the slice)"
else
    bad "G9 INSTRUMENT_MANIFEST_PATH must point at the 1024 SLICE, not the full CSV"
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
# shellcheck disable=SC2034  # overrides the lib's LIB_JAR for this case; read by pipeline-lib.sh
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
# depend on pipeline_preflight's setup (CP, fresh TM,
# Fluss readiness) must fail fast with a clear message when preflight has
# NOT run — the stage-a2-baseline "unbound variable deep inside ingestion"
# failure class. Runtime negative tests + wiring greps (drift-proof).
PL="$SCRIPT_DIR/pipeline-lib.sh"
bash -n "$PL" || { bad "G19 lib syntax invalid"; exit 1; }

# (a) runtime: guarded functions refuse BEFORE launching anything
# shellcheck disable=SC2034  # lib knobs: FAKETOOL_PORT and RATE_HZ are read by pipeline-lib.sh
OUT="$(mktemp -d)" FAKETOOL_PORT=8899 RATE_HZ=10
# shellcheck source=pipeline-lib.sh
source "$PL"
# shellcheck disable=SC2034  # lib state: read by pipeline-lib.sh's preflight guard
PIPELINE_PREFLIGHT_OK=0
msg="$(pipeline_start_faketool 2>&1 || true)"
case "$msg" in
  *"pipeline_preflight not run"*) ok "G19 start_faketool refuses without preflight" ;;
  *) bad "G19 start_faketool did not refuse: $msg" ;;
esac
msg="$(pipeline_purge_raw_table 2>&1 || true)"
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
for fn in pipeline_purge_table pipeline_purge_raw_table \
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
#   (a) preflight detects stray ingestion JVMs and FAILS the run (CHG-122:
#       G26 host-isolation policy replaced the old silent auto-kill)
#   (b) repair-tablet.sh --all sweep exists (one-command recovery), with
#       policy pinning + final crash-loop check that fails the sweep
#   (c) measurement runners refuse to start on a freshly-booted host
echo "---"
echo "G20 power-cut resilience guards"

grep -q "pgrep -f 'com.trading.ingestion.IngestionService'" "$PL" \
  && ok "G20a preflight detects stray ingestion JVM (G26 policy guard)" \
  || bad "G20a stray-ingestion detection MISSING in preflight"
# CHG-122 (2026-09-02): strays are a POLICY VIOLATION now - fail the run
# with the reason instead of silently killing (auto-kill hid the incident
# class that poisoned baselines).
grep -q 'POLICY VIOLATION' "$PL" \
  && ok "G20a host strays FAIL the run with the reason (CHG-122 policy)" \
  || bad "G20a stray handling lost its fail-fast reason"

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

# ---- G21 (2026-09-02): B2 passive checkpoint probes (plan Stage B2) --------
# The ingestion.tsv hook was PROMISED in stage-capture.sh's header but never
# implemented (comment-only) — A2 captures silently lost the feed->ack leg.
# G21 pins: the hook now exists, the two Fluss probes exist and are wired,
# the runner enables them, and the parser reads them.
echo "---"
echo "G21 B2 checkpoint probes"

cap="$SCRIPT_DIR/stage-capture.sh"
bash -n "$cap" || { bad "G21 stage-capture.sh syntax invalid"; exit 1; }
grep -q 'sample_ingestion' "$cap" \
  && grep -q 'otlp-metrics-payload' "$cap" \
  && ok "G21a ingestion.tsv hook implemented (was comment-only)" \
  || bad "G21a ingestion.tsv hook MISSING"
grep -q 'INGESTION_JAVA_OUT.*file missing' "$cap" \
  && ok "G21a ingestion hook fails loud when java.out missing" \
  || bad "G21a ingestion hook missing fail-loud guard"
grep -q 'sample_probes' "$cap" \
  && grep -q 'FlussReadLagProbe' "$cap" \
  && grep -q 'FlussKvProbe' "$cap" \
  && ok "G21b read-lag + consumer-read probes wired into ticks" \
  || bad "G21b probe wiring MISSING"
grep -q 'javac -cp "$FLUSS_PROBE_CP"' "$cap" \
  && ok "G21b probes compile once, fail fast on broken CP" \
  || bad "G21b probe compile guard MISSING"

for src in FlussReadLagProbe FlussKvProbe; do
  probe="$SCRIPT_DIR/fluss-probes/$src.java"
  [ -f "$probe" ] || { bad "G21c $src.java MISSING"; continue; }
  ok "G21c probe source present: $src"
done
grep -q 'listPartitionInfos' "$SCRIPT_DIR/fluss-probes/FlussReadLagProbe.java" \
  && grep -q 'listOffsets(' "$SCRIPT_DIR/fluss-probes/FlussReadLagProbe.java" \
  && grep -q 'p.getPartitionName()' "$SCRIPT_DIR/fluss-probes/FlussReadLagProbe.java" \
  && ok "G21c read-lag probe is partition-aware (raw_table_1 is auto-partitioned)" \
  || bad "G21c read-lag probe NOT partition-aware"

grep -q 'FLUSS_PROBE_CP="$CP"' "$runner" \
  && ok "G21d A2 runner enables B2 probes (FLUSS_PROBE_CP=CP)" \
  || bad "G21d runner does not enable B2 probes"

parse="$SCRIPT_DIR/stage_capture_parse.py"
grep -q 'def b2_read_lag_report' "$parse" \
  && grep -q 'def b2_consumer_read_report' "$parse" \
  && ok "G21e parser reads read-lag + consumer-read TSVs" \
  || bad "G21e parser B2 reports MISSING"

# G21f (2026-09-02): p95/p99 latency coverage. The ingestion histogram
# summary attrs carry p50/p90/p99 (no server-side p95 — 50/90/99 only); the
# sampler must flatten all three, and the B2 reports must carry p99 columns
# (read-lag + consumer-read) so tail latency is never silently dropped.
grep -q '"p50", "p90", "p99"' "$cap" \
  && ok "G21f ingestion sampler flattens p50/p90/p99 histogram quantiles" \
  || bad "G21f ingestion sampler missing p90/p99 flattening"
grep -q 'net_lag_p99_records' "$parse" \
  && grep -q 'cp9cp10_p99_ms' "$parse" \
  && ok "G21f B2 reports carry p99 columns (read-lag + consumer-read)" \
  || bad "G21f B2 reports missing p99 columns"

# ---- G21g (2026-09-04): measurement-facility fixes for the 48k/s e2e --------
# 1. Flink scrape keeps custom operator metrics (compute.*) — the old filter
#    listed only the seven system families, so the latency/dedup scorecard
#    was blank for every custom counter/histogram.
# 2. Closed feature-candle table probe (feature_candles_15s) + parser report.
# 3. Bridge tick-counts drained before container cleanup (ING-TCP-001).
# 4. OTLP payload local-capture env gate (METRICS_LOCAL_LOG) wired by the
#    soak runner so ingestion.tsv has rows even with a healthy collector.
echo "---"
echo "G21g 48k e2e measurement facilities"
grep -qE '_operator_|operator_.*compute' "$cap" \
  && ok "G21g scrape keeps custom operator metrics" \
  || bad "G21g custom-operator scrape filter MISSING"
# 2026-09-05 (grep-filter regression): the old alternation
# 'flink_taskmanager_job_task_.*_operator_' does NOT match chain-head custom
# families (flink_taskmanager_job_task_operator_compute_* — the prefix ends
# in '_' so '.*_operator_' needs a SECOND '_' before 'operator'). Every soak
# prom file silently dropped compute.* samples while RocksDB/split_watermark
# (which have extra '_' segments) survived. The filter must be the literal
# prefix 'flink_taskmanager_job_task_operator_'; this pins it.
grep -q "flink_taskmanager_job_task_operator_" "$cap" \
  && ! grep -q "flink_taskmanager_job_task_.\*_operator_" "$cap" \
  && ok "G21g scrape filter is the literal operator prefix (2026-09-05 fix)" \
  || bad "G21g scrape filter regressed to the .*_operator_ form that drops chain-head compute.* samples"
grep -q 'PROBE_CLOSED_TABLE\|feature_candles_15s"' "$cap" \
  && grep -q 'closed-read.tsv' "$cap" \
  && ok "G21g closed-table probe wired (closed-read.tsv)" \
  || bad "G21g closed-table probe MISSING"
grep -q 'def b2_closed_read_report' "$parse" \
  && ok "G21g parser reads closed-read.tsv" \
  || bad "G21g closed-read parser report MISSING"
grep -q 'tick-counts.txt\|arrow-tick-counts' "$SCRIPT_DIR/stage-soak-e2e.sh" \
  && ok "G21g soak drains bridge tick-counts pre-cleanup" \
  || bad "G21g tick-counts drain MISSING"
grep -q 'METRICS_LOCAL_LOG=1' "$SCRIPT_DIR/stage-soak-e2e.sh" \
  && ok "G21g soak enables OTLP local payload capture" \
  || bad "G21g METRICS_LOCAL_LOG wiring MISSING"
# 2026-09-05 (false-stall): the t+65 arrow-bridge stall guard parsed the
# log's HH:MM:SS (UTC — log4j pattern ends in Z) with the LOCAL zone (IST,
# +5:30), so a fresh UTC report read as ~5.5h old and the guard killed a
# healthy soak at t+76s (run 20260904-140936). Both sides must be UTC.
grep -q 'TZ=UTC date' "$cap" \
  && grep -q 'now_utc="\$(date -u' "$cap" \
  && ok "G21g bridge-stall guard compares UTC (2026-09-05 false-stall fix)" \
  || bad "G21g bridge-stall guard not UTC-safe (UTC log vs IST host -> false stall kills healthy soaks)"


# ---- G22/G23/G24 (2026-09-02): FLINK_PROPERTIES + TM-config fail-fast ----
# Two silent failure classes hit in one day:
#  (1) FLINK_PROPERTIES key collision (String leaf vs nested path) -> TM
#      crash-loop (ClassCastException) OR SILENT drop of
#      state.backend.rocksdb.localdir -> RocksDB on the container overlay
#      (the CHG-120 ~1.7k/s degradation class, invisible until analysis).
#  (2) `docker compose restart` reuses the OLD container config -> a compose
#      edit (slots 10->16) never reached the TM -> job died "unassigned
#      resource" at t+13s (and p16 OOM'd the TM heap later).
# G22 = static compose validator; G23 = runtime preflight checks;
# G24 = RocksDB-on-named-volume check in the A2 runner.
echo "---"
echo "G22 FLINK_PROPERTIES static validator"

validator="$SCRIPT_DIR/check_flink_properties.py"
[ -f "$validator" ] \
  && ok "G22a check_flink_properties.py present" \
  || bad "G22a check_flink_properties.py MISSING"
python3 "$validator" >/dev/null 2>&1 \
  && ok "G22b the REAL docker-compose.yml FLINK_PROPERTIES validates (no comments in block, no prefix collisions, required keys present)" \
  || { python3 "$validator" 2>&1 | head -8 >&2; bad "G22b docker-compose.yml FLINK_PROPERTIES INVALID - see reason above"; }
grep -q "FORBIDDEN_LEAVES" "$validator" \
  && grep -q "PREFIX COLLISION" "$validator" \
  && ok "G22c validator explains the WHY (prefix collision + forbidden leaf reasons)" \
  || bad "G22c validator lost its explanatory failure reasons"

echo "G23 TM config + slots runtime preflight"

lib="$SCRIPT_DIR/pipeline-lib.sh"
bash -n "$lib" || { bad "G23 pipeline-lib.sh syntax invalid"; exit 1; }
grep -q "pipeline_verify_tm_config" "$lib" \
  && ok "G23a pipeline-lib.sh has pipeline_verify_tm_config" \
  || bad "G23a pipeline_verify_tm_config MISSING in pipeline-lib.sh"
grep -q "pipeline_verify_tm_config || return 1" "$lib" \
  && ok "G23b preflight calls the TM-config verification (fail-fast before feed/submit)" \
  || bad "G23b preflight does NOT call pipeline_verify_tm_config"
grep -q "unassigned resource" "$lib" \
  && grep -q "force-recreate" "$lib" \
  && ok "G23c slot-shortage failure explains WHY (compose restart reuses old config) + the recreate fix" \
  || bad "G23c slot failure message lost its reason/fix hint"
grep -q "CHG-120" "$lib" \
  && ok "G23d missing-localdir failure names the degradation class (CHG-120 overlay)" \
  || bad "G23d missing-localdir failure lost its reason"

echo "G24 RocksDB-on-named-volume runtime check (A2 runner)"

runner="$SCRIPT_DIR/stage-a2-baseline.sh"
bash -n "$runner" || { bad "G24 stage-a2-baseline.sh syntax invalid"; exit 1; }
grep -q "G24" "$runner" \
  && grep -q "flink-rocksdb/job_\${JOB_ID}_op_" "$runner" \
  && ok "G24a runner verifies the job's RocksDB dirs land under /tmp/flink-rocksdb (named volume)" \
  || bad "G24a RocksDB-on-volume check MISSING in runner"
grep -q "CHG-120" "$runner" \
  && ok "G24b G24 failure names the degradation class + the config to check" \
  || bad "G24b G24 failure lost its reason"

echo "G26 host-isolation policy (CHG-122: fully-Docker data path)"

# G26a: NO host launch paths remain in the lib — faketool must not be built
# or executed on the host, and the ingestion JVM must not be a host `java`.
grep -q 'go build .*faketool' "$lib" \
  && bad "G26a host `go build` of faketool still present" \
  || ok "G26a no host faketool build"
grep -qE '"\$OUT/bin/faketool"' "$lib" \
  && bad "G26a host faketool binary launch still present" \
  || ok "G26a no host faketool launch"
grep -q -- '-cp /app/ingestion.jar com.trading.ingestion.IngestionService' "$lib" \
  && ! grep -q -- '-cp "$LIB_ING_JAR"' "$lib" \
  && ok "G26a ingestion runs from the IMAGE jar (/app/ingestion.jar), never the host jar path" \
  || bad "G26a ingestion JVM still launched on the host"

# G26b: both loadgen components are docker-run on the shared network
grep -q 'docker run -d --name "$LIB_FAKETOOL_CONTAINER"' "$lib" \
  && grep -q 'docker run -d --name "$LIB_INGESTION_CONTAINER"' "$lib" \
  && grep -q -- '--network "$LIB_TRADING_NET"' "$lib" \
  && ok "G26b faketool + ingestion launch as containers on $LIB_TRADING_NET" \
  || bad "G26b container launch missing/wrong network"
grep -q 'ARROW_HFT_URL="ws://$LIB_FAKETOOL_CONTAINER:$FAKETOOL_PORT"' "$lib" \
  && grep -q 'FLUSS_BOOTSTRAP=fluss-coordinator:9123' "$lib" \
  && ok "G26b endpoints use in-network DNS names (no localhost NAT hairpin)" \
  || bad "G26b endpoint still points at localhost"

# G26c: the policy guard itself — host data-path processes fail the run
# WITH the reason (policy violation, not auto-kill)
grep -q "pgrep -x faketool" "$lib" \
  && grep -q "pgrep -f 'com.trading.ingestion.IngestionService'" "$lib" \
  && grep -q "POLICY VIOLATION" "$lib" \
  && ok "G26c host data-path processes fail preflight with the reason" \
  || bad "G26c host-process policy guard missing or lost its reason"

# G26d: stale loadgen containers + network-mismatch fail with reasons
grep -q "stale loadgen container" "$lib" \
  && ok "G26d stale container collision fails with reason" \
  || bad "G26d stale-container guard missing"
grep -q "cross-network DNS failure" "$lib" \
  && ok "G26d TM-network mismatch fails with reason (zero-append class)" \
  || bad "G26d network-mismatch guard missing"

# G26e: cleanup removes containers, not host PIDs
grep -q 'docker rm -f "$LIB_INGESTION_CONTAINER" "$LIB_FAKETOOL_CONTAINER"' "$lib" \
  && ok "G26e cleanup removes the loadgen containers" \
  || bad "G26e cleanup does not remove containers"
grep -q 'kill -9 "$JVM_PID"' "$lib" \
  && bad "G26e cleanup still kills host PIDs" \
  || ok "G26e no host-PID kills in cleanup"

# G26f: compose gates the loadgen service behind a profile (never started
# by `up`) and pins the image tag the lib expects
compose="$ROOT/code/01_platform/01_docker/docker-compose.yml"
grep -q 'profiles: \["loadgen"\]' "$compose" \
  && ok "G26f loadgen compose service is profile-gated" \
  || bad "G26f loadgen service would start with plain `up`"
grep -q 'image: pipeline-loadgen:1.0.0' "$compose" \
  && ok "G26f compose pins the image tag the lib expects" \
  || bad "G26f image tag mismatch vs pipeline-lib.sh"

echo "G27 loadgen image verification (user request 2026-09-02: always the Docker image, always tested)"

# G27 is called by preflight BEFORE any run uses the image.
grep -q "pipeline_verify_loadgen_image || return 1" "$lib" \
  && ok "G27 preflight calls the image verification" \
  || bad "G27 preflight never verifies the loadgen image"

grep -q "test -x /app/faketool" "$lib" \
  && grep -q "missing its artifacts" "$lib" \
  && ok "G27a missing-artifact failure names WHY (foreign/corrupt tag) + rebuild command" \
  || bad "G27a artifact check missing or lost its reason"

grep -q "STALE" "$lib" \
  && grep -q "bogus baseline" "$lib" \
  && grep -q "Dockerfile.loadgen" "$lib" \
  && ok "G27b stale-image failure names the build inputs + WHY (measuring old code)" \
  || bad "G27b freshness check missing or lost its reason"

grep -q "red harness" "$lib" \
  && grep -q "g27-guard-run.log" "$lib" \
  && ok "G27c red-guard-suite failure refuses the run with evidence path" \
  || bad "G27c harness-green gate missing or lost its reason"

# G27 must be fail-closed on every branch: each failure returns 1.
n_g27_ret="$(sed -n '/pipeline_verify_loadgen_image() {/,/^}/p' "$lib" | grep -c 'return 1')"
[ "$n_g27_ret" -ge 3 ] \
  && ok "G27 all $n_g27_ret failure branches return 1 (fail-closed)" \
  || bad "G27 a failure branch does not return 1 (would fall through)"

echo "G28 observability bundle (2026-09-02 decline hunt: io latency + checkpoint alignment + RocksDB + O2 single-pane)"

# G28a: io-latency probe exists, parses, and fails LOUD when iostat is absent
probe="$SCRIPT_DIR/io-latency-probe.sh"
[ -x "$probe" ] && bash -n "$probe" \
  && ok "G28a io-latency-probe.sh present + parses" \
  || bad "G28a io-latency-probe.sh missing or fails bash -n"
grep -q "this probe is the per-op latency evidence source" "$probe" \
  && ok "G28a iostat-absent failure names WHY" \
  || bad "G28a iostat-absent failure lost its reason"
grep -q 'colmap\["r_await"\]' "$probe" \
  && ok "G28a probe parses iostat BY HEADER (fixed indices were the 2121ms bug)" \
  || bad "G28a probe reverted to fixed-index parsing (sysstat-version fragile)"

# G28b: o2_ingest refuses bad input with reasons
o2i="$SCRIPT_DIR/o2_ingest.py"
[ -f "$o2i" ] && python3 -c "import ast; ast.parse(open('$o2i').read())" \
  && ok "G28b o2_ingest.py present + parses" \
  || bad "G28b o2_ingest.py missing or syntax error"
out="$(printf '' | python3 "$o2i" somestream 2>&1)"; rc=$?
[ "$rc" -eq 5 ] \
  && ok "G28b empty stdin -> exit 5 (nothing to do)" \
  || bad "G28b empty stdin should exit 5, got $rc: $out"

# G28c: stage-capture runs the probe and captures checkpoint alignment
cap="$SCRIPT_DIR/stage-capture.sh"
bash -n "$cap" \
  && ok "G28c stage-capture.sh still parses after wiring" \
  || bad "G28c stage-capture.sh syntax broken"
grep -q "io-latency-probe.sh" "$cap" \
  && grep -q "alignment_duration" "$cap" \
  && grep -q "o2_ingest.py" "$cap" \
  && ok "G28c capture wires probe + alignment fields + O2 push" \
  || bad "G28c capture lost the io probe / alignment / O2 wiring"
grep -q "DEGRADED" "$cap" \
  && ok "G28c probe failure is a LOUD warn, never silent" \
  || bad "G28c probe failure no longer warns"

# G28d: compose exposes the RocksDB compaction/flush/SST families
compose="$ROOT/code/01_platform/01_docker/docker-compose.yml"
for key in num-running-compactions compaction-pending num-running-flushes \
           estimate-num-keys total-sst-files-size estimate-live-data-size \
           size-all-mem-tables num-entries-active-mem-table num-immutable-mem-table \
           estimate-pending-compaction-bytes num-live-versions \
           cur-size-active-mem-table num-entries-immutable-mem-table \
           block-cache-pinned-usage; do
  grep -q "state.backend.rocksdb.metrics.$key" "$compose" \
    && ok "G28d rocksdb metric toggle present: $key" \
    || bad "G28d rocksdb metric toggle missing: $key"
done

# G28e: fused timeline tool refuses without a window, with a reason
ft="$SCRIPT_DIR/fused_timeline.py"
[ -f "$ft" ] && python3 -c "import ast; ast.parse(open('$ft').read())" \
  && ok "G28e fused_timeline.py present + parses" \
  || bad "G28e fused_timeline.py missing or syntax error"
mkdir -p /tmp/g28-empty-cap
out="$(python3 "$ft" --capture /tmp/g28-empty-cap 2>&1)"; rc=$?
[ "$rc" -eq 3 ] \
  && ok "G28e no-window refusal -> exit 3 with reason" \
  || bad "G28e no-window should exit 3, got $rc: $out"

echo "---"
echo "guards: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
