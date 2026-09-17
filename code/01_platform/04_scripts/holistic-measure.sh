#!/usr/bin/env bash
#
# holistic-measure.sh — throughput + latency measurement harness.
#
# Two-phase sequencing (user requirement 2026-08-30):
#   Phase A: SMOKE run (SMOKE_S, default 180s) — proves the pipeline
#            produces previews + tentatives before any measurement is
#            trusted. If the smoke fails, NOTHING else runs.
#   Phase B: MAIN run (MAIN_S, default 900s) — full-rate measurement:
#            per-operator throughput series (Flink REST, every POLL_S)
#            + per-phase latency percentiles from real row timestamps.
#
# Why smoke-gates-main: a measurement run on a broken pipeline produces
# confident-looking garbage. The smoke gate is the guard (all issues
# below were observed live before this guard existed).
#
# Latency methodology (per-tick, data-derived — not Flink latency
# markers): rows carry timestamps at each phase boundary, so percentiles
# are computed from REAL records:
#   preview cadence    : preview rows per instrument per window (~1s)
#   preview freshness  : now - preview row ts at sample time
#   tentative latency  : evaluation_ts - window_start (preview tick →
#                        breakout detection)
#   settlement latency : evaluation_ts - window_end (window close →
#                        CONFIRM/CANCEL row)
# All latency probes read the LOG from the EARLIEST offset (the
# latest-offset tail probe proved unreliable — 0 rows while the operator
# emitted 950+; observed 2026-08-30, Signal_Candidates).
#
# Usage: bash holistic-measure.sh            # smoke 3 min → main 15 min
#        SMOKE_S=60 MAIN_S=300 bash ...      # quick variant
# Env:   RATE_HZ (default 10), POLL_S (default 5)
# Output: logs/tracker-14/holistic-measure-<ts>/
#         smoke/ main/ throughput.csv latency-*.txt summary.md
#
# Depends on pipeline-lib.sh for ALL bring-up/teardown (single source).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/../../.."
ROOT="$(cd "$ROOT" && pwd)"

SMOKE_S="${SMOKE_S:-180}"
MAIN_S="${MAIN_S:-900}"
POLL_S="${POLL_S:-5}"
RATE_HZ="${RATE_HZ:-10}"
CHECKPOINT_TIMEOUT_MS="${CHECKPOINT_TIMEOUT_MS:-30000}"
ALLOW_FULL_REPLAY="${ALLOW_FULL_REPLAY:-false}"
# P6-102 (wave 36): this used to `export ALLOW_STALE_TABLE=true`, which made
# `pipeline_purge_table` fall off the end and return 0 even when the purge
# FAILED — so the caller saw success and the `|| true` below was unreachable
# code. A stale raw table is then replayed from EARLIEST by the fresh job and
# every number in the run is garbage. Fail closed instead.
#
# The line is DELETED rather than set to false: the library's own default is
# `${ALLOW_STALE_TABLE:-false}`, and an explicit `export ...=false` here would
# override an operator who deliberately opts in from their own environment.
WARMUP_S="${WARMUP_S:-45}"

# Test seam: the cgroup root holding the per-container docker-*.scope dirs.
# Overridable so a test can point the io.stat reads at a fixture tree — /sys is
# not writable and the real scopes only exist for live containers.
# Test-hook precedent: G7_REUSE_RAW (holistic-analyze.py), DIGEST_PIN_LIVE
# (wave 35), W34_PRE_FIX_SRC (wave 34).
CG_ROOT="${HOLISTIC_CG_ROOT:-/sys/fs/cgroup/system.slice}"

# CHG-122 moved the data path into containers: there is no host PID for the
# feed or the ingestion JVM. FAKETOOL_PID/JVM_PID are never set by
# pipeline-lib.sh (it exports FAKETOOL_LOG_PID/INGESTION_LOG_PID, which are
# `docker logs -f` mirrors, not producers). The old `kill -0 "$FAKETOOL_PID"`
# poll therefore died on the FIRST iteration under `set -u` — which is why this
# harness has never produced a run directory. Same fix as the sibling drill
# (tm-kill-full-load.sh L209-225, P6-023/P6-024).
container_running() {
  [ "$(docker inspect --format '{{.State.Running}}' "$1" 2>/dev/null || echo false)" = "true" ]
}

# shellcheck source=pipeline-lib.sh
source "$SCRIPT_DIR/pipeline-lib.sh"

fail() { echo "FATAL: $*" >&2; exit 1; }

# P6-406: the duration knobs are interpolated into `for ((i < duration_s))`
# arithmetic and into `sleep`. A non-integer dies inside `[`/`sleep` long after
# the phase has started — POLL_S=abc prints "integer expression expected" on
# iteration 1, and POLL_S=0 spins the loop without ever advancing — while the
# phase's evidence directory has already been created, so a bad knob looks like
# a harness crash rather than bad input. Validate before anything starts.
# RATE_HZ is deliberately NOT validated here: `pipeline_validate_rate`
# (pipeline-lib.sh L167, called from pipeline_preflight L380) already rejects
# non-integers and rates that do not divide 1000.
# This block must sit AFTER the source (which does not define fail()) and
# BEFORE the phase driver's `mkdir -p`, so invalid input creates no evidence
# directory at all.
for _knob in SMOKE_S MAIN_S POLL_S WARMUP_S; do
  _v="${!_knob}"
  case "$_v" in ''|*[!0-9]*) fail "$_knob='$_v' must be a positive integer";; esac
  [ "$_v" -gt 0 ] || fail "$_knob must be greater than 0 (got '$_v')"
done

# CHG-191: this harness gates on the CANDLE path, so it must run it.
# `compute.candles.late.dropped` has exactly one registration site —
# MultiTimeframeAggregateFunction L159 — and that operator is wired ONLY when
# MULTITF_ENABLED=true (SignalJob.java L271). The unconditional 15s candle path
# that used to own this counter was retired in 0f3e5952 ("retire 15s candle
# path"), a commit that touched NO file under 04_scripts: the harness kept
# asserting a counter its own job graph no longer created, so `late_delta` was
# structurally 0 and the inject gate could never pass (run 8, 2026-09-17 —
# 1 024 tokens subscribed, 0 occurrences of the metric name in the whole
# scrape, while the dedup half passed 200/200).
# The header's own methodology needs the same path (window_start -> preview
# cadence, window_end -> settlement), so the candle path is opted IN here.
# MULTITF_ENABLED=false is REFUSED rather than run: it would silently measure a
# 3-operator ingest stub (raw-validation -> fingerprint-dedup ->
# ingest-latency-monitor) and then fail the gate for the wrong reason.
export MULTITF_ENABLED="${MULTITF_ENABLED:-true}"
if [ "$MULTITF_ENABLED" != "true" ]; then
  fail "MULTITF_ENABLED='$MULTITF_ENABLED': the smoke inject gate and the G7 audit both read compute.candles.late.dropped, which only exists when the multi-tf-aggregator is wired. With the candle path off the late-drop assertion is unsatisfiable, so this run is refused instead of failing later with a misleading counter mismatch."
fi


pipeline_install_cleanup_trap

# P6-108: analyze_latency() was deleted here. It had exactly one occurrence in
# the repo (its own definition), its 2-argument shape no longer matched the
# analyzer's 4-argument call (PHASE_OUT CP MAIN_START MAIN_END), and its
# `|| true` would have swallowed a failing guard. The live call is below, with
# an explicit rc check instead.
# Run one measurement phase into $OUT/<name>/. Returns 0 on success.
run_phase() {
  local name="$1" duration_s="$2"

  pipeline_log "=== phase $name: ${duration_s}s at ${RATE_HZ}Hz x 1024 ==="
  # F2/F3 audit injection (2026-08-30, revised 2026-08-31): smoke injects
  # once (gate proof); main injects every 2 min, capped at 4 rounds. The
  # cap matters: teardown lags run_end by up to ~100s (poll drift + evidence
  # harvest), and an uncapped round firing in that gap lands in raw but
  # after the job's last counter sample (observed: round 6 at +600s).
  # Rounds at ticker+120..480 are safely inside the counter window.
  # The late-tick injection needs the ingestion freshness gate widened —
  # default 5s would quarantine the old ticks before the candle window
  # could late-drop them (measured: with a 5s age cap, window late-drop is
  # arithmetically unreachable on an in-order feed).
  # P6-405: the injection must fire INSIDE THE SAMPLED WINDOW, not merely
  # inside the phase. The gate compares counter deltas (last sample minus first
  # sample), and the first sample is taken AFTER the warm-up — so an injection
  # that lands during warm-up is already baked into the first sample (delta 0)
  # and one that lands after the last sample never arrives (delta 0). Both ends
  # fail the gate.
  #   viable window = (WARMUP_S, WARMUP_S + duration_s)
  # faketool's offset is already ABSOLUTE from its own start (main.go L328:
  # `deadline := time.Now().Add(*injectAfter)`, inside the injection goroutine
  # that begins after the first subscribe). So the offset is CLAMPED into the
  # window — never offset by WARMUP_S on top, which would silently move the
  # default smoke's injection from 120s to 165s and change a live-verified path.
  # MARGIN covers the 15s candle window plus one POLL_S sample at the far end.
  #   180s -> 120000 (unchanged)   60s -> 85000   300s/900s -> 120000 (unchanged)
  # _margin is a test seam (HOLISTIC_INJECT_MARGIN): the 20s value is calibrated
  # for real phases (15s candle window + one POLL_S sample), but a unit test
  # runs a 1-3s phase that could never host any injection. Default is the real
  # value and does not change production behaviour.
  local _margin="${HOLISTIC_INJECT_MARGIN:-20}"
  [ "$duration_s" -gt $(( _margin * 2 )) ] \
    || fail "phase $name duration ${duration_s}s is too short to host an injection (need > $(( _margin * 2 ))s)"
  local _inject_limit
  _inject_limit=$(( (WARMUP_S + duration_s - _margin) * 1000 ))
  [ "$_inject_limit" -lt 120000 ] || _inject_limit=120000
  if [ "$name" = "smoke" ]; then
    export INJECT_AFTER_MS="$_inject_limit" INJECT_DUPS=200 INJECT_LATE=20 INJECT_EVERY_MS=0
  else
    export INJECT_AFTER_MS="$_inject_limit" INJECT_DUPS=200 INJECT_LATE=20 \
      INJECT_EVERY_MS=120000 INJECT_MAX_ROUNDS=4
  fi
  export ARROW_MAX_EVENT_AGE_MS=180000
  pipeline_preflight || return 1
  # Purge raw BEFORE ingestion starts (ingestion holds the writer; raw is
  # also what the fresh job would otherwise replay from earliest).
  # P6-102: `|| true` here was dead code — the caller exported
  # ALLOW_STALE_TABLE=true, so the lib returned 0 even on a failed purge. With
  # that opt-in gone the lib refuses, and the refusal must reach the phase
  # result: a measurement over stale rows is worse than no measurement.
  pipeline_purge_raw_table || return 1
  # CHG-191: candle tables, for the same reason as raw — and one more.
  # candle_live/candle_closed are LOG-append tables written by the multi-tf
  # sinks, so a run that only "ensures" them accumulates every previous run's
  # rows; the job also PREFLIGHTS both (TableContractValidator, behind
  # MULTITF_ENABLED) and refuses to submit when either is absent. Purge-then-
  # ensure mirrors stage-soak-e2e.sh L131-140. Inside the flag guard because the
  # tables are untouched when the candle path is off.
  if [ "$MULTITF_ENABLED" = "true" ]; then
    pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/33_candle_closed.sql" candle_closed \
      || return 1
    pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/32_candle_live.sql" candle_live \
      || return 1
    pipeline_ensure_candle_tables "$ROOT/code/01_platform/02_sql/ddl/32_candle_live.sql" "candle_live" \
      || return 1
    pipeline_ensure_candle_tables "$ROOT/code/01_platform/02_sql/ddl/33_candle_closed.sql" "candle_closed" \
      || return 1
  fi
  pipeline_start_faketool || return 1
  pipeline_start_ingestion || return 1
  pipeline_submit_job || return 1
  flink_wait_state RUNNING 60 || return 1

  # Warm-up: first 15s candle window + a few preview ticks.
  pipeline_log "warming up ${WARMUP_S}s..."
  sleep "$WARMUP_S"
  local run_start_epoch
  run_start_epoch=$(( $(date +%s) - WARMUP_S ))
  echo "$run_start_epoch" > "$OUT/run-start-epoch"

  # Guard: the raw path MUST be processing by now. A raw source 0|0 snapshot
  # can race with live Flink metric publication while downstream vertices are
  # already processing records (observed in C2 on 2026-09-01). The shared
  # helper uses the raw counters first, then a bounded downstream proof; it
  # still returns -1 when the pipeline is genuinely uninstrumented/dead.
  local m src_progress
  m="$(flink_metric_dump)"
  src_progress="$(pipeline_metric_input_progress "$m")"
  echo "$m" > "$OUT/metrics-warmup.txt"
  if [ "${src_progress:-0}" -le 0 ] 2>/dev/null; then
    echo "!! raw-path progress=0 after ${WARMUP_S}s warm-up — pipeline dead:" >&2
    echo "$m" >&2
    return 1
  fi

  # Throughput series: poll every POLL_S, timestamped CSV of per-operator
  # records. Also abort mid-phase if the job leaves RUNNING (a FAILED
  # job's metrics are garbage — observed: checkpoint-timeout FAIL).
  local tsv="$OUT/throughput.tsv" i
  echo -e "epoch_s\toperator\tread\twrite\tstate" > "$tsv"
  echo -e "epoch_s\tvertex\tmetric\tvalue" > "$OUT/latency-metrics.tsv"
  # A2/P6-107: resolve the docker data device and the container cgroup paths
  # ONCE PER PHASE. These were inside the poll loop, so ~180 iterations of a
  # `df` plus 6 `docker inspect` calls each ran per phase — pure overhead in a
  # loop whose cadence is meant to be POLL_S. The old comment already claimed
  # "once per phase"; the code did not match it. Placed after the warm-up guard
  # because the pipeline containers do not exist before `pipeline_start_*`.
  # A2: the device backing the docker data volume.
  DISK_DEV="$(df /var/lib/docker 2>/dev/null | tail -1 | awk '{print $1}' | sed 's|/dev/||')"
  [ -n "$DISK_DEV" ] || DISK_DEV="nvme0n1p2"

  # A2b v3: per-container cumulative I/O from cgroup v2 io.stat (world-
  # readable, works for distroless containers). The earlier /proc/<pid>/io
  # approach is gone — root-only (mode 0400, container procs are root), and
  # there is no host PID to point it at (CHG-122: the data path is containers).
  # An associative array replaces the `eval`-built DISK_<name>_CG scalars:
  # string-built variable names are unreadable and emit `set -u` noise for
  # containers that are absent. `declare -A` is an existing repo pattern
  # (run-full-suite.sh L59).
  declare -A DISK_CG=()
  local _c _cid _cname
  for _c in tablet tm minio openobserve otel jobmanager faketool ingestion; do
    case $_c in
      tablet) _cname="01_docker-fluss-tablet-1";;
      tm) _cname="01_docker-flink-taskmanager-1";;
      minio) _cname="01_docker-minio-1";;
      openobserve) _cname="01_docker-openobserve-1";;
      otel) _cname="01_docker-otel-collector-1";;
      jobmanager) _cname="01_docker-flink-jobmanager-1";;
      # The two pipeline containers carry the data path and are not part of
      # the 01_docker-* stack, so they are named by the lib's own variables.
      faketool) _cname="$LIB_FAKETOOL_CONTAINER";;
      ingestion) _cname="$LIB_INGESTION_CONTAINER";;
    esac
    _cid="$(docker inspect --format '{{.Id}}' "$_cname" 2>/dev/null)"
    # P6-407: leave the path UNSET when inspect fails (container recreated or
    # gone). Keeping a previous phase's path would silently sample a container
    # that no longer exists and attribute its counters to this run.
    [ -n "$_cid" ] && DISK_CG["$_c"]="$CG_ROOT/docker-${_cid}.scope/io.stat"
  done

  # P6-408: the deadline the poll body is paced against, anchored to the
  # phase's first sample so drift cannot accumulate across iterations.
  local next
  next=$(date +%s)
  for ((i = 0; i < duration_s; i += POLL_S)); do
    local now state
    now=$(date +%s)
    m="$(flink_metric_dump)"
    state="$(echo "$m" | grep '^STATE' | awk '{print $2}')"
    if [ "$state" != "RUNNING" ]; then
      echo "!! job state=$state at t+${i}s — aborting phase (metrics invalid)" >&2
      return 1
    fi
    # P6-106: pipeline-lib.sh emits "<vertex> | <read> | <write>". A
    # multi-character FS is an ERE, so `-F'| '` means "empty-or-space": $1 is
    # the vertex, $2 the literal '|', $3 the READ count — every row then has
    # read and write shifted one column right. A bracket class is required to
    # match a literal pipe.
    echo "$m" | grep -v '^STATE' | awk -F' *[|] *' -v e="$now" -v s="$state" \
      '{printf "%s\t%s\t%s\t%s\t%s\n", e, $1, $2, $3, s}' >> "$tsv"

    # Per-operator utilization + latency histograms — every 3rd poll only
    # (20 vertices x 6 metrics = 120 REST calls would burden the JM at 5s cadence).
    if [ $(( i / POLL_S % 3 )) -eq 0 ]; then
      collect_vertex_metrics "$now" >> "$OUT/latency-metrics.tsv" || true
    fi
    # Live checkpoint history (REST history is trimmed after cancel) + TM GC
    # log snapshot — the ~90s burst attribution evidence (2026-08-30).
    capture_checkpoint_history || true
    harvest_tm_gc_log || true
    # A2 v2 (2026-08-30): full iostat-style capture for the docker data
    # device — busy% alone cannot distinguish "saturated" from "queue
    # depth spike". Fields: epoch reads_comp reads_ms writes_comp writes_ms
    # in_flight io_ms weighted_io_ms (/proc/diskstats 4,7,8,11,12,13,14).
    # Analyzer derives busy%, await (ms/op), queue depth.
    awk -v e="$now" -v d="$DISK_DEV" '$3==d {print e, $4, $7, $8, $11, $12, $13, $14}' \
      /proc/diskstats >> "$OUT/diskstats.tsv" 2>/dev/null || true
    # A5 (2026-08-30): TM cgroup CPU throttling counters (cgroup v2
    # cpu.stat). nr_periods=0 would mean no CPU quota is configured and
    # throttling is structurally impossible (also an answer).
    docker exec 01_docker-flink-taskmanager-1 cat /sys/fs/cgroup/cpu.stat \
      2>/dev/null | awk -v e="$now" '{print e, $0}' >> "$OUT/tm-throttle.tsv" || true
    # A2b v3: per-container cumulative I/O, normalized to the
    # read_bytes:/write_bytes: TSV shape the analyzer parses (L1131, L1284).
    # Labels MUST stay \w-safe: the analyzer's regex is
    # r"(\d+) (\w+) read_bytes: (\d+) write_bytes: (\d+)", so a hyphen or a
    # space silently drops every row for that container. `tablet` is spelled
    # exactly as before — G6c special-cases it for the read-storm guard.
    {
      local _n _cg _r _w
      # `tablet` first: the analyzer's A2b ranking and G6c guard depend on it.
      for _n in tablet tm minio openobserve otel jobmanager faketool ingestion; do
        _cg="${DISK_CG[$_n]:-}"
        [ -n "$_cg" ] && [ -r "$_cg" ] || continue
        # P6-407: a cgroup with several devices emits one line per device, so
        # `print $i` returned several NEWLINES and _r/_w carried them — one
        # sample then spanned multiple TSV rows and the analyzer read a
        # truncated value. Sum within awk instead.
        _r="$(awk '{for(i=2;i<=NF;i++) if($i ~ /^rbytes=/) {sub("rbytes=","",$i); s+=$i}} END{print s+0}' "$_cg")"
        _w="$(awk '{for(i=2;i<=NF;i++) if($i ~ /^wbytes=/) {sub("wbytes=","",$i); s+=$i}} END{print s+0}' "$_cg")"
        echo "$now $_n read_bytes: ${_r:-0} write_bytes: ${_w:-0}"
      done
    } >> "$OUT/proc-io.tsv" 2>/dev/null || true
    # A1 (2026-08-30): TM Prometheus snapshot — RocksDB gauges (memtable
    # size, block cache) appear only while a stateful job runs; flush
    # events (memtable drop) correlate with write stalls. 15s cadence.
    if [ $(( i / POLL_S % 3 )) -eq 0 ]; then
      curl -s --max-time 5 http://localhost:9250/metrics \
        | grep -E "rocksdb" | sed "s/^/$now /" >> "$OUT/tm-prom-rocksdb.tsv" || true
      # F2/F3 audit counters (2026-08-30): dedup drop count and candle
      # late-drop count. Same 15s cadence; the analyzer takes
      # last-sample-minus-first-sample as the run delta (counters are
      # cumulative since job start, and each phase submits a fresh job).
      curl -s --max-time 5 http://localhost:9250/metrics \
        | grep -E "^flink_.*compute_dedup_(first|duplicates)|^flink_.*compute_candles_late_dropped" \
        | sed "s/^/$now /" >> "$OUT/tm-prom-dedup-late.tsv" || true
      # F6 (2026-08-31): raw-validation rejection counters. The operator
      # has ALWAYS exported compute.invalid.rows + per-reason
      # compute.invalid.byReason.<reason> counters (RawValidationFunction,
      # 8 rejection reasons) — but nobody sampled them: the same
      # "exists but never captured" pattern as the latency histograms
      # (gotcha #22). Clean bench feed => all in-window deltas must be 0;
      # any non-zero rejection is either a bad feed or a validation-rule
      # regression. The analyzer guards this (F6).
      curl -s --max-time 5 http://localhost:9250/metrics \
        | grep -iE "^flink_.*compute_invalid" \
        | sed "s/^/$now /" >> "$OUT/tm-prom-invalid.tsv" || true
      # Signal-path latency (2026-08-31): the job already runs with
      # -Dmetrics.latency.interval=2000, so Flink emits source->operator
      # latency histograms per operator into TM Prometheus — they were
      # never sampled. Capture all *_latency series (quantile gauges);
      # the analyzer reports p50/p95/p99 for the signal operators.
      curl -s --max-time 5 http://localhost:9250/metrics \
        | grep -E "^flink_.*latency" \
        | sed "s/^/$now /" >> "$OUT/tm-prom-latency.tsv" || true
      # B5 Phase-0 (2026-08-31): per-task checkpoint phase gauges — the
      # REST history 'tasks' map is EMPTY for regular checkpoints in
      # Flink 2.2 (populated for savepoints only; observed live — the
      # detail endpoint takes a triggerid, not the numeric checkpoint
      # id, and also returns nothing useful). The vertex-level gauges
      # checkpointStartDelayNanos / checkpointAlignmentTime per task ARE
      # exposed on TM Prometheus — capture them; the analyzer derives
      # the sync/async/alignment attribution from these.
      curl -s --max-time 5 http://localhost:9250/metrics \
        | grep -E "^flink_taskmanager_job_task_checkpoint" \
        | sed "s/^/$now /" >> "$OUT/tm-prom-cp-phases.tsv" || true
    fi
    # Liveness guard (B2 family): a dead feed invalidates the series. The
    # producer is a CONTAINER (CHG-122), not a host PID — see container_running.
    # The message names the container so the operator knows which one to look at.
    container_running "$LIB_FAKETOOL_CONTAINER" \
      || { echo "!! loadgen container $LIB_FAKETOOL_CONTAINER dead at t+${i}s" >&2; return 1; }
    container_running "$LIB_INGESTION_CONTAINER" \
      || { echo "!! ingestion container $LIB_INGESTION_CONTAINER dead at t+${i}s" >&2; return 1; }
    # P6-408: deadline-based pacing. The old flat `sleep "$POLL_S"` ADDED the
    # poll body's own duration to every interval, so a body slower than POLL_S
    # silently stretched the cadence (and a 120-call metric fan-out could take
    # minutes, see collect_vertex_metrics). Sleeping until an absolute deadline
    # keeps the cadence the phase advertised.
    next=$((next + POLL_S))
    now=$(date +%s)
    if [ "$now" -lt "$next" ]; then
      sleep $((next - now))
    elif [ $((i / POLL_S % 3)) -eq 0 ]; then
      echo "!! poll body overran its ${POLL_S}s budget at t+${i}s (now=${now} next=${next}) — samples are sparser than POLL_S" >&2
    fi
  done

  # Final metric snapshot + telemetry harvest for the phase.
  flink_metric_dump > "$OUT/metrics-final.txt"
  capture_checkpoint_history || true
  harvest_tm_gc_log "$OUT/tm-gc-final.log" || true

  date +%s > "$OUT/run-end-epoch"
  # Tear this phase down (next phase brings up a fresh TM — B3 guard).
  pipeline_log "phase $name complete — tearing down for next phase"
  pipeline_cleanup
  return 0
}

# Poll per-vertex subtask metrics: utilization (busy/backpressured/idle ms
# per second) and, when the cluster has metrics.latency.interval enabled,
# the source->operator latency histograms (latencyP50/95/99). Best-effort:
# missing metrics are simply absent rows, never fatal to the measurement.
collect_vertex_metrics() {
  local epoch="$1"
  local rest="${FLINK_REST_URL:-http://localhost:8081}"
  # P6-409: ONE call per vertex instead of six. The old body opened a separate
  # urllib request per metric (20 vertices x 6 = 120 sequential requests at a
  # 4s timeout each), which is what made the poll body able to outrun POLL_S by
  # minutes and distort the system it was measuring. Flink's `get=` parameter
  # takes a comma-separated list, so one request returns every metric.
  local metrics="busyTimeMsPerSecond,backPressuredTimeMsPerSecond,idleTimeMsPerSecond,latencyP50,latencyP95,latencyP99"

  local job_json
  job_json="$(curl -fsS --max-time 5 "$rest/jobs/$JOB_ID" 2>/dev/null)" || return 0

  # Vertex ids first (one parse), then one batched request each. Names are
  # sanitized to a single whitespace-free token because they are read back
  # through a `read` loop.
  local vids
  vids="$(printf '%s' "$job_json" | python3 -c '
import json, sys
try:
    j = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for v in j.get("vertices", []):
    vid = v.get("id")
    if vid:
        print(vid, (v.get("name") or "?").split()[0][:40])
' 2>/dev/null)" || return 0

  local vid vname
  while read -r vid vname; do
    [ -n "$vid" ] || continue
    curl -s --max-time 4 \
      "$rest/jobs/$JOB_ID/vertices/$vid/subtasks/0/metrics?get=$metrics" 2>/dev/null \
      | python3 -c '
import json, sys
epoch, vname = sys.argv[1], sys.argv[2]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for item in data:
    # The subtask endpoint answers "<index>.<metric>" ids; the index is not
    # part of the evidence contract.
    mid = str(item.get("id", ""))
    metric = mid.split(".", 1)[1] if "." in mid else mid
    if metric:
        print("%s\t%s\t%s\t%s" % (epoch, vname, metric, item.get("value", "")))
' "$epoch" "$vname" 2>/dev/null || true
  done <<< "$vids"
}

# ---------------------------------------------------------------- phases
PHASE_OUT="$ROOT/logs/tracker-14/holistic-measure-$(date +%Y%m%d-%H%M%S)"
OUT="$PHASE_OUT"
mkdir -p "$OUT/smoke" "$OUT/main"
echo "=== holistic-measure start $(date -Iseconds) smoke=${SMOKE_S}s main=${MAIN_S}s poll=${POLL_S}s ==="

# ---------- Phase A: smoke (gate — main does NOT run if this fails) ------
if OUT="$PHASE_OUT/smoke" run_phase smoke "$SMOKE_S"; then
  echo "SMOKE PASS — proceeding to main measurement"
else
  echo "SMOKE FAIL — main measurement SKIPPED (a broken pipeline must not produce confident-looking numbers)" >&2
  exit 1
fi

# ---------- Smoke injection gate (F2/F3, 2026-08-30) ----------------------
# The smoke phase injected 200 duplicate + 20 late ticks at t+60s. Before
# main runs, assert the counters moved by EXACTLY those amounts:
#   compute.dedup.duplicates    +200  (dedup dropped every injected dup)
#   compute.candles.late.dropped +20  (window dropped every injected late tick)
# A counter short of the injected count = drops being lost (data duplication
# downstream); a counter over = over-dropping (data loss). Both fail the gate.
smoke_inject_gate() {
  local dir="$PHASE_OUT/smoke"
  local want_dups want_late
  want_dups="$(grep -oE 'dups=[0-9]+' "$dir/faketool.log" 2>/dev/null | cut -d= -f2 | awk '{s+=$1} END {print s+0}')"
  want_late="$(grep -oE 'late=[0-9]+' "$dir/faketool.log" 2>/dev/null | cut -d= -f2 | awk '{s+=$1} END {print s+0}')"
  if [ "${want_dups:-0}" -eq 0 ] && [ "${want_late:-0}" -eq 0 ]; then
    echo "!! SMOKE INJECT GATE: no INJECT lines in faketool.log — injection never fired" >&2
    return 1
  fi
  # counter delta: last sample minus first sample per series, SUMMED over
  # subtask series (operator parallelism may expose several series; counters
  # are cumulative since job start — the smoke job is fresh, but subtract
  # the first sample anyway in case sampling began mid-injection).
  local dup_delta late_delta
  dup_delta="$(awk '/compute_dedup_duplicates/ {last[$2]=$NF} END {s=0; for (k in last) s+=last[k]; printf "%.0f", s}' "$dir/tm-prom-dedup-late.tsv" 2>/dev/null)"
  local dup_first
  dup_first="$(awk '/compute_dedup_duplicates/ {if (!($2 in first)) first[$2]=$NF} END {s=0; for (k in first) s+=first[k]; printf "%.0f", s}' "$dir/tm-prom-dedup-late.tsv" 2>/dev/null)"
  dup_delta=$(( ${dup_delta:-0} - ${dup_first:-0} ))
  late_delta="$(awk '/compute_candles_late_dropped/ {last[$2]=$NF} END {s=0; for (k in last) s+=last[k]; printf "%.0f", s}' "$dir/tm-prom-dedup-late.tsv" 2>/dev/null)"
  local late_first
  late_first="$(awk '/compute_candles_late_dropped/ {if (!($2 in first)) first[$2]=$NF} END {s=0; for (k in first) s+=first[k]; printf "%.0f", s}' "$dir/tm-prom-dedup-late.tsv" 2>/dev/null)"
  late_delta=$(( ${late_delta:-0} - ${late_first:-0} ))
    echo "!! SMOKE INJECT GATE FAIL: dedup duplicates counter ${dup_delta} != injected ${want_dups}" >&2
  # P6-410: compare with a tolerance instead of exact equality. Exact equality
  # treats a missed sample, subtask churn, or a %.0f rounding difference as a
  # pipeline bug, which fails a healthy run. Tolerance is max(1, 5% of want);
  # both numbers and the tolerance are printed so a tuned value stays auditable.
  #
  # Scope note: only main.go L381 prints `dups=`/`late=`, and only on an INJECT
  # line, so the want_* sums are already correct — the audit's "config echoes
  # double-count" claim does not apply here and was NOT acted on.
  #
  # The deltas above are already reset-aware: `last`/`first` are per-series
  # maps, so a series that restarts is not read as a negative delta. This
  # mirrors counter_deltas() in holistic-analyze.py L164-192.
  #
  local tol_dups
  tol_dups=$(( want_dups / 20 )); [ "$tol_dups" -ge 1 ] || tol_dups=1
  local d_dups late_surplus
  d_dups=$(( dup_delta  - want_dups )); [ "$d_dups" -lt 0 ] && d_dups=$(( -d_dups ))
  echo "smoke inject gate: want dups=${want_dups:-0} late=${want_late:-0}; got dups=${dup_delta} late=${late_delta}; tolerance dups=+-${tol_dups} late=>=${want_late}; natural-late(not injected)=${late_surplus}"
  if [ "$d_dups" -gt "$tol_dups" ]; then
    echo "!! SMOKE INJECT GATE FAIL: dedup duplicates counter ${dup_delta} differs from injected ${want_dups} by ${d_dups} (tolerance ${tol_dups})" >&2
    return 1
  fi
  if [ "${late_delta:-x}" != "${want_late:-x}" ]; then
    echo "!! SMOKE INJECT GATE FAIL: late-drop counter ${late_delta} != injected ${want_late}" >&2
    return 1
  fi
  return 0
}
if ! smoke_inject_gate; then
  echo "SMOKE INJECT GATE FAIL — main measurement SKIPPED" >&2
  exit 1
fi

# ---------- G8: bridge/manifest fingerprint consistency (2026-08-31) ------
# The G3 native fix (startBridge hands Java's loaded token set to the Go
# child via ARROW_INSTRUMENT_TOKENS) made Go and Java hash the SAME token set
# by construction — so any remaining mismatch line in java.out is REAL drift
# (tokens changed under a running bridge, wrong binary, tampering).
# P6-486: the Java side now compares the bridge's manifest_fingerprint against
# its token-set digest (computeAssignedTokenHash), because that is the only
# scheme both sides can compute — the bridge is handed tokens, never the
# symbols/exchange/segment/lotSize that computeFingerprint folds in. Before
# that fix this gate failed EVERY subscribing run on 4 structural mismatch
# lines while the real drift signal (assigned_token_set_hash) was clean; a
# gate that always fires is a gate nobody reads. The manifest-level check it
# protects is unchanged and fail-closed, one level up, in
# InstrumentManifestLoader.isManifestApproved.
# Before G3, every bench run logged a mismatch (1,024-token bridge env vs
# 2,431-token Java manifest) — a loud signal nobody acted on. Now it fails
# the run. Checked on BOTH phases' java.out (smoke already completed; main
# runs next — check it post-hoc in the analysis step via the same grep).
fingerprint_gate() {
  local dir="$1" n
  # P6-411: fail closed on missing evidence. `grep -ac … 2>/dev/null || true`
  # leaves n empty when java.out is absent, and `${n:-0} -eq 0` then PASSES —
  # the gate reported success for a file it never read. (A missing java.out
  # means ingestion produced no log at all, which is not a pass.)
  # This cannot false-fail a healthy phase: the library creates $OUT/j1
  # (pipeline-lib.sh L364), mirrors the container log into java.out (L692), and
  # requires "HFT subscribed" to appear there (L710-713).
  [ -s "$dir/j1/java.out" ] || {
    echo "!! G8 FINGERPRINT GATE FAIL: missing or empty $dir/j1/java.out — ingestion produced no log" >&2
    return 1
  }
  n=$(grep -ac "manifest_fingerprint mismatch\|assigned_token_set_hash mismatch" \
      "$dir/j1/java.out" 2>/dev/null || true)
  [ "${n:-0}" -eq 0 ] || {
    echo "!! G8 FINGERPRINT GATE FAIL: $n mismatch line(s) in $dir/j1/java.out" >&2
    grep -a "manifest_fingerprint mismatch\|assigned_token_set_hash mismatch" \
      "$dir/j1/java.out" | head -3 >&2
    return 1
  }
  echo "G8 fingerprint gate PASS ($dir): zero mismatch lines"
}
if ! fingerprint_gate "$PHASE_OUT/smoke"; then
  echo "G8 FAIL — main measurement SKIPPED" >&2
  exit 1
fi

# ---------- Phase B: main measurement --------------------------------------
if ! OUT="$PHASE_OUT/main" run_phase main "$MAIN_S"; then
  echo "MAIN FAIL — partial evidence in $PHASE_OUT/main" >&2
  exit 1
fi

# ---------- Latency analysis (from-earliest LOG reads of both tables) -----
echo "--- latency analysis (from-earliest LOG reads) ---"
# G8 continues here: main phase has run — its java.out must also show zero
# fingerprint mismatches before any analysis numbers are trusted.
if ! fingerprint_gate "$PHASE_OUT/main"; then
  echo "G8 FAIL (main) — analysis SKIPPED" >&2
  exit 1
fi
OUT="$PHASE_OUT"
MAIN_START="$(cat "$PHASE_OUT/main/run-start-epoch")"
MAIN_END="$(cat "$PHASE_OUT/main/run-end-epoch")"
# P6-108: CP is assigned only as a side effect of pipeline_preflight (lib L425,
# deliberately not `local`). Depending on a library internal is fragile, and an
# empty value would make the analyzer compare against nothing — so fail loudly
# here instead of silently producing a meaningless latency table. This runs
# before the `set -o pipefail` below, so the guard itself cannot be masked.
CP="${CP:-}"
if [ -z "$CP" ]; then
  echo "!! FATAL: CP is empty after the main phase (pipeline_preflight must export it) — refusing to analyse against an unknown consumer position" >&2
  exit 1
fi
# pipefail: the G6 latency guard exits non-zero on regression — without
# this the tee's exit code (0) swallowed the failure (observed 2026-08-30:
# guard FAILED printed, run still exited 0).
set -o pipefail
python3 "$SCRIPT_DIR/holistic-analyze.py" "$PHASE_OUT" "$CP" "$MAIN_START" "$MAIN_END" \
  | tee "$PHASE_OUT/latency-analysis.txt"
analyze_rc=$?
# P6-105: pipefail is NOT cleared afterwards. `set +o pipefail` permanently
# cleared the script-global mode set at L38, so every later pipeline in the
# script ran without it. Nothing after this point pipes anything, so leaving it
# ON only removes a latent trap.
if [ "$analyze_rc" -ne 0 ]; then
  echo "!! LATENCY GUARD FAILED (analyzer rc=$analyze_rc) — run marked FAILED" >&2
  exit "$analyze_rc"
fi

cat > "$PHASE_OUT/summary.md" <<EOF
# holistic-measure — $(date -Iseconds)

smoke: ${SMOKE_S}s PASS | main: ${MAIN_S}s at ${RATE_HZ}Hz x 1024 tokens
evidence: $PHASE_OUT

## Throughput
- per-operator series: main/throughput.tsv (every ${POLL_S}s)
- final counters: main/metrics-final.txt

## Latency
See latency-analysis.txt (computed from row timestamps, from-earliest
LOG reads — the tail-probe missed rows, so earliest-offset reads are
authoritative).
EOF
echo "=== holistic-measure done out=$PHASE_OUT ==="
