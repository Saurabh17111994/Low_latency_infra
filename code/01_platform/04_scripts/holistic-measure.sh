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
WARMUP_S="${WARMUP_S:-45}"

# shellcheck source=pipeline-lib.sh
source "$SCRIPT_DIR/pipeline-lib.sh"

fail() { echo "FATAL: $*" >&2; exit 1; }
pipeline_install_cleanup_trap

# Latency analysis: reads preview + candidates rows (from EARLIEST — see
# header) and computes the percentiles per phase. Emits a markdown table
# section on stdout.
analyze_latency() {
  local dir="$1" cp="$2"
  python3 "$SCRIPT_DIR/holistic-analyze.py" "$dir" "$cp" 2>&1 || true
}

# Run one measurement phase into $OUT/<name>/. Returns 0 on success.
run_phase() {
  local name="$1" duration_s="$2"

  pipeline_log "=== phase $name: ${duration_s}s at ${RATE_HZ}Hz x 1024 ==="
  pipeline_preflight || return 1
  pipeline_start_faketool || return 1
  pipeline_start_ingestion || return 1
  pipeline_purge_preview_table || true
  pipeline_submit_job || return 1
  flink_wait_state RUNNING 60 || return 1

  # Warm-up: first 15s candle window + a few preview ticks.
  pipeline_log "warming up ${WARMUP_S}s..."
  sleep "$WARMUP_S"
  local run_start_epoch
  run_start_epoch=$(( $(date +%s) - WARMUP_S ))
  echo "$run_start_epoch" > "$OUT/run-start-epoch"

  # Guard: the source MUST be reading by now. A source read=0 after
  # warm-up means the pipeline is dead (observed: full-replay segment
  # downloads; missing remote-data mount) — abort, do not measure.
  local m src_read
  m="$(flink_metric_dump)"
  src_read="$(echo "$m" | grep 'raw-table-1' | awk -F'| ' '{print $2}')"
  echo "$m" > "$OUT/metrics-warmup.txt"
  if [ "${src_read:-0}" -le 0 ] 2>/dev/null; then
    echo "!! source read=0 after ${WARMUP_S}s warm-up — pipeline dead:" >&2
    echo "$m" >&2
    return 1
  fi

  # Throughput series: poll every POLL_S, timestamped CSV of per-operator
  # records. Also abort mid-phase if the job leaves RUNNING (a FAILED
  # job's metrics are garbage — observed: checkpoint-timeout FAIL).
  local tsv="$OUT/throughput.tsv" i
  echo -e "epoch_s\toperator\tread\twrite\tstate" > "$tsv"
  echo -e "epoch_s\tvertex\tmetric\tvalue" > "$OUT/latency-metrics.tsv"
  for ((i = 0; i < duration_s; i += POLL_S)); do
    local now state
    now=$(date +%s)
    m="$(flink_metric_dump)"
    state="$(echo "$m" | grep '^STATE' | awk '{print $2}')"
    if [ "$state" != "RUNNING" ]; then
      echo "!! job state=$state at t+${i}s — aborting phase (metrics invalid)" >&2
      return 1
    fi
    echo "$m" | grep -v '^STATE' | awk -F'| ' -v e="$now" -v s="$state" \
      '{printf "%s\t%s\t%s\t%s\t%s\n", e, $1, $2, $3, s}' >> "$tsv"
    # A2: resolve the device backing the docker data volume once per phase.
  DISK_DEV="$(df /var/lib/docker 2>/dev/null | tail -1 | awk '{print $1}' | sed 's|/dev/||')"
  [ -n "$DISK_DEV" ] || DISK_DEV="nvme0n1p2"

  # A2b v3: resolve container IDs once per phase. Per-process I/O is read
  # from cgroup v2 io.stat (world-readable, works for distroless containers).
  # v2 tried host-side /proc/<pid>/io — root-only (mode 0400, container
  # procs run as root) so every container row silently came back empty.
  local _c
  for _c in tablet tm minio openobserve otel jobmanager; do
    local _cid
    _cid="$(docker inspect --format '{{.Id}}' "01_docker-$(case $_c in
      tablet) echo fluss-tablet-1;; tm) echo flink-taskmanager-1;;
      minio) echo minio-1;; openobserve) echo openobserve-1;;
      otel) echo otel-collector-1;; jobmanager) echo flink-jobmanager-1;; esac)" 2>/dev/null)"
    [ -n "$_cid" ] && eval "DISK_${_c}_CG=/sys/fs/cgroup/system.slice/docker-${_cid}.scope/io.stat"
  done

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
    # A2b v2 (2026-08-30): per-process cumulative I/O via HOST-side
    # /proc/<pid>/io using each container's host PID (docker inspect).
    # v1 used `docker exec ... sh` which silently fails for distroless
    # containers (minio, openobserve have no shell) — two of the top
    # writer candidates were never sampled. Host-side read needs no exec.
    # NOTE: DISK_*_PID are resolved once per phase (host PIDs are stable).
    {
      # cgroup v2 io.stat line: "<maj:min> rbytes=N wbytes=N rios=N wios=N"
      # → normalized to the read_bytes:/write_bytes: TSV format the analyzer
      # already parses. Block-level accounting (excludes page-cache hits) —
      # exactly the disk-load signal A2b wants.
      local _n _cg _r _w
      for _n in tablet tm minio openobserve otel jobmanager; do
        _cg="$(eval echo \$DISK_${_n}_CG)"
        if [ -n "$_cg" ] && [ -r "$_cg" ]; then
          _r="$(awk '{for(i=2;i<=NF;i++) if($i ~ /^rbytes=/) {sub("rbytes=","",$i); print $i}}' "$_cg")"
          _w="$(awk '{for(i=2;i<=NF;i++) if($i ~ /^wbytes=/) {sub("wbytes=","",$i); print $i}}' "$_cg")"
          echo "$now $_n read_bytes: ${_r:-0} write_bytes: ${_w:-0}"
        fi
      done
      echo "$now jvm $(grep -E '^(read|write)_bytes' /proc/$JVM_PID/io 2>/dev/null | tr '\n' ' ')"
      local BPID
      BPID=$(pgrep -f "arrow-bridge" | head -1)
      [ -n "$BPID" ] && echo "$now bridge $(grep -E '^(read|write)_bytes' /proc/$BPID/io 2>/dev/null | tr '\n' ' ')"
    } >> "$OUT/proc-io.tsv" 2>/dev/null || true
    # A1 (2026-08-30): TM Prometheus snapshot — RocksDB gauges (memtable
    # size, block cache) appear only while a stateful job runs; flush
    # events (memtable drop) correlate with write stalls. 15s cadence.
    if [ $(( i / POLL_S % 3 )) -eq 0 ]; then
      curl -s --max-time 5 http://localhost:9250/metrics \
        | grep -E "rocksdb" | sed "s/^/$now /" >> "$OUT/tm-prom-rocksdb.tsv" || true
    fi
    # Liveness guard (B2 family): a dead feed invalidates the series.
    kill -0 "$FAKETOOL_PID" 2>/dev/null || { echo "!! faketool dead at t+${i}s" >&2; return 1; }
    kill -0 "$JVM_PID" 2>/dev/null || { echo "!! ingestion JVM dead at t+${i}s" >&2; return 1; }
    sleep "$POLL_S"
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
  curl -s --max-time 5 "http://localhost:8081/jobs/$JOB_ID" \
    | python3 -c "
import json, sys, urllib.request
try:
    j = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for v in j.get('vertices', []):
    vid = v.get('id')
    if not vid:
        continue
    for metric in ('busyTimeMsPerSecond','backPressuredTimeMsPerSecond',
                   'idleTimeMsPerSecond','latencyP50','latencyP95','latencyP99'):
        try:
            r = urllib.request.urlopen(
                'http://localhost:8081/jobs/%s/vertices/%s/subtasks/0/metrics?get=%s'
                % ('$JOB_ID', vid, metric), timeout=4)
            data = json.load(r)
            for item in data:
                print('%s\t%s\t%s\t%s' % ('$epoch', v.get('name','?')[:40], metric, item.get('value','')))
        except Exception:
            pass
" 2>/dev/null
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

# ---------- Phase B: main measurement --------------------------------------
if ! OUT="$PHASE_OUT/main" run_phase main "$MAIN_S"; then
  echo "MAIN FAIL — partial evidence in $PHASE_OUT/main" >&2
  exit 1
fi

# ---------- Latency analysis (from-earliest LOG reads of both tables) -----
echo "--- latency analysis (from-earliest LOG reads) ---"
OUT="$PHASE_OUT"
MAIN_START="$(cat "$PHASE_OUT/main/run-start-epoch")"
MAIN_END="$(cat "$PHASE_OUT/main/run-end-epoch")"
# pipefail: the G6 latency guard exits non-zero on regression — without
# this the tee's exit code (0) swallowed the failure (observed 2026-08-30:
# guard FAILED printed, run still exited 0).
set -o pipefail
python3 "$SCRIPT_DIR/holistic-analyze.py" "$PHASE_OUT" "$CP" "$MAIN_START" "$MAIN_END" \
  | tee "$PHASE_OUT/latency-analysis.txt"
analyze_rc=$?
set +o pipefail
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
