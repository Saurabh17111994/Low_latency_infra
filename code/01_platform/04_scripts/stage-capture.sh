#!/usr/bin/env bash
# =============================================================================
# stage-capture.sh — aligned per-stage latency/throughput capture (plan:
# docs/plans/2026-09-01-stage-throughput-latency-detection-plan.md, Stage A).
#
# Samples, every CAPTURE_INTERVAL_S (default 5), ALL stages of the RUNNING
# SignalJob on one timeline:
#   stages.tsv          per-operator numRecordsIn/Out sums (cross-subtask),
#                       busy/backpressured/idle ms-per-second sums
#   watermark-lag.tsv   per-source-subtask currentWatermark + max event time
#   latency.tsv         per-operator latency-histogram percentiles (TM :9249)
#   flink-checkpoints.jsonl  every checkpoint: id/status/e2e/size
#   run-meta.txt        env + job id + vertex map (frozen at start)
#
# Ingestion/Fluss-side files (ingestion.tsv, fluss-side.tsv per the plan) are
# filled by the caller-provided log paths when set (INGESTION_JAVA_OUT,
# optional) — stage A keeps them as optional hooks so the harness runs with
# zero live dependencies beyond Flink REST + TM Prometheus.
#
# Measurement-only: reads REST/Prometheus; never writes to the data path.
# Fail-closed: exits non-zero if the job is not RUNNING at start or when it
# leaves RUNNING mid-capture (recorded as `job_state` transitions).
#
# Usage:
#   DURATION_S=900 bash stage-capture.sh            # 15 min capture
#   JOB_ID=<jid> DURATION_S=180 bash stage-capture.sh
#   INGESTION_JAVA_OUT=<j1/java.out> bash ...       # + ingestion.tsv (feed->ack)
#   FLUSS_PROBE_CP=<cp> PROBE_TOKENS=4,7 bash ...   # + read-lag.tsv + consumer-read.tsv
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
TM_PROM_URL="${TM_PROM_URL:-http://localhost:9250}"
JOB_ID="${JOB_ID:-}"
DURATION_S="${DURATION_S:-900}"
CAPTURE_INTERVAL_S="${CAPTURE_INTERVAL_S:-5}"
OUT_DIR="${OUT_DIR:-logs/tracker-14/stage-capture-$(date +%Y%m%d-%H%M%S)}"

# B2 hooks (2026-09-02): optional, env-gated. When set, each tick also samples
# the ingestion JVM's OTLP metrics payloads (java.out) into ingestion.tsv and
# runs the two passive Fluss probes (read-lag.tsv = log-end offsets; the Flink
# consumed side is offline from stages.tsv; consumer-read.tsv = KV preview
# lookups). All probes are read-only admin/KV calls (<= 1/s); see
# docs/plans/2026-09-01-stage-throughput-latency-detection-plan.md Stage B2.
# Enabled-but-broken FAILS FAST (see probe_* below) — a silent gap in the
# measurement timeline is worse than no capture.
INGESTION_JAVA_OUT="${INGESTION_JAVA_OUT:-}"
FLUSS_PROBE_CP="${FLUSS_PROBE_CP:-}"       # classpath for FlussReadLagProbe/FlussKvProbe
FLUSS_PROBE_DIR="${FLUSS_PROBE_DIR:-$SCRIPT_DIR/fluss-probes}"
PROBE_TABLE="${PROBE_TABLE:-feature_candles_15s_preview}"
PROBE_CLOSED_TABLE="${PROBE_CLOSED_TABLE:-feature_candles_15s}"
PROBE_TOKENS="${PROBE_TOKENS:-4,7,13,17,19}"
PROBE_BOOTSTRAP="${PROBE_BOOTSTRAP:-localhost:9123}"

mkdir -p "$OUT_DIR"

# ---------------------------------------------------------------------------
# Resolve the RUNNING SignalJob (fail closed if none / ambiguous).
# ---------------------------------------------------------------------------
if [ -z "$JOB_ID" ]; then
  running="$(curl -fsS --max-time 10 "$FLINK_REST_URL/jobs/overview" 2>/dev/null | \
    python3 -c '
import json, sys
try:
    jobs = json.load(sys.stdin)["jobs"]
except Exception:
    sys.exit(1)
running = [j for j in jobs if j.get("state") == "RUNNING"]
if len(running) == 1:
    print(running[0]["jid"])
elif not running:
    sys.exit(1)
else:
    # Prefer the signal job by name when several run.
    for j in running:
        if "signal" in (j.get("name") or "").lower():
            print(j["jid"])
            break
    else:
        print(running[0]["jid"])
' )" || { echo "!! no RUNNING job found at $FLINK_REST_URL"; exit 1; }
  [ -n "$running" ] || { echo "!! no RUNNING job found"; exit 1; }
  JOB_ID="$running"
fi

job_state() {
  curl -fsS --max-time 10 "$FLINK_REST_URL/jobs/$JOB_ID" 2>/dev/null | \
    python3 -c 'import json,sys; print(json.load(sys.stdin).get("state","?"))' 2>/dev/null || echo "UNREACHABLE"
}

STATE="$(job_state)"
[ "$STATE" = "RUNNING" ] || { echo "!! job $JOB_ID state=$STATE (need RUNNING)"; exit 1; }

# Mid-run leg-liveness check state (2026-09-04): after the first ~30s the
# ingestion OTLP payloads (10s flush cadence) and the per-tick Fluss probes
# MUST already have produced rows. A leg still header-only at t+30 is dead
# for the whole run — fail at t+30 with the direct reason instead of
# burning the remaining DURATION_S and discovering it post-run.
LEG_CHECKED_AT30=0
BRIDGE_CHECKED_AT65=0

echo "stage-capture: job=$JOB_ID duration=${DURATION_S}s interval=${CAPTURE_INTERVAL_S}s out=$OUT_DIR"

# ---------------------------------------------------------------------------
# Pre-run liveness gate (2026-09-04). The TM Prometheus endpoint and the
# ingestion java.out MUST already be serving before the timed capture starts
# — a leg that is dead at t=0 would silently produce an empty evidence file
# and only be noticed after the run. Observed: TM prom endpoint dead/empty
# while the job was RUNNING (TM restarting); ingestion java.out missing
# entirely. Fail NOW with the exact leg + reason, not after DURATION_S.
# ---------------------------------------------------------------------------
if ! curl -fsS --max-time 8 "$TM_PROM_URL/metrics" >/dev/null 2>&1; then
  echo "!! FAIL: TM prom endpoint $TM_PROM_URL/metrics not answering at capture start — the latency/custom legs would be blank. Check the TM is up and metrics.reporter.prom is enabled." >&2
  exit 1
fi
if [ -n "$INGESTION_JAVA_OUT" ] && [ ! -f "$INGESTION_JAVA_OUT" ]; then
  echo "!! FAIL: INGESTION_JAVA_OUT=$INGESTION_JAVA_OUT does not exist at capture start — ingestion.tsv (feed->ack leg) would be empty. Start the ingestion container(s) with logs at that path (and METRICS_LOCAL_LOG=1) first." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Freeze the vertex map (id -> name) once; fail closed if the shape is wrong.
# ---------------------------------------------------------------------------
curl -fsS --max-time 10 "$FLINK_REST_URL/jobs/$JOB_ID" | python3 -c '
import json, sys
job = json.load(sys.stdin)
rows = []
for v in job.get("vertices", []):
    rows.append((v["id"], v.get("name", "?")))
if not rows:
    sys.exit(1)
for vid, name in rows:
    print(f"{vid}\t{name}")
' > "$OUT_DIR/vertex-map.tsv" || { echo "!! failed to read vertices"; exit 1; }

vertex_ids="$(cut -f1 "$OUT_DIR/vertex-map.tsv" | tr '\n' ' ')"
# Mirror of vertex-map.tsv consumed by the per-tick REST custom-metric fetch.
cp "$OUT_DIR/vertex-map.tsv" "$OUT_DIR/.vertex-names.tsv"

{
  echo "started_epoch=$(date +%s)"
  echo "job_id=$JOB_ID"
  echo "flink_rest=$FLINK_REST_URL"
  echo "tm_prom=$TM_PROM_URL"
  echo "duration_s=$DURATION_S"
  echo "interval_s=$CAPTURE_INTERVAL_S"
  echo "rate_hz=${RATE_HZ:-unset}"
  echo "checkpoint_interval_ms=${CHECKPOINT_INTERVAL_MS:-unset}"
  echo "checkpoint_timeout_ms=${CHECKPOINT_TIMEOUT_MS:-unset}"
  echo "load_avg=$(cut -d' ' -f1-3 /proc/loadavg)"
  echo "ingestion_java_out=${INGESTION_JAVA_OUT:-unset}"
  echo "fluss_probe_cp=${FLUSS_PROBE_CP:+set}"       # never echo the cp path (huge)
  echo "probe_table=${PROBE_TABLE}"
  echo "probe_tokens=${PROBE_TOKENS}"
} > "$OUT_DIR/run-meta.txt"

echo -e "epoch\tvertex_id\toperator\tnumRecordsIn\tnumRecordsOut\tbusyMsSum\tbackpressuredMsSum\tidleMsSum" \
  > "$OUT_DIR/stages.tsv"
echo -e "epoch\tvertex_id\toperator\tsubtask\twatermark" > "$OUT_DIR/watermark-lag.tsv"
echo -e "epoch_ms\tvertex_id\toperator\tmetric\tsum" > "$OUT_DIR/custom-rest.tsv"

# Output-file declaration (2026-09-04): a hook that is env-enabled but whose
# leg never produced data is a silent measurement failure. Declare exactly
# the files that MUST receive rows for this run's hook configuration, so the
# post-run presence check fails loud instead of the scorecard reading a
# header-only TSV as "no data".
{
  echo "stages.tsv"
  # watermark-lag.tsv is NOT declared: this topology exposes no source
  # subtask currentWatermark to the scrape, so it legitimately stays
  # header-only (verified on the 2026-09-04 soak captures).
  [ -n "$INGESTION_JAVA_OUT" ] && echo "ingestion.tsv"
  [ -n "$FLUSS_PROBE_CP" ] && echo "read-lag.tsv"
  [ -n "$FLUSS_PROBE_CP" ] && echo "consumer-read.tsv"
  [ -n "$FLUSS_PROBE_CP" ] && echo "closed-read.tsv"
} > "$OUT_DIR/.expected-outputs"

# B2 output files (headers). Files exist (possibly empty) whenever the
# corresponding hook env is set; the parser keys on presence.
if [ -n "$INGESTION_JAVA_OUT" ]; then
  echo -e "epoch_ms\tmetric\tvalue" > "$OUT_DIR/ingestion.tsv"
fi
if [ -n "$FLUSS_PROBE_CP" ]; then
  echo -e "epoch_ms\ttable\tpartitions\tbuckets\tlog_end_sum" > "$OUT_DIR/read-lag.tsv"
  echo -e "epoch_ms\ttoken\twindow_start\toutput_ts\tlast_event_ts\tread_lag_ms" > "$OUT_DIR/consumer-read.tsv"
  echo -e "epoch_ms\ttoken\twindow_start\toutput_ts\tlast_event_ts\tread_lag_ms" > "$OUT_DIR/closed-read.tsv"
fi


# ---------------------------------------------------------------------------
# Main loop.
# ---------------------------------------------------------------------------
capture_stall_diagnostics() {
  # Stall diagnostics (2026-09-04 t+~90s stall hunt): when a mid-run guard
  # fires (TM prom endpoint dead, job left RUNNING), the run is about to end
  # and its failure evidence dies with the container teardown. Capture the
  # TM JVM thread state, the ingestion container states + java.out tails,
  # and the bridge lifecycle-event tail into $OUT_DIR/stall-diagnostics/
  # BEFORE exiting, so the next step is reading the dump, not re-running
  # blind.
  local reason="$1"
  local d="$OUT_DIR/stall-diagnostics"
  mkdir -p "$d"
  local now
  now="$(date +%Y%m%d-%H%M%S)"
  echo "$now $reason" > "$d/00-reason.txt"

  # TM JVM: thread dump via jcmd/jstack from inside the container (best
  # effort — the image may lack them), else record what IS reachable.
  {
    echo "=== TM container state ==="
    docker ps -a --filter name=flink-taskmanager --format '{{.Names}} {{.Status}} restarts={{.RestartCount}}'
    echo "=== TM job state (REST) ==="
    curl -fsS --max-time 5 "$FLINK_REST_URL/jobs/$JOB_ID" 2>/dev/null | python3 -c \
      'import json,sys; d=json.load(sys.stdin); print("state:", d.get("state"), "vertices:", [(v.get("name"), v.get("status")) for v in d.get("vertices",[])])' 2>/dev/null \
      || echo "(REST job query failed)"
  } > "$d/01-tm-state.txt" 2>&1

  local tm_ctr
  tm_ctr="$(docker ps -q --filter name=flink-taskmanager | head -1)"
  if [ -n "$tm_ctr" ]; then
    docker exec "$tm_ctr" sh -c \
      'pid=$(pgrep -f "org.apache.flink.runtime.taskexecutor.TaskManagerRunner" | head -1); \
       if [ -n "$pid" ] && command -v jcmd >/dev/null 2>&1; then jcmd "$pid" Thread.print; \
       elif [ -n "$pid" ] && [ -x "$JAVA_HOME/bin/jstack" ]; then "$JAVA_HOME/bin/jstack" "$pid"; \
       else echo "(no jcmd/jstack in TM image; pid=$pid)"; fi' \
      > "$d/02-tm-threaddump.txt" 2>&1 || true
  fi

  # Ingestion containers: state + log tail (bridge event lines kept, the
  # per-tick chunk noise filtered).
  local i ctr
  for i in 0 1 2; do
    ctr="e2e-ingestion-$i"
    if docker ps -a --filter "name=$ctr" --format '{{.Names}}' | grep -q "$ctr"; then
      docker ps -a --filter "name=$ctr" --format '{{.Status}} restarts={{.RestartCount}}' > "$d/03-ing-$i-state.txt"
      docker logs --tail 25 "$ctr" 2>&1 | grep -vE "chunk=[0-9]+/" | tail -15 \
        > "$d/03-ing-$i-javaout.txt" 2>&1 || true
    else
      echo "(container $ctr not present)" > "$d/03-ing-$i-state.txt"
    fi
  done

  echo "stall diagnostics captured in $d (reason: $reason)" >&2
}

# ---------------------------------------------------------------------------
# One sample tick: stages + watermarks (+ checkpoint events since last tick).
# ---------------------------------------------------------------------------
sample_tick() {
  local epoch; epoch="$(date +%s)"
  python3 - "$FLINK_REST_URL" "$JOB_ID" "$epoch" "$OUT_DIR" $vertex_ids <<'PYEOF'
import json
import sys
import urllib.request

rest, jid, epoch, out_dir = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
vertex_ids = sys.argv[5:]

def fetch(url, timeout=6):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)

stage_rows = []
wm_rows = []

for vid in vertex_ids:
    # counters
    try:
        items = fetch(f"{rest}/jobs/{jid}/vertices/{vid}/subtasks/metrics"
                      "?get=numRecordsIn,numRecordsOut")
        sums = {}
        for item in items:
            mid = str(item.get("id", ""))
            if mid in ("numRecordsIn", "numRecordsOut"):
                total = item.get("sum")
                if isinstance(total, (int, float)):
                    sums[mid] = int(total)
        rin = sums.get("numRecordsIn", "")
        rout = sums.get("numRecordsOut", "")
    except Exception:
        rin = rout = ""
    # time-share metrics (may answer empty on some vertices — recorded blank)
    busy = bpress = idle = ""
    try:
        items = fetch(f"{rest}/jobs/{jid}/vertices/{vid}/subtasks/metrics"
                      "?get=busyBackPressuredTimeMsPerSecond,backPressuredTimeMsPerSecond,idleTimeMsPerSecond")
        agg = {}
        for item in items:
            mid = str(item.get("id", ""))
            total = item.get("sum")
            if isinstance(total, (int, float)):
                agg[mid] = int(total)
        busy = agg.get("busyBackPressuredTimeMsPerSecond", "")
        bpress = agg.get("backPressuredTimeMsPerSecond", "")
        idle = agg.get("idleTimeMsPerSecond", "")
    except Exception:
        pass
    stage_rows.append((vid, rin, rout, busy, bpress, idle))

    # watermark per source subtask (only vertices that expose it)
    try:
        items = fetch(f"{rest}/jobs/{jid}/vertices/{vid}/subtasks/metrics"
                      "?get=currentWatermark")
        for item in items:
            mid = str(item.get("id", ""))
            val = item.get("value")
            if mid == "currentWatermark" and val not in (None, ""):
                wm_rows.append((vid, "all", str(val)))
            elif "." in mid and mid.endswith("currentWatermark"):
                wm_rows.append((vid, mid.split(".", 1)[0], str(val)))
    except Exception:
        pass

names = {}
try:
    with open(f"{out_dir}/vertex-map.tsv") as f:
        for line in f:
            v, name = line.rstrip("\n").split("\t", 1)
            names[v] = name
except Exception:
    pass

with open(f"{out_dir}/stages.tsv", "a") as f:
    for vid, rin, rout, busy, bpress, idle in stage_rows:
        name = names.get(vid, "?").replace("\t", " ")
        f.write(f"{epoch}\t{vid}\t{name}\t{rin}\t{rout}\t{busy}\t{bpress}\t{idle}\n")

with open(f"{out_dir}/watermark-lag.tsv", "a") as f:
    for vid, sub, wm in wm_rows:
        name = names.get(vid, "?").replace("\t", " ")
        f.write(f"{epoch}\t{vid}\t{name}\t{sub}\t{wm}\n")
PYEOF

# --- TM Prometheus scrape: busy/backpressured/idle + watermarks + latency
#     (SYSTEM families only — see below for custom operator metrics).
#     FAIL-FAST (2026-09-04): a 0-byte prom-*.txt (curl dead) is a silent
#     evidence gap — the scorecard's busy/backpressure/latency legs would
#     read as "no data" after the run (the old `|| true` swallowed TM
#     restarts). Fail LOUD at first sight instead.
  # REST answered empty aggregates for the time-share metrics on Flink 2.2.1
  # (live-observed 2026-09-01); TM :9249 exposes per-subtask gauges instead.
  # Scraped every tick into prom-<epoch>.txt; the parser merges later.
  # 2026-09-05 (corrected twice): the earlier widened filter kept every
  # metric whose name contains "_operator_", expecting custom operator
  # samples (compute.dedup.duplicates etc.). A 2026-09-04 analysis claimed
  # Flink 2.2.1 exports operator-scope metrics as HELP-only with ZERO sample
  # lines, so the filter was narrowed away from them and the custom leg was
  # moved to REST — but the REST leg then queried BARE names (see below) and
  # came back empty, and the false "HELP-only" belief hid the real cause.
  # Live probes (2026-09-04/05) prove operator-scope custom metrics DO carry
  # real sample lines on the TM prom endpoint. Keep the scrape wide enough
  # to keep them as a cross-check; the authoritative custom leg is
  # custom-rest.tsv (REST full-id query, fixed 2026-09-05).
  local prom_file
  prom_file="$OUT_DIR/prom-$(date +%s).txt"
  if ! curl -fsS --max-time 8 "$TM_PROM_URL/metrics" 2>/dev/null \
      | grep -E 'busyTimeMsPerSecond|backPressuredTimeMsPerSecond|hardBackPressuredTimeMsPerSecond|idleTimeMsPerSecond|currentWatermark|latency_source_id|flink_taskmanager_job_task_operator_' \
      > "$prom_file"; then
    echo "!! FAIL: TM prom scrape dead at $(date +%s) ($TM_PROM_URL/metrics empty/failed) — TM likely restarting/heartbeat-lost; the latency/custom legs would be silent. Check TM logs." >&2
    capture_stall_diagnostics "TM prom scrape dead at $(date +%s) — endpoint $TM_PROM_URL/metrics empty/failed"
    exit 2
  fi

  # B2 hooks: ingestion OTLP payloads (10s cadence — new rows every ~2 ticks)
  # and the two passive Fluss probes.
  sample_ingestion
  sample_probes

  # Custom operator counters (compute.dedup.duplicates, compute.candles.*,
  # compute.signals.*, ...), read from the Flink REST per-vertex metrics
  # endpoint. IMPORTANT (2026-09-05, root-caused): REST metric IDs are
  # FULLY-QUALIFIED: per-subtask listings carry
  # "<subtask>.<operator>.<metric_with_underscores>" (e.g.
  # "3.fingerprint-dedup.compute_dedup_first"); the AGGREGATE subtasks
  # endpoint serves "<operator>.<metric_with_underscores>" and dots in the
  # metric name become underscores. A bare-name get= (compute.dedup.first)
  # returns [] ALWAYS — the pre-2026-09-05 leg queried bare names and was
  # therefore empty on every soak while the pipeline exported the values all
  # along (the metrics were ALSO visible with real sample lines on the TM
  # prom endpoint — the earlier "HELP-only on Flink 2.2.1" claim in this
  # comment was WRONG, misattributed from those empty legs; live probes
  # 2026-09-04/05 show real sample lines). The fix: list
  # /vertices/{vid}/metrics once per vertex, derive "<operator>.<metric>"
  # pairs from the per-subtask full ids, then fetch aggregate values via
  # /subtasks/metrics?get=<operator>.<metric>.
  # Rows: epoch, vertex_id, operator, metric(dotted), sum.
  local custom_names
  custom_names="compute.dedup.first,compute.dedup.duplicates,compute.candles.emitted,compute.candles.late.updates,compute.candles.late.dropped,compute.candles.previews.emitted,compute.candles.previews.late.dropped,compute.candles.previews.restored_timer_noop,compute.candles.restored_timer_noop,compute.candles.duplicate_window,compute.candles.invalid.total,compute.candles.invalid.,compute.forming.bar.updates,compute.signal.passed,compute.signal.dropped.active_exists,compute.signal.cleared.closed,compute.signal.cleared.admin,compute.signal.active.count,compute.signals.detected,compute.signals.detected.forming,compute.signals.early.tentative,compute.signals.early.confirmed,compute.signals.early.confirmed_early,compute.signals.early.cancelled,compute.signals.early.marker_dropped,compute.invalid.rows,compute.invalid.byReason,compute.execution_intent.rejected,babysitter.positions.applied,babysitter.positions.conflict,babysitter.positions.duplicate,babysitter.positions.observed,babysitter.positions.stale,babysitter.positions.latest_observed_version,preview.trigger.previewFires,preview.trigger.eventTimeFires,rows.malformed,rows.skipped,transitions.applied"
  local epoch_ms
  epoch_ms="$(date +%s%3N)"
  local names_file="$OUT_DIR/.vertex-names.tsv"
  python3 - "$FLINK_REST_URL" "$JOB_ID" "$epoch_ms" "$custom_names" "$OUT_DIR" "$names_file" <<'PYEOF'
import json, sys, urllib.request
rest, jid, epoch_ms, names, out_dir, names_file = sys.argv[1:7]
# Dotted names we want -> the underscored last segment REST uses. Keep both
# so rows stay readable (dotted) while matching is done on the underscored
# form REST actually serves.
wanted = [n for n in names.split(",") if n]
wanted_underscore = {n: n.replace(".", "_") for n in wanted}
vertex_names = {}
try:
    with open(names_file) as f:
        for line in f:
            v, n = line.rstrip("\n").split("\t", 1)
            vertex_names[v] = n
except Exception:
    pass
def fetch(url, timeout=6):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)
rows = []
for vid in vertex_names:
    op = vertex_names[vid].split(" -> ", 1)[0].replace("\t", " ")
    try:
        # 1) List the ids THIS vertex actually serves (they are
        #    fully-qualified: "<subtask>.<operator>.<underscored_metric>").
        available = fetch(f"{rest}/jobs/{jid}/vertices/{vid}/metrics")
    except Exception:
        continue
    # 2) Collect "<operator>.<underscored>" query ids whose metric segment
    #    matches a wanted metric. The operator segment comes from the id
    #    itself (the vertex may be a chain — the OWNING operator is in the
    #    id, e.g. "3.fingerprint-dedup.compute_dedup_first").
    pairs: dict[str, str] = {}  # "<operator>.<underscored>" -> dotted name
    for m in available:
        mid = str(m.get("id", ""))
        parts = mid.split(".")
        if len(parts) < 3:
            continue
        op_seg = parts[1]
        last_seg = parts[-1]
        for dotted, underscored in wanted_underscore.items():
            if last_seg == underscored:
                pairs[f"{op_seg}.{underscored}"] = dotted
                break
    if not pairs:
        continue
    # 3) Fetch aggregate values for the "<operator>.<metric>" pairs (one
    #    comma-joined request on the aggregate subtasks endpoint).
    try:
        items = fetch(f"{rest}/jobs/{jid}/vertices/{vid}/subtasks/metrics?get="
                      + ",".join(pairs))
    except Exception:
        continue
    for item in items:
        qid = str(item.get("id", ""))
        dotted = pairs.get(qid)
        if not dotted:
            continue
        total = item.get("sum")
        if isinstance(total, (int, float)):
            rows.append(f"{epoch_ms}\t{vid}\t{op}\t{dotted}\t{int(total)}")
if rows:
    with open(f"{out_dir}/custom-rest.tsv", "a") as f:
        f.write("\n".join(rows) + "\n")
PYEOF
}

# ---------------------------------------------------------------------------
# B2 hooks (2026-09-02): ingestion.tsv + read-lag.tsv + consumer-read.tsv.
# All three sample functions are measurement-only and fail LOUD (a printed
# WARN once per hook — not silent, not per-tick spam) when enabled but broken.
# ---------------------------------------------------------------------------

# Tail java.out for OTLP payloads appended since the last tick. The ingestion
# JVM flushes one payload line every 10s (OtlpMetricsEmitter). Each payload is
# a single-line JSON with cumulative counters (tick.throughput, ...) and
# histograms (append.latency.ms with count/sum/p50/p90/p99, stage.*_latency).
# We flatten each metric to one TSV row: epoch_ms, metric, value. For
# histograms the "value" is the p50 (the count column carries the sample
# count via a separate metric "<name>.count" row).
INGESTION_OFFSET_FILE="$OUT_DIR/.ingestion-java.out.offset"

sample_ingestion() {
  [ -n "$INGESTION_JAVA_OUT" ] || return 0
  [ -f "$INGESTION_JAVA_OUT" ] || {
    # Fail-fast: enabled but the file never appeared. One loud warn (guarded
    # by the offset file's absence — it is created on first successful read).
    if [ ! -f "$INGESTION_OFFSET_FILE" ]; then
      echo "!! WARN: INGESTION_JAVA_OUT=$INGESTION_JAVA_OUT set but file missing — ingestion.tsv stays empty (feed->ack leg lost)" >&2
      touch "$INGESTION_OFFSET_FILE"
    fi
    return 0
  }

  local start_off=0
  [ -f "$INGESTION_OFFSET_FILE" ] && start_off="$(cat "$INGESTION_OFFSET_FILE")"
  local fsize; fsize="$(stat -c %s "$INGESTION_JAVA_OUT" 2>/dev/null || echo 0)"
  # File shrank (rotation) → restart from 0.
  if [ "$fsize" -lt "$start_off" ]; then start_off=0; fi

  # 2026-09-02 fix: the OTLP parse used to sit behind a heredoc
  # (python3 ... <<'PYEOF') while ALSO receiving the tail pipe on stdin — the
  # heredoc redirection WINS over the pipe, so python read the heredoc (its
  # own source) instead of the java.out tail and ingestion.tsv stayed empty
  # (observed in the 15:56 A/B capture: offset advanced, zero rows written).
  # Python now opens the file itself at the offset — no pipe, no heredoc
  # stdin conflict.
  python3 - "$INGESTION_JAVA_OUT" "$start_off" "$OUT_DIR" "$(date +%s%3N)" <<'PYEOF'
import json, os, re, sys
java_out, start_off, out_dir, epoch = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
rows = []
with open(java_out, "r", encoding="utf-8", errors="replace") as f:
    f.seek(start_off)
    for line in f:
        m = re.search(r"otlp-metrics-payload: (.*)", line)
        if not m:
            continue
        try:
            d = json.loads(m.group(1))
        except Exception:
            continue
        try:
            metrics = d["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        except Exception:
            continue
        for met in metrics:
            name = met.get("name", "?")
            # Sum (cumulative counter) or gauge
            for kind in ("sum", "gauge"):
                dp = met.get(kind, {}).get("dataPoints", [])
                if dp:
                    val = dp[0].get("asInt", dp[0].get("asDouble", ""))
                    rows.append(f"{epoch}\t{name}\t{val}")
                    break
            # Histogram: count + one row per quantile attr present
            # (p50/p90/p99 — the ingestion histogram summary carries these;
            # p95 does not exist server-side, only 50/90/99).
            hist = met.get("histogram", {}).get("dataPoints", [])
            if hist:
                h = hist[0]
                rows.append(f"{epoch}\t{name}.count\t{h.get('count','')}")
                for a in h.get("attributes", []):
                    if a.get("key") in ("p50", "p90", "p99"):
                        v = a.get("value", {})
                        q = v.get("intValue", v.get("doubleValue", ""))
                        rows.append(f"{epoch}\t{name}.{a['key']}\t{q}")
if rows:
    with open(f"{out_dir}/ingestion.tsv", "a") as f:
        f.write("\n".join(rows) + "\n")
PYEOF
  echo "$fsize" > "$INGESTION_OFFSET_FILE"
}

# Compile the two B2 probes once (fail fast if the classpath is broken).
FLUSS_PROBE_BIN="$OUT_DIR/probes"
if [ -n "$FLUSS_PROBE_CP" ]; then
  mkdir -p "$FLUSS_PROBE_BIN"
  for src in FlussReadLagProbe FlussKvProbe; do
    javac -cp "$FLUSS_PROBE_CP" -d "$FLUSS_PROBE_BIN" \
      "$FLUSS_PROBE_DIR/$src.java" > "$OUT_DIR/javac-$src.log" 2>&1 \
      || { echo "!! FAIL: B2 probe $src compile failed — see $OUT_DIR/javac-$src.log (FLUSS_PROBE_CP broken?)" >&2; exit 1; }
  done
  echo "B2 probes compiled -> $FLUSS_PROBE_BIN"
fi

sample_probes() {
  [ -n "$FLUSS_PROBE_CP" ] || return 0
  # Probe 1: raw log-end offsets (CP3 side). One Admin.listOffsets RPC.
  java -Dlog.dir=/tmp/fluss-probe-logs -cp "$FLUSS_PROBE_BIN:$FLUSS_PROBE_CP" \
    FlussReadLagProbe default raw_table_1 "$PROBE_BOOTSTRAP" \
    >> "$OUT_DIR/read-lag.tsv" 2>/dev/null || echo "!! WARN: FlussReadLagProbe failed this tick" >&2
  # Probe 2: KV preview lookups (CP9->CP10). 3 lookups, <=1/s aggregate.
  java -Dlog.dir=/tmp/fluss-probe-logs -cp "$FLUSS_PROBE_BIN:$FLUSS_PROBE_CP" \
    FlussKvProbe "$PROBE_TABLE" 15000 "$PROBE_TOKENS" "$PROBE_BOOTSTRAP" \
    >> "$OUT_DIR/consumer-read.tsv" 2>/dev/null || echo "!! WARN: FlussKvProbe failed this tick" >&2
  # Probe 3: CLOSED feature-candle table (CP9->CP10 for the closed leg).
  # Same lookups against feature_candles_15s; FlussKvProbe picks the closed
  # column layout (output_ts only, no last_event_ts).
  java -Dlog.dir=/tmp/fluss-probe-logs -cp "$FLUSS_PROBE_BIN:$FLUSS_PROBE_CP" \
    FlussKvProbe "$PROBE_CLOSED_TABLE" 15000 "$PROBE_TOKENS" "$PROBE_BOOTSTRAP" \
    >> "$OUT_DIR/closed-read.tsv" 2>/dev/null || echo "!! WARN: FlussKvProbe(closed) failed this tick" >&2
}

START=$(date +%s)
LAST_CP_COUNT=0

# Host per-op disk latency probe (2026-09-02 decline hunt): 2s-cadence
# iostat -x (r_await/w_await/aqu-sz/%util/iops) -> stages/io-latency.tsv
# (raw truth) + OpenObserve host_io_latency stream (single-pane policy).
# Probe fails LOUD but never blocks the capture (evidence files still land).
HERE_SC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IO_PROBE_LOG="$OUT_DIR/io-latency-probe.log"   # OUT_DIR IS the stages dir
IO_PROBE_DEVICE="${IO_PROBE_DEVICE:-nvme0n1}"
mkdir -p "$OUT_DIR"   # redirect target must exist BEFORE the fork
IO_PROBE_STAGES_DIR="$OUT_DIR" \
bash "$HERE_SC/io-latency-probe.sh" "$OUT_DIR" "$((DURATION_S + 30))" \
  "$IO_PROBE_DEVICE" >"$IO_PROBE_LOG" 2>&1 &
IO_PROBE_PID=$!
while :; do
  NOW=$(date +%s)
  ELAPSED=$((NOW - START))
  if [ "$ELAPSED" -ge "$DURATION_S" ]; then
    echo "stage-capture: duration reached (${DURATION_S}s)"
    break
  fi
  STATE="$(job_state)"
  if [ "$STATE" != "RUNNING" ]; then
    echo "!! job left RUNNING at t+${ELAPSED}s (state=$STATE) — failing closed"
    capture_stall_diagnostics "job left RUNNING at t+${ELAPSED}s (state=$STATE)"
    exit 2
  fi
  sample_tick || echo "warn: one sample tick failed (continuing)"

  # t+30 leg-liveness: header-only ingestion/probe legs after 3 flush
  # intervals are dead — fail with the direct reason instead of waiting.
  if [ "$LEG_CHECKED_AT30" -eq 0 ] && [ "$ELAPSED" -ge 30 ]; then
    LEG_CHECKED_AT30=1
    if [ -n "$INGESTION_JAVA_OUT" ] \
        && [ "$(($(wc -l < "$OUT_DIR/ingestion.tsv" 2>/dev/null || echo 1) - 1))" -le 0 ]; then
      echo "!! FAIL: ingestion.tsv still header-only at t+${ELAPSED}s — the ingestion OTLP payload leg is dead (METRICS_LOCAL_LOG not reaching the JVM, java.out not mirroring, or emitter flush broken). Check the ingestion container logs now." >&2
      exit 2
    fi
    if [ -n "$FLUSS_PROBE_CP" ]; then
      for f in read-lag consumer-read closed-read; do
        if [ "$(($(wc -l < "$OUT_DIR/$f.tsv" 2>/dev/null || echo 1) - 1))" -le 0 ]; then
          echo "!! FAIL: $f.tsv still header-only at t+${ELAPSED}s — the Fluss probe leg is dead (probe classpath/table/bootstrapping broken). Check FLUSS_PROBE_CP and the probe javac logs." >&2
          exit 2
        fi
      done
    fi
    if [ "$(($(wc -l < "$OUT_DIR/custom-rest.tsv" 2>/dev/null || echo 1) - 1))" -le 0 ]; then
      echo "!! FAIL: custom-rest.tsv still header-only at t+${ELAPSED}s — the REST custom-metric leg is dead. 2026-09-05 root cause: REST serves operator metrics as '<operator>.<underscored_metric>' (bare-name get= returns []); the fixed leg lists /vertices/{vid}/metrics and queries /subtasks/metrics?get=<operator>.<metric>. Empty here = the fix regressed, the job has no such operators, or REST stopped answering — check the capture log and the Flink REST endpoint now." >&2
      exit 2
    fi
    echo "stage-capture: legs alive at t+${ELAPSED}s (ingestion + probes + custom-rest have rows)"
  fi

  # Bridge-cadence guard (2026-09-04): the ingestion arrow-bridge writes a
  # tick-count report (stderr -> java.out) every ARROW_TICK_COUNTS=30s.
  # A bridge stall (030617: interval report stopped after t+33s, no bridge
  # event logged, Java otlp kept flushing) leaves java.out WITHOUT new
  # arrow-tick-counts lines while ingestion.tsv still grows — the t+30
  # leg check cannot see it. After t+65s the 2nd report (~t+63s) must have
  # arrived; fail with a direct reason + stall dump ~45s before the prom
  # death cascade.
  if [ "$BRIDGE_CHECKED_AT65" -eq 0 ] && [ "$ELAPSED" -ge 65 ]; then
    BRIDGE_CHECKED_AT65=1
    if [ -n "$INGESTION_JAVA_OUT" ] && [ -f "$INGESTION_JAVA_OUT" ]; then
      last_report="$(grep -E 'arrow-tick-counts: total' "$INGESTION_JAVA_OUT" | tail -1 | cut -c1-12)"
      if [ -z "$last_report" ]; then
        echo "!! FAIL: no arrow-tick-counts report line ever in $INGESTION_JAVA_OUT by t+${ELAPSED}s — bridge tick counter disabled or stderr not mirrored. Check ARROW_TICK_COUNTS and the ingestion container." >&2
        capture_stall_diagnostics "no arrow-tick-counts line ever in java.out by t+${ELAPSED}s"
        exit 2
      fi
      # last_report is a log timestamp (HH:MM:SS.mmm) — compare wall-clock.
      last_epoch="$(date -d "$(echo "$last_report" | sed 's/\..*//')" +%s 2>/dev/null)"
      if [ -n "$last_epoch" ] && [ $((NOW - last_epoch)) -gt 40 ]; then
        echo "!! FAIL: last arrow-tick-counts report $((NOW - last_epoch))s old at t+${ELAPSED}s (report at $last_report) — the arrow-bridge interval report stopped (bridge stall / feed stall); Java otlp may still flush. Dumping stall diagnostics now." >&2
        capture_stall_diagnostics "arrow-bridge tick-count report stalled $((NOW - last_epoch))s (last $last_report)"
        exit 2
      fi
    fi
  fi

  # checkpoint events (append any new ones)
  curl -fsS --max-time 10 "$FLINK_REST_URL/jobs/$JOB_ID/checkpoints" 2>/dev/null | \
    python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
hist = sorted(data.get("history", []), key=lambda c: c.get("id", 0))
print(json.dumps({"count": len(hist),
                  "events": [{k: c.get(k) for k in
                              ("id", "status", "end_to_end_duration", "state_size",
                               "trigger_timestamp",
                               # 2026-09-02 decline hunt: alignment + phase
                               # breakdown. alignment_* are the checkpoint
                               # ALIGNMENT evidence (was a capture gap).
                               "alignment_duration", "alignment_buffered",
                               "start_delay", "sync_dur", "async_dur",
                               "processed_data", "persisted_data",
                               "num_subtasks", "num_ack_subtasks")} for c in hist]}))
' >> "$OUT_DIR/flink-checkpoints.jsonl" 2>/dev/null || true

  sleep "$CAPTURE_INTERVAL_S"
done

# ---------------------------------------------------------------------------
# Post-run evidence verification (2026-09-04). Every declared output file
# must have data rows — a header-only or missing file means that leg was
# silently dead and the scorecard would misread "no data" as a real zero.
# Also fail if the last scrape was long ago (TM/heartbeat died near the end).
# Each message names the exact leg + reason so the next action is direct.
# ---------------------------------------------------------------------------
echo "stage-capture: verifying evidence completeness..."
missing=""
if [ -s "$OUT_DIR/.expected-outputs" ]; then
  while IFS= read -r f; do
    [ -f "$OUT_DIR/$f" ] || { missing="$missing $f(missing)"; continue; }
    # Data rows = lines beyond the header (all our TSVs are header + rows).
    rows="$(($(wc -l < "$OUT_DIR/$f") - 1))"
    [ "$rows" -gt 0 ] || missing="$missing $f(header-only)"
  done < "$OUT_DIR/.expected-outputs"
fi
if [ -n "$missing" ]; then
  echo "!! FAIL: evidence files with NO data rows:$missing — the corresponding scorecard leg is empty; fix that leg's capture, do not trust this run." >&2
  exit 2
fi
latest_prom="$(ls -1 "$OUT_DIR"/prom-*.txt 2>/dev/null | sort | tail -1)"
if [ -n "$latest_prom" ]; then
  now_s="$(date +%s)"
  last_scrape_s="$(basename "$latest_prom" .txt | sed 's/prom-//')"
  if [ $((now_s - last_scrape_s)) -gt 25 ]; then
    echo "!! FAIL: last prom scrape was $((now_s - last_scrape_s))s ago (TM prom endpoint died before capture end) — latency/custom legs are truncated; check TM heartbeat." >&2
    exit 2
  fi
fi
echo "stage-capture: evidence complete (all declared files have data rows, prom fresh)"

# Wait for the io probe to finish writing its tail samples (it runs
# DURATION_S+30s of iostat; the loop ended at DURATION_S — it exits on its
# own; never kill mid-write or the TSV loses its tail). Non-fatal WARN.
wait "$IO_PROBE_PID" 2>/dev/null || \
  echo "stage-capture: WARN — io-latency probe failed (see $IO_PROBE_LOG; disk-latency evidence DEGRADED)"

# Single-pane push: checkpoint events -> OpenObserve flink_checkpoints
# stream. flink-checkpoints.jsonl holds a FULL history snapshot per tick;
# emit each checkpoint once from the LAST snapshot line.
if [ -s "$OUT_DIR/flink-checkpoints.jsonl" ]; then
  python3 - "$OUT_DIR/flink-checkpoints.jsonl" <<'PYEOF2' | \
    python3 "$HERE_SC/o2_ingest.py" flink_checkpoints || \
    echo "stage-capture: WARN — O2 checkpoint push failed (raw truth: $OUT_DIR/flink-checkpoints.jsonl)"
import json, sys
last = None
with open(sys.argv[1], encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if line:
            try:
                last = json.loads(line)
            except json.JSONDecodeError:
                pass
if last:
    for ev in last.get("events", []):
        ts = ev.get("trigger_timestamp")
        if isinstance(ts, (int, float)):
            ev["@timestamp"] = int(ts)
        print(json.dumps(ev))
PYEOF2
fi

# Optional: latency histograms from TM Prometheus (one scrape per tick is
# heavy; sampled every LATENCY_EVERY_N ticks, default 2 => ~10s cadence).
echo "stage-capture: done -> $OUT_DIR"
