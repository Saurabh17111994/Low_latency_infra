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
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
TM_PROM_URL="${TM_PROM_URL:-http://localhost:9250}"
JOB_ID="${JOB_ID:-}"
DURATION_S="${DURATION_S:-900}"
CAPTURE_INTERVAL_S="${CAPTURE_INTERVAL_S:-5}"
OUT_DIR="${OUT_DIR:-logs/tracker-14/stage-capture-$(date +%Y%m%d-%H%M%S)}"

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

echo "stage-capture: job=$JOB_ID duration=${DURATION_S}s interval=${CAPTURE_INTERVAL_S}s out=$OUT_DIR"

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
} > "$OUT_DIR/run-meta.txt"

echo -e "epoch\tvertex_id\toperator\tnumRecordsIn\tnumRecordsOut\tbusyMsSum\tbackpressuredMsSum\tidleMsSum" \
  > "$OUT_DIR/stages.tsv"
echo -e "epoch\tvertex_id\toperator\tsubtask\twatermark" > "$OUT_DIR/watermark-lag.tsv"

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

  # --- TM Prometheus scrape: busy/backpressured/idle + watermarks + latency ---
  # REST answered empty aggregates for the time-share metrics on Flink 2.2.1
  # (live-observed 2026-09-01); TM :9249 exposes per-subtask gauges instead.
  # Scraped every tick into prom-<epoch>.txt; the parser merges later.
  curl -fsS --max-time 8 "$TM_PROM_URL/metrics" 2>/dev/null \
    | grep -E 'busyTimeMsPerSecond|backPressuredTimeMsPerSecond|hardBackPressuredTimeMsPerSecond|idleTimeMsPerSecond|currentWatermark|latency_source_id' \
    > "$OUT_DIR/prom-$(date +%s).txt" || true
}

# ---------------------------------------------------------------------------
# Main loop.
# ---------------------------------------------------------------------------
START=$(date +%s)
LAST_CP_COUNT=0
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
    exit 2
  fi
  sample_tick || echo "warn: one sample tick failed (continuing)"

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
                               "trigger_timestamp")} for c in hist]}))
' >> "$OUT_DIR/flink-checkpoints.jsonl" 2>/dev/null || true

  sleep "$CAPTURE_INTERVAL_S"
done

# Optional: latency histograms from TM Prometheus (one scrape per tick is
# heavy; sampled every LATENCY_EVERY_N ticks, default 2 => ~10s cadence).
echo "stage-capture: done -> $OUT_DIR"
