#!/usr/bin/env bash
# Load-test collector with FEED-LIVENESS GUARD (audit #9 G8, 2026-08-28).
#
# The 5.8s-p99 catastrophe was a measurement artifact: the feed STALLED (safety
# halt) but the rolling latency histogram kept stale values, so p99 looked like
# real degradation. This collector tags every snapshot VALID only if the source
# rate is actually flowing (numRecordsInPerSecond on the raw source > threshold);
# INVALID snapshots are excluded from the summary. A report built from INVALID
# snapshots alone prints a loud warning instead of numbers.
#   B2. minimum-duration guard: runs shorter than 190s get NO verdict (the stale
#       storm needs >47s to appear, so a 90s clean run is a false negative)  -> exit 2 below
#
# Usage: bash loadtest-collect.sh <out_dir> <duration_s> <interval_s> <job_id>  (duration_s >= 190)
# Requires: running Flink (REST :8081), Prometheus (:9250), TM container stats.
set -uo pipefail

OUT="${1:?out_dir required}"
DURATION_S="${2:-240}"
INTERVAL_S="${3:-30}"
JOB_ID="${4:-e641dc3e5de1b9f9d8f66248fbc4383c}"

# ---------- B2: minimum-duration guard (audit #9 lesson) ----------
# A 90s run is a FALSE NEGATIVE: the stale storm takes >47s to appear, so a
# short clean run "proves" nothing. Refuse to emit any verdict below 190s.
if [ "$DURATION_S" -lt 190 ]; then
  echo "WARN: DURATION_S=$DURATION_S < 190 — verdict INVALID: the stale storm needs >47s to appear, so sub-190s runs are false-negatives. Refusing to produce a summary." >&2
  exit 2
fi

PROM="http://localhost:9250/metrics"
FLINK="http://localhost:8081"
LIVE_THRESHOLD_RATE=500            # source records/s below this => feed stalled

# --- source feed-liveness: raw-table-1 source numRecordsInPerSecond, summed ---
feed_rate() {
  curl -s --max-time 5 "$PROM" 2>/dev/null > /tmp/loadtest-metrics.txt
  python3 - "$JOB_ID" /tmp/loadtest-metrics.txt <<'PY'
import re, sys
job, path = sys.argv[1], sys.argv[2]
tot = 0.0
for line in open(path):
    m = re.match(r'^flink_taskmanager_job_task_numRecordsInPerSecond\{([^}]*)\}\s+([0-9.]+)$', line)
    if not m: continue
    lab = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
    if lab.get('job_id') == job:
        tot += float(m.group(2))
print(f"{tot:.0f}")
PY
}

# --- per-operator records/s (all source-ish operators, for the summary) ---
operator_rates() {
  curl -s --max-time 5 "$PROM" 2>/dev/null > /tmp/loadtest-metrics.txt
  python3 - "$JOB_ID" /tmp/loadtest-metrics.txt <<'PY'
import re, sys, urllib.request, json
job, path = sys.argv[1], sys.argv[2]
# map task_id -> name
try:
    with urllib.request.urlopen(f"http://localhost:8081/jobs/{job}", timeout=5) as r:
        verts = {v['id']: v['name'] for v in json.load(r)['vertices']}
except Exception:
    verts = {}
rates = {}
for line in open(path):
    m = re.match(r'^flink_taskmanager_job_task_numRecordsInPerSecond\{([^}]*)\}\s+([0-9.]+)$', line)
    if not m: continue
    lab = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
    if lab.get('job_id') != job: continue
    name = verts.get(lab.get('task_id',''), lab.get('task_id',''))
    rates[name] = rates.get(name, 0) + float(m.group(2))
for k in sorted(rates):
    print(f"{k}\t{rates[k]:.0f}")
PY
}

# --- p99 from the latency histogram (max across subtasks) ---
p99_snapshot() {
  curl -s --max-time 5 "$PROM" 2>/dev/null > /tmp/loadtest-metrics.txt
  python3 - "$JOB_ID" /tmp/loadtest-metrics.txt <<'PY'
import re, sys, json
job, path = sys.argv[1], sys.argv[2]
d = {}
for line in open(path):
    if 'quantile="0.99"' not in line: continue
    m = re.match(r'^flink_taskmanager_job_task_latency_source_id_operator_id_operator_subtask_index_latency\{(.+)\}\s+([0-9.eE+-]+)$', line)
    if not m: continue
    lab = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
    if lab.get('job_id') != job: continue
    name = lab.get('task_name','?')
    val = float(m.group(2))
    d[name] = max(d.get(name, 0), val)
print(json.dumps(d))
PY
}

header="ts\tfeed_rate\tfeed_valid\tflink_rss\tflink_cpu\tckpt_ok\tckpt_fail\tp99_dedup\tp99_writer\tp99_detection\tp99_builder\tp99_sink"
echo "$header" > "$OUT/snapshots.tsv"
header_busy="ts\toperator\tbusy_ms_s\tbackpressure_ms_s\tidle_ms_s\tinput_queue_len"
echo "$header_busy" > "$OUT/busy.tsv"

# --- per-operator busy/backpressure/idle (Finding #17 diagnostic) ---
# Samples every vertex's aggregated busy/backpressure/idle ms/s from the
# Flink REST API. Runs in background so the main loop's cadence is unchanged.
busy_sampler() {
  while :; do
    TS=$(date -Iseconds)
    # 1) get all vertices for the job
    VERTICES=$(curl -s --max-time 5 "$FLINK/jobs/$JOB_ID" 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    for v in d.get('vertices',[]):
        print(v['id'], v.get('name',''), sep='\t')
except Exception:
    pass
" 2>/dev/null)
    # 2) for each vertex, get its busy/backpressure/idle aggregate
    while IFS=$'\t' read -r VID VNAME; do
      [ -z "$VID" ] && continue
      # The aggregate endpoint needs the subtask-scope prefix '0.' (verified:
      # bare names return [] in Flink 2.2.1). '0.' = aggregated across subtasks.
      # The aggregate endpoint needs the subtask-scope prefix '0.' (verified:
      # bare names return [] in Flink 2.2.1). '0.' = aggregated across subtasks.
      # inputQueueLength = buffers waiting in the operator's input queue — the
      # Step-0 diagnostic: high+full => operator slow to consume (own cost);
      # low/empty while backpressured => artifact or downstream pressure.
      MET=$(curl -s --max-time 5 "$FLINK/jobs/$JOB_ID/vertices/$VID/metrics?get=0.busyTimeMsPerSecond,0.backPressuredTimeMsPerSecond,0.idleTimeMsPerSecond,0.Shuffle.Netty.Input.Buffers.inputQueueLength" 2>/dev/null | python3 -c "
import json,sys
try:
    d={m['id']:m['value'] for m in json.load(sys.stdin)}
    print(f\"{d.get('0.busyTimeMsPerSecond','n/a')}\t{d.get('0.backPressuredTimeMsPerSecond','n/a')}\t{d.get('0.idleTimeMsPerSecond','n/a')}\t{d.get('0.Shuffle.Netty.Input.Buffers.inputQueueLength','n/a')}\")
except Exception:
    print('n/a\tn/a\tn/a\tn/a')
" 2>/dev/null)
      echo -e "$TS\t$VNAME\t$MET" >> "$OUT/busy.tsv"
    done <<< "$VERTICES"
    sleep "$INTERVAL_S"
  done
}
busy_sampler &
BUSY_PID=$!

START=$SECONDS
VALID_COUNT=0; INVALID_COUNT=0
while [ $(( SECONDS - START )) -lt "$DURATION_S" ]; do
  TS=$(date -Iseconds)
  FR=$(feed_rate)
  # G8: the guard — a snapshot is VALID only if the feed is live
  if [ "$FR" -ge "$LIVE_THRESHOLD_RATE" ] 2>/dev/null; then VALID="VALID"; VALID_COUNT=$((VALID_COUNT+1)); else VALID="INVALID"; INVALID_COUNT=$((INVALID_COUNT+1)); fi
  RSS=$(docker stats --no-stream --format '{{.MemUsage}}' 01_docker-flink-taskmanager-1 2>/dev/null | cut -d/ -f1)
  CPU=$(docker stats --no-stream --format '{{.CPUPerc}}' 01_docker-flink-taskmanager-1 2>/dev/null)
  CK=$(curl -s --max-time 5 "$FLINK/jobs/$JOB_ID/checkpoints" 2>/dev/null | python3 -c "
import json,sys
try:
    c=json.load(sys.stdin).get('counts',{})
    print(f\"{c.get('completed',0)}\t{c.get('failed',0)}\")
except Exception:
    print('n/a\tn/a')
" 2>/dev/null)
  P99=$(p99_snapshot)
  P99_LINE=$(echo "$P99" | python3 -c "
import json,sys
d=json.load(sys.stdin)
def g(*names):
    for n in names:
        if n in d: return f\"{d[n]:.0f}\"
    return 'n/a'
print('\t'.join([g('fingerprint_dedup'), g('forming_bar_writer'), g('forming_bar_detection'), g('forming_bar_builder'), g('forming_bar_sink:_Writer')]))
" 2>/dev/null)
  echo -e "$TS\t$FR\t$VALID\t$RSS\t$CPU\t$CK\t$P99_LINE" >> "$OUT/snapshots.tsv"
  sleep "$INTERVAL_S"
done

# stop the busy sampler
kill "$BUSY_PID" 2>/dev/null || true

# --- summary: ONLY VALID snapshots contribute p99; INVALID-only => loud warning ---
echo ""
echo "=== summary (feed-liveness guarded) ==="
python3 - "$OUT/snapshots.tsv" "$VALID_COUNT" "$INVALID_COUNT" <<'PY'
import sys
path, vc, ic = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
print(f"snapshots: {vc} VALID (feed live), {ic} INVALID (feed stalled)")
print(f"feed_rate column: {open(path).read().splitlines()[1].split(chr(9))[1] if len(open(path).read().splitlines())>1 else 'n/a'} (first) ... ")
if vc == 0:
    print("!! NO VALID snapshots — feed was stalled the whole run; p99 values are NOT meaningful.")
    print("!! Investigate: faketool real-rate? bridge subscription? safety halt (STALE_BROKER_TIMESTAMP)?")
else:
    print("VALID snapshots only (first 3 + last 3):")
    lines = [l for l in open(path).read().splitlines() if '\tVALID\t' in l]
    for l in (lines[:3] + (['...'] if len(lines)>6 else []) + lines[-3:]):
        print(" ", l)
PY

echo "collector done -> $OUT/snapshots.tsv (VALID=$VALID_COUNT INVALID=$INVALID_COUNT)"
