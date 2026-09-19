#!/usr/bin/env bash
# Load-test collector with FEED-LIVENESS GUARD (audit #9 G8, 2026-08-28).
#
# The 5.8s-p99 catastrophe was a measurement artifact: the feed STALLED (safety
# halt) but the rolling latency histogram kept stale values, so p99 looked like
# real degradation. This collector tags every snapshot VALID only if the source
# rate is actually flowing (numRecordsInPerSecond on the raw source > threshold);
# INVALID snapshots are excluded from the summary. A report built from INVALID
# snapshots alone prints a loud warning instead of numbers. A FAILED SCRAPE is
# UNKNOWN, never VALID or INVALID: "Prometheus unreachable" is not "feed stalled".
#   B2. minimum-duration guard: runs shorter than 190s get NO verdict (the stale
#       storm needs >47s to appear, so a 90s clean run is a false negative)  -> exit 2 below
#
# Usage: bash loadtest-collect.sh <out_dir> <duration_s> <interval_s> <job_id>  (duration_s >= 190)
# Requires: running Flink (REST :8081), Prometheus (:9250), TM container stats.
# Exit: 0 = ran (the summary carries the verdict), 2 = refused input/precondition.
set -uo pipefail

OUT="${1:?out_dir required}"
DURATION_S="${2:-240}"
INTERVAL_S="${3:-30}"
JOB_ID="${4:-e641dc3e5de1b9f9d8f66248fbc4383c}"

# H3 (2026-08-29): endpoints env-overridable with localhost defaults.
PROM="${PROM_URL:-http://localhost:9250/metrics}"
FLINK="${FLINK_URL:-http://localhost:8081}"
LIVE_THRESHOLD_RATE="${LIVE_THRESHOLD_RATE:-500}"   # source records/s below this => feed stalled
TM_CONTAINER="${TM_CONTAINER:-01_docker-flink-taskmanager-1}"

# ---------- input validation, before ANYTHING (including $OUT) is created ----------
# P6-441: a non-numeric DURATION_S/INTERVAL_S used to reach the integer tests
# (stderr suppressed there => a bogus INVALID verdict) and the `sleep` calls (an
# error every interval, i.e. a hot loop). Refuse up front. 2 is this script's
# "no verdict" code, matching B2 below (the load-test runner refuses with 1).
validate_pos_int() { # validate_pos_int <name> <value> [what]
  local name="$1" val="${2:-}" what="${3:-}"
  case "$val" in
    ''|*[!0-9]*) echo "FATAL: $name='$val' is not a positive integer${what:+ ($what)}" >&2; return 2;;
  esac
  [ "$val" -gt 0 ] || { echo "FATAL: $name must be a positive integer, got '$val'${what:+ ($what)}" >&2; return 2; }
  return 0
}
validate_pos_int DURATION_S "$DURATION_S" "collect.sh gives no verdict below 190s" || exit $?
validate_pos_int INTERVAL_S "$INTERVAL_S" || exit $?

# ---------- B2: minimum-duration guard (audit #9 lesson) ----------
# A 90s run is a FALSE NEGATIVE: the stale storm takes >47s to appear, so a
# short clean run "proves" nothing. Refuse to emit any verdict below 190s.
# Its own function so the guards self-test can exercise BOTH sides of the
# boundary (189 refuses, 190 accepts) with no cluster running (P6-586).
require_min_duration() { # require_min_duration <secs>: rc 2 below 190
  local d="$1"
  if [ "$d" -lt 190 ]; then
    echo "WARN: DURATION_S=$d < 190 — verdict INVALID: the stale storm needs >47s to appear, so sub-190s runs are false-negatives. Refusing to produce a summary." >&2
    return 2
  fi
  return 0
}
require_min_duration "$DURATION_S" || exit $?

mkdir -p "$OUT" || { echo "FATAL: cannot create out_dir: $OUT" >&2; exit 2; }

# ---------- scrape plumbing: one PRIVATE scrape per interval ----------
# P6-123: the three metric readers below used to curl Prometheus into the same
# predictable path under /tmp. Two concurrent benches clobbered each other, `>`
# would follow a pre-planted symlink, and a failed request left a truncated file
# that the parser read as rate 0 — a collection failure looked exactly like a
# stalled feed. P6-442: the HTTP status was never checked either, so an error
# body (no metric lines at all) parsed as "0 records/s" the same way.
SCRAPE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/loadtest-collect.XXXXXX")
SCRAPE="$SCRAPE_DIR/prom.txt"

cleanup() { # EXIT trap: reap the sampler, drop the private scrape dir
  local rc=$?
  if [ -n "${BUSY_PID:-}" ]; then
    kill "$BUSY_PID" 2>/dev/null || true
    wait "$BUSY_PID" 2>/dev/null || true     # P6-124: never orphan the infinite loop
  fi
  [ -n "${SCRAPE_DIR:-}" ] && rm -rf "$SCRAPE_DIR"
  return "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

scrape_prom() { # scrape_prom: fills $SCRAPE; rc != 0 when the scrape itself failed
  local code
  code=$(curl -sS --max-time 5 -o "$SCRAPE" -w '%{http_code}' "$PROM" 2>/dev/null) || return 1
  [ "$code" = "200" ] || return 1
  [ -s "$SCRAPE" ] || return 1
  return 0
}

# ---------- feed-liveness classification (P6-125) ----------
# VALID = feed live, INVALID = feed stalled, UNKNOWN = the scrape failed. An
# empty rate used to fall through `[ "$FR" -ge <threshold> ] 2>/dev/null` into
# the INVALID branch, so "Prometheus unreachable" was reported as a stall and
# the two cases were indistinguishable in the report.
classify_feed_rate() { # classify_feed_rate <rate|"">: VALID|INVALID|UNKNOWN (rc 1 = UNKNOWN)
  local fr="${1:-}" thr="${LIVE_THRESHOLD_RATE:-500}"
  case "$fr" in
    ''|*[!0-9]*) echo "UNKNOWN"; return 1;;
  esac
  if [ "$fr" -ge "$thr" ]; then echo "VALID"; else echo "INVALID"; fi
  return 0
}

# --- source feed-liveness: raw-table-1 source numRecordsInPerSecond ---
# FIX (2026-08-29): feed_rate previously summed numRecordsInPerSecond across
# ALL tasks of the job — forming_bar_detection (1431/s), candle sinks, signal
# sinks etc. — inflating the "feed" to ~124k/s when the true source feed was
# 20.5k/s. It must measure ONLY the raw source task (task_name ~ "raw-table-1"),
# summed across its subtasks.
feed_rate() { # feed_rate: reads $SCRAPE (filled by scrape_prom), prints records/s
  python3 - "$JOB_ID" "$SCRAPE" <<'PY'
import re, sys
job, path = sys.argv[1], sys.argv[2]
tot = 0.0
for line in open(path):
    m = re.match(r'^flink_taskmanager_job_task_numRecordsInPerSecond\{([^}]*)\}\s+([0-9.eE+-]+)$', line)
    if not m: continue
    lab = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
    # Flink sanitizes operator names in Prometheus labels: spaces/dashes
    # become underscores — "Source: raw-table-1 -> raw-validation" is exposed
    # as "Source:_raw_table_1____raw_validation". Match the sanitized prefix.
    if lab.get('job_id') == job and 'Source:_raw_table_1' in lab.get('task_name', ''):
        tot += float(m.group(2))
print(f"{tot:.0f}")
PY
}

# --- p99 from the latency histogram (max across subtasks per operator) ---
# P6-126: Flink exposes the SANITIZED CHAIN whenever an operator is chained, so
# "fingerprint_dedup -> ingest_latency_monitor" arrives as
# "fingerprint_dedup____ingest_latency_monitor" (verified in
# logs/soak-e2e-20260904-234313/stages/prom-*.txt) while the summary looked up
# the bare operator name — that column silently became n/a. Alias every chain
# segment, so a chained or renamed operator keeps its column.
p99_snapshot() { # p99_snapshot: reads $SCRAPE, prints JSON {task_name|segment: p99}
  python3 - "$JOB_ID" "$SCRAPE" <<'PY'
import re, sys, json
job, path = sys.argv[1], sys.argv[2]
CHAIN = "____"   # Flink's sanitizer maps each char of " -> " to one underscore
d = {}
for line in open(path):
    if 'quantile="0.99"' not in line: continue
    m = re.match(r'^flink_taskmanager_job_task_latency_source_id_operator_id_operator_subtask_index_latency\{(.+)\}\s+([0-9.eE+-]+)$', line)
    if not m: continue
    lab = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
    if lab.get('job_id') != job: continue
    name = lab.get('task_name', '?')
    val = float(m.group(2))
    for key in (name, *name.split(CHAIN)):
        if key:
            d[key] = max(d.get(key, 0.0), val)
print(json.dumps(d))
PY
}

p99_row() { # p99_row <json from p99_snapshot>: prints 5 tab-separated p99 cells
  python3 -c '
import json, sys
try:
    d = json.loads(sys.argv[1])
except Exception:
    d = {}
def g(*names):
    for n in names:
        if n in d: return f"{d[n]:.0f}"
    return "n/a"
print("\t".join([g("fingerprint_dedup"), g("forming_bar_writer"), g("forming_bar_detection"), g("forming_bar_builder"), g("forming_bar_sink:_Writer")]))
' "${1:-}"
}

# ---------- TM container stats (P6-445) ----------
# Two per-snapshot container-stats queries (each a ~0.5-1s round trip to the
# daemon) and `cut -d/ -f1`, which left a trailing space and whichever unit the
# daemon picked (MiB today, GiB tomorrow), so the flink_rss column mixed units.
mem_to_mib() { # mem_to_mib <"3.3GiB">: integer MiB, 'n/a' for an unknown unit
  local v="$1" num unit
  num="${v%%[A-Za-z]*}"; num="${num// /}"
  unit="${v##*[0-9.]}"; unit="${unit// /}"
  case "$unit" in
    B)        awk -v n="$num" 'BEGIN{printf "%.0f", n/1048576}';;
    kB|KB)    awk -v n="$num" 'BEGIN{printf "%.0f", n/1000}';;
    KiB)      awk -v n="$num" 'BEGIN{printf "%.0f", n/1024}';;
    MB|MiB|M) awk -v n="$num" 'BEGIN{printf "%.0f", n}';;
    GB)       awk -v n="$num" 'BEGIN{printf "%.0f", n*1000}';;
    GiB|G)    awk -v n="$num" 'BEGIN{printf "%.0f", n*1024}';;
    *)        echo "n/a";;
  esac
}

container_stats() { # container_stats: "<rss_mib>\t<cpu_percent>", 'n/a' on failure
  local raw mem cpu
  raw=$(docker stats --no-stream --format '{{.MemUsage}}|{{.CPUPerc}}' "$TM_CONTAINER" 2>/dev/null) || raw=""
  if [ -z "$raw" ]; then printf 'n/a\tn/a\n'; return 0; fi
  mem="${raw%%|*}"; cpu="${raw##*|}"
  printf '%s\t%s\n' "$(mem_to_mib "${mem%%/*}")" "${cpu:-n/a}"
}

row_shape_ok() { # row_shape_ok <line> <want_cols>: rc 0 when the tab-field count matches
  local line="$1" want="$2" n
  n=$(printf '%s' "$line" | awk -F'\t' '{print NF}')
  [ "$n" -eq "$want" ]
}

# P6-444: headers were one string with `\t` written by plain echo, which does NOT
# expand escapes — so the header was a single field full of literal backslashes
# while the data rows carried real tabs. printf writes both, and every row is
# shape-checked against its header (P6-446).
SNAP_COLS=12; BUSY_COLS=6
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  ts feed_rate feed_valid flink_rss flink_cpu ckpt_ok ckpt_fail \
  p99_dedup p99_writer p99_detection p99_builder p99_sink > "$OUT/snapshots.tsv"
printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
  ts operator busy_ms_s backpressure_ms_s idle_ms_s input_queue_len > "$OUT/busy.tsv"

# --- per-operator busy/backpressure/idle (Finding #17 diagnostic) ---
# Samples every vertex's aggregated busy/backpressure/idle ms/s from the
# Flink REST API. Runs in background so the main loop's cadence is unchanged;
# the EXIT trap reaps it and waits for it (P6-124), so an abort cannot leave an
# infinite loop sampling a cluster whose run is over.
busy_sampler() {
  while :; do
    TS=$(date -Iseconds)
    # 1) all vertices of the job
    VERTICES=$(curl -s --max-time 5 "$FLINK/jobs/$JOB_ID" 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    for v in d.get('vertices',[]):
        print(v['id'], v.get('name',''), sep='\t')
except Exception:
    pass
" 2>/dev/null)
    if [ -z "$VERTICES" ]; then
      echo "WARN: busy_sampler: no vertices for job $JOB_ID (Flink REST unreachable?) — busy.tsv has a gap" >&2
    fi
    # 2) per vertex: its busy/backpressure/idle aggregate
    while IFS=$'\t' read -r VID VNAME; do
      [ -z "$VID" ] && continue
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
      BROW=$(printf '%s\t%s\t%s' "$TS" "$VNAME" "$MET")
      row_shape_ok "$BROW" "$BUSY_COLS" || echo "WARN: busy row does not have $BUSY_COLS columns: $BROW" >&2
      printf '%s\n' "$BROW" >> "$OUT/busy.tsv"
    done <<< "$VERTICES"
    sleep "$INTERVAL_S"
  done
}
busy_sampler &
BUSY_PID=$!

START=$SECONDS
VALID_COUNT=0; INVALID_COUNT=0; UNKNOWN_COUNT=0; SHAPE_BAD=0
while [ $(( SECONDS - START )) -lt "$DURATION_S" ]; do
  TS=$(date -Iseconds)
  # one scrape per interval, shared by the feed guard and the p99 snapshot
  if scrape_prom; then
    FR=$(feed_rate)
    P99=$(p99_snapshot)
    VALID=$(classify_feed_rate "$FR") || true      # rc 1 == UNKNOWN (rate unreadable)
  else
    FR=""; P99='{}'; VALID=UNKNOWN                 # P6-125/442: a failed scrape, not a stall
  fi
  case "$VALID" in
    VALID)   VALID_COUNT=$((VALID_COUNT+1));;
    INVALID) INVALID_COUNT=$((INVALID_COUNT+1));;
    *)       UNKNOWN_COUNT=$((UNKNOWN_COUNT+1));;
  esac
  ST=$(container_stats)
  RSS="${ST%%$'\t'*}"; CPU="${ST##*$'\t'}"
  CK=$(curl -s --max-time 5 "$FLINK/jobs/$JOB_ID/checkpoints" 2>/dev/null | python3 -c "
import json,sys
try:
    c=json.load(sys.stdin).get('counts',{})
    print(f\"{c.get('completed',0)}\t{c.get('failed',0)}\")
except Exception:
    print('n/a\tn/a')
" 2>/dev/null)
  [ -n "$CK" ] || CK=$'n/a\tn/a'
  # P6-446: p99_row always emits 5 cells and every other field has an explicit
  # default, so a failed sub-query cannot shorten the row.
  P99_LINE=$(p99_row "${P99:-}")
  ROW=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s' \
    "$TS" "${FR:-n/a}" "$VALID" "${RSS:-n/a}" "${CPU:-n/a}" "$CK" "$P99_LINE")
  row_shape_ok "$ROW" "$SNAP_COLS" || {
    SHAPE_BAD=$((SHAPE_BAD+1))
    echo "WARN: snapshot row does not have $SNAP_COLS columns: $ROW" >&2
  }
  printf '%s\n' "$ROW" >> "$OUT/snapshots.tsv"
  sleep "$INTERVAL_S"
done

# stop the busy sampler here too (the EXIT trap is the backstop for aborts)
if [ -n "${BUSY_PID:-}" ]; then
  kill "$BUSY_PID" 2>/dev/null || true
  wait "$BUSY_PID" 2>/dev/null || true
  unset BUSY_PID
fi

# ---------- summary: ONLY VALID snapshots contribute p99; INVALID-only => loud warning ----------
# P6-229/P6-585: a function rather than an inline heredoc, so the guard self-test
# can drive it with fixture TSVs instead of grepping a file it wrote itself.
loadtest_summary() { # loadtest_summary <snapshots.tsv> <valid> <invalid> <unknown>
  local path="$1" vc="${2:-0}" ic="${3:-0}" uc="${4:-0}"
  python3 - "$path" "$vc" "$ic" "$uc" <<'PY'
import sys
path = sys.argv[1]
vc, ic, uc = (int(x) for x in sys.argv[2:5])
print(f"snapshots: {vc} VALID (feed live), {ic} INVALID (feed stalled), {uc} UNKNOWN (scrape failed)")
try:
    rows = open(path).read().splitlines()
except OSError as exc:
    print(f"!! cannot read {path}: {exc}")
    sys.exit(0)
valid_rows = [l for l in rows if '\tVALID\t' in l]
cells = [l.split('\t') for l in valid_rows]
cols = ['p99_dedup', 'p99_writer', 'p99_detection', 'p99_builder', 'p99_sink']
if cells:
    for i, name in enumerate(cols):
        idx = 7 + i
        if all(len(c) > idx and c[idx] == 'n/a' for c in cells):
            print(f"!! {name}: n/a in ALL {len(cells)} VALID snapshots — the operator name may have been renamed or chained; check the task_name labels")
    first = cells[0]
    print(f"feed_rate column (first VALID row): {first[1] if len(first) > 1 else 'n/a'}")
if vc == 0:
    print("!! NO VALID snapshots — feed was stalled the whole run; p99 values are NOT meaningful.")
    print("!! Investigate: faketool real-rate? bridge subscription? safety halt (STALE_BROKER_TIMESTAMP)?")
    if uc:
        print(f"!! {uc} snapshot(s) were UNKNOWN (Prometheus unreachable), not stalls — fix collection first")
else:
    print("VALID snapshots only (first 3 + last 3):")
    for l in (valid_rows[:3] + (['...'] if len(valid_rows) > 6 else []) + valid_rows[-3:]):
        print(" ", l)
PY
}

echo ""
echo "=== summary (feed-liveness guarded) ==="
loadtest_summary "$OUT/snapshots.tsv" "$VALID_COUNT" "$INVALID_COUNT" "$UNKNOWN_COUNT"
[ "$SHAPE_BAD" -eq 0 ] || echo "WARN: $SHAPE_BAD snapshot row(s) had a bad column count — see the warnings above" >&2
if [ "$(wc -l < "$OUT/busy.tsv")" -le 1 ]; then
  echo "WARN: busy.tsv has no samples — the per-operator busy/backpressure diagnostics are missing" >&2
fi
echo "collector done -> $OUT/snapshots.tsv (VALID=$VALID_COUNT INVALID=$INVALID_COUNT UNKNOWN=$UNKNOWN_COUNT)"
