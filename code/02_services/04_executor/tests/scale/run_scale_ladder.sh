#!/usr/bin/env bash
# Bucket B — load / scale ladder harness (09 §7 SCALE-*, §8 REC-LOAD, §9 resource-exhaustion).
#
# Runs a rate ladder (default 10k→25k→50k→75k→100k ticks/s), samples the running Swarm
# stack + an optional latency endpoint, and writes a TSV report (throughput, p50/p95/p99,
# CPU, RPO-style observability columns). The harness is deposit-ready: on the M3 rig the
# credentialed 50k/s / p99<100ms / one-VM-loss acceptance is one command.
#
# Usage:
#   ./run_scale_ladder.sh ["10k,25k,50k" as "10000,25000,50000"]
# Env:
#   STACK_NAME   docker stack filter to sample        (default "nautilus")
#   DURATION     seconds per ladder rung               (default 15)
#   METRICS_URL  optional gateway/metrics URL to curl for latency
#   LOAD_CMD     command that applies $RATE ticks/s (drives the real workload)
#
# NOTE: on a local single-node dev swarm this is a SANITY ladder (functional, not
# production-credentialed). The documented acceptance numbers are M3/rig evidence.

set -euo pipefail
RATES_IN="${1:-10000,25000,50000,75000,100000}"
DURATION="${DURATION:-15}"
STACK_NAME="${STACK_NAME:-nautilus}"
METRICS_URL="${METRICS_URL:-}"
LOAD_CMD="${LOAD_CMD:-}"
OUT="scale_report_$(date +%Y%m%d_%H%M%S).tsv"
printf 'rung\trate_per_s\tduration_s\tsamples\tp50_ms\tp95_ms\tp99_ms\tp_max_ms\tcpu_pct\n' > "$OUT"

percentiles() {  # $1: file with one latency-ms value per line -> "p50 p95 p99 max"
# The program arrives on stdin (heredoc), so the data must arrive by argv —
# with the data piped in, the heredoc replaces it and every percentile is NA.
python3 - "$1" <<'PY'
import sys
with open(sys.argv[1]) as fh:
    v = sorted(float(l) for l in fh if l.strip())
if not v:
    print("NA NA NA NA"); sys.exit(0)
p = lambda q: v[min(len(v)-1, int(q*len(v)))]
# With n samples the p99 estimate is the nearest-rank max until n >= 100; saying
# so is honest, printing a "p99" that is really the max is not.
p99 = f"{p(.99):.1f}" if len(v) >= 100 else "NA"
print(f"{p(.5):.1f} {p(.95):.1f} {p99} {max(v):.1f}")
PY
}

LOAD_FAILED=0
rung=0
IFS=',' read -r -a RATES <<< "$RATES_IN"
for RATE in "${RATES[@]}"; do
  rung=$((rung+1))
  echo "== rung $rung: ${RATE} ticks/s for ${DURATION}s =="
  LOAD_PID=""
    # Resolve the stack's containers up front and sample their CPU **while the load runs**:
    # the reading used to be taken after `wait`, i.e. after the load had already stopped, so
    # it described an idle stack. `--filter name=` is a substring match (an unrelated
    # container whose name merely contains STACK_NAME used to be counted) and a filter that
    # matched nothing produced a silent CPU=NA, so the Swarm stack label is preferred, the
    # Compose project label is the fallback (M3 rig runs swarm, dev rig runs compose), and a
    # filter matching nothing says so.
    CPU="NA"; CPU_FILE=""
    CONTAINERS=$(docker ps -q --filter "label=com.docker.stack.namespace=$STACK_NAME" 2>/dev/null || true)
    [ -n "$CONTAINERS" ] || CONTAINERS=$(docker ps -q --filter "label=com.docker.compose.project=$STACK_NAME" 2>/dev/null || true)
    if [ -n "$CONTAINERS" ]; then
      # Sampled at the rung's midpoint in the background, so it overlaps the load window.
      CPU_FILE="/tmp/scale_cpu_$$"
      ( sleep $((DURATION/2))
        docker stats --no-stream --format '{{.CPUPerc}}' $CONTAINERS 2>/dev/null \
          | awk '{s+=$1; n++} END{if(n)printf "%.1f", s/n; else print "NA"}' > "$CPU_FILE" ) &
      CPU_PID=$!
    else
      echo "  warning: no containers carry STACK_NAME=$STACK_NAME as a swarm stack namespace or compose project label" >&2
    fi
  if [ -n "$LOAD_CMD" ]; then
    ( RATE="$RATE" DURATION="$DURATION" bash -c "$LOAD_CMD" ) &
    LOAD_PID=$!
  fi

  LAT="/tmp/scale_lat_$$"; : > "$LAT"
  # Sample until the rung's wall-clock budget is spent, not for a fixed number of
  # iterations: a slow endpoint used to stretch the rung past DURATION, because
  # every iteration paid curl's full (unbounded) time plus the 1s sleep. Each
  # request is bounded now, so an unresponsive endpoint costs at most 2s per
  # sample and the rung still ends on time.
  RUNG_END=$((SECONDS + DURATION))
  while [ "$SECONDS" -lt "$RUNG_END" ]; do
    if [ -n "$METRICS_URL" ]; then
      curl --max-time 2 --connect-timeout 1 -so /dev/null -w '%{time_total}\n' "$METRICS_URL" 2>/dev/null \
        | awk '{printf "%.3f\n", $1*1000}' >> "$LAT" || true
    fi
    sleep 1
  done
  # P3-028: a crashed load generator used to be swallowed by `|| true`, so the rung still
  # reported latency/throughput as if the requested rate had been applied. Record the
  # status, name the rung, and fail the run at the end — an acceptance run must not "pass"
  # on no load.
  if [ -n "$LOAD_PID" ]; then
    if wait "$LOAD_PID"; then
      :
    else
      echo "  FAIL: load generator exited $? for rung $rung (${RATE} ticks/s)" >&2
      LOAD_FAILED=1
    fi
  fi
  if [ -n "$CPU_FILE" ]; then
    wait "$CPU_PID" 2>/dev/null || true
    CPU=$(cat "$CPU_FILE" 2>/dev/null || echo NA)
    rm -f "$CPU_FILE"
  fi

  read P50 P95 P99 PMAX < <(percentiles "$LAT")
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$rung" "$RATE" "$DURATION" "$(wc -l < "$LAT")" "$P50" "$P95" "$P99" "$PMAX" "$CPU" >> "$OUT"
  rm -f "$LAT"
done
if [ "$LOAD_FAILED" -ne 0 ]; then
  echo "scale ladder FAILED: at least one rung's load generator exited non-zero (report: $OUT)" >&2
  exit 1
fi
echo "scale ladder report: $OUT"
