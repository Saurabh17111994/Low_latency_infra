#!/usr/bin/env bash
# =============================================================================
# bench-throughput.sh — 20k/s multi-instrument throughput bench (Phase 5).
#
# One ingestion container runs against the fake HFT broker in -real-rate mode
# (1024 subscribed ids x 20 Hz = 20,480 frames/s, faketool Phase 4). Three
# 60-second windows measure:
#   - append rate  = append_latency_ms_count delta over the window
#   - p50/p99      = append_latency_ms_p50/_p99 gauges when O2 exposes them;
#                    else the mean (append_latency_ms_sum / _count) — O2
#                    derives only count/sum/min/max/bucket from OTLP
#                    histograms, so the mean is the confirmed fallback.
#   - decode_errors delta over the window
#
# PASS (every window): rows >= 15,000 AND decode_errors delta == 0
#                      AND p99 < 1000 ms.
#
# The soak suite (run-full-suite.sh) is untouched: this bench only builds a
# fresh ingestion image, runs one container for ~3 minutes of measurement,
# and tears everything down.
#
# docker-compose.bench.yml (bench-only, not in the suite) raises
# CLOCK_OFFSET_LIMIT_MS to 500ms: this host's clock runs ~120-190ms fast vs
# the configured NTP servers (measured against three independent servers), so
# the default 100ms limit would keep the container permanently unhealthy and
# the bench would never start measuring. Override: BENCH_CLOCK_OFFSET_LIMIT_MS.
#
# Usage:  ./bench-throughput.sh
# Evidence: $OUT/bench/bench-throughput.tsv + $OUT/bench/result.txt
# =============================================================================
set -euo pipefail

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CODE_DIR="$PROJECT_ROOT/code"
DOCKER_DIR="$CODE_DIR/01_platform/01_docker"
BRIDGE_DIR="$CODE_DIR/02_services/01_ingestion/go-bridge"
INGESTION_DIR="$CODE_DIR/02_services/01_ingestion"
JAR="$INGESTION_DIR/target/ingestion.jar"
MANIFEST="/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv"
O2_BASE="http://localhost:5080"
INGESTION_CONTAINER="01_docker-ingestion-1"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="${OUT_DIR:-$PROJECT_ROOT/logs/soak/bench-$STAMP}"
mkdir -p "$OUT/bench/journal" "$OUT/bin"
RUN_LOG="$OUT/bench/run.log"
RESULT_FILE="$OUT/bench/result.txt"
TSV="$OUT/bench/bench-throughput.tsv"
: > "$RUN_LOG"

# Never echo credentials: O2 auth header value read from .env, kept in a var.
# M-13 (2026-08-31): O2_AUTH_BASIC moved from .env to secrets.env — read
# .env first, then fall back to secrets.env (compose env-file order).
O2_AUTH="$(awk -F= '/^O2_AUTH_BASIC=/{print $2; exit}' "$DOCKER_DIR/.env" 2>/dev/null || true)"
[ -z "$O2_AUTH" ] && O2_AUTH="$(awk -F= '/^O2_AUTH_BASIC=/{print $2; exit}' "$DOCKER_DIR/secrets.env" 2>/dev/null || true)"

# Everything lands in run.log AND the console.
exec > >(tee -a "$RUN_LOG") 2>&1

echo "=== bench-throughput start $STAMP (out: $OUT)"

# ── Helpers ──────────────────────────────────────────────────────────────────
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
port_open() { # $1=host $2=port — listener check via ss
	ss -ltn 2>/dev/null | grep -qE "[::0-9.*]:$2[[:space:]]"
}

# Copied verbatim from run-full-suite.sh (o2_query): $1 = SQL → latest value
# column (or UNAVAILABLE).
o2_query() {
	local sql="$1" payload val
	payload="$(python3 - "$sql" <<'PY'
import json, sys, time
now = int(time.time() * 1_000_000)
print(json.dumps({
    "query": {"sql": sys.argv[1],
              "start_time": now - 3_600_000_000,  # 1h window
              "end_time": now,
              "size": 5}}))
PY
)"
	val="$(curl -s -m 15 -H "Authorization: Basic $O2_AUTH" -H 'Content-Type: application/json' \
		-X POST "$O2_BASE/api/default/_search?type=metrics" -d "$payload" 2>/dev/null \
		| python3 -c 'import json,sys
try:
    d=json.load(sys.stdin)
    hits=d.get("hits", [])
    if not hits:
        print("NO_HITS")
    else:
        src=hits[0].get("_source") or {}
        print(src.get("value", hits[0].get("value", "NO_VALUE")))
except Exception:
    print("UNAVAILABLE")' || true)"
	echo "${val:-UNAVAILABLE}"
}

# Non-numeric O2 replies (UNAVAILABLE / NO_HITS / NO_VALUE) → empty.
# O2 serves counters as floats ("5637597.0") — coerce to integer for arithmetic.
num() { case "$1" in ''|*[!0-9.-]*) echo "" ;; *) printf '%.0f' "$1" ;; esac; }

FAILED=0
RESULT="PASS"
WINDOW_FAILS=""
WINDOWS_DONE=0

fail() { FAILED=1; RESULT="FAIL"; echo "!! $*"; }

# ── Teardown (always) ────────────────────────────────────────────────────────
FAKETOOL_PID=""
cleanup() {
	local rc=$?
	if [ "$WINDOWS_DONE" -ne 3 ]; then
		FAILED=1
		RESULT="FAIL"
		echo "!! bench aborted before completing all 3 windows ($WINDOWS_DONE/3) — result forced to FAIL"
	fi
	echo "-- teardown ($(now))"
	[ -n "$FAKETOOL_PID" ] && kill "$FAKETOOL_PID" 2>/dev/null || true
	[ -n "$FAKETOOL_PID" ] && wait "$FAKETOOL_PID" 2>/dev/null || true
	(cd "$DOCKER_DIR" && docker compose --env-file .env --env-file secrets.env \
		-f docker-compose.yml \
		-f docker-compose.soak.yml -f docker-compose.bench.yml stop ingestion) >/dev/null 2>&1 || true
	if port_open 127.0.0.1 8899; then
		echo "!! teardown: port 8899 still busy"
		FAILED=1
		RESULT="FAIL"
	else
		echo "teardown: port 8899 free"
	fi
	{
		echo "BENCH-THROUGHPUT RESULT — $STAMP — $RESULT"
		echo "evidence: $OUT"
		echo "---"
		echo "command: faketool -port 8899 -real-rate -real-rate-hz 20 (1024 ids x 20Hz = 20,480 frames/s)"
		echo "gates (every window): rows >= 15000, decode_errors delta == 0, p99 < 1000 ms"
		[ -n "$WINDOW_FAILS" ] && echo "failures: $WINDOW_FAILS"
		[ -f "$TSV" ] && cat "$TSV"
	} > "$RESULT_FILE"
	echo "=== bench-throughput end — $RESULT (result: $RESULT_FILE)"
	exit "$rc"
}
trap cleanup EXIT

# ── Preflight (copied verbatim from run-full-suite.sh Stage 0) ───────────────
echo "=== preflight"
FAILED=0
command -v docker >/dev/null || { echo "!! docker missing"; FAILED=1; }
command -v go >/dev/null || { echo "!! go missing"; FAILED=1; }
command -v mvn >/dev/null || { echo "!! mvn missing"; FAILED=1; }
command -v python3 >/dev/null || { echo "!! python3 missing"; FAILED=1; }
command -v curl >/dev/null || { echo "!! curl missing"; FAILED=1; }
java -version 2>&1 | grep -q 'version "17' || { echo "!! java 17 missing"; FAILED=1; }
port_open localhost 9123 || { echo "!! Fluss :9123 not reachable"; FAILED=1; }
[ -n "$O2_AUTH" ] || { echo "!! O2_AUTH_BASIC missing from .env"; FAILED=1; }
[ -f "$MANIFEST" ] || { echo "!! manifest not found: $MANIFEST"; FAILED=1; }
if pgrep -f 'com.trading.ingestion.IngestionService' >/dev/null 2>&1; then
	echo "!! an IngestionService is already running (native or containerized) — refusing to double-run; stop it first (docker compose stop ingestion for the container)"; FAILED=1
fi
if port_open 127.0.0.1 8899; then
	echo "!! port 8899 busy — a fake broker may already run"; FAILED=1
fi
if [ "$FAILED" = 1 ]; then
	echo "preflight FAILED"
	exit 1
fi
echo "preflight OK ($(now))"

# ── Builds (jar + bridge + faketool; same commands as the suite) ─────────────
echo "=== builds (jar + bridge + faketool)"
(cd "$CODE_DIR" && mvn -o -q -DskipTests package -pl 02_services/01_ingestion -am) \
	|| { fail "mvn package failed"; exit 1; }
(cd "$BRIDGE_DIR" && go build -o arrow-bridge .) \
	|| { fail "bridge build failed"; exit 1; }
(cd "$BRIDGE_DIR" && go build -tags faketool -o "$OUT/bin/faketool" ./faketool) \
	|| { fail "faketool build failed"; exit 1; }
[ -f "$JAR" ] || { fail "jar missing after build"; exit 1; }
echo "jar: $JAR bridge: $BRIDGE_DIR/arrow-bridge faketool: $OUT/bin/faketool"

# ── Start fake broker in real-rate mode ──────────────────────────────────────
echo "=== fake broker (-real-rate -real-rate-hz 20 → 20,480 frames/s)"
"$OUT/bin/faketool" -port 8899 -real-rate -real-rate-hz 20 \
	> "$OUT/bench/faketool.log" 2>&1 &
FAKETOOL_PID=$!
for _ in $(seq 1 30); do port_open 127.0.0.1 8899 && break; sleep 1; done
port_open 127.0.0.1 8899 || { fail "faketool did not open :8899"; exit 1; }

# ── Build + start ingestion container (fresh image carries the async writer
#    and the 20ms client linger) ──────────────────────────────────────────────
echo "=== ingestion container up (fresh image)"
(cd "$DOCKER_DIR" && docker compose --env-file .env --env-file secrets.env build ingestion) \
	|| { fail "compose build ingestion failed"; exit 1; }
(cd "$DOCKER_DIR" && SOAK_JOURNAL_DIR="$OUT/bench/journal" \
	docker compose --env-file .env --env-file secrets.env \
	-f docker-compose.yml -f docker-compose.soak.yml \
	-f docker-compose.bench.yml up -d ingestion) \
	|| { fail "compose up ingestion failed"; exit 1; }

CONTAINER_HEALTH="none"
for _ in $(seq 1 60); do
	CONTAINER_HEALTH="$(docker inspect -f '{{.State.Health.Status}}' "$INGESTION_CONTAINER" 2>/dev/null || echo none)"
	[ "$CONTAINER_HEALTH" = healthy ] && break
	sleep 5
done
echo "container health: $CONTAINER_HEALTH"
[ "$CONTAINER_HEALTH" = healthy ] || { fail "container not healthy"; exit 1; }

# Full 1024-token subscription ack in the container journal (message field).
ACKS=0
for _ in $(seq 1 60); do
	[ -f "$OUT/bench/journal/ingestion.json" ] && {
		ACKS="$(grep -cE 'subscription_ack.*acknowledged[=:][[:space:]]?1024' \
			"$OUT/bench/journal/ingestion.json" 2>/dev/null || true)"
		[ "${ACKS:-0}" -ge 1 ] && break
	}
	sleep 1
done
echo "journal subscription acks (acknowledged=1024): ${ACKS:-0}"
[ "${ACKS:-0}" -ge 1 ] || { fail "no full subscription ack in journal"; exit 1; }

# ── Baseline ─────────────────────────────────────────────────────────────────
# The OTLP append counter restarts per process; O2 may still serve the previous
# process's last point for a few seconds after start. Settle past one emitter
# interval (10s) so baseline belongs to THIS container.
echo "=== baseline settle (15s for fresh OTLP counter)"
sleep 15
echo "=== baseline (O2)"
CNT0="$(num "$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')")"
ERR0="$(num "$(o2_query 'select value from "decode_errors" order by _timestamp desc limit 1')")"
echo "baseline: append_latency_ms_count=$CNT0 decode_errors=$ERR0"

# ── Three 60s measurement windows ────────────────────────────────────────────
echo "=== measurement: 3 x 60s windows"
{
	echo -e "window\trows_s\tp50_ms\tp99_ms\tdecode_errors_delta"
} > "$TSV"

quantile_or_mean() { # $1=metric p50|p99 $2=sum $3=count → ms value or empty
	local g
	g="$(o2_query "select value from \"append_latency_ms_$1\" order by _timestamp desc limit 1")"
	g="$(num "$g")"
	if [ -n "$g" ]; then
		echo "$g"
		return
	fi
	# Fallback: mean = sum / count (O2 exposes no quantiles for OTLP
	# histograms — count/sum/min/max only; confirmed at runtime).
	if [ -n "$2" ] && [ -n "$3" ] && [ "$3" -gt 0 ] 2>/dev/null; then
		echo $(( $2 / $3 ))
	else
		echo ""
	fi
}

for w in 1 2 3; do
	CNT_A="$(num "$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')")"
	ERR_A="$(num "$(o2_query 'select value from "decode_errors" order by _timestamp desc limit 1')")"
	[ -n "$CNT_A" ] && [ -n "$ERR_A" ] || { fail "window $w: metric sample unavailable at start"; continue; }
	sleep 60
	CNT_B="$(num "$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')")"
	ERR_B="$(num "$(o2_query 'select value from "decode_errors" order by _timestamp desc limit 1')")"
	SUM="$(num "$(o2_query 'select value from "append_latency_ms_sum" order by _timestamp desc limit 1')")"
	CNT_END="$(num "$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')")"
	if [ -z "$CNT_B" ] || [ -z "$ERR_B" ]; then
		fail "window $w: metric sample unavailable at end"
		continue
	fi

	rows=$(( CNT_B - CNT_A ))
	if [ "$rows" -lt 0 ]; then
		fail "window $w: append counter went backwards ($CNT_A → $CNT_B) — stale process data?"
		continue
	fi
	rows_s=$(( rows / 60 ))
	err_delta=$(( ERR_B - ERR_A ))
	p50="$(quantile_or_mean p50 "$SUM" "$CNT_END")"
	p99="$(quantile_or_mean p99 "$SUM" "$CNT_END")"
	[ -n "$p50" ] || p50="-1"
	[ -n "$p99" ] || p99="-1"

	verdict="PASS"
	if [ "$rows" -lt 15000 ]; then
		fail "window $w: rows=$rows < 15000"
		verdict="FAIL"
	fi
	if [ "$err_delta" -ne 0 ]; then
		fail "window $w: decode_errors delta=$err_delta != 0"
		verdict="FAIL"
	fi
	if [ "$p99" -ge 1000 ] 2>/dev/null; then
		fail "window $w: p99=$p99 >= 1000 ms"
		verdict="FAIL"
	fi
	[ "$verdict" = PASS ] || WINDOW_FAILS="$WINDOW_FAILS $w"
	printf '%d\t%d\t%s\t%s\t%d\t%s\n' "$w" "$rows_s" "$p50" "$p99" "$err_delta" "$verdict" >> "$TSV"
	echo "window $w: rows=$rows (${rows_s}/s) p50=${p50}ms p99=${p99}ms decode_errors_delta=$err_delta → $verdict"
	WINDOWS_DONE=$(( WINDOWS_DONE + 1 ))
done

if [ "$WINDOWS_DONE" -ne 3 ]; then
	fail "only $WINDOWS_DONE of 3 windows completed"
fi
if [ "$FAILED" = 1 ]; then
	echo "=== RESULT: FAIL (see failures above and $RESULT_FILE)"
	exit 1
fi
echo "=== RESULT: PASS (all windows)"
exit 0
