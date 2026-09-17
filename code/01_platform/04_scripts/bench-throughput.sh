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
# PASS (every window): rows >= MIN_RATE x EXPECTED_RPS x elapsed_s AND
#                      decode_errors delta == 0 AND p99 < 1000 ms.
#
# P6-001: this gate used to be a flat `rows >= 15000` over a hardcoded 60s — an
# effective 250 rows/s, ~82x below the 20,480 frames/s this run generates
# (1024 subscribed ids x 20 Hz). A PASS therefore proved almost nothing: the
# stream could arrive at ~2% of the expected rate, or stop altogether for most
# of the window, and the bench still went green. The floor is now derived from
# the expected rate and the window length actually measured (P6-032).
#
# Env overrides: BENCH_EXPECTED_RPS (20480), BENCH_MIN_RATE (0.90),
#                BENCH_WINDOW_S (60), BENCH_PORT (8899), OUT_DIR, O2_BASE_URL,
#                INGESTION_CONTAINER_NAME, INSTRUMENT_MANIFEST_HOST_PATH,
#                BENCH_BASELINE_SETTLE_S (15), BENCH_BASELINE_TRIES (6),
#                BENCH_WINDOWS (3).
#
# The baseline polls append_latency_ms_count until it ADVANCES (up to
# BENCH_BASELINE_TRIES x BENCH_BASELINE_SETTLE_S), because O2 keeps serving a
# just-stopped process's last sample until the new one flushes its first OTLP
# point. A frozen counter fails the run as `baseline counter frozen`; a window
# whose decode_errors counter goes backwards is skipped as a stale sample
# (CHG-176).
#
# The soak suite (run-full-suite.sh) is untouched: this bench only builds a
# fresh ingestion image, runs one container for 3 x 60s of measurement,
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
# P6-309: derived from this checkout (the same file docker-compose.soak.yml
# bind-mounts to /instruments/NSE_CM_EQUITY.csv) and overridable with the
# variable the compose files themselves honour. The old absolute path existed on
# one machine only, and nothing but this existence check ever read it.
MANIFEST="${INSTRUMENT_MANIFEST_HOST_PATH:-$PROJECT_ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv}"
# P6-310: both overridable — renaming the compose project or moving O2 used to
# leave the bench unable to point anywhere else.
O2_BASE="${O2_BASE_URL:-http://localhost:5080}"
INGESTION_CONTAINER="${INGESTION_CONTAINER_NAME:-${COMPOSE_PROJECT_NAME:-01_docker}-ingestion-1}"
# P6-001/P6-032: the rate this bench claims to generate (see the faketool
# invocation below: 1024 subscribed ids x 20 Hz) and the share of it that must
# actually arrive for a window to pass.
EXPECTED_RPS="${BENCH_EXPECTED_RPS:-20480}"
MIN_RATE="${BENCH_MIN_RATE:-0.90}"
WINDOW_S="${BENCH_WINDOW_S:-60}"
BENCH_PORT="${BENCH_PORT:-8899}"
# Three windows is the documented measurement; the count is a knob so a smoke
# run (or a test) can prove the gate wiring without paying three of them.
WINDOW_COUNT="${BENCH_WINDOWS:-3}"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="${OUT_DIR:-$PROJECT_ROOT/logs/soak/bench-$STAMP}"
mkdir -p "$OUT/bench/journal" "$OUT/bin"
# P1-137 dropped the ingestion container to uid 65532, which writes its
# journal into this host bind mount; the default 0775/uid-1000 mode denies
# it and the entrypoint fails closed (P1-137 follow-up gate).
chmod 0777 "$OUT/bench/journal"
RUN_LOG="$OUT/bench/run.log"
RESULT_FILE="$OUT/bench/result.txt"
TSV="$OUT/bench/bench-throughput.tsv"
: > "$RUN_LOG"

# P6-028: strip the key prefix instead of splitting on '='. `awk -F=` returned
# only the text before the value's FIRST '=', so a base64 secret ending in '='
# or '==' lost that padding and every request 401'd; quotes and CR survived too.
# (same reader as run-full-suite.sh)
read_o2_auth() { # $1 = env file
	local v
	v="$(awk '/^O2_AUTH_BASIC=/{sub(/^[^=]*=/,""); gsub(/\r/,""); gsub(/^["'"'"']|["'"'"']$/,""); print; exit}' "$1" 2>/dev/null || true)"
	printf '%s' "$v"
}
O2_AUTH="$(read_o2_auth "$DOCKER_DIR/.env")"
[ -z "$O2_AUTH" ] && O2_AUTH="$(read_o2_auth "$DOCKER_DIR/secrets.env")"
# P6-029: the credential now travels in a 0600 curl config file (-K) instead of
# a command line, where `ps`/`/proc/<pid>/cmdline` exposed it to every local
# user for the duration of the call. The EXIT trap removes the file.
O2_AUTH_FILE="$(umask 077; mktemp)"
printf 'header = "Authorization: Basic %s"\n' "$O2_AUTH" > "$O2_AUTH_FILE"

# Everything lands in run.log AND the console.
exec > >(tee -a "$RUN_LOG") 2>&1

echo "=== bench-throughput start $STAMP (out: $OUT)"

# ── Helpers ──────────────────────────────────────────────────────────────────
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
port_open() { # $1=host $2=port — TCP connect; the host argument is honoured
	# 2>/dev/null: callers ask about ports they expect to be CLOSED (preflight
	# checks, teardown verification) — bash's connect errors are not diagnostics.
	timeout 2 bash -c 'exec 3<>/dev/tcp/"$1"/"$2"' _ "$1" "$2" 2>/dev/null
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
	val="$(curl -s -m 15 -K "$O2_AUTH_FILE" -H 'Content-Type: application/json' \
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

# Non-numeric O2 replies (UNAVAILABLE / NO_HITS / NO_VALUE) → empty, which is
# what the callers test for. O2 serves counters as floats ("5637597.0").
# P6-711: a real numeric match. The old character-class test accepted "-", ".",
# "1.2.3" and "--5" (printf then warned and coerced them to 0/1) and rejected
# scientific notation that O2 can legitimately serve ("1e6").
num() { awk -v v="$1" 'BEGIN { if (v ~ /^[-+]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][-+]?[0-9]+)?$/) printf "%.0f\n", v }'; }

FAILED=0
RESULT="PASS"
WINDOW_FAILS=""
WINDOWS_DONE=0

fail() { FAILED=1; RESULT="FAIL"; echo "!! $*"; }

# ── Teardown (always) ────────────────────────────────────────────────────────
FAKETOOL_PID=""
cleanup() {
	local rc=$?
	rm -f "${O2_AUTH_FILE:-}"
	if [ "$WINDOWS_DONE" -ne "$WINDOW_COUNT" ]; then
		FAILED=1
		RESULT="FAIL"
		echo "!! bench aborted before completing all $WINDOW_COUNT windows ($WINDOWS_DONE/$WINDOW_COUNT) — result forced to FAIL"
	fi
	echo "-- teardown ($(now))"
	[ -n "$FAKETOOL_PID" ] && kill "$FAKETOOL_PID" 2>/dev/null || true
	[ -n "$FAKETOOL_PID" ] && wait "$FAKETOOL_PID" 2>/dev/null || true
	(cd "$DOCKER_DIR" && docker compose --env-file .env --env-file secrets.env \
		-f docker-compose.yml \
		-f docker-compose.soak.yml -f docker-compose.bench.yml stop ingestion) >/dev/null 2>&1 || true
	if port_open 127.0.0.1 "$BENCH_PORT"; then
		echo "!! teardown: port $BENCH_PORT still busy"
		FAILED=1
		RESULT="FAIL"
	else
		echo "teardown: port $BENCH_PORT free"
	fi
	{
		echo "BENCH-THROUGHPUT RESULT — $STAMP — $RESULT"
		echo "evidence: $OUT"
		echo "---"
		echo "command: faketool -port $BENCH_PORT -real-rate -real-rate-hz 20 (1024 ids x 20Hz = 20,480 frames/s)"
		echo "gates (every window): rows >= ${MIN_RATE} x ${EXPECTED_RPS}/s x elapsed_s, decode_errors delta == 0, p99 < 1000 ms"
		[ -n "$WINDOW_FAILS" ] && echo "failures: $WINDOW_FAILS"
		[ -f "$TSV" ] && cat "$TSV"
	} > "$RESULT_FILE"
	echo "=== bench-throughput end — $RESULT (result: $RESULT_FILE)"
	# P6-030: `rc` is only the status the script had already exited with. When the
	# failure is detected HERE — the broker would not die, the broker port stayed busy,
	# or a window was skipped — rc is still 0 on the PASS path, and `exit 0` told
	# CI the bench was green while result.txt said FAIL.
	if [ "$rc" -eq 0 ] && [ "$FAILED" = 1 ]; then
		echo "!! teardown detected a failure the exit path did not — exiting 1"
		rc=1
	fi
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
command -v timeout >/dev/null || { echo "!! timeout missing"; FAILED=1; }
# P6-712: accept any JDK >= 17. Grepping for the literal 'version "17' rejected
# 21 and 25 even though the build runs on them.
JAVA_MAJOR="$(java -version 2>&1 | awk -F'"' '/version/ {print $2; exit}' | awk -F. '{print ($1 == "1") ? $2 : $1}')"
[ -n "$JAVA_MAJOR" ] && [ "$JAVA_MAJOR" -ge 17 ] 2>/dev/null \
	|| { echo "!! java >= 17 required (found: ${JAVA_MAJOR:-none})"; FAILED=1; }
port_open localhost 9123 || { echo "!! Fluss :9123 not reachable"; FAILED=1; }
[ -n "$O2_AUTH" ] || { echo "!! O2_AUTH_BASIC missing from .env"; FAILED=1; }
[ -f "$MANIFEST" ] || { echo "!! manifest not found: $MANIFEST"; FAILED=1; }
if pgrep -f 'com.trading.ingestion.IngestionService' >/dev/null 2>&1; then
	echo "!! an IngestionService is already running (native or containerized) — refusing to double-run; stop it first (docker compose stop ingestion for the container)"; FAILED=1
fi
if port_open 127.0.0.1 "$BENCH_PORT"; then
	echo "!! port $BENCH_PORT busy — a fake broker may already run"; FAILED=1
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
"$OUT/bin/faketool" -port "$BENCH_PORT" -real-rate -real-rate-hz 20 \
	> "$OUT/bench/faketool.log" 2>&1 &
FAKETOOL_PID=$!
for _ in $(seq 1 30); do port_open 127.0.0.1 "$BENCH_PORT" && break; sleep 1; done
port_open 127.0.0.1 "$BENCH_PORT" || { fail "faketool did not open :$BENCH_PORT"; exit 1; }

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
	# P6-313: an image with no HEALTHCHECK has no .State.Health, so the old
	# template errored, normalized to "none", and the bench always failed on a
	# container that was up. Report Running when there is no health field.
	CONTAINER_HEALTH="$(docker inspect -f \
		'{{if .State.Health}}{{.State.Health.Status}}{{else}}running={{.State.Running}}{{end}}' \
		"$INGESTION_CONTAINER" 2>/dev/null || echo none)"
	case "$CONTAINER_HEALTH" in
		healthy|running=true) break ;;
	esac
	sleep 5
done
echo "container health: $CONTAINER_HEALTH"
case "$CONTAINER_HEALTH" in
	healthy|running=true) ;;
	*) fail "container not healthy (state: $CONTAINER_HEALTH)"; exit 1 ;;
esac

# Full 1024-token subscription ack in the container journal (message field).
# P1-132 names the journal ingestion-${HOST}-${VM_ID}.json (a shared volume must
# not interleave writers), so the old fixed ingestion.json matched nothing after
# 2026-09-07 and this gate could never pass. Glob the directory instead: it also
# covers the root-era name and any future per-replica naming.
JOURNAL_DIR="$OUT/bench/journal"
ACKS=0
for _ in $(seq 1 60); do
	# P6-314: the old pattern matched a bare `1024` with at most one optional
	# space, so `acknowledged=10240` also counted, and it counted matching
	# LINES rather than acks. Anchor the token and allow any whitespace.
	ACKS="$(grep -hoE 'subscription_ack.*acknowledged[=:][[:space:]]*1024\b' \
		"$JOURNAL_DIR"/ingestion*.json 2>/dev/null | wc -l)"
	[ "${ACKS:-0}" -ge 1 ] && break
	sleep 1
done
echo "journal subscription acks (acknowledged=1024): ${ACKS:-0}"
[ "${ACKS:-0}" -ge 1 ] || { fail "no full subscription ack in journal"; exit 1; }

# ── Baseline ─────────────────────────────────────────────────────────────────
# The OTLP append counter restarts per process; O2 may still serve the previous
# process's last point for a few seconds after start. Settle past one emitter
# interval (10s) so baseline belongs to THIS container.
# P6-315: this is also the seam the tests use to prove the baseline is
# validated, so it names the poll INTERVAL rather than a fixed wait. 15s is one
# emitter interval, which no test needs to pay.
SETTLE_S="${BENCH_BASELINE_SETTLE_S:-15}"
# A fixed sleep cannot prove the sample belongs to THIS process. On 2026-09-15
# the new container's first OTLP point landed at exactly settle+0, so the
# baseline read the DEAD process's tail (append_latency_ms_count=0,
# decode_errors=7) while window 1 read the live one — decode_errors went
# 7 -> 3 and the window failed as "delta=-4 != 0", blaming the feed for junk.
# Poll for the counter to ADVANCE instead: a stopped process cannot advance,
# and the previous container is stopped above, so advancement proves the sample
# is this process's. Both counters ride in one OTLP payload on the same 10s
# tick, so a fresh append sample proves the same-moment decode_errors sample is
# fresh too. SETTLE_S is the poll interval, so the tests stay free.
BASE_TRIES="${BENCH_BASELINE_TRIES:-6}"
echo "=== baseline freshness (poll ${BASE_TRIES}x${SETTLE_S}s for a live counter)"
CNT0=""
prev=""
last=""
for _ in $(seq 1 "$BASE_TRIES"); do
	CNT0="$(num "$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')")"
	if [ -n "$CNT0" ]; then
		if [ -n "$prev" ] && [ "$CNT0" -gt "$prev" ]; then
			break
		fi
		prev="$CNT0"
		last="$CNT0"
	fi
	CNT0=""
	sleep "$SETTLE_S"
done
# No advance after the full budget: the emitter is not producing samples (or
# the broker is not feeding the appends it counts). Either way there is nothing
# to measure, and the per-window gates would only blame throughput and latency.
if [ -z "$CNT0" ]; then
	# Frozen and absent are different faults, so they get different messages.
	if [ -n "$last" ]; then
		fail "baseline counter frozen at $last across ${BASE_TRIES} reads over ${SETTLE_S}s each — O2 serves a sample but the container's OTLP emitter (or the broker feeding it) is not advancing it"
	else
		fail "baseline unavailable (count='' errors='') from $O2_BASE — is O2 up and the emitter running?"
	fi
	exit 1
fi
echo "=== baseline (O2)"
ERR0="$(num "$(o2_query 'select value from "decode_errors" order by _timestamp desc limit 1')")"
echo "baseline: append_latency_ms_count=$CNT0 decode_errors=$ERR0"
# P6-315: this is the "is O2 serving THIS run's metrics?" probe, and it used to
# be echoed and ignored — an O2 that answered nothing still burned the full ~3
# minutes failing window after window. Fail here instead, while it is cheap.
if [ -z "$CNT0" ] || [ -z "$ERR0" ]; then
	fail "baseline unavailable (count='${CNT0:-}' errors='${ERR0:-}') from $O2_BASE — is O2 up and the emitter running?"
	exit 1
fi

# ── Three measurement windows ────────────────────────────────────────────────
echo "=== measurement: ${WINDOW_COUNT} x ${WINDOW_S}s windows"
{
	echo -e "window\trows_s\tp50_ms\tp99_ms\tdecode_errors_delta\tverdict"
} > "$TSV"

# P6-032: the window is measured around the sleep, and the rate is divided by
# the elapsed time actually observed. The old code slept a fixed 60s and always
# divided by 60, even though each o2_query can spend up to 15s in curl, so the
# real window was longer than the divisor and the reported rate was inflated.
# The threshold scales with the same measured width, so it means the same thing
# whatever the window costs.
win_start_ms() { date +%s%3N; }

window_mean_ms() { # $1=sum delta $2=count delta → ms mean over THIS window
	# P6-031: the previous fallback divided lifetime-cumulative SUM by
	# lifetime-cumulative CNT_END, both read at one instant, so the value
	# labelled p50/p99 was the mean of the whole process uptime — recent
	# tail dilution included. Window deltas only.
	if [ -n "$1" ] && [ -n "$2" ] && [ "$2" -gt 0 ] 2>/dev/null; then
		echo $(( $1 / $2 ))
	else
		echo ""
	fi
}

# P6-713: every early exit below used to `continue` without touching
# WINDOW_FAILS, so a window skipped for a missing sample was absent from both
# the TSV and result.txt's failures list — the artefact that is supposed to
# say what went wrong. Record it.
skip_window() { # $1=window $2=reason
	fail "window $1: $2"
	WINDOW_FAILS="$WINDOW_FAILS $1"
	printf '%d\t\t\t\t\t%s\n' "$1" "SKIP" >> "$TSV"
}

for w in $(seq 1 "$WINDOW_COUNT"); do
	# P6-312: the broker was checked once at startup and never again, so a
	# faketool that died mid-run made all three windows fail with throughput and
	# latency verdicts that blamed the pipeline. Fail fast with the real cause.
	if [ -n "${FAKETOOL_PID:-}" ] && ! kill -0 "$FAKETOOL_PID" 2>/dev/null; then
		skip_window "$w" "fake broker died (pid $FAKETOOL_PID) — no throughput measured"
		break
	fi
	CNT_A="$(num "$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')")"
	ERR_A="$(num "$(o2_query 'select value from "decode_errors" order by _timestamp desc limit 1')")"
	SUM_A="$(num "$(o2_query 'select value from "append_latency_ms_sum" order by _timestamp desc limit 1')")"
	[ -n "$CNT_A" ] && [ -n "$ERR_A" ] || { skip_window "$w" "metric sample unavailable at start"; continue; }
	T_A="$(win_start_ms)"
	sleep "$WINDOW_S"
	CNT_B="$(num "$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')")"
	ERR_B="$(num "$(o2_query 'select value from "decode_errors" order by _timestamp desc limit 1')")"
	SUM_B="$(num "$(o2_query 'select value from "append_latency_ms_sum" order by _timestamp desc limit 1')")"
	# T_B is read after the closing samples, so the divisor can only be a shade
	# larger than the true counter span — a slightly stiffer gate, never a laxer one.
	T_B="$(win_start_ms)"
	if [ -z "$CNT_B" ] || [ -z "$ERR_B" ]; then
		skip_window "$w" "metric sample unavailable at end"
		continue
	fi

	rows=$(( CNT_B - CNT_A ))
	if [ "$rows" -lt 0 ]; then
		skip_window "$w" "append counter went backwards ($CNT_A → $CNT_B) — stale process data?"
		continue
	fi
	elapsed_s="$(awk -v a="$T_A" -v b="$T_B" 'BEGIN { printf "%.1f", (b - a) / 1000 }')"
	if awk -v e="$elapsed_s" 'BEGIN { exit !(e > 0) }'; then :; else
		skip_window "$w" "window measured 0s"
		continue
	fi
	rows_s="$(awk -v r="$rows" -v e="$elapsed_s" 'BEGIN { printf "%d", r / e }')"
	err_delta=$(( ERR_B - ERR_A ))
	# The rows counter above already refuses to measure across a process
	# boundary; the decode-error counter needs the same guard. A live counter
	# cannot decrease, so a negative delta means the two samples came from
	# different processes — unmeasurable, not evidence of bad ticks.
	if [ "$err_delta" -lt 0 ]; then
		skip_window "$w" "decode_errors counter went backwards ($ERR_A → $ERR_B) — stale process data?"
		continue
	fi
	# Gauges first, as the header documents; the window-delta mean is the
	# fallback for when O2 exposes no quantiles for the OTLP histogram.
	p50="$(num "$(o2_query 'select value from "append_latency_ms_p50" order by _timestamp desc limit 1')")"
	p99="$(num "$(o2_query 'select value from "append_latency_ms_p99" order by _timestamp desc limit 1')")"
	if [ -z "$p50" ] || [ -z "$p99" ]; then
		SUM_DELTA=""
		if [ -n "$SUM_A" ] && [ -n "$SUM_B" ] && [ "$rows" -gt 0 ]; then
			SUM_DELTA=$(( SUM_B - SUM_A ))
			[ "$SUM_DELTA" -ge 0 ] || SUM_DELTA=""
		fi
		if [ -n "$SUM_DELTA" ]; then
			mean="$(window_mean_ms "$SUM_DELTA" "$rows")"
			[ -n "$p50" ] || p50="$mean"
			[ -n "$p99" ] || p99="$mean"
		fi
	fi

	verdict="PASS"
	# P6-001: the floor is derived from the expected rate and the MEASURED
	# window width — not a flat row count that any trickle satisfied.
	rows_min="$(awk -v e="$EXPECTED_RPS" -v m="$MIN_RATE" -v s="$elapsed_s" \
		'BEGIN { printf "%.0f", e * m * s }')"
	if awk -v r="$rows" -v n="$rows_min" 'BEGIN { exit !(r < n) }'; then
		fail "window $w: rows=$rows < $rows_min (${MIN_RATE} x ${EXPECTED_RPS}/s x ${elapsed_s}s)"
		verdict="FAIL"
	fi
	if [ "$err_delta" -ne 0 ]; then
		fail "window $w: decode_errors delta=$err_delta != 0"
		verdict="FAIL"
	fi
	# P6-033: a latency sample that could not be read was reported as -1, and
	# `-1 >= 1000` is false — so the window passed with no latency evidence at
	# all. Unmeasurable latency is a FAIL.
	if [ -z "$p99" ]; then
		fail "window $w: no latency sample (neither a p99 gauge nor sum/count) — cannot verify p99 < 1000 ms"
		p50="-1"; p99="-1"
		verdict="FAIL"
	elif [ "$p99" -ge 1000 ] 2>/dev/null; then
		fail "window $w: p99=$p99 >= 1000 ms"
		verdict="FAIL"
	fi
	[ "$verdict" = PASS ] || WINDOW_FAILS="$WINDOW_FAILS $w"
	printf '%d\t%s\t%s\t%s\t%d\t%s\n' "$w" "$rows_s" "$p50" "$p99" "$err_delta" "$verdict" >> "$TSV"
	echo "window $w: rows=$rows (${rows_s}/s over ${elapsed_s}s) p50=${p50}ms p99=${p99}ms decode_errors_delta=$err_delta → $verdict"
	WINDOWS_DONE=$(( WINDOWS_DONE + 1 ))
done

if [ "$WINDOWS_DONE" -ne "$WINDOW_COUNT" ]; then
	fail "only $WINDOWS_DONE of $WINDOW_COUNT windows completed"
fi
if [ "$FAILED" = 1 ]; then
	echo "=== RESULT: FAIL (see failures above and $RESULT_FILE)"
	exit 1
fi
echo "=== RESULT: PASS (all windows)"
exit 0
