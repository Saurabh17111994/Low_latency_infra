#!/usr/bin/env bash
# =============================================================================
# run-full-suite.sh — Steps 1-4 of the remaining plan, ONE unattended run.
#
#   Stage 0a Python unit suites (tests/ — incl. the ING-TCP-002
#            reconcile-compare comparator) — fast static fail before builds
#   Stage 1  Monday verification gates (Go -race, E2E binaries, docker build
#            smoke, Java full gate FLUSS+MANIFEST+PERF+E2E, full doc audit
#            incl. C16 env-key drift + beyond-scanner sweeps, schema/perf
#            cert, SIGTERM-drain regression)
#   Stage 2  100-cycle reconnect marathon (wall clock, REAL backoff) against
#            the fake HFT broker on the host — journal + FD/thread evidence
#   Stage 3  Container runtime run — ingestion image in compose against the
#            fake broker: readiness healthcheck, O2 metrics from the
#            containerized emitter, journal on a host bind mount, one
#            crash-restart cycle (soak-reconnect-loop.sh, budget=1)
#   Stage 4  7-hour soak (overnight) in the container, forced 10s feed
#            interruptions at minutes 10/60/180, hourly snapshots
#            (journal + O2 counters), headroom scan, teardown
#
# The suite needs NO human or AI attention between stages: every stage fails
# fast (exit != 0) with the evidence preserved under logs/soak/full-suite-<ts>/.
# A SUMMARY.txt is always written at the end.
#
# Stage 4 is OFF by default (user decision 2026-08-09): the suite stops after
# Stage 3. Enable the overnight soak with RUN_STAGE_4=true.
#
# Prereqs (checked in Stage 0): docker compose stack up (Fluss :9123, O2
# :5080), JDK 17, Maven (offline ~/.m2 warm), Go, shellcheck, port 8899 free,
# no running IngestionService, approved 1024-instrument manifest present.
#
# Usage:  ./run-full-suite.sh            # Stages 1-3, then stops
#         RUN_STAGE_4=true ./run-full-suite.sh   # Stages 1-4 (7h soak)
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
OUT="${OUT_DIR:-$PROJECT_ROOT/logs/soak/full-suite-$STAMP}"
mkdir -p "$OUT/marathon/journal" "$OUT/soak/journal" "$OUT/bin" "$OUT/reconnect" "$OUT/gates"
RUN_LOG="$OUT/run.log"
SUMMARY="$OUT/SUMMARY.txt"
: > "$RUN_LOG"

# Never echo credentials: O2 auth header value read from .env, kept in a var.
O2_AUTH="$(awk -F= '/^O2_AUTH_BASIC=/{print $2; exit}' "$DOCKER_DIR/.env" 2>/dev/null || true)"

# Everything the operator needs lands in run.log AND the console.
exec > >(tee -a "$RUN_LOG") 2>&1

echo "=== full-suite start $STAMP (out: $OUT)"

# ── Stage bookkeeping ─────────────────────────────────────────────────────────
declare -A STAGE
RESULT="RUNNING"
stage_pass() { STAGE["$1"]="PASS"; echo "=== Stage $1: PASS"; }
stage_fail() { STAGE["$1"]="FAIL"; echo "=== Stage $1: FAIL — $2"; }

write_summary() {
	{
		echo "FULL-SUITE SUMMARY — $STAMP — result: $RESULT"
		echo "evidence: $OUT"
		echo "---"
		echo "reconcile: bootstrap_ok=${BOOTSTRAP_OK:-0} owned_ok=${RECONCILE_OK:-0} (see $OUT/bootstrap/reconcile-after.txt)"
		for s in 1 2 3 4; do
			echo "stage $s: ${STAGE[$s]:-NOT-REACHED}"
		done
		echo "---"
		echo "gates evidence: ${GATES_EVIDENCE:-none}"
		echo "marathon: acks=${MARATHON_ACKS:-0} max_epoch=${MARATHON_MAXEPOCH:-0} journal=$OUT/marathon/journal/ingestion.json monitor=$OUT/marathon/monitor.log"
		echo "container: health=${CONTAINER_HEALTH:-n/a} reconnect_cycles=${RECONNECT_CYCLES:-0}"
		echo "soak: append_count_start=${APPEND0:-n/a} append_count_end=${APPEND1:-n/a} recoveries=${RECOVERIES:-0}/3 snapshots=$OUT/soak/snapshots.tsv monitor=$OUT/soak/monitor.log"
	} > "$SUMMARY"
}
trap 'write_summary' EXIT

# ── Helpers ───────────────────────────────────────────────────────────────────
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
port_open() { # $1=host $2=port — listener check via ss (bash /dev/tcp is unreliable here)
	ss -ltn 2>/dev/null | grep -qE "[::0-9.*]:$2[[:space:]]"
}

o2_query() { # $1 = SQL → latest value column (or UNAVAILABLE)
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

journal_acks() { # $1 = journal path
	awk '/subscription_ack/{n++} END{print n+0}' "$1" 2>/dev/null || echo 0
}
journal_maxepoch() { # $1 = journal path
	awk -F'epoch=' '{n=split($2,a,"[ ,\"]"); if (a[1]+0>m) m=a[1]+0} END{print m+0}' "$1" 2>/dev/null || echo 0
}
journal_errors() { # $1 = journal path, $2 = level (ERROR|WARN)
	grep -c "\"level\":\"$2\"" "$1" 2>/dev/null || true
}

wait_until() { # $1 = epoch second
	while [ "$(date +%s)" -lt "$1" ]; do sleep 20; done
}

# ── Stage 0: preflight ────────────────────────────────────────────────────────
echo "=== Stage 0: preflight"
FAILED=0
command -v docker >/dev/null || { echo "!! docker missing"; FAILED=1; }
command -v go >/dev/null || { echo "!! go missing"; FAILED=1; }
command -v mvn >/dev/null || { echo "!! mvn missing"; FAILED=1; }
command -v shellcheck >/dev/null || { echo "!! shellcheck missing (gates static stage)"; FAILED=1; }
command -v python3 >/dev/null || { echo "!! python3 missing"; FAILED=1; }
command -v curl >/dev/null || { echo "!! curl missing"; FAILED=1; }
java -version 2>&1 | grep -q 'version "17' || { echo "!! java 17 missing"; FAILED=1; }
command -v javac >/dev/null || { echo "!! javac missing (dropper compile)"; FAILED=1; }
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
	stage_fail 0 "preflight — see run.log"
	RESULT="FAIL"
	exit 1
fi
echo "preflight OK ($(now))"

# ── Stage 0a: Python unit suites (fast static fail) ─────────────────────────
# The ING-TCP-002 reconcile-compare suite lives here (tests/test_reconcile_compare.py)
# and underpins the count-based losslessness proof; docs-audit C16 (env-key
# drift) runs inside Stage 1's Monday gates after the Java suite.
echo "=== Stage 0a: Python unit suites (reconcile-compare + gate helpers)"
if ! python3 -m unittest discover -s "$SCRIPT_DIR/tests" -p "test_*.py" \
	> "$OUT/gates/python-tests.log" 2>&1; then
	stage_fail 0 "python unit suites failed — see $OUT/gates/python-tests.log"
	RESULT="FAIL"
	exit 1
fi
echo "python unit suites OK (reconcile-compare comparator + gate helpers) — $(now)"

# ── Stage 0b: builds ──────────────────────────────────────────────────────────
echo "=== Stage 0b: builds (jar + bridge + faketool + dropper)"
(cd "$CODE_DIR" && mvn -o -q -DskipTests package -pl 02_services/01_ingestion -am) \
	|| { stage_fail 0 "mvn package failed"; RESULT="FAIL"; exit 1; }
# The committed arrow-bridge binary can go stale vs the Go sources (identity
# fields etc.) — always rebuild from current sources.
(cd "$BRIDGE_DIR" && go build -o arrow-bridge .) \
	|| { stage_fail 0 "bridge build failed"; RESULT="FAIL"; exit 1; }
(cd "$BRIDGE_DIR" && go build -tags faketool -o "$OUT/bin/faketool" ./faketool) \
	|| { stage_fail 0 "faketool build failed"; RESULT="FAIL"; exit 1; }
[ -f "$JAR" ] || { stage_fail 0 "jar missing after build"; RESULT="FAIL"; exit 1; }
echo "jar: $JAR bridge: $BRIDGE_DIR/arrow-bridge faketool: $OUT/bin/faketool"

# ── Stage 0c: schema reconcile + bootstrap ────────────────────────────────────
# The live dev Fluss cluster predates Phase 6 (28-col raw_table_1). The v2
# DDL gate is production-gated, and DdlBootstrap.ensureTables is create-only,
# so the owned tables with a stale column count are DROPPED first (dev-only
# tool; non-owned tables untouched) and then recreated by a short bootstrap
# run with ALLOW_RUNTIME_DDL=true (the documented local-dev path). After it,
# all later stages run in the PRODUCTION posture (verifyTables read-only).
echo "=== Stage 0c: schema reconcile + bootstrap — $(now)"
cat > "$OUT/bin/FlussDropTables.java" <<'JAVA'
// FlussDropTables — DEV-ONLY schema reconciliation helper for the unattended
// full-suite run. Drops owned tables whose column count does not match the
// authoritative DDL so the create-only DdlBootstrap can rebuild them.
// Usage: FlussDropTables [--probe] table:expectedCols ...
public final class FlussDropTables {
    public static void main(String[] args) throws Exception {
        boolean probe = false;
        int i = 0;
        if (args.length > 0 && args[0].equals("--probe")) {
            probe = true;
            i = 1;
        }
        if (args.length - i < 1) {
            System.err.println("usage: FlussDropTables [--probe] table:expectedCols ...");
            System.exit(2);
        }
        String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "localhost:9123");
        org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (org.apache.fluss.client.Connection c =
                        org.apache.fluss.client.ConnectionFactory.createConnection(conf);
                org.apache.fluss.client.admin.Admin admin = c.getAdmin()) {
            for (; i < args.length; i++) {
                String[] parts = args[i].split(":");
                String table = parts[0];
                int expected = Integer.parseInt(parts[1]);
                org.apache.fluss.metadata.TablePath path =
                        org.apache.fluss.metadata.TablePath.of("default", table);
                if (!admin.tableExists(path).get()) {
                    System.out.println(table + ": absent (nothing to do)");
                    continue;
                }
                int actual = admin.getTableSchema(path).get().getSchema().getColumns().size();
                if (actual == expected) {
                    System.out.println(table + ": ok (" + actual + " cols, matches DDL)");
                } else if (probe) {
                    System.out.println(table + ": MISMATCH would-drop actual=" + actual
                            + " expected=" + expected);
                } else {
                    admin.dropTable(path, false).get();
                    System.out.println(table + ": DROPPED actual=" + actual + " expected=" + expected);
                }
            }
        }
    }
}
JAVA
javac -cp "$JAR" -d "$OUT/bin" "$OUT/bin/FlussDropTables.java" \
	|| { stage_fail 0 "dropper compile failed"; RESULT="FAIL"; exit 1; }
DROPPER="java -cp $OUT/bin:$JAR FlussDropTables"
OWNED_EXPECT="raw_table_1:20 suspected_discontinuities:11 ingestion_quarantine:10"

echo "-- probe before:"
$DROPPER --probe $OWNED_EXPECT 2>/dev/null
echo "-- drop mismatched:"
$DROPPER $OWNED_EXPECT 2>/dev/null

# Short bootstrap run (ALLOW_RUNTIME_DDL=true) to recreate owned tables.
BOOTSTRAP_JOURNAL="$OUT/bootstrap/journal"
mkdir -p "$BOOTSTRAP_JOURNAL"
"$OUT/bin/faketool" -port 8899 -disconnect-every 0 -tick-interval-ms 500 \
	> "$OUT/bootstrap/faketool.log" 2>&1 &
FAKETOOL_PID=$!
for _ in $(seq 1 30); do port_open 127.0.0.1 8899 && break; sleep 1; done

export ARROW_HFT_URL="ws://127.0.0.1:8899"
export ARROW_BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge"
export ARROW_APP_ID="soak" ARROW_APP_SECRET="soaksecret" ARROW_TOKEN="soaktoken"
unset ARROW_USER_ID ARROW_PASSWORD ARROW_TOTP_KEY 2>/dev/null || true
export FLUSS_BOOTSTRAP="localhost:9123" FLUSS_BOOTSTRAP_SERVERS="localhost:9123"
export OTEL_COLLECTOR_HOST="localhost:4318"
export RAW_TABLE_NAME="raw_table_1"
export ARROW_MAX_EVENT_AGE_MS="5000" ARROW_MAX_FUTURE_EVENT_SKEW_MS="2000"
export ARROW_HFT_LATENCY_MS="50"
export GO_ARROW_SDK_VERSION="v0.2.0"
export ARROW_HFT_CONNECTIONS="1"
export INSTRUMENT_MANIFEST_PATH="$MANIFEST" ARROW_INSTRUMENT_MANIFEST="$MANIFEST"
export LOG_DIR="$BOOTSTRAP_JOURNAL"
export CLOCK_CHECK_REQUIRED="false"
export NTP_SERVER="ntp.ubuntu.com,time.google.com,in.pool.ntp.org"
export ALLOW_RUNTIME_DDL="true"

java --add-opens=java.base/java.nio=ALL-UNNAMED -Dlog.dir="$BOOTSTRAP_JOURNAL" \
	-cp "$JAR" com.trading.ingestion.IngestionService > "$OUT/bootstrap/java.out" 2>&1 &
BOOTSTRAP_JAVA_PID=$!

BOOTSTRAP_OK=0
for _ in $(seq 1 90); do # up to 180s
	if [ -f "$BOOTSTRAP_JOURNAL/ingestion.json" ] \
		&& [ "$(journal_acks "$BOOTSTRAP_JOURNAL/ingestion.json")" -ge 1 ] \
		&& grep -q 'tables ok' "$BOOTSTRAP_JOURNAL/ingestion.json"; then
		BOOTSTRAP_OK=1
		break
	fi
	kill -0 "$BOOTSTRAP_JAVA_PID" 2>/dev/null || { echo "!! bootstrap java exited early"; break; }
	sleep 2
done

kill -TERM "$BOOTSTRAP_JAVA_PID" 2>/dev/null || true
for _ in $(seq 1 30); do kill -0 "$BOOTSTRAP_JAVA_PID" 2>/dev/null || break; sleep 2; done
kill -9 "$BOOTSTRAP_JAVA_PID" 2>/dev/null || true
kill "$FAKETOOL_PID" 2>/dev/null || true
wait "$BOOTSTRAP_JAVA_PID" 2>/dev/null || true
wait "$FAKETOOL_PID" 2>/dev/null || true
unset ALLOW_RUNTIME_DDL

echo "-- probe after (expect ok for all three):"
$DROPPER --probe $OWNED_EXPECT 2>/dev/null | tee "$OUT/bootstrap/reconcile-after.txt"
RECONCILE_OK="$($DROPPER --probe $OWNED_EXPECT 2>/dev/null | grep -c ': ok ' || true)"
if [ "$BOOTSTRAP_OK" != 1 ] || [ "$RECONCILE_OK" != 3 ]; then
	stage_fail 0 "schema reconcile incomplete: bootstrap_ok=$BOOTSTRAP_OK reconcile_ok=$RECONCILE_OK"
	RESULT="FAIL"
	exit 1
fi
echo "schema reconcile OK: owned tables at DDL column counts ($(now))"

# ── Stage 1: Monday gates ─────────────────────────────────────────────────────
echo "=== Stage 1: Monday gates (run-monday-gates.sh) — $(now)"
if bash "$SCRIPT_DIR/run-monday-gates.sh" > "$OUT/gates/gates.out" 2>&1; then
	stage_pass 1
else
	stage_fail 1 "gates failed — see $OUT/gates/gates.out"
	RESULT="FAIL"
	exit 1
fi
GATES_EVIDENCE="$(ls -td "$PROJECT_ROOT"/logs/soak/monday-gates-* 2>/dev/null | head -1 || echo none)"
echo "gates evidence: $GATES_EVIDENCE"

# ── Stage 2: 100-cycle reconnect marathon (native, real backoff) ─────────────
echo "=== Stage 2: 100-cycle reconnect marathon — $(now)"
echo "-- artifacts from stage 0b: jar + faketool (no rebuild)"

MARATHON_JOURNAL="$OUT/marathon/journal"
export ARROW_HFT_URL="ws://127.0.0.1:8899"
export ARROW_BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge"
export ARROW_APP_ID="soak" ARROW_APP_SECRET="soaksecret" ARROW_TOKEN="soaktoken"
unset ARROW_USER_ID ARROW_PASSWORD ARROW_TOTP_KEY 2>/dev/null || true
export FLUSS_BOOTSTRAP="localhost:9123" FLUSS_BOOTSTRAP_SERVERS="localhost:9123"
export OTEL_COLLECTOR_HOST="localhost:4318"
export RAW_TABLE_NAME="raw_table_1"
export ARROW_MAX_EVENT_AGE_MS="5000" ARROW_MAX_FUTURE_EVENT_SKEW_MS="2000"
export ARROW_HFT_LATENCY_MS="50"
export GO_ARROW_SDK_VERSION="v0.2.0"
export ARROW_HFT_CONNECTIONS="1"
export INSTRUMENT_MANIFEST_PATH="$MANIFEST" ARROW_INSTRUMENT_MANIFEST="$MANIFEST"
export LOG_DIR="$MARATHON_JOURNAL"
export CLOCK_CHECK_REQUIRED="false"
export NTP_SERVER="ntp.ubuntu.com,time.google.com,in.pool.ntp.org"

echo "-- starting fake broker (disconnect-every=1, tick 500ms)"
"$OUT/bin/faketool" -port 8899 -disconnect-every 1 -tick-interval-ms 500 \
	> "$OUT/marathon/faketool.log" 2>&1 &
FAKETOOL_PID=$!
for _ in $(seq 1 30); do port_open 127.0.0.1 8899 && break; sleep 1; done

echo "-- starting native IngestionService (pid will be logged)"
java --add-opens=java.base/java.nio=ALL-UNNAMED -Dlog.dir="$MARATHON_JOURNAL" \
	-cp "$JAR" com.trading.ingestion.IngestionService > "$OUT/marathon/java.out" 2>&1 &
JAVA_PID=$!
echo "java pid=$JAVA_PID faketool pid=$FAKETOOL_PID"

LOG_FILE="$MARATHON_JOURNAL/ingestion.json" OUT_DIR="$OUT/marathon" \
	"$SCRIPT_DIR/soak-monitor.sh" 3700 10 > "$OUT/marathon/monitor.log" 2>&1 &
MONITOR_PID=$!

MARATHON_OK=0
for _ in $(seq 1 240); do # 240 x 15s = 60 min budget
	[ -f "$MARATHON_JOURNAL/ingestion.json" ] || { sleep 15; continue; }
	ACKS="$(journal_acks "$MARATHON_JOURNAL/ingestion.json")"
	if [ "${ACKS:-0}" -ge 100 ]; then MARATHON_OK=1; break; fi
	kill -0 "$JAVA_PID" 2>/dev/null || { echo "!! java exited early"; break; }
	sleep 15
done

MARATHON_ACKS="$(journal_acks "$MARATHON_JOURNAL/ingestion.json")"
MARATHON_MAXEPOCH="$(journal_maxepoch "$MARATHON_JOURNAL/ingestion.json")"
echo "marathon end: acks=$MARATHON_ACKS max_epoch=$MARATHON_MAXEPOCH (target ≥100)"

kill -TERM "$JAVA_PID" 2>/dev/null || true
for _ in $(seq 1 30); do kill -0 "$JAVA_PID" 2>/dev/null || break; sleep 2; done
kill -9 "$JAVA_PID" 2>/dev/null || true
kill "$MONITOR_PID" 2>/dev/null || true
kill "$FAKETOOL_PID" 2>/dev/null || true
wait "$JAVA_PID" 2>/dev/null || true
wait "$MONITOR_PID" 2>/dev/null || true
wait "$FAKETOOL_PID" 2>/dev/null || true

if [ "$MARATHON_OK" != 1 ] || [ "${MARATHON_MAXEPOCH:-0}" -lt 100 ]; then
	stage_fail 2 "marathon incomplete: acks=$MARATHON_ACKS max_epoch=${MARATHON_MAXEPOCH:-0}"
	RESULT="FAIL"
	exit 1
fi
stage_pass 2
echo "marathon evidence: journal=$MARATHON_JOURNAL/ingestion.json monitor=$OUT/marathon/monitor.log errors=$(journal_errors "$MARATHON_JOURNAL/ingestion.json" ERROR) warns=$(journal_errors "$MARATHON_JOURNAL/ingestion.json" WARN)"

# ── Stage 3: container runtime run ────────────────────────────────────────────
echo "=== Stage 3: container runtime run — $(now)"
echo "-- docker compose build ingestion"
(cd "$DOCKER_DIR" && docker compose build ingestion) > "$OUT/gates/docker-build.log" 2>&1 \
	|| { stage_fail 3 "docker build failed — see $OUT/gates/docker-build.log"; RESULT="FAIL"; exit 1; }

SOAK_JOURNAL="$OUT/soak/journal"
# R-221: Stage 2's faketool (disconnect-every=1) is killed after the marathon,
# but the container's readiness marker requires a live feed (brokerConnected +
# subscriptionComplete + fresh ticks). Start a no-drop soak-mode broker here;
# Stage 4 restarts its own, so this one is stopped after the reconnect cycle.
echo "-- starting fake broker in soak mode (no drops, tick 500ms)"
"$OUT/bin/faketool" -port 8899 -disconnect-every 0 -tick-interval-ms 500 \
	> "$OUT/reconnect/faketool.log" 2>&1 &
STAGE3_FAKETOOL_PID=$!
for _ in $(seq 1 30); do port_open 127.0.0.1 8899 && break; sleep 1; done
echo "-- compose up ingestion (soak override)"
(cd "$DOCKER_DIR" && SOAK_JOURNAL_DIR="$SOAK_JOURNAL" \
	docker compose -f docker-compose.yml -f docker-compose.soak.yml up -d ingestion) \
	|| { stage_fail 3 "compose up failed"; RESULT="FAIL"; exit 1; }

CONTAINER_HEALTH="none"
for _ in $(seq 1 60); do
	CONTAINER_HEALTH="$(docker inspect -f '{{.State.Health.Status}}' "$INGESTION_CONTAINER" 2>/dev/null || echo none)"
	[ "$CONTAINER_HEALTH" = healthy ] && break
	sleep 5
done
echo "container health: $CONTAINER_HEALTH"

# First subscription ack in the container journal (readiness + feed OK).
CONT_ACKS=0
for _ in $(seq 1 60); do
	[ -f "$SOAK_JOURNAL/ingestion.json" ] && {
		CONT_ACKS="$(journal_acks "$SOAK_JOURNAL/ingestion.json")"
		[ "${CONT_ACKS:-0}" -ge 1 ] && break
	}
	sleep 2
done
echo "container journal acks: ${CONT_ACKS:-0}"

# O2 metrics from the containerized emitter (soft evidence — warn, don't fail).
O2_SLOT="$(o2_query 'select value from "bridge_slot_capacity_used_percent" order by _timestamp desc limit 1')"
O2_GOROUTINES="$(o2_query 'select value from "go_goroutines" order by _timestamp desc limit 1')"
echo "O2 from container emitter: slot_capacity_used_percent=$O2_SLOT go_goroutines=$O2_GOROUTINES"

if [ "$CONTAINER_HEALTH" != healthy ] || [ "${CONT_ACKS:-0}" -lt 1 ]; then
	stage_fail 3 "container unhealthy: health=$CONTAINER_HEALTH journal_acks=${CONT_ACKS:-0}"
	RESULT="FAIL"
	exit 1
fi

# One crash-restart cycle inside the container (MAX_BRIDGE_RESTARTS=1 budget).
echo "-- crash-restart cycle (soak-reconnect-loop.sh 1 8)"
LOG_FILE="$SOAK_JOURNAL/ingestion.json" OUT_DIR="$OUT/reconnect" CONTAINER="$INGESTION_CONTAINER" \
	"$SCRIPT_DIR/soak-reconnect-loop.sh" 1 8 \
	|| { stage_fail 3 "reconnect loop failed"; RESULT="FAIL"; exit 1; }
RECONNECT_CYCLES=1
kill "$STAGE3_FAKETOOL_PID" 2>/dev/null || true
wait "$STAGE3_FAKETOOL_PID" 2>/dev/null || true
stage_pass 3

# ── Stage 4 gate ─────────────────────────────────────────────────────────────
# The 7h soak is NOT started by default (user decision 2026-08-09): the suite
# stops after Stage 3. Set RUN_STAGE_4=true to enable the overnight soak.
if [ "${RUN_STAGE_4:-false}" != true ]; then
	echo "=== Stage 4 skipped (RUN_STAGE_4 != true) — suite stops after Stage 3"
	RESULT="PASS"
	exit 0
fi

# ── Stage 4: 7h soak (in the container) ───────────────────────────────────────
echo "=== Stage 4: 7h soak in container — $(now)"
echo "-- restarting fake broker in soak mode (no drops, tick 500ms)"
"$OUT/bin/faketool" -port 8899 -disconnect-every 0 -tick-interval-ms 500 \
	> "$OUT/soak/faketool.log" 2>&1 &
FAKETOOL_PID=$!
for _ in $(seq 1 30); do port_open 127.0.0.1 8899 && break; sleep 1; done

# Confirm the container feed resumes before starting the soak clock.
SOAK_ACK_BASE="$(journal_acks "$SOAK_JOURNAL/ingestion.json")"
for _ in $(seq 1 60); do
	[ "$(journal_acks "$SOAK_JOURNAL/ingestion.json")" -gt "$SOAK_ACK_BASE" ] && break
	sleep 2
done
echo "feed resumed in container journal (acks ${SOAK_ACK_BASE} → $(journal_acks "$SOAK_JOURNAL/ingestion.json"))"

SOAK_START="$(date +%s)"
APPEND0="$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')"
echo "soak start $(now): append_latency_ms_count=$APPEND0"

LOG_FILE="$SOAK_JOURNAL/ingestion.json" OUT_DIR="$OUT/soak" \
	"$SCRIPT_DIR/soak-monitor.sh" 25380 30 > "$OUT/soak/monitor.log" 2>&1 &
MONITOR_PID=$!

RECOVERIES=0
snapshot() { # $1 = label
	local acks epoch health o2a o2f
	acks="$(journal_acks "$SOAK_JOURNAL/ingestion.json")"
	epoch="$(journal_maxepoch "$SOAK_JOURNAL/ingestion.json")"
	health="$(docker inspect -f '{{.State.Health.Status}}' "$INGESTION_CONTAINER" 2>/dev/null || echo none)"
	o2a="$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')"
	o2f="$(o2_query 'select value from "otelcol_exporter_send_failed_metric_points" order by _timestamp desc limit 1')"
	echo -e "$(now)\t$1\tacks=$acks\tepoch=$epoch\thealth=$health\tappend=$o2a\tsend_failed=$o2f" | tee -a "$OUT/soak/snapshots.tsv"
}
verify_recovery() { # $1 = label — feed restore must produce a NEW ack + readiness
	local before after
	before="$(journal_acks "$SOAK_JOURNAL/ingestion.json")"
	after="$before"
	for _ in $(seq 1 60); do # up to 120s
		after="$(journal_acks "$SOAK_JOURNAL/ingestion.json")"
		[ "$after" -gt "$before" ] && break
		sleep 2
	done
	readiness="$(docker exec "$INGESTION_CONTAINER" sh -c 'cat /tmp/ingestion.ready 2>/dev/null' 2>/dev/null || echo unreachable)"
	if [ "$after" -gt "$before" ]; then
		RECOVERIES=$((RECOVERIES + 1))
		echo "recovery [$1] OK: ack ${before}→${after}, readiness='$readiness' ($(now))" | tee -a "$OUT/soak/recoveries.txt"
	else
		echo "recovery [$1] FAIL: no new ack within 120s, readiness='$readiness' ($(now))" | tee -a "$OUT/soak/recoveries.txt"
	fi
}
interrupt() { # $1 = label, $2 = minute
	echo "--- interruption [$1] at minute $2 ($(now))"
	kill -9 "$FAKETOOL_PID" 2>/dev/null || true
	wait "$FAKETOOL_PID" 2>/dev/null || true
	sleep 10 # feed fully down
	"$OUT/bin/faketool" -port 8899 -disconnect-every 0 -tick-interval-ms 500 \
		>> "$OUT/soak/faketool.log" 2>&1 &
	FAKETOOL_PID=$!
	echo "--- broker restarted pid=$FAKETOOL_PID ($(now))"
	verify_recovery "$1"
	snapshot "$1"
}

snapshot start
wait_until $((SOAK_START + 600))    # minute 10
interrupt i1 10
wait_until $((SOAK_START + 3600))   # minute 60
interrupt i2 60
snapshot h1
wait_until $((SOAK_START + 7200))
snapshot h2
wait_until $((SOAK_START + 10800))  # minute 180
interrupt i3 180
snapshot h3
wait_until $((SOAK_START + 14400))
snapshot h4
wait_until $((SOAK_START + 18000))
snapshot h5
wait_until $((SOAK_START + 21600))
snapshot h6
wait_until $((SOAK_START + 25200))
snapshot h7
wait "$MONITOR_PID" || true

APPEND1="$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1')"
echo "soak end $(now): append_latency_ms_count=$APPEND1 (start=$APPEND0)"
echo "recoveries: $RECOVERIES/3"

# ── Final evidence + teardown ─────────────────────────────────────────────────
echo "=== final evidence collection — $(now)"
echo "-- headroom scan"
LOG_FILE="$SOAK_JOURNAL/ingestion.json" OUT_DIR="$OUT/soak" \
	"$SCRIPT_DIR/soak-headroom.sh" "$SOAK_JOURNAL/ingestion.json" \
	> "$OUT/soak/headroom.out" 2>&1 || true
echo "-- tick viewer sample (last 3 persisted rows)"
(cd "$INGESTION_DIR" && timeout 15 env FLUSS_BOOTSTRAP=localhost:9123 RAW_TABLE_NAME=raw_table_1 \
	java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$JAR" com.trading.ingestion.TickTableViewer 3) \
	> "$OUT/soak/tick-viewer.out" 2>&1 || true
echo "-- docker health log"
docker inspect -f '{{json .State.Health.Log}}' "$INGESTION_CONTAINER" > "$OUT/soak/health-log.json" 2>/dev/null || true
echo "-- journal stats"
echo "acks=$(journal_acks "$SOAK_JOURNAL/ingestion.json") max_epoch=$(journal_maxepoch "$SOAK_JOURNAL/ingestion.json") errors=$(journal_errors "$SOAK_JOURNAL/ingestion.json" ERROR) warns=$(journal_errors "$SOAK_JOURNAL/ingestion.json" WARN)" | tee "$OUT/soak/journal-stats.txt"

echo "-- teardown"
kill "$FAKETOOL_PID" 2>/dev/null || true
kill "$MONITOR_PID" 2>/dev/null || true
wait "$FAKETOOL_PID" 2>/dev/null || true
wait "$MONITOR_PID" 2>/dev/null || true
(cd "$DOCKER_DIR" && SOAK_JOURNAL_DIR="$SOAK_JOURNAL" \
	docker compose -f docker-compose.yml -f docker-compose.soak.yml stop ingestion) \
	|| echo "!! compose stop ingestion failed (check manually)"

# ── Stage verdicts ─────────────────────────────────────────────────────────────
SOAK_FAIL=""
[ "$RECOVERIES" -ge 3 ] || SOAK_FAIL="recoveries=$RECOVERIES/3"
[ -f "$OUT/soak/monitor.log" ] || SOAK_FAIL="${SOAK_FAIL:-} monitor missing"
[ -f "$OUT/soak/snapshots.tsv" ] || SOAK_FAIL="${SOAK_FAIL:-} snapshots missing"
[ "$APPEND1" != "UNAVAILABLE" ] && [ "$APPEND1" != "$APPEND0" ] || SOAK_FAIL="${SOAK_FAIL:-} append counter did not advance"
if [ -n "$SOAK_FAIL" ]; then
	stage_fail 4 "soak evidence incomplete: $SOAK_FAIL"
	RESULT="FAIL"
else
	stage_pass 4
	RESULT="PASS"
fi

echo "=== full-suite finished: $RESULT ($(now))"
exit 0
