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
# P6-513: derive the manifest from the repo instead of an absolute home path;
# an explicit MANIFEST= in the environment still wins.
MANIFEST="${MANIFEST:-$PROJECT_ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv}"
O2_BASE="http://localhost:5080"
# The ingestion container is discovered after `compose up` (P6-513): docker
# inspect/exec take the id, and an id survives a renamed compose project.
INGESTION_CONTAINER="${INGESTION_CONTAINER:-}"

# Test seam: `RUN_FULL_SUITE_LIB=true . run-full-suite.sh` defines the helpers and
# returns without running a stage, so the helper tests can call them directly.
LIB_MODE="${RUN_FULL_SUITE_LIB:-false}"


# ── Stage bookkeeping ─────────────────────────────────────────────────────────
declare -A STAGE
RESULT="RUNNING"
stage_pass() { STAGE["$1"]="PASS"; echo "=== Stage $1: PASS"; }
stage_fail() { STAGE["$1"]="FAIL"; echo "=== Stage $1: FAIL — $2"; }

write_summary() {
	# P6-514: a `set -e` abort never sets a verdict; report FAIL, not RUNNING.
	[ "$RESULT" = RUNNING ] && RESULT="FAIL (aborted before a verdict)"
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
		echo "marathon: acks=${MARATHON_ACKS:-0} distinct_epochs=${MARATHON_EPOCHS:-0} max_epoch=${MARATHON_MAXEPOCH:-0} journal=$OUT/marathon/journal/ingestion.json monitor=$OUT/marathon/monitor.log"
		echo "container: health=${CONTAINER_HEALTH:-n/a} reconnect_cycles=${RECONNECT_CYCLES:-0}"
		echo "soak: append_count_start=${APPEND0:-n/a} append_count_end=${APPEND1:-n/a} recoveries=${RECOVERIES:-0}/3 snapshots=$OUT/soak/snapshots.tsv monitor=$OUT/soak/monitor.log"
	} > "$SUMMARY"
}
# P6-178: an abort must not leave the broker, monitor, java or container behind
# (a leaked container also trips the double-run guard on the next run). P6-183:
# the container is stopped here too, so the Stage-4 skip path stops it as well.
CONTAINER_STARTED=0
SOAK_KEEP_CONTAINER="${SOAK_KEEP_CONTAINER:-false}"
cleanup() {
	local rc=$? pid alive
	# TERM, bounded grace poll, then KILL: a plain `kill` leaves the child
	# visibly ALIVE for ~1 run in 2 on a loaded box (SIGTERM needs a scheduler
	# slice before `ps` stops listing the pid), which made the
	# cleanup-kills-children probe flaky. Stages 2/3 already use this
	# TERM→KILL shape; cleanup matches it. No `wait` here: the EXIT-trap shell
	# exits right after cleanup (no zombies possible), and `wait` on these
	# pids hung under the test's piped stdio.
	for pid in "${MONITOR_PID:-}" "${JAVA_PID:-}" "${BOOTSTRAP_JAVA_PID:-}" "${FAKETOOL_PID:-}" "${STAGE3_FAKETOOL_PID:-}"; do
		[ -n "$pid" ] || continue
		kill -TERM "$pid" 2>/dev/null || true
	done
	for _ in 1 2 3 4 5; do
		alive=0
		for pid in "${MONITOR_PID:-}" "${JAVA_PID:-}" "${BOOTSTRAP_JAVA_PID:-}" "${FAKETOOL_PID:-}" "${STAGE3_FAKETOOL_PID:-}"; do
			[ -n "$pid" ] || continue
			kill -0 "$pid" 2>/dev/null && alive=1
		done
		[ "$alive" = 0 ] && break
		sleep 0.1
	done
	for pid in "${MONITOR_PID:-}" "${JAVA_PID:-}" "${BOOTSTRAP_JAVA_PID:-}" "${FAKETOOL_PID:-}" "${STAGE3_FAKETOOL_PID:-}"; do
		[ -n "$pid" ] || continue
		kill -0 "$pid" 2>/dev/null || continue
		kill -KILL "$pid" 2>/dev/null || true
	done
	if [ "${CONTAINER_STARTED:-0}" = 1 ] && [ "$SOAK_KEEP_CONTAINER" != true ]; then
		(cd "$DOCKER_DIR" && SOAK_JOURNAL_DIR="${SOAK_JOURNAL:-}" \
			docker compose --env-file .env --env-file secrets.env -f docker-compose.yml -f docker-compose.soak.yml stop ingestion) >/dev/null 2>&1 \
			|| echo "!! cleanup: could not stop the ingestion container — stop it manually"
	fi
	rm -f "${O2_AUTH_FILE:-}"
	write_summary
	return $rc
}
trap cleanup EXIT
# ── Helpers ───────────────────────────────────────────────────────────────────
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# O2 auth is read without the `awk -F=` truncation that dropped base64 padding
# (P6-177) and is never placed on a command line (P6-179 — o2_query reads it
# from a 0600 curl config file with -K).
read_o2_auth() { # $1 = env file
	local v
	v="$(awk '/^O2_AUTH_BASIC=/{sub(/^[^=]*=/,""); gsub(/\r/,""); gsub(/^["'"'"']|["'"'"']$/,""); print; exit}' "$1" 2>/dev/null || true)"
	printf '%s' "$v"
}

port_open() { # $1=host $2=port — listener check via ss; 1 also means "cannot tell"
	local host="${1:-localhost}" port="${2:?port_open: port required}"
	command -v ss >/dev/null 2>&1 || { echo "port_open: ss not installed — cannot check $host:$port" >&2; return 1; }
	case "$host" in
		localhost|127.0.0.1|0.0.0.0|\[::1\]|::1) ;;
		*) echo "port_open: refusing non-local host '$host' — ss only sees local sockets" >&2; return 1 ;;
	esac
	ss -ltn 2>/dev/null | grep -qE ":${port}[[:space:]]"
}

# Journal statistics from the log4j2 JSON layout, one object per line:
# {"...","level":"INFO","loggerName":"...","message":"bridge lifecycle event=subscription_ack slot=hft-0 state=ACTIVE epoch=1 ..."}
# A marker counts only when it is that field's value, so a line that merely
# mentions subscription_ack in prose cannot pass for a cycle (P6-180); a file
# that is absent or has no match reports 0 rather than nothing (P6-516); a torn
# tail line is counted and skipped instead of hiding the rest of the file; and
# distinct/monotonic epochs are reported, so a gap cannot hide behind maxepoch
# (P6-180: max_epoch >= 100 does not prove 100 cycles).
journal_stats() { # $1 = journal path — prints key=value lines; missing file = zeros
	python3 - "${1:-/nonexistent}" <<'PYJ'
import glob, json, os, re, sys

# P1-132 names the journal ingestion-${HOST}-${VM_ID}.json (a volume shared with
# the collector must not interleave writers), so the fixed ingestion.json every
# caller passes has matched nothing since 2026-09-07 and every gate below read
# zeros. Resolve the real per-host name here, once, instead of at ~20 call sites:
# the newest file in the directory is this run's journal.
_path = sys.argv[1]
if not os.path.exists(_path) and os.path.basename(_path) == "ingestion.json":
    _cands = glob.glob(os.path.join(os.path.dirname(_path), "ingestion*.json"))
    if _cands:
        _path = max(_cands, key=os.path.getmtime)

ACK = re.compile(r"\bevent=subscription_ack\b")
EPOCH = re.compile(r"\bepoch=(\d+)\b")
acks = errors = warns = torn = 0
epochs = []
try:
    fh = open(_path, encoding="utf-8", errors="replace")
except OSError:
    fh = None
if fh is not None:
    with fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except ValueError:
                torn += 1          # torn tail line or a non-JSON banner
                continue
            if not isinstance(row, dict):
                continue
            if row.get("level") == "ERROR":
                errors += 1
            elif row.get("level") == "WARN":
                warns += 1
            message = row.get("message")
            if not isinstance(message, str):
                continue
            if ACK.search(message):
                acks += 1
            found = EPOCH.search(message)
            if found:
                epochs.append(int(found.group(1)))
print(f"acks={acks}")
print(f"errors={errors}")
print(f"warns={warns}")
print(f"torn={torn}")
print(f"maxepoch={max(epochs) if epochs else 0}")
print(f"distinct_epochs={len(set(epochs))}")
print(f"monotonic={1 if all(b >= a for a, b in zip(epochs, epochs[1:])) else 0}")
PYJ
}
o2_query() { # $1 = SQL, $2 = window in microseconds (default 1h) -> latest value or UNAVAILABLE
	local sql="$1" window_us="${2:-3600000000}" payload val
	payload="$(python3 - "$sql" "$window_us" <<'PYQ'
import json, sys, time
now = int(time.time() * 1_000_000)
print(json.dumps({
    "query": {"sql": sys.argv[1],
              "start_time": now - int(sys.argv[2]),
              "end_time": now,
              "size": 5}}))
PYQ
)"
	# P6-179: curl reads the Authorization header from a 0600 config file (-K), so
	# the credential never appears in this or any child process's argv.
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

journal_file() { # $1 = journal dir or the fixed ingestion.json path -> real path
	# P1-132 names the journal ingestion-${HOST}-${VM_ID}.json, so a caller-
	# supplied ingestion.json does not exist. Mirrors journal_stats's resolver:
	# newest ingestion*.json beside it wins. Prints the input unchanged if
	# nothing matches, so callers keep their missing-file behaviour.
	local want="$1" dir found
	if [ -f "$want" ]; then printf '%s\n' "$want"; return 0; fi
	case "$want" in
		*/ingestion.json) dir="${want%/ingestion.json}" ;;
		*) dir="$want" ;;
	esac
	found="$(ls -1t "$dir"/ingestion*.json 2>/dev/null | head -1)"
	if [ -n "$found" ]; then printf '%s\n' "$found"; else printf '%s\n' "$want"; fi
}

journal_field() { # $1 = journal, $2 = field
	journal_stats "$1" 2>/dev/null | awk -F= -v k="$2" '$1 == k { print $2; exit }'
}
_num() { local v; v="$(journal_field "$1" "$2")"; echo "${v:-0}"; }
journal_acks() { _num "$1" acks; }
journal_errors() { _num "$1" errors; }
journal_warns() { _num "$1" warns; }
journal_maxepoch() { _num "$1" maxepoch; }
journal_distinct_epochs() { _num "$1" distinct_epochs; }

wait_until() { # $1 = epoch second — 5s steps so a deadline is not overshot by 20s (P6-774)
	local target="$1" late
	while [ "$(date +%s)" -lt "$target" ]; do sleep 5; done
	late=$(( $(date +%s) - target ))
	[ "$late" -gt 60 ] && echo "!! wait_until: ${late}s late for deadline $target (clock jump?)" >&2
	return 0
}

# Start the fake broker and refuse to continue if it never binds (P6-181); the
# same check is what makes a soak-time broker restart safe (P6-184).
FAKETOOL_WAIT_SECS="${FAKETOOL_WAIT_SECS:-30}"
faketool_up() { # $1 = log file, remaining args = faketool args
	local log="$1"
	shift
	"$OUT/bin/faketool" -port 8899 -tick-interval-ms 500 "$@" >> "$log" 2>&1 &
	FAKETOOL_PID=$!
	for _ in $(seq 1 "$FAKETOOL_WAIT_SECS"); do		port_open 127.0.0.1 8899 && return 0
		kill -0 "$FAKETOOL_PID" 2>/dev/null || break
		sleep 1
	done
	echo "!! faketool did not bind 127.0.0.1:8899 within ${FAKETOOL_WAIT_SECS}s — see $log" >&2
	kill "$FAKETOOL_PID" 2>/dev/null || true
	FAKETOOL_PID=""
	return 1
}

# P6-185: the soak verdict reads every check as evidence, not as a file that
# merely exists. `> monitor.log` creates the file even if the monitor died at
# once, one snapshot line is not eleven, an ERROR-spamming soak is not a pass,
# and a counter that reset (or came back NO_HITS) is not an advance (P6-521).
SOAK_ERROR_BUDGET="${SOAK_ERROR_BUDGET:-20}"   # 3 forced feed interruptions; raise deliberately
soak_verdict() { # reads the soak globals, fills SOAK_FAIL
	local rows errors
	SOAK_FAIL=""
	[ "${RECOVERIES:-0}" -ge 3 ] || SOAK_FAIL="recoveries=${RECOVERIES:-0}/3"
	[ -s "$OUT/soak/monitor.log" ] || SOAK_FAIL="${SOAK_FAIL:-} monitor missing or empty"
	rows="$(wc -l < "$OUT/soak/snapshots.tsv" 2>/dev/null || true)"
	[ "${rows:-0}" -ge 11 ] || SOAK_FAIL="${SOAK_FAIL:-} snapshots incomplete (${rows:-0}/11: start + i1-i3 + h1-h7)"
	case "${APPEND0:-x}${APPEND1:-x}" in
		*[!0-9]*) SOAK_FAIL="${SOAK_FAIL:-} append counter not numeric (start=$APPEND0 end=$APPEND1)" ;;
		*) [ "$APPEND1" -gt "$APPEND0" ] || SOAK_FAIL="${SOAK_FAIL:-} append counter did not advance (start=$APPEND0 end=$APPEND1)" ;;
	esac
	errors="$(journal_errors "$SOAK_JOURNAL/ingestion.json")"
	[ "${errors:-0}" -le "$SOAK_ERROR_BUDGET" ] || SOAK_FAIL="${SOAK_FAIL:-} journal ERRORs=${errors} over budget ${SOAK_ERROR_BUDGET}"
	return 0
}

# ── Source-only mode (tests) ──
# Everything above this line is a definition; no stage has run yet.
if [ "$LIB_MODE" = true ]; then
	trap - EXIT   # a sourcing test process has no run to clean up
	return 0 2>/dev/null || exit 0
fi

# ── Run setup ──
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="${OUT_DIR:-$PROJECT_ROOT/logs/soak/full-suite-$STAMP}"
mkdir -p "$OUT/marathon/journal" "$OUT/soak/journal" "$OUT/bin" "$OUT/reconnect" "$OUT/gates"
# P1-137 dropped the ingestion container to uid 65532, which writes its
# journal into these host bind mounts; the default 0775/uid-1000 mode denies
# it and the entrypoint fails closed (P1-137 follow-up gate).
chmod 0777 "$OUT/marathon/journal" "$OUT/soak/journal"
RUN_LOG="$OUT/run.log"
SUMMARY="$OUT/SUMMARY.txt"
: > "$RUN_LOG"

# Never echo credentials: O2 auth header value read from .env, kept in a var.
O2_AUTH="$(read_o2_auth "$DOCKER_DIR/.env")"
# P6-179: the header value goes into a 0600 curl config file, never onto a
# command line (ps, /proc/<pid>/cmdline, audit logs). The EXIT trap removes it.
O2_AUTH_FILE="$(umask 077; mktemp)"
printf 'header = "Authorization: Basic %s"\n' "$O2_AUTH" > "$O2_AUTH_FILE"
# Everything the operator needs lands in run.log AND the console.
exec > >(tee -a "$RUN_LOG") 2>&1

echo "=== full-suite start $STAMP (out: $OUT)"

# ── Stage 0: preflight ────────────────────────────────────────────────────────
echo "=== Stage 0: preflight"
FAILED=0
command -v docker >/dev/null || { echo "!! docker missing"; FAILED=1; }
command -v go >/dev/null || { echo "!! go missing"; FAILED=1; }
command -v mvn >/dev/null || { echo "!! mvn missing"; FAILED=1; }
command -v shellcheck >/dev/null || { echo "!! shellcheck missing (gates static stage)"; FAILED=1; }
command -v python3 >/dev/null || { echo "!! python3 missing"; FAILED=1; }
command -v curl >/dev/null || { echo "!! curl missing"; FAILED=1; }
command -v ss >/dev/null || { echo "!! ss missing (listener checks)"; FAILED=1; }
java -version 2>&1 | grep -q 'version "17' || { echo "!! java 17 missing"; FAILED=1; }
command -v javac >/dev/null || { echo "!! javac missing (dropper compile)"; FAILED=1; }
port_open localhost 9123 || { echo "!! Fluss :9123 not reachable"; FAILED=1; }
[ -n "$O2_AUTH" ] || { echo "!! O2_AUTH_BASIC missing from .env"; FAILED=1; }
[ -f "$MANIFEST" ] || { echo "!! manifest not found: $MANIFEST"; FAILED=1; }
RUNNING_CONTAINER="$( (cd "$DOCKER_DIR" && docker compose --env-file .env --env-file secrets.env -f docker-compose.yml ps -q ingestion) 2>/dev/null | head -1 || true)"
if [ -n "$RUNNING_CONTAINER" ]; then
	echo "!! an ingestion container is already running ($RUNNING_CONTAINER) — stop it first (P6-517: no double-run)"; FAILED=1
fi
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
PROBE_BEFORE="$($DROPPER --probe $OWNED_EXPECT)" \
	|| { stage_fail 0 "dropper probe failed (Fluss unreachable or rejected the DDL read?) — see run.log"; RESULT="FAIL"; exit 1; }
printf '%s\n' "$PROBE_BEFORE"
if printf '%s\n' "$PROBE_BEFORE" | grep -q 'would-drop'; then
	# P6-015: this branch DROPS tables irreversibly (a broker-side table has no
	# backup here). It needs an explicit operator opt-in, a local (dev) bootstrap
	# server, and visible stderr — no `2>/dev/null`, so a connect or auth failure
	# fails the stage instead of looking like a clean run.
	if [ "${ALLOW_DESTRUCTIVE_DROP:-false}" != true ]; then
		stage_fail 0 "refusing to drop owned tables — set ALLOW_DESTRUCTIVE_DROP=true (dev cluster only)"; RESULT="FAIL"; exit 1
	fi
	case "${FLUSS_BOOTSTRAP:-localhost:9123}" in
		localhost*|127.0.0.1*|0.0.0.0*|\[::1\]*) ;;
		*) stage_fail 0 "refusing destructive drop against non-local FLUSS_BOOTSTRAP=${FLUSS_BOOTSTRAP}"; RESULT="FAIL"; exit 1 ;;
	esac
	echo "-- drop mismatched:"
	$DROPPER $OWNED_EXPECT \
		|| { stage_fail 0 "dropper failed while dropping mismatched tables"; RESULT="FAIL"; exit 1; }
else
	echo "-- no mismatched tables — nothing to drop"
fi

# Short bootstrap run (ALLOW_RUNTIME_DDL=true) to recreate owned tables.
BOOTSTRAP_JOURNAL="$OUT/bootstrap/journal"
mkdir -p "$BOOTSTRAP_JOURNAL"
	faketool_up "$OUT/bootstrap/faketool.log" -disconnect-every 0 \
		|| { stage_fail 0 "faketool did not bind :8899"; RESULT="FAIL"; exit 1; }

export ARROW_HFT_URL="ws://127.0.0.1:8899"
export ARROW_BRIDGE_BIN="$BRIDGE_DIR/arrow-bridge"
	# P6-179: the broker credential triple is passed to the java child only
	# (prefix assignment), not exported to every child of this script.
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

ARROW_APP_ID="soak" ARROW_APP_SECRET="soaksecret" ARROW_TOKEN="soaktoken" \
java --add-opens=java.base/java.nio=ALL-UNNAMED -Dlog.dir="$BOOTSTRAP_JOURNAL" \	-cp "$JAR" com.trading.ingestion.IngestionService > "$OUT/bootstrap/java.out" 2>&1 &
BOOTSTRAP_JAVA_PID=$!

BOOTSTRAP_OK=0
for _ in $(seq 1 90); do # up to 180s
	_bj="$(journal_file "$BOOTSTRAP_JOURNAL/ingestion.json")"
	if [ -f "$_bj" ] \
		&& [ "$(journal_acks "$_bj")" -ge 1 ] \
		&& grep -q 'tables ok' "$_bj"; then
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
RECONCILE_OK="$($DROPPER --probe $OWNED_EXPECT | grep -c ': ok ' || true)"
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
GATES_EVIDENCE="$(ls -td "$PROJECT_ROOT"/logs/soak/monday-gates-* 2>/dev/null | head -1 || true)"
# P6-519: `| head -1` exits 0 even when the glob matched nothing, so `|| echo none`
# never fired and the summary printed an empty evidence path.
[ -n "$GATES_EVIDENCE" ] || GATES_EVIDENCE=none
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
	faketool_up "$OUT/marathon/faketool.log" -disconnect-every 1 \
		|| { stage_fail 2 "faketool did not bind :8899"; RESULT="FAIL"; exit 1; }

echo "-- starting native IngestionService (pid will be logged)"
ARROW_APP_ID="soak" ARROW_APP_SECRET="soaksecret" ARROW_TOKEN="soaktoken" \
java --add-opens=java.base/java.nio=ALL-UNNAMED -Dlog.dir="$MARATHON_JOURNAL" \	-cp "$JAR" com.trading.ingestion.IngestionService > "$OUT/marathon/java.out" 2>&1 &
JAVA_PID=$!
echo "java pid=$JAVA_PID faketool pid=$FAKETOOL_PID"

LOG_FILE="$(journal_file "$MARATHON_JOURNAL/ingestion.json")" OUT_DIR="$OUT/marathon" \
	"$SCRIPT_DIR/soak-monitor.sh" 3700 10 > "$OUT/marathon/monitor.log" 2>&1 &
MONITOR_PID=$!

MARATHON_OK=0
for _ in $(seq 1 240); do # 240 x 15s = 60 min budget
	_mj="$(journal_file "$MARATHON_JOURNAL/ingestion.json")"
	[ -f "$_mj" ] || { sleep 15; continue; }
	ACKS="$(journal_acks "$_mj")"
	if [ "${ACKS:-0}" -ge 100 ]; then MARATHON_OK=1; break; fi
	kill -0 "$JAVA_PID" 2>/dev/null || { echo "!! java exited early"; break; }
	sleep 15
done

MARATHON_ACKS="$(journal_acks "$MARATHON_JOURNAL/ingestion.json")"
MARATHON_MAXEPOCH="$(journal_maxepoch "$MARATHON_JOURNAL/ingestion.json")"
# P6-180: max_epoch >= 100 does not prove 100 cycles (one jump to 100 would pass, a
# monotonic run with a gap would not). Gate on the number of distinct epochs.
MARATHON_EPOCHS="$(journal_distinct_epochs "$MARATHON_JOURNAL/ingestion.json")"
echo "marathon end: acks=$MARATHON_ACKS max_epoch=$MARATHON_MAXEPOCH distinct_epochs=$MARATHON_EPOCHS (target ≥100)"

kill -TERM "$JAVA_PID" 2>/dev/null || true
for _ in $(seq 1 30); do kill -0 "$JAVA_PID" 2>/dev/null || break; sleep 2; done
kill -9 "$JAVA_PID" 2>/dev/null || true
kill "$MONITOR_PID" 2>/dev/null || true
kill "$FAKETOOL_PID" 2>/dev/null || true
wait "$JAVA_PID" 2>/dev/null || true
wait "$MONITOR_PID" 2>/dev/null || true
wait "$FAKETOOL_PID" 2>/dev/null || true

if [ "$MARATHON_OK" != 1 ] || [ "${MARATHON_EPOCHS:-0}" -lt 100 ]; then
	stage_fail 2 "marathon incomplete: acks=$MARATHON_ACKS distinct_epochs=${MARATHON_EPOCHS:-0} max_epoch=${MARATHON_MAXEPOCH:-0}"
	RESULT="FAIL"
	exit 1
fi
stage_pass 2
echo "marathon evidence: journal=$MARATHON_JOURNAL/ingestion.json monitor=$OUT/marathon/monitor.log errors=$(journal_errors "$MARATHON_JOURNAL/ingestion.json" ERROR) warns=$(journal_errors "$MARATHON_JOURNAL/ingestion.json" WARN)"

# ── Stage 3: container runtime run ────────────────────────────────────────────
echo "=== Stage 3: container runtime run — $(now)"
echo "-- docker compose build ingestion"
(cd "$DOCKER_DIR" && docker compose --env-file .env --env-file secrets.env build ingestion) > "$OUT/gates/docker-build.log" 2>&1 \
	|| { stage_fail 3 "docker build failed — see $OUT/gates/docker-build.log"; RESULT="FAIL"; exit 1; }

SOAK_JOURNAL="$OUT/soak/journal"
# R-221: Stage 2's faketool (disconnect-every=1) is killed after the marathon,
# but the container's readiness marker requires a live feed (brokerConnected +
# subscriptionComplete + fresh ticks). Start a no-drop soak-mode broker here;
# Stage 4 restarts its own, so this one is stopped after the reconnect cycle.
echo "-- starting fake broker in soak mode (no drops, tick 500ms)"
	faketool_up "$OUT/reconnect/faketool.log" -disconnect-every 0 \
		|| { stage_fail 3 "faketool did not bind :8899"; RESULT="FAIL"; exit 1; }
	STAGE3_FAKETOOL_PID="$FAKETOOL_PID"
echo "-- compose up ingestion (soak override)"
# P6-182: --force-recreate, so a container leaked by an earlier run cannot
# serve a stale image or stale env to this stage.
(cd "$DOCKER_DIR" && SOAK_JOURNAL_DIR="$SOAK_JOURNAL" \
		docker compose --env-file .env --env-file secrets.env -f docker-compose.yml -f docker-compose.soak.yml up -d --force-recreate ingestion) \
		|| { stage_fail 3 "compose up failed"; RESULT="FAIL"; exit 1; }
	CONTAINER_STARTED=1
	# P6-513: discover the container instead of assuming the project name.
	if [ -z "$INGESTION_CONTAINER" ]; then
		INGESTION_CONTAINER="$( (cd "$DOCKER_DIR" && docker compose --env-file .env --env-file secrets.env -f docker-compose.yml -f docker-compose.soak.yml ps -q ingestion) | head -1)"
	fi
	[ -n "$INGESTION_CONTAINER" ] \
		|| { stage_fail 3 "could not discover the ingestion container (compose ps -q)"; RESULT="FAIL"; exit 1; }

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
	_sj="$(journal_file "$SOAK_JOURNAL/ingestion.json")"
	if [ -f "$_sj" ]; then
		CONT_ACKS="$(journal_acks "$_sj")"
		[ "${CONT_ACKS:-0}" -ge 1 ] && break
	fi
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
LOG_FILE="$(journal_file "$SOAK_JOURNAL/ingestion.json")" OUT_DIR="$OUT/reconnect" CONTAINER="$INGESTION_CONTAINER" \
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
	faketool_up "$OUT/soak/faketool.log" -disconnect-every 0 \
		|| { stage_fail 4 "faketool did not bind :8899 before the soak"; RESULT="FAIL"; exit 1; }

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

LOG_FILE="$(journal_file "$SOAK_JOURNAL/ingestion.json")" OUT_DIR="$OUT/soak" \
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
	faketool_up "$OUT/soak/faketool.log" -disconnect-every 0 \
		|| { stage_fail 4 "broker did not come back after interruption [$1]"; RESULT="FAIL"; exit 1; }
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

	# 8h window: the 1h default cannot see a counter written 7h ago (P6-521).
	APPEND1="$(o2_query 'select value from "append_latency_ms_count" order by _timestamp desc limit 1' 28800000000)"
echo "soak end $(now): append_latency_ms_count=$APPEND1 (start=$APPEND0)"
echo "recoveries: $RECOVERIES/3"

# ── Final evidence + teardown ─────────────────────────────────────────────────
echo "=== final evidence collection — $(now)"
echo "-- headroom scan"
LOG_FILE="$(journal_file "$SOAK_JOURNAL/ingestion.json")" OUT_DIR="$OUT/soak" \
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
	docker compose --env-file .env --env-file secrets.env -f docker-compose.yml -f docker-compose.soak.yml stop ingestion) \
	|| echo "!! compose stop ingestion failed (check manually)"

# ── Stage verdicts ─────────────────────────────────────────────────────────────
soak_verdict
if [ -n "$SOAK_FAIL" ]; then
	stage_fail 4 "soak evidence incomplete: $SOAK_FAIL"
	RESULT="FAIL"
else
	stage_pass 4
	RESULT="PASS"
fi

echo "=== full-suite finished: $RESULT ($(now))"
# P6-016: FAIL must reach the caller — a soak that writes FAIL into SUMMARY.txt
# and exits 0 turns every regression green in CI.
[ "$RESULT" = PASS ] || exit 1
exit 0
