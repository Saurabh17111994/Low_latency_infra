#!/usr/bin/env bash
# eod-controller-test.sh — EOD controller machinery test (scope: controller
# + safety guarantees via the mock executor; real data export to MinIO/R2 is
# a separate follow-up with lake tiering).
#
# Structure (user requirement 2026-08-30): guard tests + 2-min smoke +
# 10-min main.
#
# Controller semantics this test encodes (derived interactively 2026-08-30):
#   - Leases live in eod_offload_state and PERSIST across process death
#     (default TTL 30m). A second run/reconcile while a lease is live is
#     refused with exit 5. => every subtest purges the state table first
#     (drop+recreate, the same hygiene pattern as pipeline_purge_table) and
#     passes --lease-ttl explicitly.
#   - `run --offload none` (fail-closed default) leaves the day record
#     FAILED_RETRYABLE and exits non-zero — never VERIFIED.
#   - A second mock run for an already-VERIFIED day is a no-op (DAYS=0).
#   - extend/reconcile on a clean or fully-VERIFIED state exit 0.
#   - EOD protection targets the durable closed-candle table `candle_closed`
#     (7d TTL, DDL 33). The retired 15s preview table (60s TTL) that used to
#     exercise the EXTENSION_REQUIRED path is gone (2026-09-05 cutover).
#
# Env: EOD_TEST_PHASE=guards|smoke|main|all (default all)
#      EOD_SMOKE_S (120), EOD_MAIN_S (600), RUN_DATE (today Asia/Kolkata)
# Exit: 0 all passed; non-zero = guard failure or phase error.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
EOD="$SCRIPT_DIR/eod_controller.py"
CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"
STATE_DDL="$ROOT/code/01_platform/02_sql/ddl/26_eod_offload_state.sql"
LOGDIR="$ROOT/logs/eod-test"
mkdir -p "$LOGDIR"
TS="$(date +%Y%m%d-%H%M%S)"
EVIDENCE="$LOGDIR/eod-test-$TS.log"
RUN_DATE="${RUN_DATE:-$(TZ=Asia/Kolkata date +%F)}"
EOD_TEST_PHASE="${EOD_TEST_PHASE:-all}"
EOD_SMOKE_S="${EOD_SMOKE_S:-120}"
EOD_MAIN_S="${EOD_MAIN_S:-600}"
EOD_TABLES="candle_closed"   # 7d TTL durable closed-candle table (see header note)

pass=0; fail=0
FIXTURE_DIRS=()      # P6-362: mktemp dirs to remove on any exit path
FEED_STATE=""        # P6-076: state file of the feed currently running (if any)
ok()   { printf 'ok    %s\n' "$*" | tee -a "$EVIDENCE"; pass=$((pass+1)); }
bad()  { printf '!! FAIL %s\n' "$*" | tee -a "$EVIDENCE" >&2; fail=$((fail+1)); }
info() { printf '      %s\n' "$*" | tee -a "$EVIDENCE"; }

# teardown on ANY exit: a SIGINT mid-feed used to leave the containers, the
# `docker logs -f` mirrors and the fixture dirs behind (P6-362/P6-076).
cleanup_on_exit() {
  local d
  for d in "${FIXTURE_DIRS[@]:-}"; do [ -n "$d" ] && rm -rf "$d"; done
  [ -n "$FEED_STATE" ] && stop_feed "exit" "$FEED_STATE" || true
  return 0
}
trap cleanup_on_exit EXIT
trap 'exit 130' INT TERM

# eod <args...> — run controller, echo sentinel lines, return its exit code.
eod() {
  local out rc
  out="$(python3 "$EOD" "$@" 2>&1)"; rc=$?
  printf '%s\n' "$out" | grep -E "eod-controller: (RESULT|ALERT|lease|dry-run|run |extend |reconcile )" \
    | tee -a "$EVIDENCE" || true
  return $rc
}

# purge_state — drop+recreate eod_offload_state (clears records AND leases).
purge_state() {
  [ -r "$CP_FILE" ] || { bad "purge: cp.txt missing (build ingestion)"; return 1; }
  local cp; cp="$(cat "$CP_FILE")"
  # P6-363/P6-362: a private mktemp dir, not a fixed path in world-writable
  # /tmp (symlink race), and the compile error is surfaced instead of swallowed.
  local tmpd; tmpd="$(mktemp -d)"; FIXTURE_DIRS+=("$tmpd")
  cat > "$tmpd/EodPurge.java" <<'JAVAEOF'
import com.trading.common.schema.ddl.DdlText;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import java.nio.file.Files; import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
public class EodPurge {
    public static void main(String[] a) throws Exception {
        String ddl = Files.readString(Path.of(a[0]));
        DdlText.ParsedDdl p = DdlText.parse(ddl, a[0]);
        TablePath tp = TablePath.of("default", p.tableName());
        Configuration c = new Configuration(); c.setString("bootstrap.servers","localhost:9123");
        try (Connection cn = ConnectionFactory.createConnection(c); Admin ad = cn.getAdmin()) {
            try { ad.dropTable(tp,false).get(60,TimeUnit.SECONDS); } catch (Exception e) {}
            ad.createTable(tp, DdlText.toDescriptor(p), false).get(60,TimeUnit.SECONDS);
            System.out.println("PURGED " + p.tableName());
        }}}
JAVAEOF
  local jout
  if ! jout="$(javac -cp "$cp" -d "$tmpd" "$tmpd/EodPurge.java" 2>&1)"; then
    bad "purge: javac failed: $(printf '%s' "$jout" | tail -3 | tr '\n' ' ')"
    rm -rf "$tmpd"
    return 1
  fi
  ( cd "$tmpd" && java --add-opens=java.base/java.lang=ALL-UNNAMED \
      --add-opens=java.base/java.nio=ALL-UNNAMED \
      -cp "$tmpd:$cp" EodPurge "$STATE_DDL" 2>&1 | tail -1 ) | tee -a "$EVIDENCE" \
    | grep -q "PURGED eod_offload_state"
  # P6-364/P6-360: after `( … ) | tee | grep -q`, PIPESTATUS[0] is the subshell's
  # status — tail's, i.e. always 0 — so a missing PURGED marker still returned 0.
  # `set -o pipefail` is on, so `$?` is the rightmost non-zero status of the pipe.
  local rc=$?
  rm -rf "$tmpd"
  [ "$rc" = "0" ]
}

# P6-365/P6-359: `2>/dev/null` plus `|| echo 0` / `|| true` collapsed a controller
# crash into "0 days", which made G-EOD-0 and G-EOD-1 pass against a broken
# controller. Propagate the failure instead — callers compare the output against
# "0", so both the bare number (days_on_file) and the distinct failure string
# below are part of the contract.
days_on_file() {
  local out rc
  out="$(python3 "$EOD" status 2>&1)"; rc=$?
  [ "$rc" = "0" ] || { printf 'STATUS_FAILED rc=%s' "$rc"; return 1; }
  printf '%s\n' "$out" | grep -oE 'DAYS=[0-9]+' | grep -oE '[0-9]+' | head -1
}
verified_days() {
  local out rc
  out="$(python3 "$EOD" status 2>&1)"; rc=$?
  [ "$rc" = "0" ] || { printf 'STATUS_FAILED rc=%s' "$rc"; return 1; }
  printf '%s\n' "$out" | grep -cE 'state=VERIFIED'
}

# wait_for_day_state <date> <states-regex> <timeout_s> — poll status until the
# run has written its day record in one of those states. P6-366/P6-361: the
# previous fixed `sleep 2` raced the controller's JVM start (if the mock run
# finished first the lease was released and the check could pass for the wrong
# reason). Bounded, so a wedged controller cannot hang the test.
wait_for_day_state() {
  local want_date="$1" want_states="$2" timeout_s="$3"
  local deadline=$((SECONDS + timeout_s))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if python3 "$EOD" status 2>/dev/null \
        | grep -qE "^eod-controller:   day $want_date state=($want_states)"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# wait_bounded <pid> <timeout_s> — reap a background child, but never forever
# (P6-361): a watchdog kills it once the timeout elapses.
wait_bounded() {
  local pid="$1" timeout_s="$2" timer rc
  ( sleep "$timeout_s"; kill -9 "$pid" 2>/dev/null ) &
  timer=$!
  wait "$pid" 2>/dev/null; rc=$?
  kill "$timer" 2>/dev/null || true
  wait "$timer" 2>/dev/null || true
  return "$rc"
}

# ----------------------------------------------------------------------------
# PHASE 1: GUARD TESTS
# ----------------------------------------------------------------------------
guards() {
  printf '=== guard tests (no feed) ===\n' | tee -a "$EVIDENCE"

  # G-EOD-0: purge works and yields a clean slate (residue visible otherwise)
  if purge_state && [ "$(days_on_file)" = "0" ]; then
    ok "G-EOD-0 purge + clean slate (DAYS=0)"
  else
    bad "G-EOD-0 purge failed / state not clean (DAYS=$(days_on_file))"
    return 1
  fi

  # G-EOD-1: fail-closed — offload=none must never verify a day
  eod run --offload none --run-date "$RUN_DATE" --lease-ttl 5s \
    --tables "$EOD_TABLES" >/dev/null 2>&1
  local rc1=$?
  local v1; v1="$(verified_days)"
  if [ "$rc1" -ne 0 ] && [ "${v1:-0}" = "0" ]; then
    ok "G-EOD-1 fail-closed: offload=none rc=$rc1, VERIFIED days=0"
  else
    bad "G-EOD-1 fail-closed violated: rc=$rc1 verified=$v1"
  fi
  purge_state || true

  # G-EOD-2: lease fencing — concurrent run refused with exit 5
  python3 "$EOD" run --offload mock --run-date "$RUN_DATE" --lease-ttl 60s \
    --tables "$EOD_TABLES" >/dev/null 2>&1 &
  local bg=$!
  # P6-366/P6-361: wait until the holder is really mid-run (record written =
  # lease acquired), then FREEZE its JVM so the lease cannot be released under
  # us; the concurrent attempt is then refused deterministically instead of
  # racing a fixed 2s sleep. The reap below is bounded.
  local jvm=""
  if wait_for_day_state "$RUN_DATE" "PENDING|COMMITTED" 30; then
    jvm="$(pgrep -P "$bg" 2>/dev/null | head -1)"
    [ -n "$jvm" ] && kill -STOP "$jvm" 2>/dev/null
  fi
  eod run --offload mock --run-date "$RUN_DATE" --lease-ttl 60s \
    --tables "$EOD_TABLES" >/dev/null 2>&1
  local rc2=$?
  [ -n "$jvm" ] && kill -CONT "$jvm" 2>/dev/null
  wait_bounded "$bg" 60 || true
  if [ -z "$jvm" ]; then
    bad "G-EOD-2 could not hold the lease: no controller JVM for $RUN_DATE was observable (concurrent rc=$rc2)"
  elif [ "$rc2" = "5" ]; then
    ok "G-EOD-2 lease fencing: concurrent run refused (rc=5, holder frozen)"
  else
    bad "G-EOD-2 lease fencing NOT enforced: concurrent rc=$rc2 (want 5)"
  fi
  purge_state || true

  # G-EOD-3: extend + reconcile are no-ops (rc 0) on a clean state
  eod extend --tables "$EOD_TABLES" >/dev/null 2>&1; local rc3a=$?
  eod reconcile --tables "$EOD_TABLES" >/dev/null 2>&1; local rc3b=$?
  if [ "$rc3a" = "0" ] && [ "$rc3b" = "0" ]; then
    ok "G-EOD-3 extend/reconcile no-op on clean state (rc=$rc3a/$rc3b)"
  else
    bad "G-EOD-3 clean-state no-op violated: extend rc=$rc3a reconcile rc=$rc3b"
  fi
  purge_state || true

  # G-EOD-4: idempotency — second mock run finds nothing due, records stable
  eod run --offload mock --run-date "$RUN_DATE" --lease-ttl 5s \
    --tables "$EOD_TABLES" >/dev/null 2>&1
  # P6-074/P6-071: a COMPLETED run releases its lease, so this second run is the
  # documented VERIFIED-day no-op (rc=0), not a refusal — the old check inferred
  # it from DAYS=0 alone, which a crash or a refusal also satisfies. Assert the
  # exit code of the very invocation whose output is parsed.
  local second rc4 g4out="$LOGDIR/$TS-g4.out"
  python3 "$EOD" run --offload mock --run-date "$RUN_DATE" \
    --lease-ttl 5s --tables "$EOD_TABLES" > "$g4out" 2>&1
  rc4=$?
  second="$(grep -oE 'DAYS=[0-9]+' "$g4out" | head -1)"
  local nrecs; nrecs="$(python3 "$EOD" status 2>/dev/null | grep -cE '^eod-controller:   day ')"
  if [ "$rc4" = "0" ] && [ "$second" = "DAYS=0" ] && [ "$nrecs" = "1" ]; then
    ok "G-EOD-4 idempotent: 2nd run rc=$rc4 due=0, still 1 record"
  else
    bad "G-EOD-4 idempotency broken: 2nd run rc=$rc4 $second, records=$nrecs"
  fi
  purge_state || true

  # G-EOD-5: reconcile after a verified run leaves state intact
  eod run --offload mock --run-date "$RUN_DATE" --lease-ttl 5s \
    --tables "$EOD_TABLES" >/dev/null 2>&1
  sleep 6   # let the 5s lease expire
  eod reconcile --tables "$EOD_TABLES" >/dev/null 2>&1
  local rc5=$?
  local nrecs5; nrecs5="$(python3 "$EOD" status 2>/dev/null | grep -cE '^eod-controller:   day ')"
  if [ "$rc5" = "0" ] && [ "$nrecs5" = "1" ]; then
    ok "G-EOD-5 reconcile no-op on verified state (rc=0, 1 record)"
  else
    bad "G-EOD-5 reconcile disturbed verified state: rc=$rc5 records=$nrecs5"
  fi
  purge_state || true

  printf '=== guards: %d passed, %d failed ===\n' "$pass" "$fail" | tee -a "$EVIDENCE"
  [ "$fail" -eq 0 ]
}

# ----------------------------------------------------------------------------
# PHASE 2/3: feed + EOD cycle
# ----------------------------------------------------------------------------
feed_and_cycle() {
  local phase="$1" duration_s="$2"
  printf '=== %s phase: %ss feed + EOD cycle (run-date %s) ===\n' \
    "$phase" "$duration_s" "$RUN_DATE" | tee -a "$EVIDENCE"

  purge_state || { bad "$phase: pre-phase purge failed"; return 1; }

  # P6-076/P6-073: the feed lives in a `bash -c` subshell, so its pipeline-lib
  # state dies with it. Record the log-mirror PIDs and the container names the
  # lib actually used, so the teardown can kill EXACTLY those instead of
  # pattern-matching host command lines (the old sweep for faketool/ingestion.jar
  # matched unrelated processes and other runs' containers too).
  FEED_STATE="$LOGDIR/$phase-$TS.feed-state"
  rm -f "$FEED_STATE"
  ROOT="$ROOT" RATE_HZ=10 OUT="$LOGDIR/$phase-$TS" DURATION_S="$duration_s" \
    FEED_STATE="$FEED_STATE" \
    bash -c '
    source "'"$SCRIPT_DIR"'/pipeline-lib.sh"
    mkdir -p "$OUT"
    pipeline_preflight || exit 1
    pipeline_start_faketool || exit 1
    pipeline_start_ingestion || exit 1
    pipeline_submit_job || exit 1
    {
      printf "FEED_PIDS=%s %s\n" "${FAKETOOL_LOG_PID:-}" "${INGESTION_LOG_PID:-}"
      printf "FEED_CONTAINERS=%s %s\n" "$LIB_FAKETOOL_CONTAINER" "$LIB_INGESTION_CONTAINER"
      printf "FEED_JOB=%s\n" "${JOB_ID:-}"
    } > "$FEED_STATE"
    echo "feeding ${DURATION_S}s..."
    sleep "$DURATION_S"
  ' >> "$EVIDENCE" 2>&1 || { bad "$phase: pipeline bring-up/feed failed"; stop_feed "$phase" "$FEED_STATE"; return 1; }

  # EOD cycle (mock) against the live-fed durable candle table
  eod run --offload mock --run-date "$RUN_DATE" --lease-ttl 5s \
    --tables "$EOD_TABLES"
  local run_rc=$?

  local nrecs
  nrecs="$(python3 "$EOD" status 2>/dev/null \
    | grep -cE "^eod-controller:   day $RUN_DATE")"
  # P6-075/P6-070: bad() only bumps the counter. Falling through to `return 0`
  # meant `feed_and_cycle … || rc_total=1` never fired, so the documented
  # "non-zero = phase error" contract was silently violated.
  local cyc_rc=0
  if [ "$nrecs" = "1" ] && [ "$run_rc" = "0" ]; then
    ok "$phase: EOD cycle verified 1 day-record for $RUN_DATE (rc=0)"
  else
    bad "$phase: EOD cycle rc=$run_rc records=$nrecs (want 1/0)"
    cyc_rc=1
  fi

  stop_feed "$phase" "$FEED_STATE"
  return "$cyc_rc"
}

# stop_feed <phase> <state-file> — teardown by PID and exact container name,
# never by command-line pattern (P6-076/P6-073). Idempotent and safe on a
# half-started feed: it removes the state file so a later trap cannot kill a
# recycled PID.
stop_feed() {
  local phase="$1" state="${2:-}"
  local pids="" containers=""
  if [ -n "$state" ] && [ -r "$state" ]; then
    pids="$(sed -n 's/^FEED_PIDS=//p' "$state")"
    containers="$(sed -n 's/^FEED_CONTAINERS=//p' "$state")"
    rm -f "$state"
  fi
  FEED_STATE=""
  local jid
  jid="$(curl -s http://localhost:8081/jobs/overview 2>/dev/null \
    | grep -oE '"jid":"[0-9a-f]{32}"' | head -1 | cut -d'"' -f4)"
  [ -n "$jid" ] && curl -s -X PATCH "http://localhost:8081/jobs/$jid?mode=cancel" >/dev/null 2>&1
  if [ -n "${pids// /}" ]; then
    # shellcheck disable=SC2086  # deliberate: a space-separated PID list we wrote
    kill $pids 2>/dev/null || true
    info "$phase: feed log mirrors stopped (pids: $pids)"
  fi
  if [ -n "${containers// /}" ]; then
    # shellcheck disable=SC2086  # deliberate: exact names as reported by the lib
    docker rm -f $containers >/dev/null 2>&1 || true
    info "$phase: feed containers removed ($containers)"
    sleep 2   # let the containers/ports settle before the next phase re-checks them
  fi
  return 0
}

# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
printf 'eod-controller-test: phase=%s run-date=%s evidence=%s\n' \
  "$EOD_TEST_PHASE" "$RUN_DATE" "$EVIDENCE" | tee -a "$EVIDENCE"

rc_total=0
# P6-072/P6-077: `main` matched neither case, and any unknown value ran zero
# phases and still exited 0 — while still purging the live state table. Validate
# first; an invalid phase does no cluster work at all.
case "$EOD_TEST_PHASE" in
  guards|smoke|main|all) ;;
  *) bad "unknown EOD_TEST_PHASE='$EOD_TEST_PHASE' (want guards|smoke|main|all)"
     rc_total=1 ;;
esac

if [ "$rc_total" = "0" ]; then
  case "$EOD_TEST_PHASE" in
    guards|all) guards || rc_total=1 ;;
  esac
  case "$EOD_TEST_PHASE" in
    smoke|all) feed_and_cycle smoke "$EOD_SMOKE_S" || rc_total=1 ;;
  esac
  case "$EOD_TEST_PHASE" in
    main|all) feed_and_cycle main "$EOD_MAIN_S" || rc_total=1 ;;
  esac

  if purge_state && [ "$(days_on_file)" = "0" ]; then
    info "state table left clean"
  else
    bad "state table NOT clean at end — residue present"
    rc_total=1
  fi
fi

# P6-078: the fail counter is authoritative too — any bad() anywhere must be a
# non-zero exit, not just the phases that returned non-zero. Set before the
# summary so the printed rc is the real one.
[ "$fail" -eq 0 ] || rc_total=1

printf '=== eod-controller-test done: %d passed, %d failed, rc=%d ===\n' \
  "$pass" "$fail" "$rc_total" | tee -a "$EVIDENCE"
exit "$rc_total"
