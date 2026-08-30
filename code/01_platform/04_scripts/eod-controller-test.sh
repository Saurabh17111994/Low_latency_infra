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
#     (drop+recreate, the same hygiene pattern as the preview purge) and
#     passes --lease-ttl explicitly.
#   - `run --offload none` (fail-closed default) leaves the day record
#     FAILED_RETRYABLE and exits non-zero — never VERIFIED.
#   - A second mock run for an already-VERIFIED day is a no-op (DAYS=0).
#   - extend/reconcile on a clean or fully-VERIFIED state exit 0.
#   - The preview table (60s TTL < 7d safety floor) legitimately triggers
#     EXTENSION_REQUIRED on extend; EOD protection is really about the
#     durable candle table (7d TTL) — the test scope is feature_candles_15s.
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
EOD_TABLES="feature_candles_15s"   # 7d TTL durable table (see header note)

pass=0; fail=0
ok()   { printf 'ok    %s\n' "$*" | tee -a "$EVIDENCE"; pass=$((pass+1)); }
bad()  { printf '!! FAIL %s\n' "$*" | tee -a "$EVIDENCE" >&2; fail=$((fail+1)); }
info() { printf '      %s\n' "$*" | tee -a "$EVIDENCE"; }

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
  cat > /tmp/EodPurge.java <<'JAVAEOF'
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
  ( cd /tmp && javac -cp "$cp" -d /tmp EodPurge.java 2>/dev/null \
      && java --add-opens=java.base/java.lang=ALL-UNNAMED \
         --add-opens=java.base/java.nio=ALL-UNNAMED \
         -cp "/tmp:$cp" EodPurge "$STATE_DDL" 2>&1 | tail -1 ) | tee -a "$EVIDENCE" \
    | grep -q "PURGED eod_offload_state"
  local rc=${PIPESTATUS[0]}
  rm -f /tmp/EodPurge.java /tmp/EodPurge.class
  [ "$rc" = "0" ]
}

days_on_file() {
  python3 "$EOD" status 2>/dev/null | grep -oE 'DAYS=[0-9]+' | grep -oE '[0-9]+' || echo 0
}
verified_days() {
  python3 "$EOD" status 2>/dev/null | grep -cE 'state=VERIFIED' || true
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
  sleep 2
  eod run --offload mock --run-date "$RUN_DATE" --lease-ttl 60s \
    --tables "$EOD_TABLES" >/dev/null 2>&1
  local rc2=$?
  wait "$bg" 2>/dev/null
  if [ "$rc2" = "5" ]; then
    ok "G-EOD-2 lease fencing: concurrent run refused (rc=5)"
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
  local second
  second="$(python3 "$EOD" run --offload mock --run-date "$RUN_DATE" \
    --lease-ttl 5s --tables "$EOD_TABLES" 2>&1 | grep -oE 'DAYS=[0-9]+' | head -1)"
  local nrecs; nrecs="$(python3 "$EOD" status 2>/dev/null | grep -cE '^eod-controller:   day ')"
  if [ "$second" = "DAYS=0" ] && [ "$nrecs" = "1" ]; then
    ok "G-EOD-4 idempotent: 2nd run due=0, still 1 record"
  else
    bad "G-EOD-4 idempotency broken: 2nd run $second, records=$nrecs"
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

  ROOT="$ROOT" RATE_HZ=10 OUT="$LOGDIR/$phase-$TS" DURATION_S="$duration_s" \
    bash -c '
    source "'"$SCRIPT_DIR"'/pipeline-lib.sh"
    mkdir -p "$OUT"
    pipeline_preflight || exit 1
    pipeline_start_faketool || exit 1
    pipeline_start_ingestion || exit 1
    pipeline_submit_job || exit 1
    echo "feeding ${DURATION_S}s..."
    sleep "$DURATION_S"
  ' >> "$EVIDENCE" 2>&1 || { bad "$phase: pipeline bring-up/feed failed"; return 1; }

  # EOD cycle (mock) against the live-fed durable candle table
  eod run --offload mock --run-date "$RUN_DATE" --lease-ttl 5s \
    --tables "$EOD_TABLES"
  local run_rc=$?

  local nrecs
  nrecs="$(python3 "$EOD" status 2>/dev/null \
    | grep -cE "^eod-controller:   day $RUN_DATE")"
  if [ "$nrecs" = "1" ] && [ "$run_rc" = "0" ]; then
    ok "$phase: EOD cycle verified 1 day-record for $RUN_DATE (rc=0)"
  else
    bad "$phase: EOD cycle rc=$run_rc records=$nrecs (want 1/0)"
  fi

  # teardown feed
  local jid
  jid="$(curl -s http://localhost:8081/jobs/overview 2>/dev/null \
    | grep -oE '"jid":"[0-9a-f]{32}"' | head -1 | cut -d'"' -f4)"
  [ -n "$jid" ] && curl -s -X PATCH "http://localhost:8081/jobs/$jid?mode=cancel" >/dev/null 2>&1
  sleep 2
  pkill -f "faketool" 2>/dev/null || true
  pkill -f "ingestion.jar" 2>/dev/null || true
  sleep 2
  return 0
}

# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
printf 'eod-controller-test: phase=%s run-date=%s evidence=%s\n' \
  "$EOD_TEST_PHASE" "$RUN_DATE" "$EVIDENCE" | tee -a "$EVIDENCE"

rc_total=0
case "$EOD_TEST_PHASE" in
  guards|all) guards || rc_total=1 ;;
esac
case "$EOD_TEST_PHASE" in
  smoke|all) feed_and_cycle smoke "$EOD_SMOKE_S" || rc_total=1 ;;
esac
[ "$EOD_TEST_PHASE" = "all" ] && { feed_and_cycle main "$EOD_MAIN_S" || rc_total=1; }

if purge_state && [ "$(days_on_file)" = "0" ]; then
  info "state table left clean"
else
  bad "state table NOT clean at end — residue present"
  rc_total=1
fi

printf '=== eod-controller-test done: %d passed, %d failed, rc=%d ===\n' \
  "$pass" "$fail" "$rc_total" | tee -a "$EVIDENCE"
exit "$rc_total"
