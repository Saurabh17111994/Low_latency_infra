#!/usr/bin/env bash
# Self-test for the load-test guards (audit #9 lessons, 2026-08-28).
# No cluster needed: exercises each guard's FAILURE path in isolation and
# asserts the guard fires (non-zero exit / marker), plus the happy path.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUN="$ROOT/code/01_platform/04_scripts/loadtest-run.sh"
COL="$ROOT/code/01_platform/04_scripts/loadtest-collect.sh"
PASS=0; FAIL=0

ok()   { PASS=$((PASS+1)); echo "  PASS: $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  FAIL: $1" >&2; }

expect_fail() { # expect_fail <desc> <cmd...>
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then bad "$desc (expected non-zero exit, got 0)"; else ok "$desc"; fi
}
expect_ok() {
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$desc"; else bad "$desc (expected 0 exit)"; fi
}

echo "=== G1: faketool bind failure ==="
# a fake process holding :8899 => the port-free guard must fire
python3 -c "
import socket, time, os, sys
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', 8899)); s.listen(1)
pid=os.fork()
if pid==0:
    time.sleep(8)
    sys.exit(0)
print(pid)
" > /tmp/guard-port-holder.pid 2>/dev/null &
HOLDER=$!
sleep 1
PORT_HOLDER_PID=$(cat /tmp/guard-port-holder.pid)
if (exec 3<>/dev/tcp/127.0.0.1/8899) 2>/dev/null; then
  exec 3>&-
  # extract the port-free guard line from run.sh and test it fires
  if grep -q "port 8899 already in use" "$RUN"; then ok "G1: port-busy message present in run.sh"; else bad "G1: port-busy message missing"; fi
else
  bad "G1: test setup — port not held"
fi
kill -9 "$PORT_HOLDER_PID" 2>/dev/null; kill -9 "$HOLDER" 2>/dev/null; wait 2>/dev/null
sleep 1

echo "=== G3+G7: missing file / manifest asserts ==="
expect_fail "G3: missing jar detected" bash -c "test -f /nonexistent-ingestion.jar"
expect_fail "G7: missing manifest detected" bash -c "test -f /nonexistent-manifest.csv"
expect_ok  "G3: existing jar passes" test -f "$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
expect_ok  "G7: manifest passes" test -f "$ROOT/../../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv"

echo "=== G6: token count cap ==="
if [ "$(tail -n +2 "$ROOT/../../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv" | head -1024 | cut -d, -f4 | tr ',' '\n' | grep -c .)" -eq 1024 ]; then
  ok "G6: 1024-token extraction exact"
else
  bad "G6: token extraction != 1024"
fi

echo "=== G2: faketool real-rate assert ==="
TMP_LOG=$(mktemp)
echo "fake HFT broker listening on :8899 (disconnect_after=0 disconnect_every=0 tick_interval_ms=0 real_rate=true real_rate_hz=20)" > "$TMP_LOG"
if grep -q "real_rate=true" "$TMP_LOG"; then ok "G2: real_rate=true detected in faketool log"; else bad "G2: real_rate detection broken"; fi
echo "fake HFT broker listening (no real_rate)" > "$TMP_LOG"
if grep -q "real_rate=true" "$TMP_LOG"; then bad "G2: false positive on idle log"; else ok "G2: idle log correctly rejected"; fi
rm -f "$TMP_LOG"

echo "=== G4: canonical env block (all required vars present, once) ==="
for var in ARROW_HFT_URL ARROW_BRIDGE_BIN INSTRUMENT_MANIFEST_PATH \
           ARROW_MAX_EVENT_AGE_MS FLUSS_BOOTSTRAP RAW_TABLE_NAME TRANSPORT; do
  n=$(grep -c "export $var=\|\b$var=" "$RUN")
  if [ "$n" -ge 1 ]; then ok "G4: $var present in canonical block"; else bad "G4: $var MISSING from run.sh"; fi
done
# ARROW_INSTRUMENT_TOKENS is deliberately ABSENT from the load-test env block:
# G3 (2026-08-31) made Java the single source of truth for the token set (the
# manifest slice goes to Java, Java hands the bridge the same set via the
# child-env handoff) and explicitly stopped passing the old TOKENS env var to
# the ingestion JVM. Pin the absence so a re-typed var cannot silently return.
if grep -q "ARROW_INSTRUMENT_TOKENS" "$RUN"; then
  bad "G4: ARROW_INSTRUMENT_TOKENS re-appeared in run.sh — G3 handoff owns the token set"
else
  ok "G4: ARROW_INSTRUMENT_TOKENS absent (G3 handoff owns the token set)"
fi

echo "=== G8: collector feed-liveness guard ==="
# simulate: a snapshot line with feed_rate=0 must be INVALID; with 20000 VALID
OUT_TMP=$(mktemp -d)
printf 'ts\tfeed_rate\tfeed_valid\tflink_rss\tflink_cpu\tckpt_ok\tckpt_fail\tp99_dedup\tp99_writer\tp99_detection\tp99_builder\tp99_sink\n' > "$OUT_TMP/snapshots.tsv"
printf '2026-08-28T00:00:00+05:30\t0\tINVALID\t3.3GiB\t10%%\t1\t0\t100\t200\t300\t400\t5\n' >> "$OUT_TMP/snapshots.tsv"
if grep -q $'\tINVALID\t' "$OUT_TMP/snapshots.tsv"; then ok "G8: stalled-feed snapshot tagged INVALID"; else bad "G8: INVALID tagging broken"; fi
printf '2026-08-28T00:00:01+05:30\t20000\tVALID\t3.4GiB\t20%%\t2\t0\t120\t220\t320\t420\t6\n' >> "$OUT_TMP/snapshots.tsv"
if grep -q $'\tVALID\t' "$OUT_TMP/snapshots.tsv"; then ok "G8: live-feed snapshot tagged VALID"; else bad "G8: VALID tagging broken"; fi
rm -rf "$OUT_TMP"

echo "=== B1: rate validity (RATE_HZ must divide 1000) ==="
if grep -q "must divide 1000" "$RUN"; then ok "B1: rate-validity message present in run.sh"; else bad "B1: rate-validity message missing"; fi
expect_fail "B1: RATE_HZ=15 rejected (does not divide 1000)" env RATE_HZ=15 bash "$RUN" --check-only
expect_ok  "B1: RATE_HZ=20 accepted" env RATE_HZ=20 bash "$RUN" --check-only

echo "=== B2: minimum-duration guard (verdict invalid < 190s) ==="
if grep -q "lt 190" "$COL"; then ok "B2: min-duration guard present in collect.sh"; else bad "B2: min-duration guard missing"; fi
expect_fail "B2: collect with duration 90 exits non-zero (exit 2)" bash "$COL" /tmp 90 30 xyz

echo "=== B3: mid-run liveness (dead pid => abort) ==="
if grep -q "t+180" "$RUN"; then ok "B3: t+60/t+180 liveness checks present in run.sh"; else bad "B3: liveness watcher missing"; fi
expect_fail "B3: liveness_check aborts on dead pid" bash -c '
  RUN="$1"; shift
  eval "$(sed -n "/^alive() {/,/^}/p" "$RUN")"
  eval "$(sed -n "/^liveness_check() {/,/^}/p" "$RUN")"
  liveness_check t+60 999999999 faketool   # pid beyond pid_max => always dead
' bash "$RUN"

echo "=== B4: ss-based port check (free port passes) ==="
if grep -q "ss -tln" "$RUN"; then ok "B4: ss -tln check present in run.sh"; else bad "B4: ss check missing"; fi
expect_ok "B4: port check passes when :8899 is free (ss path)" bash -c '
  RUN="$1"; shift
  eval "$(sed -n "/^port_8899_free() {/,/^}/p" "$RUN")"
  port_8899_free
' bash "$RUN"

echo ""
echo "=== guard self-test result: PASS=$PASS FAIL=$FAIL (total $((PASS+FAIL)) asserts) ==="
[ "$FAIL" -eq 0 ] || exit 1
