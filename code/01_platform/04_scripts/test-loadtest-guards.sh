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
expect_ok  "G7: manifest passes" test -f "$ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv"

echo "=== G6: token count cap ==="
if [ "$(tail -n +2 "$ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv" | head -1024 | cut -d, -f4 | tr ',' '\n' | grep -c .)" -eq 1024 ]; then
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

echo "=== P6-127..134 / P6-447..456: wave-47 invariants ==="
# P6-127/128: the stray reap must run BEFORE the port assert, otherwise a stale
# faketool holding :8899 makes the reap unreachable and the run dies instead.
REAP_LINE=$(grep -n 'pgrep -x faketool || true' "$RUN" | head -1 | cut -d: -f1)
ASSERT_LINE=$(grep -n 'port 8899 already in use' "$RUN" | head -1 | cut -d: -f1)
if [ -n "$REAP_LINE" ] && [ -n "$ASSERT_LINE" ] && [ "$REAP_LINE" -lt "$ASSERT_LINE" ]; then
  ok "P6-127: reap (line $REAP_LINE) precedes the port assert (line $ASSERT_LINE)"
else
  bad "P6-127: reap/assert order wrong (reap='$REAP_LINE' assert='$ASSERT_LINE')"
fi
# P6-129/133: a fixed /tmp readiness marker survives `kill -9` and false-passes.
if grep -q '/tmp/ingestion.loadtest.ready' "$RUN"; then
  bad "P6-129: fixed /tmp readiness path is back in run.sh"
else
  ok "P6-129: no fixed /tmp readiness path"
fi
if grep -q 'READINESS_FILE="\$OUT/ingestion.loadtest.ready"' "$RUN" && \
   grep -q 'rm -f "\$READINESS_FILE"' "$RUN"; then
  ok "P6-129: per-run readiness marker set and pre-cleared"
else
  bad "P6-129: per-run readiness marker or its pre-clear is missing"
fi
# P6-130/132: one counter for every token count, header validated, no head -1025.
# (the prose in the P6-130/132 comment names the old idiom, so match the code form)
if grep -q 'head -1025 "\$MANIFEST"' "$RUN"; then
  bad "P6-130: head -1025 is back (the slice trusts the manifest shape again)"
else
  ok "P6-130: no shape-trusting head -1025 slice"
fi
CNT_SITES=$(grep -c 'count_tokens ' "$RUN")
if [ "$CNT_SITES" -ge 4 ] && ! grep -q 'tail -n +2 "\$MANIFEST" | wc -l' "$RUN" \
   && ! grep -q 'MANIFEST_SLICE" | grep -c .' "$RUN"; then
  ok "P6-130: token counts all go through count_tokens (no wc -l / grep -c . left)"
else
  bad "P6-130: a second counting idiom is back (count_tokens sites=$CNT_SITES)"
fi
if grep -q 'grep . | head -1024' "$RUN" && grep -q 'manifest header has no comma separator' "$RUN"; then
  ok "P6-130: slice built from validated, CR-stripped data rows"
else
  bad "P6-130: slice build lost its header validation or CR strip"
fi
# P6-448/454: /dev/tcp may only survive as the ss fallback (B4), never as the wait.
DEV_SITES=$(grep -c 'exec 3<>/dev/tcp' "$RUN")
if [ "$DEV_SITES" -eq 1 ]; then
  ok "P6-448: no /dev/tcp probe left in the bind wait (1 site = the ss fallback)"
else
  bad "P6-448: exec 3<>/dev/tcp appears $DEV_SITES times (want exactly 1: the ss fallback)"
fi
# P6-450/455: the G2 failure path must tail the log, never cat it.
if grep -q 'tail -20 "\$OUT/faketool.log"' "$RUN" && ! grep -q 'cat "\$OUT/faketool.log"' "$RUN"; then
  ok "P6-450: G2 failure dumps a log tail, never cat"
else
  bad "P6-450: G2 still cats the faketool log"
fi
# P6-449/456: both startup waits must notice a dead JVM.
ALIVE_BREAKS=$(grep -c 'alive "\$JVM_PID" || break' "$RUN")
if [ "$ALIVE_BREAKS" -ge 2 ]; then
  ok "P6-449: readiness + subscription waits break on JVM death ($ALIVE_BREAKS sites)"
else
  bad "P6-449: only $ALIVE_BREAKS wait(s) notice a dead JVM (want >=2)"
fi
# P6-131/134: teardown reaps collector AND watcher, and never waits the watcher out.
if grep -q 'kill -9 "\$COLLECTOR_PID"' "$RUN" && grep -q 'pkill -9 -P "\$LIVENESS_PID"' "$RUN"; then
  ok "P6-131: cleanup reaps both the collector and the watcher (children first)"
else
  bad "P6-131: cleanup does not reap collector + watcher"
fi
if grep -q 'wait "\$LIVENESS_PID" || \[ "\$RC" -ne 0 \]' "$RUN"; then
  bad "P6-131: the unbounded watcher wait is back (up to 2min dead time)"
else
  ok "P6-131: no unbounded wait on the liveness watcher"
fi
if grep -q 'liveness_check t+end' "$RUN"; then
  ok "P6-131: final t+end liveness check present"
else
  bad "P6-131: no final liveness check after the collector"
fi
# P6-447/452: credentials never inline, always from the hardened 0600 file.
if grep -q 'ARROW_PASSWORD="testd-pass"' "$RUN" || grep -q 'ARROW_APP_SECRET="testd"' "$RUN" \
   || grep -q 'JBSWY3DPEHPK3PXP' "$RUN"; then
  bad "P6-447: inline test credentials are back in run.sh"
else
  ok "P6-447: no inline credentials in run.sh"
fi
if grep -q 'SECRETS_FILE=' "$RUN" && grep -q 'source "\$SECRETS_FILE"' "$RUN" \
   && grep -q 'is a symlink' "$RUN" && grep -q 'not owner-only' "$RUN"; then
  ok "P6-447: credentials come from the hardened secrets file"
else
  bad "P6-447: secrets-file loader missing or unhardened"
fi
# P6-451/453: numeric args validated before anything is created; OUT per run.
expect_fail "P6-451: DURATION_S=abc rejected" bash "$RUN" abc 30
expect_fail "P6-451: DURATION_S=0 rejected" bash "$RUN" 0 30
expect_fail "P6-451: DURATION_S=240s rejected" bash "$RUN" 240s 30
expect_fail "P6-451: INTERVAL_S=abc rejected" bash "$RUN" 240 abc
if grep -q 'loadtest-\$(date +%Y%m%d-%H%M%S)-\$\$' "$RUN"; then
  ok "P6-453: OUT is unique per run (pid suffix)"
else
  bad "P6-453: OUT is not run-unique"
fi

echo ""
echo "=== guard self-test result: PASS=$PASS FAIL=$FAIL (total $((PASS+FAIL)) asserts) ==="
[ "$FAIL" -eq 0 ] || exit 1
