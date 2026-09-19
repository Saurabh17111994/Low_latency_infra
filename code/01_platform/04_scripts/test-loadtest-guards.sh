#!/usr/bin/env bash
# Self-test for the load-test guards (audit #9 lessons, 2026-08-28).
# No cluster needed: exercises each guard's FAILURE path in isolation and
# asserts the guard fires (exact exit code / marker), plus the happy path.
#
# W48 (2026-08-30) rewrote the weak asserts (P6-226..230, P6-581..587): each one
# now calls the REAL guard functions lifted out of the scripts — with a declare -F
# check, so a broken extraction fails loudly instead of counting "command not
# found" (127) as a pass — instead of re-implementing the logic or grepping a
# fixture this file wrote itself.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUN="$ROOT/code/01_platform/04_scripts/loadtest-run.sh"
COL="$ROOT/code/01_platform/04_scripts/loadtest-collect.sh"
MANIFEST="${MANIFEST:-$ROOT/../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv}"
PASS=0; FAIL=0
FIX=$(mktemp -d "${TMPDIR:-/tmp}/loadtest-guards.XXXXXX")
trap 'rm -rf "$FIX"' EXIT    # P6-583/585: one fixture dir, removed on ANY exit

ok()  { PASS=$((PASS+1)); echo "  PASS: $1"; }
bad() { FAIL=$((FAIL+1)); echo "  FAIL: $1" >&2; }

# P6-583: capture the output and print it when an assert fails — the old
# /dev/null redirect made a red test undebuggable.
run_capture() { LAST_OUT="$("$@" 2>&1)"; LAST_RC=$?; }
expect_rc() { # expect_rc <want_rc> <desc> <cmd...>
  local want="$1" desc="$2"; shift 2
  run_capture "$@"
  if [ "$LAST_RC" -eq "$want" ]; then ok "$desc"; else
    bad "$desc (want rc=$want, got $LAST_RC; output: $(printf '%s' "$LAST_OUT" | head -3 | tr '\n' '|'))"
  fi
}
expect_fail() { # any non-zero exit is enough
  local desc="$1"; shift
  run_capture "$@"
  if [ "$LAST_RC" -ne 0 ]; then ok "$desc"; else
    bad "$desc (expected non-zero, got 0; output: $(printf '%s' "$LAST_OUT" | head -3 | tr '\n' '|'))"
  fi
}
expect_ok() {
  local desc="$1"; shift
  run_capture "$@"
  if [ "$LAST_RC" -eq 0 ]; then ok "$desc"; else
    bad "$desc (expected 0, got $LAST_RC; output: $(printf '%s' "$LAST_OUT" | head -3 | tr '\n' '|'))"
  fi
}

# P6-230: lift `name() { ... }` out of a script and PROVE it is defined. The old
# sed+eval idiom silently produced an empty definition whenever the col-0 brace
# moved, and the assert then passed because "command not found" is also non-zero.
extract_fn() { # extract_fn <func> <file>
  local fn="$1" file="$2" body
  body=$(sed -n "/^$fn() {/,/^}/p" "$file")
  [ -n "$body" ] || { echo "extract_fn: empty extraction for $fn() from $file" >&2; return 1; }
  eval "$body"
  declare -F "$fn" >/dev/null || { echo "extract_fn: $fn() undefined after eval (range broke?)" >&2; return 1; }
}
need_fn() { # need_fn <func> <file>: extraction must succeed AND define the function
  if extract_fn "$1" "$2"; then ok "lifted $1() out of $(basename "$2")"; else bad "could not lift $1() out of $2"; fi
}

echo "=== G1: faketool bind failure (the REAL port guard must fire) ==="
# P6-226/P6-581: the holder forked and wrote its pid to a FIXED path under /tmp,
# and the test then killed -9 whatever that file contained — a stale file meant
# killing an unrelated, recycled pid; a bare `wait` waited on every job. Now: one
# process, pid from $!, no file, TERM then a targeted bounded wait.
# P6-587: the forked child also inherited the listening socket, so :8899 stayed
# bound for its 8s sleep — the old `sleep 1` "released" assert was a latent flake.
python3 -c "
import socket, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', 8899)); s.listen(1)
time.sleep(30)
" &
HOLDER_PID=$!
need_fn port_8899_free "$RUN"
PORT_HELD=0
for _ in $(seq 1 50); do
  if (exec 3<>/dev/tcp/127.0.0.1/8899) 2>/dev/null; then exec 3>&-; PORT_HELD=1; break; fi
  kill -0 "$HOLDER_PID" 2>/dev/null || break
  sleep 0.1
done
if [ "$PORT_HELD" -eq 1 ]; then
  ok "G1: :8899 held by pid $HOLDER_PID (setup)"
  if grep -q "port 8899 already in use" "$RUN"; then ok "G1: port-busy message present in run.sh"; else bad "G1: port-busy message missing"; fi
  # P6-227: the message grep above passes even with a broken guard — call it.
  expect_rc 1 "G1: real port_8899_free refuses the busy port" port_8899_free
else
  bad "G1: setup — could not hold :8899 (is another process using it?)"
fi
kill "$HOLDER_PID" 2>/dev/null || true
wait "$HOLDER_PID" 2>/dev/null || true
RELEASED=0
for _ in $(seq 1 50); do
  port_8899_free && { RELEASED=1; break; }
  sleep 0.1
done
[ "$RELEASED" -eq 1 ] && ok "G1: :8899 released after the holder died (bounded poll)" || bad "G1: :8899 still busy after the holder died"

echo "=== G3+G7: preflight files (the REAL --check-only path, sandbox tree) ==="
# P6-228: these asserts were `bash -c "test -f /nonexistent"` — a test of the
# shell builtin, not of run.sh's preflight. Copy the runner into a repo-shaped
# sandbox and let its own --check-only decide.
SB="$FIX/sb/streaming_project_New"
SBRUN="$SB/code/01_platform/04_scripts/loadtest-run.sh"
SBMAN="$FIX/sb/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv"
mkdir -p "$(dirname "$SBRUN")" "$SB/code/02_services/01_ingestion/target" \
         "$SB/code/02_services/01_ingestion/go-bridge/faketool" "$(dirname "$SBMAN")"
cp "$RUN" "$SBRUN"
expect_ok "G3: real ingestion.jar exists"  test -f "$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
expect_ok "G7: real manifest exists"       test -f "$MANIFEST"
expect_rc 1 "G3: --check-only refuses a missing jar" bash "$SBRUN" --check-only
run_capture bash "$SBRUN" --check-only
case "$LAST_OUT" in *"ingestion jar missing"*) ok "G3: refusal names the missing jar";; *) bad "G3: refusal did not name the jar: $LAST_OUT";; esac
: > "$SB/code/02_services/01_ingestion/target/ingestion.jar"
: > "$SB/code/02_services/01_ingestion/go-bridge/arrow-bridge"
: > "$SB/code/02_services/01_ingestion/go-bridge/faketool/main.go"
expect_rc 1 "G7: --check-only refuses a missing manifest" bash "$SBRUN" --check-only
head -1025 "$MANIFEST" > "$SBMAN"
expect_ok "G3+G7: a complete sandbox tree passes --check-only" bash "$SBRUN" --check-only

echo "=== G6: token count cap (the runner's OWN counter) ==="
# P6-582: the old assert re-implemented the count with
# `cut -d, -f4 | tr ',' '\n' | grep -c .`, which mis-parses any quoted comma, and
# it never touched count_tokens() — the function the preflight actually calls.
need_fn count_tokens "$RUN"
N=$(count_tokens "$MANIFEST")
[ "$N" -eq 1024 ] && ok "G6: count_tokens(real manifest) = 1024" || bad "G6: count_tokens=$N (want 1024)"
SHORT="$FIX/short.csv"
{ printf 'Exchange,Segment,ExchSeg,Token,Symbol\n'; for i in $(seq 1 10); do printf 'NSE,CM,NSECM,%s,SYM\n' "$i"; done; } > "$SHORT"
N=$(count_tokens "$SHORT")
[ "$N" -eq 10 ] && ok "G6: a 10-row manifest counts 10 (the <1024 refusal case)" || bad "G6: 10-row manifest counted $N"
{ printf 'h1,h2,h3,h4\n'; printf 'a,b,c,d\r\n'; printf '\n'; printf 'e,f,g,h\r'; } > "$SHORT"
N=$(count_tokens "$SHORT")
[ "$N" -eq 2 ] && ok "G6: CRLF and blank lines do not inflate the count" || bad "G6: CRLF/blank handling gave $N (want 2)"

echo "=== G2: faketool real-rate assert (the runner's OWN poll) ==="
# P6-229: the old assert wrote a log and grepped that same file back — it tested
# `grep`, not the runner's G2 logic. wait_real_rate() is lifted from run.sh.
need_fn alive "$RUN"
need_fn wait_real_rate "$RUN"
RL="$FIX/rate.log"
echo "fake HFT broker listening on :8899 (disconnect_after=0 tick_interval_ms=0 real_rate=true real_rate_hz=20)" > "$RL"
sleep 30 & LIVE_PID=$!
expect_rc 0 "G2: a log with real_rate=true is accepted" wait_real_rate "$RL" "$LIVE_PID" 2
echo "fake HFT broker listening (no real_rate)" > "$RL"
expect_rc 1 "G2: a log without real_rate=true is refused" wait_real_rate "$RL" "$LIVE_PID" 2
kill "$LIVE_PID" 2>/dev/null || true
wait "$LIVE_PID" 2>/dev/null || true
expect_rc 1 "G2: a dead faketool pid is refused without waiting the poll out" wait_real_rate "$RL" 999999999 2
if grep -q 'wait_real_rate "\$OUT/faketool.log"' "$RUN"; then
  ok "G2: run.sh calls wait_real_rate at the G2 site"
else
  bad "G2: run.sh G2 site no longer calls wait_real_rate"
fi

echo "=== G4: canonical env block (all required vars present, exactly once) ==="
# P6-584: the count used a BRE alternation and `-ge 1`, so a duplicated var passed
# even though the header claims "once"; and a grep against an unreadable file made
# the integer test error out silently.
if [ -r "$RUN" ]; then ok "G4: run.sh is readable"; else bad "G4: run.sh is not readable"; exit 1; fi
for var in ARROW_HFT_URL ARROW_BRIDGE_BIN INSTRUMENT_MANIFEST_PATH \
           ARROW_MAX_EVENT_AGE_MS FLUSS_BOOTSTRAP RAW_TABLE_NAME TRANSPORT; do
  n=$(grep -cE "(^|[[:space:]])${var}=" "$RUN")
  if [ "$n" -eq 1 ]; then ok "G4: $var defined exactly once in run.sh"; else bad "G4: $var appears $n times (want 1)"; fi
done
# ARROW_INSTRUMENT_TOKENS is deliberately ABSENT from the load-test env block:
# G3 (2026-08-31) made Java the single source of truth for the token set (the
# manifest slice goes to Java, Java hands the bridge the same set via the
# child-env handoff) and explicitly stopped passing the old TOKENS env var to
# the ingestion JVM. W46 re-verified that the PLURAL is canonical in the Java
# config (IngestionConfig.java), so the name is not a typo — the absence is the
# contract. Pin it so a re-typed var cannot silently return.
if grep -q "ARROW_INSTRUMENT_TOKENS" "$RUN"; then
  bad "G4: ARROW_INSTRUMENT_TOKENS re-appeared in run.sh — G3 handoff owns the token set"
else
  ok "G4: ARROW_INSTRUMENT_TOKENS absent (G3 handoff owns the token set)"
fi

echo "=== G8: collector feed-liveness (the collector's OWN classifier + summary) ==="
# P6-229/P6-585: the old asserts wrote a TSV and grepped that same file back. The
# collector's decision logic is now a function, so drive the real thing.
need_fn classify_feed_rate "$COL"
need_fn loadtest_summary "$COL"
check_class() { # check_class <rate> <want> <want_rc> <desc>
  local rate="$1" want="$2" want_rc="$3" desc="$4" got rc=0
  got=$(classify_feed_rate "$rate") || rc=$?
  if [ "$got" = "$want" ] && [ "$rc" -eq "$want_rc" ]; then ok "$desc"; else bad "$desc (got '$got' rc=$rc, want '$want' rc=$want_rc)"; fi
}
check_class 20000 VALID   0 "G8: rate 20000 => VALID"
check_class 500   VALID   0 "G8: rate 500 (the threshold) => VALID"
check_class 499   INVALID 0 "G8: rate 499 => INVALID"
check_class 0     INVALID 0 "G8: rate 0 => INVALID (feed stalled)"
check_class ""    UNKNOWN 1 "G8: empty rate => UNKNOWN (rc 1)"
check_class abc   UNKNOWN 1 "G8: non-numeric rate => UNKNOWN (rc 1)"
SNAP="$FIX/snapshots.tsv"
HDR=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' ts feed_rate feed_valid flink_rss flink_cpu ckpt_ok ckpt_fail p99_dedup p99_writer p99_detection p99_builder p99_sink)
{ printf '%s\n' "$HDR"
  printf '%s\t0\tINVALID\t3.3GiB\t10%%\t1\t0\t100\t200\t300\t400\t5\n' "2026-08-28T00:00:00+05:30"
  printf '%s\t0\tINVALID\t3.3GiB\t10%%\t1\t0\tn/a\tn/a\tn/a\tn/a\tn/a\n' "2026-08-28T00:00:30+05:30"
} > "$SNAP"
run_capture loadtest_summary "$SNAP" 0 2 0
case "$LAST_OUT" in *"NO VALID snapshots"*) ok "G8: an INVALID-only fixture trips the loud warning";; *) bad "G8: no warning for INVALID-only: $LAST_OUT";; esac
case "$LAST_OUT" in *"2 INVALID (feed stalled)"*) ok "G8: the INVALID count is reported";; *) bad "G8: INVALID count missing";; esac
{ printf '%s\n' "$HDR"
  printf '%s\t20000\tVALID\t3.3GiB\t10%%\t1\t0\tn/a\tn/a\tn/a\tn/a\tn/a\n' "2026-08-28T00:01:00+05:30"
} > "$SNAP"
run_capture loadtest_summary "$SNAP" 1 0 0
case "$LAST_OUT" in *"p99_dedup: n/a in ALL 1 VALID"*) ok "G8: an all-n/a p99 column is called out (renamed/chained operator)";; *) bad "G8: renamed-column warning missing: $LAST_OUT";; esac
{ printf '%s\n' "$HDR"
  printf '%s\t20000\tVALID\t3.3GiB\t10%%\t1\t0\t120\t220\t320\t420\t6\n' "2026-08-28T00:01:30+05:30"
} > "$SNAP"
run_capture loadtest_summary "$SNAP" 1 0 0
case "$LAST_OUT" in *"feed_rate column (first VALID row): 20000"*) ok "G8: a healthy fixture reports its first VALID feed_rate";; *) bad "G8: first-VALID feed_rate missing: $LAST_OUT";; esac
case "$LAST_OUT" in *"n/a in ALL"*) bad "G8: false n/a warning on a healthy fixture";; *) ok "G8: a healthy fixture produces no n/a warning";; esac

echo "=== B1: rate validity (RATE_HZ must divide 1000) ==="
if grep -q "must divide 1000" "$RUN"; then ok "B1: rate-validity message present in run.sh"; else bad "B1: rate-validity message missing"; fi
expect_fail "B1: RATE_HZ=15 rejected (does not divide 1000)" env RATE_HZ=15 bash "$RUN" --check-only
expect_ok  "B1: RATE_HZ=20 accepted" env RATE_HZ=20 bash "$RUN" --check-only

echo "=== B2: minimum-duration guard (both sides of the boundary, exact rc) ==="
if grep -q "lt 190" "$COL"; then ok "B2: min-duration guard present in collect.sh"; else bad "B2: min-duration guard missing"; fi
need_fn require_min_duration "$COL"
rc=0; ( require_min_duration 189 ) >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 2 ] && ok "B2: require_min_duration 189 => rc=2" || bad "B2: 189 gave rc=$rc (want 2)"
rc=0; ( require_min_duration 190 ) >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 0 ] && ok "B2: require_min_duration 190 => accepted" || bad "B2: 190 gave rc=$rc (want 0)"
OUT_TMP="$FIX/collect-refused"
expect_rc 2 "B2: collect with duration 90 exits exactly 2" bash "$COL" "$OUT_TMP" 90 30 xyz
[ ! -e "$OUT_TMP" ] && ok "B2: the refusal created no out_dir" || bad "B2: out_dir created before the duration guard"
run_capture bash "$COL" "$FIX/collect-bad" 240 abc
case "$LAST_OUT" in *"INTERVAL_S='abc' is not a positive integer"*) ok "P6-441: a bad INTERVAL_S is refused by name";; *) bad "P6-441: bad-INTERVAL_S message missing: $LAST_OUT";; esac
[ "$LAST_RC" -eq 2 ] && ok "P6-441: a bad INTERVAL_S exits 2" || bad "P6-441: bad INTERVAL_S rc=$LAST_RC (want 2)"
[ ! -e "$FIX/collect-bad" ] && ok "P6-441: bad args created nothing" || bad "P6-441: bad args created $FIX/collect-bad"

echo "=== B3: mid-run liveness (exact codes, no false pass) ==="
if grep -q "t+180" "$RUN"; then ok "B3: t+60/t+180 liveness checks present in run.sh"; else bad "B3: liveness watcher missing"; fi
need_fn alive "$RUN"
need_fn liveness_check "$RUN"
rc=0; ( liveness_check t+60 999999999 faketool ) >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && ok "B3: liveness_check on a dead pid => rc=1 (not merely non-zero)" || bad "B3: dead pid gave rc=$rc (want 1)"
sleep 30 & LP=$!
rc=0; ( liveness_check t+60 "$LP" faketool ) >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 0 ] && ok "B3: liveness_check on a live pid => rc=0" || bad "B3: live pid gave rc=$rc (want 0)"
kill "$LP" 2>/dev/null || true
wait "$LP" 2>/dev/null || true

echo "=== B4: ss-based port check (free port passes) ==="
if grep -q "ss -tln" "$RUN"; then ok "B4: ss -tln check present in run.sh"; else bad "B4: ss check missing"; fi
rc=0; ( port_8899_free ) >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 0 ] && ok "B4: port_8899_free => rc=0 when :8899 is free" || bad "B4: free-port check gave rc=$rc (want 0)"

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

echo "=== P6-123..126 / P6-441..446: wave-48 invariants (collector) ==="
# P6-123/442: no shared predictable scrape file; private temp dir; status checked.
if grep -qF '/tmp/loadtest-metrics.txt' "$COL"; then bad "P6-123: the shared /tmp scrape file is back"; else ok "P6-123: no shared /tmp scrape file"; fi
if grep -q 'mktemp -d' "$COL" && grep -q '%{http_code}' "$COL"; then ok "P6-123: private scrape dir + curl status checked"; else bad "P6-123: private scrape dir or curl status check missing"; fi
# P6-125: a failed scrape is UNKNOWN, never VALID or INVALID.
if grep -q 'classify_feed_rate' "$COL" && grep -q 'UNKNOWN_COUNT' "$COL"; then ok "P6-125: a failed scrape is tagged UNKNOWN"; else bad "P6-125: the UNKNOWN path is missing"; fi
# P6-126: p99 keys are matched per sanitized CHAIN SEGMENT, so a chained or
# renamed operator keeps its column instead of silently turning n/a.
if grep -q 'p99_row' "$COL" && grep -qF '"____"' "$COL"; then ok "P6-126: p99 lookup aliases sanitized chain segments"; else bad "P6-126: chain-segment aliasing missing"; fi
# P6-443: the never-called per-operator rate dump is gone, and no REST call is
# hardcoded to localhost any more (PROM/FLINK env overrides own the endpoints).
if grep -q '^operator_rates() {' "$COL"; then bad "P6-443: the dead per-operator rate dump is back"; else ok "P6-443: dead per-operator rate dump removed"; fi
if grep -q 'localhost:8081/jobs/' "$COL"; then bad "P6-443: a hardcoded localhost REST call is back"; else ok "P6-443: no hardcoded REST endpoint"; fi
# P6-444/446: printf for headers and rows, explicit n/a defaults, shape checked.
if grep -q 'echo -e ' "$COL"; then bad "P6-444: echo -e is back (mangles tabs and backslashes)"; else ok "P6-444: no echo -e in the collector"; fi
if grep -qF "printf '%s\t%s" "$COL"; then ok "P6-444: headers and rows are written with printf"; else bad "P6-444: printf headers/rows missing"; fi
if grep -q 'row_shape_ok' "$COL"; then ok "P6-446: snapshot and busy rows are shape-checked"; else bad "P6-446: row shape check missing"; fi
# P6-445: ONE container-stats query per snapshot, container overridable, units
# normalized (the old pair left a trailing space and mixed MiB/GiB).
DSITES=$(grep -cF 'docker stats' "$COL")
if [ "$DSITES" -eq 1 ]; then ok "P6-445: one container-stats query per snapshot"; else bad "P6-445: $DSITES container-stats call sites (want 1)"; fi
if grep -q 'TM_CONTAINER' "$COL" && grep -q 'mem_to_mib' "$COL"; then ok "P6-445: container env-overridable, memory normalized to MiB"; else bad "P6-445: container override or unit normalization missing"; fi
# P6-124: the background sampler is reaped (killed AND waited) by a trap.
if grep -q 'trap cleanup EXIT' "$COL" && grep -q 'wait "\$BUSY_PID"' "$COL"; then ok "P6-124: sampler killed and waited on exit"; else bad "P6-124: sampler lifecycle trap missing"; fi
# P6-441: out_dir created (the old script redirected into a directory it assumed).
if grep -q 'mkdir -p "\$OUT"' "$COL"; then ok "P6-441: out_dir is created"; else bad "P6-441: mkdir -p \$OUT missing"; fi

echo ""
echo "=== guard self-test result: PASS=$PASS FAIL=$FAIL (total $((PASS+FAIL)) asserts) ==="
[ "$FAIL" -eq 0 ] || exit 1
