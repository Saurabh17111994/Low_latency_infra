#!/usr/bin/env bash
# W41 (P6-027, 289, 295, 682, 683, 685, 686, 691, 692, 693, 694) —
# ingestion launcher contract.
#
# Contract under test, for both launchers:
#   code/run-ingestion-full.sh   (chained full pipeline)
#   run-ingestion.sh             (repo-root entry, execs the above)
#
#   TOTP-only: ARROW_TOKEN is removed (2026-08-24). A secrets file that sets
#     it must FATAL in the launcher with that wording — not silently skip the
#     USER/PASSWORD/TOTP guards and let the JVM reject it later.
#   Credentials sourced from the secrets file MUST reach the child process.
#   The secrets file must be a regular file, owner-only (600 or 400), not a
#     symlink.
#   A missing manifest is a friendly FATAL, never a raw `cut` error.
#   The child's exit code is the launcher's exit code.
#
# Behavioural: every case executes the real launcher against stub tooling
# (fake java, fake bridge, a real TCP listener for the Fluss probe, a 1024-row
# manifest), so the assertions pin observable behaviour rather than file text.
# No live stack, no docker, no network beyond 127.0.0.1.
#
# Run directly:  bash test_ingestion_launchers.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# tests/ → 04_scripts/ → 01_platform/ → code/
CODE_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
REPO_ROOT="$(cd "$CODE_DIR/.." && pwd)"
FULL="$CODE_DIR/run-ingestion-full.sh"
ENTRY="$REPO_ROOT/run-ingestion.sh"

for f in "$FULL" "$ENTRY"; do
	if [ ! -r "$f" ]; then
		echo "FAIL: launcher not readable: $f"
		exit 1
	fi
done

FAILED=0
PASSED=0

ok() { echo "ok: $1"; PASSED=$((PASSED + 1)); }
bad() { echo "FAIL: $1"; FAILED=1; }

assert_eq() { # $1=got $2=want $3=label
	if [ "$1" != "$2" ]; then
		bad "$3 — got exit=$1 want exit=$2"
		return 1
	fi
	ok "$3 (exit=$1)"
}

assert_contains() { # $1=haystack $2=needle $3=label
	if [[ "$1" != *"$2"* ]]; then
		bad "$3 — message not found: $2"
		echo "--- captured output ---"
		echo "$1"
		echo "----------------------"
		return 1
	fi
	ok "$3"
}

assert_not_contains() { # $1=haystack $2=needle $3=label
	if [[ "$1" == *"$2"* ]]; then
		bad "$3 — unexpected text present: $2"
		return 1
	fi
	ok "$3"
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"

# ── Stub tooling ─────────────────────────────────────────────────────────────
# Fake java: reports 17 for -version, and on a pipeline run dumps its
# environment so the test can prove what the child actually inherited.
cat > "$TMP/bin/java" <<'STUB'
#!/usr/bin/env bash
for a in "$@"; do
	if [ "$a" = "-version" ]; then
		echo 'openjdk version "17.0.19" 2026-04-21' >&2
		exit 0
	fi
done
if [ -n "${ENV_DUMP:-}" ]; then
	env > "$ENV_DUMP"
fi
exit "${STUB_JAVA_EXIT:-0}"
STUB
chmod +x "$TMP/bin/java"

# The launcher derives JAR and ARROW_BRIDGE_BIN from CODE_DIR, and both must
# exist for the preflight to pass. Build a minimal stub CODE_DIR tree that
# satisfies those two paths, so no real build is needed.
make_stub_tree() { # $1 = code dir to populate
	mkdir -p "$1/02_services/01_ingestion/target" \
		"$1/02_services/01_ingestion/go-bridge"
	: > "$1/02_services/01_ingestion/target/ingestion.jar"
	: > "$1/02_services/01_ingestion/go-bridge/arrow-bridge"
	chmod +x "$1/02_services/01_ingestion/go-bridge/arrow-bridge"
}
STUB_CODE="$TMP/tree/code"
make_stub_tree "$STUB_CODE"

# 1024-instrument manifest: header + 1024 rows, token in column 4.
MANIFEST="$TMP/manifest.csv"
{
	echo "name,exchange,segment,instrument_token"
	i=1
	while [ "$i" -le 1024 ]; do
		echo "INSTR$i,NSE,CM,$i"
		i=$((i + 1))
	done
} > "$MANIFEST"

# Real TCP listener so the launcher's Fluss reachability probe succeeds.
PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
python3 -c "
import socket,sys
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('127.0.0.1',$PORT)); s.listen(8)
while True:
    try: c,_=s.accept(); c.close()
    except Exception: break
" &
LISTENER_PID=$!
trap 'rm -rf "$TMP"; kill "$LISTENER_PID" 2>/dev/null || true' EXIT
sleep 0.3
FLUSS="127.0.0.1:$PORT"

# Owner-only secrets file with the TOTP triad.
SECRETS="$TMP/secrets.env"
cat > "$SECRETS" <<'CREDS'
ARROW_APP_ID=app-id-value
ARROW_APP_SECRET=app-secret-value
ARROW_USER_ID=user-id-value
ARROW_PASSWORD=password-value
ARROW_TOTP_KEY=totp-key-value
CREDS
chmod 600 "$SECRETS"

# run_full launches the real full launcher with stub tooling.
# Extra env vars are appended by the caller via "$@".
run_full() {
	local out rc
	out="$(env -i \
		PATH="$TMP/bin:/usr/bin:/bin" \
		HOME="$TMP" \
		ENV_DUMP="$TMP/child-env.txt" \
		SECRETS_FILE="$SECRETS" \
		ARROW_INSTRUMENT_MANIFEST="$MANIFEST" \
		CODE_DIR="$STUB_CODE" \
		FLUSS_BOOTSTRAP="$FLUSS" \
		UNCERTAINTY_JOURNAL_DIR="$TMP/journal" \
		"$@" \
		bash "$FULL" 2>&1)"
	rc=$?
	printf '%s|%s' "$rc" "$out"
}

split_run() { # $1 = "<exit>|<output>"
	RUN_EXIT="${1%%|*}"
	RUN_OUT="${1#*|}"
}

# ── Case 1: ARROW_TOKEN set → FATAL (P6-027) ─────────────────────────────────
# TOTP-only since 2026-08-24. The buggy launcher skipped the USER/PASSWORD/
# TOTP guards whenever ARROW_TOKEN was set, so a token-only secrets file got
# all the way to the JVM, which then rejected it. The launcher must fail
# itself, with the removal stated.
echo "=== case 1: ARROW_TOKEN set must FATAL in the launcher (P6-027) ==="
TOKEN_SECRETS="$TMP/secrets-token.env"
cat > "$TOKEN_SECRETS" <<'CREDS'
ARROW_APP_ID=app-id-value
ARROW_APP_SECRET=app-secret-value
ARROW_TOKEN=legacy-token-value
CREDS
chmod 600 "$TOKEN_SECRETS"
split_run "$(run_full SECRETS_FILE="$TOKEN_SECRETS")"
assert_eq "$RUN_EXIT" 1 "token-only secrets exits 1"
assert_contains "$RUN_OUT" "ARROW_TOKEN removed 2026-08-24" "token removal is stated"
assert_not_contains "$RUN_OUT" "instrument token count" "guard fires before manifest parsing"

# ── Case 2: missing manifest → friendly FATAL (P6-289) ───────────────────────
# The manifest was parsed (cut/tail/sed/paste) before its own -f preflight, so
# a missing path surfaced as a raw `cut` error with no FATAL line.
echo "=== case 2: missing manifest must be a friendly FATAL (P6-289) ==="
split_run "$(run_full ARROW_INSTRUMENT_MANIFEST="$TMP/no-such-manifest.csv")"
assert_eq "$RUN_EXIT" 1 "missing manifest exits 1"
assert_contains "$RUN_OUT" "instrument manifest not found" "missing manifest is a FATAL"
assert_not_contains "$RUN_OUT" "cut: " "no raw cut error leaks"

# ── Case 3: symlinked secrets file → FATAL (P6-682) ──────────────────────────
# `-f` follows symlinks and `stat` measures the target, so an attacker-swapped
# link was accepted; `source` then ran code with caller privileges.
echo "=== case 3: symlinked secrets file must be refused (P6-682) ==="
ln -s "$SECRETS" "$TMP/secrets-link.env"
split_run "$(run_full SECRETS_FILE="$TMP/secrets-link.env")"
assert_eq "$RUN_EXIT" 1 "symlinked secrets exits 1"
assert_contains "$RUN_OUT" "symlink" "symlink refusal is stated"

# ── Case 4: mode 400 secrets accepted (P6-682) ───────────────────────────────
# 400 is equally owner-only; a strict `!= 600` rejected it.
echo "=== case 4: mode 400 secrets file is accepted (P6-682) ==="
MODE400="$TMP/secrets-400.env"
cp "$SECRETS" "$MODE400"
chmod 400 "$MODE400"
split_run "$(run_full SECRETS_FILE="$MODE400" STUB_JAVA_EXIT=7)"
assert_eq "$RUN_EXIT" 7 "mode 400 secrets accepted, child exit code propagated"

# ── Case 5: credentials reach the child, ARROW_TOKEN does not (P6-027) ───────
# Also proves the child's exit code is the launcher's (P6-686): a non-zero
# `wait` under `set -e` used to abort before the code was captured.
echo "=== case 5: child environment + exit code (P6-027, P6-686) ==="
rm -f "$TMP/child-env.txt"
split_run "$(run_full STUB_JAVA_EXIT=0)"
assert_eq "$RUN_EXIT" 0 "clean child exits 0"
if [ -r "$TMP/child-env.txt" ]; then
	CHILD_ENV="$(cat "$TMP/child-env.txt")"
	assert_contains "$CHILD_ENV" "ARROW_APP_ID=app-id-value" "ARROW_APP_ID reaches the child"
	assert_contains "$CHILD_ENV" "ARROW_TOTP_KEY=totp-key-value" "ARROW_TOTP_KEY reaches the child"
	assert_not_contains "$CHILD_ENV" "ARROW_TOKEN" "ARROW_TOKEN never reaches the child"
else
	bad "child environment was not captured — the launcher never ran the JVM"
fi

# ── Case 6: run log name is unique per launch (P6-685) ───────────────────────
# The name had 1-second granularity, so two launchers in the same second shared
# one file and interleaved through `tee -a`. Time is frozen with a stub `date`
# that always reports the same second, so this asserts the filename contract
# itself rather than hoping two launches land in the same second. (Two real
# concurrent launches cannot be used here: the duplicate-JVM guard correctly
# refuses the second one before it writes a log.)
echo "=== case 6: run log name is collision-free (P6-685) ==="
LOG_DIR_FROZEN="$TMP/journal-frozen"
# Stub date: same timestamp for every call, so only the PID can distinguish the
# two runs. Delegates to the real date for anything but +FORMAT.
cat > "$TMP/bin/date" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = "+%Y%m%d-%H%M%S" ]; then
	echo "20260101-000000"
	exit 0
fi
exec /usr/bin/date "$@"
STUB
chmod +x "$TMP/bin/date"

RUN_LOG_1="$(env -i \
	PATH="$TMP/bin:/usr/bin:/bin" \
	HOME="$TMP" \
	ENV_DUMP="$TMP/child-env-frozen.txt" \
	SECRETS_FILE="$SECRETS" \
	ARROW_INSTRUMENT_MANIFEST="$MANIFEST" \
	CODE_DIR="$STUB_CODE" \
	FLUSS_BOOTSTRAP="$FLUSS" \
	UNCERTAINTY_JOURNAL_DIR="$LOG_DIR_FROZEN" \
	STUB_JAVA_EXIT=0 \
	bash "$FULL" 2>&1 | grep -o 'log -> [^ ]*' | head -1)"
RUN_LOG_2="$(env -i \
	PATH="$TMP/bin:/usr/bin:/bin" \
	HOME="$TMP" \
	ENV_DUMP="$TMP/child-env-frozen.txt" \
	SECRETS_FILE="$SECRETS" \
	ARROW_INSTRUMENT_MANIFEST="$MANIFEST" \
	CODE_DIR="$STUB_CODE" \
	FLUSS_BOOTSTRAP="$FLUSS" \
	UNCERTAINTY_JOURNAL_DIR="$LOG_DIR_FROZEN" \
	STUB_JAVA_EXIT=0 \
	bash "$FULL" 2>&1 | grep -o 'log -> [^ ]*' | head -1)"
if [ -n "$RUN_LOG_1" ] && [ -n "$RUN_LOG_2" ] && [ "$RUN_LOG_1" != "$RUN_LOG_2" ]; then
	ok "two launches in the same second choose different run logs"
else
	bad "same-second launches share one run log ('$RUN_LOG_1' vs '$RUN_LOG_2') — runs interleave"
fi
LOG_COUNT="$(find "$LOG_DIR_FROZEN" -name 'ingestion-*.log' | wc -l | tr -d ' ')"
if [ "$LOG_COUNT" -ge 2 ]; then
	ok "both same-second launches left their own log file ($LOG_COUNT)"
else
	bad "both same-second launches must leave a log file (found $LOG_COUNT)"
fi
# ── Case 7: root entry launcher — env + exec contract ────────────────────────
# run-ingestion.sh is exercised from a copied tree so its relative exec target
# is a stub that dumps the environment it received. Covers P6-295 (sourced
# creds were shell-only and never exported), P6-693 (defaults forced with
# `export` blocked operator override) and P6-694 (bare exec, no check).
echo "=== case 7: root launcher export/exec contract (P6-295, P6-693, P6-694) ==="
COPY="$TMP/copy"
mkdir -p "$COPY/code"
cp "$ENTRY" "$COPY/run-ingestion.sh"
cat > "$COPY/code/run-ingestion-full.sh" <<'STUB'
#!/usr/bin/env bash
env > "$ENV_DUMP"
exit "${STUB_JAVA_EXIT:-0}"
STUB
chmod +x "$COPY/code/run-ingestion-full.sh"

ENTRY_SECRETS="$TMP/secrets-entry.env"
cat > "$ENTRY_SECRETS" <<'CREDS'
ARROW_APP_ID=app-id-value
ARROW_APP_SECRET=app-secret-value
ARROW_USER_ID=user-id-value
ARROW_PASSWORD=password-value
ARROW_TOTP_KEY=totp-key-value
CREDS
chmod 600 "$ENTRY_SECRETS"

rm -f "$TMP/entry-env.txt"
ENTRY_OUT_CAPTURE="$(env -i \
	PATH="$TMP/bin:/usr/bin:/bin" \
	HOME="$TMP" \
	ENV_DUMP="$TMP/entry-env.txt" \
	SECRETS_FILE="$ENTRY_SECRETS" \
	FLUSS_BOOTSTRAP="remote-fluss:9123" \
	ARROW_HFT_LATENCY_MS=999 \
	bash "$COPY/run-ingestion.sh" 2>&1)"
ENTRY_RC=$?
assert_eq "$ENTRY_RC" 0 "root launcher execs the chained launcher"
if [ -r "$TMP/entry-env.txt" ]; then
	ENTRY_ENV="$(cat "$TMP/entry-env.txt")"
	assert_contains "$ENTRY_ENV" "ARROW_TOTP_KEY=totp-key-value" "sourced creds are exported to the child (P6-295)"
	assert_contains "$ENTRY_ENV" "FLUSS_BOOTSTRAP=remote-fluss:9123" "FLUSS_BOOTSTRAP override respected (P6-693)"
	assert_contains "$ENTRY_ENV" "ARROW_HFT_LATENCY_MS=999" "ARROW_HFT_LATENCY_MS override respected (P6-693)"
else
	bad "root launcher child environment not captured"
fi

# Missing exec target must be a friendly FATAL, not a bare exec error.
rm -f "$COPY/code/run-ingestion-full.sh"
ENTRY_OUT_CAPTURE="$(env -i \
	PATH="$TMP/bin:/usr/bin:/bin" \
	HOME="$TMP" \
	SECRETS_FILE="$ENTRY_SECRETS" \
	bash "$COPY/run-ingestion.sh" 2>&1)"
ENTRY_RC=$?
assert_eq "$ENTRY_RC" 1 "missing chained launcher exits 1 (P6-694)"
assert_contains "$ENTRY_OUT_CAPTURE" "FATAL" "missing chained launcher is a FATAL (P6-694)"

# ARROW_TOKEN in the root launcher's secrets file must also FATAL (P6-027).
ENTRY_OUT_CAPTURE="$(env -i \
	PATH="$TMP/bin:/usr/bin:/bin" \
	HOME="$TMP" \
	SECRETS_FILE="$TOKEN_SECRETS" \
	bash "$COPY/run-ingestion.sh" 2>&1)"
ENTRY_RC=$?
assert_eq "$ENTRY_RC" 1 "root launcher rejects ARROW_TOKEN (P6-027)"
assert_contains "$ENTRY_OUT_CAPTURE" "ARROW_TOKEN removed 2026-08-24" "root launcher states token removal"

# ── Case 8: manifest default is checkout-relative (P6-683) ───────────────────
# The default was a hardcoded /home/<user>/... absolute path, so the launcher
# only worked on the machine it was written on. Copy the tree with a sibling
# Arrow_broker/ and run with ARROW_INSTRUMENT_MANIFEST unset.
echo "=== case 8: manifest default resolves relative to the checkout (P6-683) ==="
REL="$TMP/reloc"
mkdir -p "$REL/repo/code" "$REL/Arrow_broker/instruments/cash_stocks"
cp "$FULL" "$REL/repo/code/run-ingestion-full.sh"
make_stub_tree "$REL/repo/code"
cp "$MANIFEST" "$REL/Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv"
REL_OUT="$(env -i \
	PATH="$TMP/bin:/usr/bin:/bin" \
	HOME="$TMP" \
	ENV_DUMP="$TMP/rel-env.txt" \
	SECRETS_FILE="$SECRETS" \
	CODE_DIR="$REL/repo/code" \
	FLUSS_BOOTSTRAP="$FLUSS" \
	UNCERTAINTY_JOURNAL_DIR="$TMP/journal" \
	STUB_JAVA_EXIT=0 \
	bash "$REL/repo/code/run-ingestion-full.sh" 2>&1)"
REL_RC=$?
assert_eq "$REL_RC" 0 "relocated checkout runs without an absolute manifest default"
assert_not_contains "$REL_OUT" "/home/" "no /home/<user> path in the relocated run"

# ── Verdict ──────────────────────────────────────────────────────────────────
echo
if [ "$FAILED" = 1 ]; then
	echo "W41 ingestion launchers: FAIL ($PASSED assertions passed)"
	exit 1
fi
echo "W41 ingestion launchers: PASS ($PASSED assertions)"
