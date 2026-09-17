#!/usr/bin/env bash
# run-monday-gates.sh — ONE command to run the full Monday verification gate.
#
# Orchestrates Workstream A from the Monday soak plan:
# The step list is deliberately NOT enumerated here: the script prints
# `[N/19] <step>` as it runs and writes the authoritative verdicts to
# $OUT_DIR/SUMMARY.txt. A hand-maintained copy only drifts — this header
# claimed five steps while the script ran thirteen.
#
# Coverage: static checks, compose config, Python unit suites, entrypoint
# harness, Go bridge suite (-race), E2E test binaries, docker build smoke,
# image staleness (scoped to ddl-apply, the one service image the gate starts),
# the full Java gate + live Fluss drills, the execution gateway / compute / Rust
# module suites, the full doc audit,
# the DDL apply smoke + evidence-ownership check, the schema/perf certification
# gates, and the CHG-015 SIGTERM-drain regression. The E2E-binary and the
# SIGTERM-drain steps are explicit because `go test` builds neither E2E binary
# (R-016) and a pom/surefire change could silently drop or env-gate the drain
# tests (ING-UNIT-023/024), so `make full-audit` and the Java suite alone would
# not notice.
#
# Usage:  ./run-monday-gates.sh
#   - The script FAILS (exit != 0) if any suite fails, so you can wire it
#     into CI or a cron.
#
# Repair runs: do NOT re-run the whole gate once per fix. Run the failing step
# standalone with the same env — copy its block from this file, its log lands in
# $OUT_DIR — falsify the fix against the pre-fix code, and only then re-run the
# full gate. A fix touching image sources needs `make images` FIRST: step 8
# compares the images against the tree, so an un-rebuilt tree fails there (or,
# worse, passes against a stale image). Probing several never-run steps in one
# batch beats one gate cycle per step.
#
# Modes (a repair loop does not have to pay for all 19 steps, or for one failure
# at a time; the certifying run below stays fail-fast and unchanged):
#   --steps LIST  run only those steps, e.g. `--steps 12-19` or `--steps 9,11`.
#                 SUBSET mode: SUMMARY ends with SUBSET RESULT, never GATE RESULT,
#                 so a scoped green can not be quoted as a certificate.
#   --sweep       run the selection to the end past failing steps (each failure is
#                 recorded and the run resumes at the next step) and report every
#                 failure as SWEEP RESULT. Non-certifying.
# The certificate is this script with no arguments: 19/19, fail-fast.
#

# Prereqs: Fluss up (docker compose up -d), local ~/.m2 warm, Go 1.24+, JDK 17.

set -euo pipefail

# ── Arguments (see Modes in the header) ──────────────────────────────────────
STEP_SELECTION=""
SWEEP=0
# note = print REPLAYED@<time> and keep going; refuse = --no-replay
REPLAY_POLICY="note"
while [ $# -gt 0 ]; do
	case "$1" in
		--steps)
			[ $# -ge 2 ] || { echo "FATAL: --steps needs a list, e.g. --steps 12-19" >&2; exit 2; }
			STEP_SELECTION="$2"; shift 2 ;;
		--steps=*) STEP_SELECTION="${1#--steps=}"; shift ;;
		--sweep) SWEEP=1; shift ;;
		--no-replay) REPLAY_POLICY="refuse"; shift ;;
		--help|-h)
			echo "usage: $(basename "$0") [--steps LIST] [--sweep] [--no-replay]"
			echo "  --steps 9,11,12-19  run only those steps (subset: never certifies)"
			echo "  --sweep             run the selection, continue past failures, report all"
			echo "  --no-replay         refuse to certify a (tree, stack) pair that was already certified"
			exit 0 ;;
		*) echo "FATAL: unknown argument '$1' (try --help)" >&2; exit 2 ;;
	esac
done


# ── Config (override via env; defaults derived from the script location) ─────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
CODE_DIR="${CODE_DIR:-$PROJECT_ROOT/code}"
BRIDGE_DIR="${BRIDGE_DIR:-$CODE_DIR/02_services/01_ingestion/go-bridge}"
COMPUTE_DIR="$CODE_DIR/02_services/02_compute"
MOCK_ARROW_DIR="$CODE_DIR/02_services/05_mock_arrow"
EXECUTOR_DIR="$CODE_DIR/02_services/04_executor"
INGESTION_DIR="${INGESTION_DIR:-$CODE_DIR/02_services/01_ingestion}"
OUT_DIR="${OUT_DIR:-$PROJECT_ROOT/logs/soak/monday-gates-$(date +%Y%m%d-%H%M%S)}"
# Written by a failing sweep child, read by the driver (must exist in both).
SWEEP_FAILED_FILE="$OUT_DIR/sweep-failed.txt"
# Compose paths: constant for the whole run, so they live here and not inside
# step 2 — `--steps 8` skips step 2 and read these under `set -u`.
COMPOSE_FILE="$CODE_DIR/01_platform/01_docker/docker-compose.yml"
COMPOSE_ENV_DIR="$(dirname "$COMPOSE_FILE")"

# Per-run log paths, fixed up front: --steps may skip the step that writes one,
# and the evidence list and the steps themselves reference them by name.
GO_LOG="$OUT_DIR/go-suite.log"
JAVA_LOG="$OUT_DIR/java-suite.log"
PREFLIGHT_LOG="$OUT_DIR/preflight.log"
STATIC_LOG="$OUT_DIR/static-checks.log"
GUARD_LOG="$OUT_DIR/harness-guards.log"

# ── Failure labelling ────────────────────────────────────────────────────────
# A run that dies before committing to a verdict — a `set -u` abort, a SIGTERM,
# an unexpected command failure — leaves a truncated SUMMARY that reads like a
# broken repository. GATE_DECIDED is set wherever this script does commit to one
# (a step failure, a refusal, the verdict lines), and GATE_RUN_LIVE once the steps
# are actually being attempted, so the EXIT trap can tell "the harness broke"
# apart from "a step failed". A successful sweep child exits 0 with no verdict of
# its own, hence the non-zero exit requirement.
GATE_DECIDED=0
GATE_RUN_LIVE=0
_harness_abort() {
	rc=$?
	if [ "$GATE_DECIDED" = 0 ] && [ "$GATE_RUN_LIVE" = 1 ] && [ "$rc" -ne 0 ]; then
		# Plain redirects, not `tee`: this runs during exit and must not recurse.
		echo "" >&2
		echo "HARNESS ABORT — run-monday-gates.sh exited $rc without reaching a step decision." >&2
		echo "  NOT a repository failure: no step reported FAIL and no verdict was printed." >&2
		echo "  The last step banner above shows how far the run got; see $SUMMARY" >&2
		{
			echo ""
			echo "HARNESS ABORT — exited $rc without reaching a step decision (no step FAIL, no verdict)."
			echo "  This is the gate harness, not the repository. Last banner above shows the extent."
		} >>"$SUMMARY" 2>/dev/null || true
	fi
}
trap '_harness_abort; rm -f "${_script_list:-}"' EXIT
# A kill must say the same thing instead of leaving a truncated SUMMARY behind.
trap 'exit 143' TERM
trap 'exit 130' INT
PY_LOG="$OUT_DIR/python-tests.log"
ENTRYPOINT_LOG="$OUT_DIR/entrypoint.log"
IMAGE_LOG="$OUT_DIR/image-staleness.log"
DRILL_LOG="$OUT_DIR/drill-live.log"
AUDIT_LOG="$OUT_DIR/full-audit.log"
DDL_SMOKE_LOG="$OUT_DIR/ddl-smoke.log"
SCHEMA_PERF_LOG="$OUT_DIR/schema-perf.log"
SHUTDOWN_LOG="$OUT_DIR/shutdown-regression.log"
GATEWAY_LOG="$OUT_DIR/gateway-suite.log"
NAUTILUS_LOG="$OUT_DIR/nautilus-suite.log"
COMPUTE_LOG="$OUT_DIR/compute-suite.log"
MOCK_ARROW_LOG="$OUT_DIR/mock-arrow-suite.log"
COMPOSE_CONFIG_LOG="$OUT_DIR/compose-config.log"
DOCKER_BUILD_LOG="$OUT_DIR/docker-build-smoke.log"
E2E_BUILD_LOG="$OUT_DIR/e2e-build.log"
OWNERSHIP_LOG="$OUT_DIR/evidence-ownership.log"
PIN_LOG="$OUT_DIR/pin-discipline.log"
IMAGES_LOG="$OUT_DIR/images-build.log"
STALE_ALL_LOG="$OUT_DIR/image-staleness-all.log"

# Suite timeouts (R-281) — a stuck JVM/Fluss must not block the gate forever.
GO_TIMEOUT_SEC="${GO_TIMEOUT_SEC:-1800}"
JAVA_TIMEOUT_SEC="${JAVA_TIMEOUT_SEC:-3600}"
CARGO_TIMEOUT_SEC="${CARGO_TIMEOUT_SEC:-1800}"
# Building all 8 compose images is the longest step on a cold builder cache; warm, most layers
# are CACHED. A timeout is a failure, not a skip: an unbuilt image is exactly the gap step 18
# exists to close (E0463, 2026-09-13).
IMAGES_TIMEOUT_SEC="${IMAGES_TIMEOUT_SEC:-2400}"

# ── Preflight: required tooling + paths (R-150) ──────────────────────────────
for tool in go mvn java make; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		echo "FATAL: required tool '$tool' not found on PATH" >&2
		exit 1
	fi
done
for dir in "$CODE_DIR" "$BRIDGE_DIR" "$INGESTION_DIR"; do
	if [ ! -d "$dir" ]; then
		echo "FATAL: expected directory missing: $dir" >&2
		exit 1
	fi
done

mkdir -p "$OUT_DIR"
SUMMARY="$OUT_DIR/SUMMARY.txt"

# Verification accounting: a step that never ran must not read as a pass. The
# verdict below prints the count, so "14/14" and "13/14 + 1 skipped" can never be
# confused. Gate attempt 11 skipped step 11 (no FLUSS_BOOTSTRAP in its shell)
# while the verdict still read a bare PASS.
GATE_TOTAL=19   # keep in step with the [N/19] labels (checked against the banners below)
GATE_SKIPS=0
SKIPPED_STEPS=""
note_skip() { GATE_SKIPS=$((GATE_SKIPS + 1)); SKIPPED_STEPS="$SKIPPED_STEPS $1"; }

# --steps parsing: a set of step numbers, empty = every step. Ranges are checked
# against GATE_TOTAL so a typo can not silently run fewer steps than the caller
# believes; the banner count at the end re-verifies the outcome.
STEPS_SET=""
if [ -n "$STEP_SELECTION" ]; then
	for token in ${STEP_SELECTION//,/ }; do
		case "$token" in
			*-*) lo="${token%%-*}"; hi="${token##*-}" ;;
			*) lo="$token"; hi="$token" ;;
		esac
		case "${lo}${hi}" in
			''|*[!0-9]*) echo "FATAL: bad --steps entry '$token' (use N or N-M)" >&2; exit 2 ;;
		esac
		if [ "$lo" -lt 1 ] || [ "$hi" -gt "$GATE_TOTAL" ] || [ "$lo" -gt "$hi" ]; then
			echo "FATAL: --steps '$token' is outside 1-$GATE_TOTAL" >&2; exit 2
		fi
		n="$lo"
		while [ "$n" -le "$hi" ]; do STEPS_SET="$STEPS_SET $n"; n=$((n + 1)); done
	done
	# Ascending and deduplicated: the --sweep resume point is a numeric floor, so
	# an out-of-order or repeated selection would otherwise be skipped in silence.
STEPS_SET="$(printf '%s\n' $STEPS_SET | sort -n -u | tr '\n' ' ' | sed 's/ *$//')"
fi

# step_active <n>: is this step selected for THIS run (--steps, and not before the
# --sweep resume point)? Also names CURRENT_STEP so gate_fail can record which
# step aborted, which is how the sweep driver knows where to resume.
CURRENT_STEP=""
step_active() {
	[ "$1" -ge "${GATE_SWEEP_FROM:-1}" ] || return 1
	if [ -n "$STEPS_SET" ]; then
		case " $STEPS_SET " in *" $1 "*) : ;; *) return 1 ;; esac
	fi
	CURRENT_STEP="$1"
	return 0
}
SELECTED_COUNT=0
for _n in $(seq 1 "$GATE_TOTAL"); do
	step_active "$_n" && SELECTED_COUNT=$((SELECTED_COUNT + 1)) || true
done
CURRENT_STEP=""
# Fixed before any step runs: gate_fail prints this label, so a scoped or swept
# run can never fail under the certificate's name.
if [ "$SWEEP" -eq 1 ]; then
	VERDICT_LABEL="SWEEP RESULT"
	VERDICT_NOTE=" (non-certifying run)"
elif [ -n "$STEPS_SET" ]; then
	VERDICT_LABEL="SUBSET RESULT"
	VERDICT_NOTE=" (subset run — not a certificate; run with no arguments for that)"
else
	VERDICT_LABEL="GATE RESULT"
	VERDICT_NOTE=""
fi


echo "run-monday-gates: output → $OUT_DIR"
echo "run-monday-gates: go timeout=${GO_TIMEOUT_SEC}s, java timeout=${JAVA_TIMEOUT_SEC}s, cargo timeout=${CARGO_TIMEOUT_SEC}s"

gate_fail() {
	GATE_DECIDED=1
	# A sweep child records its step and exits 3 so the driver can resume after it;
	# only the driver prints a verdict for a sweep. Everything else fails here.
	if [ "${SWEEP:-0}" = "1" ] && [ -n "${GATE_SWEEP_CHILD:-}" ]; then
		echo "SWEEP: step ${CURRENT_STEP:-unknown} failed — recorded; the sweep continues" | tee -a "$SUMMARY"
		echo "${CURRENT_STEP:-unknown}" >>"$SWEEP_FAILED_FILE"
		exit 3
	fi
	echo "$VERDICT_LABEL: FAIL" | tee -a "$SUMMARY"
	exit 1
}
# Steps 12/13/14/16 pin test classes or a module, and they pass
# -Dsurefire.failIfNoSpecifiedTests=false, so a renamed or moved class makes
# surefire run nothing while maven still reports BUILD SUCCESS. Assert a
# non-zero test count instead of trusting the build status alone. (The Rust step
# has its own passed>0 check, and the pytest step asserts ^OK.)
#
# P6-189 (revisited, 2026-09-15): `failIfNoSpecifiedTests=true` is NOT usable
# here. `-pl <service> -am` drags `common` into the reactor, the pinned pattern
# matches nothing there, and surefire aborts that module with BUILD FAILURE — so
# a `true` value fails the pin on every run, for the wrong reason (proved with a
# real offline build: `No tests matching pattern … were executed!` in `common`).
# The flag stays `false`; the strictness comes from require_class_ran below,
# which asserts each NAMED class reported its own non-zero `Tests run:` line.
# require_tests_run alone cannot see this: a pattern with one surviving class
# still reports a positive maximum count.
tests_run_summary() { # $1 = maven log -> highest "Tests run: N" in it, or empty
	grep -aoE 'Tests run: [0-9]+' "$1" 2>/dev/null | grep -oE '[0-9]+' | sort -n | tail -1 || true
}
require_tests_run() { # $1 = maven log, $2 = step label
	local count
	count="$(tests_run_summary "$1")"
	if [ -z "$count" ] || [ "$count" -lt 1 ]; then
		echo "FAIL: $2 ran 0 tests — a renamed/moved class is not a pass (surefire.failIfNoSpecifiedTests=false hides it) — see $1" | tee -a "$SUMMARY"
		gate_fail
	fi
}
# Maven colourises its own log even when stdout is a file (TERM=xterm-256color,
# no .mvn/maven.config needed): the class name arrives as "\e[1m<FQCN>\e[m", so
# every escape has to go before a per-class line can be matched. Verified against
# a real run — see tests/test_gate_class_pin.py.
strip_ansi() { # $1 = file -> stdout with SGR colour sequences removed
	sed $'s/\033\\[[0-9;]*m//g' "$1" 2>/dev/null
}
# P6-189: surefire 3.x closes each executed class with
# "Tests run: N, Failures: …, Skipped: … -- in <fully.qualified.Class>". Assert
# that line exists with a non-zero N for a named class: a rename, a move, or a
# pom/surefire change that stops selecting it turns the gate red, which the
# `-Dtest` pattern alone cannot do in a multi-module reactor. grep -c (not -q)
# so sed reads the whole stream and pipefail cannot see a SIGPIPE as a failure.
require_class_ran() { # $1 = maven log, $2 = fully.qualified.Class, $3 = step label
	local hits
	hits="$(strip_ansi "$1" | grep -cE "^\[INFO\] Tests run: [1-9][0-9]*, .* -- in $2$")" || hits=0
	if [ "${hits:-0}" -lt 1 ]; then
		echo "FAIL: $3 — $2 ran no tests (renamed, moved, or dropped by surefire); a BUILD SUCCESS without it is not a pass — see $1" | tee -a "$SUMMARY"
		gate_fail
	fi
}

# Go prints one "ok <pkg>" line per package that actually ran tests and
# "?  <pkg> [no test files]" for packages that have none. A run where every test
# file is excluded — a build tag, a rename, the wrong module directory — still
# exits 0, so the step asserts both directions: packages really ran, and the tree
# still holds tests. (The Rust step has required a non-zero passed count since the
# nautilus suite landed; this is the same guard for Go.)
require_go_suite_evidence() { # $1 = go test log, $2 = module dir, $3 = step label
	local ok_pkgs files
	ok_pkgs="$(grep -cE '^ok[[:space:]]' "$1" 2>/dev/null || true)"
	files="$(grep -rlE '^func Test' --include='*_test.go' "$2" 2>/dev/null | wc -l | tr -d ' ')"
	if [ "${ok_pkgs:-0}" -lt 1 ] || [ "${files:-0}" -lt 1 ]; then
		echo "FAIL: $3 — ${ok_pkgs:-0} packages reported ok against ${files:-0} test files: a suite that ran nothing is not a pass — see $1" | tee -a "$SUMMARY"
		gate_fail
	fi
	echo "PASS: Go suite (-race; $ok_pkgs packages ok, $files test files)" | tee -a "$SUMMARY"
}


# ── 0. Preflight: environment drift must fail in seconds, not at step 9 or 11 ─
# Attempts 22/24/26 (2026-09-13) each spent ~10 minutes of gate before hitting
# drift this detects in ~2s: a wiped Fluss catalog (surfaced at step 9) and a
# compose-form divergence that recreated fluss-coordinator mid-run, underneath
# the step-11 apply. Read-only — it reports drift and prints the remedy
# (`make up`, the canonical compose form). A missing prerequisite is recorded as
# a SKIP, never a pass, so the verdict line stays honest about what was verified.
# The sweep driver runs the preflight once for the whole sweep, so a child skips
# it: a drifting stack must stop the driver, not each step.
if [ -z "${GATE_SWEEP_CHILD:-}" ]; then
if [ "${SWEEP:-0}" = "1" ]; then
	MODE="sweep (non-certifying: continues past failures; SWEEP RESULT is the only verdict)"
elif [ -n "$STEPS_SET" ]; then
	MODE="subset (steps: ${STEPS_SET:-1-19} — non-certifying; run with no arguments for the certificate)"
else
	MODE="gate (certifying: frozen tree, fail-fast, $GATE_TOTAL/$GATE_TOTAL steps)"
fi
echo "MODE=$MODE" >>"$SUMMARY"
# ── Replay memo: a second green on the same (tree, stack) is not new evidence ──
# A certificate is evidence about one frozen commit on one stack. Re-running the
# same gate on the same pair produces a green that looks like new evidence and is
# not. The preflight prints the pair as `fingerprint=<tree>:<stack>`; these two
# functions read and write logs/.gate-memo.jsonl (logs/ is untracked, so
# remembering cannot dirty the tree). A lookup failure is silent — no memo is not
# a reason to refuse; a refusal is only ever --no-replay against a real record.
GATE_MEMO_PY="$SCRIPT_DIR/gate_memo.py"

gate_memo_check() { # $1 = fingerprint
	[ -n "$1" ] || return 0
	local seen
	if seen=$(timeout -k 30 60 python3 "$GATE_MEMO_PY" --root "$PROJECT_ROOT" lookup "$1" 2>/dev/null); then
		if [ "$REPLAY_POLICY" = "refuse" ]; then
			GATE_DECIDED=1
			echo "$seen" >&2
			echo "  --no-replay: this tree on this stack has already been certified, so a second green here would not be new evidence." >&2
			echo "  Nothing was run. Move the tree or the stack, or drop --no-replay to record the replay instead." >&2
			exit 4
		fi
		echo "NOTE: $seen — this is a replay of an earlier certificate, not new evidence; the memo is left unchanged." | tee -a "$SUMMARY"
	fi
	return 0
}

gate_memo_record() { # $1 = fingerprint, $2 = run dir
	[ -n "$1" ] || return 0
	if ! timeout -k 30 60 python3 "$GATE_MEMO_PY" --root "$PROJECT_ROOT" record "$1" "$2" >/dev/null 2>&1; then
		echo "HARNESS: could not record the replay memo — this certificate stands, but an identical re-run would not be flagged as a replay." >&2
	fi
	return 0
}

echo "=== [preflight] environment drift (tree, compose convergence, catalog, stack) ===" | tee -a "$SUMMARY"
PREFLIGHT_RC=0
# --- one gate at a time on this tree -------------------------------------------
# Two gates on one tree fight over the same containers and images, and every
# stack_generation either of them printed would describe a stack the other one was
# mutating. flock releases on process exit, so a killed run leaves no stale lock.
# The sweep driver holds it; its children inherit the right to run.
if [ -z "${GATE_SWEEP_CHILD:-}" ]; then
	if ! command -v flock >/dev/null 2>&1; then
		GATE_DECIDED=1
		echo "HARNESS: the flock(1) utility is missing — refusing to run a gate that cannot lock this tree." >&2
		exit 4
	fi
	GATE_LOCK_FILE="$PROJECT_ROOT/logs/.monday-gates.lock"
	mkdir -p "$(dirname "$GATE_LOCK_FILE")"
	exec 9<>"$GATE_LOCK_FILE"
	if ! flock -n 9; then
		holder=$(cat "$GATE_LOCK_FILE" 2>/dev/null || true)
		echo "GATE BUSY — another gate run holds $GATE_LOCK_FILE${holder:+ (pid $holder)}." >&2
		echo "  It is mutating this tree's stack; wait for it to finish, then re-run. Nothing was touched." >&2
		GATE_DECIDED=1
		exit 4
	fi
	# We hold the lock, so rewriting the pid record by name cannot race.
	printf '%s\n' "$$" >"$GATE_LOCK_FILE"
	# Our children may legitimately mutate the stack (they are inside this locked
	# section); stack-lock.sh passes them through instead of contending with us.
	export STACK_LOCK_HELD=1
fi

# A certifying run needs a frozen commit. --steps/--sweep are repair-loop runs:
# they may run a dirty tree, and the preflight records the working state in the
# fingerprint instead of blocking them.
PREFLIGHT_MODE=()
if [ -n "$STEPS_SET" ] || [ "$SWEEP" = "1" ]; then
	PREFLIGHT_MODE=( --allow-dirty )
fi
timeout -k 60 180 python3 "$SCRIPT_DIR/gate_preflight.py" "${PREFLIGHT_MODE[@]}" >"$PREFLIGHT_LOG" 2>&1 || PREFLIGHT_RC=$?
if [ "$PREFLIGHT_RC" -eq 2 ]; then
	note_skip preflight
	echo "SKIP: preflight — prerequisite missing, so drift was NOT verified — see $PREFLIGHT_LOG" | tee -a "$SUMMARY"
elif [ "$PREFLIGHT_RC" -ne 0 ]; then
	echo "FAIL: preflight — environment drift, this run cannot certify anything — see $PREFLIGHT_LOG" | tee -a "$SUMMARY"
	gate_fail
else
	echo "PASS: preflight ($(grep -o 'stack_generation=[0-9a-f]*' "$PREFLIGHT_LOG" | tail -1 || echo 'stack_generation=n/a'))" | tee -a "$SUMMARY"
	GATE_FINGERPRINT="$(grep -o 'fingerprint=[0-9a-f:]*' "$PREFLIGHT_LOG" | tail -1 || true)"
	GATE_FINGERPRINT="${GATE_FINGERPRINT#fingerprint=}"
	gate_memo_check "$GATE_FINGERPRINT"
fi

# ── 0. Static checks: bash -n + shellcheck on every script (Phase 8 G4) ─────
fi

# ── Sweep driver: --sweep must discover every failing step, not just the first ─
# A failing step calls gate_fail, which ends a run; that is right for the
# certificate and wrong for a repair loop. So in --sweep mode the driver re-execs
# this script once per step (GATE_SWEEP_FROM), sharing OUT_DIR, and each failure is
# recorded in $OUT_DIR/sweep-failed.txt. Steps that already passed are not re-run,
# and the children print no verdict of their own: the SWEEP RESULT below is the
# only one, and it never says GATE RESULT.
if [ "${SWEEP:-0}" = "1" ] && [ -z "${GATE_SWEEP_CHILD:-}" ]; then
	: >"$SWEEP_FAILED_FILE"
	SWEEP_FROM=1
	SWEEP_ITER=0
	SWEEP_ARGS=( --sweep )
	[ -n "$STEP_SELECTION" ] && SWEEP_ARGS+=( --steps "$STEP_SELECTION" )
	while [ "$SWEEP_FROM" -le "$GATE_TOTAL" ]; do
		SWEEP_ITER=$((SWEEP_ITER + 1))
		if [ "$SWEEP_ITER" -gt "$GATE_TOTAL" ]; then
			echo "SWEEP: aborting — more than $GATE_TOTAL resumptions (a step fails without recording itself)" >&2
			exit 1
		fi
		echo "SWEEP: running from step $SWEEP_FROM"
		SWEEP_RC=0
		GATE_SWEEP_CHILD=1 GATE_SWEEP_FROM="$SWEEP_FROM" OUT_DIR="$OUT_DIR" \
			"$0" "${SWEEP_ARGS[@]}" || SWEEP_RC=$?
		if [ "$SWEEP_RC" -eq 0 ]; then
			SWEEP_FROM=$((GATE_TOTAL + 1))
		elif [ "$SWEEP_RC" -eq 3 ]; then
			SWEEP_LAST="$(tail -1 "$SWEEP_FAILED_FILE" 2>/dev/null || true)"
			case "$SWEEP_LAST" in
				''|*[!0-9]*) echo "SWEEP: aborting — a step failed without recording its number" >&2; exit 1 ;;
			esac
			if [ "$SWEEP_LAST" -lt "$SWEEP_FROM" ]; then
				echo "SWEEP: aborting — recorded step $SWEEP_LAST is before the resume point $SWEEP_FROM" >&2
				exit 1
			fi
			SWEEP_FROM=$((SWEEP_LAST + 1))
		else
			echo "SWEEP: aborting — a failure outside the step blocks (exit $SWEEP_RC)" >&2
			exit "$SWEEP_RC"
		fi
	done
	SWEEP_FAILED="$(sort -n -u "$SWEEP_FAILED_FILE" | tr '\n' ' ' | sed 's/ *$//')"
	# Only the driver sees the whole run, so it checks that every selected step
	# produced a banner: a missing one was skipped, not passed.
	SWEEP_BANNERS="$(grep -cE "^=== \[[0-9]+/$GATE_TOTAL\] " "$SUMMARY" || true)"
	if [ "$SWEEP_BANNERS" -ne "$SELECTED_COUNT" ]; then
		echo "SWEEP: aborting — SUMMARY.txt holds $SWEEP_BANNERS step banner(s) but $SELECTED_COUNT step(s) were selected: the sweep did not attempt every selected step" | tee -a "$SUMMARY"
		exit 1
	fi
	echo "=== SWEEP COMPLETE ===" | tee -a "$SUMMARY"
	if [ -n "$SWEEP_FAILED" ]; then
		GATE_DECIDED=1
		echo "SWEEP RESULT: FAIL (non-certifying) — $SELECTED_COUNT selected step(s), failed: ${SWEEP_FAILED% }" | tee -a "$SUMMARY"
		echo "Fix the failing step(s) and re-run with --steps <numbers> before the certificate." | tee -a "$SUMMARY"
		exit 1
	fi
	GATE_DECIDED=1
	echo "SWEEP RESULT: PASS (non-certifying) — $SELECTED_COUNT selected step(s) passed: ${STEPS_SET:-1-19}" | tee -a "$SUMMARY"
	echo "A sweep green is not a certificate: run this script with no arguments for that." | tee -a "$SUMMARY"
	exit 0
fi


GATE_RUN_LIVE=1
if step_active 1; then
echo "=== [1/19] Static checks (bash -n, shellcheck, harness guards) ===" | tee -a "$SUMMARY"
: >"$STATIC_LOG"
STATIC_FAIL=0
# Every repo shell script (excludes third_party vendored sources).
# Enumerated via a temp file (process substitution < <() needs /dev/fd which
# some sandboxes/CI chroots do not provide), with a `git ls-files` fallback
# for environments where find/sort cannot load their shared libraries.
_script_list=$(mktemp)
SCRIPTS=()
BASH_MAJOR="${BASH_VERSINFO[0]:-0}"
if ! (cd "$CODE_DIR" && find . -name '*.sh' -not -path '*/target/*' -not -path '*/third_party/*' 2>/dev/null | sort 2>/dev/null) >"$_script_list"; then
	: >"$_script_list"
fi
if [ "$BASH_MAJOR" -ge 4 ]; then mapfile -t SCRIPTS <"$_script_list"; else while IFS= read -r _l; do SCRIPTS+=("$_l"); done <"$_script_list"; fi
if [ "${#SCRIPTS[@]}" -eq 0 ]; then
	(cd "$CODE_DIR" && git ls-files '*.sh' 2>/dev/null | grep -v -E '(^|/)(target|third_party)/') >"$_script_list" || true
	if [ "$BASH_MAJOR" -ge 4 ]; then mapfile -t SCRIPTS <"$_script_list"; else while IFS= read -r _l; do SCRIPTS+=("$_l"); done <"$_script_list"; fi
fi
rm -f "$_script_list"
SHELLCHECK_OK=0
if command -v shellcheck >/dev/null 2>&1; then
	SHELLCHECK_OK=1
else
	echo "WARN: shellcheck not installed — skipping (bash -n still enforced)" | tee -a "$SUMMARY"
fi
if [ "${#SCRIPTS[@]}" -eq 0 ]; then
	echo "FAIL: no shell scripts found to check" | tee -a "$SUMMARY"
	gate_fail
fi
for s in "${SCRIPTS[@]}"; do
	if ! bash -n "$CODE_DIR/$s" >>"$STATIC_LOG" 2>&1; then
		echo "FAIL: bash -n $s" | tee -a "$STATIC_LOG"
		STATIC_FAIL=1
	fi
	if [ "$SHELLCHECK_OK" = "1" ]; then
		if ! shellcheck -S warning "$CODE_DIR/$s" >>"$STATIC_LOG" 2>&1; then
			echo "FAIL: shellcheck $s" | tee -a "$STATIC_LOG"
			STATIC_FAIL=1
		fi
	fi
done
if [ "$STATIC_FAIL" -ne 0 ]; then
	echo "FAIL: static checks — see $STATIC_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: static checks (${#SCRIPTS[@]} scripts bash -n + shellcheck -S warning clean)" | tee -a "$SUMMARY"

# ── 0a. Harness guards (test-pipeline-lib.sh) ─────────────────────────────────
# test-pipeline-lib.sh pins pipeline-lib.sh's contract, but its only caller was
# holistic-measure's own preflight (pipeline-lib.sh G27c) — so a change made by
# any OTHER caller of the library (tm-kill-full-load.sh, stage-soak-e2e.sh, the
# soak scripts) could contradict a guard and nothing would notice until someone
# happened to run the measurement. Static and fast (no cluster), so it runs here
# beside bash -n/shellcheck.
if ! bash "$SCRIPT_DIR/test-pipeline-lib.sh" >"$GUARD_LOG" 2>&1; then
	echo "FAIL: harness guards (test-pipeline-lib.sh) — see $GUARD_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
# Fail closed on a suite that exits 0 without reporting a full pass — the same
# shape as the Python step's "Ran 0 tests" refusal below.
GUARD_LINE="$(grep -oE 'guards: [0-9]+ passed, [0-9]+ failed' "$GUARD_LOG" | tail -1 || true)"
case "$GUARD_LINE" in
	*", 0 failed") ;;
	*)
		echo "FAIL: harness guards did not report 'guards: N passed, 0 failed' (got '${GUARD_LINE:-nothing}') — see $GUARD_LOG" | tee -a "$SUMMARY"
		gate_fail
		;;
esac
echo "PASS: harness guards ($GUARD_LINE — static, pinning pipeline-lib.sh)" | tee -a "$SUMMARY"

# ── 0b. Compose config validation (G4) ────────────────────────────────────────
fi
if step_active 2; then
echo "=== [2/19] docker compose config ===" | tee -a "$SUMMARY"
# COMPOSE_FILE / COMPOSE_ENV_DIR come from the shared region above: this step
# can be skipped by --steps while step 8 still needs them.
# Same form as `make up` (see the Makefile COMPOSE variable): a bare `-f`
# resolves a different config, so this step would validate the wrong stack.
if command -v docker >/dev/null 2>&1 && [ -f "$COMPOSE_FILE" ] && [ -f "$COMPOSE_ENV_DIR/.env" ] && [ -f "$COMPOSE_ENV_DIR/secrets.env" ]; then
	if ! docker compose --env-file "$COMPOSE_ENV_DIR/.env" --env-file "$COMPOSE_ENV_DIR/secrets.env" -f "$COMPOSE_FILE" config >"$COMPOSE_CONFIG_LOG" 2>&1; then
		echo "FAIL: docker compose config invalid — see $COMPOSE_CONFIG_LOG" | tee -a "$SUMMARY"
		gate_fail
	fi
	echo "PASS: docker compose config" | tee -a "$SUMMARY"
else
	echo "WARN: compose file or env files missing ($COMPOSE_ENV_DIR) — skipping compose config" | tee -a "$SUMMARY"
fi

# ── 0c. Python unit suites (tests/ — incl. ING-TCP-002 reconcile-compare) ────
# The comparator underpins the count-based losslessness proof; a regression
# could silently pass a reconcile. No cluster needed — synthetic fixtures only.
# docs-audit C16 (env-key drift) runs inside the full doc audit step after the
# Java gate.
fi
if step_active 3; then
echo "=== [3/19] Python unit suites (reconcile-compare ING-TCP-002 + gate helpers) ===" | tee -a "$SUMMARY"
PY_TIMEOUT_SEC="${PY_TIMEOUT_SEC:-300}"
if ! timeout -k 60 "$PY_TIMEOUT_SEC" python3 -m unittest discover -s "$SCRIPT_DIR/tests" -p "test_*.py" \
	>"$PY_LOG" 2>&1; then
	echo "FAIL: python unit suites — see $PY_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
if ! grep -q "^OK" "$PY_LOG" || grep -qE 'Ran 0 tests|FAILED \(' "$PY_LOG"; then
	echo "FAIL: python unit suites did not report OK with >=1 test — see $PY_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: python unit suites ($(grep -oE 'Ran [0-9]+ tests' "$PY_LOG" | head -1 || echo 'all tests'))" | tee -a "$SUMMARY"

# ── 0d. Entrypoint harness (ING-INT-006) ───────────────────────────────────
# docker-entrypoint.sh FATAL paths: missing FLUSS_BOOTSTRAP (2), missing
# manifest (2), missing bridge binary (1) — exit codes AND messages must
# match the documented contract. Runs the entrypoint under env -i so a
# polluted gate environment cannot mask a FATAL; bash -n + shellcheck on
# this file run in the static stage above.
fi
if step_active 4; then
echo "=== [4/19] Entrypoint harness (ING-INT-006) ===" | tee -a "$SUMMARY"
ENTRYPOINT_TIMEOUT_SEC="${ENTRYPOINT_TIMEOUT_SEC:-300}"
if ! timeout -k 60 "$ENTRYPOINT_TIMEOUT_SEC" bash "$SCRIPT_DIR/tests/test_docker_entrypoint.sh" >"$ENTRYPOINT_LOG" 2>&1; then
	echo "FAIL: entrypoint harness — see $ENTRYPOINT_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: entrypoint harness (exit codes + messages)" | tee -a "$SUMMARY"

# ── 1. Go suite with race detector (Phase 8: go test -race) ──────────────────
fi
if step_active 5; then
echo "=== [5/19] Go bridge suite (-race) ===" | tee -a "$SUMMARY"
# The output goes to $GO_LOG: the FAIL message below points there (and the
# final evidence list advertises it), plus the race reports are large.
if ! (cd "$BRIDGE_DIR" && timeout -k 60 "$GO_TIMEOUT_SEC" go test -race -count=1 ./...) >"$GO_LOG" 2>&1; then
	echo "FAIL: Go suite failed or timed out — see $GO_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
	require_go_suite_evidence "$GO_LOG" "$BRIDGE_DIR" "Go suite"

# ── 2. Build E2E test binaries (R-016) + docker build smoke ───────────────
fi
if step_active 6; then
echo "=== [6/19] Building E2E test binaries (faketool + arrow-bridge) ===" | tee -a "$SUMMARY"
# Own log (P6-530): sharing $GO_LOG discarded the compiler output on failure
# and pointed at the wrong record; gate_fail exits, so step 5's log is complete
# before this step ever runs.
E2E_BUILD_TIMEOUT_SEC="${E2E_BUILD_TIMEOUT_SEC:-600}"
if ! (cd "$BRIDGE_DIR" && timeout -k 60 "$E2E_BUILD_TIMEOUT_SEC" go build -tags faketool -o faketool/faketool ./faketool && timeout -k 60 "$E2E_BUILD_TIMEOUT_SEC" go build -o arrow-bridge .) >"$E2E_BUILD_LOG" 2>&1; then
	echo "FAIL: could not build E2E test binaries — see $E2E_BUILD_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: E2E binaries built (faketool/faketool, arrow-bridge)" | tee -a "$SUMMARY"

# ── 5. Docker build smoke (G4): ingestion image must build from the reactor root ──
fi
if step_active 7; then
echo "=== [7/19] docker build smoke (ingestion image) ===" | tee -a "$SUMMARY"
if command -v docker >/dev/null 2>&1 && [ -f "$CODE_DIR/02_services/01_ingestion/Dockerfile" ]; then
	# The build needs network (base images + go/maven deps). Offline runs must
	# not fail the gate on the network — but WITH images present, a build
	# failure is the R-002 build-context defect and MUST fail.
	if docker image inspect golang:1.24-alpine maven:3.9-eclipse-temurin-17 >/dev/null 2>&1; then
		# R-002: the ingestion Dockerfile requires the Maven reactor root as
		# build context (parent POM + common module). Tag locally; never push.
		if ! (cd "$CODE_DIR" && docker build -q \
			-f 02_services/01_ingestion/Dockerfile -t ingestion-gate-smoke:local \
			. >"$DOCKER_BUILD_LOG" 2>&1); then
			echo "FAIL: docker build smoke failed (images present) — see $DOCKER_BUILD_LOG" | tee -a "$SUMMARY"
			gate_fail
		fi
		docker rmi ingestion-gate-smoke:local >/dev/null 2>&1 || true
		echo "PASS: docker build smoke (ingestion image from reactor root)" | tee -a "$SUMMARY"
	else
		echo "WARN: base images not cached and network likely unavailable — " \
			"skipping build smoke (CI with network runs it)" | tee -a "$SUMMARY"
	fi
else
	echo "WARN: docker unavailable or Dockerfile missing — skipping build smoke" | tee -a "$SUMMARY"
fi

# ── 5b. CHG-101: stale-image guard — no compose build: image may be older
# than the last change to the source it packages (2026-08-24 gateway/bridge
# incident: 08-20 images vs 08-24 source went unnoticed until a readyz probe).
fi
if step_active 8; then
echo "=== [8/19] image staleness (the service image this gate runs) ===" | tee -a "$SUMMARY"
if command -v docker >/dev/null 2>&1 && [ -f "$COMPOSE_FILE" ]; then
	# Scope (2026-09-12): ddl-apply is the only *service* image the gate starts
	# (step 11). Step 7 builds ingestion under the throwaway
	# ingestion-gate-smoke:local tag, so it never touches a compose image.
	# Demanding all 8 current meant a full rebuild before every gate for images
	# this run never starts. The other 7 are not left unguarded: `make images`
	# ends with the full --require-stamps check, and the runbook requires
	# `make check-image-stale` before any enable.
	# --require-stamps (2026-09-13): without it a stamp-less image passes as
	# "FRESH (timestamp proxy)" — the clock is not evidence that the image
	# contains the sources, which is the whole point of CHG-101.
	if ! timeout -k 60 120 python3 "$SCRIPT_DIR/image_staleness_check.py" \
		--git-root "$PROJECT_ROOT" --compose "$COMPOSE_FILE" --service ddl-apply \
		--require-stamps >"$IMAGE_LOG" 2>&1; then
		echo "FAIL: stale, unstamped or missing ddl-apply image (CHG-101) — see $IMAGE_LOG" | tee -a "$SUMMARY"
		gate_fail
	fi
	echo "PASS: image staleness (ddl-apply — the service image this gate runs)" | tee -a "$SUMMARY"
	echo "NOTE: this check (and 'make images') demands a real build stamp — an image whose only evidence is its build clock is refused. Step 18 of this same run builds all 8 images and stamp-checks them (until 2026-09-14 that happened only at release), so an unbuildable or unstamped image now fails the certificate instead of surviving to release." | tee -a "$SUMMARY"
else
	note_skip 8
	echo "SKIP: image staleness (no docker/compose) — unverified, not green" | tee -a "$SUMMARY"
fi

# ── 3. Full Java gate with ALL integration flags + the LIVE Fluss drills ──────
# The module scope below is ingestion (common rides along as a dependency), so the
# gateway's drills were outside it, and neither the flags nor the surrounding env
# export FLUSS_BOOTSTRAP — every class gated on it records 0 tests here. A green
# step 9 therefore proved nothing about the live store paths; the drill block at
# the end of this step runs them.
fi
if step_active 9; then
echo "=== [9/19] Java full gate (FLUSS+MANIFEST+PERF+E2E) + live Fluss drills ===" | tee -a "$SUMMARY"
	# FLUSS_BOOTSTRAP is deliberately unset for the plain Java run: common's ten
	# FLUSS_BOOTSTRAP-gated live classes would otherwise execute into the plain
	# target/surefire-reports and rewrite their XMLs from 0 to real counts
	# (627 -> 649, measured 2026-09-12), so the doc audit two steps later would
	# disagree with the documented triple. Those classes belong to the drill step
	# below, which supplies the bootstrap itself and writes to the isolated drills
	# dir. Ingestion's live classes are unaffected: they gate on the
	# INGESTION_INT_TEST_* flags and resolve FLUSS_BOOTSTRAP_SERVERS (default
	# localhost:9123).
	if ! (cd "$CODE_DIR" && timeout -k 60 "$JAVA_TIMEOUT_SEC" env -u FLUSS_BOOTSTRAP INGESTION_INT_TEST_E2E=true INGESTION_INT_TEST_FLUSS=true INGESTION_INT_TEST_MANIFEST=true INGESTION_INT_TEST_PERF=true mvn -o test -pl 02_services/01_ingestion -am) >"$JAVA_LOG" 2>&1; then
	echo "FAIL: Java suite failed or timed out — see $JAVA_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: Java suite" | tee -a "$SUMMARY"

# Live Fluss drills (make drill-live: common + gateway classes gated on
# FLUSS_BOOTSTRAP). Reports go to target/surefire-reports-drills, so the C6 test
# counts in the next step still see the plain suite: a live run rewrites the same
# class XMLs with real (non-zero) test counts and would otherwise make C6 disagree
# with the documented triple. Bootstrap defaults to the local stack — the Java step
# above already requires it up (INGESTION_INT_TEST_FLUSS=true).
DRILL_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
if ! (cd "$PROJECT_ROOT" && timeout -k 60 "$JAVA_TIMEOUT_SEC" env FLUSS_BOOTSTRAP="$DRILL_BOOTSTRAP" MVN_FLAGS=-o make drill-live) >"$DRILL_LOG" 2>&1; then
	echo "FAIL: live Fluss drills (bootstrap $DRILL_BOOTSTRAP — is the stack up?) — see $DRILL_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: live Fluss drills (common + gateway, bootstrap $DRILL_BOOTSTRAP)" | tee -a "$SUMMARY"

# ── 3b. Full doc audit (make full-audit: scanners + sweeps + trio) ──────────
# Runs AFTER the Java gate so Layer 1b's docs-audit C6 (test counts vs surefire
# reports) sees fresh results. full_audit.sh is the whole doc-truth command:
# the three machine gates (stale-claim scanner --upstream — table kinds, phase
# status, numeric drift, test counts, C6 triples; docs-audit incl. C16 env-key
# drift + C14 change records; DDL/manifest parity) + the beyond-scanner sweeps
# (live ranking/reservation claims, stale 'pending implementation' prose) + the
# master-dossier trio coherence. Wired here so the beyond-scanner sweeps can't
# rot undetected — they silently drifted at HEAD once (CHG-026/027 era) because
# only the machine gates were ever run in CI.
fi
if step_active 10; then
echo "=== [10/19] full doc audit (make full-audit: scanners + sweeps + trio coherence) ===" | tee -a "$SUMMARY"
if ! timeout -k 60 300 bash "$SCRIPT_DIR/full_audit.sh" >"$AUDIT_LOG" 2>&1; then
	echo "FAIL: full doc audit — see $AUDIT_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
if ! grep -q "FULL-AUDIT: all layers green" "$AUDIT_LOG"; then
	echo "FAIL: full doc audit did not report all-layers-green — see $AUDIT_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: full doc audit (stale claims + doc↔code truth + DDL parity + sweeps + trio, incl. C16 env-key drift)" | tee -a "$SUMMARY"

# ── 3c. DDL apply exit-code contract smoke (scratch catalogs) ────────────────
fi
if step_active 11; then
echo "=== [11/19] DDL apply exit-code smoke ===" | tee -a "$SUMMARY"
DDL_SMOKE_TIMEOUT_SEC="${DDL_SMOKE_TIMEOUT_SEC:-1800}"
# Env-gated: the smoke reports itself SKIPPED when it gets no bootstrap; any
# deviation from the 0/6/1 contract, the sentinels, or the evidence record FAILS
# the gate. The bootstrap defaults to the same local stack the drills use, so a
# missing export can no longer turn this step into an invisible skip. A skip now
# also needs the explicit GATE_ALLOW_NO_FLUSS=1 opt-out for a host without a
# cluster, and it is counted in the verdict either way.
if [ "${GATE_ALLOW_NO_FLUSS:-0}" = "1" ]; then
	echo "note: GATE_ALLOW_NO_FLUSS=1 — the DDL smoke may report itself skipped" | tee -a "$SUMMARY"
else
	: "${FLUSS_BOOTSTRAP:=localhost:9123}"
	export FLUSS_BOOTSTRAP
fi
if ! timeout -k 60 "$DDL_SMOKE_TIMEOUT_SEC" python3 \
	"$SCRIPT_DIR/ddl_apply_smoke.py" >"$DDL_SMOKE_LOG" 2>&1; then
	echo "FAIL: DDL apply exit-code smoke — see $DDL_SMOKE_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
if grep -q "ddl-apply-smoke: SKIPPED" "$DDL_SMOKE_LOG"; then
	if [ "${GATE_ALLOW_NO_FLUSS:-0}" = "1" ]; then
		note_skip 11
		echo "SKIP: DDL apply smoke (no Fluss; GATE_ALLOW_NO_FLUSS=1) — see $DDL_SMOKE_LOG" | tee -a "$SUMMARY"
	else
		echo "FAIL: DDL apply smoke skipped with no opt-out — set GATE_ALLOW_NO_FLUSS=1 to allow that" | tee -a "$SUMMARY"
		gate_fail
	fi
else
	echo "PASS: DDL apply exit-code smoke (0/6/1 + sentinels)" | tee -a "$SUMMARY"
fi
# Non-root ownership contract: every container-written evidence record must be
# group-writable and none root-owned (evidence_ownership_check.py). No cluster
# needed; vacuous when no container-written records exist (host-side records
# are out of scope). Also wired into docs-audit C15 + make evidence-ownership-check.
# Its own log (P6-776): sharing $DDL_SMOKE_LOG made a DDL SKIP + ownership FAIL
# (or vice versa) unattributable, and the manifest listed only one path.
if ! timeout -k 30 60 python3 "$SCRIPT_DIR/evidence_ownership_check.py" \
	>"$OWNERSHIP_LOG" 2>&1; then
	echo "FAIL: evidence ownership check — see $OWNERSHIP_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: evidence ownership check (container-written records group-writable)" | tee -a "$SUMMARY"

# ── 4. Schema agreement + perf certification explicit gates (G5) ─────────────
fi
if step_active 12; then
echo "=== [12/19] SchemaAgreementTest + PerfBaselineTest explicit ===" | tee -a "$SUMMARY"
SCHEMA_PERF_TIMEOUT_SEC="${SCHEMA_PERF_TIMEOUT_SEC:-1200}"
if ! (cd "$CODE_DIR" && timeout -k 60 "$SCHEMA_PERF_TIMEOUT_SEC" env INGESTION_INT_TEST_PERF=true mvn -o test -pl 02_services/01_ingestion -am -Dtest='SchemaAgreementTest,DdlBootstrapSchemaAgreementTest,PerfBaselineTest' -Dsurefire.failIfNoSpecifiedTests=false) >"$SCHEMA_PERF_LOG" 2>&1; then
	echo "FAIL: schema agreement / perf certification — see $SCHEMA_PERF_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
if ! grep -q "BUILD SUCCESS" "$SCHEMA_PERF_LOG"; then
	echo "FAIL: schema/perf gate did not report BUILD SUCCESS — see $SCHEMA_PERF_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
require_tests_run "$SCHEMA_PERF_LOG" "step 12 (schema agreement + perf baseline pins)"
require_class_ran "$SCHEMA_PERF_LOG" com.trading.ingestion.SchemaAgreementTest "step 12"
require_class_ran "$SCHEMA_PERF_LOG" com.trading.ingestion.DdlBootstrapSchemaAgreementTest "step 12"
require_class_ran "$SCHEMA_PERF_LOG" com.trading.ingestion.PerfBaselineTest "step 12"
echo "PASS: SchemaAgreementTest + PerfBaselineTest (certification gates)" | tee -a "$SUMMARY"

# ── 3d. CHG-015 SIGTERM-drain regression explicit (ING-UNIT-023/024) ────────
# The bridge's final arrow-tick-counts report must be drained from stderr after
# the graceful SIGTERM path — both the in-process shutdown() path (ING-UNIT-023
# BridgeShutdownRegressionTest) and the REAL JVM shutdown-hook path with a
# spawned driver + real SIGTERM + exit 143 (ING-UNIT-024 BridgeShutdownHookTest).
# Both are default-run (already covered by step 8's full-module gate); this
# explicit pin makes the CHG-015 regression a NAMED gate — a future pom/surefire
# change that silently drops or env-gates them now fails CI instead of quietly
# shrinking the plain suite. Cluster-free: scripted fake bridge, no Fluss, no
# Go binaries (runs on a bare checkout). POSIX-only (SIGTERM semantics).
fi
if step_active 13; then
echo "=== [13/19] SIGTERM-drain regression explicit (ING-UNIT-023/024, CHG-015) ===" | tee -a "$SUMMARY"
SHUTDOWN_TIMEOUT_SEC="${SHUTDOWN_TIMEOUT_SEC:-1200}"
if ! (cd "$CODE_DIR" && timeout -k 60 "$SHUTDOWN_TIMEOUT_SEC" mvn -o test -pl 02_services/01_ingestion -am -Dtest='BridgeShutdownRegressionTest,BridgeShutdownHookTest' -Dsurefire.failIfNoSpecifiedTests=false) >"$SHUTDOWN_LOG" 2>&1; then
	echo "FAIL: SIGTERM-drain regression (ING-UNIT-023/024) — see $SHUTDOWN_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
if ! grep -q "BUILD SUCCESS" "$SHUTDOWN_LOG"; then
	echo "FAIL: SIGTERM-drain regression did not report BUILD SUCCESS — see $SHUTDOWN_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
require_tests_run "$SHUTDOWN_LOG" "step 13 (SIGTERM-drain pins)"
require_class_ran "$SHUTDOWN_LOG" com.trading.ingestion.BridgeShutdownRegressionTest "step 13"
require_class_ran "$SHUTDOWN_LOG" com.trading.ingestion.BridgeShutdownHookTest "step 13"
echo "PASS: SIGTERM-drain regression (ING-UNIT-023 in-process + ING-UNIT-024 real hook)" | tee -a "$SUMMARY"

# ── 4b. Execution gateway module suite (unit + regression) ─────────────────
# Step 9's module scope is ingestion (common rides along as a dependency) and the
# drill block names 14 classes, so this module's other tests — readiness, HTTP
# approval authority, halt tails and the reader-death pin (P3-064) — ran in no
# gate step at all. ~40 s offline; it needs no cluster.
fi
if step_active 14; then
echo "=== [14/19] Execution gateway module suite (unit + regression) ===" | tee -a "$SUMMARY"
if ! (cd "$CODE_DIR" && timeout -k 60 "$JAVA_TIMEOUT_SEC" mvn -o test -pl 02_services/06_execution_gateway) >"$GATEWAY_LOG" 2>&1; then
	echo "FAIL: execution gateway suite — see $GATEWAY_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
if ! grep -q "BUILD SUCCESS" "$GATEWAY_LOG"; then
	echo "FAIL: execution gateway suite did not report BUILD SUCCESS — see $GATEWAY_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
require_tests_run "$GATEWAY_LOG" "step 14 (execution gateway suite)"
echo "PASS: execution gateway suite ($(grep -aoE 'Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+' "$GATEWAY_LOG" | tail -1))" | tee -a "$SUMMARY"

# Cheap suite first: nautilus is ~51 s, compute ~3 min, so a red compute no longer
# hides the Rust verdict from this run (both still gate the verdict).
fi
if step_active 15; then
echo "=== [15/19] Nautilus (Rust executor) suite — offline against the pinned lockfile ===" | tee -a "$SUMMARY"
# --offline is this repo's documented form (docs/plans/2026-08-25-live-readiness-
# unified-plan.md): it proves Cargo.lock resolves from the cached registry. On a
# cold ~/.cargo this FAILS loudly on purpose — run `cargo fetch` once, do not
# turn it into a skip.
# --features paper: the t9-paper bins are `required-features = ["paper"]` (P3-173), so the
# default build skips them; passing the feature keeps their compile + unit coverage in the gate.
if ! (cd "$EXECUTOR_DIR" && timeout -k 60 "$CARGO_TIMEOUT_SEC" cargo test --offline --features paper) >"$NAUTILUS_LOG" 2>&1; then
	echo "FAIL: nautilus Rust suite — see $NAUTILUS_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
NAUTILUS_PASSED="$(grep -E '^test result:' "$NAUTILUS_LOG" | grep -oE '[0-9]+ passed' | awk '{s+=$1} END {print s+0}')"
NAUTILUS_FAILED="$(grep -E '^test result:' "$NAUTILUS_LOG" | grep -oE '[0-9]+ failed' | awk '{s+=$1} END {print s+0}')"
if [ "$NAUTILUS_FAILED" -ne 0 ] || [ "$NAUTILUS_PASSED" -eq 0 ]; then
	echo "FAIL: nautilus Rust suite — ${NAUTILUS_PASSED:-0} passed, ${NAUTILUS_FAILED:-0} failed (a suite that ran nothing is not a pass) — see $NAUTILUS_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: nautilus Rust suite ($NAUTILUS_PASSED passed, 0 failed)" | tee -a "$SUMMARY"
fi

if step_active 16; then
echo "=== [16/19] Compute module suite (Fluss/fingerprint/candle unit + integration) ===" | tee -a "$SUMMARY"
# 02_services/02_compute is deliberately NOT in the code/pom.xml reactor (R-272),
# so it is tested from its own pom; common/ingestion resolve from ~/.m2 like the
# gateway suite does.
if ! (cd "$COMPUTE_DIR" && timeout -k 60 "$JAVA_TIMEOUT_SEC" mvn -o test) >"$COMPUTE_LOG" 2>&1; then
	echo "FAIL: compute suite — see $COMPUTE_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
if ! grep -q "BUILD SUCCESS" "$COMPUTE_LOG"; then
	echo "FAIL: compute suite did not report BUILD SUCCESS — see $COMPUTE_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
require_tests_run "$COMPUTE_LOG" "step 16 (compute suite)"
echo "PASS: compute suite ($(grep -aoE 'Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+' "$COMPUTE_LOG" | tail -1))" | tee -a "$SUMMARY"
fi

# Step 17 (2026-09-14): the pin discipline the rest of the certificate assumes. `gate-fast` has
# run it since P3-418, but a release certificate could still be minted over floating tags — a
# toolchain, corpus revision, base image or action tag that moved changes what "green" means
# without changing a line in the tree.
if step_active 17; then
echo "=== [17/19] pin discipline (exact versions and digests across the tree) ===" | tee -a "$SUMMARY"
if ! timeout -k 60 300 make -C "$PROJECT_ROOT" pin-check >"$PIN_LOG" 2>&1; then
	echo "FAIL: pin discipline — see $PIN_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
# A run that printed nothing is not a pass either: `make` can exit 0 with the checker skipped
# (missing tool, empty selection), and the pins are the assumption under every other step.
if ! grep -aq "pin-check: PASS" "$PIN_LOG"; then
	echo "FAIL: pin-check did not report PASS — see $PIN_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
echo "PASS: pin discipline (pin-check: PASS)" | tee -a "$SUMMARY"
fi

# Step 18 (2026-09-14): build every compose image and demand a content stamp. Until this step
# existed, the certificate only ever checked the one service image the gate starts (step 8), so a
# broken *image* — the executor's missing rstest dependency (E0463) — could not fail a gate: the
# break only surfaced when someone built that image by hand. Building here also refreshes the
# content stamps `make check-image-stale` requires at release, so the gate stops manufacturing the
# very "NO-STAMP" condition the release check refuses. Runs LAST: nothing above may depend on a
# freshly built image, and a build failure must not be able to mask a suite failure.
if step_active 18; then
echo "=== [18/19] compose images: build all 8 + content stamps + staleness ===" | tee -a "$SUMMARY"
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
	if ! timeout -k 60 "$IMAGES_TIMEOUT_SEC" make -C "$PROJECT_ROOT" images >"$IMAGES_LOG" 2>&1; then
		echo "FAIL: compose image build (a broken image cannot fail the certificate) — see $IMAGES_LOG" | tee -a "$SUMMARY"
		gate_fail
	fi
	if ! timeout -k 60 300 make -C "$PROJECT_ROOT" check-image-stale >"$STALE_ALL_LOG" 2>&1; then
		echo "FAIL: an image is stale or carries no content stamp after the build — see $STALE_ALL_LOG" | tee -a "$SUMMARY"
		gate_fail
	fi
	echo "PASS: compose images built ($(grep -aoE 'PASS — [0-9]+ image\(s\) current' "$STALE_ALL_LOG" | tail -1))" | tee -a "$SUMMARY"
else
	# Unverified, not green: the images a release would deploy are exactly what was not proven.
	note_skip 18
	echo "SKIP: compose image build + staleness (no docker/compose) — unverified, not green" | tee -a "$SUMMARY"
fi
fi

# Step 19 (2026-09-14): the mock-arrow broker suite. 02_services/05_mock_arrow is not a module of
# the code/pom.xml reactor, so nothing in the gate ever ran its tests — they passed once, by hand,
# during wave-4 batch B3 and were green only because someone remembered to run them. The mock is
# the offline counterpart other suites reach for, so a broken mock belongs in the certificate with
# the suites that depend on it.
if step_active 19; then
echo "=== [19/19] mock-arrow broker suite (offline mock used by the gateway/bridge slices) ===" | tee -a "$SUMMARY"
if ! (cd "$MOCK_ARROW_DIR" && timeout -k 60 "$JAVA_TIMEOUT_SEC" mvn -o test) >"$MOCK_ARROW_LOG" 2>&1; then
	echo "FAIL: mock-arrow suite — see $MOCK_ARROW_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
if ! grep -q "BUILD SUCCESS" "$MOCK_ARROW_LOG"; then
	echo "FAIL: mock-arrow suite did not report BUILD SUCCESS — see $MOCK_ARROW_LOG" | tee -a "$SUMMARY"
	gate_fail
fi
require_tests_run "$MOCK_ARROW_LOG" "step 19 (mock-arrow suite)"
echo "PASS: mock-arrow suite ($(grep -aoE 'Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+' "$MOCK_ARROW_LOG" | tail -1))" | tee -a "$SUMMARY"
fi

# A --sweep child stops here: a verdict from a partial run must never be quoted.
if [ -n "${GATE_SWEEP_CHILD:-}" ]; then
	exit 0
fi

STEPS_RUN="$(grep -cE "^=== \[[0-9]+/$GATE_TOTAL\] " "$SUMMARY" || true)"
if [ "$STEPS_RUN" -ne "$SELECTED_COUNT" ]; then
	echo "FAIL: SUMMARY.txt holds $STEPS_RUN step banners but $SELECTED_COUNT step(s) were selected (--steps '${STEP_SELECTION:-all}') — the labels and the selection have drifted apart" | tee -a "$SUMMARY"
	gate_fail
fi
echo "=== $([ -n "$STEPS_SET" ] && echo 'ALL SELECTED STEPS' || echo 'ALL GATES') PASSED ===" | tee -a "$SUMMARY"

if [ "$GATE_SKIPS" -gt 0 ]; then
	GATE_DECIDED=1
	echo "$VERDICT_LABEL: PASS — $((SELECTED_COUNT - GATE_SKIPS))/$SELECTED_COUNT verified, $GATE_SKIPS skipped (steps:$SKIPPED_STEPS)$VERDICT_NOTE" | tee -a "$SUMMARY"
	echo "Not a clean pass: a skipped step is unverified, not green." | tee -a "$SUMMARY"
else
	GATE_DECIDED=1
	echo "$VERDICT_LABEL: PASS — $SELECTED_COUNT/$SELECTED_COUNT verified, 0 skipped$VERDICT_NOTE" | tee -a "$SUMMARY"
	# Only a clean certificate is remembered: a scoped/swept run is never a
	# certificate, and a pass with skips is not a clean one.
	if [ "$VERDICT_LABEL" = "GATE RESULT" ]; then
		gate_memo_record "$GATE_FINGERPRINT" "$OUT_DIR"
	fi
fi
echo "Evidence:" | tee -a "$SUMMARY"
echo "  Static: ${STATIC_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Compose config: ${COMPOSE_CONFIG_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Docker build smoke: ${DOCKER_BUILD_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  E2E build: ${E2E_BUILD_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Python suites: ${PY_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Entrypoint: ${ENTRYPOINT_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Go:   ${GO_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Java: ${JAVA_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  full doc audit: ${AUDIT_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Image staleness: ${IMAGE_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  DDL smoke: ${DDL_SMOKE_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Evidence ownership: ${OWNERSHIP_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Schema/Perf: ${SCHEMA_PERF_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  SIGTERM-drain: ${SHUTDOWN_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Gateway suite: ${GATEWAY_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Compute suite: ${COMPUTE_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Mock-arrow suite: ${MOCK_ARROW_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Nautilus suite: ${NAUTILUS_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Pin discipline: ${PIN_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Compose images: ${IMAGES_LOG:-not-run}" | tee -a "$SUMMARY"
echo "  Image staleness (all): ${STALE_ALL_LOG:-not-run}" | tee -a "$SUMMARY"
