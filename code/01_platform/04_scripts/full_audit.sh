#!/usr/bin/env bash
# Full documentation audit — one command, three layers (2026-08-16 campaign
# consolidation; wired as `make full-audit`):
#
#   Layer 1 — the machine gates:
#     a. stale-claim scanner --upstream (table kinds, phase status, numeric
#        drift, test counts, C6 triples) — the `make stale-tables` gate
#     b. docs-audit (manifest, ownership matrix, schema-state diagram,
#        compat vocabulary, stale phrases, test counts, version pins)
#     c. --ddl mode (DDL files + schema_manifest.json table-kind parity)
#
#   Layer 2 — beyond-scanner sweeps (claims the scanner's pattern set cannot
#     see):
#     a. live Ranking/Reservations/Decisions claims (post-CHG-005 removal) —
#        the only allowed hits are the intentional whitelist: C8-required
#        matrix/REQ rows, struck-through lines, and the dated changelog entry
#     b. stale 'pending implementation / still pending' prose in the upstream
#        layers (requirements, architecture, contracts, project)
#
#   Layer 3 — master-dossier coherence: docs/08_implementation/04-signal-job.md
#     is the single signal-job dossier (13/14 deleted 2026-08-17; content
#     absorbed) and must carry the 2026-08-13 re-scope markers, the DEC-038
#     externalization landing, and the P11 status. The deletion is asserted too:
#     a resurrected 13/14 copy silently re-forks the dossier.
#
# Exit codes: 0 = all three layers ran and passed; 1 = a claim failed or a sweep
# could not run; 2 = the audit did not run at all (wrong interpreter, missing
# tooling, or a gate run in flight). Run from the repo root.
#
# Fail-closed contract: a sweep that cannot read its input is a FAILURE, never a
# green. grep exit 2 (missing or unreadable path) is reported as "sweep did not
# run"; only exit 1 (no matches) and exit 0 (hits, all whitelisted) can be green.
# The swept directories are preflighted for the same reason.
#
# Concurrency: a gate run rewrites the surefire XML that Layer 1b reads, so this
# audit refuses to start while one is in flight — except when it *is* that gate's
# own sequential step (the gate invokes it: step 10/19), which is exempt by
# ancestry, because a lock-based exemption would need a gate-side change.
#
# Boundary: this audit NEVER executes tests (no mvn/go/java). Executable test
# pins — e.g. the CHG-015 SIGTERM-drain regression (ING-UNIT-023/024), the
# Schema/Perf certification trio — are wired into run-monday-gates.sh
# (make gate), the single canonical CI gate. A mvn step here would clobber the
# surefire reports that Layer 1b's docs-audit C6 reads.
#
# The interpreter guard comes first, before `set -u`/`pipefail`: under sh (dash)
# or zsh those expansions are unset or unsupported, so a mis-invocation must
# diagnose itself instead of dying mid-audit.
if [ -z "${BASH_VERSION:-}" ]; then
	echo "FULL-AUDIT: FATAL — run this audit with bash (under sh/zsh BASH_SOURCE and pipefail are unavailable)" >&2
	exit 2
fi

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCRIPTS="$ROOT/code/01_platform/04_scripts"
DOCS="$ROOT/docs"
TRIO_DIR="$DOCS/08_implementation"

FAIL=0

step() { echo; echo "===== $1 ====="; }
pass() { echo "  PASS  $1"; }
fail() { echo "  FAIL  $1"; FAIL=1; }

cd "$ROOT" || { echo "FULL-AUDIT: FATAL — cannot enter $ROOT" >&2; exit 2; }

# ---------------------------------------------------------------------------
# Preflight — a missing input is diagnosed as such, never audited as "clean"
# ---------------------------------------------------------------------------
step "Preflight: interpreter, audit scripts, audited directories"

command -v python3 >/dev/null 2>&1 || {
	echo "FULL-AUDIT: FATAL — python3 not found (layers 1a-1c cannot run)" >&2
	exit 2
}
pass "python3 interpreter: $(command -v python3)"

for s in stale_table_kind_scan.py docs_audit.py; do
	[ -f "$SCRIPTS/$s" ] || fail "missing audit script: $SCRIPTS/$s"
done

# The swept directories, so a moved layer fails loudly here instead of sweeping
# nothing and reporting a green.
for d in "$SCRIPTS" "$DOCS" "$TRIO_DIR" \
	"$DOCS/02_requirements" "$DOCS/03_architecture" "$DOCS/04_contracts" "$DOCS/01_project"; do
	[ -d "$d" ] || fail "missing audited directory: $d — the layer reading it would otherwise sweep nothing"
done

gate_running() {
	if ! command -v pgrep >/dev/null 2>&1; then
		echo "FULL-AUDIT: WARN — pgrep not found, cannot detect a concurrent gate run" >&2
		return 1
	fi
	pgrep -f '[r]un-monday-gates[.]sh' >/dev/null 2>&1
}

inside_gate() { # walk up to 10 ancestors looking for the gate that started us
	local pid=$$ args hops=0
	while [ "$hops" -lt 10 ]; do
		args="$(ps -o args= -p "$pid" 2>/dev/null)" || return 1
		case "$args" in *run-monday-gates.sh*) return 0 ;; esac
		pid="$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')"
		case "$pid" in '' | *[!0-9]*) return 1 ;; esac
		[ "$pid" -gt 1 ] || return 1
		hops=$((hops + 1))
	done
	return 1
}

if gate_running && ! inside_gate; then
	echo "FULL-AUDIT: refusing to run — a gate run (run-monday-gates.sh) is in flight and this audit" >&2
	echo "            is not its own step: Layer 1b would read surefire XML while it is being rewritten." >&2
	echo "            Re-run once the gate finishes." >&2
	exit 2
fi
pass "no conflicting gate run in flight"

# ---------------------------------------------------------------------------
# Layer 1 — machine gates
# ---------------------------------------------------------------------------
step "Layer 1a: stale-claim scanner (--upstream)"

if python3 "$SCRIPTS/stale_table_kind_scan.py" --upstream; then
	pass "stale_table_kind_scan --upstream (table kinds / phase status / numeric drift / test counts / C6)"
else
	fail "stale_table_kind_scan --upstream found un-annotated claims"
fi

step "Layer 1b: docs-audit"

if python3 "$SCRIPTS/docs_audit.py"; then
	pass "docs_audit.py (manifest / ownership / schema-state / compat / stale phrases / counts / pins)"
else
	fail "docs_audit.py failed"
fi

step "Layer 1c: DDL + manifest parity (--ddl)"

if python3 "$SCRIPTS/stale_table_kind_scan.py" --ddl; then
	pass "stale_table_kind_scan --ddl (DDL files + schema_manifest.json kinds)"
else
	fail "stale_table_kind_scan --ddl found DDL/manifest drift"
fi

# ---------------------------------------------------------------------------
# Layer 2 — beyond-scanner sweeps
# ---------------------------------------------------------------------------
step "Layer 2a: live Ranking/Reservations/Decisions claims (post-CHG-005)"

# ERE throughout (`-E`): BRE alternation `\|` is GNU-only and would match a literal
# `ranking|reservation` under BSD/macOS grep. Options precede the pattern and paths,
# and the first stage's exit status is captured on its own — a later stage exiting 1
# ("everything filtered out") must not be able to mask a stage that exited 2 (unreadable).
RANKING_RC=0
RANKING_RAW="$(grep -rn -iE --include='*.md' 'ranking|reservation' "$DOCS")" || RANKING_RC=$?

if [ "$RANKING_RC" -ge 2 ]; then
	fail "layer 2a sweep did not run: grep exited $RANKING_RC (unreadable or missing path under $DOCS)"
	RANKING_HITS=""
elif [ "$RANKING_RC" -eq 1 ]; then
	RANKING_HITS=""
else
	RANKING_HITS="$(printf '%s\n' "$RANKING_RAW" \
		| grep -viE 'removed|out of scope|removal|preservation|resource reservations' \
		| grep -vE '^[^:]*/(change-records|04-decisions)/' \
		| grep -vE '^[^:]*/(10-ranking|05-risks)' \
		| grep -viE 'historical|postponed|deferred|superseded|stub retained|cross-reference|AC-RNK|REQ-RNK|ASM-RNK' \
		| grep -vE '### Ranking|\| Ranking \||REQ-FLS-007|REQ-FLS-014|REQ-FLS-016|2026-07-23|~~' || true)"
fi

if [ -z "$RANKING_HITS" ]; then
	pass "no live Ranking/Reservations/Decisions claims beyond the intentional whitelist"
else
	fail "unexpected live ranking/reservation claims:"
	echo "$RANKING_HITS"
fi

step "Layer 2b: stale 'pending implementation' prose in upstream layers"

# No `2>/dev/null` here: swallowing stderr is what let an unreadable layer look clean.
PENDING_RC=0
PENDING_RAW="$(grep -rn -iE 'still pending|pending implementation|remains pending|not yet implemented' \
	"$DOCS/02_requirements" "$DOCS/03_architecture" "$DOCS/04_contracts" "$DOCS/01_project")" || PENDING_RC=$?

if [ "$PENDING_RC" -ge 2 ]; then
	fail "layer 2b sweep did not run: grep exited $PENDING_RC (unreadable or missing upstream layer)"
	PENDING_HITS=""
elif [ "$PENDING_RC" -eq 1 ]; then
	PENDING_HITS=""
else
	PENDING_HITS="$(printf '%s\n' "$PENDING_RAW" \
		| grep -viE 'removed|out of scope|resolved|landed|superseded|historical' || true)"
fi

if [ -z "$PENDING_HITS" ]; then
	pass "no un-annotated 'pending implementation' claims in upstream layers"
else
	fail "un-annotated pending-implementation claims:"
	echo "$PENDING_HITS"
fi

# ---------------------------------------------------------------------------
# Layer 3 — dossier-trio coherence
# ---------------------------------------------------------------------------
step "Layer 3: master-dossier coherence (04-signal-job holds the trio markers)"

TRIO_FAIL=0
trio_check() { # desc, file, literal substring
	if [ ! -f "$2" ]; then
		echo "  FAIL  $1 — file missing: $2"
		TRIO_FAIL=1
		return 1
	fi
	if grep -qF -- "$3" "$2"; then
		echo "  PASS  $1"
	else
		echo "  FAIL  $1 — missing '$3' in $2"
		TRIO_FAIL=1
	fi
}

# The other half of the same contract: 13/14 were deleted 2026-08-17 and their
# content absorbed here. A resurrected copy would re-fork the dossier while every
# positive check above stayed green, so the retirement is asserted, not assumed.
TRIO_STALE_SEEN=0
for stale in "$TRIO_DIR/13-candle-log-kv-replay-safety.md" \
	"$TRIO_DIR/14-candle-log-kv-replay-safety_2.md"; do
	if [ -e "$stale" ]; then
		echo "  FAIL  stale dossier resurrected: $stale"
		TRIO_FAIL=1
		TRIO_STALE_SEEN=1
	fi
done
[ "$TRIO_STALE_SEEN" -eq 0 ] && echo "  PASS  the retired 13/14 dossiers stay deleted (2026-08-17 absorption)"

# Doc 04 (04-signal-job.md): DEC-038 banner + SIG-PERF-001 halves.
trio_check "doc04: DEC-038 banner records the externalization landing" \
	"$TRIO_DIR/04-signal-job.md" \
	"SUPERSEDED SAME-DAY (2026-08-15): the live-cluster externalization measurement LANDED"
trio_check "doc04: DEC-038 status line records the live writer wiring landing" \
	"$TRIO_DIR/04-signal-job.md" \
	"SUPERSEDED SAME-DAY (2026-08-15): the live writer wiring LANDED"
trio_check "doc04: SIG-PERF-001 halves disambiguated (benchmark landed / decision-p99 removed)" \
	"$TRIO_DIR/04-signal-job.md" \
	"externalization-benchmark half LANDED"

# Absorbed trio markers (13/14 deleted 2026-08-17; content moved into 04):
# candle [LOG+KV] retired + signal tables carry the facility; P11 + re-scope markers.
trio_check "doc04: candle [LOG+KV] RETIRED banner" \
	"$TRIO_DIR/04-signal-job.md" \
	"CANDLE [LOG + KV] RETIRED"
trio_check "doc04: feature_candles_15s_current retired" \
	"$TRIO_DIR/04-signal-job.md" \
	"feature_candles_15s_current"
trio_check "doc04: P11 section present" \
	"$TRIO_DIR/04-signal-job.md" \
	"P11 — DEC-038 state ownership: dedup externalization"
trio_check "doc04: re-scope LANDED marker (CANDLE-CANONICAL-001 cell)" \
	"$TRIO_DIR/04-signal-job.md" \
	"re-scope LANDED 2026-08-13"
trio_check "doc04: P11 landed re-target note" \
	"$TRIO_DIR/04-signal-job.md" \
	"P11 landed 2026-08-15"

if [ "$TRIO_FAIL" -ne 0 ]; then
	fail "master-dossier coherence broken — 04-signal-job.md disagrees with the reconciled truth"
fi

# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------
echo
if [ "$FAIL" -eq 0 ]; then
	echo "FULL-AUDIT: all layers green (gates + beyond-scanner sweeps + trio coherence) — exit 0"
	exit 0
fi
echo "FULL-AUDIT: FAILURES above — exit 1"
exit 1
