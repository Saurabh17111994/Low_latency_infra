# Wave 37 — Signal-chain E2E + CEP guards (19 findings) — DETAILED plan

Date: 2026-09-17. Status: VERIFIED read-only. Scope doc v2 (detailed, post-review).
Audit: `p6-ops-scripts-tests-chaos-audit.md` §10 (e9f995f4, e3ba8745).
Rulebook: AGENTS.md wave workflow (verify → scope doc → approval → region-sliced commits + CHG each → gate → tick).

## 0. Verdict summary

19/19 REPRODUCE against the tree, 0 fully stale. 6 deviations from the audit's
letter (all evidence-backed, none skip a fix):

| # | Finding | Deviation from audit prescription | Why |
|---|---------|-----------------------------------|-----|
| 1 | P6-537 | Flag fix only; no test resurrection; `cd` half stale | Test retired deliberately in `0f3e5952`; resurrection is a separate decision. `set -e` already exits on failed `&&` list — explicit `cd` anyway (free). |
| 2 | P6-324 | Document standalone-equivalence instead of `-pl :compute` | Root reactor excludes compute per R-272 (`code/pom.xml` modules: common, ingestion, gateway) → `-pl :compute` fails. |
| 3 | P6-040 | `set -f` (noglob) scoped around call, not quoting | Quoting breaks intentional flags-splitting (`MVN_FLAGS=-o` must split); audit allows either. |
| 4 | P6-535 | Skip mvn `-o` cold-cache probe | No cheap reliable probe; would add false-fail risk. |
| 5 | P6-039 | Flags only, no extra report-file check | Surefire flags already fail loud; extra check redundant. |
| 6 | P6-777 | Delete var, don't "use it" | One-line delete beats rewiring classpath assembly. |
| 7 | P6-536 | Pre-`rm` + `-s` check, no `trap` (see slice 1) | Audit's `trap ... EXIT` would clobber the P6-533 EXIT trap. |
| 8 | P6-533 | Trap only when dir is defaulted (see slice 1) | Must not `rm -rf` a caller-supplied dir. |

## 1. Slice 1 — `code/01_platform/04_scripts/run-signal-chain-e2e.sh` (9 findings, 1 commit + CHG)

### 1.1 P6-533 — unique checkpoint dir, trap only the default
Replace L32:
```bash
: "${E2E_CHECKPOINT_DIR:=file:///tmp/signal-chain-e2e-checkpoints}"
```
with:
```bash
if [ -z "${E2E_CHECKPOINT_DIR:-}" ]; then
    E2E_CHECKPOINT_DIR="file:///tmp/signal-chain-e2e-checkpoints-$$-$(date +%s)"
    trap 'rm -rf "${E2E_CHECKPOINT_DIR#file://}"' EXIT INT TERM
fi
```

### 1.2 P6-534 — input validation block
Insert after the OTEL default block (all `: "${...:=...}"` done), before `INGESTION_JAR_DIR`:
```bash
case "$E2E_RUN_MINUTES" in ''|*[!0-9]*|0) echo "chain-e2e: FATAL — E2E_RUN_MINUTES must be a positive integer (got '$E2E_RUN_MINUTES')" >&2; exit 2;; esac
case "$PARALLELISM" in ''|*[!0-9]*|0) echo "chain-e2e: FATAL — PARALLELISM must be a positive integer (got '$PARALLELISM')" >&2; exit 2;; esac
case "$STATE_BACKEND" in rocksdb|hashmap) ;; *) echo "chain-e2e: FATAL — STATE_BACKEND must be rocksdb|hashmap (got '$STATE_BACKEND')" >&2; exit 2;; esac
case "$OTEL_COLLECTOR_HOST" in *:*) ;; *) echo "chain-e2e: FATAL — OTEL_COLLECTOR_HOST must be HOST:PORT (got '$OTEL_COLLECTOR_HOST')" >&2; exit 2;; esac
```
Enum anchor: `SignalJobConfig` accepts only `rocksdb|hashmap` (verified).

### 1.3 P6-777 — delete dead var
Delete `: "${INGESTION_JAR_DIR:=$CODE_ROOT/02_services/01_ingestion/target}"` (defined once, referenced nowhere).

### 1.4 P6-191 — warn-and-unset manifest
Replace the `exit 2` block with:
```bash
: "${INSTRUMENT_MANIFEST_PATH:=$MANIFEST_DEFAULT}"
if [ ! -f "$INSTRUMENT_MANIFEST_PATH" ]; then
    echo "chain-e2e: WARN — manifest not found: $INSTRUMENT_MANIFEST_PATH; falling back to test default" >&2
    unset INSTRUMENT_MANIFEST_PATH
fi
```
AND change `export INSTRUMENT_MANIFEST_PATH` (export block) to:
```bash
if [ -n "${INSTRUMENT_MANIFEST_PATH:-}" ]; then export INSTRUMENT_MANIFEST_PATH; fi
```
(required: `set -u` would abort on the unset var).

### 1.5 P6-192 — export all five Arrow vars
Replace:
```bash
[ "$E2E_BROKER" != "faketool" ] && export ARROW_APP_ID ARROW_APP_SECRET # ARROW_TOKEN removed
```
with:
```bash
if [ "$E2E_BROKER" != "faketool" ]; then
    export ARROW_APP_ID ARROW_APP_SECRET ARROW_USER_ID ARROW_PASSWORD ARROW_TOTP_KEY
fi
```
(go-bridge `main.go`/`emitter_record.go` read all five — verified.)

### 1.6 P6-017 (CRIT) — faketool `-o` path
Replace `(cd "$BRIDGE_DIR" && go build -tags faketool -o faketool ./faketool)` with
`(cd "$BRIDGE_DIR" && go build -tags faketool -o faketool/faketool ./faketool)`.
(`FAKETOOL_BIN` already points at `faketool/faketool`; `faketool/` is a dir — verified.)

### 1.7 P6-535 — fail-fast preflight (before builds echo)
```bash
command -v go >/dev/null 2>&1 || { echo "chain-e2e: FATAL — go not found on PATH" >&2; exit 2; }
command -v mvn >/dev/null 2>&1 || { echo "chain-e2e: FATAL — mvn not found on PATH" >&2; exit 2; }
fluss_host="${FLUSS_BOOTSTRAP%:*}"; fluss_port="${FLUSS_BOOTSTRAP##*:}"
timeout 5 bash -c "</dev/tcp/$fluss_host/$fluss_port" 2>/dev/null || { echo "chain-e2e: FATAL — Fluss unreachable at $FLUSS_BOOTSTRAP" >&2; exit 2; }
otel_host="${OTEL_COLLECTOR_HOST%:*}"; otel_port="${OTEL_COLLECTOR_HOST##*:}"
timeout 5 bash -c "</dev/tcp/$otel_host/$otel_port" 2>/dev/null || { echo "chain-e2e: FATAL — OTEL collector unreachable at $OTEL_COLLECTOR_HOST" >&2; exit 2; }
```
Pure bash+coreutils, no new deps. Cold-`.m2` probe skipped (deviation 4).

### 1.8 P6-536 — cp-file hygiene without trap juggling
Replace:
```bash
INGESTION_CP="$CODE_ROOT/02_services/01_ingestion/target/classes:$(cat "$INGESTION_CP_FILE")"
rm -f "$INGESTION_CP_FILE"
```
with:
```bash
rm -f "$INGESTION_CP_FILE"
(cd "$CODE_ROOT" && mvn -q -o dependency:build-classpath ...) || { ...; exit 1; }  # unchanged block, preceded by pre-rm
[ -s "$INGESTION_CP_FILE" ] || { echo "chain-e2e: FATAL — ingestion classpath file missing/empty: $INGESTION_CP_FILE" >&2; exit 1; }
INGESTION_CP="$CODE_ROOT/02_services/01_ingestion/target/classes:$(cat "$INGESTION_CP_FILE")"
rm -f "$INGESTION_CP_FILE"
```
(i.e. add pre-`rm` before the mvn block and the `-s` gate after; no EXIT trap — deviation 7.)

### 1.9 P6-537 — loud finale
Replace:
```bash
cd "$CODE_ROOT/02_services/02_compute" &&
	mvn -o test -Dtest=SignalChainLiveE2ETest
```
with:
```bash
cd "$CODE_ROOT/02_services/02_compute" || { echo "chain-e2e: FATAL — compute module dir missing" >&2; exit 1; }
mvn -o test -Dtest=SignalChainLiveE2ETest -DfailIfNoTests=true -Dsurefire.failIfNoSpecifiedTests=true
```
(With the test retired, this now fails LOUD instead of false-passing — intended.)

### Slice-1 tests
- `bash -n` + shellcheck (if present).
- Fast-fail paths (no cluster, seconds each): `E2E_BROKER=bogus` → exit 2; `E2E_RUN_MINUTES=0` → exit 2; `STATE_BACKEND=foo` → exit 2; `OTEL_COLLECTOR_HOST=barehost` → exit 2; `FLUSS_BOOTSTRAP=localhost:9` → exit 2 (probe).
- Faketool `go build -tags faketool -o faketool/faketool ./faketool` in `go-bridge` (cheap, local; needs go toolchain — if absent, note as gap).
- NOT run: full script (test retired → fails by design at finale + 30-min live + builds burn ~10 min).

## 2. Slice 2 — CEP guards (8 findings, 1 commit + CHG)

### 2.1 `cep_guard.sh` — P6-036 + P6-037 + P6-322 + P6-038
```bash
ROOT="${1:-.}"

# R-091: refuse a non-directory root (exit 2 = infra, distinct from hits = 1).
if [ ! -d "$ROOT" ]; then
	echo "ERROR: scan root '$ROOT' is not a directory — refusing to run (the guard would scan nothing)." >&2
	exit 2
fi

ERR_FILE="$(mktemp)"
HITS="$(grep -rEn \
	--exclude-dir=.git --exclude-dir=target --exclude-dir=node_modules \
	--include=pom.xml --include='*.java' --include='*.scala' --include='*.kt' --include='*.kts' \
	--include='build.gradle' --include='build.gradle.kts' --include='settings.gradle' --include='settings.gradle.kts' \
	--include='build.sbt' --include='libs.versions.toml' --include='extensions.xml' \
	'flink-cep|org\.apache\.flink\.cep' -- "$ROOT" 2>"$ERR_FILE" || true)"
if [ -s "$ERR_FILE" ]; then
	echo "ERROR: cep_guard infra failure while scanning '$ROOT':" >&2
	cat "$ERR_FILE" >&2
	rm -f "$ERR_FILE"
	exit 2
fi
rm -f "$ERR_FILE"

if [ -n "$HITS" ]; then
	echo "ERROR: Flink CEP usage is forbidden by project policy (no CEP in MVP order path)."
	echo "$HITS"
	exit 1
fi

echo "OK: no Flink CEP references found in dependency/source files."
```
Notes: `--` before `"$ROOT"` (P6-322); stderr captured not swallowed, non-empty stderr = infra → exit 2 (P6-038); `|| true` retained because grep exit 1 (no matches) must not trip `set -e`; exit 0 + `OK:` line preserved (JUnit agreement leg asserts both); `extensions.xml` basename covers `.mvn/extensions.xml` (`--include` is basename-matched).

### 2.2 `cep_module_check.sh` — P6-323 + P6-039 + P6-040
[1/2] (capture without `set -e` self-trip — `VAR=$(cmd)` failing as a standalone
statement exits the script, so the `||` shield is load-bearing):
```bash
echo "=== [1/2] shell guard on the compute module ==="
GUARD_RC=0
GUARD_OUT="$(bash "$GUARD" "$MODULE" 2>&1)" || GUARD_RC=$?
if [ "$GUARD_RC" -eq 2 ]; then
	echo "$GUARD_OUT" >&2
	fail "cep_guard.sh INFRA failure on the compute module (exit 2) — triage output above"
elif [ "$GUARD_RC" -ne 0 ]; then
	echo "$GUARD_OUT" >&2
	fail "cep_guard.sh found CEP references in the compute module"
fi
echo "$GUARD_OUT"
```
[2/2]:
```bash
set -f
if ! mvn ${MVN_FLAGS:-} -q -f "$POM" test -Dtest=CepDependencyGuardTest -DfailIfNoTests=true -Dsurefire.failIfNoSpecifiedTests=true; then
	set +f
	fail "CepDependencyGuardTest failed (in-JVM scan / shell-guard agreement / scope parity)"
fi
set +f
```
(`set -f` = noglob: keeps intentional flags-splitting, kills glob expansion — deviation 3.)
P6-324: append a comment block above [2/2] documenting standalone-equivalence
(R-272 excludes compute from the root reactor, so `-f $POM` is the only shape;
verify at implement time that compute pom has no intra-repo SNAPSHOT deps —
if it does, note the `~/.m2` staleness caveat in the comment).

### 2.3 `CepDependencyGuardTest.java` — lockstep (same commit)
- `SCANNED_SUFFIXES` → `List.of(".java", ".scala", ".kt", ".kts")`
- `SCANNED_FILENAMES` → `List.of("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "build.sbt", "libs.versions.toml", "extensions.xml")`
- ProcessBuilder `--include` args mirror the guard exactly.
- Javadoc leg-1: "java/scala sources" → "java/scala/kotlin sources and JVM build files".
- Must not spell the forbidden literals (assemble from parts — existing style).

### Slice-2 tests
- New `tests/test_cep_guards.sh` (hermetic tmp fixtures, follow existing `ok`/`bad` style): kt-hit → exit 1; leading-dash dir → exit 0 (proves `--`); missing dir → exit 2; clean tree → exit 0 + `OK:`; infra-stub (unreadable dir) → exit 2.
- `bash -n` both scripts.
- Wrapper clean-path integration on the real compute module with `MVN_FLAGS=-o` (runs real [2/2] → doubles as JUnit verification). Wrapper negative path ([1/2] exit-2 message) by inspection + guard-level exit-2 test.
- Standalone JUnit: `mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=CepDependencyGuardTest` (fallback online if `-o` fails).

## 3. Slice 3 — `check_flink_properties.py` (2 findings, 1 commit + CHG)

### 3.1 P6-347 — duplicate keys fail
After `keys = [k for k, _ in props]` insert:
```python
    # E. duplicate keys (last-wins merge silently discards earlier values —
    # the same silent-drop class as the prefix collisions in B).
    seen: dict[str, int] = {}
    for k, _ in props:
        seen[k] = seen.get(k, 0) + 1
    for k, c in seen.items():
        if c > 1:
            failures.append(
                f"DUPLICATE KEY {k!r} appears {c}x in FLINK_PROPERTIES\n"
                f"    WHY: duplicate entries are merged with last-wins semantics, silently\n"
                f"    discarding the earlier value (the same silent-drop class as the prefix\n"
                f"    collisions this validator blocks).\n"
                f"    FIX: keep a single definition of the key."
            )
```
Update module docstring (add E + block-not-found to the fail-loud list).

### 3.2 P6-722 — `main()` catches the missing block
```python
    try:
        failures = check()
    except ValueError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 1
```

### Slice-3 tests (extend `tests/test_check_flink_properties.py`)
- `test_duplicate_key_fails`: VALID_BLOCK + one repeated key → exactly one DUPLICATE failure.
- `test_main_reports_missing_block_gracefully`: block-last compose (no following key) + `monkeypatch.setattr(validator, "COMPOSE", p)` → `main()` returns 1, `FAIL` on stderr, no raise (import module as `validator` alongside existing `from` import).
- `pytest` the file. Implement-time check: run validator against the REAL compose — if the new dup check fires on the real file, fix the compose in the same commit (it would also trip G22 in the gate).

## 4. Close-out

1. Three region-sliced commits, one CHG record each (locate CHG ledger at implement time — ask if ambiguous).
2. Full `make gate` (background, ~90 min guard; hands off cluster).
3. Tick all 19 in the audit file with `Fixed:` + commit + evidence; run `p6_map_refresh.py --write`.
4. Pop `stash@{0}` after green.

## 5. Standing risks / non-goals

- JUnit parity coupling (mitigated: same-commit lockstep + parity test run).
- `make cep-check` (repo-wide) gains ~10 includes — negligible; verdicts unchanged (no such files exist).
- Full 30-min E2E NOT testable (retired test; resurrection = separate decision, out of scope).
- Script remains DRAFT with no gate callers; gate stays green by construction.
- Zero cluster writes in all slices (`go build` + unit tests are local).
