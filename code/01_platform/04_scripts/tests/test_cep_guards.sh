#!/usr/bin/env bash
# Pins cep_guard.sh's exit contract (W37 slice 2: P6-036/037/038/322):
#   0 + OK line  = clean scan
#   1            = CEP references found (incl. Kotlin sources + build files)
#   2            = infra failure (missing root, unreadable tree) — never a pass
# Hermetic: all fixtures live under a mktemp dir. Safe to run anywhere:
# only *.kt/*.gradle/etc FIXTURE files carry the forbidden literals, and the
# guard never scans *.sh, so this file cannot trip `make cep-check` on the repo.
set -uo pipefail # no -e: non-zero exits are the assertions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD="$SCRIPT_DIR/../cep_guard.sh"

pass=0; fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "BAD: $1"; fail=$((fail + 1)); }
expect_rc() { # want got label
	if [ "$1" = "$2" ]; then ok "$3 (rc=$2)"; else bad "$3 (want rc=$1, got rc=$2)"; fi
}

T="$(mktemp -d)"
trap 'chmod -R u+rwX "$T" 2>/dev/null; rm -rf "$T"' EXIT

# 1. clean tree -> 0 + OK line
mkdir -p "$T/clean/src"
echo 'public class A {}' > "$T/clean/src/A.java"
out="$(bash "$GUARD" "$T/clean" 2>&1)"; rc=$?
expect_rc 0 "$rc" "clean tree exits 0"
case "$out" in *"OK: no Flink CEP references found"*) ok "clean tree prints OK line";; *) bad "clean tree OK line missing: $out";; esac

# 2. Kotlin hit -> 1 (P6-036)
mkdir -p "$T/kotlin/src"
echo 'import org.apache.flink.cep.PatternStream' > "$T/kotlin/src/Rule.kt"
out="$(bash "$GUARD" "$T/kotlin" 2>&1)"; rc=$?
expect_rc 1 "$rc" "kotlin CEP import exits 1"
case "$out" in *"Rule.kt"*) ok "kotlin hit names the file";; *) bad "kotlin hit output: $out";; esac

# 3. Gradle coordinate hit -> 1 (P6-037)
mkdir -p "$T/gradle"
echo 'implementation "org.apache.flink:flink-cep:2.1"' > "$T/gradle/build.gradle"
out="$(bash "$GUARD" "$T/gradle" 2>&1)"; rc=$?
expect_rc 1 "$rc" "gradle CEP coordinate exits 1"

# 4. leading-dash scan root -> 0, not a grep-option error (P6-322)
mkdir -p "$T/-dash/src"
echo 'public class B {}' > "$T/-dash/src/B.java"
out="$(bash "$GUARD" "$T/-dash" 2>&1)"; rc=$?
expect_rc 0 "$rc" "leading-dash root scanned as path"
case "$out" in *"invalid option"*) bad "dash root leaked into grep options: $out";; *) ok "dash root not parsed as options";; esac

# 5. missing root -> 2, never a pass (R-091 + P6-323)
out="$(bash "$GUARD" "$T/does-not-exist" 2>&1)"; rc=$?
expect_rc 2 "$rc" "missing scan root exits 2"

# 6. unreadable tree -> 2 with surfaced error (P6-038)
mkdir -p "$T/noperm/sub"
echo 'public class C {}' > "$T/noperm/sub/C.java"
chmod 000 "$T/noperm/sub"
out="$(bash "$GUARD" "$T/noperm" 2>&1)"; rc=$?
chmod 755 "$T/noperm/sub"
if [ "$(id -u)" = "0" ]; then
	ok "unreadable-tree case skipped (running as root)"
else
	expect_rc 2 "$rc" "unreadable tree exits 2"
	case "$out" in *"infra failure"*) ok "infra error surfaced";; *) bad "infra error output: $out";; esac
fi

echo "----"
echo "cep-guard pins: pass=$pass fail=$fail"
[ "$fail" -eq 0 ]
