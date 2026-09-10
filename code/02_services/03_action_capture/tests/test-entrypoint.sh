#!/usr/bin/env bash
# Functional check for docker-entrypoint.sh (P2 8.3: 088 export, 204 arg/JAVA_OPTS forwarding).
# Runs the real entrypoint against a stub `java` that records argv + env.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
EP="$HERE/../docker-entrypoint.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

cat > "$TMP/java" <<'JEOF'
#!/usr/bin/env bash
printf '%s\n' "$@" > "$JAVA_ARGS_FILE"
env | grep '^FLUSS_BOOTSTRAP=' >> "$JAVA_ARGS_FILE"
JEOF
chmod +x "$TMP/java"

# Case 1: no FLUSS_BOOTSTRAP set → exported default; JAVA_OPTS split; CMD args forwarded.
export JAVA_ARGS_FILE="$TMP/args1"
PATH="$TMP:$PATH" env -u FLUSS_BOOTSTRAP \
  JAVA_OPTS="-Xmx1g -Dlog4j2.configurationFile=/opt/log4j2.xml" \
  "$EP" --help --version
grep -q -- "-Xmx1g" "$TMP/args1" || fail "JAVA_OPTS not forwarded"
grep -q -- "-Dlog4j2.configurationFile=/opt/log4j2.xml" "$TMP/args1" || fail "JAVA_OPTS not split"
grep -qx -- "-jar" "$TMP/args1" || fail "jar flag not passed"
grep -qx -- "/app/action-capture.jar" "$TMP/args1" || fail "jar path not passed"
grep -qx -- "--help" "$TMP/args1" || fail "--help not passed after jar"
grep -q -- "--help" "$TMP/args1" || fail "CMD arg --help dropped"
grep -q -- "--version" "$TMP/args1" || fail "CMD arg --version dropped"
grep -q "^FLUSS_BOOTSTRAP=fluss-coordinator:9123$" "$TMP/args1" || fail "default FLUSS_BOOTSTRAP not exported"

# Case 2: explicit FLUSS_BOOTSTRAP wins; empty JAVA_OPTS stays clean.
export JAVA_ARGS_FILE="$TMP/args2"
PATH="$TMP:$PATH" \
  FLUSS_BOOTSTRAP="fluss-other:9199" JAVA_OPTS="" \
  "$EP" || true
grep -q "^FLUSS_BOOTSTRAP=fluss-other:9199$" "$TMP/args2" || fail "explicit FLUSS_BOOTSTRAP lost"
[ "$(grep -c -- "-Xmx1g" "$TMP/args2")" = "0" ] || fail "empty JAVA_OPTS leaked content"

echo "entrypoint OK: export + JAVA_OPTS split + argv forwarding verified"
