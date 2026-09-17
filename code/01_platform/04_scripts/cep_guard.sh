#!/usr/bin/env bash
# Fail the build if Apache Flink CEP (Complex Event Processing) is introduced
# into dependency or source files. Project rule (01-foundation.md): no CEP
# dependency/usage in the MVP order path.
#
# Scans only dependency declarations (pom.xml) and source (java/scala).
# Documentation that *prohibits* CEP is intentionally NOT scanned, so the
# guard does not trip on the rule text itself.
set -euo pipefail

ROOT="${1:-.}"

# R-091: a nonexistent/typo'd scan root silently matches nothing and the
# guard would "pass" without scanning anything. Fail loudly instead.
if [ ! -d "$ROOT" ]; then
	echo "ERROR: scan root '$ROOT' is not a directory — refusing to run (the guard would scan nothing)." >&2
	exit 2
fi
# P6-323: exit 2 = infra failure, distinct from hits = 1, so callers can tell
# "CEP found" apart from "scan broken".

# P6-038: grep stderr is captured, not swallowed — a non-empty error capture
# means an infra failure (missing binary, permission, bad root) and exits 2.
# `|| true` stays: grep exit 1 (no matches) is the normal clean path and must
# not trip set -e.
# P6-036/P6-037: Kotlin sources + JVM build files are scanned too — a CEP
# coordinate via Gradle/sbt/TOML or a Kotlin import must not bypass the guard.
# (Keep in lockstep with CepDependencyGuardTest's parity leg.)
# P6-322: `--` ends grep options so a scan root starting with `-` is a path.
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
