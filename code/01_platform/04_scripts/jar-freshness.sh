#!/usr/bin/env bash
# =============================================================================
# jar-freshness.sh — the single compute-jar freshness guard (CHG-522/CHG-523).
#
# Sourced (never executed) by:
#   - rollout-savepoint.sh   restore / rolling-update path, in preflight
#   - pipeline-lib.sh        fresh submit path, pipeline_submit_job
#
# Contract: the caller sets ROOT (repo root) before calling. The check is
# mtime-based against the jar's real inputs — the compute module src/pom, its
# common dependency src/pom (the jar shades common's classes) and the parent
# pom. A jar older than any of them deploys old code while every runtime probe
# (RUNNING, checkpoint, dedup continuity, day board) stays green; measured
# 2026-10-02: a 12:03 jar predating the 13:43 CHG-516 commit was restored
# silently.
#
# jar_freshness_check <jar> prints the reason and returns:
#   0 fresh (or ALLOW_STALE_JAR=1 — deliberate rollback escape hatch)
#   1 stale
#   2 jar missing/unreadable
# =============================================================================

JAR_FRESHNESS_ROOTS=(
	code/02_services/02_compute/src/main
	code/02_services/02_compute/pom.xml
	code/common/src/main
	code/common/pom.xml
	code/pom.xml
)

jar_freshness_check() { # $1 = jar path; prints the verdict; statuses above
	local jar="$1" newest
	[ -r "$jar" ] || {
		printf 'jar not readable: %s (build it first: cd code/02_services/02_compute && mvn -o package -DskipTests)\n' "$jar"
		return 2
	}
	if [ "${ALLOW_STALE_JAR:-0}" = "1" ]; then
		printf 'ALLOW_STALE_JAR=1 — jar freshness NOT checked for %s (deliberate rollback?)\n' "$jar"
		return 0
	fi
	newest="$(find "${JAR_FRESHNESS_ROOTS[@]/#/$ROOT/}" -type f -newer "$jar" \
		-printf '%T@ %p\n' 2>/dev/null | sort -rn | head -n 1)"
	if [ -n "$newest" ]; then
		printf 'compute jar is STALE: %s predates %s — rebuild it first: cd code/02_services/02_compute && mvn -o package -DskipTests (ALLOW_STALE_JAR=1 overrides for a deliberate rollback)\n' \
			"$jar" "${newest#* }"
		return 1
	fi
	printf 'jar freshness: %s is newer than the compute/common sources\n' "$jar"
	return 0
}
