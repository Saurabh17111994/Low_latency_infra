#!/usr/bin/env bash
# ddl-apply-concurrency-bench.sh — how does the DDL apply behave when N of them run at once?
#
# Why: the certificate's step 11 is three full manifest applies (S1 full, S2 no-ack, S4
# container bad-ownership) and the drill applies the same 27-table manifest again, so the
# apply path is the largest single block of live work in the gate. The evidence report's
# section 6 asked the obvious question — is any of it parallelisable, and what does the
# cluster do when it is? — and this is the measurement, not the answer: it runs N applies
# concurrently against DISJOINT scratch prefixes and records wall clock, exit codes and the
# cluster's own reaction (storm / NotLeader / ERROR lines) per level.
#
# It never touches the platform tables: every apply runs with DDL_APPLY_TABLE_PREFIX, so the
# tool creates `bench<N>L<level>_i_`-prefixed scratch tables and drops them on its terminal
# paths. After each level the harness checks the catalog is back to its baseline count and
# says so; a level that leaves tables behind is reported, and the leftover prefix is named so
# `--cleanup-prefix` can be run by hand.
#
# Deliberately NOT `set -e`: a failing apply at level 8 is a measurement, not a reason to
# abandon the run. Failures are counted and printed instead.
#
# Usage: bash code/01_platform/04_scripts/ddl-apply-concurrency-bench.sh [levels] [timeout_s]
#   levels     comma-separated concurrency levels (default 1,2,4,8,12)
#   timeout_s  per-apply timeout (default 1800)

set -uo pipefail

LEVELS="${1:-1,2,4,8,12}"
APPLY_TIMEOUT_S="${2:-1800}"
BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
MATRIX_EVIDENCE="${DDL_APPLY_MATRIX_EVIDENCE:-logs/schema-compat/composite-pk-raw-client-20260815.md}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$REPO_ROOT/logs/ddl-bench/phase2/$STAMP"
SUMMARY_TSV="$RUN_DIR/summary.tsv"
COORDINATOR="01_docker-fluss-coordinator-1"
TABLET="01_docker-fluss-tablet-1"

mkdir -p "$RUN_DIR"
cd "$REPO_ROOT" || exit 2

if [ ! -f "$MATRIX_EVIDENCE" ]; then
	echo "FATAL: matrix evidence not found: $MATRIX_EVIDENCE" >&2
	exit 2
fi

catalog_count() {
	python3 - "$REPO_ROOT" <<'PY'
import sys
sys.path.insert(0, sys.argv[1] + "/code/01_platform/04_scripts")
import gate_preflight
print(gate_preflight.catalog_tables())
PY
}

# Log lines that mean the cluster felt the load. TableNotExistException is the run-4b storm
# signature; NotLeader is the layer-2 churn the evidence report describes.
cluster_health() { # $1 = RFC3339 start, $2 = RFC3339 end -> "storm/notleader/errors"
	local start="$1" end="$2" storm notleader errors
	storm=$(docker logs --since "$start" --until "$end" "$COORDINATOR" "$TABLET" 2>&1 \
		| grep -c 'TableNotExistException' || true)
	notleader=$(docker logs --since "$start" --until "$end" "$COORDINATOR" "$TABLET" 2>&1 \
		| grep -c 'NotLeader' || true)
	errors=$(docker logs --since "$start" --until "$end" "$COORDINATOR" "$TABLET" 2>&1 \
		| grep -c ' ERROR ' || true)
	printf '%s/%s/%s' "$storm" "$notleader" "$errors"
}

sample_memory() { # $1 = out file, $2 = pid of the sampler
	while [ -d "/proc/$2" ]; do
		printf '%s %s\n' "$(date +%s)" "$(free -m | awk 'NR==2{print $7}')" >>"$1"
		sleep 5
	done
}

BASELINE="$(catalog_count)"
echo "baseline catalog: ${BASELINE} tables"
printf 'level\twall_s\tapplies\tfailures\tstorm/notleader/errors\tcatalog_after\tmin_available_mb\n' >"$SUMMARY_TSV"

IFS=',' read -r -a LEVEL_ARRAY <<<"$LEVELS"
for level in "${LEVEL_ARRAY[@]}"; do
	level_dir="$RUN_DIR/level$level"
	mkdir -p "$level_dir"
	start_epoch="$(date +%s)"
	start_iso="$(date -u -d "@$start_epoch" +%Y-%m-%dT%H:%M:%SZ)"
	mem_file="$level_dir/memory.txt"
	pids=()
	rcs=()

	for i in $(seq 1 "$level"); do
		apply_log="$level_dir/apply$i.log"
		apply_evidence="$level_dir/evidence$i"
		mkdir -p "$apply_evidence"
		FLUSS_BOOTSTRAP="$BOOTSTRAP" \
			DDL_APPLY_TABLE_PREFIX="bench${level}L${i}_" \
			DDL_APPLY_SKIP_SMOKE=1 \
			DDL_APPLY_EVIDENCE_DIR="$apply_evidence" \
			timeout -k 30 "$APPLY_TIMEOUT_S" python3 -u \
			code/01_platform/04_scripts/ddl_apply.py --apply-verified \
			--matrix-evidence "$MATRIX_EVIDENCE" >"$apply_log" 2>&1 &
		pids+=("$!")
	done

	sample_memory "$mem_file" $$ &
	sampler="$!"

	for pid in "${pids[@]}"; do
		rc=0
		wait "$pid" || rc=$?
		rcs+=("$rc")
	done
	kill "$sampler" 2>/dev/null
	wait "$sampler" 2>/dev/null

	end_epoch="$(date +%s)"
	end_iso="$(date -u -d "@$end_epoch" +%Y-%m-%dT%H:%M:%SZ)"
	wall=$((end_epoch - start_epoch))

	failures=0
	for rc in "${rcs[@]}"; do
		[ "$rc" -ne 0 ] && failures=$((failures + 1))
	done

	health="$(cluster_health "$start_iso" "$end_iso")"
	after="$(catalog_count)"
	min_mem="$(sort -k2 -n "$mem_file" | head -1 | awk '{print $2}')"
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$level" "$wall" "$level" "$failures" "$health" "$after" "${min_mem:-?}" >>"$SUMMARY_TSV"
	printf 'level=%-3s wall=%-4ss failures=%s cluster(storm/NotLeader/ERROR)=%s catalog=%s (baseline %s) min_available=%sMB\n' \
		"$level" "$wall" "$failures" "$health" "$after" "$BASELINE" "${min_mem:-?}"

	if [ "$after" != "$BASELINE" ]; then
		echo "WARNING: catalog is $after, baseline $BASELINE — level $level left scratch tables behind."
		echo "  prefixes to clean: $(grep -ho "bench${level}L[0-9]*_" "$level_dir"/apply*.log 2>/dev/null | sort -u | tr '\n' ' ')"
		echo "  stopping here: piling more churn onto an unclean catalog would measure the wrong thing."
		echo "RESULT: INCONCLUSIVE (leftover tables at level $level)"
		exit 1
	fi
done

echo "summary: $SUMMARY_TSV"
echo "evidence: $RUN_DIR"
echo "RESULT: ${#LEVEL_ARRAY[@]} level(s) measured — see summary.tsv"
