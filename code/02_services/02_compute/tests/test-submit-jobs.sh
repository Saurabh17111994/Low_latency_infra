#!/usr/bin/env bash
# End-to-end test for submit-jobs.sh hardening (P2-080/081/083/085/201 +
# 7.2: 082/198/199/200). Runs the REAL launcher against a stub JobManager
# (tests/stub-jm.py) serving adversarial JSON and scripted failures.
# Requires: bash, curl, jq, python3. Usage: ./test-submit-jobs.sh
set -uo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
LAUNCHER="${DIR}/../submit-jobs.sh"
PASS=0; FAIL=0

# run_case name scenario expect_rc expect_grep [port] [jm_value]
run_case() {
	local name="$1" scenario="$2" expect_rc="$3" expect_grep="$4"
	local port="${5:-8081}" jm="${6:-127.0.0.1}"
	local jar out rc stub
	jar="$(mktemp)"; echo dummy > "$jar"
	STUB_SCENARIO="$scenario" STUB_PORT="$port" python3 "${DIR}/stub-jm.py" & stub=$!
	for _ in $(seq 1 50); do
		curl -fsS "http://127.0.0.1:${port}/v1/config" >/dev/null 2>&1 && break
		sleep 0.1
	done
	out=$(FLINK_JOBMANAGER="$jm" COMPUTE_SUBMIT_SIGNAL=0 \
		COMPUTE_JAR="$jar" bash "$LAUNCHER" 2>&1)
	rc=$?
	kill "$stub" 2>/dev/null; wait "$stub" 2>/dev/null
	rm -f "$jar"
	if [ "$rc" -eq "$expect_rc" ] && printf '%s' "$out" | grep -q "$expect_grep"; then
		PASS=$((PASS+1)); echo "PASS: $name"
	else
		FAIL=$((FAIL+1)); echo "FAIL: $name (rc=$rc, want $expect_rc, grep '$expect_grep')"
		printf '%s\n' "$out" | tail -n 10
	fi
}

# run_gate name extra_env expect_grep  — production-branch gate, no stub.
run_gate() {
	local name="$1" extra_env="$2" expect_grep="$3"
	local out rc
	out=$(env DEPLOYMENT_ENV=production $extra_env bash "$LAUNCHER" 2>&1)
	rc=$?
	if [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "$expect_grep"; then
		PASS=$((PASS+1)); echo "PASS: $name"
	else
		FAIL=$((FAIL+1)); echo "FAIL: $name (rc=$rc, want 1, grep '$expect_grep')"
		printf '%s\n' "$out" | tail -n 6
	fi
}

# --- 7.1 regression (still must pass) ---
run_case "happy-adversarial-json" happy 0 "launcher finished — jobs submitted"
run_case "flaky-polls-retry" flaky-polls 0 "launcher finished — jobs submitted"
run_case "dead-upload-fatal" dead-upload 1 "FATAL — jar upload"
run_case "dead-run-fatal" dead-run 1 "FATAL — .* submit failed"

# --- P2-082: terminal-only states ---
# CANCELING is transient — old code FATALed on first CANCELING; new keeps polling.
run_case "cancel-then-running-transient" cancel-then-running 0 "launcher finished — jobs submitted"
# FAILED is terminal — must stop with FATAL, not poll 30x.
run_case "job-fails-terminal" job-fails 1 "entered terminal state FAILED"

# --- P2-198: production gates (exit before any JM contact) ---
run_gate "backend-case-variant-rejected" "STATE_BACKEND=HashMap" "STATE_BACKEND must be 'rocksdb'"
run_gate "backend-bare-scheme-rejected" "STATE_BACKEND=rocksdb CHECKPOINT_DIR=s3:// AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y S3_ENDPOINT=z" "CHECKPOINT_DIR must be an S3"
run_gate "deployment-env-bogus" "DEPLOYMENT_ENV=staging" "DEPLOYMENT_ENV must be 'dev' or 'production', got 'staging'"

# P2-198: a VALID prod config must pass the gate (fails later at JM-ready,
# proving it was NOT stopped by the gate itself).
{
	jar="$(mktemp)"; echo dummy > "$jar"
	out=$(DEPLOYMENT_ENV=production STATE_BACKEND=rocksdb CHECKPOINT_DIR=s3://bucket/k \
		AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y S3_ENDPOINT=z \
		COMPUTE_JAR="$jar" bash "$LAUNCHER" 2>&1)
	rc=$?
	rm -f "$jar"
	if [ "$rc" -eq 1 ] && ! printf '%s' "$out" | grep -q "STATE_BACKEND must\|CHECKPOINT_DIR must"; then
		PASS=$((PASS+1)); echo "PASS: prod-valid-config-passes-gate"
	else
		FAIL=$((FAIL+1)); echo "FAIL: prod-valid-config-passes-gate (rc=$rc)"; printf '%s\n' "$out" | tail -n 6
	fi
}

# --- P2-199: scheme + explicit port must not double-append :8081 ---
run_case "jm-scheme-port-not-doubled" happy 0 "launcher finished — jobs submitted" 18081 "http://127.0.0.1:18081"

echo "test-submit-jobs: ${PASS} passed, ${FAIL} failed"
[ "$FAIL" -eq 0 ]
