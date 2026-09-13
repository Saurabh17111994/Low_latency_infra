#!/usr/bin/env bash
# =============================================================================
# pipeline-lib.sh — shared orchestration for the pipeline harness scripts.
#
# Sourced (never executed directly) by holistic-measure.sh (and formerly
# loadtest-preview.sh, deleted 2026-09-05 with the early-signal rule it
# drove). Provides ONE battle-tested bring-up/teardown/metric path so gate
# scripts and measurement scripts cannot drift apart (decision 2026-08-30).
#
# Contract with the sourcing script:
#   - The source script MUST define, BEFORE sourcing this file:
#       ROOT                 project root (streaming_project_New/)
#       OUT                  evidence directory (created by lib preflight)
#       RATE_HZ              faketool rate (validated here)
#     and MAY override (defaults shown):
#       FAKETOOL_PORT=8899, ALLOW_FULL_REPLAY=false, WARMUP_S=45,
#       CHECKPOINT_TIMEOUT_MS=30000, DURATION_S=300
#   - Functions print diagnostics and `return 1` on failure — the caller
#     decides fail-fast (gate script) vs warn-and-continue (measurement).
#   - pipeline_install_cleanup_trap() installs the EXIT trap that removes
#     the loadgen containers and cancels the job. JOB_ID lives here; faketool
#     + ingestion are CONTAINERS (CHG-122) whose stdout a `docker logs -f`
#     mirror copies into the evidence dir (it exits with its container, so it
#     needs no handle).
#
# Bugs this lib exists to prevent (each observed live, 2026-08-29/30):
#   B1. `docker compose -f` without BOTH --env-file flags: required
#       interpolation vars (FLINK_IMAGE:? ...) hard-fail. The COMPOSE
#       variable here always carries the env files.
#   B2. Stale faketool holding :8899: preflight passed because the port
#       check had single quotes around $FAKETOOL_PORT (literal grep that
#       never matched). Double-quoted + anchored end here, PLUS a
#       liveness check after launch (a faketool that dies on bind leaves
#       a dead PID that must be caught immediately).
#   B3. TM direct-buffer accumulation across cancelled jobs → OOM at the
#       Fluss sinks. The TM is restarted before every run.
#   B4. Full-replay mode re-downloads tiered log segments (~80s each,
#       ~6 min cold) and stalls barriers past a 30s checkpoint timeout →
#       job FAILs. LATEST startup mode is the default here.
#   B6. Shading Fluss into compute.jar creates a second class universe. The
#       Flink image owns the connector/client; the pre-submit artifact guard
#       below rejects duplicate org.apache.fluss classes before a job can
#       reach RUNNING with a broken ServiceLoader (C2, 2026-09-01).
# =============================================================================

# Paths (derived from ROOT so callers only set ROOT + OUT)
LIB_JAR="$ROOT/code/02_services/02_compute/target/compute.jar"
LIB_ING_JAR="$ROOT/code/02_services/01_ingestion/target/ingestion.jar"
LIB_BRIDGE_DIR="$ROOT/code/02_services/01_ingestion/go-bridge"
LIB_MANIFEST="$ROOT/../../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv"
LIB_FAKETOOL_SRC="$LIB_BRIDGE_DIR/faketool/main.go"
FAKETOOL_PORT="${FAKETOOL_PORT:-8899}"
LIB_COMPOSE_FILE="$ROOT/code/01_platform/01_docker/docker-compose.yml"
# B1 guard: always carry both env files.
COMPOSE="docker compose -f $LIB_COMPOSE_FILE --env-file $ROOT/code/01_platform/01_docker/.env --env-file $ROOT/code/01_platform/01_docker/secrets.env"

# Preflight-state guard (2026-09-02): launch-phase functions require
# pipeline_preflight to have run — it populates CP,
# restarts the TM fresh, and waits for Fluss/TM registration. A caller that
# skips it (stage-a2-baseline.sh first attempt) previously got "unbound
# variable" deep inside ingestion and a broken JVM launch. Guarded functions
# now fail fast with a clear message instead.
PIPELINE_PREFLIGHT_OK=0
pipeline_require_preflight() {
  local caller="${FUNCNAME[1]:-unknown-caller}"
  [ "${PIPELINE_PREFLIGHT_OK:-0}" -eq 1 ] || {
    pipeline_fail "pipeline_preflight not run — refusing $caller (run pipeline_preflight first; it sets CP and restarts the TM)"
    return 1
  }
}

LIB_COMPOSE_DIR="$ROOT/code/01_platform/01_docker"
LIB_CP_FILE="$ROOT/code/02_services/01_ingestion/target/cp.txt"
FLUSS_COORDINATOR_CONTAINER="${FLUSS_COORDINATOR_CONTAINER:-01_docker-fluss-coordinator-1}"
# CHG-122 (2026-09-02): fully-containerized data path. faketool + the
# ingestion JVM run as Docker containers on the SAME network as Flink/Fluss
# — never as host processes. WHY the policy change: the old hybrid layout
# (host JVMs -> published ports -> NAT hairpin -> Fluss) made "is the
# environment clean?" a manual audit and let host state (leftover JVMs,
# host memory pressure) contaminate captures. Guard G26 in preflight fails
# a run if any host data-path process is found.
LIB_LOADGEN_IMAGE="${LIB_LOADGEN_IMAGE:-pipeline-loadgen:1.0.0}"
LIB_TRADING_NET="${LIB_TRADING_NET:-01_docker_trading-net}"
LIB_FAKETOOL_CONTAINER="pipeline-faketool"
LIB_INGESTION_CONTAINER="pipeline-ingestion"
FLINK_TM_CONTAINER="${FLINK_TM_CONTAINER:-01_docker-flink-taskmanager-1}"
FLUSS_TABLET_CONTAINER="${FLUSS_TABLET_CONTAINER:-01_docker-fluss-tablet-1}"
FLUSS_READY_TIMEOUT_S="${FLUSS_READY_TIMEOUT_S:-180}"
FLUSS_READY_TABLE="${FLUSS_READY_TABLE:-raw_table_1}"

# Run state (owned by the lib; teardown reads JOB_ID). CHG-122: the data path
# is containers; the `docker logs -f` mirrors die with theirs.
JOB_ID=""

pipeline_log() { echo "[pipeline $(date +%H:%M:%S)] $*"; }
pipeline_fail() { echo "[pipeline FATAL] $*" >&2; return 1; }

# ---------- compute artifact classpath guard (B6) ----------
# The Flink image is the single owner of org.apache.fluss.* at runtime. A
# shaded copy in compute.jar can make ServiceLoader load a provider (for
# example HdfsSecurityTokenReceiver) against a different SecurityTokenReceiver
# interface, producing the misleading "not a subtype" failure only when the
# source first touches tiered storage. Reject the artifact before copying or
# submitting it so the failure is local, deterministic, and actionable.
pipeline_validate_compute_jar() {
  command -v jar >/dev/null 2>&1 \
    || { pipeline_fail "JDK jar tool missing — cannot validate compute.jar classpath"; return 1; }
  jar tf "$LIB_JAR" >/dev/null 2>&1 \
    || { pipeline_fail "compute.jar is not a readable ZIP/JAR: $LIB_JAR"; return 1; }
  local fluss_classes fluss_services
  fluss_classes=$(jar tf "$LIB_JAR" 2>/dev/null \
    | awk '/^org\/apache\/fluss\/.*\.class$/ { n++ } END { print n + 0 }')
  [ "${fluss_classes:-0}" -eq 0 ] \
    || { pipeline_fail "compute.jar bundles $fluss_classes org.apache.fluss classes — Flink /opt/flink/lib must be the single Fluss classpath owner; rebuild with fluss dependencies provided"; return 1; }
  fluss_services=$(jar tf "$LIB_JAR" 2>/dev/null \
    | grep -c '^META-INF/services/org\.apache\.fluss\.' || true)
  [ "${fluss_services:-0}" -eq 0 ] \
    || { pipeline_fail "compute.jar bundles $fluss_services Fluss ServiceLoader descriptors — refusing a split Fluss runtime"; return 1; }
}

# ---------- Compose bind-source guard (B7) ----------
# Docker Compose creates a directory when a short-syntax bind source is
# missing. The later container start then fails with the opaque OCI error
# "not a directory" (observed 2026-09-01 after the plugin artifacts were
# temporarily absent). Keep the existing short syntax for compatibility, but
# refuse to run a measurement until every file-mounted source is a regular
# file. This turns a daemon-time failure into a local actionable failure.
pipeline_validate_compose_bind_sources() {
  local relative path
  for relative in \
    "flink-log4j-console.properties" \
    "alert-consumer.py" \
    "fluss-plugins/iceberg/fluss-flink-2.2-0.9.1-incubating.jar" \
    "fluss-plugins/iceberg/fluss-flink-tiering-0.9.1-incubating.jar" \
    "fluss-plugins/iceberg/fluss-lake-iceberg-0.9.1-incubating.jar" \
    "fluss-plugins/iceberg/fluss-fs-s3-0.9.1-incubating.jar" \
    "fluss-plugins/iceberg/fluss-fs-hdfs-0.9.1-incubating.jar" \
    "fluss-plugins/iceberg/fluss-fs-hadoop-shaded-0.9-SNAPSHOT.jar" \
    "fluss-plugins/iceberg/hadoop-mapreduce-compat-2.8.5.jar"; do
    path="$LIB_COMPOSE_DIR/$relative"
    [ -f "$path" ] \
      || { pipeline_fail "Compose bind source is missing or not a regular file: $path (short bind syntax otherwise creates a directory and the container fails with OCI exit 127)"; return 1; }
  done
}

# ---------- rate validation (mirrors loadtest-run.sh B1) ----------
pipeline_validate_rate() {
  local hz="$1"
  case "$hz" in
    ''|*[!0-9]*) echo "RATE_HZ='$hz' is not a positive integer; valid rates: 1,2,4,5,8,10,20,25,40,50,100,125,200,250,500,1000" >&2; return 1;;
  esac
  [ "$hz" -gt 0 ] || { echo "RATE_HZ must be a positive integer, got '$hz'" >&2; return 1; }
  [ $(( 1000 % 10#$hz )) -eq 0 ] || { echo "RATE_HZ=$hz is invalid: faketool -real-rate-hz must divide 1000" >&2; return 1; }
}

# ---------- port availability (B2 guard) ----------
pipeline_port_free() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    # Double quotes so $port expands; anchor the port at end of the
    # address column (ss prints *:8899 or 0.0.0.0:8899).
    if ss -tln 2>/dev/null | awk 'NR>1 {print $4}' | grep -q ":$port"'$'; then return 1; fi
    return 0
  fi
  if (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null; then exec 3>&-; return 1; fi
  return 0
}

# ---------- Fluss readiness (B8 guard) --------------------------------------
# A running coordinator is not enough to serve table metadata: after an
# unclean tablet shutdown the coordinator can accept connections while every
# table bucket is still leaderless.  TablePurge then fails with the misleading
# "Alive tablet server is empty".  Require both client listeners and both
# containers to be running before any table drop/create or job submission.
pipeline_fluss_port_open() {
  local host="$1" port="$2"
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null
}

pipeline_compile_fluss_ready_probe() {
  local probe="$OUT/FlussReadyProbe.java"
  cat > "$probe" <<'JAVAEOF'
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import java.util.concurrent.TimeUnit;

/** Read-only readiness probe: client metadata initialization requires a live tablet. */
public class FlussReadyProbe {
    public static void main(String[] args) throws Exception {
        String table = args.length > 0 ? args[0] : "raw_table_1";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:9123");
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {
            var info = admin.getTableInfo(TablePath.of("default", table))
                    .get(5, TimeUnit.SECONDS);
            if (info.getNumBuckets() <= 0) {
                throw new IllegalStateException("table has no buckets: " + table);
            }
            System.out.println("READY " + info.getTablePath());
        }
    }
}
JAVAEOF
  javac -cp "$CP" -d "$OUT" "$probe" > "$OUT/fluss-ready-compile.log" 2>&1 \
    || { pipeline_fail "Fluss readiness probe compilation failed — see $OUT/fluss-ready-compile.log"; return 1; }
}

pipeline_fluss_metadata_ready() {
  java --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    -cp "$OUT:$CP" FlussReadyProbe "$FLUSS_READY_TABLE" \
    > "$OUT/fluss-ready-probe.log" 2>&1
}

pipeline_wait_for_fluss_ready() {
  case "$FLUSS_READY_TIMEOUT_S" in
    ''|*[!0-9]*|0)
      pipeline_fail "FLUSS_READY_TIMEOUT_S='$FLUSS_READY_TIMEOUT_S' must be a positive integer"
      return 1
      ;;
  esac
  local i coord_state tablet_state
  for ((i=0; i<FLUSS_READY_TIMEOUT_S; i+=2)); do
    coord_state="$(docker inspect --format '{{.State.Status}}' "$FLUSS_COORDINATOR_CONTAINER" 2>/dev/null || true)"
    tablet_state="$(docker inspect --format '{{.State.Status}}' "$FLUSS_TABLET_CONTAINER" 2>/dev/null || true)"
    if [ "$coord_state" = "running" ] && [ "$tablet_state" = "running" ] \
        && pipeline_fluss_port_open 127.0.0.1 9123 \
        && pipeline_fluss_port_open 127.0.0.1 9124 \
        && pipeline_fluss_metadata_ready; then
      pipeline_log "Fluss ready (coordinator/tablet listeners + metadata probe; waited ${i}s)"
      return 0
    fi
    if (( i == 0 || i % 20 == 0 )); then
      pipeline_log "waiting for Fluss tablet readiness (coordinator=$coord_state tablet=$tablet_state, elapsed=${i}s)"
    fi
    sleep 2
  done
  coord_state="$(docker inspect --format '{{.State.Status}}' "$FLUSS_COORDINATOR_CONTAINER" 2>/dev/null || echo missing)"
  tablet_state="$(docker inspect --format '{{.State.Status}}' "$FLUSS_TABLET_CONTAINER" 2>/dev/null || echo missing)"
  pipeline_fail "Fluss did not become ready within ${FLUSS_READY_TIMEOUT_S}s (coordinator=$coord_state tablet=$tablet_state; see $OUT/fluss-ready-probe.log; inspect tablet logs and run code/01_platform/04_scripts/fluss-repair/repair-tablet.sh for a crash-looping tablet)"
  return 1
}

# ---------- G27: loadgen image verification (2026-09-02, user request) ----------
# Every run MUST use the containerized loadgen image AND that image must
# have passed its checks. Three failure classes, each with the WHY:
#   (a) wrong/foreign image retagged as pipeline-loadgen:1.0.0 — the
#       binaries are missing inside; docker run would die at launch
#   (b) stale image: build-input sources (go-bridge, ingestion src, poms,
#       Dockerfile.loadgen) newer than the image — the run would measure
#       OLD code and produce a bogus baseline
#   (c) guards red: test-pipeline-lib.sh failing — running on a known-bad
#       harness contradicts the every-fix-pinned-by-a-guard policy
# G27b helper: sha256 over EVERY build input of the loadgen image (the
# exact set the Dockerfile COPYs). Sorted file list -> per-file sha ->
# final digest, so any content change flips the stamp.
pipeline_loadgen_input_stamp() {
  { # everything the Dockerfile COPYs from go-bridge (go/mod/sum, *.go,
    # marketdata, vendored third_party) EXCEPT the host-built arrow-bridge
    # binary and test binaries — they are build OUTPUTS on the host, not
    # image inputs, and would flip the stamp spuriously.
    find "$ROOT/code/02_services/01_ingestion/go-bridge" \
        -type f ! -name 'arrow-bridge' ! -name '*.test' -print 2>/dev/null
    find "$ROOT/code/02_services/01_ingestion/src" -type f -print 2>/dev/null
    printf '%s\n' \
      "$ROOT/code/02_services/01_ingestion/Dockerfile.loadgen" \
      "$ROOT/code/02_services/01_ingestion/pom.xml" \
      "$ROOT/code/02_services/06_execution_gateway/pom.xml"
    find "$ROOT/code/common" -type f -name '*.java' -print 2>/dev/null
  } | sort | grep -v '^$' | xargs -r sha256sum 2>/dev/null \
    | sha256sum | cut -d' ' -f1
}

pipeline_verify_loadgen_image() {
  # (a) contents: all three artifacts present + executables runnable
  if ! docker run --rm --network none "$LIB_LOADGEN_IMAGE" \
      sh -c 'test -x /app/faketool && test -x /app/arrow-bridge && test -f /app/ingestion.jar' \
      >/dev/null 2>&1; then
    pipeline_fail "G27a: loadgen image $LIB_LOADGEN_IMAGE is missing its artifacts (/app/faketool, /app/arrow-bridge, /app/ingestion.jar) — a foreign or corrupt image was tagged with this name. Rebuild honestly: $COMPOSE build loadgen (and check no other image stole the tag: docker images pipeline-loadgen)"
    return 1
  fi
  # (b) freshness: content-addressed. The image carries a BUILD_STAMP
  # (sha256 of all build inputs, baked in at build time via compose build
  # arg). Recompute now and compare — mismatch = the image was built from
  # DIFFERENT sources than the tree (bogus baseline). Content-based on
  # purpose: the mtime first draft false-positived on git checkout/touch
  # (proven live 2026-09-02) and a fully-cached rebuild never bumps the
  # image Created time, so it could never clear again.
  local stamp_in_image
  stamp_in_image="$(docker run --rm --network none "$LIB_LOADGEN_IMAGE" cat /app/build-stamp 2>/dev/null | tr -d '[:space:]')"
  if [ -z "$stamp_in_image" ] || [ "$stamp_in_image" != "$LIB_LOADGEN_STAMP" ]; then
    pipeline_fail "G27b: loadgen image $LIB_LOADGEN_IMAGE is STALE — its build stamp ($stamp_in_image) does not match the current sources ($LIB_LOADGEN_STAMP). The run would measure OLD code (bogus baseline). Rebuild: $COMPOSE build loadgen (with LOADGEN_BUILD_STAMP exported — pipeline_preflight does this for you)"
    return 1
  fi
  # (c) harness guards green: the run script's own guard suite must pass.
  if ! bash "$ROOT/code/01_platform/04_scripts/test-pipeline-lib.sh" >"$OUT/g27-guard-run.log" 2>&1; then
    pipeline_fail "G27c: test-pipeline-lib.sh FAILED — refusing to run on a red harness (every fix is pinned by a guard; a red suite means a known-broken state). See $OUT/g27-guard-run.log; run it yourself, fix the FAIL lines, then re-run"
    return 1
  fi
  pipeline_log "G27 OK: image $LIB_LOADGEN_IMAGE verified (artifacts present, no stale build inputs, guard suite green)"
}

# ---------- preflight ----------
# Checks artifacts, kills stray faketools, verifies the port is free,
# restarts the TM (B3), resolves the 1024-token set, creates $OUT.
# On success sets: CP, TOKENS (caller-readable).
pipeline_preflight() {
  PIPELINE_PREFLIGHT_OK=0
  mkdir -p "$OUT" "$OUT/bin" "$OUT/j1"
  [ -f "$LIB_JAR" ] || { pipeline_fail "compute jar missing: $LIB_JAR (run: cd code/02_services/02_compute && mvn package)"; return 1; }
  pipeline_validate_compute_jar || return 1
  pipeline_validate_compose_bind_sources || return 1
  [ -f "$LIB_ING_JAR" ] || { pipeline_fail "ingestion jar missing: $LIB_ING_JAR"; return 1; }
  [ -f "$LIB_CP_FILE" ] || { pipeline_fail "ingestion classpath file missing: $LIB_CP_FILE (run mvn package in 01_ingestion)"; return 1; }
  [ -f "$LIB_BRIDGE_DIR/arrow-bridge" ] || { pipeline_fail "arrow-bridge binary missing: $LIB_BRIDGE_DIR/arrow-bridge"; return 1; }
  # faketool source is a BUILD INPUT of the loadgen image (Dockerfile.loadgen)
  [ -f "$LIB_FAKETOOL_SRC" ] || { pipeline_fail "faketool source missing: $LIB_FAKETOOL_SRC (build input of Dockerfile.loadgen - the loadgen image cannot be built without it)"; return 1; }
  [ -f "$LIB_MANIFEST" ] || { pipeline_fail "manifest CSV missing: $LIB_MANIFEST"; return 1; }
  pipeline_validate_rate "$RATE_HZ" || return 1
  pipeline_port_free "$FAKETOOL_PORT" || { pipeline_fail "port $FAKETOOL_PORT already in use — stale faketool/broker running (kill it or set FAKETOOL_PORT)"; return 1; }

  # G26 (2026-09-02, CHG-122): host-isolation policy guard. The load-test
  # data path (faketool, arrow-bridge, ingestion JVM) MUST run inside
  # Docker. The old hybrid layout ran them as host processes; orphans
  # (observed after SIGKILLed runners) re-appended to the raw table and
  # poisoned baselines, and host state was unauditable. POLICY: no host
  # data-path processes, ever. This is fail-fast, NOT auto-kill: a policy
  # violation must be investigated, not papered over mid-run.
  # Stale loadgen containers from a crashed prior run would collide with
  # our docker run names and mix old/new feeds — fail with the reason.
  local stale_ct
  stale_ct="$(docker ps -a --format '{{.Names}}' 2>/dev/null | grep -E '^(pipeline-faketool|pipeline-ingestion)$' || true)"
  [ -z "$stale_ct" ] || {
    pipeline_fail "G26: stale loadgen container(s) from a prior run exist: $(echo $stale_ct | tr '\n' ' ') — remove them first: docker rm -f pipeline-faketool pipeline-ingestion (they are leftovers of a crashed run; a name collision would abort our docker run mid-preflight)"
    return 1
  }
  local stray p
  stray=""
  # pgrep sees CONTAINER JVMs through the host /proc (same kernel) — a
  # container-owned PID (docker cgroup) is NOT a host process. Filter by
  # cgroup: only PIDs OUTSIDE any docker*.scope cgroup are true host
  # data-path processes. (False-positive observed 2026-09-02 post-reboot:
  # the compose ingestion service's JVM tripped G26 and blocked the run.)
  for p in $(pgrep -x faketool; pgrep -x arrow-bridge; pgrep -f 'com.trading.ingestion.IngestionService'; true); do
    if ! grep -qs 'docker-' "/proc/$p/cgroup" 2>/dev/null; then
      stray="$stray $p"
    fi
  done
  stray="$(echo "$stray" | tr -s '[:space:]' ' ' | sed 's/^ //;s/ $//')"
  [ -z "$stray" ] || {
    pipeline_fail "G26: host data-path process(es) running (pids: $stray) — POLICY VIOLATION (CHG-122: pipeline must be fully Docker-based). A host faketool/arrow-bridge/ingestion JVM bypasses the container network and can poison the baseline (e.g. re-append to the raw table). Find what harness started them, kill them (kill -9 $stray), then re-run. NOTE: container JVMs are host-visible via /proc - if `docker ps -a` shows pipeline-faketool/pipeline-ingestion, that is a stale-container case, remove those first"
    return 1
  }

  # Fluss must be reachable (tables applied)
  docker exec "$FLUSS_COORDINATOR_CONTAINER" sh -c 'exit 0' 2>/dev/null \
    || { pipeline_fail "fluss-coordinator container not up — run make up first"; return 1; }
  CP="$(cat "$LIB_CP_FILE")"
  [ -n "$CP" ] || { pipeline_fail "empty classpath from $LIB_CP_FILE"; return 1; }
  pipeline_compile_fluss_ready_probe || return 1
  pipeline_wait_for_fluss_ready || return 1
  # B3 guard: fresh TM for every run.
  pipeline_log "restarting flink-taskmanager for direct-buffer hygiene..."
  $COMPOSE restart flink-taskmanager >/dev/null 2>&1 \
    || { pipeline_fail "flink-taskmanager restart failed"; return 1; }
  sleep 12
  # B5 guard (2026-08-30): wait until the TM is REGISTERED with the JM before
  # allowing job submit. A container "running" is not enough — observed job
  # f074d765 submitted while Registered TMs: 0 → NoResourceAvailableException
  # → job RESTARTING → phase aborted. Poll /taskmanagers for up to 60s.
  # NOTE: /taskmanagers returns {"taskmanagers":[...]} — parse JSON and assert
  # on a non-empty array (the first pass grepped a nonexistent numRegisteredTMs
  # field and a later pass was whitespace-sensitive).
  local tm_ok=0
  for _ in $(seq 1 30); do
    if curl -fsS --max-time 3 http://localhost:8081/taskmanagers 2>/dev/null \
        | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get("taskmanagers") else 1)' \
        >/dev/null 2>&1; then
      tm_ok=1; break
    fi
    sleep 2
  done
  [ "$tm_ok" -eq 1 ] \
    || { pipeline_fail "TM not registered with JM after 60s — refusing to submit job"; return 1; }

  # G23 (2026-09-02): TM config + slots fail-fast. Two silent failure
  # classes observed 2026-09-02, both AFTER preflight and mid-run:
  #   (a) the FLINK_PROPERTIES prefix collision silently dropped
  #       state.backend.rocksdb.localdir from the generated config.yaml ->
  #       RocksDB back on the container overlay (the CHG-120 ~1.7k/s
  #       degradation class, invisible until throughput analysis).
  #   (b) `docker compose restart` reuses the OLD container's config — a
  #       compose edit (slots 10->16) never reached the running TM and the
  #       p16 job died on "unassigned resource" at t+13s.
  # Both are caught here, BEFORE any feed or job submit, with the reason.
  pipeline_verify_tm_config || return 1

  # G26 (CHG-122): loadgen containers must share the TM's network so the
  # ingestion JVM reaches Fluss by service name (fluss-coordinator:9123)
  # and faketool by container name — no published ports, no NAT hairpin.
  local tm_net
  tm_net="$(docker inspect --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$FLINK_TM_CONTAINER" 2>/dev/null | awk '{print $1}')"
  [ "$tm_net" = "$LIB_TRADING_NET" ] || {
    pipeline_fail "G26: flink-taskmanager is on network '$tm_net' but LIB_TRADING_NET=$LIB_TRADING_NET — the loadgen containers would land on a DIFFERENT network and the ingestion JVM could not reach Fluss by service name. WHY this matters: silent cross-network DNS failure = zero appends (observed 2026-09-02 run -192421). Fix: LIB_TRADING_NET='$tm_net' (or point it at the compose trading-net)"
    return 1
  }
  # CHG-122: build (cached) the loadgen image here so a broken Dockerfile
  # fails preflight with the reason, not mid-run. G27b: the current
  # sources' stamp is passed as a build arg so the image records what it
  # was built from (content-addressed freshness, checked right after).
  LIB_LOADGEN_STAMP="$(pipeline_loadgen_input_stamp)"
  export LOADGEN_BUILD_STAMP="$LIB_LOADGEN_STAMP"
  pipeline_log "building loadgen image (cached): $LIB_LOADGEN_IMAGE (stamp ${LIB_LOADGEN_STAMP:0:12}...)"
  $COMPOSE build loadgen >"$OUT/loadgen-build.log" 2>&1 \
    || { pipeline_fail "G26: loadgen image build failed — see $OUT/loadgen-build.log (Dockerfile.loadgen at code/02_services/01_ingestion/; build with: $COMPOSE build loadgen)"; return 1; }
  docker image inspect "$LIB_LOADGEN_IMAGE" --format '{{.Id}}' >/dev/null 2>&1 \
    || { pipeline_fail "G26: loadgen image $LIB_LOADGEN_IMAGE not present after build — the compose service tagged a different image name; check the `image:` key of the loadgen service in docker-compose.yml"; return 1; }
  # G27 (2026-09-02): the image itself must be verified before ANY run
  # uses it — artifacts present, not stale, harness guards green.
  pipeline_verify_loadgen_image || return 1

  local mtok
  mtok=$(tail -n +2 "$LIB_MANIFEST" | wc -l)
  [ "$mtok" -ge 1024 ] || { pipeline_fail "manifest has only $mtok tokens; need >=1024"; return 1; }
  # G3 single source of truth (2026-08-31): the manifest SLICE handed to
  # Java (INSTRUMENT_MANIFEST_PATH) is the ONE token set for the run — the
  # bridge receives it from Java via the child-env handoff (startBridge
  # overwrites ARROW_INSTRUMENT_TOKENS with the loaded set), so the old
  # TOKENS env plumbing (and the 1,024-vs-2,431 skew that tripped the
  # fingerprint cross-check on every bridge event) is gone.
  local slice
  slice="$OUT/instruments-1024.csv"
  head -1025 "$LIB_MANIFEST" > "$slice"
  local ntok
  ntok=$(tail -n +2 "$slice" | grep -c .)
  [ "$ntok" -eq 1024 ] || { pipeline_fail "expected exactly 1024 tokens in slice, got $ntok"; return 1; }
  # shellcheck disable=SC2034  # deprecated but kept: callers still reference it
  TOKENS=""   # deprecated: kept as empty for callers that still reference it

  mkdir -p "$OUT" "$OUT/bin" "$OUT/j1"
  PIPELINE_PREFLIGHT_OK=1
  pipeline_log "preflight OK (jars, bridge, manifest, port $FAKETOOL_PORT free, fluss up, TM fresh)"
}

# G23 (2026-09-02): verify the TM's GENERATED config and REGISTERED slots.
# Called from pipeline_preflight after the TM-registration wait. Fails fast
# (pipeline_fail carries the WHY + the exact fix command) on:
#   - config.yaml unreadable / poisoned with '[ERROR]' (entrypoint merge
#     failed - FLINK_PROPERTIES key collision; see G22 /
#     check_flink_properties.py)
#   - localdir / incremental missing from the generated config (silent
#     drop by the prefix collision; RocksDB would run on the overlay)
#   - configured or registered slots < PARALLELISM (stale container: a
#     compose edit needs a RECREATE, not a restart)
pipeline_verify_tm_config() {
  local par="${PARALLELISM:-8}"
  local recreate_hint="recreate the containers (a plain 'docker compose restart' reuses the OLD config): cd code/01_platform/01_docker && docker compose --env-file .env --env-file secrets.env up -d --force-recreate flink-jobmanager flink-taskmanager"

  local cfg
  cfg="$($COMPOSE exec -T flink-taskmanager cat /opt/flink/conf/config.yaml 2>/dev/null || true)"
  [ -n "$cfg" ] || { pipeline_fail "G23: cannot read flink-taskmanager /opt/flink/conf/config.yaml - is the container up (or crash-looping? check: docker logs 01_docker-flink-taskmanager-1)"; return 1; }
  if printf '%s\n' "$cfg" | head -1 | grep -q "ERROR"; then
    pipeline_fail "G23: TM config.yaml starts with '[ERROR]' - the docker-entrypoint config merge FAILED (FLINK_PROPERTIES key collision; see code/01_platform/04_scripts/check_flink_properties.py, guard G22). The TM will crash-loop. Fix the compose file, then $recreate_hint"
    return 1
  fi
  printf '%s\n' "$cfg" | grep -q "localdir" \
    || { pipeline_fail "G23: TM config.yaml is missing state.backend.rocksdb.localdir - the FLINK_PROPERTIES merge silently dropped it (prefix collision; RocksDB would run on the container overlay = the CHG-120 ~1.7k/s degradation). See check_flink_properties.py (G22). Fix the compose file, then $recreate_hint"; return 1; }
  printf '%s\n' "$cfg" | grep -q "incremental" \
    || { pipeline_fail "G23: TM config.yaml is missing state.backend.incremental - the FLINK_PROPERTIES merge silently dropped it (prefix collision; checkpoints silently lose incremental mode). See check_flink_properties.py (G22). Fix the compose file, then $recreate_hint"; return 1; }

  local slots
  slots="$(printf '%s\n' "$cfg" | grep -oE "numberOfTaskSlots: ?'?[0-9]+" | grep -oE "[0-9]+" | head -1)"
  if [ -z "$slots" ] || [ "$slots" -lt "$par" ]; then
    pipeline_fail "G23: TM config.yaml has numberOfTaskSlots=${slots:-MISSING} but the job needs PARALLELISM=$par - the task would fail on 'unassigned resource'. WHY: a compose edit does NOT reach a restarted container. $recreate_hint"
    return 1
  fi

  local reg
  reg="$(curl -fsS --max-time 5 http://localhost:8081/taskmanagers 2>/dev/null \
    | python3 -c 'import json,sys
tms = json.load(sys.stdin).get("taskmanagers", [])
print(max((t.get("slotsNumber", 0) for t in tms), default=0))' 2>/dev/null || echo 0)"
  if [ "${reg:-0}" -lt "$par" ]; then
    pipeline_fail "G23: TM registered only ${reg:-0} slots with the JM but the job needs PARALLELISM=$par - submit would fail on 'unassigned resource' (observed 2026-09-02: compose edit to 16 slots, TM still registered 10). $recreate_hint"
    return 1
  fi
  pipeline_log "G23: TM config verified (localdir+incremental present, slots ${slots} >= PARALLELISM ${par}, registered ${reg})"
  return 0
}

# ---------- faketool ----------
# CHG-122 (2026-09-02): faketool runs as a CONTAINER on trading-net (was a
# host process talking through published ports). The binary is built into
# the loadgen image (Dockerfile.loadgen) — no host `go build` here anymore.
# B2 guard kept: after launch, verifies the container is STILL RUNNING —
# an early exit (bad flag, missing binary) must abort now, not mid-run.
pipeline_start_faketool() {
  pipeline_require_preflight || return 1
  local inject_args=()
  # F2/F3 audit injection (2026-08-30): optional, from INJECT_* env vars.
  # See faketool main.go -inject-* flags for semantics.
  if [ "${INJECT_AFTER_MS:-0}" -gt 0 ]; then
    inject_args=(
      -inject-after-ms "${INJECT_AFTER_MS}ms"
      -inject-dups "${INJECT_DUPS:-0}"
      -inject-late "${INJECT_LATE:-0}"
      -inject-late-ms "${INJECT_LATE_MS:-90000}"
    )
    [ "${INJECT_EVERY_MS:-0}" -gt 0 ] \
      && inject_args+=(-inject-every-ms "${INJECT_EVERY_MS}ms")
    [ "${INJECT_MAX_ROUNDS:-0}" -gt 0 ] \
      && inject_args+=(-inject-max-rounds "${INJECT_MAX_ROUNDS}")
    pipeline_log "injection enabled: after=${INJECT_AFTER_MS}ms dups=${INJECT_DUPS:-0} late=${INJECT_LATE:-0} every=${INJECT_EVERY_MS:-0}ms"
  fi
  docker run -d --name "$LIB_FAKETOOL_CONTAINER" \
    --network "$LIB_TRADING_NET" \
    "$LIB_LOADGEN_IMAGE" \
    /app/faketool -port "$FAKETOOL_PORT" -real-rate -real-rate-hz "$RATE_HZ" \
    "${inject_args[@]}" \
    || { pipeline_fail "faketool container failed to start — check: docker logs $LIB_FAKETOOL_CONTAINER (image: $LIB_LOADGEN_IMAGE, network: $LIB_TRADING_NET)"; return 1; }
  # Mirror container stdout into the evidence dir continuously; `docker
  # logs -f` exits when the container is removed at cleanup.
  docker logs -f "$LIB_FAKETOOL_CONTAINER" > "$OUT/faketool.log" 2>&1 &

  # Readiness: faketool prints real_rate=true once serving. Poll the
  # mirrored log + container liveness (B2: alive after start).
  local ready=0 i running
  for i in $(seq 1 15); do
    running="$(docker inspect --format '{{.State.Running}}' "$LIB_FAKETOOL_CONTAINER" 2>/dev/null || echo false)"
    [ "$running" = "true" ] || break
    if grep -q "real_rate=true" "$OUT/faketool.log" 2>/dev/null; then ready=1; break; fi
    sleep 1
  done
  if [ "$ready" != 1 ]; then
    pipeline_fail "faketool container not ready in 15s (running=$running) — log tail (full log: $OUT/faketool.log):"
    tail -5 "$OUT/faketool.log" >&2 || true
    return 1
  fi
  pipeline_log "faketool container $LIB_FAKETOOL_CONTAINER on :$FAKETOOL_PORT (${RATE_HZ}Hz x 1024 = $((RATE_HZ * 1024))/s), network $LIB_TRADING_NET, real-rate confirmed"
}

# ---------- ingestion JVM ----------
# CHG-122 (2026-09-02): the ingestion JVM (+ its arrow-bridge child) runs as
# a CONTAINER on trading-net. Canonical env block kept 1:1 from the host
# launch EXCEPT the three endpoint values, which now use in-network names:
#   ARROW_HFT_URL   ws://127.0.0.1:8899        -> ws://pipeline-faketool:8899
#   FLUSS_BOOTSTRAP localhost:9123 (NAT)       -> fluss-coordinator:9123
#   OTEL collector  localhost:4319 (was dead)  -> otel-collector:4318
# $OUT is bind-mounted at /run (manifest slice + readiness marker) and
# $OUT/j1 at /logs (gc.log, JSON logs) so every evidence file lands in the
# same place as before. stdout is mirrored to j1/java.out (stage-capture
# parses it for ingestion.tsv).
pipeline_start_ingestion() {
  pipeline_require_preflight || return 1
  # Fail-closed readiness: an interrupted prior run can leave the readiness
  # marker behind. Remove it before starting; otherwise the first poll
  # below can accept a dead process as ready (observed 2026-08-31).
  rm -f "$OUT/ingestion.loadtest.ready"
  docker run -d --name "$LIB_INGESTION_CONTAINER" \
    --network "$LIB_TRADING_NET" \
    -v "$OUT":/run -v "$OUT/j1":/logs \
    -e LOG_DIR=/logs \
    -e READINESS_FILE_PATH=/run/ingestion.loadtest.ready \
    -e ARROW_HFT_URL="ws://$LIB_FAKETOOL_CONTAINER:$FAKETOOL_PORT" \
    -e ARROW_BRIDGE_BIN=/app/arrow-bridge \
    -e ARROW_FAKE_BROKER=1 \
    -e TRANSPORT=proto \
    -e SECRETS_VIA_ENV_FILE=1 \
    -e ARROW_APP_ID=testd -e ARROW_APP_SECRET=testd \
    -e ARROW_USER_ID=testd-user -e ARROW_PASSWORD=testd-pass \
    -e ARROW_TOTP_KEY=JBSWY3DPEHPK3PXP \
    -e INSTRUMENT_MANIFEST_PATH=/run/instruments-1024.csv \
    -e FLUSS_BOOTSTRAP=fluss-coordinator:9123 \
    -e FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 \
    -e RAW_TABLE_NAME=raw_table_1 \
    -e ARROW_HFT_CONNECTIONS=1 \
    -e ARROW_MAX_EVENT_AGE_MS="${ARROW_MAX_EVENT_AGE_MS:-5000}" \
    -e ARROW_MAX_FUTURE_EVENT_SKEW_MS=2000 \
    -e ARROW_HFT_LATENCY_MS=50 \
    -e CLOCK_CHECK_REQUIRED=false \
    -e OTEL_COLLECTOR_HOST=otel-collector:4318 \
    -e FLUSS_WRITER_MODE=generic -e FLUSS_WRITERS=1 -e FLUSS_WRITER_BATCH_SIZE_BYTES=0 \
    "$LIB_LOADGEN_IMAGE" \
    java --add-opens=java.base/java.nio=ALL-UNNAMED \
      -Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m \
      -Xlog:gc*,safepoint:file=/logs/gc.log:time,uptime,level,tags \
      -Dlog.dir=/logs \
      -cp /app/ingestion.jar com.trading.ingestion.IngestionService \
    || { pipeline_fail "ingestion container failed to start — check: docker logs $LIB_INGESTION_CONTAINER (image: $LIB_LOADGEN_IMAGE, network: $LIB_TRADING_NET)"; return 1; }
  # Mirror stdout (OTLP feed->ack payloads) — stage-capture parses this
  # file for ingestion.tsv. Dies with the container at cleanup.
  docker logs -f "$LIB_INGESTION_CONTAINER" > "$OUT/j1/java.out" 2>&1 &

  # Readiness = marker file via the /run bind mount + bridge subscription.
  local ready=0 i running
  for i in $(seq 1 60); do
    running="$(docker inspect --format '{{.State.Running}}' "$LIB_INGESTION_CONTAINER" 2>/dev/null || echo false)"
    [ "$running" = "true" ] || break
    [ -f "$OUT/ingestion.loadtest.ready" ] && { ready=1; break; }
    sleep 2
  done
  if [ "$ready" != 1 ]; then
    pipeline_fail "ingestion container not ready in 120s (running=$running) — log tail (full log: $OUT/j1/java.out):"
    tail -10 "$OUT/j1/java.out" >&2 || true
    return 1
  fi
  for i in $(seq 1 30); do grep -q "HFT subscribed" "$OUT/j1/java.out" 2>/dev/null && break; sleep 1; done
  grep -q "HFT subscribed" "$OUT/j1/java.out" 2>/dev/null \
    || { pipeline_fail "bridge never subscribed — log tail (full log: $OUT/j1/java.out):"; tail -10 "$OUT/j1/java.out" >&2; return 1; }
  pipeline_log "ingestion container $LIB_INGESTION_CONTAINER ready + bridge subscribed (1024 tokens)"
}

# ---------- SignalJob submit ----------
# Deploys the jar and submits with the canonical env recipe. Sets JOB_ID.
# Knobs the caller may inject via the environment:
#   ALLOW_FULL_REPLAY (default false — B4 guard: LATEST startup mode),
#   CHECKPOINT_TIMEOUT_MS (default 30000).
# Purge the preview KV table (drop + recreate from its DDL) at RUN START.
# 2026-08-30 (tiering-off verify run): Fluss TTL is CALENDAR-DAY based, so
# the preview table's "60s TTL" never expires same-day data. Across many
# bench runs it accumulated 11.6M live keys → preview upserts slowed
# progressively (volume collapsed 722k → 63k → 15k → 0 across four runs)
# and the analyzer's from-earliest read (30s budget) couldn't reach the run
# window. Dropping + recreating before each run resets both. Preview data
# is transient diagnostics — nothing else consumes it between runs.
pipeline_purge_table() {
  pipeline_require_preflight || return 1
  # Generic drop+recreate from a DDL file. Used to bound what a fresh job
  # replays (the SignalJob source reads from EARLIEST — an unpurged table
  # means every phase replays all prior phases' rows) and to keep the
  # from-earliest evidence reads bounded (raw grows ~6M rows per 10-min
  # phase; reading all history would blow the analyzer's memory).
  local ddl_file="$1" label="$2"
  pipeline_log "purging $label table (drop + recreate)"
  cat > /tmp/TablePurge.java <<'JAVAEOF'
import com.trading.common.schema.ddl.DdlText;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class TablePurge {
    public static void main(String[] args) throws Exception {
        String ddl = Files.readString(Path.of(args[0]));
        DdlText.ParsedDdl parsed = DdlText.parse(ddl, args[0]);
        TablePath tp = TablePath.of("default", parsed.tableName());
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:9123");
        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin()) {
            try {
                admin.dropTable(tp, false).get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.out.println("drop skipped: " + e.getMessage());
            }
            admin.createTable(tp, DdlText.toDescriptor(parsed), false)
                    .get(60, TimeUnit.SECONDS);
            System.out.println("PURGED " + tp);
        }
    }
}
JAVAEOF
  local purge_out
  purge_out="$(cd /tmp && javac -cp "$CP" -d /tmp TablePurge.java 2>&1 \
      && java --add-opens=java.base/java.lang=ALL-UNNAMED \
       --add-opens=java.base/java.nio=ALL-UNNAMED \
       -cp "/tmp:$CP" TablePurge "$ddl_file" 2>&1)" || true
  rm -f /tmp/TablePurge.java /tmp/TablePurge.class
  if echo "$purge_out" | grep -q "PURGED"; then
    pipeline_log "$label table purged"
  else
    # Normal measurement runs keep the historical non-fatal behavior: the
    # smoke gate will catch a failed CREATE. Fault drills can set
    # PURGE_STRICT=true because stale rows make their reconciliation result
    # invalid rather than merely reducing measurement quality.
    pipeline_log "WARN: $label purge output: $(echo "$purge_out" | tail -2)"
    if [ "${PURGE_STRICT:-false}" = "true" ]; then
      pipeline_fail "$label purge did not report PURGED — refusing to run a fault drill with stale data"
      return 1
    fi
  fi
}

pipeline_purge_raw_table() {
  # Raw must be purged BEFORE pipeline_start_ingestion (the ingestion JVM writes it).
  pipeline_purge_table "$ROOT/code/01_platform/02_sql/ddl/02_raw_table_1.sql" raw
}

# Ensure the multi-timeframe candle tables exist (cutover: the strategy host
# reads candle_live/candle_closed, DDLs 32/33). Create-if-absent, never drop.
# Uses the same create-if-absent TableEnsure pattern as pipeline_purge_table's
# drop+recreate.
pipeline_ensure_candle_tables() {
  pipeline_require_preflight || return 1
  local ddl_file="$1" label="$2"
  pipeline_log "ensuring $label table exists (create-if-absent)"
  cat > /tmp/TableEnsureCandle.java <<'JAVAEOF'
import com.trading.common.schema.ddl.DdlText;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class TableEnsureCandle {
    public static void main(String[] args) throws Exception {
        String ddl = Files.readString(Path.of(args[0]));
        DdlText.ParsedDdl parsed = DdlText.parse(ddl, args[0]);
        TablePath tp = TablePath.of("default", parsed.tableName());
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:9123");
        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin()) {
            try {
                admin.getTableInfo(tp).get(10, TimeUnit.SECONDS);
                System.out.println("EXISTS " + tp);
            } catch (Exception notFound) {
                admin.createTable(tp, DdlText.toDescriptor(parsed), false)
                        .get(60, TimeUnit.SECONDS);
                System.out.println("CREATED " + tp);
            }
        }
    }
}
JAVAEOF
  local ensure_out
  ensure_out="$(cd /tmp && javac -cp "$CP" -d /tmp TableEnsureCandle.java 2>&1 \
      && java --add-opens=java.base/java.lang=ALL-UNNAMED \
       --add-opens=java.base/java.nio=ALL-UNNAMED \
       -cp "/tmp:$CP" TableEnsureCandle "$ddl_file" 2>&1)" || true
  rm -f /tmp/TableEnsureCandle.java /tmp/TableEnsureCandle.class
  if echo "$ensure_out" | grep -qE "EXISTS|CREATED"; then
    pipeline_log "$label table ready ($(echo "$ensure_out" | grep -oE 'EXISTS|CREATED'))"
  else
    pipeline_fail "$label table ensure failed: $(echo "$ensure_out" | tail -2)"
    return 1
  fi
}

pipeline_submit_job() {
  pipeline_require_preflight || return 1
  pipeline_log "deploying SignalJob (previews 1s, early signals on, confirm-after 4s)"
  docker exec 01_docker-flink-jobmanager-1 mkdir -p /opt/flink/jobs 2>/dev/null \
    || { pipeline_fail "mkdir /opt/flink/jobs in flink-jobmanager failed"; return 1; }
  docker cp "$LIB_JAR" 01_docker-flink-jobmanager-1:/opt/flink/jobs/compute.jar \
    || { pipeline_fail "jar copy to flink-jobmanager failed"; return 1; }
  local submit_out
  # Single-timeline rule (2026-08-30): WATERMARK_OUT_OF_ORDER_MS is passed
  # explicitly so every run log shows the active value; default 500ms
  # matches the code default. NOTE: keep comments OUT of the continued
  # command — a '#' line between backslash continuations broke the whole
  # submit ("requires at least 2 args", observed 2026-08-30).
  # B5 Phase-1 (2026-08-31): UNALIGNED_CHECKPOINTS=true appends the
  # unaligned-checkpoint flag to the submit (falsification test — the
  # backpressure audit predicts no effect, but a negative result closes
  # the lever with evidence). Default OFF: baseline behavior unchanged.
  local extra_flags=(-Dmetrics.latency.interval="${LATENCY_TRACKING_MS:-2000}")
  # Tolerable checkpoint failures (2026-09-02): a TM-kill drill's catch-up
  # replay backpressures the pipeline and a checkpoint can EXPIRE (barriers
  # crawl; observed checkpoint 17 queue-sat 20s then expired → with the
  # default tolerance 0, Flink escalated ONE expired checkpoint to a global
  # failure and restarted all 152 tasks at t+75s — drills 20260902-005900
  # and 20260902-025251 both failed this way, flaky by replay timing).
  # A transient checkpoint miss is benign here: the NEXT checkpoint
  # completes (wait_new_checkpoint proves it) and G7c verifies end-state
  # data integrity. 3 tolerated = bounded, still fails fast on persistent
  # checkpoint breakage.
  extra_flags+=(-Dexecution.checkpointing.tolerable-failed-checkpoints="${TOLERABLE_FAILED_CHECKPOINTS:-3}")
  # C2 20k bottleneck (2026-09-02) - opt-in only, NO benefit measured:
  # RocksDB write-path tuning via JOB-LEVEL -D flags (writebuffer.count,
  # write-buffer-ratio, threads.write) for the fingerprint-dedup hot
  # operator. 2026-09-02 falsification series at 20Hz (captures
  # 20260902-175855/183643/184626/185513): tuned knobs 5.4k/s, default
  # values 3.9k/s, NO flags 3.4k/s - versus the 17:01 baseline of 19.85k/s
  # with no flags. The decline is MONOTONIC PER-RUN and independent of
  # these flags (root cause under investigation: cumulative per-run
  # degradation; see the plan evolution log). The flags showed no benefit
  # in any configuration, so they stay opt-in OFF by default; ROCKSDB_TUNING
  # requires all three knobs explicitly (partial sets are untested).
  if [ "${ROCKSDB_TUNING:-false}" = "true" ]; then
    if [ -z "${ROCKSDB_WRITEBUFFER_COUNT:-}" ] || [ -z "${ROCKSDB_WRITE_BUFFER_RATIO:-}" ] || [ -z "${ROCKSDB_THREADS_WRITE:-}" ]; then
      pipeline_fail "ROCKSDB_TUNING=true requires ALL THREE knobs (ROCKSDB_WRITEBUFFER_COUNT, ROCKSDB_WRITE_BUFFER_RATIO, ROCKSDB_THREADS_WRITE) - partial sets degrade the managed-memory pool unevenly (measured 2026-09-02)"
      return 1
    fi
    extra_flags+=(-Dstate.backend.rocksdb.writebuffer.count="${ROCKSDB_WRITEBUFFER_COUNT}")
    extra_flags+=(-Dstate.backend.rocksdb.memory.write-buffer-ratio="${ROCKSDB_WRITE_BUFFER_RATIO}")
    extra_flags+=(-Dstate.backend.rocksdb.threads.write="${ROCKSDB_THREADS_WRITE}")
  fi
  if [ "${UNALIGNED_CHECKPOINTS:-false}" = "true" ]; then
    extra_flags+=(-Dexecution.checkpointing.unaligned=true)
  fi
  submit_out="$($COMPOSE exec -T \
    -e ALLOW_FULL_REPLAY="${ALLOW_FULL_REPLAY:-false}" \
    -e DEPLOYMENT_ENV=dev \
    -e CONFIGURATION_VERSION=1.0.0 \
    -e ALGORITHM_VERSION=candle-15s-v1 \
    -e PARALLELISM="${PARALLELISM:-8}" \
    -e DEDUP_WINDOW_ENTRIES="${DEDUP_WINDOW_ENTRIES:-200}" -e CANDLE_WINDOW_MS=15000 \
    -e WATERMARK_OUT_OF_ORDER_MS="${WATERMARK_OUT_OF_ORDER_MS:-500}" \
    -e CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-60000}" -e CHECKPOINT_TIMEOUT_MS="${CHECKPOINT_TIMEOUT_MS:-30000}" -e MAX_CONCURRENT_CHECKPOINTS=1 \
    -e PREVIEW_ENABLED=true -e PREVIEW_INTERVAL_MS="${PREVIEW_INTERVAL_MS:-500}" \
    -e RESTART_MAX_ATTEMPTS="${RESTART_MAX_ATTEMPTS:-3}" \
    -e RESTART_DELAY_MS="${RESTART_DELAY_MS:-30000}" \
    -e FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 \
    -e SIGNAL_CANDIDATES_TABLE=Signal_Candidates \
    -e SIGNAL_CURRENT_TABLE=Signal_Candidates_current \
    -e MULTITF_ENABLED="${MULTITF_ENABLED:-false}" \
    -e STRATEGY_HOST_ENABLED="${STRATEGY_HOST_ENABLED:-false}" \
    -e STRATEGIES="${STRATEGIES:-}" \
    -e EXECUTION_INTENT_ENABLED="${EXECUTION_INTENT_ENABLED:-false}" \
    -e ACCOUNT_SCOPE_ID="${ACCOUNT_SCOPE_ID:-}" \
    -e EXECUTION_PARTITION_ID="${EXECUTION_PARTITION_ID:-}" \
    -e EXECUTION_PRODUCT_TYPE="${EXECUTION_PRODUCT_TYPE:-CNC}" \
    -e EXECUTION_TIME_IN_FORCE="${EXECUTION_TIME_IN_FORCE:-DAY}" \
    -e MULTITF_SESSION_BYPASS="${MULTITF_SESSION_BYPASS:-false}" \
    -e MULTITF_SIGNAL_CONTEXT_ENABLED="${MULTITF_SIGNAL_CONTEXT_ENABLED:-true}" \
    -e CANDLE_LIVE_TABLE="${CANDLE_LIVE_TABLE:-candle_live}" \
    -e CANDLE_CLOSED_TABLE="${CANDLE_CLOSED_TABLE:-candle_closed}" \
    -e MULTITF_LIVE_SNAPSHOT_INTERVAL_MS="${MULTITF_LIVE_SNAPSHOT_INTERVAL_MS:-1000}" \
    flink-jobmanager flink run -d "${extra_flags[@]}" \
      -c com.trading.compute.signaljob.SignalJob /opt/flink/jobs/compute.jar 2>&1)"
  JOB_ID="$(echo "$submit_out" | grep -oE 'JobID [a-f0-9]+' | awk '{print $2}' | head -1)"
  if [ -z "$JOB_ID" ]; then
    echo "!! SignalJob submit output:" >&2; echo "$submit_out" >&2
    return 1
  fi
  pipeline_log "SignalJob submitted: job_id=$JOB_ID"
}

# ---------- teardown ----------
pipeline_cleanup() {
  # CHG-122: the data path is containers — remove them. The `docker logs -f`
  # mirror processes exit when their container disappears.
  docker rm -f "$LIB_INGESTION_CONTAINER" "$LIB_FAKETOOL_CONTAINER" >/dev/null 2>&1 || true
  rm -f "$OUT/ingestion.loadtest.ready"
  if [ -n "$JOB_ID" ]; then
    echo "cleanup: cancelling SignalJob $JOB_ID"
    $COMPOSE exec -T flink-jobmanager flink cancel "$JOB_ID" >/dev/null 2>&1 || true
  fi
  echo "cleanup: removed containers=$LIB_FAKETOOL_CONTAINER,$LIB_INGESTION_CONTAINER job=$JOB_ID"
}
pipeline_install_cleanup_trap() { trap pipeline_cleanup EXIT; }

# ---------- checkpoint + GC telemetry (2026-08-30) ----------
# The REST /jobs/<id>/checkpoints history is TRIMMED after job cancel, so
# post-hoc queries only return the last few entries — live capture during
# the run is the only reliable record (observed: 10 of ~90 checkpoints
# survived after cancel).
capture_checkpoint_history() {
  # One snapshot of the full checkpoint history → JSONL appended per poll.
  local dest="${1:-$OUT/checkpoints.jsonl}"
  curl -s --max-time 10 "http://localhost:8081/jobs/$JOB_ID/checkpoints" \
    | python3 -c "
import json, sys
try:
    j = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for c in j.get('history', []):
    print(json.dumps({'id': c.get('id'), 'trigger_ts': c.get('trigger_timestamp'),
                      'duration_ms': c.get('end_to_end_duration'),
                      'size': c.get('state_size'), 'status': c.get('status')}))
" >> "$dest" 2>/dev/null || true
  # B5 Phase-0 (2026-08-31): the slim projection above cannot answer WHICH
  # phase of a checkpoint freezes emission (sync / async / alignment).
  # The per-task breakdown IS available live via the detail endpoint — the
 # history 'tasks' map is empty after cancel, so it must be captured during
  # the run. Written to a separate file to keep the slim file's schema
  # stable for the existing analyzer paths.
  local detail_dest="${dest%.jsonl}-detail.jsonl"
  curl -s --max-time 10 "http://localhost:8081/jobs/$JOB_ID/checkpoints" \
    | python3 -c "
import json, sys
try:
    j = json.load(sys.stdin)
except Exception:
    sys.exit(0)
# latest completed checkpoint only (poll runs every POLL_S seconds; the
# detail endpoint returns the FULL tasks map per checkpoint — capturing all
# history each poll would duplicate megabytes)
done = [c for c in j.get('history', []) if c.get('status') == 'COMPLETED']
if done:
    c = max(done, key=lambda x: x.get('latest_ack_timestamp') or 0)
    tasks = c.get('tasks') or {}
    rows = []
    for tname, t in tasks.items():
        dur = t.get('duration') or {}
        rows.append({
            'task': tname,
            'sync_ms': dur.get('sync'),
            'async_ms': dur.get('async'),
            'alignment_ms': dur.get('alignment'),
            'start_delay_ms': dur.get('start_delay'),
            'bytes': t.get('checkpointed_size'),
        })
    print(json.dumps({'id': c.get('id'),
                      'trigger_ts': c.get('trigger_timestamp'),
                      'e2e_ms': c.get('end_to_end_duration'),
                      'alignment_buffered': c.get('alignment_buffered'),
                      'tasks': rows}))
" >> "$detail_dest" 2>/dev/null || true
  if [ -s "$detail_dest" ]; then
    local tmpd
    tmpd="$(mktemp)"
    awk 'match($0, /"id": *[0-9]+/) { k=substr($0, RSTART, RLENGTH); if (!seen[k]++) print }' \
      "$detail_dest" > "$tmpd" && mv "$tmpd" "$detail_dest" || true
  fi
  # Dedupe on the checkpoint ID, not $1 — every JSONL line starts with
  # '{"id":' so $1 is identical for all lines and the old awk dropped
  # everything after the first entry (observed 2026-08-30: 600s run with a
  # 10s interval showed exactly ONE checkpoint — the checkpoint-burst
  # correlation was computed blind from a single sample).
  if [ -s "$dest" ]; then
    local tmp
    tmp="$(mktemp)"
    awk 'match($0, /"id": *[0-9]+/) { k=substr($0, RSTART, RLENGTH); if (!seen[k]++) print }' \
      "$dest" > "$tmp" && mv "$tmp" "$dest" || true
  fi
}

# Copy the TM's GC log (written by FLINK_ENV_JAVA_OPTS in docker-compose)
# into the evidence dir. Safe to call any time; empty if GC logging absent.
harvest_tm_gc_log() {
  local dest="${1:-$OUT/tm-gc.log}"
  docker exec 01_docker-flink-taskmanager-1 sh \
      -c 'cat /opt/flink/log/gc.log 2>/dev/null' > "$dest" 2>/dev/null || true
}

# ---------- Flink metrics ----------
# Emit lines of "name|read|write" per vertex (the format the gate scripts and
# measurement script consume).  A RUNNING Flink job can report zero/stale
# read-records and write-records in the /jobs/{id} vertex summary; those
# counters were only populated after the C2 job was canceled (2026-09-01).
# Query the live aggregate metrics endpoint first and keep the job-summary
# counters only as a terminal-job fallback for post-run evidence.
flink_metric_dump() {
  local job="${1:-$JOB_ID}"
  local rest="${FLINK_REST_URL:-http://localhost:8081}"
  curl -fsS --max-time 10 "$rest/jobs/$job" | \
    FLINK_METRIC_REST="$rest" FLINK_METRIC_JOB="$job" python3 -c '
import json
import os
import sys
import urllib.request

try:
    job = json.load(sys.stdin)
except Exception:
    sys.exit(0)

base = os.environ["FLINK_METRIC_REST"].rstrip("/")
jid = os.environ["FLINK_METRIC_JOB"]
print("STATE", job.get("state", "?"))

def fetch(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)

def live_subtask_sums(vertex_id):
    # Sum the counter across ALL subtasks (2026-09-01, CHG-120: the previous
    # 0.numRecordsIn/0.numRecordsOut read counted only subtask 0 — 1/8 of the
    # true totals at parallelism 8, so drill evidence under-reported progress
    # by 8x).  Live-verified on Flink 2.2.1: the subtasks endpoint answers an
    # AGGREGATE object per metric — [{"id":"numRecordsIn","min":73360.0,
    # "max":93010.0,"avg":83840.0,"sum":670720.0,"skew":5.27}, ...] — whose
    # "sum" is the cross-subtask total.  A per-subtask shape
    # ([{"id":"0.numRecordsIn","value":"123"}, ...]) is also accepted and
    # summed, in case a deployment answers that form.
    try:
        items = fetch(
            f"{base}/jobs/{jid}/vertices/{vertex_id}/subtasks/metrics"
            "?get=numRecordsIn,numRecordsOut"
        )
    except Exception:
        return None
    sums = {}
    for item in items:
        metric_id = str(item.get("id", ""))
        if metric_id in ("numRecordsIn", "numRecordsOut"):
            # Aggregate shape: take the precomputed cross-subtask sum.
            total = item.get("sum")
            if isinstance(total, (int, float)):
                sums[metric_id] = sums.get(metric_id, 0) + int(total)
        elif "." in metric_id:
            # Per-subtask shape: accumulate the per-index values.
            name = metric_id.split(".", 1)[1]
            value = str(item.get("value", ""))
            if not value.isdigit():
                # A non-counter value means this metric cannot be summed
                # reliably; fail closed to the next fallback.
                return None
            sums[name] = sums.get(name, 0) + int(value)
    if {"numRecordsIn", "numRecordsOut"} <= set(sums):
        return sums
    return None

def live_subtask_zero(vertex_id):
    # Last-resort live fallback: the vertex aggregate endpoint exposes one
    # subtask at a time, so this counts subtask 0 only (documented partial
    # view — kept for Flink versions without the subtasks endpoint).
    try:
        metrics = fetch(
            f"{base}/jobs/{jid}/vertices/{vertex_id}/metrics"
            "?get=0.numRecordsIn,0.numRecordsOut"
        )
    except Exception:
        return None
    live = {
        item.get("id"): item.get("value", "-")
        for item in metrics
        if item.get("id") in {"0.numRecordsIn", "0.numRecordsOut"}
    }
    if "0.numRecordsIn" in live and "0.numRecordsOut" in live:
        return {
            "numRecordsIn": live["0.numRecordsIn"],
            "numRecordsOut": live["0.numRecordsOut"],
        }
    return None

for vertex in job.get("vertices", []):
    vertex_id = vertex.get("id")
    if not vertex_id:
        continue

    summary = vertex.get("metrics", {})
    # Priority: full-subtask sum > subtask-0 live > terminal job summary.
    # A terminal job may no longer expose live vertex metrics; the
    # accumulated summary then remains the evidence of record.
    totals = live_subtask_sums(vertex_id) or live_subtask_zero(vertex_id) or summary
    read = totals.get("numRecordsIn", totals.get("read-records", "-"))
    write = totals.get("numRecordsOut", totals.get("write-records", "-"))
    print(vertex.get("name", "?"), "|", read, "|", write)
' 2>/dev/null
}

# Return the cumulative counter used to prove that the raw feed is traversing
# the running job.  The raw source's live aggregate can briefly report 0|0
# while downstream vertices are already processing records (observed in C2 on
# 2026-09-01); treating that snapshot as a dead pipeline causes a false
# negative.  Prefer the raw source read/output counters, then use the largest
# read counter from the bounded set of operators immediately downstream of the
# raw path.  An absent signal remains -1 so callers still fail closed.
pipeline_metric_input_progress() {
  local dump="$1"
  printf '%s\n' "$dump" | awk -F'|' '
    function number(raw, value) {
      value = raw
      gsub(/^[ \t]+|[ \t]+$/, "", value)
      return (value ~ /^[0-9]+$/) ? value + 0 : -1
    }
    {
      name = $1
      read = number($2)
      write = number($3)
      if (name ~ /raw[-_]table[-_]1/) {
        raw_seen = 1
        if (read > raw_read) raw_read = read
        if (write > raw_write) raw_write = write
      }
      if (name ~ /fingerprint[-_]dedup|candle[-_]15s|forming[-_]bar[-_](builder|detection|writer)/ \
          && read > downstream_read) downstream_read = read
    }
    END {
      if (raw_read > 0) print raw_read
      else if (raw_write > 0) print raw_write
      else if (downstream_read > 0) print downstream_read
      else print -1
    }'
}

# Wait until the job reaches the given state (default RUNNING) or timeout.
flink_wait_state() {
  local want="${1:-RUNNING}" timeout_s="${2:-60}" i state
  for i in $(seq 1 "$timeout_s"); do
    state="$(curl -s --max-time 5 "http://localhost:8081/jobs/$JOB_ID" | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('state',''))
except Exception: print('')" 2>/dev/null)"
    [ "$state" = "$want" ] && { pipeline_log "job state=$want after ${i}s"; return 0; }
    sleep 1
  done
  pipeline_fail "job did not reach state $want within ${timeout_s}s (last=$state)"
  return 1
}
