"""PROD — Production-hardening beyond the 120 IDs (08-local-compose.md is explicitly NOT prod-HA).

These tests prove local Compose cannot be mistaken for Swarm prod and that
the single-node dev simplifications are explicit. All offline PASS (CI green);
live probes gate on Swarm/prod env vars so they SKIP locally, and PROD-011 also
gates on the docker CLI plus the gitignored .env/secrets.env (P6-614).
"""
import json, re, shutil, subprocess, unittest

import yaml
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
COLLECTOR = ROOT / "code/01_platform/01_docker/otel-collector-config.yaml"
PLATFORM_CONFIG = ROOT / "code/common/src/main/java/com/trading/common/config/PlatformConfig.java"
GATE_RS = ROOT / "code/02_services/04_executor/src/gate.rs"

def compose_text():
    return COMPOSE.read_text()

def collector_text():
    return COLLECTOR.read_text() if COLLECTOR.exists() else ""

def service_block(text, name):
    """Return one service's compose source, comments removed, up to the next key.

    Indentation decides the boundary: a service is `  name:` and its body is
    more indented, so the block ends at the next line matching `  <key>:`.

    Comments are stripped so a pin cannot be satisfied by prose: a `#` line
    mentioning R2_ENDPOINT is not a setting, and an `assertNotIn` must not pass
    merely because the stale value survives only inside a comment.
    """
    lines = text.splitlines()
    start = next(
        (i for i, l in enumerate(lines) if l.rstrip() == f"  {name}:"), None
    )
    if start is None:
        raise AssertionError(f"service {name!r} not found in compose")
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if re.match(r"^  \S", lines[i]):
            end = i
            break
    body = (l for l in lines[start:end] if not l.lstrip().startswith("#"))
    return "\n".join(body)

def fluss_properties(compose_yaml):
    """Every service's FLUSS_PROPERTIES value, as parsed — not regex-matched (P6-817).

    The block is the live server config, so a leak scan over it must cover all of
    them and must be able to tell "nothing found" from "nothing scanned".
    """
    parsed = yaml.safe_load(compose_yaml)
    return [svc["environment"]["FLUSS_PROPERTIES"]
            for svc in parsed["services"].values()
            if isinstance(svc.get("environment"), dict)
            and "FLUSS_PROPERTIES" in svc["environment"]]


def s3a_properties(xml):
    """Return the (name, value) pairs of an Hadoop core-site.xml, comments stripped.

    The two copies of this file carry different explanatory headers, so only the
    configuration body is comparable.
    """
    body = re.sub(r"<!--.*?-->", "", xml, flags=re.S)
    return re.findall(r"<name>(.*?)</name>\s*<value>(.*?)</value>", body, flags=re.S)

class ProdHardeningTest(unittest.TestCase):
    def test_PROD_001_single_node_simplification_explicit(self):
        """PROD-001: local is single ZK + single Fluss (not 3-node HA) — doc + compose must say so."""
        self.assertIn("ZooKeeper (single node", (ROOT / "docs/08_implementation/08-local-compose.md").read_text())
        self.assertIn("single-host", (ROOT / "docs/08_implementation/08-local-compose.md").read_text())
        text = compose_text()
        self.assertIn("zookeeper:", text)
        self.assertIn("fluss-coordinator:", text)
        self.assertNotIn("ensemble", text.lower(), "PROD-001: local compose must not claim ensemble")

    def test_PROD_002_checkpoints_are_local_volume_not_s3(self):
        """PROD-002: checkpoints are flink-checkpoints local volume; prod Swarm needs s3:// — never hard-code s3 here."""
        text = compose_text()
        self.assertIn("flink-checkpoints:", text, "PROD-002: local checkpoint volume missing")
        self.assertIn("flink-checkpoints:/", text)
        # allow s3:// only inside comments/docs — forbid hard-coded S3 image/endpoint without ${}
        self.assertIn("${S3_WAREHOUSE_PATH", text, "PROD-002: warehouse must be env-interpolated")
        self.assertIn("s3://", compose_text(), "PROD-002: compose must document s3:// production requirement in header comment")

    def test_PROD_003_no_prod_endpoint_accepted(self):
        """PROD-003: local profile rejects prod marker/endpoint/bucket/creds — SEC-001/002 + CONFIG-003."""
        # production marker must gate
        self.assertIn("ENVIRONMENT=production", (ROOT / "docs/08_implementation/08-local-compose.md").read_text())
        env_example = (ROOT / "code/01_platform/01_docker/.env.example").read_text()
        self.assertIn("EXECUTION_ENABLED=false", env_example)
        self.assertIn("${", compose_text(), "PROD-003: endpoints must be env-interpolated, not hard-coded prod")

    def test_PROD_004_no_secret_in_git_example(self):
        """PROD-004: .env.example contains no real secret, only placeholders."""
        ex = (ROOT / "code/01_platform/01_docker/.env.example").read_text()
        # placeholders use ${} or example/test/sandbox tokens, not real creds
        self.assertNotRegex(ex.lower(), r"sk-live|prod.*token.*[a-f0-9]{20}", "PROD-004: .env.example leaks prod-like token")
        # P6-612: `if exists else True` made a deleted .gitignore look like a pass.
        self.assertIn(".env", (ROOT / ".gitignore").read_text(),
                      "PROD-004: .env must be gitignored")

    def test_PROD_005_no_latest_digests_pinned(self):
        """PROD-005: no :latest; digests pinned where required (golang@sha256, rust:1.97.1)."""
        text = compose_text()
        self.assertNotIn(":latest", text, "PROD-005: :latest forbidden")
        self.assertTrue("FLUSS_IMAGE" in text or "golang:" in text, "PROD-005: base image pin missing")
        # digest is enforced via ${FLUSS_IMAGE:?set ... to an immutable digest} (env holds digest-pin)
        self.assertIn("immutable digest", text, "PROD-005: images must require immutable digest via env pin")

    def test_PROD_006_resource_limits_and_jvm_wiring(self):
        """PROD-006: resource envelopes + JVM 65/35/85 (dev-sized, not prod-sized)."""
        self.assertIn("jobmanager.memory.process.size", compose_text())
        self.assertIn("taskmanager.memory.process.size", compose_text())
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT = 65", cfg)
        self.assertIn("NON_HEAP_MEMORY_RESERVE_PERCENT = 35", cfg)
        self.assertIn("CONTAINER_MEMORY_ALERT_PERCENT = 85", cfg)
        self.assertIn("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT", (ROOT / "docs/08_implementation/08-local-compose.md").read_text())

    def test_PROD_007_restart_policy_safe(self):
        """PROD-007: restart policy is always/unless-stopped (safe locally), never 'no' for infra."""
        text = compose_text()
        # infra must auto-recover locally
        self.assertIn("restart:", text)
        self.assertIn("restart: unless-stopped", text)
        self.assertIn("restart: always", text)  # zookeeper = always
        # no prod-only policy drift

    def test_PROD_008_ddl_idempotency_and_evidence_ownership(self):
        """PROD-008: DDL apply is idempotent + evidence 2775/664 (not root-owned)."""
        self.assertTrue((ROOT / "code/01_platform/04_scripts/ddl_apply.py").exists())
        self.assertTrue((ROOT / "code/01_platform/04_scripts/evidence_ownership_check.py").exists())
        # P6-816: the image must run the repo's own orchestrator, and the non-root
        # contract (02-schema-storage.md: the ENGINE runs as uid/gid 10001) lives in
        # the entrypoint, not in a Dockerfile USER line.
        dockerfile = (ROOT / "code/01_platform/01_docker/ddl-apply/Dockerfile").read_text()
        self.assertIn("04_scripts/ddl_apply.py", dockerfile,
                      "PROD-008: the image must run the repo's ddl_apply.py, not a copy")
        entrypoint = (ROOT / "code/01_platform/01_docker/ddl-apply/ddl-apply-entrypoint.sh").read_text()
        self.assertIn("setpriv", entrypoint, "PROD-008: the engine must drop privileges via setpriv")
        self.assertIn("10001", entrypoint, "PROD-008: the engine must run as the documented uid/gid 10001")
        # ownership gate is documented in Makefile
        self.assertIn("evidence-ownership-check", (ROOT / "Makefile").read_text())

    def test_PROD_009_otel_retry_and_ingestion_cred_free(self):
        """PROD-009: collector retry_on_failure/max_elapsed_time + holds O2 auth; ingestion is cred-free."""
        ct = collector_text()
        self.assertIn("retry_on_failure", ct)
        self.assertIn("max_elapsed_time: 5m", ct)
        self.assertIn("send_failed", ct, "PROD-009: send_failed metric name changed")
        self.assertIn("http://openobserve:5080", ct)
        # P6-613: report, do not swallow — the walk used to keep its match in a
        # variable nobody read. Source-level on purpose: every service carries the
        # shared env file, so a rendered-config check false-fires (l9 NETWORK-008).
        leaked = [str(p) for p in (ROOT / "code/02_services/01_ingestion").rglob("*.java")
                  if "O2_AUTH" in p.read_text()]
        self.assertEqual([], leaked, "PROD-009: ingestion must not hold O2 cred")
        # filelog receiver is on collector, not ingestion
        self.assertIn("filelog", ct)

    def test_PROD_010_gate_monotonic_and_safety_halt_only_regress(self):
        """PROD-010: gate HALTED→RECONCILING→APPROVAL_PENDING→ENABLED, only safety_halt regresses."""
        src = GATE_RS.read_text()
        # DEC-044 form (2026-08-21): sanctioned transitions are a `matches!` tuple list;
        # `Enabled` is NOT in that list — it is reachable only via Gate::enable after approval.
        self.assertIn("(ExecState::Halted, ExecState::Reconciling)", src)
        self.assertIn("(ExecState::Reconciling, ExecState::ApprovalPending)", src)
        self.assertIn("Gate::enable", src)
        self.assertIn("safety_halt", src)
        # illegal jumps must error (covers 005 regress)
        self.assertIn("InvalidTransition", src)

    def test_PROD_011_execution_t3_disabled_by_default(self):
        """PROD-011: bridge disabled by default; needs --profile execution-t3 to appear.

        P6-614: the module's one test that needs the docker CLI *and* the two
        gitignored env files, so it skips when either is absent instead of erroring
        in a Docker-less checkout.
        """
        env_files = [COMPOSE.parent / ".env", COMPOSE.parent / "secrets.env"]
        if shutil.which("docker") is None or not all(f.exists() for f in env_files):
            self.skipTest("needs the docker CLI plus .env/secrets.env (both gitignored)")
        base = ["docker", "compose", "-f", str(COMPOSE)]
        for f in env_files:
            base += ["--env-file", str(f)]
        cfg_default = json.loads(subprocess.check_output([*base, "config", "--format", "json"], text=True))
        self.assertNotIn("execution-bridge", cfg_default.get("services", {}))
        cfg_t3 = json.loads(subprocess.check_output([*base, "--profile", "execution-t3", "config", "--format", "json"], text=True))
        self.assertIn("execution-bridge", cfg_t3["services"])

    def test_PROD_012_no_aws_creds_in_fluss_properties(self):
        """PROD-012: S3A creds via env (AWS_*) only, never in FLUSS_PROPERTIES."""
        text = compose_text()
        # FLUSS_PROPERTIES must not contain AWS_SECRET
        # P6-817: scan the parsed values, not a regex over the raw text. The old
        # delimiter guessed "6 spaces then an uppercase letter"; when it missed, the
        # block list came back empty and the leak assertion passed on nothing.
        props = fluss_properties(text)
        self.assertTrue(props, "PROD-012: no FLUSS_PROPERTIES block found — the leak scan would be vacuous")
        leaked_props = [b for b in props if "AWS_SECRET_ACCESS_KEY:" in b]
        self.assertEqual([], leaked_props, "PROD-012: creds leaked into FLUSS_PROPERTIES")
        # env interpolation is correct
        self.assertIn("${AWS_ACCESS_KEY_ID", text)
        self.assertIn("${AWS_SECRET_ACCESS_KEY", text)
        self.assertIn("never in fluss_properties", text.lower(), "PROD-012: FLUSS_PROPERTIES must document env-only creds")

    def test_PROD_013_lake_snapshot_remote_dir_shared(self):
        """PROD-013: whatever serves getLakeSnapshot (coordinator OR tablet) must
        read the SAME remote.data.dir, because lake-snapshot metadata and tiered
        log segments are written there.

        W35x (2026-09-16): the shared *volume* this used to require is gone — dev
        moved remote.data.dir to a Cloudflare R2 bucket, where sharing is by
        construction rather than by a mount. The invariant is unchanged, so this
        pins the new form: both servers name the same remote.data.dir, that value
        is the env-interpolated bucket URI, and the container-local path (which
        only THAT container can read — the bug W35x fixed) must NOT come back.
        """
        text = compose_text()
        self.assertIn("${R2_BUCKET", text, "PROD-013: remote.data.dir must use the R2 bucket")
        self.assertGreaterEqual(
            text.count("remote.data.dir: s3://${R2_BUCKET"),
            2,
            "PROD-013: coordinator AND tablet must both point remote.data.dir at the same R2 bucket",
        )
        self.assertNotIn(
            "remote.data.dir: /tmp/fluss/remote-data",
            text,
            "PROD-013: a container-local remote.data.dir is readable only from inside "
            "that container, so other readers silently lose the tiered rows",
        )
        # The path must be the same string in both services, or they disagree.
        remote_dirs = re.findall(r"remote\.data\.dir:\s*(\S+)", text)
        self.assertEqual(
            len(set(remote_dirs)),
            1,
            f"PROD-013: coordinator and tablet disagree on remote.data.dir: {sorted(set(remote_dirs))}",
        )

    def test_PROD_014_ten_vs_1024_instrument_manifest(self):
        """PROD-014: local smoke is 10 random instruments; live bench is 1024 cap (2433-row NSE file cannot be serviced)."""
        md = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("10-instrument", md)
        self.assertIn("10 random instruments", md)
        self.assertIn("1,024 instruments", compose_text())
        self.assertIn("1024 tokens/connection", compose_text())
        self.assertIn("LOCAL-INT-004", md)

    def test_PROD_015_checkpoint_restart_constants_pinned(self):
        """PROD-015: checkpoint/restart governed pins in PlatformConfig (not tunable per-env)."""
        cfg = PLATFORM_CONFIG.read_text()
        for needle in ["CHECKPOINT_INTERVAL_MS = 10_000", "CHECKPOINT_TIMEOUT_MS = 30_000", "MAX_CONCURRENT_CHECKPOINTS = 1", "RESTART_MAX_ATTEMPTS = 3", "RESTART_DELAY_MS = 30_000", "SINK_WRITE_STALL_TIMEOUT_MS = 15_000"]:
            self.assertIn(needle, cfg, f"PROD-015: governed pin missing: {needle}")

    def test_PROD_016_ingestion_backpressure_guards(self):
        """PROD-016: MAX_PENDING 150k (T2 tunable, 80% warn), baseline 20 ticks, reconnect 1s/30s."""
        cfg = PLATFORM_CONFIG.read_text()
        self.assertIn("BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC = 20", cfg)
        # Backpressure pins moved from PlatformConfig to IngestionConfig
        # (T2 streaming-3000): warning percent 80% + bounded-halt defaults.
        ing_path = ROOT / "code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java"
        ing_cfg = ing_path.read_text()
        # 80% is owned by AlertThresholds now (W2/CHG-139); the config derives 0.80.
        thresholds = (ROOT / "code/common/src/main/java/com/trading/common/observability/AlertThresholds.java").read_text()
        self.assertIn("PENDING_APPEND_WARNING_PERCENT = 80", thresholds)
        self.assertIn("AlertThresholds.PENDING_APPEND_WARNING_PERCENT / 100.0", ing_cfg)
        self.assertIn("PENDING_APPEND_WARNING_PERCENT", ing_cfg)
        self.assertIn("150_000", ing_cfg)
        self.assertIn("reconnect", ing_cfg.lower())
        # recomputed: max concurrent checkpoint is 1 so no concurrent stall
        self.assertIn("MAX_CONCURRENT_CHECKPOINTS = 1", cfg)

    def test_PROD_017_audit_gates_exist(self):
        """PROD-017: docs-audit / stale-tables / full-audit / pin-check exist."""
        make = (ROOT / "Makefile").read_text()
        for target in ["docs-audit:", "stale-tables:", "full-audit:", "pin-check:"]:
            self.assertIn(target, make, f"PROD-017: Makefile target missing: {target}")
        self.assertTrue((ROOT / "code/01_platform/04_scripts/docs_audit.py").exists())
        self.assertTrue((ROOT / "code/01_platform/04_scripts/stale_table_kind_scan.py").exists())

    def test_PROD_018_implementation_gate_no_cep(self):
        """PROD-018: CEP dependency ban in order path + stale-table scan (feature_candles_15s not LOG, etc)."""
        make = (ROOT / "Makefile").read_text()
        self.assertIn("cep-check", make)
        self.assertIn("stale_table_kind_scan.py --upstream", make)
        # guards are exercised by run-monday-gates.sh (gate target)
        self.assertIn("gate:", make)
        self.assertIn("run-monday-gates.sh", make)

    def test_PROD_019_no_orphan_fluss_properties_file(self):
        """PROD-019 (P5-014/023/029/030): the Fluss server config is the
        FLUSS_PROPERTIES env block in compose/stack — the MVP-era
        03_fluss/fluss.properties was never wired (no COPY/mount/script ref)
        and its content was stale (ap-south-1 region, empty endpoint,
        non-canonical tablet.host). It must not drift back: an unwired
        properties file reads as authoritative while silently doing nothing."""
        orphan = ROOT / "code/01_platform/03_fluss/fluss.properties"
        self.assertFalse(orphan.exists(),
                         "orphan fluss.properties resurrected — wire it into the "
                         "images or delete it; never keep dead server config")
        self.assertIn("FLUSS_PROPERTIES:", compose_text(),
                      "the live server-config source must stay in compose")


    def test_PROD_020_taskmanager_can_read_tiered_data(self):
        """PROD-020 (CHG-180): a task manager must be able to read Fluss data
        that has tiered out to object storage.

        remote.data.dir is an s3:// URI, so a scanning source subtask fetches
        those segments itself — the tablet hands it a path, not the bytes. Two
        things make that possible and neither is inherited from the tablet:

          * R2_ENDPOINT / AWS_REGION in the task manager's own environment;
          * /etc/hadoop/conf/core-site.xml, which is what turns those variables
            into fs.s3a.* for the Hadoop client the Fluss S3 plugin uses.

        Without the conf, Hadoop keeps its default endpoint and the request
        goes to Amazon S3, failing 403 InvalidAccessKeyId (observed 2026-09-16
        against R2). Without the region it fails 400 Bad Request.

        The old shared fluss-remote-data volume must NOT come back: it held the
        bytes only while remote.data.dir was a local path, and a permanent empty
        mount reads as "the tiered data is gone" while the client silently
        under-counts rows — the defect W35x (P6-380) fixed.

        Source pins only. The behaviour evidence is the live R2 list in
        CHG-180, which needs Docker, network and credentials.
        """
        text = compose_text()
        tm = service_block(text, "flink-taskmanager")
        self.assertIn("R2_ENDPOINT:", tm,
                      "PROD-020: the task manager needs R2_ENDPOINT — the S3 "
                      "plugin resolves the endpoint from env, not from the tablet")
        self.assertIn("AWS_REGION:", tm,
                      "PROD-020: without a region S3A answers 400 Bad Request")
        self.assertIn(
            "./flink-runtime/core-site.xml:/etc/hadoop/conf/core-site.xml:ro",
            tm,
            "PROD-020: the task manager must mount core-site.xml at the path "
            "Flink's bin/config.sh auto-detects (/etc/hadoop/conf); without it "
            "s3:// resolves against Amazon S3 and fails 403",
        )
        self.assertNotIn(
            "fluss-remote-data:/tmp/fluss/remote-data",
            tm,
            "PROD-020: a local remote-data mount is stale — the bytes live on "
            "R2 now, and an empty mount silently under-counts tiered rows",
        )
        # The mount's source must be the SAME file the servers' client uses, or
        # the two ends disagree about the endpoint. Both are tracked copies of
        # one config, so compare the property bodies, not the prose headers.
        probe_conf = ROOT / "code/01_platform/04_scripts/fluss-probes/hadoop-conf/core-site.xml"
        image_conf = ROOT / "code/01_platform/01_docker/flink-runtime/core-site.xml"
        self.assertEqual(
            s3a_properties(probe_conf.read_text()),
            s3a_properties(image_conf.read_text()),
            "PROD-020: the Flink image's core-site.xml and the host probe's copy "
            "have diverged — the readers would resolve different endpoints",
        )


    def test_PROD_021_flink_runtime_conf_is_a_guarded_bind_source(self):
        """PROD-021 (CHG-180): the compose bind source added for the task
        manager must be registered in pipeline-lib's B7 guard.

        The guard exists because compose creates a *directory* when a
        short-syntax bind source is missing, and the container then dies with an
        opaque OCI exit 127. A source that is mounted but not registered loses
        that protection silently: the failure returns only on the node where the
        file happens to be absent. Registering it is what turns that into a
        local, named, pre-start error.
        """
        lib = (ROOT / "code/01_platform/04_scripts/pipeline-lib.sh").read_text()
        block = re.search(
            r"pipeline_validate_compose_bind_sources\(\)\s*\{.*?for relative in \\\n(.*?); do",
            lib,
            re.S,
        )
        self.assertIsNotNone(block, "PROD-021: bind-source guard not found in pipeline-lib.sh")
        guarded = set(re.findall(r'"([^"]+)"', block.group(1)))
        self.assertIn(
            "flink-runtime/core-site.xml",
            guarded,
            "PROD-021: the task manager mounts this file, so the B7 guard must "
            "check it — unregistered, a missing file becomes an OCI exit 127",
        )

if __name__ == "__main__":
    unittest.main()
