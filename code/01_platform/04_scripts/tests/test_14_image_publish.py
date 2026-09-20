"""D1.2/D1.3 — offline tests for image-publish.sh (no registry, no push, no VMs).

The push path needs a registry and real images, so these tests cover what has to
be right *before* a registry exists:

  * the deploy-environment rewrite — the one operation that can corrupt the file
    a deploy reads, so it is tested for preservation, appending and idempotency;
  * stack parity — every `${X_IMAGE:?}` the production stack demands must be
    either pushed by this script or already pinned in `runtime.lock`, because a
    missing one fails the deploy with "set X_IMAGE to an immutable digest";
  * the refusal paths — an unreachable registry and a missing env file must stop
    the run rather than half-write something.
"""

import os
import re
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(SCRIPTS, "image-publish.sh")
STACK = os.path.join(SCRIPTS, "..", "01_docker", "docker-stack.yml")
LOCK = os.path.join(SCRIPTS, "..", "01_docker", "runtime.lock")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPTS)))
GUIDE = os.path.join(REPO, "docs", "05_deployment", "PROD_VM_PROVISIONING.md")


def run(*args, stdin=None):
    return subprocess.run(["bash", SCRIPT, *args], input=stdin,
                          capture_output=True, text=True)


def env_file(tmp_path, body):
    path = tmp_path / ".env"
    path.write_text(body)
    return str(path)


SAMPLE_ENV = """# deploy environment
STACK_NAME=prod
FLINK_IMAGE=flink:2.2.1-scala_2.12-java17
# a comment that must survive
CHECKPOINT_DIR=s3://bucket/flink-checkpoints
"""


def test_merge_env_replaces_the_line_and_preserves_the_rest(tmp_path):
    """The rewrite touches its own variables and nothing else."""
    path = env_file(tmp_path, SAMPLE_ENV)
    ref = "10.0.0.11:5000/trading-flink-runtime:prod@sha256:" + "a" * 64

    result = run("--merge-env", path, stdin=f"FLINK_IMAGE={ref}\n")
    assert result.returncode == 0, result.stderr

    text = open(path).read()
    assert f"FLINK_IMAGE={ref}" in text
    assert "flink:2.2.1-scala_2.12-java17" not in text
    assert "STACK_NAME=prod" in text
    assert "# a comment that must survive" in text
    assert "CHECKPOINT_DIR=s3://bucket/flink-checkpoints" in text


def test_merge_env_appends_a_variable_that_is_absent(tmp_path):
    """The four app images are not in .env at all today."""
    path = env_file(tmp_path, SAMPLE_ENV)
    ref = "10.0.0.11:5000/01_docker-ingestion:prod@sha256:" + "b" * 64

    result = run("--merge-env", path, stdin=f"INGESTION_IMAGE={ref}\n")
    assert result.returncode == 0, result.stderr

    lines = open(path).read().splitlines()
    assert f"INGESTION_IMAGE={ref}" in lines
    # appended, not inserted into the middle of the existing file
    assert lines.index(f"INGESTION_IMAGE={ref}") >= len(SAMPLE_ENV.splitlines())
    assert lines[0] == "# deploy environment"


def test_merge_env_is_idempotent(tmp_path):
    """Running it twice must not duplicate or reorder anything."""
    path = env_file(tmp_path, SAMPLE_ENV)
    ref = "10.0.0.11:5000/01_docker-nautilus:prod@sha256:" + "c" * 64
    updates = f"NAUTILUS_IMAGE={ref}\n"

    assert run("--merge-env", path, stdin=updates).returncode == 0
    once = open(path).read()
    assert run("--merge-env", path, stdin=updates).returncode == 0
    twice = open(path).read()

    assert once == twice
    assert twice.count("NAUTILUS_IMAGE=") == 1


def test_merge_env_refuses_a_missing_file(tmp_path):
    """A missing .env is a `cp .env.example .env` mistake — say so, do not guess."""
    missing = str(tmp_path / "absent.env")
    result = run("--merge-env", missing, stdin="FLINK_IMAGE=x@sha256:" + "d" * 64 + "\n")
    assert result.returncode == 3
    assert "not found" in (result.stderr + result.stdout).lower()
    assert not os.path.exists(missing)


def test_every_stack_image_variable_is_covered():
    """Parity: a `${X_IMAGE:?}` the stack demands must be pushable or already pinned.

    This is the check that turns "the deploy failed with a cryptic interpolation
    error" into a failing test on the workstation.
    """
    stack = open(STACK).read()
    demanded = set(re.findall(r"\$\{([A-Z_]+_IMAGE):\?", stack))
    assert demanded, "the stack should demand at least one image variable"

    map_out = run("--print-map")
    assert map_out.returncode == 0, map_out.stderr
    pushed = {line.split("=", 1)[0] for line in map_out.stdout.splitlines() if "=" in line}

    lock = open(LOCK).read()
    pinned = {line.split("=", 1)[0] for line in lock.splitlines()
              if re.match(r"^[A-Za-z_][A-Za-z0-9_]*_IMAGE=", line)
              and "@sha256:" in line}

    uncovered = demanded - pushed - pinned
    assert not uncovered, (
        f"the stack demands {sorted(uncovered)} but neither image-publish.sh "
        f"pushes them nor runtime.lock pins them")

    # FLINK_IMAGE/FLUSS_IMAGE are legitimately both pushed and lock-pinned: the
    # lock carries the stock upstream digest until the push happens (CHG-218),
    # and is re-pinned from the push. What must never happen is the script
    # pushing the STOCK image — the stock Flink image cannot run this job at all
    # (CHG-179), and a deploy that pins it fails only at runtime.
    pairs = dict(line.split("=", 1) for line in map_out.stdout.splitlines() if "=" in line)
    assert not pairs["FLINK_IMAGE"].startswith("flink:"), (
        "FLINK_IMAGE must be the built runtime image, not the stock upstream one")
    assert not pairs["FLUSS_IMAGE"].startswith("apache/fluss"), (
        "FLUSS_IMAGE must be the built runtime image, not the stock upstream one")


def test_print_map_names_the_local_images_and_their_variables():
    out = run("--print-map")
    assert out.returncode == 0, out.stderr
    pairs = dict(line.split("=", 1) for line in out.stdout.splitlines() if "=" in line)

    assert pairs["FLINK_IMAGE"] == "trading-flink-runtime:0.1.0"
    assert pairs["FLUSS_IMAGE"] == "trading-fluss-runtime:0.1.0"
    assert pairs["INGESTION_IMAGE"] == "01_docker-ingestion:latest"
    assert pairs["NAUTILUS_IMAGE"] == "01_docker-nautilus:latest"
    assert pairs["EXECUTION_BRIDGE_IMAGE"] == "01_docker-execution-bridge:latest"
    assert pairs["EXECUTION_GATEWAY_IMAGE"] == "01_docker-execution-gateway:latest"
    assert pairs["DDL_APPLY_IMAGE"] == "01_docker-ddl-apply:latest"


def test_the_first_boot_catalog_step_runs_a_published_tool_image():
    """CHG-256 — the runbook's first-boot DDL step must run an image this script pushes.

    The step is not a stack service, so nothing else pins its image: it can name
    a tool that exists only on the workstation, and the deploy then fails on VM1
    with `docker run` against an empty catalog — with ingestion already
    fail-closed, that is the state where no data loop can start.
    """
    guide = open(GUIDE).read()
    marker = "### S7b — Apply the DDL catalog (first boot only)"
    assert marker in guide, "the runbook must carry the first-boot catalog step"
    step = guide.split(marker, 1)[1].split("### S8", 1)[0]

    assert '"$DDL_APPLY_IMAGE" apply' in step, (
        "the step must invoke the tool image by the variable this script publishes")
    assert "DDL-APPLY-RESULT: PASS" in step, "the step must name the tool's own sentinel"
    assert "allowRuntimeDdl=false" in step, (
        "the step must say why it exists: ingestion refuses an empty catalog")

    assert "DDL_APPLY_IMAGE" in dict(
        line.split("=", 1) for line in run("--print-map").stdout.splitlines() if "=" in line
    ), "the image the runbook runs must be in the push map"


def test_self_check_passes_and_writes_nothing(tmp_path):
    """--self-check is the pre-flight an operator runs before touching a VM."""
    before = os.path.getmtime(STACK)
    out = run("--self-check")
    assert out.returncode == 0, out.stdout + out.stderr
    assert "PASS" in out.stdout
    assert os.path.getmtime(STACK) == before


def test_unreachable_registry_is_refused_before_any_push():
    """Port 1 is closed: the probe must stop the run, not fall through to push."""
    out = run("--registry", "127.0.0.1:1")
    assert out.returncode == 2
    assert "registry" in (out.stdout + out.stderr).lower()


def test_usage_error_exits_two():
    out = run()
    assert out.returncode == 2
    assert "usage" in (out.stdout + out.stderr).lower()


# --- GHCR landing path (Decision 2026-09-21: public GHCR, anonymous pulls) ----
# A real GHCR run differs from the local-registry rehearsal in exactly two ways:
# the registry address carries an owner path (`ghcr.io/<owner>/<image>`), and its
# probe answers **401** instead of 200 because the registry is auth-enabled. The
# script already treats 401 as "answers" (the auth-challenge case) and separates
# the push address from the deploy address with `--env-registry`. Both are pinned
# here against fakes so the behaviour cannot silently regress before a VM exists.


class _ChallengeHandler(BaseHTTPRequestHandler):
    """Answers `/v2/` with 401 + Bearer challenge, like GHCR does.

    Strictly path-bound: anything else is 404. A handler that challenged *every*
    path would also accept a probe built from the owner path (`/<owner>/v2/`),
    which is the bug this covers — a fake that answers everything passes the
    broken script too.
    """

    def do_GET(self):  # noqa: N802 (http.server API)
        if self.path == "/v2/":
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Bearer realm="https://ghcr.io/token"')
        else:
            self.send_response(404)
        self.end_headers()

    def log_message(self, *args):  # keep pytest output clean
        pass


FAKE_DOCKER = """#!/usr/bin/env bash
# Fake docker: logs every call and answers the four verbs image-publish.sh uses.
printf '%s\\n' "$*" >>"$FAKE_DOCKER_LOG"
case "$1" in
info) exit 0 ;;
image)
	[ "$2" = inspect ] && exit 0
	exit 0
	;;
tag) exit 0 ;;
push)
	# a real push prints the digest it stored; stdout is not parsed
	exit 0
	;;
buildx)
	# digest-pin.sh asks for the manifest digest and parses stdout strictly
	for arg in "$@"; do
		case "$arg" in
		*:*) ref="$arg" ;;
		esac
	done
	printf '%s' "$ref" | sha256sum | awk '{print "sha256:" $1}'
	exit 0
	;;
esac
exit 0
"""


def _fake_docker(tmp_path):
    """Put a fake `docker` first on PATH; return (env, log path)."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    exe = bindir / "docker"
    exe.write_text(FAKE_DOCKER)
    exe.chmod(0o755)
    log = tmp_path / "docker.log"
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}",
               FAKE_DOCKER_LOG=str(log))
    return env, log


def test_owner_path_registry_with_a_401_probe_publishes_ghcr_refs(tmp_path):
    """`--registry <host:port>/<owner> --env-registry ghcr.io/<owner>` must push
    under the owner path and write the GHCR address, digest-pinned, into env."""
    env, log = _fake_docker(tmp_path)

    server = HTTPServer(("127.0.0.1", 0), _ChallengeHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        deploy_env = env_file(
            tmp_path,
            "\n".join(
                ["# deploy environment", "STACK_NAME=prod"]
                + [f"{var}=bare-tag:latest" for var in (
                    "FLINK_IMAGE", "FLUSS_IMAGE", "INGESTION_IMAGE", "NAUTILUS_IMAGE",
                    "EXECUTION_BRIDGE_IMAGE", "EXECUTION_GATEWAY_IMAGE", "DDL_APPLY_IMAGE")]
                + ["# a comment that must survive", "CHECKPOINT_DIR=s3://bucket/checkpoints"]
            ) + "\n",
        )
        out = subprocess.run(
            ["bash", SCRIPT,
             "--registry", f"http://127.0.0.1:{port}/saurabh17111994",
             "--env-registry", "ghcr.io/saurabh17111994",
             "--tag", "prod", "--write-env", deploy_env],
            capture_output=True, text=True, env=env,
        )
    finally:
        server.shutdown()

    assert out.returncode == 0, out.stdout + out.stderr

    # 1. the push went to the owner path on the probed host, not to ghcr.io
    pushed = log.read_text()
    for name in ("trading-flink-runtime", "01_docker-ingestion", "01_docker-ddl-apply"):
        assert f"push 127.0.0.1:{port}/saurabh17111994/{name}:prod" in pushed, pushed
    assert "push ghcr.io/" not in pushed, "the push must not go to the deploy address"

    # 2. the deploy environment carries GHCR refs, each digest-pinned
    written = open(deploy_env).read()
    refs = re.findall(r"^[A-Z_]+IMAGE=(\S+)$", written, re.M)
    assert len(refs) == 7, written
    for ref in refs:
        # the tag survives ahead of the digest — `name:prod@sha256:…` is what the
        # local-registry publish already wrote (rehearsal.env), and the digest is
        # what the pull resolves, so the tag is informative, not load-bearing
        assert re.fullmatch(
            r"ghcr\.io/saurabh17111994/[\w.\-/]+:prod@sha256:[0-9a-f]{64}", ref
        ), ref

    # 3. nothing else in the file moved
    assert "# a comment that must survive" in written
    assert "CHECKPOINT_DIR=s3://bucket/checkpoints" in written
    assert "bare-tag:latest" not in written
