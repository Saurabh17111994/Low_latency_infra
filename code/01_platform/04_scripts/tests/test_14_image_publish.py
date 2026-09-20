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
