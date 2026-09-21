"""Hermetic tests for deploy_preflight.py.

The script reads files and (only on request) calls two helper scripts. Every test here builds its own
environment file and stack file in a temp directory and, when a helper is involved, points the script
at a stub — so nothing touches the network, the real lake, or the real swarm.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "deploy_preflight.py"
DIGEST = "sha256:" + "a" * 64

GOOD_ENV = f"""\
DEPLOYMENT_ENV=production
S3_WAREHOUSE_PATH=s3://fluss-warehouse
R2_ENDPOINT=https://account.r2.cloudflarestorage.com
R2_BUCKET=fluss-warehouse
ARROW_APP_ID=app-id
ARROW_USER_ID=user-id
CHECKPOINT_DIR=s3://fluss-checkpoints/flink
APP_IMAGE=ghcr.io/owner/app@{DIGEST}
UPSTREAM_IMAGE=flink:2.2.1-scala_2.12-java17
"""

STACK = """\
services:
  app:
    image: ${APP_IMAGE}
    environment:
      PORT: ${PORT:-8080}
      TIMEOUT: ${TIMEOUT:-30s}
  other:
    image: ${UPSTREAM_IMAGE:-flink:2.2.1}
"""


def write(tmp: Path, name: str, text: str) -> Path:
    path = tmp / name
    path.write_text(text)
    return path


def stub(tmp: Path, name: str, body: str, marker: str | None = None) -> Path:
    path = tmp / name
    touch = f'touch "{marker}"\n' if marker else ""
    path.write_text(f"#!/bin/sh\n{touch}{body}\n")
    return path


def run(tmp: Path, env_text: str = GOOD_ENV, stack_text: str = STACK, *args: str):
    env_file = write(tmp, "deploy.env", env_text)
    stack_file = write(tmp, "stack.yml", stack_text)
    argv = [sys.executable, str(SCRIPT), "--env-file", str(env_file), "--stack-file", str(stack_file), *args]
    return subprocess.run(argv, capture_output=True, text=True)


def failures(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith("[FAIL]")]


# ------------------------------------------------------------------ the environment as a whole

def test_a_coherent_production_environment_passes():
    with tempfile.TemporaryDirectory() as t:
        r = run(Path(t), GOOD_ENV)
        assert r.returncode == 0, r.stdout
        assert "0 failure(s)" in r.stdout
        assert "all 4 variable(s) the stack interpolates resolve to a non-empty value" in r.stdout
        assert "the 6 values the platform cannot invent are all set" in r.stdout
        assert "every image a node would pull is pinned" in r.stdout


def test_an_env_file_that_does_not_exist_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        r = subprocess.run([sys.executable, str(SCRIPT), "--env-file", str(Path(t) / "nope.env"),
                            "--stack-file", str(write(Path(t), "stack.yml", STACK))],
                           capture_output=True, text=True)
        assert r.returncode >= 1 and "does not exist" in r.stdout


def test_an_empty_required_value_is_named():
    with tempfile.TemporaryDirectory() as t:
        text = GOOD_ENV.replace("CHECKPOINT_DIR=s3://fluss-checkpoints/flink", "CHECKPOINT_DIR=")
        out = run(Path(t), text).stdout
        assert "CHECKPOINT_DIR is empty" in out
        assert len(failures(out)) == 1, out


def test_a_local_checkpoint_directory_is_refused():
    with tempfile.TemporaryDirectory() as t:
        text = GOOD_ENV.replace("CHECKPOINT_DIR=s3://fluss-checkpoints/flink", "CHECKPOINT_DIR=file:///tmp/cp")
        out = run(Path(t), text).stdout
        assert "production checkpoints must be s3://<bucket>" in out


def test_an_http_lake_endpoint_is_refused():
    with tempfile.TemporaryDirectory() as t:
        text = GOOD_ENV.replace("https://account.r2.cloudflarestorage.com", "http://minio:9000")
        out = run(Path(t), text).stdout
        assert "R2_ENDPOINT is 'http://minio:9000'" in out


def test_dev_expectation_skips_the_production_only_values():
    with tempfile.TemporaryDirectory() as t:
        text = GOOD_ENV.replace("CHECKPOINT_DIR=s3://fluss-checkpoints/flink", "CHECKPOINT_DIR=file:///tmp/cp")
        r = run(Path(t), text, STACK, "--expect", "dev")
        assert r.returncode == 0, r.stdout
        assert "production-only value checks are skipped" in r.stdout


# ------------------------------------------------------------------ every image a node would pull

def test_an_unpinned_registry_image_is_refused():
    with tempfile.TemporaryDirectory() as t:
        out = run(Path(t), GOOD_ENV.replace(f"@{DIGEST}", ":prod")).stdout
        assert "APP_IMAGE=ghcr.io/owner/app:prod is not digest-pinned" in out


def test_a_loopback_registry_image_is_fatal_in_production_and_noted_in_dev():
    with tempfile.TemporaryDirectory() as t:
        text = GOOD_ENV.replace(f"ghcr.io/owner/app@{DIGEST}", f"localhost:5000/app@{DIGEST}")
        tmp = Path(t)
        assert "points at a loopback registry" in run(tmp, text).stdout
        assert run(tmp, text, STACK, "--expect", "dev").returncode == 0


def test_a_stack_default_for_an_image_is_judged_too():
    with tempfile.TemporaryDirectory() as t:
        # No UPSTREAM_IMAGE in the environment, so the stack's own default is what a node would pull.
        text = GOOD_ENV.replace("UPSTREAM_IMAGE=flink:2.2.1-scala_2.12-java17\n", "")
        stack = STACK.replace("${UPSTREAM_IMAGE:-flink:2.2.1}", "${UPSTREAM_IMAGE:-localhost:5000/stale:dev}")
        out = run(Path(t), text, stack).stdout
        assert "UPSTREAM_IMAGE=localhost:5000/stale:dev points at a loopback registry" in out


def test_a_pinned_upstream_default_is_left_alone():
    with tempfile.TemporaryDirectory() as t:
        text = GOOD_ENV.replace("UPSTREAM_IMAGE=flink:2.2.1-scala_2.12-java17\n", "")
        stack = STACK.replace("${UPSTREAM_IMAGE:-flink:2.2.1}", f"${{UPSTREAM_IMAGE:-quay.io/thing@{DIGEST}}}")
        r = run(Path(t), text, stack)
        assert r.returncode == 0, r.stdout


# ------------------------------------------------------------------ the stack's own references

def test_an_image_the_stack_references_without_a_default_must_be_set():
    with tempfile.TemporaryDirectory() as t:
        stack = STACK.replace("    image: ${APP_IMAGE}", "    image: ${MISSING_IMAGE}")
        out = run(Path(t), GOOD_ENV, stack).stdout
        assert "${MISSING_IMAGE} is referenced by the stack with no default" in out


def test_a_non_image_variable_may_rely_on_its_default():
    with tempfile.TemporaryDirectory() as t:
        assert run(Path(t), GOOD_ENV).returncode == 0


def test_a_reference_that_exists_only_in_a_comment_is_not_interpolated():
    with tempfile.TemporaryDirectory() as t:
        stack = STACK.replace("services:", "services:\n# ${COMMENT_ONLY_VAR} is documented here, not interpolated")
        r = run(Path(t), GOOD_ENV, stack)
        assert r.returncode == 0, r.stdout
        assert "COMMENT_ONLY_VAR" not in r.stdout


def test_a_reference_inside_a_block_scalar_still_counts():
    with tempfile.TemporaryDirectory() as t:
        stack = STACK + """  properties:
    FLUSS_PROPERTIES: |
      bootstrap.servers=${BLOCK_VAR}
"""
        out = run(Path(t), GOOD_ENV, stack).stdout
        assert "${BLOCK_VAR} is referenced by the stack with no default" in out


def test_a_comment_does_not_hide_a_real_reference():
    with tempfile.TemporaryDirectory() as t:
        stack = STACK.replace("services:", "services:\n# ${MISSING_IMAGE} also appears in this comment")
        stack = stack.replace("    image: ${APP_IMAGE}", "    image: ${MISSING_IMAGE}")
        out = run(Path(t), GOOD_ENV, stack).stdout
        assert "${MISSING_IMAGE} is referenced by the stack with no default" in out


# ------------------------------------------------------------------ the two helpers

def test_the_lake_is_not_contacted_without_the_flag():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        marker = tmp / "called"
        helper = stub(tmp, "r2.sh", 'echo "must not run" >&2; exit 1', str(marker))
        r = run(tmp, GOOD_ENV, STACK, "--r2-list", str(helper))
        assert r.returncode == 0 and not marker.exists()
        assert "the lake is not contacted" in r.stdout


def test_a_refused_signed_list_is_reported_with_its_reason():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        helper = stub(tmp, "r2.sh", 'echo "r2-list: R2_ENDPOINT must be an https:// URL, got http" >&2; exit 1')
        out = run(tmp, GOOD_ENV, STACK, "--check-lake", "--r2-list", str(helper)).stdout
        assert "the lake refused the signed LIST: r2-list: R2_ENDPOINT must be an https:// URL, got http" in out


def test_a_lake_that_answers_is_a_pass():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        helper = stub(tmp, "r2.sh", 'printf "key\\t1\\t2026-09-20T00:00:00Z\\nkey2\\t2\\t2026-09-20T00:00:00Z\\n"; exit 0')
        out = run(tmp, GOOD_ENV, STACK, "--check-lake", "--r2-list", str(helper), "--secrets-file", str(tmp / "s.env")).stdout
        assert "the lake answered a signed LIST (2 object(s) listed)" in out


def test_the_secrets_check_is_fatal_only_in_production():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        helper = stub(tmp, "secrets.sh", 'echo "[FAIL] missing secret: o2_password"; exit 1')
        args = ("--secrets-check", "--secrets-check-helper", str(helper))
        out = run(tmp, GOOD_ENV, STACK, *args).stdout
        assert "the secrets check failed: [FAIL] missing secret: o2_password" in out
        assert run(tmp, GOOD_ENV, STACK, *args, "--expect", "dev").returncode == 0


# ------------------------------------------------------------------ the reader's contract

def test_parse_env_matches_the_shell_reader():
    sys.path.insert(0, str(SCRIPT.parent))
    import importlib.util

    spec = importlib.util.spec_from_file_location("deploy_preflight", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    parsed = module.parse_env(
        "# comment\nA=1\nA=2\r\nexport B = 'two words' \nC=\"quoted\"\nD=\nnot a key\n# E=3\n"
    )
    assert parsed == {"A": "2", "B": "two words", "C": "quoted", "D": ""}, parsed
