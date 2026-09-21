#!/usr/bin/env python3
"""deploy_preflight.py — fail closed on a deploy environment that cannot work, BEFORE it deploys.

Why this exists: the deploy environment is a file of values that compose interpolates silently. A
key that is missing becomes an empty string, a tag instead of a digest becomes "whatever the node
happened to pull", and a checkpoint directory that is a local path becomes a single point of failure
that only shows itself when that node dies. None of those are visible in the stack file, and the
four checks this repository already owns run after the deploy (stack_selfcheck), on the job submit
path (submit-jobs.sh), on the lake guard's own schedule (lake-guard), or not at all.

This is the check that runs first and can only pass on a coherent environment. It is read-only: it
reads files, optionally performs one signed LIST against the lake, and never deploys or mutates.

Usage:
  deploy_preflight.py --env-file <path> [--expect production|dev] [--check-lake] [--secrets-check]

Exit code = number of FAILs (0 = ready to deploy).
"""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
import sys
import yaml
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_STACK = HERE.parents[0] / "01_docker" / "docker-stack.yml"
DEFAULT_R2_LIST = HERE / "r2-list.sh"
DEFAULT_SECRETS_CHECK = HERE / "secrets-bootstrap.sh"

# The deploy values the running platform cannot invent. These are the same six stack_selfcheck.sh
# requires; here they are checked before the deploy rather than after it.
REQUIRED_IN_PRODUCTION = (
    "S3_WAREHOUSE_PATH", "R2_ENDPOINT", "R2_BUCKET", "ARROW_APP_ID", "ARROW_USER_ID", "CHECKPOINT_DIR",
)
# Rule: every image the deploy environment pulls from *our* registry must be digest-pinned. Values
# carrying an upstream registry are left alone — upstream tags are their authors' contract, ours are not.
IMAGE_KEYS_SUFFIX = "_IMAGE"
OUR_REGISTRY_HINT = re.compile(r"(^|/)localhost:\d+|(^|/)127\.0\.0\.1:\d+|^ghcr\.io/")
DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
LOCAL_HOST = re.compile(r"(^|/)(localhost|127\.0\.0\.1|0\.0\.0\.0)(:\d+)?/")
VAR_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}")


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.checks: list[str] = []

    def ok(self, message: str) -> None:
        self.checks.append(f"[PASS] {message}")

    def fail(self, message: str) -> None:
        self.failures.append(message)
        self.checks.append(f"[FAIL] {message}")

    def info(self, message: str) -> None:
        self.checks.append(f"[INFO] {message}")

    def render(self) -> str:
        return "\n".join(self.checks + ["", f"{len(self.failures)} failure(s)"])


def parse_env(text: str) -> dict[str, str]:
    """Mirror r2-env.sh's rules: last key wins, `export` and CR tolerated, quotes stripped, values trimmed."""
    values: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.rstrip("\r")
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        values[key] = value
    return values


def stack_references(text: str) -> dict[str, str | None]:
    """Every ${VAR} the stack file interpolates -> the default it supplies, or None.

    Parsed as YAML rather than scanned as raw text. A ${VAR} that appears only inside a
    comment is not interpolated by docker, and reporting one is a false alarm: the note
    beside FLUSS_BOOTSTRAP in docker-stack.yml made this script fail a stack that
    deploys correctly. Block scalars still count — their contents are values, not
    comments.
    """
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError:
        # Never under-report: if the file will not parse, fall back to the raw scan,
        # which can only over-report.
        return _references_in(text)
    return _references_in_node(document)


def _references_in_node(node: object) -> dict[str, str | None]:
    """Collect references from every string in a parsed YAML document."""
    found: dict[str, str | None] = {}
    if isinstance(node, dict):
        for key, value in node.items():
            found.update(_references_in_node(key))
            found.update(_references_in_node(value))
    elif isinstance(node, list):
        for item in node:
            found.update(_references_in_node(item))
    elif isinstance(node, str):
        found.update(_references_in(node))
    return found


def _references_in(text: str) -> dict[str, str | None]:
    return {m.group(1): (m.group(3) if m.group(2) is not None else None) for m in VAR_REF.finditer(text)}


def check_env_file(path: Path, report: Report) -> None:
    if not path.is_file():
        report.fail(f"deploy environment {path} does not exist or is not a file")
    else:
        report.ok(f"deploy environment {path} is readable")


def check_stack_references(refs: dict[str, str | None], env: dict[str, str], report: Report) -> None:
    if not refs:
        report.fail("the stack file references no ${VAR} at all — is this the deploy stack file?")
        return
    unresolved = sorted(v for v, default in refs.items() if default is None and not env.get(v))
    if unresolved:
        for var in unresolved:
            report.fail(f"${{{var}}} is referenced by the stack with no default and the environment is "
                        f"empty — docker would deploy an empty value")
    else:
        report.ok(f"all {len(refs)} variable(s) the stack interpolates resolve to a non-empty value")


def check_production_values(env: dict[str, str], report: Report) -> None:
    missing = [k for k in REQUIRED_IN_PRODUCTION if not env.get(k)]
    if missing:
        for key in missing:
            report.fail(f"{key} is empty — the platform cannot start without it (stack_selfcheck.sh requires it too)")
    else:
        report.ok(f"the {len(REQUIRED_IN_PRODUCTION)} values the platform cannot invent are all set")

    endpoint = env.get("R2_ENDPOINT", "")
    if endpoint and not endpoint.startswith("https://"):
        report.fail(f"R2_ENDPOINT is '{endpoint}' — the lake is https:// only (this is also what r2-list.sh enforces)")
    elif endpoint:
        report.ok("R2_ENDPOINT is an https:// endpoint")

    checkpoints = env.get("CHECKPOINT_DIR", "")
    if checkpoints:
        if not checkpoints.startswith("s3://") or not checkpoints[len("s3://"):].split("/")[0]:
            report.fail(f"CHECKPOINT_DIR is '{checkpoints}' — production checkpoints must be s3://<bucket>/…, "
                        f"a local path dies with the node that holds it")
        else:
            report.ok(f"CHECKPOINT_DIR is remote ({checkpoints.split('/')[0]}//…)")


def check_images(env: dict[str, str], refs: dict[str, str | None], expect: str,
                 report: Report) -> None:
    """Judge the image a node would actually pull: the environment's value, else the stack's default."""
    effective: dict[str, str] = {}
    for var, default in refs.items():
        if not var.endswith(IMAGE_KEYS_SUFFIX):
            continue
        value = env.get(var) or default
        if value:
            effective[var] = value
    for var, value in env.items():
        if var.endswith(IMAGE_KEYS_SUFFIX) and value and var not in effective:
            effective[var] = value
    if not effective:
        report.info("no *_IMAGE values to judge")
        return

    def verdict(message: str) -> None:
        report.fail(message) if expect == "production" else report.info(message + " (dev: reported, not fatal)")

    loopback = sorted(k for k, v in effective.items() if LOCAL_HOST.search(v))
    for key in loopback:
        verdict(f"{key}={effective[key]} points at a loopback registry — on a node, localhost is that "
                f"node, not the registry")
    ours = {k: v for k, v in effective.items() if OUR_REGISTRY_HINT.search(v) and k not in loopback}
    for key in sorted(k for k, v in ours.items() if not DIGEST.search(v)):
        verdict(f"{key}={ours[key]} is not digest-pinned — a tag is 'whatever the node happened to pull', "
                f"a digest is the image that was tested")
    pinned = sorted(k for k, v in ours.items() if DIGEST.search(v))
    upstream = sorted(k for k in effective if k not in ours and k not in loopback)
    if not loopback and not [k for k, v in ours.items() if not DIGEST.search(v)]:
        report.ok(f"every image a node would pull is pinned: {len(pinned)} digest-pinned, "
                  f"{len(upstream)} upstream image(s) left to their author (tag or digest)")


def run_helper(argv: list[str], env: dict[str, str], timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(argv, capture_output=True, text=True, env=env, timeout=timeout)


def check_lake(env_file: Path, secrets_file: Path, helper: Path, base_env: dict[str, str], report: Report) -> None:
    if not helper.is_file():
        report.fail(f"the R2 helper {helper} is missing — cannot verify the lake before deploying")
        return
    env = dict(base_env)
    env["R2_ENV_FILE"] = str(env_file)
    env["R2_SECRETS_FILE"] = str(secrets_file)
    try:
        proc = run_helper(["bash", str(helper), "all"], env)
    except subprocess.TimeoutExpired:
        report.fail("the lake did not answer the ListObjectsV2 request within 30s")
        return
    if proc.returncode == 0:
        report.ok(f"the lake answered a signed LIST ({len(proc.stdout.splitlines())} object(s) listed)")
    else:
        detail = (proc.stderr or proc.stdout or "no output").strip().splitlines()
        report.fail(f"the lake refused the signed LIST: {detail[0] if detail else 'no output'}")


def check_secrets(helper: Path, base_env: dict[str, str], required: bool, report: Report) -> None:
    if not helper.is_file():
        message = f"the secrets check {helper} is missing"
        report.fail(message) if required else report.info(message + " — skipped")
        return
    try:
        proc = run_helper(["bash", str(helper), "--check"], base_env, timeout=60)
    except subprocess.TimeoutExpired:
        report.fail("the secrets check did not finish within 60s")
        return
    if proc.returncode == 0:
        report.ok("every secret the stack needs already exists")
    else:
        detail = (proc.stdout + proc.stderr).strip().splitlines()
        line = detail[-1] if detail else "no output"
        if required:
            report.fail(f"the secrets check failed: {line}")
        else:
            report.info(f"the secrets check failed ({line}) — expected before S6, fatal after it")


def build_report(args: argparse.Namespace) -> Report:
    report = Report()
    env_file, secrets_file, stack_file = Path(args.env_file), Path(args.secrets_file), Path(args.stack_file)
    check_env_file(env_file, report)
    env = parse_env(env_file.read_text()) if env_file.is_file() else {}
    refs: dict[str, str | None] = {}
    if stack_file.is_file():
        refs = stack_references(stack_file.read_text())
        check_stack_references(refs, env, report)
    else:
        report.fail(f"stack file {stack_file} does not exist")
    if args.expect == "production":
        check_production_values(env, report)
    else:
        report.info("--expect dev: the production-only value checks are skipped")
    check_images(env, refs, args.expect, report)
    if args.check_lake:
        check_lake(env_file, secrets_file, Path(args.r2_list), dict(**__import__("os").environ), report)
    else:
        report.info("the lake is not contacted without --check-lake (a signed LIST needs the real credentials)")
    if args.secrets_check:
        check_secrets(Path(args.secrets_check_helper), dict(**__import__("os").environ), args.expect == "production", report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fail closed on a deploy environment that cannot work.")
    parser.add_argument("--env-file", default=str(HERE.parents[0] / "01_docker" / ".env"),
                        help="the environment file compose will interpolate")
    parser.add_argument("--secrets-file", default=str(HERE.parents[0] / "01_docker" / "secrets.env"),
                        help="the secrets file the R2 helper reads (never printed)")
    parser.add_argument("--stack-file", default=str(DEFAULT_STACK))
    parser.add_argument("--expect", choices=("production", "dev"), default="production")
    parser.add_argument("--check-lake", action="store_true", help="perform one real signed LIST")
    parser.add_argument("--secrets-check", action="store_true", help="also run secrets-bootstrap.sh --check")
    parser.add_argument("--r2-list", default=str(DEFAULT_R2_LIST))
    parser.add_argument("--secrets-check-helper", default=str(DEFAULT_SECRETS_CHECK))
    args = parser.parse_args(argv)
    report = build_report(args)
    print(report.render())
    return len(report.failures)


if __name__ == "__main__":
    sys.exit(main())
