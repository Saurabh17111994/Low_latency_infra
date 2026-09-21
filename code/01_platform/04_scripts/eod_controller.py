#!/usr/bin/env python3
"""EOD controller CLI launcher (SCH-23): runs the plain-JVM
com.trading.common.schema.eod.EodControllerTool against the live Fluss
cluster, mirroring the ddl-apply host-side pattern (same pinned jar set,
FLUSS_BOOTSTRAP via env, machine-readable RESULT sentinel).

Usage:
  python3 eod_controller.py <subcommand> [tool args...]   # status|run|extend|reconcile|reset

Env:
  FLUSS_BOOTSTRAP  (default localhost:9123)   passed to the tool
  EOD_*            (EOD_DATABASE, EOD_STATE_TABLE, EOD_TABLES, EOD_TTL,
                    EOD_SAFETY_FLOOR, EOD_EXTENSION, EOD_OFFLOAD, EOD_ZONE ...)
                   read directly by the tool
  EOD_M2_REPO      overrides ~/.m2/repository

The tool's exit code is propagated; the sentinel line the tool already prints
(`eod-controller: RESULT=... EXIT=...`) is echoed unchanged.
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# EOD_M2_REPO first (host runs), then the layout the ddl-apply image already sets for its own
# tool — the same jars, so a container needs no extra configuration to find them.
M2_REPO = (os.environ.get("EOD_M2_REPO") or os.environ.get("DDL_APPLY_M2_REPO")
           or os.path.expanduser("~/.m2/repository"))

# Pinned Fluss + Jackson versions — same set the ddl-apply engine uses
# (ddl_apply.py JACKSON_VERSION/SLF4J_VERSION).
FLUSS_VERSION = os.environ.get("FLUSS_VERSION", "0.9.1-incubating")
JACKSON_VERSION = os.environ.get("JACKSON_VERSION", "2.16.1")
SLF4J_VERSION = os.environ.get("SLF4J_VERSION", "2.0.9")

COMMON_CLASSES = os.path.normpath(
    os.path.join(SCRIPT_DIR, "..", "..", "common", "target", "classes")
)

MAIN_CLASS = "com.trading.common.schema.eod.EodControllerTool"

# r2-list.sh reads its config from FILES, through r2-env.sh's r2_var. On a host
# those are the repo's own .env / secrets.env; a container has the values as
# variables and the credentials as Swarm secret files under /run/secrets/.
DOCKER_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "01_docker"))
DEFAULT_R2_ENV_FILE = os.path.join(DOCKER_DIR, ".env")
DEFAULT_R2_SECRETS_FILE = os.path.join(DOCKER_DIR, "secrets.env")
R2_LIST_SCRIPT = os.path.join(SCRIPT_DIR, "r2-list.sh")
R2_ENV_KEYS = ("R2_ENDPOINT", "R2_BUCKET", "S3_WAREHOUSE_PATH")
R2_SECRET_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
# What may be written into a KEY="value" line: no quote, no space, no newline,
# and no '#', which r2_var would strip as a trailing comment.
R2_VALUE_OK = re.compile(r"^[A-Za-z0-9+/=._:@-]+$")


class R2ConfigError(RuntimeError):
    """A named reason. Never a half-written config: an unreadable or empty
    value must fail the run, because the alternative is a listing that comes
    back empty and reads as "the tiering job is behind"."""


def r2_value(env, key):
    value = env.get(key)
    if value:
        return value
    secret_file = env.get(key + "_FILE")
    if secret_file:
        try:
            value = open(secret_file, encoding="utf-8").read().strip()
        except OSError as error:
            raise R2ConfigError(f"{key}_FILE unreadable: {secret_file} ({error.strerror})")
        if value:
            return value
    raise R2ConfigError(f"{key} missing (set {key} or {key}_FILE)")


def write_private(path, values):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f'{key}="{value}"\n')


def r2_list_child_env(env, staging_dir):
    """The environment for the JVM, with r2-list.sh's config in place.

    Only lake mode needs it, and only when the default files are absent: on a
    host the script finds the repo's own files, so nothing is staged there and
    the existing behaviour is untouched.
    """
    if env.get("EOD_OFFLOAD", "none").strip().lower() != "lake":
        return dict(env)
    child = dict(env)
    child.setdefault("R2_LIST_SCRIPT", R2_LIST_SCRIPT)
    if os.path.exists(DEFAULT_R2_ENV_FILE) and os.path.exists(DEFAULT_R2_SECRETS_FILE):
        return child

    values = {key: r2_value(env, key) for key in R2_ENV_KEYS + R2_SECRET_KEYS}
    unsafe = sorted(key for key, value in values.items() if not R2_VALUE_OK.match(value))
    if unsafe:
        raise R2ConfigError("unsupported characters in " + ", ".join(unsafe))

    env_file = os.path.join(staging_dir, "r2.env")
    secrets_file = os.path.join(staging_dir, "r2-secrets.env")
    write_private(env_file, {key: values[key] for key in R2_ENV_KEYS})
    write_private(secrets_file, {key: values[key] for key in R2_SECRET_KEYS})
    child["R2_ENV_FILE"] = env_file
    child["R2_SECRETS_FILE"] = secrets_file
    return child


def build_classpath():
    jars = [
        (FLUSS_VERSION, "org/apache/fluss", "fluss-client"),
        (JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-databind"),
        (JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-core"),
        (JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-annotations"),
        (SLF4J_VERSION, "org/slf4j", "slf4j-api"),
    ]
    entries = [COMMON_CLASSES]
    missing = []
    for version, group, name in jars:
        path = os.path.join(M2_REPO, *group.split("/"), name, version, f"{name}-{version}.jar")
        if os.path.isfile(path):
            entries.append(path)
        else:
            missing.append(path)
    if not os.path.isfile(os.path.join(COMMON_CLASSES,
                                       "com/trading/common/schema/eod/EodControllerTool.class")):
        missing.append(COMMON_CLASSES + " (run `cd code && mvn -o compile -pl common` first)")
    if missing:
        print("EOD CONTROLLER CLASSPATH INCOMPLETE:", file=sys.stderr)
        for m in missing:
            print("  - " + m, file=sys.stderr)
        return None
    return os.pathsep.join(entries)


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return 2 if not args else 0

    classpath = build_classpath()
    if classpath is None:
        return 2

    cmd = ["java", "-cp", classpath, MAIN_CLASS] + args
    # Inherit the environment — the tool reads FLUSS_BOOTSTRAP / EOD_*. Lake mode
    # additionally needs r2-list.sh's config as files, which a container has not.
    staging_dir = tempfile.mkdtemp(prefix="eod-r2-config-")
    try:
        child_env = r2_list_child_env(os.environ, staging_dir)
    except R2ConfigError as error:
        shutil.rmtree(staging_dir, ignore_errors=True)
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    try:
        try:
            proc = subprocess.run(cmd, env=child_env)
        except FileNotFoundError:
            print("ERROR: java not found on PATH", file=sys.stderr)
            return 2
    finally:
        # The staged files hold credentials; they do not outlive the run.
        shutil.rmtree(staging_dir, ignore_errors=True)
    print(f"eod-controller-launcher: EXIT={proc.returncode} CMD={args[0]}")
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
