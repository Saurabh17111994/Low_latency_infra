"""C2 — the lake mode's config bridge, and the format it must satisfy.

`r2-list.sh` reads its config from FILES (`R2_ENV_FILE` / `R2_SECRETS_FILE`,
read by `r2_var`), while a container has the values in the environment: the
stack passes `R2_ENDPOINT` / `R2_BUCKET` / `S3_WAREHOUSE_PATH` as variables and
the credentials as Swarm secret files under `/run/secrets/`. So when the
default files are absent, `eod_controller.py` writes the pair the reader
expects and points the seams at it.

Three properties matter more than the writing itself, and each is a test here:
a missing value fails closed with a *named* reason instead of producing a
half-written config (which would read as "no objects in the lake" — the exact
false negative the lake checks exist to prevent), the written file is read back
through the real `r2_var`, and no value ever reaches stdout.

Plain functions on purpose: the documented runner passes `-p no:unittest`, so a
`unittest.TestCase` here would never be collected.
"""

import importlib.util
import os
import pathlib
import stat
import subprocess

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
CONTROLLER = SCRIPTS / "eod_controller.py"
R2_ENV_SH = SCRIPTS / "r2-env.sh"

ENV_VALUES = {
    "R2_ENDPOINT": "https://example-account.r2.cloudflarestorage.com",
    "R2_BUCKET": "example-bucket",
    "S3_WAREHOUSE_PATH": "s3://example-bucket/lake",
    "AWS_ACCESS_KEY_ID": "AKIAEXAMPLEACCESSKEY",
    "AWS_SECRET_ACCESS_KEY": "example-secret-key-material-not-a-real-value",
}


def load_controller():
    spec = importlib.util.spec_from_file_location("eod_controller_under_test", CONTROLLER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def container_env(tmp_path, **overrides):
    """The shape a container has: values as variables, credentials as files."""
    env = {"EOD_OFFLOAD": "lake"}
    for key in ("R2_ENDPOINT", "R2_BUCKET", "S3_WAREHOUSE_PATH"):
        env[key] = overrides.get(key, ENV_VALUES[key])
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        if key in overrides:
            env[key + "_FILE"] = overrides[key]
            continue
        path = tmp_path / ("secret-" + key.lower())
        path.write_text(ENV_VALUES[key] + "\n")
        env[key + "_FILE"] = str(path)
    return env


def read_back(env_file, key):
    """Read a key through the real reader, so the format is proven, not assumed."""
    proc = subprocess.run(
        ["bash", "-c", f'. "{R2_ENV_SH}" && r2_var "{env_file}" {key}'],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def test_not_lake_leaves_the_environment_untouched(tmp_path, monkeypatch):
    module = load_controller()
    monkeypatch.setattr(module, "DEFAULT_R2_ENV_FILE", str(tmp_path / "absent.env"))
    monkeypatch.setattr(module, "DEFAULT_R2_SECRETS_FILE", str(tmp_path / "absent-secrets.env"))
    env = container_env(tmp_path)
    env["EOD_OFFLOAD"] = "none"
    assert module.r2_list_child_env(env, tmp_path) == env
    assert not list(tmp_path.glob("r2*")), "nothing may be written when lake mode is off"


def test_a_host_run_keeps_the_scripts_own_defaults(tmp_path, monkeypatch):
    module = load_controller()
    default_env = tmp_path / ".env"
    default_secrets = tmp_path / "secrets.env"
    default_env.write_text("R2_ENDPOINT=https://host-run\n")
    default_secrets.write_text("AWS_ACCESS_KEY_ID=host-run\n")
    monkeypatch.setattr(module, "DEFAULT_R2_ENV_FILE", str(default_env))
    monkeypatch.setattr(module, "DEFAULT_R2_SECRETS_FILE", str(default_secrets))
    env = container_env(tmp_path)
    out = module.r2_list_child_env(env, tmp_path)
    assert "R2_ENV_FILE" not in out and "R2_SECRETS_FILE" not in out, \
        "a host run must keep reading the repo's own .env and secrets.env"
    assert out["R2_LIST_SCRIPT"].endswith("r2-list.sh")


def test_container_shape_materialises_both_files_readable_by_r2_var(tmp_path, monkeypatch):
    module = load_controller()
    monkeypatch.setattr(module, "DEFAULT_R2_ENV_FILE", str(tmp_path / "absent.env"))
    monkeypatch.setattr(module, "DEFAULT_R2_SECRETS_FILE", str(tmp_path / "absent-secrets.env"))
    out = module.r2_list_child_env(container_env(tmp_path), tmp_path)

    env_file = pathlib.Path(out["R2_ENV_FILE"])
    secrets_file = pathlib.Path(out["R2_SECRETS_FILE"])
    assert env_file.exists() and secrets_file.exists()
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(secrets_file.stat().st_mode) == 0o600

    # The reader is the contract: these five keys are what r2_load asks for.
    for key in ("R2_ENDPOINT", "R2_BUCKET", "S3_WAREHOUSE_PATH"):
        assert read_back(env_file, key) == ENV_VALUES[key]
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        assert read_back(secrets_file, key) == ENV_VALUES[key]


def test_a_missing_value_fails_closed_and_writes_nothing(tmp_path, monkeypatch):
    module = load_controller()
    monkeypatch.setattr(module, "DEFAULT_R2_ENV_FILE", str(tmp_path / "absent.env"))
    monkeypatch.setattr(module, "DEFAULT_R2_SECRETS_FILE", str(tmp_path / "absent-secrets.env"))
    env = container_env(tmp_path)
    del env["R2_BUCKET"]
    try:
        module.r2_list_child_env(env, tmp_path)
    except module.R2ConfigError as error:
        assert "R2_BUCKET" in str(error), f"the reason must name the missing key: {error}"
    else:
        raise AssertionError("a missing R2_BUCKET must not produce a config")
    assert not list(tmp_path.glob("r2*")), "a refused config must leave no partial files"


def test_a_value_that_cannot_be_written_safely_is_refused(tmp_path, monkeypatch):
    """A quote or a newline in a value would change what the reader parses."""
    module = load_controller()
    monkeypatch.setattr(module, "DEFAULT_R2_ENV_FILE", str(tmp_path / "absent.env"))
    monkeypatch.setattr(module, "DEFAULT_R2_SECRETS_FILE", str(tmp_path / "absent-secrets.env"))
    env = container_env(tmp_path)
    env["AWS_SECRET_ACCESS_KEY"] = 'injected"\nAWS_ACCESS_KEY_ID=attacker'
    del env["AWS_SECRET_ACCESS_KEY_FILE"]
    try:
        module.r2_list_child_env(env, tmp_path)
    except module.R2ConfigError as error:
        assert "AWS_SECRET_ACCESS_KEY" in str(error)
    else:
        raise AssertionError("an unwritable value must be refused, not quoted blindly")


def test_no_value_reaches_stdout(tmp_path, monkeypatch, capsys):
    module = load_controller()
    monkeypatch.setattr(module, "DEFAULT_R2_ENV_FILE", str(tmp_path / "absent.env"))
    monkeypatch.setattr(module, "DEFAULT_R2_SECRETS_FILE", str(tmp_path / "absent-secrets.env"))
    module.r2_list_child_env(container_env(tmp_path), tmp_path)
    captured = capsys.readouterr()
    for value in ENV_VALUES.values():
        assert value not in captured.out and value not in captured.err
