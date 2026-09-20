"""D1.2/D1.3 — offline tests for secrets-bootstrap.sh (no swarm, no real secrets).

The script's whole job is to move nine values into a Swarm without ever exposing
them, so these tests drive it through a **fake `docker` on PATH** that records
every argv and every stdin body. That makes the security property assertable:
for all nine secrets the value must appear on stdin and never in an argument,
where `ps` and shell history would see it.

Pairs with CHG-248. The stack file is authoritative for the nine names — the
test reads them from `docker-stack.yml` rather than repeating them, so a name
added to the stack without a matching entry here fails at the workstation and
not as "secret not found" during a deploy.
"""

import base64
import json
import os
import subprocess
import sys

import yaml

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(SCRIPTS, "secrets-bootstrap.sh")
STACK = os.path.join(SCRIPTS, "..", "01_docker", "docker-stack.yml")

# The five values an operator must supply, as they appear in the values file.
SUPPLIED = {
    "arrow_app_secret": "app-secret-value",
    "arrow_password": "broker-password-value",
    "arrow_totp_key": "TOTPKEYVALUE",
    "aws_access_key_id": "AKIAEXAMPLEKEY",
    "aws_secret_access_key": "aws-secret-value",
}
O2_USER = "admin@example.com"
# OpenObserve validates this value itself and panics on a weak one, so the
# fixture carries a compliant password: lowercase, uppercase, digit, special.
O2_PASSWORD = "Test-Passw0rd!"


def stack_secret_names():
    """The nine names the stack declares — the script's list must equal this."""
    with open(STACK) as fh:
        return set(yaml.safe_load(fh)["secrets"])


def fake_docker(tmp_path, existing=()):
    """A `docker` stub that records argv + stdin and answers the script's calls."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    log = tmp_path / "calls.jsonl"
    stub = bindir / "docker"
    stub.write_text(
        "#!" + sys.executable + "\n"
        "import json, pathlib, sys\n"
        f"log = pathlib.Path({str(log)!r})\n"
        "argv = sys.argv[1:]\n"
        "entry = {'argv': argv, 'stdin': ''}\n"
        "if argv[:2] == ['secret', 'create']:\n"
        "    entry['stdin'] = sys.stdin.read()\n"
        f"if argv[:2] == ['secret', 'ls']:\n"
        f"    print(chr(10).join({list(existing)!r}))\n"
        "if argv[:1] == ['info']:\n"
        "    print('true')\n"
        "with log.open('a') as fh:\n"
        "    fh.write(json.dumps(entry) + chr(10))\n"
    )
    stub.chmod(0o755)
    return bindir, log


def calls(log):
    if not os.path.exists(log):
        return []
    return [json.loads(line) for line in open(log) if line.strip()]


def values_file(tmp_path, extra=""):
    path = tmp_path / "vm-secrets.env"
    body = "# git-ignored values file\n"
    body += f"o2_user={O2_USER}\n"
    body += f"o2_password={O2_PASSWORD}\n"
    body += "".join(f"{k}={v}\n" for k, v in SUPPLIED.items())
    body += extra
    path.write_text(body)
    return str(path)


def run(tmp_path, *args, existing=(), path_env=True):
    bindir, log = fake_docker(tmp_path, existing=existing)
    env = dict(os.environ)
    env["PATH"] = f"{bindir}{os.pathsep}{env['PATH']}" if path_env else env["PATH"]
    result = subprocess.run(["bash", SCRIPT, *args], capture_output=True,
                            text=True, env=env, stdin=subprocess.DEVNULL)
    return result, log


def test_name_list_equals_the_stack_secret_block():
    """Parity with the stack — the drift that fails a deploy with 'secret not found'."""
    out = subprocess.run(["bash", SCRIPT, "--print-names"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    printed = {line.strip() for line in out.stdout.splitlines() if line.strip()}
    assert printed == stack_secret_names(), (
        f"script creates {sorted(printed)}; the stack declares "
        f"{sorted(stack_secret_names())}")
    assert len(printed) == 9


def test_a_missing_supplied_value_creates_nothing(tmp_path):
    """Half a secret set is worse than none: it looks deployed and fails at runtime."""
    path = tmp_path / "partial.env"
    path.write_text("arrow_app_secret=only-one\n")

    result, log = run(tmp_path, "--values-file", str(path))
    assert result.returncode == 3
    assert "arrow_password" in (result.stdout + result.stderr)
    assert not [c for c in calls(log) if c["argv"][:2] == ["secret", "create"]], (
        "a refused run must not create any secret")


def test_every_value_travels_on_stdin_and_never_in_argv(tmp_path):
    """The security property: no secret value may be visible to `ps`."""
    result, log = run(tmp_path, "--values-file", values_file(tmp_path))
    assert result.returncode == 0, result.stdout + result.stderr

    creates = [c for c in calls(log) if c["argv"][:2] == ["secret", "create"]]
    assert len(creates) == 9
    for call in creates:
        assert call["argv"][-1] == "-", f"secret not read from stdin: {call['argv']}"
        assert call["stdin"], f"no value reached stdin for {call['argv'][2]}"
        joined = " ".join(call["argv"])
        for value in SUPPLIED.values():
            assert value not in joined, f"value leaked into argv of {call['argv']}"


def test_supplied_values_reach_the_right_secrets(tmp_path):
    result, log = run(tmp_path, "--values-file", values_file(tmp_path))
    assert result.returncode == 0, result.stdout + result.stderr
    created = {c["argv"][2]: c["stdin"] for c in calls(log)
               if c["argv"][:2] == ["secret", "create"]}

    for name, value in SUPPLIED.items():
        assert created[name] == value, f"{name} did not receive its supplied value"


def test_o2_auth_basic_is_bare_base64_with_no_basic_prefix(tmp_path):
    """otel reads `Authorization: "Basic ${file:...}"`, so the file must NOT
    repeat the scheme — `Basic Basic …` is a 401 with no useful error."""
    result, log = run(tmp_path, "--values-file", values_file(tmp_path))
    assert result.returncode == 0, result.stdout + result.stderr
    created = {c["argv"][2]: c["stdin"] for c in calls(log)
               if c["argv"][:2] == ["secret", "create"]}

    password = created["o2_password"]
    assert password
    expected = base64.b64encode(f"{O2_USER}:{password}".encode()).decode()
    assert created["o2_auth_basic"] == expected
    assert not created["o2_auth_basic"].startswith("Basic ")


def test_generated_secrets_are_distinct_and_non_empty(tmp_path):
    """The two internal tokens: each unique, neither empty, both long enough."""
    result, log = run(tmp_path, "--values-file", values_file(tmp_path))
    assert result.returncode == 0, result.stdout + result.stderr
    created = {c["argv"][2]: c["stdin"] for c in calls(log)
               if c["argv"][:2] == ["secret", "create"]}

    generated = [created[n] for n in
                 ("execution_bridge_auth_token", "gateway_shared_secret")]
    assert all(len(v) >= 32 for v in generated), "generated values are too short"
    assert len(set(generated)) == 2, "generated values must not repeat"
    assert created["o2_password"] == O2_PASSWORD, (
        "o2_password must be the supplied value, never a generated one: the stack "
        "passes it to OpenObserve as ZO_ROOT_USER_PASSWORD from the deploy env")


def test_o2_password_must_be_supplied_and_meet_the_policy(tmp_path):
    """OpenObserve panics at startup on a weak password, so refuse it here.

    Measured in the rehearsal: `ZO_ROOT_USER_PASSWORD is too weak: Password must
    be 8-128 characters and contain at least one lowercase letter, one uppercase
    letter, one digit, an[d special char]`, then `backend job init failed: channel
    closed` — the container never serves. A hex value (what this script used to
    generate) fails that policy because it has no uppercase and no special.
    """
    missing = tmp_path / "no-o2-value.env"
    missing.write_text("o2_user=" + O2_USER + "\n"
                       + "".join(f"{k}={v}\n" for k, v in SUPPLIED.items()))
    result, log = run(tmp_path, "--values-file", str(missing))
    assert result.returncode == 3, result.stdout + result.stderr
    assert "o2_password" in (result.stdout + result.stderr)
    assert not [c for c in calls(log) if c["argv"][:2] == ["secret", "create"]]

    for weak in ("f" * 64, "Lowercase1", "NoDigitHere!", "Sh0rt!"):
        result, log = run(tmp_path, "--values-file",
                          values_file(tmp_path, extra=f"o2_password={weak}\n"))
        assert result.returncode == 3, f"accepted the weak o2_password {weak!r}"
        assert "policy" in (result.stdout + result.stderr)
        assert not [c for c in calls(log) if c["argv"][:2] == ["secret", "create"]]

    result, log = run(tmp_path, "--values-file", values_file(tmp_path))
    assert result.returncode == 0, result.stdout + result.stderr
    created = {c["argv"][2]: c["stdin"] for c in calls(log)
               if c["argv"][:2] == ["secret", "create"]}
    assert created["o2_password"] == O2_PASSWORD


def test_a_supplied_generated_secret_wins_over_generation(tmp_path):
    """An operator rotating a token by hand must not be second-guessed."""
    pinned = "gateway_shared_secret=" + "f" * 48 + "\n"
    result, log = run(tmp_path, "--values-file", values_file(tmp_path, extra=pinned))
    assert result.returncode == 0, result.stdout + result.stderr
    created = {c["argv"][2]: c["stdin"] for c in calls(log)
               if c["argv"][:2] == ["secret", "create"]}
    assert created["gateway_shared_secret"] == "f" * 48


def test_an_existing_secret_refuses_the_whole_run(tmp_path):
    """Swarm secret values are immutable: silently skipping would hide stale data."""
    result, log = run(tmp_path, "--values-file", values_file(tmp_path),
                      existing=["aws_access_key_id"])
    assert result.returncode == 4
    assert "aws_access_key_id" in (result.stdout + result.stderr)
    assert not [c for c in calls(log) if c["argv"][:2] == ["secret", "create"]]


def test_check_reports_missing_secrets_by_name(tmp_path):
    result, _ = run(tmp_path, "--check")
    assert result.returncode != 0
    out = result.stdout + result.stderr
    for name in sorted(stack_secret_names()):
        assert name in out, f"--check should list the missing secret {name}"


def test_check_passes_when_all_nine_exist(tmp_path):
    result, _ = run(tmp_path, "--check", existing=sorted(stack_secret_names()))
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS" in result.stdout


def test_no_secret_value_is_ever_printed(tmp_path):
    """A value in the log would defeat the point of using Swarm secrets."""
    result, _ = run(tmp_path, "--values-file", values_file(tmp_path))
    combined = result.stdout + result.stderr
    for value in SUPPLIED.values():
        assert value not in combined, "the script printed a supplied value"


def test_refuses_without_a_usable_swarm_manager(tmp_path):
    """Creating a secret needs a manager; a worker must be refused clearly."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    st = bindir / "docker"
    st.write_text("#!/usr/bin/env bash\n"
                  "if [ \"$1\" = info ]; then echo false; exit 0; fi\n"
                  "exit 0\n")
    st.chmod(0o755)
    env = dict(os.environ)
    env["PATH"] = f"{bindir}{os.pathsep}{env['PATH']}"
    result = subprocess.run(["bash", SCRIPT, "--values-file", values_file(tmp_path)],
                            capture_output=True, text=True, env=env,
                            stdin=subprocess.DEVNULL)
    assert result.returncode == 2
    assert "manager" in (result.stdout + result.stderr).lower()


def test_self_check_proves_parity_and_derivation_offline():
    """House pattern: every script ships a --self-check that needs no daemon."""
    out = subprocess.run(["bash", SCRIPT, "--self-check"], capture_output=True, text=True)
    assert out.returncode == 0, out.stdout + out.stderr
    assert "[PASS]" in out.stdout
    assert "GENERATED (on this host)" in out.stdout


def test_values_file_can_arrive_over_stdin_and_never_touch_disk(tmp_path):
    """The documented workflow pipes the file over SSH. If this breaks, the
    fallback is copying secret values onto a VM's disk — exactly what is banned."""
    bindir, log = fake_docker(tmp_path)
    env = dict(os.environ)
    env["PATH"] = f"{bindir}{os.pathsep}{env['PATH']}"
    body = "o2_user=" + O2_USER + "\n" + "".join(f"{k}={v}\n" for k, v in SUPPLIED.items())
    body += f"o2_password={O2_PASSWORD}\n"

    result = subprocess.run(["bash", SCRIPT, "--values-file", "/dev/stdin"],
                            input=body, capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    created = {c["argv"][2]: c["stdin"] for c in calls(log)
               if c["argv"][:2] == ["secret", "create"]}
    assert len(created) == 9
    for name, value in SUPPLIED.items():
        assert created[name] == value, f"{name} lost its value through the pipe"
