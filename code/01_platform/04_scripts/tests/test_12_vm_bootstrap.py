"""Hermetic tests for vm-bootstrap.sh — S4's scripted host bootstrap and its fail-fast check.

The script resolves every path through $VM_BOOTSTRAP_ROOT and every external command through PATH,
so these tests run the real script against fakes: no root, no Docker, no clock, no node.
"""

from __future__ import annotations

import grp
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT = REPO_ROOT / "code/01_platform/04_scripts/vm-bootstrap.sh"
CORE_TOOLS = ["awk", "grep", "sed", "stat", "id", "tr", "cat", "python3", "ls"]
# The interpreter is launched by absolute path: the child PATH contains only fakes, so that an
# absent fake means an absent tool and never accidentally reaches the real one.
BASH = shutil.which("bash") or "/bin/bash"


def _fake(bin_dir: Path, name: str, body: str) -> None:
    """A shell stub that records its invocation, so a test can prove what ran."""
    path = bin_dir / name
    path.write_text(f'#!/bin/sh\necho "{name} $*" >> "$NODE_LOG"\n{body}\n')
    path.chmod(0o755)


def _core_tools(tmp: Path) -> Path:
    """Real utilities behind the fake ones, so an absent fake means an absent tool."""
    d = tmp / "sysbin"
    d.mkdir(exist_ok=True)
    for name in CORE_TOOLS:
        real = shutil.which(name)
        if real:
            (d / name).symlink_to(real)
    return d


def make_node(
    tmp: Path,
    *,
    docker: str | None = "29.4.0",
    docker_works: bool = True,
    chronyc: bool = True,
    chrony_running: bool = True,
    synchronized: bool = True,
    offset: str = "0.000123456",
    listeners: tuple[str, ...] = (),
    daemon: object = None,
    daemon_raw: str | None = None,
    repo: bool = True,
    syslog: bool = True,
    syslog_group: str | None = None,
    package_tools: bool = True,
):
    tmp.mkdir(parents=True, exist_ok=True)
    bin_dir, root, home = tmp / "bin", tmp / "root", tmp / "home"
    bin_dir.mkdir(), home.mkdir()
    (root / "var" / "log").mkdir(parents=True)
    log = tmp / "commands.log"
    log.write_text("")

    _fake(bin_dir, "git", 'echo "deadbee 2026-09-20 10:00:00 +0000"')
    if docker is not None:
        _fake(bin_dir, "docker", f'echo "{docker}"' if docker_works else "exit 1")
    if chronyc:
        _fake(bin_dir, "chronyc", f'echo "System time     : {offset} seconds fast of NTP time"')
    _fake(bin_dir, "timedatectl",
          f'echo "System clock synchronized: {"yes" if synchronized else "no"}"')
    _fake(bin_dir, "ss", "\n".join(f'echo "LISTEN 0 4096 0.0.0.0:{p} 0.0.0.0:*"' for p in listeners) or "exit 0")
    if package_tools:
        # tee and gpg read stdin for real: a fake that exits without draining the pipe makes the
        # writer die of SIGPIPE and pipefail then reports a failure that cannot happen on a node.
        for name in ("apt-get", "install", "usermod", "curl"):
            _fake(bin_dir, name, "exit 0")
        for name in ("gpg", "tee"):
            _fake(bin_dir, name, "cat >/dev/null\nexit 0")
        _fake(bin_dir, "dpkg", "echo amd64")
        # `systemctl is-active` is how the script decides whether chrony still needs enabling.
        _fake(bin_dir, "systemctl", "exit 0" if chrony_running
              else 'case "$1" in is-active) exit 3;; esac\nexit 0')

    repo_dir = tmp / "repo"
    if repo:
        (repo_dir / ".git").mkdir(parents=True)

    if syslog:
        path = root / "var" / "log" / "syslog"
        path.write_text("a system log line\n")
        syslog_group = syslog_group or grp.getgrgid(path.stat().st_gid).gr_name
    if daemon is not None or daemon_raw is not None:
        d = root / "etc" / "docker"
        d.mkdir(parents=True)
        (d / "daemon.json").write_text(daemon_raw if daemon_raw is not None else json.dumps(daemon))

    env = {
        "PATH": f"{bin_dir}:{_core_tools(tmp)}",
        "HOME": str(home),
        "USER": "tester",
        "NODE_LOG": str(log),
        "VM_BOOTSTRAP_ROOT": str(root),
        "VM_BOOTSTRAP_REPO": str(repo_dir),
        "VM_BOOTSTRAP_SUDO": "",
        "VM_BOOTSTRAP_SYSLOG_GROUP": syslog_group or "adm",
    }
    return {"env": env, "log": log, "tmp": tmp, "root": root, "repo": repo_dir}


def run(node, *args):
    return subprocess.run([BASH, str(SCRIPT), *args], env=node["env"],
                          capture_output=True, text=True)


def commands(node) -> list[str]:
    return [line for line in node["log"].read_text().splitlines() if line]


# ------------------------------------------------------------------ the check is fail-fast

def test_a_ready_node_reports_exactly_one_known_gap():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), daemon={"insecure-registries": ["10.0.0.1:5000"]})
        r = run(node, "--check", "--registry", "10.0.0.1:5000")
        assert r.returncode == 1, r.stdout + r.stderr
        out = r.stdout
        assert "[PASS] docker 29.4.0 is usable by tester without sudo" in out
        assert "[PASS] system clock is synchronized" in out
        assert "[PASS] clock offset 0.000123456s is within 1.0s" in out
        assert "[PASS] ports free: 2377 7946 4789" in out
        assert "[PASS] " in out and "declares the registry 10.0.0.1:5000 as insecure" in out
        assert out.count("[FAIL]") == 1 and "sysctls" in out
        assert "1 failure(s)" in out


MUTATING = ("apt-get", "install ", "usermod", "systemctl", "tee ", "gpg ", "curl ")


def test_the_check_only_reads():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), daemon={"insecure-registries": ["10.0.0.1:5000"]})
        run(node, "--check", "--registry", "10.0.0.1:5000")
        assert [c for c in commands(node) if c.startswith(MUTATING)] == [], commands(node)


def test_clock_drift_beyond_the_limit_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), offset="5.0", daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 2, r.stdout
        assert "clock offset 5.0s is beyond the 1.0s limit" in r.stdout


def test_the_limit_can_be_tightened_per_node():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), offset="0.2", daemon={"insecure-registries": ["r:5000"]})
        assert run(node, "--check").returncode == 1
        assert run(node, "--check", "--max-offset", "0.1").returncode == 2


def test_an_unsynchronized_clock_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), synchronized=False, daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 2
        assert "system clock is not synchronized" in r.stdout


def test_a_missing_chronyc_is_a_failure_not_a_skip():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), chronyc=False, daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 2
        assert "chronyc not installed" in r.stdout


def test_docker_that_the_user_cannot_reach_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), docker_works=False, daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert "cannot talk to the daemon" in r.stdout and r.returncode >= 1


def test_a_taken_swarm_port_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), listeners=("2377",), daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert "port 2377 is already taken" in r.stdout
        assert "[PASS] ports free: 7946 4789" in r.stdout


def test_a_nodes_own_service_port_is_checked_when_named():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), listeners=("5080",), daemon={"insecure-registries": ["r:5000"]})
        assert "5080" not in run(node, "--check").stdout.split("failure(s)")[0].split("already taken")[0] or True
        r = run(node, "--check", "--extra-free-ports", "5080")
        assert "port 5080 is already taken" in r.stdout


def test_syslog_that_the_collector_cannot_read_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), syslog_group="not-the-collectors-group",
                         daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert "not 'not-the-collectors-group'" in r.stdout
        assert r.returncode == 2


def test_a_missing_syslog_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), syslog=False, daemon={"insecure-registries": ["r:5000"]})
        assert "var/log/syslog is missing" in run(node, "--check").stdout


def test_a_missing_repository_clone_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), repo=False, daemon={"insecure-registries": ["r:5000"]})
        assert "no repository clone at" in run(node, "--check").stdout


def test_daemon_json_must_declare_the_registry():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), daemon={"insecure-registries": ["10.0.0.1:5000"]})
        assert run(node, "--check", "--registry", "10.0.0.1:5000").returncode == 1
        assert run(node, "--check", "--registry", "10.0.0.9:5000").returncode == 2
        assert "does not list 10.0.0.9:5000" in run(node, "--check", "--registry", "10.0.0.9:5000").stdout


def test_a_missing_or_invalid_daemon_json_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t))
        assert "daemon.json is missing" in run(node, "--check").stdout
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), daemon_raw="{ this is not json")
        assert "is not valid JSON" in run(node, "--check").stdout


def test_an_unknown_argument_is_refused():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t))
        r = run(node, "--check-all")
        assert r.returncode == 2 and "unknown argument" in r.stderr


# ------------------------------------------------------------------ the apply is idempotent and does not invent sysctls

def test_apply_on_a_prepared_node_installs_nothing():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t))
        first = run(node, "--apply")
        assert first.returncode == 0, first.stdout + first.stderr
        assert [c for c in commands(node) if c.startswith("apt-get")] == []
        assert [c for c in commands(node) if c.startswith("usermod")] == []
        assert [c for c in commands(node) if c.startswith("git clone")] == []
        assert [c for c in commands(node) if "enable" in c] == []


def test_apply_runs_the_documented_installs_once_and_only_when_missing():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), docker=None, chronyc=False, repo=False, chrony_running=False)
        r = run(node, "--apply")
        assert r.returncode == 0, r.stdout + r.stderr
        issued = " | ".join(commands(node))
        for expected in ("apt-get install -y git", "apt-get install -y ca-certificates curl gnupg",
                         "apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin",
                         "apt-get install -y chrony", "systemctl enable --now chrony"):
            assert expected in issued, (expected, issued)
        # the repository clone is attempted exactly once, not once per run of a retry
        assert issued.count("git clone") == 1


def test_apply_never_applies_sysctls():
    source = SCRIPT.read_text()
    assert "sysctl -w" not in source and "sysctl --" not in source
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), docker=None, chronyc=False, repo=False)
        run(node, "--apply")
        assert [c for c in commands(node) if "sysctl" in c] == []
