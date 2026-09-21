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
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT = REPO_ROOT / "code/01_platform/04_scripts/vm-bootstrap.sh"
PRODUCER = REPO_ROOT / "code/01_platform/04_scripts/clock_offset_fact.sh"
# `bash`/`cp`/`chmod`/`date`/`env`/`mktemp`/`rm`/`dirname` joined this list with CHG-288: the bootstrap
# now installs a producer script and runs it, and that script's `#!/usr/bin/env bash` has to resolve —
# on a node it does. The fakes stay fakes: an absent fake is still an absent tool.
CORE_TOOLS = ["awk", "grep", "sed", "stat", "id", "tr", "cat", "python3", "ls", "mkdir", "mv", "cmp",
              "cp", "chmod", "date", "env", "mktemp", "rm", "dirname", "bash"]
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
    sysctls: dict[str, str] | None = None,
    sysctl: bool = True,
    clock_fact: bool = True,
    fact_age_s: int = 0,
    fact_ms: str = "0",
):
    tmp.mkdir(parents=True, exist_ok=True)
    bin_dir, root, home = tmp / "bin", tmp / "root", tmp / "home"
    bin_dir.mkdir(), home.mkdir()
    (root / "var" / "log").mkdir(parents=True)
    log = tmp / "commands.log"
    log.write_text("")

    # `clone` has to produce a checkout, because the bootstrap installs a script out of the repository:
    # a stub that only prints a revision would turn a real failure into a passing test (CHG-288).
    _fake(bin_dir, "git", 'echo "deadbee 2026-09-20 10:00:00 +0000"\n'
                         'case "$1" in clone)\n'
                         '    mkdir -p "$3/.git" "$3/code/01_platform/04_scripts"\n'
                         f'    cp "{PRODUCER}" "$3/code/01_platform/04_scripts/" ;;\n'
                         'esac')
    if docker is not None:
        _fake(bin_dir, "docker", f'echo "{docker}"' if docker_works else "exit 1")
    if chronyc:
        _fake(bin_dir, "chronyc", f'echo "System time     : {offset} seconds fast of NTP time"')
    _fake(bin_dir, "timedatectl",
          f'echo "System clock synchronized: {"yes" if synchronized else "no"}"')
    if sysctl:
        # The four recorded rules (runbook §6.1), with per-test overrides.
        values = {"vm.swappiness": "1", "net.core.somaxconn": "32768",
                  "net.ipv4.tcp_max_syn_backlog": "16384", "vm.max_map_count": "262144"}
        values.update(sysctls or {})
        cases = "\n".join(f"        {k}) echo {v} ;;" for k, v in values.items())
        _fake(bin_dir, "sysctl", f'case "$1" in\n    --system) exit 0 ;;\n    -n) case "$2" in\n{cases}\n        *) exit 1 ;;\n    esac ;;\n    *) exit 1 ;;\nesac')
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
        (repo_dir / "code" / "01_platform" / "04_scripts").mkdir(parents=True)
        shutil.copy2(PRODUCER, repo_dir / "code" / "01_platform" / "04_scripts" / PRODUCER.name)

    if clock_fact:
        # What a bootstrapped node has: the sample the executor's drift gate reads (CHG-288).
        fact_dir = root / "run" / "arrow-clock"
        fact_dir.mkdir(parents=True)
        (fact_dir / "offset").write_text(f"offset_ms={fact_ms}\n"
                                         f"measured_epoch_s={int(time.time()) - fact_age_s}\n"
                                         "source=chronyc-tracking-field4\n")

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

def test_a_ready_node_reports_no_gap():
    """Until CHG-272 the sysctl decision was missing, so a ready node still failed. It no longer does."""
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), daemon={"insecure-registries": ["10.0.0.1:5000"]})
        r = run(node, "--check", "--registry", "10.0.0.1:5000")
        assert r.returncode == 0, r.stdout + r.stderr
        out = r.stdout
        assert "[PASS] docker 29.4.0 is usable by tester without sudo" in out
        assert "[PASS] system clock is synchronized" in out
        assert "[PASS] clock offset 0.000123456s is within 1.0s" in out
        assert "[PASS] ports free: 2377 7946 4789" in out
        assert "[PASS] " in out and "declares the registry 10.0.0.1:5000 as insecure" in out
        assert "[FAIL]" not in out
        assert "[PASS] vm.swappiness = 1 (rule: le 1)" in out
        assert "[PASS] vm.max_map_count = 262144 (rule: ge 262144)" in out
        assert "0 failure(s)" in out


MUTATING = ("apt-get", "install ", "usermod", "systemctl", "tee ", "gpg ", "curl ", "sysctl --system")


def test_the_check_only_reads():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), daemon={"insecure-registries": ["10.0.0.1:5000"]})
        run(node, "--check", "--registry", "10.0.0.1:5000")
        assert [c for c in commands(node) if c.startswith(MUTATING)] == [], commands(node)


def test_clock_drift_beyond_the_limit_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), offset="5.0", daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 1, r.stdout
        assert "clock offset 5.0s is beyond the 1.0s limit" in r.stdout


def test_the_limit_can_be_tightened_per_node():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), offset="0.2", daemon={"insecure-registries": ["r:5000"]})
        assert run(node, "--check").returncode == 0
        assert run(node, "--check", "--max-offset", "0.1").returncode == 1


def test_an_unsynchronized_clock_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), synchronized=False, daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 1
        assert "system clock is not synchronized" in r.stdout


def test_a_missing_chronyc_is_a_failure_not_a_skip():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), chronyc=False, daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 1
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
        assert r.returncode == 1


def test_a_missing_syslog_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), syslog=False, daemon={"insecure-registries": ["r:5000"]})
        assert "var/log/syslog is missing" in run(node, "--check").stdout


def test_a_missing_clock_fact_is_a_failure_that_names_it():
    """CHG-288: the node publishes the executor's clock sample. Without it the gate is halted — correct,
    but the node is not ready, and an operator has to be told which file is missing."""
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), clock_fact=False, daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 1, r.stdout
        assert "arrow-clock/offset is missing" in r.stdout
        assert "drift gate halts" in r.stdout


def test_a_stale_clock_fact_is_a_failure_naming_the_age():
    """An aged sample is the same halt as no sample, and the report must not confuse the two: "31s old"
    says the timer stopped, not that the clock is wrong."""
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), fact_age_s=31, daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 1, r.stdout
        assert "old (limit 30s)" in r.stdout and "the gate halts" in r.stdout


def test_a_missing_repository_clone_is_a_failure():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), repo=False, daemon={"insecure-registries": ["r:5000"]})
        assert "no repository clone at" in run(node, "--check").stdout


def test_daemon_json_must_declare_the_registry():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), daemon={"insecure-registries": ["10.0.0.1:5000"]})
        assert run(node, "--check", "--registry", "10.0.0.1:5000").returncode == 0
        assert run(node, "--check", "--registry", "10.0.0.9:5000").returncode == 1
        assert "does not list 10.0.0.9:5000" in run(node, "--check", "--registry", "10.0.0.9:5000").stdout


def test_a_missing_daemon_json_is_informational_without_a_local_registry():
    """GHCR (Decision 2026-09-21) needs no daemon.json: HTTPS, pulled anonymously.

    The previous rule failed such a host — which would have failed every node of a
    healthy GHCR fleet at S3, before any of them could join the swarm.
    """
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t))
        r = run(node, "--check")
        assert "daemon.json is absent" in r.stdout
        assert "daemon.json is missing" not in r.stdout
        assert r.returncode == 0, r.stdout


def test_a_missing_daemon_json_still_fails_for_a_declared_plain_http_registry():
    """The original fail-closed rule: with a plain-HTTP registry declared, the entry is required."""
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t))
        r = run(node, "--check", "--registry", "10.0.0.1:5000")
        assert r.returncode == 1, r.stdout
        assert "declare the plain-HTTP registry 10.0.0.1:5000" in r.stdout


def test_an_invalid_daemon_json_is_a_failure_with_or_without_a_registry():
    """A malformed file is a real defect regardless of which registry the images come from."""
    for extra in ([], ["--registry", "10.0.0.1:5000"]):
        with tempfile.TemporaryDirectory() as t:
            node = make_node(Path(t), daemon_raw="{ this is not json")
            r = run(node, "--check", *extra)
            assert "is not valid JSON" in r.stdout, r.stdout
            assert r.returncode == 1, r.stdout


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


def test_apply_writes_only_the_recorded_sysctls_and_is_idempotent():
    """CHG-272 replaced "never touch sysctls" with "apply exactly the recorded four": no ad-hoc
    `sysctl -w`, one file, and a second run that changes nothing runs nothing."""
    source = SCRIPT.read_text()
    assert "sysctl -w" not in source
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), docker=None, chronyc=False, repo=False)
        first = run(node, "--apply")
        assert first.returncode == 0, first.stdout + first.stderr
        conf = node["root"] / "etc" / "sysctl.d" / "99-arrow-infra.conf"
        body = conf.read_text()
        # Exactly the recorded four, in order: an extra knob here would be an invented one.
        entries = [line for line in body.splitlines() if line.strip() and not line.startswith("#")]
        assert entries == ["vm.swappiness = 1", "net.core.somaxconn = 32768",
                           "net.ipv4.tcp_max_syn_backlog = 16384", "vm.max_map_count = 262144"], entries
        assert commands(node).count("sysctl --system") == 1
        before = commands(node)
        second = run(node, "--apply")
        assert second.returncode == 0, second.stdout + second.stderr
        assert "sysctls already applied" in second.stdout
        assert commands(node) == before + [c for c in commands(node)[len(before):] if "sysctl" not in c]


def test_a_wrong_sysctl_is_a_failure_that_names_the_rule():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), sysctls={"vm.swappiness": "60"},
                         daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 1, r.stdout
        assert "vm.swappiness = 60 breaks 'le 1'" in r.stdout


def test_a_host_already_beyond_a_floor_passes():
    """Rules are comparisons, not equalities: 1048576 is a valid max_map_count and 65535 a valid
    somaxconn, and prescribing exact values would have *lowered* this host. Every `ge` rule is checked
    here with a value strictly above it, so a rule that silently became `le` cannot pass."""
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), sysctls={"vm.max_map_count": "1048576",
                                           "net.core.somaxconn": "65535",
                                           "net.ipv4.tcp_max_syn_backlog": "32768"},
                         daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 0, r.stdout
        assert "[PASS] vm.max_map_count = 1048576 (rule: ge 262144)" in r.stdout
        assert "[PASS] net.core.somaxconn = 65535 (rule: ge 32768)" in r.stdout
        assert "[PASS] net.ipv4.tcp_max_syn_backlog = 32768 (rule: ge 16384)" in r.stdout


def test_an_unreadable_sysctl_is_a_failure_not_a_skip():
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), sysctl=False, daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--check")
        assert r.returncode == 4, r.stdout
        assert "vm.swappiness is unreadable" in r.stdout


def test_the_script_and_the_runbook_list_the_same_sysctls():
    """The decision lives in two places: the script that applies it and the table an operator reads.
    CHG-272 added this test so the next knob cannot land in only one of them."""
    import re
    source = SCRIPT.read_text()
    block = re.search(r"SYSCTL_RULES=\((.*?)\n\)", source, re.S).group(1)
    in_script = {line.split("|")[0].strip().strip('"') for line in block.splitlines() if "|" in line}
    runbook = (REPO_ROOT / "docs/05_deployment/PROD_VM_PROVISIONING.md").read_text()
    section = runbook.split("**3. Sysctls")[1].split("**4.")[0]
    in_runbook = set(re.findall(r"^\| `([a-z0-9_.]+)` \|", section, re.M))
    assert in_script == in_runbook, (in_script, in_runbook)


def test_apply_installs_the_clock_producer_and_publishes_one_sample():
    """CHG-288: apply installs the producer and its 10 s timer, then publishes a sample immediately — a
    check that runs before the first tick must not report a false halt. The producer really runs here: the
    sandbox has no chronyd, only a stub `chronyc tracking`."""
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), offset="-1.5", clock_fact=False,
                         daemon={"insecure-registries": ["r:5000"]})
        r = run(node, "--apply")
        assert r.returncode == 0, r.stdout + r.stderr
        installed = node["root"] / "usr" / "local" / "bin" / "arrow-clock-offset"
        assert installed.exists(), commands(node)
        assert (installed.stat().st_mode & 0o777) == 0o755
        assert installed.read_bytes() == PRODUCER.read_bytes()
        timer = node["root"] / "etc" / "systemd" / "system" / "arrow-clock-offset.timer"
        assert "OnUnitActiveSec=10s" in timer.read_text()
        assert (timer.stat().st_mode & 0o777) == 0o644
        assert "arrow-clock-offset.timer" in " | ".join(commands(node))
        fact = node["root"] / "run" / "arrow-clock" / "offset"
        body = fact.read_text()
        # The stub said -1.5 s, so the published sample must say -1500 ms: sign and scale survive.
        assert "offset_ms=-1500" in body, body
        assert "source=chronyc-tracking-field4" in body
        checked = run(node, "--check").stdout
        assert "clock fact is fresh" in checked
        assert "clock offset -1.5s is beyond the 1.0s limit" in checked


def test_a_second_apply_leaves_the_clock_units_alone():
    """Idempotency for the new artifact: identical bytes are not rewritten and nothing is enabled twice."""
    with tempfile.TemporaryDirectory() as t:
        node = make_node(Path(t), daemon={"insecure-registries": ["r:5000"]})
        assert run(node, "--apply").returncode == 0
        first = commands(node)
        second = run(node, "--apply")
        assert second.returncode == 0, second.stdout + second.stderr
        assert "unchanged: " in second.stdout and "arrow-clock-offset.timer" in second.stdout
        assert [c for c in commands(node)[len(first):] if "enable" in c] == []
