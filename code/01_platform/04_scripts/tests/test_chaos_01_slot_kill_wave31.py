"""Wave-31 regression tests for chaos-01-slot-kill.sh (P6-041, 042, 327, 328, 329, 330, 717).

The drill is a shell script that drives the Go toolchain, so this suite runs the
REAL entry point (`bash chaos-01-slot-kill.sh`) with `go` shimmed through a PATH
prefix.  The shim records every argv it is handed and serves scripted stdout /
exit codes per test name, so the state under test is this script's decisions and
no Go test is compiled or run.

`W31_TREE` points at a fake repo root holding
`code/01_platform/04_scripts/chaos/chaos-01-slot-kill.sh` (and, for the config
test, no `go-bridge`); it defaults to the real repository and exists so the red
leg can run this same suite against the pre-wave copy.

Red leg: every assertion marked `# disc` fails against the pre-wave script.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


def _default_tree() -> Path:
    """The repo root: nearest ancestor holding the chaos scripts."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "code" / "01_platform" / "04_scripts" / "chaos").is_dir():
            return parent
    return here.parent


TREE = Path(os.environ.get("W31_TREE", _default_tree()))
SCRIPT_REL = Path("code/01_platform/04_scripts/chaos/chaos-01-slot-kill.sh")
SCRIPT = TREE / SCRIPT_REL

# Resolved from the outer environment: a test that empties PATH must still be
# able to start bash.
BASH = shutil.which("bash") or "/usr/bin/bash"
GO = shutil.which("go") or ""

PREFIX = "SLOT-KILL-CHAOS-01"
PASS_LINE = f"{PREFIX}: PASS —"

GATES = [
    ("TestSubscriptionPlanShards3000Tokens", "sharding"),
    ("TestSupervisorAuthTerminalIsolatedPerSlot", "terminal isolated"),
    ("TestINGRES001HealthySlotNotInterruptedByPeerReconnect", "healthy slot not interrupted"),
    ("TestReconnectLoopRecoversAfterFailures", "reconnect recovers"),
    ("TestReconnectLoopEpochAndBackoffAfterForcedDisconnect", "epoch and backoff"),
    ("TestINGRES001OneHundredForcedDisconnectReconnectCycles", "100 cycles"),
]

# `go` shim: records argv, then serves per-test stdout/exit code from GO_OUT.
# Handles both the pre-wave invocation (`test -run Name -count 1 -v`) and the
# fixed one (`test -timeout T -run ^Name$ -count 1 -v ./...`), so one shim
# covers the red and the green leg.
GO_SHIM = r"""#!/usr/bin/env bash
# Records one self-describing line per invocation, then serves scripted stdout
# and an exit code from GO_OUT. Handles both the pre-wave invocation shape
# (`test -run Name -count 1 -v`) and the fixed one
# (`test -timeout T -run ^Name$ -count 1 -v ./...`), so one shim covers the red
# and the green leg.
run_re=""; mode="test-like"; timeout_seen="0"; scope_seen="0"
prev=""
for a in "$@"; do
  if [[ "${prev}" == "-run" ]]; then run_re="${a}"; fi
  case "${a}" in
    test) mode="test" ;;
    vet) mode="vet" ;;
    -timeout) timeout_seen="1" ;;
    ./...) scope_seen="1" ;;
  esac
  prev="${a}"
done
if [[ "${mode}" == "vet" ]]; then
  printf 'VET|timeout=%s|scope=%s|argv=%s\n' "${timeout_seen}" "${scope_seen}" "$*" >> "${GO_LOG}"
  if [[ -f "${GO_OUT}/vet.out" ]]; then command cat "${GO_OUT}/vet.out"; fi
  exit "$(command cat "${GO_OUT}/vet.rc" 2>/dev/null || printf '0')"
fi
name="${run_re#^}"
name="${name%$}"
printf 'TEST|name=%s|timeout=%s|scope=%s|regex=%s|argv=%s\n' \
  "${name}" "${timeout_seen}" "${scope_seen}" "${run_re}" "$*" >> "${GO_LOG}"
if [[ -f "${GO_OUT}/${name}.out" ]]; then command cat "${GO_OUT}/${name}.out"; fi
exit "$(command cat "${GO_OUT}/${name}.rc" 2>/dev/null || printf '0')"
"""


def _pass_output(name: str, seconds: str = "0.01s") -> str:
    return f"=== RUN   {name}\n--- PASS: {name} ({seconds})\nPASS\nok  \tgithub.com/trading/arrow-bridge\t0.020s\n"


def _farm(tmp: Path, names: list[str]) -> Path:
    """A PATH holding only symlinks to the named real binaries."""
    farm = tmp / "farm"
    farm.mkdir()
    for name in names:
        real = shutil.which(name)
        if real:
            (farm / name).symlink_to(real)
    return farm


class Chaos01Test(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="w31-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.shim_dir = self.tmp / "shim"
        self.shim_dir.mkdir()
        self.go_out = self.tmp / "go-out"
        self.go_out.mkdir()
        self.go_log = self.tmp / "go.log"
        shim = self.shim_dir / "go"
        shim.write_text(GO_SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    # ---- helpers ---------------------------------------------------------
    def script(self, tree: Path | None = None) -> Path:
        return (tree or TREE) / SCRIPT_REL

    def run_drill(
        self,
        *,
        tree: Path | None = None,
        pass_all: bool = True,
        per_gate: dict[str, tuple[str, int]] | None = None,
        vet: tuple[str, int] | None = None,
        env: dict[str, str] | None = None,
        path: Path | None = None,
    ) -> subprocess.CompletedProcess:
        for name, _ in GATES:
            out = ""
            if per_gate and name in per_gate:
                out = per_gate[name][0]
            elif pass_all:
                out = _pass_output(name)
            # A fixture the test wrote before calling run_drill wins: that is
            # how a single gate is scripted to fail or to skip itself.
            if out and not (self.go_out / f"{name}.out").exists():
                (self.go_out / f"{name}.out").write_text(out)
        if vet is not None:
            (self.go_out / "vet.out").write_text(vet[0])
            (self.go_out / "vet.rc").write_text(str(vet[1]))
        run_env = dict(os.environ)
        if path is not None:
            # An absence test: this PATH must be the ONLY one, or the real
            # binary on the outer PATH would answer `command -v`.
            run_env["PATH"] = str(path)
        else:
            run_env["PATH"] = str(self.shim_dir) + os.pathsep + os.environ.get("PATH", "")
        run_env["GO_LOG"] = str(self.go_log)
        run_env["GO_OUT"] = str(self.go_out)
        run_env.pop("CHAOS_GO_TEST_TIMEOUT", None)
        if env:
            run_env.update(env)
        return subprocess.run(
            [BASH, str(self.script(tree))],
            capture_output=True,
            text=True,
            env=run_env,
            timeout=120,
        )

    def records(self, kind: str) -> list[dict[str, str]]:
        """Every shim record of one kind (`TEST` or `VET`), in call order."""
        out = []
        for line in self.log().splitlines():
            if not line.startswith(f"{kind}|"):
                continue
            out.append(dict(f.split("=", 1) for f in line[len(kind) + 1:].split("|")))
        return out

    def log(self) -> str:
        return self.go_log.read_text() if self.go_log.exists() else ""

    def gate_records(self) -> list[dict[str, str]]:
        return self.records("TEST")

    def gate_names(self) -> list[str]:
        """The test name the shim parsed out of each `-run` regex, in call order."""
        return [r["name"] for r in self.gate_records()]

    def gate_argvs(self) -> list[str]:
        return [r["argv"] for r in self.gate_records()]

    # ---- green path ------------------------------------------------------
    def test_all_gates_pass_exit_0(self) -> None:
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(PASS_LINE, r.stdout)

    def test_every_gate_announced_and_run_once(self) -> None:
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        # exactly one go test invocation per gate, in inventory order
        self.assertEqual(self.gate_names(), [n for n, _ in GATES])
        for name, _ in GATES:
            self.assertIn(f"{PREFIX}: running {name}", r.stdout)

    def test_go_called_with_bridge_dir(self) -> None:
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertTrue(self.gate_argvs())
        for argv in self.gate_argvs():
            fields = argv.split()
            self.assertIn("-C", fields)
            self.assertTrue(fields[fields.index("-C") + 1].endswith("01_ingestion/go-bridge"), argv)

    def test_count_and_verbose_flags_kept(self) -> None:
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        for argv in self.gate_argvs():
            fields = argv.split()
            self.assertIn("-count", fields)
            self.assertEqual(fields[fields.index("-count") + 1], "1")
            self.assertIn("-v", fields)

    def test_pass_line_names_the_prefix(self) -> None:
        r = self.run_drill(vet=("", 0))
        self.assertIn(f"{PREFIX}: PASS —", r.stdout)

    # ---- P6-041: a gate that did not run is not a pass -------------------
    def test_renamed_test_is_fail_not_pass(self) -> None:  # disc
        """`go test -run NoMatch` exits 0 with `[no tests to run]`."""
        (self.go_out / "TestSubscriptionPlanShards3000Tokens.out").write_text(
            "PASS\nok  \tgithub.com/trading/arrow-bridge\t0.001s [no tests to run]\n"
        )
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertNotIn(PASS_LINE, r.stdout)
        self.assertIn("exited 0 without running", r.stderr)
        self.assertIn("no invariant was proven", r.stderr)

    def test_run_regex_is_anchored_for_every_gate(self) -> None:  # disc
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        relines = self.log().splitlines()
        for name, _ in GATES:
            # the shim records the name it parsed out of the -run regex; an
            # unanchored regex would also have matched a longer test name
            self.assertTrue(
                any(l.startswith(f"TEST|name={name}|") for l in relines),
                f"no go test invocation for {name}: {relines}",
            )
            # the recorded regex is exactly the anchored name, so a longer test
            # sharing the prefix cannot satisfy the gate
            self.assertEqual(
                [r["regex"] for r in self.gate_records() if r["name"] == name], [f"^{name}$"]
            )

    def test_package_scope_is_recursive(self) -> None:  # disc
        """Without ./... only BRIDGE_DIR's own package is searched."""
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        for argv in self.gate_argvs():
            self.assertIn("./...", argv.split(), argv)

    def test_go_test_timeout_is_passed(self) -> None:  # disc
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        for argv in self.gate_argvs():
            fields = argv.split()
            self.assertIn("-timeout", fields, argv)
            self.assertEqual(fields[fields.index("-timeout") + 1], "5m")

    def test_custom_timeout_reaches_go(self) -> None:  # disc
        r = self.run_drill(vet=("", 0), env={"CHAOS_GO_TEST_TIMEOUT": "90s"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        for argv in self.gate_argvs():
            fields = argv.split()
            self.assertEqual(fields[fields.index("-timeout") + 1], "90s")

    def test_empty_timeout_falls_back_to_the_default(self) -> None:  # disc
        """An empty knob is not a config error: `${VAR:-5m}` reads it as unset."""
        r = self.run_drill(vet=("", 0), env={"CHAOS_GO_TEST_TIMEOUT": ""})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        for argv in self.gate_argvs():
            self.assertEqual(argv.split()[argv.split().index("-timeout") + 1], "5m")

    def test_timeout_is_passed_to_every_gate(self) -> None:  # disc
        """No gate may run under Go's implicit default: the suite must be bounded."""
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(len(self.gate_records()), len(GATES))
        for rec in self.gate_records():
            self.assertEqual(rec["timeout"], "1", rec)

    def test_self_skipped_gate_is_skip_exit_3(self) -> None:  # disc
        name = "TestINGRES001HealthySlotNotInterruptedByPeerReconnect"
        (self.go_out / f"{name}.out").write_text(
            f"=== RUN   {name}\n    resilience_100_test.go:326: "
            "ING-RES-001: healthy-slot clause skipped in -short mode\n"
            f"--- SKIP: {name} (0.00s)\nPASS\nok  \tgithub.com/trading/arrow-bridge\t0.003s\n"
        )
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 3, r.stdout + r.stderr)
        self.assertIn(f"{PREFIX}: SKIP —", r.stderr)
        self.assertNotIn(PASS_LINE, r.stdout)

    def test_skip_reason_is_quoted(self) -> None:  # disc
        name = "TestINGRES001OneHundredForcedDisconnectReconnectCycles"
        (self.go_out / f"{name}.out").write_text(
            f"=== RUN   {name}\n    resilience_100_test.go:125: "
            "ING-RES-001: 100-cycle soak skipped in -short mode\n"
            f"--- SKIP: {name} (0.00s)\nPASS\nok  \tgithub.com/trading/arrow-bridge\t0.003s\n"
        )
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 3, r.stdout + r.stderr)
        self.assertIn("100-cycle soak skipped in -short mode", r.stderr)

    # ---- P6-327: config error, not six misleading test failures ----------
    def test_missing_bridge_dir_is_config_error(self) -> None:  # disc
        tree = self.tmp / "flat"
        (tree / SCRIPT_REL.parent).mkdir(parents=True)
        shutil.copy2(self.script(), tree / SCRIPT_REL)
        r = self.run_drill(tree=tree, vet=("", 0))
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("bridge dir not found", r.stderr)
        self.assertFalse(self.gate_argvs(), "go must not be called without a bridge dir")

    # ---- P6-330: vet is advisory ----------------------------------------
    def test_dirty_vet_is_advisory(self) -> None:  # disc
        r = self.run_drill(vet=("absorb.go:12:2: unused variable\n", 1))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("WARN", r.stderr)
        self.assertIn("unused variable", r.stderr)
        self.assertIn(PASS_LINE, r.stdout)

    def test_vet_result_is_named_in_pass_line(self) -> None:  # disc
        clean = self.run_drill(vet=("", 0))
        self.assertIn("go vet clean", clean.stdout)
        dirty = self.run_drill(vet=("unused.go:1:1: x\n", 1))
        self.assertIn("go vet DIRTY", dirty.stdout)

    # ---- P6-042: the PASS line does not claim a live kill ----------------
    def test_pass_line_does_not_claim_live_kill(self) -> None:  # disc
        r = self.run_drill(vet=("", 0))
        line = [l for l in r.stdout.splitlines() if l.startswith(PASS_LINE)][0]
        self.assertIn("no live slot kill", line)
        self.assertNotIn("1-of-3 slot kill:", line)
        self.assertNotIn("peers healthy, reconnect clean", line)

    # ---- P6-717: one inventory -------------------------------------------
    def test_pass_line_reports_gate_count(self) -> None:  # disc
        r = self.run_drill(vet=("", 0))
        self.assertIn(f"all {len(GATES)} Go gates", r.stdout)

    def test_single_inventory_and_one_call_site(self) -> None:  # disc
        text = self.script().read_text()
        self.assertEqual(text.count("GATES=("), 1)
        self.assertEqual(
            text.count('"${BRIDGE_DIR}" test'), 1, "one gate call site, driven by the inventory"
        )
        for name, _ in GATES:
            self.assertEqual(text.count(name), 1, f"{name} should appear only in the inventory")

    def test_gates_inventory_present(self) -> None:  # disc
        text = self.script().read_text()
        self.assertIn("GATES=(", text)
        for name, label in GATES:
            self.assertIn(f"{name}|{label}", text)

    # ---- unchanged contracts (pass before and after the wave) ------------
    def test_failed_gate_exits_1_and_names_label(self) -> None:
        name = "TestSupervisorAuthTerminalIsolatedPerSlot"
        (self.go_out / f"{name}.out").write_text(f"=== RUN   {name}\n--- FAIL: {name} (0.10s)\nFAIL\n")
        (self.go_out / f"{name}.rc").write_text("1")
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn(f"FAIL — terminal isolated", r.stderr)

    def test_failure_output_is_echoed(self) -> None:
        name = "TestReconnectLoopRecoversAfterFailures"
        (self.go_out / f"{name}.out").write_text(
            f"=== RUN   {name}\n    reconnect_test.go:99: want ACTIVE got IDLE\n--- FAIL: {name} (0.10s)\nFAIL\n"
        )
        (self.go_out / f"{name}.rc").write_text("1")
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("want ACTIVE got IDLE", r.stdout + r.stderr)

    def test_stops_at_the_first_failing_gate(self) -> None:
        name = "TestSupervisorAuthTerminalIsolatedPerSlot"
        (self.go_out / f"{name}.out").write_text(f"--- FAIL: {name} (0.10s)\nFAIL\n")
        (self.go_out / f"{name}.rc").write_text("1")
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertEqual(self.gate_names(), [n for n, _ in GATES][:2])

    def test_go_absent_is_skip_exit_3(self) -> None:
        farm = _farm(self.tmp, ["bash", "env", "dirname", "grep", "sed", "mktemp", "cat"])
        r = self.run_drill(vet=("", 0), path=farm)
        self.assertEqual(r.returncode, 3, r.stdout + r.stderr)
        self.assertIn("SKIP — go not found", r.stderr)
        self.assertFalse(self.gate_argvs())

    def test_skip_is_not_a_pass_line(self) -> None:
        farm = _farm(self.tmp, ["bash", "env", "dirname", "grep", "sed", "mktemp", "cat"])
        r = self.run_drill(vet=("", 0), path=farm)
        self.assertNotIn(PASS_LINE, r.stdout)

    def test_no_repo_litter(self) -> None:
        """The drill keeps per-gate logs in its own temp dir, not in the repo."""
        before = {p.name for p in TREE.iterdir()}
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(before, {p.name for p in TREE.iterdir()})

    def test_usage_does_not_require_arguments(self) -> None:
        r = subprocess.run([BASH, str(self.script())], capture_output=True, text=True, timeout=60)
        self.assertNotIn("command not found", r.stderr)
        self.assertNotIn("unbound variable", r.stderr)

    def test_vet_runs_after_the_gates(self) -> None:
        r = self.run_drill(vet=("", 0))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(len(self.records("VET")), 1)
        self.assertTrue(self.records("VET")[0]["argv"].endswith("vet ./..."))

    def test_shell_syntax_is_valid(self) -> None:
        r = subprocess.run([BASH, "-n", str(self.script())], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_no_live_fault_injection_surface(self) -> None:
        """The drill is offline by construction: no signal, container or service tooling."""
        text = self.script().read_text()
        for tool in ("pkill", "kill -", "docker ", "systemctl", "killall", "nsenter"):
            self.assertNotIn(tool, text, f"{tool} has no place in an offline gate")


if __name__ == "__main__":
    unittest.main()
