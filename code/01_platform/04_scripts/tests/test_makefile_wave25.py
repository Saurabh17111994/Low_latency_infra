"""Wave-25 guards for the Makefile's file-less targets (P6-302/303/304/707/708).

Each recipe is exercised by a real `make` in a temp dir holding a copy of the
Makefile plus stub scripts, so the shell actually runs without touching the stack
or the repository. PATH is a symlink toolbox and nothing else, so `rg`,
`shellcheck` and `docker` mean exactly what a test says they mean: this host has
ripgrep in /usr/bin as well as ~/.cargo/bin, so no ordinary PATH expresses "rg is
not installed".
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
MAKEFILE = REPO / "Makefile"
SCRIPTS_REL = "code/01_platform/04_scripts"
TOOLS = ("bash", "find", "sort", "mktemp", "head", "cat", "rm", "grep", "printf", "sed", "seq")
MAKE = shutil.which("make") or "/usr/bin/make"
# P6-302's evidence: the recipe targets that were missing from .PHONY.
WERE_MISSING = """
test-local test-network test-08-phaseA test-08-phaseB test-08-phaseC test-08-phaseD
test-execution test-failure test-observability test-performance test-25-smoke
test-prod-hardening test-all test-all-plus-prod alert-routing-test
check-loadtest-env check-ingestion-clean test-loadtest-guards loadtest-20k-regression
disaster-drills eod-controller
""".split()

DOCKER_STUB = """#!/bin/sh
mode="${W25_DOCKER_MODE:-clean}"
case "$1" in
compose)
	# $(COMPOSE) ps -q <service>  (or an exec-style subcommand)
	for a in "$@"; do
		if [ "$a" = "ps" ]; then
			[ "$mode" = "ps-fail" ] && exit 1
			[ "$mode" = "ps-empty" ] && exit 0
			echo 0123456789abcdef
			exit 0
		fi
	done
	exit 0
	;;
exec)
	[ "$mode" = "exec-fail" ] && exit 1
	[ "$mode" = "ps-empty" ] && exit 1  # no such container: exec fails, as real docker does
	case "$*" in
	*ARROW_FAKE_BROKER*)
		[ "$mode" = "fake-set" ] && { echo 1; exit 0; }
		echo ""
		exit 0
		;;
	*ARROW_INSTRUMENT_TOKENS*)
		echo ""
		exit 0
		;;
	esac
	exit 0
	;;
esac
exit 0
"""

LOADTEST_STUB = """#!/bin/bash
for i in $(seq 1 30); do echo "preflight line $i"; done
echo "check-loadtest-env: OK (stub)"
exit "${W25_PREFLIGHT_RC:-0}"
"""


def _targets_and_phony(text: str) -> tuple[list[str], set[str]]:
    targets: list[str] = []
    phony: set[str] = set()
    for line in text.split("\n"):
        m = re.match(r"^([A-Za-z0-9_][A-Za-z0-9_.-]*):(?!=)", line)
        if m and not m.group(1).startswith("."):
            targets.append(m.group(1))
        if line.startswith(".PHONY:"):
            phony |= set(line.split(":", 1)[1].split())
    return targets, phony


def _recipe(text: str, target: str) -> str:
    """The recipe body of `target`: indented lines after its rule line."""
    lines = text.split("\n")
    start = next(i for i, l in enumerate(lines) if l.startswith(f"{target}:"))
    body = []
    for line in lines[start + 1:]:
        if line and not line[0].isspace():
            break
        body.append(line)
    return "\n".join(body)


class _Sandbox:
    """A throwaway copy of the Makefile with stub tools in front of PATH."""

    def __init__(self, *, docker: bool = False, loadtest: bool = False, rg: bool = False):
        self.dir = Path(tempfile.mkdtemp(prefix="w25-make-"))
        (self.dir / SCRIPTS_REL).mkdir(parents=True)
        shutil.copy2(MAKEFILE, self.dir / "Makefile")
        self.bin = self.dir / "bin"
        self.bin.mkdir()
        for tool in TOOLS:
            found = shutil.which(tool)
            if found:
                os.symlink(found, self.bin / tool)
        # Shadow the real shellcheck: its verdict on the stub scripts is not what
        # these tests are about, and its absence would only shorten the loop.
        self._write_tool("shellcheck", "#!/bin/sh\nexit 0\n")
        if docker:
            self._write_tool("docker", DOCKER_STUB)
        if loadtest:
            self._write_script("loadtest-run.sh", LOADTEST_STUB)
        if rg:
            # Reports one hit, which is all the guard needs to raise `fail=1`.
            self._write_tool("rg", "#!/bin/sh\necho 'code/hit.sh:1: curl -s :9250/metrics | grep -oE \"[0-9]+$\"'\nexit 0\n")

    def _write_tool(self, name: str, body: str) -> None:
        p = self.bin / name
        p.write_text(body)
        p.chmod(0o755)

    def _write_script(self, name: str, body: str) -> None:
        p = self.dir / SCRIPTS_REL / name
        p.write_text(body)
        p.chmod(0o755)

    def make(self, target: str, env: dict | None = None, timeout: int = 120):
        full = dict(os.environ)
        # Toolbox only: every tool the recipes use is symlinked into it, and
        # make itself is invoked by absolute path.
        full["PATH"] = str(self.bin)
        full.update(env or {})
        return subprocess.run(
            [MAKE, "-C", str(self.dir), target],
            capture_output=True, text=True, timeout=timeout, env=full,
        )

    def cleanup(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)


class PhonyContractTest(unittest.TestCase):
    """P6-302: a file-less recipe that is not .PHONY can be skipped silently."""

    def test_every_recipe_target_is_phony(self):
        targets, phony = _targets_and_phony(MAKEFILE.read_text())
        missing = [t for t in targets if t not in phony]
        self.assertEqual([], missing, f"targets missing from .PHONY: {missing}")

    def test_the_targets_the_audit_named_are_declared(self):
        _, phony = _targets_and_phony(MAKEFILE.read_text())
        self.assertEqual([], sorted(set(WERE_MISSING) - phony))


class BranchGuardCommentTest(unittest.TestCase):
    """P6-707: the comment agents MUST obey named a branch that does not exist."""

    def test_the_comment_names_main_and_no_other_branch(self):
        text = MAKEFILE.read_text()
        header = text.split("branch-check:", 1)[0]
        comment = header[header.rindex(".") :] if "." in header else header
        self.assertIn("`main`", text.split("branch-check:", 1)[0])
        self.assertNotIn("low-latency branch", MAKEFILE.read_text())
        self.assertNotIn("low-latency branch", comment)


class LoadtestPreflightTargetTest(unittest.TestCase):
    """P6-303: `|| true` after `| head` made a broken environment look green."""

    def test_the_recipe_does_not_discard_a_refusal(self):
        body = _recipe(MAKEFILE.read_text(), "check-loadtest-env")
        self.assertNotIn("|| true", body)

    def test_a_refusal_propagates_and_prints_the_full_output(self):
        box = _Sandbox(loadtest=True)
        try:
            proc = box.make("check-loadtest-env", {"W25_PREFLIGHT_RC": "7"})
        finally:
            box.cleanup()
        self.assertNotEqual(0, proc.returncode, "a refused preflight must fail the target")
        self.assertIn("Error 7", proc.stderr)
        self.assertIn("check-loadtest-env: FAIL (rc=7)", proc.stderr)
        self.assertIn("preflight line 30", proc.stderr)

    def test_a_pass_still_truncates_but_says_it_passed(self):
        box = _Sandbox(loadtest=True)
        try:
            proc = box.make("check-loadtest-env")
        finally:
            box.cleanup()
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn("check-loadtest-env: OK (preflight passed)", proc.stdout)
        self.assertIn("preflight line 20", proc.stdout)
        self.assertNotIn("preflight line 21", proc.stdout, "head -20 truncation lost")


class IngestionCleanTargetTest(unittest.TestCase):
    """P6-304: discarded errors made a stopped container print OK."""

    def _run(self, mode: str):
        box = _Sandbox(docker=True)
        try:
            return box.make("check-ingestion-clean", {"W25_DOCKER_MODE": mode})
        finally:
            box.cleanup()

    def test_a_missing_container_fails_with_the_reason(self):
        proc = self._run("ps-empty")
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("no running 'ingestion' container", proc.stderr)

    def test_a_failed_compose_ps_is_not_reported_as_a_missing_container(self):
        proc = self._run("ps-fail")
        self.assertNotEqual(0, proc.returncode)
        # The tail of the message identifies the branch without repeating the
        # invocation form the compose-contract scanner watches for.
        self.assertIn("(no daemon, or unreadable compose/env files)", proc.stderr)

    def test_a_failing_docker_exec_fails_loudly(self):
        proc = self._run("exec-fail")
        self.assertNotEqual(0, proc.returncode, "docker exec failure must not read as a clean feed")
        self.assertNotIn("OK (no fake broker", proc.stdout + proc.stderr)
        # Pre-fix this aborted through `set -e` with a bare "Error 1": the exit
        # status was right and the operator was told nothing.
        self.assertIn("'docker exec' failed on", proc.stderr)

    def test_a_fake_broker_fails(self):
        proc = self._run("fake-set")
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("ARROW_FAKE_BROKER is set", proc.stderr)

    def test_a_clean_container_passes(self):
        proc = self._run("clean")
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn("check-ingestion-clean: OK (no fake broker, no test tokens)", proc.stdout)


class StaticCheckTargetTest(unittest.TestCase):
    """P6-708: the float-trap scan vanished without rg, and wrote a shared path."""

    def test_the_recipe_does_not_write_a_shared_tmp_path(self):
        self.assertNotIn("/tmp/prom-float-trap", MAKEFILE.read_text())

    def test_a_missing_rg_is_reported_not_silent(self):
        box = _Sandbox()  # the toolbox has no rg
        try:
            (box.dir / "code" / "clean.sh").write_text("#!/bin/sh\nexit 0\n")
            proc = box.make("static-check")
        finally:
            box.cleanup()
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn("rg (ripgrep) not installed", proc.stderr)
        self.assertIn("SKIPPED", proc.stderr)

    def test_the_float_trap_is_still_caught_when_rg_is_present(self):
        box = _Sandbox(rg=True)
        try:
            proc = box.make("static-check")
        finally:
            box.cleanup()
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("Prometheus float-trap pattern found", proc.stderr)
        self.assertIn("9250", proc.stderr)


if __name__ == "__main__":
    unittest.main()
