#!/usr/bin/env python3
"""Fail-closed behaviour of full_audit.sh (P6-005/006/101/394/395/396/397/398/736/737).

Every case builds a throwaway tree that mirrors the real layout, so the script's own ROOT
resolution points into it:

    <tmp>/code/01_platform/04_scripts/{full_audit.sh, stale_table_kind_scan.py, docs_audit.py}
    <tmp>/docs/{02_requirements,03_architecture,04_contracts,01_project,08_implementation}

The two scanner scripts are stubs that exit 0, which isolates full_audit.sh's own control
flow (preflight, sweep exit codes, dossier checks) from the real scanners' findings.

FULL_AUDIT_SCRIPT=<path> overrides the script under test; point it at a copy of the pre-fix
version to watch these cases go red.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

DEFAULT_SCRIPT = Path(__file__).resolve().parents[1] / "full_audit.sh"
SCRIPT_SRC = Path(os.environ.get("FULL_AUDIT_SCRIPT", DEFAULT_SCRIPT))

UPSTREAM_DIRS = ("02_requirements", "03_architecture", "04_contracts", "01_project")

# The literals Layer 3 requires in docs/08_implementation/04-signal-job.md.
TRIO_MARKERS = (
    "SUPERSEDED SAME-DAY (2026-08-15): the live-cluster externalization measurement LANDED",
    "SUPERSEDED SAME-DAY (2026-08-15): the live writer wiring LANDED",
    "externalization-benchmark half LANDED",
    "CANDLE [LOG + KV] RETIRED",
    "feature_candles_15s_current",
    "P11 — DEC-038 state ownership: dedup externalization",
    "re-scope LANDED 2026-08-13",
    "P11 landed 2026-08-15",
)

RETIRED_DOSSIERS = (
    "13-candle-log-kv-replay-safety.md",
    "14-candle-log-kv-replay-safety_2.md",
)


def running_under_gate() -> bool:
    """True when this test suite is itself a descendant of run-monday-gates.sh."""
    pid = os.getpid()
    for _ in range(10):
        try:
            cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().decode("utf-8", "replace")
        except OSError:
            return False
        if "run-monday-gates.sh" in cmdline:
            return True
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        except OSError:
            return False
        # comm may contain spaces and parentheses: ppid is the field after the last ')'.
        fields = stat[stat.rfind(")") + 2 :].split()
        if len(fields) < 2 or not fields[1].isdigit():
            return False
        pid = int(fields[1])
        if pid <= 1:
            return False
    return False


class FullAuditFailClosedTest(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        tmp = tempfile.mkdtemp(prefix="full-audit-test-")
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        self.root = Path(tmp)

        scripts = self.root / "code/01_platform/04_scripts"
        scripts.mkdir(parents=True)
        shutil.copy2(SCRIPT_SRC, scripts / "full_audit.sh")
        for stub in ("stale_table_kind_scan.py", "docs_audit.py"):
            (scripts / stub).write_text("import sys\nsys.exit(0)\n", encoding="utf-8")

        docs = self.root / "docs"
        for d in UPSTREAM_DIRS + ("08_implementation",):
            (docs / d).mkdir(parents=True, exist_ok=True)
        (docs / "01_project/note.md").write_text("clean prose, no claims\n", encoding="utf-8")
        (docs / "08_implementation/04-signal-job.md").write_text(
            "\n".join(TRIO_MARKERS) + "\n", encoding="utf-8"
        )

    def run_audit(self, shell: str = "bash") -> subprocess.CompletedProcess:
        return subprocess.run(
            [shell, str(self.root / "code/01_platform/04_scripts/full_audit.sh")],
            capture_output=True,
            text=True,
            cwd=str(self.root),
        )

    # ── baseline: the harness itself is green, so a red below means the guard ──
    def test_green_baseline(self) -> None:
        r = self.run_audit()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("FULL-AUDIT: all layers green", r.stdout)

    def test_whitelisted_paths_and_prose_stay_green(self) -> None:
        recs = self.root / "docs/01_project/change-records"
        recs.mkdir()
        # Only the path-scoped filter can save this one: no whitelist word is present.
        (recs / "CHG-900.md").write_text("ranking appears here\n", encoding="utf-8")
        # Content whitelist, case-insensitive.
        (self.root / "docs/01_project/hist.md").write_text(
            "Historical postponement note about ranking\n", encoding="utf-8"
        )
        r = self.run_audit()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    # ── P6-101/395/396: a live claim is still caught ──────────────────────────
    def test_live_claim_is_caught(self) -> None:
        (self.root / "docs/01_project/live.md").write_text(
            "the ranking path is live\n", encoding="utf-8"
        )
        r = self.run_audit()
        self.assertEqual(r.returncode, 1)
        self.assertIn("unexpected live ranking/reservation claims", r.stdout)

    # ── P6-005: a missing docs tree is a failure, not a green ─────────────────
    def test_missing_docs_tree_fails(self) -> None:
        shutil.rmtree(self.root / "docs")
        r = self.run_audit()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("missing audited directory", r.stdout)
        self.assertNotIn("all layers green", r.stdout)

    # ── P6-006: a missing upstream layer is a failure ────────────────────────
    def test_missing_upstream_layer_fails(self) -> None:
        shutil.rmtree(self.root / "docs/03_architecture")
        r = self.run_audit()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("missing audited directory", r.stdout)

    # ── P6-005: grep exit 2 (unreadable input) must not be swallowed ──────────
    def test_unreadable_doc_makes_the_sweep_fail_closed(self) -> None:
        if os.geteuid() == 0:
            self.skipTest("running as root: mode 000 is not enforced, grep cannot fail")
        target = self.root / "docs/01_project/note.md"
        target.chmod(0o000)
        self.addCleanup(target.chmod, 0o600)
        r = self.run_audit()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("sweep did not run", r.stdout)
        self.assertNotIn("all layers green", r.stdout)

    # ── P6-397: the retired 13/14 dossiers must stay deleted ─────────────────
    def test_resurrected_dossiers_fail(self) -> None:
        for name in RETIRED_DOSSIERS:
            (self.root / "docs/08_implementation" / name).write_text(
                "resurrected\n", encoding="utf-8"
            )
        r = self.run_audit()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("stale dossier resurrected", r.stdout)
        for name in RETIRED_DOSSIERS:
            self.assertIn(name, r.stdout)

    # ── P6-398: a missing dossier file is reported, not a grep usage error ───
    def test_missing_dossier_file_fails_with_a_named_message(self) -> None:
        (self.root / "docs/08_implementation/04-signal-job.md").unlink()
        r = self.run_audit()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("file missing:", r.stdout)

    # ── P6-736: a mis-invocation diagnoses itself instead of crashing ────────
    def test_sh_invocation_is_diagnosed(self) -> None:
        if not Path("/bin/sh").exists():
            self.skipTest("no /bin/sh")
        r = self.run_audit(shell="/bin/sh")
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("run this audit with bash", r.stderr)

    # ── P6-394: a gate run in flight is refused unless we are its own step ────
    def _fake_gate(self, body: str) -> subprocess.Popen:
        """Start a process whose argv names the gate — the guard matches argv, not files."""
        gate_script = self.root / "run-monday-gates.sh"
        gate_script.write_text("#!/usr/bin/env bash\n" + body + "\n", encoding="utf-8")
        # DEVNULL, not pipes: a surviving sleep child would otherwise hold the pipe open.
        return subprocess.Popen(
            ["bash", str(gate_script)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def test_concurrent_gate_run_is_refused(self) -> None:
        if running_under_gate():
            self.skipTest("suite runs under the gate, so the audit is legitimately exempt")
        if not shutil.which("pgrep"):
            self.skipTest("pgrep not available")
        gate = self._fake_gate("sleep 30")
        self.addCleanup(gate.wait)
        self.addCleanup(gate.kill)
        r = self.run_audit()
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("refusing to run", r.stderr)

    def test_the_gate_own_step_stays_exempt(self) -> None:
        """The gate runs this audit as step 10/19: an ancestry-exempted invocation must work."""
        r = subprocess.run(
            ["bash", str(self._wrapper_gate())],
            capture_output=True,
            text=True,
            cwd=str(self.root),
        )
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("all layers green", r.stdout)

    def _wrapper_gate(self) -> Path:
        """A stand-in gate that invokes the audit, i.e. the audit's ancestor is the gate."""
        gate_script = self.root / "run-monday-gates.sh"
        gate_script.write_text(
            "#!/usr/bin/env bash\nbash {}\n".format(
                self.root / "code/01_platform/04_scripts/full_audit.sh"
            ),
            encoding="utf-8",
        )
        return gate_script

    # ── P6-737: missing tooling is a hard stop with its own exit code ────────
    def test_missing_audit_script_is_named(self) -> None:
        (self.root / "code/01_platform/04_scripts/docs_audit.py").unlink()
        r = self.run_audit()
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("missing audit script", r.stdout)


if __name__ == "__main__":
    unittest.main()
