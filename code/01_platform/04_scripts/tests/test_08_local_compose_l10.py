"""L10 End-to-end 10-instrument smoke — LOCAL-INT-004."""
import importlib.util
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
HARNESS = ROOT / "code/01_platform/04_scripts/local_int_004_smoke.py"
MAKEFILE = ROOT / "Makefile"

class LocalInt004Test(unittest.TestCase):
    def run_harness(self, *args, harness=None):
        """Run the harness and surface ITS output when it exits non-zero (P6-599).

        The harness reports every result on stdout and returns 1 on any failure, so a
        bare check_output would show only the return code and throw away the line that
        says what actually broke.
        """
        cmd = ["python3", str(harness or HARNESS), *args]
        try:
            return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            self.fail(f"{' '.join(cmd)} exited {e.returncode}:\n{e.output}")

    def test_LOCAL_INT_004_offline_contract(self):
        """LOCAL-INT-004 offline: 10 instruments, fake bridge lifecycle, no live Arrow egress — must PASS without containers."""
        out = self.run_harness("--offline")
        self.assertIn("PASS LOCAL-INT-004 [offline", out, f"LOCAL-INT-004 offline failed: {out}")

    def test_LOCAL_INT_004_live_is_either_pass_or_contract_only(self):
        """LOCAL-INT-004 live: when execution-t3 stack is up, drive real smoke; otherwise contract-only is OK."""
        out = self.run_harness("--live")
        self.assertIn("PASS LOCAL-INT-004", out, f"LOCAL-INT-004 live failed: {out}")
        self.assertNotIn("FAIL", out)

    def test_LOCAL_INT_004_harness_is_importable_and_wired(self):
        """P6-600: what is actually true of the harness — it imports, and the Makefile calls it.

        The exec bit is irrelevant: every caller invokes `python3 <path>`, never the path
        itself, so asserting the bit tested nothing and failed on filesystems that do not
        preserve it.
        """
        self.assertTrue(HARNESS.is_file(), "harness missing")
        spec = importlib.util.spec_from_file_location("local_int_004_smoke", HARNESS)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)   # import-time side effects would run the harness
        self.assertTrue(callable(getattr(module, "main", None)), "P6-600: harness exposes no main()")
        self.assertIn("local_int_004_smoke.py --offline", MAKEFILE.read_text(),
                      "P6-600: the Makefile no longer wires the harness")

if __name__ == "__main__":
    unittest.main()
