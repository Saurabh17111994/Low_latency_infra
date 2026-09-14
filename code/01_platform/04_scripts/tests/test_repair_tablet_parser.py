"""Keep the tablet repair offset/filename parser fail-closed."""

import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
REPAIR = ROOT / "code" / "01_platform" / "04_scripts" / "fluss-repair" / "repair-tablet.sh"


class RepairTabletParserTest(unittest.TestCase):
    def test_script_uses_exact_path_and_offset_extraction(self):
        script = REPAIR.read_text()
        self.assertIn('sub(/: size=.*/, "", path)', script)
        self.assertIn('substr($0, index($0, "=") + 1)', script)
        self.assertNotIn('substr($0, 14)} "$SCAN_LOG"', script)
        # The boundary/zero-tail check moved out of the truncate loop into the
        # all-or-nothing pre-flight verifier (P6-098/P6-099); the script must call it.
        verifier = REPAIR.parent / "verify-and-truncate.py"
        self.assertIn("unsafe truncation", verifier.read_text())
        self.assertIn("verify-and-truncate.py", script)

    def test_scan_output_extracts_full_offset_without_diagnostic_suffix(self):
        scan_output = (
            "/d/default/raw_table_1-90/20260901-p21/log-6/00000000000000000000.log: "
            "size=32157696 last_complete_batch_end=32156904 zero_tail=792 bytes\n"
            "TRUNCATE_TO=32156904\n"
        )
        awk = r"""
/^TRUNCATE_TO=/ {
    path = prev
    sub(/: size=.*/, "", path)
    if (path !~ /^\/d\/.*\.log$/ || $0 !~ /^TRUNCATE_TO=[1-9][0-9]*$/) {
        bad = 1
    } else {
        print path "\t" substr($0, index($0, "=") + 1)
    }
}
{ prev = $0 }
END { if (bad) exit 1 }
"""
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as handle:
            handle.write(scan_output)
            handle.flush()
            result = subprocess.run(
                ["awk", awk, handle.name],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(
            result.stdout.strip(),
            "/d/default/raw_table_1-90/20260901-p21/log-6/00000000000000000000.log\t32156904",
        )


if __name__ == "__main__":
    unittest.main()
