"""Keep the tablet repair offset/filename parser fail-closed."""

import re
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
REPAIR = ROOT / "code" / "01_platform" / "04_scripts" / "fluss-repair" / "repair-tablet.sh"


def embedded_awk():
    """The parser repair-tablet.sh actually runs (P6-841).

    This test used to carry its own copy, so the shipped parser could drift free of
    it — only three string fragments were pinned. Extract the block from the script,
    and fail loudly if the extraction stops finding it: silently running an empty
    program would be worse than a red test.
    """
    script = REPAIR.read_text()
    match = re.search(r"awk '\n(.*?)\n' \"\$SCAN_LOG\"", script, re.S)
    assert match, "the awk block moved in repair-tablet.sh — fix this extraction"
    body = match.group(1)
    assert "bad = 1" in body and "END { if (bad) exit 1 }" in body, \
        "the extracted block is not the fail-closed parser"
    return body


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
        awk = embedded_awk()
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

    def test_malformed_scan_output_is_rejected(self):
        """P6-626: the parser is fail-closed — a non-/d/ path and a zero offset must
        exit non-zero with no output, instead of repairing the wrong boundary."""
        scan_output = (
            "/etc/passwd: size=12 last_complete_batch_end=0 zero_tail=12 bytes\n"
            "TRUNCATE_TO=0\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as handle:
            handle.write(scan_output)
            handle.flush()
            result = subprocess.run(["awk", embedded_awk(), handle.name],
                                    check=False, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "")


if __name__ == "__main__":
    unittest.main()
