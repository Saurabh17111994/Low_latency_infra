"""C6 must measure each module's surefire total once (P6-724).

The compute total was evaluated twice with identical arguments — once for the
check, once for its message — and each evaluation globs, parses and walks the
compute test tree. This pins single evaluation without changing what C6 reports.
"""

import glob
import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import docs_audit  # noqa: E402


class ComputeCountCachingTests(unittest.TestCase):
    def test_c6_measures_each_module_once(self):
        comp_dir = os.path.join(docs_audit.ROOT, "code", "02_services", "02_compute")
        if not glob.glob(os.path.join(comp_dir, "target", "surefire-reports", "TEST-*.xml")):
            self.skipTest("compute surefire reports absent — C6 skips the module here")

        calls = []
        real = docs_audit.surefire_total

        def counting(module_dir, test_src_dir=None):
            calls.append(module_dir)
            return real(module_dir, test_src_dir)

        with mock.patch.object(docs_audit, "surefire_total", counting):
            with redirect_stdout(io.StringIO()):
                docs_audit.c6_test_counts()

        self.assertEqual(
            len(calls), len(set(calls)), f"a module was measured twice: {calls}")
        self.assertEqual(sum(1 for c in calls if c == comp_dir), 1, calls)


if __name__ == "__main__":
    unittest.main()
