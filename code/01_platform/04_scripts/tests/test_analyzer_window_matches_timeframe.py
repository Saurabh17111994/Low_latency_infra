#!/usr/bin/env python3
"""The analyzer's 15s grid must equal Timeframe.FIFTEEN_S — nothing joins them.

The "15-second candle window" lives in four places and no test joined any two:
Timeframe.java (FIFTEEN_S millis), holistic-analyze.py (grid arithmetic plus the
G7c family width), pipeline-lib.sh (CANDLE_WINDOW_MS handed to the compose
ingest path), and a Java test fixture (out of scope — a fixture, not a
contract). A Java-side change to the enum touches no file under 04_scripts/,
so the analyzer would silently misalign — the same failure shape as the
retired-table incident: a leg comparing an empty set, failing late with a
misleading verdict instead of failing at the contract.

The pin is to FIFTEEN_S specifically, NOT to min(Timeframe): the parity leg
compares the 15s candle family, so a hypothetical 5s timeframe must NOT move
this constant. Today min == FIFTEEN_S; deliberately not asserted.
"""

import ast
import importlib.util
import io
import os
import re
import tokenize
import unittest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
ANALYZE = os.path.join(SCRIPTS_DIR, "holistic-analyze.py")
PIPELINE_LIB = os.path.join(SCRIPTS_DIR, "pipeline-lib.sh")
TIMEFRAME = os.path.join(
    SCRIPTS_DIR, "..", "..", "02_services", "02_compute", "src", "main",
    "java", "com", "trading", "compute", "signaljob", "Timeframe.java")

GRID_VALUES = (14999, 15000)  # W - 1 and W spellings of the 15s grid


def _timeframe_millis():
    """{member: millis} parsed from the enum (fails closed if it moves)."""
    path = os.path.normpath(TIMEFRAME)
    assert os.path.isfile(path), f"Timeframe.java not where expected: {path}"
    with open(path) as fh:
        src = fh.read()
    found = {m: int(d.replace("_", ""))
             for m, d in re.findall(r"^\s*([A-Z][A-Z0-9_]*)\(\s*([\d_]+)\s*L",
                                    src, re.MULTILINE)}
    assert found and "FIFTEEN_S" in found, \
        f"could not parse Timeframe members from {path}"
    return found


_MOD = None


def _load_analyze():
    """holistic-analyze.py via importlib (repo pattern: hyphenated name)."""
    global _MOD
    if _MOD is None:
        spec = importlib.util.spec_from_file_location(
            "holistic_analyze", ANALYZE)
        _MOD = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_MOD)
    return _MOD


class AnalyzerWindowMatchesTimeframe(unittest.TestCase):
    def test_constant_equals_fifteen_s_millis(self) -> None:
        mod = _load_analyze()
        fifteen_s = _timeframe_millis()["FIFTEEN_S"]
        self.assertEqual(mod.CANDLE_PARITY_WINDOW_MS, fifteen_s,
                         "analyzer grid != Timeframe.FIFTEEN_S millis")

    def test_no_bare_second_copy_of_the_grid(self) -> None:
        """Every 15000/14999 in code must be the named constant, not a literal.

        tokenize skips the docstring/comments that explain the constant, so
        only live code is checked; the constant's own definition line is the
        one allowed site (found via AST, not by line number).
        """
        with open(ANALYZE) as fh:
            src = fh.read()
        tree = ast.parse(src)
        def_lines = {n.lineno for n in ast.walk(tree)
                     if isinstance(n, ast.Assign)
                     and any(isinstance(t, ast.Name)
                             and t.id == "CANDLE_PARITY_WINDOW_MS"
                             for t in n.targets)}
        self.assertEqual(len(def_lines), 1,
                         "expected exactly one CANDLE_PARITY_WINDOW_MS definition")
        strays = [(tok.start[0], tok.string)
                  for tok in tokenize.generate_tokens(io.StringIO(src).readline)
                  if tok.type == tokenize.NUMBER
                  and tok.string.replace("_", "").isdigit()
                  and int(tok.string.replace("_", "")) in GRID_VALUES
                  and tok.start[0] not in def_lines]
        self.assertEqual(strays, [],
                         f"bare 15s-grid literals outside the constant: {strays}")

    def test_pipeline_lib_candle_window_matches(self) -> None:
        """pipeline-lib.sh hands the same width to the compose ingest path."""
        with open(PIPELINE_LIB) as fh:
            lib = fh.read()
        found = [int(v) for v in re.findall(r"CANDLE_WINDOW_MS=(\d+)", lib)]
        self.assertTrue(found, "CANDLE_WINDOW_MS not found in pipeline-lib.sh")
        mod = _load_analyze()
        for value in found:
            self.assertEqual(value, mod.CANDLE_PARITY_WINDOW_MS,
                             "pipeline-lib CANDLE_WINDOW_MS != analyzer grid")


if __name__ == "__main__":
    unittest.main()
