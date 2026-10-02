#!/usr/bin/env python3
"""Behavioural tests for jar-freshness.sh (CHG-523).

The shared guard is sourced by rollout-savepoint.sh (restore/rollout path) and
pipeline-lib.sh (fresh submit path). It prints a reason and returns:
  0 = fresh (or ALLOW_STALE_JAR=1 override)
  1 = stale (a source is newer than the jar)
  2 = jar missing/unreadable
These tests source the real lib against a fake ROOT tree with controlled
mtimes — no cluster, no docker, no repo artifacts touched.
"""
from __future__ import annotations

import os
import pathlib
import re
import subprocess
import tempfile
import unittest

TESTS_DIR = pathlib.Path(__file__).resolve().parent
SCRIPTS = TESTS_DIR.parent
LIB = SCRIPTS / "jar-freshness.sh"


class JarFreshness(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.t = pathlib.Path(self.tmp.name)
        self.root = self.t / "fake-root"
        for rel in ("code/02_services/02_compute/src/main", "code/common/src/main"):
            (self.root / rel).mkdir(parents=True, exist_ok=True)
        (self.root / "code/02_services/02_compute/pom.xml").write_text("<project/>")
        (self.root / "code/common/pom.xml").write_text("<project/>")
        (self.root / "code/pom.xml").write_text("<project/>")
        self.app = self.root / "code/02_services/02_compute/src/main/App.java"
        self.common = self.root / "code/common/src/main/Common.java"
        self.app.write_text("//")
        self.common.write_text("//")
        self.jar = self.t / "compute.jar"
        self.jar.write_bytes(b"jar")

    def tearDown(self):
        self.tmp.cleanup()

    def _times(self, jar_time, compute_time, common_time=None):
        subprocess.run(["touch", "-d", compute_time, str(self.app),
                        str(self.root / "code/02_services/02_compute/pom.xml"),
                        str(self.root / "code/pom.xml")], check=True)
        subprocess.run(["touch", "-d", common_time or compute_time, str(self.common),
                        str(self.root / "code/common/pom.xml")], check=True)
        subprocess.run(["touch", "-d", jar_time, str(self.jar)], check=True)

    def run_check(self, extra_env=None, jar=None):
        env = dict(os.environ)
        env.update(extra_env or {})
        cmd = (f'source "{LIB}"; ROOT="{self.root}"; '
               f'jar_freshness_check "{jar or self.jar}"; echo "RC=$?"')
        return subprocess.run(["bash", "-c", cmd], env=env, capture_output=True,
                              text=True, timeout=60)

    def test_stale_compute_source_refuses(self):
        self._times("2026-10-02 12:00:00", "2026-10-02 13:00:00")
        r = self.run_check()
        self.assertIn("RC=1", r.stdout, r.stdout + r.stderr)
        self.assertIn("STALE", r.stdout)
        self.assertIn("mvn -o package", r.stdout, "the refusal must carry the rebuild command")

    def test_stale_common_source_refuses(self):
        # The compute jar shades common (312 classes): a newer common source
        # must refuse even when every compute source is older.
        self._times("2026-10-02 12:00:00", "2026-10-02 11:00:00",
                    "2026-10-02 13:00:00")
        r = self.run_check()
        self.assertIn("RC=1", r.stdout, r.stdout + r.stderr)
        self.assertIn("Common.java", r.stdout)

    def test_fresh_jar_passes(self):
        self._times("2026-10-02 14:00:00", "2026-10-02 12:00:00")
        r = self.run_check()
        self.assertIn("RC=0", r.stdout, r.stdout + r.stderr)
        self.assertIn("jar freshness", r.stdout)

    def test_allow_stale_jar_override(self):
        self._times("2026-10-02 12:00:00", "2026-10-02 13:00:00")
        r = self.run_check({"ALLOW_STALE_JAR": "1"})
        self.assertIn("RC=0", r.stdout, r.stdout + r.stderr)
        self.assertIn("NOT checked", r.stdout)

    def test_missing_jar_refuses(self):
        r = self.run_check(jar=str(self.t / "nope.jar"))
        self.assertIn("RC=2", r.stdout, r.stdout + r.stderr)
        self.assertIn("not readable", r.stdout)

    def test_shared_lib_shape(self):
        src = LIB.read_text()
        self.assertIn("JAR_FRESHNESS_ROOTS=(", src)
        for root in ("code/02_services/02_compute/src/main",
                     "code/02_services/02_compute/pom.xml",
                     "code/common/src/main", "code/common/pom.xml",
                     "code/pom.xml"):
            self.assertIn(root, src,
                          "the shaded common classes / parent pom must be covered")
        self.assertIn("compute jar is STALE", src)
        self.assertIn("ALLOW_STALE_JAR", src)
        self.assertIsNone(re.search(r"^\s*set\s+-", src, re.M),
                          "a sourced lib must not change the caller's shell options")
        self.assertIsNone(re.search(r"^\s*exit\b", src, re.M),
                          "the shared lib must return a status, never exit")


if __name__ == "__main__":
    unittest.main()
