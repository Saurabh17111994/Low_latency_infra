#!/usr/bin/env python3
"""Guard tests for ddl_apply_smoke.py's docker probes (wave 14, P6-352).

The S4 drill shells out to the docker CLI on the host. Two of those probes had
no timeout and no exception handling while every sibling docker call in the same
file used 120s/900s timeouts, so a daemon that accepts the socket but never
answers hung the drill instead of degrading to a SKIP. These tests pin the
degradation without needing a docker daemon: the timeout is injected.
"""
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "code/01_platform/04_scripts"))

import ddl_apply_smoke as smoke  # noqa: E402


class DockerProbeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.compose = Path(self.tmp.name) / "docker-compose.yml"
        self.compose.write_text("services: {}\n")

    def test_docker_absent_is_a_skip(self):
        with mock.patch.object(smoke.shutil, "which", return_value=None):
            image, reason = smoke._docker_smoke_available(str(self.compose))
        self.assertIsNone(image)
        self.assertIn("docker CLI not found", reason)

    def test_unresponsive_daemon_degrades_instead_of_hanging(self):
        with mock.patch.object(smoke.shutil, "which", return_value="/usr/bin/docker"), \
             mock.patch.object(smoke.subprocess, "run",
                               side_effect=subprocess.TimeoutExpired("docker", 120)):
            image, reason = smoke._docker_smoke_available(str(self.compose))
        self.assertIsNone(image)
        self.assertIn("compose config probe did not run", reason)

    def test_unresponsive_daemon_on_image_inspect_also_degrades(self):
        ok = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(smoke.shutil, "which", return_value="/usr/bin/docker"), \
             mock.patch.object(smoke.subprocess, "run",
                               side_effect=[ok, subprocess.TimeoutExpired("docker", 120)]):
            image, reason = smoke._docker_smoke_available(str(self.compose))
        self.assertIsNone(image)
        self.assertIn("docker image inspect did not run", reason)

    def test_invalid_compose_config_still_reports_its_own_reason(self):
        bad = mock.Mock(returncode=1, stdout="", stderr="missing variable")
        with mock.patch.object(smoke.shutil, "which", return_value="/usr/bin/docker"), \
             mock.patch.object(smoke.subprocess, "run", return_value=bad):
            image, reason = smoke._docker_smoke_available(str(self.compose))
        self.assertIsNone(image)
        self.assertIn("compose config invalid", reason)


if __name__ == "__main__":
    unittest.main()
