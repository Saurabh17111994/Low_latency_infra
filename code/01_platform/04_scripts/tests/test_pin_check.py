"""Hermetic tests for pin-check.sh checks 4-5 (P6 wave 20: P6-138..141, P6-761).

pin-check.sh cds to the repo root and runs six checks; probing checks 4-5 in
place needs fixture versions.pin/runtime.lock. Instead each probe copies the
REPO TREE's script into a sandbox repo skeleton (versions.pin + runtime.lock +
stub check scripts) and runs it there with REPO_ROOT-relative paths intact.
Checks 1-3/6 are stubbed to pass (their own suites own them); only 4-5 vary.
"""
import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

REPO = pathlib.Path(__file__).resolve().parents[4]
SRC = (REPO / "code/01_platform/04_scripts/pin-check.sh").read_text()

REAL_PIN = (REPO / "code/01_platform/04_scripts/versions.pin").read_text()
REAL_LOCK = (REPO / "code/01_platform/01_docker/runtime.lock").read_text()

STUB_PASS = '#!/usr/bin/env bash\nexit 0\n'

# The four locally built images runtime.lock pins by image ID (P6-140). Their
# digests can only come from the build host: no registry manifest exists for an
# image that was never pushed, so this list is also the live check's input.
LOCAL_BUILD_IMAGES = {
    "INGESTION_IMAGE": "01_docker-ingestion",
    "NAUTILUS_IMAGE": "01_docker-nautilus",
    "EXECUTION_BRIDGE_IMAGE": "01_docker-execution-bridge",
    "EXECUTION_GATEWAY_IMAGE": "01_docker-execution-gateway",
}


def parse_image_refs(lock_text):
    """name -> ref, normalising the shapes pin-check counts (P6-139)."""
    refs = {}
    for line in lock_text.splitlines():
        line = re.sub(r"^\s+", "", line)
        line = re.sub(r"^export(\s+|$)", "", line)
        m = re.match(r"([A-Za-z0-9_]+_IMAGE)\s*=\s*(.*)$", line)
        if m:
            refs[m.group(1)] = re.sub(r"\s+#.*$", "", m.group(2)).strip()
    return refs


class PinCheckHarness(unittest.TestCase):
    def setUp(self):
        self.t = pathlib.Path(tempfile.mkdtemp())
        repo = self.t / "repo"
        scripts = repo / "code/01_platform/04_scripts"
        docker = repo / "code/01_platform/01_docker"
        scripts.mkdir(parents=True)
        docker.mkdir(parents=True)
        (repo / "code/01_platform/04_scripts/pin-check.sh").write_text(SRC)
        for stub in ("version_matrix_verify.py", "corpus-pin.sh",
                     "pom-snapshot-scan.py", "rust_toolchain_pin_check.sh"):
            p = scripts / stub
            p.write_text(STUB_PASS if stub.endswith(".sh")
                         else '#!/usr/bin/env python3\nimport sys; sys.exit(0)\n')
            p.chmod(0o755)
        (scripts / "version_matrix.yaml").write_text("stub: true\n")
        self.pin = scripts / "versions.pin"
        self.lock = docker / "runtime.lock"
        self.repo = repo
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(self.t)], check=False))

    def run_check(self, pin_text=REAL_PIN, lock_text=REAL_LOCK, timeout=60):
        self.pin.write_text(pin_text)
        self.lock.write_text(lock_text)
        return subprocess.run(["bash", str(self.repo / "code/01_platform/04_scripts/pin-check.sh")],
                              capture_output=True, text=True, timeout=timeout,
                              cwd=str(self.t))

    def section(self, out, tag):
        i = out.find(tag)
        j = out.find("== [", i + 1)
        return out[i:j if j > 0 else len(out)]

    # P6-138: strict pins — ranges/wildcards/placeholders fail, real pins pass.
    def test_strict_version_pins(self):
        for bad_value in ("*", "^2.2.1", "~2.2.1", "[2.0,3.0)", "2.2.1 ", "latest",
                          "2.2.1-SNAPSHOT", "1.2"):
            pin = REAL_PIN.replace("FLINK_VERSION=2.2.1", f"FLINK_VERSION={bad_value}")
            r = self.run_check(pin_text=pin)
            sec = self.section(r.stdout + r.stderr, "[4/6]")
            self.assertIn("FAIL", sec, f"{bad_value!r} must fail check 4: {sec}")
        r = self.run_check()
        self.assertIn("FLINK_VERSION pinned", r.stdout)
        self.assertIn("FLUSS_VERSION pinned", r.stdout)

    def test_missing_versions_pin_fails_explicitly(self):
        r = self.run_check(pin_text=REAL_PIN)
        self.pin.unlink()
        r = subprocess.run(["bash", str(self.repo / "code/01_platform/04_scripts/pin-check.sh")],
                           capture_output=True, text=True, timeout=60, cwd=str(self.t))
        sec = self.section(r.stdout + r.stderr, "[4/6]")
        self.assertIn("versions.pin missing", sec, sec)

    # P6-140: 12-hex short IDs fail; P6-141: zero refs fail (no abort).
    def test_short_digests_fail(self):
        """P6-140: 12-hex and 63-hex truncations are not pins."""
        lock = ("SHORT_IMAGE=repo:1@sha256:" + "a" * 12 + "\n"
                "ALMOST_IMAGE=repo:1@sha256:" + "b" * 63 + "\n")
        r = self.run_check(lock_text=lock)
        sec = self.section(r.stdout + r.stderr, "[5/6]")
        self.assertIn("FAIL", sec, sec)
        self.assertIn("SHORT_IMAGE", sec)
        self.assertIn("ALMOST_IMAGE", sec)

    def test_real_runtime_lock_check_5_passes(self):
        """The lock that ships must satisfy the tightened P6-140 rule."""
        r = self.run_check(lock_text=REAL_LOCK)
        sec = self.section(r.stdout + r.stderr, "[5/6]")
        self.assertNotIn("FAIL", sec, sec)
        self.assertIn("all digest-pinned", sec)

    def test_local_build_pins_match_the_local_images(self):
        """Each local-build line is the image ID of the image on this host.

        The four images are never pushed, so their ID is the only verifiable
        digest; this leg fails when a rebuild makes the lock stale.
        """
        if shutil.which("docker") is None:
            self.skipTest("no docker binary")
        refs = parse_image_refs(REAL_LOCK)
        for var, repo in LOCAL_BUILD_IMAGES.items():
            with self.subTest(image=var):
                self.assertIn(var, refs, f"{var} missing from runtime.lock")
                pinned = refs[var]
                self.assertRegex(pinned, r"@sha256:[0-9a-f]{64}$")
                host = subprocess.run(
                    ["docker", "image", "inspect", f"{repo}:latest"],
                    capture_output=True, text=True, timeout=60)
                if host.returncode != 0:
                    self.skipTest(f"{repo}:latest not on this host (daemon down or image absent)")
                live = json.loads(host.stdout)[0]["Id"]
                self.assertEqual(pinned.split("@")[1], live,
                                 f"{var} pins {pinned.split('@')[1][:20]}... but {repo}:latest is {live[:20]}...")

    def test_zero_refs_fail_closed(self):
        r = self.run_check(lock_text="# no images here\nFOO=bar\n")
        sec = self.section(r.stdout + r.stderr, "[5/6]")
        self.assertIn("no _IMAGE= refs found", sec, sec)
        self.assertNotIn("OK: 0", r.stdout + r.stderr)

    def test_fully_pinned_lock_passes(self):
        good = ("FLUSS_IMAGE=apache/fluss:0.9.1@sha256:" + "a" * 64 + "\n"
                "  export FLINK_IMAGE = flink:2.2.1@sha256:" + "b" * 64 + "  # trailing comment\n")
        r = self.run_check(lock_text=good)
        sec = self.section(r.stdout + r.stderr, "[5/6]")
        self.assertIn("OK: 2 image refs", sec, sec)

    # P6-139: odd-but-real refs are COUNTED (old filter skipped them silently).
    def test_normalised_refs_are_counted_not_skipped(self):
        lock = ("  SPACE_IMAGE=repo:1@sha256:" + "a" * 64 + "\n"
                "export EXP_IMAGE=repo:1@sha256:" + "b" * 64 + "\n"
                "SPACED_IMAGE = repo:1@sha256:" + "c" * 64 + "\n")
        r = self.run_check(lock_text=lock)
        sec = self.section(r.stdout + r.stderr, "[5/6]")
        self.assertIn("OK: 3 image refs", sec, sec)

    def test_bare_tag_in_normalised_shape_fails(self):
        r = self.run_check(lock_text="  SPACE_IMAGE=repo:1.2.3\n")
        sec = self.section(r.stdout + r.stderr, "[5/6]")
        self.assertIn("FAIL", sec, sec)
        self.assertIn("SPACE_IMAGE", sec)

    # P6-761: the finding is stale — the script already says Six everywhere.
    def test_check_count_is_six(self):
        self.assertIn("# Six checks:", SRC)
        for n in range(1, 7):
            self.assertIn(f"[{n}/6]", SRC)


if __name__ == "__main__":
    unittest.main()
