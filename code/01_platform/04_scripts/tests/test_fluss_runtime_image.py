#!/usr/bin/env python3
"""Offline contract tests for the production Fluss runtime image (CHG-183).

WHAT THIS SUITE PROTECTS
code/01_platform/01_docker/fluss-runtime/ builds the image the production Swarm
stack runs for the Fluss servers. The defect it fixes is silent in its absence:
the dev compose makes lake tiering work by bind-mounting two jars out of
fluss-plugins/ — a gitignored host tree. A fresh clone on the production VM has
neither those files nor any way to mount them (a Swarm stack cannot express a
per-node bind mount portably), so Fluss would start with no S3A in the iceberg
plugin classloader and lake tiering would fail there and only there.

  * a jar copied from the host plugins tree -> the build depends on untracked
    binaries again, which is the exact defect this image exists to fix
  * the jars landing in plugins/s3 or plugins/hdfs instead of plugins/iceberg
    -> the iceberg classloader cannot see them and nothing changes
  * a jar whose SHA256 is not pinned -> a swapped upstream artifact ships
  * the unpublished SNAPSHOT jar being fetched -> an unpinnable artifact enters
    a supposedly reproducible image
  * the compat jar being added -> dead weight; the servers do not write parquet

These are pins, not behaviour proofs: they read source text and would stay green
if someone reintroduced a bug in another shape. The behaviour was proven by hand
against a really built image on 2026-09-16 (class resolution in the iceberg
classloader before/after, server boot) — that evidence is recorded in CHG-183,
because it needs Docker and cannot live in this hermetic suite.

FAIL-CLOSED INTENT
The pin table is checked for *coverage* as well as content: if a jar is added to
the Dockerfile without a corresponding pin, that is a failure here rather than a
silent unverified COPY.
"""

import pathlib
import re
import unittest

TESTS_DIR = pathlib.Path(__file__).resolve().parent
REPO = TESTS_DIR.parents[3]
RUNTIME = REPO / "code/01_platform/01_docker/fluss-runtime"
DOCKERFILE = RUNTIME / "Dockerfile"
FETCH = RUNTIME / "fetch-jars.sh"
README = RUNTIME / "README.md"
STACK = REPO / "code/01_platform/01_docker/docker-stack.yml"
LOCK = REPO / "code/01_platform/01_docker/runtime.lock"
FLINK_FETCH = REPO / "code/01_platform/01_docker/flink-runtime/fetch-jars.sh"

FLUSS_VERSION = "1.0.0"

# The pinned base image. Must equal runtime.lock's FLUSS_IMAGE: the derived
# image is built FROM the very digest the stack would otherwise run, so the two
# cannot silently diverge.
PINNED_BASE = (
    "apache/fluss:1.0.0@sha256:"
    "ff461b45438033da4fd1c2556d3f978f3603bb3632fe075c2bd57388339a58cb"
)

# Every jar the Dockerfile installs into /opt/fluss/plugins/iceberg/.
EXPECTED_PLUGIN_JARS = {
    f"fluss-fs-s3-{FLUSS_VERSION}.jar",
    f"fluss-fs-hdfs-{FLUSS_VERSION}.jar",
}

ICEBERG_PLUGIN_DIR = "/opt/fluss/plugins/iceberg/"


def code_lines(text: str) -> list:
    """Source lines with comments and blanks removed.

    Assertions about what a script DOES must not be satisfied by prose about
    it: these files are heavily commented (the SNAPSHOT jar is discussed at
    length precisely because it is absent), and a commented-out COPY would
    otherwise read as an installed jar.
    """
    out = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0] if not raw.lstrip().startswith("#") else ""
        line = line.strip()
        if line:
            out.append(line)
    return out


def pinned_jars(text: str) -> dict:
    """name -> sha256 from the PINS table in fetch-jars.sh.

    The table is written with shell references ("fluss-fs-s3-${FLUSS_VERSION}.jar")
    so a version bump is one edit; expand them back to the literal filename the
    Dockerfile installs, or the coverage comparison below silently passes.
    """
    versions = dict(re.findall(r'^(FLUSS_VERSION)="([^"]+)"', text, re.M))
    found = {}
    for m in re.finditer(r'"([^"|]+)\|([0-9a-f]{64})\|([^"]+)"', text):
        name = m.group(1)
        for var, value in versions.items():
            name = name.replace("${" + var + "}", value)
        found[name] = m.group(2)
    return found


class DockerfileTests(unittest.TestCase):
    """The image definition itself."""

    def setUp(self) -> None:
        self.assertTrue(DOCKERFILE.is_file(), f"{DOCKERFILE} missing")
        self.text = DOCKERFILE.read_text(encoding="utf-8")
        self.lines = code_lines(self.text)

    def test_base_image_is_digest_pinned(self) -> None:
        froms = [ln for ln in self.lines if ln.upper().startswith("FROM")]
        self.assertEqual(len(froms), 1, f"expected exactly one FROM, got {froms}")
        self.assertIn(PINNED_BASE, froms[0],
                      "base image must be pinned by digest, not by tag alone")

    def test_base_matches_the_digest_the_stack_would_otherwise_run(self) -> None:
        """The derived image supersedes FLUSS_IMAGE, so they must agree.

        If runtime.lock moves to a different Fluss digest and this FROM does
        not, the stack silently runs an older Fluss than the one it advertises.
        """
        lock = LOCK.read_text(encoding="utf-8")
        m = re.search(r"^FLUSS_IMAGE=(\S+)", lock, re.M)
        self.assertIsNotNone(m, "FLUSS_IMAGE must be defined in runtime.lock")
        self.assertEqual(m.group(1), PINNED_BASE,
                         "the Dockerfile FROM and runtime.lock's FLUSS_IMAGE must be the "
                         "same digest — otherwise the two drift apart silently")

    def test_every_expected_jar_is_installed_into_the_iceberg_plugin_dir(self) -> None:
        """plugins/iceberg is the only directory the iceberg loader can see.

        Fluss loads each plugin directory as its own classloader, so copying a
        jar into plugins/s3 (where an identically named file already sits) is a
        no-op. This is the assertion that catches that.
        """
        copies = [ln for ln in self.lines
                  if ln.upper().startswith("COPY") and "plugins/" in ln]
        installed = set()
        for ln in copies:
            m = re.search(r"jars/(\S+)", ln)
            if m:
                installed.add(m.group(1))
            self.assertIn(ICEBERG_PLUGIN_DIR, ln,
                          f"jar must land in the iceberg plugin dir: {ln}")
        self.assertEqual(installed, EXPECTED_PLUGIN_JARS,
                         f"jar set drifted\n only in Dockerfile: {installed - EXPECTED_PLUGIN_JARS}\n"
                         f" only expected: {EXPECTED_PLUGIN_JARS - installed}")

    def test_no_jar_is_copied_from_the_host_plugins_tree(self) -> None:
        """Host plugin jars are gitignored, unversioned binaries.

        Verified 2026-09-16: `git ls-files .../fluss-plugins/` returns one file
        (the README). Depending on that tree is precisely the defect this image
        fixes, so the build must source jars from fetch-jars.sh only.
        """
        for ln in self.lines:
            if ln.upper().startswith("COPY"):
                self.assertNotIn("fluss-plugins", ln,
                                 f"COPY must not source the host plugins tree: {ln}")

    def test_no_secrets_bridge_is_baked_in(self) -> None:
        """The stack already delivers the bridge as a Swarm config + entrypoint.

        Baking a second copy would make two sources of truth for the R2
        credentials path. This image carries jars, not scripts.

        Only executable lines are checked: the Dockerfile discusses the bridge
        in prose precisely to record why it is absent.
        """
        for ln in self.lines:
            self.assertNotIn("r2-secrets-from-file", ln,
                             f"the secrets bridge is a Swarm config, not part of this image: {ln}")

    def test_the_compat_jar_is_not_added_here(self) -> None:
        """M-15's compat jar is for iceberg's parquet WRITE path.

        That path runs in the Flink tiering job, not in the Fluss servers; the
        dev tablet has no compat jar and lake tiering works there.
        """
        for ln in self.lines:
            self.assertNotIn("mapreduce", ln.lower(),
                             f"the servers do not need the compat jar: {ln}")

    def test_the_forbidden_hadoop_uber_jar_is_absent(self) -> None:
        """M-16: hadoop-uber 2.8.3 breaks S3A with NoSuchMethodError."""
        for ln in self.lines:
            self.assertNotIn("flink-shaded-hadoop-2-uber", ln,
                             f"M-16 violation — the uber jar must never enter the image: {ln}")


class FetchScriptTests(unittest.TestCase):
    """The supply-chain half: every byte entering the image is pinned."""

    def setUp(self) -> None:
        self.assertTrue(FETCH.is_file(), f"{FETCH} missing")
        self.text = FETCH.read_text(encoding="utf-8")
        self.pins = pinned_jars(self.text)

    def test_every_installed_jar_is_covered_by_a_pin(self) -> None:
        missing = EXPECTED_PLUGIN_JARS - set(self.pins)
        self.assertFalse(missing,
                         f"jars installed by the Dockerfile with no pin: {sorted(missing)}")

    def test_every_downloaded_jar_is_installed(self) -> None:
        unused = set(self.pins) - EXPECTED_PLUGIN_JARS
        self.assertFalse(unused,
                         f"pinned but never installed (dead weight): {sorted(unused)}")

    def test_pins_are_full_length_sha256(self) -> None:
        self.assertTrue(self.pins, "no pins parsed from fetch-jars.sh")
        for name, sha in self.pins.items():
            self.assertRegex(sha, r"^[0-9a-f]{64}$", f"{name} has a malformed pin")

    def test_pins_match_the_flink_image_pins(self) -> None:
        """The same two artifacts enter both images, so they must agree.

        A divergence means one image would carry a jar the other does not
        expect — exactly the kind of silent drift that is invisible at runtime.
        """
        flink_pins = pinned_jars(FLINK_FETCH.read_text(encoding="utf-8"))
        self.assertTrue(flink_pins, "the Flink image pins must be parseable")
        for name, sha in self.pins.items():
            self.assertIn(name, flink_pins,
                          f"{name} is pinned here but not by the Flink image")
            self.assertEqual(sha, flink_pins[name],
                             f"{name} has a different pin in each image")

    def test_a_mismatch_aborts_the_build(self) -> None:
        self.assertIn("die", self.text, "fetch-jars must have a fatal path")
        self.assertRegex(self.text, r"refusing to build an image from it",
                         "the mismatch message must state the build is refused")

    def test_the_unpublished_snapshot_jar_is_not_fetched(self) -> None:
        """fluss-fs-hadoop-shaded is 404 on every public repo (and is gone in 1.0.0:
    its Hadoop classes now ship inside fluss-fs-s3/hdfs, so no mount needs it).

        The name appears in this file's comments (explaining why it is absent),
        so only executable lines are checked.
        """
        for line in code_lines(self.text):
            self.assertNotIn("hadoop-shaded", line,
                             f"the unpublished SNAPSHOT jar must not be fetched: {line}")
            self.assertNotIn("SNAPSHOT", line,
                             f"a SNAPSHOT cannot be part of a reproducible image: {line}")

    def test_verify_mode_is_offline(self) -> None:
        """--verify must not reach the network: the suite runs it hermetically."""
        verify_block = self.text.split("--verify)", 1)
        self.assertEqual(len(verify_block), 2, "--verify mode missing")
        tail = verify_block[1].split("esac", 1)[0]
        self.assertNotIn("curl", tail, "--verify must be offline (no downloads)")

    def test_failed_verification_exits_nonzero(self) -> None:
        self.assertRegex(self.text, r"return \"\$rc\"|exit 1|die ",
                         "verification failure must produce a non-zero exit")


class StackWiringTests(unittest.TestCase):
    """How the image reaches the running stack."""

    def setUp(self) -> None:
        self.assertTrue(STACK.is_file(), f"{STACK} missing")
        self.stack = STACK.read_text(encoding="utf-8")

    def test_stack_still_pins_fluss_image_by_digest_with_a_failclosed_default(self) -> None:
        """Swapping to a derived image must not weaken the pin contract.

        The stack must keep `FLUSS_IMAGE` fail-closed (`:?`) and the lockfile
        must keep a real digest, so an unpushed local tag cannot silently become
        the production image.
        """
        self.assertIn("image: ${FLUSS_IMAGE:?", self.stack,
                      "FLUSS_IMAGE must stay fail-closed and digest-pinned")
        lock = LOCK.read_text(encoding="utf-8")
        m = re.search(r"^FLUSS_IMAGE=(\S+)", lock, re.M)
        self.assertIsNotNone(m, "FLUSS_IMAGE must be defined in runtime.lock")
        self.assertRegex(m.group(1), r"@sha256:[0-9a-f]{64}$",
                         "runtime.lock's FLUSS_IMAGE must carry a manifest digest")

    def test_stack_carries_no_plugin_bind_mount(self) -> None:
        """A Swarm stack cannot portably express a per-node bind mount.

        If a plugin bind mount ever appears, the deploy depends on host files
        again — the failure this image removes.
        """
        for ln in code_lines(self.stack):
            self.assertNotIn("fluss-plugins", ln,
                             f"the stack must not bind-mount host plugin jars: {ln}")


class ReadmeTests(unittest.TestCase):
    """The README is what the next operator reads before a deploy."""

    def setUp(self) -> None:
        self.assertTrue(README.is_file(), f"{README} missing")
        self.text = README.read_text(encoding="utf-8")

    def test_documents_why_the_stock_image_is_insufficient(self) -> None:
        self.assertRegex(self.text, r"classloader",
                         "the README must explain the per-plugin classloader, which is "
                         "the whole reason the jars cannot stay in plugins/s3")

    def test_documents_the_unpushed_image_pinning_caveat(self) -> None:
        """A locally built image has no manifest digest.

        Without this warning an operator pins repo@sha256:<image-id>, which does
        not resolve — and the failure only appears at deploy time.
        """
        self.assertRegex(self.text, r"manifest digest",
                         "the README must state that push precedes pinning")


if __name__ == "__main__":
    unittest.main()
