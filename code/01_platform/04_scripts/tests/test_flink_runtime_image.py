#!/usr/bin/env python3
"""Offline contract tests for the production Flink runtime image (CHG-179).

WHAT THIS SUITE PROTECTS
code/01_platform/01_docker/flink-runtime/ builds the image the production Swarm
stack runs. Every defect it fixes was silent in its absence, so the pins here
are mostly about *not regressing into silence*:

  * a jar whose SHA256 is not pinned  -> a swapped upstream artifact ships
  * the compat jar built from the full mapreduce jar -> the hadoop 2.8.3
    Configuration clash M-16 warns about (NoSuchMethodError getTimeDuration)
  * the file copied from the host plugins dir -> the build depends on
    unversioned, gitignored host binaries
  * ENABLE_BUILT_IN_PLUGINS dropped -> checkpoints have no s3:// filesystem
  * core-site.xml gaining a literal credential -> a secret in git
  * prod losing `command:` -> the container prints usage and exits 0
  * prod losing AWS_REGION -> S3A returns 400

These are pins, not behaviour proofs: they read source text and would stay
green if someone reintroduced a bug in another shape. The behaviour was proven
by hand against a really built image and real R2 on 2026-09-16 (write, read
back, list, delete; negatives for missing conf, missing region, unreadable
secret) — that evidence is recorded in CHG-179, because it needs Docker and
network and cannot live in this hermetic suite.

FAIL-CLOSED INTENT
The pin table is checked for *coverage* as well as content: if a jar is added
to the Dockerfile without a corresponding pin, that is a failure here rather
than a silent unverified COPY.
"""

import pathlib
import re
import unittest

TESTS_DIR = pathlib.Path(__file__).resolve().parent
REPO = TESTS_DIR.parents[3]
RUNTIME = REPO / "code/01_platform/01_docker/flink-runtime"
DOCKERFILE = RUNTIME / "Dockerfile"
FETCH = RUNTIME / "fetch-jars.sh"
CORE_SITE = RUNTIME / "core-site.xml"
BRIDGE = RUNTIME / "20-r2-secrets-from-file.sh"
README = RUNTIME / "README.md"
STACK = REPO / "code/01_platform/01_docker/docker-stack.yml"

FLUSS_VERSION = "0.9.1-incubating"

# The pinned base image (P2-105: a tag-only FROM accepts upstream rebuilds).
PINNED_BASE = (
    "flink:2.2.1-scala_2.12-java17@sha256:"
    "1934ab4b984d0873ef46825b1140377eed34475904aa077062497712d6e259f8"
)

# Every jar the Dockerfile installs into /opt/flink/lib, and whether
# fetch-jars.sh must verify it against a pinned SHA256.
EXPECTED_LIB_JARS = {
    f"fluss-flink-2.2-{FLUSS_VERSION}.jar",
    f"fluss-flink-tiering-{FLUSS_VERSION}.jar",
    f"fluss-lake-iceberg-{FLUSS_VERSION}.jar",
    f"fluss-fs-s3-{FLUSS_VERSION}.jar",
    f"fluss-fs-hdfs-{FLUSS_VERSION}.jar",
    "hadoop-mapreduce-compat-2.8.5.jar",
}


def code_lines(text: str) -> list:
    """Source lines with comments and blanks removed.

    Assertions about what a script DOES must not be satisfied by prose about
    it: these files are heavily commented, and a commented-out COPY would
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
    Dockerfile installs, or the coverage comparison below compares apples to
    oranges and silently passes.
    """
    versions = dict(re.findall(r'^(FLUSS_VERSION|MAPREDUCE_VERSION)="([^"]+)"', text, re.M))
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

    def test_every_expected_jar_is_installed_into_flink_lib(self) -> None:
        copies = [ln for ln in self.lines if ln.upper().startswith("COPY") and "/opt/flink/lib/" in ln]
        installed = set()
        for ln in copies:
            m = re.search(r"jars/(\S+)", ln)
            if m:
                installed.add(m.group(1))
        self.assertEqual(installed, EXPECTED_LIB_JARS,
                         f"jar set drifted\n only in Dockerfile: {installed - EXPECTED_LIB_JARS}\n"
                         f" only expected: {EXPECTED_LIB_JARS - installed}")

    def test_no_jar_is_copied_from_the_host_plugins_tree(self) -> None:
        """Host plugin jars are gitignored, unversioned binaries.

        The build must not depend on whatever a developer happens to have
        downloaded into fluss-plugins/ — the fetch script is the only source.
        """
        for ln in self.lines:
            if ln.upper().startswith("COPY"):
                self.assertNotIn("fluss-plugins", ln,
                                 f"COPY must not source the host plugins tree: {ln}")
        self.assertNotIn("fluss-plugins", self.text.replace("dev compose", ""))

    def test_the_forbidden_hadoop_uber_jar_is_absent(self) -> None:
        """M-16: hadoop-uber 2.8.3 breaks S3A with NoSuchMethodError."""
        for ln in self.lines:
            self.assertNotIn("flink-shaded-hadoop-2-uber", ln,
                             f"M-16 violation — the uber jar must never enter the image: {ln}")

    def test_compat_jar_comes_from_the_derived_staging_dir(self) -> None:
        """The compat jar must be the DERIVED mapreduce-only package.

        Copying hadoop-mapreduce-client-core wholesale would bring
        org/apache/hadoop/mapred/** — precisely the clash M-16 documents.
        """
        compat = [ln for ln in self.lines if "mapreduce-compat" in ln and ln.upper().startswith("COPY")]
        self.assertEqual(len(compat), 1, f"expected one compat COPY, got {compat}")
        self.assertIn("jars/hadoop-mapreduce-compat-2.8.5.jar", compat[0])
        for ln in self.lines:
            self.assertNotIn("hadoop-mapreduce-client-core", ln,
                             f"the full mapreduce jar must not be installed: {ln}")

    def test_built_in_plugin_is_enabled(self) -> None:
        """Without this, s3:// has no FileSystemFactory and checkpoints fail."""
        envs = [ln for ln in self.lines if ln.startswith("ENV ")]
        self.assertTrue(any("ENABLE_BUILT_IN_PLUGINS" in ln and "flink-s3-fs-hadoop" in ln
                            for ln in envs),
                        f"ENABLE_BUILT_IN_PLUGINS must enable flink-s3-fs-hadoop; ENV lines: {envs}")

    def test_core_site_is_baked_into_the_auto_detected_hadoop_conf_dir(self) -> None:
        """/etc/hadoop/conf is on Flink's classpath with zero settings."""
        hits = [ln for ln in self.lines
                if ln.upper().startswith("COPY") and "/etc/hadoop/conf" in ln]
        self.assertEqual(len(hits), 1, f"expected one core-site COPY, got {hits}")

    def test_entrypoint_is_the_secret_bridge_with_a_fallback_cmd(self) -> None:
        """ENTRYPOINT clears CMD, and a null CMD exits 0 silently.

        Observed: with no CMD the base entrypoint prints nothing and returns 0,
        so a misconfigured deploy looks started while doing nothing.
        """
        entry = [ln for ln in self.lines if ln.startswith("ENTRYPOINT")]
        self.assertEqual(len(entry), 1, f"expected one ENTRYPOINT, got {entry}")
        self.assertIn("20-r2-secrets-from-file.sh", entry[0])
        cmds = [ln for ln in self.lines if ln.startswith("CMD")]
        self.assertTrue(cmds, "CMD must be redeclared — setting ENTRYPOINT clears the base default")
        self.assertIn("help", cmds[0],
                      "CMD must fall back to the base image's help path")

    def test_returns_to_an_unprivileged_user(self) -> None:
        users = [ln for ln in self.lines if ln.startswith("USER ")]
        self.assertTrue(users, "no USER directive")
        self.assertEqual(users[-1], "USER flink",
                         "the final USER must be the base image's unprivileged user")

    def test_bridge_is_copied_executable(self) -> None:
        hits = [ln for ln in self.lines if "20-r2-secrets-from-file.sh" in ln
                and ln.upper().startswith("COPY")]
        self.assertEqual(len(hits), 1, f"expected one bridge COPY, got {hits}")
        self.assertIn("--chmod=0555", hits[0],
                      "the bridge is an ENTRYPOINT — it must be executable for uid 9999")


class FetchScriptTests(unittest.TestCase):
    """The supply-chain half: every byte entering the image is pinned."""

    def setUp(self) -> None:
        self.assertTrue(FETCH.is_file(), f"{FETCH} missing")
        self.text = FETCH.read_text(encoding="utf-8")
        self.pins = pinned_jars(self.text)

    def test_every_installed_jar_is_covered_by_a_pin(self) -> None:
        """Coverage: a new COPY without a pin must fail here, not ship unverified.

        Two pin mechanisms exist and both count: the PINS table (downloaded
        artifacts) and COMPAT_SHA256 (the derived jar). A jar installed from a
        third, unpinned source is the regression this catches.
        """
        compat = re.search(r'COMPAT_JAR="([^"]+)"', self.text)
        self.assertIsNotNone(compat, "COMPAT_JAR must be named")
        derived = compat.group(1).replace("${MAPREDUCE_VERSION}", "2.8.5")
        covered = set(self.pins) | {derived}
        missing = EXPECTED_LIB_JARS - covered
        self.assertFalse(missing,
                         f"jars installed by the Dockerfile with no pin anywhere: {sorted(missing)}")

    def test_every_downloaded_jar_is_either_installed_or_a_declared_input(self) -> None:
        """The inverse check: a pinned download nobody installs is dead weight.

        hadoop-mapreduce-client-core is a legitimate exception — it is fetched
        and verified only so the compat jar can be derived from it, and it must
        NOT be installed (it carries org/apache/hadoop/mapred/**)."""
        declared_inputs = {"hadoop-mapreduce-client-core-2.8.5.jar"}
        unused = set(self.pins) - EXPECTED_LIB_JARS - declared_inputs
        self.assertFalse(unused,
                         f"pinned but neither installed nor declared as a build input: {sorted(unused)}")

    def test_pins_are_full_length_sha256(self) -> None:
        self.assertTrue(self.pins, "no pins parsed from fetch-jars.sh")
        for name, sha in self.pins.items():
            self.assertRegex(sha, r"^[0-9a-f]{64}$", f"{name} has a malformed pin")

    def test_compat_jar_has_its_own_pin(self) -> None:
        self.assertIn("COMPAT_SHA256", self.text)
        m = re.search(r'COMPAT_SHA256="([0-9a-f]{64})"', self.text)
        self.assertIsNotNone(m, "COMPAT_SHA256 must be a full 64-hex pin")
        self.assertNotIn(m.group(1), set(self.pins.values()),
                         "the derived jar must not share a pin with a downloaded artifact")

    def test_a_mismatch_aborts_the_build(self) -> None:
        """A verified-but-wrong artifact must stop, not warn."""
        self.assertIn("die", self.text, "fetch-jars must have a fatal path")
        self.assertRegex(self.text, r"refusing to build an image from it",
                         "the mismatch message must state the build is refused")

    def test_derivation_confines_itself_to_the_mapreduce_package(self) -> None:
        """M-15/M-16: mapreduce/** only — never org/apache/hadoop/mapred/**."""
        self.assertIn("'org/apache/hadoop/mapreduce/*'", self.text,
                      "the extraction filter must select mapreduce/** exactly")
        self.assertRegex(self.text, r"grep -v '\^org/apache/hadoop/mapreduce/'",
                         "the derivation must assert nothing outside mapreduce/** survived")

    def test_derivation_pins_file_mtimes(self) -> None:
        """Without a fixed mtime the zip bytes vary and the pin is meaningless."""
        self.assertRegex(self.text, r"touch -t \"?\$?\{?DERIVE_MTIME",
                         "the derivation must normalise file timestamps")
        self.assertRegex(self.text, r'DERIVE_MTIME="\d{12}"')

    def test_derivation_pins_the_collation(self) -> None:
        """Without a pinned collation the entry order follows the caller's locale.

        Measured 2026-09-21: the same classes sorted under en_IN.UTF-8 hash to
        14c5a8e5…, sorted under C to c19c414b…. The pin is the C form, so the
        first CI run — a C-locale runner — failed a derivation that had passed on
        the workstation for weeks. An unpinned `sort` reintroduces that.
        """
        self.assertRegex(self.text, r"LC_ALL=C sort",
                         "the derivation must pin the sort collation, not inherit the locale")

    def test_the_unpublished_snapshot_jar_is_not_fetched(self) -> None:
        """fluss-fs-hadoop-shaded-0.9-SNAPSHOT cannot be checksum-pinned.

        The name appears in this file's comments (explaining why it is absent),
        so only executable lines are checked — a commented-out download reads
        as prose, but an uncommented one would be a real regression.
        """
        for line in code_lines(self.text):
            self.assertNotIn("hadoop-shaded", line,
                             f"the unpublished SNAPSHOT jar must not be fetched: {line}")
            self.assertNotIn("SNAPSHOT", line,
                             f"a SNAPSHOT cannot be part of a reproducible image: {line}")

    def test_verify_mode_is_offline(self) -> None:
        """--verify must not reach the network: the test suite runs it hermetically."""
        verify_block = self.text.split("--verify)", 1)
        self.assertEqual(len(verify_block), 2, "--verify mode missing")
        tail = verify_block[1].split("esac", 1)[0]
        self.assertNotIn("curl", tail, "--verify must be offline (no downloads)")

    def test_failed_verification_exits_nonzero(self) -> None:
        """The test suite asserts exit codes; the script must set a real one."""
        self.assertRegex(self.text, r"return \"\$rc\"|exit 1|die ",
                         "verification failure must produce a non-zero exit")


class CoreSiteTests(unittest.TestCase):
    """The Hadoop config must carry settings but never secrets."""

    def setUp(self) -> None:
        self.assertTrue(CORE_SITE.is_file(), f"{CORE_SITE} missing")
        self.text = CORE_SITE.read_text(encoding="utf-8")

    def test_uses_environment_placeholders(self) -> None:
        for var in ("R2_ENDPOINT", "AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            self.assertIn("${env." + var + "}", self.text,
                          f"{var} must be read from the environment at expansion time")

    def test_carries_no_literal_credential(self) -> None:
        """A real key is 32 chars, a real secret 64 — neither may appear."""
        # Only the <value> bodies can leak a credential.
        values = re.findall(r"<value>([^<]*)</value>", self.text)
        self.assertTrue(values, "no properties parsed — the file shape changed")
        for v in values:
            v = v.strip()
            if v.startswith("${env.") or v in ("true", "false"):
                continue
            self.fail(f"literal, non-placeholder value in core-site.xml: {v!r}")

    def test_pins_r2_addressing(self) -> None:
        """R2 needs path-style access and TLS; without them S3A misroutes."""
        self.assertIn("fs.s3a.path.style.access", self.text)
        self.assertIn("fs.s3a.endpoint", self.text)

    def test_region_is_present(self) -> None:
        """Verified: an unset region makes S3A return 400 Bad Request."""
        self.assertIn("fs.s3a.endpoint.region", self.text,
                      "without a region, S3A requests fail 400")


class BridgeTests(unittest.TestCase):
    """The entrypoint wrapper: secrets in, fail closed when unreadable."""

    def setUp(self) -> None:
        self.assertTrue(BRIDGE.is_file(), f"{BRIDGE} missing")
        self.text = BRIDGE.read_text(encoding="utf-8")
        self.lines = code_lines(self.text)

    def test_bridges_both_credential_variables(self) -> None:
        for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            self.assertRegex(self.text, rf"bridge\s+{var}" + r"\b",
                             f"{var} must be bridged from its _FILE counterpart")

    def test_derives_the_file_variable_from_the_target(self) -> None:
        """`X_FILE` must track `X`, so adding a secret cannot silently skip it."""
        self.assertRegex(self.text, r'_FILE', "the *_FILE naming convention must be derived")

    def test_strips_trailing_newlines(self) -> None:
        """Swarm secret files end with a newline; a signature must not."""
        self.assertIn("tr -d", self.text)

    def test_unreadable_secret_fails_closed(self) -> None:
        self.assertRegex(self.text, r"! -r ", "readability must be tested before use")
        self.assertRegex(self.text, r"exit 1", "an unreadable secret must abort startup")
        self.assertRegex(self.text, r"refusing to start",
                         "the failure must say it is refusing to start")

    def test_missing_variable_is_not_an_error(self) -> None:
        """Local compose passes the values directly; the bridge must no-op then."""
        self.assertRegex(self.text, r'file="\$\{!file_var:-\}"|:-\}',
                         "an unset *_FILE must be tolerated, not treated as a failure")

    def test_execs_the_base_entrypoint_preserving_arguments(self) -> None:
        self.assertRegex(self.text, r'exec /docker-entrypoint\.sh "\$@"',
                         "the bridge must hand off to the base entrypoint with our args")

    def test_is_hermetic_under_set_u(self) -> None:
        """`set -u` with an absent *_FILE would otherwise abort the container."""
        self.assertIn("set -euo pipefail", self.text)


class ProductionStackTests(unittest.TestCase):
    """The production defects this image exposes must stay fixed."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.text = STACK.read_text(encoding="utf-8")

    def _service_block(self, name: str) -> str:
        """The YAML block for one top-level service, by indentation."""
        lines = self.text.splitlines()
        start = None
        for i, ln in enumerate(lines):
            if ln.startswith(f"  {name}:"):
                start = i
                break
        self.assertIsNotNone(start, f"service {name} not found in docker-stack.yml")
        block = []
        for ln in lines[start + 1:]:
            if ln.rstrip() and not ln.startswith("    ") and not ln.startswith("  #"):
                break
            block.append(ln)
        return "\n".join(block)

    def test_flink_services_declare_what_to_run(self) -> None:
        """Verified: with no command the container prints usage and exits 0.

        A deploy would report the task as started while nothing runs.
        """
        for svc, cmd in (("flink-jobmanager", "jobmanager"), ("flink-taskmanager", "taskmanager")):
            with self.subTest(service=svc):
                block = self._service_block(svc)
                self.assertRegex(block, rf'command:\s*\["{cmd}"\]',
                                 f"{svc} must declare command: [\"{cmd}\"] — "
                                 f"the image's CMD is only the usage fallback")

    def test_flink_services_receive_a_region(self) -> None:
        """Verified: without a region S3A returns 400 Bad Request."""
        for svc in ("flink-jobmanager", "flink-taskmanager"):
            with self.subTest(service=svc):
                block = self._service_block(svc)
                self.assertRegex(block, r"AWS_REGION:\s*\$\{AWS_REGION",
                                 f"{svc} needs AWS_REGION for its S3A reads/writes")

    def test_flink_services_consume_the_swarm_secrets(self) -> None:
        """The bridge reads *_FILE, so the secret files must actually be mounted."""
        for svc in ("flink-jobmanager", "flink-taskmanager"):
            with self.subTest(service=svc):
                block = self._service_block(svc)
                self.assertIn("aws_access_key_id", block)
                self.assertIn("aws_secret_access_key", block)

    def test_stack_still_cannot_build(self) -> None:
        """Swarm ignores build: — the image must arrive pre-built."""
        for ln in self.text.splitlines():
            self.assertNotRegex(ln, r"^\s+build:", "docker-stack.yml must not contain build:")

    def test_flink_image_is_referenced_from_the_lock(self) -> None:
        self.assertIn("${FLINK_IMAGE:?set FLINK_IMAGE to an immutable digest}", self.text,
                      "FLINK_IMAGE must stay digest-pinned and fail if unset")


class DocumentationTests(unittest.TestCase):
    """The README is the operator's only map for building and pinning."""

    def setUp(self) -> None:
        self.assertTrue(README.is_file(), f"{README} missing")
        self.text = README.read_text(encoding="utf-8")

    def test_documents_the_build_entry_point(self) -> None:
        self.assertIn("make flink-image", self.text)

    def test_documents_that_pushing_precedes_pinning(self) -> None:
        """An unpushed image has no manifest digest — repo@image-id does not resolve."""
        self.assertIn("docker push", self.text)
        self.assertIn("digest-pin.sh", self.text)

    def test_records_why_the_snapshot_jar_is_absent(self) -> None:
        self.assertIn("hadoop-shaded", self.text)
        self.assertIn("unpublished", self.text.lower())

    def test_records_the_compat_jar_derivation(self) -> None:
        self.assertIn("mapreduce", self.text)
        self.assertIn("M-15", self.text, "the compat jar's origin must cite the finding")

    def test_states_the_offline_limit_honestly(self) -> None:
        """The suite cannot prove a running tiering job; the README must say so."""
        self.assertIn("Residual risk", self.text)


if __name__ == "__main__":
    unittest.main()
