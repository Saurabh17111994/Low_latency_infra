#!/usr/bin/env python3
"""Unit tests for image_staleness_check.py — CHG-101 stale-image guard.

Pure helpers get synthetic inputs; the git-backed functions run against a
real throwaway repository with pinned commit dates (GIT_AUTHOR_DATE /
GIT_COMMITTER_DATE) so source epochs are deterministic. Docker is never
touched: image_created_epoch is monkeypatched.

Run: python3 -m unittest discover -s code/01_platform/04_scripts/tests -v
"""
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import image_staleness_check as isc  # noqa: E402

EPOCH_A = 1787500000  # fixed "source last commit" epoch
EPOCH_B = EPOCH_A + 100


def make_repo(path: Path, filename: str = "src/file.txt", epoch: int = EPOCH_A):
    """Init a throwaway git repo with one commit at `epoch`."""
    repo = path / "repo"
    repo.mkdir()
    env = dict(os.environ,
               GIT_AUTHOR_DATE=f"@{epoch} +0000",
               GIT_COMMITTER_DATE=f"@{epoch} +0000",
               GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t",
               GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo,
                   env=env, check=True, capture_output=True)
    src = repo / filename
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("v1\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, env=env,
                   capture_output=True)
    subprocess.run(["git", "commit", "-q", "-m", "one"], cwd=repo, check=True,
                   env=env, capture_output=True)
    return repo


class BuildServicesTest(unittest.TestCase):
    def test_build_services_parses_contexts(self):
        compose = {"services": {
            "a": {"build": {"context": "..", "dockerfile": "A/Dockerfile"},
                  "profiles": ["x"]},
            "b": {"image": "apache/fluss:0.9.1-incubating"},  # pull-only
        }}
        services = isc.build_services(compose)
        self.assertEqual(list(services), ["a"])
        self.assertEqual(services["a"]["context"], "..")
        self.assertEqual(services["a"]["dockerfile"], "A/Dockerfile")
        self.assertEqual(services["a"]["profile"], ["x"])

    def test_load_compose_rejects_missing_services(self):
        with self.assertRaises(ValueError):
            isc.load_compose(Path("/dev/null"))


class DeclaredImageTest(unittest.TestCase):
    """A compose `image:` declaration must be the reference the checker
    inspects. Regression: loadgen declares pipeline-loadgen:1.0.0, so the
    derived default 01_docker-loadgen never exists and a built, fresh image
    was reported MISSING (gate step 8 could not pass)."""

    def test_build_services_captures_declared_image(self):
        compose = {"services": {
            "loadgen": {"build": {"context": "../.."},
                        "image": "pipeline-loadgen:1.0.0",
                        "profiles": ["loadgen"]},
            "plain": {"build": {"context": "."}},
        }}
        services = isc.build_services(compose)
        self.assertEqual(services["loadgen"]["image"], "pipeline-loadgen:1.0.0")
        self.assertIsNone(services["plain"]["image"], "absent declaration -> None")

    def test_declared_image_wins_over_derived_default(self):
        self.assertEqual(
            isc.image_ref("01_docker", "loadgen", {"image": "pipeline-loadgen:1.0.0"}),
            "pipeline-loadgen:1.0.0")

    def test_derived_default_when_no_declaration(self):
        self.assertEqual(isc.image_ref("01_docker", "ingestion", {"image": None}),
                         "01_docker-ingestion")
        self.assertEqual(isc.image_ref("01_docker", "ingestion"),
                         "01_docker-ingestion")


class StampTest(unittest.TestCase):
    """The content stamp must move on content — never on clocks, layout, or
    build outputs. That is the whole point of replacing the timestamp proxy
    (proven unsound live on 2026-09-02 and again 2026-09-12)."""

    @staticmethod
    def _tree(td: Path) -> Path:
        root = td / "root"
        for name in ("a.txt", "sub/b.txt", "sub/deep/c.txt"):
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"content of {name}\n", encoding="utf-8")
        return root

    def test_touch_and_identical_rewrite_do_not_move_the_stamp(self):
        with tempfile.TemporaryDirectory() as td:
            root = self._tree(Path(td))
            with mock.patch.dict(isc.SERVICE_SOURCES, {"probe": ["."]}):
                before = isc.input_stamp(root, "probe")
                (root / "a.txt").write_text("content of a.txt\n", encoding="utf-8")
                # The clock change must be the LAST mutation: an earlier utime
                # would be overwritten by the rewrite, and on a coarse-grained
                # filesystem a clock-dependent implementation could then slip
                # through this guard (caught while falsifying it, 2026-09-12).
                os.utime(root / "a.txt", (1, 1))
                self.assertEqual(isc.input_stamp(root, "probe"), before,
                                 "mtime-only changes must be invisible")

    def test_one_byte_edit_moves_the_stamp(self):
        with tempfile.TemporaryDirectory() as td:
            root = self._tree(Path(td))
            with mock.patch.dict(isc.SERVICE_SOURCES, {"probe": ["."]}):
                before = isc.input_stamp(root, "probe")
                (root / "sub" / "deep" / "c.txt").write_text("x\n", encoding="utf-8")
                self.assertNotEqual(isc.input_stamp(root, "probe"), before)

    def test_build_outputs_are_not_inputs(self):
        with tempfile.TemporaryDirectory() as td:
            root = self._tree(Path(td))
            with mock.patch.dict(isc.SERVICE_SOURCES, {"probe": ["."]}):
                before = isc.input_stamp(root, "probe")
                for junk in ("target/classes/A.class", "sub/__pycache__/a.pyc",
                             "node_modules/pkg/index.js", ".git/HEAD"):
                    path = root / junk
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("junk\n", encoding="utf-8")
                self.assertEqual(isc.input_stamp(root, "probe"), before,
                                 "build outputs must not move the stamp")

    def test_stamp_is_independent_of_the_absolute_path(self):
        with tempfile.TemporaryDirectory() as one, tempfile.TemporaryDirectory() as two:
            left, right = self._tree(Path(one)), self._tree(Path(two))
            with mock.patch.dict(isc.SERVICE_SOURCES, {"probe": ["."]}):
                self.assertEqual(isc.input_stamp(left, "probe"),
                                 isc.input_stamp(right, "probe"))

    def test_compose_dockerfile_is_an_input(self):
        with tempfile.TemporaryDirectory() as td:
            root = self._tree(Path(td))
            dockerfile = root / "Dockerfile"
            dockerfile.write_text("FROM scratch\n", encoding="utf-8")
            entry = {"context": ".", "dockerfile": "Dockerfile"}
            with mock.patch.dict(isc.SERVICE_SOURCES, {}, clear=True):
                before = isc.input_stamp(root, "probe", entry, root)
                dockerfile.write_text("FROM scratch\nLABEL x=y\n", encoding="utf-8")
                after = isc.input_stamp(root, "probe", entry, root)
            self.assertIsNotNone(before, "the Dockerfile alone is an input")
            self.assertNotEqual(before, after)

    def test_stamp_is_none_when_nothing_can_be_hashed(self):
        with tempfile.TemporaryDirectory() as td:
            root = self._tree(Path(td))
            self.assertIsNone(isc.input_stamp(root, "unmapped-service"))

    def test_stamp_is_a_sha256_hex(self):
        with tempfile.TemporaryDirectory() as td:
            root = self._tree(Path(td))
            with mock.patch.dict(isc.SERVICE_SOURCES, {"probe": ["."]}):
                stamp = isc.input_stamp(root, "probe")
            self.assertRegex(stamp, r"^[0-9a-f]{64}$")

    def test_env_var_name_matches_compose_interpolation(self):
        self.assertEqual(isc.env_var_name("execution-gateway"),
                         "EXECUTION_GATEWAY_BUILD_STAMP")
        self.assertEqual(isc.env_var_name("loadgen"), "LOADGEN_BUILD_STAMP")


class StampCheckServiceTest(unittest.TestCase):
    def test_a_matching_stamp_beats_a_missing_timestamp(self):
        """The stamp is the primary signal: an image with no readable Created
        epoch (would be MISSING) is FRESH when its stamp matches the tree."""
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td), filename="Dockerfile")
            entry = {"context": ".", "dockerfile": "Dockerfile"}
            with mock.patch.dict(isc.SERVICE_SOURCES, {"probe": ["Dockerfile"]}):
                stamp = isc.input_stamp(repo, "probe", entry, repo)
                with mock.patch.object(isc, "image_created_epoch", lambda image: None), \
                        mock.patch.object(isc, "image_label",
                                          lambda image, key=isc.STAMP_LABEL: stamp):
                    result = isc.check_service("probe", "probe:1", repo,
                                               {"probe": entry}, repo)
            self.assertEqual(result["status"], "FRESH")
            self.assertEqual(result["stamp_image"], stamp)
            self.assertEqual(result["stamp_sources"], stamp)


class VerdictTest(unittest.TestCase):
    def test_fresh_when_image_newer(self):
        status, _ = isc.verdict(EPOCH_B, EPOCH_A, dirty=False)
        self.assertEqual(status, "FRESH")

    def test_stale_when_image_older(self):
        status, _ = isc.verdict(EPOCH_A, EPOCH_B, dirty=False)
        self.assertEqual(status, "STALE")

    def test_missing_image(self):
        status, detail = isc.verdict(None, EPOCH_A, dirty=False)
        self.assertEqual(status, "MISSING")
        self.assertIn("not built", detail)

    def test_no_history_sources(self):
        status, _ = isc.verdict(EPOCH_B, None, dirty=False)
        self.assertEqual(status, "NO-HISTORY")

    def test_dirty_wins_over_stale(self):
        status, _ = isc.verdict(EPOCH_A, EPOCH_B, dirty=True)
        self.assertEqual(status, "DIRTY-WARN")

    def test_matching_stamp_is_fresh_without_any_timestamps(self):
        status, detail = isc.verdict(None, None, dirty=False,
                                     stamp_image="a" * 64, stamp_sources="a" * 64)
        self.assertEqual(status, "FRESH")
        self.assertIn("matches the sources", detail)

    def test_stamp_mismatch_is_stale_even_when_the_image_looks_newer(self):
        status, detail = isc.verdict(EPOCH_B, EPOCH_A, dirty=False,
                                     stamp_image="a" * 64, stamp_sources="b" * 64)
        self.assertEqual(status, "STALE", "content decides, not the clock")
        self.assertIn("make images", detail)

    def test_without_a_stamp_the_timestamp_proxy_still_decides(self):
        status, detail = isc.verdict(EPOCH_B, EPOCH_A, dirty=False)
        self.assertEqual(status, "FRESH")
        self.assertIn("timestamp proxy", detail)

    def test_dirty_beats_a_matching_stamp(self):
        status, _ = isc.verdict(EPOCH_B, EPOCH_A, dirty=True,
                                stamp_image="a" * 64, stamp_sources="a" * 64)
        self.assertEqual(status, "DIRTY-WARN",
                         "the gate runs committed truth: an uncommitted tree is "
                         "reported even when the image matches the working copy")


class GitEpochTest(unittest.TestCase):
    def test_source_epoch_and_dirty(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td))
            self.assertEqual(isc.source_epoch(
                repo, ["src/file.txt"]), EPOCH_A)
            self.assertFalse(isc.worktree_dirty(repo, ["src/file.txt"]))
            (repo / "src/file.txt").write_text("v2\n", encoding="utf-8")
            self.assertTrue(isc.worktree_dirty(repo, ["src/file.txt"]))

    def test_source_epoch_untracked_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td))
            self.assertIsNone(isc.source_epoch(repo, ["src/never.txt"]))

    def test_source_epoch_multiple_paths_uses_latest(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td))
            env = dict(os.environ,
                       GIT_AUTHOR_DATE=f"@{EPOCH_B} +0000",
                       GIT_COMMITTER_DATE=f"@{EPOCH_B} +0000",
                       GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t",
                       GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
            other = repo / "other"
            other.mkdir()
            (other / "b.txt").write_text("b\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True,
                           env=env, capture_output=True)
            subprocess.run(["git", "commit", "-q", "-m", "two"], cwd=repo,
                           check=True, env=env, capture_output=True)
            self.assertEqual(isc.source_epoch(
                repo, ["src/file.txt", "other/b.txt"]), EPOCH_B)


class CheckServiceTest(unittest.TestCase):
    def test_check_service_fresh(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td),
                             filename="code/02_services/04_executor/src/lib.rs")
            with mock.patch.object(isc, "image_created_epoch",
                                   return_value=EPOCH_A + 50):
                result = isc.check_service("nautilus", "img", repo)
            self.assertEqual(result["status"], "FRESH")
            self.assertEqual(result["image"], "img")

    def test_check_service_stale(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td),
                             filename="code/02_services/04_executor/src/lib.rs")
            with mock.patch.object(isc, "image_created_epoch",
                                   return_value=EPOCH_A - 50):
                result = isc.check_service("nautilus", "img", repo)
            self.assertEqual(result["status"], "STALE")

    def test_check_service_fallback_to_compose(self):
        """Unlisted service uses the build context + dockerfile from compose."""
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td), filename="ctx/x.txt")
            compose_services = {"custom": {"context": "ctx",
                                           "dockerfile": "ctx/Dockerfile"}}
            with mock.patch.object(isc, "image_created_epoch",
                                   return_value=EPOCH_A + 50):
                result = isc.check_service("custom", "img", repo,
                                           compose_services)
            self.assertEqual(result["status"], "FRESH")

    def test_sources_overlay_is_consistent(self):
        # Every overlay path must exist in the real repo (catch typos early).
        repo_root = Path(__file__).resolve().parents[4]
        for service, paths in isc.SERVICE_SOURCES.items():
            for path in paths:
                self.assertTrue((repo_root / path).exists(),
                                f"{service}: {path} missing")
            self.assertGreaterEqual(len(paths), 1, service)


class ComposeStampLabelTest(unittest.TestCase):
    """Every build: service in the real compose must declare the content-stamp
    label, so a new service cannot silently inherit the (unsound) timestamp
    proxy. Guarded here rather than in review, because the failure mode is a
    false verdict months later, not a broken build today."""

    @classmethod
    def setUpClass(cls):
        cls.repo_root = Path(__file__).resolve().parents[4]
        cls.compose_path = (cls.repo_root / "code" / "01_platform" / "01_docker"
                            / "docker-compose.yml")

    def test_every_build_service_declares_the_stamp_label(self):
        services = isc.build_services(isc.load_compose(self.compose_path))
        self.assertGreaterEqual(len(services), 8, "expected the 8 build services")
        for name, entry in sorted(services.items()):
            with self.subTest(service=name):
                declared = (entry.get("labels") or {}).get(isc.STAMP_LABEL)
                self.assertEqual(
                    declared, "${%s:-none}" % isc.env_var_name(name),
                    f"{name}: build.labels.{isc.STAMP_LABEL} must interpolate "
                    f"${{{isc.env_var_name(name)}}} so `make images` can stamp it")


class MainTest(unittest.TestCase):
    def test_declared_image_is_the_reference_checked(self):
        """End-to-end through main(): a declared image is the only reference
        inspected — it must never fall back to the derived default."""
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td), filename="Dockerfile")
            compose = Path(td) / "compose.yml"
            compose.write_text(textwrap.dedent("""\
                services:
                  custom:
                    build: {context: ".", dockerfile: "Dockerfile"}
                    image: pipeline-loadgen:1.0.0
                """), encoding="utf-8")
            asked: list[str] = []

            def fake_epoch(image: str):
                asked.append(image)
                return EPOCH_B

            with mock.patch.object(isc, "image_created_epoch", fake_epoch):
                rc = isc.main(["--git-root", str(repo), "--compose", str(compose)])
            self.assertEqual(rc, 0, "a fresh declared image must pass")
            self.assertEqual(asked, ["pipeline-loadgen:1.0.0"])

    def test_unknown_service_fails_usage(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td))
            compose = td + "/compose.yml"
            Path(compose).write_text(
                textwrap.dedent("""\
                services:
                  ing: {build: {context: .}}
                """), encoding="utf-8")
            rc = isc.main(["--git-root", str(repo), "--compose", compose,
                           "--service", "nope"])
            self.assertEqual(rc, 2)


class RequireStampsTest(unittest.TestCase):
    """--require-stamps turns a missing build label into a failure; without the
    flag the timestamp proxy still decides, so pre-CHG-124 images stay
    checkable. It exists because a build can succeed while nothing was stamped:
    on 2026-09-12 the stamps were eval'ed inside the recipe's shell, docker
    compose therefore read the default `${VAR:-none}` for all eight services,
    and the checker still said PASS — by clock."""

    @staticmethod
    def _repo_and_compose(td: str):
        repo = make_repo(Path(td), filename="Dockerfile")
        # The compose file lives inside the repo so the declared Dockerfile
        # resolves against git_root and the fixture yields a real stamp.
        compose = repo / "compose.yml"
        compose.write_text(textwrap.dedent("""\
            services:
              ing: {build: {context: ".", dockerfile: "Dockerfile"}}
            """), encoding="utf-8")
        return repo, compose

    def _run(self, repo: Path, compose: Path, stamp_image, extra: list[str]):
        with mock.patch.object(isc, "image_created_epoch",
                               lambda image: EPOCH_B), \
                mock.patch.object(isc, "image_label",
                                  lambda image, key=isc.STAMP_LABEL: stamp_image):
            return isc.main(["--git-root", str(repo), "--compose", str(compose)]
                            + extra)

    def test_missing_label_fails_under_require_stamps(self):
        with tempfile.TemporaryDirectory() as td:
            repo, compose = self._repo_and_compose(td)
            rc = self._run(repo, compose, None, ["--require-stamps"])
        self.assertEqual(rc, 1, "an unlabelled image must fail --require-stamps")

    def test_missing_label_passes_without_the_flag(self):
        with tempfile.TemporaryDirectory() as td:
            repo, compose = self._repo_and_compose(td)
            rc = self._run(repo, compose, None, [])
        self.assertEqual(rc, 0, "pre-stamp images stay checkable via the proxy")

    def test_matching_label_passes_under_require_stamps(self):
        with tempfile.TemporaryDirectory() as td:
            repo, compose = self._repo_and_compose(td)
            stamp = isc.input_stamp(
                repo, "ing", {"context": ".", "dockerfile": "Dockerfile"},
                compose.parent)
            self.assertIsNotNone(stamp, "the fixture must produce a stamp")
            rc = self._run(repo, compose, stamp, ["--require-stamps"])
        self.assertEqual(rc, 0, "a matching stamp must pass")


class MakeImagesRecipeTest(unittest.TestCase):
    """Guard the transport the 2026-09-12 no-op broke: the stamps must be handed
    to the build command itself. `eval` sets shell-only variables that the
    compose child never inherits — every image stayed unlabelled and the checker
    passed by clock, which is the failure this whole change exists to remove."""

    @classmethod
    def setUpClass(cls):
        cls.repo_root = Path(__file__).resolve().parents[4]
        cls.makefile = (cls.repo_root / "Makefile").read_text(encoding="utf-8")

    def _recipe(self) -> str:
        parts = self.makefile.split("\nimages:\n", 1)
        self.assertEqual(len(parts), 2, "Makefile must define an `images:` target")
        return parts[1].split("\n\n", 1)[0]

    def test_stamps_are_applied_to_the_build_command(self):
        self.assertIn(
            "env $$stamps", self._recipe(),
            "the stamps must ride on the build command's environment "
            "(env VAR=... compose build), not be eval'ed into the recipe shell")

    def test_recipe_does_not_eval_the_stamps(self):
        self.assertNotIn("eval", self._recipe(),
                         "eval sets shell-only variables: docker compose, a "
                         "child process, never sees them")

    def test_recipe_reverifies_with_require_stamps(self):
        self.assertIn("--require-stamps", self._recipe(),
                      "make images must fail loudly if an image ends up "
                      "unlabelled")

    def test_printed_stamps_are_bare_and_applyable_with_env(self):
        """`env $(... --print-stamps-env)` works only for bare VAR=value lines:
        an `export ` prefix would be taken as the program name by env."""
        out = subprocess.run(
            [sys.executable,
             str(self.repo_root / "code" / "01_platform" / "04_scripts"
                 / "image_staleness_check.py"),
             "--git-root", str(self.repo_root), "--print-stamps-env"],
            capture_output=True, text=True, check=True).stdout
        lines = [ln for ln in out.splitlines() if ln.strip()]
        self.assertTrue(lines, "the checker must print one line per build service")
        for line in lines:
            with self.subTest(line=line):
                self.assertFalse(line.startswith("export "),
                                 "`env $(...)` cannot consume an export prefix")
                self.assertRegex(line, r"^[A-Z0-9_]+_BUILD_STAMP=([0-9a-f]{64}|none)$")


class NativeSplitGuardTest(unittest.TestCase):
    """Guard the native Flink split: the compute image is the platform only,
    the jar is a host artifact. These tests fail if someone re-bakes the jar
    into the image (the rebuild-per-code-change anti-pattern)."""

    @classmethod
    def setUpClass(cls):
        cls.repo_root = Path(__file__).resolve().parents[4]
        cls.dockerfile = (cls.repo_root / "code" / "02_services" /
                          "02_compute" / "Dockerfile").read_text(encoding="utf-8")
        cls.compose = (cls.repo_root / "code" / "01_platform" / "01_docker" /
                       "docker-compose.yml").read_text(encoding="utf-8")

    def test_compute_dockerfile_does_not_bake_the_jar(self):
        """The Dockerfile must NOT copy source in or compile the jar —
        that couples every code change to an image rebuild."""
        # The anti-pattern is a *build step* that copies source + compiles:
        #   COPY 02_services /workspace/02_services
        #   RUN mvn ... package
        # Comments mentioning `mvn package` are fine — the build step is not.
        # The launcher script COPY is allowed (platform); the source TREE
        # COPY (the whole 02_services dir, or a src/ dir) is the anti-pattern.
        self.assertNotRegex(
            self.dockerfile,
            r"COPY\s+\S*02_services\S*\s+/workspace",
            "Dockerfile must not COPY the 02_services source tree (jar is a host artifact)")
        self.assertNotIn("COPY 02_services", self.dockerfile,
                         "Dockerfile must not COPY the bare 02_services dir")
        self.assertNotRegex(
            self.dockerfile,
            r"RUN\s+\S*mvn\S*",
            "Dockerfile must not compile the jar (mvn build belongs on the host)")
        self.assertNotRegex(
            self.dockerfile,
            r"COPY\s+.*compute\.jar",
            "Dockerfile must not COPY the jar (it is volume-mounted at runtime)")

    def test_compute_dockerfile_keeps_platform_and_launcher(self):
        """The image still carries the platform (flink base) + launcher script."""
        self.assertIn("FROM flink:", self.dockerfile)
        self.assertIn("submit-jobs.sh", self.dockerfile)

    def test_compute_service_mounts_the_host_jar(self):
        """The compose compute service must volume-mount the host-built jar
        so a code change needs only `mvn package` (no image rebuild)."""
        self.assertIn(
            "target/compute.jar:/opt/flink-jobs/compute.jar",
            self.compose,
            "compute service must mount the host jar (native split)")

    def test_compute_image_sources_exclude_the_jar(self):
        """CHG-101 staleness sources for the compute image must not include
        the jar/02_services — otherwise every code change flags the image
        STALE and forces a rebuild."""
        for path in isc.SERVICE_SOURCES.get("compute", []):
            # Only the Dockerfile + launcher may be sources; the job SOURCE
            # TREE (02_services/02_compute/src, target/) must not be — but the
            # Dockerfile/launcher paths themselves live under 02_services, so
            # assert on the *code* paths specifically.
            self.assertNotIn("02_compute/src", path,
                             f"compute image source '{path}' must not include "
                             f"the job source tree (jar is a host artifact)")
            self.assertNotIn("/target/", path,
                             f"compute image source '{path}' must not include "
                             f"the build output (jar is a host artifact)")


if __name__ == "__main__":
    unittest.main()
