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
