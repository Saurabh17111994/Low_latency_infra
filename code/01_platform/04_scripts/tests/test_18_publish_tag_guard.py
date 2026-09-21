"""A tag-triggered publish may only name the commit that is actually published.

Hermetic: throwaway git repositories in a temporary directory, with a bare repo standing in for
`origin`, so the tag-fetch fallback is exercised offline. No Docker, no network, no GHCR.

Why the guard exists: `publish-images.yml` always checks out the default branch and commits the
digest fragment back to it, because a digest that lives only in a run log is not durable. A
`push: tags` trigger therefore publishes the default branch tip whatever the tag points at — so a
tag left on an older commit would label digests with a version they do not describe.

Run: python3 -m unittest discover -s code/01_platform/04_scripts/tests    (TestCase-style on
purpose: the repo's pytest-only files are invisible to unittest discover.)
"""

import os
import shutil
import subprocess
import tempfile
import unittest

import yaml

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPTS)))
GUARD = os.path.join(SCRIPTS, "publish-tag-guard.sh")
WORKFLOW = os.path.join(REPO, ".github", "workflows", "publish-images.yml")

# Keep the throwaway repos independent of the operator's git config: an identity for the fixture
# commits, and no signing (a signing key configured on this machine must not decide whether CI's
# guard works).
GIT_FLAGS = [
    "-c", "user.name=test",
    "-c", "user.email=test@example.invalid",
    "-c", "commit.gpgsign=false",
    "-c", "tag.gpgSign=false",
]
GIT_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
}


def _git(cwd, *args):
    proc = subprocess.run(
        ["git", *GIT_FLAGS, *args], cwd=cwd, env=GIT_ENV, capture_output=True, text=True
    )
    if proc.returncode != 0:
        raise AssertionError(f"git {' '.join(args)} failed in {cwd}:\n{proc.stderr}")
    return proc.stdout.strip()


def _run_guard(cwd, *args):
    proc = subprocess.run(
        ["bash", GUARD, *args], cwd=cwd, env=GIT_ENV, capture_output=True, text=True
    )
    return proc.returncode, proc.stdout, proc.stderr


class TagGuardFixture(unittest.TestCase):
    """A work repo with one commit, an `origin` bare repo, and optional tag / later commit."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="tag-guard-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.origin = os.path.join(self.root, "origin.git")
        self.work = os.path.join(self.root, "work")

    def _commit(self, text):
        with open(os.path.join(self.work, "f.txt"), "a", encoding="utf-8") as handle:
            handle.write(text + "\n")
        _git(self.work, "add", "f.txt")
        _git(self.work, "commit", "-m", text)
        return _git(self.work, "rev-parse", "HEAD")

    def build(self, tag=None, annotate=False, move_on=False, tag_on_origin_only=False):
        """Create the fixture; return (tagged_sha or None, tip_sha)."""
        os.makedirs(self.work)
        _git(self.work, "init", "--quiet")
        first = self._commit("one")

        _git(self.root, "init", "--bare", "--quiet", self.origin)
        _git(self.work, "remote", "add", "origin", self.origin)
        _git(self.work, "push", "--quiet", "origin", "HEAD")

        tagged = None
        if tag:
            _git(self.work, "tag", *(["-a", "-m", "release"] if annotate else []), tag)
            _git(self.work, "push", "--quiet", "origin", tag)
            tagged = first
            if tag_on_origin_only:
                # Stand in for actions/checkout: the branch is here, the tag is not.
                _git(self.work, "tag", "-d", tag)
            if move_on:
                tagged = first
                self._commit("two")
                _git(self.work, "push", "--quiet", "origin", "HEAD")

        return tagged, _git(self.work, "rev-parse", "HEAD")


class TestPublishTagGuard(TagGuardFixture):
    def test_a_tag_on_the_tip_passes(self):
        tagged, tip = self.build(tag="v1")
        rc, out, err = _run_guard(self.work, "v1")
        self.assertEqual(rc, 0, err)
        self.assertIn(tip, out)
        self.assertEqual(tagged, tip)

    def test_an_annotated_tag_is_peeled_to_its_commit(self):
        # A tag object has its own sha: comparing the tag's sha to HEAD would fail here even though
        # the tag names exactly the commit being published.
        tagged, tip = self.build(tag="v1", annotate=True)
        rc, out, err = _run_guard(self.work, "v1")
        self.assertEqual(rc, 0, err)
        self.assertIn(tip, out)

    def test_a_tag_left_on_an_older_commit_is_refused(self):
        tagged, tip = self.build(tag="v1", move_on=True)
        self.assertNotEqual(tagged, tip)
        rc, out, err = _run_guard(self.work, "v1")
        self.assertEqual(rc, 1)
        self.assertIn(tagged, err)
        self.assertIn(tip, err)
        self.assertNotIn("Traceback", err)
        self.assertEqual(out, "")

    def test_a_tag_missing_from_a_shallow_clone_is_fetched(self):
        # The tag exists on origin but not in the checkout — the shape actions/checkout produces.
        self.build(tag="v1", tag_on_origin_only=True)
        self.assertNotIn("v1", _git(self.work, "tag"))
        rc, out, err = _run_guard(self.work, "v1")
        self.assertEqual(rc, 0, err)
        self.assertIn("ok:", out)

    def test_an_unknown_tag_is_refused_with_a_readable_message(self):
        self.build(tag="v1")
        rc, out, err = _run_guard(self.work, "v-does-not-exist")
        self.assertEqual(rc, 1)
        self.assertIn("v-does-not-exist", err)
        self.assertNotIn("Traceback", err)

    def test_no_argument_is_a_usage_error(self):
        self.build(tag="v1")
        rc, out, err = _run_guard(self.work)
        self.assertEqual(rc, 2)
        self.assertIn("usage:", err)
        self.assertNotIn("Traceback", err)


class TestPublishWorkflowWiring(unittest.TestCase):
    """The workflow half: a guard is only useful if the trigger and the checkout are as designed."""

    def setUp(self):
        with open(WORKFLOW, encoding="utf-8") as handle:
            self.workflow = yaml.safe_load(handle)
        # YAML 1.1 parses a bare `on:` key as the boolean True.
        self.triggers = self.workflow.get("on", self.workflow.get(True))
        self.steps = self.workflow["jobs"]["publish"]["steps"]

    def test_both_the_manual_and_the_tag_trigger_are_declared(self):
        self.assertIn("workflow_dispatch", self.triggers)
        self.assertEqual(self.triggers["push"]["tags"], ["v*"])

    def test_the_checkout_still_pins_the_default_branch(self):
        # This is what makes the guard necessary and the tag a trigger rather than a revision
        # selector: whatever is published is always the default branch tip.
        checkout = self.steps[0]
        self.assertIn("checkout", checkout["uses"])
        self.assertIn("github.event.repository.default_branch", checkout["with"]["ref"])

    def test_the_guard_is_conditional_on_a_tag_push(self):
        guards = [s for s in self.steps if "publish-tag-guard.sh" in str(s.get("run", ""))]
        self.assertEqual(len(guards), 1, "expected exactly one guard step")
        self.assertIn("push", str(guards[0].get("if", "")))

    def test_the_guard_runs_after_the_checkout(self):
        index = {id(s): i for i, s in enumerate(self.steps)}
        guard = next(s for s in self.steps if "publish-tag-guard.sh" in str(s.get("run", "")))
        checkout = self.steps[0]
        self.assertGreater(index[id(guard)], index[id(checkout)])

    def test_it_still_commits_the_digest_fragment_back(self):
        # The guard exists because of this: the fragment must land on the default branch.
        self.assertEqual(self.workflow["permissions"]["contents"], "write")
        self.assertEqual(self.workflow["permissions"]["packages"], "write")


if __name__ == "__main__":
    unittest.main()
