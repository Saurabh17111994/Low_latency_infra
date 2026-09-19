#!/usr/bin/env python3
"""Unit tests for evidence_ownership_check.py — the non-root ownership gate.

Ownership can't be set without root, so the check's stat_fn is injected with a
fake returning crafted (uid, gid, mode): one stat for the evidence root dir and
one per record basename. The corpus files still exist so the walk enumerates
them. Stdlib unittest, no third-party deps.

Run: python3 -m unittest discover -s code/01_platform/04_scripts/tests -v
"""
import io
import os
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import evidence_ownership_check  # noqa: E402

UID = 10001
GID = 10001
HOST_UID = 1000
HOST_GID = 1000
DIR_2775 = 0o2775


def fake_stat(corpus_dir, by_basename, dir_stat):
    """stat_fn: dir_stat (uid, gid, mode) for the corpus root, else per file."""
    def fn(path):
        if path == corpus_dir:
            return types.SimpleNamespace(
                st_uid=dir_stat[0], st_gid=dir_stat[1], st_mode=dir_stat[2])
        uid, gid, mode = by_basename[os.path.basename(path)]
        return types.SimpleNamespace(st_uid=uid, st_gid=gid, st_mode=mode)
    return fn


class CheckEvidenceDirTest(unittest.TestCase):
    def _corpus(self, tmp, records):
        """Create one apply.json per record name under its own subdir."""
        for name in records:
            sub = os.path.join(tmp, "r_" + name)
            os.makedirs(sub, exist_ok=True)
            with open(os.path.join(sub, "apply.json"), "w") as fh:
                fh.write("{}")
        return tmp

    def test_container_corpus_with_contract_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a", "b"])
            problems, (checked, cw, host) = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (UID, GID, 0o664)},
                          (UID, GID, DIR_2775)))
            self.assertEqual(problems, [])
            self.assertEqual((checked, cw, host), (2, 2, 0))

    def test_root_owned_dir_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, _ = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (UID, GID, 0o664)}, (0, 0, DIR_2775)))
            self.assertEqual(len(problems), 1)
            self.assertIn("root-owned", problems[0])

    def test_dir_missing_setgid_or_group_write_fails(self):
        for mode, label in ((0o2754, "no group-write"), (0o0775, "no setgid")):
            with tempfile.TemporaryDirectory() as tmp:
                self._corpus(tmp, ["a"])
                problems, _ = evidence_ownership_check.check_evidence_dir(
                    tmp, UID, GID,
                    fake_stat(tmp, {"apply.json": (UID, GID, 0o664)},
                              (UID, GID, mode)))
                self.assertEqual(len(problems), 1, label)
                self.assertIn("missing setgid+group-write", problems[0], label)

    def test_container_written_not_group_writable_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, _ = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (UID, GID, 0o644)}, (UID, GID, DIR_2775)))
            self.assertEqual(len(problems), 1)
            self.assertIn("NOT group-writable", problems[0])

    def test_container_written_wrong_group_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, _ = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (UID, HOST_GID, 0o664)},
                          (UID, GID, DIR_2775)))
            self.assertEqual(len(problems), 1)
            self.assertIn(f"group {HOST_GID} != engine GID {GID}", problems[0])

    def test_root_owned_record_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, _ = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (0, 0, 0o644)}, (UID, GID, DIR_2775)))
            self.assertEqual(len(problems), 1)
            self.assertIn("root-owned", problems[0])

    def test_host_owned_corpus_out_of_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, (checked, cw, host) = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (HOST_UID, HOST_GID, 0o644)},
                          (HOST_UID, HOST_GID, 0o2775)))
            self.assertEqual(problems, [])
            self.assertEqual((checked, cw, host), (1, 0, 1))

    def test_missing_corpus_is_vacuous_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "nope")
            problems, counts = evidence_ownership_check.check_evidence_dir(missing, UID)
            self.assertEqual(problems, [])
            self.assertEqual(counts, (0, 0, 0))

    def test_non_record_files_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "notes.txt"), "w") as fh:
                fh.write("not a record")
            os.makedirs(os.path.join(tmp, "r1"))
            with open(os.path.join(tmp, "r1", "other.json"), "w") as fh:
                fh.write("{}")
            problems, counts = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"notes.txt": (0, 0, 0o644),
                                "other.json": (UID, GID, 0o644)},
                          (UID, GID, DIR_2775)))
            self.assertEqual(problems, [])
            self.assertEqual(counts, (0, 0, 0))


    def test_root_owned_dir_fails_even_when_every_record_is_host_owned(self):
        """P6-726: the dir check is owner-scoped, not record-scoped."""
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, (checked, cw, host) = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (HOST_UID, HOST_GID, 0o644)},
                          (0, 0, DIR_2775)))
            self.assertEqual((checked, cw, host), (1, 0, 1))
            self.assertEqual(len(problems), 1, problems)
            self.assertIn("root-owned", problems[0])

    @unittest.skipUnless(os.geteuid() != 0, "root can traverse mode-000 dirs")
    def test_unreadable_subdirectory_fails_instead_of_being_skipped(self):
        """P6-367: os.walk swallows read errors unless onerror is handled."""
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            blocked = os.path.join(tmp, "blocked")
            os.makedirs(blocked)
            os.chmod(blocked, 0o000)
            try:
                problems, _ = evidence_ownership_check.check_evidence_dir(
                    tmp, UID, GID,
                    fake_stat(tmp, {"apply.json": (UID, GID, 0o664)},
                              (UID, GID, DIR_2775)))
            finally:
                os.chmod(blocked, 0o755)
            self.assertTrue(
                any("cannot read" in p and "blocked" in p for p in problems), problems)


class UidOverrideTests(unittest.TestCase):
    """P6-725: a bad DDL_APPLY_UID/GID is a named refusal, not a traceback."""

    def test_absent_or_blank_override_keeps_the_default(self):
        self.assertEqual(evidence_ownership_check._uid_env("NOPE_NOT_SET", 10001), 10001)
        with mock.patch.dict(os.environ, {"DDL_APPLY_UID": "   "}):
            self.assertEqual(evidence_ownership_check._uid_env("DDL_APPLY_UID", 10001), 10001)

    def test_whitespace_padded_number_is_accepted(self):
        with mock.patch.dict(os.environ, {"DDL_APPLY_UID": " 4242 "}):
            self.assertEqual(evidence_ownership_check._uid_env("DDL_APPLY_UID", 10001), 4242)

    def test_non_numeric_override_exits_with_a_named_message(self):
        with mock.patch.dict(os.environ, {"DDL_APPLY_UID": "abc"}):
            with mock.patch("sys.stderr", new_callable=io.StringIO) as err:
                with self.assertRaises(SystemExit) as caught:
                    evidence_ownership_check._uid_env("DDL_APPLY_UID", 10001)
        self.assertEqual(caught.exception.code, 2)
        self.assertIn("DDL_APPLY_UID", err.getvalue())
        self.assertIn("not an integer", err.getvalue())

    def test_bad_override_on_the_command_line_is_not_a_traceback(self):
        script = evidence_ownership_check.__file__
        proc = subprocess.run(
            [sys.executable, script],
            env=dict(os.environ, DDL_APPLY_UID="abc"),
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertIn("FATAL", proc.stdout + proc.stderr)
        self.assertNotIn("Traceback", proc.stderr)


class ModuleDefaultsTest(unittest.TestCase):
    def test_defaults_point_at_repo_evidence_root(self):
        """P6-834: the module reads DDL_APPLY_* at import time, so this must reload
        it under blank overrides — otherwise a CI or host that legitimately exports
        them fails this assertion for a reason unrelated to the defaults."""
        import importlib
        with mock.patch.dict(os.environ, {"DDL_APPLY_EVIDENCE_DIR": "",
                                          "DDL_APPLY_UID": "",
                                          "DDL_APPLY_GID": ""}):
            try:
                module = importlib.reload(evidence_ownership_check)
                self.assertTrue(module.EVIDENCE_DIR.endswith(
                    os.path.join("logs", "ddl-apply")))
                self.assertEqual(module.CONTAINER_UID, 10001)
                self.assertEqual(module.CONTAINER_GID, 10001)
            finally:
                importlib.reload(evidence_ownership_check)


if __name__ == "__main__":
    unittest.main()
