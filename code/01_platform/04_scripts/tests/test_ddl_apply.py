#!/usr/bin/env python3
"""Unit tests for ddl_apply.py — the Java apply-engine orchestration.

Covers the 2026-08-15 additions: tool classpath building (build_tool_classpath),
evidence enrichment (enrich_evidence), and the apply invocation (run_apply_tool)
with the subprocess mocked. Stdlib unittest, no third-party deps.

Run: python3 -m unittest discover -s code/01_platform/04_scripts/tests -v
"""
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import ddl_apply  # noqa: E402


def make_fake_m2(root):
    """Create a fake ~/.m2 tree with the 5 classpath jars present."""
    jars = [
        ("0.9.1-incubating", "org/apache/fluss", "fluss-client"),
        (ddl_apply.JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-databind"),
        (ddl_apply.JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-core"),
        (ddl_apply.JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-annotations"),
        (ddl_apply.SLF4J_VERSION, "org/slf4j", "slf4j-api"),
    ]
    for version, group, name in jars:
        path = os.path.join(root, *group.split("/"), name, version, f"{name}-{version}.jar")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write("fake jar")
    return jars


class BuildClasspathTest(unittest.TestCase):
    def test_full_classpath_lists_common_classes_and_all_jars(self):
        with tempfile.TemporaryDirectory() as tmp:
            jars = make_fake_m2(tmp)
            # P6-830: use the fixture's return value — it is the self-check that the
            # five jars this classpath test expects were really created.
            self.assertEqual(5, len(jars))
            with mock.patch.object(ddl_apply, "COMMON_CLASSES",
                                   os.path.join(tmp, "common-classes")):
                os.makedirs(os.path.join(tmp, "common-classes",
                                         "com/trading/common/schema/ddl"), exist_ok=True)
                open(os.path.join(tmp, "common-classes",
                                  "com/trading/common/schema/ddl/DdlApplyTool.class"),
                     "w").close()
                cp = ddl_apply.build_tool_classpath("0.9.1-incubating", m2_repo=tmp)
        self.assertIsNotNone(cp)
        parts = cp.split(os.pathsep)
        self.assertEqual(len(parts), 6)
        self.assertTrue(any(p.endswith("fluss-client-0.9.1-incubating.jar") for p in parts))
        self.assertTrue(any(p.endswith("jackson-databind-" + ddl_apply.JACKSON_VERSION + ".jar")
                            for p in parts))

    def test_missing_jar_reports_and_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            make_fake_m2(tmp)
            os.unlink(os.path.join(tmp, "org/apache/fluss/fluss-client",
                                   "0.9.1-incubating/fluss-client-0.9.1-incubating.jar"))
            with mock.patch.object(ddl_apply, "COMMON_CLASSES",
                                   os.path.join(tmp, "common-classes")):
                os.makedirs(os.path.join(tmp, "common-classes",
                                         "com/trading/common/schema/ddl"), exist_ok=True)
                cp = ddl_apply.build_tool_classpath("0.9.1-incubating", m2_repo=tmp)
        self.assertIsNone(cp)

    def test_uncompiled_tool_class_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            make_fake_m2(tmp)
            with mock.patch.object(ddl_apply, "COMMON_CLASSES",
                                   os.path.join(tmp, "missing-classes")):
                cp = ddl_apply.build_tool_classpath("0.9.1-incubating", m2_repo=tmp)
        self.assertIsNone(cp)


class EnrichEvidenceTest(unittest.TestCase):
    def test_attaches_matrix_evidence_and_sha256(self):
        with tempfile.TemporaryDirectory() as tmp:
            evidence = os.path.join(tmp, "apply.json")
            with open(evidence, "w") as fh:
                json.dump({"status": "PASS", "applied_manifest_id": "abc"}, fh)
            matrix = os.path.join(tmp, "matrix.md")
            with open(matrix, "w") as fh:
                fh.write("capability evidence\n")
            ddl_apply.enrich_evidence(evidence, matrix)
            with open(evidence) as fh:
                record = json.load(fh)
            self.assertEqual(record["matrix_evidence"], matrix)
            self.assertEqual(record["matrix_evidence_sha256"],
                             ddl_apply.sha256_of(matrix))


class EvidenceRootOverrideTest(unittest.TestCase):
    """DDL_APPLY_EVIDENCE_DIR redirects where the apply records land (the
    ddl-apply container entrypoint repairs ownership on that exact path)."""

    def test_env_override_wires_evidence_root_then_restores_default(self):
        import importlib
        try:
            with mock.patch.dict(os.environ,
                                 {"DDL_APPLY_EVIDENCE_DIR": "/custom/evidence"}):
                importlib.reload(ddl_apply)
                self.assertEqual(ddl_apply.EVIDENCE_ROOT, "/custom/evidence")
        finally:
            # P6-618: make "unset" explicit. DDL_APPLY_EVIDENCE_DIR is a documented
            # override, so relying on the ambient environment would fail this
            # assertion wherever it is legitimately set.
            saved = os.environ.pop("DDL_APPLY_EVIDENCE_DIR", None)
            try:
                importlib.reload(ddl_apply)
            finally:
                if saved is not None:
                    os.environ["DDL_APPLY_EVIDENCE_DIR"] = saved
        self.assertEqual(
            ddl_apply.EVIDENCE_ROOT,
            os.path.join(ddl_apply.REPO_ROOT, "logs", "ddl-apply"),
            "unset DDL_APPLY_EVIDENCE_DIR keeps the repo-root default")


class EchoOwnershipContractTest(unittest.TestCase):
    """The host-side orchestrator echoes the same non-root ownership contract
    the container wrapper prints, so operators see one expectation either way."""

    def test_echo_mentions_gate_contract_and_evidence_root(self):
        import contextlib
        import io
        buf = io.StringIO()
        with mock.patch.object(ddl_apply.os, "getuid", return_value=1000), \
             mock.patch.dict(os.environ, {"DDL_APPLY_UID": "10001",
                                          "DDL_APPLY_GID": "10001"}), \
             contextlib.redirect_stdout(buf):
            ddl_apply.echo_ownership_contract()
        line = buf.getvalue().strip()
        self.assertIn("ddl-apply: evidence root", line)
        self.assertIn(ddl_apply.EVIDENCE_ROOT, line)
        self.assertIn("host run", line)
        self.assertIn("uid 1000", line)
        self.assertIn("gate enforces uid 10001 gid 10001", line)
        self.assertIn("group-writable 664", line)
        self.assertIn("out of scope", line)

    def test_echo_honors_engine_uid_gid_overrides(self):
        import contextlib
        import io
        buf = io.StringIO()
        with mock.patch.dict(os.environ, {"DDL_APPLY_UID": "4242",
                                          "DDL_APPLY_GID": "4243"}), \
             contextlib.redirect_stdout(buf):
            ddl_apply.echo_ownership_contract()
        self.assertIn("gate enforces uid 4242 gid 4243", buf.getvalue())

    def test_echo_suppressed_in_container(self):
        """DDL_APPLY_IN_CONTAINER (set by the image runner) suppresses the host
        echo — the entrypoint wrapper already emitted the APPLIED contract."""
        import contextlib
        import io
        buf = io.StringIO()
        with mock.patch.dict(os.environ, {"DDL_APPLY_IN_CONTAINER": "1"}), \
             contextlib.redirect_stdout(buf):
            ddl_apply.echo_ownership_contract()
        self.assertEqual(buf.getvalue(), "",
                         "in-container runs must not double-emit the contract")


class RunApplyToolTest(unittest.TestCase):
    def test_success_writes_enriched_evidence_and_returns_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            matrix = os.path.join(tmp, "matrix.md")
            with open(matrix, "w") as fh:
                fh.write("evidence")
            versions = {"FLINK_VERSION": "2.2.1", "FLUSS_VERSION": "0.9.1-incubating"}
            with mock.patch.object(ddl_apply, "build_tool_classpath",
                                   return_value="fake-cp"), \
                 mock.patch.object(ddl_apply, "EVIDENCE_ROOT", os.path.join(tmp, "logs")), \
                 mock.patch.object(ddl_apply.subprocess, "run") as run:
                # The real tool writes the evidence record itself; simulate that
                # by writing it at the --evidence-out path the orchestration passes.
                def fake_run(cmd, **kwargs):
                    out = cmd[cmd.index("--evidence-out") + 1]
                    with open(out, "w") as fh:
                        json.dump({"status": "PASS", "applied_manifest_id": "abc123"}, fh)
                    return mock.Mock(returncode=0, stdout="ddl-apply: APPLIED 21 "
                                       "tables applied_manifest_id=abc123\n", stderr="")
                run.side_effect = fake_run
                rc = ddl_apply.run_apply_tool(versions, matrix)
            self.assertEqual(rc, 0)
            record_path = os.path.join(tmp, "logs",
                                       os.listdir(os.path.join(tmp, "logs"))[0], "apply.json")
            with open(record_path) as fh:
                record = json.load(fh)
            self.assertEqual(record["status"], "PASS")
            self.assertEqual(record["matrix_evidence"], matrix)
            # The engine received the pinned versions and the DDL dir.
            args = run.call_args[0][0]
            self.assertIn("--flink-version", args)
            self.assertEqual(args[args.index("--flink-version") + 1], "2.2.1")
            self.assertIn("--fluss-version", args)
            self.assertEqual(args[args.index("--fluss-version") + 1], "0.9.1-incubating")
            self.assertIn("--add-opens=java.base/java.nio=ALL-UNNAMED", args)

    def test_failure_propagates_exit_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            matrix = os.path.join(tmp, "matrix.md")
            with open(matrix, "w") as fh:
                fh.write("evidence")
            with mock.patch.object(ddl_apply, "build_tool_classpath",
                                   return_value="fake-cp"), \
                 mock.patch.object(ddl_apply, "EVIDENCE_ROOT", os.path.join(tmp, "logs")), \
                 mock.patch.object(ddl_apply.subprocess, "run") as run:
                run.return_value = mock.Mock(returncode=3,
                                             stdout="ddl-apply: REFUSED — catalog not empty\n",
                                             stderr="")
                rc = ddl_apply.run_apply_tool({"FLUSS_VERSION": "0.9.1-incubating"}, matrix)
            self.assertEqual(rc, 3)

    def test_acknowledged_partial_returns_exit_6_with_sentinel(self):
        """Exit 6 = acknowledged PASS_WITH_LIMITATION: propagated distinctly
        from full PASS (0) and failures (1), with a machine-readable sentinel."""
        with tempfile.TemporaryDirectory() as tmp:
            matrix = os.path.join(tmp, "matrix.md")
            with open(matrix, "w") as fh:
                fh.write("evidence")
            with mock.patch.object(ddl_apply, "build_tool_classpath",
                                   return_value="fake-cp"), \
                 mock.patch.object(ddl_apply, "EVIDENCE_ROOT", os.path.join(tmp, "logs")), \
                 mock.patch.object(ddl_apply.subprocess, "run") as run:
                def fake_run(cmd, **kwargs):
                    out = cmd[cmd.index("--evidence-out") + 1]
                    with open(out, "w") as fh:
                        json.dump({"status": "PASS_WITH_LIMITATION",
                                   "applied_manifest_id": "abc123"}, fh)
                    return mock.Mock(returncode=6, stdout="ddl-apply: RESULT="
                                       "PASS_WITH_LIMITATION EXIT=6 TABLES=21\n", stderr="")
                run.side_effect = fake_run
                rc = ddl_apply.run_apply_tool({"FLUSS_VERSION": "0.9.1-incubating"}, matrix)
            self.assertEqual(rc, 6, "acknowledged partial apply must return the dedicated 6")


class ManifestGuardTest(unittest.TestCase):
    """P6-350: a manifest that exists but cannot be parsed is not "absent"."""

    def test_absent_manifest_is_absence_not_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ddl_apply, "MANIFEST_PATH",
                                   os.path.join(tmp, "schema_manifest.json")):
                self.assertIsNone(ddl_apply.load_existing_manifest())

    def test_corrupt_manifest_raises_instead_of_reading_as_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad = os.path.join(tmp, "schema_manifest.json")
            with open(bad, "w") as fh:
                fh.write('{"tables": [')  # truncated write
            with mock.patch.object(ddl_apply, "MANIFEST_PATH", bad):
                with self.assertRaises(ddl_apply.ManifestUnreadable):
                    ddl_apply.load_existing_manifest()

    def test_valid_manifest_still_loads(self):
        with tempfile.TemporaryDirectory() as tmp:
            good = os.path.join(tmp, "schema_manifest.json")
            with open(good, "w") as fh:
                json.dump({"tables": [{"table_name": "t"}]}, fh)
            with mock.patch.object(ddl_apply, "MANIFEST_PATH", good):
                self.assertEqual(ddl_apply.load_existing_manifest()["tables"][0]["table_name"], "t")

    def _run_main(self, argv, manifest_path, force_regen_ok):
        """main() with the version gate and DDL parsing neutralized."""
        with mock.patch.object(sys, "argv", ["ddl_apply.py"] + argv), \
             mock.patch.object(ddl_apply, "MANIFEST_PATH", manifest_path), \
             mock.patch.object(ddl_apply, "echo_ownership_contract"), \
             mock.patch.object(ddl_apply, "load_versions", return_value={"FLUSS_VERSION": "x"}), \
             mock.patch.object(ddl_apply, "enforce_version_gate", return_value=[]), \
             mock.patch.object(ddl_apply, "compute_manifest_entries", return_value=[]):
            return ddl_apply.main()

    def test_main_refuses_a_corrupt_manifest_with_exit_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad = os.path.join(tmp, "schema_manifest.json")
            with open(bad, "w") as fh:
                fh.write("not json at all")
            self.assertEqual(self._run_main([], bad, False), 2)
            # refused means refused: the unparseable file is still there
            with open(bad) as fh:
                self.assertEqual(fh.read(), "not json at all")

    def test_main_force_regenerates_over_a_corrupt_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad = os.path.join(tmp, "schema_manifest.json")
            with open(bad, "w") as fh:
                fh.write("not json at all")
            self.assertEqual(self._run_main(["--force"], bad, True), 0)
            with open(bad) as fh:
                self.assertEqual(json.load(fh), {"schema_manifest_version": "1", "tables": []})


class SweepGuardTest(unittest.TestCase):
    """P6-351: sweep mode must exit 2, not traceback, when the pin is unreadable."""

    def test_non_utf8_pin_file_returns_two(self):
        with tempfile.TemporaryDirectory() as tmp:
            pin = os.path.join(tmp, "versions.pin")
            with open(pin, "wb") as fh:
                fh.write(b"FLUSS_VERSION=0.9.1\xff\xfe\n")
            with mock.patch.object(ddl_apply, "VERSIONS_PIN", pin):
                self.assertEqual(ddl_apply.run_sweep_tool(), 2)

    def test_unreadable_pin_file_returns_two(self):
        with tempfile.TemporaryDirectory() as tmp:
            pin = os.path.join(tmp, "versions.pin")
            with open(pin, "w") as fh:
                fh.write("FLUSS_VERSION=0.9.1-incubating\n")
            os.chmod(pin, 0)
            try:
                with mock.patch.object(ddl_apply, "VERSIONS_PIN", pin):
                    self.assertEqual(ddl_apply.run_sweep_tool(), 2)
            finally:
                os.chmod(pin, 0o644)

    def test_sweep_dispatch_is_inside_the_guarded_block(self):
        """main() must not let a sweep failure escape as a traceback."""
        with tempfile.TemporaryDirectory() as tmp:
            pin = os.path.join(tmp, "versions.pin")
            with open(pin, "w") as fh:
                fh.write("FLUSS_VERSION=0.9.1-incubating\n")
            os.chmod(pin, 0)
            try:
                with mock.patch.object(sys, "argv", ["ddl_apply.py"]), \
                     mock.patch.dict(os.environ, {"DDL_APPLY_SWEEP": "1"}), \
                     mock.patch.object(ddl_apply, "VERSIONS_PIN", pin), \
                     mock.patch.object(ddl_apply, "echo_ownership_contract"):
                    self.assertEqual(ddl_apply.main(), 2)
            finally:
                os.chmod(pin, 0o644)


class EvidenceDirUniquenessTest(unittest.TestCase):
    """P6-723: a same-second retry must not overwrite the previous apply.json."""

    def test_second_apply_in_the_same_second_gets_its_own_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ddl_apply, "EVIDENCE_ROOT", tmp):
                stamp = "20260914T120000Z"
                first = ddl_apply._unique_evidence_dir(stamp)
                self.assertNotEqual(first, os.path.join(tmp, f"ddl-apply-{stamp}"))
                self.assertIn(f"ddl-apply-{stamp}-", first)
                os.makedirs(first)
                second = ddl_apply._unique_evidence_dir(stamp)
                self.assertNotEqual(first, second)
                self.assertTrue(second.startswith(first + "-"))


if __name__ == "__main__":
    unittest.main()
