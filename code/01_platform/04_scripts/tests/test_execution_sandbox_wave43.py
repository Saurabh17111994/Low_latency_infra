"""Wave 43 — execution sandbox tooling (P6-221, P6-368, P6-575, P6-576, P6-577,
P6-728, P6-729, P6-795, P6-796, P6-797).

Hermetic: no docker, no network, no cluster. The three scripts are imported and
their pure parts driven directly; the two docker-facing fixes are pinned by
source invariants plus the pure helpers they were extracted into.
"""
from __future__ import annotations

import inspect
import json
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import execution_network_check as enc  # noqa: E402
import t8_sandbox_contract_check as t8  # noqa: E402
import t9_order_sandbox as t9  # noqa: E402

try:
    import yaml  # noqa: F401
    HAVE_YAML = True
except ImportError:  # pragma: no cover - environment dependent
    HAVE_YAML = False


class P6_221FailureAccounting(unittest.TestCase):
    def tearDown(self) -> None:
        t9.FAILURES.clear()

    def test_failure_recorded_after_the_snapshot_is_returned(self) -> None:
        # errs is a snapshot taken when offline_contract() starts; a check that
        # fails afterwards must still reach the caller.
        t9.FAILURES.clear()
        snapshot = list(t9.FAILURES)
        t9.FAILURES.append("late-check")
        self.assertEqual(t9._all_failures(snapshot), ["late-check"])

    def test_no_exit_path_returns_the_stale_snapshot(self) -> None:
        src = inspect.getsource(t9.offline_contract)
        self.assertNotIn("return errs", src)
        self.assertGreaterEqual(src.count("return _all_failures(errs)"), 3)


class P6_577EnvelopeTypes(unittest.TestCase):
    def _envelope(self) -> str:
        text, _, _ = t9.encode_envelope(
            "wave43-secret", t9.PROTOCOL_VERSION, "APPROVED_ORDER",
            "req-1", "acct-1", "part-1", {"k": "v"},
            7, "fence-1", t9.JW_DEADLINE)
        return text

    def test_well_typed_envelope_still_verifies(self) -> None:
        ok, reason = t9.verify_envelope(self._envelope(), "wave43-secret",
                                        t9.PROTOCOL_VERSION, t9.JW_DEADLINE - 1)
        self.assertTrue(ok, reason)

    def _mutated(self, key, value) -> str:
        env = json.loads(self._envelope())
        env[key] = value
        return json.dumps(env)

    def test_ill_typed_fields_are_reported_not_raised(self) -> None:
        for key, value in (("deadline_epoch_ms", "soon"),
                           ("deadline_epoch_ms", True),
                           ("gate_epoch", "seven"),
                           ("authentication", 12345),
                           ("payload_hash", None),
                           ("request_id", 99)):
            with self.subTest(key=key, value=value):
                ok, reason = t9.verify_envelope(self._mutated(key, value),
                                                "wave43-secret",
                                                t9.PROTOCOL_VERSION,
                                                t9.JW_DEADLINE - 1)
                self.assertFalse(ok)
                self.assertEqual(reason, "malformed envelope")


class P6_796_797ContractAndFileHandling(unittest.TestCase):
    def test_documented_wire_contract_names_payload_hash(self) -> None:
        self.assertIn("payload_hash= hex(sha256", t9.__doc__ or "")
        self.assertNotIn("payload_has=", t9.__doc__ or "")

    def test_missing_contract_file_is_reported_not_raised(self) -> None:
        self.assertIsNone(t9._read_text("/definitely/not/here/SignalJobConfig.java"))

    def test_readable_contract_file_is_returned(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "SignalJobConfig.java"
            path.write_text("class SignalJobConfig {}\n", encoding="utf-8")
            self.assertEqual(t9._read_text(path), "class SignalJobConfig {}\n")


class P6_368NetworkEnvNames(unittest.TestCase):
    def test_both_compose_env_forms_yield_names(self) -> None:
        names = enc._names
        self.assertEqual(names(["ARROW_X=1", "ARROW_Y=2"]), {"ARROW_X", "ARROW_Y"})
        self.assertEqual(names(["ARROW_X"]), {"ARROW_X"})
        self.assertEqual(names([{"ARROW_X": "1"}]), {"ARROW_X"})
        self.assertEqual(names({"ARROW_X": "1"}), {"ARROW_X"})
        self.assertEqual(names(None), set())

    def test_list_form_leak_is_reported(self) -> None:
        leaked = sorted(enc.ORDER_ARROW_ENV)[0]
        resolved = {"services": {"executor": {}}, "networks": {}}
        source = {"services": {"executor": {"environment": [f"{leaked}=x"]}}}
        errors = enc.validate_config(resolved, source=source)
        self.assertTrue(any(leaked in err for err in errors), errors)

    def test_mapping_form_leak_is_still_reported(self) -> None:
        leaked = sorted(enc.ORDER_ARROW_ENV)[0]
        resolved = {"services": {"executor": {}}, "networks": {}}
        source = {"services": {"executor": {"environment": {leaked: "x"}}}}
        errors = enc.validate_config(resolved, source=source)
        self.assertTrue(any(leaked in err for err in errors), errors)


class P6_728_729ProfilesAndTimeout(unittest.TestCase):
    def test_every_declared_profile_is_collected(self) -> None:
        if not HAVE_YAML:
            self.skipTest("PyYAML not installed")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "compose.yml"
            path.write_text(
                "services:\n"
                "  loadgen:\n    profiles: [loadgen]\n"
                "  executor:\n    profiles: [execution-t3]\n"
                "  plain: {}\n", encoding="utf-8")
            self.assertEqual(enc._declared_profiles(path), ["execution-t3", "loadgen"])
            path.write_text("services:\n  plain: {}\n", encoding="utf-8")
            self.assertEqual(enc._declared_profiles(path), [])

    def test_compose_config_is_bounded_and_caught(self) -> None:
        self.assertGreater(enc.COMPOSE_CONFIG_TIMEOUT_SEC, 0)
        src = inspect.getsource(enc.load_resolved_compose)
        self.assertIn("timeout=COMPOSE_CONFIG_TIMEOUT_SEC", src)
        self.assertIn("TimeoutExpired", src)
        self.assertIn("CalledProcessError", src)
        # P6-728: the profile list is derived, never pinned to one.
        self.assertIn("_declared_profiles(path)", src)
        self.assertNotIn('"--profile", "execution-t3"', src)


class P6_575_576_795SandboxContract(unittest.TestCase):
    def test_true_forms_are_caught_including_substitution_and_quotes(self) -> None:
        for text in ("EXECUTION_ENABLED: true", "EXECUTION_ENABLED=true",
                     'EXECUTION_ENABLED: "true"', "EXECUTION_ENABLED: 'true'",
                     "EXECUTION_ENABLED: ${EXECUTION_ENABLED:-true}"):
            with self.subTest(text=text):
                self.assertTrue(t8._EXEC_ENABLED_TRUE_RE.search(text), text)

    def test_false_forms_are_not_flagged(self) -> None:
        for text in ("EXECUTION_ENABLED: false", "EXECUTION_ENABLED=${FLAG:-false}",
                     "EXECUTION_DISABLED: true"):
            with self.subTest(text=text):
                self.assertFalse(t8._EXEC_ENABLED_TRUE_RE.search(text), text)

    def test_summary_uses_the_documented_prefix(self) -> None:
        src = inspect.getsource(t8)
        self.assertIn('t8-sandbox-contract: all', src)
        self.assertNotIn("local-sandbox-contract", src)

    def test_bare_self_reference_is_blank_but_a_real_default_is_not(self) -> None:
        nonblank = t8._nonblank_env
        for text in ("ARROW_X=${VAR}", "ARROW_X=${VAR:-}", 'ARROW_X="${VAR}"',
                     "ARROW_X=", "# ARROW_X=real", "ARROW_OTHER=real"):
            with self.subTest(text=text):
                self.assertFalse(nonblank(text, "ARROW_X"), text)
        for text in ("ARROW_X=real", "ARROW_X=${VAR:-real}", 'ARROW_X="${VAR:-real}"'):
            with self.subTest(text=text):
                self.assertTrue(nonblank(text, "ARROW_X"), text)


if __name__ == "__main__":
    unittest.main()
