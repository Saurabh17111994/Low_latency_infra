"""Fluss R2 secret bridge - offline static validation.

The plan's Task 3: Swarm delivers R2 credentials as FILES (/run/secrets),
but the Fluss server.yaml reads them from the ENVIRONMENT
($${env.AWS_ACCESS_KEY_ID} placeholders). The stock Fluss image has no
*_FILE handling, so fluss-r2-secrets-from-file.sh bridges them - and must
fail closed when a named file is unusable.

Two halves: the script does what it claims (BridgeScriptTests),
and the stack actually wires it into all four Fluss services (StackWiringTests).
"""

import pathlib
import re
import unittest

TESTS_DIR = pathlib.Path(__file__).resolve().parent
REPO = TESTS_DIR.parents[3]
BRIDGE = REPO / "code/01_platform/01_docker/fluss-r2-secrets-from-file.sh"
STACK = REPO / "code/01_platform/01_docker/docker-stack.yml"

FLUSS_SERVICES = [
    ("fluss-coordinator", "coordinatorServer"),
    ("fluss-tablet-1", "tabletServer"),
    ("fluss-tablet-2", "tabletServer"),
    ("fluss-tablet-3", "tabletServer"),
]


def code_lines(text: str) -> list:
    """Source lines with comments and blanks removed.

    Assertions about what a script DOES must not be satisfied by prose about
    it: this file is heavily commented, and a paragraph describing the dash
    caveat would otherwise read as handling it.
    """
    out = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0] if not raw.lstrip().startswith("#") else ""
        line = line.strip()
        if line:
            out.append(line)
    return out


class BridgeScriptTests(unittest.TestCase):
    """The bridge script: secrets in, fail closed when unreadable."""

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
        self.assertIn("_FILE", "\n".join(self.lines),
                      "the *_FILE naming convention must appear in code, not just comments")

    def test_strips_trailing_newlines(self) -> None:
        """Swarm secret files end with a newline; a signature must not."""
        self.assertIn("tr -d", "\n".join(self.lines))

    def test_unreadable_secret_fails_closed(self) -> None:
        self.assertRegex(self.text, r"! -r ", "readability must be tested before use")
        self.assertRegex(self.text, r"exit 1", "an unreadable secret must abort startup")
        self.assertRegex(self.text, r"FATAL", "the failure must say FATAL")

    def test_empty_secret_fails_closed(self) -> None:
        """An empty file reads fine but signs nothing - must abort, not bridge blank."""
        self.assertRegex("\n".join(self.lines), r"-z ",
                         "emptiness must be tested after stripping")

    def test_missing_variable_is_not_an_error(self) -> None:
        """Dev compose passes the values directly; the bridge must no-op then."""
        self.assertRegex(self.text, r":-\}",
                         "an unset *_FILE must be tolerated, not treated as a failure")

    def test_execs_the_base_entrypoint_preserving_arguments(self) -> None:
        self.assertRegex("\n".join(self.lines), r'exec /docker-entrypoint\.sh "\$@"',
                         "the bridge must hand off to the base entrypoint with our args")

    def test_header_documents_the_dash_caveat(self) -> None:
        """The reason for read-then-check (not inline export) must survive refactors."""
        self.assertRegex(self.text, r"[Dd]ash",
                         "the header must name the dash substitution caveat")

    def test_runs_under_plain_sh(self) -> None:
        """The entrypoint is /bin/sh (dash); bash-isms would die on first boot."""
        self.assertRegex(self.text, r"^#!/bin/sh",
                         "shebang must be plain sh - the stack runs it via /bin/sh")


class BrokenVariantTests(unittest.TestCase):
    """Each pin above must name its break: run it against a sabotaged script."""

    def setUp(self) -> None:
        self.text = BRIDGE.read_text(encoding="utf-8")

    def check_break(self, sabotage: str, probe: str, name: str) -> None:
        broken = self.text.replace(sabotage, "", 1)
        self.assertNotEqual(broken, self.text, f"sabotage missed for {name}")
        self.assertNotRegex("\n".join(code_lines(broken)), probe,
                            f"pin for {name} still passes on the broken variant")

    def test_pin_names_missing_bridge_call(self) -> None:
        self.check_break("bridge AWS_SECRET_ACCESS_KEY",
                         r"bridge\s+AWS_SECRET_ACCESS_KEY\b", "secret bridge call")

    def test_pin_names_missing_readability_gate(self) -> None:
        self.check_break("! -r ", r"! -r ", "readability gate")

    def test_pin_names_missing_empty_gate(self) -> None:
        self.check_break("-z ", r"-z ", "empty gate")

    def test_pin_names_missing_handoff(self) -> None:
        self.check_break('exec /docker-entrypoint.sh "$@"',
                         r'exec /docker-entrypoint\.sh "\$@"', "entrypoint handoff")


class StackWiringTests(unittest.TestCase):
    """All four Fluss services must actually run the bridge."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.text = STACK.read_text(encoding="utf-8")

    def _service_block(self, name: str) -> str:
        lines = self.text.splitlines()
        start = next(i for i, ln in enumerate(lines) if ln == f"  {name}:")
        end = len(lines)
        for i in range(start + 1, len(lines)):
            ln = lines[i]
            if ln.startswith("  ") and not ln.startswith("    ") and ln.rstrip().endswith(":"):
                end = i
                break
        return "\n".join(lines[start:end])

    def test_bridge_script_is_registered_as_a_config(self) -> None:
        self.assertRegex(self.text,
                         r"fluss-r2-secrets-bridge:\s*\n\s*file: \./fluss-r2-secrets-from-file\.sh",
                         "the bridge must be registered under top-level configs:")

    def test_each_fluss_service_runs_the_bridge_entrypoint(self) -> None:
        for svc, _ in FLUSS_SERVICES:
            with self.subTest(service=svc):
                block = self._service_block(svc)
                self.assertRegex(
                    block,
                    r'entrypoint:\s*\["/bin/sh",\s*"/opt/fluss/bin/r2-secrets-from-file\.sh"\]',
                    f"{svc} must run the bridge as its entrypoint")

    def test_each_fluss_service_mounts_the_bridge(self) -> None:
        for svc, _ in FLUSS_SERVICES:
            with self.subTest(service=svc):
                block = self._service_block(svc)
                self.assertIn("fluss-r2-secrets-bridge", block,
                              f"{svc} must mount the bridge config")

    def test_each_fluss_service_names_both_secret_files(self) -> None:
        for svc, _ in FLUSS_SERVICES:
            with self.subTest(service=svc):
                block = self._service_block(svc)
                self.assertIn("AWS_ACCESS_KEY_ID_FILE: /run/secrets/aws_access_key_id", block)
                self.assertIn("AWS_SECRET_ACCESS_KEY_FILE: /run/secrets/aws_secret_access_key", block)

    def test_no_credential_value_in_environment(self) -> None:
        """Files are the only source - a literal key in environment: would leak to logs."""
        for svc, _ in FLUSS_SERVICES:
            with self.subTest(service=svc):
                for ln in self._service_block(svc).splitlines():
                    s = ln.strip()
                    if s.startswith("AWS_ACCESS_KEY_ID:") or s.startswith("AWS_SECRET_ACCESS_KEY:"):
                        self.fail(f"{svc} carries a credential value in environment: {s!r}")


if __name__ == "__main__":
    unittest.main()
