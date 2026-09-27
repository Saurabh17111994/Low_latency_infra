"""CHG-333 — cross-service pin for the `Execution_Intent.instruction_id` grammar.

The compute builder mints `ei-v1-` + sha256 hex (70 chars); the gateway
`IntentValidator` must accept that exact form or every real intent is unreadable
and the gateway halts at offset 0 (CHG-331 attempt 2). No test crossed the
service boundary, so both suites stayed green while the deployed path was dead.

This test extracts both production sources and asserts the formats agree.
Per-module Java pins (`ExecutionIntentBuilderTest`, `IntentValidatorTest`) pin
each side separately; this is the compatibility pin between them.

Hermetic: no docker, no network, no cluster — source text only.
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
BUILDER = (REPO / "code/02_services/02_compute/src/main/java/com/trading/compute"
                  "/signaljob/ExecutionIntentBuilder.java")
VALIDATOR = (REPO / "code/02_services/06_execution_gateway/src/main/java/com/trading/execution"
                    "/gateway/IntentValidator.java")

# `return "ei-v1-" + sha256(identityContent(intent));`
BUILDER_PREFIX = re.compile(r'return\s+"([^"]+)"\s*\+\s*sha256\(identityContent\(')
# `INSTRUCTION_ID = ... Pattern.compile("<regex>");` (declaration may span lines).
VALIDATOR_PATTERN = re.compile(
    r'INSTRUCTION_ID\s*=\s*[\s\S]{0,200}?Pattern\.compile\("([^"]+)"\)')


def _java_literal_to_regex(literal: str) -> str:
    """Decode a Java source string literal into the regex it denotes."""
    return literal.replace(r"\\", "\\").replace(r"\"", '"')


class InstructionIdContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        builder = BUILDER.read_text(encoding="utf-8")
        cls.builder_match = BUILDER_PREFIX.search(builder)
        assert cls.builder_match is not None, (
            f"canonical instruction_id prefix not found in {BUILDER} — did the builder move?")
        validator = VALIDATOR.read_text(encoding="utf-8")
        cls.validator_match = VALIDATOR_PATTERN.search(validator)
        assert cls.validator_match is not None, (
            f"INSTRUCTION_ID pattern not found in {VALIDATOR} — did the validator move?")
        cls.pattern = re.compile(_java_literal_to_regex(cls.validator_match.group(1)))

    def test_validator_accepts_the_builder_canonical_form(self) -> None:
        canonical = self.builder_match.group(1) + "0123456789abcdef" * 4  # 64 lowercase hex
        self.assertTrue(self.pattern.fullmatch(canonical),
                        f"canonical id {canonical!r} must match the gateway "
                        f"INSTRUCTION_ID pattern {self.validator_match.group(1)!r}")

    def test_bounded_client_ids_still_accepted(self) -> None:
        # Harness/manual ids (t9 signs control actions with T9-SB-*; gateway e2e
        # hand-crafts halt-instr-*) keep working under the client branch.
        for client_id in ("T9-SB-20260927-120000", "halt-instr-0001"):
            self.assertTrue(self.pattern.fullmatch(client_id),
                            f"bounded client id {client_id!r} must still match")

    def test_non_canonical_forms_fail_closed(self) -> None:
        prefix = self.builder_match.group(1)
        # Canonical prefix with 63 hex chars (69 total): not the minted form.
        self.assertIsNone(self.pattern.fullmatch(prefix + "a" * 63))
        # Canonical prefix with 64 uppercase hex: the minted form is lowercase.
        self.assertIsNone(self.pattern.fullmatch(prefix + "A" * 64))
        # 65-char client id: past the client bound and not the canonical form.
        self.assertIsNone(self.pattern.fullmatch("x" * 65))


if __name__ == "__main__":
    unittest.main()
