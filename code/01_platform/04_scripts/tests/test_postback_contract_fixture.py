#!/usr/bin/env python3
"""C1-6: the shared postback fixture and the contracts agree.

Gate-discovered (test_*.py). Fails when:
  - a contract Arrow reportType loses its fixture coverage,
  - the fixture emits a canonical value contracts 06/07 do not define,
  - a fixture case loses a normalized key,
  - the fixture file moves away from the one include_str! path the Rust side uses.

The fixture is consumed by go-bridge/postback_normalization_test.go and by the
executor's dispatch test (include_str!). This test keeps the third side — the
documented Arrow vocabulary — pinned to the same file.
"""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
FIXTURE = ROOT / "code" / "testdata" / "postback-report-types.json"
ARROW_CONTRACT = ROOT / "docs" / "04_contracts" / "arrow_broker.md"
ACTION_CONTRACT = ROOT / "docs" / "04_contracts" / "06-action-capture.md"
EXECUTOR_CONTRACT = ROOT / "docs" / "04_contracts" / "07-executor.md"

CANONICAL = {
    "order_filled",
    "order_canceled",
    "order_rejected",
    "order_accepted",
    "order_unknown",
}
# The bridge accepts Arrow's British spelling as an alias; the contract table lists
# "Canceled". An alias case is allowed but never required.
REPORT_TYPE_ALIASES = {"cancelled"}


def _fixture_cases() -> list[dict]:
    if not FIXTURE.is_file():
        raise AssertionError(f"shared postback fixture is missing: {FIXTURE}")
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    cases = data.get("cases")
    if not isinstance(cases, list) or not cases:
        raise AssertionError("fixture must carry a non-empty `cases` list")
    return cases


def _arrow_report_types() -> list[str]:
    """The reportType vocabulary from arrow_broker.md section 4."""
    text = ARROW_CONTRACT.read_text(encoding="utf-8")
    match = re.search(r"`reportType`\s*\(([^)]+)\)", text)
    if not match:
        raise AssertionError("arrow_broker.md no longer documents a reportType vocabulary")
    return [value.strip().strip("`") for value in match.group(1).split("/") if value.strip()]


class PostbackContractFixtureTest(unittest.TestCase):
    def test_fixture_covers_the_arrow_vocabulary(self) -> None:
        vocabulary = _arrow_report_types()
        covered = {}
        for case in _fixture_cases():
            report_type = case["arrow"].get("reportType", "")
            covered.setdefault(report_type.lower(), []).append(case["name"])
        for value in vocabulary:
            self.assertIn(
                value.lower(),
                covered,
                f"the Arrow reportType {value!r} has no fixture case (covered: {sorted(covered)})",
            )
        for case in _fixture_cases():
            report_type = case["arrow"].get("reportType", "")
            if not report_type:
                continue
            known = (
                report_type.lower() in {v.lower() for v in vocabulary}
                or report_type.lower() in REPORT_TYPE_ALIASES
            )
            if not known:
                # A deliberate unknown-vocabulary case is how the fail-closed halt is pinned;
                # any other non-contract value is drift.
                self.assertEqual(
                    case["event_type"],
                    "order_unknown",
                    f"fixture case {case['name']!r} uses reportType {report_type!r} that neither "
                    "the contract nor the alias list defines, yet does not map to order_unknown",
                )

    def test_fixture_event_types_are_the_canonical_set(self) -> None:
        for case in _fixture_cases():
            self.assertIn(
                case.get("event_type"),
                CANONICAL,
                f"case {case.get('name')!r} maps to a non-canonical event_type {case.get('event_type')!r}",
            )
        values = {case.get("event_type") for case in _fixture_cases()}
        self.assertTrue(
            {"order_filled", "order_canceled", "order_rejected", "order_accepted", "order_unknown"}
            <= values,
            f"the fixture must exercise every canonical value; it has {sorted(values)}",
        )

    def test_contracts_define_the_canonical_set(self) -> None:
        text = ACTION_CONTRACT.read_text(encoding="utf-8") + EXECUTOR_CONTRACT.read_text(encoding="utf-8")
        defined = {
            value
            for value in re.findall(r"`(order_(?:filled|canceled|rejected|accepted|unknown))`", text)
        }
        self.assertEqual(
            defined,
            CANONICAL,
            "contracts 06/07 must name exactly the canonical event_type set; "
            f"contracts define {sorted(defined)}, fixture uses {sorted(CANONICAL)}",
        )

    def test_every_case_carries_the_normalized_shape(self) -> None:
        required = {"fill_quantity", "fill_price", "reject_reason"}
        for case in _fixture_cases():
            self.assertEqual(
                set(case.get("normalized", {})),
                required,
                f"case {case.get('name')!r} normalized keys drifted",
            )
            self.assertIn("arrow", case, f"case {case.get('name')!r} lost its arrow input")
            self.assertIn("name", case, f"case {case.get('name')!r} lost its name")

if __name__ == "__main__":
    unittest.main()