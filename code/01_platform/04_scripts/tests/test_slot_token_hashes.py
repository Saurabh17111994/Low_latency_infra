"""H2-4 guard: the shared slot-token-hash fixture is internally consistent and
equals an independent carve + SHA-256 implementation.

The fixture ``code/testdata/slot-token-hashes.json`` is generated from the Go
bridge's carve (``BuildSubscriptionPlan`` + ``TokenSetHash``). Java's
``TokenSetHashTest`` and the Go ``slot_token_hashes_fixture_test.go`` pin their
own implementations to it; this validator re-implements the carve and the digest
from the spec in pure Python — no shared code with either side — so a change
that drifts BOTH implementations in the same way still fails here.
"""

from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
FIXTURE = REPO / "code/testdata/slot-token-hashes.json"


def carve(tokens, slot_count, connection_limit):
    """BuildSubscriptionPlan's shard: sort ascending, slice by connection limit."""
    ordered = sorted(tokens)
    slots = []
    for i in range(slot_count):
        start = i * connection_limit
        if start >= len(ordered):
            break
        slots.append((f"hft-{i}", ordered[start:start + connection_limit]))
    return slots


def token_set_hash(tokens):
    """SHA-256 over sorted tokens, each encoded as 8 big-endian bytes."""
    digest = hashlib.sha256()
    for token in sorted(tokens):
        digest.update(int(token).to_bytes(8, "big"))
    return digest.hexdigest()


class SlotTokenHashFixtureTest(unittest.TestCase):

    def test_fixture_matches_an_independent_carve_and_hash(self):
        fixture = json.loads(FIXTURE.read_text())
        self.assertEqual(1024, fixture["connectionLimit"])
        self.assertIn("do not hand-edit", fixture["comment"])
        self.assertTrue(fixture["cases"])
        limit = fixture["connectionLimit"]
        for case in fixture["cases"]:
            name = case["name"]
            slots = carve(case["tokens"], case["slotCount"], limit)
            listed = case["slots"]
            self.assertEqual([s["slotId"] for s in listed], [sid for sid, _ in slots], name)
            for (_, slice_tokens), want in zip(slots, listed):
                self.assertEqual(len(slice_tokens), want["size"], f"{name}/{want['slotId']}: size")
                self.assertEqual(token_set_hash(slice_tokens), want["hash"], f"{name}/{want['slotId']}: digest")
                self.assertRegex(want["hash"], r"^[0-9a-f]{64}$")
                self.assertLessEqual(want["size"], limit, name)
            self.assertEqual(token_set_hash(case["tokens"]), case["manifestFingerprint"], name)
            # Shape invariants: the slices are sorted, disjoint, and cover the input.
            flat = [t for _, slice_tokens in slots for t in slice_tokens]
            self.assertEqual(sorted(set(case["tokens"])), flat, name)
            for _, slice_tokens in slots:
                self.assertEqual(sorted(slice_tokens), slice_tokens, name)

    def test_fixture_has_a_multi_slot_case_where_the_digests_differ(self):
        # The defect H2-4 fixed is invisible unless a slot digest differs from
        # the full-set digest — pin that the fixture actually exercises it.
        fixture = json.loads(FIXTURE.read_text())
        for case in fixture["cases"]:
            full = case["manifestFingerprint"]
            per_slot = [s["hash"] for s in case["slots"]]
            if len(per_slot) > 1:
                self.assertNotIn(full, per_slot, f"{case['name']}: full-set digest equals a slot digest")
                return
        self.fail("fixture has no multi-slot case")

    def test_fixture_pins_the_real_datastream_universe(self):
        # The daily VM runs UNIVERSE=full: 2433 NSE cash instruments over 3
        # sockets (day_run.py). The fixture must carry that exact shape so any
        # carve change is caught against the real universe, not just synthetic
        # token counts.
        fixture = json.loads(FIXTURE.read_text())
        case = next((c for c in fixture["cases"] if c["name"] == "datastream-2433"), None)
        self.assertIsNotNone(case, "fixture must pin the real datastream universe")
        self.assertEqual(2433, len(case["tokens"]))
        self.assertEqual(3, case["slotCount"])
        self.assertEqual([1024, 1024, 385], [s["size"] for s in case["slots"]])


if __name__ == "__main__":
    unittest.main()
