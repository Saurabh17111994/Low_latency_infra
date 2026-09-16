"""Unit tests for env_facts.py - the ENVIRONMENT.md ledger keeper.

Offline: every test builds ledger text in memory (no repo file touched)
except the repo-ledger acceptance test, which only reads.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import env_facts as ef


def ledger(rows):
    return ("# T\n\n## Rules\n\n## How to add a fact\n\n## Ledger\n\n" + "\n".join(rows) + "\n")


def good_row(num=1, status="LIVE"):
    return (f"### FACT-{num:03d}: claim {num}\n"
            f"Status: {status}\n"
            f"Verified: 2026-09-16 - cmd shows x\n"
            f"Check: test 1 = 1\n"
            f"Recheck when: never\n"
            f"Body line.\n")


def good_ledger(n=2):
    return ledger([good_row(i) for i in range(1, n + 1)])


class ParseTests(unittest.TestCase):
    def test_clean_ledger_parses_without_errors(self):
        _, rows, errors = ef.parse_ledger(good_ledger(3))
        self.assertEqual(errors, [])
        self.assertEqual([r["id"] for r in rows],
                         ["FACT-001", "FACT-002", "FACT-003"])

    def test_missing_field_is_an_error(self):
        bad = good_row(1).replace("Check: test 1 = 1\n", "")
        _, _, errors = ef.parse_ledger(ledger([bad]))
        self.assertTrue(any("missing 'Check:'" in e for e in errors), errors)

    def test_wrapped_verified_value_still_parses(self):
        wrapped = good_row(1).replace(
            "Verified: 2026-09-16 - cmd shows x\n",
            "Verified: 2026-09-16 - a very long proof\nthat wraps lines\n")
        _, rows, errors = ef.parse_ledger(ledger([wrapped]))
        self.assertEqual(errors, [])
        self.assertIn("a very long proof", rows[0]["fields"]["Verified"])

    def test_duplicate_id_is_an_error(self):
        _, _, errors = ef.parse_ledger(ledger([good_row(1), good_row(1)]))
        self.assertTrue(any("duplicate" in e for e in errors), errors)

    def test_nonsequential_id_is_an_error(self):
        _, _, errors = ef.parse_ledger(ledger([good_row(1), good_row(3)]))
        self.assertTrue(any("FACT-002" in e for e in errors), errors)

    def test_bad_status_is_an_error(self):
        bad = good_row(1, status="MAYBE")
        _, _, errors = ef.parse_ledger(ledger([bad]))
        self.assertTrue(any("bad Status" in e for e in errors), errors)

    def test_dead_link_to_missing_fact_is_an_error(self):
        bad = good_row(1, status="DEAD (superseded by FACT-099)")
        _, _, errors = ef.parse_ledger(ledger([bad]))
        self.assertTrue(any("FACT-099" in e for e in errors), errors)

    def test_dead_link_to_live_fact_is_clean(self):
        ok = (good_row(1, status="DEAD (superseded by FACT-002)")
              + good_row(2))
        _, _, errors = ef.parse_ledger(ledger([ok]))
        self.assertEqual(errors, [])


class RepoAcceptanceTests(unittest.TestCase):
    def test_repo_ledger_is_clean(self):
        text = ef.read_ledger()
        self.assertIsNotNone(text, "docs/ENVIRONMENT.md unreadable")
        _, rows, errors = ef.parse_ledger(text)
        self.assertEqual(errors, [])
        live = [r for r in rows if r["fields"]["Status"] == "LIVE"]
        self.assertGreaterEqual(len(live), 10, "seed facts went missing")


if __name__ == "__main__":
    unittest.main()
