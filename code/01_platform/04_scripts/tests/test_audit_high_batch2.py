"""Audit HIGH batch 2 guards (2026-10-01): host-launcher feed, soak manifest
mount, and the gate-forced manifest test — portability fixes for H6/H7/H8.

- H6: `code/run-ingestion-full.sh` must default ARROW_FEED=token. The Go bridge
  defaults to HFT and the broker rejects HFT (PLAN_NOT_SUBSCRIBED since
  2026-09-24), so the documented host path would ingest nothing.
- H7: `docker-compose.soak.yml` must allow overriding the 1,024-instrument
  manifest source instead of hardcoding this PC's absolute path.
- H8: `ManifestLoadTest` runs under the Monday gate (INGESTION_INT_TEST_MANIFEST=true)
  and must resolve its manifests through env overrides + existence guards, so a
  machine without the dev-PC directory does not fail the gate.

Offline only: reads files, never runs the tools.
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
RUNNER = ROOT / "code/run-ingestion-full.sh"
SOAK = ROOT / "code/01_platform/01_docker/docker-compose.soak.yml"
MANIFEST_TEST = (
    ROOT / "code/02_services/01_ingestion/src/test/java/com/trading/ingestion"
    / "ManifestLoadTest.java"
)


class AuditHighBatch2Tests(unittest.TestCase):
    def test_host_launcher_defaults_to_the_token_stream(self):
        """H6: an unset ARROW_FEED must not silently select the dead HFT feed."""
        text = RUNNER.read_text()
        self.assertIn(
            'export ARROW_FEED="${ARROW_FEED:-token}"',
            text,
            "the host launcher must default ARROW_FEED=token like compose does "
            "(the broker rejects HFT since 2026-09-24)",
        )

    def test_soak_manifest_source_is_overridable(self):
        """H7: no bare dev-PC absolute path; an override variable must exist."""
        text = SOAK.read_text()
        source_lines = [
            line.strip() for line in text.splitlines() if line.strip().startswith("source:")
        ]
        self.assertTrue(
            any("${SOAK_MANIFEST_PATH:-" in line for line in source_lines),
            "the soak manifest bind must use ${SOAK_MANIFEST_PATH:-...}",
        )
        bare = [line for line in source_lines if re.match(r"source:\s*/home/", line)]
        self.assertEqual(bare, [], f"bare absolute manifest sources found: {bare}")

    def test_gate_manifest_test_has_env_overrides_and_existence_guards(self):
        """H8: the Monday gate forces this test on; it must not require /home/saurabh."""
        text = MANIFEST_TEST.read_text()
        self.assertIn(
            "INGESTION_TEST_MANIFEST_PATH",
            text,
            "the full-manifest test must accept INGESTION_TEST_MANIFEST_PATH",
        )
        self.assertIn(
            "INGESTION_TEST_MANIFEST_1024_PATH",
            text,
            "the 1024-manifest test must accept INGESTION_TEST_MANIFEST_1024_PATH",
        )
        self.assertNotRegex(
            text,
            r'loadFromPath\(\s*"/home/',
            "no loadFromPath on a bare dev-PC path: resolve the path through the override",
        )


if __name__ == "__main__":
    unittest.main()
