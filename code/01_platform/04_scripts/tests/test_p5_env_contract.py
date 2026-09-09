"""P5-012 — instrument env-name contract between infra and the apps.

The apps read exactly three instrument-manifest env keys (verified readers):
  - ARROW_INSTRUMENT_TOKENS  (go-bridge main.go + IngestionConfig)
  - ARROW_INSTRUMENT_MANIFEST (go-bridge main.go:141, singular CSV path)
  - INSTRUMENT_MANIFEST_PATH  (Java InstrumentManifestLoader:55)
The plural ARROW_INSTRUMENT_MANIFESTS had NO reader — an unbuilt T1
alternative removed 2026-09-09 (single-CSV auto-shard supersedes it).
These guards keep infra files defining the real keys and stop the dead
plural (one letter from the live singular) from drifting back.
"""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
DOCKER = ROOT / "code/01_platform/01_docker"

INFRA_FILES = [
    DOCKER / "docker-compose.yml",
    DOCKER / "docker-stack.yml",
    DOCKER / ".env.example",
]

LIVE_KEYS = {
    # yml uses "KEY:", .env files use "KEY=" — accept either form.
    "ARROW_INSTRUMENT_TOKENS": ("ARROW_INSTRUMENT_TOKENS:", "ARROW_INSTRUMENT_TOKENS="),
    "ARROW_INSTRUMENT_MANIFEST": ("ARROW_INSTRUMENT_MANIFEST:", "ARROW_INSTRUMENT_MANIFEST="),
    "INSTRUMENT_MANIFEST_PATH": ("INSTRUMENT_MANIFEST_PATH:", "INSTRUMENT_MANIFEST_PATH="),
}
DEAD_KEY = "ARROW_INSTRUMENT_MANIFESTS"  # plural — never read by any app


class EnvContractTests(unittest.TestCase):
    def test_live_keys_defined_everywhere(self):
        for f in INFRA_FILES:
            text = f.read_text()
            for key, forms in LIVE_KEYS.items():
                self.assertTrue(any(form in text for form in forms),
                                f"{f.name}: live env key {key} missing")

    def test_dead_plural_key_stays_out(self):
        for f in INFRA_FILES:
            self.assertNotIn(DEAD_KEY, f.read_text(),
                             f"{f.name}: {DEAD_KEY} is dead (no reader) — "
                             f"use ARROW_INSTRUMENT_MANIFEST (singular)")

    def test_app_readers_still_exist(self):
        """The contract these env names serve — if an app renames its key,
        infra must follow; this fails loudly instead of silently decoupling."""
        go_bridge = (ROOT / "code/02_services/01_ingestion/go-bridge/main.go").read_text()
        self.assertIn('os.Getenv("ARROW_INSTRUMENT_MANIFEST")', go_bridge)
        self.assertIn("ARROW_INSTRUMENT_TOKENS", go_bridge)
        loader = (ROOT / "code/02_services/01_ingestion/src/main/java/com/"
                  "trading/ingestion/InstrumentManifestLoader.java").read_text()
        self.assertIn('getenv("INSTRUMENT_MANIFEST_PATH")', loader)


if __name__ == "__main__":
    unittest.main()
