"""SCH-22 manifest-contract guard (M4-1).

The manifest gate must stay a real, requirement-backed check:

  * the uncalled exact version/count/fingerprint check stays deleted,
  * the parsed-manifest minimum is enforced at startup via ``belowMinimum``,
  * the key is read by ``IngestionConfig`` and documented in the dossier,
  * production and the daily VM pin 1024; the dev compose base keeps the
    code default (1).

This is the machine guard for the M4-1 finding: a future edit that resurrects
the dead exact check, drops the startup gate, or unpins the profiles fails here.
"""

import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[4]
ING = ROOT / "code/02_services/01_ingestion/src/main/java/com/trading/ingestion"
LOADER = ING / "InstrumentManifestLoader.java"
SERVICE = ING / "IngestionService.java"
CONFIG = ING / "config/IngestionConfig.java"
DOSSIER = ROOT / "docs/08_implementation/03-ingestion.md"
STACK = ROOT / "code/01_platform/01_docker/docker-stack.yml"
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
VM_ENV = ROOT / "code/01_platform/01_docker/.env.vm.example"
DAY_RUN = ROOT / "code/01_platform/04_scripts/day_run.py"


class Sch22ManifestContractTest(unittest.TestCase):

    def test_no_uncalled_exact_manifest_check(self):
        text = LOADER.read_text()
        self.assertNotIn("isManifestApproved", text,
                         "the uncalled exact version/count/fingerprint check must stay deleted")
        self.assertIn("belowMinimum", text,
                      "the parsed-manifest minimum gate must exist")

    def test_startup_enforces_the_minimum(self):
        text = SERVICE.read_text()
        self.assertIn("InstrumentManifestLoader.belowMinimum", text,
                      "startup must enforce the parsed-manifest minimum")
        self.assertIn("instrumentManifestMinCount", text,
                      "startup must pass the configured minimum")

    def test_config_reads_and_dossier_documents_the_key(self):
        self.assertIn("INSTRUMENT_MANIFEST_MIN_COUNT", CONFIG.read_text(),
                      "IngestionConfig must read the key")
        self.assertIn("INSTRUMENT_MANIFEST_MIN_COUNT", DOSSIER.read_text(),
                      "the dossier must document the key")

    def test_profiles_pin_the_real_universe(self):
        self.assertIn("INSTRUMENT_MANIFEST_MIN_COUNT: ${INSTRUMENT_MANIFEST_MIN_COUNT:-1024}",
                      STACK.read_text(), "production must pin 1024")
        self.assertIn("INSTRUMENT_MANIFEST_MIN_COUNT: ${INSTRUMENT_MANIFEST_MIN_COUNT:-1}",
                      COMPOSE.read_text(), "dev keeps the code default")
        self.assertIn("INSTRUMENT_MANIFEST_MIN_COUNT=1024", VM_ENV.read_text(),
                      "the daily VM profile must pin 1024")
        self.assertIn('"INSTRUMENT_MANIFEST_MIN_COUNT": "1024"', DAY_RUN.read_text(),
                      "the daily runner's effective env must pin 1024")


if __name__ == "__main__":
    unittest.main()
