"""M1-1: the execution decks must resolve the executor's partition/scope like the gateway's.

The forwarded envelope carries `execution_partition_id` (and the H2-5 gate report carries
`account_scope_id`); the executor refuses a foreign partition with 403 under the send lock and
the gateway refuses a foreign report row. If the two services resolve their env differently —
which they did in `docker-stack.yml` before M1-1: the gateway defaulted to `prod-*` while the
executor was pinned to `dev-*` — every order in that deck would be refused, and the mismatch
would only surface once execution was enabled.

This pins the raw values identical between `execution-gateway` and `nautilus` in both decks, so
an edit to one side that forgets the other fails here instead of in production.

Config-only: YAML parsing, no containers.
"""
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[4]
DOCKER = ROOT / "code/01_platform/01_docker"
DECKS = (DOCKER / "docker-compose.yml", DOCKER / "docker-stack.yml")
KEYS = ("EXECUTION_PARTITION_ID", "ACCOUNT_SCOPE_ID")


class DeckPartitionAlignment(unittest.TestCase):

    def test_gateway_and_executor_resolve_the_same_partition_and_scope(self):
        for path in DECKS:
            deck = yaml.safe_load(path.read_text(encoding="utf-8"))
            gateway = deck["services"]["execution-gateway"]["environment"]
            nautilus = deck["services"]["nautilus"]["environment"]
            for key in KEYS:
                self.assertIn(key, gateway, f"{path.name}: gateway must carry {key}")
                self.assertIn(key, nautilus, f"{path.name}: executor must carry {key}")
                self.assertEqual(
                    gateway[key],
                    nautilus[key],
                    f"{path.name}: {key} must resolve identically on both sides — a mismatch "
                    f"refuses every forwarded order (M1-1) and every H2-5 gate report",
                )


if __name__ == "__main__":
    unittest.main()
