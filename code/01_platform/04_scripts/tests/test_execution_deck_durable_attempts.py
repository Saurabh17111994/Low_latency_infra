"""H1-3: the durable attempt guard is ON in both execution decks — and actually durable.

The Workstream-D guard landed deliberately OFF (CHG-136). With it OFF, a gateway or
bridge restart could re-send an attempt the executor had already recorded: the
gateway's dedup table and the bridge's restart-lost cache were the only barriers.
This pins the flip in BOTH decks (compose and stack), plus the wiring the flip
depends on, because either half alone is a guard that does not guard:

* `DURABLE_ATTEMPTS_ENABLED="true"` and `DURABLE_DIR=/data/durable` in the nautilus
  service environment;
* the dir is the NAMED volume `nautilus-durable` mounted at that path, so the journal
  outlives container recreation — the exact restart the guard exists for;
* the image creates the mount point owned by the runtime user (`nobody`): Docker
  seeds a fresh named volume from the image directory, ownership included. Without
  it the enabled flag fails closed at boot (EACCES) instead of trading unguarded.

Config-only: YAML + Dockerfile text, no containers.
"""
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[4]
DOCKER = ROOT / "code/01_platform/01_docker"
COMPOSE = DOCKER / "docker-compose.yml"
STACK = DOCKER / "docker-stack.yml"
DOCKERFILE = ROOT / "code/02_services/04_executor/Dockerfile"

DURABLE_DIR = "/data/durable"
VOLUME = "nautilus-durable"


class DurableAttemptGuardDecks(unittest.TestCase):

    def test_flag_dir_and_named_volume_in_both_decks(self):
        for path in (COMPOSE, STACK):
            deck = yaml.safe_load(path.read_text(encoding="utf-8"))
            service = deck["services"]["nautilus"]
            env = service["environment"]
            self.assertEqual(
                env.get("DURABLE_ATTEMPTS_ENABLED"),
                "true",
                f"{path.name}: the durable attempt guard must be ON in this deck",
            )
            self.assertEqual(
                env.get("DURABLE_DIR"),
                DURABLE_DIR,
                f"{path.name}: the journal must sit at the documented path",
            )
            mounts = [str(mount) for mount in service.get("volumes", [])]
            self.assertIn(
                f"{VOLUME}:{DURABLE_DIR}",
                mounts,
                f"{path.name}: the journal must be on a named volume, not the container layer",
            )
            self.assertIn(
                VOLUME,
                deck.get("volumes") or {},
                f"{path.name}: the named volume must be declared at the top level",
            )

    def test_image_seeds_the_mount_point_owned_by_the_runtime_user(self):
        text = DOCKERFILE.read_text(encoding="utf-8")
        self.assertIn(
            "chown -R nobody:nogroup /data/durable",
            text,
            "Docker seeds an empty named volume from the image path: the directory must exist "
            "and be owned by the runtime user, or the enabled flag fails closed at boot",
        )
        self.assertLess(
            text.index("mkdir -p /data/durable"),
            text.index("\nUSER nobody"),
            "the mount point must be created and chowned before the process drops to nobody",
        )


if __name__ == "__main__":
    unittest.main()
