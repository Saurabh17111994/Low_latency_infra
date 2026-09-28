"""F8 — KV snapshots survive the dev stack: mount + interval pins (CHG-359).

2026-09-28: every cold start replayed the whole retained changelog — each KV
bucket logged "No snapshot found" and rebuilt from log offset 0 (measured
22 min 56 s, dominated by candle_live: ~90 s x 16 buckets). The compose knob
already existed but the snapshot store had no persistent mount, so enabling the
interval alone would still lose the snapshots on every container recreation.
The fix is three pins that must hold together:

1. the tablet mounts the shared snapshot store (it writes + reads snapshots),
2. the coordinator mounts the same store (it serves the lake-snapshot RPC the
   tiering path reads from the same directory),
3. the interval stays parameterized with the prod-safe default (``0s``) while
   the dev env template enables it, so production switches deliberately.

Offline only: reads the compose source and the env template.
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
ENV_EXAMPLE = ROOT / "code/01_platform/01_docker/.env.example"
SNAPSHOT_MOUNT = "fluss-remote-data:/tmp/fluss-remote-data"


def service_block(text, name):
    """One service's compose source, comments removed, up to the next key.

    Comments are stripped so a pin cannot be satisfied by prose (the same
    reason test_08_local_compose_prod.py strips them).
    """
    lines = text.splitlines()
    start = next(
        (i for i, l in enumerate(lines) if l.rstrip() == f"  {name}:"), None
    )
    if start is None:
        raise AssertionError(f"service {name!r} not found in compose")
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if re.match(r"^  \S", lines[i]):
            end = i
            break
    body = (l for l in lines[start:end] if not l.lstrip().startswith("#"))
    return "\n".join(body)


class KvSnapshotPinsTests(unittest.TestCase):
    def setUp(self):
        self.compose = COMPOSE.read_text()
        self.env_example = ENV_EXAMPLE.read_text()

    def test_tablet_mounts_the_snapshot_store(self):
        block = service_block(self.compose, "fluss-tablet")
        self.assertIn(SNAPSHOT_MOUNT, block,
                      "the tablet must mount the KV snapshot store, or every "
                      "cold start replays the whole changelog again")

    def test_coordinator_mounts_the_snapshot_store(self):
        block = service_block(self.compose, "fluss-coordinator")
        self.assertIn(SNAPSHOT_MOUNT, block,
                      "the coordinator serves the lake-snapshot RPC over the "
                      "same store; unmounted, its view diverges from the tablet")

    def test_interval_knob_keeps_the_prod_safe_default(self):
        block = service_block(self.compose, "fluss-tablet")
        self.assertIn("kv.snapshot.interval: ${FLUSS_KV_SNAPSHOT_INTERVAL:-0s}",
                      block,
                      "production (docker-stack.yml) runs snapshots off unless "
                      "an environment switches them on deliberately")

    def test_dev_env_example_enables_one_minute_snapshots(self):
        self.assertRegex(
            self.env_example, r"(?m)^FLUSS_KV_SNAPSHOT_INTERVAL=1m$",
            "the dev env template must enable the interval the snapshot "
            "mount exists for")


if __name__ == "__main__":
    unittest.main()
