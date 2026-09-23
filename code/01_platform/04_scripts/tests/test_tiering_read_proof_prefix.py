"""The read proof appends /log/<db> to remote.data.dir and greps for its own table.

Two silent failure modes cost a run on 2026-09-23: polling the LAKE prefix
(r2_list_lake with no argument = S3_WAREHOUSE_PATH), where remote-log tiering never
writes, and deriving the prefix from the compose so naively that it kept the literal
${R2_BUCKET:?...} template. Both produce "0 objects" and therefore look identical to
"tiering did not run". This asserts the derivation instead of trusting it.
"""
from __future__ import annotations

import pathlib
import re
import subprocess

REPO = pathlib.Path(__file__).resolve().parents[4]
SCRIPT = REPO / "code/01_platform/04_scripts/tiering-remote-read-verify.sh"
COMPOSE = REPO / "code/01_platform/01_docker/docker-compose.yml"


def _derivation() -> str:
    text = SCRIPT.read_text()
    return text[text.index("RDD_RAW=$(grep"):text.index('echo "  polling')]


def test_prefix_derivation_is_concrete() -> None:
    r = subprocess.run(
        ["bash", "-c", _derivation() + '\nprintf "%s" "$RDD"'],
        cwd=REPO, capture_output=True, text=True,
    )
    assert r.returncode == 0, f"derivation aborted: {r.stderr.strip()}"
    assert r.stdout == "remote-data", f"derived {r.stdout!r}"


def test_remote_data_dir_is_the_tiering_destination() -> None:
    assert re.search(r"remote\.data\.dir:.*remote-data", COMPOSE.read_text())
