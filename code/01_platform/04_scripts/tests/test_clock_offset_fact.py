#!/usr/bin/env python3
"""The node's clock-offset publisher: it must publish the real number, or nothing at all.

WHY THIS EXISTS (CHG-288)
    `clock_offset_fact.sh` is the only writer of the fact the executor's drift gate reads. Two
    failure modes matter and neither is visible from a passing deploy: publishing a wrong number
    (which silently widens or narrows the gate) and publishing *something* when the clock is not
    being measured (which turns "nobody is watching" into a comfortable lie). So the tests assert
    both the value and the refusal, and every refusal asserts that the previous sample survived —
    the consumer's staleness limit is what converts that into a halt.

    The script is driven with a stub `chronyc` on PATH, so no chrony daemon is needed and nothing
    touches /run: hermetic, no network, no wall clock beyond the freshness field.

HONEST LIMITATION
    This proves the writer's contract, not that a real chronyd reports a real offset. That is S4's
    `vm-bootstrap.sh --check` on the VM, and T9's positive/negative pair in the post-verification
    plan.
"""

from __future__ import annotations

import os
import stat
import subprocess
import time
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "clock_offset_fact.sh"

# A `chronyc tracking` sample in the shape chronyd prints (only the `System time` line is read).
TRACKING = """\
Reference ID    : 1B2C3D4E (ntp.example)
System time     : {offset} seconds {direction} of NTP time
Leap status     : Normal
"""


def run_script(tmp_path: Path, chronyc_body: str | None) -> subprocess.CompletedProcess[str]:
    """Run the publisher with a stub chronyc (or none at all) and a private output directory."""
    env = {
        **os.environ,
        "CLOCK_OFFSET_DIR": str(tmp_path / "arrow-clock"),
    }
    invoked = tmp_path / "chronyc-was-called"
    if chronyc_body is not None:
        stub_dir = tmp_path / "bin"
        stub_dir.mkdir(parents=True, exist_ok=True)
        stub = stub_dir / "chronyc"
        stub.write_text(
            "#!/bin/sh\n"
            f": > {invoked}\n"  # proves the script ran the stub rather than trusting an old file
            f"{chronyc_body}\n"
        )
        stub.chmod(0o755)
        env["CHRONYC"] = str(stub)
    else:
        env["CHRONYC"] = str(tmp_path / "does-not-exist")
    proc = subprocess.run(
        ["bash", str(SCRIPT)], capture_output=True, text=True, env=env, check=False
    )
    proc.invoked = invoked.exists()  # type: ignore[attr-defined]
    return proc


def fact(tmp_path: Path) -> Path:
    return tmp_path / "arrow-clock" / "offset"


def read_fact(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text().splitlines():
        key, _, value = line.partition("=")
        out[key] = value
    return out


def test_publishes_the_offset_with_its_sign_and_a_sample_time(tmp_path: Path) -> None:
    before = int(time.time())
    proc = run_script(
        tmp_path,
        f"cat <<'EOF'\n{TRACKING.format(offset='0.350000000', direction='fast')}EOF",
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.invoked, "the stub chronyc must have been run"

    published = read_fact(fact(tmp_path))
    assert published["offset_ms"] == "350"
    assert published["source"] == "chronyc-tracking-field4"
    assert before <= int(published["measured_epoch_s"]) <= int(time.time()) + 1
    # the container user is `nobody`: the directory must be traversable and the file readable
    mode = stat.S_IMODE(fact(tmp_path).stat().st_mode)
    assert mode == 0o644, f"offset file mode {oct(mode)}"
    assert stat.S_IMODE(fact(tmp_path).parent.stat().st_mode) == 0o755


@pytest.mark.parametrize(
    ("offset", "direction", "expected"),
    [
        ("0.350000000", "fast", "350"),
        ("-0.045678901", "slow", "-46"),  # the sign survives the seconds->ms conversion
        ("0.000500000", "fast", "1"),  # half away from zero, like the consumer's f64::round
        ("-0.000500000", "slow", "-1"),
        ("0.000000001", "fast", "0"),  # sub-millisecond drift is a real, reportable zero
    ],
)
def test_rounds_like_the_consumer(
    tmp_path: Path, offset: str, direction: str, expected: str
) -> None:
    proc = run_script(
        tmp_path, f"cat <<'EOF'\n{TRACKING.format(offset=offset, direction=direction)}EOF"
    )
    assert proc.returncode == 0, proc.stderr
    assert read_fact(fact(tmp_path))["offset_ms"] == expected


@pytest.mark.parametrize(
    ("name", "body"),
    [
        ("failing", "exit 1"),
        ("silent", "exit 0"),  # no output at all
        ("no-system-time", "echo 'Leap status     : Normal'"),
        ("unparseable", "echo 'System time     : soon seconds fast of NTP time'"),
        ("empty-offset", "echo 'System time     : seconds fast of NTP time'"),
    ],
)
def test_publishes_nothing_when_it_cannot_measure(
    tmp_path: Path, name: str, body: str
) -> None:
    proc = run_script(tmp_path, body)
    assert proc.returncode != 0, f"{name}: a broken measurement must not exit 0"
    assert not fact(tmp_path).exists(), f"{name}: no sample may be published"


def test_a_failed_run_leaves_the_previous_sample_for_the_staleness_limit(tmp_path: Path) -> None:
    """The refusal is *not* a delete: an old sample plus the consumer's limit is the halt."""
    good = f"cat <<'EOF'\n{TRACKING.format(offset='0.120000000', direction='fast')}EOF"
    assert run_script(tmp_path, good).returncode == 0
    first = fact(tmp_path).read_text()

    assert run_script(tmp_path, "exit 1").returncode != 0
    assert fact(tmp_path).read_text() == first, "a failed run must not touch the published sample"

    # ...and a later good run replaces it in place, so a directory-mounted reader sees the new one.
    better = f"cat <<'EOF'\n{TRACKING.format(offset='0.010000000', direction='fast')}EOF"
    assert run_script(tmp_path, better).returncode == 0
    assert read_fact(fact(tmp_path))["offset_ms"] == "10"
    assert not list(fact(tmp_path).parent.glob("*.tmp.*")), "no temp file may be left behind"


def test_replaces_a_directory_left_by_a_missing_bind_mount(tmp_path: Path) -> None:
    """Docker creates a directory for a missing mount source; the writer must not move into it."""
    fact(tmp_path).mkdir(parents=True)
    proc = run_script(
        tmp_path, f"cat <<'EOF'\n{TRACKING.format(offset='0.200000000', direction='fast')}EOF"
    )
    assert proc.returncode == 0, proc.stderr
    assert fact(tmp_path).is_file()
    assert read_fact(fact(tmp_path))["offset_ms"] == "200"
