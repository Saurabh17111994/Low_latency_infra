"""M2-2: a missed or failed EOD slot is caught up the same day, not skipped to tomorrow.

Hermetic: the controller is a fake script and the scheduler is the real process loop, so
what is tested is the *recovery* — that a slot which passed with no success fires now,
that a failed fire retries inside the day, and that the durable state records which slot
succeeded so a restart does not re-fire a day that is already archived.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path
from zoneinfo import ZoneInfo

SCRIPT = Path(__file__).resolve().parents[1] / "eod_schedule.py"
KOLKATA = ZoneInfo("Asia/Kolkata")


def _fake_controller(directory: Path, codes: list[int]) -> tuple[Path, Path]:
    """A controller that exits with the Nth code from `codes` (last repeats) and records calls."""
    log = directory / "calls.log"
    path = directory / "fake_controller.py"
    path.write_text(
        "import sys\n"
        f"log = {str(log)!r}\n"
        f"codes = {codes!r}\n"
        "with open(log, 'a') as fh:\n"
        "    fh.write('call\\n')\n"
        "n = sum(1 for _ in open(log))\n"
        "sys.exit(codes[min(n, len(codes)) - 1] if codes else 0)\n"
    )
    return path, log


def _passed_at(minutes_ago: int = 2) -> str:
    """A time-of-day that already passed today (IST), so catch-up must trigger."""
    return (dt.datetime.now(KOLKATA) - dt.timedelta(minutes=minutes_ago)).strftime("%H:%M")


def _start(tmp_path: Path, *args: str) -> tuple[subprocess.Popen, queue.Queue]:
    """Start the real scheduler loop; a reader thread feeds every output line into a queue.

    A queue, not select+readline: readline() buffers ahead, so select on the pipe fd can
    say "not ready" while the line the test waits for sits in the TextIOWrapper buffer.
    """
    proc = subprocess.Popen(
        [sys.executable, str(SCRIPT), *args],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        env={**os.environ}, cwd=str(tmp_path), bufsize=1,
    )
    lines: queue.Queue = queue.Queue()

    def pump() -> None:
        for line in proc.stdout:
            lines.put(line.rstrip())
        lines.put(None)  # EOF

    threading.Thread(target=pump, daemon=True).start()
    return proc, lines


def _wait_for_line(lines: queue.Queue, needle: str, timeout: float = 30.0) -> str:
    """Consume output until `needle` appears; fail the test on EOF or timeout."""
    deadline = time.monotonic() + timeout
    seen: list[str] = []
    while time.monotonic() < deadline:
        try:
            line = lines.get(timeout=min(0.5, max(0.05, deadline - time.monotonic())))
        except queue.Empty:
            continue
        if line is None:
            raise AssertionError(f"process exited before {needle!r}; output so far: {seen}")
        seen.append(line)
        if needle in line:
            return line
    raise AssertionError(f"never saw {needle!r}; output so far: {seen}")


def _stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def test_a_missed_slot_is_caught_up_and_a_failed_fire_retries_until_success(tmp_path):
    controller, log = _fake_controller(tmp_path, [1, 0])  # fail, then the retry succeeds
    state = tmp_path / "eod-state.json"
    proc, lines = _start(tmp_path,
                         "--at", _passed_at(), "--zone", "Asia/Kolkata",
                         "--controller", str(controller),
                         "--heartbeat", str(tmp_path / "hb"),
                         "--state", str(state),
                         "--max-retries", "0", "--retry-delay", "0",
                         "--retry-delay-sec", "0.05", "--sleep-chunk", "0.05")
    try:
        line = _wait_for_line(lines, "CATCH-UP")
        assert "no success record" in line
        _wait_for_line(lines, "EOD RUN FAILED")
        _wait_for_line(lines, "same-day retry")
        _wait_for_line(lines, "EOD RUN OK")
        _wait_for_line(lines, "next fire")  # printed after the state write — no read race
    finally:
        _stop(proc)

    assert len(log.read_text().splitlines()) == 2, "one failed fire, one same-day retry"
    saved = json.loads(state.read_text())
    assert saved["slot_date"] == dt.datetime.now(KOLKATA).date().isoformat()
    assert saved["rc"] == 0, "the state records the recovered outcome"
    assert saved["missed"] is True, "the slot was missed — that fact survives the catch-up"
    assert saved["catch_up"] is False, "the catch-up attempt is over"


def test_a_restart_retries_todays_failed_slot_instead_of_waiting_for_tomorrow(tmp_path):
    controller, log = _fake_controller(tmp_path, [0])
    state = tmp_path / "eod-state.json"
    state.write_text(json.dumps({
        "slot_date": dt.datetime.now(KOLKATA).date().isoformat(),
        "last_attempt_at": "2026-09-29T10:00:00+00:00",
        "rc": 1, "missed": True, "catch_up": True,
    }) + "\n")
    proc, lines = _start(tmp_path,
                         "--at", _passed_at(), "--zone", "Asia/Kolkata",
                         "--controller", str(controller),
                         "--heartbeat", str(tmp_path / "hb"),
                         "--state", str(state),
                         "--max-retries", "0", "--retry-delay", "0",
                         "--retry-delay-sec", "0.05", "--sleep-chunk", "0.05")
    try:
        _wait_for_line(lines, "retrying today's")
        _wait_for_line(lines, "EOD RUN OK")
        _wait_for_line(lines, "next fire")  # printed after the state write — no read race
    finally:
        _stop(proc)

    assert len(log.read_text().splitlines()) == 1
    saved = json.loads(state.read_text())
    assert saved["rc"] == 0 and saved["catch_up"] is False
    assert saved["missed"] is True


def test_a_slot_that_already_succeeded_is_not_fired_again(tmp_path):
    """The loop must wait for tomorrow after today's success — not re-fire on every iteration."""
    controller, log = _fake_controller(tmp_path, [0])
    state = tmp_path / "eod-state.json"
    state.write_text(json.dumps({
        "slot_date": dt.datetime.now(KOLKATA).date().isoformat(),
        "last_attempt_at": "2026-09-29T10:00:00+00:00",
        "rc": 0, "missed": False, "catch_up": False,
    }) + "\n")
    proc, lines = _start(tmp_path,
                         "--at", _passed_at(), "--zone", "Asia/Kolkata",
                         "--controller", str(controller),
                         "--heartbeat", str(tmp_path / "hb"),
                         "--state", str(state),
                         "--max-retries", "0", "--retry-delay", "0",
                         "--retry-delay-sec", "0.05", "--sleep-chunk", "0.05")
    try:
        _wait_for_line(lines, "next fire")
        time.sleep(0.3)  # a few sleep-chunk periods: a re-fire would show here
    finally:
        _stop(proc)

    assert not log.exists() or log.read_text().strip() == "", \
        "today's slot already succeeded; the scheduler must only wait"
