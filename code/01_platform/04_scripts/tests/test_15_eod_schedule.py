"""Hermetic tests for the EOD trigger (SCH-23 / operational §6.13).

No JVM, no Fluss, no network: the controller is a fake shell script that records its invocations, so
what is tested is the *schedule* — when it fires, that a failed process is retried, and that the
health signal a container needs cannot be faked by an empty file.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

SCRIPT = Path(__file__).resolve().parents[1] / "eod_schedule.py"
SPEC = importlib.util.spec_from_file_location("eod_schedule", SCRIPT)
eod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(eod)

KOLKATA = ZoneInfo("Asia/Kolkata")
NEW_YORK = ZoneInfo("America/New_York")


def _fake_controller(directory: Path, codes: list[int]) -> Path:
    """A controller that exits with the Nth code from `codes` (last code repeats) and records calls."""
    log = directory / "calls.log"
    path = directory / "fake_controller.py"
    path.write_text(
        "import os, sys\n"
        f"log = {str(log)!r}\n"
        f"codes = {codes!r}\n"
        "with open(log, 'a') as fh:\n"
        "    fh.write(' '.join(sys.argv[1:]) + '\\n')\n"
        "n = sum(1 for _ in open(log))\n"
        "sys.exit(codes[min(n, len(codes)) - 1] if codes else 0)\n"
    )
    return path, log


def _run(tmp_path: Path, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    full = {**os.environ, **(env or {})}
    return subprocess.run([sys.executable, str(SCRIPT), *args],
                          capture_output=True, text=True, env=full, cwd=str(tmp_path))


# ── the fire time ────────────────────────────────────────────────────────────

def test_next_fire_is_the_next_local_occurrence_strictly_after_now():
    at = dt.time(23, 30)
    before = dt.datetime(2026, 9, 21, 17, 0, tzinfo=dt.timezone.utc)   # 22:30 in Kolkata
    fire = eod.next_fire(before, at, KOLKATA)
    assert fire.astimezone(dt.timezone.utc) == dt.datetime(2026, 9, 21, 18, 0, tzinfo=dt.timezone.utc)
    assert fire.utcoffset() == dt.timedelta(hours=5, minutes=30)


def test_next_fire_never_returns_the_instant_it_was_given():
    at = dt.time(23, 30)
    exact = dt.datetime(2026, 9, 21, 18, 0, tzinfo=dt.timezone.utc)
    fire = eod.next_fire(exact, at, KOLKATA)
    assert fire > exact, "a fire that just happened must not be re-selected, or the loop spins"
    assert fire.astimezone(dt.timezone.utc) == dt.datetime(2026, 9, 22, 18, 0, tzinfo=dt.timezone.utc)


def test_next_fire_advances_about_a_day_across_a_dst_boundary():
    # 2026-03-08 02:00 America/New_York jumps to 03:00; a 02:30 EOD does not exist that day.
    at = dt.time(2, 30)
    instants = [dt.datetime(2026, 3, 7, 6, 0, tzinfo=dt.timezone.utc)
                + dt.timedelta(hours=h) for h in range(0, 96)]
    fires = [eod.next_fire(t, at, NEW_YORK) for t in instants]
    assert all(f > t for f, t in zip(fires, instants))
    # Successive fire times step forward, never repeat, and never skip more than a day.
    for first, second in zip(fires, fires[1:]):
        if second <= first:
            continue  # a later `now` may select the same upcoming fire — that is correct
    unique = sorted({f for f in fires})
    gaps = [(b - a).total_seconds() / 3600 for a, b in zip(unique, unique[1:])]
    assert gaps and all(23 <= gap <= 25 for gap in gaps), gaps


def test_the_loop_would_fire_once_per_day_over_a_week():
    at = dt.time(23, 30)
    cursor = dt.datetime(2026, 9, 21, 6, 0, tzinfo=dt.timezone.utc)
    fires = []
    for _ in range(7):
        cursor = eod.next_fire(cursor, at, KOLKATA)
        fires.append(cursor)
    assert len(set(fires)) == 7
    assert all((b - a).total_seconds() == 86400 for a, b in zip(fires, fires[1:]))


def test_a_malformed_time_or_unknown_zone_is_a_configuration_error():
    assert _run(Path("/tmp"), "--at", "25:00", "--dry-run").returncode == 2
    assert _run(Path("/tmp"), "--at", "2330", "--dry-run").returncode == 2
    assert _run(Path("/tmp"), "--zone", "Mars/Olympus", "--dry-run").returncode == 2


# ── firing ───────────────────────────────────────────────────────────────────

def test_once_invokes_the_controller_and_reports_success(tmp_path):
    controller, log = _fake_controller(tmp_path, [0])
    hb = tmp_path / "hb"
    r = _run(tmp_path, "--once", "--controller", str(controller), "--heartbeat", str(hb))
    assert r.returncode == 0, r.stderr
    assert log.read_text().strip() == "run", "the default controller argument is `run`"
    assert "EOD RUN OK" in r.stdout
    assert hb.is_file() and eod.heartbeat_age(hb) is not None


def test_controller_arguments_are_passed_through_in_order(tmp_path):
    controller, log = _fake_controller(tmp_path, [0])
    r = _run(tmp_path, "--once", "--controller", str(controller), "--heartbeat", str(tmp_path / "hb"),
             "--controller-arg", "run", "--controller-arg=--offload", "--controller-arg", "lake")
    assert r.returncode == 0, r.stderr
    assert log.read_text().strip() == "run --offload lake"


def test_a_failing_controller_is_retried_and_the_failure_is_not_hidden(tmp_path):
    controller, log = _fake_controller(tmp_path, [1])
    r = _run(tmp_path, "--once", "--controller", str(controller), "--heartbeat", str(tmp_path / "hb"),
             "--max-retries", "2", "--retry-delay", "0.01")
    assert r.returncode == 1
    assert len(log.read_text().splitlines()) == 3, "the first attempt plus two retries"
    assert "EOD RUN FAILED" in r.stderr
    assert "is not\nverified" in r.stderr or "not verified" in r.stderr


def test_a_retry_that_succeeds_ends_clean(tmp_path):
    controller, log = _fake_controller(tmp_path, [1, 1, 0])
    r = _run(tmp_path, "--once", "--controller", str(controller), "--heartbeat", str(tmp_path / "hb"),
             "--max-retries", "3", "--retry-delay", "0.01")
    assert r.returncode == 0, r.stderr
    assert len(log.read_text().splitlines()) == 3
    assert "EOD RUN OK" in r.stdout


def test_a_missing_controller_is_refused_before_any_fire(tmp_path):
    r = _run(tmp_path, "--once", "--controller", str(tmp_path / "absent.py"))
    assert r.returncode == 2
    assert "not found" in r.stderr


def test_dry_run_prints_the_plan_and_never_fires(tmp_path):
    controller, log = _fake_controller(tmp_path, [0])
    r = _run(tmp_path, "--dry-run", "--days", "3", "--at", "23:30", "--zone", "Asia/Kolkata",
             "--controller", str(controller))
    assert r.returncode == 0, r.stderr
    assert r.stdout.count("next fire") == 3
    assert not log.exists(), "--dry-run must not touch the controller"


def test_the_environment_can_configure_the_schedule(tmp_path):
    r = _run(tmp_path, "--dry-run", "--days", "1",
             env={"EOD_AT": "15:45", "EOD_ZONE": "America/New_York"})
    assert r.returncode == 0, r.stderr
    assert "15:45" in r.stdout and "New_York" in r.stdout


# ── the health signal a container uses ───────────────────────────────────────

def test_a_missing_heartbeat_is_unhealthy(tmp_path):
    r = _run(tmp_path, "--check-heartbeat", "--heartbeat", str(tmp_path / "absent"))
    assert r.returncode == 1
    assert "missing or unreadable" in r.stdout


def test_a_stale_heartbeat_is_unhealthy_and_a_fresh_one_is_healthy(tmp_path):
    hb = tmp_path / "hb"
    hb.write_text((dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=600)).isoformat() + "\n")
    stale = _run(tmp_path, "--check-heartbeat", "--heartbeat", str(hb), "--max-age", "120")
    assert stale.returncode == 1 and "stale" in stale.stdout
    hb.write_text(dt.datetime.now(dt.timezone.utc).isoformat() + "\n")
    fresh = _run(tmp_path, "--check-heartbeat", "--heartbeat", str(hb), "--max-age", "120")
    assert fresh.returncode == 0 and "fresh" in fresh.stdout


def test_an_unparseable_heartbeat_is_unhealthy_rather_than_fresh(tmp_path):
    hb = tmp_path / "hb"
    hb.write_text("not a timestamp\n")
    r = _run(tmp_path, "--check-heartbeat", "--heartbeat", str(hb))
    assert r.returncode == 1
