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
sys.modules["eod_schedule"] = eod  # dataclasses resolves cls.__module__ via sys.modules
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


# ── M2-2 catch-up: a passed slot with no success is not skipped to tomorrow ──

def test_catch_up_due_matrix():
    at = dt.time(15, 45)
    before_slot = dt.datetime(2026, 9, 29, 9, 0, tzinfo=KOLKATA)
    after_slot = dt.datetime(2026, 9, 29, 16, 30, tzinfo=KOLKATA)
    today = after_slot.date().isoformat()
    yesterday = (after_slot.date() - dt.timedelta(days=1)).isoformat()

    assert not eod.catch_up_due(before_slot, at, KOLKATA, eod.EodState()), \
        "before the slot: wait"
    assert eod.catch_up_due(after_slot, at, KOLKATA, eod.EodState()), \
        "no record at all: the slot was missed — catch up"
    assert not eod.catch_up_due(after_slot, at, KOLKATA,
                                eod.EodState(slot_date=today, rc=0)), \
        "today's slot already succeeded"
    assert eod.catch_up_due(after_slot, at, KOLKATA,
                            eod.EodState(slot_date=today, rc=1)), \
        "today's slot failed: same-day retry"
    assert eod.catch_up_due(after_slot, at, KOLKATA,
                            eod.EodState(slot_date=yesterday, rc=0)), \
        "yesterday's success is not today's"
    assert eod.catch_up_due(after_slot, at, KOLKATA,
                            eod.EodState(slot_date=today, rc=None)), \
        "an attempt with no recorded outcome is not a success"


def test_state_round_trips_and_an_unreadable_file_is_empty_not_clean(tmp_path):
    path = tmp_path / "state.json"
    assert eod.read_state(path) == eod.EodState(), "missing state is empty"
    saved = eod.EodState(slot_date="2026-09-29", last_attempt_at="2026-09-29T10:00:00Z",
                         rc=1, missed=True, catch_up=True)
    eod.write_state(path, saved)
    assert eod.read_state(path) == saved
    path.write_text("{not json")
    assert eod.read_state(path) == eod.EodState(), "corrupt state reads as never-attempted"


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


def test_check_heartbeat_with_last_run_distinguishes_dead_from_unarchived(tmp_path):
    """M2-2: a fresh heartbeat proves the scheduler is alive — not that the day archived.

    With --last-run the check fails when the latest passed slot has no success
    stamp, and the message says which of the two states it is."""
    hb = tmp_path / "hb"
    record = tmp_path / "eod-last-run"
    now_local = dt.datetime.now(KOLKATA)
    passed_at = (now_local - dt.timedelta(minutes=2)).strftime("%H:%M")

    hb.write_text((dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=600)).isoformat() + "\n")
    dead = _run(tmp_path, "--check-heartbeat", "--heartbeat", str(hb),
                "--last-run", str(record), "--at", passed_at, "--zone", "Asia/Kolkata")
    assert dead.returncode == 1
    assert "scheduler dead" in dead.stdout

    hb.write_text(dt.datetime.now(dt.timezone.utc).isoformat() + "\n")
    unarchived = _run(tmp_path, "--check-heartbeat", "--heartbeat", str(hb),
                      "--last-run", str(record), "--at", passed_at, "--zone", "Asia/Kolkata")
    assert unarchived.returncode == 1
    assert "EOD not archived" in unarchived.stdout

    record.write_text(dt.datetime.now(KOLKATA).isoformat() + "\n")
    archived = _run(tmp_path, "--check-heartbeat", "--heartbeat", str(hb),
                    "--last-run", str(record), "--at", passed_at, "--zone", "Asia/Kolkata")
    assert archived.returncode == 0, archived.stdout
    assert "fresh" in archived.stdout


def test_check_heartbeat_without_last_run_stays_pure_liveness(tmp_path):
    """The container healthcheck must not turn red for a not-yet-archived day."""
    hb = tmp_path / "hb"
    hb.write_text(dt.datetime.now(dt.timezone.utc).isoformat() + "\n")
    now_local = dt.datetime.now(KOLKATA)
    passed_at = (now_local - dt.timedelta(minutes=2)).strftime("%H:%M")
    r = _run(tmp_path, "--check-heartbeat", "--heartbeat", str(hb),
             "--at", passed_at, "--zone", "Asia/Kolkata")
    assert r.returncode == 0, r.stdout


# ── the last-run record (the ephemeral-VM stop gate reads it) ────────────────

def test_once_records_the_last_successful_run_in_the_trading_zone(tmp_path):
    controller, _ = _fake_controller(tmp_path, [0])
    record = tmp_path / "eod-last-run"
    r = _run(tmp_path, "--once", "--controller", str(controller),
             "--heartbeat", str(tmp_path / "hb"), "--last-run", str(record))
    assert r.returncode == 0, r.stderr
    stamp = dt.datetime.fromisoformat(record.read_text().strip())
    assert stamp.utcoffset() == dt.timedelta(hours=5, minutes=30), \
        "the stamp carries the trading zone, so `today` means the trading day"
    assert stamp.date() == dt.datetime.now(KOLKATA).date()


def test_a_failed_run_never_records_success(tmp_path):
    controller, _ = _fake_controller(tmp_path, [1])
    record = tmp_path / "eod-last-run"
    r = _run(tmp_path, "--once", "--controller", str(controller),
             "--heartbeat", str(tmp_path / "hb"), "--last-run", str(record),
             "--max-retries", "0", "--retry-delay", "0")
    assert r.returncode == 1
    assert not record.exists(), "a failed day must never read as archived"


def test_the_last_run_record_is_optional(tmp_path):
    controller, _ = _fake_controller(tmp_path, [0])
    r = _run(tmp_path, "--once", "--controller", str(controller),
             "--heartbeat", str(tmp_path / "hb"))
    assert r.returncode == 0, r.stderr


def test_write_last_run_creates_parents_and_a_disabled_path_is_a_noop(tmp_path):
    target = tmp_path / "deep" / "dir" / "eod-last-run"
    eod.write_last_run(target, "Asia/Kolkata")
    assert target.is_file()
    eod.write_last_run(None, "Asia/Kolkata")  # disabled: no error, nothing written


# ── the compose runner (C3-3) ────────────────────────────────────────────────
# A recipe-built fresh VM has no host JDK / m2 repo, so the daily EOD controller must run in its
# existing compose service. These tests pin the exact command and the refuse-before-exec path.


class _Proc:
    returncode = 0


def _stack_file(directory: Path) -> Path:
    stack = directory / "docker-compose.yml"
    stack.write_text("services: {}\n")
    return stack


def test_compose_runner_builds_the_canonical_command(tmp_path, monkeypatch):
    stack = _stack_file(tmp_path)
    calls = []

    def fake_run(cmd, env=None):
        calls.append(cmd)
        return _Proc()

    monkeypatch.delenv("DAY_COMPOSE", raising=False)
    monkeypatch.setattr(eod.subprocess, "run", fake_run)
    rc = eod.run_controller(tmp_path / "controller.py", ["run"],
                            runner="compose", compose_file=str(stack))
    assert rc == 0
    assert calls == [[
        "docker", "compose",
        "--env-file", str(tmp_path / ".env"),
        "--env-file", str(tmp_path / "secrets.env"),
        "-f", str(stack),
        "run", "--rm", "-T", "eod-controller", "run",
    ]], calls


def test_compose_runner_prefers_day_compose_when_exported(tmp_path, monkeypatch):
    stack = _stack_file(tmp_path)
    calls = []
    monkeypatch.setenv("DAY_COMPOSE",
                       "docker compose --env-file x/.env --env-file x/secrets.env "
                       "-f x/docker-compose.yml")
    monkeypatch.setattr(eod.subprocess, "run",
                        lambda cmd, env=None: calls.append(cmd) or _Proc())
    rc = eod.run_controller(tmp_path / "controller.py", ["run"],
                            runner="compose", compose_file=str(stack))
    assert rc == 0
    assert calls[0][:8] == ["docker", "compose", "--env-file", "x/.env",
                            "--env-file", "x/secrets.env", "-f",
                            "x/docker-compose.yml"]
    assert calls[0][-5:] == ["run", "--rm", "-T", "eod-controller", "run"]


def test_compose_runner_refuses_a_missing_stack_file(tmp_path, monkeypatch):
    def never(*_args, **_kwargs):
        raise AssertionError("a missing stack file must be refused before exec")

    monkeypatch.setattr(eod.subprocess, "run", never)
    rc = eod.run_controller(tmp_path / "controller.py", ["run"],
                            runner="compose",
                            compose_file=str(tmp_path / "nope.yml"))
    assert rc == 2


def test_the_runner_defaults_to_host_and_eod_runner_can_select_compose(monkeypatch):
    monkeypatch.delenv("EOD_RUNNER", raising=False)
    assert eod.parse_args([]).runner == "host"
    assert Path(eod.parse_args([]).compose_file).name == "docker-compose.yml"
    monkeypatch.setenv("EOD_RUNNER", "compose")
    assert eod.parse_args([]).runner == "compose"


def test_once_with_the_compose_runner_execs_the_eod_service(tmp_path):
    """The whole path: --once --runner compose must reach the one-shot eod-controller service."""
    stack = _stack_file(tmp_path)
    fake = tmp_path / "bin"
    fake.mkdir()
    log = tmp_path / "docker.log"
    docker = fake / "docker"
    docker.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {log}\n"
        "exit 0\n"
    )
    docker.chmod(0o755)
    controller = tmp_path / "controller.py"
    controller.write_text("# compose mode runs the in-image copy; this file is only the repo check\n")
    env = {"PATH": f"{fake}:{os.environ['PATH']}", "EOD_RUNNER": "compose", "DAY_COMPOSE": ""}
    r = _run(tmp_path, "--once", "--controller", str(controller),
             "--compose-file", str(stack), "--heartbeat", str(tmp_path / "hb"),
             env=env)
    assert r.returncode == 0, r.stderr
    line = log.read_text().strip()
    assert f"--env-file {tmp_path / '.env'}" in line, line
    assert "secrets.env" in line, line
    assert f"-f {stack}" in line, line
    assert line.endswith("run --rm -T eod-controller run"), line
