#!/usr/bin/env python3
"""EOD trigger (SCH-23 / operational §6.13): fire the EOD controller once per trading day.

The controller (`eod_controller.py`, a one-shot) already owns manifest creation, verification,
retry/backoff, retention extension and reconciliation, and keeps its state and lease in Fluss.
Nothing in production invoked it — no cron, no timer, no service — so the manifest lifecycle never
started. This is the missing *when*: it sleeps until the configured local time in the trading zone,
runs the controller, and reports the outcome. It decides only the timing; idempotency stays in the
controller's state table and lease, so a second fire on the same day is harmless by construction.

  once (rehearsal):   python3 eod_schedule.py --once
  the plan:           python3 eod_schedule.py --dry-run --days 3
  the service loop:   python3 eod_schedule.py --at 23:30 --zone Asia/Kolkata
  container health:   python3 eod_schedule.py --check-heartbeat --max-age 120

Runner: host python by default; the golden VM sets `EOD_RUNNER=compose` (`.env.vm`), which runs the
controller in the one-shot `eod-controller` compose service — no host JDK or m2 repo needed.

Exit codes: the controller's own code for --once (0 = clean), 0 for --dry-run, 0 fresh / 1 stale for
--check-heartbeat, 2 for a usage or configuration error.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

DEFAULT_AT = "23:30"
DEFAULT_ZONE = "Asia/Kolkata"
DEFAULT_HEARTBEAT = "/tmp/eod-scheduler-heartbeat"
DEFAULT_SLEEP_CHUNK_SEC = 60.0
DEFAULT_RETRY_DELAY_SEC = 900.0


def _die(message: str) -> None:
    """A configuration error: say what is wrong and exit 2, the code argparse uses.

    `SystemExit(2, "msg")` is not this — Python prints the tuple and exits 1.
    """
    print(f"eod-schedule: {message}", file=sys.stderr)
    raise SystemExit(2)


def parse_at(text: str) -> dt.time:
    """`HH:MM` (24h) to a time. Anything else is a configuration error, not a guess."""
    try:
        hour, minute = text.split(":")
        value = dt.time(int(hour), int(minute))
    except ValueError as exc:
        _die(f"--at must be HH:MM (24-hour), got {text!r}: {exc}")
    return value


def zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except Exception as exc:  # ZoneInfoNotFoundError and friends
        _die(f"--zone {name!r} is not available (is tzdata installed?): {exc}")


def next_fire(now: dt.datetime, at: dt.time, tz: ZoneInfo) -> dt.datetime:
    """The first instant strictly after `now` whose local time in `tz` is `at`.

    Strictly-after matters: a fire that just happened must not be re-selected, or the loop would
    spin. Computed through the zone so a DST shift cannot make it fire twice or skip a day.
    """
    local = now.astimezone(tz)
    candidate = local.replace(hour=at.hour, minute=at.minute, second=0, microsecond=0)
    if candidate <= local:
        candidate = (local + dt.timedelta(days=1)).replace(
            hour=at.hour, minute=at.minute, second=0, microsecond=0
        )
    return candidate


def write_heartbeat(path: Path, now: dt.datetime | None = None) -> None:
    stamp = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(stamp.isoformat() + "\n")
    except OSError as exc:
        # A missing heartbeat is a health problem, never a reason to stop the schedule.
        print(f"eod-schedule: WARN cannot write heartbeat {path}: {exc}", file=sys.stderr)


def heartbeat_age(path: Path, now: dt.datetime | None = None) -> float | None:
    """Seconds since the last heartbeat, or None when there is no readable stamp."""
    try:
        stamp = dt.datetime.fromisoformat(path.read_text().strip())
    except (OSError, ValueError):
        return None
    return ((now or dt.datetime.now(dt.timezone.utc)) - stamp).total_seconds()


def write_last_run(path: Path | None, zone_name: str,
                   now: dt.datetime | None = None) -> None:
    """Record the moment of a successful controller run.

    The ephemeral-VM stop gate reads this file to prove the day was archived
    before the disk dies. Disabled unless EOD_LAST_RUN_FILE / --last-run names
    it; a failed write is a warning, never a run failure. The stamp carries the
    trading zone, so "today" means the trading day, not the host's clock.
    """
    if path is None:
        return
    stamp = now or dt.datetime.now(zone(zone_name))
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(stamp.isoformat() + "\n")
    except OSError as exc:
        print(f"eod-schedule: WARN cannot write last-run {path}: {exc}", file=sys.stderr)


def read_last_run(path: Path) -> dt.datetime | None:
    """The last-run stamp, or None when missing/unreadable (unverified, not clean)."""
    try:
        return dt.datetime.fromisoformat(path.read_text().strip())
    except (OSError, ValueError):
        return None


@dataclasses.dataclass
class EodState:
    """M2-2: durable per-slot outcome, so a missed/failed slot is caught up.

    ``slot_date`` is the trading-zone date the record belongs to; ``rc`` is the
    last controller exit code (None = never attempted).
    """

    slot_date: str = ""
    last_attempt_at: str = ""
    rc: int | None = None
    missed: bool = False
    catch_up: bool = False


def read_state(path: Path | None) -> EodState:
    if path is None:
        return EodState()
    try:
        raw = json.loads(path.read_text())
        return EodState(
            slot_date=str(raw.get("slot_date", "")),
            last_attempt_at=str(raw.get("last_attempt_at", "")),
            rc=(None if raw.get("rc") is None else int(raw["rc"])),
            missed=bool(raw.get("missed", False)),
            catch_up=bool(raw.get("catch_up", False)),
        )
    except (OSError, ValueError, TypeError):
        return EodState()


def write_state(path: Path | None, state: EodState) -> None:
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(dataclasses.asdict(state)) + "\n")
    except OSError as exc:
        print(f"eod-schedule: WARN cannot write state {path}: {exc}", file=sys.stderr)


def catch_up_due(now: dt.datetime, at: dt.time, tz: ZoneInfo, state: EodState) -> bool:
    """M2-2: True when the latest passed slot has no success and it is still its day.

    Before today's local slot the scheduler waits (False). After it, False only
    when the state records rc==0 for that same date — a missed slot (the
    scheduler was down at the time) or a failed one (rc != 0) must run now, and
    the controller's lease/state keeps the re-fire idempotent.
    """
    local = now.astimezone(tz)
    slot = local.replace(hour=at.hour, minute=at.minute, second=0, microsecond=0)
    if slot > local:
        return False
    return not (state.slot_date == local.date().isoformat() and state.rc == 0)


def sleep_with_heartbeat(seconds: float, heartbeat: Path, chunk: float) -> None:
    """Sleep in chunks, refreshing the heartbeat — a long retry must not look dead."""
    remaining = seconds
    while remaining > 0:
        step = min(remaining, max(1.0, chunk))
        time.sleep(step)
        write_heartbeat(heartbeat)
        remaining -= step


def compose_command(compose_file: str, controller_args: list[str]) -> list[str]:
    """C3-3: the canonical compose form for the one-shot EOD service.

    ``DAY_COMPOSE`` (the Makefile's registered compose form, exported by
    ``make day``) wins when present; otherwise both env files are derived from
    the stack file's directory. The literals are separate list elements — the
    same shape the daily runner uses — and the stack file must exist before
    this is called.
    """
    override = os.environ.get("DAY_COMPOSE", "").strip()
    if override:
        base = shlex.split(override)
    else:
        stack = Path(compose_file)
        base = [
            "docker", "compose",
            "--env-file", str(stack.parent / ".env"),
            "--env-file", str(stack.parent / "secrets.env"),
            "-f", str(stack),
        ]
    return base + ["run", "--rm", "-T", "eod-controller"] + (controller_args or ["run"])


def run_controller(controller: Path, controller_args: list[str],
                   runner: str = "host", compose_file: str = "") -> int:
    """Run the controller once: host python (default) or the eod-controller compose service.

    The compose service exists for hosts without the JDK/m2 repo (the golden VM);
    timing, the heartbeat and the last-run stamp stay in this process either way.
    """
    if runner == "compose":
        stack = Path(compose_file)
        if not stack.is_file():
            print(f"eod-schedule: ERROR stack file not found: {stack} "
                  "(set EOD_COMPOSE_FILE or --compose-file)", file=sys.stderr)
            return 2
        cmd = compose_command(str(stack), controller_args)
    else:
        cmd = [sys.executable, str(controller)] + controller_args
    print(f"eod-schedule: run {controller_args[0] if controller_args else ''} -> {' '.join(cmd)}", flush=True)
    try:
        proc = subprocess.run(cmd, env=os.environ.copy())
    except OSError as exc:
        print(f"eod-schedule: ERROR cannot start the controller: {exc}", file=sys.stderr)
        return 2
    return proc.returncode


def fire(controller: Path, controller_args: list[str], retries: int, delay: float,
         heartbeat: Path, last_run: Path | None = None,
         zone_name: str = DEFAULT_ZONE, runner: str = "host",
         compose_file: str = "") -> int:
    """Run the controller, retrying a failed process with exponential backoff.

    Retrying the *process* is not a substitute for the controller's own backoff: a crash before it
    can record state leaves nothing to resume from, and that is exactly the case this covers.
    """
    write_heartbeat(heartbeat)
    rc = run_controller(controller, controller_args, runner, compose_file)
    attempt = 0
    while rc != 0 and attempt < retries:
        attempt += 1
        wait = delay * (2 ** (attempt - 1))
        print(f"eod-schedule: rc={rc}, retry {attempt}/{retries} in {wait:.0f}s", flush=True)
        time.sleep(wait)
        rc = run_controller(controller, controller_args, runner, compose_file)
    write_heartbeat(heartbeat)
    if rc == 0:
        write_last_run(last_run, zone_name)
        print("eod-schedule: EOD RUN OK", flush=True)
    else:
        print(f"eod-schedule: EOD RUN FAILED rc={rc} after {attempt} retr(ies) — the day is not "
              f"verified; the controller's lease/backoff owns the next attempt", file=sys.stderr)
    return rc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--at", default=os.environ.get("EOD_AT", DEFAULT_AT),
                   help=f"local EOD time, HH:MM (env EOD_AT; default {DEFAULT_AT})")
    p.add_argument("--zone", default=os.environ.get("EOD_ZONE", DEFAULT_ZONE),
                   help=f"trading zone (env EOD_ZONE; default {DEFAULT_ZONE})")
    p.add_argument("--controller", default=str(Path(__file__).resolve().parent / "eod_controller.py"),
                   help="the one-shot controller to invoke (default: the sibling eod_controller.py)")
    p.add_argument("--runner", choices=("host", "compose"),
                   default=os.environ.get("EOD_RUNNER", "host"),
                   help="how to execute the controller: host python (default) or the "
                        "eod-controller compose service (env EOD_RUNNER)")
    p.add_argument("--compose-file", default=os.environ.get(
                       "EOD_COMPOSE_FILE",
                       str(Path(__file__).resolve().parents[1] / "01_docker" / "docker-compose.yml")),
                   help="stack file for --runner compose (env EOD_COMPOSE_FILE)")
    p.add_argument("--controller-arg", action="append", default=[], dest="controller_args",
                   help="argument passed through to the controller (repeatable; default: run). "
                        "A value that starts with a dash needs the = form: --controller-arg=--offload")
    p.add_argument("--once", action="store_true", help="fire once now and exit (rehearsal)")
    p.add_argument("--dry-run", action="store_true", help="print the next fire times and exit")
    p.add_argument("--days", type=int, default=3, help="how many fire times --dry-run prints")
    p.add_argument("--heartbeat", default=os.environ.get("EOD_HEARTBEAT", DEFAULT_HEARTBEAT),
                   help="heartbeat file (env EOD_HEARTBEAT)")
    p.add_argument("--last-run", default=os.environ.get("EOD_LAST_RUN_FILE", ""),
                   help="record the last successful controller run here "
                        "(env EOD_LAST_RUN_FILE; empty disables)")
    p.add_argument("--state", default=os.environ.get("EOD_STATE_FILE", ""),
                   help="durable slot-state JSON for catch-up/retries "
                        "(env EOD_STATE_FILE; empty disables the state record)")
    p.add_argument("--retry-delay-sec", type=float,
                   default=float(os.environ.get("EOD_RETRY_DELAY_SEC",
                                                DEFAULT_RETRY_DELAY_SEC)),
                   help="same-day retry delay after a failed/missed slot, seconds "
                        "(env EOD_RETRY_DELAY_SEC)")
    p.add_argument("--check-heartbeat", action="store_true",
                   help="report whether the heartbeat is fresh, then exit (container health). "
                        "With --last-run it also fails when the latest passed slot has no "
                        "success stamp (EOD not archived vs scheduler dead)")
    p.add_argument("--max-age", type=float, default=120.0,
                   help="seconds a heartbeat may age before --check-heartbeat calls it stale")
    p.add_argument("--max-retries", type=int, default=3, help="process-level retries after a failure")
    p.add_argument("--retry-delay", type=float, default=30.0, help="first retry delay, seconds (doubles)")
    p.add_argument("--sleep-chunk", type=float, default=DEFAULT_SLEEP_CHUNK_SEC,
                   help="longest single sleep; the fire time is recomputed after each chunk")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    heartbeat = Path(args.heartbeat)
    now = dt.datetime.now(dt.timezone.utc)

    if args.check_heartbeat:
        age = heartbeat_age(heartbeat, now)
        if age is None:
            print(f"eod-schedule: heartbeat {heartbeat} missing or unreadable "
                  f"— scheduler dead")
            return 1
        if age > args.max_age:
            print(f"eod-schedule: heartbeat {heartbeat} is stale ({age:.0f}s > "
                  f"{args.max_age:.0f}s) — scheduler dead or wedged")
            return 1
        if args.last_run.strip():
            # M2-2: with a last-run file, freshness alone is not enough — a live
            # scheduler that never archived the day must read unhealthy too.
            check_tz = zone(args.zone)
            check_at = parse_at(args.at)
            local = now.astimezone(check_tz)
            slot = local.replace(hour=check_at.hour, minute=check_at.minute,
                                 second=0, microsecond=0)
            stamp = read_last_run(Path(args.last_run))
            if slot <= local and (stamp is None
                                  or stamp.astimezone(check_tz).date() != local.date()):
                print(f"eod-schedule: heartbeat is fresh but the {args.at} {args.zone} "
                      f"slot for {local.date()} has no success stamp in {args.last_run} "
                      f"— EOD not archived (scheduler alive)")
                return 1
        print(f"eod-schedule: heartbeat {heartbeat} is fresh ({age:.0f}s)")
        return 0

    tz = zone(args.zone)
    at = parse_at(args.at)
    controller = Path(args.controller)
    controller_args = args.controller_args or ["run"]
    last_run = Path(args.last_run) if args.last_run.strip() else None

    if args.dry_run:
        cursor = now
        for _ in range(max(1, args.days)):
            cursor = next_fire(cursor, at, tz)
            print(f"eod-schedule: next fire {cursor.isoformat()} ({cursor.tzinfo}) "
                  f"= {cursor.astimezone(dt.timezone.utc).isoformat()}Z")
        return 0

    if args.once:
        if not controller.is_file():
            print(f"eod-schedule: controller {controller} not found", file=sys.stderr)
            return 2
        return fire(controller, controller_args, args.max_retries, args.retry_delay,
                    heartbeat, last_run, args.zone, args.runner, args.compose_file)

    if not controller.is_file():
        print(f"eod-schedule: controller {controller} not found", file=sys.stderr)
        return 2

    state_path = Path(args.state) if args.state.strip() else None
    state = read_state(state_path)
    print(f"eod-schedule: scheduling {controller_args[0]} at {args.at} {args.zone}; "
          f"heartbeat {heartbeat}; state {state_path or 'disabled'}; pid {os.getpid()}",
          flush=True)
    write_heartbeat(heartbeat, now)
    while True:
        loop_now = dt.datetime.now(dt.timezone.utc)
        if catch_up_due(loop_now, at, tz, state):
            # M2-2: a slot that passed with no success (the scheduler was down at
            # the time, or the fire failed) runs now — the controller's lease/state
            # makes the extra fire idempotent.
            local_date = loop_now.astimezone(tz).date().isoformat()
            first_attempt = state.slot_date != local_date
            state.slot_date = local_date
            state.last_attempt_at = loop_now.isoformat()
            if first_attempt:
                state.missed = True
            state.catch_up = True
            if first_attempt:
                print(f"eod-schedule: CATCH-UP — the {args.at} {args.zone} slot for "
                      f"{local_date} has no success record; firing now", flush=True)
            else:
                print(f"eod-schedule: retrying today's {args.at} slot "
                      f"(last rc={state.rc})", flush=True)
            rc = fire(controller, controller_args, args.max_retries, args.retry_delay,
                      heartbeat, last_run, args.zone, args.runner, args.compose_file)
            state.rc = rc
            if rc == 0:
                # The slot WAS missed — keep that fact; catch_up only says the
                # catch-up attempt is over.
                state.catch_up = False
                write_state(state_path, state)
                continue
            write_state(state_path, state)
            print(f"eod-schedule: slot {local_date} still failed (rc={rc}); same-day "
                  f"retry in {args.retry_delay_sec:.0f}s", flush=True)
            sleep_with_heartbeat(args.retry_delay_sec, heartbeat, args.sleep_chunk)
            continue
        target = next_fire(loop_now, at, tz)
        print(f"eod-schedule: next fire {target.isoformat()} ({target.tzinfo})", flush=True)
        while True:
            remaining = (target - dt.datetime.now(dt.timezone.utc)).total_seconds()
            if remaining <= 0:
                break
            time.sleep(min(remaining, args.sleep_chunk))
            write_heartbeat(heartbeat)
        # Recompute after the fire: a clock jump or a suspended container must not re-select it.
        state.slot_date = target.astimezone(tz).date().isoformat()
        state.last_attempt_at = dt.datetime.now(dt.timezone.utc).isoformat()
        state.missed = False
        state.catch_up = False
        rc = fire(controller, controller_args, args.max_retries, args.retry_delay,
                  heartbeat, last_run, args.zone, args.runner, args.compose_file)
        state.rc = rc
        write_state(state_path, state)
        if rc != 0:
            print(f"eod-schedule: slot {state.slot_date} failed (rc={rc}); same-day retry "
                  f"in {args.retry_delay_sec:.0f}s", flush=True)
            sleep_with_heartbeat(args.retry_delay_sec, heartbeat, args.sleep_chunk)


if __name__ == "__main__":
    sys.exit(main())
