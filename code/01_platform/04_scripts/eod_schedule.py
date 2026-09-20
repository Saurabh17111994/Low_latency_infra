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

Exit codes: the controller's own code for --once (0 = clean), 0 for --dry-run, 0 fresh / 1 stale for
--check-heartbeat, 2 for a usage or configuration error.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

DEFAULT_AT = "23:30"
DEFAULT_ZONE = "Asia/Kolkata"
DEFAULT_HEARTBEAT = "/tmp/eod-scheduler-heartbeat"
DEFAULT_SLEEP_CHUNK_SEC = 60.0


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


def run_controller(controller: Path, controller_args: list[str]) -> int:
    cmd = [sys.executable, str(controller)] + controller_args
    print(f"eod-schedule: run {controller_args[0] if controller_args else ''} -> {' '.join(cmd)}", flush=True)
    try:
        proc = subprocess.run(cmd, env=os.environ.copy())
    except OSError as exc:
        print(f"eod-schedule: ERROR cannot start the controller: {exc}", file=sys.stderr)
        return 2
    return proc.returncode


def fire(controller: Path, controller_args: list[str], retries: int, delay: float,
         heartbeat: Path) -> int:
    """Run the controller, retrying a failed process with exponential backoff.

    Retrying the *process* is not a substitute for the controller's own backoff: a crash before it
    can record state leaves nothing to resume from, and that is exactly the case this covers.
    """
    write_heartbeat(heartbeat)
    rc = run_controller(controller, controller_args)
    attempt = 0
    while rc != 0 and attempt < retries:
        attempt += 1
        wait = delay * (2 ** (attempt - 1))
        print(f"eod-schedule: rc={rc}, retry {attempt}/{retries} in {wait:.0f}s", flush=True)
        time.sleep(wait)
        rc = run_controller(controller, controller_args)
    write_heartbeat(heartbeat)
    if rc == 0:
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
    p.add_argument("--controller-arg", action="append", default=[], dest="controller_args",
                   help="argument passed through to the controller (repeatable; default: run). "
                        "A value that starts with a dash needs the = form: --controller-arg=--offload")
    p.add_argument("--once", action="store_true", help="fire once now and exit (rehearsal)")
    p.add_argument("--dry-run", action="store_true", help="print the next fire times and exit")
    p.add_argument("--days", type=int, default=3, help="how many fire times --dry-run prints")
    p.add_argument("--heartbeat", default=os.environ.get("EOD_HEARTBEAT", DEFAULT_HEARTBEAT),
                   help="heartbeat file (env EOD_HEARTBEAT)")
    p.add_argument("--check-heartbeat", action="store_true",
                   help="report whether the heartbeat is fresh, then exit (container health)")
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
            print(f"eod-schedule: heartbeat {heartbeat} missing or unreadable")
            return 1
        if age > args.max_age:
            print(f"eod-schedule: heartbeat {heartbeat} is stale ({age:.0f}s > {args.max_age:.0f}s)")
            return 1
        print(f"eod-schedule: heartbeat {heartbeat} is fresh ({age:.0f}s)")
        return 0

    tz = zone(args.zone)
    at = parse_at(args.at)
    controller = Path(args.controller)
    controller_args = args.controller_args or ["run"]

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
        return fire(controller, controller_args, args.max_retries, args.retry_delay, heartbeat)

    if not controller.is_file():
        print(f"eod-schedule: controller {controller} not found", file=sys.stderr)
        return 2

    print(f"eod-schedule: scheduling {controller_args[0]} at {args.at} {args.zone}; "
          f"heartbeat {heartbeat}; pid {os.getpid()}", flush=True)
    write_heartbeat(heartbeat, now)
    while True:
        target = next_fire(dt.datetime.now(dt.timezone.utc), at, tz)
        while True:
            remaining = (target - dt.datetime.now(dt.timezone.utc)).total_seconds()
            if remaining <= 0:
                break
            time.sleep(min(remaining, args.sleep_chunk))
            write_heartbeat(heartbeat)
        # Recompute after the fire: a clock jump or a suspended container must not re-select it.
        fire(controller, controller_args, args.max_retries, args.retry_delay, heartbeat)


if __name__ == "__main__":
    sys.exit(main())
