#!/usr/bin/env python3
"""day_run.py — daily single-command platform runner (plan 2026-09-26, CHG-324).

Interface (plan §1, D6)::

    make day ARGS="start"    # morning: chain up, coordinated, verified, board
    make day ARGS="status"   # read-only board, safe any time
    make day ARGS="stop"     # graceful stop; checkpoints/volumes preserved

The runner composes what the platform already owns and never reimplements it:

  stack bring-up      -> ``make up`` (artifact verify + compose + catalog guard)
  SignalJob restore   -> ``make rollout-savepoint ARGS=RECOVERY_PATH=<path>``
  SignalJob fresh     -> ``make up`` with ``COMPUTE_SUBMIT_SIGNAL=1`` (launcher)
  stop                -> ``make down`` (volumes preserved; savepoint is separate)

All decisions live in pure functions (unit-tested by ``tests/test_day_run.py``);
I/O lives in :class:`Runner` (commands/HTTP) and :class:`Collector` (read-only
probes), both replaceable by fakes in tests.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

# --------------------------------------------------------------------------
# paths / constants
# --------------------------------------------------------------------------

ROOT = pathlib.Path(__file__).resolve().parents[3]
STACK = ROOT / "code" / "01_platform" / "01_docker"
ENV_FILE = STACK / ".env"
SECRETS_FILE = STACK / "secrets.env"
COMPOSE_FILE = STACK / "docker-compose.yml"
STACK_LOCK = ROOT / "code" / "01_platform" / "04_scripts" / "stack-lock.sh"
FLUSS_PROBE_SRC = (ROOT / "code" / "01_platform" / "04_scripts" / "fluss-probes"
                   / "FlussReadLagProbe.java")
CP_FILE = ROOT / "code" / "02_services" / "01_ingestion" / "target" / "cp.txt"
IMAGE_STALENESS_CHECK = (ROOT / "code" / "01_platform" / "04_scripts"
                         / "image_staleness_check.py")
EVIDENCE_ROOT = ROOT / "logs" / "day"

FULL_MANIFEST = (ROOT.parent / "Arrow_broker" / "instruments" / "cash_stocks"
                 / "NSE_CM_EQUITY.csv")
APPROVED_MANIFEST = (ROOT.parent / "Arrow_broker" / "instruments" / "cash_stocks"
                     / "NSE_CM_EQUITY (1024).csv")

DEFAULT_UNIVERSE = "full"
MAX_TOKENS_PER_CONNECTION = 1024
MAX_CONNECTIONS = 3
EXECUTION_PROFILE = "execution-t3"
SIGNAL_JOB_NAME = "signal-job-compute"
COMPANION_JOB_NAMES = ("Babysitter Positions observer", "Safety-halt consumer")
CHECKPOINT_INTERVAL_MS = 10_000  # compose pin (CHECKPOINT_INTERVAL_MS)
# F1 (2026-09-28): ceiling for the ready wait. The wait itself is state-based
# (all services are running + Fluss metadata readable + Flink reachable, two
# consecutive good polls), so the ceiling only bounds how long a stuck start may
# wait. MEASURED 2026-09-28 (off-hours cold-start drill, real feed, 58.6M raw
# records): the tablet needed 22 min 56 s to serve raw_table_1 - dominated by
# candle_live/candle_closed KV changelog replay ("No snapshot found" per bucket),
# so a 900 s ceiling went RED while recovery was still progressing. Default is
# 60 min (~2.6x margin); the structural fix is reducing KV replay/retention (F8),
# not a longer wait. Warm starts still exit in seconds.
DEFAULT_READY_TIMEOUT_S = 3600
FLINK_JOBMANAGER = "flink-jobmanager"
EXECUTION_SERVICES = ("execution-bridge", "execution-gateway", "nautilus")

# I8 error-budget vocabulary (ingestion + cluster logs; messages from
# IngestionService / the bridge wrapper).
ERROR_LOG_PATTERNS = ("FATAL", "BRIDGE_CRASH", "backpressure critical")
ERROR_LOG_SERVICES = ("ingestion", "flink-jobmanager",
                      "fluss-coordinator", "fluss-tablet")
# The board only needs a bounded error sample; a recovery can write far more
# than any reader should hold (2026-09-28 memory incident: 12 GB RSS).
MAX_LOG_ERROR_HITS = 50

MANIFEST_LOADED_RE = re.compile(r"manifest loaded \(instruments=(\d+)")

EXIT_OK = 0
EXIT_RED = 1
EXIT_USAGE = 2
EXIT_REFUSED = 3
EXIT_BUSY = 4

IST = ZoneInfo("Asia/Kolkata")


class Refusal(Exception):
    """Fail-closed preflight refusal (exit 3): message names the fix."""


# --------------------------------------------------------------------------
# data model
# --------------------------------------------------------------------------


@dataclasses.dataclass
class Universe:
    mode: str
    tokens: int
    connections: int
    approval: bool
    manifest: pathlib.Path
    deploy_env: str


@dataclasses.dataclass
class Check:
    ident: str
    label: str
    ok: bool
    detail: str
    recovery: str = ""
    pending: bool = False


@dataclasses.dataclass
class SignalJobDecision:
    action: str  # keep | restore | fresh | refuse | red
    path: str = ""
    reason: str = ""
    recovery: str = ""


@dataclasses.dataclass
class Facts:
    expected: list = dataclasses.field(default_factory=list)
    services: dict = dataclasses.field(default_factory=dict)
    jobs: list = dataclasses.field(default_factory=list)
    checkpoints: dict = dataclasses.field(default_factory=dict)
    fluss: dict = dataclasses.field(default_factory=dict)
    log_errors: list = dataclasses.field(default_factory=list)
    state: dict = dataclasses.field(default_factory=dict)
    container_env: dict = dataclasses.field(default_factory=dict)
    effective_tokens: int | None = None
    universe: dict = dataclasses.field(default_factory=dict)
    session: dict = dataclasses.field(default_factory=dict)
    notes: list = dataclasses.field(default_factory=list)


# --------------------------------------------------------------------------
# pure decisions
# --------------------------------------------------------------------------


def parse_env_file(path) -> dict:
    """KEY=VALUE env file parser (comments and blank lines skipped)."""
    out = {}
    try:
        text = pathlib.Path(path).read_text()
    except OSError:
        return out
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if value and not value.startswith(("'", '"')):
            value = value.split(" #", 1)[0].strip()
        out[key.strip()] = value.strip("'\"")
    return out


def effective_value(key: str, default: str = "") -> str:
    """Shell precedence, then the compose env file, then the default
    (mirrors compose ``${KEY:-default}`` interpolation)."""
    if key in os.environ and os.environ[key] != "":
        return os.environ[key]
    value = parse_env_file(ENV_FILE).get(key, "")
    return value if value != "" else default


def resolve_universe(mode: str, deploy_env: str, counts: dict) -> Universe:
    """P2-1: one selection expands to the existing compose knobs (plan §3.2).

    ``full`` = the unfiltered 2 433-row manifest on 3 approved dev sockets;
    ``approved`` = the N=1 approved 1 024-row manifest. Production stays
    single-socket (CHG-320) -- ``full`` is refused there.
    """
    mode = (mode or "").strip().lower()
    prod = deploy_env.strip().lower() in {"prod", "production"}
    if mode == "full":
        if prod:
            raise Refusal(
                "UNIVERSE=full (multi-socket) is dev-only; DEPLOYMENT_ENV="
                f"{deploy_env} keeps production single-socket (CHG-320). "
                "Use UNIVERSE=approved in production."
            )
        universe = Universe("full", counts.get("full", 0), MAX_CONNECTIONS,
                            True, FULL_MANIFEST, deploy_env)
    elif mode == "approved":
        universe = Universe("approved", counts.get("approved", 0), 1,
                            False, APPROVED_MANIFEST, deploy_env)
    else:
        raise Refusal(f"UNIVERSE must be 'full' or 'approved', got '{mode}'")

    if universe.tokens <= 0:
        raise Refusal(
            f"manifest has no instrument rows: {universe.manifest} "
            "(check Arrow_broker/instruments/cash_stocks/)"
        )
    capacity = universe.connections * MAX_TOKENS_PER_CONNECTION
    if universe.tokens > capacity:
        raise Refusal(
            f"manifest tokens={universe.tokens} exceed subscription capacity="
            f"{capacity} (ARROW_HFT_CONNECTIONS={universe.connections} x "
            f"ARROW_HFT_MAX_TOKENS_PER_CONNECTION={MAX_TOKENS_PER_CONNECTION}); "
            "raise ARROW_HFT_CONNECTIONS (up to 3) in dev, or use a manifest "
            "within capacity (CHG-323 preflight will enforce the same bound)"
        )
    if not universe.manifest.exists():
        raise Refusal(f"manifest file not found: {universe.manifest}")
    return universe


def count_manifest_rows(path) -> int:
    """Instrument rows = non-empty lines after the header (matches the
    profiler's ``tail -n +2 | grep -c .`` counting)."""
    count = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for index, line in enumerate(fh):
            if index == 0:
                continue
            if line.strip():
                count += 1
    return count


def posture_violations(env: dict) -> list:
    """P2-2: live-enablement flags the runner must never set or tolerate.

    ``EXECUTION_ENABLED`` must be false everywhere it is read; the bridge mode
    must be ``disabled`` (CHG-047). Blank values fall back to the fail-closed
    compose defaults, so they are not violations by themselves.
    """
    bad = []
    enabled = str(env.get("EXECUTION_ENABLED", "")).strip().lower()
    if enabled not in ("", "false"):
        bad.append(f"EXECUTION_ENABLED={env['EXECUTION_ENABLED']}")
    mode = str(env.get("EXECUTION_BRIDGE_MODE", "")).strip().lower()
    if mode not in ("", "disabled"):
        bad.append(f"EXECUTION_BRIDGE_MODE={env['EXECUTION_BRIDGE_MODE']}")
    return bad


def decide_signaljob(jobs: list, state: dict, allow_fresh: bool) -> SignalJobDecision:
    """Singleton rules (plan §3.3): keep / restore / refuse-fresh, never two."""
    signals = [j for j in jobs if j.get("name") == SIGNAL_JOB_NAME]
    if len(signals) > 1:
        ids = ", ".join(sorted(j.get("id", "?") for j in signals))
        return SignalJobDecision(
            "red",
            reason=f"{len(signals)} SignalJobs observed ({ids}) -- split-brain risk",
            recovery=("cancel the duplicates via Flink REST PATCH /jobs/<id>?mode=cancel "
                      "(runbook docs/06_operations/01-runbooks.md daily section), then re-run"),
        )
    if len(signals) == 1:
        job = signals[0]
        if job.get("state") == "RUNNING":
            return SignalJobDecision("keep", reason=f"RUNNING id={job.get('id')}")
        return SignalJobDecision(
            "red",
            reason=f"SignalJob exists but state={job.get('state')} (id={job.get('id')})",
            recovery=("inspect: make logs SVC=flink-jobmanager; cancel the failed job "
                      "(PATCH /jobs/<id>?mode=cancel) and re-run start to restore"),
        )
    if state.get("latest_savepoint"):
        return SignalJobDecision("restore", path=state["latest_savepoint"],
                                 reason="latest savepoint")
    if state.get("latest_checkpoint"):
        return SignalJobDecision("restore", path=state["latest_checkpoint"],
                                 reason="latest completed checkpoint")
    if allow_fresh:
        return SignalJobDecision("fresh", reason="ALLOW_FRESH=1")
    return SignalJobDecision(
        "refuse",
        reason="no running SignalJob and no savepoint/checkpoint state found",
        recovery=("bootstrap deliberately with ALLOW_FRESH=1 make day ARGS=\"start\" "
                  "(starts at LATEST in dev -- no backlog replay), or restore a state "
                  "path with make rollout-savepoint ARGS=\"RECOVERY_PATH=<path>\""),
    )


def _completed_checkpoint_ms(payload: dict) -> int | None:
    """End timestamp (ms) of the latest completed checkpoint.

    Flink's ``/jobs/<id>/checkpoints`` reports ``latest.completed`` with
    ``latest_ack_timestamp``/``trigger_timestamp`` -- not ``end_time`` (that
    field exists only on some Flink versions); accept all three, preferring
    the ack time.
    """
    latest = ((payload or {}).get("latest") or {}).get("completed") or {}
    for key in ("end_time", "latest_ack_timestamp", "trigger_timestamp"):
        value = latest.get(key)
        if value:
            return int(value)
    return None


def session_now(now: dt.datetime | None = None) -> dict:
    now = now.astimezone(IST) if now else dt.datetime.now(IST)
    minutes = now.hour * 60 + now.minute
    open_now = now.weekday() < 5 and (9 * 60 + 15) <= minutes <= (15 * 60 + 30)
    return {
        "open": open_now,
        "label": f"{'OPEN' if open_now else 'CLOSED'} ({now:%H:%M} IST)",
        "date": now.strftime("%Y-%m-%d"),
    }


def _checkpoint_age_ms(facts: Facts) -> int | None:
    for job in facts.jobs:
        if job.get("name") != SIGNAL_JOB_NAME:
            continue
        cp = facts.checkpoints.get(job.get("id"), {}) or {}
        sampled = cp.get("age_ms")
        if sampled is not None:
            return int(sampled)  # observed with the checkpoint read (P4-4 fix)
        end = cp.get("latest_completed_ms")
        if end:
            return int(time.time() * 1000) - int(end)
    return None


def evaluate(facts: Facts) -> list:
    """The nine coordination invariants (plan §1, D5). Pure over ``facts``."""
    checks = []
    running = {name for name, info in facts.services.items()
               if info.get("state") == "running"}
    expected = list(facts.expected)
    missing = [s for s in expected if s not in running]
    unhealthy = [s for s in expected
                 if s in running and facts.services.get(s, {}).get("health") == "unhealthy"]
    if expected:
        i1_ok, i1_detail = not missing, (
            f"{len(expected) - len(missing)}/{len(expected)} long-running services"
            + (f"; unhealthy: {','.join(unhealthy)}" if unhealthy else ""))
    else:
        i1_ok, i1_detail = False, "could not read the compose service contract (docker unavailable?)"
    checks.append(Check(
        "I1", "stack", i1_ok, i1_detail,
        recovery=("make up  (then: make logs SVC=<service>)"
                  if missing else "docker compose config (verify docker is up)"),
    ))

    raw = facts.fluss.get("raw", {})
    checks.append(Check(
        "I2", "fluss",
        bool(raw.get("ok")),
        (f"metadata probe raw_table_1 ok (log_end={raw.get('log_end')})" if raw.get("ok")
         else f"metadata probe failed: {raw.get('error', 'no sample')}"),
        recovery=("wait or re-run make up; if the tablet crash-loops see "
                  "code/01_platform/04_scripts/fluss-repair/repair-tablet.sh"),
    ))

    if not facts.session.get("open"):
        checks.append(Check("I3", "ingestion", True,
                            "PENDING - market closed (off-session); appends judged in session",
                            pending=True))
    else:
        delta = raw.get("delta")
        detail = (f"raw appends advancing (+{delta} rows in "
                  f"{facts.fluss.get('window_s', '?')}s)" if delta and delta > 0
                  else f"raw appends stalled (delta={delta if delta is not None else 'n/a'})")
        checks.append(Check(
            "I3", "ingestion", bool(delta and delta > 0), detail,
            recovery=("make logs SVC=ingestion  (check ARROW_FEED/token and "
                      "the broker connection; runbook daily section)"),
        ))

    candles = facts.fluss.get("candles", {})
    signals = facts.fluss.get("signals", {})
    if not facts.session.get("open"):
        checks.append(Check("I4", "data-flow", True,
                            "PENDING - market closed (off-session); downstream judged in session",
                            pending=True))
    else:
        c_delta = candles.get("delta") or 0
        s_delta = signals.get("delta") or 0
        moving = (candles.get("ok") and c_delta > 0) or (signals.get("ok") and s_delta > 0)
        checks.append(Check(
            "I4", "data-flow", moving,
            f"candles=+{c_delta} signals=+{s_delta}"
            + ("" if moving else " (no downstream movement)"),
            recovery=("make rollout-savepoint  (native SignalJob redeploy) "
                      "or make logs SVC=flink-jobmanager"),
        ))

    decision = decide_signaljob(facts.jobs, facts.state, allow_fresh=False)
    if decision.action == "keep":
        age = _checkpoint_age_ms(facts)
        threshold = 2 * CHECKPOINT_INTERVAL_MS
        ok = age is not None and age < threshold
        checks.append(Check(
            "I5", "signaljob", ok,
            (f"RUNNING exactly one; checkpoint age={age}ms (< {threshold}ms)"
             if ok else
             (f"RUNNING exactly one; checkpoint age="
              f"{age if age is not None else 'none'}ms (threshold {threshold}ms)")),
            recovery=("wait for the next checkpoint; persistent misses: "
                      "make rollout-savepoint (restore path)"),
        ))
    else:
        if decision.action == "restore":
            detail = (f"no running SignalJob; restorable from {decision.reason} "
                      f"({decision.path}) -- run start")
            recovery = ('make day ARGS="start"  (restores through '
                        'rollout-savepoint RECOVERY_PATH=<path>)')
        else:
            detail = f"not healthy: {decision.reason}"
            recovery = decision.recovery
        checks.append(Check("I5", "signaljob", False, detail, recovery=recovery))

    exec_running = [s for s in EXECUTION_SERVICES if s in running]
    env = facts.container_env or {}
    flags_ok = True
    flag_detail = []
    for svc in EXECUTION_SERVICES:
        if svc not in exec_running:
            continue
        svc_env = env.get(svc, {})
        bad = posture_violations(svc_env)
        if bad:
            flags_ok = False
            flag_detail.append(f"{svc}: {','.join(bad)}")
    halted = bool(facts.notes and "nautilus_halted" in facts.notes)
    exec_ok = len(exec_running) == len(EXECUTION_SERVICES) and flags_ok and halted
    if len(exec_running) != len(EXECUTION_SERVICES):
        detail = (f"profile up={len(exec_running)}/{len(EXECUTION_SERVICES)} "
                  "(not started)")
    elif flag_detail:
        detail = "live flags set: " + "; ".join(flag_detail)
    elif not halted:
        detail = ("profile up=3/3 flags=offline "
                  "(executor HALTED boot line not observed)")
    else:
        detail = "profile up=3/3 flags=offline; executor=HALTED(design)"
    checks.append(Check(
        "I6", "execution", exec_ok, detail,
        recovery=("COMPOSE_PROFILES=execution-t3 make up  (offline posture only; "
                  "never set EXECUTION_ENABLED=true -- docs/06_operations/08-live-readiness-gaps.md)"),
    ))

    live = []
    for svc, svc_env in env.items():
        for bad in posture_violations(svc_env):
            live.append(f"{svc}: {bad}")
    universe = facts.universe or {}
    expected_tokens = universe.get("tokens")
    manifest_ok = (facts.effective_tokens is None or expected_tokens is None
                   or facts.effective_tokens == expected_tokens)
    if not live and manifest_ok:
        effective = (facts.effective_tokens if facts.effective_tokens is not None
                     else "n/a (ingestion not running)")
        config_detail = (f"effective-manifest={effective}; expected {expected_tokens}; "
                         "live flags off")
        config_recovery = ""
    elif live:
        config_detail = "live-enablement flags in effective config: " + "; ".join(live)
        config_recovery = ("unset the flag in .env/shell (the runner never flips them); "
                           "docs/06_operations/08-live-readiness-gaps.md")
    else:
        config_detail = (f"effective-manifest={facts.effective_tokens} tokens does not "
                         f"match contract {expected_tokens}")
        config_recovery = ("check UNIVERSE / INSTRUMENT_MANIFEST_HOST_PATH and re-run start "
                           "(the container reports the manifest it actually loaded)")
    checks.append(Check(
        "I7", "config", not live and manifest_ok, config_detail,
        recovery=config_recovery,
    ))

    checks.append(Check(
        "I8", "errors", not facts.log_errors,
        ("no FATAL/BRIDGE_CRASH/backpressure-critical lines in the window"
         if not facts.log_errors else
         f"{len(facts.log_errors)} error line(s): {facts.log_errors[0][:120]}"),
        recovery="make logs SVC=ingestion  (runbook daily section maps each line to an action)",
    ))

    savepoint = facts.state.get("latest_savepoint")
    checkpoint = facts.state.get("latest_checkpoint")
    restorable = savepoint or checkpoint
    checks.append(Check(
        "I9", "restart", bool(restorable),
        (f"latest savepoint={savepoint}" if savepoint else
         (f"latest checkpoint={checkpoint}" if checkpoint else
          "no savepoint/checkpoint discovered for the next start")),
        recovery=("start the stack (a completed checkpoint appears within "
                  "2x CHECKPOINT_INTERVAL_MS) or run make rollout-savepoint"),
    ))
    return checks


# --------------------------------------------------------------------------
# board rendering
# --------------------------------------------------------------------------


def render(checks: list, facts: Facts, universe: dict | None = None) -> str:
    lines = []
    universe = universe or facts.universe or {}
    if universe:
        lines.append(
            f"[day] universe    {'OK':<5} mode={universe.get('mode')} "
            f"tokens={universe.get('tokens')} sockets={universe.get('connections')} "
            f"approval={'dev-only' if universe.get('approval') else 'n/a'}")
    for check in checks:
        if check.pending:
            status = "PENDING"
        else:
            status = "OK" if check.ok else "FAIL"
        lines.append(f"[day] {check.label:<11} {status:<5} {check.ident} {check.detail}")
        if not check.ok and not check.pending and check.recovery:
            lines.append(f"[day]                            recovery: {check.recovery}")
    reds = [c for c in checks if not c.ok and not c.pending]
    pendings = [c for c in checks if c.pending]
    lines.append(f"[day] session     INFO  market={facts.session.get('label', 'unknown')}")
    if reds:
        lines.append(f"[day] verdict     RED exit=1 · first failing invariant: "
                     f"{reds[0].ident} ({reds[0].label})")
    elif pendings:
        lines.append(f"[day] verdict     PENDING {len(checks) - len(pendings)}/"
                     f"{len(checks)} invariants · data predicates judged in session")
    else:
        lines.append(f"[day] verdict     GREEN {len(checks)}/{len(checks)} invariants "
                     f"· next: make day ARGS=\"status\"")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# I/O: command runner and read-only probes
# --------------------------------------------------------------------------


class Runner:
    """Thin command/HTTP runner. Tests replace it with a fake."""

    def __init__(self, out=None):
        self.out = out or sys.stdout
        self.commands = []

    def emit(self, line: str):
        print(line, file=self.out, flush=True)

    def run(self, argv, env=None, check=True, capture=True, timeout=1800):
        self.commands.append(list(argv))
        merged = dict(os.environ)
        if env:
            merged.update(env)
        proc = subprocess.run(argv, cwd=str(ROOT), env=merged, timeout=timeout,
                              stdout=subprocess.PIPE if capture else None,
                              stderr=subprocess.STDOUT if capture else None,
                              text=True)
        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, argv,
                                                output=proc.stdout)
        return proc.stdout or ""

    def compose(self, args, env=None, check=True, timeout=300):
        base = shlex.split(os.environ.get("DAY_COMPOSE", "")) or [
            "docker", "compose",
            "--env-file", str(ENV_FILE), "--env-file", str(SECRETS_FILE),
            "-f", str(COMPOSE_FILE),
        ]
        merged = {"COMPOSE_PROFILES": EXECUTION_PROFILE}
        if env:
            merged.update(env)
        return self.run(base + list(args), env=merged, check=check, timeout=timeout)

    def run_stream(self, argv, env=None):
        """Like ``run(capture=True)`` but yields stdout line by line.

        Log readers must tolerate policy-heavy outputs — a Fluss recovery can
        write GBs into the 15-minute window the board reads. Capturing that
        whole output is what ballooned the runner to 12 GB RSS + 16 GB swap on
        2026-09-28; this holds one line at a time. Stopping the generator early
        terminates the child (the log readers break on early hits)."""
        argv = [str(a) for a in argv]
        self.commands.append(list(argv))
        merged = dict(os.environ)
        if env:
            merged.update(env)
        proc = subprocess.Popen(argv, cwd=str(ROOT), env=merged,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                text=True)
        try:
            if proc.stdout is None:  # pragma: no cover - PIPE guarantees a stream
                return
            for line in proc.stdout:
                yield line.rstrip("\n")
        finally:
            if proc.stdout is not None:
                proc.stdout.close()
            proc.wait()

    def compose_lines(self, args, env=None):
        base = shlex.split(os.environ.get("DAY_COMPOSE", "")) or [
            "docker", "compose",
            "--env-file", str(ENV_FILE), "--env-file", str(SECRETS_FILE),
            "-f", str(COMPOSE_FILE),
        ]
        merged = {"COMPOSE_PROFILES": EXECUTION_PROFILE}
        if env:
            merged.update(env)
        yield from self.run_stream(base + list(args), env=merged)

    def make(self, target, args="", env=None, check=True, timeout=1800):
        argv = ["make", "-C", str(ROOT), target]
        if args:
            argv.append(f"ARGS={args}")
        return self.run(argv, env=env, check=check, timeout=timeout)

    def make_locked(self, target, args="", env=None, check=True, timeout=1800):
        argv = ["bash", str(STACK_LOCK), "make", "-C", str(ROOT), target]
        if args:
            argv.append(f"ARGS={args}")
        return self.run(argv, env=env, check=check, timeout=timeout)

    def http_json(self, url: str, timeout=10):
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def image_staleness(self) -> tuple:
        """Native content-stamp check (image_staleness_check.py): does a
        compose build image predate the source it packages? Read-only."""
        proc = subprocess.run(
            [sys.executable, str(IMAGE_STALENESS_CHECK), "--git-root", str(ROOT),
             "--compose", str(COMPOSE_FILE)],
            cwd=str(ROOT), capture_output=True, text=True, timeout=300)
        report = (proc.stdout or proc.stderr or "").strip()
        return proc.returncode != 0, report


class Collector:
    """Read-only probes. Every call is idempotent and never mutates state."""

    def __init__(self, runner: Runner, window_s: int | None = None):
        self.runner = runner
        self.window_s = int(os.environ.get("DAY_DATA_WINDOW_S", "20")) \
            if window_s is None else window_s
        self.jm_url = os.environ.get("DAY_JM_URL", "http://localhost:8081")

    # -- compose -----------------------------------------------------------
    def services(self) -> dict:
        try:
            raw = self.runner.compose(["ps", "--format", "json"], check=True)
        except subprocess.CalledProcessError as exc:
            return {}
        out = {}
        for line in (raw or "").splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            out[item.get("Service", "")] = {
                "state": item.get("State", ""),
                "health": item.get("Health", "") or "",
                "name": item.get("Name", ""),
            }
        return out

    def expected_services(self) -> list:
        """Long-running services: a restart policy marks most of them; the
        execution chain is part of the daily contract even where compose leaves
        ``restart`` unset (execution-bridge, nautilus). Known one-shots
        (ddl-apply, compute, eod-controller -- the local profile runs EOD as a
        one-shot; the scheduler is the production stack service) have
        ``restart: "no"`` and are excluded by the policy filter."""
        try:
            raw = self.runner.compose(["config", "--format", "json"], check=True)
            config = json.loads(raw)
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            return []
        expected = []
        for name, svc in (config.get("services") or {}).items():
            if svc.get("restart") in ("always", "unless-stopped", "on-failure"):
                expected.append(name)
            elif name in EXECUTION_SERVICES:
                expected.append(name)
        return sorted(set(expected))

    # -- flink -------------------------------------------------------------
    def jobs(self) -> list:
        try:
            data = self.runner.http_json(f"{self.jm_url}/jobs/overview")
        except (urllib.error.URLError, OSError, ValueError, TimeoutError):
            return []
        return [{"id": j.get("jid"), "name": j.get("name"), "state": j.get("state")}
                for j in data.get("jobs", [])]

    def checkpoints(self, jobs: list) -> dict:
        out = {}
        for job in jobs:
            if job.get("name") != SIGNAL_JOB_NAME:
                continue
            try:
                data = self.runner.http_json(
                    f"{self.jm_url}/jobs/{job['id']}/checkpoints")
            except (urllib.error.URLError, OSError, ValueError, TimeoutError):
                continue
            latest_ack = _completed_checkpoint_ms(data)
            out[job["id"]] = {
                "latest_completed_ms": latest_ack,
                # Age observed at read time (P4-4 fix): collection keeps
                # working for 20s+ after this point (Fluss probes), so an
                # evaluate-time recomputation reported a false I5 RED.
                "age_ms": (int(time.time() * 1000) - latest_ack) if latest_ack else None,
                "counts": data.get("counts", {}),
            }
        return out
    # -- filesystem state inside the JM container --------------------------
    def state_paths(self) -> dict:
        script = ("ls -1dt /checkpoints/savepoints/savepoint-* 2>/dev/null | head -1; "
                  "echo ---; ls -1dt /checkpoints/*/chk-* 2>/dev/null | head -1")
        try:
            raw = self.runner.compose(
                ["exec", "-T", FLINK_JOBMANAGER, "sh", "-lc", script], check=False)
        except subprocess.CalledProcessError:
            return {}
        savepoint, _, checkpoint = (raw or "").partition("---")
        savepoint = savepoint.strip().splitlines()
        checkpoint = checkpoint.strip().splitlines()

        def as_uri(path):
            if path and path.startswith("/"):
                return "file://" + path
            return path

        return {
            "latest_savepoint": as_uri(savepoint[-1]) if savepoint else None,
            "latest_checkpoint": as_uri(checkpoint[-1]) if checkpoint else None,
        }

    # -- container env (posture audit) -------------------------------------
    def container_env(self, services) -> dict:
        out = {}
        for svc in services:
            try:
                cid = self.runner.compose(["ps", "-q", svc], check=False).strip()
                if not cid:
                    continue
                raw = self.runner.run(
                    ["docker", "inspect", "--format", "{{json .Config.Env}}", cid],
                    check=False)
                out[svc] = dict(
                    entry.split("=", 1) for entry in json.loads(raw) if "=" in entry)
            except (subprocess.CalledProcessError, json.JSONDecodeError, ValueError):
                continue
        return out

    # -- fluss passive probe ----------------------------------------------
    def _compile_fluss_probe(self, workdir: pathlib.Path) -> str:
        cls = workdir / "FlussReadLagProbe.class"
        src = FLUSS_PROBE_SRC
        cp = CP_FILE.read_text().strip() if CP_FILE.exists() else ""
        if not cp:
            raise FileNotFoundError(
                f"classpath file missing: {CP_FILE} (run `make test` or "
                "`cd code && mvn -q -pl 02_services/01_ingestion -am package -DskipTests`)")
        if not cls.exists() or cls.stat().st_mtime < src.stat().st_mtime:
            javac = shutil.which("javac")
            if not javac:
                raise FileNotFoundError("javac not found on PATH")
            subprocess.run([javac, "-cp", cp, "-d", str(workdir), str(src)],
                           check=True, capture_output=True, text=True, timeout=120)
        return cp

    def fluss_log_end(self, table: str, workdir: pathlib.Path) -> dict:
        try:
            cp = self._compile_fluss_probe(workdir)
            proc = subprocess.run(
                ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED",
                 "--add-opens=java.base/java.nio=ALL-UNNAMED",
                 "-cp", f"{workdir}:{cp}", "FlussReadLagProbe",
                 "default", table, "localhost:9123"],
                capture_output=True, text=True, timeout=30)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
                FileNotFoundError) as exc:
            return {"ok": False, "error": str(exc)[:200]}
        lines = [ln for ln in (proc.stdout or "").splitlines() if ln.strip()]
        if proc.returncode not in (0, 3) or not lines:
            return {"ok": False, "error": (proc.stderr or "no sample").strip()[:200]}
        parts = lines[-1].split("\t")
        if len(parts) < 5:
            return {"ok": False, "error": f"unexpected probe output: {lines[-1][:120]}"}
        return {"ok": True, "log_end": int(parts[-1]), "sampled_at_ms": int(parts[0])}

    # -- logs --------------------------------------------------------------
    def log_errors(self, since: str) -> list:
        """Stream the error-service logs; keep only a bounded sample of hits.

        ``compose_lines`` (not ``compose``) is deliberate: during a recovery
        these services write far more than the evidence needs, and the old
        whole-output capture held the entire 15-minute window of four services
        in memory (2026-09-28)."""
        hits = []
        try:
            for line in self.runner.compose_lines(
                    ["logs", "--no-log-prefix", "--since", since,
                     *ERROR_LOG_SERVICES]):
                if any(pattern in line for pattern in ERROR_LOG_PATTERNS):
                    hits.append(line.strip())
                    if len(hits) >= MAX_LOG_ERROR_HITS:
                        break
        except (OSError, subprocess.SubprocessError):
            return hits
        return hits

    def effective_tokens(self, ingestion_running: bool) -> int | None:
        if not ingestion_running:
            return None  # exited container logs are last run's truth, not today's
        found = None
        try:
            for line in self.runner.compose_lines(
                    ["logs", "--no-log-prefix", "ingestion"]):
                match = MANIFEST_LOADED_RE.search(line)
                if match:
                    found = int(match.group(1))
        except (OSError, subprocess.SubprocessError):
            return found
        return found

    def nautilus_halted(self) -> bool:
        try:
            return any("gate HALTED" in line for line in self.runner.compose_lines(
                ["logs", "--no-log-prefix", "nautilus"]))
        except (OSError, subprocess.SubprocessError):
            return False

    # -- full snapshot -----------------------------------------------------
    def collect(self, universe: dict | None = None, evidence_dir: pathlib.Path | None = None,
                samples: bool | None = None, readiness: bool = False) -> Facts:
        evidence_dir = evidence_dir or (EVIDENCE_ROOT / "collect")
        evidence_dir.mkdir(parents=True, exist_ok=True)
        session = session_now()
        facts = Facts(session=session, universe=universe or {})
        facts.expected = self.expected_services()
        facts.services = self.services()
        facts.jobs = self.jobs()
        if readiness:
            # Readiness-only snapshot (2026-09-28 memory incident): the wait
            # loop calls this every 5s while a recovery may be writing GBs of
            # logs; log scans, container envs, state paths and the other-table
            # probes are board facts, not readiness gates. Keep the wait loop
            # to services + the one table it gates on.
            facts.fluss["raw"] = self.fluss_log_end("raw_table_1", evidence_dir)
            facts.fluss["window_s"] = self.window_s
            return facts
        facts.checkpoints = self.checkpoints(facts.jobs)
        facts.state = self.state_paths()
        facts.container_env = self.container_env(EXECUTION_SERVICES)
        if self.nautilus_halted():
            facts.notes.append("nautilus_halted")
        facts.log_errors = self.log_errors(
            f"{os.environ.get('DAY_LOG_WINDOW_MIN', '15')}m")
        ingestion_running = facts.services.get("ingestion", {}).get("state") == "running"
        facts.effective_tokens = self.effective_tokens(ingestion_running)

        want_samples = session["open"] if samples is None else samples
        for key, table in (("raw", "raw_table_1"),
                           ("candles", "feature_candles_15s"),
                           ("signals", "Signal_Candidates")):
            first = self.fluss_log_end(table, evidence_dir)
            facts.fluss[key] = first
        if want_samples and facts.fluss.get("raw", {}).get("ok"):
            time.sleep(self.window_s)
            for key, table in (("raw", "raw_table_1"),
                               ("candles", "feature_candles_15s"),
                               ("signals", "Signal_Candidates")):
                second = self.fluss_log_end(table, evidence_dir)
                if second.get("ok") and facts.fluss.get(key, {}).get("ok"):
                    facts.fluss[key] = {
                        "ok": True,
                        "log_end": second["log_end"],
                        "delta": second["log_end"] - facts.fluss[key]["log_end"],
                    }
        facts.fluss["window_s"] = self.window_s
        return facts


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------


def preflight(guard_posture: bool = True) -> Universe:
    deploy_env = effective_value("DEPLOYMENT_ENV", effective_value("DEPLOY_ENV", "dev"))
    mode = effective_value("UNIVERSE", DEFAULT_UNIVERSE)
    counts = {
        "full": count_manifest_rows(FULL_MANIFEST) if FULL_MANIFEST.exists() else 0,
        "approved": count_manifest_rows(APPROVED_MANIFEST)
        if APPROVED_MANIFEST.exists() else 0,
    }
    universe = resolve_universe(mode, deploy_env, counts)
    if not guard_posture:
        # status stays usable for inspection even when a live flag is present;
        # I6/I7 on the board report it.
        return universe
    violations = posture_violations(dict(os.environ))
    # .env values only matter when the shell does not override them; compose
    # interpolation is shell-first, so reading both in precedence order is the
    # effective config for these keys.
    env_file = parse_env_file(ENV_FILE)
    for key in ("EXECUTION_ENABLED", "EXECUTION_BRIDGE_MODE"):
        if key not in os.environ and key in env_file:
            violations.extend(posture_violations({key: env_file[key]}))
    if violations:
        raise Refusal(
            "live-enablement flag(s) present in the effective config: "
            + "; ".join(violations)
            + " -- the daily runner only brings the execution chain up in its "
            "offline posture and never flips these flags "
            "(docs/06_operations/08-live-readiness-gaps.md)"
        )
    manifest_env = os.environ.get("INSTRUMENT_MANIFEST_PATH", "")
    if manifest_env and manifest_env != "/instruments/NSE_CM_EQUITY.csv":
        raise Refusal(
            "INSTRUMENT_MANIFEST_PATH is set in the shell to "
            f"'{manifest_env}'; the runner owns the container path "
            "(/instruments/NSE_CM_EQUITY.csv) and only switches the host file "
            "(INSTRUMENT_MANIFEST_HOST_PATH). Unset it."
        )
    return universe


def universe_env(universe: Universe) -> dict:
    return {
        "INSTRUMENT_MANIFEST_HOST_PATH": str(universe.manifest),
        "ARROW_HFT_CONNECTIONS": str(universe.connections),
        "ARROW_HFT_MULTI_CONNECTION_APPROVED": "true" if universe.approval else "false",
        "DEPLOYMENT_ENV": universe.deploy_env or "dev",
    }


def wait_ready(collector: Collector, universe: Universe, runner: Runner,
               timeout_s: int = DEFAULT_READY_TIMEOUT_S) -> None:
    """Bounded wait for stack services + Flink REST + Fluss metadata."""
    deadline = time.time() + timeout_s
    last = "starting"
    stable = 0
    while time.time() < deadline:
        facts = collector.collect(universe=dataclasses.asdict(universe), readiness=True)
        running = {n for n, info in facts.services.items() if info.get("state") == "running"}
        missing = [s for s in facts.expected
                   if s not in running and s not in EXECUTION_SERVICES]
        fluss_ok = facts.fluss.get("raw", {}).get("ok")
        flink_ok = bool(facts.jobs) or _flink_reachable(collector)
        last = (f"services missing={','.join(missing) or 'none'} "
                f"fluss={'ok' if fluss_ok else 'waiting'} "
                f"flink={'ok' if flink_ok else 'waiting'}")
        if not missing and fluss_ok and flink_ok:
            # Two consecutive good polls: a container that comes up and exits
            # within seconds (e.g. a stale bridge binary refusing the plan)
            # must not be mistaken for a healthy stack.
            stable += 1
            if stable >= 2:
                runner.emit(f"[day] ready       OK   {last}")
                return
        else:
            stable = 0
        runner.emit(f"[day] waiting     ...  {last}")
        time.sleep(5)
    raise Refusal(f"stack did not become ready within {timeout_s}s ({last}); "
                  "run make day ARGS=\"status\" and make logs SVC=<service>")


def _flink_reachable(collector: Collector) -> bool:
    try:
        collector.runner.http_json(f"{collector.jm_url}/config")
        return True
    except (urllib.error.URLError, OSError, ValueError, TimeoutError):
        return False


def wait_signaljob(collector: Collector, runner: Runner, timeout_s: int = 240) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        jobs = collector.jobs()
        signals = [j for j in jobs if j.get("name") == SIGNAL_JOB_NAME]
        if len(signals) == 1 and signals[0].get("state") == "RUNNING":
            cps = collector.checkpoints(signals)
            if cps.get(signals[0]["id"], {}).get("latest_completed_ms"):
                runner.emit("[day] signaljob   OK   RUNNING + first checkpoint completed")
                return
        time.sleep(5)
    raise Refusal("SignalJob did not reach RUNNING with a completed checkpoint within "
                  f"{timeout_s}s; check make logs SVC=flink-jobmanager")


def ensure_signaljob(collector: Collector, runner: Runner, env: dict,
                     allow_fresh: bool) -> None:
    facts = Facts(jobs=collector.jobs(), state=collector.state_paths())
    decision = decide_signaljob(facts.jobs, facts.state, allow_fresh)
    if decision.action == "keep":
        runner.emit(f"[day] signaljob   OK   keep ({decision.reason})")
        return
    if decision.action == "restore":
        runner.emit(f"[day] signaljob   ...  restore from {decision.reason}: {decision.path}")
        runner.make_locked("rollout-savepoint",
                           args=f"RECOVERY_PATH={decision.path}", env=env)
        wait_signaljob(collector, runner)
        return
    if decision.action == "fresh":
        runner.emit("[day] signaljob   ...  fresh submit (ALLOW_FRESH=1; dev starts at LATEST)")
        runner.make("up", env={**env, "COMPUTE_SUBMIT_SIGNAL": "1"})
        wait_signaljob(collector, runner)
        return
    if decision.action == "refuse":
        raise Refusal(f"{decision.reason} -- {decision.recovery}")
    raise Refusal(f"SignalJob not healthy: {decision.reason} -- {decision.recovery}")


def cmd_start(runner: Runner, make_collector) -> int:
    try:
        universe = preflight()
    except Refusal as refusal:
        runner.emit(f"[day] preflight   FAIL {refusal}")
        return EXIT_REFUSED
    runner.emit(f"[day] preflight   OK   host={universe.deploy_env} "
                f"compose=true exec-posture=offline(verified)")
    env = universe_env(universe)
    if os.environ.get("DAY_SKIP_IMAGE_CHECK") != "1":
        stale, report = runner.image_staleness()
        if stale and os.environ.get("REBUILD") == "1":
            runner.emit("[day] images      ...  stale build image(s); rebuilding (REBUILD=1)")
            runner.make("images", env=env)
            stale, report = runner.image_staleness()
        if stale:
            tail = report.splitlines()[-1] if report else "see make check-image-stale"
            runner.emit(f"[day] preflight   FAIL stale build image(s): {tail}")
            runner.emit("[day]             recovery: make images  "
                        "(or REBUILD=1 make day ARGS=\"start\")")
            return EXIT_REFUSED
        runner.emit("[day] images      OK   build stamps current")
    try:
        runner.make("up", env=env)
        collector = make_collector(runner)
        wait_ready(collector, universe, runner,
                   timeout_s=int(os.environ.get("DAY_READY_TIMEOUT_S",
                                                str(DEFAULT_READY_TIMEOUT_S))))
        ensure_signaljob(collector, runner, env, allow_fresh=_allow_fresh())
        # Phase 2: execution-t3 chain in its offline posture (profile-gated;
        # the runner only starts it, it never flips the gate flags).
        runner.make("up", env={**env, "COMPOSE_PROFILES": EXECUTION_PROFILE})
        facts = collector.collect(universe=dataclasses.asdict(universe))
    except Refusal as refusal:
        runner.emit(f"[day] verdict     RED exit=3 · {refusal}")
        return EXIT_REFUSED
    except subprocess.CalledProcessError as exc:
        runner.emit(f"[day] verdict     RED exit={exc.returncode} · command failed: "
                    f"{' '.join(map(str, exc.cmd))}")
        return EXIT_RED
    checks = evaluate(facts)
    board = render(checks, facts, universe=dataclasses.asdict(universe))
    print(board)
    write_evidence("start", board, facts)
    return EXIT_RED if any(not c.ok and not c.pending for c in checks) else EXIT_OK


def cmd_status(runner: Runner, make_collector) -> int:
    try:
        universe = preflight(guard_posture=False)
    except Refusal as refusal:
        runner.emit(f"[day] preflight   FAIL {refusal}")
        return EXIT_REFUSED
    collector = make_collector(runner)
    facts = collector.collect(universe=dataclasses.asdict(universe))
    checks = evaluate(facts)
    board = render(checks, facts, universe=dataclasses.asdict(universe))
    print(board)
    write_evidence("status", board, facts)
    return EXIT_RED if any(not c.ok and not c.pending for c in checks) else EXIT_OK


def cmd_stop(runner: Runner, make_collector) -> int:
    collector = make_collector(runner)
    state = collector.state_paths()
    savepoint = state.get("latest_savepoint")
    checkpoint = state.get("latest_checkpoint")
    try:
        runner.make("down", env={"COMPOSE_PROFILES": EXECUTION_PROFILE})
    except subprocess.CalledProcessError as exc:
        runner.emit(f"[day] stop        FAIL exit={exc.returncode}")
        return EXIT_RED
    runner.emit("[day] stop        OK   stack down; volumes preserved "
                "(flink-checkpoints, fluss-*, flink-rocksdb)")
    runner.emit(f"[day] restart     INFO savepoint={savepoint or 'none'} "
                f"checkpoint={checkpoint or 'none'}")
    runner.emit("[day]             INFO next morning: make day ARGS=\"start\" "
                "(state restore is automatic)")
    write_evidence("stop", f"savepoint={savepoint or 'none'} "
                           f"checkpoint={checkpoint or 'none'}", None)
    return EXIT_OK


def write_evidence(verb: str, board: str, facts: Facts | None) -> pathlib.Path:
    """Evidence under logs/day/<timestamp>-<verb>/ (plan §4): board + facts."""
    stamp = dt.datetime.now(IST).strftime("%Y%m%d-%H%M%S")
    directory = pathlib.Path(os.environ.get(
        "DAY_EVIDENCE_DIR", EVIDENCE_ROOT / f"{stamp}-{verb}"))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "board.txt").write_text(board + "\n")
    if facts is not None:
        payload = dataclasses.asdict(facts)
        payload["session"] = facts.session
        (directory / "facts.json").write_text(json.dumps(payload, indent=2, default=str))
    return directory


def _allow_fresh() -> bool:
    return os.environ.get("ALLOW_FRESH", "0") == "1"


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------


def main(argv=None, runner: Runner | None = None, collector_factory=None) -> int:
    parser = argparse.ArgumentParser(
        prog="make day ARGS=", add_help=True,
        description="daily single-command platform runner (start|status|stop)")
    parser.add_argument("verb", choices=["start", "status", "stop"])
    args = parser.parse_args(argv)
    runner = runner or Runner()
    factory = collector_factory or (lambda r: Collector(r))
    if args.verb == "start":
        return cmd_start(runner, factory)
    if args.verb == "status":
        return cmd_status(runner, factory)
    return cmd_stop(runner, factory)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
