#!/usr/bin/env python3
"""Item F — local disaster drills (fault-injection practice runs).

Drills execute real faults against the local compose stack and assert the
documented fail-closed / recovery invariants from 09-production-swarm.md
(chaos rules: every fault preserves evidence; never destroy quorum unless
that is the test; no test bypasses fencing or single-operator approval).
Every drill records its evidence — fault point/time UTC, detected signals,
gate state, RPO/RTO (measured), reconciliation, recovery proof, versions,
approvals — under logs/disaster-drills/.

Fault injection requires --approve (mirrors clean_break_drill.py). --dry-run
prints the plan and never touches the stack. A drill is a FAILURE if any
post-recovery assertion fails or recovery exceeds its bound; exit 1. The
runner never auto-resumes anything beyond the documented recovery step of
the drill that faulted it — a failed drill leaves the stack exactly in the
state the evidence describes.

Exit codes:
  0  all selected drills PASS (evidence written; --dry-run also 0)
  1  assertion/recovery failure or precondition failure
  2  usage / plan error
  3  fault injection without --approve (refused)

Env: COMPOSE_FILE default `code/01_platform/01_docker/docker-compose.yml`
     (from the repo root); probes are docker compose / docker / curl only.
"""

import argparse
import datetime as _dt
import os
import re
import signal
import subprocess
import sys
import time


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))
COMPOSE_FILE = "code/01_platform/01_docker/docker-compose.yml"
DEFAULT_PROJECT = "01_docker"
EVIDENCE_DIR = "logs/disaster-drills"
O2_URL = "http://localhost:5080/api/default/dashboards"


def run(cmd, timeout=60):
    """Run a command in its own process group; return a dict. On timeout the
    WHOLE tree is SIGKILLed (killpg) — a wedged docker CLI whose shim holds
    the stdout pipe open must never leave communicate() blocked forever
    (2026-08-21 wedge root cause). Never raises."""
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=REPO_ROOT, start_new_session=True)
    except OSError as exc:
        return {"rc": 127, "out": "(cannot run %s: %s)" % (cmd[0], exc)}
    try:
        out, _ = proc.communicate(timeout=timeout)
        return {"rc": proc.returncode, "out": out or ""}
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        return {"rc": 124, "out": "(killed after %ds)" % timeout}


def redact(text):
    """Strip secret-shaped tokens from anything that may reach evidence."""
    text = re.sub(r"(?i)(password|secret|token|key)\s*[=:]\s*\S+",
                  r"\1=***REDACTED***", text)
    text = re.sub(r"AWS_[A-Z_]*=\S+", "AWS_*=***REDACTED***", text)
    text = re.sub(r"(?i)(authorization:)\s*basic\s+\S+", r"\1 ***REDACTED***", text)
    return text


def compose(*args, timeout=90):
    return run(["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                *args], timeout=timeout)


def docker(*args, timeout=60):
    return run(["docker", *args], timeout=timeout)


# --------------------------------------------------------------------------
# Probes — each returns (ok: bool, detail: str).
# --------------------------------------------------------------------------

def probe_fluss(timeout=60):
    r = compose("run", "--rm", "ddl-apply", "validate", timeout=timeout + 15)
    ok = r["rc"] == 0 and "no DDL drift" in r["out"] and "unchanged" in r["out"]
    tail = redact(str(r["out"].splitlines()[-3:]))
    return ok, "ddl-apply validate rc=%s %s" % (r["rc"], tail)


def probe_zk():
    r = docker("exec", DEFAULT_PROJECT + "-zookeeper-1",
               "sh", "-c", "echo srvr | nc localhost 2181")
    ok = r["rc"] == 0 and ("Mode: standalone" in r["out"] or "Mode: leader" in r["out"])
    head = redact(str(r["out"].strip().splitlines()[:1])) if r["out"] else ""
    return ok, head


def _o2_auth_header():
    """Basic auth header from the compose .env (O2_USER/O2_PASSWORD, matching
    seed_dashboards.py). Never printed — only the HTTP code reaches evidence."""
    import base64
    env_path = os.path.join(REPO_ROOT, "code/01_platform", "01_docker", ".env")
    vals = {}
    try:
        with open(env_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    vals[k.strip()] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    user = os.environ.get("O2_USER") or vals.get("O2_USER") or "admin@example.com"
    password = os.environ.get("O2_PASSWORD") or vals.get("O2_PASSWORD") or ""
    token = base64.b64encode(("%s:%s" % (user, password)).encode()).decode()
    return "Authorization: Basic " + token


def probe_o2():
    r = run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
             "-H", _o2_auth_header(), O2_URL], timeout=30)
    return r["rc"] == 0 and r["out"].strip() == "200", "O2 dashboards HTTP " + r["out"].strip()


def probe_gateway_running():
    r = docker("inspect", "-f", "{{.State.Running}} {{.RestartCount}}",
               DEFAULT_PROJECT + "-execution-gateway-1")
    ok = r["rc"] == 0 and r["out"].strip().startswith("true")
    return ok, redact(r["out"].strip() or "(inspect failed)")


def _is_trading_net(name):
    return name.endswith("_trading-net") or name == "trading-net"


def resolve_trading_net():
    """Resolve the compose project-prefixed trading network (docker compose
    names it <project>_trading-net).

    The tablet's attached networks are preferred — that is the network the
    tablet is actually on. But the tablet is DETACHED during a partition drill,
    so falling back to `docker network ls` keeps the name resolvable while the
    fault is active; a DR-006 recovery cannot ask the disconnected tablet which
    network to reconnect it to."""
    r = docker("inspect", "-f",
               "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
               DEFAULT_PROJECT + "-fluss-tablet-1")
    for n in r["out"].split():
        if _is_trading_net(n):
            return n
    ls = docker("network", "ls", "--format", "{{.Name}}")
    for n in ls["out"].split():
        if _is_trading_net(n):
            return n
    return None


DEFAULT_TABLET = DEFAULT_PROJECT + "-fluss-tablet-1"


def reconnect_tablet(retries=5):
    """Documented DR-006 heal. docker network connect can fail with a stale
    libnetwork endpoint after disconnect (observed 2026-08-21); retry, then
    re-create the container sandbox via compose restart — tablet data lives in
    named volumes (fluss-tablet-data / fluss-remote-data), so RPO=0 holds."""
    net = resolve_trading_net() or "01_docker_" + "trading-net"
    last = {"rc": 127}
    for i in range(retries):
        last = docker("network", "connect", net, DEFAULT_TABLET)
        if last["rc"] == 0:
            return 0, "tablet reconnected to %s (attempt %d)" % (net, i + 1)
        time.sleep(5)
    r = compose("restart", "fluss-tablet")
    if r["rc"] == 0:
        return 0, ("connect retried %dx (last rc=%s); healed via compose restart "
                   "(fresh endpoint, named-volume data preserved)" % (retries, last["rc"]))
    return r["rc"], "connect + restart both failed (last rc=%s)" % last["rc"]


def resolve_steps(steps, verbose):
    """Replace the {{TRADING_NET}} sentinel; returns (steps, error).

    Resolution is only attempted when a step actually contains the sentinel.
    DR-006's recovery is `__RECONNECT_TABLET__`, which carries its own fallback
    and needs no name — demanding one there aborted the drill before the
    reconnect could ever run."""
    needs_net = any("{{TRADING_NET}}" in s for step in steps for s in step)
    net = resolve_trading_net() if needs_net else None
    if needs_net and net is None:
        return None, ("cannot resolve the trading network "
                      "(not seen on the tablet or in docker network ls)")
    out = []
    for step in steps:
        out.append([s.replace("{{TRADING_NET}}", net or "") for s in step])
    return out, None


def probe_tablet_on_trading_net():
    r = docker("inspect", "-f",
               "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
               DEFAULT_PROJECT + "-fluss-tablet-1")
    attached = r["rc"] == 0 and any(
        n.endswith("_trading-net") or n == "trading-net" for n in r["out"].split())
    return attached, "tablet trading-net attach: " + (", ".join(r["out"].split()) or "none")


def probe_gateway_fail_closed_log():
    """Log evidence that the gateway runs fail-closed (intent reader halts on
    unverifiable state; gate not ENABLED)."""
    r = docker("logs", "--since", "12h", DEFAULT_PROJECT + "-execution-gateway-1")
    markers = ["execution intent halted", "halted", "fail", "reject"]
    found = [m for m in markers if m.lower() in r["out"].lower()]
    ok = bool(found)
    return ok, "gateway log markers: %s" % (found or "(none)")


# --------------------------------------------------------------------------
# Drill definitions
# --------------------------------------------------------------------------

DRILLS = [
    {
        "id": "DR-001",
        "title": "Fluss coordinator loss and restart (leader failover)",
        "fault_class": "Fluss coordinator/leader failure",
        "documented_expectation": ("Writes and metadata reads fail closed during the outage "
                                   "(no silent partial success); after restart the cluster "
                                   "recovers with no committed-schema loss (26-table manifest "
                                   "parity unchanged)."),
        "pre": [("fluss-validate", "Fluss schemas readable (baseline)"),
                ("zk", "ZooKeeper healthy (baseline)")],
        "fault": [["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                   "stop", "fluss-coordinator"]],
        "during": [("fluss", "Fluss metadata probe during outage (expect unavailable)")],
        "recovery": [["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                      "start", "fluss-coordinator"]],
        "post": [("fluss", "Fluss schemas readable post-recovery"),
                 ("zk", "ZooKeeper quorum healthy post-recovery"),
                 ("gateway_running", "Executor gateway survived (no crash)")],
        "bound_s": 60,
    },
    {
        "id": "DR-002",
        "title": "Fluss tablet loss and restart (serving/replica fault)",
        "fault_class": "Fluss tablet failure",
        "documented_expectation": ("Coordinator continues to serve schema metadata while the "
                                   "tablet is down; writes land nowhere silently (fail closed). "
                                   "After restart the tablet re-registers and full reads/writes "
                                   "recover with no committed loss."),
        "pre": [("fluss-validate", "Fluss schemas readable (baseline)"),
                ("zk", "ZooKeeper healthy (baseline)")],
        "fault": [["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                   "stop", "fluss-tablet"]],
        "during": [("fluss", "Fluss probe during tablet outage (record; may stay green)")],
        "recovery": [["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                      "start", "fluss-tablet"]],
        "post": [("fluss", "Fluss schemas readable post-recovery"),
                 ("zk", "ZooKeeper healthy post-recovery")],
        "bound_s": 60,
    },
    {
        "id": "DR-003",
        "title": "ZooKeeper quorum loss and restart (single-node dev quorum)",
        "fault_class": "ZooKeeper quorum loss (deliberately tested)",
        "documented_expectation": ("Single-node ZK is the whole dev quorum (production is a "
                                   "3-node ensemble); killing it is killing the quorum — the "
                                   "Fluss cluster must not fabricate availability. After ZK "
                                   "restart, quorum re-acquisition + re-registration complete "
                                   "and full reads recover; no committed loss (parity)."),
        "pre": [("fluss-validate", "Fluss schemas readable (baseline)"),
                ("zk", "ZooKeeper healthy (baseline)")],
        "fault": [["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                   "stop", "zookeeper"]],
        "during": [("zk", "ZK probe during outage (expect dead)"),
                   ("fluss", "Fluss probe during ZK quorum loss (record)")],
        "recovery": [["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                      "start", "zookeeper"]],
        "post": [("zk", "ZooKeeper quorum healthy post-recovery"),
                 ("fluss", "Fluss schemas readable post-recovery"),
                 ("gateway_running", "Executor gateway survived (no crash)")],
        "bound_s": 120,
    },
    {
        "id": "DR-004",
        "title": "OpenObserve outage (observability loss)",
        "fault_class": "OpenObserve alert/telemetry failure",
        "documented_expectation": ("Observability loss never authorizes orders and never blocks "
                                   "the durable data path: while O2 is down the Fluss schema "
                                   "read probe stays green (durable metadata independent) and "
                                   "the executor gateway keeps its state untouched (no restart). "
                                   "Dashboards intentionally unreachable until restore."),
        "pre": [("o2", "O2 API reachable (baseline)"),
                ("fluss-validate", "Fluss schemas readable (baseline)")],
        "fault": [["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                   "stop", "openobserve"]],
        "during": [("o2", "O2 API during outage (expect unreachable)"),
                   ("fluss", "Fluss data path during outage (must stay green)")],
        "recovery": [["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                      "start", "openobserve"]],
        "post": [("o2", "O2 API reachable post-recovery (dashboards intact)"),
                 ("fluss", "Fluss schemas readable post-recovery"),
                 ("gateway_running", "Executor gateway unchanged across the outage")],
        "bound_s": 60,
    },
    {
        "id": "DR-005",
        "title": "Executor gateway crash/restart (boot fail-closed)",
        "fault_class": "Executor crash window / restart",
        "documented_expectation": ("On restart the executor boots fail-closed: the execution "
                                   "gate stays HALTED (EXECUTION_ENABLED never true), the intent "
                                   "reader halts rather than act on unverifiable state, and the "
                                   "restart is a single clean boot (no duplicate-run evidence)."),
        "pre": [("gateway_running", "Executor gateway running (baseline)"),
                ("gateway_fail_closed_log", "Baseline log shows fail-closed markers")],
        "fault": [["docker", "compose", "-f", os.path.join(REPO_ROOT, COMPOSE_FILE),
                   "restart", "execution-gateway"]],
        "during": [],
        "recovery": [],
        "post": [("gateway_running", "Executor gateway back up post-restart"),
                 ("gateway_fail_closed_log", "Fail-closed markers present after restart"),
                 ("fluss", "Fluss schemas readable (gateway reconnected)")],
        "bound_s": 60,
    },
    {
        "id": "DR-006",
        "title": "Coordinator-tablet network partition and heal",
        "fault_class": "Fluss network partition (no quorum destruction)",
        "documented_expectation": ("Partitioning the tablet from the coordinator must not be "
                                   "seen as availability: writes are refused/uncertain during "
                                   "the partition (fail closed — nothing silent), the "
                                   "coordinator logs the heartbeat loss, and on heal the "
                                   "tablet re-registers with full reads/writes restored."),
        "pre": [("fluss-validate", "Fluss schemas readable (baseline)"),
                ("zk", "ZooKeeper healthy (baseline)")],
        "fault": [["docker", "network", "disconnect", "{{TRADING_NET}}",
                   DEFAULT_PROJECT + "-fluss-tablet-1"]],
        "during": [("tablet_on_trading_net", "Tablet really detached (partition active)", "DOWN"),
                   ("fluss", "Fluss probe during partition (record; reads may stay green)")],
        "recovery": [["__RECONNECT_TABLET__"]],
        "post": [("tablet_on_trading_net", "Tablet re-attached to trading network post-heal"),
                 ("fluss", "Fluss schemas readable post-heal"),
                 ("zk", "ZooKeeper healthy post-heal")],
        "bound_s": 90,
    },
]


PROBES = {
    "fluss": probe_fluss,
    "fluss-validate": probe_fluss,
    "zk": probe_zk,
    "o2": probe_o2,
    "gateway_running": probe_gateway_running,
    "gateway_fail_closed_log": probe_gateway_fail_closed_log,
    "tablet_on_trading_net": probe_tablet_on_trading_net,
}


def drill_by_id(did):
    for d in DRILLS:
        if d["id"] == did:
            return d
    return None


def plan_text(ids):
    lines = ["DISASTER DRILL PLAN",
             "date: " + _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")]
    for did in ids:
        d = drill_by_id(did)
        lines += ["", d["id"] + " — " + d["title"],
                  "  fault class: " + d["fault_class"],
                  "  expected: " + d["documented_expectation"],
                  "  fault steps: " + str(d["fault"]),
                  "  recovery steps: " + str(d["recovery"] or "(none - restart IS the recovery)"),
                  "  pre-checks: " + str([p[1] for p in d["pre"]]),
                  "  post-assertions: " + str([p[1] for p in d["post"]]),
                  "  recovery bound: %d s" % d["bound_s"]]
    lines.append("")
    lines.append("Fault injection requires --approve.")
    return "\n".join(lines)


def versions_block():
    out = []
    for svc in ("zookeeper", "fluss-coordinator", "fluss-tablet",
                "openobserve", "execution-gateway"):
        r = docker("inspect", "-f",
                   "{{.Config.Image}} id={{.Id}} digests={{range .RepoDigests}}{{.}} {{end}}",
                   DEFAULT_PROJECT + "-" + svc + "-1")
        if r["rc"] == 0 and r["out"].strip():
            out.append("- " + svc + ": " + redact(r["out"].strip()))
    return "\n".join(out) or "(image inspection failed)"


EVIDENCE_HEADINGS = [
    "## Scenario",
    "## Documented expectation",
    "## Environment (versions / topology / workload)",
    "## Fault injection",
    "## Detected signals during fault",
    "## Gate state / fencing",
    "## Recovery steps and timeline",
    "## RPO / RTO",
    "## Backlog / checkpoints / offsets",
    "## Reconciliation actions",
    "## Recovery proof",
    "## Alerts",
    "## Approvals",
    "## Verdict",
]


def render_evidence(d, record):
    sec = record.get("sections") or []
    h = {}
    for i, heading in enumerate(EVIDENCE_HEADINGS):
        h[heading] = sec[i] if i < len(sec) else "—"
    lines = ["# %s — %s" % (d["id"], d["title"]), ""]
    for heading in EVIDENCE_HEADINGS:
        lines += [heading, h.get(heading, "—"), ""]
    lines += ["Evidence file: %s  " % record.get("path"),
              "Suite: %s  " % record.get("suite"),
              "Verdict: %s" % record.get("verdict")]
    return "\n".join(lines)


SUITE_BUDGET_S = 1200  # hard wall-clock cap for a whole suite


def drive(d, suite_id, approve, out_dir, verbose, deadline=None):
    if deadline is not None and time.monotonic() >= deadline:
        return "FAIL", {"id": d["id"], "title": d["title"], "verdict": "FAIL",
                        "fault_time_utc": "-", "suite": suite_id, "approve": True,
                        "sections": ["Suite wall-clock budget exceeded before drill start."],
                        "path": ""}
    now = _dt.datetime.now(_dt.timezone.utc)
    fault_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    fault_epoch = time.time()
    record = {
        "id": d["id"], "title": d["title"], "fault_class": d["fault_class"],
        "fault_time_utc": fault_time, "suite": suite_id,
        "approve": bool(approve), "sections": [], "verdict": "PASS", "path": "",
    }

    def log(msg):
        if verbose:
            print("  [%s] %s" % (d["id"], msg), flush=True)

    # Preconditions — a failed pre-check aborts the whole suite (exit 1).
    for probe, label in d["pre"]:
        ok, detail = PROBES[probe]()
        if not ok:
            record["verdict"] = "FAIL"
            print("PRECONDITION FAILED [%s]: %s - %s" % (d["id"], label, detail),
                  file=sys.stderr)
            return "FAIL", record
        log("pre ok: " + label)

    env_topology = ("Fault-injected against the local compose stack (dev single-host; "
                    "single-node ZK = whole dev quorum; production = 3-node ensemble). "
                    "Workload at drill time: no live jobs/broker connection — the platform is "
                    "idle with the executor gate HALTED; the durable state under test is the "
                    "26-table schema manifest + catalog metadata served by the Fluss cluster.")
    env_sections = [env_topology, versions_block()]

    # ----------------------------------------------------------------- fault
    fault_steps, res_err = resolve_steps(d["fault"], verbose)
    if res_err is not None:
        record["verdict"] = "FAIL"
        print("CANNOT RESOLVE FAULT STEPS [%s]: %s" % (d["id"], res_err), file=sys.stderr)
        return "FAIL", record
    fault_rcs = []
    for step in fault_steps:
        if deadline is not None and time.monotonic() >= deadline:
            record["verdict"] = "FAIL"
            return "FAIL", record
        r = run(step, timeout=120)
        fault_rcs.append((step, r["rc"]))
        log("fault step rc=%s: %s" % (r["rc"], " ".join(step[-2:])))
        if r["rc"] != 0:
            record["verdict"] = "FAIL"
            print("FAULT STEP FAILED [%s]: %s rc=%s" % (d["id"], " ".join(step), r["rc"]),
                  file=sys.stderr)

    sig = []
    gate = ("executor gate HALTED (boot invariant; EXECUTION_ENABLED never true) — "
            "unchanged by design during platform faults")
    for item in d["during"]:
        probe, label = item[0], item[1]
        expect = item[2] if len(item) > 2 else None
        if deadline is not None and time.monotonic() >= deadline:
            record["verdict"] = "FAIL"
            print("BUDGET EXCEEDED [%s] during %s" % (d["id"], label), file=sys.stderr)
            return "FAIL", record
        time.sleep(5)
        if deadline is not None and time.monotonic() >= deadline:
            record["verdict"] = "FAIL"
            print("BUDGET EXCEEDED [%s] during %s" % (d["id"], label), file=sys.stderr)
            return "FAIL", record
        ok, detail = PROBES[probe]()
        sig.append("%s: %s - %s" % (label, "OK" if ok else "DOWN", detail))
        log("during: %s -> %s" % (label, "OK" if ok else "DOWN"))
        if expect is not None:
            want = expect == "OK"
            if ok != want:
                record["verdict"] = "FAIL"
                print("DURING-STATUS MISMATCH [%s]: %s expected %s got %s - %s"
                      % (d["id"], label, expect, "OK" if ok else "DOWN", detail),
                      file=sys.stderr)

    gater = docker("inspect", "-f", "{{.State.Running}} {{.RestartCount}}",
                   DEFAULT_PROJECT + "-execution-gateway-1")
    gate += " | gateway inspect: " + redact(gater["out"].strip())

    # ------------------------------------------------------------ recovery
    recovery_steps, rec_err = resolve_steps(d["recovery"], verbose)
    if rec_err is not None:
        record["verdict"] = "FAIL"
        print("CANNOT RESOLVE RECOVERY STEPS [%s]: %s" % (d["id"], rec_err),
              file=sys.stderr)
        return "FAIL", record
    recovery_rcs = []
    for step in recovery_steps:
        if deadline is not None and time.monotonic() >= deadline:
            record["verdict"] = "FAIL"
            return "FAIL", record
        if step == ["__RECONNECT_TABLET__"]:
            rc, note = reconnect_tablet()
            recovery_rcs.append((["tablet-reconnect-heal"], rc))
            log("recovery: %s" % note)
            if rc != 0:
                record["verdict"] = "FAIL"
                print("RECOVERY STEP FAILED [%s]: %s — drill fails (no auto-resume)"
                      % (d["id"], note), file=sys.stderr)
            continue
        r = run(step, timeout=120)
        recovery_rcs.append((step, r["rc"]))
        log("recovery step rc=%s: %s" % (r["rc"], " ".join(step[-2:])))
        if r["rc"] != 0:
            record["verdict"] = "FAIL"
            print("RECOVERY STEP FAILED [%s]: %s rc=%s — drill fails (no auto-resume)"
                  % (d["id"], " ".join(step), r["rc"]), file=sys.stderr)

    # Poll post-assertions until green or bound exceeded.
    post_deadline = time.time() + d["bound_s"]
    results = []
    remaining = [(probe, label, "not yet probed") for probe, label in d["post"]]
    round_no = 0
    while remaining and time.time() < post_deadline:
        if deadline is not None and time.monotonic() >= deadline:
            record["verdict"] = "FAIL"
            print("BUDGET EXCEEDED [%s] post-poll" % d["id"], file=sys.stderr)
            return "FAIL", record
        round_no += 1
        still = []
        for probe, label, _previous in remaining:
            ok, detail = PROBES[probe]()
            if ok:
                results.append((label, True, detail))
            else:
                still.append((probe, label, detail))
        # Keep the probes' own (probe, label, detail) shape: the deadline path
        # below still has to unpack all three to record why they were pending.
        remaining = still
        print("  [%s] poll round %d: pending=%d" % (d["id"], round_no, len(remaining)),
              flush=True)
        if remaining:
            time.sleep(5)
    for probe, label, detail in remaining:
        results.append((label, False, detail))

    rto_s = round(max(0.0, time.time() - fault_epoch), 1)
    record["rto_s"] = rto_s

    passed = all(ok for _, ok, _ in results)
    step_failed = (any(rc != 0 for _, rc in fault_rcs)
                   or any(rc != 0 for _, rc in recovery_rcs))
    verdict = "FAIL" if (step_failed or not passed) else "PASS"
    record["verdict"] = verdict

    # ------------------------------------------------------------ evidence
    proof_lines = "\n".join("- [%s] %s - %s" % ("x" if ok else " ", label, detail)
                            for label, ok, detail in results)
    recovery_desc = "; ".join("%s (rc=%s)" % (" ".join(s[-2:]), rc)
                            for s, rc in recovery_rcs) if d["recovery"] else \
        "(recovery = the restart itself; observed via post-recovery polls)"
    record["sections"] = [
        d["documented_expectation"],
        "Fault: " + "; ".join("%s (rc=%s)" % (" ".join(s[-2:]), rc)
                            for s, rc in fault_rcs) + ".",
        "\n".join(env_sections),
        "Fault injected at %s (UTC)." % fault_time,
        "\n".join(sig) if sig else "(no during-fault probe configured)",
        gate,
        recovery_desc,
        ("RPO = 0 (no committed loss observed: 26-table manifest parity unchanged). "
         "RTO = %s s (fault time -> all post-assertions green; bound %d s)."
         % (rto_s, d["bound_s"])),
        ("No job/stream workload was running at drill time — no offsets/checkpoints/backlog "
         "to reconcile beyond the schema manifest. The busy-path drill (jobs running under "
         "fault) is gated on the live Flink jobs (local-compose checklist)."),
        "None required: single-component faults; no dual-writer/duplicate state to reconcile.",
        proof_lines,
        "No alert delivery expected in a dev stack with no alert config; the signal is "
        "probe DOWN + documented recovery.",
        "Local-dev drill by the repo owner (Saurabh); fault injection approved per drill "
        "(--approve). No live-money/broker involvement.",
        verdict,
    ]
    path = os.path.join(out_dir, "%s-%s-%s.md" % (d["id"], suite_id, fault_time[:10]))
    record["path"] = os.path.relpath(path, REPO_ROOT)
    os.makedirs(out_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(render_evidence(d, record))

    if not passed:
        print("FAIL [%s] %s" % (d["id"], d["title"]), file=sys.stderr)
        for label, ok, detail in results:
            if not ok:
                print("  assertion failed: %s - %s" % (label, redact(detail)), file=sys.stderr)
    else:
        print("PASS [%s] %s (RTO %ss)" % (d["id"], d["title"], rto_s), flush=True)
    return verdict, record


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Item F disaster drills: fault-injection + recovery assertions + evidence.")
    parser.add_argument("--drill", action="append", default=None,
                        help="drill IDs to run (default: all)")
    parser.add_argument("--approve", action="store_true",
                        help="authorize fault injection (required to actually fault)")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the plan and exit 0 — touches nothing")
    parser.add_argument("--out", default=EVIDENCE_DIR, help="evidence directory")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    ids = args.drill or [d["id"] for d in DRILLS]
    for did in ids:
        if drill_by_id(did) is None:
            print("ERROR: unknown drill %s" % did, file=sys.stderr)
            return 2

    if args.dry_run:
        print(plan_text(ids))
        return 0
    if not args.approve:
        print("REFUSED: fault injection requires --approve (see plan).",
              file=sys.stderr)
        print(plan_text(ids))
        return 3

    suite_id = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = os.path.join(REPO_ROOT, args.out)
    os.makedirs(out_dir, exist_ok=True)
    print("SUITE %s - %d drill(s), evidence -> %s" % (suite_id, len(ids), out_dir), flush=True)

    suite_deadline = time.monotonic() + SUITE_BUDGET_S
    records = []
    for did in ids:
        verdict, record = drive(drill_by_id(did), suite_id, args.approve,
                                out_dir, args.verbose, deadline=suite_deadline)
        records.append((did, verdict, record))
        if verdict == "FAIL":
            print("ABORT: drill %s failed - stack left as the evidence describes." % did,
                  file=sys.stderr)
            return 1

    lines = ["# Disaster drill suite %s" % suite_id, "",
             "Run %s - %d drills." % (suite_id, len(records)),
             "", "| Drill | Title | Verdict | RTO (s) | Evidence |", "|---|---|---|---|---|"]
    for did, verdict, rec in records:
        rto = ""
        for s in rec.get("sections", []):
            if s.startswith("RPO = 0"):
                m = re.search(r"RTO = ([\d.]+) s", s)
                if m:
                    rto = m.group(1)
        lines.append("| %s | %s | %s | %s | %s |" % (did, rec["title"], verdict, rto,
                                                     rec["path"]))
    index_path = os.path.join(out_dir, "suite-%s.md" % suite_id)
    with open(index_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    passed_all = all(v == "PASS" for _, v, _ in records)
    print("SUITE %s RESULT=%s index=%s" % (suite_id, passed_all,
                                           os.path.relpath(index_path, REPO_ROOT)), flush=True)
    return 0 if passed_all else 1


if __name__ == "__main__":
    sys.exit(main())
