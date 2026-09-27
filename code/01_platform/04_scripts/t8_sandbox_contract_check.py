#!/usr/bin/env python3
"""T8 (Phase 5) local-sandbox contract check (offline, credential-free).

Parses docker-compose.yml / .env.example and submit-jobs.sh and asserts the T8
exit-gate properties that are provable WITHOUT a Docker daemon or live cluster:

  1. Private execution network:  `execution-net` exists and is `internal: true`.
  2. Arrow-egress isolation:     `arrow-egress` exists and ONLY execution-bridge
                                 (plus any explicitly allowed peer) attaches.
  3. No public execution route:  gateway/bridge expose zero host `ports:`.
  4. Execution default HALTED:   EXECUTION_BRIDGE_MODE defaults disabled/fake
                                 (never `live`); EXECUTION_ENABLED defaults false;
                                 executor/action-capture remain disabled.
  5. Compute Arrow isolation:    the compute service passes no ARROW_* variable.
  6. No production credentials:  .env.example secrets are blank placeholders.
  7. Checkpoint readiness gate:  submit-jobs.sh waits for a completed Flink
                                 checkpoint (counts.completed > 0) after RUNNING.
  8. Hop matrix — bridge switch: `disabled`/`fake`/`live` branches exist;
                                 `fake` selects the offline FakeBroker (no
                                 Arrow client); `live` requires credentials.
  9. Hop matrix — signed gate:   the executor exposes signed `/v1/approve` +
                                 `/v1/halt` control routes (DEC-044), so a
                                 mode hop still needs a signed approval.
 10. Hop matrix — daily stays:   the daily runner refuses `fake`/`live` and
                                 `EXECUTION_ENABLED=true` (D1: the hop is a
                                 separate sanctioned procedure).
  11. Hop matrix — gateway switch: the gateway's projection intake
                                  (`/v1/events`) has its own variable
                                  (`GATEWAY_EXECUTION_ENABLED`, default false),
                                  decoupled from the executor's boot guard, so a
                                  sanctioned window can open the projection while
                                  the executor still boots `HALTED` (CHG-332).

Exit code is 0 only when every check passes; machine-readable summary on the
last line (prefix `t8-sandbox-contract:`). Mirrors the APP/change-control
contract-check convention. Read-only — never mutates the stack.
"""

import json
import os
import re
import subprocess
import sys

try:
    import yaml  # PyYAML (6.x)
except ImportError:  # pragma: no cover
    print("t8-sandbox-contract: FATAL — PyYAML is required (pip install pyyaml)", file=sys.stderr)
    sys.exit(2)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
COMPOSE = os.path.join(ROOT, "code", "01_platform", "01_docker", "docker-compose.yml")
ENV_EXAMPLE = os.path.join(ROOT, "code", "01_platform", "01_docker", ".env.example")
SUBMIT = os.path.join(ROOT, "code", "02_services", "02_compute", "submit-jobs.sh")
BRIDGE_MAIN = os.path.join(ROOT, "code", "02_services", "06_execution_bridge",
                           "go-bridge", "main.go")
EXECUTOR_HTTP = os.path.join(ROOT, "code", "02_services", "04_executor", "src",
                             "http.rs")

# P6-575: also catch the substitution-default and quoted forms, e.g.
# ${EXECUTION_ENABLED:-true} or EXECUTION_ENABLED: "true".
_EXEC_ENABLED_TRUE_RE = re.compile(
    r"""EXECUTION_ENABLED\s*[:=]\s*(?:['\"]?true['\"]?|\$\{[^:}]+:-\s*['\"]?true['\"]?\})""",
    re.I)

FAILURES = []


def check(name, ok, detail):
    tag = "PASS" if ok else "FAIL"
    print(f"[{tag}] {name}: {detail}")
    if not ok:
        FAILURES.append(name)


def main():
    with open(COMPOSE, "r", encoding="utf-8") as fh:
        compose = yaml.safe_load(fh)

    networks = compose.get("networks", {})
    services = compose.get("services", {})

    # 1. Private execution network
    exnet = networks.get("execution-net", {})
    check(
        "execution-net is private (internal: true)",
        bool(exnet) and exnet.get("internal") is True,
        "internal=True" if exnet.get("internal") is True else "missing/not internal",
    )
    check(
        "arrow-egress network exists",
        "arrow-egress" in networks,
        "present" if "arrow-egress" in networks else "missing",
    )

    # 2. Only execution-bridge joins arrow-egress (direct Arrow isolation).
    arrow_egress_joiners = sorted(
        name for name, svc in services.items()
        if isinstance(svc, dict) and "arrow-egress" in (svc.get("networks") or [])
    )
    check(
        "only execution-bridge on arrow-egress",
        arrow_egress_joiners == ["execution-bridge"],
        f"joiners={arrow_egress_joiners}",
    )

    # 3. No public execution route: gateway/bridge expose no host ports.
    no_port_services = ["execution-bridge", "execution-gateway", "gateway", "nautilus"]
    for sname in no_port_services:
        svc = services.get(sname)
        if not svc:
            continue
        ports = svc.get("ports") or []
        check(
            f"{sname} exposes no host port",
            len(ports) == 0,
            f"ports={ports}" if ports else "no host port",
        )

    # 4. Execution default HALTED / never live.
    bridge = services.get("execution-bridge", {})
    bridge_env = bridge.get("environment") or {}
    mode = isinstance(bridge_env, dict) and bridge_env.get("EXECUTION_BRIDGE_MODE")
    # Resolve Compose shell substitution defaults: ${VAR:-default} -> default.
    resolved_mode = mode
    if isinstance(mode, str):
        m = re.match(r"^\$\{[^:}]+:-([^}]*)\}$", mode)
        if m:
            resolved_mode = m.group(1)
    check(
        "execution-bridge mode defaults disabled/fake (never live)",
        (resolved_mode or "disabled") in ("disabled", "fake") and resolved_mode != "live",
        f"mode={resolved_mode!r}",
    )

    # EXECUTION_ENABLED must never default to true anywhere in compose (live order route).
    composed_text = open(COMPOSE, encoding="utf-8").read()
    check(
        "no EXECUTION_ENABLED default true in compose",
        not _EXEC_ENABLED_TRUE_RE.search(composed_text),
        "scanned compose",
    )
    check(
        "executor/action-capture execution services stay disabled (no live default)",
        not re.search(r"^\s{2}executor:|^\s{2}action-capture:", composed_text, re.M),
        "executor/action-capture remain commented/disabled",
    )

    # 5. Compute Arrow isolation: compute service passes no ARROW_* variable.
    compute = services.get("compute")
    if isinstance(compute, dict):
        compute_env = compute.get("environment") or {}
        arrow_vars = [
            k for k in (compute_env if isinstance(compute_env, dict) else [])
            if str(k).startswith("ARROW_")
        ]
        check(
            "compute passes no ARROW_* variable",
            not arrow_vars,
            f"arrow_vars={arrow_vars}" if arrow_vars else "Arrow-free",
        )

    # 6. No production credentials: .env.example secret placeholders are blank.
    if os.path.exists(ENV_EXAMPLE):
        env_text = open(ENV_EXAMPLE, encoding="utf-8").read()
        secret_keys = [
            "ARROW_APP_ID", "ARROW_APP_SECRET",  "ARROW_USER_ID",
            "ARROW_PASSWORD", "ARROW_TOTP_KEY", "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY", "O2_PASSWORD",
        ]
        embedded = [k for k in secret_keys if _nonblank_env(env_text, k)]
        check(
            ".env.example has no embedded production secrets",
            not embedded,
            f"embedded={embedded}" if embedded else "all placeholder values blank",
        )

    # 7. submit-jobs.sh waits for a completed checkpoint after RUNNING.
    if os.path.exists(SUBMIT):
        sub = open(SUBMIT, encoding="utf-8").read()
        check(
            "submit-jobs.sh waits for a completed checkpoint (counts.completed)",
            "wait_for_checkpoint" in sub and "/checkpoints" in sub,
            "checkpoint poll wired",
        )
        check(
            "submit-jobs.sh never passes Arrow env to jobs",
            "ARROW_" not in sub,
            "no ARROW_* in launcher",
        )

    # 8. Hop matrix — the bridge mode switch exists and `fake` stays offline.
    bridge_src = _read_text(BRIDGE_MAIN)
    if bridge_src is None:
        check("bridge main.go readable", False, f"missing at {BRIDGE_MAIN}")
    else:
        branches = {m: f'case "{m}":' in bridge_src
                    for m in ("disabled", "fake", "live")}
        check(
            "bridge mode switch has disabled/fake/live branches",
            all(branches.values()),
            f"branches={branches}",
        )
        fake_branch = _go_case_body(bridge_src, "fake")
        check(
            "mode=fake selects the offline FakeBroker (no Arrow client)",
            "NewFakeBroker()" in fake_branch
            and "arrow.NewClient" not in fake_branch
            and "NewArrowBroker" not in fake_branch,
            "FakeBroker only" if "NewFakeBroker()" in fake_branch else "branch not found",
        )
        live_branch = _go_case_body(bridge_src, "live")
        check(
            "mode=live requires Arrow credentials (fails closed without)",
            "ARROW_APP_ID" in live_branch and "ARROW_USER_ID" in live_branch
            and "AutoLogin" in live_branch,
            "credentialed AutoLogin path",
        )

    # 9. Hop matrix — gate enablement is a signed DEC-044 action.
    http_src = _read_text(EXECUTOR_HTTP)
    if http_src is None:
        check("executor http.rs readable", False, f"missing at {EXECUTOR_HTTP}")
    else:
        check(
            "executor exposes signed /v1/approve + /v1/halt control routes",
            '"/v1/approve"' in http_src and '"/v1/halt"' in http_src,
            "control routes wired",
        )
        check(
            "control envelopes are signed (GATE_APPROVE/GATE_HALT constants)",
            "GATE_APPROVE" in http_src and "GATE_HALT" in http_src,
            "signed message types present",
        )

    # 10. Hop matrix — the daily runner refuses paper/live enablement (D1).
    #     Behavioral: call the runner's own posture predicate in a subprocess.
    probe = (
        "import json, day_run;"
        "print(json.dumps({"
        "'fake': bool(day_run.posture_violations({'EXECUTION_BRIDGE_MODE': 'fake'})),"
        "'live': bool(day_run.posture_violations({'EXECUTION_BRIDGE_MODE': 'live'})),"
        "'enabled': bool(day_run.posture_violations({'EXECUTION_ENABLED': 'true'})),"
        "'disabled_ok': not day_run.posture_violations("
        "{'EXECUTION_BRIDGE_MODE': 'disabled', 'EXECUTION_ENABLED': 'false'})"
        "}))"
    )
    try:
        r = subprocess.run([sys.executable, "-c", probe], cwd=SCRIPTS_DIR,
                           capture_output=True, text=True, timeout=30)
        data = json.loads(r.stdout.strip().splitlines()[-1]) if r.returncode == 0 else {}
        ok = (data.get("fake") is True and data.get("live") is True
              and data.get("enabled") is True and data.get("disabled_ok") is True)
        detail = ("refused fake/live/enabled" if ok
                  else f"missed={data or (r.stderr or '').strip()[:160]}")
        check("daily runner refuses fake/live/enabled postures (D1)", ok, detail)
    except (subprocess.SubprocessError, ValueError, IndexError) as exc:
        check("daily runner refuses fake/live/enabled postures (D1)", False, str(exc))

    # 11. Hop matrix — gateway projection switch (CHG-332). The gateway's master
    #     switch is its own variable, default false, so a sanctioned window can open
    #     /v1/events while nautilus keeps its boot guard (which refuses true at boot).
    gw = services.get("execution-gateway", {})
    gw_env = gw.get("environment") or {}
    gw_switch = gw_env.get("EXECUTION_ENABLED") if isinstance(gw_env, dict) else None
    naut = services.get("nautilus", {})
    naut_env = naut.get("environment") or {}
    naut_switch = naut_env.get("EXECUTION_ENABLED") if isinstance(naut_env, dict) else None
    check(
        "gateway projection switch decoupled (GATEWAY_EXECUTION_ENABLED, default false)",
        isinstance(gw_switch, str) and "GATEWAY_EXECUTION_ENABLED" in gw_switch
        and _substitution_default(gw_switch) == "false",
        f"gateway EXECUTION_ENABLED={gw_switch!r}",
    )
    check(
        "nautilus boot guard stays on EXECUTION_ENABLED (not the gateway switch)",
        isinstance(naut_switch, str) and "GATEWAY_EXECUTION_ENABLED" not in naut_switch
        and _substitution_default(naut_switch) == "false",
        f"nautilus EXECUTION_ENABLED={naut_switch!r}",
    )

    summary = f"t8-sandbox-contract: all {len(FAILURES) == 0 and 'checks pass' or f'{len(FAILURES)} check(s) FAILED'}"
    print(summary)
    sys.exit(1 if FAILURES else 0)


def _read_text(path):
    """File text, or None when unreadable — a moved file fails a check, not the script."""
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return None


def _substitution_default(value):
    """The default of a `${VAR:-default}` compose value, or None when absent."""
    m = re.match(r"^\$\{[^:}]+:-([^}]*)\}$", str(value))
    return m.group(1) if m else None


def _go_case_body(src, mode):
    """The body of `case "<mode>":` up to the next case/default at the same level."""
    start = src.find(f'case "{mode}":')
    if start == -1:
        return ""
    rest = src[start:]
    nxt = re.search(r'\n\s*(?:case "|default:)', rest)
    return rest[:nxt.start()] if nxt else rest


def _nonblank_env(text, key):
    """True if KEY= has a non-empty, non-comment value (a real embedded secret)."""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        if k.strip() != key or not v.strip():
            continue
        val = v.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\'\"":
            val = val[1:-1].strip()
        # P6-795: a bare self-reference (${VAR} or ${VAR:-}) is a blank
        # placeholder, quoted or not. A real default (${VAR:-value}) is not.
        if re.fullmatch(r"\$\{[A-Za-z_][A-Za-z0-9_]*(?::-)?\}", val):
            continue
        return True
    return False


if __name__ == "__main__":
    main()
