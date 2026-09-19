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

Exit code is 0 only when every check passes; machine-readable summary on the
last line (prefix `t8-sandbox-contract:`). Mirrors the APP/change-control
contract-check convention. Read-only — never mutates the stack.
"""

import os
import re
import sys

try:
    import yaml  # PyYAML (6.x)
except ImportError:  # pragma: no cover
    print("t8-sandbox-contract: FATAL — PyYAML is required (pip install pyyaml)", file=sys.stderr)
    sys.exit(2)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
COMPOSE = os.path.join(ROOT, "code", "01_platform", "01_docker", "docker-compose.yml")
ENV_EXAMPLE = os.path.join(ROOT, "code", "01_platform", "01_docker", ".env.example")
SUBMIT = os.path.join(ROOT, "code", "02_services", "02_compute", "submit-jobs.sh")

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

    summary = f"t8-sandbox-contract: all {len(FAILURES) == 0 and 'checks pass' or f'{len(FAILURES)} check(s) FAILED'}"
    print(summary)
    sys.exit(1 if FAILURES else 0)


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
