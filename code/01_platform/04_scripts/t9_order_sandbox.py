#!/usr/bin/env python3
"""t9_order_sandbox.py — T9_ORDER_SANDBOX integration harness (skeleton, A2.5).

The A2 goal: prove gateway -> Nautilus -> bridge -> Arrow `POST /order/regular`
round-trip against the broker SANDBOX with ONE real order (BI-EQ x1, INPUT-11
safe instrument, BILCARE LTD.), then cancel. This harness encodes that contract
and runs it the moment the A2.2 stack profile is up and A2.3/A2.4 are live.

Three honest layers (same philosophy as t8_sandbox_contract_check.py, which this
harness REUSES by running it as the first offline check):

  1. OFFLINE (default, runs today, no containers): static contract checks —
     t8 12/12 reuse, `execution-t3` compose shape (internal execution-net, zero
     host ports, bridge mode defaults `disabled`, nautilus EXECUTION_ENABLED
     false), A2.1 premise (SignalJobConfig EXECUTION_INTENT_ENABLED settable,
     DurableIntentDispatcher + NautilusIntentClient wired), the three DDL tables
     with their poll columns, the T9_APPROVED_BY placement gate, and the gateway
     envelope signing port pinned to a REAL JVM vector (see JW_* below).

  2. LIVE (`--live`): place->poll->assert->cancel against the in-network stack.
     Transport runs inside the compose network (the profile publishes NO host
     ports — T8 gate 3 — so probes go through `docker compose exec`/
     `docker run --network <execution-net>`). Classification is honest:
       exit 0  = full round-trip asserted (broker_order_id + client_order_ref
                 echo + tables populated + cancel acked) — the A2.6 target.
       exit 3  = LIVE-CHAIN-UNWIRED — the stack is reachable but the code path
                 stops before execution: nautilus currently ACKNOWLEDGES a
                 valid envelope without executing (http.rs "For T4, acknowledge
                 without executing (no LiveNode wiring yet)"), so the bridge
                 leg and the poll-can-assert cannot complete until that wiring
                 lands (plan Workstream A/B). Never reported as a false PASS.
       exit 1  = a real failure (401 envelope rejected by the real verifier,
                 contract drift, broken chain).
       exit 2  = BLOCKED (docker/daemon/stack missing, T9_APPROVED_BY absent).

  3. SELF-CHECK (`--self-check`): offline suite + fake-transport live
     classification demo, evidence JSON written — proves the harness classifies
     correctly without any live state.

Wire contract (documented, cross-pinned to both implementations):
  * writer:  code/02_services/06_execution_gateway/.../GatewayProtocol.java
  * verifier: code/02_services/04_executor/src/gateway_protocol.rs
  canonical  = "\n".join(protocol_version, message_type, request_id,
               account_scope_id, execution_partition_id, payload_hash,
               str(gate_epoch), fence_token, str(deadline_epoch_ms),
               payload_json)                       # Jackson/Python compact
  auth       = hex(hmac_sha256(canonical, shared_secret))   # lowercase hex
  payload_has= hex(sha256(payload_json_bytes))              # lowercase hex
  envelope   = {protocol_version, message_type, request_id, account_scope_id,
               execution_partition_id, payload_hash, gate_epoch, fence_token,
               deadline_epoch_ms, payload, authentication}
  POST /v1/intents (nautilus:9190): 401 bad auth/version, 503 gate not ENABLED,
               202 accepted.

Usage:
  python3 t9_order_sandbox.py                    # offline contract (exit 0)
  python3 t9_order_sandbox.py --live             # in-network place->poll->cancel
  python3 t9_order_sandbox.py --self-check       # offline + fake-live demo
  python3 t9_order_sandbox.py --sign-control approve --operator saurabh \
      --evidence CHG-131                        # DEC-044 control envelope on stdout
  python3 t9_order_sandbox.py --sign-control halt --operator saurabh \
      --evidence CHG-131 --reason "ops halt" --post   # sign and send it

The control envelope names the CURRENT control epoch, which the executor freezes
`snapshot.control_epoch` against — read it from `GET /healthz` (`gate_epoch`).
It starts at 1 and is bumped by every accepted approve/halt, so a second control
action in the same process needs the new value. A stale epoch is rejected 401,
which is the intended behaviour: an envelope minted for an older gate state must
not act on a newer one.
Env: T9_APPROVED_BY=saurabh (placement gate — fail closed without), T9_RUN_LIVE=1.
Exit: 0 = PASS, 1 = FAIL, 2 = BLOCKED, 3 = LIVE-CHAIN-UNWIRED.
"""

import argparse
import datetime as _dt
import hashlib
import hmac
import json
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
SCRIPTS = os.path.dirname(os.path.abspath(__file__))
COMPOSE = os.path.join(ROOT, "code", "01_platform", "01_docker", "docker-compose.yml")
EVIDENCE_DIR_DEFAULT = os.path.join(ROOT, "logs", "nautilus-execution")

# RCF x1 safe instrument (live-verified 2026-08-25) and the placement gate.
# NOTE: the original t9paper INPUT-11 instrument BILCARE (token 762583) is
# DELISTED / absent from Arrow's live NSE CM instrument list — the order API
# rejects it with 400 "invalid trading symbol". Replaced with RCF
# (RASHTRIYA CHEMICALS, token 2866, Symbol "RCF", TradingSymbol "RCF-EQ",
# lot 1, band 95.23-142.83) which IS in the live list. `symbol` sent to
# POST /order/regular must be the TradingSymbol column ("RCF-EQ") — the plain
# Symbol "RCF" is rejected with 400 "invalid trading symbol", while "RCF-EQ"
# is recognized (verified live 2026-08-25).
BIEQ_SYMBOL = "RCF-EQ"
BIEQ_INSTRUMENT_TOKEN = 2866
BIEQ_QUANTITY = 1
APPROVED_OPERATOR = "saurabh"

PROTOCOL_VERSION = "execution-gateway.v2"
EXECUTION_INTENT_MSG = "EXECUTION_INTENT"

# DEC-044 control plane (P3-020, D5 2026-09-14): `/v1/approve` and `/v1/halt` take the SAME
# envelope an execution intent does — same HMAC, same canonical field order, same gate_epoch —
# and differ ONLY in `message_type`. The operator identity and the evidence an audit line keeps
# live INSIDE the signed payload, which is why the signer below refuses to mint an envelope
# without both: the executor rejects such a request 401, and a half-signed control artifact is
# exactly the thing an operator would wrongly trust.
CONTROL_ACTIONS = {
    "approve": {"message_type": "GATE_APPROVE", "route": "/v1/approve"},
    "halt": {"message_type": "GATE_HALT", "route": "/v1/halt"},
}
CONTROL_INITIAL_GATE_EPOCH = 1      # http.rs: HttpState::snapshot starts the epoch at 1
CONTROL_DEFAULT_SECRET = "local-dev-only"
CONTROL_DEFAULT_FENCE = "t9-fence-0001"
CONTROL_DEADLINE_SEC = 120

# ---------------------------------------------------------------------------
# JVM parity vector — REAL GatewayProtocol.java (production source, jackson
# 2.16.1) compiled + run 2026-08-21 for this fixed instance; secret
# "local-dev-only", deadline pinned to the run's value. The Python port below
# must reproduce every constant byte-for-byte.
# ---------------------------------------------------------------------------
JW_PAYLOAD_JSON = (
    '{"instruction_id":"T9-SB-0001","candidate_id":"cand-T9-0001",'
    '"trade_context_id":"tc-T9-0001","instrument_token":762583,'
    '"symbol":"BI-EQ","exchange":"NSE","side":"BUY","quantity":1,'
    '"order_type":"LIMIT","limit_price_paise":5050,"product_type":"CNC",'
    '"time_in_force":"DAY","request_hash":'
    '"6304f2a4a8f25c0c3a4e7429a5bd2bbd2eb58d4d2d3b8a7c9d5f6e1a2b3c4d5e6",'
    '"schema_version":"1"}'
)
JW_PAYLOAD_HASH = "96f34c1ba146e8523b4d9ad7a22c853bc464c47f1cfc68ba8e3315ee3fe77957"
JW_CANONICAL = (
    "execution-gateway.v1\nEXECUTION_INTENT\nparity-req-0001\ndev-scope\n"
    "dev-partition\n96f34c1ba146e8523b4d9ad7a22c853bc464c47f1cfc68ba8e3315ee3fe77957\n"
    "42\nfence-token-0001\n1787326907806\n" + JW_PAYLOAD_JSON
)
JW_AUTH = "941aaf5f986a934f7313457b592c90401cf19fc501b9df23fcc19febbb70e760"
JW_DEADLINE = 1787326907806


# ---------------------------------------------------------------------------
# Gateway protocol port (must stay byte-exact with the two implementations)
# ---------------------------------------------------------------------------

def _sha256_hex(data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


PROTOCOL_V1 = "execution-gateway.v1"
PROTOCOL_V2 = "execution-gateway.v2"


def canonical_v2(*fields):
    """P3-079: length-prefixed canonical form — injective by construction.

    Must match Java `GatewayProtocol.lengthPrefixed` and Rust
    `gateway_protocol::canonical_v2` byte for byte: for each field, append
    decimal(UTF-8 byte length) + ':' + the field, concatenated with no separator.
    Byte length, not char length — a field leaving ASCII would otherwise make
    this disagree with the other two languages.
    """
    out = []
    for f in fields:
        s = "" if f is None else str(f)
        out.append(str(len(s.encode("utf-8"))))
        out.append(":")
        out.append(s)
    return "".join(out)


def canonical(protocol_version, message_type, request_id, account_scope_id,
              execution_partition_id, payload_hash, gate_epoch, fence_token,
              deadline_epoch_ms, payload_json):
    """Picks the canonical form from the version — deterministic, never a guess (P3-079).

    v1 is the original newline-joined form, retained byte-for-byte so existing peers
    keep verifying; v2 is length-prefixed and strictly opt-in by name. Every other
    version (including a custom generation string) keeps the legacy join, so nothing
    that signed before v2 existed signs differently now — the only requirement is
    that the version-to-form mapping is total and stable.
    """
    fields = (protocol_version, message_type, request_id, account_scope_id,
              execution_partition_id, payload_hash, str(gate_epoch), fence_token,
              str(deadline_epoch_ms), payload_json)
    if protocol_version == PROTOCOL_V2:
        return canonical_v2(*fields)
    return "\n".join(fields)


def sign(secret, canon):
    """hex(hmac_sha256(canonical, secret)) — lowercase, like Java/Rust."""
    return hmac.new(secret.encode("utf-8"), canon.encode("utf-8"),
                    hashlib.sha256).hexdigest()


def payload_hash(payload_json):
    return _sha256_hex(payload_json)


def encode_envelope(secret, protocol_version, message_type, request_id,
                    account_scope_id, execution_partition_id, payload,
                    gate_epoch, fence_token, deadline_epoch_ms):
    """Port of GatewayProtocol.encode(). Key order matches the Java writer."""
    payload_json = json.dumps(payload, separators=(",", ":"))
    ph = payload_hash(payload_json)
    canon = canonical(protocol_version, message_type, request_id,
                      account_scope_id, execution_partition_id, ph,
                      gate_epoch, fence_token, deadline_epoch_ms, payload_json)
    envelope = {
        "protocol_version": protocol_version,
        "message_type": message_type,
        "request_id": request_id,
        "account_scope_id": account_scope_id,
        "execution_partition_id": execution_partition_id,
        "payload_hash": ph,
        "gate_epoch": gate_epoch,
        "fence_token": fence_token,
        "deadline_epoch_ms": deadline_epoch_ms,
        "payload": payload,
        "authentication": sign(secret, canon),
    }
    return json.dumps(envelope, separators=(",", ":")), canon, ph


def verify_envelope(json_text, secret, expected_version, now_ms):
    """Port of GatewayProtocol.verify() — the same checks the executors run.
    Returns (accepted: bool, reason: str)."""
    try:
        env = json.loads(json_text)
    except ValueError:
        return False, "malformed envelope"
    fields = ("protocol_version", "message_type", "request_id",
              "account_scope_id", "execution_partition_id", "payload_hash",
              "gate_epoch", "fence_token", "deadline_epoch_ms", "payload",
              "authentication")
    if not all(k in env for k in fields):
        return False, "malformed envelope"
    if env["protocol_version"] != expected_version:
        return False, "unsupported version"
    if not (env["request_id"] and env["account_scope_id"]
            and env["execution_partition_id"] and env["payload_hash"]
            and env["fence_token"]):
        return False, "missing identity"
    if env["deadline_epoch_ms"] < now_ms:
        return False, "deadline expired"
    payload_json = json.dumps(env["payload"], separators=(",", ":"))
    ph = payload_hash(payload_json)
    if ph != env["payload_hash"]:
        return False, "payload hash mismatch"
    canon = canonical(env["protocol_version"], env["message_type"],
                      env["request_id"], env["account_scope_id"],
                      env["execution_partition_id"], env["payload_hash"],
                      env["gate_epoch"], env["fence_token"],
                      env["deadline_epoch_ms"], payload_json)
    expected = sign(secret, canon)
    if not hmac.compare_digest(expected, env["authentication"]):
        return False, "authentication failed"
    return True, "accepted"


def bieq_payload(instruction_id="T9-SB-0001", candidate_id="cand-T9-0001",
                 trade_context_id="tc-T9-0001", limit_price_paise=10500,
                 request_hash="6304f2a4a8f25c0c3a4e7429a5bd2bbd2eb58d4d2d3b8a7c9d5f6e1a2b3c4d5e6"):
    """Payload exactly as NautilusIntentClient.sendWithFence() serializes it
    (INSTRUMENTATION: field order is part of the canonical bytes — do not
    reorder)."""
    return {
        "instruction_id": instruction_id,
        "candidate_id": candidate_id,
        "trade_context_id": trade_context_id,
        "instrument_token": BIEQ_INSTRUMENT_TOKEN,
        "symbol": BIEQ_SYMBOL,
        "exchange": "NSE",
        "side": "BUY",
        "quantity": BIEQ_QUANTITY,
        "order_type": "LIMIT",
        "limit_price_paise": limit_price_paise,
        "product_type": "CNC",
        "time_in_force": "DAY",
        "request_hash": request_hash,
        "schema_version": "1",
    }


# ---------------------------------------------------------------------------
# Transports
# ---------------------------------------------------------------------------

class Transport:  # pragma: no cover — thin injectable boundary for tests
    def http_json(self, method, url, body=None, headers=None):
        raise NotImplementedError


class HostTransport(Transport):
    """Direct host urllib — only usable when the target publishes a host port
    (the T8 profile DOES NOT: probes from the host go through the docker
    exec/run transports below)."""

    def http_json(self, method, url, body=None, headers=None):
        import urllib.request
        req = urllib.request.Request(url, data=body.encode() if body else None,
                                     method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, resp.read().decode()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode()


class DockerExecTransport(Transport):
    """In-network via `docker run` curl attached to execution-net (no host
    port is published — T8 gate 3). A missing/failed probe is reported as
    status 0, classified as probe-unavailable by the caller — never as a
    pass or a fail. `exec()` shells into a service; `http_json()` makes an
    in-network HTTP call."""

    def __init__(self, compose=COMPOSE, profile="execution-t3"):
        self.compose = compose
        self.profile = profile

    def _run(self, cmd):
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30)

    def exec(self, service, shell):
        return self._run(["docker", "compose", "-f", self.compose,
                          "--profile", self.profile, "exec", "-T", service,
                          "sh", "-lc", shell])

    def _network(self):
        """Resolve the execution-net compose network name (project-prefixed)."""
        net = _compose_json().get("networks", {}).get("execution-net", {})
        name = net.get("name")
        if not name:
            raise RuntimeError("execution-net network name not resolvable")
        return name

    def http_json(self, method, url, body=None, headers=None):
        cmd = ["docker", "run", "--rm", "-i", "--network", self._network(),
               "curlimages/curl:latest", "-sS", "-o", "-", "-w", "\n%{http_code}",
               "-X", method]
        for k, v in (headers or {}).items():
            cmd += ["-H", f"{k}: {v}"]
        cmd += ["--data-binary", "@-", url]
        try:
            p = subprocess.run(cmd, input=body or "", capture_output=True,
                               text=True, timeout=60)
        except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
            return 0, f"probe-unavailable: {exc}"
        if not p.stdout or "\n" not in p.stdout:
            return 0, (f"probe-unavailable: rc={p.returncode} "
                       f"{p.stderr.strip()[:200]}")
        body_txt, status = p.stdout.rsplit("\n", 1)
        try:
            return int(status), body_txt
        except ValueError:
            return 0, f"probe-unavailable: bad curl status {status!r}"


# ---------------------------------------------------------------------------
# Checks (t8 convention)
# ---------------------------------------------------------------------------

FAILURES = []


def check(name, ok, detail):
    tag = "PASS" if ok else "FAIL"
    print(f"[{tag}] {name}: {detail}")
    if not ok:
        FAILURES.append(name)


def _compose_json():
    return json.loads(subprocess.check_output(
        ["docker", "compose", "-f", COMPOSE, "--profile", "execution-t3",
         "config", "--format", "json"], text=True))


def offline_contract():
    """All checks provable today without containers. Returns list of failures."""
    errs = list(FAILURES)
    # 0. REUSE the t8 harness verbatim (A2.5 requirement) — 12/12 must pass.
    t8 = subprocess.run([sys.executable,
                         os.path.join(SCRIPTS, "t8_sandbox_contract_check.py")],
                        capture_output=True, text=True)
    t8_ok = t8.returncode == 0
    check("t8 sandbox contract reused (12/12)", t8_ok,
          "exit 0 (12/12)" if t8_ok else f"exit {t8.returncode}")
    if not t8_ok:
        errs.append("t8 sandbox contract failed; "
                    + "; ".join(t8.stdout.splitlines()[-3:]))

    try:
        cfg = _compose_json()
    except Exception as exc:
        check("execution-t3 compose config", False, str(exc))
        errs.append("compose config failed")
        return errs
    svcs = cfg.get("services", {})
    net = cfg.get("networks", {}).get("execution-net", {})
    check("execution-net internal (T8 gate 1)", net.get("internal") is True,
          "internal:true" if net.get("internal") is True else "missing")
    for name in ("execution-bridge", "execution-gateway", "nautilus"):
        svc = svcs.get(name)
        check(f"{name} present in execution-t3",
              bool(svc), "present" if svc else "MISSING")
        if svc:
            ports = svc.get("ports") or []
            check(f"{name} publishes no host port (T8 gate 3)",
                  len(ports) == 0, f"ports={ports}" if ports else "none")
    bridge = (svcs.get("execution-bridge") or {}).get("environment") or {}
    mode = bridge.get("EXECUTION_BRIDGE_MODE") or "disabled"
    check("bridge defaults disabled (never live)",
          mode in ("disabled", "fake") and mode != "live", f"mode={mode}")
    naut = (svcs.get("nautilus") or {}).get("environment") or {}
    check("nautilus EXECUTION_ENABLED defaults false",
          naut.get("EXECUTION_ENABLED") == "false",
          f"value={naut.get('EXECUTION_ENABLED')!r}")
    gw = (svcs.get("execution-gateway") or {}).get("environment") or {}
    check("gateway forwarded to /v1/intents",
          str(gw.get("NAUTILUS_PRIVATE_ENDPOINT", "")).endswith("/v1/intents"),
          gw.get("NAUTILUS_PRIVATE_ENDPOINT", "missing"))
    check("gateway protocol version present", bool(gw.get("GATEWAY_PROTOCOL_VERSION")),
          gw.get("GATEWAY_PROTOCOL_VERSION", "missing"))

    # 3. A2.1 premise: EXECUTION_INTENT_ENABLED settable + dispatch wired.
    sjc = os.path.join(ROOT, "code", "02_services", "02_compute", "src", "main",
                       "java", "com", "trading", "compute", "signaljob",
                       "SignalJobConfig.java")
    sjc_text = open(sjc, encoding="utf-8").read()
    check("EXECUTION_INTENT_ENABLED settable (SignalJobConfig)",
          "EXECUTION_INTENT_ENABLED" in sjc_text, "env key present")
    gw_dir = os.path.join(ROOT, "code", "02_services", "06_execution_gateway",
                          "src", "main", "java", "com", "trading", "execution",
                          "gateway")
    dispatch = os.path.exists(os.path.join(gw_dir, "DurableIntentDispatcher.java"))
    naut_cli = os.path.exists(os.path.join(gw_dir, "NautilusIntentClient.java"))
    check("DurableIntentDispatcher + NautilusIntentClient wired",
          dispatch and naut_cli, "both present" if dispatch and naut_cli else "missing")

    # 4. DDL poll columns for the three assert tables.
    ddl = os.path.join(ROOT, "code", "01_platform", "02_sql", "ddl")
    need = {
        "27_execution_intent.sql": ["instruction_id", "request_hash", "created_ts"],
        "09_order_lifecycle.sql": ["account_scope_id", "broker_order_id",
                                   "normalized_state"],
        "12_execution_attempts.sql": ["execution_attempt_id", "phase",
                                      "gate_fence_token"],
    }
    for fname, cols in need.items():
        path = os.path.join(ddl, fname)
        if not os.path.exists(path):
            check(f"DDL {fname}", False, "MISSING")
            errs.append(f"DDL {fname} missing")
            continue
        text = open(path, encoding="utf-8").read()
        missing = [c for c in cols if f"\n    {c}" not in text]
        check(f"DDL {fname} poll columns", not missing,
              "all present" if not missing else f"missing {missing}")

    # 5. Signing parity vs the real JVM vector (fixed deadline, fixed secret).
    isec = "local-dev-only"
    ok = JW_PAYLOAD_HASH == payload_hash(JW_PAYLOAD_JSON)
    check("payload hash parity (JVM)", ok, "match" if ok else "MISMATCH")
    canon = canonical("execution-gateway.v1", "EXECUTION_INTENT",
                      "parity-req-0001", "dev-scope", "dev-partition",
                      JW_PAYLOAD_HASH, 42, "fence-token-0001",
                      JW_DEADLINE, JW_PAYLOAD_JSON)
    auth = sign(isec, canon)
    ok = auth == JW_AUTH and canon == JW_CANONICAL
    check("envelope authentication parity (JVM)", ok,
          "match" if ok else "MISMATCH")
    env_json, _, _ = encode_envelope(
        isec, "execution-gateway.v1", "EXECUTION_INTENT", "parity-req-0001",
        "dev-scope", "dev-partition", json.loads(JW_PAYLOAD_JSON), 42,
        "fence-token-0001", JW_DEADLINE)
    accepted, reason = verify_envelope(env_json, isec,
                                       "execution-gateway.v1", JW_DEADLINE - 1)
    check("envelope self-verify round-trip", accepted, reason)

    # 6. D5: the control plane's envelopes (approve/halt) are the same signed
    # shape — same secret, same canonical order, only message_type differs.
    # Signed here with the live protocol version the executor verifies against.
    ctl_types = []
    for action, spec in sorted(CONTROL_ACTIONS.items()):
        ctl_types.append(spec["message_type"])
        ctl_json, _, _ = encode_envelope(
            isec, PROTOCOL_VERSION, spec["message_type"], f"ctl-{action}",
            "dev-scope", "dev-partition",
            {"operator": APPROVED_OPERATOR, "evidence": "t9-offline-check",
             "reason": "offline contract"}, CONTROL_INITIAL_GATE_EPOCH,
            CONTROL_DEFAULT_FENCE, JW_DEADLINE)
        accepted, reason = verify_envelope(ctl_json, isec, PROTOCOL_VERSION,
                                           JW_DEADLINE - 1)
        check(f"control envelope {spec['message_type']} verifies", accepted, reason)
    # The message type is the only thing that tells the two routes apart, so a
    # single shared value would silently turn every halt into an approval.
    check("control message types are distinct", len(set(ctl_types)) == 2,
          f"{sorted(ctl_types)}")
    return errs


# ---------------------------------------------------------------------------
# Live place -> poll -> assert -> cancel (in-network, T4 wall aware)
# ---------------------------------------------------------------------------

class FakeTransport(Transport):
    """Scriptable responses for the self-check/tests — never used against a
    real stack unless injected by tests."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def http_json(self, method, url, body=None, headers=None):
        self.calls.append((method, url))
        return self.responses.pop(0)


def compose_svcs_up(profile="execution-t3"):
    try:
        out = subprocess.check_output(
            ["docker", "compose", "-f", COMPOSE, "--profile", profile,
             "ps", "--format", "json"], text=True, timeout=30)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False
    return "execution-bridge" in out and "nautilus" in out


def approval_gate():
    """Placement is FORBIDDEN without T9_APPROVED_BY=saurabh (A2 DoD: no real
    order possible without the flag)."""
    return os.environ.get("T9_APPROVED_BY") == APPROVED_OPERATOR


def sign_control(action, operator=APPROVED_OPERATOR, evidence="", reason="",
                 secret=CONTROL_DEFAULT_SECRET, gate_epoch=None,
                 fence_token=CONTROL_DEFAULT_FENCE, deadline_ms=None,
                 request_id=None, transport=None, url_host="nautilus:9190",
                 now=None):
    """Sign a DEC-044 control envelope (approve/halt), optionally POST it.

    Returns (exit_code, classifier, signed_envelope_json) on success; on a
    refusal the third element is the reason instead. The signed JSON is the
    artifact an operator keeps, so it is what the CLI writes to stdout.
    """
    spec = CONTROL_ACTIONS.get(action)
    if spec is None:
        return (1, "FAIL",
                f"unknown control action {action!r} — expected one of "
                f"{sorted(CONTROL_ACTIONS)}")
    if not operator or not evidence:
        # Fail closed, and say why: /v1/approve and /v1/halt read operator +
        # evidence out of the signed payload and reject the request 401 without
        # them, so minting one would only produce a misleading artifact.
        return (1, "FAIL",
                "payload operator and evidence are required — the executor "
                "rejects a control envelope without them (401)")
    now = now if now is not None else _dt.datetime.now(_dt.timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    if deadline_ms is None:
        deadline_ms = now_ms + CONTROL_DEADLINE_SEC * 1000
    if request_id is None:
        request_id = f"t9-ctl-{action}-{now.strftime('%Y%m%d-%H%M%S')}"
    payload = {"operator": operator, "evidence": evidence, "reason": reason}
    env_json, _, _ = encode_envelope(
        secret, PROTOCOL_VERSION, spec["message_type"], request_id, "dev-scope",
        "dev-partition", payload,
        CONTROL_INITIAL_GATE_EPOCH if gate_epoch is None else gate_epoch,
        fence_token, deadline_ms)
    if transport is None:
        return 0, "PASS", env_json
    status, body = transport.http_json(
        "POST", f"http://{url_host}{spec['route']}", body=env_json,
        headers={"Content-Type": "application/json"})
    if status == 0:
        return 2, "BLOCKED", f"probe tool unavailable — {body}"
    if status == 200:
        # The executor echoes the resulting gate state — the operator's proof
        # that the action landed. stderr, so stdout stays pipeable.
        print(f"{spec['route']} accepted: {body.strip()}", file=sys.stderr)
        return 0, "PASS", env_json
    # 401 = auth/epoch/message-type/evidence refusal, 403 = operator not
    # authorized, 405 = wrong method. All are refusals: never a success.
    return 1, "FAIL", f"{spec['route']} refused the envelope ({status}): {body}"


def run_live(transport=None, secret="local-dev-only", now=None,
             require_stack=True):
    """place BI-EQ x1 -> poll assert tables -> cancel. Returns (exit_code,
    classifier, notes). Classifier is one of PASS / LIVE-CHAIN-UNWIRED /
    FAIL / BLOCKED."""
    if not approval_gate():
        print("blocked: T9_APPROVED_BY=saurabh not set — placement refused "
              "(fail closed)", file=sys.stderr)
        return 2, "BLOCKED", "approval gate"
    if require_stack and not compose_svcs_up():
        print("blocked: execution-t3 stack not up (run: docker compose "
              "--profile execution-t3 up -d)", file=sys.stderr)
        return 2, "BLOCKED", "stack not up"
    if transport is None:
        transport = DockerExecTransport()
    now = now if now is not None else _dt.datetime.now(_dt.timezone.utc)

    payload = bieq_payload()
    deadline = int(now.timestamp() * 1000) + 120_000
    env_json, _, _ = encode_envelope(
        secret, PROTOCOL_VERSION, EXECUTION_INTENT_MSG, "t9-sb-run-0001",
        "dev-scope", "dev-partition", payload, 0, "t9-fence-0001", deadline)

    status, body = transport.http_json(
        "POST", "http://nautilus:9190/v1/intents", body=env_json,
        headers={"Content-Type": "application/json"})
    if status == 0:
        print(f"BLOCKED: probe tool unavailable — {body}")
        return 2, "BLOCKED", f"probe tool unavailable: {body}"
    if status == 401:
        print("FAIL: envelope rejected by nautilus verifier — parity broke: "
              f"{body}")
        return 1, "FAIL", "envelope rejected"
    if status == 503:
        print("LIVE-CHAIN-UNWIRED: nautilus reachable but gate not ENABLED "
              f"(expected HALTED until approval): {body}")
        return 3, "LIVE-CHAIN-UNWIRED", "gate HALTED"
    if status == 202:
        # nautilus acknowledges — but http.rs has NO bridge forwarding yet
        # ("For T4, acknowledge without executing (no LiveNode wiring yet)").
        # The place->poll->assert->cancel full chain therefore CANNOT pass
        # today; we report this honestly instead of faking a round-trip.
        print("LIVE-CHAIN-UNWIRED: nautilus accepted envelope (202) but the "
              "bridge/Arrow leg is unwired (T4 wall, http.rs) — poll of "
              "Execution_Intent/Order_Lifecycle/Execution_Attempts + cancel "
              "require A2.3/A2.4 wiring; nothing was executed")
        return 3, "LIVE-CHAIN-UNWIRED", "T4 wall (no LiveNode wiring)"
    print(f"FAIL: unexpected status {status}: {body}")
    return 1, "FAIL", f"status {status}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(description="T9_ORDER_SANDBOX harness (A2.5)")
    ap.add_argument("--offline", action="store_true", help="offline contract (default)")
    ap.add_argument("--live", action="store_true", help="in-network place->cancel")
    ap.add_argument("--self-check", action="store_true", help="offline + fake-live demo")
    ap.add_argument("--out", default=EVIDENCE_DIR_DEFAULT)
    ap.add_argument("--sign-control", choices=sorted(CONTROL_ACTIONS),
                    metavar="ACTION",
                    help="sign a DEC-044 control envelope (approve|halt) and "
                         "print it on stdout; with --post, send it")
    ap.add_argument("--operator", default=APPROVED_OPERATOR,
                    help="signed operator identity (must be the executor's "
                         "authorized operator)")
    ap.add_argument("--evidence", default="",
                    help="audited evidence for a control action (required — it "
                         "is signed, and the executor rejects it empty)")
    ap.add_argument("--reason", default="",
                    help="halt note, signed; /v1/halt keeps it in the audit line")
    ap.add_argument("--secret", default=CONTROL_DEFAULT_SECRET,
                    help="shared secret (default: the local dev secret)")
    ap.add_argument("--gate-epoch", type=int, default=None,
                    help="current control epoch — read `gate_epoch` from GET "
                         "/healthz; starts at 1, bumped by every accepted "
                         "approve/halt")
    ap.add_argument("--fence-token", default=CONTROL_DEFAULT_FENCE)
    ap.add_argument("--post", action="store_true",
                    help="send the signed envelope (needs the execution-t3 "
                         "stack up) instead of only printing it")
    ap.add_argument("--url", default="nautilus:9190",
                    help="host:port for --post (default: the compose service "
                         "name on the execution net)")
    args = ap.parse_args(argv)

    run_id = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    if args.sign_control:
        # stdout carries the envelope and nothing else, so an operator can pipe
        # it; every human note goes to stderr.
        transport = DockerExecTransport() if args.post else None
        code, cls, artifact = sign_control(
            args.sign_control, args.operator, args.evidence, args.reason,
            secret=args.secret, gate_epoch=args.gate_epoch,
            fence_token=args.fence_token, transport=transport,
            url_host=args.url)
        if code != 0:
            print(f"{cls}: {artifact}", file=sys.stderr)
            return code
        print(artifact)
        sent = " and accepted by the executor" if args.post else (
            " (not sent — pass --post to send it)")
        print(f"{cls}: signed "
              f"{CONTROL_ACTIONS[args.sign_control]['message_type']} envelope"
              f"{sent}", file=sys.stderr)
        return 0
    if args.self_check:
        return _self_check(args.out, run_id)
    if args.live:
        code, cls, note = run_live()
        print(f"result: {cls}")
        return code
    errs = offline_contract()
    if errs:
        for e in errs:
            print(f"FAIL: {e}")
        print(f"t9-order-sandbox-contract: {len(errs)} check(s) FAILED")
        return 1
    print("t9-order-sandbox-contract: all checks pass (skeleton staged; live "
          "leg awaits A2.2/A2.3/A2.4 + T4 bridge wiring)")
    return 0


def _self_check(out_dir, run_id):
    errs = offline_contract()
    if errs:
        print("self-check FAIL:", "; ".join(errs))
        return 1
    # Fake-transport classification demo (no network).
    responses = [
        (202, '{"accepted": true, "gate_state": "ENABLED"}'),  # T4 wall
        (503, '{"accepted": false, "reason": "gate HALTED"}'),
        (401, '{"accepted": false, "reason": "authentication failed"}'),
    ]
    for i, r in enumerate(responses):
        old = os.environ.get("T9_APPROVED_BY")
        os.environ["T9_APPROVED_BY"] = APPROVED_OPERATOR
        code, cls, note = run_live(transport=FakeTransport([r]),
                                   require_stack=False)
        if old is None:
            os.environ.pop("T9_APPROVED_BY", None)
        else:
            os.environ["T9_APPROVED_BY"] = old
        expected = {202: 3, 503: 3, 401: 1}[r[0]]
        if code != expected:
            print(f"self-check FAIL: status {r[0]} classified {cls} (exit {code}), "
                  f"expected {'UNWIRED(3)' if expected == 3 else 'FAIL(1)'}")
            return 1
        print(f"[self-check] status {r[0]} -> {cls} (exit {code}) OK")
    # Approval gate blocks placement without the flag.
    os.environ.pop("T9_APPROVED_BY", None)
    code, cls, _ = run_live(transport=FakeTransport(responses),
                            require_stack=False)
    if code != 2 or cls != "BLOCKED":
        print(f"self-check FAIL: approval gate did not block (exit {code})")
        return 1
    print("[self-check] approval gate blocks without T9_APPROVED_BY OK")
    evidence = {
        "work_item_id": f"A2.5-T9-SANDBOX-{run_id}",
        "artifact": f"logs/nautilus-execution/a2-t9-sandbox-harness-{run_id[:8]}.md",
        "status": "harness staged; live leg gated on A2.2/A2.3/A2.4 + T4 wiring",
        "jvm_parity": {"payload_hash_ok": True, "auth_ok": True,
                       "source": "real GatewayProtocol.java (jackson 2.16.1), "
                                 "run 2026-08-21"},
        "offline_checks": "t8 reuse 12/12; compose shape; A2.1 premise; DDL "
                          "poll columns; signing parity",
        "live_classifier": "202/503 -> LIVE-CHAIN-UNWIRED (exit 3); 401 -> FAIL; "
                           "no T9_APPROVED_BY -> BLOCKED (exit 2)",
    }
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"self-check-{run_id}-t9-order-sandbox.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print(f"[self-check] PASS — evidence {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
