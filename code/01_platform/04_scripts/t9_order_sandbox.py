#!/usr/bin/env python3
"""t9_order_sandbox.py — T9_ORDER_SANDBOX integration harness (A2.5).

The A2 goal: prove the in-network order path — nautilus `/v1/intents` -> bridge
-> broker — with ONE order (RCF-EQ x1, INPUT-11 safe instrument; BILCARE/BI-EQ
was delisted and replaced 2026-08-25), then cancel. The same command runs
against `EXECUTION_BRIDGE_MODE=fake` (paper drill) and `=live` (sandbox), so the
paper and sandbox runs differ only in the mode value and the funded account.

Three honest layers (same philosophy as t8_sandbox_contract_check.py, which this
harness REUSES by running it as the first offline check):

  1. OFFLINE (default, runs today, no containers): static contract checks —
     t8 contract reuse (all checks), `execution-t3` compose shape (internal
     execution-net, zero host ports, bridge mode defaults `disabled`, nautilus
     EXECUTION_ENABLED false, gateway projection switch decoupled as
     GATEWAY_EXECUTION_ENABLED false), A2.1 premise (SignalJobConfig
     EXECUTION_INTENT_ENABLED settable, DurableIntentDispatcher +
     NautilusIntentClient wired), the three DDL tables with their poll columns,
     the T9_APPROVED_BY placement gate, and the gateway envelope signing port
     pinned to a REAL JVM vector (see JW_* below).

  2. LIVE (`--live`): /healthz gate+epoch -> place -> poll -> cancel against
     the in-network stack. Transport runs inside the compose network (the
     profile publishes NO host ports — T8 gate 3 — so probes go through
     `docker compose exec`/`docker run --network <execution-net>`), and the
     table poll reuses the maintained `FlussReadLagProbe` log-end counter (the
     same host pattern day_run.py uses). The hop also opens the gateway's
     projection intake (`GATEWAY_EXECUTION_ENABLED=true`, CHG-332) so the
     route's emitted lifecycle event lands in `Order_Lifecycle` — the required
     table. Classification is honest:
       exit 0  = full round-trip asserted (broker_order_id + Order_Lifecycle
                 appended + cancel acked) — A2.6 target.
       exit 3  = LIVE-CHAIN-UNWIRED — the stack is reachable but the gate is not
                 ENABLED (approve first with `--sign-control approve --post`) or
                 the bridge answered HALTED; never a false PASS.
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
  payload_hash= hex(sha256(payload_json_bytes))              # lowercase hex
  envelope   = {protocol_version, message_type, request_id, account_scope_id,
               execution_partition_id, payload_hash, gate_epoch, fence_token,
               deadline_epoch_ms, payload, authentication}
  POST /v1/intents (nautilus:9190): 401 bad auth/version, 503 gate not ENABLED,
               202 accepted.

Usage:
  python3 t9_order_sandbox.py                    # offline contract (exit 0)
  python3 t9_order_sandbox.py --live             # in-network place->poll->cancel (gate ENABLED + gateway intake required)
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
Exit: 0 = PASS, 1 = FAIL, 2 = BLOCKED, 3 = LIVE-CHAIN-UNWIRED (gate not ENABLED).
"""

import argparse
import datetime as _dt
import hashlib
import hmac
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
SCRIPTS = os.path.dirname(os.path.abspath(__file__))
COMPOSE = os.path.join(ROOT, "code", "01_platform", "01_docker", "docker-compose.yml")
EVIDENCE_DIR_DEFAULT = os.path.join(ROOT, "logs", "nautilus-execution")

# Live-leg table assertions: the required table must show appended rows after a
# 202; the others are reported. Order_Lifecycle is the gateway projection of the
# route's emitted lifecycle event — the only Fluss table this direct-to-nautilus
# route can grow (Execution_Attempts belongs to the gateway's own intent path,
# and nautilus keeps its attempt store in a file; CHG-331/CHG-332).
FLUSS_PROBE_SRC = os.path.join(SCRIPTS, "fluss-probes", "FlussReadLagProbe.java")
FLUSS_CP_FILE = os.path.join(ROOT, "code", "02_services", "01_ingestion",
                             "target", "cp.txt")
LIVE_ASSERT_TABLES = ("Order_Lifecycle",)
LIVE_OPTIONAL_TABLES = ("Execution_Attempts", "Fills", "Positions")
LIVE_POLL_TIMEOUT_S = 30
LIVE_POLL_INTERVAL_S = 2

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
    # P6-577: a keyed-but-ill-typed envelope crashed on the comparisons below
    # (deadline compare, hmac.compare_digest) instead of reporting a bad envelope.
    if not all(isinstance(env[k], str) for k in fields
               if k not in ("gate_epoch", "deadline_epoch_ms", "payload")):
        return False, "malformed envelope"
    if any(isinstance(env[k], bool) or not isinstance(env[k], int)
           for k in ("gate_epoch", "deadline_epoch_ms")):
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
        # A GET carries no body: --data-binary would force Content-Length: 0
        # (and a form content-type) onto the health read.
        if body is not None:
            cmd += ["--data-binary", "@-"]
        cmd += [url]
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


def _all_failures(errs):
    """P6-221: FAILURES grows while the checks run, but errs is the snapshot
    taken at entry — returning errs alone silently dropped later failures."""
    return list(dict.fromkeys([*errs, *FAILURES]))


def _read_text(path):
    """P6-797: a moved file must fail a check, not kill the harness."""
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read()
    except OSError:
        return None


def offline_contract():
    """All checks provable today without containers. Returns list of failures."""
    errs = list(FAILURES)
    # 0. REUSE the t8 harness verbatim (A2.5 requirement) — every check passes.
    t8 = subprocess.run([sys.executable,
                         os.path.join(SCRIPTS, "t8_sandbox_contract_check.py")],
                        capture_output=True, text=True)
    t8_ok = t8.returncode == 0
    t8_tail = (t8.stdout or "").strip().splitlines()
    check("t8 sandbox contract reused", t8_ok,
          (t8_tail[-1] if t8_ok and t8_tail else f"exit {t8.returncode}"))
    if not t8_ok:
        errs.append("t8 sandbox contract failed; "
                    + "; ".join(t8.stdout.splitlines()[-3:]))

    try:
        cfg = _compose_json()
    except Exception as exc:
        check("execution-t3 compose config", False, str(exc))
        errs.append("compose config failed")
        return _all_failures(errs)
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
    sjc_text = _read_text(sjc)
    if sjc_text is None:
        check("SignalJobConfig.java readable", False, f"missing at {sjc}")
        return _all_failures(errs)
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
    return _all_failures(errs)


# ---------------------------------------------------------------------------
# Live gate check -> place -> poll -> cancel (in-network)
# ---------------------------------------------------------------------------

class FakeTransport(Transport):
    """Scriptable responses for the self-check/tests — never used against a
    real stack unless injected by tests."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.bodies = []

    def http_json(self, method, url, body=None, headers=None):
        self.calls.append((method, url))
        self.bodies.append(body)
        return self.responses.pop(0)


class FlussLogEndProbe:
    """Reads a table's total appended-record count via the maintained
    fluss-probes/FlussReadLagProbe (the same host pattern day_run.py uses:
    classpath from the ingestion target, bootstrap localhost:9123).

    Returns (count, error): a missing classpath, a failed compile, a missing
    table or a failed read all come back as (None, reason) — never an
    exception, so the caller can classify BLOCKED honestly.
    """

    def __init__(self, workdir=None, bootstrap="localhost:9123"):
        self.workdir = workdir or tempfile.mkdtemp(prefix="t9-fluss-probe-")
        self.bootstrap = bootstrap
        self._cp = None

    def _classpath(self):
        if self._cp is None:
            try:
                with open(FLUSS_CP_FILE, encoding="utf-8") as fh:
                    self._cp = fh.read().strip()
            except OSError:
                self._cp = ""
        return self._cp or None

    def log_end(self, table):
        cp = self._classpath()
        if not cp:
            return None, (f"probe-unavailable: classpath missing ({FLUSS_CP_FILE}); "
                          "run `make test` to build it")
        cls = os.path.join(self.workdir, "FlussReadLagProbe.class")
        if not os.path.exists(cls) or os.path.getmtime(cls) < os.path.getmtime(FLUSS_PROBE_SRC):
            javac = shutil.which("javac")
            if not javac:
                return None, "probe-unavailable: javac not found on PATH"
            try:
                r = subprocess.run([javac, "-cp", cp, "-d", self.workdir,
                                    FLUSS_PROBE_SRC],
                                   capture_output=True, text=True, timeout=120)
            except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
                return None, f"probe-unavailable: compile failed: {exc}"
            if r.returncode != 0:
                return None, f"probe-unavailable: compile failed: {r.stderr[:200]}"
        try:
            p = subprocess.run(
                ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED",
                 "--add-opens=java.base/java.nio=ALL-UNNAMED",
                 "-cp", f"{self.workdir}:{cp}", "FlussReadLagProbe",
                 "default", table, self.bootstrap],
                capture_output=True, text=True, timeout=30)
        except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
            return None, f"probe-unavailable: {exc}"
        lines = [ln for ln in (p.stdout or "").splitlines() if ln.strip()]
        if p.returncode not in (0, 3) or not lines:
            return None, f"probe failed: {(p.stderr or 'no sample').strip()[:200]}"
        parts = lines[-1].split("\t")
        if len(parts) < 5:
            return None, f"probe failed: unexpected output {lines[-1][:120]!r}"
        try:
            return int(parts[-1]), ""
        except ValueError:
            return None, f"probe failed: bad count {parts[-1]!r}"


class FakeFlussProbe:
    """Programmed counts per table per read (self-check/tests — never live).

    `samples[table]` is a list of counts consumed in order; the last value
    repeats once the list is down to one entry, so a poll loop terminates
    deterministically. A missing/None entry reports probe-unavailable/failed.
    """

    def __init__(self, samples):
        self.samples = {k: list(v) for k, v in samples.items()}
        self.calls = []

    def log_end(self, table):
        self.calls.append(table)
        series = self.samples.get(table)
        if not series:
            return None, f"probe-unavailable: no programmed sample for {table}"
        value = series.pop(0) if len(series) > 1 else series[0]
        if value is None:
            return None, f"probe failed: {table} unavailable"
        return value, ""


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


def _read_log_end(probe, table):
    """(count, error) from a probe; a broken probe is BLOCKED, not a traceback."""
    try:
        return probe.log_end(table)
    except Exception as exc:  # noqa: BLE001 — the boundary must classify
        return None, f"probe-unavailable: {exc}"


def run_live(transport=None, probe=None, secret="local-dev-only", now=None,
             require_stack=True, poll_timeout_s=LIVE_POLL_TIMEOUT_S,
             out_dir=None, run_id=None):
    """Gate check -> place RCF-EQ x1 -> poll the assert tables -> cancel.

    Returns (exit_code, classifier, notes). Classifier is one of PASS /
    LIVE-CHAIN-UNWIRED / FAIL / BLOCKED. The gate state and control epoch are
    READ from /healthz — never assumed; a HALTED gate is refused before any
    envelope is signed (approve first via --sign-control approve --post).
    """
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
    if probe is None:
        probe = FlussLogEndProbe()
    now = now if now is not None else _dt.datetime.now(_dt.timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    run_tag = run_id or now.strftime("%Y%m%d-%H%M%S")

    # 1. Gate state + control epoch from /healthz (never assumed).
    status, body = transport.http_json("GET", "http://nautilus:9190/healthz")
    if status == 0:
        return 2, "BLOCKED", f"probe tool unavailable — {body}"
    if status != 200:
        return 1, "FAIL", f"/healthz returned {status}: {body[:200]}"
    try:
        health = json.loads(body)
    except ValueError:
        return 1, "FAIL", f"/healthz not JSON: {body[:120]}"
    gate = str(health.get("gate_state", "")).upper()
    epoch = health.get("gate_epoch")
    if gate != "ENABLED":
        print(f"LIVE-CHAIN-UNWIRED: gate {gate or 'UNKNOWN'} — approve first: "
              f"t9_order_sandbox.py --sign-control approve --operator "
              f"{APPROVED_OPERATOR} --evidence <CHG> --post", file=sys.stderr)
        return 3, "LIVE-CHAIN-UNWIRED", f"gate {gate or 'UNKNOWN'}"
    if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
        return 1, "FAIL", f"/healthz gate_epoch unusable: {epoch!r}"

    # 2. Baseline log-end counts (maintained FlussReadLagProbe, day_run pattern).
    baseline = {}
    for table in LIVE_ASSERT_TABLES + LIVE_OPTIONAL_TABLES:
        baseline[table] = _read_log_end(probe, table)
    unreadable = [t for t in LIVE_ASSERT_TABLES if baseline[t][0] is None]
    if unreadable:
        return 2, "BLOCKED", ("required table(s) unreadable: "
                              + "; ".join(f"{t}: {baseline[t][1]}" for t in unreadable))

    # 3. Place. Fresh instruction identity per run: a retry of a consumed
    #    identity is answered from the durable record (202/409), and re-using an
    #    instruction with a different payload is a contract violation — never a
    #    second order (claim_for_send, http.rs).
    instruction_id = f"T9-SB-{run_tag}"
    request_hash = hashlib.sha256(f"place|{run_tag}|{now_ms}".encode()).hexdigest()
    payload = bieq_payload(instruction_id=instruction_id, request_hash=request_hash)
    env_json, _, _ = encode_envelope(
        secret, PROTOCOL_VERSION, EXECUTION_INTENT_MSG, f"t9-sb-{run_tag}",
        "dev-scope", "dev-partition", payload, epoch, "t9-fence-0001",
        now_ms + 120_000)
    status, body = transport.http_json(
        "POST", "http://nautilus:9190/v1/intents", body=env_json,
        headers={"Content-Type": "application/json"})
    place = {"status": status, "body": body}
    if status == 0:
        return 2, "BLOCKED", f"probe tool unavailable — {body}"
    if status == 401:
        return 1, "FAIL", f"envelope rejected by nautilus verifier: {body[:200]}"
    if status == 409:
        return 1, "FAIL", f"place rejected: {body[:200]}"
    if status == 503:
        try:
            doc = json.loads(body)
        except ValueError:
            doc = {}
        if str(doc.get("gate_state", "")).upper() == "HALTED":
            return 3, "LIVE-CHAIN-UNWIRED", "gate HALTED"
        return 1, "FAIL", f"bridge UNKNOWN: {body[:200]}"
    if status != 202:
        return 1, "FAIL", f"unexpected status {status}: {body[:200]}"
    try:
        doc = json.loads(body)
    except ValueError:
        return 1, "FAIL", f"202 body not JSON: {body[:120]}"
    broker_order_id = str(doc.get("broker_order_id", "")).strip()
    if not doc.get("accepted") or not broker_order_id:
        return 1, "FAIL", f"202 without accepted+broker_order_id: {body[:200]}"
    emission = str(doc.get("event_emission", ""))
    if emission.lower().startswith("failed"):
        return 1, "FAIL", ("gateway refused the lifecycle event: " + emission
                           + " — the hop needs the gateway projection intake "
                             "(GATEWAY_EXECUTION_ENABLED=true, CHG-332)")
    print(f"place accepted: broker_order_id={broker_order_id} "
          f"attempt={doc.get('execution_attempt_id', '')} "
          f"event_emission={doc.get('event_emission', '')}", file=sys.stderr)

    # 4. Poll until the required tables show the appended rows (bounded).
    samples = []
    deadline_mono = time.monotonic() + max(0.0, poll_timeout_s)
    grown = False
    while True:
        round_counts = {t: _read_log_end(probe, t)[0] for t in LIVE_ASSERT_TABLES}
        samples.append(dict(round_counts))
        grown = all(
            round_counts[t] is not None
            and baseline[t][0] is not None
            and round_counts[t] > baseline[t][0]
            for t in LIVE_ASSERT_TABLES)
        if grown or time.monotonic() >= deadline_mono:
            break
        time.sleep(LIVE_POLL_INTERVAL_S)
    if not grown:
        return 1, "FAIL", ("no append observed in " + "/".join(LIVE_ASSERT_TABLES)
                           + f" within {poll_timeout_s}s: {samples}")

    # Optional tables are reported, not required (the fake lifecycle may not
    # fill/position; only the live broker session settles that).
    optional = {}
    for table in LIVE_OPTIONAL_TABLES:
        count, err = _read_log_end(probe, table)
        optional[table] = {"before": baseline[table][0], "after": count,
                           "error": err}

    # 5. Cancel with a DISTINCT instruction id: same instruction + different
    #    payload is a contract violation (halts), not a retry.
    cancel_payload = dict(payload)
    cancel_payload.update({
        "instruction_id": f"{instruction_id}-C",
        "action": "cancel",
        "broker_order_id": broker_order_id,
        "request_hash": hashlib.sha256(
            f"cancel|{run_tag}|{broker_order_id}".encode()).hexdigest(),
    })
    cancel_env, _, _ = encode_envelope(
        secret, PROTOCOL_VERSION, EXECUTION_INTENT_MSG, f"t9-sb-{run_tag}-c",
        "dev-scope", "dev-partition", cancel_payload, epoch, "t9-fence-0001",
        now_ms + 120_000)
    status, body = transport.http_json(
        "POST", "http://nautilus:9190/v1/intents", body=cancel_env,
        headers={"Content-Type": "application/json"})
    cancel = {"status": status, "body": body}
    if status != 202:
        return 1, "FAIL", f"cancel not acknowledged ({status}): {body[:200]}"
    print(f"cancel acknowledged: {body[:200]}", file=sys.stderr)

    evidence = {
        "run_id": run_tag,
        "classifier": "PASS",
        "healthz": health,
        "instruction_id": instruction_id,
        "broker_order_id": broker_order_id,
        "place": place,
        "poll_samples": samples,
        "optional_tables": optional,
        "cancel": cancel,
        "tables": {"required": list(LIVE_ASSERT_TABLES),
                   "optional": list(LIVE_OPTIONAL_TABLES)},
    }
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"live-{run_tag}-t9-order-sandbox.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(evidence, fh, indent=2)
        print(f"evidence: {path}", file=sys.stderr)
    return 0, "PASS", evidence


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
        code, cls, note = run_live(out_dir=args.out, run_id=run_id)
        print(f"result: {cls}")
        return code
    errs = offline_contract()
    if errs:
        for e in errs:
            print(f"FAIL: {e}")
        print(f"t9-order-sandbox-contract: {len(errs)} check(s) FAILED")
        return 1
    print("t9-order-sandbox-contract: all checks pass (offline contract; "
          "--live needs the execution-t3 stack with the gate ENABLED)")
    return 0


def _self_check(out_dir, run_id):
    errs = offline_contract()
    if errs:
        print("self-check FAIL:", "; ".join(errs))
        return 1
    # Fake-transport classification demos (no network, no stack).
    place_202 = (202, json.dumps({
        "accepted": True, "gate_state": "ENABLED", "action": "place",
        "instruction_id": "T9-SB-selfcheck", "execution_attempt_id": "attempt-1",
        "client_order_ref": "cor-1", "broker_order_id": "fake-broker-order-1",
        "order_status": "ACCEPTED", "event_emission": "accepted"}))
    cancel_202 = (202, json.dumps({
        "accepted": True, "action": "cancel",
        "broker_order_id": "fake-broker-order-1"}))
    demos = [
        ("full round-trip", [
            (200, json.dumps({"gate_state": "ENABLED", "gate_epoch": 2})),
            place_202, cancel_202],
         {"Execution_Attempts": [5, 6], "Order_Lifecycle": [10, 11],
          "Fills": [3, 3], "Positions": [2, 2]}, 0, "PASS"),
        ("gate HALTED", [
            (200, json.dumps({"gate_state": "HALTED", "gate_epoch": 1}))],
         {}, 3, "LIVE-CHAIN-UNWIRED"),
        ("auth failure", [
            (200, json.dumps({"gate_state": "ENABLED", "gate_epoch": 2})),
            (401, '{"accepted": false, "reason": "authentication failed"}')],
         {"Execution_Attempts": [1], "Order_Lifecycle": [1]}, 1, "FAIL"),
        ("bridge UNKNOWN", [
            (200, json.dumps({"gate_state": "ENABLED", "gate_epoch": 2})),
            (503, json.dumps({"accepted": False, "outcome": "UNKNOWN",
                              "reason": "bridge UNKNOWN outcome",
                              "gate_state": "ENABLED"}))],
         {"Execution_Attempts": [1], "Order_Lifecycle": [1]}, 1, "FAIL"),
        ("no table growth", [
            (200, json.dumps({"gate_state": "ENABLED", "gate_epoch": 2})),
            place_202],
         {"Execution_Attempts": [5, 5, 5, 5],
          "Order_Lifecycle": [10, 10, 10, 10]}, 1, "FAIL"),
    ]
    results = []
    for name, responses, samples, want_code, want_cls in demos:
        old = os.environ.get("T9_APPROVED_BY")
        os.environ["T9_APPROVED_BY"] = APPROVED_OPERATOR
        try:
            code, cls, note = run_live(transport=FakeTransport(responses),
                                       probe=FakeFlussProbe(samples),
                                       require_stack=False, poll_timeout_s=0)
        finally:
            if old is None:
                os.environ.pop("T9_APPROVED_BY", None)
            else:
                os.environ["T9_APPROVED_BY"] = old
        if (code, cls) != (want_code, want_cls):
            print(f"self-check FAIL: {name} classified {cls} (exit {code}), "
                  f"expected {want_cls} ({want_code})")
            return 1
        results.append({"demo": name, "exit": code, "classifier": cls})
        print(f"[self-check] {name} -> {cls} (exit {code}) OK")
    # Approval gate blocks placement without the flag.
    os.environ.pop("T9_APPROVED_BY", None)
    code, cls, _ = run_live(transport=FakeTransport([]),
                            probe=FakeFlussProbe({}), require_stack=False)
    if code != 2 or cls != "BLOCKED":
        print(f"self-check FAIL: approval gate did not block (exit {code})")
        return 1
    print("[self-check] approval gate blocks without T9_APPROVED_BY OK")
    evidence = {
        "work_item_id": f"A2.5-T9-SANDBOX-{run_id}",
        "artifact": f"logs/nautilus-execution/a2-t9-sandbox-harness-{run_id[:8]}.md",
        "status": "harness live leg complete: healthz gate+epoch, place, "
                  "table poll, cancel",
        "jvm_parity": {"payload_hash_ok": True, "auth_ok": True,
                       "source": "real GatewayProtocol.java (jackson 2.16.1), "
                                 "run 2026-08-21"},
        "offline_checks": "t8 reuse 12/12; compose shape; A2.1 premise; DDL "
                          "poll columns; signing parity",
        "live_classifier": "gate HALTED -> LIVE-CHAIN-UNWIRED (exit 3); "
                           "202+table growth+cancel -> PASS (exit 0); "
                           "401/409/UNKNOWN/no-growth -> FAIL (exit 1); "
                           "no T9_APPROVED_BY or unreadable table -> BLOCKED (exit 2)",
        "demos": results,
    }
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"self-check-{run_id}-t9-order-sandbox.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print(f"[self-check] PASS — evidence {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
