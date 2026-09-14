"""A2.5 — offline tests for the T9_ORDER_SANDBOX harness (t9_order_sandbox.py).

No containers, no credentials, no market hours required. The trust anchor is the
JW_* parity block inside the harness: those constants were printed by the REAL
production GatewayProtocol.java (compiled against jackson 2.16.1 from ~/.m2 and
run on this host 2026-08-21 for one fixed instance) — the Python signing port
must reproduce every byte, or the live envelope the harness signs would be
rejected by the nautilus verifier with 401. Second anchor: the DDL poll columns
and the A2.1 premise are asserted against the actual sources, so A2.4's table
assertions have real columns to poll.
"""

import json
import os
import subprocess
import sys
import tempfile
import datetime as _dt

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS)
import t9_order_sandbox as t9  # noqa: E402

HARNESS = os.path.join(SCRIPTS, "t9_order_sandbox.py")
ROOT = t9.ROOT


def test_offline_contract_exit_zero():
    """The plan's A2.5 offline leg must pass today: skeleton + contract."""
    out = subprocess.run([sys.executable, HARNESS, "--offline"],
                         capture_output=True, text=True)
    assert out.returncode == 0, f"rc={out.returncode}\n{out.stdout}\n{out.stderr}"
    assert "all checks pass" in out.stdout
    assert "FAIL" not in out.stdout


def test_t8_harness_reused():
    """A2.5 explicitly reuses t8_sandbox_contract_check.py — the harness must
    run it as its first check (12/12)."""
    out = subprocess.run([sys.executable, HARNESS, "--offline"],
                         capture_output=True, text=True)
    assert "t8 sandbox contract reused (12/12)" in out.stdout
    assert "[PASS] t8 sandbox contract reused" in out.stdout


def test_jvm_payload_hash_parity():
    assert t9.payload_hash(t9.JW_PAYLOAD_JSON) == t9.JW_PAYLOAD_HASH


def test_jvm_canonical_and_authentication_parity():
    canon = t9.canonical("execution-gateway.v1", "EXECUTION_INTENT",
                         "parity-req-0001", "dev-scope", "dev-partition",
                         t9.JW_PAYLOAD_HASH, 42, "fence-token-0001",
                         t9.JW_DEADLINE, t9.JW_PAYLOAD_JSON)
    assert canon == t9.JW_CANONICAL
    assert t9.sign("local-dev-only", canon) == t9.JW_AUTH


def test_encode_then_verify_accepted():
    env_json, _, ph = t9.encode_envelope(
        "local-dev-only", "execution-gateway.v1", "EXECUTION_INTENT",
        "parity-req-0001", "dev-scope", "dev-partition",
        json.loads(t9.JW_PAYLOAD_JSON), 42, "fence-token-0001", t9.JW_DEADLINE)
    assert ph == t9.JW_PAYLOAD_HASH
    accepted, reason = t9.verify_envelope(env_json, "local-dev-only",
                                          "execution-gateway.v1",
                                          t9.JW_DEADLINE - 1)
    assert accepted, reason
    # same wire keys/order as the Java encode()
    env = json.loads(env_json)
    assert list(env.keys()) == [
        "protocol_version", "message_type", "request_id", "account_scope_id",
        "execution_partition_id", "payload_hash", "gate_epoch", "fence_token",
        "deadline_epoch_ms", "payload", "authentication",
    ]


def test_verify_rejects_tamper_wrong_secret_expired_version():
    env_json, _, _ = t9.encode_envelope(
        "local-dev-only", "execution-gateway.v1", "EXECUTION_INTENT",
        "parity-req-0001", "dev-scope", "dev-partition",
        json.loads(t9.JW_PAYLOAD_JSON), 42, "fence-token-0001", t9.JW_DEADLINE)
    assert not t9.verify_envelope(env_json, "wrong-secret",
                                  "execution-gateway.v1", 0)[0]
    assert not t9.verify_envelope(env_json, "local-dev-only",
                                  "other.v1", 0)[0]
    assert not t9.verify_envelope(env_json, "local-dev-only",
                                  "execution-gateway.v1", t9.JW_DEADLINE + 1)[0]
    tampered = json.loads(env_json)
    tampered["payload"]["quantity"] = 2  # would double the order -> 401
    tampered_json = json.dumps(tampered, separators=(",", ":"))
    assert not t9.verify_envelope(tampered_json, "local-dev-only",
                                  "execution-gateway.v1", 0)[0]


def test_bieq_payload_schema_matches_nautilus_client():
    """Field list must equal NautilusIntentClient.sendWithFence() exactly —
    the canonical bytes depend on it."""
    p = t9.bieq_payload()
    assert list(p.keys()) == [
        "instruction_id", "candidate_id", "trade_context_id",
        "instrument_token", "symbol", "exchange", "side", "quantity",
        "order_type", "limit_price_paise", "product_type", "time_in_force",
        "request_hash", "schema_version",
    ]
    # Pin the LIVE sample instrument. The original INPUT-11 sample (BILCARE,
    # token 762583) is DELISTED and the order API rejects it with 400 "invalid
    # trading symbol", so t9 replaced it with RCF (token 2866, TradingSymbol
    # "RCF-EQ", verified live 2026-08-25). Pinning both the literals and the
    # payload-vs-constant agreement means drift on either side fails here.
    assert t9.BIEQ_SYMBOL == "RCF-EQ" and t9.BIEQ_INSTRUMENT_TOKEN == 2866
    assert p["symbol"] == t9.BIEQ_SYMBOL
    assert p["instrument_token"] == t9.BIEQ_INSTRUMENT_TOKEN
    assert p["quantity"] == 1  # T9 safe instrument INPUT-11


def test_offline_checks_a21_premise_and_ddl_columns():
    sjc = os.path.join(ROOT, "code", "02_services", "02_compute", "src", "main",
                       "java", "com", "trading", "compute", "signaljob",
                       "SignalJobConfig.java")
    assert "EXECUTION_INTENT_ENABLED" in open(sjc, encoding="utf-8").read()
    gw_dir = os.path.join(ROOT, "code", "02_services", "06_execution_gateway",
                          "src", "main", "java", "com", "trading", "execution",
                          "gateway")
    for f in ("DurableIntentDispatcher.java", "NautilusIntentClient.java"):
        assert os.path.exists(os.path.join(gw_dir, f)), f"{f} missing"
    ddl = os.path.join(ROOT, "code", "01_platform", "02_sql", "ddl")
    for fname, cols in (
            ("27_execution_intent.sql", ["instruction_id", "request_hash",
                                         "created_ts"]),
            ("09_order_lifecycle.sql", ["account_scope_id", "broker_order_id",
                                        "normalized_state"]),
            ("12_execution_attempts.sql", ["execution_attempt_id", "phase",
                                           "gate_fence_token"])):
        text = open(os.path.join(ddl, fname), encoding="utf-8").read()
        for c in cols:
            assert f"\n    {c}" in text, f"{fname} missing poll column {c}"


def test_approval_gate_blocks_without_flag():
    """Placement without T9_APPROVED_BY=saurabh must fail closed (exit 2)."""
    old = os.environ.pop("T9_APPROVED_BY", None)
    try:
        assert not t9.approval_gate()
        code, cls, _ = t9.run_live(transport=t9.FakeTransport([]))
        assert code == 2 and cls == "BLOCKED"
    finally:
        if old is not None:
            os.environ["T9_APPROVED_BY"] = old


def test_live_classifier_t4_wall_and_failures():
    """202/503 -> LIVE-CHAIN-UNWIRED (exit 3, honest — not a fake PASS);
    401 -> FAIL (exit 1)."""
    old = os.environ.get("T9_APPROVED_BY")
    os.environ["T9_APPROVED_BY"] = "saurabh"
    # sendWithFence-equivalent envelope must be produced for every call — the
    # classifier is transport-independent, so FakeTransport suffices.
    cases = [
        ((202, '{"accepted": true}'), 3, "T4 wall (no LiveNode wiring)"),
        ((503, '{"accepted": false, "reason": "gate HALTED"}'), 3, "gate HALTED"),
        ((401, '{"accepted": false, "reason": "authentication failed"}'), 1,
         "envelope rejected"),
    ]
    try:
        for (status, body), want_code, want_note in cases:
            code, cls, note = t9.run_live(transport=t9.FakeTransport([(status, body)]),
                                          require_stack=False)
            assert code == want_code, f"status {status}: {cls}/{note}"
            assert cls in ("LIVE-CHAIN-UNWIRED", "FAIL")
            assert note == want_note
    finally:
        if old is None:
            os.environ.pop("T9_APPROVED_BY", None)
        else:
            os.environ["T9_APPROVED_BY"] = old


def test_cli_self_check_exit_zero_writes_evidence():
    with tempfile.TemporaryDirectory() as out:
        rc = subprocess.run([sys.executable, HARNESS, "--self-check", "--out", out],
                            capture_output=True, text=True)
        assert rc.returncode == 0, f"{rc.stdout}\n{rc.stderr}"
        assert "PASS" in rc.stdout
        ev = [f for f in os.listdir(out) if f.endswith("-t9-order-sandbox.json")]
        assert ev, "evidence not written"
        with open(os.path.join(out, ev[0]), encoding="utf-8") as fh:
            data = json.load(fh)
        assert data["jvm_parity"]["payload_hash_ok"] and data["jvm_parity"]["auth_ok"]


def test_sign_control_mints_a_verifiable_control_envelope():
    """D5 (2026-09-14): an operator can sign the DEC-044 approve/halt envelopes
    without hand-assembling canonical bytes. The message type is the only thing
    separating the two routes, so it must survive the round trip, and the
    operator/evidence the executor audits must be inside the signed payload."""
    now = _dt.datetime(2026, 9, 14, 12, 0, tzinfo=_dt.timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    for action, msg_type in (("approve", "GATE_APPROVE"), ("halt", "GATE_HALT")):
        code, cls, signed = t9.sign_control(
            action, t9.APPROVED_OPERATOR, "CHG-131", reason="wave-4 d5",
            secret="local-dev-only", now=now)
        assert (code, cls) == (0, "PASS"), f"{action}: {signed}"
        envelope = json.loads(signed)
        assert envelope["message_type"] == msg_type, action
        assert envelope["protocol_version"] == t9.PROTOCOL_VERSION
        assert envelope["payload"]["operator"] == t9.APPROVED_OPERATOR
        assert envelope["payload"]["evidence"] == "CHG-131"
        accepted, reason = t9.verify_envelope(signed, "local-dev-only",
                                              t9.PROTOCOL_VERSION, now_ms + 1)
        assert accepted, f"{action}: {reason}"


def test_sign_control_fails_closed_on_unknown_action_and_empty_evidence():
    """Never mint a control artifact an operator would wrongly trust: the
    executor rejects an empty operator/evidence pair with 401, and an unknown
    action has no message type at all."""
    now = _dt.datetime(2026, 9, 14, 12, 0, tzinfo=_dt.timezone.utc)
    code, cls, note = t9.sign_control("halt", t9.APPROVED_OPERATOR, "", now=now)
    assert (code, cls) == (1, "FAIL"), note
    assert "evidence" in note
    code, cls, note = t9.sign_control("open-the-gate", t9.APPROVED_OPERATOR, "x",
                                     now=now)
    assert (code, cls) == (1, "FAIL"), note
    # A *different* operator is NOT refused here: authorization is the
    # executor's decision (403 against its configured operator), and the
    # harness must stay able to mint that envelope to exercise the 403 path.
    code, cls, signed = t9.sign_control("halt", "not-the-operator", "x", now=now)
    assert (code, cls) == (0, "PASS"), signed
    assert json.loads(signed)["payload"]["operator"] == "not-the-operator"


def test_sign_control_cli_prints_only_the_envelope_on_stdout():
    """The signed envelope must be pipeable: stdout carries the JSON and
    nothing else, human notes go to stderr."""
    rc = subprocess.run([sys.executable, HARNESS, "--sign-control", "halt",
                         "--evidence", "CHG-131", "--operator", "saurabh",
                         "--secret", "local-dev-only"],
                        capture_output=True, text=True)
    assert rc.returncode == 0, f"{rc.stdout}\n{rc.stderr}"
    payload = json.loads(rc.stdout)
    assert payload["message_type"] == "GATE_HALT"
    assert payload["payload"]["evidence"] == "CHG-131"


def test_sign_control_cli_refuses_without_evidence():
    rc = subprocess.run([sys.executable, HARNESS, "--sign-control", "approve"],
                        capture_output=True, text=True)
    assert rc.returncode == 1, f"{rc.stdout}\n{rc.stderr}"
    assert rc.stdout.strip() == ""
    assert "evidence" in rc.stderr
