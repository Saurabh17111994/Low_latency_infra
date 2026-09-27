"""A2.5 — offline tests for the T9_ORDER_SANDBOX harness (t9_order_sandbox.py).

Nothing here starts a container, and no credentials or market hours are required
(P6-616: the harness's `--offline` leg does need a working docker CLI — one of its
checks parses the execution-t3 compose config). The trust anchor is the
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
    run it as its first check (all checks; the count is the checker's)."""
    out = subprocess.run([sys.executable, HARNESS, "--offline"],
                         capture_output=True, text=True)
    assert "t8 sandbox contract reused" in out.stdout
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


def _live_env(flag="saurabh"):
    """Set T9_APPROVED_BY for a test and return the old value for restore."""
    old = os.environ.get("T9_APPROVED_BY")
    if flag is None:
        os.environ.pop("T9_APPROVED_BY", None)
    else:
        os.environ["T9_APPROVED_BY"] = flag
    return old


def _restore_env(old):
    if old is None:
        os.environ.pop("T9_APPROVED_BY", None)
    else:
        os.environ["T9_APPROVED_BY"] = old


def _healthz(state="ENABLED", epoch=2):
    return (200, json.dumps({"gate_state": state, "gate_epoch": epoch,
                             "process_alive": True,
                             "trading_ready": state == "ENABLED"}))


def _place_202():
    return (202, json.dumps({
        "accepted": True, "gate_state": "ENABLED", "action": "place",
        "instruction_id": "T9-SB-0001", "execution_attempt_id": "attempt-0001",
        "client_order_ref": "cor-0001",
        "broker_order_id": "fake-broker-order-1",
        "order_status": "ACCEPTED", "event_emission": "accepted"}))


def _cancel_202():
    return (202, json.dumps({"accepted": True, "action": "cancel",
                             "broker_order_id": "fake-broker-order-1"}))


def test_live_halted_gate_reports_not_enabled_before_signing():
    """A HALTED gate is refused from /healthz before any placement envelope
    is signed or sent — exit 3 GATE-NOT-ENABLED (approve first), never a placement."""
    old = _live_env()
    try:
        transport = t9.FakeTransport([_healthz(state="HALTED", epoch=1)])
        code, cls, note = t9.run_live(transport=transport,
                                      probe=t9.FakeFlussProbe({}),
                                      require_stack=False)
        assert (code, cls) == (3, "GATE-NOT-ENABLED"), f"{cls}/{note}"
        assert "HALTED" in note
        assert all("/v1/intents" not in call[1] for call in transport.calls)
    finally:
        _restore_env(old)


def test_live_full_round_trip_passes():
    """The A2.6 target: gate ENABLED, place 202 with a broker id,
    Order_Lifecycle shows the appended row, cancel acknowledged — PASS (exit 0)."""
    old = _live_env()
    try:
        transport = t9.FakeTransport([_healthz(), _place_202(), _cancel_202()])
        probe = t9.FakeFlussProbe({
            "Execution_Attempts": [5, 6],
            "Order_Lifecycle": [10, 11],
            "Fills": [3, 3],
            "Positions": [2, 2],
        })
        code, cls, note = t9.run_live(transport=transport, probe=probe,
                                      require_stack=False)
        assert (code, cls) == (0, "PASS"), f"{cls}/{note}"
        assert transport.calls[0] == ("GET", "http://nautilus:9190/healthz")
        intents = [c for c in transport.calls if c[1].endswith("/v1/intents")]
        assert len(intents) == 2, transport.calls  # place + cancel
    finally:
        _restore_env(old)


def test_live_signs_place_with_healthz_epoch():
    """The envelope's gate_epoch is READ from /healthz — a hard-coded epoch
    would sign a stale control identity (the old harness signed 0)."""
    old = _live_env()
    try:
        transport = t9.FakeTransport([_healthz(epoch=7), _place_202(),
                                      _cancel_202()])
        code, cls, _ = t9.run_live(
            transport=transport,
            probe=t9.FakeFlussProbe({"Execution_Attempts": [1, 2],
                                     "Order_Lifecycle": [1, 2]}),
            require_stack=False)
        assert (code, cls) == (0, "PASS")
        place_envelope = json.loads(transport.bodies[1])
        assert place_envelope["gate_epoch"] == 7
    finally:
        _restore_env(old)


def test_live_rejects_401_409_and_unknown():
    """401 (parity) and 409 (rejected) are FAIL; a 503 bridge UNKNOWN with the
    gate ENABLED is FAIL too (exit 3 GATE-NOT-ENABLED is only for a HALTED gate)."""
    old = _live_env()
    try:
        cases = [
            ((401, '{"accepted": false, "reason": "authentication failed"}'),
             1, "FAIL"),
            ((409, '{"accepted": false, "outcome": "REJECTED", '
                   '"reason": "rejected by venue"}'), 1, "FAIL"),
            ((503, '{"accepted": false, "outcome": "UNKNOWN", '
                   '"reason": "bridge UNKNOWN outcome", '
                   '"gate_state": "ENABLED"}'), 1, "FAIL"),
        ]
        for (status, body), want_code, want_cls in cases:
            transport = t9.FakeTransport([_healthz(), (status, body)])
            code, cls, note = t9.run_live(
                transport=transport,
                probe=t9.FakeFlussProbe({"Execution_Attempts": [1],
                                         "Order_Lifecycle": [1]}),
                require_stack=False)
            assert (code, cls) == (want_code, want_cls), \
                f"status {status}: {cls}/{note}"
    finally:
        _restore_env(old)


def test_live_no_table_growth_fails():
    """A 202 whose required tables never grow is a FAIL — an accepted bridge
    call is not the same as a projected order row."""
    old = _live_env()
    try:
        transport = t9.FakeTransport([_healthz(), _place_202()])
        probe = t9.FakeFlussProbe({"Execution_Attempts": [5, 5, 5, 5],
                                   "Order_Lifecycle": [10, 10, 10, 10]})
        code, cls, note = t9.run_live(transport=transport, probe=probe,
                                      require_stack=False, poll_timeout_s=0)
        assert (code, cls) == (1, "FAIL"), f"{cls}/{note}"
        assert "no append" in note.lower()
    finally:
        _restore_env(old)


def test_live_gateway_event_refusal_fails_with_the_reason():
    """CHG-331's real failure mode: a 202 whose lifecycle event the gateway
    refused must FAIL immediately with the gateway reason — not wait out the
    poll and report a bare 'no append'."""
    old = _live_env()
    try:
        body = json.loads(_place_202()[1])
        body["event_emission"] = ("failed: gateway /v1/events responded 503: "
                                  "execution disabled via EXECUTION_ENABLED")
        transport = t9.FakeTransport([_healthz(), (202, json.dumps(body))])
        code, cls, note = t9.run_live(
            transport=transport,
            probe=t9.FakeFlussProbe({"Order_Lifecycle": [10, 11]}),
            require_stack=False, poll_timeout_s=0)
        assert (code, cls) == (1, "FAIL"), f"{cls}/{note}"
        assert "gateway" in note.lower() and "503" in note
        assert len(transport.calls) == 2  # healthz + place; no cancel on a failed emit
    finally:
        _restore_env(old)


def test_live_required_table_is_order_lifecycle_only():
    """CHG-332: Order_Lifecycle is the projection of the route's emitted event —
    the only Fluss table this direct-to-nautilus route can grow. Attempts/Fills/
    Positions are reported, never required."""
    old = _live_env()
    try:
        transport = t9.FakeTransport([_healthz(), _place_202(), _cancel_202()])
        code, cls, note = t9.run_live(
            transport=transport,
            probe=t9.FakeFlussProbe({"Order_Lifecycle": [10, 11]}),
            require_stack=False)
        assert (code, cls) == (0, "PASS"), f"{cls}/{note}"
        assert note["tables"]["required"] == ["Order_Lifecycle"]
        assert "Execution_Attempts" in note["tables"]["optional"]

        # An attempt-only append must NOT satisfy the assert.
        transport = t9.FakeTransport([_healthz(), _place_202()])
        code, cls, note = t9.run_live(
            transport=transport,
            probe=t9.FakeFlussProbe({"Execution_Attempts": [5, 6],
                                     "Order_Lifecycle": [10, 10]}),
            require_stack=False, poll_timeout_s=0)
        assert (code, cls) == (1, "FAIL"), f"{cls}/{note}"
        assert "Order_Lifecycle" in note
    finally:
        _restore_env(old)


def test_live_probe_unavailable_blocks():
    """An unreadable required table is BLOCKED (exit 2): the assert cannot be
    made, so the harness must not report PASS or FAIL."""
    old = _live_env()
    try:
        transport = t9.FakeTransport([_healthz()])
        code, cls, note = t9.run_live(transport=transport,
                                      probe=t9.FakeFlussProbe({}),
                                      require_stack=False)
        assert (code, cls) == (2, "BLOCKED"), f"{cls}/{note}"
        assert "unreadable" in note
    finally:
        _restore_env(old)


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
