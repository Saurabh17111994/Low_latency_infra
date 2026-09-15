"""Wave 32 tests for the disaster-drill runner: the two paths that could not
reach their own verdict (P6-068, P6-069).

P6-068 — DR-006 partitions the tablet by disconnecting it from the trading
network, then runs `resolve_steps()` for the RECOVERY leg, which asked the
disconnected tablet which network to reconnect it to. It answered "none", and
the drill aborted at CANNOT RESOLVE RECOVERY STEPS with `__RECONNECT_TABLET__`
never reached: the drill could not pass, ever.

P6-069 — the post-assertion poll loop narrowed `remaining` to 2-tuples, but the
deadline path below still unpacked 3 values. That path is exactly the one that
has to write FAIL evidence and exit 1; instead it died with a traceback, the
pending probes never entered `results`, and no evidence file was written.

Probes, docker and the command runner are stubbed here — no faults are injected
and no live stack is touched.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import disaster_drills as dd


TRADING_NET = "01_docker_trading-net"


def _green_probes():
    return {k: (lambda: (True, "ok")) for k in dd.PROBES}


def _stub_docker(monkeypatch, tablet_networks="", network_ls=""):
    """docker stub: .NetworkSettings.Networks for inspect, name list for ls."""
    def fake_docker(*args, **kwargs):
        if args[:2] == ("network", "ls"):
            return {"rc": 0, "out": network_ls}
        if args and args[0] == "inspect":
            return {"rc": 0, "out": tablet_networks}
        return {"rc": 0, "out": ""}
    monkeypatch.setattr(dd, "docker", fake_docker)


# --------------------------------------------------------------- P6-068


def test_detached_tablet_still_resolves_the_network(monkeypatch):  # disc
    """The fault disconnects the tablet; that must not make the name
    unresolvable — `docker network ls` still knows it."""
    _stub_docker(monkeypatch, tablet_networks="", network_ls="bridge " + TRADING_NET + " ")

    assert dd.resolve_trading_net() == TRADING_NET


def test_tablet_attachment_wins_over_the_network_list(monkeypatch):  # pin
    _stub_docker(monkeypatch, tablet_networks="bridge " + TRADING_NET + " ",
                 network_ls="stale_trading-net")

    assert dd.resolve_trading_net() == TRADING_NET


def test_no_trading_network_anywhere_is_still_none(monkeypatch):  # pin
    """The fallback must not invent a network name out of unrelated ones."""
    _stub_docker(monkeypatch, tablet_networks="bridge ", network_ls="bridge host none")

    assert dd.resolve_trading_net() is None


def test_a_recovery_step_needing_no_network_does_not_resolve_one(monkeypatch):  # disc
    """DR-006's recovery is the reconnect sentinel. Resolution must not even be
    attempted: the old code aborted the drill here."""
    def explode(*_a, **_k):
        raise AssertionError("resolve_trading_net must not be called for this leg")
    monkeypatch.setattr(dd, "resolve_trading_net", explode)

    steps, err = dd.resolve_steps([["__RECONNECT_TABLET__"]], False)

    assert err is None
    assert steps == [["__RECONNECT_TABLET__"]]


def test_a_recovery_step_needing_the_network_still_resolves_it(monkeypatch):  # disc
    _stub_docker(monkeypatch, tablet_networks="", network_ls=TRADING_NET)
    steps, err = dd.resolve_steps(
        [["docker", "network", "disconnect", "{{TRADING_NET}}", "tablet"]], False)

    assert err is None
    assert steps == [["docker", "network", "disconnect", TRADING_NET, "tablet"]]


def test_an_unresolvable_sentinel_is_still_a_hard_error(monkeypatch):  # disc
    """Fail-closed on the leg that really needs the name: a command with an
    empty network argument would disconnect the wrong thing."""
    _stub_docker(monkeypatch, tablet_networks="", network_ls="bridge host")

    _steps, err = dd.resolve_steps(
        [["docker", "network", "disconnect", "{{TRADING_NET}}", "tablet"]], False)

    assert err is not None
    assert "cannot resolve" in err
    assert "docker network ls" in err


def test_dr006_reaches_its_reconnect_sentinel(monkeypatch, tmp_path):  # disc
    """End to end: DR-006 with the tablet detached (as its own fault made it)
    must run the reconnect heal instead of aborting before it."""
    healed = []

    def fake_reconnect(retries=5):
        healed.append(True)
        return 0, "tablet reconnected to " + TRADING_NET

    monkeypatch.setattr(dd, "run", lambda cmd, timeout=60: {"rc": 0, "out": "ok"})
    monkeypatch.setattr(dd, "reconnect_tablet", fake_reconnect)
    monkeypatch.setattr(dd, "PROBES", _green_probes())
    monkeypatch.setattr(dd.time, "sleep", lambda _s: None)
    # The tablet is detached for the whole drill: inspect sees nothing, ls does.
    _stub_docker(monkeypatch, tablet_networks="", network_ls=TRADING_NET)

    verdict, record = dd.drive(dd.drill_by_id("DR-006"), "TEST", True,
                               str(tmp_path), False)

    assert healed == [True], "the reconnect heal must actually run"
    assert verdict == "PASS", record["sections"]
    assert os.listdir(str(tmp_path)), "an evidence file must be written"


# --------------------------------------------------------------- P6-069


def _pending_drill(bound_s=0):
    """A drill whose ONLY assertions are the two that never go green.

    A synthetic spec is used because the real drills reuse the same probe for
    pre- and post-assertions; stubbing that probe red would fail the
    preconditions and the run would never reach the poll loop under test.
    """
    return {
        "id": "DR-TEST", "title": "pending-probe fixture", "fault_class": "test",
        "documented_expectation": "n/a", "pre": [], "fault": [["true"]],
        "during": [], "recovery": [],
        "post": [("wave32_stuck", "never recovers"),
                 ("wave32_also_stuck", "also pending")],
        "bound_s": bound_s,
    }


def _drive_with_pending_probes(monkeypatch, tmp_path, bound_s=0.2):
    """Drive the fixture until its poll bound expires with both assertions
    still red, so the deadline path (the one that used to raise ValueError)
    is the one that runs. The poll sleeps are collapsed — the wait is not
    what is under test."""
    probes = _green_probes()
    probes["wave32_stuck"] = lambda: (False, "still down")
    probes["wave32_also_stuck"] = lambda: (False, "still down")
    monkeypatch.setattr(dd, "PROBES", probes)
    monkeypatch.setattr(dd, "run", lambda cmd, timeout=60: {"rc": 0, "out": "ok"})
    _stub_docker(monkeypatch, tablet_networks=TRADING_NET + " ")
    monkeypatch.setattr(dd.time, "sleep", lambda _s: None)
    return dd.drive(_pending_drill(bound_s), "TEST", True, str(tmp_path), False)


def test_pending_probes_at_the_deadline_do_not_crash(monkeypatch, tmp_path):  # disc
    verdict, record = _drive_with_pending_probes(monkeypatch, tmp_path)

    assert verdict == "FAIL"
    assert record["verdict"] == "FAIL"


def test_pending_probes_are_recorded_in_the_evidence(monkeypatch, tmp_path):  # disc
    """The whole point of the FAIL path: the evidence must name what was still
    pending when the bound expired."""
    _verdict, record = _drive_with_pending_probes(monkeypatch, tmp_path)

    proofs = record["sections"][10]  # ## Recovery proof
    assert "never recovers" in proofs
    assert "still down" in proofs
    assert proofs.count("- [ ]") == 2, proofs


def test_the_pending_failure_still_writes_an_evidence_file(monkeypatch, tmp_path):  # disc
    _verdict, record = _drive_with_pending_probes(monkeypatch, tmp_path)

    files = os.listdir(str(tmp_path))
    assert files, "the failure path must leave evidence behind"
    content = open(os.path.join(str(tmp_path), files[0]), encoding="utf-8").read()
    assert "Verdict: FAIL" in content


def test_a_probe_that_recovers_late_is_not_reported_pending(monkeypatch, tmp_path):  # pin
    """The round loop and the deadline path must agree on the tuple shape: a
    probe that turns green in round 2 must not reappear in the failure list.

    This one passes pre-wave too — the old round loop unpacked 2-tuples that
    matched its own narrowed assignment, and the crash only happened when the
    loop exited with probes still pending (covered above). It stays as a pin
    on the shape agreement the fix has to keep."""
    drill = _pending_drill(bound_s=30)
    drill["post"] = [("wave32_flaky", "recovers on the second poll")]

    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        return (calls["n"] > 1), ("ok" if calls["n"] > 1 else "starting up")

    monkeypatch.setattr(dd, "run", lambda cmd, timeout=60: {"rc": 0, "out": "ok"})
    monkeypatch.setattr(dd, "reconnect_tablet", lambda retries=5: (0, "healed"))
    _stub_docker(monkeypatch, tablet_networks=TRADING_NET + " ")
    probes = _green_probes()
    probes["wave32_flaky"] = flaky
    monkeypatch.setattr(dd, "PROBES", probes)
    monkeypatch.setattr(dd.time, "sleep", lambda _s: None)

    verdict, record = dd.drive(drill, "TEST", True, str(tmp_path), False)

    assert verdict == "PASS"
    assert "recovers on the second poll" in record["sections"][10]
    assert "- [x]" in record["sections"][10]
