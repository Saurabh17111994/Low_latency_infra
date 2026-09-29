#!/usr/bin/env python3
"""L5-4: the projection-ledger durability boundary is where the code says it is.

WHY THIS EXISTS
---------------
`com.trading.common.schema.projection.FlussProjectionLedgerStore` opened a
Connection and a Table, then served every lookup/put from an in-memory map —
`table` was never read or written. The name promised durability it did not
provide, and a second writer on Postback_Projection_Ledger would have been a
dual-writer corruption hazard (P4-010). The class is deleted; the common
interface's only implementation is the in-memory oracle, and durability lives
in the execution gateway's own store (single writer, H1-1/M1-6).

WHAT IS CHECKED
---------------
* no `*LedgerStore*` source outside `06_execution_gateway` may import
  `org.apache.fluss` — a Fluss-named ledger outside the gateway is the exact
  trap (a re-added delegating class fails here);
* the common `ProjectionLedgerStore` interface is implemented only by
  `InMemoryProjectionLedgerStore` inside common;
* the common ledger port files carry no Fluss client imports (the port is
  in-memory; Fluss wiring belongs to the gateway);
* the gateway's durable store still exists, really writes (UpsertWriter), and
  is wired as the single writer in `GatewayStartup.openStores`.

The test is auto-discovered by gate step 3 (``test_*.py``), so the boundary
joins the Monday gate with no gate change.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
CODE = ROOT / "code"
GATEWAY_DIR = CODE / "02_services" / "06_execution_gateway"
COMMON_PROJECTION = CODE / "common" / "src" / "main" / "java" / "com" / "trading" / "common" / "schema" / "projection"
COMMON_LEDGER_PORT = (
    COMMON_PROJECTION / "ProjectionLedgerStore.java",
    COMMON_PROJECTION / "InMemoryProjectionLedgerStore.java",
    COMMON_PROJECTION / "ProjectionLedgerEntry.java",
)
GATEWAY_LEDGER = GATEWAY_DIR / "src" / "main" / "java" / "com" / "trading" / "execution" / "gateway" / "FlussProjectionLedgerStore.java"
GATEWAY_STARTUP = GATEWAY_DIR / "src" / "main" / "java" / "com" / "trading" / "execution" / "gateway" / "GatewayStartup.java"


def _java_sources() -> list[Path]:
    out = []
    for p in CODE.rglob("*.java"):
        if "target" in p.parts:
            continue
        out.append(p)
    return out


def test_no_fluss_named_ledger_outside_the_gateway():
    offenders = []
    for p in _java_sources():
        if "LedgerStore" not in p.name:
            continue
        if GATEWAY_DIR in p.parents:
            continue
        if "org.apache.fluss" in p.read_text():
            offenders.append(str(p.relative_to(ROOT)))
    assert not offenders, (
        "a Fluss-importing ledger store lives outside the execution gateway — "
        "durability belongs to the gateway's single writer (H1-1/M1-6); a "
        "common-module class must not carry a Fluss name: " + ", ".join(sorted(offenders)))


def test_common_ledger_interface_is_implemented_in_memory_only():
    impls = set()
    for p in _java_sources():
        if COMMON_PROJECTION not in p.parents:
            continue
        text = p.read_text()
        if re.search(r"implements\s+ProjectionLedgerStore\b", text):
            impls.add(p.name)
    assert impls == {"InMemoryProjectionLedgerStore.java"}, (
        "the common ProjectionLedgerStore port must have exactly one implementation, "
        f"the in-memory oracle; found {sorted(impls)}")


def test_common_ledger_port_carries_no_fluss_imports():
    for p in COMMON_LEDGER_PORT:
        assert p.exists(), f"{p.relative_to(ROOT)} is missing"
        assert "org.apache.fluss" not in p.read_text(), (
            f"{p.relative_to(ROOT)} imports a Fluss client class — the common ledger "
            "port is in-memory; Fluss wiring belongs to the gateway store")


def test_gateway_durable_ledger_store_is_present_and_wired():
    assert GATEWAY_LEDGER.exists(), "the gateway's durable ledger store is gone"
    store = GATEWAY_LEDGER.read_text()
    assert "org.apache.fluss" in store, "the gateway store must be the Fluss-backed one"
    assert "UpsertWriter" in store, "the gateway store must really write (UpsertWriter)"
    startup = GATEWAY_STARTUP.read_text()
    assert "FlussProjectionLedgerStore.open(" in startup, (
        "GatewayStartup.openStores must construct the durable ledger store — it is the "
        "single writer the durability contract names")
