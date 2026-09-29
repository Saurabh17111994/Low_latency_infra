#!/usr/bin/env python3
"""M5-2: the mock broker speaks the canonical decoded-tick dialect, or it is caught here.

WHY THIS EXISTS
---------------
`MockArrowServer` emitted a private vocabulary — `instrument_token`, `exchange_ts`,
`last_price_paise`, `ohlc_*`, nested `depth_*`, `change_pct` — that no in-repo consumer
could read: the Go bridge decodes the proto `TickEvent` / Go `Tick` names. A fake broker
whose dialect nothing can decode silently rots the harnesses that point at it, and the
old test pinned the bad keys instead of the contract.

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* The mock's `tick.put` key set equals the committed fixture
  `code/testdata/mock-tick-sample.json` (which the Go strict-decode test consumes).
* Every mock key is a proto `TickEvent` field name (Java subset of the wire contract).
* Every mock key is a Go `Tick` JSON tag (the decoder the fixture is proven against).
* The legacy vocabulary is gone from the emitted key set.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
MOCK = (ROOT / "code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/"
               "MockArrowServer.java")
FIXTURE = ROOT / "code/testdata/mock-tick-sample.json"
PROTO = ROOT / "proto/market_data.proto"
GO_MAIN = ROOT / "code/02_services/01_ingestion/go-bridge/main.go"

LEGACY_KEYS = {
    "instrument_token", "exchange_ts", "last_price_paise", "last_qty", "change_pct",
    "buy_quantity", "sell_quantity", "ohlc_open_paise", "ohlc_high_paise",
    "ohlc_low_paise", "ohlc_close_paise", "depth_buy", "depth_sell",
}


def _mock_keys() -> set[str]:
    return set(re.findall(r'tick\.put\("([a-z_]+)"', MOCK.read_text()))


def _fixture_keys() -> set[str]:
    return set(json.loads(FIXTURE.read_text()))


def _proto_tickevent_fields() -> set[str]:
    text = PROTO.read_text()
    start = text.index("message TickEvent {")
    end = text.index("\n}", start)
    block = text[start:end]
    return set(re.findall(
        r"^\s*(?:repeated\s+|optional\s+)?"
        r"(?:string|int32|int64|uint32|uint64|sint32|sint64|fixed32|fixed64|"
        r"sfixed32|sfixed64|bytes|bool|double|float)\s+"
        r"([a-z_][a-z0-9_]*)\s*=\s*\d+;", block, re.MULTILINE))


def _go_tick_tags() -> set[str]:
    text = GO_MAIN.read_text()
    start = text.index("type Tick struct {")
    end = text.index("\n}", start)
    return set(re.findall(r'json:"([a-z_][a-z0-9_]*)(?:,[^"]*)?"', text[start:end]))


def test_mock_key_set_equals_the_committed_fixture():
    keys = _mock_keys()
    assert keys, "no tick.put keys found — the mock's tick builder moved?"
    assert keys == _fixture_keys(), (
        "the mock and code/testdata/mock-tick-sample.json disagree; the Go strict-decode "
        f"test consumes the fixture, so drift is silent: {sorted(keys ^ _fixture_keys())}")


def test_every_mock_key_is_a_proto_tickevent_field():
    unknown = _mock_keys() - _proto_tickevent_fields()
    assert not unknown, f"mock keys not in proto TickEvent: {sorted(unknown)}"


def test_every_mock_key_is_a_go_tick_json_tag():
    unknown = _mock_keys() - _go_tick_tags()
    assert not unknown, f"mock keys the Go Tick reader cannot decode: {sorted(unknown)}"


def test_the_legacy_vocabulary_is_gone_from_the_emitted_keys():
    stale = _mock_keys() & LEGACY_KEYS
    assert not stale, f"legacy mock keys reintroduced: {sorted(stale)}"
