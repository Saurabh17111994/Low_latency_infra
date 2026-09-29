#!/usr/bin/env python3
"""M6-3: the p10 rehearsal overlay must remap every host port the base stack publishes.

WHY THIS EXISTS
---------------
`docker-compose.p10.yml` exists so the rehearsal stack can run alongside the live
one, and its whole job is host-port isolation. It remapped only ZK/Fluss/Flink,
leaving otel (4317/4318), OpenObserve (5080/5081) and MinIO (9000/9001) on their
base host ports — the rehearsal then collided with live at `up` time. FACT-017
recorded the base-vs-production O2 collision; this pins the overlay rule that
makes a third stack possible.

WHAT IS CHECKED (offline; parses the two compose files + the production deck)
-----------------------------------------------------------------------------
* Every base port-bearing service appears in the overlay (coverage).
* Effective p10 host ports are unique.
* Every p10 host port is in the 1xxxx rehearsal band.
* No p10 host port collides with a base host port or a production published port.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[4]
DOCKER = ROOT / "code/01_platform/01_docker"
BASE = DOCKER / "docker-compose.yml"
P10 = DOCKER / "docker-compose.p10.yml"
STACK = DOCKER / "docker-stack.yml"


class ComposeLoader(yaml.SafeLoader):
    """SafeLoader that understands the `!override` merge tag used by the p10 overlay."""


ComposeLoader.add_constructor("!override", lambda loader, node: loader.construct_sequence(node))


def _host_port(entry) -> int:
    parts = str(entry).split(":")
    if len(parts) == 2:  # HOST:CONTAINER
        return int(parts[0])
    if len(parts) == 3:  # IP:HOST:CONTAINER
        return int(parts[1])
    raise AssertionError(f"unexpected port mapping shape: {entry!r}")


def service_host_ports(path: Path) -> dict[str, list[int]]:
    data = yaml.load(path.read_text(), Loader=ComposeLoader)
    out: dict[str, list[int]] = {}
    for name, service in (data.get("services") or {}).items():
        ports = service.get("ports") or []
        if ports:
            out[name] = [_host_port(p) for p in ports]
    return out


def production_published_ports() -> set[int]:
    return {int(m) for m in re.findall(r"published:\s*(\d+)", STACK.read_text())}


def test_every_base_port_bearing_service_is_remapped_by_p10():
    missing = sorted(set(service_host_ports(BASE)) - set(service_host_ports(P10)))
    assert not missing, (
        "the p10 overlay leaves these base host ports exposed "
        f"(they collide with a live stack): {missing}")


def test_p10_host_ports_are_unique():
    ports = [p for group in service_host_ports(P10).values() for p in group]
    dupes = sorted({p for p in ports if ports.count(p) > 1})
    assert not dupes, f"two p10 services publish the same host port: {dupes}"


def test_p10_host_ports_are_in_the_rehearsal_band():
    bad = [p for group in service_host_ports(P10).values() for p in group
           if not re.fullmatch(r"1\d{4}", str(p))]
    assert not bad, f"p10 host ports must be five digits starting with 1 (1xxxx): {bad}"


def test_p10_host_ports_collide_with_neither_base_nor_production():
    base = {p for group in service_host_ports(BASE).values() for p in group}
    p10 = {p for group in service_host_ports(P10).values() for p in group}
    prod = production_published_ports()
    assert not (p10 & base), f"p10 reuses base host ports: {sorted(p10 & base)}"
    assert not (p10 & prod), f"p10 reuses production published ports: {sorted(p10 & prod)}"
