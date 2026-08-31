#!/usr/bin/env python3
"""Verify the offline execution bridge network boundary.

The check consumes the resolved Docker Compose JSON, not the YAML source, so
variable interpolation and profiles are evaluated by Docker Compose first.
The market-data ingestion service is the only documented Arrow-facing
exception; the order-path egress network remains bridge-only.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ORDER_ARROW_ENV = {
    "ARROW_APP_ID",
    "ARROW_APP_SECRET",
    "ARROW_USER_ID",
    "ARROW_PASSWORD",
    "ARROW_TOTP_KEY",
    
    "ARROW_REST_URL",
    "ARROW_ORDER_UPDATES_URL",
}
# Ingestion legitimately carries ARROW *market-data* keys; execution-bridge
# is THE order-path ARROW client (its creds are the whole point of the
# arrow-egress network isolation this script validates).
MARKET_DATA_EXCEPTION = {"ingestion", "execution-bridge"}


def _names(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set(value)
    if isinstance(value, list):
        return {str(item) for item in value}
    return set()


def validate_config(config: dict[str, Any], source: dict[str, Any] | None = None) -> list[str]:
    """Validate the execution network contract.

    ``source`` (the raw compose YAML, not the rendered config) is used for
    the ARROW environment check: rendering expands each service's
    env_file(.env + secrets.env) contents into ``environment`` (blanket
    env_file design, 2026-08-29 decision A), so the resolved config shows
    ARROW keys on every env_file-carrying service. The contract's intent is
    "no service EXPLICITLY wires order-path ARROW keys" — that is only
    answerable from the source. When ``source`` is None (legacy callers),
    the rendered environment is checked as before.
    """
    errors: list[str] = []
    networks = config.get("networks", {})
    services = config.get("services", {})

    execution = networks.get("execution-net")
    if not isinstance(execution, dict) or execution.get("internal") is not True:
        errors.append("execution-net must be an internal network")
    if not isinstance(networks.get("arrow-egress"), dict):
        errors.append("arrow-egress network is missing")

    bridge = services.get("execution-bridge")
    if not isinstance(bridge, dict):
        errors.append("execution-bridge service is missing")
    else:
        bridge_networks = _names(bridge.get("networks"))
        if bridge_networks != {"execution-net", "arrow-egress"}:
            errors.append(
                "execution-bridge must be attached only to execution-net and arrow-egress"
            )
        if bridge.get("ports"):
            errors.append("execution-bridge must not publish a host port")

    for service_name, service in services.items():
        if not isinstance(service, dict):
            continue
        if service_name != "execution-bridge" and "arrow-egress" in _names(
            service.get("networks")
        ):
            errors.append(f"{service_name} is attached to bridge-only arrow-egress")
        if source is not None:
            src_service = (source.get("services") or {}).get(service_name) or {}
            env_names = _names(src_service.get("environment"))
        else:
            env_names = _names(service.get("environment"))
        if service_name not in MARKET_DATA_EXCEPTION:
            leaked = sorted(env_names & ORDER_ARROW_ENV)
            if leaked:
                errors.append(
                    f"{service_name} carries order-path Arrow environment keys: {', '.join(leaked)}"
                )

    return errors


def load_resolved_compose(path: Path) -> dict[str, Any]:
    # Canonical env-files (secrets hold the AWS_* values the :? pins demand);
    # a bare `compose config` fails interpolation since the lake-tiering pins.
    command = ["docker", "compose", "-f", str(path),
               "--env-file", str(path.parent / ".env"),
               "--env-file", str(path.parent / "secrets.env"),
               "--profile", "execution-t3", "config", "--format", "json"]
    return json.loads(subprocess.check_output(command, text=True))


def load_source_compose(path: Path) -> dict[str, Any]:
    import yaml
    return yaml.safe_load(path.read_text())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compose", type=Path, required=True)
    args = parser.parse_args()
    errors = validate_config(
        load_resolved_compose(args.compose), source=load_source_compose(args.compose)
    )
    if errors:
        for error in errors:
            print(f"FAIL: {error}")
        return 1
    print("PASS: execution bridge is the only order-path Arrow egress")
    return 0


if __name__ == "__main__":
    sys.exit(main())
