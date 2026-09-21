#!/usr/bin/env python3
"""placement_check.py — offline: would this inventory leave a service with nowhere to land?

`cluster_check.py` asks the live swarm this same question, and `stack_selfcheck.sh CLUSTER=1`
asks it while validating a node — both need the machines to exist. This one needs only the two
files that describe the deploy, the stack deck and the node inventory, so a mis-shaped inventory
is caught *before* anything is rented or deployed. That is not hypothetical: the single-node
rehearsal already produced "no suitable node (max replicas per node limit exceed)" once.

Constraint evaluation is delegated to `cluster_check._matches`, the same function the live
checker uses, so the offline and the live verdict cannot drift apart: a node holds one value per
label key, and `role=manager` therefore excludes that node from every service that pins
`role == worker`.

Scope: "does at least one node satisfy every one of a service's label constraints?" It does not
model replica spread (`max_replicas_per_node`), so a deck needing more eligible nodes than exist
still PASSes here; that shortfall is the WARN `cluster_check.py` reports live (the runbook's
measured verdict table shows the two Flink services in exactly that state).

Exit code = number of services with nowhere to land (0 = every service has a home), the same
convention `cluster_check.py` uses, so this can gate a deploy.

Usage:
  python3 code/01_platform/04_scripts/placement_check.py \
      --stack code/01_platform/01_docker/docker-stack.yml --inventory prod_vms.json
"""

import argparse
import json
import sys

import yaml

from cluster_check import _matches  # noqa: E402 — running as a script puts this dir on sys.path

DEFAULT_STACK = "code/01_platform/01_docker/docker-stack.yml"
DEFAULT_INVENTORY = "prod_vms.json"


def load_deck(path):
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_nodes(path):
    """Swarm members from the inventory, defaulting `swarm` to true like prod_node_check does."""
    with open(path, encoding="utf-8") as fh:
        inventory = json.load(fh)
    nodes = []
    for node in inventory.get("nodes") or []:
        if not node.get("swarm", True):
            continue
        nodes.append({
            "name": node.get("name"),
            "labels": node.get("labels") or {},
            "drained": node.get("expect_availability") == "drained",
        })
    return nodes


def service_requirements(deck):
    """(name, constraints, is_global) for every service in the deck, in file order."""
    for name, service in (deck.get("services") or {}).items():
        deploy = (service or {}).get("deploy") or {}
        placement = deploy.get("placement") or {}
        yield name, list(placement.get("constraints") or []), deploy.get("mode") == "global"


def eligible_nodes(constraints, nodes):
    """(eligible node names, True when a constraint form could not be evaluated).

    A drained node accepts no new task, so it cannot host a service — the same rule the live
    checker applies through the swarm's Availability field.
    """
    eligible = []
    for node in nodes:
        verdicts = [_matches(c, node["labels"]) for c in constraints]
        if any(v is None for v in verdicts):
            return [], True  # never guess: report the form instead of passing it
        if all(verdicts) and not node["drained"]:
            eligible.append(node["name"])
    return eligible, False


def main(argv=None):
    parser = argparse.ArgumentParser(description="Offline placement-satisfiability check")
    parser.add_argument("--stack", default=DEFAULT_STACK, help="stack deck to read")
    parser.add_argument("--inventory", default=DEFAULT_INVENTORY, help="prod_vms.json inventory")
    args = parser.parse_args(argv)

    deck = load_deck(args.stack)
    nodes = load_nodes(args.inventory)

    print(f"deck:  {args.stack}")
    if not nodes:
        print(f"FAIL  inventory — {args.inventory} has no node marked swarm: true")
        return 1
    print(f"nodes: {len(nodes)} — "
          + "; ".join(f"{n['name']} {n['labels']}" for n in nodes))
    print()

    failures = 0
    for name, constraints, is_global in service_requirements(deck):
        if is_global:
            print(f"SKIP  {name:<24} mode: global — runs on every node")
            continue
        if not constraints:
            print(f"PASS  {name:<24} no placement constraint")
            continue

        eligible, unknown = eligible_nodes(constraints, nodes)
        if unknown:
            failures += 1
            print(f"FAIL  {name:<24} cannot evaluate {constraints} — unsupported constraint form")
        elif eligible:
            print(f"PASS  {name:<24} {constraints} — "
                  + f"{len(eligible)} eligible node(s): " + ", ".join(eligible))
        else:
            failures += 1
            print(f"FAIL  {name:<24} nowhere to land: {constraints} matches no node's labels")

    print()
    if failures:
        print(f"{failures} service(s) have nowhere to land — fix the labels or add a node. "
              "A node holds one value per label key, and no service in this deck pins "
              "role == manager, so role=manager is never useful here.")
    else:
        print("every service has a home")
    return min(failures, 250)


if __name__ == "__main__":
    sys.exit(main())
