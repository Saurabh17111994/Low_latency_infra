# Networking

## Environment-specific topology

### Local and integration

Docker Compose uses an isolated single-host bridge network for deterministic development and integration testing. Service names provide internal DNS. Host exposure is limited to the endpoints explicitly required by local operators and tests.

For the offline execution-bridge profile, Compose adds an internal `execution-net` for private
gateway/Nautilus/bridge traffic and a separate `arrow-egress` network. Only the Go order-path
bridge joins `arrow-egress`; it has no published host port. The policy is checked against the
resolved Compose model by `code/01_platform/04_scripts/execution_network_check.py`. Runtime
cross-container route probes are deferred to T8 because the gateway and Rust service are not yet
deployed. The existing ingestion service remains a separately documented market-data Arrow
exception and is not an order-path execution service.

### Production

Docker Swarm uses separate encrypted overlay networks (`--opt encrypted` for `trading-net`, `execution-net`) or equivalent TLS. v1: the three workload VMs are Swarm Manager+Worker and host Fluss replicas/quorum and Flink capacity with anti-co-location (all three replicas across separate VMs, 2CPU/2GB manager reserved per VM). v2: M1-3 are Manager ONLY (drained), W1-3 (+W4+) are Workers. The observability VM (O1) hosts OpenObserve outside the Swarm and is not required for order-safety correctness. Same `docker-stack.yml` works for both via role labels.

Each VM has a 250 GB SSD starting allocation. Final CPU, RAM, SSD IOPS/throughput, and network bandwidth are `EVIDENCE-BLOCKED` until `PERF-PROD-60000-001` and `FAIL-VM-LOSS-60000-001` pass.

**Provider availability disclaimer:** A cloud provider's 99.99% uptime claim is not proof of application, broker-route, or order-path availability. Platform monitoring, broker-connectivity monitoring, recovery tests, and safe-halt behaviour must be independently proven.

Compose is not production HA evidence. Production placement, resources, restart policy, health checks, ingress, rollback, and persistent storage are defined in the Swarm stack and validated by one-workload-VM failure tests.

## Logical communication paths

```text
Arrow market-data endpoint ─TLS─► Ingestion
Ingestion ─Fluss client─► Fluss coordinator/tablets
Fluss coordinator/tablets ─ZooKeeper client (2181)─► ZooKeeper ensemble
ZooKeeper ensemble members ─2888/3888─► ZooKeeper peers (leader election, quorum)
Flink JobManagers ─ZooKeeper client (2181)─► ZooKeeper ensemble (HA leadership)
Signal and Babysitter jobs ─version-pinned connector─► Fluss
Arrow postback endpoint ─TLS─► go-arrow bridge ─local protocol─► Nautilus Execution Service
Nautilus Execution Service ─Fluss changelog─► execution intent/control state
Nautilus ExecutionClient ─local authenticated protocol─► go-arrow bridge ─TLS─► Arrow REST ─► broker
Signal/capture path/platform health/operators ─Safety_Halt_Requests─► custom execution control
EOD controller ──► Fluss source + S3/lake
All components ─supported telemetry protocol─► OpenObserve
Operators ─controlled authenticated interface─► gate/reconciliation controls
```

Exact ports, endpoint paths, authentication fields, compression, subscription limits, timeout values, and protocol versions are evidence-gated. They must not be copied from historical examples into production contracts.

## Network exposure rules

Production exposes only explicitly required operator, health, or API endpoints through controlled ingress and firewall rules. Fluss tablet/internal RPC, ZooKeeper client/peer ports (2181/2888/3888 — internal-only, never publicly exposed), checkpoint storage, service-to-service data paths, and execution state are not publicly exposed.

The observability VM cannot authorize orders, change the Nautilus execution gate, or erase execution audit if OpenObserve is unavailable. Local UI exposure is for development only and is not a production security model.

## Identity and authorization boundaries

- Ingestion can append to market-data tables and write discontinuity/quarantine evidence.
- Signal jobs can write signal/candle outputs and their checkpoint state. (**Ranking/instruction outputs REMOVED 2026-08-15, CHG-005.**)
- The go-arrow bridge can hold broker credentials, consume Arrow postbacks, and call Arrow.
- Only the execution Go bridge can join the order-path `arrow-egress` network. Java, Rust, Flink,
  and Fluss services must use the internal execution network or their own platform network and
  must not receive Arrow order credentials. The ingestion market-data bridge is a separate,
  explicitly approved exception.
- Nautilus Execution Service can consume immutable intent, apply OMS/position/risk/reconciliation behavior, and emit execution events.
- Custom execution control/projection glue can consume safety-halt requests and write execution-owned control state and Fluss projections.
- EOD controller owns manifest creation/verification and retention extension.
- Babysitter reads `Positions` and writes no action in MVP.
- Operators can perform authenticated reconciliation/approval operations but cannot bypass the gate with an unaudited call.
- OpenObserve receives telemetry and cannot authorize execution.

All service identities are least-privilege and separate. Exact Fluss ACLs, mTLS support, and credential mechanisms are version-gated.

## Connectivity failure behavior

- Broker disconnect or authentication exhaustion makes the affected service not ready and alerts operations.
- Fluss unavailability or append uncertainty follows the component retry policy, exposes uncertainty, and prevents false loss claims.
- Changelog discontinuity, checkpoint failure affecting order correctness, or durable Nautilus/control state loss halts new money-moving calls.
- OpenObserve outage does not erase local durable audit; lack of required observability can make the platform not ready for live-money operation.
- Safety-halt request path must remain available through a degraded network or Executor independently falls back to local health detection.

## References

- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Operational requirements: `../02_requirements/06-operational.md`
- Security requirements: `../02_requirements/03-non-functional.md` §3.6
- Platform contract: `../04_contracts/09-platform-runtime.md`
