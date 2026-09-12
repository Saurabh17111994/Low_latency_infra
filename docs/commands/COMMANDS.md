# Project Command Reference

Audit of every command entry point in the project, categorized from a functional
perspective. Run from the repo root
(`/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project_New`)
unless a command says otherwise.

**Fast path — the 4 commands that matter for day-to-day use:**

| # | Action | Command |
|---|---|---|
| 1 | Starts everything | `make up --profile execution-t3` |
| 2 | Enables trading | `curl -X POST http://localhost:9190/v1/approve -d "saurabh"` |
| 3 | Disables trading | `curl -X POST http://localhost:9190/v1/halt -d "saurabh"` |
| 4 | Stops everything | `make down` |

---

## A. Lifecycle (start / stop the project)

| Action | Command | What it does |
|---|---|---|
| Start everything (full stack + trading trio) | `make up --profile execution-t3` | Brings up all 18 services incl. gateway/bridge/nautilus. The execution trio (`execution-bridge`, `execution-gateway`, `nautilus`) is profile-gated, so plain `make up` does **not** start the trading path (port 9190). |
| Start data pipeline only | `make up` | 12 long-running services, no execution trio |
| Stop everything | `make down` | `docker compose down` — stops all containers |
| Stop + wipe data/volumes | `make clean` | `docker compose down -v` |
| Build all service images | `make build` | Maven-packages ingestion |
| Follow logs | `make logs` | `docker compose logs -f` |
| Seed config once | `make env` | Creates `.env` from `.env.example` (one-time setup) |

## B. Trading (the money-moving surface)

| Action | Command | What it does |
|---|---|---|
| **Enable trading** | `curl -X POST http://localhost:9190/v1/approve -d "saurabh"` | Gate HALTED → ENABLED — the single human unlock (DEC-044). The operator name doubles as the evidence marker for the sandbox run (`http.rs`). |
| **Disable trading (kill-switch)** | `curl -X POST http://localhost:9190/v1/halt -d "saurabh"` | Gate → HALTED instantly; all orders refused. Also fires on any unauthorized approve attempt (tripwire). |
| Place sandbox order + cancel (round-trip proof) | `python3 code/01_platform/04_scripts/t9_order_sandbox.py --live` | The live order harness: place → poll → assert → cancel against the broker sandbox. Needs funded margin. Exit 0 = full round-trip; 3 = chain unwired; 1 = real failure; 2 = blocked. |
| Offline order contract check | `python3 code/01_platform/04_scripts/t9_order_sandbox.py` | 12/12 static checks, no containers (reuses `t8_sandbox_contract_check.py`) |
| Execution topology check | `make execution-network-check` | Verifies execution-net/arrow-egress isolation (T8 gate 3) |

## C. Testing & verification gates

| Action | Command | What it does |
|---|---|---|
| Full Monday gate (13 checks) | `make gate` | The big verification gate (`run-monday-gates.sh`) |
| Implementation-order gate | `make gate-order` | 7 tasks in mandatory sequence; first failure blocks downstream |
| Full doc audit | `make full-audit` | Docs-vs-code truth check (3 gates + beyond-scanner sweeps) |
| Doc audit | `make docs-audit` | Manifest/ownership/test-count checks + cargo clippy/fmt + go vet |
| Unit tests (common + ingestion) | `make test` | Maven tests |
| Live Fluss drills (common + gateway) | `make drill-live` | The `FLUSS_BOOTSTRAP`-gated classes — `make test` records them as 0 tests; runs inside `make gate` step 9 |
| All local-compose checks | `make test-all` | L0–L11 pytest suites |
| Pin discipline | `make pin-check` | Version pin audit (matrix shape, SNAPSHOT ban) |
| Static script hygiene | `make static-check` | `bash -n` + shellcheck on every repo shell script |
| 09 stack offline validation | `make test-09` | `docker-stack.yml` static checks (label-only placement, encrypted overlays) |
| Stack self-check | `make stack-selfcheck` | One-host swarm mimic + `docker stack config` |

## D. Operations & maintenance

| Action | Command | What it does |
|---|---|---|
| DDL apply (schema) | `make ddl APPLY=1 EVIDENCE=<file>` | Full 9-step schema contract against live Fluss |
| DDL validate only | `make ddl` | Validate without applying |
| DDL smoke | `make ddl-apply-smoke` | Live regression smoke for the exit-code contract |
| Build DDL image | `make ddl-image` | Build the ddl-apply contract container |
| Build compute jar | `cd code/02_services/02_compute && mvn -q -DskipTests package` | Build the Flink job jar (host artifact — CHG-110 native split: the compute image is platform-only; the jar is volume-mounted, so code changes need **no image rebuild**) |
| Deploy job code | `make rollout-savepoint` | Submit the freshly built jar to the running cluster + restore from savepoint (the native job-update path) |
| EOD controller | `python3 code/01_platform/04_scripts/eod_controller.py <status\|run\|extend\|reconcile\|reset>` | End-of-day lifecycle controller (SCH-23) |
| Savepoint rollout | `make rollout-savepoint ARGS="..."` | Flink job update with dedup-state continuity (G5/T12) |
| Chaos suite | `make chaos-suite` | 4 failure drills: slot / TM / tablet / VM kill |
| Disaster drills | `make disaster-drills ARGS="--dry-run"` | Fault-injection practice runs (needs `--approve` to touch stack) |
| Seed dashboards | `make seed-dashboards` | Idempotent OpenObserve dashboard provisioning (D7) |
| Alert routing selftest | `make alert-routing-test` (needs `O2_PASSWORD`) | G6 guard: O2 rule → dev-webhook → alert-consumer JSONL end-to-end proof + malformed-delivery negative proof |
| Query alert history | `docker exec 01_docker-alert-consumer-1 python3 -c "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:9999/alerts?limit=20').read().decode())"` | Recent O2 alert deliveries from the durable JSONL (also `/stats`); port not published — query via docker exec |
| Seed alerts | `python3 code/01_platform/04_scripts/seed_alerts.py` | OpenObserve alert provisioning |
| O2 provision | `python3 code/01_platform/04_scripts/o2-provision.py` | Observability provisioning (43 alerts, dashboards) |
| R2 audit-store check | `python3 code/01_platform/04_scripts/audit_r2.py` | R2 bucket/versioning/lifecycle validation |
| Repair tablet | `bash code/01_platform/04_scripts/fluss-repair/repair-tablet.sh` | Fluss tablet repair |
| Import instruments | `bash code/01_platform/04_scripts/import_instruments.sh` | Instrument manifest import |
| Evidence ownership check | `make evidence-ownership-check` | Non-root ownership contract gate (C15) |
| Lake health check | `bash code/01_platform/04_scripts/lake-guard.sh` | Daily R2 lake guard: yesterday's day-folder + manifests + today's folder after 18:30 IST (cron-able; `LAKE_GUARD_CHECK_DAY` forces a day for tests) |
| Tiering job status / restart | `bash code/01_platform/04_scripts/tiering-start.sh [--status]` | Idempotent Flink tiering-job submit; refuses to submit unless the hadoop-mapreduce-compat jar is in both flink libs and the bad uber jar is absent (CHG-117) |
| Query the R2 lake | `bash code/01_platform/04_scripts/r2-query.sh "<sql>"` | DuckDB SQL over the iceberg lake (iceberg_scan; first run installs extensions) |
| Restore one trading day | `bash code/01_platform/04_scripts/r2-restore.sh <yyyyMMdd> [out.parquet]` | Day-folder parquet export (partition-pruned glob) |
| Tiering smoke (guarded E2E) | `TIER_WAIT=420 bash code/01_platform/04_scripts/tiering-smoke.sh 300` | Write→tier→R2 proof with GUARD A–E (see `06_operations/07-lake-archive-ops.md`) |

## E. Production / VM provisioning (future 4VM)

| Action | Command | What it does |
|---|---|---|
| VM provisioning check | `python3 code/01_platform/04_scripts/prod_node_check.py --inventory prod_vms.json` | Per-VM gate (D1.2): reachability / disk / labels |
| Self-check (offline) | `python3 code/01_platform/04_scripts/prod_node_check.py --self-check` | Proves checker logic without VMs |
| Deploy to swarm | `docker stack deploy -c docker-stack.yml trading` | The one-command production deploy (run from a swarm manager) |
| Stack config compile | `make stack-config` | Compile-only `docker stack config` validation |

---

## Notes & honest caveats

- **`make up` alone does not enable trading.** The execution trio is behind
  `profiles: [execution-t3]`; use `make up --profile execution-t3` for the
  trading path (gateway/bridge/nautilus on `execution-net`, zero host ports).
- **Trading is deliberately never automatic.** Gate boots HALTED; only the
  DEC-044 single operator (`saurabh`) can approve, and recovery after any halt
  is always a fresh human approval. This is the designed safety checkpoint.
- **The 4-command fast path is the honest day-to-day surface.** Everything else
  is verification, ops, or provisioning — not required to trade.
- Commands marked "needs funded margin" / "needs 4VM" / "needs market-hours"
  are blocked on external conditions, not on code.
