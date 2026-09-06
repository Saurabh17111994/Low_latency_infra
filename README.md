# Streaming Trading Data Platform

<!-- markdownlint-disable MD013 MD060 -->

Real-time market-data → feature → trade-execution pipeline:
broker ticks → **Fluss** (streaming bus + storage) → **Flink** (features + strategy) →
trade instructions → **Executor** → **Arrow REST API** (`POST /order/regular`) → broker.
Fills are captured independently by a separate Action Capture service.

> Repository layout follows a **Spec-Driven Development (SDD)** structure.
> This README is a discoverability map — see each directory's docs for detail.

> **Branch context (read before any work):** all work happens directly on `main`
> (single-branch workflow; the old two-branch split is retired). Verify with
> `git branch --show-current` before changing code; see `AGENTS.md` → "Branch Context"
> and the implementation contract `docs/plans/2026-08-27-low-latency-ingestion-contract.md`
> (its §6 T1–T9 are the sanctioned implementation steps).

---

## Repository map

```text
streaming_project/
├── docs/        long-lived documentation — what the system IS
├── code/        production implementation — what is BUILT
├── Makefile     common dev commands (make up / down / ddl / build)
└── CLAUDE.md    agent instructions
```

### `docs/` — reference documentation

Stable, long-lived. Read these to understand the system.

**Read in numbered order, 01 → 09:** project → requirements → architecture → contracts → deployment → operations → testing → implementation dossiers → data gaps.

| Directory | Contents | Read when |
| ----------- | ---------- | ----------- |
| [`01_project/`](docs/01_project/) | Project charter, system context, quality targets, decisions, risks, delivery scope | First read |
| [`02_requirements/`](docs/02_requirements/) | Functional (per-segment), non-functional, data, interfaces, operational requirements | Designing/implementing any component |
| [`03_architecture/`](docs/03_architecture/) | Architecture overview, technology choices, data pipeline, networking, security, platform topology | Understanding the system shape |
| [`04_contracts/`](docs/04_contracts/) | One build-ready contract per atomic segment | Implementing a segment |
| [`05_deployment/`](docs/05_deployment/) | Release strategy, CI/CD, environments, rollback, secrets rotation | Deploying |
| [`06_operations/`](docs/06_operations/) | Operational strategy, runbooks, DR plan, maintenance | Running in production |
| [`08_implementation/`](docs/08_implementation/) | Implementation dossiers, complete testing rules, test IDs, state machines, config placeholders, and release evidence | Before changing production code or writing tests |
| [`09_data_gaps.md`](docs/09_data_gaps.md) | Operational input register for known data gaps, discontinuities, and reconciliation items | During live operations |

### `docs/08_implementation/` — implementation-ready dossiers

This is the bridge between stable requirements/contracts and production code. It defines internal module boundaries, state machines, schemas, configuration placeholders, failure behavior, observability, test IDs, and release evidence. These documents do not claim that implementation or runtime validation already exists.

Read [`docs/08_implementation/00-start-here.md`](docs/08_implementation/00-start-here.md) first.

> **Active implementation planning:** [`docs/08_implementation/01-foundation.md`](docs/08_implementation/01-foundation.md) is the master implementation checklist. Use the corresponding component dossier before writing code. This documentation-first workflow keeps requirements, schemas, interfaces, tests, operations, and release evidence aligned before production code is written.

### `code/` — production implementation

Implementation status varies by service — see each implementation dossier for the evidence-backed detail ([start here](docs/08_implementation/00-start-here.md)):

```text
code/
├── 01_platform/
│   ├── 01_docker/               local Compose stack
│   ├── 02_sql/ddl/              reconciled DDL proposals; version validation pending
│   └── 03_fluss/                Fluss configuration
└── 02_services/
    ├── 01_ingestion/            implemented & validated (Phase 2)
    ├── 02_compute/              Signal job Slice 1 + Slice 2.1 implemented; ranking/reservations pending
    ├── 03_action_capture/       scaffold (capture path → Execution Core dossier)
    ├── 04_executor/             scaffold (order path → Execution Core dossier)
    ├── 05_mock_arrow/           Mock Arrow broker (per-instrument, deterministic)
    ├── 06_execution_bridge/     Go Arrow execution bridge (AutoLogin auth, REST + WS)
    └── 06_execution_gateway/    Execution gateway (intent intake, dedup, projection)
```

**Schema status:** [`code/01_platform/02_sql/ddl/*.sql`](code/01_platform/02_sql/ddl/) contains reconciled proposals, but they are blocked from runtime application until the pinned Fluss/Flink compatibility and schema lifecycle tests pass. See [`docs/08_implementation/01-foundation.md`](docs/08_implementation/01-foundation.md).

---

## Quick start

```bash
make env      # copy .env.example -> .env, then edit secrets
make up       # docker compose up -d (full stack)
make ddl      # display the currently blocked DDL workflow; do not treat as applied until schema validation is implemented
make logs     # tail compose logs
make down     # stop
```

## Status

Phase 4.1 (Design & architecture) — **closed**.
Phase 4.2 (MVP) — **Implementation active** (live-money blocked). See [`docs/08_implementation/01-foundation.md`](docs/08_implementation/01-foundation.md).

**2026-08-18 (execution-core re-architecture):** Action Capture, Babysitter, and Executor are consolidated into one Execution Core ([`docs/08_implementation/05-execution-core.md`](docs/08_implementation/05-execution-core.md)) — Nautilus as the execution/position core with a go-arrow bridge as the sole Arrow-facing component (DEC-041). Build contracts and DEC-006 re-scope pending; live money remains blocked.

---

## Superseded history

The following were part of an earlier architecture phase and are no longer active:

- **OpenAlgo executor layer** — removed. The platform calls Arrow's REST API (`https://edge.arrow.trade`) directly via the Executor service.
- **`specs/` directory** — removed. Active planning content moved to `docs/08_implementation/`.
- **Root `plan.md`** — removed. Content absorbed into [`docs/08_implementation/01-foundation.md`](docs/08_implementation/01-foundation.md).
