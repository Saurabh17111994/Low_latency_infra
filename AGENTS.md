# Streaming Trading Data Platform

Real-time pipeline: broker ticks → Fluss → Flink (signals) → Executor → Arrow broker API.
Spec-driven repo: `docs/` is the spec, `code/` is the implementation.

## Before Changing Production Code

1. Read `docs/08_implementation/00-start-here.md`, the matching component dossier
   (`03-ingestion` / `04-signal-job` / `05-execution-core`), and check `01-foundation.md`.
2. Authority order on conflict: executable code+tests > active decisions > validated DDLs >
   contracts > requirements > dossiers. Record conflicts in `01-foundation.md` and keep the
   affected work blocked until reconciled — never resolve silently.
3. Documentation-complete ≠ code-complete. Runtime claims require evidence records under
   `logs/`. Never mark work done without its mapped tests and evidence.

## Commands

- Use Makefile targets (`up down logs build test static-check gate pin-check full-audit`);
  do not hand-roll docker compose, mvn, or go test equivalents.
- `make test` covers ONLY `common` + `02_services/01_ingestion`. Other services verify
  per-dossier — never claim tested beyond it.
- `make gate` = full Monday verification gate (Go bridge suite → prebuilt E2E binaries →
  Java suite with integration flags → Python suites + doc audit). Prereqs: stack up
  (`make up`), Go 1.24+, JDK 17, warm ~/.m2. Exits non-zero on any failure; evidence lands
  in `logs/soak/monday-gates-*`. Do not run underlying suites individually — the gate
  pre-builds binaries that a clean checkout otherwise fails without.
- After production-code changes: run `make gate` (or the scoped suite) + `make static-check`.

## Testing Workflow

1. **Smoke test first, then the real test.** Whenever you want to perform any test, first
   add/run a smoke test for that change, make it pass, and only then run the actual test.
   Never run the full/actual test before its smoke test has passed.
2. **Document after verification, not before.** Once a test for an entity (component,
   function, service, contract) is added AND verified passing, immediately write a
   comment (inline in code) or a short note (doc/dossier entry) stating what that entity
   does / its function. This is done only after the test exists and passes — so the next
   person does not have to re-verify what each entity does.

## Hazards

- DDL in `code/01_platform/02_sql/ddl/` is reconciled proposals, NOT applied anywhere —
  blocked until pinned Fluss/Flink compatibility and schema lifecycle tests pass. Never
  apply DDL or describe schemas as live.
- Container images are digest-pinned (`FLUSS_IMAGE` must be an immutable digest). Never
  swap digests for moving tags. `make pin-check` enforces this.
- Compose base is `docker-compose.yml` with overlays (bench/p7/p10/soak) that override
  ports via `!override` (p10 remaps to 19xxx). Do not assume base ports hold on overlays.
- Networks are security boundaries: `trading-net` / `execution-net` / `arrow-egress`.
  Executor stays isolated from market-data networks; `make execution-network-check`
  verifies. Never add casual network attachments.
- Service status varies — check the dossier before assuming behavior exists:
  `01_ingestion` implemented+validated; `02_compute` Slice 1 + 2.1 only;
  `03_action_capture` scaffold (archive); `04_executor` (Rust Nautilus) +
  `06_execution_bridge` + `06_execution_gateway` implemented offline, flag-gated.
- Canonical data facts (DEC-039): feed modes `ltpc` (40 B) + `full` (196 B);
  timestamps are epoch milliseconds. No other formats.
- `logs/tracker-14/` holds dated evidence records — append new dated files, never edit
  past evidence.
- Secrets live in `.env` (`make env` copies from `.env.example`). Never commit secrets.

## Docs

Numbered read order `docs/01` → `09`. Update the relevant dossier in the same change
when behavior, schemas, or interfaces change.

## Branch Context (read this before ANY change)

- **Active low-latency work lives on branch `low-latency-ingestion-based-project`.** All
  commits for the low-latency ingestion rewrite (contract, proto, Go batcher, Java
  writer, integration, perf) go ONLY to that branch.
- **`main` is the stable production branch.** Never commit low-latency work to `main`;
  never merge this branch into `main` without explicit operator sign-off.
- **Before starting any task, verify the current branch:**
  `git branch --show-current` — if it is NOT `low-latency-ingestion-based-project`, do
  NOT edit code; switch first (or ask the operator).
- The low-latency implementation contract is `docs/plans/2026-08-27-low-latency-ingestion-contract.md`.
  Its §6 T1–T9 are the ONLY sanctioned implementation steps for this work.
- Do not confuse the low-latency branch with `main`: `main` contains the pre-existing
  platform (gateway, compute, EOD, DDL tools). Low-latency files (proto/, batch.go,
  BoundedQueue.java, WriterWorker.java) are new files created by THIS branch's tasks.
