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

   **Mandatory for long runs: any test/measurement expected to run >5 minutes MUST be
   preceded by a smoke test** (short run, e.g. 200s) that: (a) starts the same pipeline
   path, (b) verifies end-to-end data flow (table growth / source read counters / VALID
   snapshots), and (c) is confirmed PASSING before the real run starts. Only after the
   smoke test has passed AND been verified may the actual >5min test begin. If the smoke
   test fails or shows no data flow, STOP — fix the cause first; never start a long run
   on an unverified path.

   **No artificial sleeps >30s:** never add a `sleep` (or equivalent blocking wait) longer
   than 30 seconds to wait for a process, run, or pipeline stage. Use real completion
   signals instead: poll a readiness file/port/metric with short intervals, wait on the
   actual process exit, or check the artifact (table growth, snapshot count, job state)
   before proceeding. A long fixed sleep that hides progress is not a completion signal.
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
  `04_executor` (Rust Nautilus) + `06_execution_bridge` + `06_execution_gateway`
  implemented offline, flag-gated. (`03_action_capture` retired 2026-09-10 —
  capture path runs in the Execution Core: go-arrow bridge + executor + `common`
  projection + gateway.)
- Canonical data facts (DEC-039): feed modes `ltpc` (40 B) + `full` (196 B);
  timestamps are epoch milliseconds. No other formats.
- `logs/tracker-14/` holds dated evidence records — append new dated files, never edit
  past evidence.
- Secrets live in `.env` (`make env` copies from `.env.example`). Never commit secrets.
- Machine truth lives in `docs/ENVIRONMENT.md` (the facts ledger: swarm role, prod
  topology, image shells, ports, secrets split). Before ANY docker/swarm/deploy
  reasoning, read it - re-discovering a recorded fact wastes the investigation.
  append-only with proof+expiry per row; never edit a LIVE row in place.
- **Production deploy is unproven and cannot be proven from this PC** (FACT-001,
  FACT-012). The Fluss tiering work (CHG-182) is verified by isolated two-container
  trials plus a static `docker stack config` render - **no `docker stack deploy`
  has ever run**, because this host is a Swarm worker with no manager. Do not
  re-run those trials expecting a different result, and do not claim a production
  deploy is verified. Same boundary for the 4-VM Swarm (never exercised), the
  `FLINK_IMAGE` digest pin (FACT-009: blocked on `docker push`; do not invent a
  digest), and the `FLUSS_IMAGE` digest pin (FACT-014: the derived lake-plugin
  image exists and is built + proven against R2 (CHG-183), but `FLUSS_IMAGE`
  still names the stock digest - so a deploy today still fails the lake path;
  do not claim the pin is done, and do not invent a digest).
  Lake tiering's *classloader* gap is closed (CHG-183); whether a real tiering
  job writes parquet to the lake prefix is still unproven - that needs the
  Flink tiering service plus a `table.datalake.enabled = true` table.

## Docs

Numbered read order `docs/01` → `09`. Update the relevant dossier in the same change
when behavior, schemas, or interfaces change.

## Context budget (standing user order, 2026-09-14 — no exceptions)

- Auto-compact at ~200k tokens: when `/context` shows usage at or near 200k, run
  `/compact` immediately (standing permission — do not ask first) and continue the task after.
- Between compactions keep context lean: scope reads to the task, prefer `grep`/`glob`
  over full-file reads, never re-read unchanged files.

## Branch Context (read this before ANY change)

- **The `Low_latency_infra` repository's `main` branch is the single working branch.**
  All work (low-latency ingestion rewrite and the rest of the platform) happens
  directly on `main`. The old two-branch split is retired: `main` now contains the
  low-latency history (contract, proto, Go batcher, Java writer, integration, perf).
- **Before starting any task, verify the current branch:**
  `git branch --show-current` — it MUST be `main`. If it is not, do NOT edit code;
  switch first (or ask the operator).
- The low-latency ingestion rewrite is **complete and contract-closed** (2026-08-31;
  its implementation contract was deleted as part of the 2026-09-06 closed-plan sweep).
  The ingestion dossier `docs/08_implementation/03-ingestion.md` is the current source
  of truth for the ingestion services.
- The legacy `streaming_project` repository (old `main` line, gateway/compute/EOD
  history) is archived/retired. The full platform history is reachable from this
  repository's `main` (old `main` is an ancestor of the current line).
