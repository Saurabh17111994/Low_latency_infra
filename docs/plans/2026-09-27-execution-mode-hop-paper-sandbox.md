# Execution mode hop — paper ↔ sandbox readiness (plan)

**Created:** 2026-09-27 · **Status:** active — H2-1…H2-5 landed; the paper drill passed
(attempt 4); the H4-2 honesty sweep landed (CHG-340). Remaining: H3-2 (funded broker
window), the Monday P4-4 market-hours window.

**Operator goal (2026-09-27):** make sure paper and sandbox are fully implemented, so that
when it is time we only *change configuration* and can hop between `disabled` / paper /
sandbox whenever needed — no code work at hop time.

**Scope:** the switch contract (docs + offline checks), the paper proof on the deployed
stack, and sandbox-session readiness.

**Not in scope:** live-money enablement (release-gated, `08-live-readiness-gaps.md`); the
daily-runner posture (stays offline per D1 of the daily-runner plan); the compute
signal→intent policy (implemented, off by default).

**Related:**
- `docs/08_implementation/05-execution-core.md` — execution dossier (chain truth; T4/WP-2
  status drift + a stale "Workstream A/B" reference fixed in H4-2, CHG-340).
- `code/01_platform/04_scripts/t8_sandbox_contract_check.py` — offline switch-safety
  contract (12/12 PASS today; H1-2 extends it).
- `code/01_platform/04_scripts/t9_order_sandbox.py` — signed-envelope harness (offline
  contract, `--self-check`, `--live`, `--sign-control`).
- `code/01_platform/04_scripts/run-b4-halted-e2e.sh` — the proven fail-closed half
  (signal→intent→gateway→DEFERRED).
- `docs/06_operations/08-live-readiness-gaps.md` — live-money gate rows.
- `docs/plans/2026-09-26-daily-single-command-runner.md` — daily posture offline by D1.
- `docs/plans/2026-09-24-plan-execution-protocol.md` — item execution protocol.

## 0. Live tracker

This section is the single source of truth for build status; the body sections below are
the evidence. Where a body section and this list disagree, this list wins.
**Execution protocol:** `2026-09-24-plan-execution-protocol.md` — the operator names an
item ID; verify-first, marker discipline, smoke-before-run, test + CHG + doc,
`plan_tracker.py --write/--check`.

**Marker legend**

| Marker | Meaning |
|---|---|
| `[x]` | landed and verified (evidence named) |
| `[~]` | in progress right now |
| `[ ]` | action owed — doable offline / on the local stack |
| `[L]` | action owed that needs a live window (stack gate enablement, or the broker) |
| `[?]` | needs an operator decision before it can start |
| `[-]` | no action — trigger-gated or superseded (trigger recorded) |

#### H1 — Switch contract (offline)

- [x] **H1-1** — Hop matrix page in the runbook: modes × gate states × exact env values,
  hop and revert command sequences, safety invariants; cross-linked from the dossier.
  Landed CHG-330: `01-runbooks.md` §Execution mode hop + dossier update banner.
- [x] **H1-2** — Offline hop-contract checks: extend `t8_sandbox_contract_check.py` so the
  matrix is machine-checked (defaults fail-closed, hop requires explicit env, `fake` never
  egresses, control routes exist behind signed envelopes, daily runner refuses enablement).
  Landed CHG-330: +6 checks (mode branches, `fake` Arrow-free, credentialed `live`, signed
  control routes, daily-runner refusal), wave43 `HopMatrixChecks` +6 tests.

#### H2 — Paper proof (deployed stack, `fake` mode)

- [x] **H2-1** — Fix + complete the `t9_order_sandbox.py` live leg for today's code:
  202 = executed (bridge report), poll→assert (Fluss `Execution_Attempts`/`Order_Lifecycle`,
  plus fills/positions when present), cancel, evidence; correct gate epoch; update the
  stale test pin (failing-first). Landed CHG-329: red 6 tests → green 20/20; `/healthz`
  gate+epoch, fresh identities, log-end poll, distinct-id cancel, evidence JSON.
- [x] **H2-2** — Paper drill: `EXECUTION_BRIDGE_MODE=fake` + signed approve → harness →
  assert rows → signed halt → mode `disabled` → offline posture verified; evidence under
  `logs/exec-hop/paper-drill-<date>/`. Attempt 1 (2026-09-27, CHG-331): bridge leg proven
  (202 + fake broker id); projection leg gated — gateway `/v1/events` 503 under
  `EXECUTION_ENABLED=false`; halted + reverted cleanly. Attempt 2 (2026-09-27): gateway
  enabled but failed closed on an invalid `Execution_Intent` row — the `instruction_id`
  contract break (H2-4); reverted. Attempt 3 (2026-09-27, after CHG-333): the validator fix
  proven (all 9 rows accepted, reader reached the forward leg) but blocked by H2-5 — the
  missing `Execution_Gate` row keeps `/readyz` 503; reverted. **Attempt 4 (2026-09-27, after
  CHG-334/335/336): PASS** — the durable row exists before the replay (`/readyz` 200),
  `BOOT_HALT` hydrated epoch 1, the signed approve walked the sanctioned path to `ENABLED`
  epoch 3, `RENEW` every ~10 s, `t9_order_sandbox.py --live` PASS (`event_emission:accepted`,
  `Order_Lifecycle` 0→1, cancel 202), signed halt epoch 4 / fence 2, reverted to the offline
  posture. Evidence `logs/exec-hop/paper-drill-20260927/attempt4/` + `notes.md`.
- [x] **H2-3** — Projection enablement for the paper drill (operator decision): decouple the
  gateway master switch (`GATEWAY_EXECUTION_ENABLED`, default false; gateway service only,
  nautilus keeps its boot guard), align the harness required tables (`Order_Lifecycle`
  required; attempts/fills/positions reported), extend `t8` + the runbook hop page, then
  re-run H2-2 in a second short window. Landed 2026-09-27 (CHG-332); the drill re-run landed
  with attempt 4: the gateway enabled alone, `/readyz` 200, `/v1/events` accepted the
  lifecycle event (`event_emission:accepted`) and `Order_Lifecycle` grew 0→1 — the decoupled
  switch is proven end-to-end.
- [x] **H2-4** — `instruction_id` contract break (operator decision 2026-09-27, option A:
  widen the validator to the canonical format). The compute builder emits
  `ei-v1-<sha256>` (70 chars) while the gateway validator capped at 64
  (`IntentValidator`, P3-094); the gateway failed closed on the live intent rows, so the
  compute→gateway intent path had never run end to end (CHG-331 attempt 2). Landed
  CHG-333: `INSTRUCTION_ID` = `ei-v1-[0-9a-f]{64}` OR `[A-Za-z0-9_-]{1,64}`; gateway
  tests red 2 → 18/18; compute pin mutation 69≠70 → 10/10; new cross-service
  `test_instruction_id_contract.py` 3/3; dossier grammar; gateway image rebuilt
  (`make images`). Live half proven in CHG-331 attempt 3 (the reader accepted all 9 real
  rows and reached the forward leg); the drill itself is gated by H2-5.
- [x] **H2-5** — `Execution_Gate` has no writer in the deployed topology (finding from
  CHG-331 attempt 3; operator decision 2026-09-27: native path — no gateway semantic
  exception, no data cleanup). The gateway's forward leg looks up the durable gate row in
  Fluss (`NautilusIntentClient` → `FlussControlStateStore`); the row does not exist
  because the deployed nautilus runs the in-memory gate (compose sets no `DURABLE_*`
  flags) and no other component writes the table, while the gateway wires the
  non-authoritative placeholder. A missing row flips `readiness.fluss(false, "key not
  found")` → `/readyz` 503 → `/v1/events` refuses → the projection leg cannot run while
  pending intents exist. **Decision 2026-09-27: D2** (gateway-written; executor stays
  isolated, gateway is the execution core's Fluss writer). Scope doc
  `docs/plans/2026-09-27-executor-gate-row-native.md` pins the protocol (gateway boot
  `init`s the row HALTED before the replay; new `POST /v1/gate` report endpoint; executor
  durable-first adoption; 30 s lease / 10 s renew / halt-on-loss). Landed: S1 gateway writer +
  endpoint (CHG-334 `8121c535`, boot-epoch follow-up `f08a8926`), S2 executor durable-first
  reports (CHG-335 `aae8c17c`), S3 compose/stack env + runbook (CHG-336). S4 — image rebuild +
  the H2-2 drill re-run — is the acceptance test. **S4 PASSED** (attempt 4, 2026-09-27
  ~11:23–11:26 UTC): `/readyz` 200 on the enabled boot (row created before the replay);
  `BOOT_HALT` adopted epoch 1; signed approve wrote the sanctioned path `ENABLED` epoch 3 /
  fence 1 and the executor adopted it; `RENEW` every ~10 s; t9 live **PASS**
  (`event_emission:accepted`, `Order_Lifecycle` 0→1); signed halt epoch 4 / fence 2; both
  switches reverted to the fail-closed defaults. Follow-up CHG-337 quiets the locked-posture
  boot retries (first WARN then DEBUG, 30 s cap) + the retry-then-hydrate test.

#### H3 — Sandbox readiness (funded window)

- [ ] **H3-1** — Sandbox preflight: checklist + preflight mode in the harness (no live
  call): stack up, mode `live` explicit, credentials only in the bridge, gate HALTED +
  epoch readable, market hours, instrument valid; prints the one command sequence.
- [L] **H3-2** — Funded sandbox session: one RCF-EQ ×1 place→poll→cancel against the real
  broker with the same harness; evidence + halt; closes the "live order never placed" row's
  evidence half.

#### H4 — Decisions and follow-ups

- [x] **H4-1** — Paper evidence flavor: **(A) the deployed fake-bridge drill is the paper
  evidence** (operator decision 2026-09-27); the in-process `nautilus-sandbox` engine stays
  out of scope (B deferred unless a release row later requires `engine_exercised: true`).
- [x] **H4-2** — Honesty-notes refresh after H2-2: `t9_order_sandbox.py` exit-3 premise,
  `t9paper` "awaits LiveNode wiring" + the missing "Workstream A/B" reference, dossier
  T4/WP-2 drift. Landed 2026-09-27 (CHG-340): exit-3 classifier renamed
  `GATE-NOT-ENABLED` (the chain is wired and proven; only the gate posture is classified),
  the t9paper notes now cite the H4-1 decision, the dossier T4/WP-2 rows read DONE
  (hosted `LiveNodeRuntime`, CHG-054/079); test pins 22/22, self-check PASS, 336 Rust lib
  pass, docs gates green.
- [-] **H4-3** — Live-money enablement — release-gated, not this plan (trigger: H3-2 green
  + the `08-live-readiness-gaps.md` rows + approvals).

**Roll-up**

| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| H1 — Switch contract (offline) | 2 | 2 | 0 | 0 | 0 | 0 | 0 |
| H2 — Paper proof (deployed stack, `fake` mode) | 5 | 5 | 0 | 0 | 0 | 0 | 0 |
| H3 — Sandbox readiness (funded window) | 2 | 0 | 0 | 1 | 1 | 0 | 0 |
| H4 — Decisions and follow-ups | 3 | 2 | 0 | 0 | 0 | 0 | 1 |
| **Total** | **12** | **9** | **0** | **1** | **1** | **0** | **1** |

## Overview — the final product

One documented matrix (mode × gate × env), one harness command per mode, and both paths
proven:

- **paper** (`EXECUTION_BRIDGE_MODE=fake`) runs the full deployed chain — intent → nautilus
  route → bridge fake lifecycle → postback → Fluss rows — with observed evidence, then
  reverts to the offline posture in one scripted sequence;
- **sandbox** (`EXECUTION_BRIDGE_MODE=live`) uses the *same* harness and command sequence,
  so the only difference between the paper and sandbox runs is the mode value and the
  account being funded;
- **disabled** stays the default and the daily runner keeps refusing enablement;
- live money stays release-gated and untouched by this plan.

## 1. Goal and success bar

Success = all five:

1. The hop matrix is written down and machine-checked offline (H1-1, H1-2).
2. A paper drill on the local stack passes with observed evidence: intent accepted (202),
   bridge report, Fluss rows populated, cancel acknowledged (H2-1, H2-2).
3. The drill ends with the gate `HALTED` and the bridge back to `disabled`; the daily board
   reports the offline posture again (H2-2).
4. The sandbox session is one command away: preflight green, command sequence printed and
   rehearsed on paper mode (H3-1).
5. One funded market-hours session produces the live place→cancel evidence with the same
   harness (H3-2).

## 2. Verified state (2026-09-27 audit)

| Area | State today (verified in tree) | Consequence for the hop |
|---|---|---|
| Intent route `/v1/intents` | Gate ENABLED → forwarder `send_command` → 202 success / 409 rejected / 503 UNKNOWN or transport error; durable attempt guard (claim → SUBMITTING → terminal); place/cancel/amend tests exist | The executed path **exists**; the note that says "acknowledges without executing" is stale (H2-1/H4-2). |
| Bridge modes | `EXECUTION_BRIDGE_MODE` = `disabled` default / `fake` full simulated lifecycle / `live` real Arrow AutoLogin | The hop switch exists; defaults fail closed. |
| Gate control | Boots `HALTED`; `/v1/approve` + `/v1/halt` take signed envelopes (operator + evidence, current epoch); `gate_epoch` starts at 1 | Any hop to paper/sandbox needs the signed approve; the epoch must be read from `/healthz`, not assumed. |
| Compute intents | `EXECUTION_INTENT_ENABLED` off by default; B4 halted E2E proven (`run-b4-halted-e2e.sh`) | The drill can post direct to nautilus (same path the sandbox session uses); no compute change needed. |
| Offline contract checker (t8) | 12/12: execution-net internal, arrow-egress bridge-only, zero host ports, defaults HALTED/disabled, no prod creds, checkpoint gate | Extend it for the hop matrix (H1-2); do not duplicate existing checks. |
| Harness live leg (t9) | Stale: treats a 202 as "unwired", signs `gate_epoch=0`, has no poll/cancel implementation; `test_12_t9_order_sandbox.py` pins the stale premise | Fix before any drill (H2-1) — otherwise a green paper run would be misreported. |
| Postback → projections | WP-4/T6 writers live-verified; never exercised with gate ENABLED + fake mode end-to-end | H2-2 is the first full enabled-fake run; any gap it finds becomes a scoped fix. |
| T9 paper bins | Scripted scenario vectors, `engine_exercised: false`; reference "plan Workstream A/B" which does not exist in the tree | H4-1 decides whether to wire the in-process `nautilus-sandbox` engine; H4-2 fixes the reference. |
| Live evidence | One place reached Arrow 2026-08-25 (RCF-EQ ×1, `MARGIN ERROR` — unfunded); no fill/cancel/reconcile evidence | H3-2 needs a funded account + one market-hours window. |
| Daily runner | Refuses `EXECUTION_ENABLED ≠ false` and `EXECUTION_BRIDGE_MODE ∉ {"", "disabled"}` | By design (D1); the hop is a separate sanctioned procedure, never a daily action. |
| Dossier | T4 row says WP-2 "DEFERRED"; `engine.rs`/`main.rs` implement the hosted `LiveNodeRuntime` (2026-08-21) and later updates say WP-2 DONE | Doc drift; fix in H4-2. |

## 3. Workstreams

### H1-1 — Hop matrix runbook page

- **GIVES YOU:** one page an operator can follow: the three modes × gate states, the exact
  env keys and values, the hop sequences (`disabled → fake` for paper, `disabled → live` for
  sandbox), the signed approve/halt commands, the revert, and the invariants.
- **FIT:** `docs/06_operations/01-runbooks.md` execution section; linked from the dossier
  and the daily-runner runbook page.
- **COST:** docs only, ~1 page.
- **ACTION:** draft the matrix from the verified files (compose env, control routes, posture
  guard, t8/t9 scripts), cross-link, run docs-audit.
- **WRONG IF:** the matrix names an env key or command that does not exist in the files
  (verify each row against the tree, not memory).

### H1-2 — Offline hop-contract checks

- **GIVES YOU:** machine-checked switch safety, so a future edit cannot silently make the
  hop unsafe or impossible.
- **FIT:** extend `t8_sandbox_contract_check.py` (it already covers the isolation and the
  fail-closed defaults); tests under `code/01_platform/04_scripts/tests/` auto-join the gate.
- **COST:** small — pure parsing of compose/env/scripts, no stack.
- **ACTION:** add checks: (a) mode `fake` is selectable but never a default; (b) gate
  enablement requires the signed envelope routes; (c) the daily runner refuses `fake` and
  `live`; (d) `fake` mode makes no Arrow call (bridge-only egress + code path); then run the
  scripts suite; one CHG.
- **WRONG IF:** a check duplicates an existing t8 row, or asserts by substring where the
  parsed value is available.

### H2-1 — Fix + complete the harness live leg

- **GIVES YOU:** a `--live` harness that matches today's executor and reports honestly:
  202 = executed (bridge report with `broker_order_id`), poll→assert on the Fluss tables,
  cancel, evidence JSON; correct gate epoch read from `/healthz`.
- **FIT:** `t9_order_sandbox.py` + `tests/test_12_t9_order_sandbox.py` (+ the wave43 pin);
  reuses the existing in-network transport.
- **COST:** medium — one session; the classification fix is failing-first.
- **ACTION:** flip the stale test expectation (202 → executed, not `LIVE-CHAIN-UNWIRED`),
  watch it fail, implement the poll/cancel/evidence leg, keep `--self-check` offline, CHG.
- **WRONG IF:** the harness still treats a 202 as unwired, or the poll is answered from a
  template instead of reading the tables.

### H2-2 — Paper drill on the local stack

- **GIVES YOU:** observed end-to-end paper proof and the hop demonstrated:
  `disabled → fake + signed approve → drill → signed halt → disabled`.
- **FIT:** the existing execution-t3 profile; the harness from H2-1; the day board for the
  post-revert posture check.
- **COST:** one ~30–60 min window on the local stack; no market risk (`fake` never reaches
  Arrow); needs the operator's go before gate enablement.
- **ACTION:** pre-flight (stack up, t8 green) → set mode `fake` → signed approve → run the
  harness → capture rows/evidence → signed halt → mode `disabled` → verify the offline
  posture; archive under `logs/exec-hop/paper-drill-<date>/`.
- **WRONG IF:** the drill ends with the gate still ENABLED, the mode not back to `disabled`,
  or any real-broker egress is attempted.

### H3-1 — Sandbox session preflight

- **GIVES YOU:** a preflight that makes the funded session one command: it verifies every
  precondition without a live call and prints the exact approve → run → halt sequence.
- **FIT:** a preflight mode in `t9_order_sandbox.py` (or a small wrapper); documented on the
  H1-1 page.
- **COST:** small.
- **ACTION:** implement with offline fixtures; assert it performs no broker call and needs
  no credentials in the harness env (credentials live only in the bridge container); CHG.
- **WRONG IF:** preflight reaches the broker, or duplicates the harness's own guards.

### H3-2 — Funded sandbox session

- **GIVES YOU:** the live evidence row: one RCF-EQ ×1 place → poll → cancel against the real
  broker, funded, with evidence + halt.
- **FIT:** the same harness the paper drill used — that is the hop-parity proof (only the
  mode value and the account differ).
- **COST:** external — a funded account + one market-hours window + the operator present;
  DEC-044 approval review.
- **ACTION:** when the account is funded: preflight → approve → `--live` → halt; archive the
  bundle; update `08-live-readiness-gaps.md` row 1.
- **WRONG IF:** run unfunded (`MARGIN ERROR`, as 2026-08-25), without the signed approve, or
  without archiving the evidence.

### H4-1 — Paper evidence flavor (decision)

- **GIVES YOU:** the choice that fixes what "paper evidence" means for T9.
- **FIT:** A = no new code beyond H2; B = a new Rust workstream (sandbox exec client wired
  into a `LiveNode` run, observed fills).
- **COST:** A ≈ 0; B medium (Rust, pinned `nautilus-sandbox` API).
- **ACTION:** operator picks; if B, it becomes a follow-up plan item with its own CHG.
- **WRONG IF:** A is chosen but a downstream release row still requires
  `engine_exercised: true` (check which rows cite the T9 paper bins first).

### H4-2 — Honesty-notes refresh

- **GIVES YOU:** no statement in code or docs that contradicts the proven state.
- **FIT:** `t9_order_sandbox.py` docstring/exit-3 text, `t9paper` comments, the dossier T4
  row and the "Workstream A/B" reference, runbook cross-links.
- **COST:** small, after H2-2.
- **ACTION:** edit the notes to match the evidence exactly; docs-audit; include in the H2
  CHG or a dedicated docs CHG.
- **WRONG IF:** a note is "fixed" to claim more than the evidence shows.

### H4-3 — Live-money enablement (trigger-gated)

- **GIVES YOU:** the boundary, recorded: live money is not enabled by this plan.
- **FIT:** `docs/06_operations/08-live-readiness-gaps.md` + the release protocol.
- **COST:** n/a.
- **ACTION:** none until H3-2 is green **and** the gate rows and approvals land.
- **WRONG IF:** anything here is read as an enablement path.

## 4. Safety invariants and approvals

1. `fake` can never reach Arrow: only the bridge joins `arrow-egress`, and the fake path
   makes no broker call — checked offline by t8 and re-asserted by H1-2.
2. Any gate enablement is a signed DEC-044 action (operator + evidence + current epoch); no
   automatic enable or resume.
3. Revert is scripted first: the H2-2 sequence ends with a signed halt + mode `disabled`,
   and the day board must show the offline posture again.
4. The daily runner is untouched and keeps refusing enablement (D1).
5. H3-2 is the only real-broker call in this plan: funded account, market hours, operator
   present, evidence archived.
6. The hop preflight runs the intent guard (`--intent-guard --mode paper|live`, Q4 decision
   2026-09-27, CHG-342): pending uncommitted `Execution_Intent` rows are forwarded at the
   next `ENABLED` window (the gateway cannot tell old from new); paper warns, live refuses
   when a pending row is older than 1 h.

## 5. Evidence and landing

- Every code item: failing-first test, one CHG record (`CHG-329+`), doc/dossier update, and
  the §0 marker flip; the scripts suite auto-discovers new tests.
- H2-2 evidence: `logs/exec-hop/paper-drill-<date>/` — approve/halt envelopes, harness
  output, Fluss row excerpts, day-board status, pre/post mode values.
- H3-2 evidence: the harness bundle (default `logs/nautilus-execution/`) plus a summary in
  `logs/exec-hop/sandbox-<date>/`.
- Tracker: `python3 code/01_platform/04_scripts/plan_tracker.py --plan <this> --write` and
  `--check`.

## 6. Open decisions

| # | Decision | Recommendation |
|---|---|---|
| H4-1 | Paper evidence flavor: deployed fake-bridge drill only, or also the in-process sandbox engine | A (deployed drill) — it exercises the real chain; B can be added later if a release row requires it. |
| — | H2-2 window: when to enable the gate on the local stack for the paper drill | As soon as H1/H2-1 are green; the operator approves the window. |
| — | H3-2 funding: account funding status and the market-hours window | External; ties to the open broker-side question. |
