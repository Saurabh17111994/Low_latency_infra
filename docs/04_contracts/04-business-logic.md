# Segment Build Contract — Business Logic

## Boundary

The **strategy host** operator inside the Signal Flink job consumes live (forming) and closed multi-timeframe candle events and runs every `STRATEGIES`-listed strategy. Strategy evaluation is keyed by `instrument_token`. The operator never calls Arrow REST or mutates broker/position lifecycle. **(The `portfolio_id` ranking/reservation repartition is REMOVED 2026-08-15, CHG-005.)** **(TOPOLOGY UPDATE 2026-09-27: the 15 s single-timeframe candle and forming-bar stack is RETIRED — 2026-09-05 cutover, batch 3; the host reads the multi-TF live/closed candle streams — `n7-range-breakout-v1` is the first real strategy. See `08_implementation/04-signal-job.md`.)**

> **MVP scope (Slice 2.1, DEC-034; output RE-SCOPED by DEC-035, 2026-08-13):** the implemented subset is closed-candle signal detection → `Signal_Candidates` LOG append (side BUY / action ENTRY / order MARKET, execution-engine-ready; ranking fields NULL by design) + `Signal_Candidates_current` KV current-state upsert (latest/active per instrument). Forming-bar events, candidate lifecycle (bounds, supersession, expiry), ranking/reservation repartitioning, and `Trade_Decisions` are postponed — the sections below define the full-phase contract. **Phase status UPDATED 2026-08-16** (the postponed-list in this paragraph records the Slice 2.1 scope): the forming-bar **handoff** (builder → detection, placeholder mirrored-breakout rule) + `forming_bar` KV persistence landed 2026-08-16; candidate lifecycle (bounds, supersession, expiry) and the full forming-bar business logic remain pending; ranking/reservation repartitioning was **removed from scope (CHG-005, 2026-08-15 — not deferred)**; the `Trade_Decisions` dual-sink machinery is implemented but **gated off** (`TRADE_DECISIONS_ENABLED=false`, no producer in scope). **Phase status UPDATED 2026-09-27:** the forming-bar handoff, the `forming_bar` KV persistence, and the placeholder mirrored-breakout rule are RETIRED (2026-09-05 cutover, batch 3); signal detection now runs on the strategy host (`n7-range-breakout-v1` range-breakout, reading multi-TF live/closed candles). Candidate lifecycle (bounds, supersession, expiry) remains pending.
>
> **REQUIREMENT CHANGE (user decision, 2026-08-13):** the candidate output splits into a **LOG audit** (`Signal_Candidates`, append-only — one new row per fired signal, never updated; matches "immutable candidate audit") and a **KV current-state** (`Signal_Candidates_current`, PK `(instrument_token)` — latest/active candidate per instrument, supersession overwrites in place). This reverses the R-084 KV conversion for the audit table and resolves the R-084 dead-supersede-chain problem. **Implemented 2026-08-13** (Stages 3–6 executed, live DDL applied — `Signal_Candidates` LOG v3 id 607 + `Signal_Candidates_current` KV companion id 608; see `04-signal-job.md` banner).

## Identities and state

- Immutable `candidate_id` and `evaluation_id`
- Immutable `instruction_id` only when a winning executable request is created
- Stable `trade_context_id`
- Versioned strategy/configuration and setup identity
- ~~`portfolio_id` for ranking and capacity scope~~ — **REMOVED 2026-08-15 (CHG-005)**
- ~~Reservation lifecycle keyed by `reservation_id` and `portfolio_id`~~ — **REMOVED 2026-08-15 (CHG-005)**
- ~~Reservation states: `RESERVED`, `SUBMITTING`, `PENDING`, `OPEN`, `RELEASE_PENDING`, `RELEASED`, `UNKNOWN`~~ — **REMOVED 2026-08-15 (CHG-005)**
- Legal transitions and expected prior versions are explicit; stale updates are rejected
- Candidate bounds: expiry, invalidation, maximum active per instrument and portfolio, timer bounds, cleanup

~~Reservations are rebuildable from a durable event feed or KV projection. Missing, stale, conflicting, or `UNKNOWN` portfolio evidence suppresses instruction publication~~ — **REMOVED 2026-08-15 (CHG-005).**

Unknown/stale external state suppresses publication. Unknown attempts keep capacity reserved.

## Outputs

- `Signal_Candidates`: immutable candidate audit LOG (append-only, one row per fired signal — RE-SCOPED 2026-08-13, **implemented 2026-08-13 — LOG v3 id 607 (dual-sink)**; the pre-landing "current code writes KV" note is superseded)
- `Signal_Candidates_current`: candidate current-state KV, PK `(instrument_token)`, latest/active per instrument, supersession overwrites (NEW — RE-SCOPED 2026-08-13, **implemented 2026-08-13 — KV companion id 608**)
- ~~In-process candidate input to Ranking~~ — **REMOVED 2026-08-15 (CHG-005)**
- ~~Immutable `Trade_Decisions` instruction after ranking/reservation~~ — **REMOVED 2026-08-15 (CHG-005)**

Changed executable parameters create a new instruction. The old instruction is disposed/superseded before replacement submission. Same unchanged winner is audit-only.

## Acceptance

Strategy detection on the multi-TF live/closed candle path, setup repetition rules, identity collision tests, stale-state suppression, checkpoint restore, and deterministic replay from a fixed snapshot pass. **(Updated 2026-09-27: the forming-bar stack is RETIRED — 2026-09-05 cutover.)** **(Reservation transitions and immutable instruction/supersession REMOVED 2026-08-15, CHG-005.)**

## Requirement traceability

- Functional: `REQ-SS-001` through `REQ-SS-011`
- Cross-cutting: `03-non-functional.md` §§3.1–3.5, 3.8; `04-data.md` §§4.2–4.4; `05-interfaces.md` §§5.3–5.4, 5.11; `06-operational.md` §§6.2–6.5, 6.10

See `../02_requirements/02-functional/04-business-logic.md`.
