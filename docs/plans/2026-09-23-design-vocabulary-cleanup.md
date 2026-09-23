# Design-vocabulary cleanup -- retire the letters, keep the history

**Status:** APPROVED with decisions (2026-09-23). Nothing executed yet.
**Owner:** agent (implementer). Operator approved the section-8 decisions 2026-09-23.
**Prepared by:** the session that inventoried the Design-A/Design-B references.

## 1. Problem

"Design A" and "Design B" are **option labels from one decision** (DEC-040, 2026-08-17), but they
have been used in prose as if they were mechanism names. The mechanism they name has since been
replaced a second time, and the same label also names a *build artifact* that is still current. Net
effect: one string, three meanings, 121 occurrences across 27 files, and every supersession note
re-prints the name -- so the count grows with each revision.

Meanings in use today:

1. **Design A** -- dedup state in a Fluss KV table (`fingerprint_dedup`). DEC-038, 2026-08-14. **Retired.**
2. **Design B** -- dedup state as authoritative Flink keyed state: `MapState` + native `StateTtlConfig`
   expiry. DEC-040 / CHG-022, 2026-08-17. **Retired soon after.**
3. **Current** -- operator-local heap count window: `FingerprintDedupFunction`, uid
   `fingerprint-dedup-v2`, `DEDUP_WINDOW_ENTRIES` plain fields, intentionally not checkpointed.
   Executed `c0c50ee6` (2026-09-03), recorded late as CHG-303, decided as DEC-054.
4. **"The Design-B artifact" (DB2)** -- the **dual-sink signal build**. **Still current.** There is no
   build profile selecting single- vs dual-sink, and the swarm dossier states there is no
   pre-dual-sink signal build. Its operational text (never set `allowNonRestoredState`, restore from
   the dual-sink-era checkpoint, cutover/rollback directions) is **live guidance**, not history.

## 2. Authoritative inventory (case-sensitive, 121 hits / 27 files)

Method: `grep -cE 'Design-B|Design B|Design A|Design-A|DesignB|DESIGN B|DESIGN-B'` over `docs/` and
`code/`. A case-insensitive pass gives ~141; the difference is ordinary prose ("design a ..."), which
is why the guard in section 6 is case-sensitive.

| file | hits | class | disposition |
| --- | --- | --- | --- |
| docs/08_implementation/04-signal-job.md | 40 | live dossier, mechanism | consolidate into the anchor; keep dated history |
| docs/08_implementation/09-production-swarm.md | 9 | live, ARTIFACT | rename label, keep guidance |
| docs/08_implementation/11-testing-and-release.md | 7 | live, dated test rows | keep rows, mark them dated; fix current-value cells |
| docs/plans/2026-08-25-live-readiness-unified-plan.md | 9 | dated plan | DO NOT EDIT (append-only rationale trail) |
| docs/plans/2026-09-22-fluss-1.0-upgrade.md | 1 | dated plan | DO NOT EDIT |
| docs/06_operations/01-runbooks.md | 5 | live runbook | rename label; keep the alert and its threshold |
| docs/04_contracts/02-storage.md | 2 | live contract | rewrite to current mechanism + DEC ids |
| docs/01_project/04-decisions.md | 2 | decision register | ALREADY CORRECT -- no change |
| docs/05_deployment/change-records/*.md (CHG-025 7, CHG-022 7, CHG-023 5, CHG-303 3, CHG-021 2) | 24 | history | DO NOT EDIT (append-only, T8.2) |
| docs/08_implementation/00-start-here.md, 01-foundation.md, 02-schema-storage.md, 03-ingestion.md | 1 each | live, incidental | single-line fixes |
| code/02_services/02_compute/src/test/java/.../DedupRocksDbThroughputMemoryIT.java | 2 | test name lies | RENAME class + DisplayName + gate env var |
| code/02_services/02_compute/src/test/java/.../SignalJobCompactCheckpointRestoreIntegrationTest.java | 1 | already documents retirement | no change (it is the model for the rest) |
| code/02_services/02_compute/src/main/java/.../SignalJob.java | 1 | live comment (458, 463) | fix comment, verify the 128 MB setting separately |
| code/02_services/02_compute/src/main/java/.../SignalJobConfig.java | 1 | live comment (713) | fix comment only |
| code/01_platform/04_scripts/o2-provision.py | 1 | comment tied to a metric (1131) | fix comment |
| code/01_platform/01_docker/openobserve/dashboards/{manifest,dedup-state,compute-decision}.json | 4+3+3 | live dashboards | rename labels; check for dead metric queries |
| code/01_platform/02_sql/ddl/24_fingerprint_dedup.sql | 1 | parked store DDL | header wording fixed + manifest hygiene (section 8 Q1); the file itself stays |
| code/02_services/02_compute/target/... (generated .class) | 1 | build artifact | ignore (regenerated) |

## 3. Canonical vocabulary

Use these names in all live text; the letters survive only where a decision enumerates its options.

- `Fluss KV-table dedup (fingerprint_dedup, DEC-038, retired)`
- `Flink keyed MapState + state-TTL dedup (DEC-040, retired)`
- `heap count-window dedup (FingerprintDedupFunction, uid fingerprint-dedup-v2, DEC-054)`
- `dual-sink signal build` -- what the 2026-08-17 material calls the "Design-B artifact (DB2)"

## 4. The anchor

One new subsection in `docs/08_implementation/04-signal-job.md`, immediately after the existing
supersession note near the top:

```markdown
### Retired designs and their names

The options once called **Design A** and **Design B** are both retired; the letters are option labels
from DEC-040 and must not be used as mechanism names.

| label | what it was | decided | retired by |
| --- | --- | --- | --- |
| Design A | Fluss KV-table dedup (`fingerprint_dedup`) | DEC-038 (2026-08-14) | DEC-040 / CHG-022 (2026-08-17) |
| Design B | Flink keyed `MapState` + native `StateTtlConfig` | DEC-040 / CHG-022 (2026-08-17) | `c0c50ee6` (2026-09-03), DEC-054, CHG-303 |

Current dedup: heap count-window (`FingerprintDedupFunction`, uid `fingerprint-dedup-v2`, DEC-054).
The words "Design-B artifact" / "DB2" in dated material mean today's **dual-sink signal build**.
```

Plus two one-line pointers: in `docs/01_project/04-decisions.md` (above the table) and in
`docs/06_operations/01-runbooks.md` at its first mention.

## 5. Edits, in order

1. **Manifest hygiene (section 8 Q1):** drop the `24_fingerprint_dedup.sql` entry from
   `code/01_platform/02_sql/ddl/schema_manifest.json`, and fix the DDL header so it stops instructing a
   removal that already happened (it also credits the retirement to Design B alone, while the heap
   window came later). Precondition: check whether a manifest/directory drift gate compares the
   directory listing against manifest entries -- if it does, take the documented fallback in 8 Q1.
2. **Anchor** (section 4) + the two pointers. No other file changes in this step.
3. **Live docs, mechanism sense:** in `04-signal-job.md`, `02-storage.md`, `01-foundation.md`,
   `00-start-here.md`, `02-schema-storage.md`, `03-ingestion.md` -- replace current-tense Design-A/B
   prose with the canonical name plus a link to the anchor; leave already-dated historical blocks
   alone (they are the audit trail inside the dossier).
4. **Live docs, artifact sense:** in `09-production-swarm.md`, `11-testing-and-release.md`, the
   runbook, and `SignalJob.java`/`SignalJobConfig.java`/`o2-provision.py` comments -- replace the label
   with "dual-sink signal build"; keep every operational instruction verbatim.
5. **Test rename -- WITHDRAWN 2026-09-23 on inspection.** The class name is accurate: its own javadoc
   and `@DisplayName` say "Design-B throughput + memory validation on the RocksDB state backend", and
   it measures exactly that (state count, checkpoint bytes, RocksDB local-dir footprint). Renaming it
   to "heap-window" would have made the name a lie in the other direction, since the current operator
   holds no managed state. It is env-gated (`COMPUTE_INT_TEST_DEDUP_ROCKSDB`), so it is already
   parked in practice. Open question for the operator: retire the drill, or rewrite it for the
   heap-window operator -- not a rename.
6. **Do-not-rename comments:** on the `fingerprint-dedup-v2` uid and on any load-bearing name that
   looks stale, stating it is a state-compatibility contract.
7. **Dashboards:** relabel panels only; verify no panel or alert still queries
   `compute_dedup_state_count` and note the result. No query change is needed -- the live series are
   `compute.dedup.first` / `compute.dedup.duplicates` (section 8 Q2).
8. **Guard** (section 6).

## 6. The guard (what makes it permanent)

New test: `code/01_platform/04_scripts/tests/test_design_vocabulary_guard.py`. It is picked up
automatically: `Makefile:214` runs `python3 -m pytest code/01_platform/04_scripts/tests -q ...`, and
`make gate-fast` includes that suite.

Behaviour:

- case-sensitive scan for `Design A`, `Design-A`, `Design B`, `Design-B`, `DesignB` over `docs/**/*.md`
  and `code/**/*.{java,py,json,sql}` excluding `target/`, `.git/`, `logs/`;
- a hit passes if the same line carries a retirement marker (`retired`, `superseded`, `historical`,
  `dual-sink`) or the file holds a documented exemption;
- exemptions: `docs/05_deployment/change-records/**` (append-only history),
  `docs/plans/**` (dated rationale), and an explicit per-line allowlist constant inside the test for
  anything genuinely unchangeable;
- asserts the anchor subsection from section 4 still exists, so it cannot be deleted silently;
- includes its own minimal self-check (fixture strings proving a bare token fails and a marked one
  passes) -- no framework beyond pytest.

## 7. Verification

- `make static-check` and `python3 -m pytest code/01_platform/04_scripts/tests -q` -- green, guard
  included.
- The renamed IT still compiles and is discovered: `mvn -pl code/02_services/02_compute -am test-compile`
  plus a surefire listing check.
- A re-run of the case-sensitive inventory showing only the exempted paths remain.
- No behaviour change is claimed: the pass touches comments, docs, labels, and one test class name;
  the compute suite must stay green.

## 8. Decisions (accepted 2026-09-23)

The four questions in the first draft are answered. Each answer is the Fluss-1.0-native one: the
platform that owns the concern now decides the outcome.

| # | question | decision | why (evidence) |
| --- | --- | --- | --- |
| 1 | fate of `24_fingerprint_dedup.sql` | **Remove the manifest entry** (`schema_manifest.json:272`); keep the file as the parked DDL record; fix its header so it stops instructing a removal that already happened. Fallback if a manifest/directory drift gate forbids an entry removal without code change: keep the entry and amend only the header, and say so in the commit message. | The dedup is Flink state (DEC-040, DEC-054) and reactivation is parked (`AC-FLS-017` stays `NOT_IMPLEMENTED`). The file's own header says a manifest listing "is stale and must be removed", while `CHG-022` recorded the listing as deliberate -- the two records contradict each other. A stale entry means a fresh corpus creates a table nothing uses. `docs_audit.py` only asserts that the file exists and that the observability doc and the runbook mention it, so it stays green either way. |
| 2 | dashboard on `compute_dedup_state_count` | **Config-only**: confirm no panel or alert still queries it, relabel the panels. No code change. | The live series already exist (`compute.dedup.first`, `compute.dedup.duplicates` at `FingerprintDedupFunction:107-108`) and the test corpus already uses them. `compute_dedup_state_count` is documented as wrong (a parity test calls it "a wrong leaf"; `o2-provision.py` records the repoint and a ~39x over-report). Optional separate item: expose the G-DEDUP-2 live-entries guard as a gauge. |
| 3 | `SignalJob.java:463` 128 MB block cache | **No change.** At most one clause noting the envelope it cites was the retired dedup. | It is a dated root-cause note about LOCAL execution (Flink's 128 MB managed-memory default thrashing the then-~628 MB envelope) whose remedy is a local-only passthrough, with the deployment authoritative. RocksDB-backed state is live today (`StrategyHostFunction` MapState; `ValueState`s in `MultiTimeframeSinks`, `TradeDecisionsSinks`, `PositionsObservationOperator`). Memory is not retuned without a measurement. |
| 4 | letters inside DEC-040's options text | **Keep.** No change to the register. | That is the one place where option labels are meaningful. DEC-038 and DEC-040 already carry their supersessions (DEC-040: "SUPERSEDED IN PART 2026-09-03 ... see DEC-054"), so the register is already correct -- the fix is to stop citing the letters outside decisions. |

Consequences for the steps above: section 5 step 1 is the manifest edit (8 Q1); the dashboards step is
config-only (8 Q2); the test rename in section 5 is unaffected. Nothing in this plan changes runtime
behaviour. Nothing in this plan changes runtime behaviour.

## 9. Commit split

1. docs vocabulary pass (sections 4 + steps 2/3/4) + this plan file. The Q1 manifest mechanics moved to
   commit 2: `ddl_apply.py` enumerates `*.sql` in the DDL dir, so the entry can only leave the manifest
   when the file leaves the apply set, and the compute agreement test reads the DDL by path.
2. Q1 mechanics + code renames (steps 1, 5, 6): relocate the DDL out of the apply dir, regenerate the
   manifest entry list, update the `docs_audit.py` C1 pin and path assert, point the agreement test at the
   new path, and rename the IT/env var. Compute suite green.
3. the guard (section 6), added last so it lands green.
