# Wave 40 — Instrument import and corpus pin (20 findings) — DETAILED plan

- Date: 2026-09-19
- Files: `code/01_platform/04_scripts/import_instruments.sh` (17 findings),
  `code/01_platform/04_scripts/corpus-pin.sh` (3 findings)
- Findings: P6-010, P6-110, P6-111, P6-112, P6-113, P6-114, P6-115, P6-417, P6-418,
  P6-419, P6-420, P6-421, P6-422, P6-423, P6-424, P6-746, P6-747 (import) ·
  P6-060, P6-061, P6-349 (corpus pin)
- Status: verification complete (read-only, 2026-09-19); implementation staged while the
  certifying gate for `f2faf565` runs, applied after its verdict.

## 0. Verdict summary

Every finding was checked against the code on 2026-09-19; all line references were accurate.
20/20 are real defects; two have a wrong stated *mechanism* but a real defect underneath.

| finding | severity | file:line | verdict |
|---|---|---|---|
| P6-010 | CRITICAL/security | import:105 | real — unescaped `'$SYMBOL'`/`'$EXCHANGE'`/`'$INSTYPE'` in the INSERT |
| P6-110 | HIGH/bug | import:42 | real — `mktemp` with no trap; neither-client branch falls through to `Done.` + exit 0 |
| P6-111 | HIGH/bug | import:67 | real — `IFS=',' read -ra` cannot see RFC-4180 quoting, CRLF or a BOM |
| P6-112 | HIGH/security | import:88–89 | real — TOKEN/STRIKE/EXPIRY/LOT/TICK/ACTIVE concatenated raw into SQL |
| P6-113 | HIGH/security | import:93 | real — same quoting flaw for OPTYPE/SEGMENT |
| P6-114 | HIGH/bug | import:130 | **partial** — the leak and the missing error context are real; the claimed fall-through masking is not (`set -e` aborts at the client call) |
| P6-115 | HIGH/bug | import:133–134 | real — neither-client branch deletes the only artifact and reports success |
| P6-417 | MEDIUM/bug | import:42–43 | real, platform-conditional — measured: GNU mktemp 9.4 accepts a non-trailing `XXXXXX.sql`; the failure is BSD/macOS-only |
| P6-418 | MEDIUM/bug | import:43 | real, platform-conditional — measured: `date +%s%3N` works here (GNU); BSD prints a literal `%3N` |
| P6-419 | MEDIUM/bug | import:54 | real — a final line without a trailing newline is dropped |
| P6-420 | MEDIUM/bug | import:57–58 | real — `[[ -z "$line" ]]` misses `\r`/whitespace; `\r` reaches the fields |
| P6-421 | MEDIUM/bug | import:60–61 | real — header detection depends on position 1 and an exact lowercase prefix |
| P6-422 | MEDIUM/bug | import:81–82 | real — no arity check, no whitespace trim |
| P6-423 | MEDIUM/performance | import:104 | **partial** — "re-runs fail on PK conflicts" is wrong (KV table, per-row upsert by composite PK); the column-order coupling is real and already broken |
| P6-424 | MEDIUM/bug | import:140–141 | real — unconditional `rm -f` + `Done.` |
| P6-746 | LOW/maintainability | import:46–47 | real — catalog/database hardcoded, no override |
| P6-747 | LOW/maintainability | import:118 | real (cosmetic) — `if $DRY_RUN` executes the value; `DRY_RUN` can only be `true`/`false` today |
| P6-060 | HIGH/bug | corpus-pin:35 | real — `sha256sum -c` only checks listed paths, so a new unpinned corpus file passes |
| P6-061 | HIGH/bug | corpus-pin:38–40 | real — `>"$MANIFEST"` truncates the committed pin file before the pipeline can fail; glob skips dotfiles, dies on subdirs, ignores ordering |
| P6-349 | MEDIUM/bug | corpus-pin:28 | real — `case "${1:-}"` accepts `--verify anything` |

### Headline discovery (not in the finding list)

`import_instruments.sh` cannot run against the live table, and it is superseded:

- Its positional `INSERT INTO instruments VALUES (...)` supplies **12 values**; the live
  `20_instruments.sql` table has **14 columns in a different order**, with `manifest_version`
  and `schema_version` `NOT NULL`. Its documented prerequisite (`11_instruments.sql`) no
  longer exists — the file is now `20_instruments.sql`.
- The documented operator loader for this table is `InstrumentManifestWriter`
  (`docs/08_implementation/03-ingestion.md`, "Writes" row; ING-INT-004 live proof 2026-08-15),
  reached through the ingestion service with `INSTRUMENT_MANIFEST_PATH`; the manifest version
  is a loader parameter (default 1).
- DDL `20_instruments.sql` pins the loader contract to that writer —
  "enforced in `InstrumentManifestWriter.ManifestEntry` compact-ctor + unit tests, never in
  DDL": lot_size > 0; tick_size_paise NULL-or->0; option_type CE|PE iff instrument_type=OPT;
  expiry NULL iff EQUITY; single `is_active=TRUE` per instrument_token (deactivate-prior load
  sequencing); monotonic `manifest_version`; duplicate-composite-key rejection; KV-shape
  preflight. Bash cannot enforce the sequencing rules, so a repaired script would be a second
  loader with weaker guarantees than the documented one.
- Nothing calls the script (`code/01_platform/04_scripts/import_instruments.sh:104` is the only
  INSERT into `instruments` anywhere), but `docs/commands/COMMANDS.md:124` and
  `code/01_platform/05_instruments/README.md:34` document it as the operator import command —
  so it is a documented trap, not merely dead code.

**Decision (user, 2026-09-19): retire in place** — the body becomes a fail-closed refusal that
names the real loader. All 17 findings close as superseded, with this evidence cited.

## 1. Slice 1 — `import_instruments.sh`: retire in place (17 findings, CHG-229)

- Replace the body with a refusal: exit 2 (the repo's "you cannot use this" code), message
  naming `InstrumentManifestWriter`, `INSTRUMENT_MANIFEST_PATH`, the doc, and CHG-229.
- Update the two references: `docs/commands/COMMANDS.md:124` (row → retired, points at the
  writer) and `code/01_platform/05_instruments/README.md:34` (import script → loader).
- The header comment keeps the *why* (pre-v3 12-column INSERT; the contract that lives in the
  writer) so the retirement is auditable without reading this plan.
- Deliberately NOT done: fixing the 17 defects. Repairing them would produce a second loader
  for the same table that cannot enforce the documented contract.

## 2. Slice 2 — `corpus-pin.sh`: the pin must see the whole corpus (3 findings, CHG-228)

- **P6-349** — require exactly one argument (`[ "$#" -ne 1 ]` → usage, exit 2).
- **P6-060** — `--verify` adds `--strict`, then compares the pinned path set against the files
  actually on disk (`find "$REL_CORPUS" -type f`, `LC_ALL=C sort`) and fails on divergence, so a
  new unpinned corpus file can no longer pass. Measured 2026-09-19: the manifest is currently in
  sync (6 pinned = 6 on disk) and strict-clean (every line is 64 hex + two spaces), so the
  strengthened check passes today.
- **P6-061** — `--regenerate` hashes `find … -print0 | LC_ALL=C sort -z | xargs -0 sha256sum --`
  into a temp file next to the manifest and renames it atomically, instead of truncating the
  committed pin file before the pipeline runs. Deterministic order; dotfiles, spaces and
  subdirectories are handled (the old glob skipped dotfiles and died on directories).
- Gate impact: **yes** — `pin-check.sh:28` calls `--verify` and `run-monday-gates.sh:1003` runs
  `make pin-check` as a gate step. Run `make pin-check` locally before any certifying gate.

## 3. Verification plan

- Wave test `code/01_platform/04_scripts/tests/test_import_corpus_wave40.py` (offline, hermetic,
  no cluster):
  - the retired script refuses (exit 2), says nothing was imported, and names the real loader;
  - `COMMANDS.md` / the instruments README no longer present it as a working import;
  - `corpus-pin.sh --verify extra-arg` exits 2;
  - `--verify` fails when a temp corpus gains an unpinned file and passes when re-pinned;
  - `--regenerate` is atomic: a failure leaves the previous manifest intact;
  - the regenerated manifest is byte-identical to the committed one (deterministic order).
- `bash -n` on both scripts; `make pin-check`; `make docs-audit`; full `tests/` suite.
- No image-hashed source is touched (neither script is in any `SERVICE_SOURCES` list), so no
  image rebuild is required.

## 4. Out of scope / follow-ups

- P6-423's batching suggestion and P6-746's parameterization die with the script.
- The 17 findings are closed by retirement, not by repair — recorded per finding in the audit map
  tick notes.
- The instruments table keeps exactly one writer: `InstrumentManifestWriter`.
