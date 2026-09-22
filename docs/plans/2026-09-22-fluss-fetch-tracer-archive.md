# Fluss 0.9.1 fetch-path tracer — archive record

**Created:** 2026-09-22 · **Status:** archived, retired by the 1.0 upgrade · **Owner:** Platform
**Related:** `2026-09-22-fluss-1.0-upgrade.md` (Stage 0), `../../logs/fluss-fetch-tracer-20260922/`

## Why this record exists

The Fluss checkout at `fluss/Fluss_v_0.9.1` carried uncommitted diagnostic instrumentation on branch
`diag/fetch-cycle-timing`, sitting exactly at tag `v0.9.1-incubating`. The 1.0.0 upgrade replaces the
fetch path wholesale — `LogFetcher` alone moved +345/−136 — so the patches cannot be carried forward
and the branch was archived and retired (upgrade plan decision D1).

This record is the tracked half of that archive. **The bytes are machine-local**: `.gitignore:37`
carries a bare `logs` rule, so `logs/` is deliberately an untracked evidence area. Anyone with a clone
can verify a copy of the artifacts against the SHA256 below.

## Provenance

```text
source_repo:   /home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/fluss/Fluss_v_0.9.1
branch:        diag/fetch-cycle-timing
head_commit:   6bf969f71
at_tag:        v0.9.1-incubating  (a690fd127 — HEAD == tag, zero committed drift)
files_patched: 7   (+251 / -2)
```

| File | Delta |
|---|---|
| `client/table/scanner/log/LogFetcher.java` | +90 |
| `client/table/scanner/log/LogScannerImpl.java` | +49 |
| `client/table/scanner/log/LogFetchBuffer.java` | +35 |
| `client/table/scanner/log/CompletedFetch.java` | +13 |
| `flink/source/reader/FlinkSourceSplitReader.java` | +33 |
| `flink/source/emitter/FlinkRecordEmitter.java` | +22 |
| `flink/source/reader/FlinkRecordsWithSplitIds.java` | +11 |

## Archived artifacts

`logs/fluss-fetch-tracer-20260922/` (machine-local, gitignored):

| File | Notes |
|---|---|
| `fetch-cycle-timing.patch` | 27,076 bytes · `git diff --binary` of the 7 files |
| `fetch-cycle-timing.diffstat` | the per-file delta table above |
| `FetchCycleTracer.java` | 74 lines — client-side fetch-cycle instrumentation |
| `EmitTracer.java` | 47 lines — Flink emit-side instrumentation |
| `fluss-context.md` | 5,715 bytes, dated 2026-07-09 — the checkout's own project-context file |
| `PROVENANCE.txt` | machine-readable provenance + patch SHA256 |

Patch integrity — `sha256` recorded in `PROVENANCE.txt`, and confirmed two ways on 2026-09-22:

- **forward:** applies cleanly to a pristine `v0.9.1-incubating` worktree and reproduces exactly
  `7 files changed, 251 insertions(+), 2 deletions(-)`
- **reverse:** `git apply --check --reverse` succeeds against the live tree, proving the archive is
  byte-identical to the working tree it was taken from

## What the instrumentation measured

Both classes are **pure logging, zero semantic effect**. Two logger names, one per side:

- **`FLUSS_FETCH_DIAG`** — `FetchCycleTracer.phase(fetchId, phase, scanner, tableBucket, nodeId,
  tsNanos, refNanos, records, bytes, extra)`. A monotonic `AtomicLong` fetch id correlates phases
  across threads; each phase logs the wall-clock ns, `sinceRef_ms` relative to a reference event,
  record count, byte count, and thread name.
- **`FLINK_EMIT_DIAG`** — `EmitTracer.log(phase, scanner, extra, markerMs)` on the Flink source task
  thread, so the response-arrival → queue → emit → next-fetch chain can be decomposed. A
  `DIAG_BATCH_ID` counter correlates a batch across the emit log.

The instrumentation's purpose, per its own javadoc, was the **tracker-14 read-path investigation**:
decomposing where time goes inside one fetch cycle, and separately on the emit side.

## Why it is retired rather than re-ported

1.0.0 addresses the same question with first-class configuration and a rewritten fetch path:

| What was hand-instrumented | 1.0.0 native equivalent |
|---|---|
| Local-vs-remote fetch choice, measured by patching the fetch cycle | `client.scanner.log.read-preference` — `local-first` (default) / `remote-first` |
| Remote download concurrency, inferred | `client.remote-file.download-thread-num` |
| Temp-file behaviour during remote fetch | `client.scanner.io.tmpdir` |
| Phase correlation inside the fetch lifecycle | 8 new classes in `client/table/scanner/log/`, incl. `AbstractLogFetchCollector`, `ArrowLogFetchCollector` |

Re-porting would mean rebuilding the instrumentation against an API that no longer has the same shape,
to answer a question a config knob now answers directly.

## How to restore it

On any pristine `v0.9.1-incubating` tree:

```bash
git worktree add --detach /tmp/fluss-0.9.1-restore v0.9.1-incubating
cd /tmp/fluss-0.9.1-restore
git apply logs/fluss-fetch-tracer-20260922/fetch-cycle-timing.patch
# the two tracers are already inside the patch; enable their loggers at INFO:
#   logger.FLUSS_FETCH_DIAG.name = FLUSS_FETCH_DIAG
#   logger.FLUSS_FETCH_DIAG.level = INFO
```

The patch includes the `diag/` packages, so no separate file copying is needed.

## What this record does NOT contain

**The findings.** The artifact is instrumentation, not a report — the observations it produced were
made on a live run and are not recoverable from the code. If those conclusions exist, they belong in a
`logs/` evidence record with the run conditions; if they were never written down, the code is the only
survivor and the question it was asked is now answered by `client.scanner.log.read-preference`
instead. Stated plainly rather than reconstructed after the fact.
