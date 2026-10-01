# Foundation

> **2026-10-02 current test truth:** unit suites green 770/562/675 (common 753->754 on 2026-09-29: +1 M5-1 — `SignalCurrentDdlContractTest.bothDdlsPinRowSchemaVersionTwo` pins both signal DDL headers and writer-set lines at 2 (the writer constant's value) so the table-kind history can never masquerade as the row contract again, CHG-411; compute 587->590 on 2026-09-29: +3 M3 — 2 `MultiTimeframeAggregateFunctionTest` (`configuredLatenessControlsEmittedMapEviction` proves a lateness=0 aggregator evicts an emitted window as soon as the watermark passes its end — pre-fix that site hardcoded 5 s, so the configured tolerance was ignored; `processingTimeTimerNeverClosesWindow` proves a processing-time fire aligned with a 15s window end closes nothing, and the watermark then closes it exactly once — pre-fix the boundary loop was ungated) + 1 `SignalJobConfigTest.hostRequiresMultiTf` (host-on/multi-TF-off is refused naming both flags; the coherent pair parses), CHG-410; compute 590->593 on 2026-09-29: +3 L3-3 — `SourceIdleWatchdogGeneratorTest.secondGeneratorOnTheSameSubtaskSharesTheEpisodeLatch` and `.secondGeneratorOnTheSameSubtaskDoesNotResetTheJobClock` prove a second generator on one subtask inherits the shared `SourceIdleWatchdogState` (episode latch + job clock) instead of resetting them at construction, and `.theStateSupplierIsSerializableAndDeserializesFresh` pins the serializable-supplier / transient-state contract the watermark strategy ships, CHG-422; compute 593->595 on 2026-09-29: +2 L3-4 — `StrategyHostFeaturesTest.snapshotFallbackFeedsTickFeaturesOncePerSnapshot` proves the six-row snapshot fallback updates the timeframe-independent tick features once (the FIFTEEN_S forming value wins) while every row still fans out to strategies, and `.invalidLiveTimeframeIsCountedAndDoesNotBlockDelivery` pins the malformed-TF path, CHG-423; compute 595->601 on 2026-09-29: +6 — +3 CHG-441 (changelog rollout-flag legs in `RuntimeOptionsTest`) and +3 CHG-443 (the class grew 11->17: the CLAIM-on-restore pin, the fresh-start/flag-off claim-mode absence, and the storage-key absence at job level; the CHG-442 drill fix reworked `B4SignalIntentE2ETest` methods without changing its count); compute 601->621 on 2026-09-30: +20 C1 — the live-candle context fetch skeleton (`docs/plans/2026-09-30-strategy-context-live-fetch.md`): `ContextProviderTest` 12 (cache hit, single-flight, distinct keys, timeout + cooldown, late-arrival drop, failed fetch, absent row, in-flight cap, byte-budget LRU eviction, concurrent completions, view delegation, close), `FlussCandleFetcherTest` 3 (exact-PK key row + decode, empty-is-absent, mismatched row rejected), `StrategyHostConfigTest` +4 (flag off defaults, requires-host, knob parse, in-flight sanity cap), `StrategyHostFunctionTest` +1 (provider open/close lifecycle behind the flag), CHG-445; compute 621->627 on 2026-09-30: +6 C2 — the context-aware contract and host wake-up wiring (`docs/plans/2026-09-30-strategy-context-live-fetch.md`): `StrategyHostFunctionTest` +4 (a context miss arms a wake-up and the ready callback fires on the retained live snapshot, never on the closed candle; a fetch past the 100 ms timeout clears the snapshot with no callback; a request made inside `onContextReady` stays pending and resolves on its own wake-up; flag on with a non-requesting strategy is inert — no snapshot, no fetch), `ContextProviderTest` +2 (`drainArrivals` returns exactly the keys promoted in that pass; `hasPendingForToken` reports only tokens with an in-flight fetch), CHG-446; compute 627->629 on 2026-09-30: +2 C3 — the first on-demand context consumer (`docs/plans/2026-09-30-strategy-context-live-fetch.md`): `ContextProbeStrategyTest` (`readyFiresOnTheFormingLiveCandle` — on a FIFTEEN_S live row (the host's fast-feed TF) a miss emits nothing, the completed fetch re-evaluates the same live window through the host wake-up and emits one deterministic `Signal_Candidates` row stamped with the live candle before its close, the repeat evaluation of the same window is deduped by the host, and the close adds nothing; `readyAtTheCloseNeverEmits` — a ready value on a snapshot at/past its window close never fires and is counted), plus the opt-in probe rule `ctx-probe-v1` registered in `Strategies` (inert unless a `STRATEGIES` list names it), CHG-447; compute 629->637 on 2026-09-30: +8 C4 — compact context derivation (`docs/plans/2026-09-30-strategy-context-live-fetch.md`): `ContextProviderTest` +7 (`slice` serves cached windows and schedules exactly the missing ones; `slice`/`warmUp` refuse unbounded counts; `warmUp` schedules without reading, is counted once per call, and single flight (not the warm-up) prevents re-fetches; scalars retain, read back, and evict the least recently used at the 4096-entry cap; a scalar read refreshes the LRU order; the scalar and slice caps stay inside the decision-#4 8 MB budget (4096 x ~96 B < 8 MB, MAX_SLICE 64); the disabled view schedules nothing and has no scalars), `ContextProbeStrategyTest` +1 (first ready retains its derived `prevClosePaise` scalar exactly once and warms up the two windows behind the fetched one), CHG-448; common 754->756 on 2026-09-29: +2 L5-2 — `SafetyHaltRequestParserTest.longBoundIsExclusiveTwoToTheSixtyThree` rejects the whole exclusive-bound set ((double)/(float) Long.MAX_VALUE, 0x1p63, Double.MAX_VALUE, NaN, ±Infinity, below -2^63) and `.largestValueBelowTheBoundIsAcceptedExactly` pins Math.nextDown(0x1p63) = 9223372036854774784L exactly, CHG-427; common 756->761 on 2026-09-29: +5 L5-3 — three `PositionSnapshotTest` legs pin the canonical average encoding (0 iff the matching quantity is 0, both directions), `PositionsColumnsAgreementTest.ddlPinsTheCanonicalAveragePriceEncoding` pins the DDL sentence, and `FlussPositionsStateStoreDecodeTest.nullAvgEntryWithALiveQuantityFailsClosed` pins the corrupt-NULL fail-closed path, CHG-428; common 761->766 on 2026-09-29: +5 L6-2 — two `DdlTextFailFastTest` legs pin the comment-aware parser (a commented option is not an option; a quoted `--` and a comment's `PRIMARY KEY` never leak into SQL), `CompositeKvDdlInvariantTest` pins subset bucket key ⇒ `table.kv.format-version=2` over the real DDL corpus plus bucket.key ⊆ PK, and `DdlApplyToolStatusTest.compositeSubsetWithoutFormatVersionIsPredictedLimited` pins the tightened prediction, CHG-431; common 766->768 on 2026-09-29: +2 gate step 9 absorption — `DdlSmokeTwinSweepUnitTest.everyDdlColumnTypeHasASmokeDefaultMatchingTheSignature` walks every committed DDL and proves an all-default row is written by `defaultValue` for every column type and reproduces the fixture signature (the guard for `feature_values`' `MAP<INT, DOUBLE>`, which the LIVE drill caught as `no smoke default for MAP` — the unit fixture only had the six scalar types), and `.mapSmokeDefaultMatchesTheFixtureSignature` pins the one-entry MAP default and rejects a different map; `DdlApplyTool.defaultValue` gained the recursive MAP arm and `isSmokeFixtureRow` a structural MAP comparison (GenericMap.equals rejects a live ColumnarMap), CHG-435; compute 586->587 on 2026-09-29: +1 H5-1 — `legacyEmittedIdStillSuppresses` seeds a pre-upgrade ledger id and proves it still suppresses; the Churner fixture now emits the real `candidateIdFor` UUID shape and pins the prune-scan count, CHG-402; ingestion 557->559 on 2026-09-29: +2 M4-6 — −6 `ValidatePayloadHashFlagTest` deleted with the flag, +6 `IngestionPayloadHashGateTest` (a verified tick appends; a tampered 32-byte digest quarantines `HASH_MISMATCH` + metric and never appends; an empty hash quarantines `MISSING_PAYLOAD_HASH`; a 3-byte digest and a 541 B packet quarantine `INVALID_SCHEMA`; no env key, `ConfigKeys` constant, `IngestionConfig` field/method or diagnostic key can re-open the deleted switch), +2 `GoldenCorpusPayloadHashTest` proto-path cases (golden frames validate against the 32-byte digest; a tampered frame is `HASH_MISMATCH` on the byte[] path), CHG-387; ingestion 556->557 on 2026-09-29: +1 M4-5 — `RawTickWriterStartupGraceTest` proves an early ack does not close the metadata-retry window (first append acks, the next append fails metadata three times and retries to SUCCESS; pre-fix FATAL), CHG-386; ingestion 552->556 on 2026-09-29: +4 M4-2 — 3 `ControlContractVersionTest` (a v2 control record is processed and reaches slot health; v3/missing-0/other versions are quarantined `INVALID_SCHEMA` + `CONTROL_VERSION_MISMATCH` with zero health mutation; a wrong-version `bridge_metrics` is never applied) + 1 `FailureMatrixTest` T7-F16 (a v3 control frame is quarantined at decode through `ProtoFrameReader`, never processed as v2), CHG-385; ingestion 550->552 on 2026-09-29: +2 M4-1 — `ManifestLoadTest` proves a parsed 3-row manifest is refused against the 1024 minimum with the effective numbers, and the boundary is inclusive (exactly 1024 passes, 1023 refuses, a failed/empty load always refuses), CHG-384; ingestion 546->550 on 2026-09-29: +4 M4-4 — `AsyncQuarantineSinkTest` proves quarantine writes never block behind a wedged delegate (the inline path took 10 s), overflow is counted and routed as `QUARANTINE_OVERFLOW`, `close()` drains every queued record and counts late writes, and the drain budget bounds what is abandoned (queued + in-flight counted, delegate release still runs), CHG-383; ingestion 542->546 on 2026-09-29: +4 M4-3 — 2 `BoundedQueueTest` (RESUMED fires once when a WARNING episode clears and re-arms; RESUMED closes a CRITICAL halt episode after the drain) + 1 `ReadinessGatingMatrixTest` (a queue backpressure episode refuses READY and RESUMED restores it) + 1 `QueueBackpressureReadinessTest` (every queue carries the service's readiness listener, both ways), CHG-382; ingestion 534->542 on 2026-09-29: +8 H2-4 — 3 `TokenSetHashTest` (the Go-generated fixture carve including the real 2433-instrument/3-socket datastream at 1024+1024+385, the MATCH/MISMATCH/UNKNOWN_SLOT/EMPTY_SLICE verdict table, order-insensitive carve) + 5 `SlotTokenHashCrossCheckTest` (a correct multi-socket ack no longer raises TOKEN_HASH_MISMATCH — the pre-fix false positive on every multi-slot plan; the real datastream's tail socket passes; a wrong slice digest raises TOKEN_HASH_MISMATCH, an unknown socket TOKEN_HASH_UNKNOWN_SLOT, a configured-but-empty socket TOKEN_HASH_EMPTY_SLICE), CHG-381; ingestion 533->534 on 2026-09-29: +1 H2-3 honest bridge exits — BridgeRestartDecisionTest pins unrequested exit 0 as unexpected (RESTART then TERMINAL), the Go terminal exit code 3, and that an explicit teardown never restarts; the crash-loop E2E asserts the BRIDGE_CRASH-named fatal line, CHG-380; ingestion 530->533 on 2026-09-29: +3 H2-2 drop accounting — the WriterWorker drop sink reports the SKIPPED drop, and the service's shared handler counts the loss, pins exactly one drop:REJECTED journal entry, stops off-thread and treats a shutdown SKIPPED as counted-only, CHG-379; ingestion 529->530 on 2026-09-29: +1 `IngestionServiceTest.badTimeTicksInsideFreshnessGraceAreTerminal` — a STALE and a FUTURE tick inside an armed freshness grace are quarantined and append nothing (H2-1, CHG-378); common 749->753 on 2026-09-29: +4 H1-5 fill-identity tests — two same-millisecond distinct fills get distinct versions, a replay is identical, the formula and its bounds are pinned, and a same-ms pair both applies with the replay as DUPLICATE, CHG-373; common 748->749 on 2026-09-28: +1 `DdlTextFailFastTest.mapTypeParses` — the shared DDL text parser accepts composite `MAP<INT, DOUBLE>` parameters and maps them to `DataTypes.MAP` (one nesting level; a nested generic fails loudly), the type DDL 34 `feature_values` uses, CHG-358; ingestion 527->529 on 2026-09-28: +1 `IngestionConfigTest.flussStartupWaitDefaultAndDisable` (`FLUSS_STARTUP_WAIT_MS` default 3600s, 0 disables) with the write-grace default pin moved to 3600s and the zero-ack rule pinned by the renamed `earlyAckDoesNotShrinkTheGrace`, CHG-356, +1 `DdlBootstrapSchemaAgreementTest.featureValuesRegistryMatchesDdl` (the registry carries the compute-owned `feature_values` entry with the DDL 34 schema and the bootstrap never owns it, A4.4), CHG-358; common 743->748 on 2026-09-27: +5 `GateStoreTransitionTest` — the store's sanctioned forward step `HALTED->RECONCILING->APPROVAL_PENDING` (legal targets only via `GateState.legalTargets()`, epoch +1 per step, CAS witness honored, fence columns preserved, missing row null), the primitive the H2-5 gate-report path needs to reach `APPROVAL_PENDING`, CHG-334; compute 541->586 on 2026-09-27: +45 feature-layer tests -- the shared FeatureRegistry and its append-only pin (CHG-347), the strategy-host feature computation with the `FeatureView` overloads (CHG-348), the `FEATURE_ROWS` side output and contract (CHG-349), and the feature-values sink with its flags and preflight (CHG-350); the suite was green at each landing step (572/0 after CHG-348, 578/0 after CHG-349, 586/0/18-skip after CHG-350); compute 540->541 on 2026-09-27: +1 `ExecutionIntentBuilderTest.instructionIdMatchesCanonicalGrammar` pins the canonical `ei-v1-` + 64 lowercase sha256 hex id shape the gateway validator accepts, with the cross-service pin `test_instruction_id_contract.py`, CHG-333; ingestion 516->527 on 2026-09-26: +11 CHG-326 — 5 `RawTickWriterStartupGraceTest` (metadata-not-ready inside the bounded cold-start write grace retries and can still succeed; grace expiry restores the R-285 fail-closed FATAL; every other fatal class and the no-grace legacy path stay FATAL; the grace clock starts with the first append), 2 `RetryClassifierTest` (`isMetadataNotReady` walks the chain and stays narrow: stale-handle/transient classes are not it), 3 `ZeroAckWatchdogColdStartTest` (`zeroAckSuppressed`: the grace stands the watchdog down only while no append was acked; post-success and post-expiry authority is restored), 1 `IngestionConfigTest` (`INGESTION_WRITE_STARTUP_GRACE_MS` default 60s, 0 disables); ingestion 508->516 on 2026-09-26: +8 CHG-323 — 8 `SubscriptionPreflightTest` (capacity boundary 1024/1025; 2 433 tokens on one socket fails naming the numbers; 2 433 and 3 072 on three approved dev sockets pass; three sockets without approval fail; three sockets in production fail with single-socket precedence over capacity; 3 073 over three-socket capacity fails); ingestion 495->508 on 2026-09-26: +13 CHG-322 — 9 `FlussStartupReadinessTest` (bounded retryable-only Fluss startup wait: capped backoff, fatal fail-fast, R-285 fail-closed, table-missing proceeds, budget exhaustion, zero budget, action mapping) + 4 `RetryClassifierTest` (`isTableMissing` class/message/chain/narrow); compute 539->540 on 2026-09-26: +1 `SignalJobConfigTest.unalignedCheckpointsDefaultTrueWithKillSwitch` pins the landed unaligned checkpoints (default true, `UNALIGNED_CHECKPOINTS=false` kill switch, junk fails closed), CHG-318; compute 538->539 on 2026-09-26: +1 `MultiTimeframeAggregateFunctionTest.fastLiveTickFeedEmitsPerTrade` pins the per-tick `LIVE_TICK_TAG` fast feed to the strategy host (one forming row per accepted trade with fresh OHLC, none for a quote-only tick), CHG-316; compute 532->538 on 2026-09-26: +6 `SourceFetchLatencyTuningTest` pins the native raw-source fetch/flush tuning defaults, the env overrides, the fail-closed pair and the Fluss/Flink configuration translation, CHG-315; common 742->743 on 2026-09-25: the drill fixture hygiene guard `DrillFixtureHygieneTest` counts as one plain-suite skip — it is wired into `make drill-live` after the drills and self-skips without `FLUSS_BOOTSTRAP`, CHG-313; ingestion 489->495 on 2026-09-25: +6 B5 server-rejection tests — 4 `RetryClassifierTest` (out-of-date -> `REJECTED`; rejection beats a retryable wrapper; generic InvalidPartition stays FATAL; a fatal cause still wins), 1 `FailFastGuardsTest` (drop counted, no retry, writer stays up), 1 `IngestionNoSilentDropTest` (service counts the drop, no halt) — CHG-309; common 739->742 on 2026-09-24: `4ad89268` added the three `DdlSmokeTwinSweepUnitTest` serving-gate tests when the DDL smoke's write moved onto bucket-leader readiness, and this line's re-measure was not rerun for them; re-measured 2026-09-24 on the live-class rule: ingestion 475->489 and compute 530->532 reconcile exactly with the +14 and +2 `@Test` added since 2026-09-21; common 739 is the plain-suite report total - a clean plain run leaves 739 XMLs with no gated class present, and `code/common/src/test` declares 740 `@Test` annotations, so the 759 recorded earlier the same day had to be counting `*IntegrationTest` reports a targeted IT run had left in `target/surefire-reports/` (C6 sums live report files verbatim, so a non-plain invocation lifts this count without a plain run having grown); common 735→739 on 2026-09-21 — the third-column parser case: `R2LakeTieringEodOffloadExecutorTest` pinned the `key<TAB>size<TAB>LastModified` format `r2-list.sh` actually emits, CHG-290; then 736→739 with the manifest stamp rule — a manifest counts only when its LastModified is newer than the newest data object — plus three fail-closed negatives, CHG-291) (common/ingestion/compute; common 722→729 on 2026-09-15 with the wave-32 clean-break guards, CHG-172; common 729→733 on 2026-09-18 — WriteAwaitTest pins the DDL smoke's never-drop-an-unresolved-write guard, CHG-212; common 733→735 on 2026-09-18 — `FlussHandlePoolTest` +2: `close()` drains retained handles and refuses new borrows, and a handle returned after close is dropped rather than pooled, CHG-223) — common + ingestion re-run 2026-09-14 (2/8 skipped by env gates); compute last full `mvn -o test` 2026-09-13, not re-run in this pass (18 skipped by env gates) — compute 538→530 the same day (−7 for the retired test class, −1 for the routing equality guard): the candle-era residue cleanup removed the retired `CanonicalCandlePolicy` test class and the candle-vs-signal routing equality guard; C6 counts live-class surefire XML totals for classes the plain suite selects, so an `*IT` held back for a targeted run can no longer inflate them (a 2026-09-05 report for `DedupRocksDbThroughputMemoryIT` had compute reading 539 against a 538-test run). The 2026-09-01 baseline was 489/285/432; the delta is everything landed since — **common +138**: flush discipline/attempts/EOD (`87ee362`), gate fencing (`c12bb2c`), bounded quarantine read (`b87c24c`), DDL + column-ownership guards (`626986d`, `1d01838`), shared retry guard (`8dcb2cb`); **ingestion +184**: bounded-close shutdown (`3ecba3a`), native OTel observability (`ed65fa8`), write-path backpressure/retry/health (`5fd1cd4`), fail-closed config + DDL (`d09e979`), fail-fast stale-handle guards (`19ad461`); **compute +106**: multi-timeframe phases 0–3 (`0263cc6`…`63ad943`), N7 strategy host (`a9ecd7e`, `1e782dc`), chain-heap redesign (`82d3fe2`), Part A–F runtime hardening. Prior — 2026-09-01 baseline 489/285/432: 2026-09-02 CHG-121 async-marker rework added two compute tests (EarlySignalFunctionTest 13→15: marker-failure suppresses the tentative; marker-durable-before-LOG-row ordering gate) and the transient-markerHook fix +1, the lookup-retry fix +1, and the settle-clears-marker fix +1 (13→18: markerHook job-graph serialization regression — the field was transient, arrived null on every TaskManager, silently disabled markers in all cluster runs); 2026-09-01 CHG-121 marker tests +3 and CHG-120 hardening preceded (see below) — 2026-09-01 C2 hardening (CHG-120) added two common DDL partition-preservation guard tests (`DdlTextPartitionTest`) and, later the same day, five compute wall-clock idle-marking guard tests (`SourceIdleWatchdogGeneratorTest` 7→12 — the CHG-120 zero-emission fix: the watchdog marks empty-partition splits idle on the wall clock when `withIdleness`'s backpressure-paused clock is frozen); 2026-08-31 lake-archive migration (CHG-117): common +6 (`EventDayTest` 5, `R2LakeTieringEodOffloadExecutorTest` added), ingestion +5 (`RawTableSchemaParityTest` v3 21-col parity, `SchemaAgreementTest.rawTableV3ColumnCount` replaces the v2 pin, `FuzzIngestionTest` async-drain guard `awaitPipelineDrain`), compute unchanged (migration touched no compute code); prior 2026-08-29 baseline 481/280/415 — configuration-driven plan (2026-08-29) added SecretGuardTest (5), RawTableDdlContractTest (4), ConfigGuardTest (4), ContainerMemoryGuardTest +2, SignalJobConfigTest +6 (writer retries, preview table, prod-pin/dev-tunable legs), batch_env_test + Go side; common 466→481, ingestion 277→280, compute 410→415. Prior:  — 386→385 corrected 2026-08-28 (CHG-116 re-verify: surefire XML total 385, the declared 386 was an off-by-one from the 2026-08-26 stale-code audit; CHG-116 itself changed no test count — pure value edits) — CHG-102 doc-repair re-verified 2026-08-25: common surefire 469 raw minus 2 gated `FlussBundleReader*` reports (classes removed from src/test — C6 counts only live classes; 469−2=467 vs stated 466 — see `docs_audit` C6 for the live-class definition); compute plain-suite skip count 22 env-gated classes; CHG-100 DdlSmokeTwinSweepTest +4 (2 unit run everywhere, 2 env-gated live skipped in plain runs; common 464→466); plain-suite totals re-verified 2026-08-25 (mvn test per module). **2026-08-26 (stale-code audit): compute 388→386 — deleted `ActiveSignalFilterFunctionTest` (4 tests, superseded by `ActiveSignalFeedbackFunctionTest`) and added 2 ported assertions (`indefiniteBlockSurvivesProcessingTimeAdvance`, `remainsBlockedAcrossManyWindows`) to the feedback test. 2026-08-28 (T1–T9 low-latency ingestion): ingestion 247→286 — proto transport, batch queue, sequence-gap, failure-matrix, staged-latency, and soak-contract tests (CHG-113 scope, this dossier's truth line). 2026-08-28 (guards A1/A2/A4 + B1-B4): ingestion 286→288 — `freshnessGraceWindow` (A1), `readinessWriteThrottled` (A4) landed; A2 assertion added inside `fullMappingTable` (no new method count). 2026-08-29 (NDJSON transport removed — full proto): ingestion 288→277 — `BridgeEventParserTest` deleted (5), `IngestionServiceTest` GoTick parse tests deleted (5), `ProtoTransportTest.pipeParity` deleted (1), `IngestionQualityEvidenceTest` NDJSON helper tests deleted (3), fuzz/reject/no-silent-drop migrated to proto (+9 net within existing classes); proto path covered by `TickEventContractTest`/`ProtoTransportTest`/migrated fuzz corpus.** 2026-08-29 (LATEST-offset + collector feed fix): compute 385→391 — `RawSourceOffsetSelectionTest` +3 (full/latest/restore offset selection), `CandleHotPathFixTest` +3 (G2/G3 guards + equivalence proof); `SignalJobStrictGateT7Test` 2 F005-expectation tests updated to LATEST semantics (no count change).** 2026-08-29 (low-latency candles Phases 1–3): compute 391→410 — `CandlePreviewEmitFunctionTest` +2, `CandlePreviewTriggerTest` +2 (1s preview emission + schema), `SignalJobConfigTest` +2 (preview/early-signal knobs), `EarlySignalFunctionTest` +10 (tentative/confirm/cancel supersession, checkpoint-restore survival, Phase 3 confirm-window early confirm ×3), Phase 1 config-knob legs inside existing classes (preview TTL/interval, early-signal enable/rule, confirm-after-ms). 2026-09-13 (P3 broker-protocol types, batch D plus the FILL and requestTime guards): common 627→638 — the Arrow* protocol-type guard and contract tests. 2026-09-13 (positions & fills batch: P3-389 identity guards in FillEvent, P3-500 stale-reason): common 638→644 — the identity rejection pins (+4), the stale-reason pin (+1) and the stale-reason sibling pin (+1). 2026-09-14 (config-surface pass, CHG-138): common 644→664 — the two declaration guards fixed earlier the same day (ConfigGuardTest 4→13, ContainerMemoryGuardTest 8→17) plus the contracts this pass pinned (FixedScopeTest +4 DEC-045 gate/cap/peak, SecretGuardTest +1 null-env fails closed, PlatformConfigTest −3: its R-199 `maxPendingAppendBytes` helper had no production caller and the live byte ceiling is `IngestionConfig`'s validated flat default); ingestion 469 unchanged. 2026-09-14 (observability/ownership pass, CHG-139): common 664→680 — the redaction, JSON, log-event and correlation guards (+10), the ownership fail-closed rules (+4) and the alert-number pins (+2); ingestion 469→470 — the JVM gate default now reads the alert constant instead of a second 85. 2026-09-14 (audit-checker pass, CHG-141): common 680→686 — `EvidenceRecord` became a validating record, so an incomplete or aliased audit record cannot be constructed (`EvidenceRecordTest` +6); ingestion 470 unchanged. 2026-09-15 (wave-32 drill-evidence and clean-break pass, CHG-172): common 722→729 — seven guards on the clean-break convergence verdict (`CleanBreakSimulationTest` 4→11: the LWW winner is decided by (offset, sourceVersion) not list order, a table name containing the key separator cannot leak rows into another table's selection, a null event or null table fails closed instead of throwing, an empty table list is not a vacuous pass, and `RunResult.immutabilityVerified` records whether the reference came from outside the replayed log); ingestion unchanged (the wave's other four files are host-side scripts and their Python suites, which live outside the module surefire totals). 2026-09-17 (wave-36 holistic-measure repair, CHG-190): ingestion 470→471 — `SafetyHaltWriterTest.tokenSetDigestMatchesTheBridge`, the cross-language vector that pins the Java token-set digest to the bridge's own digest, so a divergence between the two G8 fingerprint derivations fails a test instead of silently weakening the gate; common 768->769 on 2026-09-30: +1 R2 archive selection S2 (`EodControllerToolTest.parseTieringModesForTheR2ArchiveSelection`), CHG-465; compute 637->638 on 2026-09-30: C6 re-measure of the plain-suite total (no A1/A2 change added a compute test); compute 638->650 on 2026-09-30: +12 Wave B — +4 `MergedCandleRowTest` +1 `SignalJobConfigTest.mergedCandleFeaturesDefaultsOffAndParses` (W-B2, CHG-469) and +7 W-B3 (3 `MergedCandleFeaturesColumnsAgreementTest`, 2 `MergedCandleRowsTest`, 2 `StrategyHostFeaturesTest` merged emission; CHG-470); ingestion 559->560 on 2026-09-30: +1 `DdlBootstrapSchemaAgreementTest.candleFeaturesRegistryMatchesDdl` (the bootstrap registry must cover DDL 35; CHG-471); compute 650->651 on 2026-09-30: +1 `MergedCandleRowsTest.readerPredicatesMatchTheSealFlag` (W-B4 reader predicates; CHG-472); compute 651->670 on 2026-09-30: Wave C W-C1..W-C3 — +5 CHG-476 (`FlussCandleFetcherTest` sealed-only legs + `SignalJobConfigTest` context-source leg), +1 CHG-477 (`legacyCandleSinksDefaultOnAndRequireMergedWhenOff`), +11 CHG-478 (7 `TableContractValidatorTest` merged-contract legs + 4 `SignalJobMergedPreflightTest`); the headline is the current full-suite surefire sum (the W-C1-era 656 was itself a mixed targeted-run reading). compute 670->657 on 2026-09-30: Wave C W-C4 -13 — retired the stored feature layer (FEATURE_ROWS / feature-values-sink / `FeatureValuesColumns` / `FEATURE_LAYER_ENABLED`+`FEATURE_TABLE`): -4 `SignalJobConfigTest` legs, -4 `TableContractValidatorTest` legs, -3 `FeatureValuesColumnsAgreementTest`, -3 `StrategyHostFeaturesTest` stored-layer legs, +1 `StrategyHostFeaturesTest.sealedMergedRowsCarryTheClosedFeatureSnapshot` (the sealed merged row carries the same snapshot; CHG-479). compute 657->627 on 2026-09-30: Wave C W-C5a decommission (full re-run 2026-10-01, 627/0/20-skip) — net -30: -10 `MultiTimeframeSinksTest` (deleted with the legacy sinks), -4 `SignalJobMergedPreflightTest` (the merged preflight is unconditional; the split acceptance is retired), -14 `TableContractValidatorTest` (legacy `candle_live`/`candle_closed` validator legs deleted), -2 `SignalJobConfigTest` (legacy flag/table keys dropped); `StrategyHostFeaturesTest` and `SignalJobOperatorUidTest` were rewritten at equal counts (merged rows ship by default; the legacy sink UIDs are pinned absent), and the two candle columns-agreement tests re-pin against DDL 35's candle prefix at no count change; ingestion 560->559 on 2026-09-30: -1 `DdlBootstrapSchemaAgreementTest.featureValuesRegistryMatchesDdl` (the bootstrap registry no longer carries `feature_values` — `candleRegistryEntriesUseRealSchemas` now pins `candle_features` plus the three retired entries absent, 8/0 green) (CHG-482; certifying full gate 19/19, 0 skipped, 2026-10-01, `logs/soak/monday-gates-20261001-012918`, tree `2bcf54fe`). compute 627->632 on 2026-10-01: +5 CHG-485 — in-memory forming-candle feature previews (`FeatureComputersTest` +2: the SMA/RSI preview equals the value after the same close and never mutates closed state; `PerInstrumentFeaturesTest` +2: `latestLive` preview/fallback/no-double-count; `StrategyHostFeaturesTest` +1: `strategySeesPerTimeframeFeaturesEvolveWithTheFormingCandle`); CHG-486 (closed-only storage) re-pinned `StrategyHostFeaturesTest` at no count change — 632/0/20 both at the intermediate CHG-485 commit and the CHG-486 end state; common 769->770 and ingestion 559->560 on 2026-10-01: +1 each CHG-487 — `RawTableDdlContractTest.storageOptionsArePinned` (the DDL pins explicit zstd and both retention constants equal `RawTableSchema`; the pre-change run failed on the absent option, and its second assertion drove the 9d->3d constant fix) and `DdlBootstrapSchemaAgreementTest.rawTableStorageOptionsMatchTheDdl` (the bootstrap descriptor carries the same option and 3d/3 retention; pre-change failed on the absent option); ingestion 560->562 on 2026-10-01: +2 CHG-488 — `TypedFlussRowConverterTest.payloadHashIsNotPersisted` and `FlussClientAdapterTickTypeTest.payloadHashIsNotPersisted` (the stored `payload_hash` must stay empty: the SHA-256 is verified at admission and recomputable from `raw_payload`; both pre-change runs failed on the legacy `abc123` cell); compute 632->637 on 2026-10-01: +5 CHG-491 — the platform-speed KPI (`docs/plans/2026-10-01-ingest-to-strategy-kpi.md`): `CandleLiveColumnsAgreementTest` +1 (the trailing `ingest_ts` probe is pinned as BIGINT/not-null after the 15-column DDL prefix and proven absent from the merged DDL), `MultiTimeframeAggregateFunctionTest` +2 (forming rows carry the close-setting tick's `ingest_ts`; a feed without one keeps the previous value, never a fabrication, and an all-unknown feed keeps the sentinel), `StrategyHostIngestKpiTest` +2 (a valid sample records `now - ingestTs`; sentinel/zero/clock-skew/null-histogram are skipped). compute 637->643 on 2026-10-01: +6 CHG-501 — the strategy market snapshot (operator request; `docs/08_implementation/04-signal-job.md` §Strategy market snapshot): `MultiTimeframeMarketSnapshotTest` +5 (a trade tick carries the captured 16 raw extras on all six forming rows; quote ticks update the snapshot without emitting and the next trade carries the fresh values; an absent field keeps the last known value; never-seen fields render NULL, never zero; the 1 s mirror fallback carries the same snapshot), `CandleLiveColumnsAgreementTest` +1 (the trailing market section is BIGINT NULL, transport-only, and absent from the DDL); `MultiTimeframeStateTest` assertions extended at no count change; the C6 total includes the gated `B4SignalIntentE2ETest` 2 skips the plain run excludes. compute 643->656 on 2026-10-01: +13 CHG-502 — the native market view (operator request; `docs/08_implementation/04-signal-job.md` §Strategy market snapshot): `MarketViewTest` +8 (presence and level bounds; totals/spread/imbalance; microprice weighting; buy and sell ladder walks; the insufficient-depth sentinel; change-clock ages), `MultiTimeframeMarketSnapshotTest` +2 (all 42 values + both clocks ride the FIFTEEN_S row while the other five rows stay NULL; the stats clock moves only on stats changes and the depth clock only on depth changes, repeats move neither, and a quote can move a clock without emitting) with its 5 CHG-501 tests rewritten to the 42-value canonical-row contract, `StrategyHostMarketViewTest` +3 (decode reads all 44 columns including both clocks and maps NULL to 0; the canonical row fills the view for the live and closed callbacks while a non-canonical row never wipes it; the context-ready callback carries the same view); `MultiTimeframeStateTest` +1 (the nested market snapshot defaults empty; the survival test now asserts the nested fields) — 656 run / 20 env-gated skips. compute 656->675 on 2026-10-02: +19 CHG-504/505 — +5 `SignalAuditJsonTest` (the v2 audit document shape pinned end-to-end: strategy/fire/candle/market sections, `0 = not provided` renders null, non-finite imbalance renders null, a missing view renders the same shape, strings are escaped) +3 `N7RangeBreakoutStrategyTest` (a live fire's v2 audit carries the full market snapshot and `path=live`; an arm-time fire carries `path=arm` and the arming candle; a legacy fire without a view renders the null-market shape) +4 `MultiTimeframeMarketSnapshotTest` (a changed quote emits one `TF=MKT` row with identity + section + clocks and no forming row; an identical repeat emits nothing; the flag off emits nothing; the fallback feed is inert) +4 `StrategyHostMarketViewTest` (a `TF=MKT` row refreshes the view and calls `onMarketUpdate` only, never `onLiveTick`; the default callback is a no-op for existing strategies; a throwing rule is isolated and the next strategy still receives the row; a strategy can fire from `onMarketUpdate` and the host forwards and dedups the row) +2 `SignalJobConfigTest` (`STRATEGY_MARKET_TICK_ENABLED` defaults off and requires the host naming both flags; an invalid boolean fails fast) +1 gated `SignalJobOperatorUidTest` leg (the flag-on graph carries exactly the pinned UID set — skipped in plain runs; the class is red on this host for the pre-existing lake-plugin classpath gap, see the 2026-10-02 gate blocker) — 675 run / 21 env-gated skips.
> The longer historical status line below remains a dated implementation record; the current
> C6 machine gate reads this line.
>
> **2026-10-02 gate blocker (pre-existing; found while landing CHG-504/505).** The
> compute UID-pin invocation of gate step 16 (`SignalJobOperatorUidTest`,
> `COMPUTE_INT_TEST_P6=true`) is red on this host: `raw_table_1` is datalake-enabled
> (2026-10-01) and `SignalJob.buildTopology` -> `FlussSourceBuilder` ->
> `LakeSourceUtils.createLakeSource` throws `UnsupportedOperationException: No
> LakeStoragePlugin available for data lake format: iceberg` because
> `fluss-lake-iceberg-1.0.0.jar` (present at
> `code/01_platform/01_docker/fluss-plugins/iceberg/`) is not on the compute test
> classpath. Every leg fails, including the pre-CHG-505 baseline legs. The new
> CHG-505 flag-on UID leg is therefore unverified on this host; the fix is either
> the plugin on the compute test classpath or a non-datalake raw-table fixture.
> `make gate` cannot certify step 16 until reconciled — do not silently work around it.

Read this file before starting any build phase. It contains the shared plan, rules, software-version checks, storage rules, and safety rules.

## Build plan

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Status block reflects current project phase (Design closed / Implementation active / Runtime validation pending / Live-money blocked) per project status vocabulary.
  - Source: 01-foundation.md -> "Status" (orig L10)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Design-ready; implementation active (live-money blocked) |
| Owner | Platform Team |
| Sources | `docs/01_project/`, `docs/02_requirements/`, `docs/04_contracts/` |
| Supersedes | Root-level `plan.md` (deleted; content fully absorbed here) |

### Purpose

#### Implementation checklist

- [x] Document purpose realized: defines fixed sequence, per-component work cards, acceptance checks, and completion gate as the tracker.
  - Source: 01-foundation.md -> "Purpose" (orig L19)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

This document defines the fixed implementation sequence, per-component work cards, mandatory acceptance checks, and completion gate for the per-instrument tick pipeline. It is the master implementation checklist for the coding agent.

### Requirement change record (2026-08-13)

Per the `00-start-here.md` conflict rule (`docs/08_implementation/00-start-here.md` §Authority and conflict rule), the user requirement change of 2026-08-13 is recorded here:

- **Candle tables are KV-only (2026-08-13 requirement, executed).** `feature_candles_15s` is the **KV upsert table** — PK exactly `(instrument_token, window_start)`, `bucket.key=instrument_token`, 16 buckets; one row per closed 15 s window per instrument, last-write-wins (replay converges, no row growth). The candle LOG+KV dual-sink era (`feature_candles_15s` LOG + `feature_candles_15s_current` KV, CANDLE-KV-REPLAY-001, DDL-22, `CandleMigrationTool`/`CandleMigrationBatchJob`/`run-batch.sh`) is **deleted** (code, DDL, tests — 2026-08-13).
- **[LOG + KV] moves to the SIGNAL tables.** `Signal_Candidates` → **LOG** (append-only, one new row per found signal, never updated — reverses the R-084 KV conversion; routing key `instrument_token`). New `Signal_Candidates_current` → **KV current-state**, PK `(instrument_token)`, latest/active per instrument, supersession overwrites (resolves the R-084 dead-supersede-chain problem).
- **Status:** docs + code updated 2026-08-15 (candle KV-only conversion executed: DDL 03 KV, manifest regen, `DdlBootstrap`, `CandleTableSchema`, `TableContractValidator.validateCandleKvTable`, KV sink serialization, tests, o2-provision.py; unit suites green 341/236/294 (ingestion +2 2026-08-18 — ING-UNIT-023 the CHG-015 bridge SIGTERM-drain regression revived from the superseded `a980a33` draft and reconciled with the M3 scripted-fake-bridge design (`BridgeShutdownRegressionTest`, see [`15-ingestion-test-hardening.md`](./15-ingestion-test-hardening.md)), CHG-032, and ING-UNIT-024 the real JVM shutdown-hook layer covering the main-thread join (`BridgeShutdownHookTest` + `BridgeShutdownDriver`), CHG-033; common +1 `SlotAssignmentResolverTest.serializableRoundTrip` 2026-08-18 — Item E safety-consumer live run found `SlotAssignmentResolver` was not `Serializable` though carried as a Flink operator field, so `SafetyHaltJob` had never been submittable; fixed `SlotAssignment extends Serializable` + `SlotEntry` serializable, CHG-026; compute +1 SIG-FAIL-001 2026-08-17 — `SignalJobCheckpointFailureIntegrationTest`, env-gated; prior count 291, the −19 DEC-038-era tests 2026-08-17 — the Design-B merge `34af190` deleted the 5 raw-client dedup classes (`FingerprintDedupStateStore`, `InMemoryFingerprintDedupStateStore`, `FlussFingerprintDedupStateStore`, `FingerprintDedupWriterFunction`, `DedupBucketAssigner`) and their 6 test classes (`DedupBucketAssignerTest`, `FingerprintDedupExternalizationBenchmarkIT`, `FingerprintDedupExternalizationTest`, `FingerprintDedupWriterFunctionTest`, `SigState002RehydrationRestoreIntegrationTest`, `SigState003FailClosedPreflightIntegrationTest`) — CHG-022/CHG-023, dedup is now authoritative Flink keyed state; then −10 CHG-023 item-1 2026-08-17 — the ComputeOtlpEmitter metric tests (18) were replaced by ComputeAlertLogsTest (5) and the REQ-FC-010 metric legs reworked to MetricGroup assertions (native flink-metrics-otel reporter); then −2 CHG-023 item-2 2026-08-17 — the native StateTtlConfig expiry swap deleted `DedupStateSizeTest.perBucketEstimateCoversExpiryIndex` and `FingerprintDedupFunctionTest.oneTimerAtSharedExpiryClearsAllEntries` (the expiry index + event-time timers are gone; expiry is native TTL; then −11 CHG-023 item-4 2026-08-17 — the StallGuardedSink watchdog + its 11-test class deleted — sinks are plain FlussSinks, the Fluss client's own client.request-timeout is the stall bound); +3 `RuntimeOptionsTest` memory/network passthrough legs 2026-08-17 (CHG-021), +2 CEP-guard agreement/scope-parity legs 2026-08-16 (`CepDependencyGuardTest` shell-guard agreement + scan-scope parity, `make cep-check-module`), +3 SIG-UNIT closure 2026-08-16 (+1 `CepDependencyGuardTest` SIG-UNIT-007 dependency scan, +2 `CandleEmitFunctionTest` SIG-UNIT-008/009 emit-half state-content assertions — the last pending SIG-UNIT rows now covered), +4 `DedupBucketAssignerTest` +1 eviction timer-deletion unit test +1 `SignalJobCompactCheckpointRestoreIntegrationTest` +3 `CandleWindowEmitHarnessTest` +2 `CandleWatermarkIdlenessTest` +1 `FingerprintDedupFunctionTest` limitation test +1 `CompatFlinkCheckpointRescaleIntegrationTest` +1 `SigState002RehydrationRestoreIntegrationTest` +1 `SigState003FailClosedPreflightIntegrationTest` +2 REQ-FC-010 metric tests 2026-08-15, SIG-STATE-001 bucket + timer + compact-restore/bounded-checkpoint fixes, Phase B items 1–9, +15 Slice 2.2 forming-bar handoff tests 2026-08-16 (`FormingBarBuilderFunctionTest` ×5, `FormingBarDetectionFunctionTest` ×7, `FormingBarTypeInfoTest` ×3), +12 forming-bar KV persistence tests 2026-08-16 (`FormingBarWriterFunctionTest` ×6 + validator/config/builder legs ×6), +1 `FormingBarWriterFunctionTest.checkpointRestoreResumesBufferedBar` + 3 live `FormingBarRehydrationIntegrationTest` legs 2026-08-16 (FORMING-BAR-REHYDRATE-001, restart-rehydration: checkpoint-restore Flink half + cold-restart rehydration through the production `FlussFormingBarStateStore` against the dev cluster, env-gated `COMPUTE_INT_TEST_FORMING_BAR_REHYDRATE`)); `CompatFlussCompositeKeyIntegrationTest` (COMPAT-FLUSS-005 raw-client composite-PK matrix, env-gated; the shared `CompositeKeyMatrixVerifier` also gates every DDL apply IN-BAND) + `CompositeKeyMatrixVerifierTest` (matrix cell-spec pin, pure JVM, shared with the in-band apply gate) + `DdlApplyToolStatusTest` (apply-status decision, pure JVM) + `ColumnOwnershipTest`/`ColumnOwnershipAgreementTest`/`OrderLifecycleColumnsAgreementTest`/`ExecutionAttemptsColumnsAgreementTest` (SCH-15 column-ownership matrix + DDL pins, pure JVM, +49) + `InMemoryAttemptStoreTest` (SCH-15 guard first consumer — prepare identity/replay guard + Task 5 transition rules, +20) + `FillEventMapperTest`/`PositionProjectorDriverTest` (SCH-20 operator-wiring core — fill mapping + per-position version-gated driver, +17) + `FlussPositionsStateStoreIntegrationTest` (SCH-20 live drill, env-gated, PASSED on the dev cluster 2026-08-15) + M1 test-hardening 2026-08-15 (`ING-FAIL-004` concurrency invariants, `ING-FAIL-005` halt-latch pinned "until restart", `ING-FAIL-007` TIME_JUMP emission, `ING-DQ-010` no-silent-drop ledger, `ING-DQ-011` seeded fuzz corpus + M3 failure-path E2E 2026-08-15 (`ING-FAIL-008` crash-loop, `ING-FAIL-009` auth-failure, `ING-FAIL-010` shutdown-deadlock, `ING-INT-005` READY gating matrix + M4 Go-bridge/data-quality 2026-08-15 (`ING-UNIT-014` freshness boundaries, `ING-UNIT-015` payload-hash edges, `ING-UNIT-016/017` golden pin + DEC-012 dedup, `ING-RES-002/003/004`, `ING-TCP-003` + M5 telemetry/ops 2026-08-15 (`ING-UNIT-021/022` OTLP scrubbing + cardinality — G6 RESOLVED, `ING-FAIL-006` warning-window throttle, `ING-UNIT-020` FATAL-message assertions, `ING-INT-006` entrypoint harness — see [`15-ingestion-test-hardening.md`](./15-ingestion-test-hardening.md))) pinned).
- **Authority chain:** requirement change (user) → master dossier [`04-signal-job.md`](./04-signal-job.md) (consolidated 2026-08-17 — absorbs tracker 14's substance; `14-candle-log-kv-replay-safety_2.md` was deleted the same day) → `docs/08_implementation/09-production-swarm.md` (P10 plan section, RE-SCOPED). Contract record: `04-business-logic.md` carries the requirement-change banner + re-scoped Outputs; `03-compute.md` is superseded on the candle-storage kind (its "candle output LOG-only" claim predates the 2026-08-13 KV-only conversion).
- **Sustained-throughput gate re-scoped 60,000 → 50,000 ticks/s (2026-08-13, DEC-036).** The average-baseline acceptance gate is now **50,000 ticks/s** (≈16.7 ticks/s/instrument over 3,000 instruments); the **90,000 ticks/s capacity-peak campaign is RETIRED** (DEC-036) — the per-instrument-cap ceiling (3,000 × 20) remains only as a generator stress bound. Per-instrument hard cap (20 ticks/s) and the 3,000-instrument envelope are unchanged; the generator retains a 20 ticks/s/instrument capability (MOCK-UNIT-002). Code: `FixedScope.MAX_SUSTAINED_TICKS_PER_SEC` → 50_000 (`PerfBaselineTest` gate 50k/48k-floor); every forward-looking perf/acceptance/storage/DR row in docs was reconciled; executed bench plans/results (P7) stay locked as historical evidence. **SUPERSEDED 2026-08-27 by DEC-045: per-instrument cap 20/s, gate re-raised to 60,000 ticks/s (3,000 × 20).**
- **H1-2 send-failure taxonomy reconciled with contract 07 (2026-09-29, CHG-369).** Contract 07's
  `UNKNOWN` row listed `401/403` among never-retried outcomes while the operator-accepted High
  remediation plan (D3, 2026-09-28) classifies the bridge's pre-dispatch rejections (`400`, `401`,
  `403`, `404`, `409`, `500 fingerprint_failed`) as retry-safe `NotSent`. Authority order (code +
  tests > active decisions > contracts) resolves it toward the plan: the executor was implemented
  with the taxonomy, and contract 07 plus `05-execution-core.md` were updated in the same change.
  Dispatch-time failures (timeout, reset/EOF after send, malformed response, other statuses) remain
  `Unknown`: never retried, process-wide halt, no terminal order event.
- **Watermark default conflict reconciled (2026-09-29, M3-4 / DEC-058).** The compute code and its
  test pin `WATERMARK_OUT_OF_ORDER_MS` at **500 ms** (single-timeline rule 2026-08-30; measured: the
  5 s default produced p95≈5.7 s end-to-end, the wait itself), while six doc sites still said 5 s.
  Per the authority order (code + tests > decisions > contracts > requirements > dossiers) the code
  value stands; the six sites are corrected in the same change and `test_compute_config_doc_parity.py`
  now fails on any future divergence. `ALLOWED_LATENESS_MS` (5 s) is a different knob — unchanged.
- **Merged now-view coverage conflict (2026-10-01, Wave C post-cutover round).** DEC-059's
  locked decisions Q22/Q26 (`docs/plans/2026-09-30-readability-and-state-growth-measurement.md`)
  require the merged table to keep forming rows for **all six** timeframes at ~1 Hz/key
  ("stored forming rows (1 Hz) serve UI/external readers", Q50). Post-W-C5a the strategy host is
  the **only** `candle_features` writer, and `SignalJob` feeds it `LIVE_TICK_TAG` (smallest TF,
  per tick) whenever `MULTITF_FAST_LIVE_FEED=true` (default) — the aggregator's all-TF `LIVE_TAG`
  snapshot stream now has **no consumer**. Measured 2026-10-01
  (`logs/wave-c-stage-profile-20261001-065919`, 900 s, 2 Hz x 2 433): FIFTEEN_S forming rows are
  fresh (p50 232 ms) but THIRTY_S..FIFTEEN_M have **no current-window row** — a fresh reader falls
  back to the just-sealed window (p50 15.8 s / 30.8 s / 89.9 s / 132.8 s / 188.9 s vs ~750 ms for
  every TF before the cutover). **Strategy half resolved 2026-10-01 (CHG-484):** the fast feed
  carries every TF's forming row per tick, so strategies see all six evolving candles (and, with
  CHG-485, their evolving features) in memory. **Storage half resolved 2026-10-01 (operator
  decision, CHG-486): closed-only storage** — `candle_features` receives one sealed row per
  closed window and nothing at the live cadence; the stored now-view requirement (Q22/Q26/Q50)
  is retired for storage: the live world is the strategy host's in-memory view (Flink memory)
  and Fluss holds closed history only. Smoke evidence
  (`logs/chg486-smoke-20261001-111805`): FIFTEEN_S writes +8,658 rows/min vs +301,888/min
  pre-change (~35x, same smoke protocol); zero unsealed rows observed by the readability probe;
  the live path metrics unchanged.

### Fixed scope

#### Implementation checklist

- [x] Fixed-scope hard limits enforced (3,000 instruments; 60k sustained gate = theoretical cap ceiling at 3,000 x 20/s, the 90k peak campaign stays retired (DEC-045); 20/instrument cap; INGESTION batch=1/wait=0; one current candidate per instrument by the `Signal_Candidates_current` KV primary key; no CEP; every tick -> raw_table_1; no Flink->Fluss->Flink round trip).
  - Source: 01-foundation.md -> "Fixed scope" (orig L23)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/config/FixedScope.java

| Item | Fixed value |
| --- | --- |
| Broker delivery rate | Variable; no fixed per-instrument arrival interval |
| Per-instrument rate | Expected baseline average ≈16.7 ticks/second at the 50k gate (generator retains 20 ticks/s capability, MOCK-UNIT-002); hard maximum 20 ticks/second |
| Active instrument count | **3,000 instruments** (fixed per trading session; runtime changes require controlled restart) |
| Total rate | **50,000 ticks/second average baseline** (≈ 3,000 × 16.7); **60,000 ticks/second acceptance gate** (= 3,000 × 20, DEC-045; the 90,000 ticks/s peak campaign is retired, DEC-036) |
| Architecture | `Arrow → Ingestion → Fluss raw_table_1 → one Signal Flink job` (**→ `Trade_Decisions` REMOVED 2026-08-15, CHG-005**); `Positions → separate Babysitter Flink job` |
| MVP CEP | Disabled. No CEP operator, CEP job, or CEP dependency permitted |
| Raw data | Every accepted tick appended to `raw_table_1`; no accepted tick silently dropped |
| Signal path | Feature calculation, business rules, candidate filtering inside one Signal Flink job (**ranking/reservation/decision publication REMOVED 2026-08-15, CHG-005**) |
| Fluss round trips | Flink must not write a temporary feature/candidate record to Fluss and read it back |

### Required configuration constants

#### Implementation checklist

- [x] Centralized config-constants module (all keys, no scattered literals); startup rejects DEDUP_TTL_MS!=60000 and CANDLE_WINDOW_MS!=15000. *(2026-09-03: `DEDUP_TTL_MS` is no longer read by the compute service — CHG-303 / DEC-054; the dedup bound is `DEDUP_WINDOW_ENTRIES`.)*
  - Source: 01-foundation.md -> "Required configuration constants" (orig L37)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/config/PlatformConfig.java

All constants are versioned runtime configuration. No numeric literals scattered through source files.

| Configuration key | Required value | Enforcement |
| --- | ---: | --- |
| `BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC` | `20` | Synthetic baseline average only; not a fixed arrival interval |
| `BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC` | `30` | Workload profile rejects a per-instrument rate above `30` |
| `INGESTION_MAX_BATCH_RECORDS` | `1` (validated 1..1000) | Append each accepted tick immediately |
| `INGESTION_MAX_BATCH_WAIT_MS` | `0` (validated 0..100) | Do not wait for a batch |
| `MAX_PENDING_APPEND_RECORDS` | `50000` (validated 100..1000000) | Stop accepting at limit; set readiness false |
| `MAX_PENDING_APPEND_BYTES` | `min(67108864, floor(container_memory_limit_bytes × 0.10))` | Stop accepting at limit; set readiness false |
| `PENDING_APPEND_WARNING_PERCENT` | `80` | Emit warning alert; set readiness false at 80% of either limit |
| `DEDUP_TTL_MS` | *(deprecated)* | No longer read (2026-09-03, CHG-303); the dedup bound is `DEDUP_WINDOW_ENTRIES` = 200 entries |
| `CANDLE_WINDOW_MS` | `15000` | Fifteen seconds; reject startup for any other value |
| `CHECKPOINT_INTERVAL_MS` | `10000` | Signal and Babysitter jobs |
| `CHECKPOINT_TIMEOUT_MS` | `30000` | Signal and Babysitter jobs |
| `MAX_CONCURRENT_CHECKPOINTS` | `1` | Signal and Babysitter jobs |
| `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT` | `65` | Java max heap = 65% of container memory limit |
| `NON_HEAP_MEMORY_RESERVE_PERCENT` | `35` | Container limit minus Java max heap ≥ 35% |
| `CONTAINER_MEMORY_ALERT_PERCENT` | `85` | Critical alert at ≥ 85% total container memory |
| `MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT` | `1` | Do not forward another active candidate. Structural (one current row per instrument, `Signal_Candidates_current` KV primary key); the declaration was removed 2026-09-14 (P6-665) |

### Mandatory implementation order

#### Implementation checklist

- [x] Mandatory implementation order gating enforced (no downstream task invents an upstream contract). _(implemented 2026-08-15: `code/01_platform/04_scripts/implementation_gate.py` + `make gate-order` — the 7 tasks run in strict sequence and the first failing or missing check blocks every downstream task; unit-tested (11 tests). First real run surfaced a genuine MOCK-UNIT-002 violation — the PEAK profile's 33 ms interval is 1000/33 = 30.3 ticks/s, above the 30/s cap; fixed to 34/35 ms (`SyntheticWorkload.nextIntervalMs`). BAB-UNIT-001/002 landed 2026-08-15 (`BabysitterJobTest` — zero-action StreamGraph inspection + fail-closed flag variants; compute suite 185 → 188), unblocking task 6 → task 7 → the gate now runs **all 7 tasks GREEN end-to-end** (verified 2026-08-15).)_
  - Source: 01-foundation.md -> "Mandatory implementation order" (orig L60)
  - Design: Design-ready | Implementation: Implementing | Evidence: Tested (11 gate unit tests + 3 BAB tests; all-7-tasks end-to-end GREEN 2026-08-15) | Live-money: Blocked
  - Location: code/01_platform/04_scripts/implementation_gate.py (+ Makefile `gate-order` target)

Tasks must be completed in this sequence. Do not begin a later task until all acceptance checks in the preceding task pass.

| Sequence | Task | Component Dossier | |
| --- | --- | --- | --- |
| 1 | Make the mock broker per-instrument, variable, and deterministic | [`11-testing-and-release.md`](./11-testing-and-release.md#performance-benchmark-procedure) | Seeded variable profile averages ≈16.7 ticks/s/instrument at the 50k-gate baseline, caps every instrument at 20 ticks/s, and supports the 60,000 ticks/s theoretical cap ceiling (generator stress bound; peak campaign retired, DEC-036) |
| 2 | Implement immediate, bounded ingestion writes | `docs/08_implementation/03-ingestion.md` | No batching; 80%/100% backpressure; per-tick append latency tracking |
| 3 | Implement compact, bounded Signal-job state | `docs/08_implementation/04-signal-job.md` | Flink state stays compact and bounded: dedup set externalized to a Fluss KV state table (DEC-038) with a bounded Flink working cache; candle state contains OHLCV fields only; Flink checkpoint is small and not a second copy of Fluss business state; no CEP |
| 4 | ~~Bound candidate work and preserve in-job ranking~~ | `docs/08_implementation/04-signal-job.md` (Ranking section) | ~~MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT=1; rejection codes; no Fluss round trip for ranking~~ — **REMOVED 2026-08-15 (CHG-005)** |
| 5 | Pin job recovery and container-memory settings | `docs/08_implementation/09-production-swarm.md` | Checkpoint 10s/30s/1; JVM 65%/35%; S3 checkpoint storage |
| 6 | Keep Babysitter state minimal | `docs/08_implementation/05-execution-core.md` (Babysitter — position observation) | POSITION_ACTIONS_ENABLED=false; only latest position version, offset, freshness, schema version, no-op counters |
| 7 | Implement required alerts and safe-stop conditions | `docs/08_implementation/10-observability.md` | 8 alert thresholds with 60s consecutive breach; idempotent Safety_Halt_Request; no auto-resume |

## Verification mapping

The required behavior above is verified by the canonical [Foundation and workload test design](./11-testing-and-release.md#foundation-and-workload): `PERF-PER-INSTRUMENT-001` to `PERF-PER-INSTRUMENT-003`, `FAIL-PENDING-001`, `FAIL-CHECKPOINT-001`, `STATE-DEDUP-001`, `STATE-CANDLE-001`, `BABYSITTER-001`, `MOCK-UNIT-001` to `MOCK-UNIT-003`, and `MOCK-PERF-001`.

### Completion gate

#### Implementation checklist

- [x] Completion-gate checks implemented/verifiable (no CEP dependency; MVP architecture unchanged; no test enables live execution; old 20/50-ms & 75k/112.5k/150k workloads absent).
  - Source: 01-foundation.md -> "Completion gate" (orig L78)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/01_platform/04_scripts/cep_guard.sh + Makefile `cep-check` target (no-CEP enforced; MVP-unchanged + no-live-execution are review facts); module-scoped CI-style check: `cep-check-module` (`code/01_platform/04_scripts/cep_module_check.sh` — runs `cep_guard.sh` on the compute module and confirms agreement with the SIG-UNIT-007 `CepDependencyGuardTest`, incl. scan-scope parity, landed 2026-08-16)

Implementation is complete only when all conditions below are true:

1. All tasks in this plan pass their acceptance checks.
2. The 30-minute variable-baseline performance test passes every applicable condition in the E2E test matrix. (The 90,000 ticks/s peak-capacity campaign is retired, DEC-036; 60,000 ticks/s gate, DEC-045.)
3. Old fixed-20/50-ms, `75k`/`112.5k`/`150k` workload requirements are absent from active code, active documentation, CI gates, benchmark examples, and release criteria.
4. MVP contains no CEP dependency or CEP implementation.
5. Architecture unchanged: one Ingestion service, Fluss raw storage, one Signal job, one separate Babysitter job.
6. No test enables live order execution.

### Related dossiers

#### Implementation checklist

- [x] Dossier cross-references resolve (ingestion/signal/babysitter/production/observability/test-catalog links valid).
  - Source: 01-foundation.md -> "Related dossiers" (orig L89)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- Ingestion: [`03-ingestion.md`](./03-ingestion.md)
- Signal job: [`04-signal-job.md`](./04-signal-job.md)
- Execution Core (Action Capture + Babysitter + Executor): [`05-execution-core.md`](./05-execution-core.md)
- Production deployment: [`09-production-swarm.md`](./09-production-swarm.md)
- Observability: [`10-observability.md`](./10-observability.md)
- Test catalog: [`11-testing-and-release.md`](./11-testing-and-release.md)
- Throughput tests: [`11-testing-and-release.md`](./11-testing-and-release.md#performance-benchmark-procedure)

## Documentation status and evidence rules

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Status banner current (implementation-ready governance; live-money disabled; Executor starts HALTED).
  - Source: 01-foundation.md -> "Status" (orig L105)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Implementation-ready governance; project remains release-blocked |
| Owner | Platform Team with Execution Team approval for money-moving behavior |
| Source of work | `docs/08_implementation/01-foundation.md` (canonical plan; all references to `plan.md` resolve to this file) |
| Live-money default | Disabled; Executor starts `HALTED` |
| Scope | Documentation, evidence, change control, and traceability; no production code |

### Purpose

#### Implementation checklist

- [x] Governance conversion purpose realized (no item treated complete merely because a file changed).
  - Source: 01-foundation.md -> "Purpose" (orig L115)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

This section defines how the build plan in this file is converted into safe implementation work. It prevents a checklist item from being treated as complete merely because a file was changed.

### Project status vocabulary

#### Implementation checklist

- [x] Project status vocabulary enforced across docs (Design closed / Implementation active / Runtime validation pending / Live-money blocked).
  - Source: 01-foundation.md -> "Project status vocabulary" (orig L119)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/evidence/StatusVocabulary.java

Every index document and status block SHALL use exactly these terms for the project phase. The current status applies across all documents.

| Term | Meaning | Current? |
| --- | --- | --- |
| **Design closed** | Architecture, requirements, contracts, and DDL proposals are stable | ✅ Phase 4.1 (closed 2026-07-23) |
| **Implementation active** | Code, DDL application, integration, and testing are in-progress | ✅ Phase 4.2 (active since 2026-07-23) |
| **Runtime validation pending** | Performance, failure, and recovery tests are not yet proven in a production-like environment | Pending |
| **Live-money blocked** | Real-money order placement is disabled by default; Executor starts `HALTED` | Blocked |

Documents SHALL use one of these statuses in their status block, not free-form equivalents such as "next," "pre-implementation," "active target," or "in design."

### Document-level status banner

#### Implementation checklist

- [x] 4-axis status banner (Design / Implementation / Evidence / Live-money) enforced on every dossier/contract/requirement.
  - Source: 01-foundation.md -> "Document-level status banner" (orig L132)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

Every implementation dossier, contract, and requirement file SHALL include a status banner. The banner SHALL report these four dimensions:

| Dimension | Options |
| --- | --- |
| **Design status** | `Draft`, `Design-ready`, `Evidence-blocked`, `Superseded` |
| **Implementation status** | `Not-implemented`, `Implementing`, `Implemented`, `Validated` |
| **Evidence status** | `Untested`, `Tested-in-sandbox`, `Production-validated` |
| **Live-money status** | `Blocked` (default), `Approved-for-testing`, `Live` |

A document is "implementation-ready" only when Design status is `Design-ready` AND every unresolved external fact is recorded as a named evidence gate. "Implementation-ready" does NOT mean implemented, tested, validated, or production-ready.

### Work item lifecycle

#### Implementation checklist

- [x] Work-item lifecycle states (OPEN->...->STRUCK_THROUGH, BLOCKED) modeled in tracking tooling with owner/missing-evidence/unblock condition.
  - Source: 01-foundation.md -> "Work item lifecycle" (orig L145)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/workitem/WorkItemState.java (+ WorkItemLifecycle.java, WorkItem.java)

```text
OPEN
  → DOCUMENTING
  → DOCUMENTATION_READY
  → APPROVED_FOR_IMPLEMENTATION
  → IMPLEMENTED
  → VERIFIED
  → STRUCK_THROUGH
```

`BLOCKED` can occur from any state when a required external fact, approval, or test environment is unavailable. A blocked item must identify the owner, missing evidence, and unblock condition.

### Evidence record

#### Implementation checklist

- [x] Evidence record format captured per claim (work item ID, requirement IDs, artifact, version, environment, workload, clock, result, owner/date, limitations).
  - Source: 01-foundation.md -> "Evidence record" (orig L159)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/evidence/EvidenceRecord.java

Every implementation or release claim uses this record:

| Field | Required content |
| --- | --- |
| Work item ID | `01_plan.md` task or issue ID |
| Requirement IDs | Exact `REQ-*`, `DEC-*`, contract, and DDL references |
| Artifact | File, image, schema, test, report, or deployment record |
| Version | Git commit, image digest, dependency/protocol version |
| Environment | Local, acceptance, or production-like Swarm |
| Workload | Rate, duration, instrument universe, connection count, message distribution |
| Clock evidence | UTC source and offset; monotonic duration source |
| Result | Pass/fail, p50/p95/p99 where relevant |
| Owner/date | Responsible reviewer and completion date |
| Limitations | Unproven behavior or remaining risk |

Evidence is invalid if it omits the version or environment that produced it.

### Placeholder policy

#### Implementation checklist

- [x] Placeholder constants defined (BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED, FLINK_VERSION_TO_BE_PINNED, ARROW_API_CONTRACT_TO_BE_VERIFIED, ...); no guessed values; each blocks live-money where safety-relevant.
  - Source: 01-foundation.md -> "Placeholder policy" (orig L178)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/version/PlaceholderVersions.java

Use explicit placeholders instead of guessed values:

```text
BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED
BROKER_POSTBACK_PROTOCOL_TO_BE_PINNED
BROKER_EVENT_ID_AVAILABILITY_TO_BE_VERIFIED
BROKER_CLIENT_REFERENCE_ECHO_TO_BE_VERIFIED
BROKER_IDEMPOTENCY_SUPPORT_TO_BE_VERIFIED
FLUSS_SERVER_VERSION_TO_BE_PINNED
FLUSS_CONNECTOR_VERSION_TO_BE_PINNED
FLINK_VERSION_TO_BE_PINNED
ARROW_API_CONTRACT_TO_BE_VERIFIED
PRODUCTION_IMAGE_DIGEST_TO_BE_PINNED
~~S3_CHECKPOINT_URI_TO_BE_DEFINED~~ — **RESOLVED 2026-08-17: Cloudflare R2** (`CHECKPOINT_DIR=s3://tradingticks-aug-2026/flink-checkpoints`, bucket `tradingticks-aug-2026`, endpoint `R2_ENDPOINT`, region `auto`; tracker 14 P4.2)
```

A placeholder is acceptable in development documentation only if:

1. Its missing value is obvious.
2. The affected behavior is disabled or safely bounded.
3. An owner and evidence method are recorded.
4. It blocks live-money readiness where the value affects safety, compatibility, identity, or durability.

Do not use `latest`, unqualified defaults, `seq_no`, `postback_seq`, or a generic `order_id` as substitutes for unknown facts.

### Change control

#### Implementation checklist

- [x] Change-control reconciliation review required for decision/requirement/DDL/identity/connector/gate/retention/topology/secret changes. _(implemented 2026-08-15: `code/01_platform/04_scripts/change_control_check.py` + docs-audit C14 — every change record in `docs/05_deployment/change-records/` (one `CHG-<N>.md` per change) must name the six required fields with a valid compatibility class and `plan_tasks`/`affected_artifacts` references that resolve to real trackers/dossiers/files (phantom refs rejected); records are filed against `_template.md`; no records on file yet, so the gate passes vacuously until the first real record is filed. Unit-tested (38 tests).)_
  - Source: 01-foundation.md -> "Change control" (orig L205)
  - Design: Design-ready | Implementation: Implementing | Evidence: Untested | Live-money: Blocked
  - Location: code/01_platform/04_scripts/change_control_check.py (+ docs-audit C14) and docs/05_deployment/change-records/

A change to any of the following requires a reconciliation review:

- Active decision
- Requirement
- DDL/schema
- Identity or event contract
- Flink state/checkpoint contract
- Broker/Arrow REST protocol adapter
- Execution gate or approval behavior
- Retention/offload policy
- Deployment topology or secret scope

The change record must identify affected artifacts, compatibility class, state/savepoint impact, test updates, rollback behavior, and plan tasks.

### Documentation acceptance rules

#### Implementation checklist

- [x] Documentation acceptance rules satisfied (dossier defines owner, inputs/outputs, identities, state transitions, ordering/dedup/idempotency, retry/failure, config/placeholders, metrics/logs/health, tests, deployment/rollback/evidence).
  - Source: 01-foundation.md -> "Documentation acceptance rules" (orig L221)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

A dossier is implementation-ready only when it defines:

- Owner and non-owner boundaries
- Inputs and outputs
- Identities and schema versions
- State transitions and invariants
- Ordering, deduplication, and idempotency
- Retry and failure behavior
- Configuration and unresolved placeholders
- Metrics, logs, health/readiness states
- Unit, integration, failure, recovery, and acceptance tests
- Deployment, rollback, and operational evidence

Implementation-ready does not mean validated. Validation requires executable tests and runtime evidence.

### Live-money stop conditions

#### Implementation checklist

- [x] Live-money stop conditions encoded as a guard checklist (no open critical risk; unverified protocol/identity; missing/fenced/corrupt executor state; etc.).
  - Source: 01-foundation.md -> "Live-money stop conditions" (orig L238)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/invariants/LiveMoneyStopCondition.java (+ LiveMoneyGuard.java)

Live-money placement remains disabled if any of these is true:

- A critical risk is open.
- A broker/protocol identity or response behavior is unverified.
- A Fluss/Flink capability is assumed but not version-tested.
- DDL and requirements disagree.
- Executor state is missing, corrupt, unfenced, or not auditable.
- An attempt has an unresolved outcome.
- Changelog continuity or checkpoint health is unknown.
- Safe-halt or single-operator (Saurabh, DEC-044) resume is unproven.
- Required observability is unavailable.
- EOD data or audit retention is unverified.

### Documentation review checklist

#### Implementation checklist

- [x] Documentation review checklist items tracked (links to requirement, no contradiction, placeholder+owner, invalid transitions, retry rules, test IDs, status updates, no prod code in doc task).
  - Source: 01-foundation.md -> "Documentation review checklist" (orig L253)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- [ ] All new behavior links to an active requirement or decision.
- [ ] No dossier silently contradicts an upstream document.
- [ ] Unknown external behavior is marked with a placeholder and evidence owner.
- [ ] Every state transition has an invalid-transition behavior.
- [ ] Every side effect has a retry/duplicate/crash-window rule.
- [ ] Every acceptance claim has a test ID or evidence record.
- [ ] Every plan task touched by the dossier has a status/evidence update.
- [ ] No production code was changed during a documentation-only task.

## Software versions and compatibility

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Version/compatibility status tracked as Evidence-blocked until matrix proven.
  - Source: 01-foundation.md -> "Status" (orig L270)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Evidence-blocked; implementation interface is defined |
| Owner | Platform Team; Execution Team owns broker/Arrow REST evidence |
| Release impact | All unresolved rows block live-money enablement |
| Source requirements | `REQ-PF-001`, `REQ-ING-002`, `REQ-AC-001`, `REQ-EXE-006`, `DEC-021` |

### Purpose

#### Implementation checklist

- [x] Compatibility-record purpose realized (no library default/broker behavior/image tag treated as stable contract).
  - Source: 01-foundation.md -> "Purpose" (orig L279)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

No implementation may treat a library default, broker behavior, or image tag as a stable contract. This dossier defines the compatibility record required before an external boundary is enabled.

### Version matrix

#### Implementation checklist

- [x] Version compatibility matrix populated with proposed (candidate) versions + owners + status; CI latest/unpinned/mutable-tag rejection enforced by `version_matrix_verify.py` (capability-evidence tests still pending).
  - Source: 01-foundation.md -> "Version matrix" (orig L283)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/01_platform/04_scripts/version_matrix.yaml (+ version_matrix_verify.py); external contracts captured in docs/04_contracts/arrow_broker.md and docs/04_contracts/openobserve.md

Populate values only from an approved artifact, lockfile, sandbox capture, or integration test.

Canonical draft artifact: `code/01_platform/04_scripts/version_matrix.yaml` (proposed candidate versions; every row `UNKNOWN` until capability tests pass). Structural validation: `version_matrix_verify.py`. Capability-evidence plan: `12-version-compatibility-evidence.md` (maps each matrix row to test IDs and the `make ddl` apply gate). External boundary contracts (market feed, postback, order API) are captured in `docs/04_contracts/arrow_broker.md`; their matrix rows carry `proposed_version` + `evidence_source` + `evidence_method` derived from that document.

| Boundary | Required value | Evidence source | Compatibility result | Owner | Status |
| --- | --- | --- | --- | --- | --- |
| Java runtime | `17.0.19` (eclipse-temurin) | Runtime image/build record | Must match Flink/Fluss clients | Platform | Pinned, awaiting evidence |
| Python runtime | `3.11.9` | Runtime image/build record | Must match Executor dependencies | Platform | **COMPATIBLE (2026-08-25)** — image rebuilt on `python:3.11.9-slim`; `ddl-apply version` proves series vs `PYTHON_VERSION` in `versions.pin` |
| Flink server/image | `2.2.1` | Official artifact/digest | Must match job API and connector | Platform | Pinned, awaiting evidence |
| Flink Java API | `2.2.1` | Dependency lock | Must match server | Platform | Pinned, awaiting evidence |
| Fluss server | `1.0.0` | Official artifact/digest | DDL/features tested | Platform | Pinned (1.0.0; final certificate pending) |
| ZooKeeper server | `3.9.2` | Official artifact/digest | Fluss metadata/coordination + Flink JobManager HA work (ensemble quorum 2-of-3) | Platform | Pinned, awaiting evidence |
| Fluss Java client | `1.0.0` | Dependency lock | Must match server | Platform | Pinned (1.0.0; final certificate pending) |
| Fluss Flink connector | `1.0.0` (flink-2.2) | Dependency lock | Must match Flink and server | Platform | Pinned (1.0.0; final certificate pending) |
| Broker market protocol | Arrow `ds.arrow.trade` (standard token stream — the live channel, `ARROW_FEED=token`; big-endian 13/17/93/249 B) and `socket.arrow.trade` (HFT, binary zstd, selectable; the 2026-08-14 Standard-feed removal note describes the HFT-only period) | `docs/04_contracts/arrow_broker.md` | Decoder compatibility | Ingestion | Pinned, awaiting evidence |
| Broker postback protocol | Arrow `order-updates.arrow.trade` (WS JSON) | `docs/04_contracts/arrow_broker.md` | Capture compatibility | Action Capture | Pinned, awaiting evidence |
| Arrow REST API | `api.arrow.trade/order/regular` | `docs/04_contracts/arrow_broker.md` | Request/response/retry behavior | Execution | Pinned, awaiting evidence |
| OpenObserve ingestion | `v0.91.5-amd64` (OTLP via `otel-collector:4317`) | `docs/04_contracts/openobserve.md` | Telemetry delivery/redaction | Operations | Pinned, awaiting evidence |
| Base images | `eclipse-temurin:17.0.19` / `flink:2.2.1-java17` / `fluss:1.0.0` / `openobserve:v0.91.5-amd64` | Registry digest/SBOM | Reproducible builds | Platform | Pinned (1.0.0 images built; digest + final certificate pending) |

### Compatibility classifications

#### Implementation checklist

- [x] Compatibility classification (COMPATIBLE / LIMITED / INCOMPATIBLE / UNKNOWN / N/A) applied per boundary.
  - Source: 01-foundation.md -> "Compatibility classifications" (orig L302)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/version/CompatibilityClass.java

| Class | Meaning | Allowed use |
| --- | --- | --- |
| `COMPATIBLE` | Tested behavior and state/wire format are compatible | Normal implementation/release |
| `COMPATIBLE_WITH_LIMITATION` | Tested with explicit limitation and mitigation | Acceptance/sandbox; production only with approval |
| `INCOMPATIBLE` | Behavior or format cannot be safely combined | Block deployment |
| `UNKNOWN` | Evidence missing | Adapter/scaffold only; blocks live money |
| `NOT_APPLICABLE` | Boundary is not used | Must include rationale |

### Required capability evidence

#### Implementation checklist

- [ ] Required capability evidence tests executed (Fluss BYTES/LOG/KV/partial_update/changelog/connector checkpoint; Flink state/checkpoint/savepoint/rescale; Arrow REST; broker corpus). _(partially done — Flink savepoint/restore/rescale proven: `SignalJobSavepointRestoreIntegrationTest` green 2026-08-13 (strict restore + 2× rescale, real `FingerprintDedupFunction` state, host-runnable); Fluss BYTES/LOG/KV/partial_update boundary covered by VM-FLUSS-SRV-005/006 evidence + SAFETY-INT-001 live; broker corpus pinned (pin-check [2/4]); Arrow REST capability still untested — remains open)_
  - Source: 01-foundation.md -> "Required capability evidence" (orig L312)
  - Design: Design-ready | Implementation: Not-implemented | Evidence: Untested | Live-money: Blocked
  - Location: _not implemented_

Before DDL or service behavior is called validated, test the exact matrix for:

- Fluss `BYTES`/VARBINARY behavior.
- LOG and KV table creation and reads/writes.
- Primary key and bucket-key rules.
- `partial_update` merge semantics and column ownership.
- FULL changelog image behavior.
- Connector checkpoint and restore semantics.
- Cross-table visibility/atomicity limits.
- Retention and lake-tiering options.
- Replication/quorum and failover behavior.
- Flink state backend, checkpoint, savepoint, and rescale compatibility.
- Arrow REST request schema, authentication, timeout, response, broker identity, and idempotency behavior.
- Broker event/postback identity, timestamps, status values, limits, reconnect, replay, and client-reference echo behavior.

### Evidence artifact format

#### Implementation checklist

- [x] Evidence artifact format recorded per matrix row (compatibility_id, versions, source_artifact, fixture, scenario, result, observed_behavior, limitations, owner, date).
  - Source: 01-foundation.md -> "Evidence artifact format" (orig L329)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/evidence/EvidenceRecord.java

Store one record per matrix row:

```text
compatibility_id: COMP-<boundary>-<number>
boundary: <component/interface>
versions: <all relevant versions and image digests>
source_artifact: <spec URL, capture hash, dependency lock, or test path>
fixture: <fixture/capture/test dataset>
scenario: <behavior exercised>
result: COMPATIBLE | LIMITED | INCOMPATIBLE | UNKNOWN
observed_behavior: <fact, not assumption>
limitations: <remaining risk>
owner: <team/person>
date: <UTC date>
```

### Implementation rules

#### Implementation checklist

- [x] Implementation rules enforced (runtime fails if version absent/latest; adapter exposes only proven behavior; unknown broker fields unavailable; protocol/connector changes require replay/restart tests).
  - Source: 01-foundation.md -> "Implementation rules" (orig L347)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/version/VersionGate.java

- Runtime configuration must fail if a required version is absent or uses `latest`.
- An adapter may expose only behavior proven by the evidence record.
- Unknown broker fields must remain unavailable rather than being synthesized.
- A protocol change increments the decoder/schema version and requires replay tests.
- A connector change requires checkpoint, duplicate, out-of-order, partial-visibility, and restart tests.
- A version change affecting state or wire format requires migration classification and rollback/readability evidence.

### Completion checklist

#### Implementation checklist

- [x] Completion checklist satisfied (every matrix row owned; unknown fields blocked; Fluss/Flink capability tests pass; CI rejects mutable tags; release links matrix). Versions/digests + corpus + CI pin gate implemented 2026-08-13 (`versions.pin`, `corpus.sha256`, `pin-check.sh`, pom-snapshot-scan); capability tests green (112/192/185 + savepoint restore; re-measured 2026-08-14). Sub-rows L549 (unknown-field blockers), L551 (Arrow REST sandbox), L554 (release record link) remain open below.
  - Source: 01-foundation.md -> "Completion checklist" (orig L356)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- [x] Every matrix row has an owner and evidence method.
- [x] Exact versions/digests are recorded. ✓ `code/01_platform/04_scripts/versions.pin` (FLINK_VERSION, FLUSS_VERSION) + `corpus.sha256` (6 golden-file digests); pin-check [1/4] verifies the 14-boundary version matrix, pin discipline satisfied.
- [ ] All unknown protocol fields have explicit blockers.
- [x] Fluss/Flink capability tests pass for the selected versions. ✓ fresh full runs 2026-08-13: common 112/0/0, ingestion 180/0/7 skipped, compute 184/0/12 skipped; re-measured 2026-08-14: ingestion **192/0/7 skipped**, compute **185/0/13 skipped** (ingestion corrected to **188** after the 2026-08-14 Standard-feed test deletion; compute re-measured **188/0/13 skipped** 2026-08-15 after the BAB-UNIT-001/002 tests); gated battery 10/10; `SignalJobSavepointRestoreIntegrationTest` green (strict restore + 2× rescale, real dedup state).
- [ ] Arrow REST sandbox evidence proves response and correlation behavior.
- [x] Broker packet/postback corpus is versioned and reproducible. ✓ `corpus.sha256` pins 6 golden files (`full-tick.frame/.golden`, `ltp-tick.frame/.golden`, `response.frame`, `unknown-packet.frame`); pin-check [2/4] re-verifies integrity every run.
- [x] CI rejects mutable image tags and unpinned dependencies. ✓ `pom-snapshot-scan.py` bans external SNAPSHOT deps (pin-check [3/4]); `versions.pin` fixes FLINK/FLUSS (pin-check [4/4]); `make pin-check` is the wired gate.
- [ ] Release record links this matrix to `docs/08_implementation/01-foundation.md`.

## Schema and storage rules

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Schema/storage status tracked (Design-ready; physical DDL version-validation blocked).
  - Source: 01-foundation.md -> "Status" (orig L373)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Design-ready; physical DDL remains version-validation blocked |
| Owner | Storage/Platform Team |
| Source requirements | `REQ-FLS-*`, `docs/02_requirements/04-data.md`, `DEC-001`, `DEC-005`, `DEC-018`, `DEC-020`, `DEC-021` |
| Migration posture | Pre-production clean break until live-money release |

### Purpose

#### Implementation checklist

- [x] Schema->validated-DDL control purpose realized (schema changes, retention, replay, lake offload, rollback controlled).
  - Source: 01-foundation.md -> "Purpose" (orig L382)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

This dossier defines how logical schemas become validated physical Fluss DDLs and how schema changes, retention, replay, lake offload, and rollback are controlled.

### Schema states

#### Implementation checklist

- [x] Schema state machine (PROPOSED->...->OBSERVED) enforced; DDL not executable authority until APPROVED for pinned matrix.
  - Source: 01-foundation.md -> "Schema states" (orig L386)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/SchemaState.java

```text
PROPOSED
  → APPROVED   (the only state carrying executable authority)
  → APPLYING
  → OBSERVED
  REJECTED     (failure exit; no authority)
```

A DDL under `code/01_platform/02_sql/ddl/` is not executable authority until it reaches `APPROVED` for the pinned Fluss/Flink matrix. The code enum is deliberately five states (`PROPOSED`/`APPROVED`/`APPLYING`/`OBSERVED`/`REJECTED`) — an earlier seven-state design with separate reconciliation and dialect/integration-validation states was simplified; the load-bearing rule is unchanged (`isExecutableAuthority()` == APPROVED only).

### Schema manifest

#### Implementation checklist

- [x] Machine-readable schema manifest implemented (schema_manifest_version, table_name, schema_version, ddl_path, ddl_sha256, table_kind, writer_owner, primary_key, bucket_key, retention_policy, lake_policy, compatibility_class, validated_matrix).
  - Source: 01-foundation.md -> "Schema manifest" (orig L400)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/SchemaManifest.java (+ SchemaManifestEntry.java)

Each release must generate a machine-readable manifest containing:

| Field | Meaning |
| --- | --- |
| `schema_manifest_version` | Version of the manifest format |
| `table_name` | Exact case-sensitive logical/physical table name |
| `schema_version` | Table contract version |
| `ddl_path` | Repository-relative DDL file |
| `ddl_sha256` | Checksum of normalized DDL |
| `table_kind` | LOG, KV, manifest, or immutable feed |
| `writer_owner` | Sole writer or column-group owners |
| `primary_key` | Physical key or `none` |
| `bucket_key` | Guaranteed non-null routing key |
| `retention_policy` | Live retention and extension rule |
| `lake_policy` | Offload/audit behavior |
| `compatibility_class` | Backward, forward, full, breaking, or clean-break |
| `validated_matrix` | Version compatibility record ID |

### DDL application contract

#### Implementation checklist

- [x] make ddl application contract (validate versions -> checksums -> parse -> apply to empty catalog -> inspect -> parity -> write/read/changelog/restore -> refuse readiness on mismatch).
  - Source: 01-foundation.md -> "DDL application contract" (orig L420)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/01_platform/04_scripts/ddl_apply.py (+ Makefile `ddl` target)

1. Validate exact Fluss/Flink versions.
2. Verify the schema manifest and DDL checksums.
3. Parse every DDL against the pinned dialect.
4. Apply to an empty acceptance catalog in deterministic order.
5. Inspect the effective schema/options from the runtime.
6. Run schema parity tests against logical requirements.
7. Run write/read/changelog/restore tests.
8. Record the applied manifest ID.
9. Refuse service readiness if required table/version differs.

`make ddl` must either execute this contract or fail closed with an explicit blocker. Printing stale paths is not an application workflow.

### Table categories and invariants

#### Implementation checklist

- [ ] Table-category invariants enforced (immutable LOG, immutable instruction feed, KV projection, gate/attempt state, manifest). _(partially done — LOG append-only enforced by Fluss + Ingestion; KvStateUpdateProtocol, SchemaManifest types exist; gate/state transition validation via GateTransitionValidator; live table-contract enforcement via TableContractValidator.validateCandleKvTable / validateSignalLogTable / validateSignalCurrentKvTable in SignalJob preflight; remaining: instruction-feed enforcement needs Executor, Phase 5)_
  - Source: 01-foundation.md -> "Table categories and invariants" (orig L434)
  - Design: Design-ready | Implementation: Implementing | Evidence: Untested | Live-money: Blocked
  - Location: code/common/model/ (GateState, AttemptPhase, GateTransitionValidator), code/common/schema/ (KvStateUpdateProtocol, SchemaManifest), code/02_services/01_ingestion/ (LOG appends)

| Category | Invariant |
| --- | --- |
| Immutable LOG | Append one platform event per delivery; no silent update/delete; duplicates explicit |
| Immutable instruction feed | Existing identity may not change content; mutation is a contract violation |
| KV projection | Keyed current state; source-version and transition rules reject stale/regressive writes |
| Gate/attempt state | Compare current epoch/phase before transition; every transition audited |
| Manifest | One approved manifest version defines active subscription state |

### Routing-key rule

#### Implementation checklist

- [x] Routing-key rule: every LOG table has a non-null routing identity.
  - Source: 01-foundation.md -> "Routing-key rule" (orig L444)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/RoutingKeyRule.java

Every LOG write must have a non-null routing identity. Nullable business fields must not be the only bucket key.

Proposed routing review:

| Table | Required routing identity | Rationale |
| --- | --- | --- |
| `raw_table_1` | `instrument_token` after validation | Per-instrument processing order |
| `feature_candles_15s` | `instrument_token` | Per-instrument window history |
| `Signal_Candidates` | `candidate_id` (KV primary key, R-084 — was LOG) → **RE-SCOPED 2026-08-13: back to LOG, routing key `instrument_token`** (immutable candidate audit; new `Signal_Candidates_current` KV takes PK `(instrument_token)` for latest/active per instrument) | Strategy locality |
| `Ranking_Results` | ~~`evaluation_id`~~ (R-136 — was `candidate_id`) — **REMOVED 2026-08-15 (CHG-005)** | ~~Avoid cross-instrument/null ambiguity~~ |
| `Fills` | `postback_event_id` when broker ID may be absent | Every delivery is routable |
| `Execution_Audit` | `audit_event_id` | Gate-only events may lack instruction ID |
| `Portfolio_Reservations` | ~~`reservation_id`~~ — **REMOVED 2026-08-15 (CHG-005)** | ~~Authoritative reservation state~~ |
| `Postback_Projection_Ledger` | `postback_event_id` | Recovery workflow state |

> Live in dev: `Postback_Projection_Ledger 705` (Fluss table id 705, 2026-08-13 dev cluster — see `02-schema-storage.md` lake-state note).
| `Safety_Halt_Requests` | `halt_request_id` | Durable control event identity |
| `Postback_Quarantine` | `quarantine_id` | Missing broker ID is expected |

Final physical keys remain evidence-gated by pinned Fluss distribution semantics.

### Immutability protocol

#### Implementation checklist

- [x] Immutability protocol (canonical content hash; same id+hash = duplicate, same id+diff hash = violation).
  - Source: 01-foundation.md -> "Immutability protocol" (orig L465)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/ImmutabilityProtocol.java

For each immutable entity, calculate a canonical versioned content hash:

```text
same identity + same hash    → idempotent duplicate evidence
same identity + different hash → contract violation
new executable content       → new identity with supersedes relation
```

Writers must persist or query enough state to detect mutation. LOG-table comments and `NOT ENFORCED` keys do not enforce this protocol.

### KV state update protocol

#### Implementation checklist

- [x] KV state-update protocol (duplicate/older/regression/conflict checks -> UNKNOWN + quarantine + halt).
  - Source: 01-foundation.md -> "KV state update protocol" (orig L477)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/KvStateUpdateProtocol.java

A projection update includes:

- Aggregate key
- Source event ID
- Source version/timestamp
- Previous expected state/version when supported
- New state
- Transition reason
- Schema/projection version

The projector must check:

1. Duplicate source event.
2. Older source version.
3. Invalid state transition.
4. Terminal-state regression.
5. Conflicting evidence at equal version.

Conflict yields `UNKNOWN`, quarantine/audit, alert, and affected order-path halt. `partial_update` merges columns but does not replace these checks.

### Schema evolution classes

#### Implementation checklist

- [x] Schema evolution classes handled (additive / behavioral / state-incompatible / wire-incompatible / breaking clean-break).
  - Source: 01-foundation.md -> "Schema evolution classes" (orig L499)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/SchemaEvolutionClass.java

| Class | Rule |
| --- | --- |
| Additive compatible | New optional field with default/null semantics; readers tolerate unknown field |
| Behavioral compatible | Same physical schema but changed algorithm/config version; replay comparison required |
| State incompatible | Flink serializer/state shape changes; savepoint migration or clean restart required |
| Wire incompatible | Protocol/event schema cannot be read by old consumer; ordered deployment required |
| Breaking clean-break | Pre-production only; destructive approval, reset, replay, and rollback evidence required |

Every change records producer-first/consumer-first order, dual-read/write needs, savepoint impact, lake synchronization, and rollback readability.

### EOD controller and offload gate

#### Implementation checklist

- [x] EOD controller state machine + offload gate (PENDING->...->VERIFIED; retention extension; no source expiry until VERIFIED).
  - Source: 01-foundation.md -> "EOD controller and offload gate" (orig L511)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/EodControllerState.java

The EOD controller is a named service or scheduled job owning manifest creation, verification, retry/backoff, retention extension, expiry protection, and manual reconciliation. Source data for a trading day SHALL not expire while the manifest is unverified, retryable, or under reconciliation.

Each table/day offload record contains:

- Trading date
- Table and schema version
- Source offset/range
- Row and byte counts
- Source and target hashes/checksums
- Iceberg snapshot/commit ID
- Verification state
- Retry count and next retry
- Earliest allowed source expiry

State machine:

```text
PENDING → WRITING → COMMITTED → VERIFYING → VERIFIED
                    ↘ FAILED_RETRYABLE
                    ↘ FAILED_MANUAL
```

Source data cannot expire unless state is `VERIFIED`, and at least three complete trading days remain live. Unverified or retryable state extends retention through a tested control mechanism; a fixed DDL TTL comment is insufficient.

### Approved audit-retention boundary

#### Implementation checklist

- [ ] Approved audit-retention boundary (one-year minimum or longer policy, encrypted immutable audit copy, key management, access audit, deletion/legal-hold, periodic reconstruction test). _(partially done 2026-08-15: the audit core is implemented and unit-tested — `AuditHashChain`, `AuditDeletionControl`, and `audit_r2.py` provisioning/validation remain applicable.)_
  - Source: 01-foundation.md -> "Approved audit-retention boundary" (superseded 2026-08-19 by DEC-043)
  - Design: Design-ready | Implementation: Implementing | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/audit/ (`AuditHashChain`, `AuditDeletionControl`; tests code/common/src/test/java/com/trading/common/audit/) + code/01_platform/04_scripts/audit_r2.py (tests code/01_platform/04_scripts/tests/test_audit_r2.py)

Short operational Fluss TTL and policy-controlled audit retention are separate contracts. Money-moving events must be copied to encrypted immutable audit storage with:

- Verified manifest — manifest format + per-event content hashes defined (`AuditHashChain.Manifest`, unit-tested); real per-day manifests still need the EOD controller (SCH-23, Phase 6)
- Encryption and key-management evidence — not yet; R2 storage is at-rest encrypted, key-rotation evidence pending
- S3 versioning/lifecycle policy — lifecycle preserved/applied by `audit_r2.py provision` (existing rules never clobbered). **R2 does NOT implement the S3 Object Lock or S3 versioning APIs (live-verified 2026-08-14: `PutBucketVersioning` and `ListObjectVersions` → NotImplemented); R2's WORM-equivalent is 'bucket locks' — prefix retention rules (duration / until-date / indefinite) via the Cloudflare dashboard/Wrangler/API.** An indefinite bucket-lock rule on the audit prefix satisfies the NFR 3.4.1 WORM control on R2 (provisioning needs a Cloudflare API token, not the S3-compat keys); `audit_r2.py validate` proves bucket existence, lifecycle, and object I/O via the S3 API and reads bucket-lock state via the Cloudflare API when a token is configured (evidence `logs/audit-r2/20260814T182520Z-audit-r2-evidence.json`, PASS 2026-08-14)
- Access audit — not yet
- Approved deletion/legal-hold behavior — `AuditDeletionControl` implemented 2026-08-14 (approved retention-policy change + legal-hold release + single authorized operator `saurabh` (DEC-044); every attempt emits an immutable deletion-evidence event)
- Periodic reconstruction test — removed from scope by user decision 2026-08-14; hash-chain verification is covered by `AuditHashChainTest`

### Test requirements

**Gate step 16 conflict note (L3-2, 2026-09-29).** The 2026-09-18 certification-hygiene decision
("keep step 16 and its selection exactly as they are … no line of `run-monday-gates.sh` is touched",
`docs/plans/2026-09-18-certification-hygiene.md:246-251`) is **partially superseded**: step 16's
existing suite, `-Dtest` selection, exclusions and log line stay byte-identical, and L3-2 appends a
second targeted invocation (`COMPUTE_INT_TEST_P6=true`, `FLUSS_BOOTSTRAP`/`FLUSS_BOOTSTRAP_SERVERS`,
`SignalJobOperatorUidTest,TradeDecisionsSinksUidTest`) guarded by `require_class_clean`
(`N>0, Failures: 0, Errors: 0, Skipped: 0`). The decision's core — no renumbering, no `--steps`
change, no change to the default selection — stands; what changes is that the restore-graph UID pins
are no longer *written but not exercised* in a default `make gate` (the gap the decision itself
named). See CHG-421.

#### Implementation checklist

- [x] Schema/storage test suite implemented (DDL parse/apply, parity, routing skew, immutable dup/mutation, KV stale, changelog/partial-update, checkpoint/replay, clean-break, EOD, 7-yr reconstruction).
  - Source: 01-foundation.md -> "Test requirements" (orig L548)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/test/java/com/trading/common/schema/ (unit: RoutingKeyRule/KvStateUpdateProtocol/ImmutabilityProtocol/SchemaState/EodControllerState/SchemaManifestSerialization/SchemaComplianceFullSuite; integration stubs: fluss/CompatFlussIntegrationTest, tagged `integration`)

- DDL parse/apply for pinned matrix.
- Effective schema/options inspection.
- Schema parity for every required field.
- Non-null routing and bucket-skew tests.
- Immutable duplicate/mutation tests.
- KV stale/regressive/conflict tests.
- Changelog and partial-update tests.
- Checkpoint/replay compatibility tests.
- Clean-break reset and replay tests.
- EOD failure/retry/expiry-protection tests.
- Approved-policy audit reconstruction simulation covering at least one year or the approved longer period.

### Completion checklist

#### Implementation checklist

- [x] Completion checklist satisfied (manifest format; DDL checksums; stale paths removed; non-null routing; immutability/stale-update protocols; pinned dialect tests). Pinned dialect tests = the dialect-pinned harnesses in the unit/integration suites; retention extension (L853) and audit-lake evidence (L854) remain open below.
  - Source: 01-foundation.md -> "Completion checklist" (orig L562)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- [x] Schema manifest format is implemented.
- [x] All DDLs have checksums and compatibility classes. ✓ 21/21 manifest entries carry `ddl_sha256` + `compatibility_class=UNKNOWN` + `validated_matrix` (candle/signal → VM-FLUSS-CONN-007, else VM-FLUSS-SRV-005); emitted by `ddl_apply.py` (single writer, `matrix_boundary()` helper), enforced by its post-write contract check (exit 2 on stale manifest) + `SchemaComplianceFullSuiteTest.committedManifestCarriesChecksumsAndCompatibilityClasses` (common suite 112 tests, 2026-08-13). UNKNOWN is honest: the matrix is PINNED_AWAITING_EVIDENCE — dev-live ≠ production-proven.
- [x] Stale DDL paths are removed from application workflow.
- [x] Pinned dialect tests pass. ✓ dialect-pinned harnesses green on the pinned versions (unit suites 156/193/188 — re-measured 2026-08-15 (common 156 after the audit-core additions, the SCHEMA-AUDIT-001 reconstruction-simulation tests, the COMPAT-FLUSS-005 composite-PK matrix test, and the `DdlApplyToolStatusTest` apply-status/limitation-prediction tests; ingestion 193 at 2026-08-15 (superseded — re-measured 2026-09-18: 285/0/8 after the M1–M5 hardening batches); the instrument-manifest-writer tests — ING-SCHEMA-002 unit + ING-INT-004 live composite-PK proof; compute 188 at 2026-08-15 (superseded — re-measured 2026-09-18: 419/0/22 after the forming-bar test batches); gated battery 10/10)); COMPAT-FLUSS-001/003 + COMPAT-FLINK-002 + COMPAT-FLUSS-005 live-evidence integration tests added 2026-08-15 (env-gated on `FLUSS_BOOTSTRAP`; live run 168 tests / 0 failures / 1 skip — the skip is the tiered-storage half of SCHEMA-AUDIT-001); missing savepoint/restore/rescale capability now covered — `SignalJobSavepointRestoreIntegrationTest` green (strict restore + 2× rescale, real dedup state); pins enforced by pin-check [3/4] external-SNAPSHOT ban + [4/4] FLINK_VERSION/FLUSS_VERSION.
- [x] Every table has a non-null routing strategy.
- [x] Immutability and stale-update protocols are implemented and tested. ✓ code `code/common/.../schema/ImmutabilityProtocol.java` + `KvStateUpdateProtocol.java` (library contracts, unit-tested: `ImmutabilityProtocolTest`, `KvStateUpdateProtocolTest`, `SchemaComplianceFullSuiteTest`). Candle KV stays LWW upsert by design (replay converges — no version column); runtime consultation wires in with the Executor-era writers (Phase 5).
- [ ] Retention extension is executable, not just documented — VERIFIED BOUNDARY (2026-08-13): NOT live-alterable in Fluss 0.9.1. `Admin.alterTable` rejects `table.log.ttl` ("The option 'table.log.ttl' is not supported to alter yet"); `log.retention.ms` and `comment` alters pass validation but are silent no-ops (`getLogTTLMs()` unchanged 604,800,000 across 8 s of polling); only `table.datalake.enabled` verifiably applies (matches the LakeDisable 2026-08-12 precedent). TTL is set at CREATE time only — DEC-018 "extends while EOD unverified" needs a table rewrite (new TTL + migrate/backfill) or a Fluss upgrade. Evidence: `logs/tracker-14/retention-l853-verification-20260813.md` + `RetentionAlterProbe.java` / `AlterWhitelistProbe.java`.
- [ ] Audit-lake retention and reconstruction evidence exist.

## Shared safety rules

<!-- markdownlint-disable MD013 -->

### Status

#### Implementation checklist

- [x] Safety-rules status tracked (implementation-ready; runtime validation pending).
  - Source: 01-foundation.md -> "Status" (orig L579)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

| Field | Value |
| --- | --- |
| Status | Implementation-ready; runtime validation pending |
| Owner | Platform Team; component owners implement locally |
| Sources | `docs/01_project/02-system-context.md`, `docs/02_requirements/04-data.md`, `docs/02_requirements/05-interfaces.md`, `DEC-005`–`DEC-020` |

### Identity invariant

#### Implementation checklist

- [x] Typed identity model implemented (candidate_id, instruction_id, execution_attempt_id, client_order_ref, broker_order_id, trade_context_id, position_id, postback_event_id, action_id, halt_request_id; ~~reservation_id~~ — **REMOVED 2026-08-15, CHG-005**); generic order_id prohibited. Scope identities (account_scope_id, ~~portfolio_id~~ — REMOVED 2026-08-15 CHG-005, execution_partition_id) enforced with isolation tests.
  - Source: 01-foundation.md -> "Identity invariant" (orig L587)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/identity/IdentityModel.java

The following identities are never interchangeable:

| Identity | Owner | Meaning |
| --- | --- | --- |
| `candidate_id` | Signal job | One detected setup/audit record |
| `instruction_id` | Signal job | One immutable execution request |
| `execution_attempt_id` | Executor | One broker submission attempt |
| `client_order_ref` | Executor | Broker-facing attempt reference |
| `broker_order_id` | Broker/Action Capture | Broker-authoritative order |
| `trade_context_id` | Signal/Execution | Entry and position-management grouping |
| `position_id` | Position projector | Fill-derived exposure aggregate |
| `postback_event_id` | Action Capture | One received postback delivery |
| `action_id` | Future Babysitter | One immutable position action |
| ~~`reservation_id`~~ | ~~Signal job~~ | ~~Portfolio capacity reservation~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `halt_request_id` | Authorized component | Durable safety-halt request |

#### Scope identities

| Scope | Purpose | Carried by |
| --- | --- | --- |
| `account_scope_id` | Broker/account isolation boundary | Gates, attempts, mappings, positions, lifecycle, halt requests, audit |
| ~~`portfolio_id`~~ | ~~Ranking/reservation/capacity boundary~~ | ~~Reservations, candidate evaluation, instruction context~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `execution_partition_id` | Fenced Executor ownership boundary | Execution gate, fencing token, attempt state |

A generic `order_id` is prohibited in new requirements, DDL, code, logs, and tests.

### Ownership matrix

#### Implementation checklist

- [x] Ownership matrix encoded (12 rows: sole owner / readers / prohibited owners per data/behavior); runtime consultation of the matrix is pending — enforcement helpers exist in `OwnershipMatrix.java` but live writers are not yet gated on them. The helpers fail closed: a null rule or actor denies, the open "Authorized components" row denies in `canWrite` (callers check `isOpenOwnerRow` and authorize themselves), a rule holds immutable copies of its sets, and `logRule` is least-privilege (2026-09-14, CHG-139).
  - Source: 01-foundation.md -> "Ownership matrix" (orig L615)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/ownership/OwnershipMatrix.java

| Data/behavior | Sole owner | Readers | Prohibited owners |
| --- | --- | --- | --- |
| Raw packet/decode | Ingestion | Signal, audit/offload | Strategy, Executor |
| Candle/forming-bar state | Signal job | Business Logic (**/ranking REMOVED 2026-08-15, CHG-005**) | Ingestion, Executor |
| Candidates | Signal job | Audit | Action Capture |
| Order lifecycle | Action Capture | Executor, operations | Signal, Babysitter |
| Position aggregate | Position projector | Babysitter, Executor | Strategy, raw ingestion |
| Order gate/attempt/mapping/audit | Executor | Action Capture/operations as needed | Signal, Babysitter (doc said "Signal, Executor" — self-contradictory, the sole owner may not be prohibited; interpreted 2026-09-14, CHG-139) |
| Postback audit/lifecycle | Action Capture | Executor, operations | Signal, Babysitter |
| ~~Portfolio reservations~~ | ~~Signal job~~ | ~~Executor, reconciliation~~ — **REMOVED 2026-08-15 (CHG-005)** | ~~N/A~~ |
| Projection ledger | Action Capture | Recovery scanner | N/A |
| Safety halt requests | Authorized components | Executor | N/A |
| Broker REST call | Executor via Arrow REST | Broker | Every other component |
| Position actions | Babysitter after MVP | Executor | Arrow REST / direct callers |

The 12 rows above are code-pinned (`OwnershipMatrix.java` — the row count and rows are enforced by `OwnershipMatrixTest`; do not add rows without a matching code+test change). The **fingerprint dedup set** (a Fluss KV state table under DEC-038) is Signal-job-owned state: the Signal job is its sole writer, the Signal job's bounded working cache reads it, storage/audit may read it, and Ingestion/Executor may not write it. It is documented here as a note rather than a new matrix row because adding a row requires the coordinated code+test change that the state-ownership implementation stage performs (04-signal-job.md DEC-038 plan).

### Delivery semantics

#### Implementation checklist

- [x] Delivery semantics enforced (at-least-once boundaries; exactly-once only within tested Flink/Fluss boundary).
  - Source: 01-foundation.md -> "Delivery semantics" (orig L632)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/DeliverySemantics.java

| Boundary | Contract |
| --- | --- |
| Broker stream → Ingestion | At-least-once; possible gaps; protocol evidence required |
| Ingestion → raw LOG | At-least-once; preserve every accepted packet |
| Compute dedup | Bounded best-effort fingerprinting |
| Flink state/sink | Exactly-once only within tested version-pinned boundary |
| Multiple Fluss outputs | Partial visibility unless a test proves a transaction boundary |
| Decision/action → Executor | At-least-once; immutable identity/request hash guard |
| Executor → broker | At-least-once or unknown; reconcile before retry |
| Postback projections | At-least-once input; idempotent/versioned projection |

### Ordering invariant

#### Implementation checklist

- [x] Ordering invariant (event-time per-instrument, deterministic fingerprint tie-break, no broker-seq assumption, no global ordering from bucket affinity).
  - Source: 01-foundation.md -> "Ordering invariant" (orig L645)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/OrderingInvariant.java

- Per-instrument event processing uses event time and deterministic fingerprint tie ordering.
- Broker sequence ordering is not assumed.
- Per-order projection uses source version/timestamp and status precedence.
- Per-position projection uses fill-derived source version and duplicate event identity.
- Gate transitions use serialized epoch compare-and-set.
- Global ordering is never inferred from Fluss bucket affinity.

### Time invariant

#### Implementation checklist

- [x] Time invariant (event_time / receive_time / persist_start / persist_ack / processing_time / schema_version; monotonic duration clock; UTC correlation).
  - Source: 01-foundation.md -> "Time invariant" (orig L654)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/TimeInvariant.java

Every event carries:

```text
event_time        = verified broker/external event time
receive_time      = local receipt time
persist_start     = local monotonic/UTC append start
persist_ack       = local append acknowledgement time
processing_time   = local processing timestamp
schema_version    = event schema
```

Duration measurements use a monotonic clock. Cross-service correlation uses UTC. Clock offset is observable and readiness-affecting when outside policy.

### Immutability invariant

#### Implementation checklist

- [x] Immutability invariant (id+hash duplicate/violation; changed params -> new id + supersession; execution state never in strategy decision fields).
  - Source: 01-foundation.md -> "Immutability invariant" (orig L669)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/schema/ImmutabilityProtocol.java

```text
immutable identity + same canonical hash
  → duplicate delivery; audit and suppress duplicate effect

immutable identity + different canonical hash
  → contract violation; quarantine and halt safety-relevant flow

changed executable parameters
  → new identity and explicit supersession
```

Execution-owned state is never written into strategy-owned immutable decision fields.

### Failure invariant

#### Implementation checklist

- [x] Failure invariant (behavior defined for before/during/ack/timeout/duplicate/restart/stale/corrupt; ambiguity -> UNKNOWN).
  - Source: 01-foundation.md -> "Failure invariant" (orig L684)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/FailureInvariant.java

Every side effect must define behavior for:

- Before side effect
- During side effect
- Successful side effect before acknowledgement persistence
- Timeout/disconnect
- Duplicate delivery
- Restart after partial completion
- Stale/out-of-order input
- Corrupt or unavailable durable state

Ambiguity is represented explicitly as `UNKNOWN` and never treated as rejection or success by assumption.

### Order safety invariant

#### Implementation checklist

- [x] Order-safety invariant (pre-call gate checks; any fail -> no call; UNKNOWN -> HALTED).
  - Source: 01-foundation.md -> "Order safety invariant" (orig L699)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/invariants/OrderSafetyGate.java

Before every money-moving call:

```text
valid immutable instruction/action
→ no unresolved duplicate/attempt
→ current gate state is ENABLED
→ current gate epoch matches attempt
→ active Executor fencing lease is valid
→ request hash/client reference are durably persisted
→ Arrow REST contract is ready
→ call is issued
```

Any failed check prevents the call. Unknown result transitions the affected gate to `HALTED`.

### Configuration invariant

#### Implementation checklist

- [x] Configuration invariant (static / deployment / secret / evidence-gated split; missing required value -> not ready, no unsafe default).
  - Source: 01-foundation.md -> "Configuration invariant" (orig L716)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/config/PlatformConfig.java

Configuration is divided into:

1. **Static build values:** exact library/image versions and schema version.
2. **Deployment values:** topology, resource, checkpoint, retention, and endpoint settings.
3. **Secret references:** credentials and keys, never secret contents in documentation.
4. **Evidence-gated values:** broker fields, limits, status mappings, idempotency, and protocol behavior.

A missing required value makes the affected component not ready. It must not fall back to an unsafe default.

### Observability invariant

#### Implementation checklist

- [x] Observability invariant (structured logs, contract-proving metrics, health != business state, immutable audit, redaction; OpenObserve outage cannot authorize orders/erase audit).
  - Source: 01-foundation.md -> "Observability invariant" (orig L727)
  - Design: Design-ready | Implementation: Implemented | Evidence: Untested | Live-money: Blocked
  - Location: code/common/src/main/java/com/trading/common/observability/

Every component emits:

- Structured logs with service/instance/version/correlation IDs.
- Metrics sufficient to prove its contract.
- Health dimensions separate from business state.
- Immutable audit for safety and money-moving decisions.
- Redaction of credentials and sensitive raw payloads.

OpenObserve outage cannot authorize orders and cannot erase durable execution audit.

### Review checklist

#### Implementation checklist

- [x] Review checklist items tracked (owner per state/table, named identities, retry/idempotency rules, timestamp semantics, UNKNOWN outcomes, connector tests, audited safety transitions, readiness explanations).
  - Source: 01-foundation.md -> "Review checklist" (orig L739)
  - Design: Design-ready | Implementation: Implemented | Evidence: N/A | Live-money: N/A
  - Location: docs/08_implementation/01-foundation.md

- [x] New code has one documented owner per state/table. ✓ code `code/common/.../ownership/OwnershipMatrix.java` (12 rows: raw packets→Ingestion, candles→Signal, lifecycle→Action Capture, gate/attempt→Executor, etc.)
- [x] Every identity is named explicitly. ✓ code `code/common/.../identity/IdentityModel.java` (15 typed identities: InstructionId, ClientOrderRef, BrokerOrderId, InstrumentToken, etc.; generic `order_id` prohibited)
- [x] Every retry has a duplicate/idempotency rule. ✓ Foundation: ImmutabilityProtocol, KvStateUpdateProtocol (duplicate/stale conflict detection). Ingestion: RawTickWriter retry loop with exponential backoff (100/200/400 ms, up to 3 attempts), FATAL failures halt immediately, UNCERTAIN outcome on timeout (no silent assumption of success). Raw ingestion does NOT deduplicate fingerprints — the Flink Signal job performs logical dedup (compute-side; the durable dedup set is Fluss-authoritative under DEC-038; see R-298). Executor-layer retry pending Phase 5.
- [x] Every timestamp has defined semantics. ✓ code `code/common/.../invariants/TimeInvariant.java` (6 canonical fields: event_time, receive_time, persist_start, persist_ack, processing_time, schema_version; monotonic clock)
- [x] Every uncertain outcome becomes explicit state. ✓ code `code/common/.../invariants/FailureInvariant.java` (Disposition.UNKNOWN — ambiguity never guessed, always explicit)
- [ ] Every cross-table assumption has a connector test. _(partially done — Flink Signal job + fluss-flink-2.2 connector boundary proven live (SAFETY-INT-001); checkpoint/replay/idempotency battery green; not every cross-table assumption covered yet — Executor-era tables pending Phase 5)_
- [ ] Every safety transition is audited. _(partially done — AuditLogger exists in foundation; full audit needs Executor + Action Capture)_
- [x] Every readiness result explains which dependency is missing. ✓ code `code/.../ingestion/health/HealthProbe.java` (`diagnostics()` returns per-dimension map: alive, fluss_ready, tracker_ready, broker_connected, subscription_complete, frame_recent, clock_offset_ms, clock_ok)
