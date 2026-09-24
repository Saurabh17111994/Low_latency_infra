# A1 ALTER probe - plan (proposed 2026-09-25)

**Item:** `A1` in `2026-09-22-fluss-1.0-native-adoption.md` - "ALTER for table options, not
drop + recreate". **Status: RUN COMPLETE 2026-09-25 — GREEN (P1-P4 all pass); adoption applied.**
Evidence: `logs/soak/a1-alter-probe-20260924T190226Z/` (the first attempt at 18:58Z had two script
bugs — double-counted error counts and a wrong log column — fixed and re-run at 19:02Z; the green run
is the clean one). Script: `logs/soak/a1-alter-probe.sh`.

## Goal

Settle, on the live dev stack, whether the rule "Fluss cannot ALTER table options; an options
change requires drop + create" can be replaced by ALTER for 1.0.0's alterable set - and re-probe
`table.datalake.enabled`, the option recorded as rejected.

**Falsifiers (the item's own WRONG IF):** an ALTER silently no-ops (accepted, behaviour
unchanged), or a rejection outside the already-documented `kv.ttl` / lake-table cases.

## Premise, re-verified offline (2026-09-25)

1. **The allowlist is real.** Fluss 1.0.0 `FlussConfigUtils.java:48-63` lists **14** alterable
   options; 0.9.1 `FlussConfigUtils.java:44-49` lists 3. `table.kv.ttl` and
   `table.replication.factor` are not on either list.
2. **The `table.datalake.enabled` rejection is conditional, not "create-only".** The upstream
   ITCase (`LakeEnableTableITCase.java`) defines the exact surface:

   | table state | expected | source |
   |---|---|---|
   | created **before** the cluster gained `datalake.format` | **refused by name** - "cannot be altered for tables that were created before the Fluss cluster enabled datalake" | `:119-127` - this is `raw_table_1`'s case (guard B) |
   | same, but a LOG table without bucket key | allowed | `:131-187` |
   | carries an explicit `table.datalake.format` | allowed | `:190-253` |
   | created after cluster format, cluster `datalake.enabled=false` | refused - "doesn't enable datalake tables" | `:301-350` |
   | **legacy cluster** (`datalake.format` set, `datalake.enabled` unset - our shape) | **allowed** | `:352-392` |

   So the "create-only" wording in `02-schema-storage.md:281-282`, `DdlText.java:25-30`, the
   runbook and `tiering-smoke.sh:122` is imprecise: the option **is** alterable, with a
   creation-context precondition. Separating those two ideas is `CLAIM-1`'s job and this probe's
   point.
3. **Already measured (repo evidence):** `table.replication.factor` refused by name;
   `table.kv.standby-replica.enabled` accepted but **inert** at rf=1
   (`logs/soak/a4-alter-probe-20260923T104939Z/`); `table.kv.ttl` refused (2026-09-22 probe).
4. **What the repo does today:** `DdlText.toDescriptor` forces `table.datalake.enabled=false`;
   `DdlApplyTool` parity asserts the forced-false carve-out (`:868-889`); the same assertion lives
   in `CompatFlussDdlParityIntegrationTest` (`:223-249`); the `Execution_Gate recreate` runbook
   (`01-runbooks.md:119-122`) encodes drop + create for option changes.

## Probe matrix (one window, dev stack only)

Fixture: scratch table `a1probe_<epoch>` (LOG, 3 buckets, no PK) in the Fluss database `fluss` -
the same "invisible to catalog-guard" trick `a4-alter-probe.sh` uses. No tablet-2 needed, no
production tables touched.

| # | case | action | expected | assertion |
|---|---|---|---|---|
| P1 | general ALTER | `ALTER TABLE ... SET ('table.log.ttl' = '9d')` (fixture created with `7d`) | accepted | ZK `/fluss/metadata/databases/fluss/tables/<t>` shows `table.log.ttl=9d`; post-ALTER write + digest read-back intact |
| P2 | the disputed option | `ALTER TABLE ... SET ('table.datalake.enabled' = 'true')` | accepted (legacy cluster, fresh table) | ZK shows `true`; optional: coordinator registers it for tiering |
| P3 | control - named refusal | `ALTER TABLE ... SET ('table.kv.ttl' = '9d')` | refused **by name** | error names `table.kv.ttl`; ZK config unchanged; table still readable |
| P4 | state-based close | post-ALTER insert + digest | fixture still works | digest matches the expected constant |

P2's expected result comes from the upstream ITCase on our exact cluster shape; the probe confirms
it on our build/image, because guard B's rejection (a pre-enablement table) was over-generalized
into "create-only".

**Cleanup / revert (scripted before the window opens):** drop the fixture; if the tiering service
picked it up, archive/remove the fixture's R2 prefix objects; re-run `catalog-guard.sh` and assert
volume count unchanged. Evidence -> `logs/soak/a1-alter-probe-<ts>/` (run.log, SQL, logs, ZK dumps,
catalog-guard). Script: `logs/soak/a1-alter-probe.sh`, mirroring `a4-alter-probe.sh`.

**Window:** ~10-15 min on the dev stack; prerequisites: stack up, `datalake.format` set and
`datalake.enabled` unset on the coordinator, no `make gate` run in flight, clean tree noted.

## Decision tree (after the run)

- **Green (P1-P3 as expected):** adopt A1 - correct the rule where it lives (`DdlText` comment +
  behaviour, `DdlApplyTool` parity carve-out + parity test, `02-schema-storage.md`, the runbook,
  the `tiering-smoke.sh` GUARD-B message) with the precise carve-outs: `kv.ttl` and
  `replication.factor` create-only; `datalake.enabled` recreate-only on tables created before
  cluster datalake enablement. Resolve `CLAIM-1`/`CLAIM-2`; unblock A2.
- **Amber (accepted but not in force):** adopt only the options proven in force; the lake flag and
  any inert option stay recreate-only; document the per-option truth.
- **Red (unexpected rejection or silent no-op outside the documented cases):** close A1 as `[-]`;
  drop + create stays the rule and A2 is blocked by the same finding.

Green does **not** rescue `raw_table_1` (a pre-enablement table - guard B stands); it settles the
rule and tells us whether the T7.2 tiering proof may run against a freshly created table.

## Out of scope

- A4's `table.replication.factor` / standby-replica (measured 2026-09-23; A4 parked on DR).
- A3's `table.auto-partition.*` (source-resolved in E2; A3 remains gated on the feed-replay
  question).
- Tiering behaviour itself (the smoke owns that); production / 4-VM anything.

## Approval checklist

1. **Approve this plan** (probe matrix + decision rules).
2. **Approve the window** - asked again immediately before the run; nothing touches the cluster
   before that.
3. **After the run:** approve the adoption edits (offline, with tests/CHG per protocol) - or the
   `[-]` closure. Marker flips at both A1 locations + `plan_tracker.py --write/--check` land with
   the adoption, not the probe.

## Result (2026-09-25, GREEN)

Two throwaway fixtures - a KV table (`a1probe_kv_*`, for P1/P3) and a LOG table (`a1probe_log_*`,
for P2) - both dropped after the run.

| # | outcome | evidence |
|---|---|---|
| P1 | `table.log.ttl` 7d → 9d accepted **and in force** (`table.log.ttl":"9d"` in ZK) | `20-alter-ttl.log`, `zk-kv-after-p1.txt` |
| P2 | `table.datalake.enabled` absent → true accepted **and in force** on a fresh table (legacy cluster shape) | `30-alter-datalake.log`, `zk-log-after-p2.txt` |
| P3 | control `table.kv.ttl` refused **by name** (`InvalidAlterTableException ... 'table.kv.ttl'`), ZK unchanged | `40-alter-kvttl.log` |
| P4 | post-ALTER write + digest intact on both fixtures (`36` = 3 rows summing 6) | `50-insert-kv.log` … `53-digest-log.log` |
| cleanup | both fixtures dropped; no R2 objects under the probe names; volumes 13→13; catalog-guard unchanged from pre-run | `run.log`, `catalog-guard.log` |

Falsifiers cleared: no silent no-op (P1/P2 values changed in ZK), no unexpected rejection (P3 is the
documented by-name refusal; nothing else refused). Adoption followed the green branch with one
correction: **no behaviour change** - `DdlText`/`DdlApplyTool` keep forcing the dev deviation to
`false` (dev policy), the edits are the corrected rule text in the six locations plus the tracker.
`raw_table_1` and the other pre-enablement tables remain recreate-only; `kv.ttl`,
`replication.factor`, and `merge-engine` remain create-only.
