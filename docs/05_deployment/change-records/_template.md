# Change Record Template

File each change as `CHG-<N>.md` in this directory (one record per change),
per `01-foundation.md` "Change control" (orig L205). A change to any of
decision / requirement / DDL-schema / identity-event-contract /
Flink-state-checkpoint-contract / broker-Arrow-REST-adapter / gate-approval /
retention-offload / topology-secret requires a reconciliation record.

Every record MUST name the six required fields below inside a fenced
```text block — `docs-audit` C14 rejects any record missing one of them.
`change_record_id`, `scope`, `owner`, and `date` are recommended for
traceability but not validated.

```text
change_record_id: CHG-001
scope: <decision|requirement|ddl|identity-contract|state-contract|protocol-adapter|gate-behavior|retention|topology-secret> (comma-separated)
owner: <owner>
date: <UTC date>
affected_artifacts: <files, schemas, contracts, or DDLs the change touches — path-shaped tokens (known extensions) must resolve: repo-relative, record-dir-relative, bare name under docs/08_implementation/ or docs/, or a unique repo-wide basename>
compatibility_class: <COMPATIBLE | COMPATIBLE_WITH_LIMITATION | INCOMPATIBLE | UNKNOWN | NOT_APPLICABLE>
savepoint_impact: <none | migration | clean-restart | replay | other — describe state/savepoint/checkpoint effect>
test_updates: <test IDs added or changed, or "none" with justification>
rollback_behavior: <rollback path and state-readability>
plan_tasks: <plan or tracker task references — `tracker-<n>` must match docs/08_implementation/<n>-*.md; `.md` paths must resolve (repo-relative, record-dir-relative, or a bare dossier name); or `none`>
```

## Artifacts that no longer exist

`affected_artifacts` must resolve against the live tree. If an artifact the
change really touched has since been deleted, do not delete it from the record
and do not leave a dead path: annotate it in place with the commit that removed
it —

```text
affected_artifacts: code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FormingBarDetectionFunction.java (retired by 0f3e595), docs/08_implementation/01-foundation.md
```

C14 accepts a dead path only with that annotation, and verifies it: the named
commit must contain the path in its parent and not contain it itself. A renamed
file does not qualify — point at the live path instead. An annotation naming a
commit that did not remove the path fails the audit.
