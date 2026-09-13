//! T9 full-25 evidence — 25 orders plus UNKNOWN reconciliation templates + audit offload/restore.
//!
//! Extends `t9_paper_25` with: reconciliation snapshots for the 3 UNKNOWN slots (templated,
//! `captured: false`, `expected_delay_ms: 220`), encrypted audit offload with a real SHA-256
//! integrity root, restore verification, and deletion-governance (legal hold / 1y policy).
//!
//! Offline only: `harness.engine_exercised` is false; snapshots are templates and the audit
//! chain hashes the scenario vector content, not live broker transitions.

use anyhow::Result;
use nautilus_core::{
    datetime::{add_n_years_nanos, unix_nanos_to_iso8601},
    UnixNanos,
};
use nautilus_execution_service::t9paper::{
    assert_no_secrets, audit_offload_and_restore, count_outcome, finalize_evidence, order_json,
    reconciliation_snapshot, scenario_for_idx, shadow_position, write_json, Run, Scenario,
    PAPER_INSTRUMENTS, RETENTION_POLICY,
};
use nautilus_sandbox::config::SandboxExecutionClientConfig;
use serde_json::{json, Value};

fn main() -> Result<()> {
    let _sandbox_cfg = SandboxExecutionClientConfig::default();

    let run = Run::start("t9-full-25")?;

    let orders: Vec<Value> = (0..PAPER_INSTRUMENTS.len())
        .map(|idx| order_json(&run.run_id, idx))
        .collect();
    let unknowing: Vec<usize> = (0..PAPER_INSTRUMENTS.len())
        .filter(|&idx| scenario_for_idx(idx) == Scenario::Unknown)
        .collect();
    let reconciliation_snapshots: Vec<Value> = unknowing
        .iter()
        .map(|&idx| reconciliation_snapshot(idx))
        .collect();
    let shadow_positions: Vec<Value> = (0..PAPER_INSTRUMENTS.len()).map(shadow_position).collect();
    let audit = audit_offload_and_restore(&run.output_dir, &run.run_id, &orders)?;

    let filled = count_outcome(&orders, Scenario::Filled);
    let partial = count_outcome(&orders, Scenario::PartialFill);
    let rejected = count_outcome(&orders, Scenario::Rejected);
    let unknown = count_outcome(&orders, Scenario::Unknown);
    let disconnect = count_outcome(&orders, Scenario::Disconnect);

    // P3-183: the retention deadline belongs to *this* run, not to a frozen literal.
    let blocked_until = deletion_blocked_until(run_start_unix_seconds(&run))?;
    let deletion_governance = json!({
        "legal_hold": false,
        "deletion_blocked_until": blocked_until,
        "policy": RETENTION_POLICY,
    });

    let evidence = finalize_evidence(run.evidence(
        "full-25",
        json!({
            "instruments": PAPER_INSTRUMENTS.len(),
            "orders": orders,
            "summary": {
                "total": PAPER_INSTRUMENTS.len(),
                "filled": filled,
                "partial_fill": partial,
                "rejected": rejected,
                "unknown_timeout": unknown,
                "disconnect": disconnect,
                "filled_or_partial": filled + partial,
                "shadow_new_broker_commands": 0,
            },
            "shadow_positions": shadow_positions,
            "reconciliation_snapshots": reconciliation_snapshots,
            "reconciliation_summary": {
                "unknowns": unknown,
                "snapshots_templated": reconciliation_snapshots.len(),
                "captured": false,
                "all_mismatch_blocks_release": true,
                "no_auto_retry": true,
            },
            "audit_offload": audit["audit_offload"],
            "audit_restore": audit["audit_restore"],
            "deletion_governance": deletion_governance,
            "checks": [
                "scenario vectors: 10 FILLED / 5 PARTIAL / 5 REJECT / 3 UNKNOWN / 2 DISCONNECT (expectations, not observed)",
                "UNKNOWN reconciliation snapshots are templates (captured: false, expected_delay_ms 220)",
                "audit offload encrypted with real SHA-256 integrity root; restore verified",
                "legal hold / 1y deletion governance applied",
                "shadow compare broker/Nautilus/Fluss all expected_match, 0 new broker commands",
                "engine_exercised: false - real sandbox round-trip awaits LiveNode wiring",
            ],
        }),
    ));

    let evidence_path = write_json(&run.output_dir, "evidence.json", &evidence)?;
    assert_no_secrets(&evidence);
    assert_eq!(unknown, 3, "UNKNOWN rows must be exactly 3");
    assert_eq!(
        reconciliation_snapshots.len(),
        3,
        "reconciliation templates must cover the 3 UNKNOWN slots"
    );
    assert_eq!(
        audit["verified"], true,
        "audit restore must verify integrity_root"
    );
    assert!(
        evidence["shadow_positions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["expected_match"] == true),
        "all shadow positions must expect a match"
    );

    println!("T9 full-25 evidence written to {}", evidence_path.display());
    println!("{}", serde_json::to_string_pretty(&evidence)?);
    println!(
        "T9 full-25 OK: reconciliation templates 3 (UNKNOWN), audit offload/restore verified ({}), shadow 0, all expected_match, evidence {}",
        audit["audit_restore"]["integrity_root"].as_str().unwrap_or_default(),
        evidence["evidence_hash"].as_str().unwrap(),
    );
    Ok(())
}

/// Unix second the evidence run started at, read from the run id (`<kind>-<unix>` — `Run` exposes
/// no separate timestamp). An unrecognised run id fails loudly rather than freezing the retention
/// date (P3-183).
fn run_start_unix_seconds(run: &Run) -> u64 {
    run.run_id
        .rsplit('-')
        .next()
        .and_then(|secs| secs.parse().ok())
        .expect("run id must end with the unix start second")
}

/// Retention deadline for this bundle: the run start plus one year, as
/// `"<YYYY-MM-DD> (1y from run <YYYY-MM-DD>)"` (P3-183).
///
/// # Errors
///
/// Returns an error if the deadline leaves the representable timestamp range.
fn deletion_blocked_until(run_start_unix: u64) -> anyhow::Result<String> {
    let start = UnixNanos::from_seconds(run_start_unix);
    let blocked = add_n_years_nanos(start, 1)?;
    Ok(format!(
        "{} (1y from run {})",
        iso_date(blocked),
        iso_date(start)
    ))
}

/// `YYYY-MM-DD` (UTC) of a timestamp, taken from the crate's ISO 8601 formatter prefix.
fn iso_date(timestamp: UnixNanos) -> String {
    unix_nanos_to_iso8601(timestamp)[..10].to_string()
}

#[cfg(test)]
mod p3_183_tests {
    use super::*;
    use nautilus_execution_service::t9paper::evidence_root;

    /// The retention deadline is only observable in the bundle `main` writes, so this drives the
    /// harness end to end (it is offline: no broker round-trip, no network).
    #[test]
    fn bundle_retention_date_is_derived_from_the_run_timestamp() {
        let root = evidence_root();
        let pre_existing = run_dirs(&root);

        main().expect("harness run succeeds");

        let run_dir = run_dirs(&root)
            .into_iter()
            .find(|dir| !pre_existing.contains(dir))
            .expect("harness created exactly one run directory");
        let raw =
            std::fs::read_to_string(run_dir.join("evidence.json")).expect("evidence.json written");
        let evidence: Value = serde_json::from_str(&raw).expect("evidence is JSON");

        let run_unix: u64 = evidence["run_id"]
            .as_str()
            .expect("run id present")
            .rsplit('-')
            .next()
            .and_then(|secs| secs.parse().ok())
            .expect("run id ends with the unix start second");
        let start = UnixNanos::from_seconds(run_unix);
        let run_date = date_of(start);
        let expiry = date_of(add_n_years_nanos(start, 1).expect("one year is representable"));

        let blocked = evidence["deletion_governance"]["deletion_blocked_until"]
            .as_str()
            .expect("deletion_blocked_until is a string");
        assert!(
            blocked.starts_with(&expiry),
            "retention must expire one year after the run date ({expiry}), got `{blocked}`"
        );
        assert!(
            blocked.contains(&run_date),
            "retention must name the run date ({run_date}), got `{blocked}`"
        );

        let _ = std::fs::remove_dir_all(&run_dir);
        if pre_existing.is_empty() {
            let _ = std::fs::remove_dir(&root);
        }
    }

    /// Independent oracle for the rendered deadline: a fixed timestamp, no shared arithmetic.
    #[test]
    fn retention_deadline_is_one_year_after_the_run_date() {
        assert_eq!(
            deletion_blocked_until(0).expect("the epoch is representable"),
            "1971-01-01 (1y from run 1970-01-01)"
        );
    }

    /// `YYYY-MM-DD` prefix of the audited crate formatter, kept out of the assertions' path.
    fn date_of(timestamp: UnixNanos) -> String {
        unix_nanos_to_iso8601(timestamp)[..10].to_string()
    }

    fn run_dirs(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(root)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .collect()
            })
            .unwrap_or_default()
    }
}
