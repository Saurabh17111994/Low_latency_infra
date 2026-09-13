//! T9 paper-25 evidence — 25 orders via nautilus-sandbox scenario vectors.
//!
//! Scenario boxes: 10 FILLED / 5 PARTIAL_FILL / 5 REJECTED / 3 UNKNOWN (timeout) / 2 DISCONNECT.
//! UNKNOWN rows demand explicit reconciliation (no auto-retry); shadow compares
//! broker/Nautilus/Fluss expected quantities with 0 new broker commands while gate is HALTED.
//!
//! Offline only: `harness.engine_exercised` is false — rows are scripted expectations, not
//! observed transitions. The evidence hash is a real SHA-256 over the bundle body.

use anyhow::Result;
use nautilus_execution_service::t9paper::{
    assert_no_secrets, count_outcome, finalize_evidence, order_json, shadow_position, write_json,
    Run, Scenario, PAPER_INSTRUMENTS,
};
use serde_json::{json, Value};

/// The documented paper-25 scenario contract: 10 FILLED / 5 PARTIAL / 5 REJECTED /
/// 3 UNKNOWN / 2 DISCONNECT across 25 scripted rows (15 filled-or-partial).
const PAPER_25_CONTRACT: [(&str, u64); 7] = [
    ("total", 25),
    ("filled", 10),
    ("partial_fill", 5),
    ("rejected", 5),
    ("unknown_timeout", 3),
    ("disconnect", 2),
    ("filled_or_partial", 15),
];

/// Builds the `summary` block of the paper-25 evidence from the scripted order rows, failing
/// loudly when the matrix drifts from [`PAPER_25_CONTRACT`] (P3-182).
fn paper_25_summary(orders: &[Value]) -> Value {
    let filled = count_outcome(orders, Scenario::Filled);
    let partial = count_outcome(orders, Scenario::PartialFill);
    let rejected = count_outcome(orders, Scenario::Rejected);
    let unknown = count_outcome(orders, Scenario::Unknown);
    let disconnect = count_outcome(orders, Scenario::Disconnect);
    let summary = json!({
        "total": orders.len(),
        "filled": filled,
        "partial_fill": partial,
        "rejected": rejected,
        "unknown_timeout": unknown,
        "disconnect": disconnect,
        "filled_or_partial": filled + partial,
        "shadow_new_broker_commands": 0,
    });
    for (field, expected) in PAPER_25_CONTRACT {
        assert_eq!(
            summary[field].as_u64(),
            Some(expected),
            "paper-25 `{field}` must be {expected} - scenario matrix drifted from the documented contract"
        );
    }
    summary
}

/// First `checks` entry: the scenario distribution rendered from the counts actually present in
/// the bundle, so the prose cannot drift from the vectors it describes. Ported from the sibling
/// `t9_paper_25_full` bin, which computes the same line the same way (P3-423 there, D6 here).
fn scenario_distribution_line(
    filled: usize,
    partial: usize,
    rejected: usize,
    unknown: usize,
    disconnect: usize,
) -> String {
    format!(
        "scenario vectors: {filled} FILLED / {partial} PARTIAL / {rejected} REJECT / {unknown} UNKNOWN / {disconnect} DISCONNECT (expectations, not observed)"
    )
}

fn main() -> Result<()> {
    let run = Run::start("t9-paper-25")?;

    let orders: Vec<Value> = (0..PAPER_INSTRUMENTS.len())
        .map(|idx| order_json(&run.run_id, idx))
        .collect();
    let unknowns: Vec<Value> = orders
        .iter()
        .filter(|order| order["outcome"] == Scenario::Unknown.as_str())
        .cloned()
        .collect();
    let shadow_positions: Vec<Value> = (0..PAPER_INSTRUMENTS.len()).map(shadow_position).collect();

    let summary = paper_25_summary(&orders);
    let scenario_line = scenario_distribution_line(
        count_outcome(&orders, Scenario::Filled),
        count_outcome(&orders, Scenario::PartialFill),
        count_outcome(&orders, Scenario::Rejected),
        count_outcome(&orders, Scenario::Unknown),
        count_outcome(&orders, Scenario::Disconnect),
    );

    let evidence = finalize_evidence(run.evidence(
        "paper-25",
        json!({
            "instruments": PAPER_INSTRUMENTS.len(),
            "orders": orders,
            "summary": summary,
            "shadow_positions": shadow_positions,
            "unknowns": unknowns,
            "checks": [
                scenario_line,
                "UNKNOWN rows demand explicit reconciliation, no auto-retry",
                "shadow positions projected from scripted outcomes (expected_match)",
                "shadow: 0 new broker commands emitted while gate HALTED",
                "engine_exercised: false - real sandbox round-trip awaits LiveNode wiring",
            ],
        }),
    ));

    let evidence_path = write_json(&run.output_dir, "evidence.json", &evidence)?;
    assert_no_secrets(&evidence);
    assert_eq!(evidence["summary"]["shadow_new_broker_commands"], 0);
    assert!(
        evidence["shadow_positions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["expected_match"] == true),
        "all shadow positions must expect a match"
    );

    println!(
        "T9 paper-25 evidence written to {}",
        evidence_path.display()
    );
    println!("{}", serde_json::to_string_pretty(&evidence)?);
    println!(
        "T9 paper-25 OK: {} orders ({} fill/{} partial/{} reject/{} UNKNOWN/{} disconnect), shadow 0, all positions expected_match, evidence {}",
        evidence["summary"]["total"],
        evidence["summary"]["filled"],
        evidence["summary"]["partial_fill"],
        evidence["summary"]["rejected"],
        evidence["summary"]["unknown_timeout"],
        evidence["summary"]["disconnect"],
        evidence["evidence_hash"].as_str().unwrap(),
    );
    Ok(())
}

#[cfg(test)]
mod p3_182_tests {
    use super::*;

    fn scripted_orders() -> Vec<Value> {
        (0..PAPER_INSTRUMENTS.len())
            .map(|idx| order_json("p3-182-test", idx))
            .collect()
    }

    #[test]
    fn documented_matrix_is_accepted() {
        let summary = paper_25_summary(&scripted_orders());
        assert_eq!(summary["total"], 25);
        assert_eq!(summary["filled"], 10);
        assert_eq!(summary["partial_fill"], 5);
        assert_eq!(summary["rejected"], 5);
        assert_eq!(summary["unknown_timeout"], 3);
        assert_eq!(summary["disconnect"], 2);
        assert_eq!(summary["filled_or_partial"], 15);
    }

    #[test]
    #[should_panic(expected = "scenario matrix drifted")]
    fn drifted_filled_box_is_rejected() {
        // Simulates `scenario_for_idx`/`PAPER_INSTRUMENTS` drift: one FILLED slot
        // reclassifies as REJECTED (filled 10->9, rejected 5->6).
        let mut orders = scripted_orders();
        orders[0]["outcome"] = json!(Scenario::Rejected.as_str());
        paper_25_summary(&orders);
    }

    #[test]
    fn scenario_prose_follows_the_counts() {
        // D6: the first `checks` entry used to be a hardcoded literal, so it could drift from the
        // scripted vectors. It is now rendered from the counts, like the sibling full-25 bin.
        assert_eq!(
            scenario_distribution_line(1, 2, 3, 4, 5),
            "scenario vectors: 1 FILLED / 2 PARTIAL / 3 REJECT / 4 UNKNOWN / 5 DISCONNECT (expectations, not observed)"
        );
        assert_eq!(
            scenario_distribution_line(10, 5, 5, 3, 2),
            "scenario vectors: 10 FILLED / 5 PARTIAL / 5 REJECT / 3 UNKNOWN / 2 DISCONNECT (expectations, not observed)",
            "the documented contract must render exactly as the literal it replaces did"
        );
    }
}
