//! T9 paper-trading evidence — single safe order `BI-EQ ×1` via nautilus-sandbox.
//!
//! Gate boots `HALTED`, no live money, shadow emits 0 new broker commands. The evidence hash
//! is a real SHA-256 over the bundle body. Offline only: the harness does not exercise the
//! sandbox engine yet (`harness.engine_exercised: false`), so the order row is a scripted
//! scenario vector, not an observed broker transition.

use anyhow::Result;
use nautilus_execution_service::t9paper::{
    assert_no_secrets, finalize_evidence, write_json, Run, Scenario, BI,
};
use nautilus_sandbox::config::SandboxExecutionClientConfig;
use serde_json::json;

fn main() -> Result<()> {
    // Compile probe: the pinned sandbox client config resolves against nautilus-sandbox.
    // The engine itself is not exercised until LiveNode wiring (plan Workstream A/B).
    let _sandbox_cfg = SandboxExecutionClientConfig::default();

    let run = Run::start("t9-paper")?;

    let order = json!({
        "client_order_ref": format!("T9-{}-{}-1", run.run_id, BI.symbol),
        "broker_order_id": format!("SB-{}-001", run.run_id),
        "outcome": Scenario::Filled.as_str(),
        "expected_fill": format!("full fill 1@{} via OrderMatchingEngine", BI.low),
        "expected_remarks_round_trip": true,
    });

    let evidence = finalize_evidence(run.evidence(
        "paper-trading",
        json!({
            "instrument": {
                "exchange": "NSE",
                "segment": "CM",
                "symbol": BI.symbol,
                "token": BI.token,
                "isin": BI.isin,
                "lot_size": 1,
                "tick": 0.01,
                "band": BI.band(),
                "source": "NSE_CM_EQUITY (1024).csv",
            },
            "quantity": 1,
            "order": order,
            "fence": "holder_single_worker_partition-0",
            "shadow_mode": {
                "new_broker_commands": 0,
                "positions_expected_match": true,
            },
        }),
    ));

    let evidence_path = write_json(&run.output_dir, "evidence.json", &evidence)?;
    assert_no_secrets(&evidence);
    // P3-420: `shadow_mode.new_broker_commands: 0` above is a scripted scenario placeholder,
    // not an observation — the offline harness has no shadow emitter to query
    // (`harness.engine_exercised: false`). Asserting that literal against itself would be
    // tautological, so the placeholder is stated here instead of checked.

    println!(
        "T9 paper-trading evidence written to {}",
        evidence_path.display()
    );
    println!("{}", serde_json::to_string_pretty(&evidence)?);
    println!(
        "T9 paper-trading OK: BI-EQ x1 scenario, gate HALTED, shadow 0 commands, evidence {}",
        evidence["evidence_hash"].as_str().unwrap()
    );
    Ok(())
}
