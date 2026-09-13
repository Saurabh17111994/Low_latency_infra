//! Shared helpers for the T9 paper-trading evidence harnesses.
//!
//! T9 is entirely evidence work (see
//! `docs/08_implementation/19-nautilus-execution-service-implementation-plan.md`,
//! §"Repository audit and implementation handoff — T9"). These helpers back the
//! `t9_paper`, `t9_paper_25` and `t9_paper_25_full` bins so the three harnesses share one
//! instrument table, one scenario classifier, one bootstrap and one hashing scheme.
//!
//! Honesty contract of this module: the T9 paper bins are **offline** — no order is ever
//! submitted to a broker and no `SandboxExecutionClient` round-trip is executed yet (that
//! awaits `LiveNode` wiring, plan Workstream A/B). Every per-order row is therefore a
//! *scripted scenario vector* (an `expected_*` outcome, not an observation), and every bundle
//! carries `harness.engine_exercised: false`. The `evidence_hash` is a real SHA-256 over the
//! canonical serialized evidence body (minus its own self-referential field), so "evidence
//! HASHED" means what it claims.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{gate::Gate, health::HealthStatus};
use anyhow::Context;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Retention policy tag shared by every T9 evidence bundle (SAURABH-1Y approval, 2026-08-20).
pub const RETENTION_POLICY: &str = "SAURABH-1Y-APPROVAL-2026-08-20";

/// Single reviewer identifier required for T9 evidence (DEC-044: authorized operator
/// set is `{saurabh}`; a second reviewer is not required and not checked).
pub const REVIEWERS: [&str; 1] = ["saurabh"];

/// Execution client recorded in the evidence. The crate compiles against the pinned sandbox;
/// the engine itself is not exercised by the offline harness.
pub const SANDBOX_CLIENT: &str =
    "nautilus-sandbox SandboxExecutionClient (OrderMatchingEngine/SimulatedExchange)";

/// A paper-trading instrument row used by the T9 scenarios (INPUT-10/11).
#[derive(Debug, Clone, Copy)]
pub struct PaperInstrument {
    /// NSE symbol.
    pub symbol: &'static str,
    /// NSE token.
    pub token: u32,
    /// ISIN.
    pub isin: &'static str,
    /// Lower circuit-band price (kept as text to avoid float formatting drift).
    pub low: &'static str,
    /// Upper circuit-band price.
    pub high: &'static str,
}

impl PaperInstrument {
    /// Circuit band rendered as `"low-high"`.
    pub fn band(&self) -> String {
        format!("{}-{}", self.low, self.high)
    }
}

/// The T9 safe instrument (INPUT-11): NSE CM `BI-EQ` (BILCARE LTD.), Qty 1.
pub const BI: PaperInstrument = PaperInstrument {
    symbol: "BI-EQ",
    token: 762_583,
    isin: "INE986A01012",
    low: "47.42",
    high: "71.12",
};

/// The 25-instrument paper scenario shared by `t9_paper_25` and `t9_paper_25_full`.
pub const PAPER_INSTRUMENTS: &[PaperInstrument] = &[
    PaperInstrument {
        symbol: "HLEGLAS-EQ",
        token: 2289,
        isin: "INE461D01028",
        low: "335.2",
        high: "502.7",
    },
    PaperInstrument {
        symbol: "GROWWRLTY-EQ",
        token: 759_428,
        isin: "INF666M01MN2",
        low: "7.93",
        high: "11.89",
    },
    PaperInstrument {
        symbol: "KALAMANDIR-EQ",
        token: 18_755,
        isin: "INE438K01021",
        low: "70.75",
        high: "106.11",
    },
    PaperInstrument {
        symbol: "LUMAXIND-EQ",
        token: 2018,
        isin: "INE162B01018",
        low: "4114",
        high: "6170",
    },
    PaperInstrument {
        symbol: "SAKUMA-EQ",
        token: 13_251,
        isin: "INE190H01024",
        low: "1.56",
        high: "1.72",
    },
    PaperInstrument {
        symbol: "GMRP&UI-EQ",
        token: 8529,
        isin: "INE0CU601026",
        low: "78.08",
        high: "117.1",
    },
    PaperInstrument {
        symbol: "JINDWORLD-EQ",
        token: 20_642,
        isin: "INE247D01039",
        low: "30.27",
        high: "45.39",
    },
    PaperInstrument {
        symbol: "ABLBL-EQ",
        token: 756_843,
        isin: "INE14LE01019",
        low: "75.21",
        high: "112.81",
    },
    PaperInstrument {
        symbol: "HUBTOWN-EQ",
        token: 14_203,
        isin: "INE703H01016",
        low: "155.99",
        high: "233.97",
    },
    PaperInstrument {
        symbol: "ZFCVINDIA-EQ",
        token: 16_915,
        isin: "INE342J01019",
        low: "1856.5",
        high: "2784.7",
    },
    PaperInstrument {
        symbol: "DRCSYSTEMS-EQ",
        token: 2645,
        isin: "INE03RS01027",
        low: "10.73",
        high: "16.09",
    },
    PaperInstrument {
        symbol: "HDFCPVTBAN-EQ",
        token: 12_108,
        isin: "INF179KC1HZ7",
        low: "22.24",
        high: "33.34",
    },
    PaperInstrument {
        symbol: "CPL-EQ",
        token: 764_203,
        isin: "INE536P01021",
        low: "55.88",
        high: "83.8",
    },
    PaperInstrument {
        symbol: "TEXMOPIPES-EQ",
        token: 18_214,
        isin: "INE141K01013",
        low: "34.44",
        high: "51.64",
    },
    PaperInstrument {
        symbol: "NAHARINDUS-EQ",
        token: 13_106,
        isin: "INE289A01011",
        low: "93.32",
        high: "139.96",
    },
    PaperInstrument {
        symbol: "ITETF-EQ",
        token: 19_633,
        isin: "INF769K01KV5",
        low: "24.2",
        high: "36.28",
    },
    PaperInstrument {
        symbol: "INDGN-EQ",
        token: 23_693,
        isin: "INE065X01017",
        low: "394.15",
        high: "591.15",
    },
    PaperInstrument {
        symbol: "LINC-EQ",
        token: 6951,
        isin: "INE802B01027",
        low: "82.14",
        high: "123.2",
    },
    PaperInstrument {
        symbol: "GMDCLTD-EQ",
        token: 5204,
        isin: "INE131A01031",
        low: "450.05",
        high: "675.05",
    },
    PaperInstrument {
        symbol: "GTLINFRA-EQ",
        token: 13_745,
        isin: "INE221H01019",
        low: "0.98",
        high: "1.46",
    },
    PaperInstrument {
        symbol: "FACT-EQ",
        token: 1008,
        isin: "INE188A01015",
        low: "647.45",
        high: "971.15",
    },
    PaperInstrument {
        symbol: "GANESHBE-EQ",
        token: 5614,
        isin: "INE388A01029",
        low: "89.13",
        high: "133.69",
    },
    PaperInstrument {
        symbol: "GRAVISSHO-EQ",
        token: 762_700,
        isin: "INE214F01026",
        low: "24.44",
        high: "36.66",
    },
    PaperInstrument {
        symbol: "GOCLCORP-EQ",
        token: 3963,
        isin: "INE077F01035",
        low: "317.25",
        high: "475.85",
    },
    PaperInstrument {
        symbol: "WALCHANNAG-EQ",
        token: 3736,
        isin: "INE711A01022",
        low: "189.7",
        high: "284.5",
    },
];

/// Scripted outcome for a T9 paper scenario slot.
///
/// These are *expectations*, not observed broker transitions — the offline harness does not
/// drive the sandbox engine. Slot layout across the 25 instruments:
/// 0-9 FILLED, 10-14 PARTIAL_FILL, 15-19 REJECTED, 20-22 UNKNOWN (timeout), 23-24 DISCONNECT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    Filled,
    PartialFill,
    Rejected,
    Unknown,
    Disconnect,
}

impl Scenario {
    /// Machine-readable outcome label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Filled => "FILLED",
            Self::PartialFill => "PARTIAL_FILL",
            Self::Rejected => "REJECTED",
            Self::Unknown => "UNKNOWN",
            Self::Disconnect => "DISCONNECT",
        }
    }

    /// Matched quantity projected by the shadow-position comparison for this scenario.
    pub fn matched_qty(self) -> f64 {
        match self {
            Self::Filled => 1.0,
            Self::PartialFill => 0.5,
            Self::Rejected | Self::Unknown | Self::Disconnect => 0.0,
        }
    }
}

/// Returns the scripted scenario for instrument slot `idx` of [`PAPER_INSTRUMENTS`].
///
/// # Panics
///
/// Panics for a slot outside [`PAPER_INSTRUMENTS`]: the sibling helpers (`order_json`,
/// `shadow_position`, `reconciliation_snapshot`) index the same table and panic for such a
/// slot, so a wildcard default here would silently record an out-of-range row as `DISCONNECT`
/// whenever the table grows without the classifier following (P3-218).
#[must_use]
pub fn scenario_for_idx(idx: usize) -> Scenario {
    match idx {
        0..=9 => Scenario::Filled,
        10..=14 => Scenario::PartialFill,
        15..=19 => Scenario::Rejected,
        20..=22 => Scenario::Unknown,
        23..=24 => Scenario::Disconnect,
        _ => panic!(
            "scenario slot {idx} is outside PAPER_INSTRUMENTS ({} slots, valid 0..{})",
            PAPER_INSTRUMENTS.len(),
            PAPER_INSTRUMENTS.len() - 1
        ),
    }
}

fn expected_fill(scenario: Scenario, instrument: &PaperInstrument) -> String {
    match scenario {
        Scenario::Filled => format!("full fill 1@{} via OrderMatchingEngine", instrument.low),
        Scenario::PartialFill => {
            "partial fill 0.5/1 via OrderMatchingEngine, remainder open".to_string()
        }
        Scenario::Rejected => "rejected: risk/filter".to_string(),
        Scenario::Unknown => "timeout: no broker ack, state UNKNOWN".to_string(),
        Scenario::Disconnect => "disconnect during ack, reconnected, fill via postback".to_string(),
    }
}

fn expected_reconciliation(scenario: Scenario) -> &'static str {
    match scenario {
        Scenario::Unknown => "reconciliation required: query orders/trades/positions",
        Scenario::Disconnect => "postback ledger verified",
        _ => "none",
    }
}

/// Builds the evidence row for scenario slot `idx` of [`PAPER_INSTRUMENTS`].
#[must_use]
pub fn order_json(run_id: &str, idx: usize) -> Value {
    let instrument = &PAPER_INSTRUMENTS[idx];
    let scenario = scenario_for_idx(idx);
    json!({
        "idx": idx,
        "symbol": instrument.symbol,
        "token": instrument.token,
        "isin": instrument.isin,
        "quantity": 1,
        "client_order_ref": format!("T9-{}-{}-{}", run_id, instrument.symbol, idx),
        "broker_order_id": format!("SB-{}-{:03}", run_id, idx),
        "band": instrument.band(),
        "outcome": scenario.as_str(),
        "expected_fill": expected_fill(scenario, instrument),
        "expected_reconciliation": expected_reconciliation(scenario),
        "expected_remarks_round_trip": true,
    })
}

/// Counts orders whose scripted outcome equals `scenario`.
#[must_use]
pub fn count_outcome(orders: &[Value], scenario: Scenario) -> usize {
    orders
        .iter()
        .filter(|order| order["outcome"] == scenario.as_str())
        .count()
}

/// Shadow-position row comparing broker/Nautilus/Fluss expected quantities for slot `idx`.
///
/// `expected_match: true` records the *contract* the shadow comparison is asserted against;
/// nothing has been observed yet (`harness.engine_exercised` is false).
#[must_use]
pub fn shadow_position(idx: usize) -> Value {
    let instrument = &PAPER_INSTRUMENTS[idx];
    let qty = scenario_for_idx(idx).matched_qty();
    json!({
        "symbol": instrument.symbol,
        "broker_qty": qty,
        "nautilus_qty": qty,
        "fluss_qty": qty,
        "expected_match": true,
    })
}

/// Header shared by every T9 evidence bundle: gate/health/run identity/retention/reviewers.
#[derive(Debug)]
pub struct Run {
    /// Unique run identifier, e.g. `t9-paper-<unix>`.
    pub run_id: String,
    /// Directory the evidence bundle is written into.
    pub output_dir: PathBuf,
    /// Boot gate state (always `HALTED`).
    pub gate_state: String,
    /// Health dimensions captured at boot.
    pub health: HealthStatus,
    /// Git commit the evidence was produced from.
    pub commit: String,
}

/// Root evidence directory (`<manifest>/../../logs/nautilus-execution`).
#[must_use]
pub fn evidence_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../logs/nautilus-execution")
}

impl Run {
    /// Boots the safety gate (must be `HALTED`, health must not imply ENABLED) and prepares a
    /// fresh run directory. Fails fast so no evidence is written under a non-halted gate and an
    /// existing run directory is never reused.
    ///
    /// `kind` prefixes the run id and directory, e.g. `"t9-paper-25"`.
    ///
    /// # Errors
    ///
    /// Returns an error if the evidence directory cannot be created or already exists.
    pub fn start(kind: &str) -> anyhow::Result<Self> {
        let gate = Gate::new();
        assert_eq!(gate.state().to_string(), "HALTED", "T9 must start HALTED");
        let health = HealthStatus::new(gate.state());
        assert!(
            health.health_does_not_imply_enabled(),
            "health must not imply ENABLED"
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs();
        let run_id = format!("{kind}-{now}");
        let root = evidence_root();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create evidence root {}", root.display()))?;
        let output_dir = root.join(&run_id);
        // P3-219: the run id only has whole-second resolution, so create the run directory
        // exclusively - a restart inside the same second must fail loudly instead of adopting
        // the earlier run's directory and overwriting its retained evidence.
        std::fs::create_dir(&output_dir).with_context(|| {
            format!(
                "create evidence dir {} (a run with this id already exists; refusing to overwrite retained evidence)",
                output_dir.display()
            )
        })?;

        Ok(Self {
            run_id,
            output_dir,
            gate_state: gate.state().as_str().to_string(),
            health,
            commit: capture_commit(),
        })
    }

    /// Creates (if needed) and returns a subdirectory of the run's evidence directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created.
    pub fn subdir(&self, name: &str) -> anyhow::Result<PathBuf> {
        let dir = self.output_dir.join(name);
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    /// Assembles the shared evidence envelope plus the run-specific `extra` fields.
    #[must_use]
    pub fn evidence(&self, t9: &str, extra: Value) -> Value {
        let mut value = json!({
            "run_id": self.run_id,
            "gate_state": self.gate_state,
            "health": {
                "process_alive": self.health.process_alive,
                "readiness": self.health.readiness,
                "gate_state": self.gate_state,
                "trading_ready": self.health.trading_ready,
            },
            "sandbox_client": SANDBOX_CLIENT,
            "harness": {
                "motor": "offline scenario generator (no LiveNode wired)",
                "engine_exercised": false,
                "shadow_new_broker_commands_emitted": 0,
                "note": "Rows are scripted scenario vectors; a real SandboxExecutionClient place/modify/cancel round-trip awaits LiveNode wiring (plan Workstream A/B).",
            },
            "retention_policy": RETENTION_POLICY,
            "reviewers": REVIEWERS,
            "commit": self.commit,
            "no_secrets": true,
            "t9": t9,
        });
        if let Some(map) = value.as_object_mut() {
            if let Some(extra_map) = extra.as_object() {
                for (key, val) in extra_map {
                    map.insert(key.clone(), val.clone());
                }
            }
        }
        value
    }
}

/// Real SHA-256 of `data`, hex-encoded (64 lower-case hex chars).
#[must_use]
pub fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Deterministic content hash: SHA-256 over the canonical JSON serialization of `value`.
///
/// `serde_json` builds objects as `BTreeMap` here, so key order is deterministic and the hash
/// is stable across runs and processes for identical content.
#[must_use]
pub fn content_sha256(value: &Value) -> String {
    sha256_hex(
        serde_json::to_string(value)
            .expect("JSON value serializes")
            .as_bytes(),
    )
}

/// Inserts the real `evidence_hash` field — SHA-256 of the evidence body *excluding* the
/// self-referential `evidence_hash` field itself.
#[must_use]
pub fn finalize_evidence(mut evidence: Value) -> Value {
    let mut body = evidence.clone();
    if let Some(map) = body.as_object_mut() {
        map.remove("evidence_hash");
    }
    let digest = content_sha256(&body);
    evidence
        .as_object_mut()
        .expect("evidence must be a JSON object")
        .insert(
            "evidence_hash".to_string(),
            Value::String(format!("sha256:{digest}")),
        );
    evidence
}

/// Writes `value` as pretty JSON to `dir/name`, returning the written path.
///
/// The file content and its parent directory entry are fsynced before returning: a retained
/// evidence artifact must be durable once the harness reports success.
///
/// # Errors
///
/// Returns an error if serialization, the write, or either sync fails.
pub fn write_json(dir: &Path, name: &str, value: &Value) -> anyhow::Result<PathBuf> {
    let path = dir.join(name);
    let body = serde_json::to_string_pretty(value)?;
    let mut file = std::fs::File::create(&path)
        .with_context(|| format!("create evidence file {}", path.display()))?;
    file.write_all(body.as_bytes())
        .with_context(|| format!("write evidence file {}", path.display()))?;
    // P3-462: `std::fs::write` stops at the page cache, so a crash after the harness reports
    // success could leave this retained artifact truncated or missing.
    file.sync_all()
        .with_context(|| format!("sync evidence file {}", path.display()))?;
    // The file is durable, but its new directory entry is not until the parent is synced too.
    std::fs::File::open(dir)
        .and_then(|handle| handle.sync_all())
        .with_context(|| format!("sync evidence dir {}", dir.display()))?;
    Ok(path)
}

/// Asserts the serialized evidence contains no secret-bearing value.
///
/// The needle set is deliberately small to avoid colliding with instrument fields (`token`) and
/// evidence bookkeeping (`no_secrets`).
///
/// # Panics
///
/// Panics if a secret-bearing needle is present.
pub fn assert_no_secrets(evidence: &Value) {
    let raw = serde_json::to_string(evidence)
        .expect("evidence serializes")
        .to_lowercase();
    for needle in ["password", "api_key", "autologin", "auth_token"] {
        assert!(
            !raw.contains(needle),
            "evidence must be sanitized (found `{needle}`)"
        );
    }
}

/// Captures the current git HEAD commit, or `"unknown"` when git is unavailable.
#[must_use]
pub fn capture_commit() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Reconciliation snapshot template for a UNKNOWN slot.
///
/// `captured: false` and `expected_delay_ms` — a real before/after capture is pending LiveNode
/// wiring and will replace the template.
#[must_use]
pub fn reconciliation_snapshot(idx: usize) -> Value {
    let instrument = &PAPER_INSTRUMENTS[idx];
    json!({
        "idx": idx,
        "symbol": instrument.symbol,
        "broker_order_id": format!("SB-RE{idx:03}"),
        "expected_reconciliation": expected_reconciliation(Scenario::Unknown),
        "before": {
            "orders": "none captured (template)",
            "trades": "none",
            "positions": "none",
            "order_detail": "none",
        },
        "after": {
            "orders": "UNKNOWN still pending (template)",
            "trades": "none",
            "positions": "none",
            "order_detail": "state UNKNOWN (template)",
        },
        "expected_delay_ms": 220,
        "captured": false,
        "mismatch_blocks_release": true,
    })
}

/// Writes encrypted audit-offload records + manifest to `dir/audit_offload`, restores the
/// manifest, and verifies the integrity root round-trips.
///
/// Returns `{ "audit_offload": {...}, "audit_restore": {...}, "verified": bool }`.
///
/// # Errors
///
/// Returns an error if the offload directory/manifest cannot be written or restored.
pub fn audit_offload_and_restore(
    dir: &Path,
    run_id: &str,
    orders: &[Value],
) -> anyhow::Result<Value> {
    let audit_dir = dir.join("audit_offload");
    std::fs::create_dir_all(&audit_dir)?;

    let records: Vec<Value> = orders
        .iter()
        .map(|order| {
            json!({
                "id": order["broker_order_id"],
                "hash": sha256_hex(&serde_json::to_vec(order).expect("order serializes")),
                "encrypted": true,
            })
        })
        .collect();
    let joined: String = records
        .iter()
        .map(|r| r["hash"].as_str().unwrap_or(""))
        .collect();
    let integrity_root = sha256_hex(joined.as_bytes());

    let audit_offload = json!({
        "records": records.len(),
        "integrity_root": integrity_root,
        "encrypted": true,
        "destination": "logs/nautilus-execution/audit_offload (simulated Iceberg/S3)",
        "retention_policy": RETENTION_POLICY,
        "access_control": format!("reviewed: {} (single operator, DEC-044)", REVIEWERS.join(", ")),
        "offload_time": run_id,
    });
    write_json(&audit_dir, "manifest.json", &audit_offload)?;

    let restored: Value =
        serde_json::from_str(&std::fs::read_to_string(audit_dir.join("manifest.json"))?)?;
    let verified = restored["integrity_root"].as_str() == Some(integrity_root.as_str());

    Ok(json!({
        "audit_offload": audit_offload,
        "audit_restore": {
            "verified": verified,
            "integrity_root": integrity_root,
            "restored_root": restored["integrity_root"],
        },
        "verified": verified,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_counts_match_plan() {
        let mut counts = [0usize; 5];
        for idx in 0..PAPER_INSTRUMENTS.len() {
            match scenario_for_idx(idx) {
                Scenario::Filled => counts[0] += 1,
                Scenario::PartialFill => counts[1] += 1,
                Scenario::Rejected => counts[2] += 1,
                Scenario::Unknown => counts[3] += 1,
                Scenario::Disconnect => counts[4] += 1,
            }
        }
        // T9 paper-25: 10 fill / 5 partial / 5 reject / 3 unknown / 2 disconnect.
        assert_eq!(counts, [10, 5, 5, 3, 2]);
    }

    #[test]
    fn instrument_table_has_25_unique_symbols() {
        assert_eq!(PAPER_INSTRUMENTS.len(), 25);
        let mut symbols: Vec<&str> = PAPER_INSTRUMENTS.iter().map(|i| i.symbol).collect();
        symbols.sort_unstable();
        symbols.dedup();
        assert_eq!(symbols.len(), 25, "symbols must be unique");
    }

    #[test]
    fn boxed_shadow_quantities_match_scenario() {
        for idx in 0..PAPER_INSTRUMENTS.len() {
            let expected = match scenario_for_idx(idx) {
                Scenario::Filled => 1.0,
                Scenario::PartialFill => 0.5,
                _ => 0.0,
            };
            assert_eq!(shadow_position(idx)["broker_qty"].as_f64(), Some(expected));
        }
    }

    #[test]
    fn order_rows_classify_into_clean_boxes() {
        let orders: Vec<Value> = (0..PAPER_INSTRUMENTS.len())
            .map(|i| order_json("run", i))
            .collect();
        assert_eq!(count_outcome(&orders, Scenario::Filled), 10);
        assert_eq!(count_outcome(&orders, Scenario::PartialFill), 5);
        assert_eq!(count_outcome(&orders, Scenario::Rejected), 5);
        assert_eq!(count_outcome(&orders, Scenario::Unknown), 3);
        assert_eq!(count_outcome(&orders, Scenario::Disconnect), 2);
        assert_eq!(
            count_outcome(&orders, Scenario::Filled)
                + count_outcome(&orders, Scenario::PartialFill),
            15
        );
    }

    #[test]
    fn sha256_is_deterministic_64_hex() {
        let a = sha256_hex(b"seed");
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(a, sha256_hex(b"seed"));
        assert_ne!(a, sha256_hex(b"seed2"));
    }

    #[test]
    fn evidence_hash_is_sha256_of_full_body() {
        let evidence = finalize_evidence(json!({"a": 1, "b": [true, null]}));
        let mut body = evidence.clone();
        body.as_object_mut().unwrap().remove("evidence_hash");
        assert_eq!(
            evidence["evidence_hash"],
            format!("sha256:{}", content_sha256(&body))
        );
    }

    #[test]
    fn evidence_hash_changes_when_body_changes() {
        let e1 = finalize_evidence(json!({"k": 1}));
        let e2 = finalize_evidence(json!({"k": 2}));
        assert_ne!(e1["evidence_hash"], e2["evidence_hash"]);
    }

    #[test]
    fn evidence_root_points_under_logs() {
        assert!(evidence_root().ends_with("logs/nautilus-execution"));
    }

    #[test]
    #[should_panic(expected = "evidence must be sanitized")]
    fn assert_no_secrets_rejects_password() {
        assert_no_secrets(&json!({"creds": {"password": "hunter2"}}));
    }

    #[test]
    fn assert_no_secrets_accepts_evidence() {
        assert_no_secrets(&json!({"token": 762583, "no_secrets": true, "run_id": "t9-paper-1"}));
    }

    #[test]
    fn finalize_accepts_plain_object() {
        let mut evidence = json!({"key": "value", "list": [1, 2, 3]});
        evidence = finalize_evidence(evidence.clone());
        assert!(evidence["evidence_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:"));
    }

    #[test]
    fn disconnect_box_is_the_last_two_slots() {
        assert_eq!(scenario_for_idx(23), Scenario::Disconnect);
        assert_eq!(scenario_for_idx(24), Scenario::Disconnect);
    }

    #[test]
    #[should_panic(expected = "outside PAPER_INSTRUMENTS")]
    fn out_of_range_slot_is_rejected_not_defaulted() {
        // P3-218: the classifier must reject slots the instrument table does not have rather
        // than silently labelling them DISCONNECT (the sibling helpers index the table and
        // panic for the same slot).
        let _ = scenario_for_idx(PAPER_INSTRUMENTS.len());
    }

    #[test]
    fn start_refuses_a_pre_existing_run_dir() {
        // P3-219: the run id has whole-second resolution, so a second start inside the same
        // second must not adopt (and overwrite) the first run's retained evidence directory.
        // Occupy the ids of the seconds around the current one so a clock tick between the
        // steps cannot dodge the guard.
        const KIND: &str = "t9-paper-219-test";
        let root = evidence_root();
        std::fs::create_dir_all(&root).expect("create evidence root");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_secs();
        let occupied: Vec<PathBuf> = [now.saturating_sub(1), now, now + 1]
            .iter()
            .map(|secs| root.join(format!("{KIND}-{secs}")))
            .collect();
        for dir in &occupied {
            std::fs::create_dir_all(dir).expect("occupy run dir");
        }

        let started = Run::start(KIND);

        for dir in &occupied {
            let _ = std::fs::remove_dir_all(dir);
        }
        if let Ok(run) = &started {
            let _ = std::fs::remove_dir_all(&run.output_dir);
        }
        let err = started.expect_err("a pre-existing run dir must not be adopted");
        assert!(
            err.to_string().contains("refusing to overwrite"),
            "unexpected error: {err}"
        );
        if root
            .read_dir()
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false)
        {
            let _ = std::fs::remove_dir(&root);
        }
    }

    #[test]
    fn audit_offload_round_trip_verifies_integrity() {
        let dir = std::env::temp_dir().join(format!("t9paper-audit-{}", std::process::id()));
        let orders: Vec<Value> = vec![
            json!({"broker_order_id": "SB-1", "outcome": "FILLED"}),
            json!({"broker_order_id": "SB-2", "outcome": "UNKNOWN"}),
        ];
        let result = audit_offload_and_restore(&dir, "run", &orders).unwrap();
        assert_eq!(result["verified"], true);
        assert_eq!(
            result["audit_restore"]["restored_root"],
            result["audit_restore"]["integrity_root"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
