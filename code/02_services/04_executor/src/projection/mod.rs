//! Rust normalized Nautilus-envelope projection emitter (WP-4 step 2 / CHG-052).
//!
//! This is the exact seam WP-2 will reuse: a faithful Rust port of the Java
//! projection core (`com.trading.common.schema.position` + `projection`) that maps a
//! bridge [`ReportEnvelope`](crate::bridge::protocol::ReportEnvelope) into the projected
//! **Positions** row. Position arithmetic is owned by the Rust/Nautilus authority (it never
//! runs in the JVM projection writer); this module replicates the documented Java parity
//! projector (`PositionProjector` / `PositionProjectorDriver`) byte-for-byte so the
//! differential parity test proves Rust == Java oracle for the same fill sequence.
//!
//! Semantics mirrored exactly (see the Java sources in `code/common/.../schema/`):
//!   - i64 arithmetic identical to Java `long` (truncating division, wrapping ops) —
//!     required for bit-identical differential parity;
//!   - version gate (`KvStateUpdateProtocol`): APPLIED / DUPLICATE / STALE / REGRESSION /
//!     CONFLICT / UNKNOWN;
//!   - lifecycle (`PositionLifecycle`): FLAT -> OPEN -> REDUCING -> CLOSED with
//!     cycle-aware re-entry (a fresh BUY after a full close re-opens to OPEN);
//!   - deterministic minting of `position_id` (`pos-<account>-<token>-<side>-<cycle>`);
//!     re-entry after CLOSED mints a NEW id.
//!
//! The projection path emits **no arithmetic of its own** beyond this authority's computed
//! snapshot — same split as the JVM (`ReferencePositionAuthority` routes position math to the
//! parity projector only at test time).

use std::collections::HashMap;

use crate::bridge::protocol::ReportEnvelope;

/// Position lifecycle (DEC-013). Mirrors `com.trading.common.model.PositionState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PositionState {
    Flat,
    Open,
    Reducing,
    Closed,
    Unknown,
}

impl PositionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            PositionState::Flat => "FLAT",
            PositionState::Open => "OPEN",
            PositionState::Reducing => "REDUCING",
            PositionState::Closed => "CLOSED",
            PositionState::Unknown => "UNKNOWN",
        }
    }
}

/// Fill direction (caller-resolved from the correlated order; the LOG has no side column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

/// A fill as the position projector consumes it. Mirrors `FillEvent` (08_fills.sql v2 subset).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillEvent {
    pub position_id: String,
    pub trade_context_id: String,
    pub account_scope_id: String,
    pub instrument_token: i64,
    pub exchange: String,
    pub symbol: String,
    pub side: Side,
    pub fill_qty: i64,
    pub fill_price_paise: i64,
    pub source_event_id: String,
    pub source_sequence: i64,
    pub event_time_ms: i64,
}

impl FillEvent {
    /// Validate invariants exactly as `FillEvent`'s compact constructor does.
    pub fn validate(&self) -> Result<(), String> {
        if self.position_id.is_empty() {
            return Err("position_id is required".into());
        }
        if self.fill_qty <= 0 {
            return Err(format!("fill_qty must be positive, got {}", self.fill_qty));
        }
        if self.fill_price_paise < 0 {
            return Err(format!(
                "fill_price_paise must be >= 0, got {}",
                self.fill_price_paise
            ));
        }
        Ok(())
    }
}

/// Immutable projected snapshot mirroring the Positions KV layout (10_positions.sql v2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionSnapshot {
    pub position_id: String,
    pub trade_context_id: String,
    pub account_scope_id: String,
    pub instrument_token: i64,
    pub exchange: String,
    pub symbol: String,
    pub side: Side,
    pub state: PositionState,
    pub open_quantity: i64,
    pub closed_quantity: i64,
    pub average_entry_paise: i64,
    pub average_exit_paise: i64,
    pub source_event_id: String,
    pub source_version: i64,
    pub created_ts: i64,
    pub last_update_ts: i64,
    pub schema_version: String,
}

impl PositionSnapshot {
    /// Validate quantity invariants (never negative, never cross).
    pub fn validate(&self) -> Result<(), String> {
        if self.position_id.is_empty() {
            return Err("position_id is required".into());
        }
        // CORR-014 schema compatibility: a row with an undefined schema version cannot be
        // safely treated as authoritative under the declared compatibility policy.
        if self.schema_version.is_empty() {
            return Err("schema_version is required (schema compatibility)".into());
        }
        if self.open_quantity < 0
            || self.closed_quantity < 0
            || self.open_quantity < self.closed_quantity
        {
            return Err(format!(
                "quantity invariant violated: open={} closed={}",
                self.open_quantity, self.closed_quantity
            ));
        }
        Ok(())
    }

    /// Current position size — derived, never persisted.
    pub fn current_quantity(&self) -> i64 {
        self.open_quantity - self.closed_quantity
    }
}

/// Version gate — port of `KvStateUpdateProtocol.evaluate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionGate {
    Applied,
    Duplicate,
    Stale,
    Regression,
    Conflict,
    Unknown,
}

impl VersionGate {
    pub fn evaluate(current_version: i64, incoming_version: i64, content_matches: bool) -> Self {
        if current_version < 0 || incoming_version < 0 {
            return VersionGate::Unknown;
        }
        if incoming_version == current_version {
            return if content_matches {
                VersionGate::Duplicate
            } else {
                VersionGate::Conflict
            };
        }
        if incoming_version < current_version {
            return if content_matches {
                VersionGate::Stale
            } else {
                VersionGate::Regression
            };
        }
        VersionGate::Applied
    }
}

/// Lifecycle helpers — port of `PositionLifecycle`.
pub mod lifecycle {
    use super::PositionState;

    pub fn derive(open_quantity: i64, closed_quantity: i64, traded: bool) -> PositionState {
        if open_quantity < 0 || closed_quantity < 0 || open_quantity < closed_quantity {
            return PositionState::Unknown;
        }
        if open_quantity == 0 && closed_quantity == 0 {
            return if traded {
                PositionState::Closed
            } else {
                PositionState::Flat
            };
        }
        if closed_quantity == 0 {
            return PositionState::Open;
        }
        if open_quantity > closed_quantity {
            PositionState::Reducing
        } else {
            PositionState::Closed
        }
    }

    pub fn is_legal_transition(from: PositionState, to: PositionState) -> bool {
        if from == PositionState::Unknown || to == PositionState::Unknown {
            return false;
        }
        if from == to {
            return true;
        }
        match from {
            PositionState::Flat => to == PositionState::Open,
            PositionState::Open => to == PositionState::Reducing || to == PositionState::Closed,
            PositionState::Reducing => to == PositionState::Open || to == PositionState::Closed,
            PositionState::Closed => to == PositionState::Open,
            PositionState::Unknown => false,
        }
    }
}

/// Pure projection core — port of `PositionProjector`. Side-effect free; never mutates input.
pub struct Projection;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionOutcome {
    Applied,
    Duplicate,
    Stale,
    Violation,
}

#[derive(Debug, Clone)]
pub struct ProjectionResult {
    pub outcome: ProjectionOutcome,
    pub snapshot: Option<PositionSnapshot>,
    pub reason: Option<String>,
}

impl Projection {
    /// Project `fill` onto `current` (None = no position yet). Mirrors `PositionProjector.apply`.
    pub fn apply(
        current: Option<&PositionSnapshot>,
        fill: &FillEvent,
        now_ms: i64,
    ) -> ProjectionResult {
        let current_version = current.map_or(0, |c| c.source_version);

        // Version gate (SCH-09).
        let content_matches = current
            .map(|c| c.source_event_id == fill.source_event_id)
            .unwrap_or(false);
        let v = VersionGate::evaluate(current_version, fill.source_sequence, content_matches);
        match v {
            VersionGate::Duplicate => {
                return ProjectionResult {
                    outcome: ProjectionOutcome::Duplicate,
                    snapshot: current.cloned(),
                    reason: None,
                };
            }
            VersionGate::Stale => {
                return ProjectionResult {
                    outcome: ProjectionOutcome::Stale,
                    snapshot: current.cloned(),
                    reason: Some(format!("stale fill version {}", current_version)),
                };
            }
            VersionGate::Regression | VersionGate::Conflict | VersionGate::Unknown => {
                return ProjectionResult {
                    outcome: ProjectionOutcome::Violation,
                    snapshot: None,
                    reason: Some(format!(
                        "version check {:?} for fill {}",
                        v, fill.source_event_id
                    )),
                };
            }
            VersionGate::Applied => {} // fall through
        }

        let open = current.map_or(0, |c| c.open_quantity);
        let closed = current.map_or(0, |c| c.closed_quantity);
        let avg_entry = current.map_or(0, |c| c.average_entry_paise);
        let avg_exit = current.map_or(0, |c| c.average_exit_paise);

        let (open, closed, avg_entry, avg_exit) =
            match fill.side {
                Side::Buy => {
                    let current_open_before = open - closed;
                    let new_open = open + fill.fill_qty;
                    let new_avg = weighted_average(
                        avg_entry,
                        current_open_before,
                        fill.fill_price_paise,
                        fill.fill_qty,
                    );
                    (new_open, closed, new_avg, avg_exit)
                }
                Side::Sell => {
                    let next_closed = closed + fill.fill_qty;
                    if next_closed > open {
                        return ProjectionResult {
                            outcome: ProjectionOutcome::Violation,
                            snapshot: None,
                            reason: Some(format!(
                            "sell overshoots open quantity: open={} sell={} (would close to {})",
                            open, fill.fill_qty, open - next_closed
                        )),
                        };
                    }
                    let new_avg = weighted_average(
                        avg_exit,
                        next_closed - fill.fill_qty,
                        fill.fill_price_paise,
                        fill.fill_qty,
                    );
                    (open, next_closed, avg_entry, new_avg)
                }
            };

        let prior_state = current.map_or(PositionState::Flat, |c| c.state);
        let next_state = next_state(prior_state, open, closed, fill.side);
        if !lifecycle::is_legal_transition(prior_state, next_state) {
            return ProjectionResult {
                outcome: ProjectionOutcome::Violation,
                snapshot: None,
                reason: Some(format!(
                    "illegal transition {:?} -> {:?} for fill {}",
                    prior_state, next_state, fill.source_event_id
                )),
            };
        }

        let snapshot = PositionSnapshot {
            position_id: fill.position_id.clone(),
            trade_context_id: fill.trade_context_id.clone(),
            account_scope_id: fill.account_scope_id.clone(),
            instrument_token: fill.instrument_token,
            exchange: fill.exchange.clone(),
            symbol: fill.symbol.clone(),
            side: fill.side,
            state: next_state,
            open_quantity: open,
            closed_quantity: closed,
            average_entry_paise: avg_entry,
            average_exit_paise: avg_exit,
            source_event_id: fill.source_event_id.clone(),
            source_version: fill.source_sequence,
            created_ts: current.map_or(now_ms, |c| c.created_ts),
            last_update_ts: now_ms,
            schema_version: "v2".to_string(),
        };
        ProjectionResult {
            outcome: ProjectionOutcome::Applied,
            snapshot: Some(snapshot),
            reason: None,
        }
    }
}

/// Cycle-aware next state: fully exited -> CLOSED; never exited -> OPEN; a fresh BUY after a
/// full close re-opens the cycle; otherwise a position with cumulative exits is REDUCING.
fn next_state(prior: PositionState, open: i64, closed: i64, side: Side) -> PositionState {
    if open == closed {
        return PositionState::Closed;
    }
    if closed == 0 {
        return PositionState::Open;
    }
    if prior == PositionState::Closed && side == Side::Buy {
        return PositionState::Open;
    }
    PositionState::Reducing
}

/// Weighted average across the existing notional and the new fill (integer, truncating).
/// Uses wrapping arithmetic to mirror Java `long` silent wraparound on overflow.
fn weighted_average(existing_avg: i64, existing_qty: i64, new_price: i64, new_qty: i64) -> i64 {
    if existing_qty + new_qty == 0 {
        return 0;
    }
    let numerator = existing_avg
        .wrapping_mul(existing_qty)
        .wrapping_add(new_price.wrapping_mul(new_qty));
    numerator / (existing_qty + new_qty)
}

/// Account/instrument/side uniqueness key (dossier position protocol).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionKey {
    pub account_scope_id: String,
    pub instrument_token: i64,
    pub side: Side,
}

/// Outcome of feeding one fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedOutcome {
    Applied,
    Duplicate,
    Stale,
    Violation,
    NotAFill,
}

#[derive(Debug, Clone)]
pub struct FeedResult {
    pub outcome: FeedOutcome,
    pub snapshot: Option<PositionSnapshot>,
    pub position_id: String,
    pub reason: Option<String>,
}

/// Stateful driver holding the current snapshot per `position_id`; mints ids and projects fills
/// through [`Projection`]. Port of `PositionProjectorDriver`.
pub struct PositionProjectorDriver {
    active: HashMap<PositionKey, String>,
    cycles: HashMap<PositionKey, u32>,
    snapshots: HashMap<String, PositionSnapshot>,
}

impl Default for PositionProjectorDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl PositionProjectorDriver {
    pub fn new() -> Self {
        PositionProjectorDriver {
            active: HashMap::new(),
            cycles: HashMap::new(),
            snapshots: HashMap::new(),
        }
    }

    /// Feed a fully resolved fill. Mirrors `PositionProjectorDriver.feed(FillEvent, nowMs)`.
    pub fn feed_fill(&mut self, fill: &FillEvent, now_ms: i64) -> FeedResult {
        let current = self.snapshots.get(&fill.position_id);
        let r = Projection::apply(current, fill, now_ms);
        let position_id = fill.position_id.clone();
        match r.outcome {
            ProjectionOutcome::Applied => {
                if let Some(s) = r.snapshot.clone() {
                    self.snapshots.insert(position_id.clone(), s);
                }
                FeedResult {
                    outcome: FeedOutcome::Applied,
                    snapshot: r.snapshot,
                    position_id,
                    reason: None,
                }
            }
            ProjectionOutcome::Duplicate => FeedResult {
                outcome: FeedOutcome::Duplicate,
                snapshot: r.snapshot,
                position_id,
                reason: None,
            },
            ProjectionOutcome::Stale => FeedResult {
                outcome: FeedOutcome::Stale,
                snapshot: r.snapshot,
                position_id,
                reason: r.reason,
            },
            ProjectionOutcome::Violation => FeedResult {
                outcome: FeedOutcome::Violation,
                snapshot: None,
                position_id,
                reason: r.reason,
            },
        }
    }

    /// Resolve or mint the position id for a key, then feed `fill` onto that position.
    /// Mirrors the operator path (`feed(GenericRow, ctx, nowMs)` -> map -> resolve -> project).
    pub fn feed_on_key(&mut self, key: &PositionKey, fill: &FillEvent, now_ms: i64) -> FeedResult {
        let position_id = self.resolve_position_id(key, fill);
        let mut positioned = fill.clone();
        positioned.position_id = position_id.clone();
        let r = self.feed_fill(&positioned, now_ms);
        // Preserve the resolved id on the result.
        FeedResult { position_id, ..r }
    }

    /// Mint or reuse the position id for a key. Mirrors
    /// `PositionProjectorDriver.resolvePositionId`, including the P3-166 guard.
    ///
    /// A re-entry cycle is minted only for a strictly newer, different event. Deciding from the
    /// prior state alone (CLOSED + BUY) gave the fresh id no snapshot, so the version gate compared
    /// against version 0 and returned APPLIED for any incoming version > 0 — a redelivered older
    /// opening fill was applied as a phantom OPEN position, bypassing STALE/DUPLICATE entirely.
    /// Anything not strictly newer stays on the closed id, where the gate rejects it.
    fn resolve_position_id(&mut self, key: &PositionKey, fill: &FillEvent) -> String {
        let Some(current_id) = self.active.get(key).cloned() else {
            return self.mint(key);
        };
        let Some(current) = self.snapshots.get(&current_id).cloned() else {
            return current_id;
        };
        if current.state == PositionState::Closed
            && key.side == Side::Buy
            && fill.source_sequence > current.source_version
            && fill.source_event_id != current.source_event_id
        {
            return self.mint(key);
        }
        current_id
    }

    fn mint(&mut self, key: &PositionKey) -> String {
        let cycle = self.cycles.entry(key.clone()).or_insert(0);
        *cycle += 1;
        let id = format!(
            "pos-{}-{}-{}-{}",
            key.account_scope_id,
            key.instrument_token,
            key.side.as_str(),
            cycle
        );
        self.active.insert(key.clone(), id.clone());
        id
    }

    pub fn snapshot(&self, position_id: &str) -> Option<&PositionSnapshot> {
        self.snapshots.get(position_id)
    }

    pub fn position_id_for(&self, key: &PositionKey) -> Option<&String> {
        self.active.get(key)
    }

    pub fn size(&self) -> usize {
        self.snapshots.len()
    }
}

/// Context required to lift a bridge [`ReportEnvelope`] into a [`FillEvent`] — order/correlation
/// data that lives on the order (not the report). The caller resolves these from the correlated
/// order exactly as `BridgeExecutionClient::handle_fill` already does.
#[derive(Debug, Clone)]
pub struct EmitterContext {
    pub trade_context_id: String,
    pub account_scope_id: String,
    pub instrument_token: i64,
    pub exchange: String,
    pub symbol: String,
    pub side: Side,
}

/// Normalized Nautilus-envelope emitter (WP-4 step 2): maps a report to the projected Positions
/// row through the projection authority. This is the seam WP-2 will reuse.
pub struct ProjectionEmitter {
    driver: PositionProjectorDriver,
}

impl Default for ProjectionEmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionEmitter {
    pub fn new() -> Self {
        ProjectionEmitter {
            driver: PositionProjectorDriver::new(),
        }
    }

    /// Emit the projected Positions row for a bridge report (a fill).
    ///
    /// `source_sequence` is the monotone version fed to the version gate (the report's own
    /// sequence in production, or an explicit test sequence for parity). Returns the snapshot
    /// result plus the resolved position id.
    pub fn emit_fill(
        &mut self,
        report: &ReportEnvelope,
        ctx: &EmitterContext,
        source_sequence: i64,
        now_ms: i64,
    ) -> Result<FeedResult, String> {
        let fill_qty = report
            .fill_quantity
            .as_deref()
            .unwrap_or("0")
            .parse::<i64>()
            .map_err(|e| format!("fill_quantity not an integer: {e}"))?;
        let fill_price_paise = report
            .fill_price
            .as_deref()
            .unwrap_or("0")
            .parse::<i64>()
            .map_err(|e| format!("fill_price not an integer: {e}"))?;
        let source_event_id = if report.postback_event_id.is_empty() {
            format!("evt-{now_ms}")
        } else {
            report.postback_event_id.clone()
        };

        let key = PositionKey {
            account_scope_id: ctx.account_scope_id.clone(),
            instrument_token: ctx.instrument_token,
            side: ctx.side,
        };
        let fill = FillEvent {
            position_id: String::new(), // resolved by feed_on_key
            trade_context_id: ctx.trade_context_id.clone(),
            account_scope_id: ctx.account_scope_id.clone(),
            instrument_token: ctx.instrument_token,
            exchange: ctx.exchange.clone(),
            symbol: ctx.symbol.clone(),
            side: ctx.side,
            fill_qty,
            fill_price_paise,
            source_event_id,
            source_sequence,
            event_time_ms: report.received_ts_ms,
        };
        // Validate the lift inputs; position_id is resolved by feed_on_key below.
        if fill.fill_qty <= 0 {
            return Err(format!(
                "fill_quantity must be positive, got {}",
                fill.fill_qty
            ));
        }
        if fill.fill_price_paise < 0 {
            return Err(format!(
                "fill_price must be >= 0, got {}",
                fill.fill_price_paise
            ));
        }
        Ok(self.driver.feed_on_key(&key, &fill, now_ms))
    }

    /// Feed an already-structured fill directly (used by differential parity / tests).
    pub fn driver(&self) -> &PositionProjectorDriver {
        &self.driver
    }

    pub fn driver_mut(&mut self) -> &mut PositionProjectorDriver {
        &mut self.driver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1000;
    const POSITION_ID: &str = "pos-acc-1-1001-BUY-1";

    fn fill(seq: i64, side: Side, qty: i64, price: i64) -> FillEvent {
        FillEvent {
            position_id: POSITION_ID.to_string(),
            trade_context_id: "tc-1".to_string(),
            account_scope_id: "acc-1".to_string(),
            instrument_token: 1001,
            exchange: "CME".to_string(),
            symbol: "wti".to_string(),
            side,
            fill_qty: qty,
            fill_price_paise: price,
            source_event_id: format!("evt-{seq}"),
            source_sequence: seq,
            event_time_ms: NOW,
        }
    }

    #[test]
    fn buy_buy_sell_sell_projects_to_closed_with_weighted_averages() {
        let mut driver = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };

        let r1 = driver.feed_on_key(&key, &fill(1, Side::Buy, 10, 1000), NOW);
        assert_eq!(r1.outcome, FeedOutcome::Applied);
        let s1 = r1.snapshot.unwrap();
        assert_eq!(s1.state, PositionState::Open);
        assert_eq!((s1.open_quantity, s1.closed_quantity), (10, 0));
        assert_eq!(s1.average_entry_paise, 1000);

        let r2 = driver.feed_on_key(&key, &fill(2, Side::Buy, 5, 1100), NOW);
        let s2 = r2.snapshot.unwrap();
        assert_eq!(s2.open_quantity, 15);
        assert_eq!(s2.average_entry_paise, 1033); // (1000*10+1100*5)/15 = 15500/15 = 1033

        let r3 = driver.feed_on_key(&key, &fill(3, Side::Sell, 8, 1050), NOW);
        let s3 = r3.snapshot.unwrap();
        assert_eq!(s3.state, PositionState::Reducing);
        assert_eq!((s3.open_quantity, s3.closed_quantity), (15, 8));
        assert_eq!(s3.average_exit_paise, 1050);

        let r4 = driver.feed_on_key(&key, &fill(4, Side::Sell, 7, 1060), NOW);
        let s4 = r4.snapshot.unwrap();
        assert_eq!(s4.state, PositionState::Closed);
        assert_eq!((s4.open_quantity, s4.closed_quantity), (15, 15));
        assert_eq!(s4.average_entry_paise, 1033);
        assert_eq!(s4.average_exit_paise, 1054); // (1050*8+1060*7)/15 = 15820/15 = 1054

        // Differential-parity anchor: closed position, open == closed == 15.
        assert_eq!(s4.position_id, POSITION_ID);
    }

    #[test]
    fn oversell_is_a_violation_and_does_not_replace_snapshot() {
        let mut driver = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };
        assert_eq!(
            driver
                .feed_on_key(&key, &fill(1, Side::Buy, 10, 1000), NOW)
                .outcome,
            FeedOutcome::Applied
        );
        let r = driver.feed_on_key(&key, &fill(2, Side::Sell, 20, 900), NOW);
        assert_eq!(r.outcome, FeedOutcome::Violation);
        assert!(r.reason.unwrap().contains("overshoots"));
        // The oversell must NOT overwrite the prior applied snapshot.
        let s = driver.snapshot(POSITION_ID).unwrap();
        assert_eq!(s.open_quantity, 10);
    }

    #[test]
    fn stale_and_duplicate_are_rejected() {
        let mut driver = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };
        // Applied at version 5 with event id e5.
        let mut v5 = fill(5, Side::Buy, 10, 1000);
        v5.source_event_id = "e5".into();
        assert_eq!(
            driver.feed_on_key(&key, &v5, NOW).outcome,
            FeedOutcome::Applied
        );
        // Duplicate: same version + same event id.
        assert_eq!(
            driver.feed_on_key(&key, &v5, NOW).outcome,
            FeedOutcome::Duplicate
        );
        // Stale: older version (3) but SAME event id -> content matches -> STALE.
        let mut v3 = fill(3, Side::Buy, 10, 1000);
        v3.source_event_id = "e5".into();
        assert_eq!(
            driver.feed_on_key(&key, &v3, NOW).outcome,
            FeedOutcome::Stale
        );
        // Different content at an older version -> REGRESSION -> VIOLATION (Java semantics).
        let mut v3c = fill(3, Side::Buy, 10, 1000);
        v3c.source_event_id = "e3".into();
        assert_eq!(
            driver.feed_on_key(&key, &v3c, NOW).outcome,
            FeedOutcome::Violation
        );
    }

    #[test]
    fn report_envelope_emits_positions_row() {
        let envelope = ReportEnvelope {
            record_type: "report".into(),
            contract_version: 2,
            request_id: "rq-1".into(),
            command: "P".into(),
            outcome: "SUCCESS".into(),
            reason: String::new(),
            instruction_id: "instr-1".into(),
            execution_attempt_id: "att-1".into(),
            client_order_ref: "co-1".into(),
            broker_order_id: "b-1".into(),
            exchange_order_id: "e-1".into(),
            postback_event_id: "pb-1".into(),
            order_status: Some("FILLED".into()),
            report_type: Some("order_filled".into()),
            fill_shares: "10".into(),
            average_price: "1000".into(),
            fill_price: Some("1000".into()),
            fill_quantity: Some("10".into()),
            fill_time: "now".into(),
            instrument_token: "1001".into(),
            received_ts_ms: NOW,
            response_fingerprint: String::new(),
            data: None,
        };
        let ctx = EmitterContext {
            trade_context_id: "tc-1".into(),
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            exchange: "CME".into(),
            symbol: "wti".into(),
            side: Side::Buy,
        };
        let mut emitter = ProjectionEmitter::new();
        let r = emitter.emit_fill(&envelope, &ctx, 1, NOW).unwrap();
        assert_eq!(r.outcome, FeedOutcome::Applied);
        let s = r.snapshot.unwrap();
        assert_eq!(s.position_id, POSITION_ID);
        assert_eq!(s.source_event_id, "pb-1");
        assert_eq!(s.open_quantity, 10);
        assert_eq!(s.state, PositionState::Open);
    }
    // --------------------------------------------------------------------------
    // STATE-* / CORR-004 / CORR-011 — offline validation matrix (pure functions).
    // REJECT or QUARANTINE bad data; never silently interpret it as valid.
    // (09 §6 STATE, §1 CORR-004/011)
    // --------------------------------------------------------------------------

    fn snapshot_with(open: i64, closed: i64) -> PositionSnapshot {
        PositionSnapshot {
            position_id: POSITION_ID.to_string(),
            trade_context_id: "tc-1".into(),
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            exchange: "CME".into(),
            symbol: "wti".into(),
            side: Side::Buy,
            state: PositionState::Open,
            open_quantity: open,
            closed_quantity: closed,
            average_entry_paise: 1000,
            average_exit_paise: 0,
            source_event_id: "e1".into(),
            source_version: 1,
            created_ts: NOW,
            last_update_ts: NOW,
            schema_version: "v2".into(),
        }
    }

    #[test]
    fn version_gate_conflict_same_version_different_content_is_violation() {
        let mut driver = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };
        let mut v5a = fill(5, Side::Buy, 10, 1000);
        v5a.source_event_id = "evt-a".into();
        assert_eq!(
            driver.feed_on_key(&key, &v5a, NOW).outcome,
            FeedOutcome::Applied
        );
        // Same version but DIFFERENT event id -> CONFLICT -> violation; prior state preserved.
        let mut v5b = fill(5, Side::Buy, 10, 1000);
        v5b.source_event_id = "evt-b".into();
        let r = driver.feed_on_key(&key, &v5b, NOW);
        assert_eq!(r.outcome, FeedOutcome::Violation);
        assert!(r.reason.unwrap().contains("Conflict"));
        let s = driver.snapshot(POSITION_ID).unwrap();
        assert_eq!(
            s.source_event_id, "evt-a",
            "a conflict must not overwrite already-applied authoritative state"
        );
    }

    #[test]
    fn fill_event_validate_rejects_negative_and_empty() {
        // Zero / negative quantity, negative price, empty id are all rejected (STATE-*).
        let zq = fill(1, Side::Buy, 0, 1000);
        assert!(zq
            .validate()
            .unwrap_err()
            .contains("fill_qty must be positive"));
        let nq = fill(1, Side::Buy, -3, 1000);
        assert!(nq
            .validate()
            .unwrap_err()
            .contains("fill_qty must be positive"));
        let np = fill(1, Side::Buy, 10, -1);
        assert!(np
            .validate()
            .unwrap_err()
            .contains("fill_price_paise must be >= 0"));
        let mut eid = fill(1, Side::Buy, 10, 1000);
        eid.position_id = String::new();
        assert!(eid
            .validate()
            .unwrap_err()
            .contains("position_id is required"));
    }

    #[test]
    fn position_snapshot_validate_rejects_cross_and_negative_quantity() {
        // open < closed is a cross -> invalid; negative open -> invalid (CORR-011 invariant).
        assert!(snapshot_with(10, 20)
            .validate()
            .unwrap_err()
            .contains("quantity invariant"));
        assert!(snapshot_with(-5, 0)
            .validate()
            .unwrap_err()
            .contains("quantity invariant"));
        let ok = snapshot_with(10, 4);
        assert!(ok.validate().is_ok());
        assert_eq!(ok.current_quantity(), 6);
    }

    #[test]
    fn version_gate_evaluate_full_matrix() {
        // Newer version -> Applied.
        assert_eq!(VersionGate::evaluate(5, 7, false), VersionGate::Applied);
        // Same version -> Duplicate (content matches) / Conflict (content differs).
        assert_eq!(VersionGate::evaluate(5, 5, true), VersionGate::Duplicate);
        assert_eq!(VersionGate::evaluate(5, 5, false), VersionGate::Conflict);
        // Older version -> Stale (content matches) / Regression (content differs).
        assert_eq!(VersionGate::evaluate(5, 3, true), VersionGate::Stale);
        assert_eq!(VersionGate::evaluate(5, 3, false), VersionGate::Regression);
        // Negative versions are unknown/corrupt.
        assert_eq!(VersionGate::evaluate(-1, 3, false), VersionGate::Unknown);
        assert_eq!(VersionGate::evaluate(5, -1, false), VersionGate::Unknown);
    }

    #[test]
    fn lifecycle_derive_and_transition_matrix() {
        use lifecycle::{derive, is_legal_transition};
        // derive
        assert_eq!(derive(0, 0, false), PositionState::Flat);
        assert_eq!(derive(0, 0, true), PositionState::Closed);
        assert_eq!(derive(10, 0, true), PositionState::Open);
        assert_eq!(derive(10, 4, true), PositionState::Reducing);
        assert_eq!(derive(4, 4, true), PositionState::Closed);
        assert_eq!(derive(-1, 0, false), PositionState::Unknown); // negative -> Unknown
                                                                  // legal forward transitions + re-entry (CORR-009/010 monotonic)
        assert!(is_legal_transition(
            PositionState::Flat,
            PositionState::Open
        ));
        assert!(is_legal_transition(
            PositionState::Open,
            PositionState::Reducing
        ));
        assert!(is_legal_transition(
            PositionState::Open,
            PositionState::Closed
        ));
        assert!(is_legal_transition(
            PositionState::Reducing,
            PositionState::Open
        ));
        assert!(is_legal_transition(
            PositionState::Reducing,
            PositionState::Closed
        ));
        assert!(is_legal_transition(
            PositionState::Closed,
            PositionState::Open
        )); // re-entry
        assert!(is_legal_transition(
            PositionState::Open,
            PositionState::Open
        )); // self
            // illegal / regressive
        assert!(!is_legal_transition(
            PositionState::Flat,
            PositionState::Reducing
        ));
        assert!(!is_legal_transition(
            PositionState::Flat,
            PositionState::Closed
        ));
        assert!(!is_legal_transition(
            PositionState::Open,
            PositionState::Flat
        )); // cannot regress to Flat
        assert!(!is_legal_transition(
            PositionState::Open,
            PositionState::Unknown
        )); // Unknown forbidden
    }

    #[test]
    fn projection_apply_reapply_is_idempotent_snapshot_unchanged() {
        let mut driver = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };
        let f = fill(1, Side::Buy, 10, 1000);
        assert_eq!(
            driver.feed_on_key(&key, &f, NOW).outcome,
            FeedOutcome::Applied
        );
        let s1 = driver.snapshot(POSITION_ID).unwrap().clone();
        // Re-applying the SAME immutable event folds to Duplicate; the authoritative
        // projection is byte-identical (CORR-001 / CORR-003 / CORR-004 determinism).
        assert_eq!(
            driver.feed_on_key(&key, &f, NOW).outcome,
            FeedOutcome::Duplicate
        );
        let s2 = driver.snapshot(POSITION_ID).unwrap();
        assert_eq!(
            &s1, s2,
            "re-apply must not change the authoritative projection"
        );
    }
    // --------------------------------------------------------------------------
    // CORR-013 — source-of-truth consistency. The projection is a pure function of the
    // authority's fed events: a regression/conflict never overwrites, and the position
    // size is always DERIVED (open - closed), never independently settable, so the
    // projection cannot silently become authority.
    // --------------------------------------------------------------------------
    #[test]
    fn corr013_projection_is_pure_function_of_authority_events() {
        let mut driver = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };
        assert_eq!(
            driver
                .feed_on_key(&key, &fill(3, Side::Buy, 10, 1000), NOW)
                .outcome,
            FeedOutcome::Applied
        );
        let s = driver.snapshot(POSITION_ID).unwrap().clone();
        assert_eq!(
            s.current_quantity(),
            s.open_quantity - s.closed_quantity,
            "size must be derived"
        );
        // A REGRESSION (older version, different event) or duplicate must never overwrite
        // the authority's last-blessed state.
        assert_eq!(
            driver
                .feed_on_key(&key, &fill(2, Side::Buy, 99, 1000), NOW)
                .outcome,
            FeedOutcome::Violation
        );
        assert_eq!(
            driver
                .feed_on_key(&key, &fill(3, Side::Buy, 99, 1000), NOW)
                .outcome,
            FeedOutcome::Duplicate
        );
        let s2 = driver.snapshot(POSITION_ID).unwrap();
        assert_eq!(
            &s, s2,
            "projection must stay a pure function of authority events (CORR-013)"
        );
    }

    // --------------------------------------------------------------------------
    // CORR-014 — schema compatibility: an undefined schema version is rejected; a
    // declared version is accepted (old state + new artifact governed by the policy).
    // --------------------------------------------------------------------------
    #[test]
    fn corr014_undefined_schema_version_is_rejected() {
        let mut no_schema = snapshot_with(10, 4);
        no_schema.schema_version = String::new();
        assert!(
            no_schema.validate().unwrap_err().contains("schema_version"),
            "undefined schema version must not be treated as authoritative"
        );
        let ok = snapshot_with(10, 4); // schema_version "v2"
        assert!(ok.validate().is_ok());
    }

    // --------------------------------------------------------------------------
    // FENCE-011 — a broker response arriving late (re-delivered after the state moved
    // on) cannot corrupt the (new) owner's state: duplicate / stale fills are fold,
    // never double-counted.
    // --------------------------------------------------------------------------
    #[test]
    fn fence011_late_duplicate_response_cannot_corrupt_state() {
        let mut driver = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };
        assert_eq!(
            driver
                .feed_on_key(&key, &fill(5, Side::Buy, 10, 1000), NOW)
                .outcome,
            FeedOutcome::Applied
        );
        let s = driver.snapshot(POSITION_ID).unwrap().clone();
        assert_eq!(s.open_quantity, 10);
        // Late re-delivery of the SAME response -> Duplicate; a STALE content-matching
        // copy (same event id, older sequence) -> Stale. Neither may double-count.
        assert_eq!(
            driver
                .feed_on_key(&key, &fill(5, Side::Buy, 10, 1000), NOW)
                .outcome,
            FeedOutcome::Duplicate
        );
        let mut stale = fill(5, Side::Buy, 10, 1000);
        stale.source_sequence = 4; // same event, older version -> content matches -> Stale
        assert_eq!(
            driver.feed_on_key(&key, &stale, NOW).outcome,
            FeedOutcome::Stale
        );
        let after = driver.snapshot(POSITION_ID).unwrap();
        assert_eq!(
            &s, after,
            "late responses may not double-count / corrupt state (FENCE-011)"
        );
        assert_eq!(after.open_quantity, 10);
    }

    // --------------------------------------------------------------------------
    // DR-007 / EOD-002 — replay the same event log twice (or repeat an offload) yields
    // the identical authoritative state: idempotent, no drift.
    // --------------------------------------------------------------------------
    #[test]
    fn dr007_replay_twice_produces_identical_state() {
        let log = [
            fill(1, Side::Buy, 10, 1000),
            fill(2, Side::Buy, 5, 1100),
            fill(3, Side::Sell, 8, 1050),
        ];
        let mut a = PositionProjectorDriver::new();
        let mut b = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };
        for f in &log {
            a.feed_on_key(&key, f, NOW);
        }
        for f in &log {
            b.feed_on_key(&key, f, NOW);
        }
        // And repeat the whole log a second time on B (idempotent repeat, EOD-002).
        for f in &log {
            b.feed_on_key(&key, f, NOW);
        }
        assert_eq!(
            a.snapshot(POSITION_ID).unwrap(),
            b.snapshot(POSITION_ID).unwrap(),
            "replaying the same log twice must yield identical authoritative state (DR-007/EOD-002)"
        );
    }

    // --------------------------------------------------------------------------
    // DR-008 / EOD-003 — an interrupted rebuild (half the log, then the rest) resumes to
    // the SAME state as an uninterrupted build: no corruption, no duplicate final objects.
    // --------------------------------------------------------------------------
    #[test]
    fn dr008_interrupted_rebuild_resumes_to_same_state() {
        let full = [
            fill(1, Side::Buy, 10, 1000),
            fill(2, Side::Buy, 5, 1100),
            fill(3, Side::Sell, 8, 1050),
            fill(4, Side::Sell, 7, 1060),
        ];
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };

        let mut uninterrupted = PositionProjectorDriver::new();
        for f in &full {
            uninterrupted.feed_on_key(&key, f, NOW);
        }

        // Interrupted halfway, then resumed with the remainder.
        let mut interrupted = PositionProjectorDriver::new();
        for f in &full[..2] {
            interrupted.feed_on_key(&key, f, NOW);
        }
        for f in &full[2..] {
            interrupted.feed_on_key(&key, f, NOW);
        }

        assert_eq!(
            uninterrupted.snapshot(POSITION_ID).unwrap(),
            interrupted.snapshot(POSITION_ID).unwrap(),
            "interrupted rebuild must resume to the uninterrupted state (DR-008/EOD-003)"
        );
    }

    // --- P3-166: re-entry mints a new cycle, but ONLY for a genuinely newer event ---

    #[test]
    fn reentry_after_close_mints_a_new_cycle_for_a_newer_event() {
        let mut driver = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };
        driver.feed_on_key(&key, &fill(1, Side::Buy, 10, 1000), NOW);
        assert_eq!(
            driver.position_id_for(&key).unwrap(),
            "pos-acc-1-1001-BUY-1"
        );
        driver.feed_on_key(&key, &fill(2, Side::Sell, 10, 1100), NOW);
        assert_eq!(
            driver.snapshot("pos-acc-1-1001-BUY-1").unwrap().state,
            PositionState::Closed
        );

        // A genuinely newer event (seq 3 > 2, different event id) on a fresh BUY opens a new
        // cycle — the guard must not disable legitimate re-entry.
        let r = driver.feed_on_key(&key, &fill(3, Side::Buy, 5, 1200), NOW);
        assert_eq!(r.outcome, FeedOutcome::Applied);
        assert_eq!(r.position_id, "pos-acc-1-1001-BUY-2");
        assert_eq!(r.snapshot.unwrap().state, PositionState::Open);
        assert_eq!(driver.size(), 2);
    }

    #[test]
    fn redelivered_opening_fill_after_close_is_not_a_phantom_reopen() {
        let mut driver = PositionProjectorDriver::new();
        let key = PositionKey {
            account_scope_id: "acc-1".into(),
            instrument_token: 1001,
            side: Side::Buy,
        };
        driver.feed_on_key(&key, &fill(1, Side::Buy, 10, 1000), NOW);
        driver.feed_on_key(&key, &fill(2, Side::Sell, 10, 1100), NOW);
        assert_eq!(
            driver.snapshot("pos-acc-1-1001-BUY-1").unwrap().state,
            PositionState::Closed
        );

        // The ORIGINAL opening fill (seq 1, evt-1) is redelivered out of order. Minting a new
        // cycle gave that id no snapshot, so the version gate compared against version 0 and
        // APPLIED it — a phantom OPEN position on pos-...-BUY-2 that bypassed STALE/DUPLICATE.
        let r = driver.feed_on_key(&key, &fill(1, Side::Buy, 10, 1000), NOW);

        assert_eq!(r.outcome, FeedOutcome::Violation);
        assert_eq!(r.position_id, "pos-acc-1-1001-BUY-1");
        assert_eq!(driver.size(), 1, "no phantom cycle was minted");
        assert_eq!(
            driver.position_id_for(&key).unwrap(),
            "pos-acc-1-1001-BUY-1"
        );
        assert!(driver.snapshot("pos-acc-1-1001-BUY-2").is_none());
        let after = driver.snapshot("pos-acc-1-1001-BUY-1").unwrap();
        assert_eq!(after.state, PositionState::Closed);
        assert_eq!((after.open_quantity, after.closed_quantity), (10, 10));
    }
}
