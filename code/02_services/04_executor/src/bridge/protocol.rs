//! Private wire protocol between the Nautilus execution service and the Go bridge.
//!
//! This is a faithful Rust mirror of
//! `code/02_services/06_execution_bridge/go-bridge/models.go` (contract version 1).
//! The Go bridge is the single Arrow-facing component; every field name and constraint
//! matches the Go implementation so the two sides interoperate without translation.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Private protocol contract version (must equal the Go bridge's `ProtocolVersion`).
pub const PROTOCOL_VERSION: u32 = 1;

/// `record_type` for command envelopes.
pub const RECORD_COMMAND: &str = "execution_command";
/// `record_type` for report envelopes.
pub const RECORD_REPORT: &str = "execution_report";

/// Commands understood by the bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Place,
    Modify,
    Cancel,
    QueryOrder,
    ReconcileOrders,
    ReconcileTrades,
    ReconcilePositions,
}

impl Command {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Place => "place",
            Self::Modify => "modify",
            Self::Cancel => "cancel",
            Self::QueryOrder => "query-order",
            Self::ReconcileOrders => "reconcile-orders",
            Self::ReconcileTrades => "reconcile-trades",
            Self::ReconcilePositions => "reconcile-positions",
        }
    }

    /// Reasoned `from_str` mirrors the Go bridge's wire classification (returns `Option`, not
    /// `FromStr`), so the trait-impl lint is intentionally suppressed.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "place" => Some(Self::Place),
            "modify" => Some(Self::Modify),
            "cancel" => Some(Self::Cancel),
            "query-order" => Some(Self::QueryOrder),
            "reconcile-orders" => Some(Self::ReconcileOrders),
            "reconcile-trades" => Some(Self::ReconcileTrades),
            "reconcile-positions" => Some(Self::ReconcilePositions),
            _ => None,
        }
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Report outcome classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportOutcome {
    Success,
    Rejected,
    Unknown,
}

impl ReportOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "SUCCESS",
            Self::Rejected => "REJECTED",
            Self::Unknown => "UNKNOWN",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "SUCCESS" => Some(Self::Success),
            "REJECTED" => Some(Self::Rejected),
            "UNKNOWN" => Some(Self::Unknown),
            _ => None,
        }
    }
}

impl fmt::Display for ReportOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `transaction_type` values (platform-neutral; the adapter converts to Arrow request fields).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionType {
    Buy,
    Sell,
}

impl TransactionType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

/// `order_type` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Lmt,
    Mkt,
    SlLmt,
    SlMkt,
}

impl OrderType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lmt => "LMT",
            Self::Mkt => "MKT",
            Self::SlLmt => "SL-LMT",
            Self::SlMkt => "SL-MKT",
        }
    }
}

/// `product` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Product {
    Intraday,
    Cash,
    Monthly,
}

impl Product {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Intraday => "I",
            Self::Cash => "C",
            Self::Monthly => "M",
        }
    }
}

/// `validity` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Validity {
    Day,
    Ioc,
}

impl Validity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Day => "DAY",
            Self::Ioc => "IOC",
        }
    }
}

/// Error produced by bridge protocol validation. Mirrors the Go bridge's validation messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCommandError(pub String);

impl fmt::Display for OrderCommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OrderCommandError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
// P3-429: no `deny_unknown_fields` — the Go bridge it mirrors ignores unknown fields
// (json.Unmarshal), and the two sides are versioned by `contract_version`, so a field
// added on the Go side must reach validate() instead of failing to decode.
#[serde(rename_all = "snake_case")]
pub struct OrderCommand {
    pub exchange: String,
    pub symbol: String,
    #[serde(default)]
    pub quantity: String,
    #[serde(default)]
    pub transaction_type: String,
    #[serde(default)]
    pub order_type: String,
    #[serde(default)]
    pub product: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub price: String,
    #[serde(default)]
    pub validity: String,
    #[serde(default)]
    pub market_protection: bool,
}

impl OrderCommand {
    pub fn new(exchange: &str, symbol: &str) -> Self {
        Self {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            ..Self::default()
        }
    }

    pub fn with_side(mut self, side: TransactionType) -> Self {
        self.transaction_type = side.as_str().to_string();
        self
    }

    pub fn with_quantity(mut self, quantity: &str) -> Self {
        self.quantity = quantity.to_string();
        self
    }

    pub fn with_order_type(mut self, order_type: OrderType) -> Self {
        self.order_type = order_type.as_str().to_string();
        self
    }

    pub fn with_product(mut self, product: Product) -> Self {
        self.product = product.as_str().to_string();
        self
    }

    pub fn with_validity(mut self, validity: Validity) -> Self {
        self.validity = validity.as_str().to_string();
        self
    }

    pub fn with_price(mut self, price: &str) -> Self {
        self.price = price.to_string();
        self
    }

    /// Validates the order command against the bridge's constraints.
    ///
    /// P3-188: ported field-for-field from the Go bridge's `validateOrderCommand`
    /// (`06_execution_bridge/go-bridge/models.go`), which gained P3-044/252/253/043/045 after
    /// this port was written. Both sides must reach the same accept/reject verdict for the same
    /// envelope, so the rules and their order mirror the Go function.
    pub fn validate(&self) -> Result<(), OrderCommandError> {
        // P3-044: compare the trimmed value — comparing the raw string let " INDEX " pass the
        // emptiness check and then fail the fold, so an index slipped through as executable.
        let exchange = self.exchange.trim();
        if exchange.is_empty() || exchange.eq_ignore_ascii_case("INDEX") {
            return Err(OrderCommandError(
                "execution exchange must be non-empty and not INDEX".to_string(),
            ));
        }
        // P3-253: these fields are upper-cased without trimming downstream and the exchange is
        // forwarded verbatim, so a padded value must fail here rather than reach the venue.
        for (name, value) in [
            ("exchange", &self.exchange),
            ("order_type", &self.order_type),
            ("product", &self.product),
            ("validity", &self.validity),
        ] {
            if value.chars().any(char::is_whitespace) {
                return Err(OrderCommandError(format!(
                    "{name} must not contain whitespace"
                )));
            }
        }
        // P3-252: the symbol is forwarded verbatim, so whitespace and unbounded length used to
        // reach the venue as part of an otherwise well-formed order.
        if self.symbol.is_empty() || self.symbol.chars().any(char::is_whitespace) {
            return Err(OrderCommandError(
                "order symbol is required and must not contain whitespace".to_string(),
            ));
        }
        if self.symbol.len() > MAX_SYMBOL_LENGTH {
            return Err(OrderCommandError(format!(
                "order symbol is limited to {MAX_SYMBOL_LENGTH} characters"
            )));
        }
        if self.quantity.trim().is_empty() {
            return Err(OrderCommandError("order quantity is required".to_string()));
        }
        if !all_digits(&self.quantity) || self.quantity.trim_start_matches('0').is_empty() {
            return Err(OrderCommandError(
                "quantity must be a positive integer string".to_string(),
            ));
        }
        // P3-252: a quantity the bridge cannot represent must fail here rather than after a
        // venue round trip.
        if self.quantity.parse::<i64>().is_err() {
            return Err(OrderCommandError("quantity is out of range".to_string()));
        }
        match self.transaction_type.trim().to_uppercase().as_str() {
            "B" | "S" | "BUY" | "SELL" => {}
            _ => {
                return Err(OrderCommandError(
                    "transaction_type must be B, S, BUY, or SELL".to_string(),
                ))
            }
        }
        let order_type = self.order_type.trim().to_uppercase();
        match order_type.as_str() {
            "LMT" | "MKT" | "SL-LMT" | "SL-MKT" => {}
            _ => {
                return Err(OrderCommandError(format!(
                    "unsupported order_type {:?}",
                    self.order_type
                )))
            }
        }
        // P3-043: this bridge cannot express a stop trigger — OrderCommand has no such field and
        // the adapter never sets the SDK's trigger price, so an SL order reached the venue with
        // no trigger at all, where it is rejected or interpreted as a different instruction.
        // Refuse it here until a trigger can be carried on both sides of the protocol.
        if order_type == "SL-LMT" || order_type == "SL-MKT" {
            return Err(OrderCommandError(format!(
                "order_type {order_type} needs a stop trigger this bridge cannot carry"
            )));
        }
        // P1-191: LMT prices must be positive numbers — empty, zero, negative or non-numeric
        // prices fail fast as REJECTED and never reach the broker. Canonical digits with one
        // optional dot; at least one non-zero digit.
        if order_type == "LMT" && !price_is_positive(&self.price) {
            return Err(OrderCommandError(format!(
                "price must be a positive number for {order_type}"
            )));
        }
        // P3-045: Arrow documents price "0" for market orders, so every zero spelling
        // ("", "0", "00", "0.0") is the same valid value and is canonicalised downstream; a
        // non-zero market price stays a caller error.
        if order_type == "MKT" && !price_is_zero(&self.price) {
            return Err(OrderCommandError(
                "MKT price must be empty or zero".to_string(),
            ));
        }
        match self.product.to_uppercase().as_str() {
            "I" | "C" | "M" => {}
            _ => return Err(OrderCommandError("product must be I, C, or M".to_string())),
        }
        match self.validity.to_uppercase().as_str() {
            "DAY" | "IOC" => {}
            _ => return Err(OrderCommandError("validity must be DAY or IOC".to_string())),
        }
        Ok(())
    }
}

/// Bounds the order symbol so an oversized value fails here instead of at the venue (P3-252),
/// mirroring the Go bridge's `maxSymbolLength`.
const MAX_SYMBOL_LENGTH: usize = 64;

/// Mirrors the Go bridge's `priceIsPositive`: canonical digits with at most one dot and at least
/// one non-zero digit (exponent, sign and separators are refused).
fn price_is_positive(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }
    let (mut dots, mut digits, mut positive) = (0u32, 0u32, false);
    for c in s.chars() {
        match c {
            '.' => {
                dots += 1;
                if dots > 1 {
                    return false;
                }
            }
            '0'..='9' => {
                digits += 1;
                if c != '0' {
                    positive = true;
                }
            }
            _ => return false,
        }
    }
    digits > 0 && positive
}

/// Mirrors the Go bridge's `priceIsZero`: an absent or semantically-zero price (optional single
/// dot, digits only, no non-zero digit). Arrow documents "0" for market orders (P3-045).
fn price_is_zero(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return true;
    }
    let (mut dots, mut digits) = (0u32, 0u32);
    for c in s.chars() {
        match c {
            '.' => {
                dots += 1;
                if dots > 1 {
                    return false;
                }
            }
            '0'..='9' => {
                digits += 1;
                if c != '0' {
                    return false;
                }
            }
            _ => return false,
        }
    }
    digits > 0
}

fn all_digits(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    s.chars().all(|c| c.is_ascii_digit())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
// P3-429: same leniency as OrderCommand above, for the same reason.
#[serde(rename_all = "snake_case")]
pub struct CommandEnvelope {
    #[serde(default)]
    pub record_type: String,
    #[serde(default)]
    pub contract_version: u32,
    pub request_id: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub instruction_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub execution_attempt_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_order_ref: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_order_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<OrderCommand>,
}

impl CommandEnvelope {
    pub fn new(command: Command, request_id: &str) -> Self {
        Self {
            record_type: RECORD_COMMAND.to_string(),
            contract_version: PROTOCOL_VERSION,
            request_id: request_id.to_string(),
            command: command.as_str().to_string(),
            ..Self::default()
        }
    }

    /// Returns the typed command from the envelope's string field.
    pub fn command(&self) -> Option<Command> {
        Command::from_str(&self.command)
    }

    /// Validates the envelope against the bridge's constraints.
    pub fn validate(&self) -> Result<(), OrderCommandError> {
        if self.record_type != RECORD_COMMAND {
            return Err(OrderCommandError(
                "record_type must be execution_command".to_string(),
            ));
        }
        if self.contract_version != PROTOCOL_VERSION {
            return Err(OrderCommandError(format!(
                "unsupported contract_version {}",
                self.contract_version
            )));
        }
        if self.request_id.trim().is_empty() {
            return Err(OrderCommandError("request_id is required".to_string()));
        }
        let cmd = match Command::from_str(&self.command) {
            Some(c) => c,
            None => {
                return Err(OrderCommandError(format!(
                    "unsupported command {}",
                    self.command
                )))
            }
        };
        match cmd {
            Command::Place | Command::Modify => {
                if self.instruction_id.trim().is_empty() {
                    return Err(OrderCommandError(format!(
                        "instruction_id is required for {}",
                        cmd
                    )));
                }
                if self.execution_attempt_id.trim().is_empty() {
                    return Err(OrderCommandError(format!(
                        "execution_attempt_id is required for {}",
                        cmd
                    )));
                }
                let order = self
                    .order
                    .as_ref()
                    .ok_or_else(|| OrderCommandError("order is required".to_string()))?;
                if cmd == Command::Place && !self.broker_order_id.trim().is_empty() {
                    return Err(OrderCommandError(
                        "broker_order_id is not allowed for place".to_string(),
                    ));
                }
                if cmd == Command::Modify && self.broker_order_id.trim().is_empty() {
                    return Err(OrderCommandError(
                        "broker_order_id is required for modify".to_string(),
                    ));
                }
                order.validate()?;
                validate_client_order_ref(&self.client_order_ref)?;
            }
            Command::Cancel | Command::QueryOrder => {
                if self.broker_order_id.trim().is_empty() {
                    return Err(OrderCommandError(format!(
                        "broker_order_id is required for {}",
                        cmd
                    )));
                }
            }
            Command::ReconcileOrders | Command::ReconcileTrades | Command::ReconcilePositions => {}
        }
        Ok(())
    }
}

fn validate_client_order_ref(r: &str) -> Result<(), OrderCommandError> {
    if !(1..=16).contains(&r.len()) {
        return Err(OrderCommandError(
            "client_order_ref must contain 1-16 ASCII letters, digits, '.', '_' or '-'".to_string(),
        ));
    }
    let ok = r
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if !ok {
        return Err(OrderCommandError(
            "client_order_ref must contain 1-16 ASCII letters, digits, '.', '_' or '-'".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub struct ReportEnvelope {
    #[serde(default)]
    pub record_type: String,
    #[serde(default)]
    pub contract_version: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(default)]
    pub command: String,
    #[serde(default)]
    pub outcome: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub instruction_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub execution_attempt_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_order_ref: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_order_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub exchange_order_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub postback_event_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report_type: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fill_shares: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub average_price: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill_quantity: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fill_time: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub instrument_token: String,
    #[serde(default)]
    pub received_ts_ms: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub response_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl ReportEnvelope {
    pub fn outcome(&self) -> Option<ReportOutcome> {
        ReportOutcome::from_str(&self.outcome)
    }

    /// Returns true when this is a positive (SUCCESS) synchronised acknowledgement.
    pub fn is_success(&self) -> bool {
        self.outcome() == Some(ReportOutcome::Success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // P3-429: `deny_unknown_fields` is stricter than the Go bridge this mirrors —
    // Go's json.Unmarshal ignores unknown fields, and the protocol is gated by
    // contract_version, so a field added on the Go side must not become a serde error
    // before validate() can report the controlled version verdict.
    #[test]
    fn unknown_fields_are_ignored_like_the_go_bridge() {
        let raw = json!({
            "record_type": "execution_command",
            "contract_version": 1,
            "request_id": "req-1",
            "command": "place",
            "instruction_id": "inst-1",
            "execution_attempt_id": "att-1",
            "client_order_ref": "CLIENT-1",
            "order": {
                "exchange": "NFO",
                "symbol": "NIFTY",
                "quantity": "10",
                "transaction_type": "BUY",
                "order_type": "LMT",
                "product": "I",
                "validity": "DAY",
                "price": "100",
                "future_field": "added on the Go side after this port"
            },
            "future_envelope_field": 7
        });
        let env: CommandEnvelope = serde_json::from_value(raw).expect(
            "an unknown field must not fail decoding: Go ignores it, and version skew is contract_version's job",
        );
        assert_eq!(env.request_id, "req-1");
        assert!(
            env.validate().is_ok(),
            "lenient decoding must not weaken validation"
        );
    }

    /// P3-188: accept/reject parity with the Go bridge's hardened `validateOrderCommand`
    /// (models.go). The Rust port predates those rules, so it accepted padded symbols,
    /// `INDEX` spellings, stop-loss types, non-numeric prices and out-of-range quantities —
    /// and refused zero spellings the Go side canonicalises. Every case is reported, so one
    /// divergence cannot hide the next.
    #[test]
    fn order_validation_matches_the_go_bridge() {
        let base = || {
            OrderCommand::new("NFO", "NIFTY")
                .with_quantity("10")
                .with_side(TransactionType::Buy)
                .with_order_type(OrderType::Lmt)
                .with_product(Product::Cash)
                .with_validity(Validity::Day)
                .with_price("100")
        };
        let mkt = |price: &str| OrderCommand {
            order_type: "MKT".into(),
            price: price.into(),
            ..base()
        };
        let cases: Vec<(&str, OrderCommand, bool)> = vec![
            (
                "padded INDEX exchange",
                OrderCommand {
                    exchange: " INDEX ".into(),
                    ..base()
                },
                false,
            ),
            (
                "padded exchange",
                OrderCommand {
                    exchange: "NFO ".into(),
                    ..base()
                },
                false,
            ),
            (
                "symbol with whitespace",
                OrderCommand {
                    symbol: "NIF TY".into(),
                    ..base()
                },
                false,
            ),
            (
                "oversized symbol",
                OrderCommand {
                    symbol: "N".repeat(65),
                    ..base()
                },
                false,
            ),
            (
                "quantity out of i64 range",
                OrderCommand {
                    quantity: "99999999999999999999".into(),
                    ..base()
                },
                false,
            ),
            (
                "stop-loss order type",
                OrderCommand {
                    order_type: "SL-LMT".into(),
                    ..base()
                },
                false,
            ),
            (
                "LMT price not a number",
                OrderCommand {
                    price: "abc".into(),
                    ..base()
                },
                false,
            ),
            (
                "LMT price zero",
                OrderCommand {
                    price: "0".into(),
                    ..base()
                },
                false,
            ),
            (
                "LMT price negative",
                OrderCommand {
                    price: "-5".into(),
                    ..base()
                },
                false,
            ),
            (
                "LMT price with two dots",
                OrderCommand {
                    price: "1.2.3".into(),
                    ..base()
                },
                false,
            ),
            ("LMT canonical price", base(), true),
            (
                "LMT price with leading zeros",
                OrderCommand {
                    price: "0100".into(),
                    ..base()
                },
                true,
            ),
            ("MKT price 0.0", mkt("0.0"), true),
            ("MKT price 00", mkt("00"), true),
            ("MKT price empty", mkt(""), true),
            ("MKT price 1", mkt("1"), false),
            (
                "transaction_type padded",
                OrderCommand {
                    transaction_type: " buy ".into(),
                    ..base()
                },
                true,
            ),
            (
                "product lowercase",
                OrderCommand {
                    product: "i".into(),
                    ..base()
                },
                true,
            ),
            (
                "validity lowercase",
                OrderCommand {
                    validity: "ioc".into(),
                    ..base()
                },
                true,
            ),
            (
                "validity GTC",
                OrderCommand {
                    validity: "GTC".into(),
                    ..base()
                },
                false,
            ),
            (
                "bare INDEX",
                OrderCommand {
                    exchange: "INDEX".into(),
                    ..base()
                },
                false,
            ),
            (
                "quantity zero",
                OrderCommand {
                    quantity: "0".into(),
                    ..base()
                },
                false,
            ),
            (
                "quantity with leading zeros",
                OrderCommand {
                    quantity: "007".into(),
                    ..base()
                },
                true,
            ),
            (
                "quantity padded",
                OrderCommand {
                    quantity: " 10".into(),
                    ..base()
                },
                false,
            ),
        ];
        let mut wrong: Vec<String> = Vec::new();
        for (name, cmd, want_ok) in cases {
            let got = cmd.validate();
            if got.is_ok() != want_ok {
                wrong.push(format!("{name}: got {got:?}"));
            }
        }
        assert!(
            wrong.is_empty(),
            "{} case(s) diverge from the Go bridge: {wrong:#?}",
            wrong.len()
        );
    }

    #[test]
    fn order_command_round_trips_through_json() {
        let o = OrderCommand::new("NFO", "NIFTY")
            .with_quantity("10")
            .with_side(TransactionType::Buy)
            .with_order_type(OrderType::Lmt)
            .with_product(Product::Cash)
            .with_validity(Validity::Day)
            .with_price("100");
        o.validate().unwrap();
        let v = serde_json::to_value(&o).unwrap();
        assert_eq!(v["exchange"], "NFO");
        assert_eq!(v["transaction_type"], "BUY");
        assert_eq!(v["order_type"], "LMT");
        assert_eq!(v["product"], "C");
        assert_eq!(v["validity"], "DAY");
        // MKT with a price is invalid
        let bad = OrderCommand::new("NFO", "NIFTY")
            .with_quantity("10")
            .with_side(TransactionType::Buy)
            .with_order_type(OrderType::Mkt)
            .with_product(Product::Cash)
            .with_validity(Validity::Day)
            .with_price("100");
        assert!(bad.validate().is_err(), "MKT must not carry a price");
    }

    #[test]
    fn command_envelope_validation_matches_go() {
        // place requires instruction_id, execution_attempt_id, order
        let env = CommandEnvelope::new(Command::Place, "req-1");
        assert!(env.validate().is_err());

        let env = CommandEnvelope {
            instruction_id: "inst-1".into(),
            execution_attempt_id: "att-1".into(),
            client_order_ref: "CLIENT-1".into(),
            order: Some(
                OrderCommand::new("NFO", "NIFTY")
                    .with_quantity("10")
                    .with_side(TransactionType::Buy)
                    .with_order_type(OrderType::Lmt)
                    .with_product(Product::Cash)
                    .with_validity(Validity::Day)
                    .with_price("100"),
            ),
            ..env
        };
        assert!(env.validate().is_ok());
    }

    #[test]
    fn report_envelope_outcome_parses() {
        let r: ReportEnvelope = serde_json::from_value(json!({
            "record_type": "execution_report",
            "contract_version": 1,
            "request_id": "req-1",
            "command": "place",
            "outcome": "SUCCESS",
            "received_ts_ms": 1
        }))
        .unwrap();
        assert!(r.is_success());
    }
}
