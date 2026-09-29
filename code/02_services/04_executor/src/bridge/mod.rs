//! Bridge client layer.
//!
//! The bridge is the single Arrow-facing component. This module mirrors the Go bridge's private
//! wire protocol (`code/02_services/06_execution_bridge/go-bridge/models.go`) in `protocol`, and
//! provides a `BridgeClient` seam with a deterministic in-process fake bridge for offline testing.

pub mod client;
pub mod fake;
pub mod protocol;
pub mod session;
pub mod transport;

pub use client::{BridgeClient, BridgeReportStream, SendFailure};
pub use fake::{CommandScript, FakeBridge};
pub use protocol::{
    Command, CommandEnvelope, OrderCommand, OrderCommandError, OrderType, Product, ReportEnvelope,
    ReportOutcome, TransactionType, Validity, PROTOCOL_VERSION, RECORD_COMMAND, RECORD_REPORT,
};
pub use session::{BridgeSession, Registry, ReportOwner, RouteContext, SessionHandle};
pub use transport::HttpBridgeClient;
