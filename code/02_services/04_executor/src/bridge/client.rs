//! `BridgeClient` trait: the single seam between the Nautilus execution client and the Go bridge.

use anyhow::Result;
use async_trait::async_trait;

use super::protocol::{CommandEnvelope, ReportEnvelope};

/// An asynchronous report stream produced by a bridge (fills, order-state updates, rejections).
/// The bridge's report stream. Bounded (see [`BRIDGE_REPORT_BUFFER`]) so a stalled consumer
/// applies backpressure to the producer instead of letting it queue reports without limit.
pub type BridgeReportStream = tokio::sync::mpsc::Receiver<ReportEnvelope>;

/// Capacity of the bridge's report channel: headroom for a burst of reports while the consumer
/// drains, and a hard bound on what a stalled consumer can accumulate.
pub const BRIDGE_REPORT_BUFFER: usize = 4096;

/// A client of the Go bridge.
///
/// T4 scope is the in-process `FakeBridge` (default offline slice); the production HTTP/WS
/// adapter (`bridge/transport.rs`) implements the full contract including the `/v1/events`
/// report intake with reconnect. Every implementation must be usable from a single-threaded
/// (non-`Send`)
/// Nautilus runtime context, so report consumption is exposed as an owned receiver that a
/// caller-owned task processes.
#[async_trait]
pub trait BridgeClient {
    /// Whether the client currently holds a live bridge connection.
    fn is_connected(&self) -> bool;

    /// Establishes the bridge connection. Idempotent.
    async fn connect(&mut self) -> Result<()>;

    /// Tears down the bridge connection. Idempotent.
    async fn disconnect(&mut self) -> Result<()>;

    /// Sends a command envelope and returns the synchronously produced report envelope.
    async fn send_command(&mut self, envelope: CommandEnvelope) -> Result<ReportEnvelope>;

    /// Hands over the receiver for asynchronous bridge reports (fills, rejects, order updates).
    ///
    /// Returns `None` if the stream has already been taken. The caller owns the returned receiver.
    fn take_reports(&mut self) -> Option<BridgeReportStream>;
}
