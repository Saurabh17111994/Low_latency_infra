//! `BridgeClient` trait: the single seam between the Nautilus execution client and the Go bridge.

use anyhow::Result;
use async_trait::async_trait;

use super::protocol::{CommandEnvelope, ReportEnvelope};

/// Why a `send_command` call produced no report envelope (H1-2).
///
/// The distinction is the *only* thing that decides whether a retry is safe: `NotSent` means the
/// command provably never reached the bridge's dispatch point (nothing was serialized, connected,
/// or accepted for processing), so re-invoking cannot double-execute. `Unknown` means bytes may
/// have reached the bridge — or the bridge may already have dispatched the command — and the
/// outcome cannot be resolved locally; re-invoking could double-execute a money-moving command, so
/// the executor must halt and let reconciliation resolve it instead.
///
/// Both variants carry the underlying error for the operator log; their *class* is the contract,
/// never the error string (a string classifier is what allowed a timeout to be retried, P0-3).
#[derive(Debug)]
pub enum SendFailure {
    /// The command did not leave this process toward the broker; bounded retry is allowed.
    NotSent(anyhow::Error),
    /// The command may have reached the broker; never retry — halt and reconcile.
    Unknown(anyhow::Error),
}

impl SendFailure {
    /// A failure that provably happened before any byte reached the bridge.
    pub fn not_sent(error: impl Into<anyhow::Error>) -> Self {
        Self::NotSent(error.into())
    }

    /// A failure whose outcome cannot be resolved locally.
    pub fn unknown(error: impl Into<anyhow::Error>) -> Self {
        Self::Unknown(error.into())
    }

    /// Whether the command provably did not leave this process (retry-safe by construction).
    #[must_use]
    pub fn is_not_sent(&self) -> bool {
        matches!(self, Self::NotSent(_))
    }

    /// The underlying error, for logging / response bodies.
    #[must_use]
    pub fn source_error(&self) -> &anyhow::Error {
        match self {
            Self::NotSent(e) | Self::Unknown(e) => e,
        }
    }
}

impl std::fmt::Display for SendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSent(e) => write!(f, "command not sent: {e}"),
            Self::Unknown(e) => write!(f, "send outcome unknown: {e}"),
        }
    }
}

impl std::error::Error for SendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source_error().as_ref())
    }
}

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
    ///
    /// A transport failure is classified as [`SendFailure::NotSent`] (safe to retry) or
    /// [`SendFailure::Unknown`] (never retry — the caller must halt). A broker decision
    /// (accept/reject) is an `Ok` report either way, never a transport failure.
    async fn send_command(
        &mut self,
        envelope: CommandEnvelope,
    ) -> Result<ReportEnvelope, SendFailure>;

    /// Hands over the receiver for asynchronous bridge reports (fills, rejects, order updates).
    ///
    /// Returns `None` if the stream has already been taken. The caller owns the returned receiver.
    fn take_reports(&mut self) -> Option<BridgeReportStream>;
}
