//! Nautilus custom execution client.
//!
//! This module contains the custom `ExecutionClient` implementation, [`BridgeExecutionClient`],
//! which is the long-lived execution/position authority of the service. It is pinned to the
//! audited Nautilus Rust APIs (`ExecutionClientCore` + `ExecutionEventEmitter`), boots into the
//! safety gate `HALTED` state, connects to the in-process `FakeBridge`, and drives the order
//! lifecycle (place/modify/cancel), fills, positions, and safety-halt/restart event handling.

pub mod client;

pub use client::BridgeExecutionClient;
