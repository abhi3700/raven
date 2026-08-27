//! Alloy-backed JSON-RPC source for Raven.
//!
//! This crate is the boundary between an EVM RPC node and Raven's
//! source-independent runtime model. Alloy owns the transport client and native
//! RPC response types; Raven owns canonical ordering, reorg semantics, and the
//! normalized `ChainEvent` values sent to its runtime.
//!
//! ```text
//! EVM JSON-RPC node
//!        |
//!        v
//! Alloy Provider + RPC types
//!        |
//!        v
//! source.rs
//!   - select HTTP polling or WebSocket subscriptions
//!   - preserve a canonical cursor across reconnects
//!   - reconcile remembered ancestry with the RPC canonical chain
//!   - emit applied and reverted blocks in dependency order
//!        |
//!        v
//! converter.rs
//!   - strip away Alloy-specific block representation
//!   - build Raven's validated BlockEvent/ChainEvent model
//!        |
//!        v
//! Raven runtime and plugins
//! ```
//!
//! The useful mental split is:
//!
//! - `source.rs` decides when to connect, which block heights Raven still needs, and whether
//!   retained blocks must be reverted.
//! - `converter.rs` decides how an Alloy block becomes Raven's normalized event representation.

mod converter;
mod error;
mod source;

pub use error::{AlloySourceError, AlloySourceResult};
pub use source::{
	AlloySource, RetryPolicy, RpcEndpointInfo, RpcTransport, SourceStart, inspect_rpc_endpoint,
};
