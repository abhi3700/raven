//! Actor-like plugin execution for Raven chain events.
//!
//! `raven-runtime` sits between validated `raven-core` events and user-defined
//! `raven-plugin-sdk::Plugin` implementations. The public API is intentionally
//! small: callers build a `Runtime`, register plugins, start worker tasks,
//! process events, subscribe to live outcomes, and shut everything down.
//!
//! ```text
//! ChainEvent
//!    |
//!    v
//! Runtime
//!    |
//!    v
//! Dispatcher
//!    |
//!    +-- mailbox A -> worker A -> Plugin A -> outcome
//!    +-- mailbox B -> worker B -> Plugin B -> outcome
//!    +-- mailbox C -> worker C -> Plugin C -> outcome
//!                              |
//!                              v
//!                    broadcast outcome bus
//! ```
//!
//! The runtime guarantees FIFO processing inside one plugin worker while
//! allowing different plugins to run independently. Delivery receipts report
//! whether each bounded mailbox accepted an event; plugin outcomes report later
//! handler results.
//!
//! The main reading path is:
//!
//! ```text
//! runtime.rs -> registry.rs -> dispatch.rs -> dispatcher.rs
//! ```

mod dispatch;
mod dispatcher;
mod error;
mod registry;
mod runtime;

pub use dispatch::{
	DispatchId, DispatchReceipt, PluginDelivery, PluginDeliveryFailure, PluginDeliveryStatus,
	PluginHealth, PluginHealthStatus, PluginOutcome, PluginOutcomeStatus,
};
pub use error::{RuntimeError, RuntimeResult};
pub use runtime::{
	DEFAULT_OUTCOME_CHANNEL_CAPACITY, DEFAULT_PLUGIN_MAILBOX_CAPACITY, PluginTimeouts, Runtime,
};
