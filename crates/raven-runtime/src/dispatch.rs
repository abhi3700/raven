//! Public vocabulary for event delivery, execution outcomes, and worker health.
//!
//! Runtime processing has two observable stages:
//!
//! ```text
//! Runtime::process(event)
//!        |
//!        v
//! DispatchReceipt
//!   immediate delivery report:
//!   did each mailbox accept the event?
//!
//! Plugin workers later emit:
//!
//! PluginOutcome
//!   asynchronous execution report:
//!   what happened inside one plugin handler?
//! ```
//!
//! A shared `DispatchId` ties both stages together. Rejected deliveries are
//! represented in both places: the receipt lets the caller react immediately,
//! and a zero-duration rejected outcome keeps outcome subscribers' accounting
//! complete.

use raven_core::ChainEvent;
use raven_plugin_sdk::PluginError;
use std::{sync::Arc, time::Duration};

/// Monotonically increasing identifier assigned to one runtime dispatch.
///
/// One call to `Runtime::process` gets one ID, and every plugin delivery/outcome
/// related to that fan-out carries the same value.
///
/// ```text
/// process(E1) -> DispatchId(1) -> outcomes from all plugins for E1
/// process(E2) -> DispatchId(2) -> outcomes from all plugins for E2
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DispatchId(u64);

impl DispatchId {
	pub(crate) const fn new(value: u64) -> Self {
		Self(value)
	}

	/// Returns the numeric dispatch identifier.
	pub const fn get(self) -> u64 {
		self.0
	}
}

/// Reason an event could not be enqueued for one plugin.
///
/// Rejections are per-plugin. A full or stopped worker does not prevent the
/// dispatcher from attempting sibling plugin mailboxes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginDeliveryFailure {
	/// The plugin's bounded mailbox had no remaining capacity.
	MailboxFull,

	/// The plugin worker had already stopped and closed its mailbox.
	WorkerStopped,
}

/// Result of attempting to enqueue an event for one plugin.
///
/// This is a mailbox result, not a handler result.
///
/// ```text
/// Accepted  -> worker will later emit Succeeded, Failed, Panicked, TimedOut,
///              or Rejected(WorkerStopped) if it dies before queued work runs
/// Rejected  -> event never entered that plugin mailbox
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginDeliveryStatus {
	/// The event was accepted by the plugin's mailbox.
	Accepted,

	/// The event was not enqueued.
	Rejected(PluginDeliveryFailure),
}

/// Per-plugin delivery result returned immediately by the dispatcher.
///
/// ```text
/// PluginDelivery
///   plugin = "block-logger"
///   status = Accepted | Rejected(...)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginDelivery {
	plugin: &'static str,
	status: PluginDeliveryStatus,
}

impl PluginDelivery {
	pub(crate) const fn new(plugin: &'static str, status: PluginDeliveryStatus) -> Self {
		Self { plugin, status }
	}

	/// Returns the plugin metadata name.
	pub const fn plugin(&self) -> &'static str {
		self.plugin
	}

	/// Returns whether the plugin mailbox accepted the event.
	pub const fn status(&self) -> PluginDeliveryStatus {
		self.status
	}

	/// Returns whether the event was accepted.
	pub const fn is_accepted(&self) -> bool {
		matches!(self.status, PluginDeliveryStatus::Accepted)
	}
}

/// Immediate receipt produced after Raven attempts non-blocking fan-out.
///
/// A receipt is returned before any plugin handler is awaited.
///
/// ```text
/// DispatchReceipt
///   |
///   +-- DispatchId(7)
///   +-- plugin A: Accepted
///   +-- plugin B: Rejected(MailboxFull)
///   +-- plugin C: Accepted
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchReceipt {
	dispatch_id: DispatchId,
	deliveries: Vec<PluginDelivery>,
}

impl DispatchReceipt {
	pub(crate) fn new(dispatch_id: DispatchId, deliveries: Vec<PluginDelivery>) -> Self {
		Self { dispatch_id, deliveries }
	}

	/// Returns the identifier shared by every outcome for this dispatch.
	pub const fn dispatch_id(&self) -> DispatchId {
		self.dispatch_id
	}

	/// Returns the delivery result for every registered plugin.
	pub fn deliveries(&self) -> &[PluginDelivery] {
		&self.deliveries
	}

	/// Returns whether every plugin mailbox accepted the event.
	pub fn all_accepted(&self) -> bool {
		self.deliveries.iter().all(PluginDelivery::is_accepted)
	}

	/// Returns the number of plugins that accepted the event.
	pub fn accepted_count(&self) -> usize {
		self.deliveries.iter().filter(|delivery| delivery.is_accepted()).count()
	}

	/// Returns the number of plugins that rejected the event.
	pub fn rejected_count(&self) -> usize {
		self.deliveries.len() - self.accepted_count()
	}
}

/// Asynchronous completion status emitted by an independent plugin worker.
///
/// Normal handler errors stay at event scope and the worker continues. Panics
/// and timeouts move the worker to quarantine because the plugin's mutable state
/// may no longer be safe to reuse.
///
/// ```text
/// handle_event
///   |
///   +-- Ok(())      -> Succeeded, continue
///   +-- Err(_)      -> Failed, continue
///   +-- panic       -> Panicked, quarantine
///   +-- timeout     -> TimedOut, quarantine
///   +-- not queued  -> Rejected
/// ```
#[derive(Debug)]
pub enum PluginOutcomeStatus {
	/// The plugin handled the event successfully.
	Succeeded,

	/// The plugin returned a normal Rust error. Its worker remains available.
	Failed(PluginError),

	/// The plugin panicked while handling the event and its worker stopped.
	Panicked(String),

	/// The handler exceeded its configured deadline and its worker stopped.
	TimedOut(Duration),

	/// The dispatcher could not enqueue the event for this plugin.
	Rejected(PluginDeliveryFailure),
}

/// Current lifecycle state of one independent plugin worker.
///
/// ```text
/// Starting -> Healthy -> Stopped
///      \        |
///       \       +-- panic, timeout, or lifecycle failure
///        \          |
///         +-------> Quarantined
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginHealthStatus {
	/// The startup hook is still running.
	Starting,
	/// The worker is accepting and processing events.
	Healthy,
	/// The worker was isolated after a panic, timeout, or lifecycle failure.
	Quarantined,
	/// The worker completed its shutdown hook.
	Stopped,
}

/// Point-in-time health information for one plugin worker.
///
/// This is a snapshot read from the dispatcher's worker handles. It is not a
/// stream and it does not replace the live outcome broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginHealth {
	plugin: &'static str,
	status: PluginHealthStatus,
}

impl PluginHealth {
	pub(crate) const fn new(plugin: &'static str, status: PluginHealthStatus) -> Self {
		Self { plugin, status }
	}

	/// Returns the plugin metadata name.
	pub const fn plugin(&self) -> &'static str {
		self.plugin
	}

	/// Returns the worker's current lifecycle state.
	pub const fn status(&self) -> PluginHealthStatus {
		self.status
	}
}

/// One plugin's independently emitted result for one dispatched event.
///
/// Outcomes are published over a broadcast channel as each worker finishes. A
/// slow subscriber can lag without applying backpressure to plugin execution.
///
/// ```text
/// PluginOutcome
///   dispatch_id -> correlates with DispatchReceipt
///   plugin      -> which worker handled it
///   event       -> shared ChainEvent
///   status      -> execution or rejection result
///   elapsed     -> handler duration, or zero for delivery rejection
/// ```
#[derive(Debug)]
pub struct PluginOutcome {
	dispatch_id: DispatchId,
	plugin: &'static str,
	event: Arc<ChainEvent>,
	status: PluginOutcomeStatus,
	elapsed: Duration,
}

impl PluginOutcome {
	pub(crate) fn new(
		dispatch_id: DispatchId,
		plugin: &'static str,
		event: Arc<ChainEvent>,
		status: PluginOutcomeStatus,
		elapsed: Duration,
	) -> Self {
		Self { dispatch_id, plugin, event, status, elapsed }
	}

	/// Returns the dispatch identifier shared with the immediate receipt.
	pub const fn dispatch_id(&self) -> DispatchId {
		self.dispatch_id
	}

	/// Returns the plugin metadata name.
	pub const fn plugin(&self) -> &'static str {
		self.plugin
	}

	/// Returns the normalized event associated with the outcome.
	pub fn event(&self) -> &ChainEvent {
		&self.event
	}

	/// Returns the worker's completion or rejection status.
	pub const fn status(&self) -> &PluginOutcomeStatus {
		&self.status
	}

	/// Returns the time spent inside `Plugin::handle_event`.
	///
	/// Dispatcher-side rejections report a zero duration.
	pub const fn elapsed(&self) -> Duration {
		self.elapsed
	}
}
