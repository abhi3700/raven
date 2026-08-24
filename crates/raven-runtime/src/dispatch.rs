use std::{sync::Arc, time::Duration};

use raven_core::ChainEvent;
use raven_plugin_sdk::PluginError;

/// Monotonically increasing identifier assigned to one runtime dispatch.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginDeliveryFailure {
	/// The plugin's bounded mailbox had no remaining capacity.
	MailboxFull,

	/// The plugin worker had already stopped and closed its mailbox.
	WorkerStopped,
}

/// Result of attempting to enqueue an event for one plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginDeliveryStatus {
	/// The event was accepted by the plugin's mailbox.
	Accepted,

	/// The event was not enqueued.
	Rejected(PluginDeliveryFailure),
}

/// Per-plugin delivery result returned immediately by the dispatcher.
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
#[derive(Debug)]
pub enum PluginOutcomeStatus {
	/// The plugin handled the event successfully.
	Succeeded,

	/// The plugin returned a normal Rust error. Its worker remains available.
	Failed(PluginError),

	/// The plugin panicked while handling the event and its worker stopped.
	Panicked(String),

	/// The dispatcher could not enqueue the event for this plugin.
	Rejected(PluginDeliveryFailure),
}

/// One plugin's independently emitted result for one dispatched event.
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
