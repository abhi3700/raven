//! Public runtime facade and lifecycle policy.
//!
//! This module owns the API surface most users interact with. It validates
//! lifecycle state, keeps the runtime bound to one chain, and delegates actual
//! worker orchestration to `dispatcher.rs`.
//!
//! ```text
//! user code
//!    |
//!    v
//! Runtime
//!   state: Created | Started | Shutdown
//!   context: PluginContext(chain_id)
//!   dispatcher: worker execution engine
//! ```

use std::{sync::Arc, time::Duration};

use tokio::sync::broadcast;

use crate::{
	DispatchReceipt, PluginHealth, PluginOutcome, RuntimeError, RuntimeResult,
	dispatcher::Dispatcher,
};
use raven_core::{ChainEvent, ChainId};
use raven_plugin_sdk::{Plugin, PluginContext};

/// Default number of queued events allowed per independent plugin worker.
pub const DEFAULT_PLUGIN_MAILBOX_CAPACITY: usize = 256;

/// Default number of live plugin outcomes retained for each subscriber.
pub const DEFAULT_OUTCOME_CHANNEL_CAPACITY: usize = 1_024;

/// Bounded deadlines for every plugin lifecycle hook.
///
/// Every plugin hook runs behind a timeout so one plugin cannot block startup,
/// event handling, or shutdown forever.
///
/// ```text
/// PluginTimeouts
///   startup  -> Plugin::start
///   handler  -> Plugin::handle_event
///   shutdown -> Plugin::shutdown
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginTimeouts {
	/// Maximum duration of `Plugin::start`.
	pub startup: Duration,
	/// Maximum duration of one `Plugin::handle_event` call.
	pub handler: Duration,
	/// Maximum duration of `Plugin::shutdown`.
	pub shutdown: Duration,
}

impl Default for PluginTimeouts {
	fn default() -> Self {
		Self {
			startup: Duration::from_secs(30),
			handler: Duration::from_secs(60),
			shutdown: Duration::from_secs(30),
		}
	}
}

/// Hosts Raven plugins and processes normalized chain events.
///
/// `Runtime` is the public policy layer:
///
/// ```text
/// Runtime
///   |
///   +-- accepts plugins only before start
///   +-- starts all workers as one readiness barrier
///   +-- validates every event belongs to its chain
///   +-- returns immediate delivery receipts
///   +-- exposes live outcome subscribers and health snapshots
///   +-- shuts down all workers as one cleanup barrier
/// ```
#[derive(Debug)]
pub struct Runtime {
	dispatcher: Dispatcher,
	context: PluginContext,
	state: RuntimeState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeState {
	/// Plugins may still be registered and configuration may still change.
	Created,
	/// Workers are running and `process` may dispatch events.
	Started,
	/// Startup failed or shutdown completed; the runtime is terminal.
	Shutdown,
}

impl Runtime {
	/// Creates a runtime for one EVM chain.
	///
	/// The chain ID is copied into every plugin's `PluginContext`, and every
	/// event passed to `process` must match it.
	pub fn new(chain_id: ChainId) -> Self {
		Self::with_capacities(
			chain_id,
			DEFAULT_PLUGIN_MAILBOX_CAPACITY,
			DEFAULT_OUTCOME_CHANNEL_CAPACITY,
		)
	}

	/// Creates a runtime with a custom bounded mailbox capacity per plugin.
	///
	/// The capacity applies independently to each worker:
	///
	/// ```text
	/// Plugin A mailbox capacity = N
	/// Plugin B mailbox capacity = N
	/// Plugin C mailbox capacity = N
	/// ```
	pub fn with_plugin_mailbox_capacity(
		chain_id: ChainId,
		mailbox_capacity: usize,
	) -> RuntimeResult<Self> {
		if mailbox_capacity == 0 {
			return Err(RuntimeError::InvalidPluginMailboxCapacity);
		}

		Ok(Self::with_capacities(chain_id, mailbox_capacity, DEFAULT_OUTCOME_CHANNEL_CAPACITY))
	}

	fn with_capacities(
		chain_id: ChainId,
		mailbox_capacity: usize,
		outcome_capacity: usize,
	) -> Self {
		Self {
			dispatcher: Dispatcher::new(
				mailbox_capacity,
				outcome_capacity,
				PluginTimeouts::default(),
			),
			context: PluginContext::new(chain_id),
			state: RuntimeState::Created,
		}
	}

	/// Overrides the bounded deadlines used for every plugin lifecycle hook.
	///
	/// This must be called before `start` because the dispatcher copies the
	/// timeout values into each worker environment.
	pub fn with_plugin_timeouts(mut self, timeouts: PluginTimeouts) -> RuntimeResult<Self> {
		if self.state != RuntimeState::Created {
			return Err(RuntimeError::TimeoutConfigurationAfterStart);
		}
		if timeouts.startup.is_zero() || timeouts.handler.is_zero() || timeouts.shutdown.is_zero() {
			return Err(RuntimeError::InvalidPluginTimeout);
		}

		self.dispatcher.set_timeouts(timeouts);
		Ok(self)
	}

	/// Registers a plugin.
	///
	/// Plugins must be registered before the runtime starts.
	///
	/// ```text
	/// Created runtime
	///    |
	///    +-- register_plugin(A)
	///    +-- register_plugin(B)
	///    v
	/// PluginRegistry owns A and B until start()
	/// ```
	pub fn register_plugin<P>(&mut self, plugin: P) -> RuntimeResult<&mut Self>
	where
		P: Plugin + 'static,
	{
		if self.state != RuntimeState::Created {
			return Err(RuntimeError::RegistrationAfterStart);
		}

		self.dispatcher.register(Box::new(plugin))?;

		Ok(self)
	}

	/// Starts all registered plugins.
	///
	/// All plugin workers are spawned first, then the runtime waits for every
	/// startup readiness signal. A single startup failure prevents the runtime
	/// from entering `Started`.
	///
	/// ```text
	/// Created
	///   |
	///   v
	/// spawn all workers -> wait for all ready
	///   |
	///   +-- all ready -> Started
	///   +-- any error -> cleanup -> Shutdown
	/// ```
	pub async fn start(&mut self) -> RuntimeResult {
		match self.state {
			RuntimeState::Created => {},
			RuntimeState::Started => {
				return Err(RuntimeError::AlreadyStarted);
			},
			RuntimeState::Shutdown => {
				return Err(RuntimeError::AlreadyShutdown);
			},
		}

		if let Err(error) = self.dispatcher.start(self.context).await {
			self.state = RuntimeState::Shutdown;
			return Err(error);
		}

		self.state = RuntimeState::Started;

		Ok(())
	}

	/// Processes one normalized chain event.
	///
	/// This method validates state and chain ID, then returns after mailbox
	/// delivery attempts. It does not wait for plugin handlers.
	///
	/// ```text
	/// process(event)
	///   |
	///   +-- validate runtime is Started
	///   +-- validate event.chain_id == runtime.chain_id
	///   +-- non-blocking dispatch
	///   +-- return DispatchReceipt
	/// ```
	pub async fn process(&mut self, event: ChainEvent) -> RuntimeResult<DispatchReceipt> {
		match self.state {
			RuntimeState::Created => {
				return Err(RuntimeError::NotStarted);
			},
			RuntimeState::Started => {},
			RuntimeState::Shutdown => {
				return Err(RuntimeError::AlreadyShutdown);
			},
		}

		self.validate_event_chain(&event)?;

		self.dispatcher.dispatch(event)
	}

	/// Shuts down all registered plugins.
	///
	/// Shutdown closes all worker mailboxes, lets workers drain accepted events,
	/// then waits for every worker task to complete.
	///
	/// ```text
	/// Started
	///   |
	///   v
	/// close mailboxes -> workers drain -> Plugin::shutdown -> Shutdown
	/// ```
	pub async fn shutdown(&mut self) -> RuntimeResult {
		match self.state {
			RuntimeState::Created => {
				return Err(RuntimeError::NotStarted);
			},
			RuntimeState::Started => {},
			RuntimeState::Shutdown => {
				return Err(RuntimeError::AlreadyShutdown);
			},
		}

		let result = self.dispatcher.shutdown().await;
		self.state = RuntimeState::Shutdown;

		result
	}

	/// Subscribes to plugin results as independent workers emit them.
	///
	/// Subscribe before dispatching if every outcome must be observed. A lagging
	/// subscriber receives [`broadcast::error::RecvError::Lagged`] rather than
	/// applying backpressure to plugin workers.
	pub fn subscribe_outcomes(&self) -> broadcast::Receiver<Arc<PluginOutcome>> {
		self.dispatcher.subscribe_outcomes()
	}

	/// Returns the chain ID used by the runtime.
	pub const fn chain_id(&self) -> ChainId {
		self.context.chain_id()
	}

	/// Returns the number of registered plugins.
	///
	/// Before startup this counts registry entries. After startup this counts
	/// worker handles.
	pub fn plugin_count(&self) -> usize {
		self.dispatcher.plugin_count()
	}

	/// Returns whether no plugins are registered.
	pub fn has_no_plugins(&self) -> bool {
		self.dispatcher.is_empty()
	}

	/// Returns a point-in-time health snapshot for every running worker.
	///
	/// A newly created runtime has no worker health because no workers exist until
	/// startup moves plugins out of the registry.
	pub fn plugin_health(&self) -> Vec<PluginHealth> {
		self.dispatcher.plugin_health()
	}

	/// Returns whether the runtime has started.
	pub const fn is_started(&self) -> bool {
		matches!(self.state, RuntimeState::Started)
	}

	fn validate_event_chain(&self, event: &ChainEvent) -> RuntimeResult {
		let expected = self.context.chain_id();
		let actual = event.chain_id();

		if expected != actual {
			return Err(RuntimeError::ChainIdMismatch {
				expected: expected.get(),
				actual: actual.get(),
			});
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{DispatchId, PluginDeliveryFailure, PluginDeliveryStatus, PluginOutcomeStatus};
	use alloy_primitives::B256;
	use async_trait::async_trait;
	use raven_core::{BlockEvent, ChainEvent, ChainId};
	use raven_plugin_sdk::{Plugin, PluginContext, PluginError, PluginMetadata, PluginResult};
	use std::sync::{
		Arc, Mutex,
		atomic::{AtomicUsize, Ordering},
	};
	use tokio::{
		sync::{Barrier, Notify, broadcast},
		time::{Duration, timeout},
	};

	const TEST_TIMEOUT: Duration = Duration::from_secs(2);

	struct LifecyclePlugin {
		starts: Arc<AtomicUsize>,
		events: Arc<AtomicUsize>,
		shutdowns: Arc<AtomicUsize>,
	}

	struct StartupBarrierPlugin {
		name: &'static str,
		barrier: Arc<Barrier>,
	}

	#[async_trait]
	impl Plugin for StartupBarrierPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new(self.name, "0.1.0", "Waits for another plugin during startup")
		}

		async fn start(&mut self, _context: &PluginContext) -> PluginResult {
			self.barrier.wait().await;
			Ok(())
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			Ok(())
		}
	}

	struct ShutdownBarrierPlugin {
		name: &'static str,
		barrier: Arc<Barrier>,
	}

	#[async_trait]
	impl Plugin for ShutdownBarrierPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new(self.name, "0.1.0", "Waits for another plugin during shutdown")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			Ok(())
		}

		async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
			self.barrier.wait().await;
			Ok(())
		}
	}

	struct GatePlugin {
		name: &'static str,
		entered: Arc<Notify>,
		release: Arc<Notify>,
		block_next_event: bool,
	}

	#[async_trait]
	impl Plugin for GatePlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new(self.name, "0.1.0", "Blocks its first event until released")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			if self.block_next_event {
				self.block_next_event = false;
				self.entered.notify_one();
				self.release.notified().await;
			}

			Ok(())
		}
	}

	struct SuccessPlugin {
		name: &'static str,
	}

	#[async_trait]
	impl Plugin for SuccessPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new(self.name, "0.1.0", "Always succeeds")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			Ok(())
		}
	}

	struct FailingPlugin;

	#[async_trait]
	impl Plugin for FailingPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("failing", "0.1.0", "Returns an error for every event")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			Err(PluginError::EventProcessing("expected failure".to_owned()))
		}
	}

	struct PanicAfterGatePlugin {
		entered: Arc<Notify>,
		release: Arc<Notify>,
	}

	#[async_trait]
	impl Plugin for PanicAfterGatePlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("panicking", "0.1.0", "Panics after a test gate opens")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			self.entered.notify_one();
			self.release.notified().await;
			panic!("expected plugin panic");
		}
	}

	struct OrderingPlugin {
		observed: Arc<Mutex<Vec<u64>>>,
	}

	struct HangingHandlerPlugin;
	struct HangingStartupPlugin;

	#[async_trait]
	impl Plugin for HangingStartupPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("hanging-startup", "0.1.0", "Never finishes startup")
		}

		async fn start(&mut self, _context: &PluginContext) -> PluginResult {
			std::future::pending().await
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			Ok(())
		}
	}

	#[async_trait]
	impl Plugin for HangingHandlerPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("hanging-handler", "0.1.0", "Never finishes a handler")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			std::future::pending().await
		}
	}

	struct ShutdownFailurePlugin {
		name: &'static str,
	}

	struct HangingShutdownPlugin;

	#[async_trait]
	impl Plugin for HangingShutdownPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("hanging-shutdown", "0.1.0", "Never finishes shutdown")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			Ok(())
		}

		async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
			std::future::pending().await
		}
	}

	#[async_trait]
	impl Plugin for ShutdownFailurePlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new(self.name, "0.1.0", "Fails during shutdown")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			Ok(())
		}

		async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
			Err(PluginError::Other(format!("{} shutdown failed", self.name)))
		}
	}

	#[async_trait]
	impl Plugin for OrderingPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("ordering", "0.1.0", "Records event order")
		}

		async fn handle_event(
			&mut self,
			event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			self.observed
				.lock()
				.expect("ordering lock should not be poisoned")
				.push(event.block_number());

			Ok(())
		}
	}

	#[async_trait]
	impl Plugin for LifecyclePlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("lifecycle-plugin", "0.1.0", "Tracks plugin lifecycle calls")
		}

		async fn start(&mut self, _context: &PluginContext) -> PluginResult {
			self.starts.fetch_add(1, Ordering::SeqCst);

			Ok(())
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			self.events.fetch_add(1, Ordering::SeqCst);

			Ok(())
		}

		async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
			self.shutdowns.fetch_add(1, Ordering::SeqCst);

			Ok(())
		}
	}

	fn test_chain_id() -> ChainId {
		ChainId::new(8_453).expect("test chain ID should be valid")
	}

	fn test_event() -> ChainEvent {
		event_at(21_000_000)
	}

	fn event_at(block_number: u64) -> ChainEvent {
		event_for_chain(test_chain_id(), block_number)
	}

	fn event_for_chain(chain_id: ChainId, block_number: u64) -> ChainEvent {
		let block = BlockEvent::new(
			chain_id,
			block_number,
			format!("0x{block_number:064x}").parse::<B256>().unwrap(),
			format!("0x{:064x}", block_number.saturating_sub(1)).parse::<B256>().unwrap(),
			1_720_000_000,
			150,
		)
		.expect("block should be valid");

		ChainEvent::BlockApplied(block)
	}

	async fn next_outcome(
		receiver: &mut broadcast::Receiver<Arc<PluginOutcome>>,
	) -> Arc<PluginOutcome> {
		timeout(TEST_TIMEOUT, receiver.recv())
			.await
			.expect("plugin outcome should arrive before the timeout")
			.expect("plugin outcome channel should remain open")
	}

	async fn outcome_for(
		receiver: &mut broadcast::Receiver<Arc<PluginOutcome>>,
		dispatch_id: DispatchId,
		plugin: &str,
	) -> Arc<PluginOutcome> {
		loop {
			let outcome = next_outcome(receiver).await;

			if outcome.dispatch_id() == dispatch_id && outcome.plugin() == plugin {
				return outcome;
			}
		}
	}

	fn delivery_status(receipt: &DispatchReceipt, plugin: &str) -> PluginDeliveryStatus {
		receipt
			.deliveries()
			.iter()
			.find(|delivery| delivery.plugin() == plugin)
			.unwrap_or_else(|| panic!("missing delivery for plugin '{plugin}'"))
			.status()
	}

	#[tokio::test]
	async fn runs_complete_plugin_lifecycle() {
		let starts = Arc::new(AtomicUsize::new(0));
		let events = Arc::new(AtomicUsize::new(0));
		let shutdowns = Arc::new(AtomicUsize::new(0));

		let mut runtime = Runtime::new(test_chain_id());

		runtime
			.register_plugin(LifecyclePlugin {
				starts: Arc::clone(&starts),
				events: Arc::clone(&events),
				shutdowns: Arc::clone(&shutdowns),
			})
			.expect("plugin should be registered");

		runtime.start().await.expect("runtime should start");

		runtime.process(test_event()).await.expect("runtime should process event");

		runtime.shutdown().await.expect("runtime should shut down");

		assert_eq!(starts.load(Ordering::SeqCst), 1);
		assert_eq!(events.load(Ordering::SeqCst), 1);
		assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
	}

	#[tokio::test]
	async fn starts_plugins_concurrently() {
		let barrier = Arc::new(Barrier::new(2));
		let mut runtime = Runtime::new(test_chain_id());

		for name in ["startup-a", "startup-b"] {
			runtime
				.register_plugin(StartupBarrierPlugin { name, barrier: Arc::clone(&barrier) })
				.expect("plugin should be registered");
		}

		timeout(TEST_TIMEOUT, runtime.start())
			.await
			.expect("parallel plugin startup should not deadlock")
			.expect("runtime should start");

		runtime.shutdown().await.expect("runtime should shut down");
	}

	#[tokio::test]
	async fn shuts_plugins_down_concurrently() {
		let barrier = Arc::new(Barrier::new(2));
		let mut runtime = Runtime::new(test_chain_id());

		for name in ["shutdown-a", "shutdown-b"] {
			runtime
				.register_plugin(ShutdownBarrierPlugin { name, barrier: Arc::clone(&barrier) })
				.expect("plugin should be registered");
		}

		runtime.start().await.expect("runtime should start");

		timeout(TEST_TIMEOUT, runtime.shutdown())
			.await
			.expect("parallel plugin shutdown should not deadlock")
			.expect("runtime should shut down");
	}

	#[tokio::test]
	async fn fast_plugin_finishes_without_waiting_for_slow_plugin() {
		let entered = Arc::new(Notify::new());
		let release = Arc::new(Notify::new());
		let mut runtime = Runtime::new(test_chain_id());
		let mut outcomes = runtime.subscribe_outcomes();

		runtime
			.register_plugin(GatePlugin {
				name: "slow",
				entered: Arc::clone(&entered),
				release: Arc::clone(&release),
				block_next_event: true,
			})
			.expect("slow plugin should be registered");
		runtime
			.register_plugin(SuccessPlugin { name: "fast" })
			.expect("fast plugin should be registered");
		runtime.start().await.expect("runtime should start");

		let receipt = timeout(TEST_TIMEOUT, runtime.process(test_event()))
			.await
			.expect("dispatch should not await plugin handlers")
			.expect("dispatch should succeed");

		assert!(receipt.all_accepted());
		entered.notified().await;

		let first_outcome = next_outcome(&mut outcomes).await;
		assert_eq!(first_outcome.plugin(), "fast");
		assert!(matches!(first_outcome.status(), PluginOutcomeStatus::Succeeded));

		release.notify_one();

		let slow_outcome = next_outcome(&mut outcomes).await;
		assert_eq!(slow_outcome.plugin(), "slow");
		assert!(matches!(slow_outcome.status(), PluginOutcomeStatus::Succeeded));

		runtime.shutdown().await.expect("runtime should shut down");
	}

	#[tokio::test]
	async fn plugin_error_is_emitted_immediately_and_worker_continues() {
		let entered = Arc::new(Notify::new());
		let release = Arc::new(Notify::new());
		let mut runtime = Runtime::new(test_chain_id());
		let mut outcomes = runtime.subscribe_outcomes();

		runtime.register_plugin(FailingPlugin).expect("failing plugin should register");
		runtime
			.register_plugin(GatePlugin {
				name: "independent",
				entered: Arc::clone(&entered),
				release: Arc::clone(&release),
				block_next_event: true,
			})
			.expect("independent plugin should register");
		runtime.start().await.expect("runtime should start");

		let first_receipt = runtime
			.process(event_at(21_000_001))
			.await
			.expect("first dispatch should succeed");
		entered.notified().await;

		let failure = next_outcome(&mut outcomes).await;
		assert_eq!(failure.plugin(), "failing");
		assert!(matches!(failure.status(), PluginOutcomeStatus::Failed(_)));

		release.notify_one();
		let success = next_outcome(&mut outcomes).await;
		assert_eq!(success.plugin(), "independent");
		assert!(matches!(success.status(), PluginOutcomeStatus::Succeeded));

		let second_receipt = runtime
			.process(event_at(21_000_002))
			.await
			.expect("second dispatch should succeed");

		let second_outcomes =
			[next_outcome(&mut outcomes).await, next_outcome(&mut outcomes).await];
		assert!(
			second_outcomes
				.iter()
				.all(|outcome| { outcome.dispatch_id() == second_receipt.dispatch_id() })
		);
		assert!(second_outcomes.iter().any(|outcome| {
			outcome.plugin() == "failing" &&
				matches!(outcome.status(), PluginOutcomeStatus::Failed(_))
		}));
		assert!(second_outcomes.iter().any(|outcome| {
			outcome.plugin() == "independent" &&
				matches!(outcome.status(), PluginOutcomeStatus::Succeeded)
		}));
		assert_ne!(first_receipt.dispatch_id(), second_receipt.dispatch_id());

		runtime.shutdown().await.expect("runtime should shut down");
	}

	#[tokio::test]
	async fn full_mailbox_rejects_only_the_slow_plugin() {
		let entered = Arc::new(Notify::new());
		let release = Arc::new(Notify::new());
		let mut runtime = Runtime::with_plugin_mailbox_capacity(test_chain_id(), 1)
			.expect("positive capacity should be valid");
		let mut outcomes = runtime.subscribe_outcomes();

		runtime
			.register_plugin(GatePlugin {
				name: "slow",
				entered: Arc::clone(&entered),
				release: Arc::clone(&release),
				block_next_event: true,
			})
			.expect("slow plugin should register");
		runtime
			.register_plugin(SuccessPlugin { name: "fast" })
			.expect("fast plugin should register");
		runtime.start().await.expect("runtime should start");

		let first = runtime.process(event_at(1)).await.expect("first dispatch should succeed");
		entered.notified().await;
		let _ = outcome_for(&mut outcomes, first.dispatch_id(), "fast").await;

		let second = runtime.process(event_at(2)).await.expect("second dispatch should succeed");
		assert_eq!(delivery_status(&second, "slow"), PluginDeliveryStatus::Accepted);
		let _ = outcome_for(&mut outcomes, second.dispatch_id(), "fast").await;

		let third = timeout(TEST_TIMEOUT, runtime.process(event_at(3)))
			.await
			.expect("a full mailbox must not block dispatch")
			.expect("dispatch should return a receipt");

		assert_eq!(
			delivery_status(&third, "slow"),
			PluginDeliveryStatus::Rejected(PluginDeliveryFailure::MailboxFull)
		);
		assert_eq!(delivery_status(&third, "fast"), PluginDeliveryStatus::Accepted);

		release.notify_one();
		runtime.shutdown().await.expect("runtime should shut down");
	}

	#[tokio::test]
	async fn queues_later_events_while_a_plugin_awaits_the_first_one() {
		let entered = Arc::new(Notify::new());
		let release = Arc::new(Notify::new());
		let mut runtime = Runtime::with_plugin_mailbox_capacity(test_chain_id(), 3)
			.expect("positive capacity should be valid");
		let mut outcomes = runtime.subscribe_outcomes();

		runtime
			.register_plugin(GatePlugin {
				name: "waiting",
				entered: Arc::clone(&entered),
				release: Arc::clone(&release),
				block_next_event: true,
			})
			.expect("waiting plugin should register");
		runtime.start().await.expect("runtime should start");

		let first = runtime.process(event_at(1)).await.expect("first dispatch should succeed");
		entered.notified().await;

		let mut receipts = vec![first];
		for block_number in 2..=4 {
			let receipt = runtime
				.process(event_at(block_number))
				.await
				.expect("queued dispatch should succeed");
			assert_eq!(delivery_status(&receipt, "waiting"), PluginDeliveryStatus::Accepted);
			receipts.push(receipt);
		}

		assert!(
			timeout(Duration::from_millis(20), outcomes.recv()).await.is_err(),
			"queued events must not run while the first handler is awaiting"
		);

		release.notify_one();

		for (expected_block, receipt) in (1..=4).zip(receipts) {
			let outcome = next_outcome(&mut outcomes).await;
			assert_eq!(outcome.dispatch_id(), receipt.dispatch_id());
			assert_eq!(outcome.event().block_number(), expected_block);
			assert!(matches!(outcome.status(), PluginOutcomeStatus::Succeeded));
		}

		runtime.shutdown().await.expect("runtime should shut down");
	}

	#[tokio::test]
	async fn plugin_panic_quarantines_only_that_worker_and_accounts_for_queued_events() {
		let entered = Arc::new(Notify::new());
		let release = Arc::new(Notify::new());
		let mut runtime = Runtime::with_plugin_mailbox_capacity(test_chain_id(), 2)
			.expect("positive capacity should be valid");
		let mut outcomes = runtime.subscribe_outcomes();

		runtime
			.register_plugin(PanicAfterGatePlugin {
				entered: Arc::clone(&entered),
				release: Arc::clone(&release),
			})
			.expect("panicking plugin should register");
		runtime
			.register_plugin(SuccessPlugin { name: "healthy" })
			.expect("healthy plugin should register");
		runtime.start().await.expect("runtime should start");

		let first = runtime.process(event_at(1)).await.expect("first dispatch should succeed");
		entered.notified().await;
		let second = runtime.process(event_at(2)).await.expect("second dispatch should succeed");
		assert_eq!(delivery_status(&second, "panicking"), PluginDeliveryStatus::Accepted);

		release.notify_one();

		let panic_outcome = outcome_for(&mut outcomes, first.dispatch_id(), "panicking").await;
		assert!(matches!(panic_outcome.status(), PluginOutcomeStatus::Panicked(_)));

		let queued_outcome = outcome_for(&mut outcomes, second.dispatch_id(), "panicking").await;
		assert!(matches!(
			queued_outcome.status(),
			PluginOutcomeStatus::Rejected(PluginDeliveryFailure::WorkerStopped)
		));

		let third =
			runtime.process(event_at(3)).await.expect("healthy worker should remain usable");
		assert_eq!(
			delivery_status(&third, "panicking"),
			PluginDeliveryStatus::Rejected(PluginDeliveryFailure::WorkerStopped)
		);
		assert_eq!(delivery_status(&third, "healthy"), PluginDeliveryStatus::Accepted);

		let healthy_outcome = outcome_for(&mut outcomes, third.dispatch_id(), "healthy").await;
		assert!(matches!(healthy_outcome.status(), PluginOutcomeStatus::Succeeded));

		runtime.shutdown().await.expect("runtime should shut down");
	}

	#[tokio::test]
	async fn preserves_order_inside_each_plugin_and_drains_on_shutdown() {
		let observed = Arc::new(Mutex::new(Vec::new()));
		let mut runtime = Runtime::new(test_chain_id());

		runtime
			.register_plugin(OrderingPlugin { observed: Arc::clone(&observed) })
			.expect("ordering plugin should register");
		runtime.start().await.expect("runtime should start");

		for block_number in 1..=5 {
			runtime.process(event_at(block_number)).await.expect("dispatch should succeed");
		}

		runtime.shutdown().await.expect("shutdown should drain accepted events");

		assert_eq!(
			*observed.lock().expect("ordering lock should not be poisoned"),
			vec![1, 2, 3, 4, 5]
		);
	}

	#[tokio::test]
	async fn handler_timeout_quarantines_only_its_worker_and_reports_health() {
		let timeouts = PluginTimeouts {
			startup: TEST_TIMEOUT,
			handler: Duration::from_millis(10),
			shutdown: TEST_TIMEOUT,
		};
		let mut runtime = Runtime::new(test_chain_id()).with_plugin_timeouts(timeouts).unwrap();
		let mut outcomes = runtime.subscribe_outcomes();
		runtime.register_plugin(HangingHandlerPlugin).unwrap();
		runtime.start().await.unwrap();
		let receipt = runtime.process(test_event()).await.unwrap();
		let outcome = outcome_for(&mut outcomes, receipt.dispatch_id(), "hanging-handler").await;
		assert!(matches!(outcome.status(), PluginOutcomeStatus::TimedOut(_)));
		assert_eq!(runtime.plugin_health()[0].status(), crate::PluginHealthStatus::Quarantined);
		runtime.shutdown().await.unwrap();
	}

	#[tokio::test]
	async fn startup_timeout_is_bounded_and_prevents_partial_runtime_start() {
		let timeouts = PluginTimeouts {
			startup: Duration::from_millis(10),
			handler: TEST_TIMEOUT,
			shutdown: TEST_TIMEOUT,
		};
		let mut runtime = Runtime::new(test_chain_id()).with_plugin_timeouts(timeouts).unwrap();
		runtime.register_plugin(HangingStartupPlugin).unwrap();

		let error = timeout(TEST_TIMEOUT, runtime.start())
			.await
			.expect("runtime startup must be bounded")
			.expect_err("hanging startup must fail");
		assert!(matches!(error, RuntimeError::PluginTimeout { operation: "startup", .. }));
		assert!(!runtime.is_started());
	}

	#[tokio::test]
	async fn reports_every_shutdown_failure_after_all_workers_finish() {
		let mut runtime = Runtime::new(test_chain_id());
		runtime.register_plugin(ShutdownFailurePlugin { name: "failure-a" }).unwrap();
		runtime.register_plugin(ShutdownFailurePlugin { name: "failure-b" }).unwrap();
		runtime.start().await.unwrap();

		let error = runtime.shutdown().await.expect_err("both shutdown hooks should fail");
		let RuntimeError::MultipleLifecycleFailures { failures } = error else {
			panic!("expected aggregated lifecycle failures");
		};
		assert_eq!(failures.len(), 2);
		assert!(failures.iter().any(|failure| failure.contains("failure-a")));
		assert!(failures.iter().any(|failure| failure.contains("failure-b")));
	}

	#[tokio::test]
	async fn shutdown_timeout_does_not_prevent_sibling_cleanup() {
		let shutdowns = Arc::new(AtomicUsize::new(0));
		let timeouts = PluginTimeouts {
			startup: TEST_TIMEOUT,
			handler: TEST_TIMEOUT,
			shutdown: Duration::from_millis(10),
		};
		let mut runtime = Runtime::new(test_chain_id()).with_plugin_timeouts(timeouts).unwrap();
		runtime.register_plugin(HangingShutdownPlugin).unwrap();
		runtime
			.register_plugin(LifecyclePlugin {
				starts: Arc::new(AtomicUsize::new(0)),
				events: Arc::new(AtomicUsize::new(0)),
				shutdowns: Arc::clone(&shutdowns),
			})
			.unwrap();
		runtime.start().await.unwrap();

		let error = timeout(TEST_TIMEOUT, runtime.shutdown())
			.await
			.expect("shutdown must be bounded")
			.expect_err("hanging shutdown must fail");
		assert!(matches!(error, RuntimeError::PluginTimeout { operation: "shutdown", .. }));
		assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
	}

	#[test]
	fn rejects_zero_plugin_mailbox_capacity() {
		let error = Runtime::with_plugin_mailbox_capacity(test_chain_id(), 0)
			.expect_err("zero capacity should fail");

		assert!(matches!(error, RuntimeError::InvalidPluginMailboxCapacity));
	}

	#[tokio::test]
	async fn rejects_processing_before_start() {
		let mut runtime = Runtime::new(test_chain_id());

		let error = runtime
			.process(test_event())
			.await
			.expect_err("processing before start should fail");

		assert!(matches!(error, RuntimeError::NotStarted));
	}

	#[tokio::test]
	async fn rejects_events_from_a_different_evm_chain() {
		let mut runtime = Runtime::new(test_chain_id());
		runtime
			.register_plugin(SuccessPlugin { name: "chain-bound" })
			.expect("plugin should register");
		runtime.start().await.expect("runtime should start");

		let other_chain_id = ChainId::new(42_161).expect("Arbitrum chain ID should be valid");
		let error = runtime
			.process(event_for_chain(other_chain_id, 21_000_000))
			.await
			.expect_err("a runtime must not mix events from different chains");

		assert!(matches!(error, RuntimeError::ChainIdMismatch { expected: 8_453, actual: 42_161 }));

		runtime.shutdown().await.expect("runtime should shut down");
	}

	#[test]
	fn rejects_duplicate_plugins() {
		let mut runtime = Runtime::new(test_chain_id());

		let first = LifecyclePlugin {
			starts: Arc::new(AtomicUsize::new(0)),
			events: Arc::new(AtomicUsize::new(0)),
			shutdowns: Arc::new(AtomicUsize::new(0)),
		};

		let second = LifecyclePlugin {
			starts: Arc::new(AtomicUsize::new(0)),
			events: Arc::new(AtomicUsize::new(0)),
			shutdowns: Arc::new(AtomicUsize::new(0)),
		};

		runtime.register_plugin(first).expect("first plugin should be registered");

		let error = runtime.register_plugin(second).expect_err("duplicate plugin should fail");

		assert!(matches!(
			error,
			RuntimeError::DuplicatePlugin { name }
				if name == "lifecycle-plugin"
		));
	}
}
