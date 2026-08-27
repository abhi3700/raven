//! Internal execution engine for plugin workers.
//!
//! The dispatcher converts the runtime's public lifecycle policy into Tokio
//! primitives:
//!
//! ```text
//! registered plugins
//!        |
//!        v
//! Dispatcher::start
//!        |
//!        +-- mpsc mailbox A -> worker task A -> Plugin A
//!        +-- mpsc mailbox B -> worker task B -> Plugin B
//!        +-- mpsc mailbox C -> worker task C -> Plugin C
//!                              |
//!                              v
//!                    broadcast PluginOutcome
//! ```
//!
//! One worker owns one plugin value. The dispatcher never calls plugin lifecycle
//! hooks directly after startup; it communicates through mailbox senders and
//! worker join handles.

use crate::{
	DispatchId, DispatchReceipt, PluginDelivery, PluginDeliveryFailure, PluginDeliveryStatus,
	PluginHealth, PluginHealthStatus, PluginOutcome, PluginOutcomeStatus, PluginTimeouts,
	RuntimeError, RuntimeResult, registry::PluginRegistry,
};
use futures_util::FutureExt;
use raven_core::ChainEvent;
use raven_plugin_sdk::{Plugin, PluginContext, PluginError, PluginMetadata};
use std::{
	any::Any,
	panic::AssertUnwindSafe,
	sync::{
		Arc,
		atomic::{AtomicU8, Ordering},
	},
	time::{Duration, Instant},
};
use tokio::{
	sync::{broadcast, mpsc, oneshot},
	task::JoinHandle,
	time::timeout,
};

/// Routes events into independent plugin worker mailboxes.
///
/// `Dispatcher` is private to the crate because callers should not bypass
/// `Runtime`'s state and chain-ID checks. It owns the low-level execution
/// topology:
///
/// ```text
/// Dispatcher
///   |
///   +-- PluginRegistry before start
///   +-- Vec<PluginWorkerHandle> after start
///   +-- next DispatchId
///   +-- outcome broadcast sender
///   +-- lifecycle timeouts copied into workers
/// ```
#[derive(Debug)]
pub(crate) struct Dispatcher {
	registry: PluginRegistry,
	workers: Vec<PluginWorkerHandle>,
	mailbox_capacity: usize,
	next_dispatch_id: u64,
	outcome_sender: broadcast::Sender<Arc<PluginOutcome>>,
	timeouts: PluginTimeouts,
}

impl Dispatcher {
	/// Creates an empty dispatcher with bounded plugin and outcome channels.
	///
	/// `mailbox_capacity` is applied to each plugin's private FIFO queue.
	/// `outcome_capacity` applies to the shared broadcast channel used by outcome
	/// subscribers.
	pub(crate) fn new(
		mailbox_capacity: usize,
		outcome_capacity: usize,
		timeouts: PluginTimeouts,
	) -> Self {
		let (outcome_sender, _) = broadcast::channel(outcome_capacity);

		Self {
			registry: PluginRegistry::new(),
			workers: Vec::new(),
			mailbox_capacity,
			next_dispatch_id: 1,
			outcome_sender,
			timeouts,
		}
	}

	/// Updates lifecycle deadlines before workers are spawned.
	pub(crate) const fn set_timeouts(&mut self, timeouts: PluginTimeouts) {
		self.timeouts = timeouts;
	}

	/// Registers a plugin before worker tasks start.
	pub(crate) fn register(&mut self, plugin: Box<dyn Plugin>) -> RuntimeResult {
		self.registry.register(plugin)
	}

	/// Starts every plugin concurrently, then opens their event mailboxes.
	///
	/// The startup path uses a one-shot readiness channel per worker. All workers
	/// are spawned before the dispatcher awaits readiness, so plugin startup hooks
	/// can overlap:
	///
	/// ```text
	/// spawn A -> Plugin::start ----+
	/// spawn B -> Plugin::start --+ |
	/// spawn C -> Plugin::start -+| |
	///                            |||
	/// readiness oneshots <-------+++
	/// ```
	///
	/// If any worker reports a startup error, panic, or timeout, the dispatcher
	/// stops every spawned worker and returns one aggregated lifecycle error when
	/// multiple cleanup failures occur.
	pub(crate) async fn start(&mut self, context: PluginContext) -> RuntimeResult {
		let plugins = self.registry.take_plugins();
		let mut workers = Vec::with_capacity(plugins.len());
		let mut readiness = Vec::with_capacity(plugins.len());

		for plugin in plugins {
			let metadata = plugin.metadata();
			let (event_sender, event_receiver) = mpsc::channel(self.mailbox_capacity);
			let (ready_sender, ready_receiver) = oneshot::channel::<RuntimeResult>();
			let outcome_sender = self.outcome_sender.clone();
			let health = Arc::new(AtomicU8::new(WorkerHealth::Starting as u8));

			let environment = PluginWorkerEnvironment {
				metadata,
				context,
				outcome_sender,
				timeouts: self.timeouts,
				health: Arc::clone(&health),
			};
			let task =
				tokio::spawn(run_plugin_worker(plugin, event_receiver, ready_sender, environment));

			workers.push(PluginWorkerHandle {
				metadata,
				event_sender: Some(event_sender),
				task,
				health,
			});
			readiness.push((metadata, ready_receiver));
		}

		let mut startup_errors = Vec::new();

		for (metadata, ready_receiver) in readiness {
			let result = match ready_receiver.await {
				Ok(result) => result,
				Err(error) => Err(RuntimeError::plugin_worker_stopped(
					metadata.name,
					"startup",
					error.to_string(),
				)),
			};

			if let Err(error) = result {
				startup_errors.push(error);
			}
		}

		if !startup_errors.is_empty() {
			if let Err(error) = stop_plugin_workers(workers).await {
				startup_errors.push(error);
			}
			return Err(combine_lifecycle_errors(startup_errors));
		}

		self.workers = workers;

		Ok(())
	}

	/// Attempts to enqueue one event for every plugin without awaiting handlers.
	///
	/// This method is intentionally synchronous and non-blocking. Each mailbox is
	/// attempted with `try_send`, so a full or stopped plugin cannot delay sibling
	/// delivery attempts.
	///
	/// ```text
	/// ChainEvent
	///    |
	///    v
	/// Arc<ChainEvent>
	///    |
	///    +-- try_send mailbox A -> Accepted
	///    +-- try_send mailbox B -> Rejected(MailboxFull)
	///    +-- try_send mailbox C -> Accepted
	///    |
	///    v
	/// DispatchReceipt
	/// ```
	///
	/// Dispatcher-side rejections are also published as zero-duration outcomes so
	/// subscribers can account for every dispatch/plugin pair they observe.
	pub(crate) fn dispatch(&mut self, event: ChainEvent) -> RuntimeResult<DispatchReceipt> {
		let next_dispatch_id =
			self.next_dispatch_id.checked_add(1).ok_or(RuntimeError::DispatchIdExhausted)?;
		let dispatch_id = DispatchId::new(self.next_dispatch_id);

		self.next_dispatch_id = next_dispatch_id;

		let event = Arc::new(event);
		let mut deliveries = Vec::with_capacity(self.workers.len());

		for worker in &self.workers {
			let queued_event = QueuedPluginEvent { dispatch_id, event: Arc::clone(&event) };
			let status = match worker.event_sender.as_ref() {
				Some(event_sender) => match event_sender.try_send(queued_event) {
					Ok(()) => PluginDeliveryStatus::Accepted,
					Err(mpsc::error::TrySendError::Full(_)) =>
						PluginDeliveryStatus::Rejected(PluginDeliveryFailure::MailboxFull),
					Err(mpsc::error::TrySendError::Closed(_)) =>
						PluginDeliveryStatus::Rejected(PluginDeliveryFailure::WorkerStopped),
				},
				None => PluginDeliveryStatus::Rejected(PluginDeliveryFailure::WorkerStopped),
			};

			if let PluginDeliveryStatus::Rejected(reason) = status {
				publish_outcome(
					&self.outcome_sender,
					PluginOutcome::new(
						dispatch_id,
						worker.metadata.name,
						Arc::clone(&event),
						PluginOutcomeStatus::Rejected(reason),
						Duration::ZERO,
					),
				);
			}

			deliveries.push(PluginDelivery::new(worker.metadata.name, status));
		}

		Ok(DispatchReceipt::new(dispatch_id, deliveries))
	}

	/// Closes every mailbox, drains accepted events, and joins all workers.
	///
	/// Dropping all senders first lets every worker concurrently drain its own
	/// accepted queue and then run `Plugin::shutdown`.
	///
	/// ```text
	/// take all senders
	///      |
	///      +-- worker A recv() -> drain -> shutdown
	///      +-- worker B recv() -> drain -> shutdown
	///      +-- worker C recv() -> drain -> shutdown
	///      |
	///      v
	/// join worker tasks
	/// ```
	pub(crate) async fn shutdown(&mut self) -> RuntimeResult {
		stop_plugin_workers(std::mem::take(&mut self.workers)).await
	}

	/// Subscribes to independently emitted plugin outcomes.
	pub(crate) fn subscribe_outcomes(&self) -> broadcast::Receiver<Arc<PluginOutcome>> {
		self.outcome_sender.subscribe()
	}

	/// Reads one point-in-time health value from every worker handle.
	pub(crate) fn plugin_health(&self) -> Vec<PluginHealth> {
		self.workers
			.iter()
			.map(|worker| {
				PluginHealth::new(worker.metadata.name, WorkerHealth::load(&worker.health).into())
			})
			.collect()
	}

	/// Returns the number of registered or running plugins.
	pub(crate) fn plugin_count(&self) -> usize {
		self.registry.len() + self.workers.len()
	}

	/// Returns whether no plugins are registered or running.
	pub(crate) fn is_empty(&self) -> bool {
		self.registry.is_empty() && self.workers.is_empty()
	}
}

/// Dispatcher-owned control handle for one spawned plugin worker.
///
/// ```text
/// PluginWorkerHandle
///   metadata     -> runtime-visible plugin identity
///   event_sender -> enqueue events or close mailbox on shutdown
///   task         -> await worker completion
///   health       -> read worker state without taking a lock
/// ```
#[derive(Debug)]
struct PluginWorkerHandle {
	metadata: PluginMetadata,
	event_sender: Option<mpsc::Sender<QueuedPluginEvent>>,
	task: JoinHandle<RuntimeResult>,
	health: Arc<AtomicU8>,
}

/// One mailbox item for one plugin worker.
///
/// The event is shared with `Arc` so fan-out does not deep-clone the normalized
/// chain event for every plugin.
///
/// ```text
/// QueuedPluginEvent
///   dispatch_id -> correlation with receipt/outcomes
///   event       -> Arc<ChainEvent>
/// ```
#[derive(Debug)]
struct QueuedPluginEvent {
	dispatch_id: DispatchId,
	event: Arc<ChainEvent>,
}

/// Immutable configuration copied into one worker task.
///
/// Each worker receives its own copy of the chain context plus shared handles
/// for outcome publication and health updates.
struct PluginWorkerEnvironment {
	metadata: PluginMetadata,
	context: PluginContext,
	outcome_sender: broadcast::Sender<Arc<PluginOutcome>>,
	timeouts: PluginTimeouts,
	health: Arc<AtomicU8>,
}

/// Runs the complete lifetime of one plugin.
///
/// This is the actor-like worker loop:
///
/// ```text
/// Plugin::start with timeout and panic catch
///        |
///        v
/// signal readiness
///        |
///        v
/// receive FIFO mailbox events
///        |
///        +-- handle_event -> outcome -> next event
///        +-- panic/timeout -> quarantine -> reject queued work -> stop loop
///        |
///        v
/// Plugin::shutdown with timeout and panic catch
/// ```
///
/// A returned handler error becomes a `Failed` outcome and the worker keeps
/// running. A panic or timeout quarantines only this worker because the plugin's
/// internal mutable state may no longer be reliable.
async fn run_plugin_worker(
	mut plugin: Box<dyn Plugin>,
	mut event_receiver: mpsc::Receiver<QueuedPluginEvent>,
	ready_sender: oneshot::Sender<RuntimeResult>,
	environment: PluginWorkerEnvironment,
) -> RuntimeResult {
	let PluginWorkerEnvironment { metadata, context, outcome_sender, timeouts, health } =
		environment;
	let startup_result =
		timeout(timeouts.startup, AssertUnwindSafe(plugin.start(&context)).catch_unwind()).await;

	match startup_result {
		Ok(Ok(Ok(()))) => {
			WorkerHealth::Healthy.store(&health);
			// If Runtime::start is cancelled, its mailbox sender is also dropped.
			// Continuing into the receive loop lets the worker observe closure and
			// still execute the plugin's shutdown hook.
			let _ = ready_sender.send(Ok(()));
		},
		Ok(Ok(Err(error))) => {
			WorkerHealth::Quarantined.store(&health);
			let _ = ready_sender.send(Err(plugin_operation_error(metadata, "startup", error)));
			return Ok(());
		},
		Ok(Err(payload)) => {
			WorkerHealth::Quarantined.store(&health);
			let source = PluginError::Other(format!(
				"plugin panicked during startup: {}",
				panic_message(payload)
			));
			let _ = ready_sender.send(Err(plugin_operation_error(metadata, "startup", source)));
			return Ok(());
		},
		Err(_) => {
			WorkerHealth::Quarantined.store(&health);
			let error = RuntimeError::plugin_timeout(metadata.name, "startup", timeouts.startup);
			let _ = ready_sender.send(Err(error));
			return Ok(());
		},
	}

	while let Some(queued_event) = event_receiver.recv().await {
		let started_at = Instant::now();
		// Three result layers are expected here:
		// timeout -> panic catch -> plugin result.
		let handler_result = timeout(
			timeouts.handler,
			AssertUnwindSafe(plugin.handle_event(&queued_event.event, &context)).catch_unwind(),
		)
		.await;

		let (status, worker_can_continue) = match handler_result {
			Ok(Ok(Ok(()))) => (PluginOutcomeStatus::Succeeded, true),
			Ok(Ok(Err(error))) => (PluginOutcomeStatus::Failed(error), true),
			Ok(Err(payload)) => (PluginOutcomeStatus::Panicked(panic_message(payload)), false),
			Err(_) => (PluginOutcomeStatus::TimedOut(timeouts.handler), false),
		};

		if !worker_can_continue {
			WorkerHealth::Quarantined.store(&health);
			// Closing before publishing the panic outcome means that observing that
			// outcome also guarantees future dispatches will see WorkerStopped.
			event_receiver.close();
		}

		publish_outcome(
			&outcome_sender,
			PluginOutcome::new(
				queued_event.dispatch_id,
				metadata.name,
				queued_event.event,
				status,
				started_at.elapsed(),
			),
		);

		if !worker_can_continue {
			// A panic quarantines only this worker. Close its receiver immediately so
			// future dispatches are rejected, then account for every event that was
			// already accepted instead of silently dropping queued work.
			while let Some(pending_event) = event_receiver.recv().await {
				publish_outcome(
					&outcome_sender,
					PluginOutcome::new(
						pending_event.dispatch_id,
						metadata.name,
						pending_event.event,
						PluginOutcomeStatus::Rejected(PluginDeliveryFailure::WorkerStopped),
						Duration::ZERO,
					),
				);
			}

			break;
		}
	}

	let shutdown_result =
		timeout(timeouts.shutdown, AssertUnwindSafe(plugin.shutdown(&context)).catch_unwind())
			.await;

	match shutdown_result {
		Ok(Ok(result)) => {
			if result.is_ok() && !matches!(WorkerHealth::load(&health), WorkerHealth::Quarantined) {
				WorkerHealth::Stopped.store(&health);
			} else if result.is_err() {
				WorkerHealth::Quarantined.store(&health);
			}
			result.map_err(|source| plugin_operation_error(metadata, "shutdown", source))
		},
		Ok(Err(payload)) => {
			WorkerHealth::Quarantined.store(&health);
			Err(plugin_operation_error(
				metadata,
				"shutdown",
				PluginError::Other(format!(
					"plugin panicked during shutdown: {}",
					panic_message(payload)
				)),
			))
		},
		Err(_) => {
			WorkerHealth::Quarantined.store(&health);
			Err(RuntimeError::plugin_timeout(metadata.name, "shutdown", timeouts.shutdown))
		},
	}
}

/// Stops workers by closing mailboxes and joining every task.
///
/// The first loop only drops senders. The second loop awaits task completion.
/// That separation keeps worker draining/shutdown concurrent even though join
/// handles are awaited one by one.
async fn stop_plugin_workers(mut workers: Vec<PluginWorkerHandle>) -> RuntimeResult {
	for worker in &mut workers {
		worker.event_sender.take();
	}

	let mut errors = Vec::new();

	for worker in workers {
		let result = match worker.task.await {
			Ok(result) => result,
			Err(error) => Err(RuntimeError::plugin_worker_stopped(
				worker.metadata.name,
				"shutdown",
				error.to_string(),
			)),
		};

		if let Err(error) = result {
			errors.push(error);
		}
	}

	if errors.is_empty() { Ok(()) } else { Err(combine_lifecycle_errors(errors)) }
}

/// Preserves single-error ergonomics while still reporting multiple failures.
fn combine_lifecycle_errors(mut errors: Vec<RuntimeError>) -> RuntimeError {
	if errors.len() == 1 {
		errors.pop().expect("one lifecycle error should exist")
	} else {
		RuntimeError::lifecycle_failures(errors)
	}
}

/// Compact internal representation for worker health.
///
/// `AtomicU8` is used because the worker task writes state while runtime callers
/// may read snapshots through dispatcher-owned handles.
///
/// ```text
/// worker task --store--> Arc<AtomicU8> --load--> Dispatcher::plugin_health
/// ```
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
enum WorkerHealth {
	Starting = 0,
	Healthy = 1,
	Quarantined = 2,
	Stopped = 3,
}

impl WorkerHealth {
	fn load(value: &AtomicU8) -> Self {
		match value.load(Ordering::Acquire) {
			0 => Self::Starting,
			1 => Self::Healthy,
			2 => Self::Quarantined,
			3 => Self::Stopped,
			_ => unreachable!("worker health is written only by WorkerHealth"),
		}
	}

	fn store(self, value: &AtomicU8) {
		value.store(self as u8, Ordering::Release);
	}
}

impl From<WorkerHealth> for PluginHealthStatus {
	fn from(value: WorkerHealth) -> Self {
		match value {
			WorkerHealth::Starting => Self::Starting,
			WorkerHealth::Healthy => Self::Healthy,
			WorkerHealth::Quarantined => Self::Quarantined,
			WorkerHealth::Stopped => Self::Stopped,
		}
	}
}

/// Publishes an outcome without letting observer lag affect worker execution.
///
/// A send can fail only when there are no subscribers. That is acceptable because
/// outcomes are a live observation stream, not durable storage.
fn publish_outcome(outcome_sender: &broadcast::Sender<Arc<PluginOutcome>>, outcome: PluginOutcome) {
	let _ = outcome_sender.send(Arc::new(outcome));
}

/// Converts plugin lifecycle hook failures into runtime operation failures.
fn plugin_operation_error(
	metadata: PluginMetadata,
	operation: &'static str,
	source: PluginError,
) -> RuntimeError {
	RuntimeError::plugin_operation(metadata.name, operation, source)
}

/// Extracts a human-readable message from Rust's untyped panic payload.
fn panic_message(payload: Box<dyn Any + Send>) -> String {
	if let Some(message) = payload.downcast_ref::<&str>() {
		(*message).to_owned()
	} else if let Some(message) = payload.downcast_ref::<String>() {
		message.clone()
	} else {
		"non-string panic payload".to_owned()
	}
}
