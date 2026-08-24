use std::{
	any::Any,
	panic::AssertUnwindSafe,
	sync::Arc,
	time::{Duration, Instant},
};

use futures_util::FutureExt;
use raven_core::ChainEvent;
use raven_plugin_sdk::{Plugin, PluginContext, PluginError, PluginMetadata, PluginResult};
use tokio::{
	sync::{broadcast, mpsc, oneshot},
	task::JoinHandle,
};

use crate::{
	DispatchId, DispatchReceipt, PluginDelivery, PluginDeliveryFailure, PluginDeliveryStatus,
	PluginOutcome, PluginOutcomeStatus, RuntimeError, RuntimeResult, registry::PluginRegistry,
};

/// Routes events into independent plugin worker mailboxes.
#[derive(Debug)]
pub(crate) struct Dispatcher {
	registry: PluginRegistry,
	workers: Vec<PluginWorkerHandle>,
	mailbox_capacity: usize,
	next_dispatch_id: u64,
	outcome_sender: broadcast::Sender<Arc<PluginOutcome>>,
}

impl Dispatcher {
	/// Creates an empty dispatcher with bounded plugin and outcome channels.
	pub(crate) fn new(mailbox_capacity: usize, outcome_capacity: usize) -> Self {
		let (outcome_sender, _) = broadcast::channel(outcome_capacity);

		Self {
			registry: PluginRegistry::new(),
			workers: Vec::new(),
			mailbox_capacity,
			next_dispatch_id: 1,
			outcome_sender,
		}
	}

	/// Registers a plugin before worker tasks start.
	pub(crate) fn register(&mut self, plugin: Box<dyn Plugin>) -> RuntimeResult {
		self.registry.register(plugin)
	}

	/// Starts every plugin concurrently, then opens their event mailboxes.
	pub(crate) async fn start(&mut self, context: PluginContext) -> RuntimeResult {
		let plugins = self.registry.take_plugins();
		let mut workers = Vec::with_capacity(plugins.len());
		let mut readiness = Vec::with_capacity(plugins.len());

		for plugin in plugins {
			let metadata = plugin.metadata();
			let (event_sender, event_receiver) = mpsc::channel(self.mailbox_capacity);
			let (ready_sender, ready_receiver) = oneshot::channel();
			let outcome_sender = self.outcome_sender.clone();

			let task = tokio::spawn(run_plugin_worker(
				plugin,
				metadata,
				context,
				event_receiver,
				ready_sender,
				outcome_sender,
			));

			workers.push(PluginWorkerHandle { metadata, event_sender: Some(event_sender), task });
			readiness.push((metadata, ready_receiver));
		}

		let mut startup_error = None;

		for (metadata, ready_receiver) in readiness {
			let result = match ready_receiver.await {
				Ok(result) =>
					result.map_err(|source| plugin_operation_error(metadata, "startup", source)),
				Err(error) => Err(RuntimeError::plugin_worker_stopped(
					metadata.name,
					"startup",
					error.to_string(),
				)),
			};

			if startup_error.is_none() {
				startup_error = result.err();
			}
		}

		if let Some(error) = startup_error {
			let _ = stop_plugin_workers(workers).await;
			return Err(error);
		}

		self.workers = workers;

		Ok(())
	}

	/// Attempts to enqueue one event for every plugin without awaiting handlers.
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
	pub(crate) async fn shutdown(&mut self) -> RuntimeResult {
		stop_plugin_workers(std::mem::take(&mut self.workers)).await
	}

	/// Subscribes to independently emitted plugin outcomes.
	pub(crate) fn subscribe_outcomes(&self) -> broadcast::Receiver<Arc<PluginOutcome>> {
		self.outcome_sender.subscribe()
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

#[derive(Debug)]
struct PluginWorkerHandle {
	metadata: PluginMetadata,
	event_sender: Option<mpsc::Sender<QueuedPluginEvent>>,
	task: JoinHandle<PluginResult>,
}

#[derive(Debug)]
struct QueuedPluginEvent {
	dispatch_id: DispatchId,
	event: Arc<ChainEvent>,
}

async fn run_plugin_worker(
	mut plugin: Box<dyn Plugin>,
	metadata: PluginMetadata,
	context: PluginContext,
	mut event_receiver: mpsc::Receiver<QueuedPluginEvent>,
	ready_sender: oneshot::Sender<PluginResult>,
	outcome_sender: broadcast::Sender<Arc<PluginOutcome>>,
) -> PluginResult {
	let startup_result = AssertUnwindSafe(plugin.start(&context)).catch_unwind().await;

	match startup_result {
		Ok(Ok(())) => {
			// If Runtime::start is cancelled, its mailbox sender is also dropped.
			// Continuing into the receive loop lets the worker observe closure and
			// still execute the plugin's shutdown hook.
			let _ = ready_sender.send(Ok(()));
		},
		Ok(Err(error)) => {
			let _ = ready_sender.send(Err(error));
			return Ok(());
		},
		Err(payload) => {
			let error = PluginError::Other(format!(
				"plugin panicked during startup: {}",
				panic_message(payload)
			));
			let _ = ready_sender.send(Err(error));
			return Ok(());
		},
	}

	while let Some(queued_event) = event_receiver.recv().await {
		let started_at = Instant::now();
		let handler_result = AssertUnwindSafe(plugin.handle_event(&queued_event.event, &context))
			.catch_unwind()
			.await;

		let (status, worker_can_continue) = match handler_result {
			Ok(Ok(())) => (PluginOutcomeStatus::Succeeded, true),
			Ok(Err(error)) => (PluginOutcomeStatus::Failed(error), true),
			Err(payload) => (PluginOutcomeStatus::Panicked(panic_message(payload)), false),
		};

		if !worker_can_continue {
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

	match AssertUnwindSafe(plugin.shutdown(&context)).catch_unwind().await {
		Ok(result) => result,
		Err(payload) => Err(PluginError::Other(format!(
			"plugin panicked during shutdown: {}",
			panic_message(payload)
		))),
	}
}

async fn stop_plugin_workers(mut workers: Vec<PluginWorkerHandle>) -> RuntimeResult {
	for worker in &mut workers {
		worker.event_sender.take();
	}

	let mut first_error = None;

	for worker in workers {
		let result = match worker.task.await {
			Ok(result) =>
				result.map_err(|source| plugin_operation_error(worker.metadata, "shutdown", source)),
			Err(error) => Err(RuntimeError::plugin_worker_stopped(
				worker.metadata.name,
				"shutdown",
				error.to_string(),
			)),
		};

		if first_error.is_none() {
			first_error = result.err();
		}
	}

	first_error.map_or(Ok(()), Err)
}

fn publish_outcome(outcome_sender: &broadcast::Sender<Arc<PluginOutcome>>, outcome: PluginOutcome) {
	let _ = outcome_sender.send(Arc::new(outcome));
}

fn plugin_operation_error(
	metadata: PluginMetadata,
	operation: &'static str,
	source: PluginError,
) -> RuntimeError {
	RuntimeError::plugin_operation(metadata.name, operation, source)
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
	if let Some(message) = payload.downcast_ref::<&str>() {
		(*message).to_owned()
	} else if let Some(message) = payload.downcast_ref::<String>() {
		message.clone()
	} else {
		"non-string panic payload".to_owned()
	}
}
