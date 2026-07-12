use crate::{RuntimeError, RuntimeResult, registry::PluginRegistry};
use raven_core::ChainEvent;
use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata};

/// Dispatches lifecycle calls and chain events to registered plugins.
#[derive(Debug)]
pub(crate) struct Dispatcher {
	registry: PluginRegistry,
}

impl Dispatcher {
	/// Creates an empty dispatcher.
	pub(crate) fn new() -> Self {
		Self { registry: PluginRegistry::new() }
	}

	/// Registers a plugin with the dispatcher.
	pub(crate) fn register(&mut self, plugin: Box<dyn Plugin>) -> RuntimeResult {
		self.registry.register(plugin)
	}

	/// Starts every registered plugin.
	pub(crate) async fn start(&mut self, context: &PluginContext) -> RuntimeResult {
		for plugin in self.registry.iter_mut() {
			let metadata = plugin.metadata();

			plugin
				.start(context)
				.await
				.map_err(|source| plugin_operation_error(metadata, "startup", source))?;
		}

		Ok(())
	}

	/// Dispatches one normalized chain event to every plugin.
	pub(crate) async fn dispatch(
		&mut self,
		event: &ChainEvent,
		context: &PluginContext,
	) -> RuntimeResult {
		for plugin in self.registry.iter_mut() {
			let metadata = plugin.metadata();

			plugin
				.handle_event(event, context)
				.await
				.map_err(|source| plugin_operation_error(metadata, "event processing", source))?;
		}

		Ok(())
	}

	/// Shuts down every registered plugin.
	pub(crate) async fn shutdown(&mut self, context: &PluginContext) -> RuntimeResult {
		for plugin in self.registry.iter_mut() {
			let metadata = plugin.metadata();

			plugin
				.shutdown(context)
				.await
				.map_err(|source| plugin_operation_error(metadata, "shutdown", source))?;
		}

		Ok(())
	}

	/// Returns the number of registered plugins.
	pub(crate) fn plugin_count(&self) -> usize {
		self.registry.len()
	}

	/// Returns whether no plugins are registered.
	pub(crate) fn is_empty(&self) -> bool {
		self.registry.is_empty()
	}
}

fn plugin_operation_error(
	metadata: PluginMetadata,
	operation: &'static str,
	source: raven_plugin_sdk::PluginError,
) -> RuntimeError {
	RuntimeError::plugin_operation(metadata.name, operation, source)
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	};

	use async_trait::async_trait;
	use raven_core::{BlockEvent, ChainEvent, ChainId};
	use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};

	use super::*;

	#[derive(Debug)]
	struct CountingPlugin {
		event_count: Arc<AtomicUsize>,
	}

	#[async_trait]
	impl Plugin for CountingPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("counting-plugin", "0.1.0", "Counts received events")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			self.event_count.fetch_add(1, Ordering::SeqCst);

			Ok(())
		}
	}

	fn event() -> ChainEvent {
		let block = BlockEvent::new(
			ChainId::ETHEREUM,
			21_000_000,
			"0x1111111111111111111111111111111111111111111111111111111111111111",
			"0x2222222222222222222222222222222222222222222222222222222222222222",
			1_720_000_000,
			150,
		)
		.expect("block should be valid");

		ChainEvent::BlockApplied(block)
	}

	#[tokio::test]
	async fn dispatches_event_to_registered_plugin() {
		let counter = Arc::new(AtomicUsize::new(0));
		let context = PluginContext::new(ChainId::ETHEREUM);
		let mut dispatcher = Dispatcher::new();

		dispatcher
			.register(Box::new(CountingPlugin { event_count: Arc::clone(&counter) }))
			.expect("plugin should be registered");

		dispatcher
			.dispatch(&event(), &context)
			.await
			.expect("event should be dispatched");

		assert_eq!(counter.load(Ordering::SeqCst), 1);
	}
}
