use crate::{RuntimeError, RuntimeResult, dispatcher::Dispatcher};
use raven_core::{ChainEvent, ChainId};
use raven_plugin_sdk::{Plugin, PluginContext};

/// Hosts Raven plugins and processes normalized chain events.
#[derive(Debug)]
pub struct Runtime {
	dispatcher: Dispatcher,
	context: PluginContext,
	state: RuntimeState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeState {
	Created,
	Started,
	Shutdown,
}

impl Runtime {
	/// Creates a runtime for one EVM chain.
	pub fn new(chain_id: ChainId) -> Self {
		Self {
			dispatcher: Dispatcher::new(),
			context: PluginContext::new(chain_id),
			state: RuntimeState::Created,
		}
	}

	/// Registers a plugin.
	///
	/// Plugins must be registered before the runtime starts.
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

		self.dispatcher.start(&self.context).await?;
		self.state = RuntimeState::Started;

		Ok(())
	}

	/// Processes one normalized chain event.
	pub async fn process(&mut self, event: ChainEvent) -> RuntimeResult {
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

		self.dispatcher.dispatch(&event, &self.context).await
	}

	/// Shuts down all registered plugins.
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

		self.dispatcher.shutdown(&self.context).await?;
		self.state = RuntimeState::Shutdown;

		Ok(())
	}

	/// Returns the chain ID used by the runtime.
	pub const fn chain_id(&self) -> ChainId {
		self.context.chain_id()
	}

	/// Returns the number of registered plugins.
	pub fn plugin_count(&self) -> usize {
		self.dispatcher.plugin_count()
	}

	/// Returns whether no plugins are registered.
	pub fn has_no_plugins(&self) -> bool {
		self.dispatcher.is_empty()
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
	use async_trait::async_trait;
	use raven_core::{BlockEvent, ChainEvent, ChainId};
	use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};
	use std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	};

	struct LifecyclePlugin {
		starts: Arc<AtomicUsize>,
		events: Arc<AtomicUsize>,
		shutdowns: Arc<AtomicUsize>,
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

	fn ethereum_event() -> ChainEvent {
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
	async fn runs_complete_plugin_lifecycle() {
		let starts = Arc::new(AtomicUsize::new(0));
		let events = Arc::new(AtomicUsize::new(0));
		let shutdowns = Arc::new(AtomicUsize::new(0));

		let mut runtime = Runtime::new(ChainId::ETHEREUM);

		runtime
			.register_plugin(LifecyclePlugin {
				starts: Arc::clone(&starts),
				events: Arc::clone(&events),
				shutdowns: Arc::clone(&shutdowns),
			})
			.expect("plugin should be registered");

		runtime.start().await.expect("runtime should start");

		runtime.process(ethereum_event()).await.expect("runtime should process event");

		runtime.shutdown().await.expect("runtime should shut down");

		assert_eq!(starts.load(Ordering::SeqCst), 1);
		assert_eq!(events.load(Ordering::SeqCst), 1);
		assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
	}

	#[tokio::test]
	async fn rejects_processing_before_start() {
		let mut runtime = Runtime::new(ChainId::ETHEREUM);

		let error = runtime
			.process(ethereum_event())
			.await
			.expect_err("processing before start should fail");

		assert!(matches!(error, RuntimeError::NotStarted));
	}

	#[test]
	fn rejects_duplicate_plugins() {
		let mut runtime = Runtime::new(ChainId::ETHEREUM);

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
