use crate::{PluginContext, PluginMetadata, PluginResult};
use async_trait::async_trait;
use raven_core::ChainEvent;

/// A reusable Raven blockchain event processor.
#[async_trait]
pub trait Plugin: std::fmt::Debug + Send + Sync {
	/// Returns static information about the plugin.
	fn metadata(&self) -> PluginMetadata;

	/// Called once before the plugin begins receiving events.
	async fn start(&mut self, _context: &PluginContext) -> PluginResult {
		Ok(())
	}

	/// Processes a normalized chain event.
	async fn handle_event(&mut self, event: &ChainEvent, context: &PluginContext) -> PluginResult;

	/// Called once before the plugin is removed or Raven shuts down.
	async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use async_trait::async_trait;
	use raven_core::{BlockEvent, ChainEvent, ChainId};

	use super::*;

	#[derive(Debug)]
	struct TestPlugin {
		handled_events: usize,
		started: bool,
		stopped: bool,
	}

	impl TestPlugin {
		fn new() -> Self {
			Self { handled_events: 0, started: false, stopped: false }
		}
	}

	#[async_trait]
	impl Plugin for TestPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("test-plugin", "0.1.0", "Plugin used by raven-plugin-sdk tests")
		}

		async fn start(&mut self, _context: &PluginContext) -> PluginResult {
			self.started = true;
			Ok(())
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			self.handled_events += 1;
			Ok(())
		}

		async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
			self.stopped = true;
			Ok(())
		}
	}

	fn block_event() -> BlockEvent {
		BlockEvent::new(
			ChainId::ETHEREUM,
			21_000_000,
			"0x1111111111111111111111111111111111111111111111111111111111111111",
			"0x2222222222222222222222222222222222222222222222222222222222222222",
			1_720_000_000,
			150,
		)
		.expect("block should be valid")
	}

	#[tokio::test]
	async fn runs_plugin_lifecycle() {
		let context = PluginContext::new(ChainId::ETHEREUM);
		let event = ChainEvent::BlockApplied(block_event());
		let mut plugin = TestPlugin::new();

		plugin.start(&context).await.expect("plugin should start");

		plugin.handle_event(&event, &context).await.expect("plugin should handle event");

		plugin.shutdown(&context).await.expect("plugin should stop");

		assert!(plugin.started);
		assert!(plugin.stopped);
		assert_eq!(plugin.handled_events, 1);
	}

	#[test]
	fn exposes_plugin_metadata() {
		let plugin = TestPlugin::new();
		let metadata = plugin.metadata();

		assert_eq!(metadata.name, "test-plugin");
		assert_eq!(metadata.version, "0.1.0");
	}
}
