use crate::{PluginContext, PluginMetadata, PluginResult};
use async_trait::async_trait;
use raven_core::ChainEvent;

/// A reusable Raven plugin that processes normalized blockchain events.
#[async_trait]
pub trait Plugin: Send + Sync {
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
	use alloy_primitives::B256;
	use async_trait::async_trait;
	use raven_core::{BlockEvent, ChainEvent, ChainId};

	use super::*;

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
			ChainId::new(8_453).expect("test chain ID should be valid"),
			21_000_000,
			B256::repeat_byte(0x11),
			B256::repeat_byte(0x22),
			1_720_000_000,
			150,
		)
		.expect("block should be valid")
	}

	fn block_event2() -> BlockEvent {
		BlockEvent::new(
			ChainId::new(8_453).expect("test chain ID should be valid"),
			21_000_001,
			B256::repeat_byte(0x11),
			B256::repeat_byte(0x22),
			1_720_000_001,
			150,
		)
		.expect("block should be valid")
	}

	#[tokio::test]
	async fn runs_plugin_lifecycle() {
		let context =
			PluginContext::new(ChainId::new(8_453).expect("test chain ID should be valid"));
		let event = ChainEvent::BlockApplied(block_event());
		let mut plugin = TestPlugin::new();

		plugin.start(&context).await.expect("plugin should start");

		plugin.handle_event(&event, &context).await.expect("plugin should handle event");

		plugin.shutdown(&context).await.expect("plugin should stop");

		assert!(plugin.started);
		assert!(plugin.stopped);
		assert_eq!(plugin.handled_events, 1);
	}

	#[tokio::test]
	async fn runs_plugin_lifecycle_w_more_events() {
		let context =
			PluginContext::new(ChainId::new(8_453).expect("test chain ID should be valid"));
		let event = ChainEvent::BlockApplied(block_event());
		let event2 = ChainEvent::BlockApplied(block_event2());
		let mut plugin = TestPlugin::new();

		plugin.start(&context).await.expect("plugin should start");

		plugin.handle_event(&event, &context).await.expect("plugin should handle event");
		plugin
			.handle_event(&event2, &context)
			.await
			.expect("plugin should handle event");

		plugin.shutdown(&context).await.expect("plugin should stop");

		assert!(plugin.started);
		assert!(plugin.stopped);
		assert_eq!(plugin.handled_events, 2);
	}

	#[test]
	fn exposes_plugin_metadata() {
		let plugin = TestPlugin::new();
		let metadata = plugin.metadata();

		assert_eq!(metadata.name, "test-plugin");
		assert_eq!(metadata.version, "0.1.0");
	}
}
