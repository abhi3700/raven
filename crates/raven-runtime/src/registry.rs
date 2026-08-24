use crate::{RuntimeError, RuntimeResult};
use raven_plugin_sdk::Plugin;

/// Stores plugins registered with a Raven runtime.
#[derive(Default)]
pub(crate) struct PluginRegistry {
	plugins: Vec<Box<dyn Plugin>>,
}

use std::fmt;

impl fmt::Debug for PluginRegistry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("PluginRegistry")
			.field("plugin_count", &self.plugins.len())
			.finish()
	}
}

impl PluginRegistry {
	/// Creates an empty plugin registry.
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Registers a plugin.
	///
	/// Plugin names must be unique within one runtime instance.
	pub(crate) fn register(&mut self, plugin: Box<dyn Plugin>) -> RuntimeResult {
		let plugin_name = plugin.metadata().name;

		let already_registered =
			self.plugins.iter().any(|registered| registered.metadata().name == plugin_name);

		if already_registered {
			return Err(RuntimeError::DuplicatePlugin { name: plugin_name.to_owned() });
		}

		self.plugins.push(plugin);

		Ok(())
	}

	/// Transfers all registered plugins to runtime workers.
	pub(crate) fn take_plugins(&mut self) -> Vec<Box<dyn Plugin>> {
		std::mem::take(&mut self.plugins)
	}

	/// Returns the number of registered plugins.
	pub(crate) fn len(&self) -> usize {
		self.plugins.len()
	}

	/// Returns whether no plugins are registered.
	pub(crate) fn is_empty(&self) -> bool {
		self.plugins.is_empty()
	}
}

#[cfg(test)]
mod tests {
	use async_trait::async_trait;
	use raven_core::ChainEvent;
	use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};

	use super::*;

	struct TestPlugin {
		name: &'static str,
	}

	#[async_trait]
	impl Plugin for TestPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new(self.name, "0.1.0", "Registry test plugin")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			Ok(())
		}
	}

	#[test]
	fn registers_plugin() {
		let mut registry = PluginRegistry::new();

		registry
			.register(Box::new(TestPlugin { name: "test-plugin" }))
			.expect("plugin should be registered");

		assert_eq!(registry.len(), 1);
		assert!(!registry.is_empty());
	}

	#[test]
	fn rejects_duplicate_plugin_name() {
		let mut registry = PluginRegistry::new();

		registry
			.register(Box::new(TestPlugin { name: "test-plugin" }))
			.expect("first plugin should be registered");

		let error = registry
			.register(Box::new(TestPlugin { name: "test-plugin" }))
			.expect_err("duplicate plugin should fail");

		assert!(matches!(
			error,
			RuntimeError::DuplicatePlugin { name }
				if name == "test-plugin"
		));
	}
}
