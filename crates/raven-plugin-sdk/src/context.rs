use raven_core::ChainId;

/// Runtime context supplied to a plugin for every event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginContext {
	chain_id: ChainId,
	// TODO: later on, add these:
	// storage

	// logger

	// metrics

	// event emitter

	// configuration

	// shutdown signal
}

impl PluginContext {
	/// Creates a new plugin execution context.
	pub const fn new(chain_id: ChainId) -> Self {
		Self { chain_id }
	}

	/// Returns the active EVM chain ID.
	pub const fn chain_id(&self) -> ChainId {
		self.chain_id
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn creates_plugin_context() {
		let chain_id = ChainId::new(8_453).expect("test chain ID should be valid");
		let context = PluginContext::new(chain_id);

		assert_eq!(context.chain_id(), chain_id);
	}
}
