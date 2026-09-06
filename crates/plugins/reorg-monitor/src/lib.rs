//! Statically linked Raven plugin for observing canonical-chain changes.
//!
//! The source is responsible for detecting a fork and ordering its normalized
//! [`raven_core::ChainEvent`] values. This plugin does not make RPC calls or
//! independently infer a reorg. It reports each applied or reverted block to
//! tracing so an operator can see the exact canonical-chain transition.
//!
//! ```text
//! source detects a shallow fork
//!             |
//!             v
//! BlockApplied(100 A) -> BlockApplied(101 B) -> BlockApplied(102 C)
//!                                                     |
//!                                           old tip no longer canonical
//!                                                     v
//!                      BlockReverted(102 C) -> BlockReverted(101 B)
//!                                                     |
//!                                           replacement branch is canonical
//!                                                     v
//!                      BlockApplied(101 D) -> BlockApplied(102 E)
//!                                                     |
//!                                                     v
//!                                            ReorgMonitorPlugin -> tracing
//! ```

use async_trait::async_trait;
use raven_core::ChainEvent;
use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};
use tracing::{info, warn};

/// Statically linked plugin that reports normalized canonical-chain transitions.
///
/// The plugin is deliberately stateless. A source has already verified the
/// retained ancestry and emits reverts from the old tip toward the common
/// ancestor, followed by replacement applies in ascending block order.
#[derive(Debug, Default)]
pub struct ReorgMonitorPlugin;

impl ReorgMonitorPlugin {
	/// Creates a monitor that writes every delivered chain transition to tracing.
	pub const fn new() -> Self {
		Self
	}
}

/// Canonical-chain direction represented by a normalized event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalTransition {
	Applied,
	Reverted,
}

impl CanonicalTransition {
	const fn from_event(event: &ChainEvent) -> Self {
		match event {
			ChainEvent::BlockApplied(_) => Self::Applied,
			ChainEvent::BlockReverted(_) => Self::Reverted,
		}
	}
}

#[async_trait]
impl Plugin for ReorgMonitorPlugin {
	fn metadata(&self) -> PluginMetadata {
		PluginMetadata::new(
			"reorg-monitor",
			env!("CARGO_PKG_VERSION"),
			"Reports applied and reverted blocks to make shallow reorgs visible",
		)
	}

	async fn start(&mut self, context: &PluginContext) -> PluginResult {
		info!(chain_id = context.chain_id().get(), "reorg monitor started");
		Ok(())
	}

	async fn handle_event(&mut self, event: &ChainEvent, _context: &PluginContext) -> PluginResult {
		let block = event.block();
		match CanonicalTransition::from_event(event) {
			CanonicalTransition::Applied => {
				info!(
					event_type = "BlockApplied",
					chain_id = block.chain_id().get(),
					block_number = block.block_number(),
					block_hash = %block.block_hash(),
					parent_hash = %block.parent_hash(),
					timestamp = block.timestamp(),
					transactions = block.transaction_count(),
					logs = block.logs().len(),
					"block joined canonical chain"
				);
			},
			CanonicalTransition::Reverted => {
				warn!(
					event_type = "BlockReverted",
					chain_id = block.chain_id().get(),
					block_number = block.block_number(),
					block_hash = %block.block_hash(),
					parent_hash = %block.parent_hash(),
					timestamp = block.timestamp(),
					transactions = block.transaction_count(),
					logs = block.logs().len(),
					"block left canonical chain; shallow reorg detected"
				);
			},
		}
		Ok(())
	}

	async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
		info!("reorg monitor stopped");
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use alloy_primitives::B256;
	use raven_core::{BlockEvent, ChainId};

	use super::*;

	fn block(number: u64, hash: u8, parent_hash: u8) -> BlockEvent {
		BlockEvent::new(
			ChainId::new(8453).unwrap(),
			number,
			B256::with_last_byte(hash),
			B256::with_last_byte(parent_hash),
			1_720_000_000,
			1,
		)
		.unwrap()
	}

	#[test]
	fn exposes_reorg_monitor_metadata() {
		let metadata = ReorgMonitorPlugin::new().metadata();

		assert_eq!(metadata.name, "reorg-monitor");
		assert!(metadata.description.contains("reverted"));
	}

	#[tokio::test]
	async fn classifies_reverted_blocks_before_replacement_blocks() {
		let context = PluginContext::new(ChainId::new(8453).unwrap());
		let mut plugin = ReorgMonitorPlugin::new();
		let events = [
			ChainEvent::BlockApplied(block(100, 0x0a, 0x09)),
			ChainEvent::BlockApplied(block(101, 0x0b, 0x0a)),
			ChainEvent::BlockApplied(block(102, 0x0c, 0x0b)),
			ChainEvent::BlockReverted(block(102, 0x0c, 0x0b)),
			ChainEvent::BlockReverted(block(101, 0x0b, 0x0a)),
			ChainEvent::BlockApplied(block(101, 0x0d, 0x0a)),
			ChainEvent::BlockApplied(block(102, 0x0e, 0x0d)),
		];

		let transitions: Vec<_> = events.iter().map(CanonicalTransition::from_event).collect();
		assert_eq!(
			transitions,
			[
				CanonicalTransition::Applied,
				CanonicalTransition::Applied,
				CanonicalTransition::Applied,
				CanonicalTransition::Reverted,
				CanonicalTransition::Reverted,
				CanonicalTransition::Applied,
				CanonicalTransition::Applied,
			]
		);

		for event in events {
			plugin.handle_event(&event, &context).await.unwrap();
		}
	}
}
