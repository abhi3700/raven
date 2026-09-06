//! # Block Logger Plugin
//!
//! This example demonstrates the smallest possible Raven plugin.
//!
//! The example does **not** require a running blockchain node. Instead, it
//! manually constructs a `ChainEvent` and invokes the plugin lifecycle.
//!
//! Eventually, this example will be replaced with a live version that receives
//! events from Raven's RPC source, making it a good reference for writing
//! custom Raven plugins.

use alloy_primitives::B256;
use async_trait::async_trait;
use eyre::Result;
use raven_core::{BlockEvent, ChainEvent, ChainId};
use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};

pub struct BlockLoggerPlugin;

#[tokio::main]
async fn main() -> Result<()> {
	let mut plugin = BlockLoggerPlugin;
	// Any non-zero EIP-155 chain ID is valid; this example uses 8453.
	let chain_id = ChainId::new(8_453)?;

	let context = PluginContext::new(chain_id);

	let block = BlockEvent::new(
		chain_id,
		21_000_000,
		B256::repeat_byte(0x11),
		B256::repeat_byte(0x22),
		1_720_000_000,
		150,
	)?;

	let event = ChainEvent::BlockApplied(block);

	println!("Starting plugin...\n");

	plugin.start(&context).await?;

	plugin.handle_event(&event, &context).await?;

	plugin.shutdown(&context).await?;

	println!("\nPlugin finished successfully.");

	Ok(())
}

#[async_trait]
impl Plugin for BlockLoggerPlugin {
	fn metadata(&self) -> PluginMetadata {
		PluginMetadata::new(
			"block-logger",
			env!("CARGO_PKG_VERSION"),
			"Prints information about newly applied blocks.",
		)
	}

	async fn start(&mut self, context: &PluginContext) -> PluginResult {
		println!(
			"🚀 Starting '{}' on chain {}",
			self.metadata().name,
			u64::from(context.chain_id())
		);

		Ok(())
	}

	async fn handle_event(&mut self, event: &ChainEvent, _context: &PluginContext) -> PluginResult {
		let block = event.block();

		println!("\n📦 Block #{}", block.block_number());

		println!("Chain ID          : {}", u64::from(block.chain_id()));
		println!("Timestamp         : {}", block.timestamp());
		println!("Transactions      : {}", block.transaction_count());
		println!("Block Hash        : {}", block.block_hash());
		println!("Parent Hash       : {}", block.parent_hash());
		println!("Applied           : {}", event.is_applied());

		Ok(())
	}

	async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
		println!("\n🛑 Shutting down plugin...");

		Ok(())
	}
}
