//! # Alloy Event Source
//!
//! This module implements Raven's **Alloy-based event source**.
//!
//! Its responsibility is to connect to an Ethereum-compatible JSON-RPC endpoint,
//! continuously observe newly mined blocks, normalize them into Raven's common
//! `ChainEvent` model, and publish those events to the rest of the system.
//!
//! ## Responsibilities
//!
//! - Connect to an Ethereum JSON-RPC endpoint.
//! - Discover and validate the remote chain ID.
//! - Poll the latest block number at a configurable interval.
//! - Fetch every newly observed block sequentially.
//! - Convert Alloy block responses into Raven's normalized `ChainEvent` model.
//! - Publish events through a Tokio `mpsc` channel.
//!
//! ## Why polling instead of `watch_full_blocks()`?
//!
//! Alloy provides `watch_full_blocks()`, which internally relies on
//! filter-based JSON-RPC APIs (e.g. `eth_newBlockFilter` and
//! `eth_getFilterChanges`).
//!
//! While this works well with many node implementations, several public HTTP
//! RPC providers either do not support these APIs or support them
//! inconsistently.
//!
//! To make Raven work reliably with **any standard HTTP RPC endpoint**, this
//! source instead:
//!
//! 1. Polls the latest block number using `eth_blockNumber`.
//! 2. Detects whether new blocks have been mined.
//! 3. Fetches each missing block using `eth_getBlockByNumber`.
//! 4. Emits one normalized `ChainEvent` per block.
//!
//! This guarantees that if multiple blocks are mined between polling
//! intervals, **every block is processed in order**.
//!
//! ## Event Flow
//!
//! ```text
//! Ethereum RPC
//!      │
//!      ▼
//! eth_blockNumber
//!      │
//!      ▼
//! Detect new blocks
//!      │
//!      ▼
//! eth_getBlockByNumber
//!      │
//!      ▼
//! convert_block()
//!      │
//!      ▼
//! ChainEvent
//!      │
//!      ▼
//! Tokio mpsc::Sender
//!      │
//!      ▼
//! Raven Runtime
//! ```
//!
//! ## Current Limitations
//!
//! The current implementation emits:
//!
//! - `ChainEvent::BlockApplied`
//!
//! It does **not yet detect blockchain reorganizations (reorgs)**.
//!
//! TODO: Reorg detection will be added in a future version by maintaining recent
//! canonical block history and emitting both:
//!
//! - `ChainEvent::BlockReverted`
//! - `ChainEvent::BlockApplied`
//!
//! when a canonical chain switch is detected.

use crate::{AlloySourceError, AlloySourceResult, converter::convert_block};
use alloy::{
	eips::BlockNumberOrTag,
	providers::{Provider, ProviderBuilder},
};
use raven_core::{ChainEvent, ChainId};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info};

/// Streams normalized EVM chain events from an Ethereum JSON-RPC endpoint.
pub struct AlloySource {
	rpc_url: String,
	poll_interval: Duration,
}

impl AlloySource {
	/// Creates an Alloy source using the default polling interval.
	pub fn new(rpc_url: impl Into<String>) -> Self {
		Self { rpc_url: rpc_url.into(), poll_interval: Duration::from_secs(4) }
	}

	/// Overrides the interval used to poll for new blocks.
	///
	/// A zero duration is rejected by [`Self::run`].
	#[must_use]
	pub const fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
		self.poll_interval = poll_interval;
		self
	}

	/// Connects to the RPC endpoint and streams normalized chain events.
	///
	/// This method continues running until:
	///
	/// - an RPC request fails;
	/// - event conversion fails; or
	/// - the receiving side of the channel is dropped.
	pub async fn run(self, sender: mpsc::Sender<ChainEvent>) -> AlloySourceResult {
		if self.poll_interval.is_zero() {
			return Err(AlloySourceError::InvalidPollInterval);
		}

		info!(
			rpc_url = %self.rpc_url,
			"connecting Alloy event source"
		);

		let provider = ProviderBuilder::new()
			.connect(&self.rpc_url)
			.await
			.map_err(|error| AlloySourceError::Connection(error.to_string()))?;

		let raw_chain_id = provider
			.get_chain_id()
			.await
			.map_err(|error| AlloySourceError::ChainIdRequest(error.to_string()))?;

		let chain_id = ChainId::new(raw_chain_id)
			.map_err(|_| AlloySourceError::InvalidChainId(raw_chain_id))?;

		info!(chain_id = chain_id.get(), "connected Alloy event source");

		let mut next_block_number = provider
			.get_block_number()
			.await
			.map_err(|error| AlloySourceError::BlockRequest(error.to_string()))?;

		let mut interval = tokio::time::interval(self.poll_interval);

		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

		loop {
			interval.tick().await;

			let latest_block_number = provider
				.get_block_number()
				.await
				.map_err(|error| AlloySourceError::BlockRequest(error.to_string()))?;

			while next_block_number <= latest_block_number {
				let block = provider
					.get_block_by_number(BlockNumberOrTag::Number(next_block_number))
					.await
					.map_err(|error| AlloySourceError::BlockRequest(error.to_string()))?
					.ok_or_else(|| {
						AlloySourceError::BlockRequest(format!(
							"block {next_block_number} was not returned by RPC"
						))
					})?;

				let event = convert_block(chain_id, &block)?;

				debug!(
					chain_id = event.chain_id().get(),
					block_number = event.block_number(),
					block_hash = %event.block().block_hash(),
					transactions = event.block().transaction_count(),
					"received block event"
				);

				sender.send(event).await.map_err(|_| AlloySourceError::EventReceiverDropped)?;

				next_block_number += 1;
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn rejects_zero_poll_interval_before_connecting() {
		let (sender, _receiver) = mpsc::channel(1);
		let source = AlloySource::new("not-a-valid-rpc-url").with_poll_interval(Duration::ZERO);

		let error = source.run(sender).await.expect_err("a zero polling interval must be rejected");

		assert!(matches!(error, AlloySourceError::InvalidPollInterval));
	}
}
