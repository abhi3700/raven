//! Converts Alloy RPC blocks into Raven core events.
//!
//! This module answers: "What Raven representation should this Alloy block
//! become?" It intentionally does not decide whether a block is canonical,
//! missing, reverted, or part of a reorg. Those ordering decisions live in
//! `source.rs`.
//!
//! ```text
//! Alloy Block
//!   - header number
//!   - header hash
//!   - parent hash
//!   - timestamp
//!   - transactions
//!        |
//!        v
//! BlockEvent::new(...)
//!   - validates Raven's source-independent block invariants
//!   - stores only normalized metadata needed by runtime/plugins
//!        |
//!        v
//! ChainEvent::BlockApplied
//! ```
//!
//! Keeping conversion small makes the source boundary clear: Alloy-specific
//! structures stop here, and the rest of Raven receives `raven_core` types.

use crate::{AlloySourceError, AlloySourceResult};
use alloy::{consensus::BlockHeader, network::BlockResponse, rpc::types::Block};
use raven_core::{BlockEvent, ChainEvent, ChainId};

/// Converts an Alloy RPC block into Raven's normalized event model.
///
/// The function extracts only the fields Raven core currently needs: chain ID,
/// block number, block hash, parent hash, timestamp, and transaction count.
/// `BlockEvent::new` is the validation boundary, so malformed source data is
/// reported as a block-conversion error before it can reach the runtime.
///
/// The returned event is always `ChainEvent::BlockApplied`. Revert events are
/// created by `source.rs` when reconciliation proves that a previously emitted
/// block is no longer part of the RPC node's canonical chain.
pub(crate) fn convert_block(chain_id: ChainId, block: &Block) -> AlloySourceResult<ChainEvent> {
	let header = block.header();
	let transaction_count = u64::try_from(block.transactions().len()).map_err(|error| {
		AlloySourceError::BlockConversion(format!("transaction count does not fit in u64: {error}"))
	})?;

	let normalized_block = BlockEvent::new(
		chain_id,
		header.number(),
		header.hash.to_string(),
		header.parent_hash().to_string(),
		header.timestamp(),
		transaction_count,
	)
	.map_err(|error| AlloySourceError::BlockConversion(error.to_string()))?;

	Ok(ChainEvent::BlockApplied(normalized_block))
}

#[cfg(test)]
mod tests {
	use alloy::{
		consensus::Header as ConsensusHeader,
		primitives::B256,
		rpc::types::{Block, Header},
	};
	use raven_core::ChainId;

	use super::*;

	#[test]
	fn converts_non_ethereum_alloy_block_into_applied_event() {
		let chain_id = ChainId::new(8_453).expect("Base chain ID should be valid");
		let consensus_header = ConsensusHeader {
			number: 21_000_000,
			parent_hash: B256::repeat_byte(0x22),
			timestamp: 1_720_000_000,
			..Default::default()
		};

		let rpc_header =
			Header { hash: B256::repeat_byte(0x11), inner: consensus_header, ..Default::default() };

		let block = Block { header: rpc_header, ..Default::default() };

		let event = convert_block(chain_id, &block).expect("block should be converted");

		assert!(event.is_applied());
		assert_eq!(event.chain_id(), chain_id);
		assert_eq!(event.block_number(), 21_000_000);
		assert_eq!(event.block().timestamp(), 1_720_000_000);
		assert_eq!(event.block().transaction_count(), 0);
	}
}
