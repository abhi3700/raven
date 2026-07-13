//! convert Alloy block → Raven ChainEvent

use crate::{AlloySourceError, AlloySourceResult};
use alloy::{consensus::BlockHeader, network::BlockResponse, rpc::types::Block};
use raven_core::{BlockEvent, ChainEvent, ChainId};

/// Converts an Alloy RPC block into Raven's normalized event model.
pub(crate) fn convert_block(chain_id: ChainId, block: &Block) -> AlloySourceResult<ChainEvent> {
	let header = block.header();

	let normalized_block = BlockEvent::new(
		chain_id,
		header.number(),
		header.hash.to_string(),
		header.parent_hash().to_string(),
		header.timestamp(),
		block.transactions().len(),
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
	fn converts_alloy_block_into_applied_event() {
		let consensus_header = ConsensusHeader {
			number: 21_000_000,
			parent_hash: B256::repeat_byte(0x22),
			timestamp: 1_720_000_000,
			..Default::default()
		};

		let rpc_header =
			Header { hash: B256::repeat_byte(0x11), inner: consensus_header, ..Default::default() };

		let block = Block { header: rpc_header, ..Default::default() };

		let event = convert_block(ChainId::ETHEREUM, &block).expect("block should be converted");

		assert!(event.is_applied());
		assert_eq!(event.chain_id(), ChainId::ETHEREUM);
		assert_eq!(event.block_number(), 21_000_000);
		assert_eq!(event.block().timestamp, 1_720_000_000);
		assert_eq!(event.block().transaction_count, 0);
	}
}
