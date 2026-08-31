//! Converts Alloy RPC blocks and logs into Raven core events.
//!
//! This module answers: "What Raven representation should this Alloy response become?" It does
//! not decide whether a block is canonical, missing, reverted, or part of a reorg. Those ordering
//! decisions live in `source.rs`.
//!
//! ```text
//! Alloy Block + validated eth_getLogs result
//!          |
//!          v
//! validate every log belongs to the block and has mined metadata
//!          |
//!          v
//! EvmLog[] sorted by (transaction_index, log_index)
//!          |
//!          v
//! BlockEvent::new_with_logs(...) -> ChainEvent::BlockApplied
//! ```
//!
//! Keeping conversion here prevents Alloy RPC types from leaking into Raven's runtime or plugins.

use crate::{AlloySourceError, AlloySourceResult};
use alloy::{
	consensus::BlockHeader,
	network::BlockResponse,
	rpc::types::{Block, Log},
};
use raven_core::{BlockEvent, ChainEvent, ChainId, EvmLog};

/// Converts an Alloy RPC block and its validated logs into Raven's normalized event model.
///
/// Revert events are created by `source.rs` when reconciliation proves that a previously emitted
/// block is no longer canonical. Because the cursor retains this complete `BlockEvent`, the
/// corresponding revert carries the same logs plugins observed during apply.
pub(crate) fn convert_block(
	chain_id: ChainId,
	block: &Block,
	logs: Vec<Log>,
) -> AlloySourceResult<ChainEvent> {
	let header = block.header();
	let transaction_count = u64::try_from(block.transactions().len()).map_err(|error| {
		AlloySourceError::BlockConversion(format!("transaction count does not fit in u64: {error}"))
	})?;
	let logs = convert_logs(logs, header.number(), header.hash, transaction_count)?;

	let normalized_block = BlockEvent::new_with_logs(
		chain_id,
		header.number(),
		header.hash,
		header.parent_hash(),
		header.timestamp(),
		transaction_count,
		logs,
	)
	.map_err(|error| AlloySourceError::BlockConversion(error.to_string()))?;

	Ok(ChainEvent::BlockApplied(normalized_block))
}

fn convert_logs(
	logs: Vec<Log>,
	block_number: u64,
	block_hash: alloy::primitives::B256,
	transaction_count: u64,
) -> AlloySourceResult<Vec<EvmLog>> {
	let mut normalized = Vec::with_capacity(logs.len());

	for log in logs {
		if log.removed {
			return Err(AlloySourceError::LogConversion(format!(
				"RPC marked a log from block {block_number} as removed"
			)));
		}
		if log.block_number != Some(block_number) {
			return Err(AlloySourceError::LogConversion(format!(
				"log block number {:?} does not match requested block {block_number}",
				log.block_number
			)));
		}
		if log.block_hash != Some(block_hash) {
			return Err(AlloySourceError::LogConversion(format!(
				"log block hash {:?} does not match requested block {block_hash}",
				log.block_hash
			)));
		}

		let transaction_hash =
			required_log_field(log.transaction_hash, "transaction_hash", block_number)?;
		let transaction_index =
			required_log_field(log.transaction_index, "transaction_index", block_number)?;
		let log_index = required_log_field(log.log_index, "log_index", block_number)?;
		let (topics, data) = log.inner.data.split();

		normalized.push(
			EvmLog::new(
				log.inner.address,
				topics,
				data,
				transaction_hash,
				transaction_index,
				log_index,
			)
			.map_err(|error| AlloySourceError::LogConversion(error.to_string()))?,
		);
	}

	normalized.sort_unstable_by_key(|log| (log.transaction_index(), log.log_index()));

	// BlockEvent owns cross-log ordering and transaction-bound validation. Running it here produces
	// a log-specific source error before the full block is constructed.
	for log in &normalized {
		if log.transaction_index() >= transaction_count {
			return Err(AlloySourceError::LogConversion(format!(
				"log transaction index {} is outside block transaction count {transaction_count}",
				log.transaction_index()
			)));
		}
	}
	if normalized.windows(2).any(|logs| {
		(logs[0].transaction_index(), logs[0].log_index()) ==
			(logs[1].transaction_index(), logs[1].log_index())
	}) {
		return Err(AlloySourceError::LogConversion(
			"RPC returned duplicate transaction/log positions".to_owned(),
		));
	}

	Ok(normalized)
}

fn required_log_field<T>(
	value: Option<T>,
	field: &'static str,
	block_number: u64,
) -> AlloySourceResult<T> {
	value.ok_or_else(|| {
		AlloySourceError::LogConversion(format!(
			"mined log from block {block_number} is missing {field}"
		))
	})
}

#[cfg(test)]
mod tests {
	use alloy::{
		consensus::Header as ConsensusHeader,
		primitives::{Address, B256, Bytes, Log as PrimitiveLog, LogData},
		rpc::types::{Block, Header, Log},
	};
	use raven_core::ChainId;

	use super::*;

	fn rpc_block(transaction_count: usize) -> Block {
		let consensus_header = ConsensusHeader {
			number: 21_000_000,
			parent_hash: B256::repeat_byte(0x22),
			timestamp: 1_720_000_000,
			..Default::default()
		};
		let rpc_header =
			Header { hash: B256::repeat_byte(0x11), inner: consensus_header, ..Default::default() };
		let mut block = Block { header: rpc_header, ..Default::default() };
		block.transactions = alloy::rpc::types::BlockTransactions::Hashes(
			(0..transaction_count).map(|index| B256::with_last_byte(index as u8)).collect(),
		);
		block
	}

	fn rpc_log(transaction_index: u64, log_index: u64) -> Log {
		Log {
			inner: PrimitiveLog {
				address: Address::repeat_byte(0x33),
				data: LogData::new(vec![B256::repeat_byte(0x44)], Bytes::from_static(&[0x55]))
					.unwrap(),
			},
			block_hash: Some(B256::repeat_byte(0x11)),
			block_number: Some(21_000_000),
			transaction_hash: Some(B256::repeat_byte(0x66)),
			transaction_index: Some(transaction_index),
			log_index: Some(log_index),
			..Default::default()
		}
	}

	#[test]
	fn converts_and_sorts_validated_logs() {
		let chain_id = ChainId::new(8_453).unwrap();
		let block = rpc_block(2);
		let event = convert_block(chain_id, &block, vec![rpc_log(1, 2), rpc_log(0, 0)]).unwrap();

		assert!(event.is_applied());
		assert_eq!(event.block().logs().len(), 2);
		assert_eq!(event.block().logs()[0].transaction_index(), 0);
		assert_eq!(event.block().logs()[1].log_index(), 2);
	}

	#[test]
	fn rejects_log_from_another_block_hash() {
		let chain_id = ChainId::new(8_453).unwrap();
		let block = rpc_block(1);
		let mut log = rpc_log(0, 0);
		log.block_hash = Some(B256::repeat_byte(0x99));

		assert!(matches!(
			convert_block(chain_id, &block, vec![log]),
			Err(AlloySourceError::LogConversion(_))
		));
	}

	#[test]
	fn rejects_log_missing_mined_metadata() {
		let chain_id = ChainId::new(8_453).unwrap();
		let block = rpc_block(1);
		let mut log = rpc_log(0, 0);
		log.transaction_hash = None;

		assert!(matches!(
			convert_block(chain_id, &block, vec![log]),
			Err(AlloySourceError::LogConversion(_))
		));
	}
}
