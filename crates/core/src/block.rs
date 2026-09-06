use crate::{ChainId, CoreError, EvmLog};
use alloy_primitives::B256;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

/// Source-independent metadata and normalized EVM logs for one canonical block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockEvent {
	/// EIP-155 chain ID.
	chain_id: ChainId,

	/// Block height.
	block_number: u64,

	/// Canonical 32-byte block hash.
	block_hash: B256,

	/// Canonical 32-byte parent block hash.
	parent_hash: B256,

	/// Unix timestamp in seconds.
	timestamp: u64,

	/// Number of transactions included in the block.
	transaction_count: u64,

	/// Mined EVM logs ordered by transaction index and then log index.
	logs: Vec<EvmLog>,
}

impl BlockEvent {
	/// Creates validated normalized block metadata without log payloads.
	pub fn new(
		chain_id: ChainId,
		block_number: u64,
		block_hash: B256,
		parent_hash: B256,
		timestamp: u64,
		transaction_count: u64,
	) -> Result<Self, CoreError> {
		Self::new_with_logs(
			chain_id,
			block_number,
			block_hash,
			parent_hash,
			timestamp,
			transaction_count,
			Vec::new(),
		)
	}

	/// Creates a validated normalized block with its mined EVM logs.
	pub fn new_with_logs(
		chain_id: ChainId,
		block_number: u64,
		block_hash: B256,
		parent_hash: B256,
		timestamp: u64,
		transaction_count: u64,
		logs: Vec<EvmLog>,
	) -> Result<Self, CoreError> {
		let block = Self {
			chain_id,
			block_number,
			block_hash,
			parent_hash,
			timestamp,
			transaction_count,
			logs,
		};

		block.validate()?;
		Ok(block)
	}

	/// Returns the EIP-155 chain ID.
	pub const fn chain_id(&self) -> ChainId {
		self.chain_id
	}

	/// Returns the block height.
	pub const fn block_number(&self) -> u64 {
		self.block_number
	}

	/// Returns the canonical 32-byte block hash.
	pub const fn block_hash(&self) -> B256 {
		self.block_hash
	}

	/// Returns the canonical 32-byte parent block hash.
	pub const fn parent_hash(&self) -> B256 {
		self.parent_hash
	}

	/// Returns the Unix timestamp in seconds.
	pub const fn timestamp(&self) -> u64 {
		self.timestamp
	}

	/// Returns the number of transactions included in the block.
	pub const fn transaction_count(&self) -> u64 {
		self.transaction_count
	}

	/// Returns mined EVM logs in deterministic block order.
	pub fn logs(&self) -> &[EvmLog] {
		&self.logs
	}

	/// Validates source-independent block invariants.
	pub fn validate(&self) -> Result<(), CoreError> {
		if self.block_number > 0 && self.block_hash == self.parent_hash {
			return Err(CoreError::IdenticalBlockAndParentHash);
		}

		for log in &self.logs {
			if log.transaction_index() >= self.transaction_count {
				return Err(CoreError::LogTransactionIndexOutOfBounds {
					transaction_index: log.transaction_index(),
					transaction_count: self.transaction_count,
				});
			}
		}

		if self.logs.windows(2).any(|logs| {
			(logs[0].transaction_index(), logs[0].log_index()) >=
				(logs[1].transaction_index(), logs[1].log_index())
		}) {
			return Err(CoreError::InvalidLogOrder);
		}

		Ok(())
	}
}

#[derive(Deserialize)]
struct BlockEventData {
	chain_id: ChainId,
	block_number: u64,
	block_hash: String,
	parent_hash: String,
	timestamp: u64,
	transaction_count: u64,
	#[serde(default)]
	logs: Vec<EvmLog>,
}

impl<'de> Deserialize<'de> for BlockEvent {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let data = BlockEventData::deserialize(deserializer)?;
		let block_hash =
			parse_prefixed_hash("block_hash", &data.block_hash).map_err(D::Error::custom)?;
		let parent_hash =
			parse_prefixed_hash("parent_hash", &data.parent_hash).map_err(D::Error::custom)?;

		Self::new_with_logs(
			data.chain_id,
			data.block_number,
			block_hash,
			parent_hash,
			data.timestamp,
			data.transaction_count,
			data.logs,
		)
		.map_err(D::Error::custom)
	}
}

fn parse_prefixed_hash(field: &'static str, value: &str) -> Result<B256, CoreError> {
	if !value.starts_with("0x") {
		return Err(CoreError::InvalidBlockHash { field });
	}
	value.parse().map_err(|_| CoreError::InvalidBlockHash { field })
}

#[cfg(test)]
mod tests {
	use alloy_primitives::{Address, B256, Bytes};

	use super::*;

	const BLOCK_HASH: B256 = B256::repeat_byte(0x11);
	const PARENT_HASH: B256 = B256::repeat_byte(0x22);

	fn test_chain_id() -> ChainId {
		ChainId::new(8_453).expect("test chain ID should be valid")
	}

	fn log(transaction_index: u64, log_index: u64) -> EvmLog {
		EvmLog::new(
			Address::repeat_byte(0x33),
			vec![B256::repeat_byte(0x44)],
			Bytes::new(),
			B256::repeat_byte(0x55),
			transaction_index,
			log_index,
		)
		.unwrap()
	}

	#[test]
	fn creates_valid_block_event() {
		let block = BlockEvent::new(
			test_chain_id(),
			21_000_000,
			BLOCK_HASH,
			PARENT_HASH,
			1_720_000_000,
			150,
		)
		.expect("block should be valid");

		assert_eq!(block.chain_id(), test_chain_id());
		assert_eq!(block.block_number(), 21_000_000);
		assert_eq!(block.block_hash(), BLOCK_HASH);
		assert_eq!(block.parent_hash(), PARENT_HASH);
		assert_eq!(block.timestamp(), 1_720_000_000);
		assert_eq!(block.transaction_count(), 150);
		assert!(block.logs().is_empty());
	}

	#[test]
	fn creates_block_with_ordered_logs() {
		let block = BlockEvent::new_with_logs(
			test_chain_id(),
			21_000_000,
			BLOCK_HASH,
			PARENT_HASH,
			1_720_000_000,
			2,
			vec![log(0, 0), log(1, 1)],
		)
		.unwrap();

		assert_eq!(block.logs().len(), 2);
	}

	#[test]
	fn rejects_identical_hashes_for_non_genesis_block() {
		let error = BlockEvent::new(
			test_chain_id(),
			21_000_000,
			BLOCK_HASH,
			BLOCK_HASH,
			1_720_000_000,
			150,
		)
		.expect_err("identical hashes should fail");

		assert_eq!(error, CoreError::IdenticalBlockAndParentHash);
	}

	#[test]
	fn rejects_log_outside_transaction_count() {
		let error = BlockEvent::new_with_logs(
			test_chain_id(),
			21_000_000,
			BLOCK_HASH,
			PARENT_HASH,
			1_720_000_000,
			1,
			vec![log(1, 0)],
		)
		.unwrap_err();

		assert_eq!(
			error,
			CoreError::LogTransactionIndexOutOfBounds {
				transaction_index: 1,
				transaction_count: 1,
			}
		);
	}

	#[test]
	fn rejects_duplicate_or_unsorted_log_positions() {
		let error = BlockEvent::new_with_logs(
			test_chain_id(),
			21_000_000,
			BLOCK_HASH,
			PARENT_HASH,
			1_720_000_000,
			2,
			vec![log(1, 1), log(0, 0)],
		)
		.unwrap_err();

		assert_eq!(error, CoreError::InvalidLogOrder);
	}

	#[test]
	fn rejects_invalid_block_during_deserialization() {
		let json = format!(
			r#"{{
				"chain_id": 8453,
				"block_number": 21000000,
				"block_hash": "{BLOCK_HASH}",
				"parent_hash": "{BLOCK_HASH}",
				"timestamp": 1720000000,
				"transaction_count": 150,
				"logs": []
			}}"#
		);

		let error = serde_json::from_str::<BlockEvent>(&json)
			.expect_err("deserialization must preserve block invariants");

		assert!(error.to_string().contains("block hash and parent hash must differ"));
	}

	#[test]
	fn rejects_unprefixed_hash_during_deserialization() {
		let json = format!(
			r#"{{
				"chain_id": 8453,
				"block_number": 21000000,
				"block_hash": "{}",
				"parent_hash": "{PARENT_HASH}",
				"timestamp": 1720000000,
				"transaction_count": 150,
				"logs": []
			}}"#,
			&BLOCK_HASH.to_string()[2..],
		);

		assert!(serde_json::from_str::<BlockEvent>(&json).is_err());
	}

	#[test]
	fn deserializes_legacy_block_without_logs() {
		let json = format!(
			r#"{{
				"chain_id": 8453,
				"block_number": 21000000,
				"block_hash": "{BLOCK_HASH}",
				"parent_hash": "{PARENT_HASH}",
				"timestamp": 1720000000,
				"transaction_count": 150
			}}"#
		);

		let block: BlockEvent = serde_json::from_str(&json).unwrap();
		assert!(block.logs().is_empty());
	}
}
