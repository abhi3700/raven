use crate::{ChainId, CoreError};
use alloy_primitives::B256;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

/// Source-independent metadata for an EVM block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockEvent {
	/// EIP-155 chain ID.
	chain_id: ChainId,

	/// Block height.
	block_number: u64,

	/// Block hash as a `0x`-prefixed 32-byte hexadecimal value.
	block_hash: String,

	/// Parent block hash as a `0x`-prefixed 32-byte hexadecimal value.
	parent_hash: String,

	/// Unix timestamp in seconds.
	timestamp: u64,

	/// Number of transactions included in the block.
	transaction_count: u64,
}

impl BlockEvent {
	/// Creates validated normalized block metadata.
	pub fn new(
		chain_id: ChainId,
		block_number: u64,
		block_hash: impl Into<String>,
		parent_hash: impl Into<String>,
		timestamp: u64,
		transaction_count: u64,
	) -> Result<Self, CoreError> {
		let block = Self {
			chain_id,
			block_number,
			block_hash: block_hash.into(),
			parent_hash: parent_hash.into(),
			timestamp,
			transaction_count,
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

	/// Returns the `0x`-prefixed 32-byte block hash.
	pub fn block_hash(&self) -> &str {
		&self.block_hash
	}

	/// Returns the `0x`-prefixed 32-byte parent block hash.
	pub fn parent_hash(&self) -> &str {
		&self.parent_hash
	}

	/// Returns the Unix timestamp in seconds.
	pub const fn timestamp(&self) -> u64 {
		self.timestamp
	}

	/// Returns the number of transactions included in the block.
	pub const fn transaction_count(&self) -> u64 {
		self.transaction_count
	}

	/// Validates source-independent block invariants.
	pub fn validate(&self) -> Result<(), CoreError> {
		validate_block_hash("block_hash", &self.block_hash)?;
		validate_block_hash("parent_hash", &self.parent_hash)?;

		if self.block_number > 0 && self.block_hash == self.parent_hash {
			return Err(CoreError::IdenticalBlockAndParentHash);
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
}

impl<'de> Deserialize<'de> for BlockEvent {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let data = BlockEventData::deserialize(deserializer)?;

		Self::new(
			data.chain_id,
			data.block_number,
			data.block_hash,
			data.parent_hash,
			data.timestamp,
			data.transaction_count,
		)
		.map_err(D::Error::custom)
	}
}

fn validate_block_hash(field: &'static str, value: &str) -> Result<(), CoreError> {
	if !value.starts_with("0x") || value.parse::<B256>().is_err() {
		return Err(CoreError::InvalidBlockHash { field });
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	const BLOCK_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

	const PARENT_HASH: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";

	fn test_chain_id() -> ChainId {
		ChainId::new(8_453).expect("test chain ID should be valid")
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
	}

	#[test]
	fn rejects_invalid_block_hash() {
		let error =
			BlockEvent::new(test_chain_id(), 21_000_000, "0x1234", PARENT_HASH, 1_720_000_000, 150)
				.expect_err("invalid block hash should fail");

		assert_eq!(error, CoreError::InvalidBlockHash { field: "block_hash" });
	}

	#[test]
	fn rejects_unprefixed_block_hash() {
		let error = BlockEvent::new(
			test_chain_id(),
			21_000_000,
			&BLOCK_HASH[2..],
			PARENT_HASH,
			1_720_000_000,
			150,
		)
		.expect_err("unprefixed block hash should fail");

		assert_eq!(error, CoreError::InvalidBlockHash { field: "block_hash" });
	}

	#[test]
	fn rejects_non_hex_block_hash() {
		let invalid_hash = format!("0x{}z", "1".repeat(63));
		let error = BlockEvent::new(
			test_chain_id(),
			21_000_000,
			invalid_hash,
			PARENT_HASH,
			1_720_000_000,
			150,
		)
		.expect_err("non-hex block hash should fail");

		assert_eq!(error, CoreError::InvalidBlockHash { field: "block_hash" });
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
	fn rejects_invalid_block_during_deserialization() {
		let json = format!(
			r#"{{
				"chain_id": 8453,
				"block_number": 21000000,
				"block_hash": "0x1234",
				"parent_hash": "{PARENT_HASH}",
				"timestamp": 1720000000,
				"transaction_count": 150
			}}"#
		);

		let error = serde_json::from_str::<BlockEvent>(&json)
			.expect_err("deserialization must preserve block invariants");

		assert!(error.to_string().contains("block_hash must be a 0x-prefixed"));
	}
}
