use crate::{ChainId, CoreError};
use serde::{Deserialize, Serialize};

/// Source-independent metadata for an EVM block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockEvent {
	/// EIP-155 chain ID.
	pub chain_id: ChainId,

	/// Block height.
	pub block_number: u64,

	/// Block hash as a `0x`-prefixed 32-byte hexadecimal value.
	pub block_hash: String,

	/// Parent block hash as a `0x`-prefixed 32-byte hexadecimal value.
	pub parent_hash: String,

	/// Unix timestamp in seconds.
	pub timestamp: u64,

	/// Number of transactions included in the block.
	pub transaction_count: usize,
}

impl BlockEvent {
	/// Creates validated normalized block metadata.
	pub fn new(
		chain_id: ChainId,
		block_number: u64,
		block_hash: impl Into<String>,
		parent_hash: impl Into<String>,
		timestamp: u64,
		transaction_count: usize,
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

fn validate_block_hash(field: &'static str, value: &str) -> Result<(), CoreError> {
	let valid = value.len() == 66 &&
		value.starts_with("0x") &&
		value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit);

	if !valid {
		return Err(CoreError::InvalidBlockHash { field });
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	const BLOCK_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

	const PARENT_HASH: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";

	#[test]
	fn creates_valid_block_event() {
		let block = BlockEvent::new(
			ChainId::ETHEREUM,
			21_000_000,
			BLOCK_HASH,
			PARENT_HASH,
			1_720_000_000,
			150,
		)
		.expect("block should be valid");

		assert_eq!(block.chain_id, ChainId::ETHEREUM);
		assert_eq!(block.block_number, 21_000_000);
		assert_eq!(block.transaction_count, 150);
	}

	#[test]
	fn rejects_invalid_block_hash() {
		let error = BlockEvent::new(
			ChainId::ETHEREUM,
			21_000_000,
			"0x1234",
			PARENT_HASH,
			1_720_000_000,
			150,
		)
		.expect_err("invalid block hash should fail");

		assert_eq!(error, CoreError::InvalidBlockHash { field: "block_hash" });
	}

	#[test]
	fn rejects_identical_hashes_for_non_genesis_block() {
		let error = BlockEvent::new(
			ChainId::ETHEREUM,
			21_000_000,
			BLOCK_HASH,
			BLOCK_HASH,
			1_720_000_000,
			150,
		)
		.expect_err("identical hashes should fail");

		assert_eq!(error, CoreError::IdenticalBlockAndParentHash);
	}
}
