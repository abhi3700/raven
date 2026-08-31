use thiserror::Error;

/// Errors produced by Raven's normalized core types.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CoreError {
	#[error("chain ID must be greater than zero")]
	InvalidChainId,

	#[error("{field} must be a 0x-prefixed 32-byte hexadecimal value")]
	InvalidBlockHash { field: &'static str },

	#[error("block hash and parent hash must differ for non-genesis blocks")]
	IdenticalBlockAndParentHash,

	#[error("EVM log has {count} topics; at most four are allowed")]
	TooManyLogTopics { count: usize },

	#[error(
		"EVM log transaction index {transaction_index} is outside block transaction count {transaction_count}"
	)]
	LogTransactionIndexOutOfBounds { transaction_index: u64, transaction_count: u64 },

	#[error("EVM logs must be strictly ordered by transaction index and log index")]
	InvalidLogOrder,
}
