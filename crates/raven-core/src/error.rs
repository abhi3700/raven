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
}
