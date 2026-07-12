use crate::CoreError;
use serde::{Deserialize, Serialize};

/// EIP-155 chain identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChainId(u64);

impl ChainId {
	/// Ethereum mainnet chain ID.
	pub const ETHEREUM: Self = Self(1);

	/// Creates a validated chain ID.
	pub const fn new(value: u64) -> Result<Self, CoreError> {
		if value == 0 {
			return Err(CoreError::InvalidChainId);
		}

		Ok(Self(value))
	}

	/// Returns the numeric chain ID.
	pub const fn get(self) -> u64 {
		self.0
	}
}

impl TryFrom<u64> for ChainId {
	type Error = CoreError;

	fn try_from(value: u64) -> Result<Self, Self::Error> {
		Self::new(value)
	}
}

impl From<ChainId> for u64 {
	fn from(chain_id: ChainId) -> Self {
		chain_id.get()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn creates_valid_chain_id() {
		let chain_id = ChainId::new(1).expect("chain ID should be valid");

		assert_eq!(chain_id.get(), 1);
		assert_eq!(chain_id, ChainId::ETHEREUM);
	}

	#[test]
	fn rejects_zero_chain_id() {
		let error = ChainId::new(0).expect_err("zero chain ID should fail");

		assert_eq!(error, CoreError::InvalidChainId);
	}
}
