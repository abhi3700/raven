use crate::CoreError;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

/// Source-independent EIP-155 chain identifier.
///
/// Raven accepts every non-zero value representable by `u64`; named constants
/// are conveniences, not a list of supported chains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
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

impl<'de> Deserialize<'de> for ChainId {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let value = u64::deserialize(deserializer)?;

		Self::new(value).map_err(D::Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn creates_valid_chain_id() {
		let chain_id = ChainId::new(8_453).expect("chain ID should be valid");

		assert_eq!(chain_id.get(), 8_453);
	}

	#[test]
	fn accepts_arbitrary_non_zero_eip155_chain_ids() {
		for value in [1, 10, 56, 137, 8_453, 42_161, u64::MAX] {
			assert_eq!(ChainId::new(value).unwrap().get(), value);
		}
	}

	#[test]
	fn rejects_zero_chain_id() {
		let error = ChainId::new(0).expect_err("zero chain ID should fail");

		assert_eq!(error, CoreError::InvalidChainId);
	}

	#[test]
	fn rejects_zero_chain_id_during_deserialization() {
		let error = serde_json::from_str::<ChainId>("0")
			.expect_err("deserialization must preserve the chain ID invariant");

		assert!(error.to_string().contains("chain ID must be greater than zero"));
	}
}
