use crate::CoreError;
use alloy_primitives::{Address, B256, Bytes, LogData};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

/// A mined EVM log normalized for source-independent plugin consumption.
///
/// The enclosing [`crate::BlockEvent`] supplies the block number and block hash. Keeping that
/// canonical context in one place prevents a log from disagreeing with its block while retaining
/// the transaction and log positions plugins need for deterministic identity.
///
/// ```text
/// BlockEvent
///   └─ logs[]
///       ├─ address + topics + data   (event payload)
///       └─ tx hash + tx/log indexes  (stable position inside the block)
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvmLog {
	/// Contract address that emitted the log.
	address: Address,

	/// Indexed topics and ABI-encoded non-indexed data.
	#[serde(flatten)]
	data: LogData,

	/// Hash of the transaction that emitted the log.
	transaction_hash: B256,

	/// Zero-based position of the transaction in the block.
	transaction_index: u64,

	/// Zero-based position of the log across the block.
	log_index: u64,
}

impl EvmLog {
	/// Creates a validated mined EVM log.
	pub fn new(
		address: Address,
		topics: Vec<B256>,
		data: Bytes,
		transaction_hash: B256,
		transaction_index: u64,
		log_index: u64,
	) -> Result<Self, CoreError> {
		let topic_count = topics.len();
		let data =
			LogData::new(topics, data).ok_or(CoreError::TooManyLogTopics { count: topic_count })?;

		Ok(Self { address, data, transaction_hash, transaction_index, log_index })
	}

	/// Returns the contract address that emitted the log.
	pub const fn address(&self) -> Address {
		self.address
	}

	/// Returns the indexed event topics, including the signature topic when present.
	pub fn topics(&self) -> &[B256] {
		self.data.topics()
	}

	/// Returns the ABI-encoded non-indexed event data.
	pub const fn data(&self) -> &Bytes {
		&self.data.data
	}

	/// Returns the transaction hash that emitted the log.
	pub const fn transaction_hash(&self) -> B256 {
		self.transaction_hash
	}

	/// Returns the transaction's zero-based position in the block.
	pub const fn transaction_index(&self) -> u64 {
		self.transaction_index
	}

	/// Returns the log's zero-based position across the block.
	pub const fn log_index(&self) -> u64 {
		self.log_index
	}

	/// Returns the validated primitive log payload used by ABI decoders.
	pub const fn log_data(&self) -> &LogData {
		&self.data
	}
}

#[derive(Deserialize)]
struct EvmLogData {
	address: Address,
	topics: Vec<B256>,
	data: Bytes,
	transaction_hash: B256,
	transaction_index: u64,
	log_index: u64,
}

impl<'de> Deserialize<'de> for EvmLog {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let data = EvmLogData::deserialize(deserializer)?;
		Self::new(
			data.address,
			data.topics,
			data.data,
			data.transaction_hash,
			data.transaction_index,
			data.log_index,
		)
		.map_err(D::Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use alloy_primitives::{Address, B256, Bytes};

	use super::*;

	#[test]
	fn round_trips_a_valid_log() {
		let log = EvmLog::new(
			Address::repeat_byte(0x11),
			vec![B256::repeat_byte(0x22)],
			Bytes::from_static(&[0x33]),
			B256::repeat_byte(0x44),
			2,
			7,
		)
		.unwrap();

		let json = serde_json::to_string(&log).unwrap();
		let restored: EvmLog = serde_json::from_str(&json).unwrap();

		assert_eq!(restored, log);
	}

	#[test]
	fn rejects_more_than_four_topics() {
		let error = EvmLog::new(Address::ZERO, vec![B256::ZERO; 5], Bytes::new(), B256::ZERO, 0, 0)
			.unwrap_err();

		assert_eq!(error, CoreError::TooManyLogTopics { count: 5 });
	}

	#[test]
	fn rejects_invalid_log_during_deserialization() {
		let json = format!(
			r#"{{
				"address": "{address}",
				"topics": ["{hash}", "{hash}", "{hash}", "{hash}", "{hash}"],
				"data": "0x",
				"transaction_hash": "{hash}",
				"transaction_index": 0,
				"log_index": 0
			}}"#,
			hash = B256::ZERO,
			address = Address::ZERO,
		);

		let error = serde_json::from_str::<EvmLog>(&json).unwrap_err();
		assert!(error.to_string().contains("at most four"));
	}
}
