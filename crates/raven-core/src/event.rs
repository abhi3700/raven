use crate::{BlockEvent, ChainId};
use serde::{Deserialize, Serialize};

/// A normalized chain event produced by a Raven event source (local or ext. RPC).
///
/// Alloy and Reth sources convert their native event types into this
/// representation before forwarding events to the Raven runtime.
///
/// Alloy events ──┐
///                ├──► ChainEvent ──► Raven Runtime ──► Plugins
/// Reth events  ──┘
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ChainEvent {
	/// A block was added to the canonical chain.
	BlockApplied(BlockEvent),

	/// A previously applied block was removed from the canonical chain.
	BlockReverted(BlockEvent),
}

impl ChainEvent {
	/// Returns the block metadata carried by the event.
	pub const fn block(&self) -> &BlockEvent {
		match self {
			Self::BlockApplied(block) | Self::BlockReverted(block) => block,
		}
	}

	/// Returns the chain ID associated with the event.
	pub const fn chain_id(&self) -> ChainId {
		self.block().chain_id()
	}

	/// Returns the block number associated with the event.
	pub const fn block_number(&self) -> u64 {
		self.block().block_number()
	}

	/// Returns whether a block was added to the canonical chain.
	pub const fn is_applied(&self) -> bool {
		matches!(self, Self::BlockApplied(_))
	}

	/// Returns whether a block was removed from the canonical chain.
	pub const fn is_reverted(&self) -> bool {
		matches!(self, Self::BlockReverted(_))
	}
}

#[cfg(test)]
mod tests {
	use alloy_primitives::B256;

	use super::*;

	fn test_chain_id() -> ChainId {
		ChainId::new(8_453).expect("test chain ID should be valid")
	}

	fn block_event() -> BlockEvent {
		BlockEvent::new(
			test_chain_id(),
			21_000_000,
			B256::repeat_byte(0x11),
			B256::repeat_byte(0x22),
			1_720_000_000,
			150,
		)
		.expect("block should be valid")
	}

	#[test]
	fn exposes_applied_event_metadata() {
		let event = ChainEvent::BlockApplied(block_event());

		assert_eq!(event.chain_id(), test_chain_id());
		assert_eq!(event.block_number(), 21_000_000);
		assert!(event.is_applied());
		assert!(!event.is_reverted());
	}

	#[test]
	fn exposes_reverted_event_metadata() {
		let event = ChainEvent::BlockReverted(block_event());

		assert_eq!(event.chain_id(), test_chain_id());
		assert_eq!(event.block_number(), 21_000_000);
		assert!(!event.is_applied());
		assert!(event.is_reverted());
	}

	#[test]
	fn round_trips_a_valid_event_through_json() {
		let event = ChainEvent::BlockApplied(block_event());
		let json = serde_json::to_string(&event).expect("event should serialize");
		let decoded: ChainEvent = serde_json::from_str(&json).expect("event should deserialize");

		assert_eq!(decoded, event);
	}
}
