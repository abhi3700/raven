use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChainEvent {
	BlockApplied(BlockEvent),
	BlockReverted(BlockEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockEvent {
	pub chain_id: u64,
	pub block_number: u64,
	pub block_hash: String,
	pub parent_hash: String,
}
