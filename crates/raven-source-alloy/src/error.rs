use thiserror::Error;

/// Result returned by the Alloy event source.
pub type AlloySourceResult<T = ()> = Result<T, AlloySourceError>;

/// Errors produced while connecting to or reading from an Alloy provider.
#[derive(Debug, Error)]
pub enum AlloySourceError {
	#[error("failed to connect to RPC endpoint: {0}")]
	Connection(String),

	#[error("failed to retrieve chain ID: {0}")]
	ChainIdRequest(String),

	#[error("RPC returned invalid chain ID {0}")]
	InvalidChainId(u64),

	#[error("failed to create block watcher: {0}")]
	Watcher(String),

	#[error("failed to retrieve watched block: {0}")]
	BlockStream(String),

	#[error("failed to normalize block: {0}")]
	BlockConversion(String),

	#[error("event receiver was dropped")]
	EventReceiverDropped,
}
