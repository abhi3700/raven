use thiserror::Error;

/// Result returned by the RPC event source.
pub type RpcSourceResult<T = ()> = Result<T, RpcSourceError>;

/// Errors produced while connecting to or reading from an Alloy provider.
#[derive(Debug, Error)]
pub enum RpcSourceError {
	#[error("poll interval must be greater than zero")]
	InvalidPollInterval,

	#[error("WebSocket reconciliation interval must be greater than zero")]
	InvalidReconciliationInterval,

	#[error("canonical reorg depth must be greater than zero")]
	InvalidReorgDepth,

	#[error("retry delays must be greater than zero and initial must not exceed maximum")]
	InvalidRetryPolicy,

	#[error("invalid resume window: {0}")]
	InvalidResumeWindow(String),

	#[error("invalid RPC URL: {0}")]
	InvalidRpcUrl(String),

	#[error("failed to connect to RPC endpoint: {0}")]
	Connection(String),

	#[error("failed to retrieve chain ID: {0}")]
	ChainIdRequest(String),

	#[error("RPC returned invalid chain ID {0}")]
	InvalidChainId(u64),

	#[error("failed to retrieve block data: {0}")]
	BlockRequest(String),

	#[error("failed to retrieve block logs: {0}")]
	LogRequest(String),

	#[error("failed to execute batched block and log request: {0}")]
	BatchRequest(String),

	#[error("failed to subscribe to new block headers: {0}")]
	Subscription(String),

	#[error("new block header subscription ended")]
	SubscriptionEnded,

	/// It means Raven could not find a common ancestor inside its retained canonical window. In
	/// practice, the chain reorganized deeper than reorg_depth, so Raven no longer has enough local
	/// history to safely emit exact BlockReverted events. Retrying with the same cursor would
	/// likely hit the same problem again, so the source should stop and surface the error rather
	/// than silently continue.
	#[error(
		"canonical reorganization is deeper than retained window starting at block {earliest_block}"
	)]
	DeepReorg { earliest_block: u64 },

	#[error("canonical chain changed while applying block {block_number}; retrying reconciliation")]
	CanonicalChanged { block_number: u64 },

	#[error("failed to normalize block: {0}")]
	BlockConversion(String),

	#[error("failed to normalize block log: {0}")]
	LogConversion(String),

	#[error("event receiver was dropped")]
	EventReceiverDropped,
}

impl RpcSourceError {
	/// Returns whether reconnecting can reasonably recover from this failure.
	pub const fn is_transient(&self) -> bool {
		matches!(
			self,
			Self::Connection(_) |
				Self::ChainIdRequest(_) |
				Self::BlockRequest(_) |
				Self::LogRequest(_) |
				Self::BatchRequest(_) |
				Self::Subscription(_) |
				Self::SubscriptionEnded |
				Self::CanonicalChanged { .. }
		)
	}
}
