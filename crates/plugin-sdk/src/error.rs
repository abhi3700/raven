use thiserror::Error;

/// Result type returned by Raven plugins.
pub type PluginResult<T = ()> = Result<T, PluginError>;

/// Errors produced while processing plugin events.
#[derive(Debug, Error)]
pub enum PluginError {
	#[error("plugin configuration is invalid: {0}")]
	InvalidConfiguration(String),

	#[error("plugin failed to process event: {0}")]
	EventProcessing(String),

	#[error("plugin storage operation failed: {0}")]
	Storage(String),

	#[error("{0}")]
	Other(String),
}
