use raven_plugin_sdk::PluginError;
use thiserror::Error;

/// Result returned by Raven runtime operations.
pub type RuntimeResult<T = ()> = Result<T, RuntimeError>;

/// Errors produced by the Raven runtime.
#[derive(Debug, Error)]
pub enum RuntimeError {
	#[error("plugin '{name}' is already registered")]
	DuplicatePlugin { name: String },

	#[error("plugin mailbox capacity must be greater than zero")]
	InvalidPluginMailboxCapacity,

	#[error("plugins cannot be registered after the runtime has started")]
	RegistrationAfterStart,

	#[error("runtime has already started")]
	AlreadyStarted,

	#[error("runtime must be started before processing events")]
	NotStarted,

	#[error("runtime has already been shut down")]
	AlreadyShutdown,

	#[error(
		"event chain ID mismatch: runtime uses chain {expected}, \
         but the event belongs to chain {actual}"
	)]
	ChainIdMismatch { expected: u64, actual: u64 },

	#[error("dispatch ID space is exhausted")]
	DispatchIdExhausted,

	#[error("plugin worker '{plugin}' stopped during {operation}: {reason}")]
	PluginWorkerStopped { plugin: String, operation: &'static str, reason: String },

	#[error("plugin '{plugin}' failed during {operation}: {source}")]
	PluginOperation {
		plugin: String,
		operation: &'static str,

		#[source]
		source: PluginError,
	},
}

impl RuntimeError {
	pub(crate) fn plugin_operation(
		plugin: impl Into<String>,
		operation: &'static str,
		source: PluginError,
	) -> Self {
		Self::PluginOperation { plugin: plugin.into(), operation, source }
	}

	pub(crate) fn plugin_worker_stopped(
		plugin: impl Into<String>,
		operation: &'static str,
		reason: impl Into<String>,
	) -> Self {
		Self::PluginWorkerStopped { plugin: plugin.into(), operation, reason: reason.into() }
	}
}
