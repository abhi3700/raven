//! Runtime error vocabulary.
//!
//! Errors in this crate describe runtime policy, lifecycle misuse, plugin
//! supervision failures, or event validation failures. Normal event-handler
//! failures are usually emitted as `PluginOutcomeStatus::Failed` instead of
//! returning `RuntimeError`, because the runtime itself can continue.
//!
//! ```text
//! RuntimeError
//!   |
//!   +-- configuration: invalid capacities or timeouts
//!   +-- lifecycle use: start/process/shutdown called in the wrong state
//!   +-- plugin identity: duplicate metadata names
//!   +-- event validity: chain ID mismatch
//!   +-- supervision: worker stopped, timed out, or lifecycle failed
//! ```

use raven_plugin_sdk::PluginError;
use thiserror::Error;

/// Result returned by Raven runtime operations.
///
/// `RuntimeResult` is `Result<(), RuntimeError>` by default. APIs that return
/// data use `RuntimeResult<T>`.
pub type RuntimeResult<T = ()> = Result<T, RuntimeError>;

/// Errors produced by the Raven runtime.
///
/// Handler errors during normal event processing are not represented here; they
/// are published as plugin outcomes. Errors here mean a runtime operation itself
/// could not complete as requested.
#[derive(Debug, Error)]
pub enum RuntimeError {
	#[error("plugin '{name}' is already registered")]
	DuplicatePlugin { name: String },

	#[error("plugin mailbox capacity must be greater than zero")]
	InvalidPluginMailboxCapacity,

	#[error("plugin lifecycle timeouts must be greater than zero")]
	InvalidPluginTimeout,

	#[error("plugin lifecycle timeouts cannot be changed after the runtime has started")]
	TimeoutConfigurationAfterStart,

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

	#[error("plugin '{plugin}' timed out during {operation} after {timeout:?}")]
	PluginTimeout { plugin: String, operation: &'static str, timeout: std::time::Duration },

	#[error("multiple plugin lifecycle failures: {failures:?}")]
	MultipleLifecycleFailures { failures: Vec<String> },

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

	pub(crate) fn plugin_timeout(
		plugin: impl Into<String>,
		operation: &'static str,
		timeout: std::time::Duration,
	) -> Self {
		Self::PluginTimeout { plugin: plugin.into(), operation, timeout }
	}

	pub(crate) fn lifecycle_failures(errors: Vec<Self>) -> Self {
		Self::MultipleLifecycleFailures {
			failures: errors.into_iter().map(|error| error.to_string()).collect(),
		}
	}
}
