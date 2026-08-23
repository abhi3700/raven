use std::time::Duration;

use async_trait::async_trait;
use eyre::Result;
use raven_core::ChainEvent;
use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};
use raven_runtime::Runtime;
use raven_source_alloy::AlloySource;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::info;

use crate::cli::{EventSource, RunArgs};

const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Runs the configured event source until it stops or the user presses Ctrl+C.
pub(crate) async fn run(args: RunArgs) -> Result<()> {
	let (event_sender, mut event_receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
	let source_task = spawn_source(&args, event_sender);
	let mut runtime = None;

	info!(source = ?args.source, "Raven is running; press Ctrl+C to stop");

	let event_loop_result: Result<EventLoopExit> = loop {
		tokio::select! {
			signal_result = tokio::signal::ctrl_c() => {
				match signal_result {
					Ok(()) => {
						info!("shutdown signal received");
						break Ok(EventLoopExit::Interrupted);
					}
					Err(error) => break Err(error.into()),
				}
			}

			maybe_event = event_receiver.recv() => {
				let Some(event) = maybe_event else {
					break Ok(EventLoopExit::SourceStopped);
				};

				if let Err(error) = process_event(&mut runtime, event).await {
					break Err(error);
				}
			}
		}
	};

	if !matches!(&event_loop_result, Ok(EventLoopExit::SourceStopped)) {
		source_task.abort();
	}

	let source_result = await_source(source_task).await;
	let shutdown_result = shutdown_runtime(runtime.as_mut()).await;

	// Cleanup always runs before the first operational error is returned.
	event_loop_result?;
	source_result?;
	shutdown_result
}

fn spawn_source(args: &RunArgs, event_sender: mpsc::Sender<ChainEvent>) -> JoinHandle<Result<()>> {
	let poll_interval = Duration::from_millis(args.poll_interval_ms);

	match args.source {
		EventSource::Alloy => {
			let source = AlloySource::new(args.rpc_url.clone()).with_poll_interval(poll_interval);

			tokio::spawn(async move { source.run(event_sender).await.map_err(Into::into) })
		},
	}
}

async fn process_event(runtime: &mut Option<Runtime>, event: ChainEvent) -> Result<()> {
	if runtime.is_none() {
		let mut initialized_runtime = Runtime::new(event.chain_id());

		initialized_runtime.register_plugin(BlockLoggerPlugin)?;
		initialized_runtime.start().await?;

		*runtime = Some(initialized_runtime);
	}

	runtime
		.as_mut()
		.expect("runtime is initialized before event processing")
		.process(event)
		.await?;

	Ok(())
}

async fn shutdown_runtime(runtime: Option<&mut Runtime>) -> Result<()> {
	if let Some(runtime) = runtime {
		runtime.shutdown().await?;
	}

	Ok(())
}

async fn await_source(source_task: JoinHandle<Result<()>>) -> Result<()> {
	match source_task.await {
		Ok(result) => result,
		Err(error) if error.is_cancelled() => Ok(()),
		Err(error) => Err(error.into()),
	}
}

#[cfg(test)]
mod tests {
	use raven_core::{BlockEvent, ChainId};

	use super::*;

	#[tokio::test]
	async fn initializes_runtime_from_first_event() {
		let block = BlockEvent::new(
			ChainId::ETHEREUM,
			21_000_000,
			"0x1111111111111111111111111111111111111111111111111111111111111111",
			"0x2222222222222222222222222222222222222222222222222222222222222222",
			1_720_000_000,
			150,
		)
		.expect("block should be valid");

		let mut runtime = None;

		process_event(&mut runtime, ChainEvent::BlockApplied(block))
			.await
			.expect("event should initialize the runtime and be processed");

		let runtime = runtime.as_mut().expect("runtime should be initialized");

		assert!(runtime.is_started());
		assert_eq!(runtime.chain_id(), ChainId::ETHEREUM);
		assert_eq!(runtime.plugin_count(), 1);

		runtime.shutdown().await.expect("runtime should shut down");
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventLoopExit {
	Interrupted,
	SourceStopped,
}

/// Built-in plugin that makes the default CLI useful without external plugins.
struct BlockLoggerPlugin;

#[async_trait]
impl Plugin for BlockLoggerPlugin {
	fn metadata(&self) -> PluginMetadata {
		PluginMetadata::new(
			"block-logger",
			env!("CARGO_PKG_VERSION"),
			"Logs normalized block events received by the Raven CLI",
		)
	}

	async fn start(&mut self, context: &PluginContext) -> PluginResult {
		info!(chain_id = context.chain_id().get(), "block logger plugin started");

		Ok(())
	}

	async fn handle_event(&mut self, event: &ChainEvent, _context: &PluginContext) -> PluginResult {
		let block = event.block();

		info!(
			block_number = block.block_number,
			block_hash = %block.block_hash,
			transactions = block.transaction_count,
			applied = event.is_applied(),
			"processed block"
		);

		Ok(())
	}

	async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
		info!("block logger plugin stopped");

		Ok(())
	}
}
