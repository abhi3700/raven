use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use eyre::{Result, WrapErr, bail};
use raven_core::ChainEvent;
use raven_plugin_erc20_transfer::Erc20TransferPlugin;
use raven_plugin_reorg_monitor::ReorgMonitorPlugin;
use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};
use raven_runtime::{DispatchReceipt, PluginOutcome, PluginOutcomeStatus, Runtime};
use raven_source_rpc::{RpcSource, SourceStart, inspect_rpc_endpoint};
use tokio::{
	sync::{broadcast, mpsc},
	task::JoinHandle,
};
use tracing::{Instrument, debug, error, info, warn};

use crate::{
	checkpoint::Checkpoint,
	cli::{RunArgs, StartArg},
	config::InstalledPlugins,
	logging::{PluginColor, PluginColorAllocator, PluginLogIdentity, plugin_span},
};

const EVENT_CHANNEL_CAPACITY: usize = 256;
const RPC_INSPECTION_TIMEOUT: Duration = Duration::from_secs(15);

/// Runs RPC ingestion until it stops or the user presses Ctrl+C.
pub(crate) async fn run(
	args: RunArgs,
	rpc_url: String,
	installed_plugins: InstalledPlugins,
) -> Result<()> {
	let endpoint = tokio::time::timeout(RPC_INSPECTION_TIMEOUT, inspect_rpc_endpoint(&rpc_url))
		.await
		.wrap_err("RPC connectivity check timed out")??;
	let (source_start, mut checkpoint) = resolve_start(&args, endpoint.chain_id)?;

	let mut runtime = Runtime::new(endpoint.chain_id);
	let mut plugin_colors = PluginColorAllocator::default();
	register_cli_plugin(&mut runtime, &mut plugin_colors, BlockLoggerPlugin)?;
	if installed_plugins.reorg_monitor_installed() {
		register_cli_plugin(&mut runtime, &mut plugin_colors, ReorgMonitorPlugin::new())?;
	}
	if let Some(settings) = installed_plugins.erc20_transfer() {
		let plugin = Erc20TransferPlugin::with_output_format(
			settings.plugin_config()?,
			settings.output_format(),
		);
		register_cli_plugin(&mut runtime, &mut plugin_colors, plugin)?;
	}
	let outcomes = runtime.subscribe_outcomes();
	runtime.start().await?;
	let mut session = RuntimeSession { runtime, outcomes };

	let (event_sender, mut event_receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
	let source_task = spawn_rpc_source(&args, rpc_url, source_start, event_sender);

	info!(
		ingestion = "rpc",
		chain_id = endpoint.chain_id.get(),
		transport = %endpoint.transport,
		latest_block = endpoint.latest_block,
		"Raven is running; press Ctrl+C to stop"
	);

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

				if let Err(error) = process_event(
					&mut session,
					&mut checkpoint,
					event,
					args.reorg_depth,
				).await {
					break Err(error);
				}
			}
		}
	};

	if !matches!(&event_loop_result, Ok(EventLoopExit::SourceStopped)) {
		source_task.abort();
	}

	let source_result = await_source(source_task).await;
	let shutdown_result = session.runtime.shutdown().await;

	// Cleanup always runs before the first operational error is returned.
	event_loop_result?;
	source_result?;
	shutdown_result?;
	Ok(())
}

fn resolve_start(
	args: &RunArgs,
	chain_id: raven_core::ChainId,
) -> Result<(SourceStart, Checkpoint)> {
	match args.start {
		StartArg::Latest => Ok((SourceStart::Latest, Checkpoint::empty(chain_id))),
		StartArg::Block(block) => Ok((SourceStart::Block(block), Checkpoint::empty(chain_id))),
		StartArg::Resume => match Checkpoint::load(chain_id)? {
			Some(checkpoint) if !checkpoint.blocks().is_empty() => {
				let keep_from = checkpoint.blocks().len().saturating_sub(args.reorg_depth);
				let window = checkpoint.blocks()[keep_from..].to_vec();
				info!(
					chain_id = chain_id.get(),
					block_number = window.last().map(raven_core::BlockEvent::block_number),
					retained_blocks = window.len(),
					"resuming from durable checkpoint"
				);
				Ok((SourceStart::Resume(window), checkpoint))
			},
			_ => {
				info!(chain_id = chain_id.get(), "no durable checkpoint found; starting at latest");
				Ok((SourceStart::Latest, Checkpoint::empty(chain_id)))
			},
		},
	}
}

fn spawn_rpc_source(
	args: &RunArgs,
	rpc_url: String,
	start: SourceStart,
	event_sender: mpsc::Sender<ChainEvent>,
) -> JoinHandle<Result<()>> {
	let source = RpcSource::new(rpc_url)
		.with_poll_interval(Duration::from_millis(args.poll_interval_ms))
		.with_reconciliation_interval(Duration::from_millis(args.reconciliation_interval_ms))
		.with_reorg_depth(args.reorg_depth)
		.with_block_fetch_mode(args.block_fetch_mode.into())
		.with_start(start);

	tokio::spawn(async move { source.run(event_sender).await.map_err(Into::into) })
}

async fn process_event(
	session: &mut RuntimeSession,
	checkpoint: &mut Checkpoint,
	event: ChainEvent,
	reorg_depth: usize,
) -> Result<()> {
	let checkpoint_event = event.clone();
	let receipt = session.runtime.process(event).await?;
	wait_for_success(&mut session.outcomes, &receipt).await?;
	checkpoint.apply_and_save(&checkpoint_event, reorg_depth)?;
	debug!(
		dispatch_id = receipt.dispatch_id().get(),
		block_number = checkpoint_event.block_number(),
		applied = checkpoint_event.is_applied(),
		"durable event checkpoint committed"
	);
	Ok(())
}

async fn wait_for_success(
	outcomes: &mut broadcast::Receiver<Arc<PluginOutcome>>,
	receipt: &DispatchReceipt,
) -> Result<()> {
	let mut pending: HashSet<&'static str> = receipt
		.deliveries()
		.iter()
		.filter(|delivery| delivery.is_accepted())
		.map(|delivery| delivery.plugin())
		.collect();

	if !receipt.all_accepted() {
		for delivery in receipt.deliveries().iter().filter(|delivery| !delivery.is_accepted()) {
			warn!(
				dispatch_id = receipt.dispatch_id().get(),
				plugin = delivery.plugin(),
				status = ?delivery.status(),
				"plugin rejected event; checkpoint will not advance"
			);
		}
		bail!(
			"dispatch {} was rejected by {} plugin(s); checkpoint was not advanced",
			receipt.dispatch_id().get(),
			receipt.rejected_count()
		);
	}

	while !pending.is_empty() {
		let outcome = outcomes.recv().await.map_err(|error| match error {
			broadcast::error::RecvError::Lagged(skipped) =>
				eyre::eyre!("missed {skipped} plugin outcome(s); checkpoint cannot advance safely"),
			broadcast::error::RecvError::Closed => {
				eyre::eyre!("plugin outcome channel closed before dispatch completed")
			},
		})?;
		log_plugin_outcome(&outcome);
		if outcome.dispatch_id() != receipt.dispatch_id() || !pending.remove(outcome.plugin()) {
			continue;
		}

		match outcome.status() {
			PluginOutcomeStatus::Succeeded => {},
			PluginOutcomeStatus::Failed(error) => bail!(
				"plugin '{}' failed dispatch {}: {error}; checkpoint was not advanced",
				outcome.plugin(),
				receipt.dispatch_id().get()
			),
			PluginOutcomeStatus::Panicked(message) => bail!(
				"plugin '{}' panicked during dispatch {}: {message}; checkpoint was not advanced",
				outcome.plugin(),
				receipt.dispatch_id().get()
			),
			PluginOutcomeStatus::TimedOut(duration) => bail!(
				"plugin '{}' timed out after {duration:?} during dispatch {}; checkpoint was not advanced",
				outcome.plugin(),
				receipt.dispatch_id().get()
			),
			PluginOutcomeStatus::Rejected(reason) => bail!(
				"plugin '{}' rejected dispatch {} ({reason:?}); checkpoint was not advanced",
				outcome.plugin(),
				receipt.dispatch_id().get()
			),
		}
	}
	Ok(())
}

fn log_plugin_outcome(outcome: &PluginOutcome) {
	let dispatch_id = outcome.dispatch_id().get();
	let plugin = outcome.plugin();
	let block_number = outcome.event().block_number();
	let elapsed_micros = outcome.elapsed().as_micros();

	match outcome.status() {
		PluginOutcomeStatus::Succeeded => {
			debug!(dispatch_id, plugin, block_number, elapsed_micros, "plugin completed event");
		},
		PluginOutcomeStatus::Failed(error) => {
			error!(dispatch_id, plugin, block_number, elapsed_micros, error = %error, "plugin failed to handle event; worker remains active");
		},
		PluginOutcomeStatus::Panicked(message) => {
			error!(dispatch_id, plugin, block_number, elapsed_micros, panic = %message, "plugin panicked; worker was quarantined");
		},
		PluginOutcomeStatus::TimedOut(duration) => {
			error!(
				dispatch_id,
				plugin,
				block_number,
				elapsed_micros,
				timeout_ms = duration.as_millis(),
				"plugin timed out; worker was quarantined"
			);
		},
		PluginOutcomeStatus::Rejected(reason) => {
			warn!(dispatch_id, plugin, block_number, ?reason, "plugin did not accept event");
		},
	}
}

async fn await_source(source_task: JoinHandle<Result<()>>) -> Result<()> {
	match source_task.await {
		Ok(result) => result,
		Err(error) if error.is_cancelled() => Ok(()),
		Err(error) => Err(error.into()),
	}
}

struct RuntimeSession {
	runtime: Runtime,
	outcomes: broadcast::Receiver<Arc<PluginOutcome>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventLoopExit {
	Interrupted,
	SourceStopped,
}

/// Adds CLI-owned tracing identity around a plugin without changing its SDK contract.
///
/// ```text
/// registered Plugin -> assigned terminal color -> CliPlugin span -> plugin tracing event
///                                                               -> [ plugin-name ] terminal tag
/// ```
fn register_cli_plugin<P>(
	runtime: &mut Runtime,
	plugin_colors: &mut PluginColorAllocator,
	plugin: P,
) -> Result<()>
where
	P: Plugin + 'static,
{
	runtime.register_plugin(CliPlugin::new(plugin, plugin_colors.assign()))?;
	Ok(())
}

/// CLI adapter that associates one terminal color with every plugin lifecycle call.
struct CliPlugin<P> {
	inner: P,
	metadata: PluginMetadata,
	log_identity: PluginLogIdentity,
}

impl<P> CliPlugin<P>
where
	P: Plugin,
{
	fn new(plugin: P, color: PluginColor) -> Self {
		let metadata = plugin.metadata();
		let log_identity = PluginLogIdentity::new(metadata.name, color);
		Self { inner: plugin, metadata, log_identity }
	}
}

#[async_trait]
impl<P> Plugin for CliPlugin<P>
where
	P: Plugin + 'static,
{
	fn metadata(&self) -> PluginMetadata {
		self.metadata
	}

	async fn start(&mut self, context: &PluginContext) -> PluginResult {
		self.inner.start(context).instrument(plugin_span(&self.log_identity)).await
	}

	async fn handle_event(&mut self, event: &ChainEvent, context: &PluginContext) -> PluginResult {
		self.inner
			.handle_event(event, context)
			.instrument(plugin_span(&self.log_identity))
			.await
	}

	async fn shutdown(&mut self, context: &PluginContext) -> PluginResult {
		self.inner.shutdown(context).instrument(plugin_span(&self.log_identity)).await
	}
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
		info!(block_number = block.block_number(), block_hash = %block.block_hash(), transactions = block.transaction_count(), applied = event.is_applied(), "processed block");
		Ok(())
	}

	async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
		info!("block logger plugin stopped");
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use alloy_primitives::B256;
	use raven_core::{BlockEvent, ChainId};
	use raven_plugin_sdk::PluginError;
	use tokio::sync::Notify;

	struct FailingPlugin;

	#[async_trait]
	impl Plugin for FailingPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("failing", "0.1.0", "Fails immediately")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			Err(PluginError::EventProcessing("expected failure".to_owned()))
		}
	}

	struct WaitingPlugin {
		entered: Arc<Notify>,
		release: Arc<Notify>,
	}

	#[async_trait]
	impl Plugin for WaitingPlugin {
		fn metadata(&self) -> PluginMetadata {
			PluginMetadata::new("waiting", "0.1.0", "Waits for test release")
		}

		async fn handle_event(
			&mut self,
			_event: &ChainEvent,
			_context: &PluginContext,
		) -> PluginResult {
			self.entered.notify_one();
			self.release.notified().await;
			Ok(())
		}
	}

	fn event() -> ChainEvent {
		ChainEvent::BlockApplied(
			BlockEvent::new(
				ChainId::new(8453).unwrap(),
				10,
				B256::with_last_byte(10),
				B256::with_last_byte(9),
				1,
				0,
			)
			.unwrap(),
		)
	}

	#[tokio::test]
	async fn commits_only_after_all_plugins_succeed() {
		let mut runtime = Runtime::new(ChainId::new(8453).unwrap());
		runtime.register_plugin(BlockLoggerPlugin).unwrap();
		let outcomes = runtime.subscribe_outcomes();
		runtime.start().await.unwrap();
		let mut session = RuntimeSession { runtime, outcomes };
		let receipt = session.runtime.process(event()).await.unwrap();
		wait_for_success(&mut session.outcomes, &receipt).await.unwrap();
		session.runtime.shutdown().await.unwrap();
	}

	#[tokio::test]
	async fn returns_first_plugin_error_without_waiting_for_slow_sibling() {
		let entered = Arc::new(Notify::new());
		let release = Arc::new(Notify::new());
		let mut runtime = Runtime::new(ChainId::new(8453).unwrap());
		runtime.register_plugin(FailingPlugin).unwrap();
		runtime
			.register_plugin(WaitingPlugin {
				entered: Arc::clone(&entered),
				release: Arc::clone(&release),
			})
			.unwrap();
		let mut outcomes = runtime.subscribe_outcomes();
		runtime.start().await.unwrap();
		let receipt = runtime.process(event()).await.unwrap();
		entered.notified().await;

		let result = tokio::time::timeout(
			Duration::from_millis(100),
			wait_for_success(&mut outcomes, &receipt),
		)
		.await
		.expect("failing plugin must report without waiting for its sibling");
		assert!(result.is_err());

		release.notify_one();
		runtime.shutdown().await.unwrap();
	}
}
