//! End-to-end Raven example.
//!
//! Flow:
//!
//! ```text
//! EVM JSON-RPC
//!     ↓
//! AlloySource
//!     ↓
//! mpsc::channel<ChainEvent>
//!     ↓
//! Runtime::process
//!     ↓
//! BlockLoggerPlugin
//! ```
//!
//! Run with:
//!
//! ```sh
//! NODE_RPC_URL=https://your-evm-rpc.example \
//! cargo run -p raven-source-alloy --example e2e
//! ```
//!
//! The endpoint's chain ID is discovered at runtime. Example output:
//! ```
//! INFO raven_source_alloy::source: connected HTTP polling event source chain_id=8453
//! INFO e2e: plugin started plugin="block-logger" chain_id=8453
//! block=#12345678   txs=25   hash=0x...
//! ```

use std::{env, time::Duration};

use async_trait::async_trait;
use eyre::WrapErr;
use raven_core::ChainEvent;
use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};
use raven_runtime::Runtime;
use raven_source_alloy::AlloySource;
use tokio::sync::mpsc;
use tracing::{error, info};

const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Minimal plugin that prints every normalized block event.
///
/// Raven's runtime automatically invokes:
///
/// 1. `start()` during initialization.
/// 2. `handle_event()` for every incoming event.
/// 3. `shutdown()` before exiting.
struct BlockLoggerPlugin;

#[async_trait]
impl Plugin for BlockLoggerPlugin {
	fn metadata(&self) -> PluginMetadata {
		PluginMetadata::new(
			"block-logger",
			env!("CARGO_PKG_VERSION"),
			"Prints normalized block events received from Raven",
		)
	}

	async fn start(&mut self, context: &PluginContext) -> PluginResult {
		info!(plugin = self.metadata().name, chain_id = context.chain_id().get(), "plugin started");

		Ok(())
	}

	async fn handle_event(&mut self, event: &ChainEvent, _context: &PluginContext) -> PluginResult {
		let block = event.block();

		println!(
			"block=#{:<10} txs={:<4} hash={}",
			block.block_number(),
			block.transaction_count(),
			block.block_hash(),
		);

		Ok(())
	}

	async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
		info!(plugin = self.metadata().name, "plugin shut down");

		Ok(())
	}
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
	init_tracing();

	dotenvy::dotenv().ok();
	let rpc_url = env::var("NODE_RPC_URL").wrap_err(
		"NODE_RPC_URL is required; provide any EVM-compatible HTTP(S) or WS(S) endpoint",
	)?;

	let (event_sender, mut event_receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

	let source = AlloySource::new(rpc_url).with_poll_interval(Duration::from_secs(4));

	let source_task = tokio::spawn(async move { source.run(event_sender).await });
	let mut runtime = None;

	info!("Raven is running. Press Ctrl+C to stop.");

	loop {
		tokio::select! {
			signal_result = tokio::signal::ctrl_c() => {
				signal_result?;
				info!("shutdown signal received");
				break;
			}

			maybe_event = event_receiver.recv() => {
				match maybe_event {
					Some(event) => {
						if runtime.is_none() {
							let chain_id = event.chain_id();
							let mut discovered_runtime = Runtime::new(chain_id);
							discovered_runtime.register_plugin(BlockLoggerPlugin)?;
							discovered_runtime.start().await?;
							runtime = Some(discovered_runtime);
						}

						runtime
							.as_mut()
							.expect("runtime is initialized from the first event")
							.process(event)
							.await?;
					}

					None => {
						error!("event channel closed");
						break;
					}
				}
			}
		}
	}

	// Stop source work, then preserve its result while cleanup still runs.
	source_task.abort();

	let source_result = match source_task.await {
		Ok(result) => result.map_err(eyre::Report::from),
		Err(error) if error.is_cancelled() => Ok(()),
		Err(error) => Err(error.into()),
	};

	let shutdown_result = match runtime {
		Some(mut runtime) => runtime.shutdown().await.map_err(eyre::Report::from),
		None => Ok(()),
	};

	source_result?;
	shutdown_result
}

fn init_tracing() {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "raven=info,raven_source_alloy=info".into()),
		)
		.init();
}
