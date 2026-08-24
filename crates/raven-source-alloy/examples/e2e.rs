//! End-to-end Raven example.
//!
//! Flow:
//!
//! ```text
//! Ethereum RPC
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
//! RAVEN_RPC_URL=https://ethereum-rpc.publicnode.com \
//! cargo run -p raven-source-alloy --example e2e
//! ```
//!
//! Expected output:
//! ```
//! 2026-07-13T13:42:11.967800Z  INFO raven_source_alloy::source: connecting Alloy event source rpc_url=https://ethereum-rpc.publicnode.com
//! 2026-07-13T13:42:12.434912Z  INFO raven_source_alloy::source: connected Alloy event source chain_id=1
//! block=#25524074   txs=25   hash=0x5e14f42e3f0e11c4f0cd26a6dc94632127757c490f456f2c98b622b31cac05c0
//! block=#25524075   txs=250  hash=0xaeb0c67d77f9eb0838ca977320ef4b3db3af7551a7cfdae3f8cb7e85b6a111ad
//! block=#25524076   txs=41   hash=0xd1e281350730f1a1a2a174c7a9aa9a1d7b0f00a0a1a9ec64ac5595b624042652
//! block=#25524077   txs=222  hash=0xafe4b696a2310cf8b22a0f8e960b65b6b7a5b5c152f062c17dc1be161b6381ee
//! ```

use std::{env, time::Duration};

use async_trait::async_trait;
use raven_core::{ChainEvent, ChainId};
use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};
use raven_runtime::Runtime;
use raven_source_alloy::AlloySource;
use tokio::sync::mpsc;
use tracing::{error, info};

const EVENT_CHANNEL_CAPACITY: usize = 256;
const DEFAULT_RPC_URL: &str = "https://ethereum-rpc.publicnode.com";

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

	let rpc_url = env::var("RAVEN_RPC_URL").unwrap_or_else(|_| DEFAULT_RPC_URL.to_owned());

	/*
	 * The current example assumes Ethereum mainnet because Runtime requires
	 * a ChainId before the Alloy source starts.
	 *
	 * The Alloy source independently verifies the RPC chain ID and embeds it
	 * into every produced ChainEvent. Runtime::process() rejects events from
	 * another chain.
	 */
	let mut runtime = Runtime::new(ChainId::ETHEREUM);

	runtime.register_plugin(BlockLoggerPlugin)?;
	runtime.start().await?;

	let (event_sender, mut event_receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

	let source = AlloySource::new(rpc_url).with_poll_interval(Duration::from_secs(4));

	let source_task = tokio::spawn(async move { source.run(event_sender).await });

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
						runtime.process(event).await?;
					}

					None => {
						error!("event channel closed");
						break;
					}
				}
			}
		}
	}

	/*
	 * AlloySource::run() is a long-running stream. Once Ctrl+C is received,
	 * abort the source task and then shut down all registered plugins.
	 */
	source_task.abort();

	match source_task.await {
		Ok(Ok(())) => {},

		Ok(Err(error)) => {
			return Err(error.into());
		},

		Err(error) if error.is_cancelled() => {},

		Err(error) => {
			return Err(error.into());
		},
	}

	runtime.shutdown().await?;

	Ok(())
}

fn init_tracing() {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "raven=info,raven_source_alloy=info".into()),
		)
		.init();
}
