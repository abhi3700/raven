//! End-to-end Raven example.
//!
//! Flow:
//!
//! ```text
//! EVM JSON-RPC
//!     ↓
//! RpcSource
//!     ↓
//! mpsc::channel<ChainEvent>
//!     ↓
//! Runtime::process
//!     ↓
//! BlockLoggerPlugin + optional Erc20TransferPlugin
//! ```
//!
//! Run with:
//!
//! ```sh
//! NODE_RPC_URL=https://your-evm-rpc.example \
//! cargo run -p raven-source-rpc --example e2e
//! ```
//!
//! Enable the transfer monitor with a raw-unit threshold and optionally one token contract:
//!
//! ```sh
//! NODE_RPC_URL=https://your-evm-rpc.example \
//! ERC20_TRANSFER_MIN_AMOUNT=1000000000000000000000 \
//! ERC20_TOKEN_ADDRESS=0x1111111111111111111111111111111111111111 \
//! cargo run -p raven-source-rpc --example e2e
//! ```
//!
//! The endpoint's chain ID is discovered at runtime. Example output:
//! ```
//! INFO raven_source_rpc::source: connected HTTP polling event source chain_id=8453
//! INFO e2e: plugin started plugin="block-logger" chain_id=8453
//! block=#12345678   txs=25   hash=0x...
//! ```

use std::{env, time::Duration};

use alloy_primitives::{Address, U256};
use async_trait::async_trait;
use eyre::{WrapErr, bail};
use raven_core::ChainEvent;
use raven_plugin_erc20_transfer::{Erc20TransferConfig, Erc20TransferPlugin};
use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};
use raven_runtime::Runtime;
use raven_source_rpc::RpcSource;
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
	let mut transfer_config = transfer_config_from_env()?;

	let (event_sender, mut event_receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

	let source = RpcSource::new(rpc_url).with_poll_interval(Duration::from_secs(4));

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
							if let Some(config) = transfer_config.take() {
								discovered_runtime.register_plugin(Erc20TransferPlugin::new(config))?;
							}
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

fn transfer_config_from_env() -> eyre::Result<Option<Erc20TransferConfig>> {
	let minimum_amount = match env::var("ERC20_TRANSFER_MIN_AMOUNT") {
		Ok(value) => value
			.parse::<U256>()
			.wrap_err("ERC20_TRANSFER_MIN_AMOUNT must be a decimal or 0x-prefixed uint256 value")?,
		Err(env::VarError::NotPresent) => {
			if env::var_os("ERC20_TOKEN_ADDRESS").is_some() {
				bail!("ERC20_TOKEN_ADDRESS requires ERC20_TRANSFER_MIN_AMOUNT");
			}
			return Ok(None);
		},
		Err(error) => return Err(error.into()),
	};

	let token_addresses = match env::var("ERC20_TOKEN_ADDRESS") {
		Ok(value) => vec![value.parse::<Address>().wrap_err("invalid ERC20_TOKEN_ADDRESS")?],
		Err(env::VarError::NotPresent) => Vec::new(),
		Err(error) => return Err(error.into()),
	};

	Ok(Some(Erc20TransferConfig::new(minimum_amount, token_addresses)?))
}

fn init_tracing() {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "raven=info,raven_source_rpc=info".into()),
		)
		.init();
}
