//! # Alloy-backed RPC Event Source
//!
//! This module implements Raven's transport-aware JSON-RPC ingestion with
//! Alloy. HTTP(S) endpoints are polled, while WS(S) endpoints use
//! `eth_subscribe("newHeads")` for low-latency notifications and periodically
//! reconcile with `eth_blockNumber` to recover any notification gaps.
//!
//! Both transports feed the same ordered catch-up path:
//!
//! ```text
//! HTTP(S) ── periodic eth_blockNumber ───────────────┐
//!                                                    ├─► fetch every missing height ─► ChainEvent
//! WS(S)   ── newHeads + periodic reconciliation ────┘
//! ```
//!
//! A WebSocket notification is a signal that work is available, not the event
//! payload Raven publishes. Raven fetches full blocks by number from its local
//! cursor through the observed height. Consequently, delayed or coalesced head
//! notifications do not skip intermediate blocks, and the reconciliation poll
//! closes gaps left by a dropped notification or reconnect.
//!
//! The source currently emits only [`ChainEvent::BlockApplied`] and does not
//! yet detect canonical-chain reorganizations. Reorg support requires retaining
//! recent block ancestry and emitting [`ChainEvent::BlockReverted`] before the
//! replacement applied events.

use crate::{AlloySourceError, AlloySourceResult, converter::convert_block};
use alloy::{
	eips::BlockNumberOrTag,
	providers::{Provider, ProviderBuilder, WsConnect},
	rpc::client::BuiltInConnectionString,
};
use futures_util::StreamExt;
use raven_core::{ChainEvent, ChainId};
use std::time::Duration;
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tracing::{debug, info};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(4);
const DEFAULT_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(30);
const SUBSCRIPTION_CHANNEL_CAPACITY: usize = 256;

/// Streams normalized events from an EVM-compatible JSON-RPC endpoint.
pub struct AlloySource {
	rpc_url: String,
	poll_interval: Duration,
	reconciliation_interval: Duration,
}

impl AlloySource {
	/// Creates an Alloy-backed RPC source with production-oriented defaults.
	pub fn new(rpc_url: impl Into<String>) -> Self {
		Self {
			rpc_url: rpc_url.into(),
			poll_interval: DEFAULT_POLL_INTERVAL,
			reconciliation_interval: DEFAULT_RECONCILIATION_INTERVAL,
		}
	}

	/// Overrides HTTP(S)'s interval for polling the latest block number.
	///
	/// This value is not used as the primary trigger for WS(S) endpoints. A
	/// zero duration is rejected by [`Self::run`].
	#[must_use]
	pub const fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
		self.poll_interval = poll_interval;
		self
	}

	/// Overrides WS(S)'s safety interval for reconciling the latest block.
	///
	/// WebSocket heads still arrive through `eth_subscribe`; this interval only
	/// repairs gaps caused by a dropped notification or reconnect. A zero
	/// duration is rejected by [`Self::run`].
	#[must_use]
	pub const fn with_reconciliation_interval(mut self, reconciliation_interval: Duration) -> Self {
		self.reconciliation_interval = reconciliation_interval;
		self
	}

	/// Connects to the endpoint and streams normalized chain events.
	///
	/// The URL scheme selects the ingestion strategy:
	///
	/// - `http://` and `https://`: periodic `eth_blockNumber` polling;
	/// - `ws://` and `wss://`: `eth_subscribe("newHeads")` plus reconciliation.
	///
	/// This method continues until an RPC/subscription request fails, event
	/// conversion fails, or the receiver side of the event channel is dropped.
	pub async fn run(self, sender: mpsc::Sender<ChainEvent>) -> AlloySourceResult {
		if self.poll_interval.is_zero() {
			return Err(AlloySourceError::InvalidPollInterval);
		}
		if self.reconciliation_interval.is_zero() {
			return Err(AlloySourceError::InvalidReconciliationInterval);
		}

		let transport = RpcTransport::from_url(&self.rpc_url)?;

		info!(
			rpc_url = %self.rpc_url,
			transport = %transport,
			"connecting RPC event source"
		);

		match transport {
			RpcTransport::Http => self.run_http(sender).await,
			RpcTransport::WebSocket => self.run_websocket(sender).await,
		}
	}

	async fn run_http(self, sender: mpsc::Sender<ChainEvent>) -> AlloySourceResult {
		let provider = ProviderBuilder::new()
			.connect(&self.rpc_url)
			.await
			.map_err(|error| AlloySourceError::Connection(error.to_string()))?;
		let chain_id = validated_chain_id(&provider).await?;
		let mut next_block_number = latest_block_number(&provider).await?;
		let mut interval = tokio::time::interval(self.poll_interval);

		interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
		info!(
			chain_id = chain_id.get(),
			poll_interval_ms = self.poll_interval.as_millis(),
			"connected HTTP polling event source"
		);

		loop {
			interval.tick().await;
			let latest = latest_block_number(&provider).await?;
			emit_through(&provider, chain_id, &sender, &mut next_block_number, latest).await?;
		}
	}

	async fn run_websocket(self, sender: mpsc::Sender<ChainEvent>) -> AlloySourceResult {
		// `connect_ws` is intentional: the generic request transport can execute
		// WS requests but does not expose Alloy's pub/sub frontend.
		let provider = ProviderBuilder::new()
			.connect_ws(WsConnect::new(&self.rpc_url))
			.await
			.map_err(|error| AlloySourceError::Connection(error.to_string()))?;
		let chain_id = validated_chain_id(&provider).await?;

		// Subscribe before reading the initial height so a head mined during
		// startup is buffered. The cursor makes any duplicate notification safe.
		let subscription = provider
			.subscribe_blocks()
			.channel_size(SUBSCRIPTION_CHANNEL_CAPACITY)
			.await
			.map_err(|error| AlloySourceError::Subscription(error.to_string()))?;
		let mut heads = subscription.into_stream();
		let mut next_block_number = latest_block_number(&provider).await?;

		info!(
			chain_id = chain_id.get(),
			reconciliation_interval_ms = self.reconciliation_interval.as_millis(),
			"connected WebSocket subscription event source"
		);

		// Emit the current head immediately, matching HTTP startup behavior.
		let initial_head = next_block_number;
		emit_through(&provider, chain_id, &sender, &mut next_block_number, initial_head).await?;

		let reconciliation_start = tokio::time::Instant::now() + self.reconciliation_interval;
		let mut reconciliation =
			tokio::time::interval_at(reconciliation_start, self.reconciliation_interval);
		reconciliation.set_missed_tick_behavior(MissedTickBehavior::Skip);

		loop {
			tokio::select! {
				maybe_header = heads.next() => {
					let header = maybe_header.ok_or(AlloySourceError::SubscriptionEnded)?;
					emit_through(
						&provider,
						chain_id,
						&sender,
						&mut next_block_number,
						header.number,
					).await?;
				}
				_ = reconciliation.tick() => {
					let latest = latest_block_number(&provider).await?;
					emit_through(
						&provider,
						chain_id,
						&sender,
						&mut next_block_number,
						latest,
					).await?;
				}
			}
		}
	}
}

async fn validated_chain_id(provider: &impl Provider) -> AlloySourceResult<ChainId> {
	let raw_chain_id = provider
		.get_chain_id()
		.await
		.map_err(|error| AlloySourceError::ChainIdRequest(error.to_string()))?;

	ChainId::new(raw_chain_id).map_err(|_| AlloySourceError::InvalidChainId(raw_chain_id))
}

async fn latest_block_number(provider: &impl Provider) -> AlloySourceResult<u64> {
	provider
		.get_block_number()
		.await
		.map_err(|error| AlloySourceError::BlockRequest(error.to_string()))
}

async fn emit_through(
	provider: &impl Provider,
	chain_id: ChainId,
	sender: &mpsc::Sender<ChainEvent>,
	next_block_number: &mut u64,
	latest_block_number: u64,
) -> AlloySourceResult {
	while *next_block_number <= latest_block_number {
		let block_number = *next_block_number;
		let block = provider
			.get_block_by_number(BlockNumberOrTag::Number(block_number))
			.await
			.map_err(|error| AlloySourceError::BlockRequest(error.to_string()))?
			.ok_or_else(|| {
				AlloySourceError::BlockRequest(format!(
					"block {block_number} was not returned by RPC"
				))
			})?;
		let event = convert_block(chain_id, &block)?;

		debug!(
			chain_id = event.chain_id().get(),
			block_number = event.block_number(),
			block_hash = %event.block().block_hash(),
			transactions = event.block().transaction_count(),
			"received block event"
		);

		sender.send(event).await.map_err(|_| AlloySourceError::EventReceiverDropped)?;
		*next_block_number = block_number.saturating_add(1);
	}

	Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcTransport {
	Http,
	WebSocket,
}

impl RpcTransport {
	fn from_url(rpc_url: &str) -> AlloySourceResult<Self> {
		match rpc_url
			.parse::<BuiltInConnectionString>()
			.map_err(|error| AlloySourceError::Connection(error.to_string()))?
		{
			BuiltInConnectionString::Http(_) => Ok(Self::Http),
			BuiltInConnectionString::Ws(_, _) => Ok(Self::WebSocket),
			_ => Err(AlloySourceError::Connection(
				"unsupported RPC transport; expected HTTP(S) or WS(S)".to_owned(),
			)),
		}
	}
}

impl std::fmt::Display for RpcTransport {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Http => formatter.write_str("http"),
			Self::WebSocket => formatter.write_str("websocket"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn rejects_zero_poll_interval_before_connecting() {
		let (sender, _receiver) = mpsc::channel(1);
		let source = AlloySource::new("not-a-valid-rpc-url").with_poll_interval(Duration::ZERO);

		let error = source.run(sender).await.expect_err("a zero polling interval must be rejected");

		assert!(matches!(error, AlloySourceError::InvalidPollInterval));
	}

	#[tokio::test]
	async fn rejects_zero_reconciliation_interval_before_connecting() {
		let (sender, _receiver) = mpsc::channel(1);
		let source =
			AlloySource::new("not-a-valid-rpc-url").with_reconciliation_interval(Duration::ZERO);

		let error = source
			.run(sender)
			.await
			.expect_err("a zero reconciliation interval must be rejected");

		assert!(matches!(error, AlloySourceError::InvalidReconciliationInterval));
	}

	#[test]
	fn selects_transport_from_connection_string() {
		for rpc_url in ["http://localhost:8545", "https://ethereum.example"] {
			assert_eq!(RpcTransport::from_url(rpc_url).unwrap(), RpcTransport::Http);
		}

		for rpc_url in ["ws://localhost:8546", "wss://ethereum.example"] {
			assert_eq!(RpcTransport::from_url(rpc_url).unwrap(), RpcTransport::WebSocket);
		}
	}
}
