//! Transport-aware Alloy JSON-RPC ingestion.
//!
//! HTTP(S) polls `eth_blockNumber`; WS(S) uses `eth_subscribe("newHeads")`
//! with periodic reconciliation. Both paths use the same bounded canonical
//! window, so a shallow fork emits removed blocks before replacements:
//!
//! ```text
//! old: A -> B -> C
//! new: A -> X -> Y
//!          |
//!          +-> Reverted(C), Reverted(B), Applied(X), Applied(Y)
//! ```
//!
//! This module answers: "Which canonical blocks should Raven process next?"
//! It does not define Raven's event schema and it does not own plugin dispatch.
//! Instead, it supervises an Alloy provider, fetches full canonical blocks by
//! number, and hands each source-native block to `converter.rs` before sending
//! normalized events over the runtime channel.
//!
//! The central state machine is `CanonicalCursor`: a bounded rear-view mirror of
//! recently emitted canonical blocks plus the next block number Raven has not
//! emitted. Every transport feeds `reconcile_through`, which first verifies that
//! the remembered window still belongs to the RPC node's canonical chain, then
//! emits any missing applied blocks through the requested head.
//!
//! ```text
//! HTTP tick / WS head / WS safety timer
//!                  |
//!                  v
//!          latest canonical height
//!                  |
//!                  v
//!           reconcile_through
//!                  |
//!        +---------+---------+
//!        |                   |
//! compare cursor        fetch missing
//! with RPC chain        full blocks
//!        |                   |
//!        v                   v
//! Reverted events      Applied events
//!        |                   |
//!        +---------+---------+
//!                  |
//!                  v
//!        mpsc::Sender<ChainEvent>
//! ```

use crate::{AlloySourceError, AlloySourceResult, converter::convert_block};
use alloy::{
	consensus::BlockHeader,
	eips::BlockNumberOrTag,
	network::BlockResponse,
	providers::{Provider, ProviderBuilder, WsConnect},
	rpc::{
		client::{BatchRequest, BuiltInConnectionString},
		types::{Block, Filter, Log},
	},
};
use futures_util::StreamExt;
use raven_core::{BlockEvent, ChainEvent, ChainId};
use std::{
	collections::VecDeque,
	time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tracing::{debug, info, warn};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(4);
const DEFAULT_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_REORG_DEPTH: usize = 64;
const SUBSCRIPTION_CHANNEL_CAPACITY: usize = 256;

/// Position used the first time an event source connects.
///
/// `Latest` starts from the RPC head observed at connection time, `Block`
/// backfills from an explicit inclusive height, and `Resume` continues after a
/// durable checkpoint while preserving enough ancestry to detect shallow
/// reorganizations.
///
/// ```text
/// Latest
///   RPC: ... #100 #101 #102
///                         ^
///                         start here
///
/// Block(100)
///   emit: #100 -> #101 -> #102 -> ...
///
/// Resume([#100, #101, #102])
///   window:     [#100, #101, #102]
///   next_block:                    #103
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SourceStart {
	/// Emit the current canonical head, then follow new heads.
	#[default]
	Latest,
	/// Emit this block (inclusive), then catch up to the current head.
	Block(u64),
	/// Continue after the last durably processed block while retaining ancestry.
	Resume(Vec<BlockEvent>),
}

/// Capped exponential retry configuration.
///
/// `AlloySource::run` applies this policy only to transient source failures,
/// such as dropped subscriptions, unavailable blocks, connection failures, or a
/// canonical-chain change observed while catching up. Permanent configuration
/// and validation errors return immediately.
///
/// ```text
/// transient error
///      |
///      v
/// delay(attempt) -> sleep -> reconnect
///
/// permanent error
///      |
///      v
/// return Err
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
	/// Delay before the first reconnect.
	pub initial_delay: Duration,
	/// Upper bound for any reconnect delay.
	pub max_delay: Duration,
	/// Symmetric jitter ratio in the range `0.0..=1.0`.
	pub jitter_ratio: f64,
}

impl Default for RetryPolicy {
	fn default() -> Self {
		Self {
			initial_delay: Duration::from_millis(500),
			max_delay: Duration::from_secs(30),
			jitter_ratio: 0.2,
		}
	}
}

impl RetryPolicy {
	fn validate(self) -> AlloySourceResult<Self> {
		if self.initial_delay.is_zero() ||
			self.max_delay.is_zero() ||
			self.initial_delay > self.max_delay ||
			!self.jitter_ratio.is_finite() ||
			!(0.0..=1.0).contains(&self.jitter_ratio)
		{
			return Err(AlloySourceError::InvalidRetryPolicy);
		}
		Ok(self)
	}

	/// Computes the capped exponential backoff delay for one retry attempt.
	///
	/// `run()` passes attempts as `1, 2, 3, ...`; an accidental attempt `0`
	/// is treated like attempt `1`. The exponent is capped before
	/// multiplication so a very large attempt cannot overflow the duration
	/// calculation.
	///
	/// ```text
	/// exponent = min(max(attempt, 1) - 1, 31)
	/// base     = min(initial_delay * 2^exponent, max_delay)
	/// factor   = 1 + random[-1, 1] * jitter_ratio
	/// delay    = min(base * factor, max_delay)
	/// ```
	///
	/// For example, with `initial_delay = 500ms`, `max_delay = 30s`, and
	/// `jitter_ratio = 0.2`, a `4s` base can sleep anywhere from `3.2s` to
	/// `4.8s`. The upper cap still applies after jitter.
	fn delay(self, attempt: u32) -> Duration {
		let multiplier = 1_u32.checked_shl(attempt.saturating_sub(1).min(31)).unwrap_or(u32::MAX);
		let base = self.initial_delay.saturating_mul(multiplier).min(self.max_delay);
		if self.jitter_ratio == 0.0 {
			return base;
		}

		// Mix wall-clock entropy and process identity so independently restarted
		// Raven processes do not reconnect in lockstep.
		let entropy =
			SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
		let mut value = entropy ^
			u64::from(std::process::id()) ^
			u64::from(attempt).wrapping_add(0x9e37_79b9_7f4a_7c15);
		value ^= value << 13;
		value ^= value >> 7;
		value ^= value << 17;
		let unit = (value as f64) / (u64::MAX as f64);
		let factor = 1.0 + ((unit * 2.0 - 1.0) * self.jitter_ratio);
		Duration::from_secs_f64((base.as_secs_f64() * factor).min(self.max_delay.as_secs_f64()))
	}
}

/// Transport selected from an RPC URL.
///
/// Raven does not expose separate source modes for HTTP and WebSocket. The URL
/// scheme selects the transport, while both transports share the same
/// reconciliation and reorg logic after connection.
///
/// ```text
/// http:// or https:// -> run_http      -> reconcile_through
/// ws://   or wss://   -> run_websocket -> reconcile_through
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcTransport {
	/// HTTP(S) polling.
	Http,
	/// WS(S) subscription with reconciliation.
	WebSocket,
}

impl RpcTransport {
	fn from_url(rpc_url: &str) -> AlloySourceResult<Self> {
		match rpc_url
			.parse::<BuiltInConnectionString>()
			.map_err(|error| AlloySourceError::InvalidRpcUrl(error.to_string()))?
		{
			BuiltInConnectionString::Http(_) => Ok(Self::Http),
			BuiltInConnectionString::Ws(_, _) => Ok(Self::WebSocket),
			_ => Err(AlloySourceError::InvalidRpcUrl(
				"unsupported transport; expected HTTP(S) or WS(S)".to_owned(),
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

/// RPC strategy used to fetch one canonical block and all of its logs.
///
/// `Batch` sends the block and block-range log calls in one JSON-RPC packet,
/// then verifies their hashes and aggregate logs bloom before conversion.
/// `Sequential` preserves the original two-step approach: fetch the block,
/// then use its hash to pin the log request.
///
/// ```text
/// Batch (default)                   Sequential
/// ------------------------------    ------------------------------
/// [getBlockByNumber(N),             getBlockByNumber(N)
///  getLogs(N..=N)]                         |
///          |                               v
///          v                        getLogs(blockHash)
/// validate hash + logs bloom
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlockFetchMode {
	/// One JSON-RPC batch packet with consistency validation.
	#[default]
	Batch,
	/// Two dependent JSON-RPC requests with a hash-pinned log filter.
	Sequential,
}

impl std::fmt::Display for BlockFetchMode {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Batch => formatter.write_str("batch"),
			Self::Sequential => formatter.write_str("sequential"),
		}
	}
}

/// Read-only RPC connectivity information used by `raven doctor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcEndpointInfo {
	/// Connected EIP-155 chain ID.
	pub chain_id: ChainId,
	/// Latest block reported during the check.
	pub latest_block: u64,
	/// Selected request/subscription transport.
	pub transport: RpcTransport,
}

/// Checks the endpoint without starting Raven's event loop.
///
/// ```text
/// raven doctor
///      |
///      v
/// inspect_rpc_endpoint
///      |
///      +-- validate URL transport
///      +-- connect with Alloy
///      +-- read eth_chainId
///      +-- read eth_blockNumber
///      |
///      v
/// RpcEndpointInfo
/// ```
pub async fn inspect_rpc_endpoint(rpc_url: &str) -> AlloySourceResult<RpcEndpointInfo> {
	let transport = RpcTransport::from_url(rpc_url)?;
	match transport {
		RpcTransport::Http => {
			let provider = ProviderBuilder::new()
				.connect(rpc_url)
				.await
				.map_err(|error| AlloySourceError::Connection(error.to_string()))?;
			inspect_provider(&provider, transport).await
		},
		RpcTransport::WebSocket => {
			let provider = ProviderBuilder::new()
				.connect_ws(WsConnect::new(rpc_url))
				.await
				.map_err(|error| AlloySourceError::Connection(error.to_string()))?;
			inspect_provider(&provider, transport).await
		},
	}
}

async fn inspect_provider(
	provider: &impl Provider,
	transport: RpcTransport,
) -> AlloySourceResult<RpcEndpointInfo> {
	Ok(RpcEndpointInfo {
		chain_id: validated_chain_id(provider).await?,
		latest_block: latest_block_number(provider).await?,
		transport,
	})
}

/// Streams normalized events from an EVM-compatible JSON-RPC endpoint.
///
/// `AlloySource` owns source configuration, not plugin execution. Its output is
/// a stream of ordered `ChainEvent` values sent through the provided channel.
/// A caller such as the CLI decides when those events are considered durably
/// acknowledged.
///
/// ```text
/// AlloySource config
///   rpc_url
///   start
///   reorg_depth
///   intervals
///   retry_policy
///   block_fetch_mode
///        |
///        v
/// run(sender)
///        |
///        v
/// ordered ChainEvent stream
/// ```
pub struct AlloySource {
	rpc_url: String,
	poll_interval: Duration,
	reconciliation_interval: Duration,
	reorg_depth: usize,
	start: SourceStart,
	retry_policy: RetryPolicy,
	block_fetch_mode: BlockFetchMode,
}

impl AlloySource {
	/// Creates an Alloy-backed RPC source with production-oriented defaults.
	///
	/// By default Raven starts at the latest observed head, retains 64 recent
	/// canonical blocks for shallow-reorg detection, polls HTTP endpoints every
	/// 4 seconds, reconciles WebSocket endpoints every 30 seconds, and retries
	/// transient source failures with capped exponential backoff. Block and log
	/// data are fetched in one validated JSON-RPC batch by default.
	pub fn new(rpc_url: impl Into<String>) -> Self {
		Self {
			rpc_url: rpc_url.into(),
			poll_interval: DEFAULT_POLL_INTERVAL,
			reconciliation_interval: DEFAULT_RECONCILIATION_INTERVAL,
			reorg_depth: DEFAULT_REORG_DEPTH,
			start: SourceStart::Latest,
			retry_policy: RetryPolicy::default(),
			block_fetch_mode: BlockFetchMode::default(),
		}
	}

	/// Overrides HTTP(S)'s latest-block polling interval.
	#[must_use]
	pub const fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
		self.poll_interval = poll_interval;
		self
	}

	/// Overrides WS(S)'s safety reconciliation interval.
	#[must_use]
	pub const fn with_reconciliation_interval(mut self, interval: Duration) -> Self {
		self.reconciliation_interval = interval;
		self
	}

	/// Sets the inclusive initial position or durable resume window.
	#[must_use]
	pub fn with_start(mut self, start: SourceStart) -> Self {
		self.start = start;
		self
	}

	/// Sets the maximum recent ancestry retained for shallow-reorg detection.
	#[must_use]
	pub const fn with_reorg_depth(mut self, reorg_depth: usize) -> Self {
		self.reorg_depth = reorg_depth;
		self
	}

	/// Overrides capped exponential retry behavior.
	#[must_use]
	pub const fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
		self.retry_policy = retry_policy;
		self
	}

	/// Selects batched or sequential block/log retrieval.
	///
	/// Batch mode is the default. Sequential mode is useful for RPC endpoints
	/// that reject JSON-RPC batch packets or when hash-pinned retrieval is
	/// preferred operationally.
	#[must_use]
	pub const fn with_block_fetch_mode(mut self, block_fetch_mode: BlockFetchMode) -> Self {
		self.block_fetch_mode = block_fetch_mode;
		self
	}

	/// Connects, retries transient RPC failures, and streams canonical events.
	///
	/// This is the outer supervisor. It derives the transport from the RPC URL,
	/// validates the remaining configuration, creates one `CanonicalCursor`, and
	/// then keeps that cursor alive across reconnect attempts:
	///
	/// ```text
	/// run()
	///   |
	///   +-- choose HTTP or WebSocket
	///   +-- create CanonicalCursor from SourceStart
	///   +-- run selected transport loop
	///   +-- retry transient failures without forgetting cursor state
	/// ```
	///
	/// Keeping the cursor outside the reconnect loop is important: after a
	/// dropped RPC connection, Raven resumes from its last emitted canonical
	/// position instead of rediscovering state from scratch.
	pub async fn run(self, sender: mpsc::Sender<ChainEvent>) -> AlloySourceResult {
		let transport = RpcTransport::from_url(&self.rpc_url)?;
		self.validate()?;
		let retry_policy = self.retry_policy.validate()?;
		let mut cursor = CanonicalCursor::new(self.start.clone(), self.reorg_depth)?;
		let mut attempt = 0_u32;

		loop {
			info!(rpc_url = %self.rpc_url, %transport, "connecting RPC event source");
			let result = match transport {
				RpcTransport::Http => self.run_http(&sender, &mut cursor).await,
				RpcTransport::WebSocket => self.run_websocket(&sender, &mut cursor).await,
			};

			match result {
				Ok(()) => return Ok(()),
				Err(error) if error.is_transient() => {
					attempt = attempt.saturating_add(1);
					let delay = retry_policy.delay(attempt);
					warn!(attempt, delay_ms = delay.as_millis(), error = %error, "RPC source retry scheduled");
					tokio::time::sleep(delay).await;
					info!(attempt, "retrying RPC event source");
				},
				Err(error) => return Err(error),
			}
		}
	}

	fn validate(&self) -> AlloySourceResult {
		if self.poll_interval.is_zero() {
			return Err(AlloySourceError::InvalidPollInterval);
		}
		if self.reconciliation_interval.is_zero() {
			return Err(AlloySourceError::InvalidReconciliationInterval);
		}
		if self.reorg_depth == 0 {
			return Err(AlloySourceError::InvalidReorgDepth);
		}
		Ok(())
	}

	/// Runs the HTTP ingestion loop by polling the latest block number.
	///
	/// HTTP endpoints do not push head updates, so the polling tick only answers
	/// "how far does the RPC node say the canonical chain goes now?" The actual
	/// event production still goes through `reconcile_through`, which fetches
	/// every missing height in order and verifies retained ancestry before
	/// emitting anything.
	///
	/// ```text
	/// eth_blockNumber -> latest #N
	///          |
	///          v
	/// reconcile_through(N)
	///          |
	///          +-- emit missing Applied blocks
	///          +-- emit Reverted blocks first if a shallow fork is detected
	/// ```
	///
	/// The initial reconciliation emits the selected startup position before the
	/// periodic polling loop begins.
	async fn run_http(
		&self,
		sender: &mpsc::Sender<ChainEvent>,
		cursor: &mut CanonicalCursor,
	) -> AlloySourceResult {
		let provider = ProviderBuilder::new()
			.connect(&self.rpc_url)
			.await
			.map_err(|error| AlloySourceError::Connection(error.to_string()))?;
		let chain_id = validated_chain_id(&provider).await?;
		cursor.validate_chain(chain_id)?;
		let latest = latest_block_number(&provider).await?;
		cursor.initialize(latest);
		let canonical_provider = RpcCanonicalBlockProvider::new(&provider, self.block_fetch_mode);
		reconcile_through(&canonical_provider, chain_id, sender, cursor, latest).await?;

		let mut interval = tokio::time::interval(self.poll_interval);
		interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
		info!(
			chain_id = chain_id.get(),
			poll_interval_ms = self.poll_interval.as_millis(),
			block_fetch_mode = %self.block_fetch_mode,
			"connected HTTP polling event source"
		);
		loop {
			interval.tick().await;
			let latest = latest_block_number(&provider).await?;
			reconcile_through(&canonical_provider, chain_id, sender, cursor, latest).await?;
		}
	}

	/// Runs the WebSocket ingestion loop using new-head subscriptions plus periodic reconciliation.
	///
	/// ```text
	///                    WebSocket RPC
	///                         │
	///               subscribe_blocks()
	///                         │
	///                         ▼
	///                    new head #N
	///                         │
	///                         ▼
	///                 reconcile_through(N)
	///                         │
	///            ┌────────────┴────────────┐
	///            │                         │
	///      canonical advance          shallow reorg
	///            │                         │
	///            ▼                         ▼
	///      Applied(#N...)         Reverted(old...)
	///                             Applied(new...)
	///
	/// Every `reconciliation_interval`, Raven also asks the RPC endpoint for the
	/// latest block and reconciles again. This safety pass covers missed or dropped
	/// subscription notifications while keeping the same canonical cursor/reorg logic.
	/// ```
	///
	/// The function returns only when the subscription ends, the receiver is dropped,
	/// or an RPC/reconciliation error occurs; transient failures are handled by `run()`.
	async fn run_websocket(
		&self,
		sender: &mpsc::Sender<ChainEvent>,
		cursor: &mut CanonicalCursor,
	) -> AlloySourceResult {
		let provider = ProviderBuilder::new()
			.connect_ws(WsConnect::new(&self.rpc_url))
			.await
			.map_err(|error| AlloySourceError::Connection(error.to_string()))?;
		let chain_id = validated_chain_id(&provider).await?;
		cursor.validate_chain(chain_id)?;
		let subscription = provider
			.subscribe_blocks()
			.channel_size(SUBSCRIPTION_CHANNEL_CAPACITY)
			.await
			.map_err(|error| AlloySourceError::Subscription(error.to_string()))?;
		let mut heads = subscription.into_stream();
		let latest = latest_block_number(&provider).await?;
		cursor.initialize(latest);
		let canonical_provider = RpcCanonicalBlockProvider::new(&provider, self.block_fetch_mode);
		reconcile_through(&canonical_provider, chain_id, sender, cursor, latest).await?;

		info!(
			chain_id = chain_id.get(),
			reconciliation_interval_ms = self.reconciliation_interval.as_millis(),
			block_fetch_mode = %self.block_fetch_mode,
			"connected WebSocket subscription event source"
		);
		let start = tokio::time::Instant::now() + self.reconciliation_interval;
		let mut reconciliation = tokio::time::interval_at(start, self.reconciliation_interval);
		reconciliation.set_missed_tick_behavior(MissedTickBehavior::Skip);

		loop {
			tokio::select! {
				maybe_header = heads.next() => {
					let header = maybe_header.ok_or(AlloySourceError::SubscriptionEnded)?;
					reconcile_through(&canonical_provider, chain_id, sender, cursor, header.number).await?;
				}
				_ = reconciliation.tick() => {
					let latest = latest_block_number(&provider).await?;
					reconcile_through(&canonical_provider, chain_id, sender, cursor, latest).await?;
				}
			}
		}
	}
}

/// Raven's local canonical-chain bookmark.
///
/// `window` is the bounded ancestry Raven has already emitted, ordered oldest
/// to newest. It is the rear-view mirror used to find a common ancestor when the
/// RPC canonical chain changes. `next_block` is the next height Raven has not
/// emitted yet. `reorg_depth` caps memory use and defines how deep a shallow
/// reorg Raven can repair automatically.
///
/// ```text
/// forgotten history        retained window             next work
///       ...          [#100 -> #101 -> #102]               #103
///                         oldest     tip                  ^
///                              reorg_depth cap            |
///                                                   next_block
/// ```
#[derive(Debug)]
struct CanonicalCursor {
	window: VecDeque<BlockEvent>,
	next_block: Option<u64>,
	reorg_depth: usize,
}

impl CanonicalCursor {
	fn new(start: SourceStart, reorg_depth: usize) -> AlloySourceResult<Self> {
		let (window, next_block) = match start {
			SourceStart::Latest => (VecDeque::new(), None),
			SourceStart::Block(block) => (VecDeque::new(), Some(block)),
			SourceStart::Resume(blocks) => {
				validate_resume_window(&blocks)?;
				let next = blocks.last().map(|block| block.block_number().saturating_add(1));
				(blocks.into(), next)
			},
		};
		Ok(Self { window, next_block, reorg_depth })
	}

	fn initialize(&mut self, latest: u64) {
		self.next_block.get_or_insert(latest);
	}

	fn validate_chain(&self, chain_id: ChainId) -> AlloySourceResult {
		if let Some(block) = self.window.front() &&
			block.chain_id() != chain_id
		{
			return Err(AlloySourceError::InvalidResumeWindow(format!(
				"checkpoint chain {} does not match RPC chain {}",
				block.chain_id().get(),
				chain_id.get()
			)));
		}
		Ok(())
	}

	fn push(&mut self, block: BlockEvent) {
		self.window.push_back(block);
		while self.window.len() > self.reorg_depth {
			self.window.pop_front();
		}
	}
}

/// Validates checkpoint ancestry before `SourceStart::Resume` can seed a cursor.
///
/// A resume window must already be one contiguous parent-linked chain for a
/// single EVM chain. Runtime reconciliation can then compare that trusted local
/// window against the RPC canonical chain and emit reverts when needed.
///
/// ```text
/// valid:
///   #10(hash A) <- parent of #11(hash B) <- parent of #12(hash C)
///
/// invalid:
///   #10(hash A)    #11(parent = X)
///        ^              |
///        +--------------+ parent link does not match
/// ```
fn validate_resume_window(blocks: &[BlockEvent]) -> AlloySourceResult {
	for pair in blocks.windows(2) {
		let parent = &pair[0];
		let child = &pair[1];
		if child.chain_id() != parent.chain_id() ||
			child.block_number() != parent.block_number().saturating_add(1) ||
			child.parent_hash() != parent.block_hash()
		{
			return Err(AlloySourceError::InvalidResumeWindow(
				"blocks must be one contiguous parent-linked chain".to_owned(),
			));
		}
	}
	Ok(())
}

/// Reconciles Raven's retained canonical view with the RPC node through `latest`.
///
/// In other words: make Raven's local chain view match the RPC node's canonical
/// chain up to the requested block height.
///
/// ```text
/// Raven currently remembers:
///
///   #10 ──▶ #11 ──▶ #12
///     A       B       C
///
/// RPC canonical chain now says:
///
///   #10 ──▶ #11' ──▶ #12'
///     A       X        Y
///
/// Reconciliation walks backward to find the common ancestor:
///
///   #12:  C == Y ?  no
///   #11:  B == X ?  no
///   #10:  A == A ?  yes  ← common ancestor
///
/// Then repair Raven's view:
///
///   Reverted(#12)
///        │
///        ▼
///   Reverted(#11)
///        │
///        ▼
///   Applied(#11')
///        │
///        ▼
///   Applied(#12')
///
/// Final state:
///
///   Raven:  #10 ──▶ #11' ──▶ #12'
///   RPC:    #10 ──▶ #11' ──▶ #12'
///
///                         ✓ synchronized
/// ```
///
/// If no retained block matches the RPC chain, the reorg is deeper than the
/// configured reorg window and `DeepReorg` error is returned.
///
/// If the canonical chain changes while catching up, or a requested block is
/// temporarily unavailable, a retryable error is returned so reconciliation
/// can resume from the last successfully emitted height.
///
/// `sender` is the boundary from source ingestion into the rest of Raven. Every
/// event sent through it has already been ordered against the local canonical
/// cursor: reverted blocks are sent from tip back to the common ancestor, and
/// applied blocks are sent from the next missing height forward. The CLI/runtime
/// side decides when those delivered events are fully acknowledged and safe to
/// checkpoint.
///
/// ```text
/// reconcile_through
///        |
///        v
/// ChainEvent::BlockReverted / ChainEvent::BlockApplied
///        |
///        v
/// sender.send(...)
///        |
///        v
/// Raven runtime -> plugin workers -> CLI checkpoint boundary
/// ```
async fn reconcile_through(
	provider: &impl CanonicalBlockProvider,
	chain_id: ChainId,
	sender: &mpsc::Sender<ChainEvent>,
	cursor: &mut CanonicalCursor,
	latest: u64,
) -> AlloySourceResult {
	if let Some(earliest) = cursor.window.front().map(BlockEvent::block_number) {
		let mut common_index = None;
		for (index, retained) in cursor.window.iter().enumerate().rev() {
			if retained.block_number() > latest {
				continue;
			}
			let canonical = fetch_block(provider, chain_id, retained.block_number()).await?;
			if canonical.block_hash() == retained.block_hash() {
				common_index = Some(index);
				break;
			}
		}

		let Some(common_index) = common_index else {
			return Err(AlloySourceError::DeepReorg { earliest_block: earliest });
		};

		let reverted_blocks = cursor.window.len().saturating_sub(common_index + 1);
		if reverted_blocks > 0 {
			let common_ancestor =
				cursor.window.get(common_index).expect("common index exists in cursor window");
			info!(
				chain_id = chain_id.get(),
				latest_block = latest,
				common_ancestor_block = common_ancestor.block_number(),
				common_ancestor_hash = %common_ancestor.block_hash(),
				reverted_blocks,
				"shallow reorg detected; reverting retained canonical blocks"
			);
		}

		while cursor.window.len() > common_index + 1 {
			let reverted = cursor.window.pop_back().expect("window exceeds common ancestor");
			info!(
				chain_id = chain_id.get(),
				block_number = reverted.block_number(),
				block_hash = %reverted.block_hash(),
				parent_hash = %reverted.parent_hash(),
				"emitting reverted canonical block event after shallow reorg"
			);
			sender
				.send(ChainEvent::BlockReverted(reverted))
				.await
				.map_err(|_| AlloySourceError::EventReceiverDropped)?;
		}
		cursor.next_block =
			cursor.window.back().map(|block| block.block_number().saturating_add(1));
	}

	let mut next = cursor.next_block.expect("cursor is initialized before reconciliation");
	while next <= latest {
		let block = fetch_block(provider, chain_id, next).await?;
		if let Some(parent) = cursor.window.back() &&
			block.block_number() == parent.block_number().saturating_add(1) &&
			block.parent_hash() != parent.block_hash()
		{
			return Err(AlloySourceError::CanonicalChanged { block_number: next });
		}

		debug!(chain_id = chain_id.get(), block_number = block.block_number(), block_hash = %block.block_hash(), transactions = block.transaction_count(), logs = block.logs().len(), "received canonical block event");
		sender
			.send(ChainEvent::BlockApplied(block.clone()))
			.await
			.map_err(|_| AlloySourceError::EventReceiverDropped)?;
		cursor.push(block);
		next = next.saturating_add(1);
		cursor.next_block = Some(next);
	}
	Ok(())
}

async fn fetch_block(
	provider: &impl CanonicalBlockProvider,
	chain_id: ChainId,
	block_number: u64,
) -> AlloySourceResult<BlockEvent> {
	provider.canonical_block(chain_id, block_number).await
}

/// Narrow block-fetching adapter used by reconciliation.
///
/// Production implementations delegate to Alloy's `Provider`; tests implement
/// this trait with an in-memory canonical chain so reorg and retry behavior can
/// be exercised without a live RPC endpoint.
///
/// ```text
/// reconcile_through
///        |
///        v
/// canonical_block(chain_id, number)
///        |
///        +-- production: Alloy Provider -> batch or sequential strategy -> convert_block
///        |
///        +-- tests: in-memory block map
/// ```
trait CanonicalBlockProvider {
	async fn canonical_block(
		&self,
		chain_id: ChainId,
		block_number: u64,
	) -> AlloySourceResult<BlockEvent>;
}

struct RpcCanonicalBlockProvider<'a, P> {
	provider: &'a P,
	block_fetch_mode: BlockFetchMode,
}

impl<'a, P> RpcCanonicalBlockProvider<'a, P> {
	const fn new(provider: &'a P, block_fetch_mode: BlockFetchMode) -> Self {
		Self { provider, block_fetch_mode }
	}
}

impl<P> CanonicalBlockProvider for RpcCanonicalBlockProvider<'_, P>
where
	P: Provider,
{
	async fn canonical_block(
		&self,
		chain_id: ChainId,
		block_number: u64,
	) -> AlloySourceResult<BlockEvent> {
		match self.block_fetch_mode {
			BlockFetchMode::Batch =>
				fetch_canonical_block_batch(self.provider, chain_id, block_number).await,
			BlockFetchMode::Sequential =>
				fetch_canonical_block_sequential(self.provider, chain_id, block_number).await,
		}
	}
}

async fn fetch_canonical_block_sequential(
	provider: &impl Provider,
	chain_id: ChainId,
	block_number: u64,
) -> AlloySourceResult<BlockEvent> {
	let block = provider
		.get_block_by_number(BlockNumberOrTag::Number(block_number))
		.await
		.map_err(|error| AlloySourceError::BlockRequest(error.to_string()))?
		.ok_or_else(|| missing_block_error(block_number))?;
	let filter = Filter::new().at_block_hash(block.header.hash);
	let logs = provider
		.get_logs(&filter)
		.await
		.map_err(|error| AlloySourceError::LogRequest(error.to_string()))?;
	Ok(convert_block(chain_id, &block, logs)?.block().clone())
}

async fn fetch_canonical_block_batch(
	provider: &impl Provider,
	chain_id: ChainId,
	block_number: u64,
) -> AlloySourceResult<BlockEvent> {
	let block_tag = BlockNumberOrTag::Number(block_number);
	let block_params = (block_tag, false);
	let filter = Filter::new().select(block_number);
	let log_params = (&filter,);
	let mut batch = BatchRequest::new(provider.client());
	let block_waiter = batch
		.add_call::<_, Option<Block>>("eth_getBlockByNumber", &block_params)
		.map_err(|error| AlloySourceError::BlockRequest(error.to_string()))?;
	let logs_waiter = batch
		.add_call::<_, Vec<Log>>("eth_getLogs", &log_params)
		.map_err(|error| AlloySourceError::LogRequest(error.to_string()))?;

	batch.await.map_err(|error| AlloySourceError::BatchRequest(error.to_string()))?;
	let block = block_waiter
		.await
		.map_err(|error| AlloySourceError::BlockRequest(error.to_string()))?
		.ok_or_else(|| missing_block_error(block_number))?;
	let logs = logs_waiter
		.await
		.map_err(|error| AlloySourceError::LogRequest(error.to_string()))?;

	validate_batch_consistency(&block, &logs, block_number)?;
	Ok(convert_block(chain_id, &block, logs)?.block().clone())
}

fn missing_block_error(block_number: u64) -> AlloySourceError {
	AlloySourceError::BlockRequest(format!("block {block_number} was not returned by RPC"))
}

fn validate_batch_consistency(
	block: &Block,
	logs: &[Log],
	requested_block: u64,
) -> AlloySourceResult {
	let header = block.header();
	if header.number() != requested_block {
		return Err(AlloySourceError::BlockConversion(format!(
			"RPC returned block {} for requested block {requested_block}",
			header.number()
		)));
	}

	if logs
		.iter()
		.any(|log| log.block_number != Some(requested_block) || log.block_hash != Some(header.hash))
	{
		return Err(AlloySourceError::CanonicalChanged { block_number: requested_block });
	}

	let returned_logs_bloom = alloy::primitives::logs_bloom(logs.iter().map(|log| &log.inner));
	if returned_logs_bloom != header.logs_bloom() {
		return Err(AlloySourceError::CanonicalChanged { block_number: requested_block });
	}

	Ok(())
}

async fn validated_chain_id(provider: &impl Provider) -> AlloySourceResult<ChainId> {
	let raw = provider
		.get_chain_id()
		.await
		.map_err(|error| AlloySourceError::ChainIdRequest(error.to_string()))?;
	ChainId::new(raw).map_err(|_| AlloySourceError::InvalidChainId(raw))
}

async fn latest_block_number(provider: &impl Provider) -> AlloySourceResult<u64> {
	provider
		.get_block_number()
		.await
		.map_err(|error| AlloySourceError::BlockRequest(error.to_string()))
}

#[cfg(test)]
mod tests {
	use super::*;
	use alloy::{
		consensus::Header as ConsensusHeader,
		primitives::{Address, B256, Bytes, Log as PrimitiveLog, LogData},
		providers::mock::Asserter,
		rpc::types::{BlockTransactions, Header, Log as RpcLog},
	};
	use raven_core::EvmLog;
	use std::{collections::BTreeMap, sync::Mutex};

	const RPC_BLOCK_NUMBER: u64 = 21_000_000;
	const RPC_BLOCK_HASH: B256 = B256::repeat_byte(0x11);

	fn rpc_log() -> RpcLog {
		RpcLog {
			inner: PrimitiveLog {
				address: Address::repeat_byte(0x33),
				data: LogData::new(vec![B256::repeat_byte(0x44)], Bytes::from_static(&[0x55]))
					.unwrap(),
			},
			block_hash: Some(RPC_BLOCK_HASH),
			block_number: Some(RPC_BLOCK_NUMBER),
			transaction_hash: Some(B256::repeat_byte(0x66)),
			transaction_index: Some(0),
			log_index: Some(0),
			..Default::default()
		}
	}

	fn rpc_block(logs: &[RpcLog]) -> Block {
		let consensus_header = ConsensusHeader {
			number: RPC_BLOCK_NUMBER,
			parent_hash: B256::repeat_byte(0x22),
			timestamp: 1_720_000_000,
			logs_bloom: alloy::primitives::logs_bloom(logs.iter().map(|log| &log.inner)),
			..Default::default()
		};
		let rpc_header =
			Header { hash: RPC_BLOCK_HASH, inner: consensus_header, ..Default::default() };
		let mut block = Block { header: rpc_header, ..Default::default() };
		block.transactions = BlockTransactions::Hashes(vec![B256::repeat_byte(0x66)]);
		block
	}

	#[test]
	fn batch_is_the_default_block_fetch_mode() {
		let source = AlloySource::new("http://localhost:8545");

		assert_eq!(BlockFetchMode::default(), BlockFetchMode::Batch);
		assert_eq!(source.block_fetch_mode, BlockFetchMode::Batch);
	}

	#[tokio::test]
	async fn batch_fetches_and_converts_block_with_logs() {
		let logs = vec![rpc_log()];
		let block = rpc_block(&logs);
		let asserter = Asserter::new();
		asserter.push_success(&block);
		asserter.push_success(&logs);
		let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
		let chain_id = ChainId::new(8_453).unwrap();

		let fetched = fetch_canonical_block_batch(&provider, chain_id, RPC_BLOCK_NUMBER)
			.await
			.unwrap();

		assert_eq!(fetched.block_number(), RPC_BLOCK_NUMBER);
		assert_eq!(fetched.block_hash(), RPC_BLOCK_HASH);
		assert_eq!(fetched.logs().len(), 1);
		assert!(asserter.read_q().is_empty());
	}

	#[tokio::test]
	async fn sequential_fetch_remains_available() {
		let logs = vec![rpc_log()];
		let block = rpc_block(&logs);
		let asserter = Asserter::new();
		asserter.push_success(&block);
		asserter.push_success(&logs);
		let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
		let chain_id = ChainId::new(8_453).unwrap();

		let fetched = fetch_canonical_block_sequential(&provider, chain_id, RPC_BLOCK_NUMBER)
			.await
			.unwrap();

		assert_eq!(fetched.block_hash(), RPC_BLOCK_HASH);
		assert_eq!(fetched.logs().len(), 1);
		assert!(asserter.read_q().is_empty());
	}

	#[test]
	fn batch_retries_when_logs_belong_to_another_canonical_block() {
		let expected_log = rpc_log();
		let block = rpc_block(std::slice::from_ref(&expected_log));
		let mut replacement_log = expected_log;
		replacement_log.block_hash = Some(B256::repeat_byte(0x99));

		let error =
			validate_batch_consistency(&block, &[replacement_log], RPC_BLOCK_NUMBER).unwrap_err();

		assert!(matches!(
			error,
			AlloySourceError::CanonicalChanged { block_number: RPC_BLOCK_NUMBER }
		));
	}

	#[test]
	fn batch_retries_when_empty_logs_conflict_with_header_bloom() {
		let block = rpc_block(&[rpc_log()]);

		let error = validate_batch_consistency(&block, &[], RPC_BLOCK_NUMBER).unwrap_err();

		assert!(matches!(
			error,
			AlloySourceError::CanonicalChanged { block_number: RPC_BLOCK_NUMBER }
		));
	}

	#[tokio::test]
	async fn rejects_zero_intervals_before_connecting() {
		let (sender, _receiver) = mpsc::channel(1);
		let error = AlloySource::new("http://localhost:8545")
			.with_poll_interval(Duration::ZERO)
			.run(sender)
			.await
			.expect_err("zero interval must fail");
		assert!(matches!(error, AlloySourceError::InvalidPollInterval));
	}

	#[tokio::test]
	async fn rejects_invalid_rpc_url_before_interval_validation() {
		let (sender, _receiver) = mpsc::channel(1);
		let error = AlloySource::new("not-a-url")
			.with_poll_interval(Duration::ZERO)
			.run(sender)
			.await
			.expect_err("invalid RPC URL should fail before interval validation");
		assert!(matches!(error, AlloySourceError::InvalidRpcUrl(_)));
	}

	#[test]
	fn selects_transport_from_connection_string() {
		for url in ["http://localhost:8545", "https://ethereum.example"] {
			assert_eq!(RpcTransport::from_url(url).unwrap(), RpcTransport::Http);
		}
		for url in ["ws://localhost:8546", "wss://ethereum.example"] {
			assert_eq!(RpcTransport::from_url(url).unwrap(), RpcTransport::WebSocket);
		}
	}

	#[test]
	fn retry_backoff_is_capped_and_deterministic_without_jitter() {
		let policy = RetryPolicy {
			initial_delay: Duration::from_millis(100),
			max_delay: Duration::from_millis(500),
			jitter_ratio: 0.0,
		};
		assert_eq!(policy.delay(1), Duration::from_millis(100));
		assert_eq!(policy.delay(2), Duration::from_millis(200));
		assert_eq!(policy.delay(3), Duration::from_millis(400));
		assert_eq!(policy.delay(4), Duration::from_millis(500));
		assert_eq!(policy.delay(20), Duration::from_millis(500));
	}

	#[test]
	fn temporary_omissions_and_transport_failures_are_retryable() {
		for error in [
			AlloySourceError::Connection("offline".to_owned()),
			AlloySourceError::BlockRequest("fixture omitted block".to_owned()),
			AlloySourceError::LogRequest("fixture omitted logs".to_owned()),
			AlloySourceError::BatchRequest("fixture rejected batch".to_owned()),
			AlloySourceError::SubscriptionEnded,
			AlloySourceError::CanonicalChanged { block_number: 12 },
		] {
			assert!(error.is_transient());
		}
		assert!(!AlloySourceError::DeepReorg { earliest_block: 10 }.is_transient());
	}

	#[test]
	fn validates_contiguous_resume_window() {
		let chain = ChainId::new(8453).unwrap();
		let first = BlockEvent::new(chain, 10, hash_value(10), hash_value(9), 1, 0).unwrap();
		let second = BlockEvent::new(chain, 11, hash_value(11), first.block_hash(), 2, 0).unwrap();
		assert!(validate_resume_window(&[first, second]).is_ok());
	}

	#[tokio::test]
	async fn shallow_fork_reverts_old_tip_before_applying_replacement() {
		let chain = ChainId::new(8453).unwrap();
		let a = block(chain, 10, 10, 9);
		let b = block(chain, 11, 11, 10);
		let c = block_with_log(chain, 12, 12, 11);
		let x = block(chain, 11, 1011, 10);
		let y = block(chain, 12, 1012, 1011);
		let provider = MockCanonicalProvider::new([a.clone(), x.clone(), y.clone()]);
		let mut cursor =
			CanonicalCursor::new(SourceStart::Resume(vec![a, b.clone(), c.clone()]), 64).unwrap();
		let (sender, mut receiver) = mpsc::channel(8);

		reconcile_through(&provider, chain, &sender, &mut cursor, 12).await.unwrap();

		assert_eq!(receiver.recv().await, Some(ChainEvent::BlockReverted(c)));
		assert_eq!(receiver.recv().await, Some(ChainEvent::BlockReverted(b)));
		assert_eq!(receiver.recv().await, Some(ChainEvent::BlockApplied(x)));
		assert_eq!(receiver.recv().await, Some(ChainEvent::BlockApplied(y)));
	}

	#[tokio::test]
	async fn explicit_start_catches_up_every_missing_height_in_order() {
		let chain = ChainId::new(8453).unwrap();
		let blocks = [block(chain, 10, 10, 9), block(chain, 11, 11, 10), block(chain, 12, 12, 11)];
		let provider = MockCanonicalProvider::new(blocks.clone());
		let mut cursor = CanonicalCursor::new(SourceStart::Block(10), 64).unwrap();
		let (sender, mut receiver) = mpsc::channel(8);
		reconcile_through(&provider, chain, &sender, &mut cursor, 12).await.unwrap();

		for block in blocks {
			assert_eq!(receiver.recv().await, Some(ChainEvent::BlockApplied(block)));
		}
	}

	/// If Raven successfully emits block #10, then fails because #11 is temporarily unavailable, it
	/// must retry from #11 — not start over from #10.
	///
	/// ------------------------------------
	///
	/// First attempt
	///
	/// #10 ✅
	/// #11 ❌
	///
	/// events:
	/// Applied(#10)
	///
	/// cursor remembers:
	/// next_block = 11
	///
	///
	/// RPC recovers
	///
	/// #10 ✅
	/// #11 ✅
	///
	///
	/// Second attempt
	///
	/// start at cursor.next_block
	///           │
	///           ▼
	///          #11
	///
	/// events:
	/// Applied(#11)
	#[tokio::test]
	async fn temporary_block_omission_retries_from_the_unemitted_height() {
		let chain = ChainId::new(8453).unwrap();
		let ten = block(chain, 10, 10, 9);
		let eleven = block(chain, 11, 11, 10);
		let provider = MockCanonicalProvider::new([ten.clone()]);
		let mut cursor = CanonicalCursor::new(SourceStart::Block(10), 64).unwrap();
		let (sender, mut receiver) = mpsc::channel(8);

		let error = reconcile_through(&provider, chain, &sender, &mut cursor, 11)
			.await
			.expect_err("fixture intentionally omits block 11");
		assert!(error.is_transient());
		assert_eq!(receiver.recv().await, Some(ChainEvent::BlockApplied(ten)));

		provider.insert(eleven.clone());
		reconcile_through(&provider, chain, &sender, &mut cursor, 11).await.unwrap();
		assert_eq!(receiver.recv().await, Some(ChainEvent::BlockApplied(eleven)));
	}

	fn block(chain: ChainId, number: u64, hash: u64, parent: u64) -> BlockEvent {
		BlockEvent::new(chain, number, hash_value(hash), hash_value(parent), 1, 0).unwrap()
	}

	fn block_with_log(chain: ChainId, number: u64, hash: u64, parent: u64) -> BlockEvent {
		let log = EvmLog::new(
			Address::repeat_byte(0x11),
			vec![B256::repeat_byte(0x22)],
			Bytes::from_static(&[0x33]),
			B256::repeat_byte(0x44),
			0,
			0,
		)
		.unwrap();
		BlockEvent::new_with_logs(
			chain,
			number,
			hash_value(hash),
			hash_value(parent),
			1,
			1,
			vec![log],
		)
		.unwrap()
	}

	fn hash_value(value: u64) -> alloy::primitives::B256 {
		format!("0x{value:064x}").parse().unwrap()
	}

	struct MockCanonicalProvider {
		blocks: Mutex<BTreeMap<u64, BlockEvent>>,
	}

	impl MockCanonicalProvider {
		fn new(blocks: impl IntoIterator<Item = BlockEvent>) -> Self {
			Self {
				blocks: Mutex::new(
					blocks.into_iter().map(|block| (block.block_number(), block)).collect(),
				),
			}
		}

		fn insert(&self, block: BlockEvent) {
			self.blocks.lock().unwrap().insert(block.block_number(), block);
		}
	}

	impl CanonicalBlockProvider for MockCanonicalProvider {
		async fn canonical_block(
			&self,
			_chain_id: ChainId,
			block_number: u64,
		) -> AlloySourceResult<BlockEvent> {
			self.blocks.lock().unwrap().get(&block_number).cloned().ok_or_else(|| {
				AlloySourceError::BlockRequest(format!("fixture omitted block {block_number}"))
			})
		}
	}
}
