//! Statically linked Raven plugin for detecting large ERC-20 transfers.
//!
//! The plugin consumes normalized logs already attached to [`raven_core::BlockEvent`]. It has no
//! provider or RPC dependency: chain access stays in the event source and detection stays a pure
//! function of the delivered event.
//!
//! ```text
//! Alloy RPC source -> BlockEvent.logs -> ERC-20 ABI decode -> threshold/filter -> tracing sink
//!                                            |
//!                          BlockApplied -----+----- BlockReverted
//!                              alert                 correction
//! ```

use alloy_primitives::{Address, B256, U256};
use alloy_sol_types::{SolEvent, sol};
use async_trait::async_trait;
use raven_core::{BlockEvent, ChainEvent, EvmLog};
use raven_plugin_sdk::{Plugin, PluginContext, PluginMetadata, PluginResult};
use std::fmt::Write as _;
use thiserror::Error;
use tracing::{debug, info};

sol! {
	/// ERC-20 transfer event from EIP-20.
	#[derive(Debug, PartialEq, Eq)]
	event Transfer(address indexed from, address indexed to, uint256 value);
}

/// Configuration for [`Erc20TransferPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Erc20TransferConfig {
	minimum_amounts: Vec<U256>,
	token_addresses: Vec<Address>,
}

impl Erc20TransferConfig {
	/// Creates a monitor with one shared inclusive threshold for all supplied tokens.
	///
	/// An empty token-address list matches every contract that emits a valid ERC-20 `Transfer` log.
	/// Raven intentionally does not guess token decimals, so callers must configure the threshold
	/// in the token's smallest unit. Use [`Self::new_with_token_thresholds`] when tokens need
	/// different minimum amounts.
	pub fn new(
		minimum_amount: U256,
		token_addresses: impl IntoIterator<Item = Address>,
	) -> Result<Self, Erc20TransferConfigError> {
		if minimum_amount.is_zero() {
			return Err(Erc20TransferConfigError::ZeroMinimumAmount);
		}

		let mut token_addresses: Vec<_> = token_addresses.into_iter().collect();
		token_addresses.sort_unstable();
		token_addresses.dedup();
		let threshold_count = token_addresses.len().max(1);

		Ok(Self { minimum_amounts: vec![minimum_amount; threshold_count], token_addresses })
	}

	/// Creates a monitor with one inclusive threshold for each token contract.
	///
	/// Each `(token, minimum_amount)` pair is kept together while the configuration is sorted for
	/// deterministic lookup. Duplicate token addresses are rejected because their intended
	/// threshold would be ambiguous.
	pub fn new_with_token_thresholds(
		token_thresholds: impl IntoIterator<Item = (Address, U256)>,
	) -> Result<Self, Erc20TransferConfigError> {
		let mut token_thresholds: Vec<_> = token_thresholds.into_iter().collect();
		if token_thresholds.is_empty() {
			return Err(Erc20TransferConfigError::EmptyTokenThresholds);
		}
		if token_thresholds.iter().any(|(_, minimum_amount)| minimum_amount.is_zero()) {
			return Err(Erc20TransferConfigError::ZeroMinimumAmount);
		}

		token_thresholds.sort_unstable_by_key(|(token, _)| *token);
		if let Some(duplicate) = token_thresholds
			.windows(2)
			.find(|pair| pair[0].0 == pair[1].0)
			.map(|pair| pair[1].0)
		{
			return Err(Erc20TransferConfigError::DuplicateTokenAddress(duplicate));
		}

		let (token_addresses, minimum_amounts) = token_thresholds.into_iter().unzip();
		Ok(Self { minimum_amounts, token_addresses })
	}

	/// Returns the smallest configured inclusive threshold in raw token units.
	pub fn minimum_amount(&self) -> U256 {
		self.minimum_amounts
			.iter()
			.copied()
			.min()
			.expect("validated transfer config always has a threshold")
	}

	/// Returns thresholds aligned positionally with [`Self::token_addresses`].
	///
	/// An all-token configuration has one threshold and no token addresses.
	pub fn minimum_amounts(&self) -> &[U256] {
		&self.minimum_amounts
	}

	/// Returns the token contracts accepted by this monitor; an empty slice means all contracts.
	pub fn token_addresses(&self) -> &[Address] {
		&self.token_addresses
	}

	/// Returns the inclusive threshold for `token`, or `None` when it is filtered out.
	pub fn minimum_amount_for(&self, token: Address) -> Option<U256> {
		if self.token_addresses.is_empty() {
			return self.minimum_amounts.first().copied();
		}

		self.token_addresses
			.binary_search(&token)
			.ok()
			.and_then(|index| self.minimum_amounts.get(index).copied())
	}
}

/// Invalid transfer-monitor configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Erc20TransferConfigError {
	/// Per-token construction requires at least one token/threshold pair.
	#[error("ERC-20 token thresholds must not be empty")]
	EmptyTokenThresholds,

	/// A zero threshold would classify every transfer as large.
	#[error("ERC-20 transfer minimum amount must be greater than zero")]
	ZeroMinimumAmount,

	/// One token cannot have multiple positional thresholds.
	#[error("duplicate ERC-20 token threshold for {0}")]
	DuplicateTokenAddress(Address),
}

/// One decoded ERC-20 transfer that met the configured filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LargeTransfer {
	/// Token contract that emitted the event.
	pub token: Address,
	/// Address tokens moved from.
	pub from: Address,
	/// Address tokens moved to.
	pub to: Address,
	/// Amount in the token contract's raw smallest unit.
	pub amount: U256,
	/// Transaction that emitted the transfer.
	pub transaction_hash: B256,
	/// Log position within the block.
	pub log_index: u64,
}

/// Bundled, statically linked plugin that reports configured large ERC-20 transfers.
pub struct Erc20TransferPlugin {
	config: Erc20TransferConfig,
	output_format: Erc20TransferOutputFormat,
}

impl Erc20TransferPlugin {
	/// Creates a transfer monitor with the default long terminal output.
	pub const fn new(config: Erc20TransferConfig) -> Self {
		Self { config, output_format: Erc20TransferOutputFormat::Long }
	}

	/// Creates a transfer monitor with an explicit terminal output format.
	pub const fn with_output_format(
		config: Erc20TransferConfig,
		output_format: Erc20TransferOutputFormat,
	) -> Self {
		Self { config, output_format }
	}

	/// Returns matching transfers in deterministic block/log order.
	pub fn matching_transfers(&self, block: &BlockEvent) -> Vec<LargeTransfer> {
		block
			.logs()
			.iter()
			.filter_map(|log| {
				let minimum_amount = self.config.minimum_amount_for(log.address())?;
				let transfer = decode_transfer(log)?;
				(transfer.amount >= minimum_amount).then_some(transfer)
			})
			.collect()
	}
}

/// Terminal presentation used for block-scoped transfer reports.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Erc20TransferOutputFormat {
	/// Preserve complete block hashes, addresses, and transaction hashes.
	#[default]
	Long,

	/// Abbreviate fixed-width hashes and addresses for quicker terminal scanning.
	Short,
}

impl Erc20TransferOutputFormat {
	fn is_short(&self) -> bool {
		self.eq(&Self::Short)
	}
}

impl Erc20TransferOutputFormat {
	const fn as_str(self) -> &'static str {
		match self {
			Self::Long => "long",
			Self::Short => "short",
		}
	}
}

fn decode_transfer(log: &EvmLog) -> Option<LargeTransfer> {
	if log.topics().first() != Some(&Transfer::SIGNATURE_HASH) {
		return None;
	}

	let decoded = Transfer::decode_log_data_validate(log.log_data()).ok()?;
	Some(LargeTransfer {
		token: log.address(),
		from: decoded.from,
		to: decoded.to,
		amount: decoded.value,
		transaction_hash: log.transaction_hash(),
		log_index: log.log_index(),
	})
}

/// Formats one block-scoped terminal report from deterministic transfer matches.
///
/// Keeping the report as one tracing event prevents concurrent plugins from
/// inserting output between its header and rows.
fn format_transfer_report(
	block: &BlockEvent,
	action: &str,
	transfers: &[LargeTransfer],
	output_format: Erc20TransferOutputFormat,
) -> String {
	let mut report = String::new();
	if output_format.is_short() {
		let _ = writeln!(
			report,
			"+-- ERC-20 transfer matches ----------------------------------------------"
		);
	} else {
		let _ = writeln!(
			report,
			"+-- ERC-20 transfer matches --------------------------------------------------------------"
		);
	}
	let _ = writeln!(
		report,
		"| block={} hash={} action={} matches={}",
		block.block_number(),
		format_identifier(block.block_hash(), output_format),
		action,
		transfers.len(),
	);
	let _ = writeln!(report, "|");

	for (index, transfer) in transfers.iter().enumerate() {
		let _ = writeln!(
			report,
			"| {:>2}. token={} amount={} log_index={}",
			index + 1,
			format_identifier(transfer.token, output_format),
			transfer.amount,
			transfer.log_index,
		);
		let _ = writeln!(
			report,
			"|     from={} -> to={} tx={}",
			format_identifier(transfer.from, output_format),
			format_identifier(transfer.to, output_format),
			format_identifier(transfer.transaction_hash, output_format),
		);
	}

	if output_format.is_short() {
		let _ = write!(
			report,
			"+------------------------------------------------------------------------"
		);
	} else {
		let _ = write!(
			report,
			"+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
		);
	}
	report
}

/// Formats fixed-width hexadecimal identifiers for the selected terminal mode.
fn format_identifier(
	value: impl std::fmt::Display,
	output_format: Erc20TransferOutputFormat,
) -> String {
	if output_format.is_short() {
		let value = value.to_string();
		const PREFIX_LENGTH: usize = 8;
		const SUFFIX_LENGTH: usize = 6;

		if value.len() <= PREFIX_LENGTH + SUFFIX_LENGTH + 3 {
			return value;
		}

		return format!("{}...{}", &value[..PREFIX_LENGTH], &value[value.len() - SUFFIX_LENGTH..])
	}
	value.to_string()
}

#[async_trait]
impl Plugin for Erc20TransferPlugin {
	fn metadata(&self) -> PluginMetadata {
		PluginMetadata::new(
			"erc20-transfer",
			env!("CARGO_PKG_VERSION"),
			"Reports large ERC-20 transfers from normalized block logs",
		)
	}

	async fn start(&mut self, context: &PluginContext) -> PluginResult {
		info!(
			chain_id = context.chain_id().get(),
			minimum_amount_floor = %self.config.minimum_amount(),
			threshold_count = self.config.minimum_amounts.len(),
			token_filter_count = self.config.token_addresses.len(),
			output_format = self.output_format.as_str(),
			"ERC-20 transfer monitor started"
		);
		Ok(())
	}

	async fn handle_event(&mut self, event: &ChainEvent, _context: &PluginContext) -> PluginResult {
		let action = if event.is_applied() { "detected" } else { "reverted" };
		let matches = self.matching_transfers(event.block());
		if matches.is_empty() {
			return Ok(());
		}

		for transfer in &matches {
			debug!(
				action,
				block_number = event.block_number(),
				block_hash = %event.block().block_hash(),
				token = %transfer.token,
				from = %transfer.from,
				to = %transfer.to,
				amount = %transfer.amount,
				transaction_hash = %transfer.transaction_hash,
				log_index = transfer.log_index,
				"large ERC-20 transfer"
			);
		}

		let report = format_transfer_report(event.block(), action, &matches, self.output_format);
		info!(raven_terminal_report = true, "{report}");
		Ok(())
	}

	async fn shutdown(&mut self, _context: &PluginContext) -> PluginResult {
		info!("ERC-20 transfer monitor stopped");
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use alloy_primitives::{Address, B256, U256};
	use alloy_sol_types::SolEvent;
	use raven_core::{BlockEvent, ChainEvent, ChainId, EvmLog};
	use raven_plugin_sdk::{Plugin, PluginContext};

	use super::*;

	const TOKEN: Address = Address::repeat_byte(0x11);
	const OTHER_TOKEN: Address = Address::repeat_byte(0x12);
	const FROM: Address = Address::repeat_byte(0x22);
	const TO: Address = Address::repeat_byte(0x33);

	fn transfer_log(token: Address, amount: u64, log_index: u64) -> EvmLog {
		let encoded = Transfer { from: FROM, to: TO, value: U256::from(amount) }.encode_log_data();
		EvmLog::new(
			token,
			encoded.topics().to_vec(),
			encoded.data,
			B256::repeat_byte(0x44),
			0,
			log_index,
		)
		.unwrap()
	}

	fn block(logs: Vec<EvmLog>) -> BlockEvent {
		BlockEvent::new_with_logs(
			ChainId::new(8453).unwrap(),
			100,
			B256::repeat_byte(0x55),
			B256::repeat_byte(0x54),
			1,
			1,
			logs,
		)
		.unwrap()
	}

	#[test]
	fn matches_inclusive_threshold_and_token_filter() {
		let config = Erc20TransferConfig::new(U256::from(100), [TOKEN]).unwrap();
		let plugin = Erc20TransferPlugin::new(config);
		let block = block(vec![
			transfer_log(TOKEN, 99, 0),
			transfer_log(TOKEN, 100, 1),
			transfer_log(OTHER_TOKEN, 200, 2),
		]);

		let matches = plugin.matching_transfers(&block);
		assert_eq!(matches.len(), 1);
		assert_eq!(matches[0].amount, U256::from(100));
		assert_eq!(matches[0].token, TOKEN);
	}

	#[test]
	fn empty_token_filter_matches_any_contract() {
		let config = Erc20TransferConfig::new(U256::from(1), []).unwrap();
		let plugin = Erc20TransferPlugin::new(config);

		assert_eq!(
			plugin.matching_transfers(&block(vec![transfer_log(OTHER_TOKEN, 2, 0)])).len(),
			1
		);
	}

	#[test]
	fn applies_positionally_paired_token_thresholds() {
		let config = Erc20TransferConfig::new_with_token_thresholds([
			(OTHER_TOKEN, U256::from(500)),
			(TOKEN, U256::from(100)),
		])
		.unwrap();
		assert_eq!(config.token_addresses(), &[TOKEN, OTHER_TOKEN]);
		assert_eq!(config.minimum_amounts(), &[U256::from(100), U256::from(500)]);

		let plugin = Erc20TransferPlugin::new(config);
		let matches = plugin.matching_transfers(&block(vec![
			transfer_log(TOKEN, 99, 0),
			transfer_log(TOKEN, 100, 1),
			transfer_log(OTHER_TOKEN, 499, 2),
			transfer_log(OTHER_TOKEN, 500, 3),
		]));

		assert_eq!(matches.len(), 2);
		assert_eq!((matches[0].token, matches[0].amount), (TOKEN, U256::from(100)));
		assert_eq!((matches[1].token, matches[1].amount), (OTHER_TOKEN, U256::from(500)));
	}

	#[test]
	fn formats_one_readable_report_for_a_matching_block() {
		let block = block(vec![transfer_log(TOKEN, 100, 1), transfer_log(TOKEN, 101, 2)]);
		let transfers =
			Erc20TransferPlugin::new(Erc20TransferConfig::new(U256::from(100), [TOKEN]).unwrap())
				.matching_transfers(&block);

		let report = format_transfer_report(
			&block,
			"detected",
			&transfers,
			Erc20TransferOutputFormat::Short,
		);

		assert!(report.starts_with("+-- ERC-20 transfer matches"));
		assert!(report.contains("block=100 hash=0x5555...555555 action=detected matches=2"));
		assert!(report.contains("|  1. token=0x1111...111111 amount=100 log_index=1"));
		assert!(report.contains("|     from=0x2222...222222 -> to=0x3333...333333"));
		assert!(report.ends_with(
			"+------------------------------------------------------------------------"
		));
	}

	#[test]
	fn long_report_preserves_complete_identifiers_by_default() {
		let block = block(vec![transfer_log(TOKEN, 100, 1)]);
		let transfers =
			Erc20TransferPlugin::new(Erc20TransferConfig::new(U256::from(100), [TOKEN]).unwrap())
				.matching_transfers(&block);

		let report =
			format_transfer_report(&block, "detected", &transfers, Erc20TransferOutputFormat::Long);

		assert!(report.contains(&block.block_hash().to_string()));
		assert!(report.contains(&TOKEN.to_string()));
		assert!(report.contains(&B256::repeat_byte(0x44).to_string()));
		assert!(!report.contains("..."));
	}

	#[test]
	fn includes_every_matching_transfer_in_the_block_report() {
		let transfers = (0..=20)
			.map(|index| LargeTransfer {
				token: TOKEN,
				from: FROM,
				to: TO,
				amount: U256::from(100),
				transaction_hash: B256::repeat_byte(0x44),
				log_index: index as u64,
			})
			.collect::<Vec<_>>();

		let report = format_transfer_report(
			&block(Vec::new()),
			"detected",
			&transfers,
			Erc20TransferOutputFormat::Long,
		);

		assert!(report.contains("|  1. token="));
		assert!(report.contains("| 21. token="));
		assert!(!report.contains("not shown"));
	}

	#[test]
	fn rejects_zero_threshold() {
		assert_eq!(
			Erc20TransferConfig::new(U256::ZERO, []).unwrap_err(),
			Erc20TransferConfigError::ZeroMinimumAmount
		);
		assert_eq!(
			Erc20TransferConfig::new(U256::ZERO, [TOKEN]).unwrap_err(),
			Erc20TransferConfigError::ZeroMinimumAmount
		);
		assert_eq!(
			Erc20TransferConfig::new(U256::ZERO, [TOKEN, OTHER_TOKEN]).unwrap_err(),
			Erc20TransferConfigError::ZeroMinimumAmount
		);
		assert_eq!(
			Erc20TransferConfig::new_with_token_thresholds([(TOKEN, U256::ZERO)]).unwrap_err(),
			Erc20TransferConfigError::ZeroMinimumAmount
		);
	}

	#[test]
	fn rejects_empty_or_duplicate_token_thresholds() {
		assert_eq!(
			Erc20TransferConfig::new_with_token_thresholds([]).unwrap_err(),
			Erc20TransferConfigError::EmptyTokenThresholds
		);
		assert_eq!(
			Erc20TransferConfig::new_with_token_thresholds([
				(TOKEN, U256::from(100)),
				(TOKEN, U256::from(200)),
			])
			.unwrap_err(),
			Erc20TransferConfigError::DuplicateTokenAddress(TOKEN)
		);
	}

	#[tokio::test]
	async fn handles_applied_and_reverted_blocks_from_the_same_payload() {
		let chain_id = ChainId::new(8453).unwrap();
		let context = PluginContext::new(chain_id);
		let mut plugin =
			Erc20TransferPlugin::new(Erc20TransferConfig::new(U256::from(100), [TOKEN]).unwrap());
		let block = block(vec![transfer_log(TOKEN, 100, 0)]);

		plugin
			.handle_event(&ChainEvent::BlockApplied(block.clone()), &context)
			.await
			.unwrap();
		plugin.handle_event(&ChainEvent::BlockReverted(block), &context).await.unwrap();
	}
}
