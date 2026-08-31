use std::str::FromStr;

use alloy_primitives::{Address, U256};
use clap::{
	Args, Parser, Subcommand, ValueEnum,
	builder::styling::{AnsiColor, Effects, Styles},
};
use raven_source_alloy::BlockFetchMode;

const RAVEN_STYLES: Styles = Styles::styled()
	.header(AnsiColor::BrightBlue.on_default().effects(Effects::BOLD))
	.usage(AnsiColor::BrightCyan.on_default().effects(Effects::BOLD))
	.literal(AnsiColor::Cyan.on_default().effects(Effects::BOLD))
	.placeholder(AnsiColor::BrightBlack.on_default())
	.error(AnsiColor::BrightRed.on_default().effects(Effects::BOLD))
	.valid(AnsiColor::BrightGreen.on_default().effects(Effects::BOLD))
	.invalid(AnsiColor::BrightYellow.on_default().effects(Effects::BOLD))
	.context(AnsiColor::BrightBlack.on_default())
	.context_value(AnsiColor::BrightCyan.on_default());

/// Raven command-line arguments.
#[derive(Debug, Parser)]
#[command(
	name = "raven",
	version,
	about = "A programmable blockchain event runtime powered by plugins",
	styles = RAVEN_STYLES
)]
pub(crate) struct Cli {
	#[command(subcommand)]
	pub(crate) command: Option<Command>,
}

/// Commands supported by the Raven CLI.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
	/// Process chain events from an EVM JSON-RPC endpoint.
	Run(RunArgs),

	/// Manage Raven plugins.
	#[command(alias = "plugin")]
	Plugins {
		#[command(subcommand)]
		command: PluginCommand,
	},

	/// Inspect the Raven installation and configuration.
	Doctor(DoctorArgs),

	/// Manage persistent Raven configuration.
	Config {
		#[command(subcommand)]
		command: ConfigCommand,
	},
}

/// Configuration for the event-processing loop.
#[derive(Debug, Args)]
pub(crate) struct RunArgs {
	/// EVM-compatible HTTP(S) or WS(S) JSON-RPC endpoint.
	#[arg(long, env = "NODE_RPC_URL")]
	pub(crate) rpc_url: Option<String>,

	/// HTTP(S) block-number polling interval.
	#[arg(long, default_value_t = 4_000, value_parser = clap::value_parser!(u64).range(1..))]
	pub(crate) poll_interval_ms: u64,

	/// WS(S) safety interval for reconciling missed block notifications.
	#[arg(long, default_value_t = 30_000, value_parser = clap::value_parser!(u64).range(1..))]
	pub(crate) reconciliation_interval_ms: u64,

	/// Initial position: `resume`, `latest`, or an inclusive block number.
	#[arg(long, default_value = "resume", value_name = "resume|latest|BLOCK")]
	pub(crate) start: StartArg,

	/// Number of recent canonical blocks retained for shallow reorgs.
	#[arg(long, default_value_t = 64, value_parser = parse_positive_usize)]
	pub(crate) reorg_depth: usize,

	/// Retrieve each block and its logs using `batch` or `sequential` RPC requests.
	#[arg(long, value_enum, default_value = "batch")]
	pub(crate) block_fetch_mode: BlockFetchModeArg,
}

/// RPC checks performed by `raven doctor`.
#[derive(Debug, Args)]
pub(crate) struct DoctorArgs {
	/// EVM-compatible HTTP(S) or WS(S) JSON-RPC endpoint.
	#[arg(long, env = "NODE_RPC_URL")]
	pub(crate) rpc_url: Option<String>,

	/// Maximum time allowed for the connectivity check.
	#[arg(long, default_value_t = 10_000, value_parser = clap::value_parser!(u64).range(1..))]
	pub(crate) timeout_ms: u64,
}

/// User-selected initial event position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartArg {
	Resume,
	Latest,
	Block(u64),
}

/// CLI representation of Raven's block/log RPC retrieval strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum BlockFetchModeArg {
	Batch,
	Sequential,
}

impl From<BlockFetchModeArg> for BlockFetchMode {
	fn from(value: BlockFetchModeArg) -> Self {
		match value {
			BlockFetchModeArg::Batch => Self::Batch,
			BlockFetchModeArg::Sequential => Self::Sequential,
		}
	}
}

impl FromStr for StartArg {
	type Err = String;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		match value {
			"resume" => Ok(Self::Resume),
			"latest" => Ok(Self::Latest),
			value => value
				.parse::<u64>()
				.map(Self::Block)
				.map_err(|_| "expected `resume`, `latest`, or a block number".to_owned()),
		}
	}
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
	let value = value.parse::<usize>().map_err(|_| "expected a positive integer".to_owned())?;
	if value == 0 {
		return Err("value must be greater than zero".to_owned());
	}
	Ok(value)
}

fn parse_positive_u256(value: &str) -> Result<U256, String> {
	let value = value
		.parse::<U256>()
		.map_err(|_| "expected a decimal or 0x-prefixed integer fitting uint256".to_owned())?;
	if value.is_zero() {
		return Err("value must be greater than zero".to_owned());
	}
	Ok(value)
}

/// Persistent-configuration commands.
#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
	/// Save the RPC URL used by `raven run` when no override is provided.
	Set {
		/// EVM-compatible HTTP(S) or WS(S) JSON-RPC endpoint.
		#[arg(long)]
		rpc_url: String,
	},

	/// Print the persisted Raven configuration.
	Get,

	/// Remove all persisted Raven configuration.
	Clear,
}

/// Plugin-management commands.
#[derive(Debug, Subcommand)]
pub(crate) enum PluginCommand {
	/// List bundled and external plugin status.
	List {
		/// Show each plugin's accepted arguments and saved values.
		#[arg(long)]
		details: bool,
	},

	/// Install and persist a bundled plugin configuration.
	Install(PluginInstallArgs),

	/// Remove a bundled plugin configuration.
	Remove {
		/// Plugin name.
		#[arg(value_name = "PLUGIN")]
		name: String,
	},
}

/// Arguments accepted while installing a bundled plugin.
#[derive(Debug, Args)]
pub(crate) struct PluginInstallArgs {
	/// Plugin name, for example `erc20-transfer`.
	#[arg(value_name = "PLUGIN")]
	pub(crate) name: String,

	/// ERC-20 shared threshold or one positional threshold per token.
	#[arg(long, num_args = 1.., value_name = "RAW_UNITS", value_parser = parse_positive_u256)]
	pub(crate) min_amount: Option<Vec<U256>>,

	/// ERC-20 token contracts in threshold-pairing order.
	#[arg(long, num_args = 1.., value_name = "ADDRESS", requires = "min_amount")]
	pub(crate) token: Vec<Address>,

	/// Abbreviate hashes and addresses in ERC-20 terminal reports.
	#[arg(long)]
	pub(crate) short: bool,
}

#[cfg(test)]
mod tests {
	use super::*;
	use clap::{ColorChoice, CommandFactory};

	#[test]
	fn renders_styled_help_when_color_is_enabled() {
		let help = Cli::command().color(ColorChoice::Always).render_help();
		let ansi_help = format!("{}", help.ansi());

		assert!(ansi_help.contains("\u{1b}["));
	}

	#[test]
	fn parses_rpc_run_configuration() {
		let cli = Cli::try_parse_from([
			"raven",
			"run",
			"--rpc-url",
			"http://localhost:8545",
			"--poll-interval-ms",
			"1000",
		])
		.expect("run arguments should parse");

		let Some(Command::Run(args)) = cli.command else {
			panic!("run command should be selected");
		};

		assert_eq!(args.rpc_url.as_deref(), Some("http://localhost:8545"));
		assert_eq!(args.poll_interval_ms, 1_000);
		assert_eq!(args.reconciliation_interval_ms, 30_000);
		assert_eq!(args.start, StartArg::Resume);
		assert_eq!(args.reorg_depth, 64);
		assert_eq!(args.block_fetch_mode, BlockFetchModeArg::Batch);
	}

	#[test]
	fn parses_sequential_block_fetch_mode() {
		let cli =
			Cli::try_parse_from(["raven", "run", "--block-fetch-mode", "sequential"]).unwrap();
		let Some(Command::Run(args)) = cli.command else { panic!("run expected") };

		assert_eq!(args.block_fetch_mode, BlockFetchModeArg::Sequential);
		assert_eq!(BlockFetchMode::from(args.block_fetch_mode), BlockFetchMode::Sequential);
	}

	#[test]
	fn rejects_plugin_configuration_on_run() {
		assert!(Cli::try_parse_from(["raven", "run", "--reorg-monitor"]).is_err());
		assert!(
			Cli::try_parse_from(["raven", "run", "--erc20-transfer-min-amount", "100"]).is_err()
		);
	}

	#[test]
	fn parses_all_start_positions() {
		for (value, expected) in [
			("resume", StartArg::Resume),
			("latest", StartArg::Latest),
			("12345", StartArg::Block(12_345)),
		] {
			let cli = Cli::try_parse_from(["raven", "run", "--start", value]).unwrap();
			let Some(Command::Run(args)) = cli.command else { panic!("run expected") };
			assert_eq!(args.start, expected);
		}
	}

	#[test]
	fn rejects_removed_source_option() {
		let result = Cli::try_parse_from(["raven", "run", "--source", "alloy"]);

		assert!(result.is_err());
	}

	#[test]
	fn rejects_zero_poll_interval() {
		let result = Cli::try_parse_from([
			"raven",
			"run",
			"--rpc-url",
			"http://localhost:8545",
			"--poll-interval-ms",
			"0",
		]);

		assert!(result.is_err());
	}

	#[test]
	fn permits_run_without_rpc_url_for_persisted_config_resolution() {
		let cli = Cli::try_parse_from(["raven", "run"]).expect("config may supply the RPC URL");

		let Some(Command::Run(args)) = cli.command else {
			panic!("run command should be selected");
		};

		assert!(args.rpc_url.is_none());
	}

	#[test]
	fn parses_config_commands() {
		let set =
			Cli::try_parse_from(["raven", "config", "set", "--rpc-url", "wss://ethereum.example"])
				.expect("config set should parse");
		assert!(matches!(
			set.command,
			Some(Command::Config {
				command: ConfigCommand::Set { rpc_url }
			}) if rpc_url == "wss://ethereum.example"
		));

		let get = Cli::try_parse_from(["raven", "config", "get"]).expect("config get should parse");
		assert!(matches!(get.command, Some(Command::Config { command: ConfigCommand::Get })));

		let clear =
			Cli::try_parse_from(["raven", "config", "clear"]).expect("config clear should parse");
		assert!(matches!(clear.command, Some(Command::Config { command: ConfigCommand::Clear })));
	}

	#[test]
	fn rejects_zero_reconciliation_interval() {
		let result = Cli::try_parse_from([
			"raven",
			"run",
			"--rpc-url",
			"ws://localhost:8546",
			"--reconciliation-interval-ms",
			"0",
		]);

		assert!(result.is_err());
	}

	#[test]
	fn parses_erc20_transfer_install_configuration() {
		let cli = Cli::try_parse_from([
			"raven",
			"plugins",
			"install",
			"erc20-transfer",
			"--min-amount",
			"1000000",
			"5000000",
			"--token",
			"0x1111111111111111111111111111111111111111",
			"0x2222222222222222222222222222222222222222",
			"--short",
		])
		.unwrap();
		let Some(Command::Plugins { command: PluginCommand::Install(args) }) = cli.command else {
			panic!("plugin install expected")
		};

		assert_eq!(args.name, "erc20-transfer");
		assert_eq!(args.min_amount, Some(vec![U256::from(1_000_000), U256::from(5_000_000)]));
		assert_eq!(args.token.len(), 2);
		assert!(args.short);
	}

	#[test]
	fn parses_one_erc20_install_threshold_for_multiple_tokens() {
		let cli = Cli::try_parse_from([
			"raven",
			"plugins",
			"install",
			"erc20-transfer",
			"--min-amount",
			"1000000",
			"--token",
			"0x1111111111111111111111111111111111111111",
			"0x2222222222222222222222222222222222222222",
		])
		.unwrap();
		let Some(Command::Plugins { command: PluginCommand::Install(args) }) = cli.command else {
			panic!("plugin install expected")
		};

		assert_eq!(args.min_amount, Some(vec![U256::from(1_000_000)]));
		assert_eq!(args.token.len(), 2);
		assert!(!args.short);
	}

	#[test]
	fn rejects_erc20_install_token_without_threshold_and_zero_threshold() {
		assert!(
			Cli::try_parse_from([
				"raven",
				"plugins",
				"install",
				"erc20-transfer",
				"--token",
				"0x1111111111111111111111111111111111111111",
			])
			.is_err()
		);
		assert!(
			Cli::try_parse_from([
				"raven",
				"plugins",
				"install",
				"erc20-transfer",
				"--min-amount",
				"0",
			])
			.is_err()
		);
	}

	#[test]
	fn parses_plugin_install_name() {
		let cli = Cli::try_parse_from(["raven", "plugins", "install", "whale-detector"])
			.expect("plugin install command should parse");

		let Some(Command::Plugins { command: PluginCommand::Install(args) }) = cli.command else {
			panic!("plugin install command should be selected");
		};

		assert_eq!(args.name, "whale-detector");
	}

	#[test]
	fn parses_plugin_list_details() {
		let cli = Cli::try_parse_from(["raven", "plugins", "list", "--details"]).unwrap();

		assert!(matches!(
			cli.command,
			Some(Command::Plugins { command: PluginCommand::List { details: true } })
		));
	}

	#[test]
	fn parses_doctor_rpc_check_options() {
		let cli = Cli::try_parse_from([
			"raven",
			"doctor",
			"--rpc-url",
			"http://localhost:8545",
			"--timeout-ms",
			"2500",
		])
		.unwrap();
		let Some(Command::Doctor(args)) = cli.command else { panic!("doctor expected") };
		assert_eq!(args.rpc_url.as_deref(), Some("http://localhost:8545"));
		assert_eq!(args.timeout_ms, 2_500);
	}
}
