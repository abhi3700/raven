use clap::{Args, Parser, Subcommand, ValueEnum};

/// Raven command-line arguments.
#[derive(Debug, Parser)]
#[command(
	name = "raven",
	version,
	about = "A programmable blockchain event runtime powered by plugins"
)]
pub(crate) struct Cli {
	#[command(subcommand)]
	pub(crate) command: Option<Command>,
}

/// Commands supported by the Raven CLI.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
	/// Start processing chain events.
	Run(RunArgs),

	/// Manage Raven plugins.
	Plugins {
		#[command(subcommand)]
		command: PluginCommand,
	},

	/// Inspect the Raven installation and configuration.
	Doctor,

	/// Manage persistent Raven configuration.
	Config {
		#[command(subcommand)]
		command: ConfigCommand,
	},
}

/// Configuration for the event-processing loop.
#[derive(Debug, Args)]
pub(crate) struct RunArgs {
	/// Event source used to ingest chain data.
	#[arg(long, value_enum, default_value_t = EventSource::Alloy)]
	pub(crate) source: EventSource,

	/// EVM-compatible HTTP(S) or WS(S) JSON-RPC endpoint.
	#[arg(long, env = "NODE_RPC_URL")]
	pub(crate) rpc_url: Option<String>,

	/// HTTP(S) block-number polling interval.
	#[arg(long, default_value_t = 4_000, value_parser = clap::value_parser!(u64).range(1..))]
	pub(crate) poll_interval_ms: u64,

	/// WS(S) safety interval for reconciling missed block notifications.
	#[arg(long, default_value_t = 30_000, value_parser = clap::value_parser!(u64).range(1..))]
	pub(crate) reconciliation_interval_ms: u64,
}

/// Event sources supported by the CLI.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub(crate) enum EventSource {
	/// Ingest an EVM JSON-RPC endpoint through Alloy.
	#[default]
	Alloy,
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
	/// List installed plugins.
	List,

	/// Install a plugin (planned).
	Install {
		/// Plugin name, for example `erc20-transfer`.
		#[arg(value_name = "PLUGIN")]
		name: String,
	},

	/// Remove an installed plugin (planned).
	Remove {
		/// Plugin name.
		#[arg(value_name = "PLUGIN")]
		name: String,
	},
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_alloy_run_configuration() {
		let cli = Cli::try_parse_from([
			"raven",
			"run",
			"--source",
			"alloy",
			"--rpc-url",
			"http://localhost:8545",
			"--poll-interval-ms",
			"1000",
		])
		.expect("run arguments should parse");

		let Some(Command::Run(args)) = cli.command else {
			panic!("run command should be selected");
		};

		assert!(matches!(args.source, EventSource::Alloy));
		assert_eq!(args.rpc_url.as_deref(), Some("http://localhost:8545"));
		assert_eq!(args.poll_interval_ms, 1_000);
		assert_eq!(args.reconciliation_interval_ms, 30_000);
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
	fn parses_plugin_install_name() {
		let cli = Cli::try_parse_from(["raven", "plugins", "install", "whale-detector"])
			.expect("plugin install command should parse");

		let Some(Command::Plugins { command: PluginCommand::Install { name } }) = cli.command
		else {
			panic!("plugin install command should be selected");
		};

		assert_eq!(name, "whale-detector");
	}
}
