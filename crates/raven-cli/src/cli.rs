use clap::{Args, Parser, Subcommand, ValueEnum};

/// Raven command-line arguments.
#[derive(Debug, Parser)]
#[command(name = "raven", version, about = "A programmable event engine for EVM chains")]
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

	/// Print Raven configuration.
	Config,
}

/// Configuration for the event-processing loop.
#[derive(Debug, Args)]
pub(crate) struct RunArgs {
	/// Event source used to ingest chain data.
	#[arg(long, value_enum, default_value_t = EventSource::Alloy)]
	pub(crate) source: EventSource,

	/// Ethereum-compatible JSON-RPC endpoint.
	#[arg(long, env = "RAVEN_RPC_URL")]
	pub(crate) rpc_url: String,

	/// Delay between block-number polls.
	#[arg(long, default_value_t = 4_000, value_parser = clap::value_parser!(u64).range(1..))]
	pub(crate) poll_interval_ms: u64,
}

/// Event sources supported by the CLI.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub(crate) enum EventSource {
	/// Poll an Ethereum JSON-RPC endpoint through Alloy.
	#[default]
	Alloy,
}

/// Plugin-management commands.
#[derive(Debug, Subcommand)]
pub(crate) enum PluginCommand {
	/// List installed plugins.
	List,

	/// Install a plugin (planned).
	Install {
		/// Plugin name, for example `erc20-transfer`.
		name: String,
	},

	/// Remove an installed plugin (planned).
	Remove {
		/// Plugin name.
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
		assert_eq!(args.rpc_url, "http://localhost:8545");
		assert_eq!(args.poll_interval_ms, 1_000);
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
}
