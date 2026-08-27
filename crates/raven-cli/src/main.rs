mod checkpoint;
mod cli;
mod config;
mod logging;
mod output;
mod runner;

use std::process::ExitCode;

use clap::{CommandFactory, Parser};
use cli::{Cli, Command, ConfigCommand, DoctorArgs, PluginCommand};
use eyre::{Result, bail};

#[tokio::main]
async fn main() -> ExitCode {
	match run_cli().await {
		Ok(()) => ExitCode::SUCCESS,
		Err(error) => {
			output::print_error(&error);
			ExitCode::FAILURE
		},
	}
}

async fn run_cli() -> Result<()> {
	logging::init();

	let cli = Cli::parse();

	match cli.command {
		Some(Command::Run(args)) => {
			let rpc_url = config::resolve_rpc_url(args.rpc_url.as_deref())?;
			output::print_banner();
			runner::run(args, rpc_url).await?;
		},

		Some(Command::Plugins { command }) => match command {
			PluginCommand::List => {
				output::print_plugins();
			},

			PluginCommand::Install { name } => {
				if is_block_logger(&name) {
					bail!(
						"external plugin installation is not available yet\n  requested: {name}\n  note: `block-logger` is built into `raven run` and needs no installation"
					);
				}

				bail!(
					"external plugin installation is not available yet\n  requested: {name}\n  status: planned"
				);
			},

			PluginCommand::Remove { name } => {
				bail!(
					"external plugin removal is not available yet\n  requested: {name}\n  status: planned"
				);
			},
		},

		Some(Command::Doctor(args)) => {
			handle_doctor(args).await?;
		},

		Some(Command::Config { command }) => handle_config_command(command)?,

		None => {
			output::print_banner();
			let mut command = Cli::command().about(None::<&'static str>);
			command.print_help()?;
			println!();
		},
	}

	Ok(())
}

async fn handle_doctor(args: DoctorArgs) -> Result<()> {
	let rpc_url = config::resolve_rpc_url(args.rpc_url.as_deref())?;
	let started = std::time::Instant::now();
	let endpoint = tokio::time::timeout(
		std::time::Duration::from_millis(args.timeout_ms),
		raven_source_alloy::inspect_rpc_endpoint(&rpc_url),
	)
	.await
	.map_err(|_| eyre::eyre!("RPC check timed out after {} ms", args.timeout_ms))??;
	output::print_doctor(env!("CARGO_PKG_VERSION"), &rpc_url, endpoint, started.elapsed());
	Ok(())
}

fn is_block_logger(name: &str) -> bool {
	matches!(name, "block-logger" | "blocklogger")
}

fn handle_config_command(command: ConfigCommand) -> Result<()> {
	match command {
		ConfigCommand::Set { rpc_url } => {
			let path = config::set_rpc_url(&rpc_url)?;
			output::print_config_saved(&path);
		},
		ConfigCommand::Get => {
			let (path, rpc_url) = config::get_rpc_url()?;
			output::print_config(rpc_url.as_deref(), &path);
		},
		ConfigCommand::Clear => {
			let (path, removed) = config::clear()?;
			output::print_config_cleared(&path, removed);
		},
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn recognizes_block_logger_names() {
		assert!(is_block_logger("block-logger"));
		assert!(is_block_logger("blocklogger"));
		assert!(!is_block_logger("whale-detector"));
	}
}
