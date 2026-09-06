mod checkpoint;
mod cli;
mod config;
mod logging;
mod output;
mod runner;

use std::process::ExitCode;

use clap::{CommandFactory, Parser};
use cli::{Cli, Command, ConfigCommand, DoctorArgs, PluginCommand, PluginInstallArgs};
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
			let installed_plugins = config::get_installed_plugins()?;
			output::print_banner();
			runner::run(args, rpc_url, installed_plugins).await?;
		},

		Some(Command::Plugins { command }) => match command {
			PluginCommand::List { details } => {
				let installed_plugins = config::get_installed_plugins()?;
				output::print_plugins(&installed_plugins, details);
			},

			PluginCommand::Install(args) => {
				handle_plugin_install(args)?;
			},

			PluginCommand::Remove { name } => {
				handle_plugin_remove(&name)?;
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
		raven_source_rpc::inspect_rpc_endpoint(&rpc_url),
	)
	.await
	.map_err(|_| eyre::eyre!("RPC check timed out after {} ms", args.timeout_ms))??;
	output::print_doctor(env!("CARGO_PKG_VERSION"), &rpc_url, endpoint, started.elapsed());
	Ok(())
}

fn is_block_logger(name: &str) -> bool {
	matches!(name, "block-logger" | "blocklogger")
}

fn known_plugin_name(name: &str) -> Option<&'static str> {
	if is_block_logger(name) {
		Some("block-logger")
	} else if matches!(name, "erc20-transfer" | "erc20transfer") {
		Some("erc20-transfer")
	} else if matches!(name, "reorg-monitor" | "reorgmonitor") {
		Some("reorg-monitor")
	} else {
		None
	}
}

fn handle_plugin_install(args: PluginInstallArgs) -> Result<()> {
	let PluginInstallArgs { name, min_amount, token, short } = args;
	match known_plugin_name(&name) {
		Some("block-logger") => bail!(
			"`block-logger` is always active with `raven run` and cannot be installed separately"
		),
		Some("erc20-transfer") => {
			let minimum_amounts = min_amount.ok_or_else(|| {
				eyre::eyre!(
					"`erc20-transfer` requires `--min-amount <RAW_UNITS>...` during installation"
				)
			})?;
			let (path, replaced) = config::install_erc20_transfer(&minimum_amounts, &token, short)?;
			output::print_plugin_installed("erc20-transfer", &path, replaced);
		},
		Some("reorg-monitor") => {
			if min_amount.is_some() || !token.is_empty() || short {
				bail!("`reorg-monitor` does not accept `--min-amount`, `--token`, or `--short`");
			}
			let (path, replaced) = config::install_reorg_monitor()?;
			output::print_plugin_installed("reorg-monitor", &path, replaced);
		},
		Some(_) => unreachable!("known plugin names are exhaustive"),
		None => bail!(
			"external plugin installation is not available yet\n  requested: {name}\n  status: loading support is planned"
		),
	}

	Ok(())
}

fn handle_plugin_remove(name: &str) -> Result<()> {
	match known_plugin_name(name) {
		Some("block-logger") =>
			bail!("`block-logger` is always active with `raven run` and cannot be removed"),
		Some("erc20-transfer") => {
			let (path, removed) = config::remove_erc20_transfer()?;
			output::print_plugin_removed("erc20-transfer", &path, removed);
		},
		Some("reorg-monitor") => {
			let (path, removed) = config::remove_reorg_monitor()?;
			output::print_plugin_removed("reorg-monitor", &path, removed);
		},
		Some(_) => unreachable!("known plugin names are exhaustive"),
		None => bail!(
			"external plugin removal is not available yet\n  requested: {name}\n  status: loading support is planned"
		),
	}

	Ok(())
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

	#[test]
	fn recognizes_known_plugin_names() {
		assert_eq!(known_plugin_name("blocklogger"), Some("block-logger"));
		assert_eq!(known_plugin_name("erc20-transfer"), Some("erc20-transfer"));
		assert_eq!(known_plugin_name("reorgmonitor"), Some("reorg-monitor"));
		assert_eq!(known_plugin_name("whale-detector"), None);
	}
}
