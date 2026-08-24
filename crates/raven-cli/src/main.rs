mod cli;
mod config;
mod runner;

use std::process::ExitCode;

use clap::{CommandFactory, Parser};
use cli::{Cli, Command, ConfigCommand, PluginCommand};
use colored::Colorize;
use eyre::{Result, bail};

#[tokio::main]
async fn main() -> ExitCode {
	match run_cli().await {
		Ok(()) => ExitCode::SUCCESS,
		Err(error) => {
			eprintln!("{} {error:#}", "Error:".red().bold());
			ExitCode::FAILURE
		},
	}
}

async fn run_cli() -> Result<()> {
	init_tracing();

	let cli = Cli::parse();

	match cli.command {
		Some(Command::Run(args)) => {
			let rpc_url = config::resolve_rpc_url(args.rpc_url.as_deref())?;
			print_banner();
			runner::run(args, rpc_url).await?;
		},

		Some(Command::Plugins { command }) => match command {
			PluginCommand::List => {
				print_plugins();
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

		Some(Command::Doctor) => {
			println!("{} Raven CLI {}", "✔".green(), env!("CARGO_PKG_VERSION"));
		},

		Some(Command::Config { command }) => handle_config_command(command)?,

		None => {
			print_banner();
			let mut command = Cli::command().about(None::<&'static str>);
			command.print_help()?;
			println!();
		},
	}

	Ok(())
}

fn print_plugins() {
	println!("{}", "Built-in plugins".bold());
	println!("  {}  bundled with `raven run`", "block-logger".cyan());
	println!();
	println!("{}", "External plugins".bold());
	println!("  {}", "None installed (installation is planned)".dimmed());
}

fn is_block_logger(name: &str) -> bool {
	matches!(name, "block-logger" | "blocklogger")
}

fn init_tracing() {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "raven=info,raven_source_alloy=info".into()),
		)
		.init();
}

fn handle_config_command(command: ConfigCommand) -> Result<()> {
	match command {
		ConfigCommand::Set { rpc_url } => {
			let path = config::set_rpc_url(&rpc_url)?;
			println!("{} RPC URL saved", "✔".green());
			println!("  {}", path.display());
		},
		ConfigCommand::Get => {
			let (path, rpc_url) = config::get_rpc_url()?;
			match rpc_url {
				Some(rpc_url) => println!("rpc_url: {rpc_url}"),
				None => println!("{}", "No persisted RPC URL is configured.".yellow()),
			}
			println!("config_file: {}", path.display());
		},
		ConfigCommand::Clear => {
			let (path, removed) = config::clear()?;
			if removed {
				println!("{} Persistent Raven configuration cleared", "✔".green());
			} else {
				println!("{}", "Persistent Raven configuration is already clear.".yellow());
			}
			println!("  {}", path.display());
		},
	}

	Ok(())
}

fn print_banner() {
	println!(
		"{}",
		r#"
██████╗  █████╗ ██╗   ██╗███████╗███╗   ██╗
██╔══██╗██╔══██╗██║   ██║██╔════╝████╗  ██║
██████╔╝███████║██║   ██║█████╗  ██╔██╗ ██║
██╔══██╗██╔══██║╚██╗ ██╔╝██╔══╝  ██║╚██╗██║
██║  ██║██║  ██║ ╚████╔╝ ███████╗██║ ╚████║
╚═╝  ╚═╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═══╝
"#
		.bright_black()
	);

	println!(
		"{}\n",
		"A programmable blockchain event runtime powered by plugins"
			.bright_cyan()
			.bold()
	);
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
