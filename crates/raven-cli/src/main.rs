mod cli;
mod runner;

use clap::Parser;
use cli::{Cli, Command, PluginCommand};
use colored::Colorize;
use eyre::{Result, bail};

#[tokio::main]
async fn main() -> Result<()> {
	init_tracing();

	let cli = Cli::parse();

	print_banner();

	match cli.command {
		Some(Command::Run(args)) => runner::run(args).await?,

		Some(Command::Plugins { command }) => match command {
			PluginCommand::List => {
				println!("{}", "No external plugins installed.".yellow());
			},

			PluginCommand::Install { name } => {
				bail!("plugin installation is planned but not implemented (requested '{name}')");
			},

			PluginCommand::Remove { name } => {
				bail!("plugin removal is planned but not implemented (requested '{name}')");
			},
		},

		Some(Command::Doctor) => {
			println!("{} Raven CLI {}", "✔".green(), env!("CARGO_PKG_VERSION"));
		},

		Some(Command::Config) => {
			print_configuration();
		},

		None => {
			println!("{}", "Run `raven --help` to see available commands.".dimmed());
		},
	}

	Ok(())
}

fn init_tracing() {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "raven=info,raven_source_alloy=info".into()),
		)
		.init();
}

fn print_configuration() {
	match std::env::var("RAVEN_RPC_URL") {
		Ok(rpc_url) => println!("RAVEN_RPC_URL={rpc_url}"),
		Err(_) => println!("{}", "RAVEN_RPC_URL is not set.".yellow()),
	}
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

	println!("{}\n", "A programmable event engine for EVM chains".bright_cyan().bold());
}
