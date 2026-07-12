use clap::{Parser, Subcommand};
use colored::Colorize;
use eyre::Result;

#[derive(Debug, Parser)]
#[command(name = "raven", version, about = "A programmable event engine for EVM chains")]
struct Cli {
	#[command(subcommand)]
	command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
	/// Start processing chain events.
	Run,

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

#[derive(Debug, Subcommand)]
enum PluginCommand {
	/// List installed plugins.
	List,

	/// Install a plugin.
	Install {
		/// Plugin name, for example `erc20-transfer`.
		name: String,
	},

	/// Remove an installed plugin.
	Remove {
		/// Plugin name.
		name: String,
	},
}

#[tokio::main]
async fn main() -> Result<()> {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "raven=info".into()),
		)
		.init();

	let cli = Cli::parse();

	print_banner();

	match cli.command {
		Some(Command::Run) => {
			println!("{}", "Starting Raven...".bright_green().bold());
		},

		Some(Command::Plugins { command }) => match command {
			PluginCommand::List => {
				println!("{}", "No plugins installed.".yellow());
			},

			PluginCommand::Install { name } => {
				println!("{} {}", "Installing plugin".bright_blue(), name.bold());
			},

			PluginCommand::Remove { name } => {
				println!("{} {}", "Removing plugin".bright_red(), name.bold());
			},
		},

		Some(Command::Doctor) => {
			println!("{}", "Raven installation looks healthy.".green());
		},

		Some(Command::Config) => {
			println!("{}", "No configuration file found.".yellow());
		},

		None => {
			println!("{}", "Run `raven --help` to see available commands.".dimmed());
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

	println!("{}\n", "A programmable event engine for EVM chains".bright_cyan().bold());
}
