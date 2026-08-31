use std::path::Path;

use colored::Colorize;

use crate::config::{Erc20TransferSettings, InstalledPlugins};

const LOGO_LINES: [&str; 6] = [
	"██████╗  █████╗ ██╗   ██╗███████╗███╗   ██╗",
	"██╔══██╗██╔══██╗██║   ██║██╔════╝████╗  ██║",
	"██████╔╝███████║██║   ██║█████╗  ██╔██╗ ██║",
	"██╔══██╗██╔══██║╚██╗ ██╔╝██╔══╝  ██║╚██╗██║",
	"██║  ██║██║  ██║ ╚████╔╝ ███████╗██║ ╚████║",
	"╚═╝  ╚═╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═══╝",
];

const LOGO_GRADIENT: [(u8, u8, u8); 6] =
	[(8, 116, 209), (12, 126, 224), (22, 139, 255), (35, 148, 255), (66, 165, 255), (96, 178, 255)];

pub(crate) fn print_error(error: &eyre::Report) {
	let rendered = format!("{error:#}");
	let mut lines = rendered.lines();
	let summary = lines.next().unwrap_or("unknown error");

	eprintln!("{} {}", "✖ Error:".bright_red().bold(), summary.bold());
	for line in lines {
		eprintln!("  {}", line.red());
	}
}

pub(crate) fn print_banner() {
	for (line, (red, green, blue)) in LOGO_LINES.iter().zip(LOGO_GRADIENT) {
		println!("{}", line.truecolor(red, green, blue).bold());
	}

	println!();
	println!(
		"{}",
		"A programmable blockchain event runtime powered by plugins"
			.truecolor(66, 165, 255)
			.bold()
	);
	println!("{}\n", "RPC ingestion  •  normalized events  •  parallel plugins".dimmed());
}

pub(crate) fn print_doctor(
	version: &str,
	rpc_url: &str,
	endpoint: raven_source_alloy::RpcEndpointInfo,
	elapsed: std::time::Duration,
) {
	println!("{} {}", "Raven CLI".bright_blue().bold(), format!("v{version}").bright_cyan());
	println!("{} {}", "status:".blue().bold(), "✔ ready".bright_green().bold());
	print_field("rpc_url", &rpc_url.bright_cyan());
	print_field("transport", &endpoint.transport.to_string().bright_cyan());
	print_field("chain_id", &endpoint.chain_id.get().to_string().bright_yellow());
	print_field("latest_block", &endpoint.latest_block.to_string().bright_yellow());
	print_field("latency_ms", &elapsed.as_millis().to_string().bright_yellow());
}

pub(crate) fn print_plugins(plugins: &InstalledPlugins, details: bool) {
	println!("{}", "Built-in plugins".bright_blue().bold());
	println!(
		"  {} {}  {}",
		"●".bright_green(),
		"block-logger".bright_cyan().bold(),
		"always active with `raven run`".dimmed()
	);
	if details {
		print_detail("origin", "built-in");
		print_detail("arguments", "none");
	}

	println!();
	println!("{}", "Bundled plugins".bright_blue().bold());
	print_bundled_plugin("erc20-transfer", plugins.erc20_transfer().is_some());
	if details {
		print_erc20_details(plugins.erc20_transfer());
	}
	print_bundled_plugin("reorg-monitor", plugins.reorg_monitor_installed());
	if details {
		print_detail("origin", "bundled");
		print_detail("arguments", "none");
	}

	println!();
	println!("{}", "External plugins".bright_blue().bold());
	println!("  {} {}", "○".bright_yellow(), "Loading support is planned".yellow());
}

fn print_bundled_plugin(name: &str, installed: bool) {
	if installed {
		println!("  {} {}  {}", "●".bright_green(), name.bright_cyan().bold(), "installed".green());
	} else {
		println!(
			"  {} {}  {}",
			"○".bright_black(),
			name.bright_black(),
			format!("not installed; use `raven plugins install {name}`").bright_black()
		);
	}
}

fn print_erc20_details(settings: Option<&Erc20TransferSettings>) {
	print_detail("origin", "bundled");
	println!("      {}", "arguments:".blue().bold());
	print_argument(
		"--min-amount <RAW_UNITS>...",
		"required; one shared value or one per token",
		settings.map(|settings| settings.minimum_amounts().join(" ")),
	);
	let saved_tokens = settings.map(|settings| {
		if settings.token_addresses().is_empty() {
			"all token contracts".to_owned()
		} else {
			settings.token_addresses().join(" ")
		}
	});
	print_argument("--token <ADDRESS>...", "optional token filter", saved_tokens);
	let saved_format = settings.map(|settings| {
		if settings.short() {
			"true (short output)".to_owned()
		} else {
			"false (long output)".to_owned()
		}
	});
	print_argument("--short", "optional; long output by default", saved_format);
}

fn print_detail(label: &str, value: &str) {
	println!("      {} {}", format!("{label}:").blue().bold(), value.dimmed());
}

fn print_argument(name: &str, description: &str, saved: Option<String>) {
	println!("        {}  {}", name.bright_cyan(), description.dimmed());
	println!(
		"          {} {}",
		"saved:".blue(),
		saved.unwrap_or_else(|| "not configured".to_owned()).bright_yellow()
	);
}

pub(crate) fn print_plugin_installed(name: &str, path: &Path, replaced: bool) {
	let action = if replaced { "configuration updated" } else { "installed" };
	println!(
		"{} {} {}",
		"✔".bright_green().bold(),
		name.bright_cyan().bold(),
		action.green().bold()
	);
	print_path(path);
}

pub(crate) fn print_plugin_removed(name: &str, path: &Path, removed: bool) {
	if removed {
		println!(
			"{} {} {}",
			"✔".bright_green().bold(),
			name.bright_cyan().bold(),
			"removed".green().bold()
		);
	} else {
		println!("{} {} was not installed", "●".bright_yellow(), name.bright_yellow());
	}
	print_path(path);
}

pub(crate) fn print_config_saved(path: &Path) {
	println!("{} {}", "✔".bright_green().bold(), "RPC URL saved".green().bold());
	print_path(path);
}

pub(crate) fn print_config(rpc_url: Option<&str>, path: &Path) {
	match rpc_url {
		Some(rpc_url) => print_field("rpc_url", &rpc_url.bright_cyan()),
		None => print_field("rpc_url", &"not configured".bright_yellow()),
	}

	print_path(path);
}

pub(crate) fn print_config_cleared(path: &Path, removed: bool) {
	if removed {
		println!(
			"{} {}",
			"✔".bright_green().bold(),
			"Persistent Raven configuration cleared".green().bold()
		);
	} else {
		println!(
			"{} {}",
			"●".bright_yellow(),
			"Persistent Raven configuration is already clear.".yellow()
		);
	}

	print_path(path);
}

fn print_path(path: &Path) {
	print_field("config_file", &path.display().to_string().dimmed());
}

fn print_field(label: &str, value: &impl std::fmt::Display) {
	println!("{} {value}", format!("{label}:").bright_blue().bold());
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn empty_plugin_state_marks_every_bundled_plugin_uninstalled() {
		let plugins = InstalledPlugins::default();

		assert!(plugins.erc20_transfer().is_none());
		assert!(!plugins.reorg_monitor_installed());
	}
}
