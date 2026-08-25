use std::path::Path;

use colored::Colorize;

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

pub(crate) fn print_doctor(version: &str) {
	println!("{} {}", "Raven CLI".bright_blue().bold(), format!("v{version}").bright_cyan());
	println!("{} {}", "status:".blue().bold(), "✔ ready".bright_green().bold());
}

pub(crate) fn print_plugins() {
	println!("{}", "Built-in plugins".bright_blue().bold());
	println!(
		"  {} {}  {}",
		"●".bright_green(),
		"block-logger".bright_cyan().bold(),
		"bundled with `raven run`".dimmed()
	);
	println!();
	println!("{}", "External plugins".bright_blue().bold());
	println!("  {} {}", "○".bright_yellow(), "None installed (installation is planned)".yellow());
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
