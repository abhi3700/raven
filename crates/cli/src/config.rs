use std::{
	fs::{self, OpenOptions},
	io::{self, Write},
	path::{Path, PathBuf},
};

use alloy_primitives::{Address, U256};
use eyre::{Result, WrapErr, bail};
use raven_plugin_erc20_transfer::{Erc20TransferConfig, Erc20TransferOutputFormat};
use serde::{Deserialize, Serialize};
use url::Url;

const CONFIG_FILE_NAME: &str = "config.json";

#[derive(Debug, Default, Deserialize, Serialize)]
struct RavenConfig {
	#[serde(skip_serializing_if = "Option::is_none")]
	rpc_url: Option<String>,

	#[serde(default, skip_serializing_if = "InstalledPlugins::is_empty")]
	plugins: InstalledPlugins,
}

/// Persisted activation state for plugins bundled with the Raven binary.
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct InstalledPlugins {
	#[serde(rename = "erc20-transfer", skip_serializing_if = "Option::is_none")]
	erc20_transfer: Option<Erc20TransferSettings>,

	#[serde(rename = "reorg-monitor", skip_serializing_if = "Option::is_none")]
	reorg_monitor: Option<ReorgMonitorSettings>,
}

impl InstalledPlugins {
	pub(crate) const fn erc20_transfer(&self) -> Option<&Erc20TransferSettings> {
		self.erc20_transfer.as_ref()
	}

	pub(crate) const fn reorg_monitor_installed(&self) -> bool {
		self.reorg_monitor.is_some()
	}

	fn is_empty(&self) -> bool {
		self.erc20_transfer.is_none() && self.reorg_monitor.is_none()
	}
}

/// Persisted ERC-20 plugin arguments, named after their install flags.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct Erc20TransferSettings {
	min_amount: Vec<String>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	token: Vec<String>,

	#[serde(default, skip_serializing_if = "is_false")]
	short: bool,
}

const fn is_false(value: &bool) -> bool {
	!*value
}

impl Erc20TransferSettings {
	fn new(minimum_amounts: &[U256], token_addresses: &[Address], short: bool) -> Result<Self> {
		let plugin_config = build_erc20_plugin_config(minimum_amounts, token_addresses)?;
		let min_amount = if minimum_amounts.len() == 1 {
			vec![minimum_amounts[0].to_string()]
		} else {
			plugin_config.minimum_amounts().iter().map(ToString::to_string).collect()
		};
		let token = plugin_config.token_addresses().iter().map(ToString::to_string).collect();

		Ok(Self { min_amount, token, short })
	}

	pub(crate) fn plugin_config(&self) -> Result<Erc20TransferConfig> {
		let minimum_amounts = self
			.min_amount
			.iter()
			.map(|value| {
				value
					.parse::<U256>()
					.wrap_err_with(|| format!("invalid persisted ERC-20 minimum amount: {value}"))
			})
			.collect::<Result<Vec<_>>>()?;
		let token_addresses = self
			.token
			.iter()
			.map(|value| {
				value
					.parse::<Address>()
					.wrap_err_with(|| format!("invalid persisted ERC-20 token address: {value}"))
			})
			.collect::<Result<Vec<_>>>()?;

		build_erc20_plugin_config(&minimum_amounts, &token_addresses)
	}

	pub(crate) const fn output_format(&self) -> Erc20TransferOutputFormat {
		if self.short { Erc20TransferOutputFormat::Short } else { Erc20TransferOutputFormat::Long }
	}

	pub(crate) fn minimum_amounts(&self) -> &[String] {
		&self.min_amount
	}

	pub(crate) fn token_addresses(&self) -> &[String] {
		&self.token
	}

	pub(crate) const fn short(&self) -> bool {
		self.short
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
struct ReorgMonitorSettings {}

fn build_erc20_plugin_config(
	minimum_amounts: &[U256],
	token_addresses: &[Address],
) -> Result<Erc20TransferConfig> {
	match minimum_amounts {
		[] => bail!("at least one ERC-20 transfer minimum amount is required"),
		[minimum_amount] =>
			Ok(Erc20TransferConfig::new(*minimum_amount, token_addresses.iter().copied())?),
		_ if minimum_amounts.len() == token_addresses.len() =>
			Ok(Erc20TransferConfig::new_with_token_thresholds(
				token_addresses.iter().copied().zip(minimum_amounts.iter().copied()),
			)?),
		_ => bail!(
			"ERC-20 transfer minimum amounts must contain one shared value or one value per token; got {} amounts and {} tokens",
			minimum_amounts.len(),
			token_addresses.len()
		),
	}
}

/// Resolves the RPC URL using CLI/environment input before persisted config.
pub(crate) fn resolve_rpc_url(cli_or_env_rpc_url: Option<&str>) -> Result<String> {
	resolve_rpc_url_from(cli_or_env_rpc_url, &config_file_path()?)
}

/// Persists an RPC URL and returns the config file path.
pub(crate) fn set_rpc_url(rpc_url: &str) -> Result<PathBuf> {
	let path = config_file_path()?;
	let mut config = load_from(&path)?;
	config.rpc_url = Some(validate_rpc_url(rpc_url)?);
	save_to(&path, &config)?;
	Ok(path)
}

/// Returns the config file path and persisted RPC URL, when present.
pub(crate) fn get_rpc_url() -> Result<(PathBuf, Option<String>)> {
	let path = config_file_path()?;
	let config = load_from(&path)?;
	Ok((path, config.rpc_url))
}

/// Returns plugin activation state saved in Raven's configuration file.
pub(crate) fn get_installed_plugins() -> Result<InstalledPlugins> {
	Ok(load_from(&config_file_path()?)?.plugins)
}

/// Installs or replaces the bundled ERC-20 plugin configuration.
pub(crate) fn install_erc20_transfer(
	minimum_amounts: &[U256],
	token_addresses: &[Address],
	short: bool,
) -> Result<(PathBuf, bool)> {
	let settings = Erc20TransferSettings::new(minimum_amounts, token_addresses, short)?;
	let path = config_file_path()?;
	let mut config = load_from(&path)?;
	let replaced = config.plugins.erc20_transfer.replace(settings).is_some();
	save_to(&path, &config)?;
	Ok((path, replaced))
}

/// Installs the bundled reorg monitor.
pub(crate) fn install_reorg_monitor() -> Result<(PathBuf, bool)> {
	let path = config_file_path()?;
	let mut config = load_from(&path)?;
	let replaced = config.plugins.reorg_monitor.replace(ReorgMonitorSettings {}).is_some();
	save_to(&path, &config)?;
	Ok((path, replaced))
}

/// Removes the bundled ERC-20 plugin configuration.
pub(crate) fn remove_erc20_transfer() -> Result<(PathBuf, bool)> {
	let path = config_file_path()?;
	let mut config = load_from(&path)?;
	let removed = config.plugins.erc20_transfer.take().is_some();
	if removed {
		save_to(&path, &config)?;
	}
	Ok((path, removed))
}

/// Removes the bundled reorg monitor configuration.
pub(crate) fn remove_reorg_monitor() -> Result<(PathBuf, bool)> {
	let path = config_file_path()?;
	let mut config = load_from(&path)?;
	let removed = config.plugins.reorg_monitor.take().is_some();
	if removed {
		save_to(&path, &config)?;
	}
	Ok((path, removed))
}

/// Removes the persisted config file. Clearing an absent file is successful.
pub(crate) fn clear() -> Result<(PathBuf, bool)> {
	let path = config_file_path()?;
	let removed = clear_at(&path)?;
	Ok((path, removed))
}

fn resolve_rpc_url_from(cli_or_env_rpc_url: Option<&str>, path: &Path) -> Result<String> {
	if let Some(rpc_url) = cli_or_env_rpc_url {
		return validate_rpc_url(rpc_url);
	}

	if let Some(rpc_url) = load_from(path)?.rpc_url {
		return validate_rpc_url(&rpc_url).wrap_err("persisted RPC URL is invalid");
	}

	bail!(
		"no RPC URL configured\n  pass `--rpc-url <URL>`, set `NODE_RPC_URL`, or run `raven config set --rpc-url <URL>`"
	)
}

fn validate_rpc_url(rpc_url: &str) -> Result<String> {
	let rpc_url = rpc_url.trim();
	if rpc_url.is_empty() {
		bail!("RPC URL must not be empty");
	}

	let parsed = Url::parse(rpc_url).wrap_err("RPC URL must be an absolute URL")?;
	if !matches!(parsed.scheme(), "http" | "https" | "ws" | "wss") {
		bail!("unsupported RPC URL scheme; expected http, https, ws, or wss");
	}
	if parsed.host_str().is_none() {
		bail!("RPC URL must include a host");
	}

	Ok(rpc_url.to_owned())
}

fn config_file_path() -> Result<PathBuf> {
	Ok(config_directory()?.join(CONFIG_FILE_NAME))
}

pub(crate) fn config_directory() -> Result<PathBuf> {
	if let Some(directory) = non_empty_env_path("RAVEN_CONFIG_DIR") {
		return Ok(directory);
	}
	if let Some(directory) = non_empty_env_path("XDG_CONFIG_HOME") {
		return Ok(directory.join("raven"));
	}

	#[cfg(target_os = "windows")]
	if let Some(directory) = non_empty_env_path("APPDATA") {
		return Ok(directory.join("raven"));
	}

	let home = non_empty_env_path("HOME").ok_or_else(|| {
		eyre::eyre!("cannot locate Raven's config directory; set RAVEN_CONFIG_DIR or HOME")
	})?;

	#[cfg(target_os = "macos")]
	return Ok(home.join("Library").join("Application Support").join("raven"));

	#[cfg(not(any(target_os = "macos", target_os = "windows")))]
	return Ok(home.join(".config").join("raven"));

	#[cfg(target_os = "windows")]
	Ok(home.join("AppData").join("Roaming").join("raven"))
}

fn non_empty_env_path(name: &str) -> Option<PathBuf> {
	std::env::var_os(name).filter(|value| !value.is_empty()).map(PathBuf::from)
}

fn load_from(path: &Path) -> Result<RavenConfig> {
	let contents = match fs::read_to_string(path) {
		Ok(contents) => contents,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(RavenConfig::default()),
		Err(error) => {
			return Err(error)
				.wrap_err_with(|| format!("failed to read config file {}", path.display()));
		},
	};

	serde_json::from_str(&contents)
		.wrap_err_with(|| format!("failed to parse config file {}", path.display()))
}

fn save_to(path: &Path, config: &RavenConfig) -> Result<()> {
	let parent = path.parent().ok_or_else(|| {
		eyre::eyre!("config file path has no parent directory: {}", path.display())
	})?;
	fs::create_dir_all(parent)
		.wrap_err_with(|| format!("failed to create config directory {}", parent.display()))?;

	let mut contents = serde_json::to_vec_pretty(config).wrap_err("failed to serialize config")?;
	contents.push(b'\n');
	let temporary_path = path.with_extension(format!("json.tmp-{}", std::process::id()));

	let write_result = (|| -> Result<()> {
		let mut options = OpenOptions::new();
		options.create(true).truncate(true).write(true);

		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt;
			options.mode(0o600);
		}

		let mut file = options.open(&temporary_path).wrap_err_with(|| {
			format!("failed to create temporary config file {}", temporary_path.display())
		})?;

		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			file.set_permissions(fs::Permissions::from_mode(0o600))
				.wrap_err("failed to secure temporary config file")?;
		}

		file.write_all(&contents).wrap_err("failed to write config")?;
		file.sync_all().wrap_err("failed to sync config")?;

		#[cfg(target_os = "windows")]
		if path.exists() {
			fs::remove_file(path).wrap_err("failed to replace existing config")?;
		}

		fs::rename(&temporary_path, path)
			.wrap_err_with(|| format!("failed to install config file {}", path.display()))?;
		Ok(())
	})();

	if write_result.is_err() {
		let _ = fs::remove_file(&temporary_path);
	}

	write_result
}

fn clear_at(path: &Path) -> Result<bool> {
	match fs::remove_file(path) {
		Ok(()) => Ok(true),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
		Err(error) =>
			Err(error).wrap_err_with(|| format!("failed to clear config file {}", path.display())),
	}
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicU64, Ordering};

	use super::*;

	static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(1);

	#[test]
	fn saves_loads_and_clears_rpc_url() {
		let directory = TestDirectory::new();
		let path = directory.path.join(CONFIG_FILE_NAME);
		let config = RavenConfig {
			rpc_url: Some("wss://ethereum.example".to_owned()),
			..RavenConfig::default()
		};

		save_to(&path, &config).expect("config should save");
		assert_eq!(
			load_from(&path).expect("config should load").rpc_url.as_deref(),
			Some("wss://ethereum.example")
		);

		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
		}

		assert!(clear_at(&path).expect("existing config should clear"));
		assert!(!clear_at(&path).expect("clearing should be idempotent"));
	}

	#[test]
	fn explicit_rpc_url_overrides_persisted_config() {
		let directory = TestDirectory::new();
		let path = directory.path.join(CONFIG_FILE_NAME);
		let config = RavenConfig {
			rpc_url: Some("https://saved.example".to_owned()),
			..RavenConfig::default()
		};
		save_to(&path, &config).unwrap();

		let resolved = resolve_rpc_url_from(Some("wss://override.example"), &path).unwrap();

		assert_eq!(resolved, "wss://override.example");
	}

	#[test]
	fn resolves_persisted_rpc_url_when_no_override_exists() {
		let directory = TestDirectory::new();
		let path = directory.path.join(CONFIG_FILE_NAME);
		let config = RavenConfig {
			rpc_url: Some("https://saved.example".to_owned()),
			..RavenConfig::default()
		};
		save_to(&path, &config).unwrap();

		let resolved = resolve_rpc_url_from(None, &path).unwrap();

		assert_eq!(resolved, "https://saved.example");
	}

	#[test]
	fn missing_rpc_url_returns_actionable_error() {
		let directory = TestDirectory::new();
		let path = directory.path.join(CONFIG_FILE_NAME);

		let error = resolve_rpc_url_from(None, &path).expect_err("missing URL must fail");
		let message = error.to_string();

		assert!(message.contains("--rpc-url"));
		assert!(message.contains("NODE_RPC_URL"));
		assert!(message.contains("raven config set"));
	}

	#[test]
	fn rejects_unsupported_or_incomplete_rpc_urls() {
		for rpc_url in ["", "localhost:8545", "ftp://example.com", "http://"] {
			assert!(validate_rpc_url(rpc_url).is_err(), "{rpc_url:?} should be rejected");
		}
	}

	#[test]
	fn plugin_settings_round_trip_without_overwriting_rpc_url() {
		let directory = TestDirectory::new();
		let path = directory.path.join(CONFIG_FILE_NAME);
		let token = Address::repeat_byte(0x11);
		let settings =
			Erc20TransferSettings::new(&[U256::from(1_000_000)], &[token], true).unwrap();
		let config = RavenConfig {
			rpc_url: Some("https://saved.example".to_owned()),
			plugins: InstalledPlugins {
				erc20_transfer: Some(settings.clone()),
				reorg_monitor: Some(ReorgMonitorSettings {}),
			},
		};

		save_to(&path, &config).unwrap();
		let loaded = load_from(&path).unwrap();

		assert_eq!(loaded.rpc_url.as_deref(), Some("https://saved.example"));
		assert_eq!(loaded.plugins.erc20_transfer(), Some(&settings));
		assert!(loaded.plugins.reorg_monitor_installed());
		assert_eq!(
			settings.plugin_config().unwrap().minimum_amount_for(token),
			Some(U256::from(1_000_000))
		);
		assert_eq!(settings.output_format(), Erc20TransferOutputFormat::Short);
	}

	#[test]
	fn legacy_config_without_plugins_uses_empty_plugin_state() {
		let config: RavenConfig =
			serde_json::from_str(r#"{"rpc_url":"https://saved.example"}"#).unwrap();

		assert!(config.plugins.is_empty());
	}

	#[test]
	fn preserves_positional_erc20_thresholds_while_normalizing_tokens() {
		let token_a = Address::repeat_byte(0x22);
		let token_b = Address::repeat_byte(0x11);
		let amount_a = U256::from(1_000_000);
		let amount_b = U256::from(5_000_000);
		let settings =
			Erc20TransferSettings::new(&[amount_a, amount_b], &[token_a, token_b], false).unwrap();

		let plugin_config = settings.plugin_config().unwrap();

		assert_eq!(plugin_config.minimum_amount_for(token_a), Some(amount_a));
		assert_eq!(plugin_config.minimum_amount_for(token_b), Some(amount_b));
		assert_eq!(settings.token_addresses()[0], token_b.to_string());
	}

	#[test]
	fn rejects_mismatched_erc20_threshold_and_token_counts() {
		let error = Erc20TransferSettings::new(
			&[U256::from(1), U256::from(2)],
			&[Address::repeat_byte(0x11)],
			false,
		)
		.unwrap_err();

		assert!(error.to_string().contains("got 2 amounts and 1 tokens"));
	}

	struct TestDirectory {
		path: PathBuf,
	}

	impl TestDirectory {
		fn new() -> Self {
			let id = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
			let path =
				std::env::temp_dir().join(format!("raven-config-test-{}-{id}", std::process::id()));
			Self { path }
		}
	}

	impl Drop for TestDirectory {
		fn drop(&mut self) {
			let _ = fs::remove_dir_all(&self.path);
		}
	}
}
