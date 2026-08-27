use std::{
	fs::{self, OpenOptions},
	io::{self, Write},
	path::{Path, PathBuf},
};

use eyre::{Result, WrapErr, bail};
use serde::{Deserialize, Serialize};
use url::Url;

const CONFIG_FILE_NAME: &str = "config.json";

#[derive(Debug, Default, Deserialize, Serialize)]
struct RavenConfig {
	#[serde(skip_serializing_if = "Option::is_none")]
	rpc_url: Option<String>,
}

/// Resolves the RPC URL using CLI/environment input before persisted config.
pub(crate) fn resolve_rpc_url(cli_or_env_rpc_url: Option<&str>) -> Result<String> {
	resolve_rpc_url_from(cli_or_env_rpc_url, &config_file_path()?)
}

/// Persists an RPC URL and returns the config file path.
pub(crate) fn set_rpc_url(rpc_url: &str) -> Result<PathBuf> {
	let path = config_file_path()?;
	let config = RavenConfig { rpc_url: Some(validate_rpc_url(rpc_url)?) };
	save_to(&path, &config)?;
	Ok(path)
}

/// Returns the config file path and persisted RPC URL, when present.
pub(crate) fn get_rpc_url() -> Result<(PathBuf, Option<String>)> {
	let path = config_file_path()?;
	let config = load_from(&path)?;
	Ok((path, config.rpc_url))
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
		let config = RavenConfig { rpc_url: Some("wss://ethereum.example".to_owned()) };

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
		let config = RavenConfig { rpc_url: Some("https://saved.example".to_owned()) };
		save_to(&path, &config).unwrap();

		let resolved = resolve_rpc_url_from(Some("wss://override.example"), &path).unwrap();

		assert_eq!(resolved, "wss://override.example");
	}

	#[test]
	fn resolves_persisted_rpc_url_when_no_override_exists() {
		let directory = TestDirectory::new();
		let path = directory.path.join(CONFIG_FILE_NAME);
		let config = RavenConfig { rpc_url: Some("https://saved.example".to_owned()) };
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
