/// Static information describing a Raven plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginMetadata {
	pub name: &'static str,
	pub version: &'static str,
	pub description: &'static str,
}

impl PluginMetadata {
	/// Creates plugin metadata.
	pub const fn new(name: &'static str, version: &'static str, description: &'static str) -> Self {
		Self { name, version, description }
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn creates_plugin_metadata() {
		let metadata =
			PluginMetadata::new("erc20-transfer", "0.1.0", "Processes ERC-20 Transfer events");

		assert_eq!(metadata.name, "erc20-transfer");
		assert_eq!(metadata.version, "0.1.0");
	}
}
