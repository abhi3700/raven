use async_trait::async_trait;
use raven_core::ChainEvent;

pub type PluginResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
pub trait Plugin: Send + Sync {
	fn name(&self) -> &'static str;

	fn version(&self) -> &'static str;

	async fn handle_event(&mut self, event: &ChainEvent) -> PluginResult;
}
