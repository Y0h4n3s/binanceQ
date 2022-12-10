use async_trait::async_trait;
pub mod chop_directional;
pub mod random_strategy;
use dyn_clone::DynClone;
use binance_q_types::StrategyEdge;
#[async_trait]
pub trait SignalGenerator: DynClone + Send + Sync {
	async fn get_signal(&self) -> StrategyEdge;
}

