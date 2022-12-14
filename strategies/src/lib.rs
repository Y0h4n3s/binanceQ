use async_trait::async_trait;
pub mod chop_directional;
pub mod random_strategy;
pub mod cpd;

use dyn_clone::DynClone;
use binance_q_types::StrategyEdge;
#[async_trait]
pub trait SignalGenerator: DynClone + Send + Sync {
	fn get_name(&self) -> String;
	async fn get_signal(&self) -> StrategyEdge;
}

