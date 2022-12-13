
use async_trait::async_trait;
use rand::Rng;
use binance_q_types::{ExchangeId, StrategyEdge, Symbol};
use crate::SignalGenerator;
#[derive(Clone)]
pub struct RandomStrategy {

}

impl RandomStrategy {
	pub fn new() -> Self {
		Self {
		}
	}
}
#[async_trait]
impl SignalGenerator for RandomStrategy {
	fn get_name(&self) -> String {
		"RandomStrategy".to_string()
	}
	async fn get_signal(&self) -> StrategyEdge {
		let symbol = Symbol {
			symbol: "BTCUSDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let choices = [StrategyEdge::Long(symbol.clone(), 70.0), StrategyEdge::Short(symbol.clone(), 70.0), StrategyEdge::CloseLong(symbol.clone(), 70.0), StrategyEdge::CloseShort(symbol.clone(), 70.0), StrategyEdge::Neutral];
		let mut rng = rand::thread_rng();
		
		return choices[rng.gen_range(0..5)].clone()
	}
}