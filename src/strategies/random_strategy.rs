use crate::strategies::{SignalGenerator, StrategyEdge};
use async_trait::async_trait;
use rand::Rng;

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
	
	async fn get_signal(&self) -> StrategyEdge {
		let choices = [StrategyEdge::Long, StrategyEdge::Short, StrategyEdge::CloseLong, StrategyEdge::CloseShort, StrategyEdge::Neutral];
		let mut rng = rand::thread_rng();
		
		return choices[rng.gen_range(0..5)]
	}
}