use async_std::sync::Arc;
use std::time::Duration;
use kanal::AsyncSender;
use crate::{ATRStudy, ChoppinessStudy, EventEmitter, GlobalConfig, Study};
use crate::strategies::{SignalGenerator, StrategyEdge};
use async_trait::async_trait;
use rand::Rng;

#[derive(Clone)]
pub struct RandomStrategy {
	subscribers: Arc<Vec<AsyncSender<StrategyEdge>>>,
	global_config: GlobalConfig,
	signal_interval: u64,
	
}

impl RandomStrategy {
	pub fn new(global_config: GlobalConfig) -> Self {
		Self {
			subscribers: Arc::new(vec![]),
			global_config,
			signal_interval: 2
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