use async_std::sync::Arc;
use std::time::Duration;
use kanal::AsyncSender;
use crate::{ATRStudy, ChoppinessStudy, EventEmitter, GlobalConfig, Study};
use crate::strategies::{SignalGenerator, StrategyEdge};
use async_trait::async_trait;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct ChopDirectionalStrategy {
	subscribers: Arc<Vec<AsyncSender<StrategyEdge>>>,
	atr_study: Box<ATRStudy>,
	choppiness_study: Box<ChoppinessStudy>,
	global_config: GlobalConfig,
	signal_interval: u64,
	
}

impl ChopDirectionalStrategy {
	pub fn new(global_config: GlobalConfig, atr_study: Box<ATRStudy>, choppiness_study: Box<ChoppinessStudy>) -> Self {
		Self {
			subscribers: Arc::new(vec![]),
			atr_study,
			choppiness_study,
			global_config,
			signal_interval: 2
		}
	}
}


#[async_trait]
impl SignalGenerator for ChopDirectionalStrategy {
	
	async fn get_signal(&self) -> StrategyEdge {
		// calculate here
		let chop = self.choppiness_study.get_entry_for_tf(self.global_config.tf3);
		if chop.value > 60.0 && chop.delta > 20.0 {
			StrategyEdge::Long
		} else {
			StrategyEdge::Neutral
		}
	}
}