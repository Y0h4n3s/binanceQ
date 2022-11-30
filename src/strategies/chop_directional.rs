use std::time::Duration;
use kanal::AsyncSender;
use crate::{ATRStudy, ChoppinessStudy, EventEmitter, GlobalConfig, Study};
use crate::strategies::{SignalGenerator, StrategyEdge};
use async_trait::async_trait;

#[derive(Clone)]
pub struct ChopDirectionalStrategy {
	subscribers: Vec<AsyncSender<StrategyEdge>>,
	atr_study: Box<ATRStudy>,
	choppiness_study: Box<ChoppinessStudy>,
	global_config: GlobalConfig,
	signal_interval: u64,
	
}

impl ChopDirectionalStrategy {
	pub fn new(global_config: GlobalConfig, atr_study: Box<ATRStudy>, choppiness_study: Box<ChoppinessStudy>) -> Self {
		Self {
			subscribers: vec![],
			atr_study: atr_study,
			choppiness_study: choppiness_study,
			global_config,
			signal_interval: 2
		}
	}
}

#[async_trait]
impl EventEmitter<StrategyEdge> for ChopDirectionalStrategy {
	fn subscribe(&mut self, sender: AsyncSender<StrategyEdge>) {
		self.subscribers.push(sender)
	}
	
	async fn emit(&self) {
		let signal = self.get_signal().await;
		for subscriber in self.subscribers {
			subscriber.send(signal).await;
		}
		tokio::time::sleep(Duration::from_secs(self.signal_interval)).await;
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