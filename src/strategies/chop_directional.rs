use crate::{ATRStudy, ChoppinessStudy, GlobalConfig, Study};
use crate::strategies::{SignalGenerator, StrategyEdge};
use async_trait::async_trait;

#[derive(Clone)]
pub struct ChopDirectionalStrategy {
	atr_study: ATRStudy,
	choppiness_study: ChoppinessStudy,
	global_config: GlobalConfig,
	
}

impl ChopDirectionalStrategy {
	pub fn new(global_config: GlobalConfig, atr_study: ATRStudy, choppiness_study: ChoppinessStudy) -> Self {
		Self {
			atr_study,
			choppiness_study,
			global_config,
		}
	}
}


#[async_trait]
impl SignalGenerator for ChopDirectionalStrategy {
	
	async fn get_signal(&self) -> StrategyEdge {
		// calculate here
		let chop = self.choppiness_study.get_entry_for_tf(self.global_config.tf3);
		let _atr = self.atr_study.get_entry_for_tf(self.global_config.tf3);
		let _tf1 = self.global_config.tf1;
		if chop.value > 60.0 && chop.delta > 20.0 {
			StrategyEdge::Long
		} else {
			StrategyEdge::Neutral
		}
	}
}