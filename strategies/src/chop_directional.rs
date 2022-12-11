
use async_trait::async_trait;
use crate::SignalGenerator;
use binance_q_studies::Study;
use binance_q_types::{StrategyEdge, GlobalConfig};
use binance_q_studies::choppiness_study::ChoppinessStudy;
use binance_q_studies::directional_index_study::DirectionalIndexStudy;

#[derive(Clone)]
pub struct ChopDirectionalStrategy {
	adi_study: DirectionalIndexStudy,
	choppiness_study: ChoppinessStudy,
	global_config: GlobalConfig,
	
}

impl ChopDirectionalStrategy {
	pub fn new(global_config: GlobalConfig, adi_study: DirectionalIndexStudy, choppiness_study: ChoppinessStudy) -> Self {
		Self {
			adi_study,
			choppiness_study,
			global_config,
		}
	}
}


#[async_trait]
impl SignalGenerator for ChopDirectionalStrategy {
	
	async fn get_signal(&self) -> StrategyEdge {
		// calculate here
		let symbol = self.global_config.symbol.clone();
		let chop = self.choppiness_study.get_entry_for_tf(self.global_config.tf1).await;
		let _atr = self.adi_study.get_entry_for_tf(self.global_config.tf3);
		let _tf1 = self.global_config.tf1;
		if chop.value > 60.0 && chop.delta > 20.0 {
			StrategyEdge::Long(symbol.clone(), 70.0)
		} else {
			StrategyEdge::Neutral(symbol.clone(), 70.0)
		}
	}
}