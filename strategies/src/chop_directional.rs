
use async_trait::async_trait;
use crate::SignalGenerator;
use binance_q_studies::Study;
use binance_q_types::{StrategyEdge, GlobalConfig};
use binance_q_studies::choppiness_study::ChoppinessStudy;
use binance_q_studies::directional_index_study::DirectionalIndexStudy;
use binance_q_utils::helpers::normalize;
#[derive(Clone)]
pub struct ChopDirectionalEntryStrategy {
	adi_study: DirectionalIndexStudy,
	choppiness_study: ChoppinessStudy,
	global_config: GlobalConfig,
	
}

impl ChopDirectionalEntryStrategy {
	pub fn new(global_config: GlobalConfig, adi_study: DirectionalIndexStudy, choppiness_study: ChoppinessStudy) -> Self {
		Self {
			adi_study,
			choppiness_study,
			global_config,
		}
	}
}


#[async_trait]
impl SignalGenerator for ChopDirectionalEntryStrategy {
	
	async fn get_signal(&self) -> StrategyEdge {
		// calculate here
		let symbol = self.global_config.symbol.clone();
		let chop = self.choppiness_study.get_entry_for_tf(self.global_config.tf2).await;
		let adi = self.adi_study.get_entry_for_tf(self.global_config.tf3).await;
		if chop.is_none() || adi.is_none() {
			return StrategyEdge::Neutral;
		}
		let chop = chop.unwrap();
		let adi = adi.unwrap();
		// println!("chop: {:?}, adi: {:?}", chop, adi);
		// println!("adi: {:?}", adi);
		let _tf1 = self.global_config.tf1;
		if chop.value > 60.0 && chop.delta > 20.0 && adi.positive > adi.negative && adi.value > 20.0 && adi.delta > 0.0 {
			let confidence = normalize(chop.value * chop.delta * adi.positive, 0.0, 10000.0 * 100.0);
			StrategyEdge::Long(symbol.clone(), confidence)
		} else if chop.value > 60.0 && chop.delta > 20.0 && adi.positive < adi.negative && adi.value > 20.0 && adi.delta > 0.0 {
			let confidence = normalize(chop.value * chop.delta * adi.negative, 0.0, 10000.0 * 100.0);
			StrategyEdge::Short(symbol.clone(), confidence)
		} else {
			StrategyEdge::Neutral
		}
	}
}