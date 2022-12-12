
use async_trait::async_trait;
use crate::SignalGenerator;
use binance_q_studies::Study;
use binance_q_types::{StrategyEdge, GlobalConfig};
use yata::core::Action;
use binance_q_studies::choppiness_study::ChoppinessStudy;
use binance_q_studies::directional_index_study::DirectionalIndexStudy;
use binance_q_utils::helpers::normalize;
use yata::prelude::*;
#[derive(Clone)]
pub struct ChopDirectionalEntryStrategy {
	adi_study: DirectionalIndexStudy,
	choppiness_study: ChoppinessStudy,
	global_config: GlobalConfig,
	
}

#[derive(Clone)]
pub struct ChopDirectionalExitStrategy {
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


impl ChopDirectionalExitStrategy {
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
		let recent_chop = self.choppiness_study.get_n_entries_for_tf(20, self.global_config.tf1).await;
		let recent_adi = self.adi_study.get_n_entries_for_tf(100, self.global_config.tf2).await;
		if recent_chop.is_none() || recent_adi.is_none() {
			return StrategyEdge::Neutral;
		}
		
		let chop = recent_chop.unwrap();
		let adi = recent_adi.unwrap();
		
		if chop.len() < 20 || adi.len() < 100 {
			return StrategyEdge::Neutral;
		}
		
		let chop_above_ninety = chop.iter().filter(|x| x.value > 90.0).count();
		let average_negative_delta = adi.iter().map(|x| x.negative_delta.abs()).sum::<f64>() / adi.len() as f64;
		let average_positive_delta = adi.iter().map(|x| x.positive_delta.abs()).sum::<f64>() / adi.len() as f64;
		let last_adi = adi.last().unwrap();
		if chop_above_ninety > 10 && last_adi.negative_delta > 0.0 && last_adi.negative_delta.abs() > average_negative_delta && last_adi.positive_delta.abs() > average_positive_delta && last_adi.positive_delta < 0.0 {
			let confidence = normalize(chop_above_ninety as f64, 0.0, 20.0);
			println!("Long confidence: {}", confidence);
			StrategyEdge::Long(symbol.clone(), confidence)
		} else if chop_above_ninety > 10 && last_adi.negative_delta < 0.0 && last_adi.negative_delta.abs() > average_negative_delta && last_adi.positive_delta.abs() > average_positive_delta && last_adi.positive_delta > 0.0 {
			let confidence = normalize(chop_above_ninety as f64, 0.0,20.0);
			println!("Short confidence: {}", confidence);
			StrategyEdge::Short(symbol.clone(), confidence)
		} else {
			StrategyEdge::Neutral
		}
	}
}

#[async_trait]
impl SignalGenerator for ChopDirectionalExitStrategy {
	
	async fn get_signal(&self) -> StrategyEdge {
		// calculate here
		let symbol = self.global_config.symbol.clone();
		let adi = self.adi_study.get_n_entries_for_tf(30, self.global_config.tf1).await;
		if adi.is_none() {
			return StrategyEdge::Neutral;
		}
		let mut adi = adi.unwrap();
		if adi.len() < 30 {
			return StrategyEdge::Neutral;
		}
		let last_adi = adi.pop().unwrap();
		let mut cross = yata::methods::Cross::default();
		for val in adi {
			cross.next(&(val.positive, val.negative));
		}
		let adi_cross = cross.next(&(last_adi.positive, last_adi.negative));
		if adi_cross == Action::SELL_ALL{
			StrategyEdge::CloseLong(symbol.clone(), 1.0)
		} else if adi_cross == Action::BUY_ALL  {
			StrategyEdge::CloseShort(symbol.clone(), 1.0)
		} else {
			StrategyEdge::Neutral
		}
	}
}