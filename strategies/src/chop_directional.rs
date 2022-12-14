
use async_trait::async_trait;
use crate::SignalGenerator;
use binance_q_types::{StrategyEdge, GlobalConfig};
use yata::core::Action;
use yata::prelude::*;
#[derive(Clone)]
pub struct ChopDirectionalEntryStrategy {

	global_config: GlobalConfig,
	
}

#[derive(Clone)]
pub struct ChopDirectionalExitStrategy {

	global_config: GlobalConfig,
	
}

impl ChopDirectionalEntryStrategy {
	pub fn new(global_config: GlobalConfig) -> Self {
		Self {

			global_config,
		}
	}
}


impl ChopDirectionalExitStrategy {
	pub fn new(global_config: GlobalConfig,) -> Self {
		Self {

			global_config,
		}
	}
}

#[async_trait]
impl SignalGenerator for ChopDirectionalEntryStrategy {
	fn get_name(&self) -> String {
		"ChopDirectionalEntry".to_string()
	}
	async fn get_signal(&self) -> StrategyEdge {
	
			StrategyEdge::Neutral
	}
}

#[async_trait]
impl SignalGenerator for ChopDirectionalExitStrategy {
	fn get_name(&self) -> String {
		"ChopDirectionalExit".to_string()
	}
	async fn get_signal(&self) -> StrategyEdge {
		
			StrategyEdge::Neutral
	}
}