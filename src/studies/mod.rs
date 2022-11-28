use tokio::task::JoinHandle;

use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use crate::AccessKey;

pub mod oi_study;
pub mod ob_study;
pub mod vol_study;
pub mod atr_study;
pub mod choppiness_study;

pub enum Sentiment {
	VeryBullish,
	Bullish,
	Neutral,
	Bearish,
	VeryBearish,
}
const RANGE: u64 = 9;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
	Bid,
	Ask,
}
pub enum StudyTypes {
	OiStudy,
	ObStudy,
	VolStudy,
	ATRStudy,
	ChoppinessStudy,
}
pub enum IndicatorTypes {
	ATR,
}

pub struct StudyConfig {
	pub symbol: String,
	pub tf1: u64,
	pub tf2: u64,
	pub tf3: u64,
}

impl From<&StudyConfig> for StudyConfig {
	fn from(config: &StudyConfig) -> Self {
		StudyConfig {
			symbol: config.symbol.clone(),
			tf1: config.tf1,
			tf2: config.tf2,
			tf3: config.tf3,
		}
	}
}

#[async_trait]
pub trait Study {
	const ID: StudyTypes;
	type Change;
	fn new(key: AccessKey, config: &StudyConfig) -> Self;
	async fn log_history(&self) -> JoinHandle<()>;
	async fn start_log(&self) -> Vec<JoinHandle<()>>;
	fn get_change(&self) -> Self::Change;
	fn sentiment(&self) -> Sentiment;
	fn sentiment_with_one<T>(&self, other: T) -> Sentiment
		where T: Study;
	fn sentiment_with_two<T, U>(&self, other1: T, other2: U) -> Sentiment
		where T: Study, U: Study;
	
	
}



pub trait Indicator {
	const ID: IndicatorTypes;
	type ValueType;
	fn get_value(&self) -> Self::ValueType;
}