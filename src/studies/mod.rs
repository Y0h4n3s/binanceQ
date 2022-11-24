pub mod oi_study;
pub mod ob_study;
pub mod vol_study;
pub mod atr_study;

use std::thread::JoinHandle;
use binance::futures::account::FuturesAccount;
use binance::futures::market::FuturesMarket;
use serde::{Serialize, Deserialize};
use crate::AccessKey;
use crate::mongodb::models::TradeEntry;

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
pub trait Study {
	const ID: StudyTypes;
	type Change;
	fn new(key: AccessKey, config: &StudyConfig) -> Self;
	fn log_history(&self) -> JoinHandle<()>;
	
	fn start_log(&self) -> Vec<JoinHandle<()>>;
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