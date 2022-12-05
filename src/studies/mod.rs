use tokio::task::JoinHandle;

use serde::{Deserialize, Serialize};

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
	ATRStudy,
	ChoppinessStudy,
}
#[derive(Clone)]
pub struct StudyConfig {
	pub symbol: String,
	pub range: u16,
	pub tf1: u64,
	pub tf2: u64,
	pub tf3: u64,
}

impl From<&StudyConfig> for StudyConfig {
	fn from(config: &StudyConfig) -> Self {
		StudyConfig {
			symbol: config.symbol.clone(),
			range: config.range,
			tf1: config.tf1,
			tf2: config.tf2,
			tf3: config.tf3,
		}
	}
}

pub trait Study {
	const ID: StudyTypes;
	type Entry;
	fn log_history(&self) -> JoinHandle<()>;
	fn get_entry_for_tf(&self, tf: u64) -> Self::Entry;
	fn get_n_entries_for_tf(&self, n: u64, tf: u64) -> Vec<Self::Entry>;
	fn sentiment(&self) -> Sentiment;
	fn sentiment_with_one<T>(&self, other: T) -> Sentiment
		where T: Study;
	fn sentiment_with_two<T, U>(&self, other1: T, other2: U) -> Sentiment
		where T: Study, U: Study;
	
	
}

