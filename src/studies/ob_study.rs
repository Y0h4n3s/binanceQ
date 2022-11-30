use tokio::task::JoinHandle;
use async_trait::async_trait;
use binance::api::Binance;
use binance::futures::market::FuturesMarket;

use crate::{AccessKey, StudyConfig};
use crate::studies::{Sentiment, Study};

pub struct ObStudy {
	market: FuturesMarket,
	key: AccessKey,
	config: StudyConfig
}


#[async_trait]
impl Study for ObStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ObStudy;
	type Entry = (f64, f64);
	
	async fn log_history(&self) -> JoinHandle<()> {
		todo!()
	}
	
	fn get_entry_for_tf(&self, tf: u64) -> Self::Entry {
		todo!()
	}
	fn get_n_entries_for_tf(&self, n: u64, tf: u64) -> Vec<Self::Entry> {
		todo!()
	}
	
	fn sentiment(&self) -> Sentiment {
		Sentiment::Neutral
	}
	
	fn sentiment_with_one<T>(&self, other: T) -> Sentiment where T: Study {

		todo!()
	}
	
	fn sentiment_with_two<T, U>(&self, other1: T, other2: U) -> Sentiment where T: Study, U: Study {
		todo!()
	}
}

