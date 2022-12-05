use tokio::task::JoinHandle;

use futures::{ TryStreamExt};
use async_trait::async_trait;
use kanal::AsyncReceiver;
use crate::{EventSink, StudyConfig};
use crate::helpers::to_tf_chunks;
use crate::mongodb::client::MongoClient;
use crate::mongodb::models::{ChoppinessIndexEntry};
use crate::studies::{RANGE, Sentiment, Study};
use crate::types::TfTrades;
use async_std::sync::Arc;
use crate::events::EventResult;
use crate::studies::Sentiment::Bearish;

#[derive(Clone)]
pub struct ChoppinessStudy {
	config: StudyConfig,
	tf_trades: Arc<AsyncReceiver<TfTrades>>,
	
}

impl ChoppinessStudy {
	pub fn new(config: &StudyConfig, tf_trades: AsyncReceiver<TfTrades>) -> Self {
		Self {
			config: StudyConfig::from(config),
			tf_trades: Arc::new(tf_trades)
		}
		
	}
}

#[async_trait]
impl Study for ChoppinessStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ChoppinessStudy;
	type Entry = ChoppinessIndexEntry;
	

	async fn log_history(&self) -> JoinHandle<()> {
		let timeframes = vec![self.config.tf1, self.config.tf2, self.config.tf3];
		let symbol = self.config.symbol.clone();
		tokio::spawn(async move {
			let mongo_client = MongoClient::new().await;
			if let Ok(past_trades) = mongo_client.trades.find(None, None).await {
				let trades = past_trades.try_collect().await.unwrap_or_else(|_| vec![]);
				for tf in timeframes {
					let mut choppiness_entries: Vec<ChoppinessIndexEntry> = vec![];
					let tf_trades =  to_tf_chunks(tf, trades.clone());
					let true_ranges = to_tf_chunks(tf, trades.clone()).iter().map(|chunk| {
						let mut high: f64 = 0.0;
						let mut low: f64 = f64::MAX;
						for trade in chunk.iter() {
							high = high.max(trade.price);
							low = low.min( trade.price);
						}
						let tr = high - low;
						(tr, chunk.iter().max_by(|a, b| a.price.partial_cmp(&b.price).unwrap()).unwrap().price, chunk.iter().min_by(|a, b| a.price.partial_cmp(&b.price).unwrap()).unwrap().price)
					}).collect::<Vec<(f64, f64, f64)>>();
					let mut i = 0;
					let mut prev_delta = 0.0;
					while i < true_ranges.len() {
						if i < RANGE as usize {
							i += 1;
							continue
						}
						let mut tr_sum = 0.0;
						let mut j = i;
						while j > i - RANGE as usize {
							tr_sum += true_ranges[j].0;
							j -= 1;
						}
						let mut high_sum = 0.0;
						let mut j = i;
						while j > i - RANGE as usize {
							if high_sum < true_ranges[j].1 {
								high_sum = true_ranges[j].1;
							}
							j -= 1;
						}
						let mut low_sum = f64::MAX;
						let mut j = i;
						while j > i - RANGE as usize {
							if low_sum > true_ranges[j].2 {
								low_sum = true_ranges[j].2;
							}
							j -= 1;
						}
						let mut highest_diff = high_sum - low_sum;
						if highest_diff == 0.0 {
							highest_diff = 1.0;
						}
						let choppiness_index = 100.0 * (tr_sum / highest_diff).log10() / (RANGE as f64).log10();
						i += 1;
						let choppiness_entry = ChoppinessIndexEntry {
							symbol: symbol.clone(),
							step_id: i as u64,
							tf,
							value: choppiness_index,
							delta: ((choppiness_index - prev_delta) * 100.0) / prev_delta,
							close_time: tf_trades[i - 1].iter().map(|t| t.timestamp).max().unwrap(),
						};
						prev_delta = choppiness_index;
						choppiness_entries.push(choppiness_entry);
						
					}
					if choppiness_entries.len() == 0 {
						continue
					}
					
					mongo_client.choppiness.insert_many(choppiness_entries, None).await.unwrap();
				}
				
			}
		})
	}
	
	
	fn sentiment(&self) -> Sentiment {
		Sentiment::Neutral
	}
	fn sentiment_with_one<T>(&self, _other: T) -> Sentiment where T: Study {
		
		Bearish
	}
	
	fn sentiment_with_two<T, U>(&self, _other1: T, _other2: U) -> Sentiment where T: Study, U: Study {
		Sentiment::VeryBullish
	}
	
	fn get_entry_for_tf(&self, _tf: u64) -> Self::Entry {
		todo!()
	}
	
	fn get_n_entries_for_tf(&self, _n: u64, _tf: u64) -> Vec<Self::Entry> {
		todo!()
	}
}

#[async_trait]
impl EventSink<TfTrades> for ChoppinessStudy {
	fn get_receiver(&self) -> Arc<AsyncReceiver<TfTrades>> {
		self.tf_trades.clone()
	}
	
	async fn handle_event(&self, _event_msg: TfTrades) -> EventResult {
		todo!()
	}
}

